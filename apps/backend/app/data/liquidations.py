"""Агрегація ліквідацій по годинах і прогалини простою (ТЗ §4.4, T1.10).

Потік Bybit не відтворює історію і не дає id події. Година символу потрапляє в `liquidations_hourly` лише якщо
його підписку підтверджено щонайменше за `CLOCK_MARGIN_MS` до початку години, годиннику весь цей час вірили
(ТЗ §4.3: свіжий вимір `fetch_time`, розбіжність < 1 с, без стрибків) і кадр прийшов щонайменше через
`CLOCK_MARGIN_MS` після її кінця: інакше нуль не відрізнити від простою. Межі години рахуються за локальним
годинником, а події — за часом біржі `T`; запас покриває допустимий зсув між ними. Тиха година, яку прослухали
цілком, пишеться нулями. Недослухана година лишається діркою `data_gaps.source = liquidations`. Повторна вставка —
`ON CONFLICT DO NOTHING`: уже записану годину пізній кадр не переписує (лог `liquidation_late_event`).

`*_qty` — базова валюта, `*_notional` — qty × bankruptcy price у USDT.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Final, Protocol

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import CLOCK_DRIFT_LIMIT_MS, now_ms
from app.core.config import H1_MS
from app.core.logging import get_logger
from app.data.backfill import EXCHANGE, GAP_SYNC_ATTEMPTS, INSERT_CHUNK_ROWS, SessionFactory
from app.data.gaps import find_gaps, is_open_gap_conflict, sync_open_gaps
from app.data.models import LiquidationsHourly
from app.data.time_sync import ClockBlockReason, ClockState
from app.exchange.instruments import candle_scan_start_ms
from app.exchange.liquidations import LiquidationEvent
from app.exchange.models import Instrument as StoredInstrument
from app.exchange.types import Timeframe

_LOG = get_logger(__name__)

LIQUIDATION_SOURCE: Final = "liquidations"
LIQUIDATION_INTERVAL: Final = Timeframe.H1.value
# Довший за ping (20 с): перед закриттям години має встигнути кадр уже після її кінця.
SEAL_MARGIN_MS: Final = 30_000
# Допустимий зсув локального годинника від біржі (ТЗ §4.3): більший `ClockGuard` не вважає перевіреним.
CLOCK_MARGIN_MS: Final = CLOCK_DRIFT_LIMIT_MS
HOUR_MS: Final = H1_MS
# Чому годиннику не можна вірити для меж години. `CLOCK_JUMP_UNRECONCILED` — про звірку рахунку, якої в колектора
# немає; сам стрибок книга бачить за зміною `jump_epoch`.
_CLOCK_UNTRUSTED: Final = frozenset({ClockBlockReason.UNVERIFIED, ClockBlockReason.DRIFT})


class ClockView(Protocol):
    """Стан годинника процесу. Реалізація — `app.data.time_sync.ClockGuard`."""

    def state(self, scope: str | None = None) -> ClockState: ...


def clock_trusted(state: ClockState) -> bool:
    """Чи можна рахувати межі години за локальним годинником: є свіжий вимір і |зсув| + похибка < 1 с."""
    return _CLOCK_UNTRUSTED.isdisjoint(state.block_reasons)


@dataclass(frozen=True, slots=True)
class SealedHour:
    """Повністю прослухана година. Усі лічильники ≥ 0; нулі — година без ліквідацій, не простій."""

    symbol: str
    ts_ms: int
    long_count: int
    long_qty: Decimal
    long_notional: Decimal
    short_count: int
    short_qty: Decimal
    short_notional: Decimal


@dataclass
class _Bucket:
    long_count: int = 0
    long_qty: Decimal = Decimal(0)
    long_notional: Decimal = Decimal(0)
    short_count: int = 0
    short_qty: Decimal = Decimal(0)
    short_notional: Decimal = Decimal(0)

    def add(self, event: LiquidationEvent) -> None:
        notional = event.qty * event.price
        if event.position_side == "long":
            self.long_count += 1
            self.long_qty += event.qty
            self.long_notional += notional
            return
        self.short_count += 1
        self.short_qty += event.qty
        self.short_notional += notional

    def sealed(self, symbol: str, ts_ms: int) -> SealedHour:
        return SealedHour(
            symbol=symbol,
            ts_ms=ts_ms,
            long_count=self.long_count,
            long_qty=self.long_qty,
            long_notional=self.long_notional,
            short_count=self.short_count,
            short_qty=self.short_qty,
            short_notional=self.short_notional,
        )


class LiquidationBook:
    """Пам'ять колектора між записами в БД. Година закривається лише цілком прослухана і не зіпсована кадром.

    Стан годинника (`clock`) читається на кожному виклику: втрата довіри або стрибок (`jump_epoch` змінився)
    відкидають відкриті години, а відлік повних годин починається заново з моменту, коли годиннику знову вірять.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        clock: ClockView,
        seal_margin_ms: int = SEAL_MARGIN_MS,
        clock_margin_ms: int = CLOCK_MARGIN_MS,
    ) -> None:
        tracked = tuple(symbols)
        if not tracked or len(set(tracked)) != len(tracked):
            raise ValueError("symbols має бути непорожнім списком без дублікатів")
        if type(seal_margin_ms) is not int or isinstance(seal_margin_ms, bool) or seal_margin_ms < 0:
            raise ValueError("seal_margin_ms має бути int мс ≥ 0")
        if type(clock_margin_ms) is not int or isinstance(clock_margin_ms, bool) or clock_margin_ms < 0:
            raise ValueError("clock_margin_ms має бути int мс ≥ 0")
        self._symbols = tracked
        self._clock = clock
        self._seal_margin_ms = seal_margin_ms
        self._clock_margin_ms = clock_margin_ms
        self._buckets: dict[tuple[str, int], _Bucket] = {}
        self._tainted: set[tuple[str, int]] = set()
        self._written: set[tuple[str, int]] = set()
        self._ready: dict[tuple[str, int], SealedHour] = {}
        # Символ → коли біржа підтвердила його підписку (локальний час). Лише поточне з'єднання.
        self._listening: dict[str, int] = {}
        # Символ → перша ще не вирішена година, яку можна закрити. Немає ключа — годиннику не віримо або не слухаємо.
        self._next_hour: dict[str, int] = {}
        self._clock_ok = False
        self._clock_epoch: int | None = None
        self._last_inbound_wall: int | None = None

    @property
    def symbols(self) -> tuple[str, ...]:
        return self._symbols

    @property
    def seal_margin_ms(self) -> int:
        return self._seal_margin_ms

    def subscribed(self, symbol: str, at_ms: int) -> None:
        """Підписку символу підтверджено. Година, що вже йшла (або почнеться раніше ніж за запас), не повна."""
        if symbol not in self._symbols:
            raise ValueError(f"символ {symbol!r} не відстежується")
        at = _require_ms("at_ms", at_ms)
        self.observe_clock()
        self._listening[symbol] = at
        self._last_inbound_wall = at
        if self._clock_ok:
            self._next_hour[symbol] = _first_full_hour(at + self._clock_margin_ms)

    def observe_clock(self) -> None:
        """Звірити стан годинника з попереднім. Колектор кличе щосекунди; кожен інший метод — теж."""
        state = self._clock.state()
        trusted = clock_trusted(state)
        if self._clock_ok and trusted and state.jump_epoch == self._clock_epoch:
            return
        if self._clock_ok:
            self._suspend(state)
        if trusted:
            self._resume(state.jump_epoch)

    def note_alive(self) -> None:
        """Будь-який кадр після підписки: доказ, що сокет ще живий."""
        self.observe_clock()
        if self._listening:
            self._last_inbound_wall = now_ms()

    def accept(self, events: Sequence[LiquidationEvent]) -> None:
        """Додати події відкритих годин. Подія вже закритої години — `liquidation_late_event`, рядок не змінюється."""
        self.note_alive()
        for event in events:
            if event.symbol not in self._symbols:
                continue
            hour = hour_start_ms(event.time_ms)
            key = (event.symbol, hour)
            if key in self._written or key in self._ready:
                _LOG.warning(
                    "liquidation_late_event",
                    symbol=event.symbol,
                    ts_ms=event.time_ms,
                    hour_ms=hour,
                    position_side=event.position_side,
                )
                continue
            start = self._next_hour.get(event.symbol)
            if start is None or hour < start or key in self._tainted:
                continue
            self._buckets.setdefault(key, _Bucket()).add(event)

    def mark_unusable(self, hour_ms: int | None) -> None:
        """Кадр, який не розібрали. Цю годину (і поточну, якщо вона інша) нулем не записуємо."""
        self.note_alive()
        hours = {hour_start_ms(now_ms())}
        if hour_ms is not None:
            hours.add(hour_start_ms(_require_ms("hour_ms", hour_ms)))
        for hour in hours:
            for symbol in self._symbols:
                key = (symbol, hour)
                if key in self._written or key in self._ready:
                    continue
                self._tainted.add(key)
                self._buckets.pop(key, None)

    def disconnect(self) -> None:
        """З'єднання впало. Уже доведені години лишаються до запису; решта кадрів години більше не повна."""
        self.capture(now_ms())
        self._listening.clear()
        self._next_hour.clear()
        self._last_inbound_wall = None
        self._buckets.clear()
        self._tainted.clear()

    def capture(self, now: int) -> tuple[SealedHour, ...]:
        """Години, які вже можна писати. Повтор віддає і ті, що ще не підтверджені записом."""
        current = _require_ms("now", now)
        self.observe_clock()
        self._capture(current)
        return tuple(self._ready.values())

    def ack(self, rows: Sequence[SealedHour]) -> None:
        """Запис у БД пройшов. Повторно цю годину не віддаємо."""
        for row in rows:
            key = (row.symbol, row.ts_ms)
            self._ready.pop(key, None)
            self._written.add(key)
            self._buckets.pop(key, None)
            self._tainted.discard(key)

    def open_hours(self) -> tuple[SealedHour, ...]:
        """Ще не закриті кошики (для тестів). Нуль без подій сюди не входить."""
        return tuple(
            bucket.sealed(symbol, hour)
            for (symbol, hour), bucket in sorted(self._buckets.items(), key=lambda item: (item[0][1], item[0][0]))
        )

    def _capture(self, now: int) -> None:
        last = self._last_inbound_wall
        if last is None:
            return
        end = liquidation_scan_end_ms(now, seal_margin_ms=self._seal_margin_ms)
        for symbol, start in list(self._next_hour.items()):
            hour = start
            # Кадр, що прийшов на запас пізніше за кінець години, доводить: сокет жив до її кінця і за часом біржі.
            while hour < end and last >= hour + HOUR_MS + self._clock_margin_ms:
                key = (symbol, hour)
                bucket = self._buckets.pop(key, None)
                if key not in self._written and key not in self._ready and key not in self._tainted:
                    self._ready[key] = (bucket or _Bucket()).sealed(symbol, hour)
                self._tainted.discard(key)
                hour += HOUR_MS
            self._next_hour[symbol] = hour

    def _suspend(self, state: ClockState) -> None:
        """Годиннику більше не віримо (або був стрибок): відкриті години відкидаються, записані й готові лишаються."""
        _LOG.warning(
            "liquidation_clock_untrusted",
            reasons=[reason.value for reason in state.block_reasons],
            jump_epoch=state.jump_epoch,
            offset_ms=state.offset_ms,
            hint="години, що йдуть зараз, не закриваються — стануть прогалинами",
        )
        self._clock_ok = False
        self._next_hour.clear()
        self._buckets.clear()
        self._tainted.clear()

    def _resume(self, epoch: int) -> None:
        """Годиннику знову віримо: повні години для підписаних символів рахуються від цього моменту."""
        self._clock_ok = True
        self._clock_epoch = epoch
        start = _first_full_hour(now_ms() + self._clock_margin_ms)
        for symbol in self._listening:
            self._next_hour[symbol] = start
        _LOG.info("liquidation_clock_trusted", jump_epoch=epoch, listening=len(self._listening))


def hour_start_ms(ts_ms: int) -> int:
    """Початок години UTC, якій належить `ts_ms`."""
    ts = _require_ms("ts_ms", ts_ms)
    return ts // HOUR_MS * HOUR_MS


def liquidation_scan_end_ms(now: int, *, seal_margin_ms: int = SEAL_MARGIN_MS) -> int:
    """Кінець сітки прогалин: години, чий кінець уже старший за `seal_margin_ms`.

    Поточна година і щойно закрита, по якій ще не було кадру після межі, у сітку не входять.
    """
    current = _require_ms("now", now)
    if type(seal_margin_ms) is not int or isinstance(seal_margin_ms, bool) or seal_margin_ms < 0:
        raise ValueError("seal_margin_ms має бути int мс ≥ 0")
    matured = current - seal_margin_ms
    if matured < HOUR_MS:
        return 0
    return (matured // HOUR_MS) * HOUR_MS


def _first_full_hour(up_since: int) -> int:
    hour = hour_start_ms(up_since)
    if up_since == hour:
        return hour
    return hour + HOUR_MS


async def save_liquidations(sessions: SessionFactory, rows: Sequence[SealedHour]) -> int:
    """Вставити закриті години. Уже наявний рядок не змінюється; повертає кількість нових."""
    if not rows:
        return 0
    payload = [_row(item) for item in rows]
    inserted = 0
    async with sessions() as session, session.begin():
        for offset in range(0, len(payload), INSERT_CHUNK_ROWS):
            chunk = payload[offset : offset + INSERT_CHUNK_ROWS]
            statement = (
                pg_insert(LiquidationsHourly).values(chunk).on_conflict_do_nothing().returning(LiquidationsHourly.ts_ms)
            )
            result = await session.execute(statement)
            inserted += len(result.scalars().all())
    return inserted


async def sync_liquidation_gaps(
    sessions: SessionFactory,
    *,
    symbols: Sequence[str],
    floor_ms: int,
    now: int,
    seal_margin_ms: int = SEAL_MARGIN_MS,
) -> None:
    """Звести відкриті прогалини `liquidations` до годин без рядка в `[floor, scan_end)`.

    Період до лістингу діркою не є, щойно в кеші `instruments` є `launch_time_ms` (як у свічок). Немає рядка
    кешу — сітка з `floor_ms`. Зіткнення на `uq_data_gaps_open` повторюється один раз.
    """
    end = liquidation_scan_end_ms(now, seal_margin_ms=seal_margin_ms)
    configured = _align_forward(_require_ms("floor_ms", floor_ms), HOUR_MS)
    if configured >= end:
        return
    for symbol in symbols:
        await _sync_symbol(sessions, symbol=symbol, configured_ms=configured, end_ms=end)


async def _sync_symbol(sessions: SessionFactory, *, symbol: str, configured_ms: int, end_ms: int) -> None:
    for attempt in range(1, GAP_SYNC_ATTEMPTS + 1):
        try:
            async with sessions() as session, session.begin():
                listed = await _listing_floor(session, symbol=symbol, floor_ms=configured_ms)
                present = await _stored_hours(session, symbol=symbol, start_ms=listed, end_ms=end_ms)
                off_grid = [ts for ts in present if (ts - listed) % HOUR_MS != 0]
                if off_grid:
                    _LOG.warning(
                        "data_gap_off_grid",
                        source=LIQUIDATION_SOURCE,
                        symbol=symbol,
                        count=len(off_grid),
                        sample_ms=off_grid[:3],
                    )
                gaps = find_gaps(present, start_ms=listed, end_ms=end_ms, step_ms=HOUR_MS) if listed < end_ms else ()
                await sync_open_gaps(
                    session,
                    source=LIQUIDATION_SOURCE,
                    exchange=EXCHANGE,
                    symbol=symbol,
                    timeframe=LIQUIDATION_INTERVAL,
                    scan_start_ms=configured_ms,
                    scan_end_ms=end_ms,
                    gaps=gaps,
                )
        except IntegrityError as exc:
            if not is_open_gap_conflict(exc):
                raise
            _LOG.warning(
                "data_gap_sync_conflict",
                source=LIQUIDATION_SOURCE,
                symbol=symbol,
                attempt=attempt,
                attempts=GAP_SYNC_ATTEMPTS,
            )
            continue
        return
    _LOG.warning(
        "data_gap_sync_conflict",
        source=LIQUIDATION_SOURCE,
        symbol=symbol,
        attempt=GAP_SYNC_ATTEMPTS,
        attempts=GAP_SYNC_ATTEMPTS,
    )


async def _listing_floor(session: AsyncSession, *, symbol: str, floor_ms: int) -> int:
    launches = list(
        await session.scalars(
            select(StoredInstrument.launch_time_ms).where(
                StoredInstrument.exchange == EXCHANGE, StoredInstrument.symbol == symbol
            )
        )
    )
    if not launches:
        return floor_ms
    launch = min(launches)
    if len(set(launches)) > 1:
        _LOG.info("instrument_launch_modes_differ", symbol=symbol, launch_time_ms=launch, rows=len(launches))
    return candle_scan_start_ms(floor_ms, launch, step_ms=HOUR_MS)


async def _stored_hours(session: AsyncSession, *, symbol: str, start_ms: int, end_ms: int) -> list[int]:
    if start_ms >= end_ms:
        return []
    rows = await session.scalars(
        select(LiquidationsHourly.ts_ms).where(
            LiquidationsHourly.exchange == EXCHANGE,
            LiquidationsHourly.symbol == symbol,
            LiquidationsHourly.interval == LIQUIDATION_INTERVAL,
            LiquidationsHourly.ts_ms >= start_ms,
            LiquidationsHourly.ts_ms < end_ms,
        )
    )
    found: list[int] = []
    for ts in rows:
        if type(ts) is not int:
            raise TypeError(f"ts_ms: очікувався int, отримано {type(ts).__name__}")
        found.append(ts)
    return found


def _row(item: SealedHour) -> Mapping[str, object]:
    return {
        "exchange": EXCHANGE,
        "symbol": item.symbol,
        "interval": LIQUIDATION_INTERVAL,
        "ts_ms": item.ts_ms,
        "long_count": item.long_count,
        "long_qty": item.long_qty,
        "long_notional": item.long_notional,
        "short_count": item.short_count,
        "short_qty": item.short_qty,
        "short_notional": item.short_notional,
    }


def _align_forward(ms: int, step: int) -> int:
    remainder = ms % step
    if remainder == 0:
        return ms
    return ms + (step - remainder)


def _require_ms(name: str, value: int) -> int:
    if type(value) is not int or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} має бути int мс ≥ 0")
    return value
