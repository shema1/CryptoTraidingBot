"""Інкрементальний backfill даних деривативів (ТЗ §4.4, §15, T1.9).

Символи — `research_symbols`. Ряди 1h: open interest, premium index, mark- і index-свічки, long/short ratio.
Повторне завантаження нічого не змінює: вставка `ON CONFLICT DO NOTHING`. Курсор ряду — `max(ts_ms)`;
хвіст не пишеться раніше за початок. Порожнє вікно курсор не зупиняє: дані біржі можуть починатися пізніше
за лістинг, і ця дірка має лишитися видимою.

Мережа — поза транзакцією. Кожне вікно (одна сторінка ендпоінта) комітиться окремо. Збій біржі на одному
символі не зупиняє інші. Коротший за `INLINE_MAX_BARS` хвіст докачує цей процес; довша історія — задача
`backfill_derivatives` (воркер, частка rate limit `DATA`). Прогалини пише `sync_open_gaps`: період до лістингу
діркою не є, відсутня точка після лістингу — є. Інтерполяції немає.

Глибина історії ендпоінтів — `docs/data/derivatives-history.md`. Підлогу «раніше біржа не віддає» код не
вигадує: якщо точки немає, рядок `data_gaps` лишається відкритим.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Final, Protocol

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from app.core.clock import monotonic_ms, now_ms
from app.core.config import H1_MS, Settings
from app.core.db import SYMBOL_RE
from app.core.logging import get_logger
from app.data.backfill import (
    EXCHANGE,
    GAP_SYNC_ATTEMPTS,
    INLINE_MAX_BARS,
    INSERT_CHUNK_ROWS,
    SYNC_RETRY_BASE_MS,
    SessionFactory,
    _system_jitter,
    backfill_is_due,
    forming_open_ms,
)
from app.data.gaps import find_gaps, is_open_gap_conflict, settled_end_ms, sync_open_gaps
from app.data.models import LongShortRatio as StoredLongShort
from app.data.models import MarkIndexCandle as StoredMark
from app.data.models import OpenInterest as StoredOpenInterest
from app.data.models import PremiumIndex as StoredPremium
from app.exchange.bybit import KLINE_PAGE, OI_PAGE, RATIO_PAGE
from app.exchange.errors import ExchangeError
from app.exchange.instruments import candle_scan_start_ms
from app.exchange.models import Instrument as StoredInstrument
from app.exchange.rate_limit import Jitter
from app.exchange.types import (
    DerivativePriceType,
    LongShortRatio,
    MarkIndexCandle,
    OpenInterest,
    PremiumIndex,
    Timeframe,
)
from app.jobs.models import Job

_SYMBOL: Final = re.compile(SYMBOL_RE)
_LOG = get_logger("app.data")

DERIVATIVES_TIMEFRAME: Final = Timeframe.H1
DERIVATIVES_INTERVAL: Final = DERIVATIVES_TIMEFRAME.value
JOB_KIND_DERIVATIVES: Final = "backfill_derivatives"
DERIVATIVES_DEDUP_KEY: Final = "backfill_derivatives"
# Більше число — пізніше за `backfill_candles` (пріоритет 200): свічки торговому процесу потрібніші.
DERIVATIVES_JOB_PRIORITY: Final = 210
_ACTIVE_JOB_STATUSES: Final = ("QUEUED", "RUNNING")

_Point = OpenInterest | PremiumIndex | MarkIndexCandle | LongShortRatio


class DerivativesSource(Protocol):
    """Читання рядів деривативів. `BybitAdapter` цей протокол задовольняє; пріоритет методів — `DATA`."""

    async def fetch_open_interest(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[OpenInterest]: ...

    async def fetch_premium_index(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[PremiumIndex]: ...

    async def fetch_mark_price_candles(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[MarkIndexCandle]: ...

    async def fetch_index_price_candles(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[MarkIndexCandle]: ...

    async def fetch_long_short_ratio(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[LongShortRatio]: ...


@dataclass(frozen=True, slots=True)
class DerivativesStats:
    """Скільки рядків реально додано. Конфлікт ключа — не додавання."""

    open_interest: int = 0
    premium_index: int = 0
    mark_candles: int = 0
    index_candles: int = 0
    long_short: int = 0

    def __add__(self, other: DerivativesStats) -> DerivativesStats:
        return DerivativesStats(
            open_interest=self.open_interest + other.open_interest,
            premium_index=self.premium_index + other.premium_index,
            mark_candles=self.mark_candles + other.mark_candles,
            index_candles=self.index_candles + other.index_candles,
            long_short=self.long_short + other.long_short,
        )

    def as_dict(self) -> dict[str, int]:
        return {
            "open_interest": self.open_interest,
            "premium_index": self.premium_index,
            "mark_candles": self.mark_candles,
            "index_candles": self.index_candles,
            "long_short": self.long_short,
        }


@dataclass(frozen=True, slots=True)
class DerivativesSyncResult:
    """Підсумок проходу. `failed_symbols` — біржа відповіла помилкою; решту символів прохід докачав."""

    inline: DerivativesStats
    job_id: uuid.UUID | None
    job_symbols: tuple[str, ...]
    failed_symbols: tuple[str, ...]


class DerivativesBackfillIncomplete(Exception):
    """Задачу докачано не для всіх символів. Решта вже в БД. Воркер позначить задачу FAILED."""

    def __init__(self, failed_symbols: tuple[str, ...], stats: DerivativesStats) -> None:
        super().__init__(f"backfill деривативів не завершено для: {', '.join(failed_symbols)}")
        self.failed_symbols = failed_symbols
        self.stats = stats


@dataclass(frozen=True, slots=True)
class _Kind:
    """Один ряд: джерело прогалини, сторінка ендпоінта, тип ціни для `mark_index_candles`."""

    gap_source: str
    page: int
    price_type: str | None = None

    @property
    def stat_name(self) -> str:
        return self.gap_source


_KINDS: Final[tuple[_Kind, ...]] = (
    _Kind("open_interest", OI_PAGE),
    _Kind("premium_index", KLINE_PAGE),
    _Kind("mark_candles", KLINE_PAGE, DerivativePriceType.MARK.value),
    _Kind("index_candles", KLINE_PAGE, DerivativePriceType.INDEX.value),
    _Kind("long_short", RATIO_PAGE),
)


def derivative_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    """Символи навчання без дублів, у заданому порядку. Порожній список — помилка."""
    found: list[str] = []
    for symbol in symbols:
        if type(symbol) is not str or _SYMBOL.fullmatch(symbol) is None:
            raise ValueError(f"symbol не в канонічному форматі CCXT: {symbol!r}")
        if symbol not in found:
            found.append(symbol)
    if not found:
        raise ValueError("немає символів для backfill деривативів")
    return tuple(found)


def derivatives_job_params(symbols: Sequence[str], start_ms: int) -> dict[str, Any]:
    """Параметри задачі `backfill_derivatives`. Кінець діапазону визначає годинник під час виконання."""
    return {"symbols": list(symbols), "interval": DERIVATIVES_INTERVAL, "start_ms": start_ms}


def parse_derivatives_job_params(params: Mapping[str, Any]) -> tuple[tuple[str, ...], int]:
    """Розібрати `jobs.params`. Чужий набір або не 1h — `ValueError` (воркер позначить задачу FAILED)."""
    if set(params) != {"symbols", "interval", "start_ms"}:
        raise ValueError("params деривативів: очікуються рівно symbols, interval, start_ms")
    if params["interval"] != DERIVATIVES_INTERVAL:
        raise ValueError("backfill деривативів підтримує лише інтервал 1h")
    raw_symbols = params["symbols"]
    if type(raw_symbols) is not list or not raw_symbols or any(type(symbol) is not str for symbol in raw_symbols):
        raise ValueError("symbols має бути непорожнім списком рядків")
    symbols = derivative_symbols(tuple(raw_symbols))
    if len(symbols) != len(raw_symbols):
        raise ValueError("symbols: дублікати")
    start_ms = params["start_ms"]
    if type(start_ms) is not int or isinstance(start_ms, bool) or start_ms < 0:
        raise ValueError("start_ms має бути int мс ≥ 0")
    if start_ms % H1_MS != 0:
        raise ValueError("start_ms не вирівняний за 1h")
    return symbols, start_ms


async def sync_derivatives(
    sessions: SessionFactory,
    source: DerivativesSource,
    *,
    symbols: Sequence[str],
    start_ms: int,
    inline_max_bars: int = INLINE_MAX_BARS,
) -> DerivativesSyncResult:
    """Один прохід: короткий хвіст зараз, довга історія — одна задача воркера.

    Задача ставиться до мережі, в одній транзакції з курсорами. Символ, у якого хоч один ряд довший за
    `inline_max_bars`, цілком віддається воркеру (два процеси один ряд не качають). Прогалини звіряються
    для кожного символу, включно з тим, що цей прохід не качав.
    """
    wanted = derivative_symbols(symbols)
    if type(inline_max_bars) is not int or isinstance(inline_max_bars, bool) or inline_max_bars < 1:
        raise ValueError("inline_max_bars має бути int ≥ 1")
    _require_aligned("start_ms", start_ms)
    current = now_ms()
    end = forming_open_ms(current)
    step = DERIVATIVES_TIMEFRAME.duration_ms
    inline: list[str] = []
    queued: list[str] = []
    async with _transaction(sessions) as session:
        for symbol in wanted:
            floor = await _series_floor(session, symbol=symbol, floor_ms=start_ms)
            far = False
            for kind in _KINDS:
                cursor = await _resume_ms(session, kind, symbol, floor_ms=floor, step=step)
                if cursor < end and (end - cursor) // step > inline_max_bars:
                    far = True
                    break
            if far:
                queued.append(symbol)
            else:
                inline.append(symbol)
        job_id: uuid.UUID | None = None
        if queued:
            job_id = await enqueue_derivatives_job(session, symbols=queued, start_ms=start_ms)
    if job_id is not None:
        _LOG.info("derivatives_enqueued", job_id=str(job_id), symbols=queued, start_ms=start_ms)
    total, failed = await _each(sessions, source, inline, start_ms=start_ms, now=current)
    for symbol in wanted:
        await _record_gaps(sessions, symbol=symbol, floor_ms=start_ms, now=current)
    return DerivativesSyncResult(inline=total, job_id=job_id, job_symbols=tuple(queued), failed_symbols=failed)


async def run_derivatives_job(
    sessions: SessionFactory, source: DerivativesSource, params: Mapping[str, Any]
) -> DerivativesStats:
    """Виконати задачу `backfill_derivatives`. Статус рядка `jobs` не змінює — це робить воркер.

    Збій біржі на одному символі не зупиняє інші: наприкінці летить `DerivativesBackfillIncomplete`.
    """
    symbols, start_ms = parse_derivatives_job_params(params)
    total, failed = await _each(sessions, source, list(symbols), start_ms=start_ms, now=now_ms())
    for symbol in symbols:
        await _record_gaps(sessions, symbol=symbol, floor_ms=start_ms, now=now_ms())
    if failed:
        raise DerivativesBackfillIncomplete(failed, total)
    return total


async def enqueue_derivatives_job(session: AsyncSession, *, symbols: Sequence[str], start_ms: int) -> uuid.UUID:
    """Поставити задачу, якщо активної з цим ключем ще немає. Повертає id існуючої або нової."""
    if not symbols:
        raise ValueError("немає символів для задачі деривативів")
    _require_aligned("start_ms", start_ms)
    existing = await _active_job_id(session)
    if existing is not None:
        return existing
    job = Job(
        kind=JOB_KIND_DERIVATIVES,
        priority=DERIVATIVES_JOB_PRIORITY,
        params=derivatives_job_params(symbols, start_ms),
        dedup_key=DERIVATIVES_DEDUP_KEY,
        progress_message=None,
        result=None,
        error=None,
    )
    try:
        async with session.begin_nested():
            session.add(job)
            await session.flush()
    except IntegrityError:
        found = await _active_job_id(session)
        if found is None:
            raise
        return found
    return job.id


class DerivativesSync:
    """Старт і розклад деривативів: перший `run_if_due` одразу, далі кожні `interval_ms` за монотонним годинником.

    Невдалий прохід повторюється через паузу `SYNC_RETRY_BASE_MS` (далі ×2, не довше за інтервал, з jitter),
    а не на кожному виклику. Символи — лише `research_symbols`.
    """

    def __init__(
        self,
        sessions: SessionFactory,
        source: DerivativesSource,
        *,
        symbols: Sequence[str],
        start_ms: int,
        interval_ms: int,
        inline_max_bars: int = INLINE_MAX_BARS,
        jitter: Jitter | None = None,
    ) -> None:
        if type(interval_ms) is not int or isinstance(interval_ms, bool) or interval_ms < 1:
            raise ValueError("interval_ms має бути int ≥ 1")
        self._sessions = sessions
        self._source = source
        self._symbols = derivative_symbols(symbols)
        self._start_ms = start_ms
        self._interval_ms = interval_ms
        self._inline_max_bars = inline_max_bars
        self._jitter = jitter if jitter is not None else _system_jitter()
        self._last_run_ms: int | None = None
        self._delay_ms = interval_ms
        self._failures = 0

    @classmethod
    def from_settings(cls, settings: Settings, sessions: SessionFactory, source: DerivativesSource) -> DerivativesSync:
        return cls(
            sessions,
            source,
            symbols=settings.research_symbols,
            start_ms=settings.backfill_start_ms,
            interval_ms=settings.backfill_interval_s * 1000,
        )

    async def run_if_due(self) -> DerivativesSyncResult | None:
        """Виконати прохід, якщо настав час. `None` — ще рано. Виняток летить далі, повтор — після паузи."""
        started = monotonic_ms()
        if not backfill_is_due(last_run_ms=self._last_run_ms, now=started, interval_ms=self._delay_ms):
            return None
        self._last_run_ms = started
        try:
            result = await sync_derivatives(
                self._sessions,
                self._source,
                symbols=self._symbols,
                start_ms=self._start_ms,
                inline_max_bars=self._inline_max_bars,
            )
        except Exception as exc:
            self._schedule_retry(reason=type(exc).__name__)
            raise
        if result.failed_symbols:
            self._schedule_retry(reason="failed_symbols")
        else:
            self._failures = 0
            self._delay_ms = self._interval_ms
        return result

    def _schedule_retry(self, *, reason: str) -> None:
        self._failures += 1
        step = SYNC_RETRY_BASE_MS << min(self._failures - 1, 16)
        high = min(self._interval_ms, step)
        low = high // 2
        chosen = self._jitter(low, high)
        self._delay_ms = chosen if type(chosen) is int and low <= chosen <= high else high
        _LOG.warning("derivatives_sync_retry", reason=reason, failures=self._failures, retry_in_ms=self._delay_ms)


async def _each(
    sessions: SessionFactory,
    source: DerivativesSource,
    symbols: Sequence[str],
    *,
    start_ms: int,
    now: int,
) -> tuple[DerivativesStats, tuple[str, ...]]:
    total = DerivativesStats()
    failed: list[str] = []
    for symbol in symbols:
        try:
            total += await _backfill_symbol(sessions, source, symbol=symbol, start_ms=start_ms, now=now)
        except ExchangeError as exc:
            _LOG.warning("derivatives_symbol_failed", symbol=symbol, error_type=type(exc).__name__)
            failed.append(symbol)
    return total, tuple(failed)


async def _backfill_symbol(
    sessions: SessionFactory,
    source: DerivativesSource,
    *,
    symbol: str,
    start_ms: int,
    now: int,
) -> DerivativesStats:
    step = DERIVATIVES_TIMEFRAME.duration_ms
    end = forming_open_ms(now)
    stats = DerivativesStats()
    async with _transaction(sessions) as session:
        floor = await _series_floor(session, symbol=symbol, floor_ms=start_ms)
    for kind in _KINDS:
        async with _transaction(sessions) as session:
            cursor = await _resume_ms(session, kind, symbol, floor_ms=floor, step=step)
        inserted = 0
        while cursor < end:
            window_end = min(cursor + kind.page * step, end)
            batch = await _fetch(source, kind, symbol=symbol, start_ms=cursor, end_ms=window_end)
            accepted = _accept(batch, symbol=symbol, cursor=cursor, end=window_end, now=now, price_type=kind.price_type)
            if accepted:
                async with _transaction(sessions) as session:
                    inserted += await _insert(session, kind, accepted)
            cursor = window_end
        stats += _stats_for(kind, inserted)
        if inserted:
            _LOG.info("derivatives_series", symbol=symbol, source=kind.gap_source, inserted=inserted)
    return stats


def _accept(
    batch: Sequence[_Point],
    *,
    symbol: str,
    cursor: int,
    end: int,
    now: int,
    price_type: str | None,
) -> list[_Point]:
    """Точки цього символу й інтервалу в `[cursor, end)`, уже закриті за локальним годинником."""
    kept: dict[int, _Point] = {}
    for point in batch:
        if point.symbol != symbol or point.timeframe is not DERIVATIVES_TIMEFRAME:
            continue
        if price_type is not None and getattr(point, "price_type", None) != price_type:
            continue
        if not cursor <= point.ts_ms < end or point.close_time_ms > now:
            continue
        kept.setdefault(point.ts_ms, point)
    return [kept[ts] for ts in sorted(kept)]


async def _fetch(
    source: DerivativesSource, kind: _Kind, *, symbol: str, start_ms: int, end_ms: int
) -> Sequence[_Point]:
    timeframe = DERIVATIVES_TIMEFRAME
    if kind.gap_source == "open_interest":
        return await source.fetch_open_interest(symbol=symbol, timeframe=timeframe, start_ms=start_ms, end_ms=end_ms)
    if kind.gap_source == "premium_index":
        return await source.fetch_premium_index(symbol=symbol, timeframe=timeframe, start_ms=start_ms, end_ms=end_ms)
    if kind.gap_source == "mark_candles":
        return await source.fetch_mark_price_candles(
            symbol=symbol, timeframe=timeframe, start_ms=start_ms, end_ms=end_ms
        )
    if kind.gap_source == "index_candles":
        return await source.fetch_index_price_candles(
            symbol=symbol, timeframe=timeframe, start_ms=start_ms, end_ms=end_ms
        )
    if kind.gap_source == "long_short":
        return await source.fetch_long_short_ratio(symbol=symbol, timeframe=timeframe, start_ms=start_ms, end_ms=end_ms)
    raise ValueError(f"невідомий ряд деривативів: {kind.gap_source}")


def _stats_for(kind: _Kind, inserted: int) -> DerivativesStats:
    return DerivativesStats(**{kind.gap_source: inserted})


async def _insert(session: AsyncSession, kind: _Kind, points: Sequence[_Point]) -> int:
    rows = [_row(kind, point) for point in points]
    table = _table(kind)
    key = table.ts_ms
    return await _insert_new(session, table, rows, key=key)


def _table(kind: _Kind) -> type[StoredOpenInterest | StoredPremium | StoredMark | StoredLongShort]:
    if kind.gap_source == "open_interest":
        return StoredOpenInterest
    if kind.gap_source == "premium_index":
        return StoredPremium
    if kind.gap_source in ("mark_candles", "index_candles"):
        return StoredMark
    return StoredLongShort


def _row(kind: _Kind, point: _Point) -> dict[str, Any]:
    base: dict[str, Any] = {
        "exchange": EXCHANGE,
        "symbol": point.symbol,
        "interval": point.timeframe.value,
        "ts_ms": point.ts_ms,
    }
    if isinstance(point, OpenInterest):
        return {**base, "open_interest": point.open_interest}
    if isinstance(point, PremiumIndex):
        return {**base, "open": point.open, "high": point.high, "low": point.low, "close": point.close}
    if isinstance(point, MarkIndexCandle):
        return {
            **base,
            "price_type": point.price_type.value,
            "open": point.open,
            "high": point.high,
            "low": point.low,
            "close": point.close,
        }
    if isinstance(point, LongShortRatio):
        return {**base, "buy_ratio": point.buy_ratio, "sell_ratio": point.sell_ratio}
    raise TypeError(f"невідома точка деривативів: {type(point).__name__}")


async def _insert_new(
    session: AsyncSession,
    table: type[Any],
    rows: Sequence[dict[str, Any]],
    *,
    key: InstrumentedAttribute[int],
) -> int:
    inserted = 0
    for offset in range(0, len(rows), INSERT_CHUNK_ROWS):
        chunk = list(rows[offset : offset + INSERT_CHUNK_ROWS])
        statement = pg_insert(table).values(chunk).on_conflict_do_nothing().returning(key)
        result = await session.execute(statement)
        inserted += len(result.scalars().all())
    return inserted


async def _resume_ms(session: AsyncSession, kind: _Kind, symbol: str, *, floor_ms: int, step: int) -> int:
    column = _table(kind).ts_ms
    stmt = select(func.max(column)).where(
        _table(kind).exchange == EXCHANGE,
        _table(kind).symbol == symbol,
        _table(kind).interval == DERIVATIVES_INTERVAL,
    )
    if kind.price_type is not None:
        stmt = stmt.where(StoredMark.price_type == kind.price_type)
    stored = await session.scalar(stmt)
    if stored is None:
        return floor_ms
    if type(stored) is not int:
        raise TypeError(f"ts_ms: очікувався int, отримано {type(stored).__name__}")
    return max(stored + step, floor_ms)


async def _series_floor(session: AsyncSession, *, symbol: str, floor_ms: int) -> int:
    """Не раніше за `floor_ms` і не раніше за найраніший лістинг у кеші. Немає рядка — `floor_ms`."""
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
    return candle_scan_start_ms(floor_ms, launch, step_ms=DERIVATIVES_TIMEFRAME.duration_ms)


async def _record_gaps(sessions: SessionFactory, *, symbol: str, floor_ms: int, now: int) -> None:
    end = settled_end_ms(now, DERIVATIVES_TIMEFRAME)
    for _attempt in range(1, GAP_SYNC_ATTEMPTS + 1):
        try:
            async with _transaction(sessions) as session:
                floor = await _series_floor(session, symbol=symbol, floor_ms=floor_ms)
                for kind in _KINDS:
                    await _sync_kind_gaps(
                        session, kind, symbol=symbol, configured_ms=floor_ms, floor_ms=floor, end_ms=end
                    )
        except IntegrityError as exc:
            if not is_open_gap_conflict(exc):
                raise
            _LOG.warning(
                "data_gap_sync_conflict",
                symbol=symbol,
                attempt=_attempt,
                attempts=GAP_SYNC_ATTEMPTS,
            )
            continue
        return


async def _sync_kind_gaps(
    session: AsyncSession,
    kind: _Kind,
    *,
    symbol: str,
    configured_ms: int,
    floor_ms: int,
    end_ms: int,
) -> None:
    step = DERIVATIVES_TIMEFRAME.duration_ms
    if end_ms <= configured_ms:
        return
    present = await _present_ms(session, kind, symbol, start_ms=floor_ms, end_ms=end_ms)
    off_grid = [ts for ts in present if (ts - floor_ms) % step != 0]
    if off_grid:
        _LOG.warning(
            "data_gap_off_grid",
            source=kind.gap_source,
            exchange=EXCHANGE,
            symbol=symbol,
            timeframe=DERIVATIVES_INTERVAL,
            count=len(off_grid),
            sample_ms=off_grid[:3],
        )
    gaps = () if floor_ms >= end_ms else find_gaps(present, start_ms=floor_ms, end_ms=end_ms, step_ms=step)
    await sync_open_gaps(
        session,
        source=kind.gap_source,
        exchange=EXCHANGE,
        symbol=symbol,
        timeframe=DERIVATIVES_INTERVAL,
        scan_start_ms=configured_ms,
        scan_end_ms=end_ms,
        gaps=gaps,
    )


async def _present_ms(session: AsyncSession, kind: _Kind, symbol: str, *, start_ms: int, end_ms: int) -> list[int]:
    column = _table(kind).ts_ms
    stmt = select(column).where(
        _table(kind).exchange == EXCHANGE,
        _table(kind).symbol == symbol,
        _table(kind).interval == DERIVATIVES_INTERVAL,
        column >= start_ms,
        column < end_ms,
    )
    if kind.price_type is not None:
        stmt = stmt.where(StoredMark.price_type == kind.price_type)
    found = list(await session.scalars(stmt))
    if any(type(ts) is not int for ts in found):
        raise TypeError("ts_ms: очікувався int")
    return found


async def _active_job_id(session: AsyncSession) -> uuid.UUID | None:
    found = await session.scalar(
        select(Job.id).where(Job.dedup_key == DERIVATIVES_DEDUP_KEY, Job.status.in_(_ACTIVE_JOB_STATUSES))
    )
    if found is None:
        return None
    if isinstance(found, uuid.UUID):
        return uuid.UUID(int=found.int)
    try:
        return uuid.UUID(str(found))
    except ValueError as exc:
        raise TypeError(f"jobs.id: очікувався UUID, отримано {type(found).__name__}") from exc


def _require_ms(name: str, value: object) -> int:
    if type(value) is not int or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} має бути int мс ≥ 0")
    return value


def _require_aligned(name: str, value: object) -> int:
    ms = _require_ms(name, value)
    if ms % H1_MS != 0:
        raise ValueError(f"{name} не вирівняний за 1h")
    return ms


@asynccontextmanager
async def _transaction(sessions: SessionFactory) -> Any:
    async with sessions() as session, session.begin():
        yield session
