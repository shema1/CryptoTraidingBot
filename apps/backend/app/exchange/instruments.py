"""Кеш правил інструментів (ТЗ §8.3, §15, T1.7).

Знімок `fetch_instruments` пишеться в `instruments` (PK `exchange, mode, symbol`). Оновлення — при старті
(`refresh`), коли рядки старші за `INSTRUMENT_MAX_AGE_MS` (24 год, `refresh_if_due`) і після відхилення
ордера кодом із `INSTRUMENT_REFRESH_REJECTION_CODES` (`refresh_after_rejection`). Мережа — поза транзакцією.

Один `(exchange, mode)` оновлюється по черзі (`pg_advisory_lock`, сесійний, не транзакційний): знімок, який
почав читати біржу раніше, не може закомітитися після новішого і повернути бота в `RUNNING`.

Порожній повний знімок кеш не чіпає (`EmptyInstrumentSnapshot`): інакше збій розбору стер би всі правила.
Символ, якого в повному знімку вже немає, лишається в таблиці зі статусом `Absent` (час лістингу зберігається
для скану прогалин) і не торгується. Торгується лише статус `Trading`.

`entry_block` — чи можна рахувати новий вхід за цим рядком. Застарілі правила блокують вхід, але не ставлять
паузу: окремої причини в ТЗ §6.1 немає. Паузу `INSTRUMENT_NOT_TRADING` ставить `apply_instrument_pauses`.

Початок скану свічок — `candle_scan_start_ms`: не раніше за `backfill_start_ms` і не раніше за першу точку
сітки, що ≥ `launchTime` (година, що почалась до лістингу, діркою не є).
"""

from __future__ import annotations

import re
import zlib
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, Literal, Protocol

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker

from app.core.clock import now_ms
from app.core.db import MODES, SYMBOL_RE
from app.core.logging import get_logger
from app.exchange.models import EXCHANGES
from app.exchange.models import Instrument as StoredInstrument
from app.exchange.types import Instrument
from app.trading.instrument_status import PauseChange, apply_instrument_pauses, log_instrument_pauses

_LOG = get_logger("app.exchange")
_SYMBOL: Final = re.compile(SYMBOL_RE)
# Сесійний advisory lock на час знімка. Не `LOCK_NAMESPACE` торгового процесу (`app.core.instance_lock`):
# той тримає один бекенд, цей — лише чергу оновлення інструментів одного режиму.
_LOCK_NAMESPACE: Final = 0x63746201

# Bybit `status`. Торгується лише цей; PreLaunch, PendingOpen, Delivering, Closed, Settling — ні
# (документація v5/enum, context7 2026-10-30). Невідомий статус теж не торгується (fail-closed).
TRADING_STATUS: Final = "Trading"
# Локальний статус: символ зник із повного знімка. Не зі словника біржі.
ABSENT_STATUS: Final = "Absent"
# ТЗ §15: оновлення раз на добу. Рівно 24 год — уже час оновити.
INSTRUMENT_MAX_AGE_MS: Final = 24 * 60 * 60 * 1000

# Коди Bybit v5, після яких правила інструмента треба перечитати (документація v5/error, context7 2026-10-30).
# Фільтр ціни, кількості або номіналу — повторення зі старим кешем відхилиться знову. `30228` — делістинг:
# нових позицій немає, статус уже не Trading. `10001` сюди не входить: це будь-яка помилка параметра.
INSTRUMENT_REFRESH_REJECTION_CODES: Final[frozenset[str]] = frozenset(
    {
        "110003",  # order price exceeds the allowable range
        "110017",  # orderQty will be truncated to zero
        "110094",  # order notional below the lower limit
        "30208",  # order price higher than the maximum buying price
        "30209",  # order price lower than the minimum selling price
        "30228",  # no new positions during delisting
    }
)

RefreshReason = Literal["startup", "scheduled", "filter_rejection"]

_RULE_COLUMNS: Final[tuple[str, ...]] = (
    "tick_size",
    "qty_step",
    "min_qty",
    "min_notional",
    "max_qty",
    "max_leverage",
    "launch_time_ms",
    "status",
    "updated_at_ms",
)


class InstrumentEntryBlock(StrEnum):
    """Чому нові входи за цим інструментом заборонені. Захист і закриття цей код не читають."""

    NOT_TRADABLE = "INSTRUMENT_NOT_TRADING"
    STALE = "INSTRUMENT_RULES_STALE"
    MISSING = "INSTRUMENT_UNKNOWN"


class EmptyInstrumentSnapshot(Exception):
    """Повний знімок не містить жодного інструмента. Кеш і паузи не змінюються."""


class InstrumentSource(Protocol):
    """Хто віддає правила. `BybitAdapter` і `FakeExchangeAdapter` це вміють."""

    async def fetch_instruments(self, *, symbols: Collection[str] | None = None) -> list[Instrument]:
        """`symbols=None` — усі; інакше лише перелічені (невідомий символ відсутній)."""
        ...


@dataclass(frozen=True)
class InstrumentSyncResult:
    """Що змінив один успішний знімок."""

    reason: RefreshReason
    upserted: int
    absent: tuple[str, ...]
    pauses: tuple[PauseChange, ...]


def is_tradable(status: str) -> bool:
    """Чи біржа приймає нові ордери. Лише `Trading`; решта, включно з `Absent`, — ні."""
    return status == TRADING_STATUS


def is_instrument_refresh_rejection(code: str) -> bool:
    """Чи відхилення ордера з цим кодом біржі має перечитати кеш інструментів."""
    if type(code) is not str:
        raise TypeError("code має бути str")
    return code in INSTRUMENT_REFRESH_REJECTION_CODES


def entry_block(*, status: str | None, updated_at_ms: int | None, now_ms_value: int) -> InstrumentEntryBlock | None:
    """Блок нового входу за рядком кешу. `None` — правила свіжі й інструмент торгується.

    Немає рядка — `MISSING`. Не `Trading` — `NOT_TRADABLE` (навіть якщо рядок свіжий). Свіжий `Trading`,
    але старший за добу або з міткою з майбутнього (годинник відкотився) — `STALE`.
    """
    _require_ms("now_ms", now_ms_value)
    if (status is None) != (updated_at_ms is None):
        raise ValueError("status і updated_at_ms задаються разом")
    if status is None or updated_at_ms is None:
        return InstrumentEntryBlock.MISSING
    if not is_tradable(status):
        return InstrumentEntryBlock.NOT_TRADABLE
    if updated_at_ms > now_ms_value or now_ms_value - updated_at_ms >= INSTRUMENT_MAX_AGE_MS:
        return InstrumentEntryBlock.STALE
    return None


def candle_scan_start_ms(backfill_start_ms: int, launch_time_ms: int, *, step_ms: int) -> int:
    """Початок сітки свічок: не раніше за історію конфігу і не раніше за першу точку ≥ лістингу.

    `launch_time_ms` всередині години зсувається вперед до наступної точки `step_ms`: свічка, що відкрилась
    до лістингу, не є діркою. Якщо `backfill_start_ms` пізніший — береться він.
    """
    _require_ms("backfill_start_ms", backfill_start_ms)
    _require_ms("launch_time_ms", launch_time_ms)
    if type(step_ms) is not int or isinstance(step_ms, bool) or step_ms < 1:
        raise ValueError("step_ms має бути int ≥ 1")
    remainder = launch_time_ms % step_ms
    listed = launch_time_ms if remainder == 0 else launch_time_ms + (step_ms - remainder)
    return max(backfill_start_ms, listed)


class InstrumentCache:
    """Знімок правил одного режиму біржі й пауза ботів, чий інструмент не торгується.

    `symbols` — торговий і дослідницькі: за їхньою свіжістю вирішується `refresh_if_due`, а якщо повний
    знімок їх не містить, боти цих символів стають на паузу.
    """

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        source: InstrumentSource,
        *,
        exchange: str,
        mode: str,
        symbols: Sequence[str],
    ) -> None:
        if exchange not in EXCHANGES:
            raise ValueError(f"exchange має бути одним із {EXCHANGES}")
        if mode not in MODES:
            raise ValueError(f"mode має бути одним із {MODES}")
        tracked = tuple(symbols)
        if not tracked or len(set(tracked)) != len(tracked):
            raise ValueError("symbols має бути непорожнім списком без дублікатів")
        for symbol in tracked:
            if type(symbol) is not str or _SYMBOL.fullmatch(symbol) is None:
                raise ValueError(f"symbol не в канонічному форматі CCXT: {symbol}")
        self._sessions = sessions
        self._source = source
        self._exchange = exchange
        self._mode = mode
        self._symbols = tracked

    async def refresh(self, *, reason: RefreshReason, symbols: Collection[str] | None = None) -> InstrumentSyncResult:
        """Перечитати правила і звести паузи. `symbols=None` — повний знімок категорії."""
        if reason not in ("startup", "scheduled", "filter_rejection"):
            raise ValueError("reason має бути startup, scheduled або filter_rejection")
        requested = _requested(symbols)
        bind = self._sessions.kw["bind"]
        if isinstance(bind, AsyncConnection):
            async with self._sessions() as session:
                return await self._refresh_locked(session, reason=reason, requested=requested)
        if not isinstance(bind, AsyncEngine):
            raise TypeError("InstrumentCache потребує sessionmaker з engine або connection")
        # Окреме з'єднання тримається відкритим на час читання біржі: commit lock-а не повертає його в пул
        # (інакше NullPool закрив би сесію і відпустив advisory lock до запису).
        async with bind.connect() as connection:
            await connection.execute(
                text("SELECT pg_advisory_lock(:namespace, :lock_id)"),
                {"namespace": _LOCK_NAMESPACE, "lock_id": _refresh_lock_id(self._exchange, self._mode)},
            )
            await connection.commit()
            try:
                session = AsyncSession(bind=connection, expire_on_commit=False)
                try:
                    return await self._refresh_fetched(session, reason=reason, requested=requested)
                finally:
                    await session.close()
            finally:
                await connection.execute(
                    text("SELECT pg_advisory_unlock(:namespace, :lock_id)"),
                    {"namespace": _LOCK_NAMESPACE, "lock_id": _refresh_lock_id(self._exchange, self._mode)},
                )
                await connection.commit()

    async def _refresh_locked(
        self,
        session: AsyncSession,
        *,
        reason: RefreshReason,
        requested: tuple[str, ...] | None,
    ) -> InstrumentSyncResult:
        """Знімок на вже відкритому з'єднанні (тестова транзакція). Lock переживає savepoint."""
        await _lock_refresh(session, exchange=self._exchange, mode=self._mode)
        try:
            return await self._refresh_fetched(session, reason=reason, requested=requested)
        finally:
            await _unlock_refresh(session, exchange=self._exchange, mode=self._mode)

    async def _refresh_fetched(
        self,
        session: AsyncSession,
        *,
        reason: RefreshReason,
        requested: tuple[str, ...] | None,
    ) -> InstrumentSyncResult:
        fetched = await self._source.fetch_instruments(symbols=requested)
        found = _unique(fetched)
        if requested is None and not found:
            raise EmptyInstrumentSnapshot("повний знімок інструментів порожній")
        current = now_ms()
        async with session.begin():
            absent = await _store(
                session,
                exchange=self._exchange,
                mode=self._mode,
                found=found,
                requested=requested,
                now_ms_value=current,
            )
            tradable = {item.symbol: is_tradable(item.status) for item in found}
            for symbol in absent:
                tradable[symbol] = False
            scope = self._symbols if requested is None else requested
            for symbol in scope:
                tradable.setdefault(symbol, False)
            pauses = await apply_instrument_pauses(
                session, mode=self._mode, tradable_by_symbol=tradable, now_ms_value=current
            )
        log_instrument_pauses(pauses, mode=self._mode, tradable_by_symbol=tradable)
        _LOG.info(
            "instruments_refreshed",
            reason=reason,
            exchange=self._exchange,
            mode=self._mode,
            upserted=len(found),
            absent=len(absent),
            pauses=len(pauses),
        )
        return InstrumentSyncResult(reason=reason, upserted=len(found), absent=absent, pauses=pauses)

    async def refresh_if_due(self) -> InstrumentSyncResult | None:
        """Повний знімок, якщо якогось символу з `symbols` немає або йому ≥ 24 год. Інакше `None`."""
        current = now_ms()
        async with self._sessions() as session:
            due = await _rules_due(
                session,
                exchange=self._exchange,
                mode=self._mode,
                symbols=self._symbols,
                now_ms_value=current,
            )
        if not due:
            return None
        return await self.refresh(reason="scheduled")

    async def refresh_after_rejection(self, *, code: str, symbol: str) -> InstrumentSyncResult | None:
        """Перечитати один символ, якщо `code` — відхилення через фільтр або делістинг. Інакше `None`."""
        if not is_instrument_refresh_rejection(code):
            return None
        if type(symbol) is not str or _SYMBOL.fullmatch(symbol) is None:
            raise ValueError(f"symbol не в канонічному форматі CCXT: {symbol}")
        return await self.refresh(reason="filter_rejection", symbols=(symbol,))


def _requested(symbols: Collection[str] | None) -> tuple[str, ...] | None:
    if symbols is None:
        return None
    requested = tuple(symbols)
    if not requested:
        raise ValueError("symbols порожній: для повного знімка передайте None")
    return requested


def _unique(fetched: Sequence[Instrument]) -> tuple[Instrument, ...]:
    by_symbol: dict[str, Instrument] = {}
    for item in fetched:
        if type(item) is not Instrument:
            raise TypeError("fetch_instruments має повертати Instrument")
        previous = by_symbol.get(item.symbol)
        if previous is not None and previous != item:
            raise ValueError(f"суперечливі правила {item.symbol}")
        by_symbol[item.symbol] = item
    return tuple(by_symbol[key] for key in sorted(by_symbol))


async def _store(
    session: AsyncSession,
    *,
    exchange: str,
    mode: str,
    found: Sequence[Instrument],
    requested: Sequence[str] | None,
    now_ms_value: int,
) -> tuple[str, ...]:
    """Записати знайдені правила. Повертає символи, чиї рядки переведено в `Absent`."""
    if found:
        rows = [_row(item, exchange=exchange, mode=mode, now_ms_value=now_ms_value) for item in found]
        insert_stmt = pg_insert(StoredInstrument).values(rows)
        excluded = insert_stmt.excluded
        await session.execute(
            insert_stmt.on_conflict_do_update(
                index_elements=["exchange", "mode", "symbol"],
                set_={column: excluded[column] for column in _RULE_COLUMNS},
            )
        )
    present = {item.symbol for item in found}
    if requested is None:
        stored = set(
            await session.scalars(
                select(StoredInstrument.symbol).where(
                    StoredInstrument.exchange == exchange, StoredInstrument.mode == mode
                )
            )
        )
        absent = tuple(sorted(stored - present))
    else:
        # Точковий знімок не чіпає інші символи. Немає рядка — пауза без `Absent` (немає правил, які зберігати).
        missing = tuple(symbol for symbol in requested if symbol not in present)
        existing = (
            set(
                await session.scalars(
                    select(StoredInstrument.symbol).where(
                        StoredInstrument.exchange == exchange,
                        StoredInstrument.mode == mode,
                        StoredInstrument.symbol.in_(missing),
                    )
                )
            )
            if missing
            else set()
        )
        absent = tuple(sorted(existing))
    if absent:
        await session.execute(
            update(StoredInstrument)
            .where(
                StoredInstrument.exchange == exchange,
                StoredInstrument.mode == mode,
                StoredInstrument.symbol.in_(absent),
            )
            .values(status=ABSENT_STATUS, updated_at_ms=now_ms_value)
        )
    return absent


def _row(item: Instrument, *, exchange: str, mode: str, now_ms_value: int) -> dict[str, object]:
    return {
        "exchange": exchange,
        "mode": mode,
        "symbol": item.symbol,
        "tick_size": item.tick_size,
        "qty_step": item.qty_step,
        "min_qty": item.min_qty,
        "min_notional": item.min_notional,
        "max_qty": item.max_qty,
        "max_leverage": item.max_leverage,
        "launch_time_ms": item.launch_time_ms,
        "status": item.status,
        "updated_at_ms": now_ms_value,
    }


async def _rules_due(
    session: AsyncSession,
    *,
    exchange: str,
    mode: str,
    symbols: Sequence[str],
    now_ms_value: int,
) -> bool:
    rows = (
        await session.execute(
            select(StoredInstrument.symbol, StoredInstrument.updated_at_ms).where(
                StoredInstrument.exchange == exchange,
                StoredInstrument.mode == mode,
                StoredInstrument.symbol.in_(symbols),
            )
        )
    ).all()
    updated: dict[str, int] = {}
    for symbol, stamp in rows:
        updated[symbol] = stamp
    for symbol in symbols:
        stamp = updated.get(symbol)
        if stamp is None or stamp > now_ms_value or now_ms_value - stamp >= INSTRUMENT_MAX_AGE_MS:
            return True
    return False


def _refresh_lock_id(exchange: str, mode: str) -> int:
    """Стабільний int4 для `pg_advisory_lock`. 0 не використовується."""
    raw = zlib.crc32(f"{exchange}\0{mode}".encode()) & 0x7FFFFFFF
    return raw or 1


async def _lock_refresh(session: AsyncSession, *, exchange: str, mode: str) -> None:
    """Зайняти чергу знімка і закрити транзакцію до мережі. Lock лишається на сесії."""
    await session.execute(
        text("SELECT pg_advisory_lock(:namespace, :lock_id)"),
        {"namespace": _LOCK_NAMESPACE, "lock_id": _refresh_lock_id(exchange, mode)},
    )
    await session.commit()


async def _unlock_refresh(session: AsyncSession, *, exchange: str, mode: str) -> None:
    """Відпустити чергу. Відкриту транзакцію спершу відкотити, зовнішню транзакцію з'єднання не чіпати."""
    if session.in_transaction():
        await session.rollback()
    await session.execute(
        text("SELECT pg_advisory_unlock(:namespace, :lock_id)"),
        {"namespace": _LOCK_NAMESPACE, "lock_id": _refresh_lock_id(exchange, mode)},
    )
    await session.commit()


def _require_ms(name: str, value: object) -> int:
    if type(value) is not int or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} має бути int мс ≥ 0")
    return value
