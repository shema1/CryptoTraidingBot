"""Кеш інструментів: знімок, пауза, добове оновлення, відхилення фільтром (T1.7)."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Collection
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker
from sqlalchemy.sql.dml import Update

from app.core.clock import FakeClock
from app.exchange.errors import ExchangeTimeout
from app.exchange.instruments import _LOCK_NAMESPACE as INSTRUMENT_LOCK_NAMESPACE
from app.exchange.instruments import (
    ABSENT_STATUS,
    INSTRUMENT_MAX_AGE_MS,
    EmptyInstrumentSnapshot,
    InstrumentCache,
    InstrumentSource,
    InstrumentSyncResult,
)
from app.exchange.models import ExchangeAccount
from app.exchange.models import Instrument as StoredInstrument
from app.exchange.types import Instrument
from app.trading.instrument_status import INSTRUMENT_NOT_TRADING, InstrumentPauseConflict
from app.trading.models import Bot
from tests.db.dbutil import make_engine
from tests.db.factories import make_account, make_bot
from tests.exchange.factories import LAUNCH_MS, SYMBOL

pytestmark = pytest.mark.integration


def _sessions(conn: AsyncConnection) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")


def _rules(*, status: str = "Trading", tick: str = "0.1", symbol: str = SYMBOL) -> Instrument:
    return Instrument(
        symbol=symbol,
        tick_size=Decimal(tick),
        qty_step=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("5"),
        max_qty=Decimal("100"),
        max_leverage=Decimal("100"),
        launch_time_ms=LAUNCH_MS,
        status=status,
    )


class _Source:
    def __init__(self, batches: list[list[Instrument]]) -> None:
        self.batches = batches
        self.calls: list[tuple[str, ...] | None] = []

    async def fetch_instruments(self, *, symbols: Collection[str] | None = None) -> list[Instrument]:
        self.calls.append(None if symbols is None else tuple(symbols))
        batch = self.batches.pop(0)
        if symbols is None:
            return list(batch)
        wanted = set(symbols)
        return [item for item in batch if item.symbol in wanted]


class _Down:
    async def fetch_instruments(self, *, symbols: Collection[str] | None = None) -> list[Instrument]:
        raise ExchangeTimeout("down")


def _cache(conn: AsyncConnection, source: InstrumentSource, *, symbols: tuple[str, ...] = (SYMBOL,)) -> InstrumentCache:
    return InstrumentCache(_sessions(conn), source, exchange="bybit", mode="demo", symbols=symbols)


async def _stored(conn: AsyncConnection, symbol: str = SYMBOL) -> tuple[str, Decimal, int]:
    row = (
        await conn.execute(
            select(StoredInstrument.status, StoredInstrument.tick_size, StoredInstrument.launch_time_ms).where(
                StoredInstrument.symbol == symbol
            )
        )
    ).one()
    return row.status, row.tick_size, row.launch_time_ms


async def _bot_state(conn: AsyncConnection, bot_id: object) -> tuple[str, list[str], int]:
    row = (await conn.execute(select(Bot.status, Bot.pause_reasons, Bot.version).where(Bot.id == bot_id))).one()
    return row.status, list(row.pause_reasons), row.version


@pytest.mark.asyncio
async def test_refresh_upserts_rules_and_a_second_snapshot_overwrites_them(conn: AsyncConnection) -> None:
    source = _Source([[_rules()], [_rules(tick="0.5")]])
    cache = _cache(conn, source)

    first = await cache.refresh(reason="startup")
    assert first.upserted == 1
    assert await _stored(conn) == ("Trading", Decimal("0.1"), LAUNCH_MS)

    second = await cache.refresh(reason="startup")
    assert second.upserted == 1
    assert await _stored(conn) == ("Trading", Decimal("0.5"), LAUNCH_MS)
    assert (await conn.execute(select(func.count()).select_from(StoredInstrument))).scalar_one() == 1


@pytest.mark.asyncio
async def test_not_trading_pauses_a_running_bot_and_trading_again_resumes_only_that_reason(
    conn: AsyncConnection,
) -> None:
    account = await make_account(conn)
    running = await make_bot(conn, account, status="RUNNING")
    manual = await make_bot(conn, account, symbol="ETH/USDT:USDT", status="PAUSED", pause_reasons=["MANUAL"])
    stopped = await make_bot(conn, account, symbol="SOL/USDT:USDT", status="EMERGENCY_STOP")
    source = _Source(
        [
            [
                _rules(status="PreLaunch"),
                _rules(symbol="ETH/USDT:USDT", status="PreLaunch"),
                _rules(symbol="SOL/USDT:USDT", status="Closed"),
            ],
            [
                _rules(),
                _rules(symbol="ETH/USDT:USDT"),
                _rules(symbol="SOL/USDT:USDT"),
            ],
        ]
    )
    cache = _cache(conn, source, symbols=(SYMBOL, "ETH/USDT:USDT", "SOL/USDT:USDT"))

    await cache.refresh(reason="startup")
    assert await _bot_state(conn, running) == ("PAUSED", [INSTRUMENT_NOT_TRADING], 2)
    assert await _bot_state(conn, manual) == ("PAUSED", ["MANUAL", INSTRUMENT_NOT_TRADING], 2)
    assert await _bot_state(conn, stopped) == ("EMERGENCY_STOP", [INSTRUMENT_NOT_TRADING], 2)

    await cache.refresh(reason="startup")
    assert await _bot_state(conn, running) == ("RUNNING", [], 3)
    assert await _bot_state(conn, manual) == ("PAUSED", ["MANUAL"], 3)
    assert await _bot_state(conn, stopped) == ("EMERGENCY_STOP", [], 3)


@pytest.mark.asyncio
async def test_a_symbol_missing_from_a_full_snapshot_becomes_absent_and_pauses(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    source = _Source([[_rules()], []])
    cache = _cache(conn, source)

    await cache.refresh(reason="startup")
    with pytest.raises(EmptyInstrumentSnapshot):
        await cache.refresh(reason="scheduled")
    assert (await _stored(conn))[0] == "Trading"
    assert (await _bot_state(conn, bot))[0] == "RUNNING"

    source.batches.append([_rules(symbol="ETH/USDT:USDT")])
    await cache.refresh(reason="scheduled")
    assert await _stored(conn) == (ABSENT_STATUS, Decimal("0.1"), LAUNCH_MS)
    assert (await _bot_state(conn, bot))[0] == "PAUSED"


@pytest.mark.asyncio
async def test_daily_refresh_waits_24h_and_a_failed_fetch_keeps_the_cache(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    source = _Source([[_rules()]])
    cache = _cache(conn, source)
    await cache.refresh(reason="startup")

    assert await cache.refresh_if_due() is None
    fake_clock.advance(INSTRUMENT_MAX_AGE_MS - 1)
    assert await cache.refresh_if_due() is None

    failed = _cache(conn, _Down())
    with pytest.raises(ExchangeTimeout):
        await failed.refresh(reason="startup")
    assert (await _stored(conn))[0] == "Trading"

    fake_clock.advance(1)
    source.batches.append([_rules(status="PreLaunch")])
    due = await cache.refresh_if_due()
    assert due is not None
    assert due.reason == "scheduled"
    assert (await _stored(conn))[0] == "PreLaunch"


@pytest.mark.asyncio
async def test_filter_rejection_refreshes_one_symbol_and_other_codes_do_not(conn: AsyncConnection) -> None:
    source = _Source([[_rules()], [_rules(tick="0.2")]])
    cache = _cache(conn, source)
    await cache.refresh(reason="startup")

    assert await cache.refresh_after_rejection(code="10001", symbol=SYMBOL) is None
    assert source.calls == [None]

    refreshed = await cache.refresh_after_rejection(code="110003", symbol=SYMBOL)
    assert refreshed is not None
    assert source.calls[-1] == (SYMBOL,)
    assert (await _stored(conn))[1] == Decimal("0.2")


def _racing_sessions(conn: AsyncConnection, *, bot_id: uuid.UUID, races: int) -> async_sessionmaker[AsyncSession]:
    """Перед кожним із перших `races` записів паузи інший процес ставить бота на `MANUAL` і бере нову версію."""
    left = [races]

    class _Racing(AsyncSession):
        async def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
            table = statement.table if isinstance(statement, Update) else None
            if left[0] and getattr(table, "name", None) == Bot.__tablename__:
                left[0] -= 1
                await super().execute(
                    update(Bot)
                    .where(Bot.id == bot_id)
                    .values(version=Bot.version + 1, status="PAUSED", pause_reasons=["MANUAL"])
                )
            return await super().execute(statement, *args, **kwargs)

    return async_sessionmaker(
        bind=conn, class_=_Racing, expire_on_commit=False, join_transaction_mode="create_savepoint"
    )


@pytest.mark.asyncio
async def test_a_concurrent_bot_update_is_retried_on_the_fresh_row(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    sessions = _racing_sessions(conn, bot_id=bot, races=1)
    cache = InstrumentCache(
        sessions, _Source([[_rules(status="Closed")]]), exchange="bybit", mode="demo", symbols=(SYMBOL,)
    )

    result = await cache.refresh(reason="startup")

    # Повтор перечитав рядок: причина іншого процесу лишилась, пауза інструмента дописана поверх неї.
    assert await _bot_state(conn, bot) == ("PAUSED", ["MANUAL", INSTRUMENT_NOT_TRADING], 3)
    assert len(result.pauses) == 1
    assert result.pauses[0].status_before == "PAUSED"
    assert result.pauses[0].reasons_before == ("MANUAL",)
    assert (await _stored(conn))[0] == "Closed"


@pytest.mark.asyncio
async def test_a_second_conflict_rolls_back_the_whole_snapshot(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    sessions = _racing_sessions(conn, bot_id=bot, races=2)
    cache = InstrumentCache(
        sessions, _Source([[_rules(status="Closed")]]), exchange="bybit", mode="demo", symbols=(SYMBOL,)
    )

    with pytest.raises(InstrumentPauseConflict):
        await cache.refresh(reason="startup")

    # Знімок відкочено цілком: правил у кеші немає (вхід блокує `INSTRUMENT_UNKNOWN`), бот без змін знімка.
    stored = await conn.execute(
        select(func.count()).select_from(StoredInstrument).where(StoredInstrument.symbol == SYMBOL)
    )
    assert stored.scalar_one() == 0
    assert await _bot_state(conn, bot) == ("RUNNING", [], 1)


_RACE_SYMBOL = "RACE/USDT:USDT"


@pytest.mark.asyncio
async def test_stale_snapshot_cannot_overwrite_a_newer_one(migrated_db: URL) -> None:
    """Знімок, що почав читання раніше, не комітиться після новішого і не знімає паузу."""
    engine = make_engine(migrated_db)
    entered = asyncio.Event()
    release = asyncio.Event()
    order: list[str] = []
    account_id: uuid.UUID | None = None
    first: asyncio.Task[InstrumentSyncResult] | None = None
    second: asyncio.Task[InstrumentSyncResult] | None = None

    class _Older:
        async def fetch_instruments(self, *, symbols: Collection[str] | None = None) -> list[Instrument]:
            order.append("old-enter")
            entered.set()
            await release.wait()
            order.append("old-leave")
            return [_rules(symbol=_RACE_SYMBOL, status="Trading")]

    class _Newer:
        async def fetch_instruments(self, *, symbols: Collection[str] | None = None) -> list[Instrument]:
            order.append("new-enter")
            return [_rules(symbol=_RACE_SYMBOL, status="Closed")]

    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with engine.begin() as setup:
            account_id = await make_account(setup)
            await make_bot(setup, account_id, symbol=_RACE_SYMBOL)
        old = InstrumentCache(sessions, _Older(), exchange="bybit", mode="demo", symbols=(_RACE_SYMBOL,))
        new = InstrumentCache(sessions, _Newer(), exchange="bybit", mode="demo", symbols=(_RACE_SYMBOL,))
        first = asyncio.create_task(old.refresh(reason="startup"))
        await entered.wait()
        second = asyncio.create_task(new.refresh(reason="filter_rejection", symbols=(_RACE_SYMBOL,)))
        async with engine.connect() as look:
            waiting = 0
            for _ in range(50):
                waiting = int(
                    await look.scalar(
                        text(
                            "SELECT count(*) FROM pg_locks "
                            "WHERE locktype = 'advisory' AND NOT granted AND classid = :namespace"
                        ),
                        {"namespace": INSTRUMENT_LOCK_NAMESPACE},
                    )
                    or 0
                )
                if waiting:
                    break
                await asyncio.sleep(0.02)
        assert waiting == 1
        assert order == ["old-enter"]
        release.set()
        await first
        await second
        assert order == ["old-enter", "old-leave", "new-enter"]
        async with engine.connect() as check:
            status = await check.scalar(
                select(StoredInstrument.status).where(
                    StoredInstrument.symbol == _RACE_SYMBOL, StoredInstrument.mode == "demo"
                )
            )
            bot_status, reasons = (
                await check.execute(
                    select(Bot.status, Bot.pause_reasons).where(Bot.symbol == _RACE_SYMBOL, Bot.mode == "demo")
                )
            ).one()
        assert status == "Closed"
        assert bot_status == "PAUSED"
        assert list(reasons) == [INSTRUMENT_NOT_TRADING]
    finally:
        release.set()
        pending = [task for task in (first, second) if task is not None and not task.done()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        async with engine.begin() as cleanup:
            await cleanup.execute(delete(Bot).where(Bot.symbol == _RACE_SYMBOL))
            if account_id is not None:
                await cleanup.execute(delete(ExchangeAccount).where(ExchangeAccount.id == account_id))
            await cleanup.execute(delete(StoredInstrument).where(StoredInstrument.symbol == _RACE_SYMBOL))
        await engine.dispose()
