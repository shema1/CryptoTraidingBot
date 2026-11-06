"""`python -m app.jobs.enqueue market-data` (T1.11): три задачі воркера, ідемпотентно, без мережі.

Тут же — обробник `refresh_instruments`: кеш інструментів лише поки ботів режиму немає (рішення 0023).
"""

from __future__ import annotations

import socket
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from app.core.config import DEFAULT_BACKFILL_START_MS, Settings
from app.core.db import create_engine, session_factory
from app.data.backfill import BACKFILL_DEDUP_KEY, JOB_KIND_BACKFILL, SessionFactory, backfill_job_params
from app.data.derivatives import DERIVATIVES_DEDUP_KEY, JOB_KIND_DERIVATIVES, derivatives_job_params
from app.exchange.models import ExchangeAccount
from app.exchange.models import Instrument as StoredInstrument
from app.jobs.enqueue import COMMAND_MARKET_DATA, enqueue_market_data, main
from app.jobs.models import Job
from app.jobs.worker import (
    INSTRUMENTS_DEDUP_KEY,
    JOB_KIND_INSTRUMENTS,
    JobContext,
    handle_instruments,
    instruments_job_params,
)
from app.trading.models import Bot
from tests.db.factories import make_account, make_bot
from tests.exchange.factories import SYMBOL, make_fake

RESEARCH = (
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
    "BNB/USDT:USDT",
    "DOGE/USDT:USDT",
)


def _settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "trading_symbol": "BTC/USDT:USDT",
        "research_symbols": RESEARCH,
        "backfill_start_ms": DEFAULT_BACKFILL_START_MS,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)  # type: ignore[call-arg]


@pytest_asyncio.fixture
async def db(migrated_db: URL) -> AsyncIterator[tuple[AsyncEngine, SessionFactory]]:
    engine = create_engine(migrated_db)
    await _wipe(engine)
    try:
        yield engine, session_factory(engine)
    finally:
        await _wipe(engine)
        await engine.dispose()


async def _wipe(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(delete(Job))
        await conn.execute(delete(StoredInstrument))


async def _jobs(sessions: SessionFactory) -> list[Job]:
    async with sessions() as session:
        return list((await session.execute(select(Job).order_by(Job.kind))).scalars())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_market_data_queues_candles_for_trading_and_research_and_derivatives_for_research(
    db: tuple[AsyncEngine, SessionFactory],
) -> None:
    _engine, sessions = db
    async with sessions() as session, session.begin():
        jobs = await enqueue_market_data(session, _settings())
    stored = {job.kind: job for job in await _jobs(sessions)}
    assert set(stored) == {JOB_KIND_INSTRUMENTS, JOB_KIND_BACKFILL, JOB_KIND_DERIVATIVES}
    instruments = stored[JOB_KIND_INSTRUMENTS]
    assert instruments.id == jobs.instruments_job_id
    assert instruments.dedup_key == INSTRUMENTS_DEDUP_KEY
    assert instruments.params == instruments_job_params(("BTC/USDT:USDT", *RESEARCH))
    # Кеш — раніше за свічки: сітка прогалин наступного проходу починається від лістингу.
    assert instruments.priority < stored[JOB_KIND_BACKFILL].priority
    candles = stored[JOB_KIND_BACKFILL]
    derivatives = stored[JOB_KIND_DERIVATIVES]
    assert candles.id == jobs.candles_job_id
    assert derivatives.id == jobs.derivatives_job_id
    assert candles.status == "QUEUED"
    assert derivatives.status == "QUEUED"
    assert candles.dedup_key == BACKFILL_DEDUP_KEY
    assert derivatives.dedup_key == DERIVATIVES_DEDUP_KEY
    # Торговий символ поза research_symbols теж отримує свічки; деривативи — лише research_symbols.
    assert candles.params == backfill_job_params(("BTC/USDT:USDT", *RESEARCH), DEFAULT_BACKFILL_START_MS)
    assert derivatives.params == derivatives_job_params(RESEARCH, DEFAULT_BACKFILL_START_MS)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_repeated_call_returns_the_active_jobs_without_new_rows(db: tuple[AsyncEngine, SessionFactory]) -> None:
    _engine, sessions = db
    async with sessions() as session, session.begin():
        first = await enqueue_market_data(session, _settings())
    async with sessions() as session, session.begin():
        second = await enqueue_market_data(session, _settings())
    assert (second.instruments_job_id, second.candles_job_id, second.derivatives_job_id) == (
        first.instruments_job_id,
        first.candles_job_id,
        first.derivatives_job_id,
    )
    assert len(await _jobs(sessions)) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_finished_jobs_do_not_block_a_new_incremental_pass(db: tuple[AsyncEngine, SessionFactory]) -> None:
    _engine, sessions = db
    async with sessions() as session, session.begin():
        first = await enqueue_market_data(session, _settings())
    async with sessions() as session, session.begin():
        for job_id in (first.instruments_job_id, first.candles_job_id, first.derivatives_job_id):
            job = await session.get(Job, job_id)
            assert job is not None
            job.status = "RUNNING"
            job.started_at_ms = 1
            job.heartbeat_at_ms = 1
    async with sessions() as session, session.begin():
        for job_id in (first.instruments_job_id, first.candles_job_id, first.derivatives_job_id):
            job = await session.get(Job, job_id)
            assert job is not None
            job.status = "DONE"
            job.finished_at_ms = 2
    async with sessions() as session, session.begin():
        second = await enqueue_market_data(session, _settings())
    assert second.candles_job_id != first.candles_job_id
    assert second.derivatives_job_id != first.derivatives_job_id
    assert second.instruments_job_id != first.instruments_job_id
    assert len(await _jobs(sessions)) == 6


def test_cli_without_a_command_is_an_argument_error() -> None:
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 2


def test_cli_rejects_an_unknown_command() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["everything"])
    assert exc.value.code == 2


def test_command_name_is_stable() -> None:
    assert COMMAND_MARKET_DATA == "market-data"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueue_never_opens_a_network_connection(
    db: tuple[AsyncEngine, SessionFactory], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Лише БД: будь-яке з'єднання не з loopback — помилка тесту (до біржі ходить воркер)."""
    _engine, sessions = db
    original = socket.socket.connect

    def guarded(self: socket.socket, address: Any) -> Any:
        host = address[0] if isinstance(address, tuple) else address
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise AssertionError(f"мережеве з'єднання з {host}")
        return original(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded)
    async with sessions() as session, session.begin():
        await enqueue_market_data(session, _settings())


def _context(sessions: SessionFactory, source: object) -> JobContext:
    return JobContext(
        job_id=uuid.uuid4(),
        kind=JOB_KIND_INSTRUMENTS,
        params=instruments_job_params((SYMBOL,)),
        sessions=sessions,
        source=source,  # type: ignore[arg-type]
    )


async def _instrument_rows(sessions: SessionFactory) -> list[StoredInstrument]:
    async with sessions() as session:
        return list((await session.execute(select(StoredInstrument))).scalars())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_instruments_job_fills_the_cache_when_there_are_no_bots(db: tuple[AsyncEngine, SessionFactory]) -> None:
    _engine, sessions = db
    fake = make_fake()
    result = await handle_instruments(_context(sessions, fake))
    rows = await _instrument_rows(sessions)
    assert result["upserted"] == len(rows) == 2
    assert {row.mode for row in rows} == {"demo"}
    assert SYMBOL in {row.symbol for row in rows}
    assert [call.method for call in fake.calls] == ["fetch_instruments"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_instruments_job_writes_nothing_while_a_bot_of_the_mode_exists(
    db: tuple[AsyncEngine, SessionFactory],
) -> None:
    """Паузи ботів зводить торговий процес (T1.7): воркер з ботами кеш не чіпає і біржу не питає."""
    engine, sessions = db
    async with engine.begin() as conn:
        account = await make_account(conn, mode="demo")
        bot = await make_bot(conn, account, mode="demo", status="STOPPED")
    try:
        fake = make_fake()
        result = await handle_instruments(_context(sessions, fake))
        assert result == {"skipped": "bots_exist", "bots": 1}
        assert await _instrument_rows(sessions) == []
        assert fake.calls == []
    finally:
        async with AsyncSession(bind=engine) as session, session.begin():
            await session.execute(delete(Bot).where(Bot.id == bot))
            await session.execute(delete(ExchangeAccount).where(ExchangeAccount.id == account))


def test_instruments_job_params_need_symbols() -> None:
    from app.jobs.worker import parse_instruments_job_params

    with pytest.raises(ValueError, match="symbols"):
        parse_instruments_job_params({"symbols": []})
    assert parse_instruments_job_params(instruments_job_params((SYMBOL,))) == (SYMBOL,)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_instruments_job_runs_when_only_bots_of_other_modes_exist(
    db: tuple[AsyncEngine, SessionFactory],
) -> None:
    """Пропуск — лише через ботів режиму `demo`; бот testnet кеш demo не блокує, а сам лишається незмінним."""
    engine, sessions = db
    async with engine.begin() as conn:
        account = await make_account(conn, mode="testnet")
        bot = await make_bot(conn, account, mode="testnet", status="RUNNING")
    try:
        result = await handle_instruments(_context(sessions, make_fake()))
        assert result["upserted"] == 2
        async with sessions() as session:
            stored = await session.get(Bot, bot)
            assert stored is not None
            assert (stored.status, list(stored.pause_reasons), stored.version) == ("RUNNING", [], 1)
    finally:
        async with AsyncSession(bind=engine) as session, session.begin():
            await session.execute(delete(Bot).where(Bot.id == bot))
            await session.execute(delete(ExchangeAccount).where(ExchangeAccount.id == account))


@pytest.mark.asyncio
async def test_enqueue_without_a_db_password_is_a_config_error() -> None:
    from app.jobs.enqueue import EXIT_CONFIG_INVALID, run

    assert await run(_settings(postgres_password=None)) == EXIT_CONFIG_INVALID


@pytest.mark.asyncio
async def test_enqueue_with_an_unreachable_db_exits_without_a_traceback() -> None:
    from app.jobs.enqueue import EXIT_DB_UNAVAILABLE, run

    # Порт 1 на loopback ніхто не слухає: з'єднання відхиляється одразу.
    assert await run(_settings(postgres_password="x" * 12, postgres_port=1)) == EXIT_DB_UNAVAILABLE
