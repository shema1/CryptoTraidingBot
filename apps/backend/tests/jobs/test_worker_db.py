"""Черга `jobs` на тестовій БД (T1.8): SKIP LOCKED, heartbeat, backfill, прострочена RUNNING."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import delete, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.clock import FakeClock, now_ms
from app.core.db import create_engine, session_factory
from app.data.backfill import (
    BACKFILL_DEDUP_KEY,
    JOB_KIND_BACKFILL,
    SessionFactory,
    backfill_job_params,
    enqueue_backfill_job,
)
from app.data.models import Candle, DataGap, FundingRate
from app.exchange.types import Candle as ExchangeCandle
from app.exchange.types import FundingRate as ExchangeFunding
from app.exchange.types import Timeframe
from app.jobs.models import Job
from app.jobs.worker import (
    ERROR_BACKFILL_INCOMPLETE,
    ERROR_BAD_PARAMS,
    ERROR_HANDLER,
    ERROR_STALE_HEARTBEAT,
    ERROR_UNKNOWN_KIND,
    PROGRESS_MESSAGE_MAX_LEN,
    STALE_AFTER_MS,
    ClaimedJob,
    JobContext,
    claim_next,
    handle_backfill,
    run_worker,
)
from tests.exchange.factories import FAKE_NOW_MS, OTHER_SYMBOL, SYMBOL, hourly_candle, make_fake

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

HOUR_MS = 3_600_000
UNKNOWN_SYMBOL = "SOL/USDT:USDT"


def _instant(clock: FakeClock) -> Callable[[float], Coroutine[Any, Any, None]]:
    async def sleep(seconds: float) -> None:
        clock.advance(max(1, round(seconds * 1000)))
        await asyncio.sleep(0)

    return sleep


async def _wipe(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(delete(DataGap))
        await conn.execute(delete(Candle))
        await conn.execute(delete(FundingRate))
        await conn.execute(delete(Job))


@pytest_asyncio.fixture
async def db(migrated_db: URL) -> AsyncIterator[tuple[AsyncEngine, SessionFactory]]:
    engine = create_engine(migrated_db)
    await _wipe(engine)
    try:
        yield engine, session_factory(engine)
    finally:
        await _wipe(engine)
        await engine.dispose()


async def _add(
    sessions: SessionFactory,
    *,
    kind: str = "hold",
    params: Mapping[str, Any] | None = None,
    priority: int = 100,
    status: str = "QUEUED",
    started_at_ms: int | None = None,
    heartbeat_at_ms: int | None = None,
    dedup_key: str | None = None,
) -> uuid.UUID:
    async with sessions() as session, session.begin():
        job = Job(
            kind=kind,
            params=dict(params or {}),
            priority=priority,
            status=status,
            started_at_ms=started_at_ms,
            heartbeat_at_ms=heartbeat_at_ms,
            dedup_key=dedup_key,
        )
        session.add(job)
        await session.flush()
        return job.id


async def _load(sessions: SessionFactory, job_id: uuid.UUID) -> dict[str, Any]:
    async with sessions() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return {
            "status": job.status,
            "progress": job.progress,
            "progress_message": job.progress_message,
            "error": job.error,
            "result": job.result,
            "heartbeat_at_ms": job.heartbeat_at_ms,
            "started_at_ms": job.started_at_ms,
            "finished_at_ms": job.finished_at_ms,
        }


async def _claim(sessions: SessionFactory) -> tuple[ClaimedJob | None, bool]:
    async with sessions() as session, session.begin():
        return await claim_next(session)


async def test_two_workers_do_not_take_the_same_job(db: tuple[AsyncEngine, SessionFactory]) -> None:
    _engine, sessions = db
    only = await _add(sessions, kind="alpha")

    async def once() -> ClaimedJob | None:
        job, _discarded = await _claim(sessions)
        return job

    first, second = await asyncio.gather(once(), once())
    claimed = [item for item in (first, second) if item is not None]
    assert [item.id for item in claimed] == [only]
    assert (await _load(sessions, only))["status"] == "RUNNING"


async def test_two_workers_take_two_jobs(db: tuple[AsyncEngine, SessionFactory]) -> None:
    _engine, sessions = db
    earlier = await _add(sessions, kind="alpha", priority=1)
    later = await _add(sessions, kind="beta", priority=2)

    async def once() -> ClaimedJob | None:
        job, _discarded = await _claim(sessions)
        return job

    first, second = await asyncio.gather(once(), once())
    claimed = {item.id for item in (first, second) if item is not None}
    assert claimed == {earlier, later}


async def test_lower_priority_number_is_taken_first(db: tuple[AsyncEngine, SessionFactory]) -> None:
    _engine, sessions = db
    low = await _add(sessions, kind="later", priority=50)
    high = await _add(sessions, kind="sooner", priority=1)
    claimed, discarded = await _claim(sessions)
    assert discarded is False
    assert claimed is not None
    assert claimed.id == high
    assert (await _load(sessions, low))["status"] == "QUEUED"


async def test_startup_fails_a_stale_running_job_and_leaves_a_fresh_one(
    db: tuple[AsyncEngine, SessionFactory], fake_clock: FakeClock
) -> None:
    _engine, sessions = db
    fresh = await _add(sessions, kind="fresh", status="RUNNING", started_at_ms=now_ms(), heartbeat_at_ms=now_ms())
    boundary = now_ms() - STALE_AFTER_MS
    alive = await _add(sessions, kind="alive", status="RUNNING", started_at_ms=boundary, heartbeat_at_ms=boundary)
    stale_at = now_ms() - STALE_AFTER_MS - 1
    stale = await _add(
        sessions,
        kind="stale",
        status="RUNNING",
        started_at_ms=stale_at,
        heartbeat_at_ms=stale_at,
        dedup_key=BACKFILL_DEDUP_KEY,
    )
    stop = asyncio.Event()
    stop.set()
    await run_worker(sessions, make_fake(), handlers={}, stop=stop, sleep=_instant(fake_clock))

    assert (await _load(sessions, fresh))["status"] == "RUNNING"
    assert (await _load(sessions, alive))["status"] == "RUNNING"
    failed = await _load(sessions, stale)
    assert failed["status"] == "FAILED"
    assert failed["error"] == ERROR_STALE_HEARTBEAT
    assert failed["finished_at_ms"] == now_ms()

    async with sessions() as session, session.begin():
        new_id = await enqueue_backfill_job(session, symbols=(SYMBOL,), start_ms=FAKE_NOW_MS - HOUR_MS)
    assert new_id != stale
    assert (await _load(sessions, new_id))["status"] == "QUEUED"


async def test_unknown_kind_and_bad_params_fail_the_job(
    db: tuple[AsyncEngine, SessionFactory], fake_clock: FakeClock
) -> None:
    _engine, sessions = db
    unknown = await _add(sessions, kind="nope")
    async with sessions() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO jobs (kind, status, priority, params, progress, attempt, created_at_ms) "
                "VALUES ('bad_params', 'QUEUED', 100, '[]'::jsonb, 0, 1, :now)"
            ),
            {"now": now_ms()},
        )
    stop = asyncio.Event()

    async def sleep(_seconds: float) -> None:
        stop.set()
        await asyncio.sleep(0)

    await run_worker(sessions, make_fake(), handlers={}, stop=stop, sleep=sleep)
    assert (await _load(sessions, unknown))["error"] == ERROR_UNKNOWN_KIND
    async with sessions() as session:
        bad = (await session.execute(text("SELECT error FROM jobs WHERE kind = 'bad_params'"))).scalar_one()
    assert bad == ERROR_BAD_PARAMS


async def test_stop_requeues_the_running_job(db: tuple[AsyncEngine, SessionFactory], fake_clock: FakeClock) -> None:
    _engine, sessions = db
    job_id = await _add(sessions, kind="hold")
    stop = asyncio.Event()

    async def handle(ctx: JobContext) -> None:
        assert ctx.job_id == job_id
        stop.set()
        await asyncio.Event().wait()

    await run_worker(sessions, make_fake(), handlers={"hold": handle}, stop=stop, sleep=_instant(fake_clock))
    row = await _load(sessions, job_id)
    assert row["status"] == "QUEUED"
    assert row["started_at_ms"] is None
    assert row["heartbeat_at_ms"] is None


async def test_handler_reports_progress_and_the_heartbeat_moves(
    db: tuple[AsyncEngine, SessionFactory], fake_clock: FakeClock
) -> None:
    _engine, sessions = db
    job_id = await _add(sessions, kind="hold")
    stop = asyncio.Event()
    seen: dict[str, Any] = {}

    async def handle(ctx: JobContext) -> dict[str, bool]:
        try:
            await ctx.report(Decimal("0.4"), "x" * (PROGRESS_MESSAGE_MAX_LEN + 20))
            mid = await _load(sessions, job_id)
            seen["progress"] = mid["progress"]
            seen["message"] = mid["progress_message"]
            for _ in range(40):
                row = await _load(sessions, job_id)
                heartbeat = row["heartbeat_at_ms"]
                started = row["started_at_ms"]
                if heartbeat is not None and started is not None and heartbeat > started:
                    seen["heartbeat"] = heartbeat
                    return {"ok": True}
                await asyncio.sleep(0)
            raise AssertionError("heartbeat не оновився")
        finally:
            stop.set()

    await run_worker(sessions, make_fake(), handlers={"hold": handle}, stop=stop, sleep=_instant(fake_clock))
    assert seen["progress"] == Decimal("0.4")
    assert seen["message"] == "x" * PROGRESS_MESSAGE_MAX_LEN
    row = await _load(sessions, job_id)
    assert row["status"] == "DONE"
    assert row["progress"] == Decimal(1)
    assert row["error"] is None
    assert row["result"] == {"ok": True}
    assert seen["heartbeat"] > row["started_at_ms"]


async def test_backfill_job_is_done_and_an_incomplete_one_is_failed(
    db: tuple[AsyncEngine, SessionFactory], fake_clock: FakeClock
) -> None:
    _engine, sessions = db
    ok_fake = make_fake()
    ok_fake.add_candles([hourly_candle(FAKE_NOW_MS - HOUR_MS, "90", symbol=SYMBOL)])
    bad_fake = make_fake()
    bad_fake.add_candles([hourly_candle(FAKE_NOW_MS - HOUR_MS, "95", symbol=OTHER_SYMBOL)])
    ok_id = await _add(
        sessions,
        kind=JOB_KIND_BACKFILL,
        priority=1,
        params=backfill_job_params((SYMBOL,), FAKE_NOW_MS - HOUR_MS),
    )
    bad_id = await _add(
        sessions,
        kind=JOB_KIND_BACKFILL,
        priority=2,
        params=backfill_job_params((UNKNOWN_SYMBOL, OTHER_SYMBOL), FAKE_NOW_MS - HOUR_MS),
    )
    sources = {ok_id: ok_fake, bad_id: bad_fake}
    current = {"id": ok_id}
    stop = asyncio.Event()

    class _Switch:
        async def fetch_candles(
            self,
            *,
            symbol: str,
            timeframe: Timeframe,
            start_ms: int,
            end_ms: int | None = None,
            limit: int | None = None,
        ) -> list[ExchangeCandle]:
            return await sources[current["id"]].fetch_candles(
                symbol=symbol, timeframe=timeframe, start_ms=start_ms, end_ms=end_ms, limit=limit
            )

        async def fetch_funding_history(
            self, *, symbol: str, start_ms: int, end_ms: int | None = None
        ) -> list[ExchangeFunding]:
            return await sources[current["id"]].fetch_funding_history(symbol=symbol, start_ms=start_ms, end_ms=end_ms)

    async def routed(ctx: JobContext) -> dict[str, int]:
        current["id"] = ctx.job_id
        try:
            return await handle_backfill(ctx)
        finally:
            if ctx.job_id == bad_id:
                stop.set()

    await run_worker(sessions, _Switch(), handlers={JOB_KIND_BACKFILL: routed}, stop=stop, sleep=_instant(fake_clock))
    done = await _load(sessions, ok_id)
    assert done["status"] == "DONE"
    assert done["progress"] == Decimal(1)
    assert done["result"] == {"candles_inserted": 1, "funding_inserted": 0}
    failed = await _load(sessions, bad_id)
    assert failed["status"] == "FAILED"
    assert failed["error"] == ERROR_BACKFILL_INCOMPLETE
    assert failed["result"]["failed_symbols"] == [UNKNOWN_SYMBOL]
    assert failed["result"]["candles_inserted"] == 1


async def test_handler_error_is_failed_without_the_exception_text(
    db: tuple[AsyncEngine, SessionFactory], fake_clock: FakeClock
) -> None:
    _engine, sessions = db
    job_id = await _add(sessions, kind="hold", params={"secret": "super-secret-value"})
    stop = asyncio.Event()

    async def handle(_ctx: JobContext) -> None:
        stop.set()
        raise RuntimeError("super-secret-value leaked")

    await run_worker(sessions, make_fake(), handlers={"hold": handle}, stop=stop, sleep=_instant(fake_clock))
    row = await _load(sessions, job_id)
    assert row["status"] == "FAILED"
    assert row["error"] == ERROR_HANDLER
    assert row["result"] == {"error_type": "RuntimeError"}
    dumped = str(row["result"]) + str(row["error"])
    assert "super-secret-value" not in dumped
