"""Реальні процеси воркера (T1.8 «Готово, коли»).

- другий воркер на тій самій БД не стартує (single-instance lock `WORKER_LOCK_ID`): код 10, задачу не бере;
- SIGTERM під час очікування lock — код 0 без задачі;
- втрата lock під час роботи — код 12, задача лишається `RUNNING` (її зніме прострочення);
- `kill -9` лишає рядок `RUNNING`; наступний старт бере звільнений lock і за старим heartbeat ставить `FAILED`;
- SIGTERM повертає поточну задачу в `QUEUED`.

`heartbeat_at_ms = 0` після kill замінює очікування `STALE_AFTER_MS` (30 с). Саму нерівність
«heartbeat старіший за вікно» перевіряє FakeClock у `test_worker_db.py`.
Процеси працюють з `<db>_test`. Готовність — за подією в stderr, без `sleep`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import sys
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Protocol

import pytest
import pytest_asyncio
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.db import create_engine, session_factory
from app.core.instance_lock import LOCK_NAMESPACE, WORKER_LOCK_ID
from app.data.backfill import SessionFactory
from app.jobs.models import Job
from app.jobs.worker import ERROR_STALE_HEARTBEAT, EXIT_LOCK_HELD, EXIT_LOCK_LOST, EXIT_OK

pytestmark = [pytest.mark.integration, pytest.mark.slow, pytest.mark.asyncio]

BACKEND_DIR = Path(__file__).resolve().parents[2]
STARTUP_TIMEOUT_S = 60
HOLD = "tests.jobs.hold_worker"
REAL = "app.jobs.worker"

# Сесії, що тримають lock воркера в поточній БД (`objsubid = 2` — ключ `pg_try_advisory_lock(int, int)`).
_TERMINATE_WORKER_LOCK_SQL = """
SELECT count(*) FILTER (WHERE pg_terminate_backend(l.pid, 5000))
FROM pg_locks AS l
WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 2
  AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND l.classid = CAST(:namespace AS bigint)::oid AND l.objid = CAST(:lock_id AS bigint)::oid
"""


class Proc:
    """Дочірній процес воркера; stderr читається як JSON-рядки логів."""

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self.proc = proc
        self.events: list[dict[str, Any]] = []
        self._changed = asyncio.Event()
        self._reader = asyncio.create_task(self._read_all())

    async def _read_all(self) -> None:
        assert self.proc.stderr is not None
        try:
            while True:
                raw = await self.proc.stderr.readline()
                if not raw:
                    return
                try:
                    parsed = json.loads(raw)
                except ValueError:
                    parsed = {"event": "<not json>", "raw": raw.decode(errors="replace")}
                self.events.append(parsed if isinstance(parsed, dict) else {"event": "<not object>"})
                self._changed.set()
        finally:
            self._changed.set()

    def event(self, name: str) -> dict[str, Any] | None:
        return next((item for item in self.events if item.get("event") == name), None)

    def count(self, name: str) -> int:
        return sum(1 for item in self.events if item.get("event") == name)

    async def wait_event(self, name: str) -> dict[str, Any]:
        async with asyncio.timeout(STARTUP_TIMEOUT_S):
            while True:
                found = self.event(name)
                if found is not None:
                    return found
                if self._reader.done() and self.proc.returncode is not None:
                    pytest.fail(f"процес завершився без події {name}: {self.events}")
                self._changed.clear()
                found = self.event(name)
                if found is not None:
                    return found
                if self._reader.done() and self.proc.returncode is not None:
                    pytest.fail(f"процес завершився без події {name}: {self.events}")
                await self._changed.wait()

    async def finish(self) -> int:
        async with asyncio.timeout(STARTUP_TIMEOUT_S):
            await self._reader
            return await self.proc.wait()


class Start(Protocol):
    async def __call__(self, name: str, module: str, *, lock_wait_s: int | None = None) -> Proc: ...


async def _wipe(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(delete(Job))


async def _insert(sessions: SessionFactory) -> uuid.UUID:
    async with sessions() as session, session.begin():
        job = Job(kind="hold", params={}, priority=100)
        session.add(job)
        await session.flush()
        return job.id


async def _load(sessions: SessionFactory, job_id: uuid.UUID) -> Job:
    async with sessions() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


async def _count(sessions: SessionFactory, status: str) -> int:
    async with sessions() as session:
        count = await session.scalar(select(func.count()).select_from(Job).where(Job.status == status))
    assert count is not None
    return int(count)


@pytest_asyncio.fixture
async def pool(migrated_db: URL, tmp_path: Path) -> AsyncIterator[tuple[AsyncEngine, SessionFactory, Start]]:
    engine = create_engine(migrated_db)
    sessions = session_factory(engine)
    started: list[Proc] = []
    await _wipe(engine)

    async def start(name: str, module: str, *, lock_wait_s: int | None = None) -> Proc:
        env = dict(os.environ)
        assert migrated_db.database is not None
        env.update(
            {
                "POSTGRES_DB": migrated_db.database,
                "LOG_DIR": str(tmp_path / name),
                "LOG_CONSOLE": "true",
                "LOG_LEVEL": "INFO",
            }
        )
        if lock_wait_s is not None:
            env["LOCK_WAIT_S"] = str(lock_wait_s)
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            module,
            cwd=BACKEND_DIR,
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        proc = Proc(child)
        started.append(proc)
        return proc

    try:
        yield engine, sessions, start
    finally:
        for proc in started:
            if proc.proc.returncode is None:
                proc.proc.kill()
                await proc.proc.wait()
            if not proc._reader.done():
                proc._reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await proc._reader
        await _wipe(engine)
        await engine.dispose()


async def test_second_worker_does_not_start_and_takes_no_job(
    pool: tuple[AsyncEngine, SessionFactory, Start],
) -> None:
    _engine, sessions, start = pool
    await _insert(sessions)
    await _insert(sessions)
    first = await start("first", HOLD)
    await first.wait_event("job_started")

    second = await start("second", HOLD, lock_wait_s=0)
    assert await second.finish() == EXIT_LOCK_HELD
    held = second.event("instance_lock_held_by_other")
    assert held is not None
    assert [holder["client_os_pid"] for holder in held["holders"]] == [first.proc.pid]
    assert held["holders"][0]["application_name"] == f"ctb-worker pid={first.proc.pid}"
    assert second.count("job_started") == 0
    assert second.event("worker_ready") is None

    assert await _count(sessions, "RUNNING") == 1
    assert await _count(sessions, "QUEUED") == 1


async def test_sigterm_while_waiting_for_the_lock_exits_cleanly(
    pool: tuple[AsyncEngine, SessionFactory, Start],
) -> None:
    _engine, sessions, start = pool
    await _insert(sessions)
    first = await start("first", HOLD)
    await first.wait_event("job_started")

    waiting = await start("waiting", HOLD, lock_wait_s=600)
    await waiting.wait_event("instance_lock_busy")
    waiting.proc.send_signal(signal.SIGTERM)
    assert await waiting.finish() == EXIT_OK
    assert waiting.count("job_started") == 0
    assert first.proc.returncode is None


async def test_lost_lock_stops_the_worker_and_leaves_the_job_running(
    pool: tuple[AsyncEngine, SessionFactory, Start],
) -> None:
    _engine, sessions, start = pool
    job_id = await _insert(sessions)
    holder = await start("holder", HOLD)
    await holder.wait_event("job_started")

    async with sessions() as session, session.begin():
        terminated = await session.scalar(
            text(_TERMINATE_WORKER_LOCK_SQL), {"namespace": LOCK_NAMESPACE, "lock_id": WORKER_LOCK_ID}
        )
    assert terminated == 1

    assert await holder.finish() == EXIT_LOCK_LOST
    assert holder.event("instance_lock_lost") is not None
    assert holder.event("job_requeued") is None
    row = await _load(sessions, job_id)
    assert row.status == "RUNNING"


async def test_kill_9_stays_running_until_the_next_start_fails_it(
    pool: tuple[AsyncEngine, SessionFactory, Start],
) -> None:
    _engine, sessions, start = pool
    job_id = await _insert(sessions)
    holder = await start("hold", HOLD)
    await holder.wait_event("job_started")
    holder.proc.kill()
    assert await holder.proc.wait() == -signal.SIGKILL

    async with sessions() as session, session.begin():
        await session.execute(update(Job).where(Job.id == job_id).values(heartbeat_at_ms=0))
    still = await _load(sessions, job_id)
    assert still.status == "RUNNING"

    recovery = await start("recovery", REAL)
    ready = await recovery.wait_event("worker_ready")
    assert recovery.event("instance_lock_acquired") is not None
    assert ready["stale_failed"] == 1
    assert recovery.event("job_stale") is not None
    failed = await _load(sessions, job_id)
    assert failed.status == "FAILED"
    assert failed.error == ERROR_STALE_HEARTBEAT

    recovery.proc.send_signal(signal.SIGTERM)
    assert await recovery.finish() == EXIT_OK


async def test_sigterm_requeues_the_running_job(
    pool: tuple[AsyncEngine, SessionFactory, Start],
) -> None:
    _engine, sessions, start = pool
    job_id = await _insert(sessions)
    holder = await start("sigterm", HOLD)
    await holder.wait_event("job_started")
    holder.proc.send_signal(signal.SIGTERM)
    assert await holder.finish() == EXIT_OK
    assert holder.event("job_requeued") is not None
    row = await _load(sessions, job_id)
    assert row.status == "QUEUED"
    assert row.started_at_ms is None
    assert row.heartbeat_at_ms is None
