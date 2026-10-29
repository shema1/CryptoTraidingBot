"""Реальні процеси торгового бекенду `python -m app` (ТЗ §6.4, T0.6 «Готово, коли»).

- другий екземпляр не стартує: код виходу `EXIT_LOCK_HELD`, у stderr — власник lock з pid ОС першого процесу;
- після `kill -9` першого процесу lock звільняється не пізніше `KEEPALIVE_TIMEOUT_S`;
- SIGTERM — чисте завершення з кодом 0 і звільненням lock.

Процеси працюють з `<db>_test` (`POSTGRES_DB` в оточенні дочірнього процесу), кожен — з власною текою логів.
Готовність процесу визначається за подією `instance_lock_acquired` у stderr (без опитування з sleep).
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import signal
import socket
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.engine import URL

from app.__main__ import EXIT_LOCK_HELD, EXIT_OK
from app.core.instance_lock import BACKEND_LOCK_ID, KEEPALIVE_TIMEOUT_S, LOCK_NAMESPACE, connect_lock_session

pytestmark = [pytest.mark.integration, pytest.mark.slow, pytest.mark.asyncio]

BACKEND_DIR = Path(__file__).resolve().parents[2]
# Запас на старт інтерпретатора й імпорти; сам lock береться за мілісекунди.
STARTUP_TIMEOUT_S = 60
# Скільки разів опитати pg_stat_activity, чекаючи завершення серверної сесії (як у tests/db/test_concurrency.py).
_MAX_POLLS = 2000


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
    return port


class Backend:
    """Дочірній процес `python -m app`; stderr читається як JSON-рядки логів."""

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self.proc = proc
        self.events: list[dict[str, Any]] = []

    async def _read_line(self) -> bool:
        assert self.proc.stderr is not None
        raw = await self.proc.stderr.readline()
        if not raw:
            return False
        try:
            record = json.loads(raw)
        except ValueError:
            record = {"event": "<not json>", "raw": raw.decode(errors="replace")}
        self.events.append(record)
        return True

    async def wait_event(self, event: str) -> dict[str, Any]:
        async with asyncio.timeout(STARTUP_TIMEOUT_S):
            while True:
                found = self.event(event)
                if found is not None:
                    return found
                if not await self._read_line():
                    pytest.fail(f"процес завершився без події {event}: {self.events}")

    async def finish(self) -> int:
        async with asyncio.timeout(STARTUP_TIMEOUT_S):
            while await self._read_line():
                pass
            return await self.proc.wait()

    def event(self, event: str) -> dict[str, Any] | None:
        return next((e for e in self.events if e.get("event") == event), None)


@pytest_asyncio.fixture
async def start_backend(test_db_url: URL, tmp_path: Path) -> AsyncIterator[Any]:
    started: list[Backend] = []

    async def start(name: str, lock_wait_s: int = 1) -> Backend:
        env = dict(os.environ)
        assert test_db_url.database is not None
        env.update(
            {
                "POSTGRES_DB": test_db_url.database,
                "LOCK_WAIT_S": str(lock_wait_s),
                "LOG_DIR": str(tmp_path / name),
                "LOG_CONSOLE": "true",
                "LOG_LEVEL": "INFO",
                # T0.7: без токена процес не стартує; порт — вільний ефемерний, не 8000 розробника.
                "API_TOKEN": secrets.token_urlsafe(32),
                "API_PORT": str(_free_port()),
            }
        )
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "app",
            cwd=BACKEND_DIR,
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        backend = Backend(proc)
        started.append(backend)
        return backend

    try:
        yield start
    finally:
        for backend in started:
            if backend.proc.returncode is None:
                backend.proc.kill()
                await backend.proc.wait()


@pytest_asyncio.fixture
async def observer(test_db_url: URL) -> AsyncIterator[Any]:
    conn = await connect_lock_session(test_db_url, server_settings={"application_name": "ctb-test-observer"})
    try:
        yield conn
    finally:
        await conn.close()


async def test_second_backend_does_not_start_and_kill_9_releases_lock(start_backend: Any, observer: Any) -> None:
    first: Backend = await start_backend("first")
    acquired = await first.wait_event("instance_lock_acquired")
    assert acquired["lock"] == [LOCK_NAMESPACE, BACKEND_LOCK_ID]

    second: Backend = await start_backend("second")
    assert await second.finish() == EXIT_LOCK_HELD
    refused = second.event("instance_lock_held_by_other")
    assert refused is not None, second.events
    assert [h["client_os_pid"] for h in refused["holders"]] == [first.proc.pid]
    assert first.proc.returncode is None  # перший працює далі

    first.proc.kill()  # kill -9: процес не встигає нічого звільнити сам
    loop = asyncio.get_running_loop()
    started = loop.time()
    # Сервер сам чекає на звільнення (без опитування); lock_timeout — межа очікування.
    await observer.execute(f"SET lock_timeout = '{KEEPALIVE_TIMEOUT_S}s'")
    await observer.execute("SELECT pg_advisory_lock($1::int, $2::int)", LOCK_NAMESPACE, BACKEND_LOCK_ID)
    elapsed = loop.time() - started
    await observer.execute("SELECT pg_advisory_unlock($1::int, $2::int)", LOCK_NAMESPACE, BACKEND_LOCK_ID)

    assert elapsed <= KEEPALIVE_TIMEOUT_S
    assert await first.proc.wait() == -signal.SIGKILL


async def test_sigterm_while_waiting_for_busy_lock_exits_cleanly(start_backend: Any, observer: Any) -> None:
    first: Backend = await start_backend("holder")
    await first.wait_event("instance_lock_acquired")
    waiting: Backend = await start_backend("waiting", lock_wait_s=600)
    await waiting.wait_event("instance_lock_busy")

    waiting.proc.send_signal(signal.SIGTERM)

    assert await waiting.finish() == EXIT_OK
    assert waiting.event("instance_lock_acquired") is None
    # Сесія процесу, що чекав, закрита (backend сервера завершується асинхронно — опитування без sleep, з межею).
    for _ in range(_MAX_POLLS):
        sessions = await observer.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", f"ctb-backend pid={waiting.proc.pid}"
        )
        if sessions == 0:
            break
    else:
        pytest.fail(f"сесія процесу {waiting.proc.pid} лишилась після його завершення")
    # Lock і далі в першого.
    assert first.proc.returncode is None
    first.proc.send_signal(signal.SIGTERM)
    assert await first.finish() == EXIT_OK


async def test_backend_releases_lock_on_sigterm(start_backend: Any, observer: Any) -> None:
    backend: Backend = await start_backend("sigterm")
    await backend.wait_event("instance_lock_acquired")

    backend.proc.send_signal(signal.SIGTERM)

    assert await backend.finish() == EXIT_OK
    assert backend.event("instance_lock_released") is not None
    assert await observer.fetchval("SELECT pg_try_advisory_lock($1::int, $2::int)", LOCK_NAMESPACE, BACKEND_LOCK_ID)
    await observer.execute("SELECT pg_advisory_unlock($1::int, $2::int)", LOCK_NAMESPACE, BACKEND_LOCK_ID)
