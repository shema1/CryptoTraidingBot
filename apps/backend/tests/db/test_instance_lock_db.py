"""Single-instance lock на реальній БД `<db>_test` (ТЗ §6.4, T0.6).

Кожен тест бере власний `lock_id` у просторі `LOCK_NAMESPACE`, тож тести не заважають ні одне одному, ні процесному
тесту (`test_instance_lock_process.py`, `BACKEND_LOCK_ID`). Очікування — `FakeClock` + `sleep`, що рухає годинник.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import os
from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.engine import URL

from app.core.clock import FakeClock
from app.core.instance_lock import (
    AUDIT_ACTION_FORCE_TAKEOVER,
    KEEPALIVE_COUNT,
    KEEPALIVE_IDLE_S,
    KEEPALIVE_INTERVAL_S,
    LOCK_NAMESPACE,
    InstanceLock,
    InstanceLockHeld,
    InstanceLockLost,
    LockHolder,
    TakeoverRefused,
    application_name,
    connect_lock_session,
    lock_server_settings,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# Ідентифікатори тестових lock-ів: далеко від BACKEND_LOCK_ID = 1.
_lock_ids = itertools.count(1000 + os.getpid() % 1000 * 1000)


class _Sleep:
    def __init__(self, clock: FakeClock, on_call: Any = None) -> None:
        self.clock = clock
        self.on_call = on_call
        self.calls = 0

    async def __call__(self, seconds: float) -> None:
        self.calls += 1
        self.clock.advance(round(seconds * 1000))
        if self.on_call is not None:
            await self.on_call(self.calls)


@pytest.fixture
def lock_id() -> int:
    return next(_lock_ids)


@pytest_asyncio.fixture
async def observer(test_db_url: URL) -> AsyncIterator[Any]:
    conn = await connect_lock_session(test_db_url, server_settings={"application_name": "ctb-test-observer"})
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def locks(test_db_url: URL, fake_clock: FakeClock) -> AsyncIterator[list[InstanceLock]]:
    created: list[InstanceLock] = []
    yield created
    for lock in created:
        await lock.release()


def _new(test_db_url: URL, created: list[InstanceLock], lock_id: int, sleep: Any) -> InstanceLock:
    lock = InstanceLock(test_db_url, lock_id=lock_id, sleep=sleep)
    created.append(lock)
    return lock


async def _holder_pids(observer: Any, lock_id: int) -> list[int]:
    rows = await observer.fetch(
        """
        SELECT pid FROM pg_locks
        WHERE locktype = 'advisory' AND granted AND objsubid = 2
          AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND classid = $1::bigint::oid AND objid = $2::bigint::oid
        """,
        LOCK_NAMESPACE,
        lock_id,
    )
    return [int(row["pid"]) for row in rows]


async def test_lock_session_has_server_keepalives(test_db_url: URL) -> None:
    conn = await connect_lock_session(test_db_url, server_settings=lock_server_settings())
    try:
        assert await conn.fetchval("SHOW tcp_keepalives_idle") == str(KEEPALIVE_IDLE_S)
        assert await conn.fetchval("SHOW tcp_keepalives_interval") == str(KEEPALIVE_INTERVAL_S)
        assert await conn.fetchval("SHOW tcp_keepalives_count") == str(KEEPALIVE_COUNT)
        assert await conn.fetchval("SHOW idle_session_timeout") == "0"
        assert await conn.fetchval("SHOW application_name") == application_name(os.getpid())
    finally:
        await conn.close()


async def test_acquired_lock_is_visible_in_pg_locks(
    test_db_url: URL, locks: list[InstanceLock], lock_id: int, fake_clock: FakeClock, observer: Any
) -> None:
    lock = _new(test_db_url, locks, lock_id, _Sleep(fake_clock))
    await lock.acquire(wait_ms=0)

    assert lock.held is True
    assert await _holder_pids(observer, lock_id) == [lock.session_pid]

    await lock.release()
    assert await _holder_pids(observer, lock_id) == []


async def test_second_lock_fails_and_names_the_holder(
    test_db_url: URL, locks: list[InstanceLock], lock_id: int, fake_clock: FakeClock
) -> None:
    first = _new(test_db_url, locks, lock_id, _Sleep(fake_clock))
    await first.acquire(wait_ms=0)
    sleep = _Sleep(fake_clock)
    second = _new(test_db_url, locks, lock_id, sleep)

    with pytest.raises(InstanceLockHeld) as info:
        await second.acquire(wait_ms=3000)

    (holder,) = info.value.holders
    assert holder.session_pid == first.session_pid
    assert holder.application_name == application_name(os.getpid())
    assert holder.client_os_pid == os.getpid()
    assert holder.usename is not None
    assert isinstance(holder.backend_start_ms, int)
    assert holder.backend_start_ms > 1_600_000_000_000  # мс, а не секунди
    assert sleep.calls == 3
    assert second.held is False
    assert first.held is True


async def test_lock_released_during_wait_is_taken(
    test_db_url: URL, locks: list[InstanceLock], lock_id: int, fake_clock: FakeClock, observer: Any
) -> None:
    first = _new(test_db_url, locks, lock_id, _Sleep(fake_clock))
    await first.acquire(wait_ms=0)

    async def release_first(call: int) -> None:
        if call == 2:
            await first.release()

    second = _new(test_db_url, locks, lock_id, _Sleep(fake_clock, on_call=release_first))
    await second.acquire(wait_ms=60_000)

    assert second.held is True
    assert await _holder_pids(observer, lock_id) == [second.session_pid]


async def test_terminated_session_is_detected_by_watch(
    test_db_url: URL, locks: list[InstanceLock], lock_id: int, fake_clock: FakeClock, observer: Any
) -> None:
    lock = _new(test_db_url, locks, lock_id, _Sleep(fake_clock))
    await lock.acquire(wait_ms=0)
    watch = asyncio.create_task(lock.watch(interval_ms=3_600_000))
    await asyncio.sleep(0)

    assert await observer.fetchval("SELECT pg_terminate_backend($1, 5000)", lock.session_pid) is True

    with pytest.raises(InstanceLockLost) as info:
        await asyncio.wait_for(watch, timeout=10)
    assert info.value.reason == "connection_closed"
    assert lock.held is False
    assert await _holder_pids(observer, lock_id) == []


async def test_heartbeat_detects_lock_released_behind_our_back(
    test_db_url: URL, locks: list[InstanceLock], lock_id: int, fake_clock: FakeClock, observer: Any
) -> None:
    lock = _new(test_db_url, locks, lock_id, _Sleep(fake_clock))
    await lock.acquire(wait_ms=0)
    # Імітація: сесія жива, але lock уже не її (напр. pg_advisory_unlock_all після помилки в коді).
    conn = lock._conn  # тест навмисно ламає стан сесії через внутрішнє з'єднання
    assert conn is not None
    await conn.execute("SELECT pg_advisory_unlock_all()")

    with pytest.raises(InstanceLockLost) as info:
        await asyncio.wait_for(lock.watch(interval_ms=0), timeout=10)
    assert info.value.reason == "lock_not_held"


async def _audit_rows(migrated_db: URL, marker: int) -> list[dict[str, Any]]:
    conn = await connect_lock_session(migrated_db, server_settings={"application_name": "ctb-test-audit"})
    try:
        rows = await conn.fetch(
            "SELECT actor, action, target, confirmed, request FROM audit_log "
            "WHERE action = $1 AND (request -> 'lock' ->> 1)::bigint = $2 ORDER BY id",
            AUDIT_ACTION_FORCE_TAKEOVER,
            marker,
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


async def test_force_takeover_terminates_holder_and_writes_audit(
    migrated_db: URL, locks: list[InstanceLock], lock_id: int, fake_clock: FakeClock, observer: Any
) -> None:
    first = _new(migrated_db, locks, lock_id, _Sleep(fake_clock))
    await first.acquire(wait_ms=0)
    first_pid = first.session_pid
    assert first_pid is not None
    first_watch = asyncio.create_task(first.watch(interval_ms=3_600_000))
    seen: list[Sequence[LockHolder]] = []

    async def confirm(holders: Sequence[LockHolder]) -> bool:
        seen.append(holders)
        return True

    second = _new(migrated_db, locks, lock_id, _Sleep(fake_clock))
    await second.acquire(wait_ms=10_000, takeover=confirm)

    assert second.held is True
    assert [h.session_pid for h in seen[0]] == [first_pid]
    assert await _holder_pids(observer, lock_id) == [second.session_pid]
    with pytest.raises(InstanceLockLost):
        await asyncio.wait_for(first_watch, timeout=10)

    (row,) = await _audit_rows(migrated_db, lock_id)
    assert (row["actor"], row["target"], row["confirmed"]) == ("cli", "backend", True)
    request = json.loads(row["request"])
    assert [h["session_pid"] for h in request["holders"]] == [first_pid]


async def test_refused_takeover_keeps_holder(
    migrated_db: URL, locks: list[InstanceLock], lock_id: int, fake_clock: FakeClock, observer: Any
) -> None:
    first = _new(migrated_db, locks, lock_id, _Sleep(fake_clock))
    await first.acquire(wait_ms=0)

    async def refuse(holders: Sequence[LockHolder]) -> bool:
        return False

    second = _new(migrated_db, locks, lock_id, _Sleep(fake_clock))
    with pytest.raises(TakeoverRefused):
        await second.acquire(wait_ms=10_000, takeover=refuse)

    assert await _holder_pids(observer, lock_id) == [first.session_pid]
    assert await _audit_rows(migrated_db, lock_id) == []
