"""Single-instance lock торгового процесу (ТЗ §6.4, T0.6): unit-тести без БД.

З'єднання PostgreSQL підмінене на межі драйвера (`connect=`) фейком із журналом подій; час — `FakeClock`,
`sleep` лише рухає фейковий годинник. Поведінку на реальній БД перевіряють `tests/db/test_instance_lock*.py`.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import deque
from collections.abc import Mapping, Sequence
from typing import Any

import pytest
from sqlalchemy.engine import URL

from app.core.clock import FakeClock
from app.core.instance_lock import (
    AUDIT_ACTION_FORCE_TAKEOVER,
    BACKEND_LOCK_ID,
    KEEPALIVE_COUNT,
    KEEPALIVE_IDLE_S,
    KEEPALIVE_INTERVAL_S,
    KEEPALIVE_TIMEOUT_S,
    LOCK_NAMESPACE,
    RETRY_INTERVAL_MS,
    InstanceLock,
    InstanceLockHeld,
    InstanceLockLost,
    InstanceLockUnavailable,
    LockHolder,
    TakeoverRefused,
    application_name,
    parse_client_os_pid,
    run_exclusive,
)

URL_ = URL.create("postgresql+asyncpg", username="u", host="127.0.0.1", port=5432, database="ctb_test")
HANG = "hang"

HOLDER_ROW: dict[str, object] = {
    "session_pid": 777,
    "application_name": "ctb-backend pid=4321",
    "client_addr": "172.18.0.1",
    "client_port": 55210,
    "backend_start_ms": 1_767_225_000_000,
    "state": "idle",
    "state_change_ms": 1_767_225_000_500,
    "usename": "ctb",
}
HOLDER = LockHolder(
    session_pid=777,
    application_name="ctb-backend pid=4321",
    client_os_pid=4321,
    client_addr="172.18.0.1",
    client_port=55210,
    backend_start_ms=1_767_225_000_000,
    state="idle",
    state_change_ms=1_767_225_000_500,
    usename="ctb",
)


class FakeDb:
    """Сценарій БД: результати спроб lock, heartbeat, помилки підключення; журнал подій усіх з'єднань."""

    def __init__(self) -> None:
        self.events: list[str] = []
        self.connect_errors: deque[BaseException] = deque()
        self.always_refuse_connect = False
        # Результат кожної наступної спроби: True/False, виняток або HANG; порожньо → `lock_free`.
        self.try_results: deque[object] = deque()
        self.lock_free = True
        self.heartbeats: deque[object] = deque()
        self.holders_rows: list[dict[str, object]] = [HOLDER_ROW]
        # Якщо задано — результати наступних запитів власників по черзі (далі — `holders_rows`).
        self.holders_sequence: deque[list[dict[str, object]]] = deque()
        # Результат умовного pg_terminate_backend: True/False — сесія ще власник; None — 0 рядків (вже не власник).
        self.terminate_result: object = True
        self.audit_error: BaseException | None = None
        self.audit_args: list[tuple[object, ...]] = []
        self.connections: list[FakeConn] = []
        self.server_settings: list[dict[str, str]] = []
        self.try_args: list[tuple[object, ...]] = []

    async def connect(self, url: URL, *, server_settings: Mapping[str, str]) -> FakeConn:
        self.events.append("connect")
        self.server_settings.append(dict(server_settings))
        if self.always_refuse_connect:
            raise ConnectionRefusedError("connect refused")
        if self.connect_errors:
            raise self.connect_errors.popleft()
        conn = FakeConn(self, pid=5000 + len(self.connections))
        self.connections.append(conn)
        return conn

    def count(self, event: str) -> int:
        return self.events.count(event)


async def _resolve(item: object) -> object:
    if item == HANG:
        await asyncio.Event().wait()
    if isinstance(item, BaseException):
        raise item
    return item


class FakeConn:
    def __init__(self, db: FakeDb, pid: int) -> None:
        self.db = db
        self.pid = pid
        self.closed = False
        self.terminated = False
        self._listeners: list[Any] = []

    async def fetchval(self, query: str, *args: object) -> object:
        if "pg_try_advisory_lock" in query:
            self.db.events.append("try_lock")
            self.db.try_args.append(args)
            return await _resolve(self.db.try_results.popleft() if self.db.try_results else self.db.lock_free)
        if "pg_terminate_backend" in query:
            self.db.events.append(f"terminate:{args[2]}")
            return self.db.terminate_result
        if "pg_backend_pid" in query:
            self.db.events.append("heartbeat")
            return await _resolve(self.db.heartbeats.popleft() if self.db.heartbeats else True)
        raise AssertionError(f"неочікуваний запит: {query}")

    async def fetch(self, query: str, *args: object) -> Sequence[Mapping[str, object]]:
        assert "pg_stat_activity" in query
        self.db.events.append("holders")
        if self.db.holders_sequence:
            return list(self.db.holders_sequence.popleft())
        return list(self.db.holders_rows)

    async def execute(self, query: str, *args: object) -> str:
        assert "audit_log" in query
        self.db.events.append("audit")
        if self.db.audit_error is not None:
            raise self.db.audit_error
        self.db.audit_args.append(args)
        return "INSERT 0 1"

    def get_server_pid(self) -> int:
        return self.pid

    def add_termination_listener(self, callback: Any) -> None:
        self._listeners.append(callback)

    def fire_termination(self) -> None:
        for callback in list(self._listeners):
            callback(self)

    async def close(self) -> None:
        self.db.events.append("close")
        self.closed = True
        self.fire_termination()

    def terminate(self) -> None:
        self.db.events.append("terminate_conn")
        self.terminated = True
        self.fire_termination()

    def is_closed(self) -> bool:
        return self.closed or self.terminated


class FakeSleep:
    """`sleep`, що лише рухає фейковий годинник (без реального очікування)."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        self.clock.advance(round(seconds * 1000))
        await asyncio.sleep(0)


@pytest.fixture
def db() -> FakeDb:
    return FakeDb()


@pytest.fixture
def sleep(fake_clock: FakeClock) -> FakeSleep:
    return FakeSleep(fake_clock)


def _lock(db: FakeDb, sleep: FakeSleep, **kwargs: Any) -> InstanceLock:
    return InstanceLock(URL_, connect=db.connect, sleep=sleep, **kwargs)


async def _yield_until(predicate: Any) -> None:
    """Віддати керування циклу подій, доки умова не стане істинною (детерміновано, без реального часу)."""
    for _ in range(1000):
        if predicate():
            return
        await asyncio.sleep(0)
    pytest.fail("умова так і не виконалась")


# --- параметри з'єднання ---


@pytest.mark.asyncio
async def test_free_lock_is_taken_with_one_attempt_on_dedicated_keepalive_session(db: FakeDb, sleep: FakeSleep) -> None:
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=60_000)

    assert lock.held is True
    assert lock.session_pid == 5000
    assert db.events == ["connect", "try_lock"]
    assert sleep.calls == []
    assert db.try_args == [(LOCK_NAMESPACE, BACKEND_LOCK_ID)]
    assert db.server_settings == [
        {
            "application_name": f"ctb-backend pid={os.getpid()}",
            "tcp_keepalives_idle": "10",
            "tcp_keepalives_interval": "5",
            "tcp_keepalives_count": "3",
            "idle_session_timeout": "0",
        }
    ]


def test_keepalive_timeout_is_idle_plus_interval_times_count() -> None:
    assert (KEEPALIVE_IDLE_S, KEEPALIVE_INTERVAL_S, KEEPALIVE_COUNT) == (10, 5, 3)
    assert KEEPALIVE_TIMEOUT_S == KEEPALIVE_IDLE_S + KEEPALIVE_INTERVAL_S * KEEPALIVE_COUNT == 25


def test_lock_key_is_fixed() -> None:
    # Зміна ключа = старий і новий процеси не бачать один одного. Лише свідомо, з міграцією процедур.
    assert (LOCK_NAMESPACE, BACKEND_LOCK_ID) == (0x63746200, 1)
    assert 0 < LOCK_NAMESPACE < 2**31


def test_application_name_is_short_ascii_and_parseable() -> None:
    name = application_name(4_194_304)
    assert name == "ctb-backend pid=4194304"
    assert name.isascii()
    assert len(name) <= 63  # NAMEDATALEN - 1: PostgreSQL обрізає довші
    assert parse_client_os_pid(name) == 4_194_304


@pytest.mark.parametrize("value", [None, "", "psql", "ctb-backend pid=", "ctb-backend pid=12a", "xctb-backend pid=1"])
def test_foreign_application_name_has_no_client_pid(value: str | None) -> None:
    assert parse_client_os_pid(value) is None


# --- повтори та дедлайн ---


@pytest.mark.asyncio
async def test_busy_lock_is_retried_every_second_until_deadline_then_reports_holders(
    db: FakeDb, sleep: FakeSleep
) -> None:
    db.lock_free = False
    lock = _lock(db, sleep)

    with pytest.raises(InstanceLockHeld) as info:
        await lock.acquire(wait_ms=3000)

    assert info.value.holders == (HOLDER,)
    assert RETRY_INTERVAL_MS == 1000
    assert sleep.calls == [1.0, 1.0, 1.0]
    assert db.count("try_lock") == 4  # t = 0, 1, 2, 3 с
    assert db.count("connect") == 1  # одне з'єднання на всі спроби
    assert lock.held is False
    assert lock.session_pid is None
    assert db.connections[0].is_closed()


@pytest.mark.asyncio
async def test_last_sleep_is_cut_to_the_deadline(db: FakeDb, sleep: FakeSleep) -> None:
    db.lock_free = False
    with pytest.raises(InstanceLockHeld):
        await _lock(db, sleep).acquire(wait_ms=2500)
    assert sleep.calls == [1.0, 1.0, 0.5]


@pytest.mark.asyncio
async def test_lock_released_during_wait_is_taken(db: FakeDb, sleep: FakeSleep) -> None:
    db.try_results.extend([False, False, True])
    lock = _lock(db, sleep)

    await lock.acquire(wait_ms=60_000)

    assert lock.held is True
    assert sleep.calls == [1.0, 1.0]
    assert db.count("try_lock") == 3


@pytest.mark.asyncio
async def test_zero_wait_makes_exactly_one_attempt(db: FakeDb, sleep: FakeSleep) -> None:
    db.lock_free = False
    with pytest.raises(InstanceLockHeld):
        await _lock(db, sleep).acquire(wait_ms=0)
    assert db.count("try_lock") == 1
    assert sleep.calls == []


@pytest.mark.asyncio
async def test_negative_wait_is_rejected(db: FakeDb, sleep: FakeSleep) -> None:
    with pytest.raises(ValueError, match="wait_ms"):
        await _lock(db, sleep).acquire(wait_ms=-1)
    assert db.events == []


@pytest.mark.asyncio
async def test_second_acquire_on_held_lock_is_an_error(db: FakeDb, sleep: FakeSleep) -> None:
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    with pytest.raises(RuntimeError):
        await lock.acquire(wait_ms=0)
    assert db.count("try_lock") == 1


@pytest.mark.asyncio
async def test_db_unavailable_for_whole_wait_raises_unavailable(db: FakeDb, sleep: FakeSleep) -> None:
    db.always_refuse_connect = True
    lock = _lock(db, sleep)

    with pytest.raises(InstanceLockUnavailable) as info:
        await lock.acquire(wait_ms=2000)

    assert info.value.last_error == "ConnectionRefusedError"
    assert db.count("connect") == 3
    assert lock.held is False


@pytest.mark.asyncio
async def test_db_recovering_during_wait_is_tolerated(db: FakeDb, sleep: FakeSleep) -> None:
    db.connect_errors.extend([ConnectionRefusedError("refused"), TimeoutError()])
    lock = _lock(db, sleep)

    await lock.acquire(wait_ms=60_000)

    assert lock.held is True
    assert db.count("connect") == 3
    assert sleep.calls == [1.0, 1.0]


@pytest.mark.asyncio
async def test_connection_dropped_during_attempt_is_replaced(db: FakeDb, sleep: FakeSleep) -> None:
    db.try_results.extend([ConnectionResetError("reset"), True])
    lock = _lock(db, sleep)

    await lock.acquire(wait_ms=60_000)

    assert lock.held is True
    assert lock.session_pid == 5001
    assert db.connections[0].terminated is True
    assert db.connections[1].is_closed() is False


@pytest.mark.asyncio
async def test_outcome_at_deadline_is_the_last_attempt(db: FakeDb, sleep: FakeSleep) -> None:
    # Спершу lock зайнятий, потім БД зникла до кінця очікування: причина відмови — недоступність БД.
    db.try_results.extend([False, ConnectionResetError("reset")])

    async def refuse_after_first(url: URL, *, server_settings: Mapping[str, str]) -> FakeConn:
        if db.connections:
            db.events.append("connect")
            raise ConnectionRefusedError("refused")
        return await db.connect(url, server_settings=server_settings)

    lock = InstanceLock(URL_, connect=refuse_after_first, sleep=sleep)
    with pytest.raises(InstanceLockUnavailable) as info:
        await lock.acquire(wait_ms=2000)
    assert info.value.last_error == "ConnectionRefusedError"


@pytest.mark.asyncio
async def test_cancellation_during_attempt_drops_the_connection(db: FakeDb, sleep: FakeSleep) -> None:
    # Скасування посеред `pg_try_advisory_lock`: сервер міг уже видати lock — з'єднання має бути закрите.
    db.try_results.append(HANG)
    lock = _lock(db, sleep)

    task = asyncio.create_task(lock.acquire(wait_ms=60_000))
    await _yield_until(lambda: "try_lock" in db.events)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert db.connections[0].terminated is True
    assert lock.held is False


# --- watchdog ---


@pytest.mark.asyncio
async def test_watch_raises_lost_when_session_no_longer_holds_lock(db: FakeDb, sleep: FakeSleep) -> None:
    db.heartbeats.extend([True, True, False])
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)

    with pytest.raises(InstanceLockLost) as info:
        await lock.watch(interval_ms=0)

    assert info.value.reason == "lock_not_held"
    assert db.count("heartbeat") == 3
    assert lock.held is False
    assert lock.lost_reason == "lock_not_held"
    # З'єднання закриває лише release() — після зупинки роботи (run_exclusive).
    assert db.connections[0].is_closed() is False
    await lock.release()
    assert db.connections[0].is_closed() is True


@pytest.mark.asyncio
async def test_watch_raises_lost_on_heartbeat_error(db: FakeDb, sleep: FakeSleep) -> None:
    db.heartbeats.append(ConnectionResetError("reset"))
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)

    with pytest.raises(InstanceLockLost) as info:
        await lock.watch(interval_ms=0)

    assert info.value.reason == "heartbeat_failed:ConnectionResetError"
    assert lock.held is False


@pytest.mark.asyncio
async def test_watch_raises_lost_on_heartbeat_timeout(db: FakeDb, sleep: FakeSleep) -> None:
    db.heartbeats.append(HANG)
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)

    with pytest.raises(InstanceLockLost) as info:
        await lock.watch(interval_ms=0, timeout_ms=10)

    assert info.value.reason == "heartbeat_timeout"
    assert lock.held is False
    # Сервер, можливо, ще вважає lock нашим: віддати його можна лише після зупинки роботи.
    assert db.connections[0].is_closed() is False


@pytest.mark.asyncio
async def test_watch_reacts_to_connection_termination_without_waiting_for_heartbeat(
    db: FakeDb, sleep: FakeSleep
) -> None:
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)

    watch = asyncio.create_task(lock.watch(interval_ms=3_600_000))
    await asyncio.sleep(0)
    db.connections[0].fire_termination()
    assert lock.held is False  # одразу, ще до того, як watchdog прокинувся
    with pytest.raises(InstanceLockLost) as info:
        await asyncio.wait_for(watch, timeout=5)

    assert info.value.reason == "connection_closed"
    assert db.count("heartbeat") == 0


@pytest.mark.asyncio
async def test_watch_on_unheld_lock_raises_immediately(db: FakeDb, sleep: FakeSleep) -> None:
    with pytest.raises(InstanceLockLost):
        await _lock(db, sleep).watch(interval_ms=0)


@pytest.mark.asyncio
async def test_ensure_held(db: FakeDb, sleep: FakeSleep) -> None:
    lock = _lock(db, sleep)
    with pytest.raises(InstanceLockLost):
        lock.ensure_held()
    await lock.acquire(wait_ms=0)
    lock.ensure_held()
    db.connections[0].fire_termination()
    with pytest.raises(InstanceLockLost):
        lock.ensure_held()


@pytest.mark.asyncio
async def test_release_closes_connection_and_is_not_a_loss(db: FakeDb, sleep: FakeSleep) -> None:
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)

    await lock.release()
    await lock.release()  # ідемпотентно

    assert db.connections[0].closed is True
    assert db.count("close") == 1
    assert lock.held is False
    assert lock.lost_reason is None


@pytest.mark.asyncio
async def test_release_without_acquire_is_noop(db: FakeDb, sleep: FakeSleep) -> None:
    await _lock(db, sleep).release()
    assert db.events == []


@pytest.mark.asyncio
async def test_release_falls_back_to_terminate_when_close_fails(db: FakeDb, sleep: FakeSleep) -> None:
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    conn = db.connections[0]

    async def broken_close() -> None:
        raise TimeoutError

    conn.close = broken_close  # type: ignore[method-assign]
    await lock.release()

    assert conn.terminated is True
    assert lock.lost_reason is None


# --- run_exclusive ---


def _never_stuck(reason: str) -> None:
    raise AssertionError(f"робота мала зупинитися вчасно ({reason})")


@pytest.mark.asyncio
async def test_run_exclusive_returns_when_body_finishes(db: FakeDb, sleep: FakeSleep) -> None:
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    done: list[bool] = []

    async def body() -> None:
        done.append(True)

    await asyncio.wait_for(run_exclusive(lock, body, on_stuck=_never_stuck, heartbeat_interval_ms=3_600_000), timeout=5)

    assert done == [True]
    assert lock.held is True  # звільняє викликач


@pytest.mark.asyncio
async def test_run_exclusive_cancels_body_and_raises_lost(db: FakeDb, sleep: FakeSleep) -> None:
    db.heartbeats.append(False)
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    cancelled: list[bool] = []

    async def body() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    with pytest.raises(InstanceLockLost) as info:
        await asyncio.wait_for(run_exclusive(lock, body, on_stuck=_never_stuck, heartbeat_interval_ms=0), timeout=5)

    assert info.value.reason == "lock_not_held"
    assert cancelled == [True]


@pytest.mark.asyncio
async def test_run_exclusive_propagates_body_error(db: FakeDb, sleep: FakeSleep) -> None:
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)

    async def body() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await asyncio.wait_for(
            run_exclusive(lock, body, on_stuck=_never_stuck, heartbeat_interval_ms=3_600_000), timeout=5
        )


@pytest.mark.asyncio
async def test_run_exclusive_refuses_to_start_without_lock(db: FakeDb, sleep: FakeSleep) -> None:
    started: list[bool] = []

    async def body() -> None:
        started.append(True)

    with pytest.raises(InstanceLockLost):
        await run_exclusive(_lock(db, sleep), body, on_stuck=_never_stuck)
    assert started == []


# --- --force-takeover ---


class Confirm:
    def __init__(self, answer: bool, clock: FakeClock | None = None, think_ms: int = 0) -> None:
        self.answer = answer
        self.clock = clock
        self.think_ms = think_ms
        self.calls: list[tuple[LockHolder, ...]] = []

    async def __call__(self, holders: Sequence[LockHolder]) -> bool:
        self.calls.append(tuple(holders))
        if self.clock is not None:
            self.clock.advance(self.think_ms)
        return self.answer


@pytest.mark.asyncio
async def test_takeover_refused_by_human_terminates_nothing(db: FakeDb, sleep: FakeSleep) -> None:
    db.lock_free = False
    confirm = Confirm(answer=False)
    lock = _lock(db, sleep)

    with pytest.raises(TakeoverRefused) as info:
        await lock.acquire(wait_ms=60_000, takeover=confirm)

    assert info.value.reason == "not_confirmed"
    assert confirm.calls == [(HOLDER,)]
    assert not any(e.startswith("terminate:") for e in db.events)
    assert db.count("audit") == 0
    assert lock.held is False
    assert db.connections[0].is_closed()


@pytest.mark.asyncio
async def test_takeover_writes_audit_before_terminating_holder(
    db: FakeDb, sleep: FakeSleep, fake_clock: FakeClock
) -> None:
    db.try_results.extend([False, True])
    confirm = Confirm(answer=True)
    lock = _lock(db, sleep)

    await lock.acquire(wait_ms=60_000, takeover=confirm)

    assert lock.held is True
    assert db.events == ["connect", "try_lock", "holders", "holders", "audit", "terminate:777", "try_lock"]
    (args,) = db.audit_args
    actor, action, target, reason, request, ts_ms = args
    assert action == AUDIT_ACTION_FORCE_TAKEOVER == "instance.force_takeover"
    assert actor == "cli"
    assert target == "backend"
    assert isinstance(reason, str)
    assert reason
    assert ts_ms == fake_clock.now_ms()
    assert isinstance(request, str)
    payload = json.loads(request)
    assert payload["holders"][0]["session_pid"] == 777
    assert payload["holders"][0]["client_os_pid"] == 4321
    assert payload["client_os_pid"] == os.getpid()
    assert payload["lock"] == [LOCK_NAMESPACE, BACKEND_LOCK_ID]


@pytest.mark.asyncio
async def test_takeover_aborts_if_audit_cannot_be_written(db: FakeDb, sleep: FakeSleep) -> None:
    db.lock_free = False
    db.audit_error = ConnectionResetError("reset")
    lock = _lock(db, sleep)

    with pytest.raises(TakeoverRefused) as info:
        await lock.acquire(wait_ms=60_000, takeover=Confirm(answer=True))

    assert info.value.reason == "audit_failed"
    assert not any(e.startswith("terminate:") for e in db.events)
    assert lock.held is False


@pytest.mark.asyncio
async def test_takeover_not_needed_when_lock_is_free(db: FakeDb, sleep: FakeSleep) -> None:
    confirm = Confirm(answer=True)
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0, takeover=confirm)
    assert lock.held is True
    assert confirm.calls == []
    assert db.count("audit") == 0


@pytest.mark.asyncio
async def test_takeover_happens_once_even_if_lock_stays_busy(db: FakeDb, sleep: FakeSleep) -> None:
    db.lock_free = False
    confirm = Confirm(answer=True)

    with pytest.raises(InstanceLockHeld):
        await _lock(db, sleep).acquire(wait_ms=2000, takeover=confirm)

    assert len(confirm.calls) == 1
    assert db.count("audit") == 1
    assert db.count("terminate:777") == 1


@pytest.mark.asyncio
async def test_takeover_wait_restarts_after_slow_confirmation(
    db: FakeDb, sleep: FakeSleep, fake_clock: FakeClock
) -> None:
    # Людина читає опис власника 2 хв: очікування звільнення після підтвердження — знову повний `wait_ms`.
    db.try_results.extend([False, False, False, True])
    lock = _lock(db, sleep)

    await lock.acquire(wait_ms=3000, takeover=Confirm(answer=True, clock=fake_clock, think_ms=120_000))

    assert lock.held is True
    assert sleep.calls == [1.0, 1.0]


@pytest.mark.asyncio
async def test_real_driver_reports_refused_connection_as_unavailable(sleep: FakeSleep) -> None:
    # Справжній asyncpg-конектор; порт 1 на loopback закритий — відмова одразу, без мережі.
    url = URL.create("postgresql+asyncpg", username="ctb", host="127.0.0.1", port=1, database="ctb_test")
    lock = InstanceLock(url, sleep=sleep)
    with pytest.raises(InstanceLockUnavailable) as info:
        await lock.acquire(wait_ms=1000)
    assert info.value.last_error is not None
    assert sleep.calls == [1.0]


# --- рев'ю T0.6: одноразовість, закритий сокет, зупинка роботи до звільнення lock, зміна власника ---


@pytest.mark.asyncio
async def test_acquire_is_single_use_after_release(db: FakeDb, sleep: FakeSleep) -> None:
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    await lock.release()
    with pytest.raises(RuntimeError):
        await lock.acquire(wait_ms=0)
    assert db.count("connect") == 1


@pytest.mark.asyncio
async def test_acquire_is_single_use_after_failure(db: FakeDb, sleep: FakeSleep) -> None:
    db.lock_free = False
    lock = _lock(db, sleep)
    with pytest.raises(InstanceLockHeld):
        await lock.acquire(wait_ms=0)
    db.lock_free = True
    with pytest.raises(RuntimeError):
        await lock.acquire(wait_ms=0)


@pytest.mark.asyncio
async def test_held_is_false_as_soon_as_socket_is_closed(db: FakeDb, sleep: FakeSleep) -> None:
    # asyncpg викликає listener лише на наступній ітерації циклу; `held` не повинен чекати на нього.
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    db.connections[0].closed = True
    assert lock.held is False
    with pytest.raises(InstanceLockLost):
        lock.ensure_held()


@pytest.mark.asyncio
async def test_run_exclusive_stops_body_before_lock_can_be_released(db: FakeDb, sleep: FakeSleep) -> None:
    db.heartbeats.append(HANG)  # таймаут heartbeat: lock на сервері, можливо, ще наш
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    seen_on_cancel: list[tuple[bool, bool]] = []

    async def body() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            seen_on_cancel.append((lock.held, db.connections[0].is_closed()))
            raise

    with pytest.raises(InstanceLockLost) as info:
        await asyncio.wait_for(
            run_exclusive(lock, body, on_stuck=_never_stuck, heartbeat_interval_ms=0, heartbeat_timeout_ms=10),
            timeout=5,
        )

    assert info.value.reason == "heartbeat_timeout"
    # Робота зупинена, поки з'єднання (і, можливо, lock) ще наше; нові входи вже заборонені.
    assert seen_on_cancel == [(False, False)]
    assert db.connections[0].is_closed() is False
    await lock.release()
    assert db.connections[0].is_closed() is True


@pytest.mark.asyncio
async def test_run_exclusive_calls_on_stuck_when_body_ignores_cancellation(db: FakeDb, sleep: FakeSleep) -> None:
    db.heartbeats.append(False)
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    let_go = asyncio.Event()
    finished: list[bool] = []
    stuck: list[str] = []

    async def stubborn_body() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await let_go.wait()  # ковтає скасування — порушення контракту body
        finished.append(True)

    with pytest.raises(InstanceLockLost):
        await asyncio.wait_for(
            run_exclusive(lock, stubborn_body, on_stuck=stuck.append, heartbeat_interval_ms=0, stop_timeout_ms=10),
            timeout=5,
        )

    assert stuck == ["lock_lost"]
    assert db.connections[0].is_closed() is False  # у процесі on_stuck = os._exit: ОС закриє сокет
    let_go.set()
    await _yield_until(lambda: finished == [True])


@pytest.mark.asyncio
async def test_run_exclusive_on_outer_cancel_stops_body_and_watchdog(db: FakeDb, sleep: FakeSleep) -> None:
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    started = asyncio.Event()
    cancelled: list[bool] = []

    async def body() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    task = asyncio.create_task(run_exclusive(lock, body, on_stuck=_never_stuck, heartbeat_interval_ms=3_600_000))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert cancelled == [True]
    assert lock.held is True  # lock не втрачено: звільняє викликач
    assert db.count("heartbeat") == 0


@pytest.mark.asyncio
async def test_run_exclusive_on_outer_cancel_reports_stuck_body(db: FakeDb, sleep: FakeSleep) -> None:
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    started = asyncio.Event()
    let_go = asyncio.Event()
    finished: list[bool] = []
    stuck: list[str] = []

    async def stubborn_body() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await let_go.wait()
        finished.append(True)

    task = asyncio.create_task(
        run_exclusive(lock, stubborn_body, on_stuck=stuck.append, heartbeat_interval_ms=3_600_000, stop_timeout_ms=10)
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert stuck == ["shutdown"]
    let_go.set()
    await _yield_until(lambda: finished == [True])


@pytest.mark.asyncio
async def test_takeover_refused_when_holder_changed_while_human_was_thinking(db: FakeDb, sleep: FakeSleep) -> None:
    db.lock_free = False
    other = {**HOLDER_ROW, "session_pid": 888, "backend_start_ms": 1_767_225_100_000}
    db.holders_sequence.extend([[HOLDER_ROW], [other]])
    lock = _lock(db, sleep)

    with pytest.raises(TakeoverRefused) as info:
        await lock.acquire(wait_ms=60_000, takeover=Confirm(answer=True))

    assert info.value.reason == "holder_changed"
    assert db.count("audit") == 0
    assert not any(e.startswith("terminate:") for e in db.events)
    assert lock.held is False


@pytest.mark.asyncio
async def test_takeover_skipped_when_holder_is_gone_after_confirmation(db: FakeDb, sleep: FakeSleep) -> None:
    db.try_results.extend([False, True])
    db.holders_sequence.extend([[HOLDER_ROW], []])
    lock = _lock(db, sleep)

    await lock.acquire(wait_ms=60_000, takeover=Confirm(answer=True))

    assert lock.held is True
    assert db.count("audit") == 0
    assert not any(e.startswith("terminate:") for e in db.events)


@pytest.mark.asyncio
async def test_takeover_terminates_only_a_session_that_still_holds_the_lock(db: FakeDb, sleep: FakeSleep) -> None:
    db.try_results.extend([False, True])
    db.terminate_result = None  # умовний pg_terminate_backend: 0 рядків — сесія вже не власник
    lock = _lock(db, sleep)

    await lock.acquire(wait_ms=60_000, takeover=Confirm(answer=True))

    assert lock.held is True
    assert db.count("audit") == 1
    assert db.count("terminate:777") == 1


@pytest.mark.asyncio
async def test_signal_during_stop_after_loss_does_not_cut_the_stop_short(db: FakeDb, sleep: FakeSleep) -> None:
    # Рев'ю, раунд 2: SIGTERM, поки робота зупиняється після втрати lock, не повинен віддати lock раніше за роботу.
    db.heartbeats.append(False)
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    body_cancelled = asyncio.Event()
    let_go = asyncio.Event()
    finished: list[bool] = []

    async def slow_to_stop() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            body_cancelled.set()
            await let_go.wait()  # прибирання триває, але вкладається в дедлайн
            finished.append(True)
            raise

    task = asyncio.create_task(
        run_exclusive(lock, slow_to_stop, on_stuck=_never_stuck, heartbeat_interval_ms=0, stop_timeout_ms=60_000)
    )
    await body_cancelled.wait()
    task.cancel()  # «сигнал» посеред зупинки
    for _ in range(20):
        await asyncio.sleep(0)
    assert task.done() is False  # run_exclusive не повертається, поки робота жива

    let_go.set()
    with pytest.raises(InstanceLockLost) as info:  # втрата lock важливіша за сигнал → код 12, а не 0
        await asyncio.wait_for(task, timeout=5)
    assert info.value.reason == "lock_not_held"
    assert finished == [True]


@pytest.mark.asyncio
async def test_watchdog_crash_marks_lock_lost_before_stopping_body(db: FakeDb, sleep: FakeSleep) -> None:
    # Рев'ю, раунд 2: виняток watchdog-а не з `_DB_ERRORS` — за lock ніхто не стежить, входи мають бути заборонені.
    db.heartbeats.append(RuntimeError("internal client error"))
    lock = _lock(db, sleep)
    await lock.acquire(wait_ms=0)
    held_on_cancel: list[bool] = []

    async def body() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            held_on_cancel.append(lock.held)
            raise

    with pytest.raises(InstanceLockLost) as info:
        await asyncio.wait_for(run_exclusive(lock, body, on_stuck=_never_stuck, heartbeat_interval_ms=0), timeout=5)

    assert info.value.reason == "watchdog_failed:RuntimeError"
    assert isinstance(info.value.__cause__, RuntimeError)
    assert held_on_cancel == [False]
    assert lock.lost_reason == "watchdog_failed:RuntimeError"
