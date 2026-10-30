"""Single-instance lock торгового процесу (ТЗ §6.4, T0.6, `docs/decisions/0006-instance-lock.md`).

PostgreSQL advisory lock не дає запустити другий екземпляр торгового бекенду на тій самій БД:
- lock береться на **окремому** raw-з'єднанні asyncpg, а не з пулу SQLAlchemy: пул чи автореконект мовчки підмінили б
  сесію на нову — вже без lock;
- сесія має серверні TCP keepalive (`tcp_keepalives_*`): PostgreSQL виявляє мертвого TCP-співрозмовника не пізніше
  `KEEPALIVE_TIMEOUT_S` і звільняє lock. Після `kill -9` ОС закриває сокет одразу; за Docker/WSL2 співрозмовник
  сервера — проксі портів, і звільнення залежить від того, що проксі передасть закриття (зазвичай — мілісекунди);
- при старті спроби повторюються кожні `RETRY_INTERVAL_MS` протягом `wait_ms` (`Settings.lock_wait_s`), зокрема поки
  БД ще не піднялась; невдача → власник (`pg_locks` + `pg_stat_activity`) у лог і `InstanceLockHeld`;
- під час роботи `watch()` перевіряє, що lock досі наш (listener закриття з'єднання + heartbeat із таймаутом);
  втрата → `run_exclusive` зупиняє роботу (≤ `BODY_STOP_TIMEOUT_MS`, інакше аварійний вихід) і лише потім
  `InstanceLockLost` → `release()`: процес завершується (fail-closed), а новий старт іде через звірку ТЗ §11.1;
- `--force-takeover` (лише людиною з TTY, `app/__main__.py`): підтвердження → перевірка, що власник той самий →
  запис `audit_log` → `pg_terminate_backend` сесії-власника, лише якщо вона досі тримає lock → звичайні спроби.

Lock діє в межах однієї БД (`POSTGRES_DB`): інша БД чи інший комп'ютер його не бачать
(захист між комп'ютерами — `allowed_host_id`, T5.1).

Той самий механізм з ключем `WORKER_LOCK_ID` і `application_name` `ctb-worker pid=N` тримає воркер задач
(`app/jobs/worker.py`, `docs/decisions/0008-single-worker.md`): на одній БД працює лише один воркер, бо частка
rate limit воркера (ТЗ §6.5) розрахована на один процес. Перехоплення для воркера немає.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Final, NoReturn, Protocol, cast

import asyncpg
from sqlalchemy.engine import URL

from app.api.models import AUDIT_ACTION_FORCE_TAKEOVER
from app.core import clock
from app.core.logging import get_logger

__all__ = [
    "APPLICATION_NAME_PREFIX",
    "AUDIT_ACTION_FORCE_TAKEOVER",
    "BACKEND_LOCK_ID",
    "BODY_STOP_TIMEOUT_MS",
    "HEARTBEAT_INTERVAL_MS",
    "HEARTBEAT_TIMEOUT_MS",
    "KEEPALIVE_COUNT",
    "KEEPALIVE_IDLE_S",
    "KEEPALIVE_INTERVAL_S",
    "KEEPALIVE_TIMEOUT_S",
    "LOCK_NAMESPACE",
    "RETRY_INTERVAL_MS",
    "TERMINATE_WAIT_MS",
    "WORKER_APPLICATION_NAME_PREFIX",
    "WORKER_LOCK_ID",
    "Connector",
    "InstanceLock",
    "InstanceLockError",
    "InstanceLockHeld",
    "InstanceLockLost",
    "InstanceLockUnavailable",
    "LockConnection",
    "LockHolder",
    "TakeoverConfirm",
    "TakeoverRefused",
    "application_name",
    "connect_lock_session",
    "lock_server_settings",
    "parse_client_os_pid",
    "run_exclusive",
]

# Ключ `pg_try_advisory_lock(int, int)`: у `pg_locks` — `classid` = простір, `objid` = ідентифікатор, `objsubid` = 2.
# Зміна ключа = старий і новий код не бачать один одного, тож лише свідомо.
LOCK_NAMESPACE: Final = 0x63746200  # "ctb\0"
BACKEND_LOCK_ID: Final = 1  # торговий процес
WORKER_LOCK_ID: Final = 2  # воркер задач: один процес на БД (частка rate limit ТЗ §6.5)
# Інший advisory-простір — знімок інструментів (`app/exchange/instruments.py`).

# Серверні keepalive сесії lock: PostgreSQL виявляє мертвого клієнта за idle + interval × count секунд.
KEEPALIVE_IDLE_S: Final = 10
KEEPALIVE_INTERVAL_S: Final = 5
KEEPALIVE_COUNT: Final = 3
KEEPALIVE_TIMEOUT_S: Final = KEEPALIVE_IDLE_S + KEEPALIVE_INTERVAL_S * KEEPALIVE_COUNT

RETRY_INTERVAL_MS: Final = 1000
HEARTBEAT_INTERVAL_MS: Final = 5000
HEARTBEAT_TIMEOUT_MS: Final = 5000
# Скільки `pg_terminate_backend` чекає завершення сесії-власника при перехопленні.
TERMINATE_WAIT_MS: Final = 5000
# Скільки `run_exclusive` чекає, поки робота зупиниться після скасування (втрата lock, сигнал).
BODY_STOP_TIMEOUT_MS: Final = 5000
_CONNECT_TIMEOUT_S: Final = 5.0
_QUERY_TIMEOUT_S: Final = 5.0
_CLOSE_TIMEOUT_S: Final = 5.0

# `application_name` сесії lock: за ним оператор знаходить процес ОС (`client_addr` за Docker — адреса шлюзу).
APPLICATION_NAME_PREFIX: Final = "ctb-backend"
WORKER_APPLICATION_NAME_PREFIX: Final = "ctb-worker"
_APPLICATION_NAME_PREFIXES: Final = (APPLICATION_NAME_PREFIX, WORKER_APPLICATION_NAME_PREFIX)
_APPLICATION_NAME_RE: Final = re.compile(
    "(?:" + "|".join(re.escape(prefix) for prefix in _APPLICATION_NAME_PREFIXES) + r") pid=(\d{1,10})"
)

_TAKEOVER_ACTOR: Final = "cli"
_TAKEOVER_TARGET: Final = "backend"
_TAKEOVER_REASON: Final = "--force-takeover: людина підтвердила в терміналі, що процес-власник lock не працює"

# Помилки з'єднання/сервера: при старті — повтор спроби, під час роботи — lock вважається втраченим.
_DB_ERRORS: Final[tuple[type[BaseException], ...]] = (
    OSError,
    TimeoutError,
    asyncpg.PostgresError,
    asyncpg.InterfaceError,
)

_TRY_LOCK_SQL: Final = "SELECT pg_try_advisory_lock($1::int, $2::int)"
_HOLDERS_SQL: Final = """
SELECT l.pid AS session_pid,
       a.application_name,
       host(a.client_addr) AS client_addr,
       a.client_port,
       (extract(epoch FROM a.backend_start) * 1000)::bigint AS backend_start_ms,
       a.state,
       (extract(epoch FROM a.state_change) * 1000)::bigint AS state_change_ms,
       a.usename::text AS usename
FROM pg_locks AS l
LEFT JOIN pg_stat_activity AS a ON a.pid = l.pid
WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 2
  AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND l.classid = $1::bigint::oid AND l.objid = $2::bigint::oid
ORDER BY l.pid
"""
_HELD_SQL: Final = """
SELECT EXISTS (
    SELECT 1 FROM pg_locks AS l
    WHERE l.pid = pg_backend_pid() AND l.locktype = 'advisory' AND l.granted AND l.objsubid = 2
      AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND l.classid = $1::bigint::oid AND l.objid = $2::bigint::oid
)
"""
# Завершити сесію-власника, лише якщо вона досі тримає lock і це та сама сесія (`backend_start`); інакше — 0 рядків.
_TERMINATE_SQL: Final = """
SELECT pg_terminate_backend(l.pid, $5::bigint)
FROM pg_locks AS l
LEFT JOIN pg_stat_activity AS a ON a.pid = l.pid
WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 2
  AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND l.classid = $1::bigint::oid AND l.objid = $2::bigint::oid
  AND l.pid = $3::int
  AND (extract(epoch FROM a.backend_start) * 1000)::bigint IS NOT DISTINCT FROM $4::bigint
"""
_AUDIT_SQL: Final = """
INSERT INTO audit_log (actor, action, target, reason, confirmed, request, ts_ms)
VALUES ($1, $2, $3, $4, true, $5::jsonb, $6)
"""


class LockConnection(Protocol):
    """Частина API `asyncpg.Connection`, якою користується lock (межа підміни в unit-тестах)."""

    async def fetchval(self, query: str, *args: object) -> object: ...

    async def fetch(self, query: str, *args: object) -> Sequence[Mapping[str, object]]: ...

    async def execute(self, query: str, *args: object) -> str: ...

    def get_server_pid(self) -> int: ...

    def add_termination_listener(self, callback: Callable[[Any], object]) -> None: ...

    async def close(self) -> None: ...

    def terminate(self) -> None: ...

    def is_closed(self) -> bool: ...


class Connector(Protocol):
    def __call__(self, url: URL, *, server_settings: Mapping[str, str]) -> Awaitable[LockConnection]: ...


Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class LockHolder:
    """Сесія PostgreSQL, що тримає lock (`pg_locks` + `pg_stat_activity`). Час — `int` мс UTC."""

    session_pid: int
    application_name: str | None
    # pid процесу ОС з `application_name` (`ctb-backend pid=N`), якщо власник — наш процес.
    client_os_pid: int | None
    client_addr: str | None
    client_port: int | None
    backend_start_ms: int | None
    state: str | None
    state_change_ms: int | None
    usename: str | None

    def as_log(self) -> dict[str, object]:
        return asdict(self)


# Підтвердження перехоплення людиною: отримує власників lock, повертає True лише при явній згоді.
TakeoverConfirm = Callable[[Sequence[LockHolder]], Awaitable[bool]]


class InstanceLockError(Exception):
    """Базовий виняток single-instance lock."""


class InstanceLockHeld(InstanceLockError):
    """Lock тримає інша сесія весь час очікування — другий екземпляр не стартує."""

    def __init__(self, holders: Sequence[LockHolder]) -> None:
        self.holders = tuple(holders)
        super().__init__(f"lock тримає інша сесія: {[h.session_pid for h in self.holders]}")


class InstanceLockUnavailable(InstanceLockError):
    """БД недоступна до кінця очікування — невідомо, чи працює інший екземпляр."""

    def __init__(self, last_error: str | None) -> None:
        self.last_error = last_error
        super().__init__(f"БД недоступна: {last_error}")


class InstanceLockLost(InstanceLockError):
    """Lock більше не наш (з'єднання закрито, heartbeat не пройшов) — процес має завершитися."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"lock втрачено: {reason}")


class TakeoverRefused(InstanceLockError):
    """Перехоплення не виконано: людина не підтвердила (`not_confirmed`) або аудит не записано (`audit_failed`)."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"перехоплення lock не виконано: {reason}")


def application_name(pid: int, *, prefix: str = APPLICATION_NAME_PREFIX) -> str:
    """`application_name` сесії lock: ASCII, ≤ 63 символів (довші PostgreSQL обрізає)."""
    _check_prefix(prefix)
    return f"{prefix} pid={pid}"


def _check_prefix(prefix: str) -> None:
    # Інший префікс `parse_client_os_pid` не розпізнав би: у лозі власника зник би pid процесу ОС.
    if prefix not in _APPLICATION_NAME_PREFIXES:
        raise ValueError(f"application_name prefix має бути одним з {_APPLICATION_NAME_PREFIXES}: {prefix!r}")


def parse_client_os_pid(name: str | None) -> int | None:
    """pid процесу ОС з `application_name` нашого процесу; для чужих сесій — None."""
    if name is None:
        return None
    match = _APPLICATION_NAME_RE.fullmatch(name)
    return int(match.group(1)) if match else None


def lock_server_settings(pid: int | None = None, *, prefix: str = APPLICATION_NAME_PREFIX) -> dict[str, str]:
    """Параметри сесії lock, що передаються серверу при підключенні (`server_settings` asyncpg)."""
    return {
        "application_name": application_name(os.getpid() if pid is None else pid, prefix=prefix),
        "tcp_keepalives_idle": str(KEEPALIVE_IDLE_S),
        "tcp_keepalives_interval": str(KEEPALIVE_INTERVAL_S),
        "tcp_keepalives_count": str(KEEPALIVE_COUNT),
        # Серверний таймаут простою не повинен закрити сесію, що просто тримає lock.
        "idle_session_timeout": "0",
    }


async def connect_lock_session(url: URL, *, server_settings: Mapping[str, str]) -> LockConnection:
    """Окреме raw-з'єднання asyncpg поза пулом. Пароль береться з `url` і не логується."""
    conn = await asyncpg.connect(
        host=url.host,
        port=url.port,
        user=url.username,
        password=url.password,
        database=url.database,
        timeout=_CONNECT_TIMEOUT_S,
        server_settings=dict(server_settings),
    )
    return cast(LockConnection, conn)


def _opt_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _opt_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _holder(row: Mapping[str, object]) -> LockHolder:
    session_pid = _opt_int(row["session_pid"])
    if session_pid is None:
        raise TypeError(f"pg_locks.pid має бути int: {row['session_pid']!r}")
    name = _opt_str(row["application_name"])
    return LockHolder(
        session_pid=session_pid,
        application_name=name,
        client_os_pid=parse_client_os_pid(name),
        client_addr=_opt_str(row["client_addr"]),
        client_port=_opt_int(row["client_port"]),
        backend_start_ms=_opt_int(row["backend_start_ms"]),
        state=_opt_str(row["state"]),
        state_change_ms=_opt_int(row["state_change_ms"]),
        usename=_opt_str(row["usename"]),
    )


class InstanceLock:
    """Advisory lock процесу (торговий — `BACKEND_LOCK_ID`, воркер — `WORKER_LOCK_ID`) на окремій сесії PostgreSQL.

    Життєвий цикл: `acquire()` → `run_exclusive()` (робота під `watch()`) → `release()`. Один екземпляр — одна спроба
    володіння; після втрати чи звільнення новий lock береться новим процесом (новий старт = звірка, ТЗ §11.1).
    """

    def __init__(
        self,
        url: URL,
        *,
        lock_id: int = BACKEND_LOCK_ID,
        application_name_prefix: str = APPLICATION_NAME_PREFIX,
        connect: Connector = connect_lock_session,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        _check_prefix(application_name_prefix)
        self._url = url
        self._key = (LOCK_NAMESPACE, lock_id)
        self._prefix = application_name_prefix
        self._connect = connect
        self._sleep = sleep
        self._conn: LockConnection | None = None
        # Сигнал listener-а asyncpg: поточне з'єднання закрито (ставиться заново для кожного з'єднання).
        self._closed = asyncio.Event()
        self._held = False
        # Екземпляр одноразовий: другий `acquire()` (після невдачі, звільнення чи втрати) — помилка.
        self._used = False
        self._session_pid: int | None = None
        self._lost_reason: str | None = None
        self._log = get_logger(__name__, lock=list(self._key))

    @property
    def held(self) -> bool:
        """Lock наш: взято, з'єднання відкрите і втрату не виявлено. Нові входи — лише при `True` (T5.2).

        `is_closed()` перевіряється окремо: listener asyncpg спрацьовує лише на наступній ітерації циклу подій.
        """
        conn = self._conn
        return self._held and conn is not None and not conn.is_closed() and not self._closed.is_set()

    @property
    def session_pid(self) -> int | None:
        """pid серверної сесії, що тримає lock (None, якщо lock не наш)."""
        return self._session_pid if self.held else None

    @property
    def lost_reason(self) -> str | None:
        """Причина втрати lock під час роботи; None, якщо не втрачався (звільнення — не втрата)."""
        return self._lost_reason

    def ensure_held(self) -> None:
        """`InstanceLockLost`, якщо lock не наш. Викликається перед дією, що збільшує експозицію (T5.2)."""
        if not self.held:
            if self._lost_reason is not None:
                reason = self._lost_reason
            elif self._held:
                reason = "connection_closed"
            else:
                reason = "not_held"
            raise InstanceLockLost(reason)

    async def acquire(self, *, wait_ms: int, takeover: TakeoverConfirm | None = None) -> None:
        """Взяти lock, повторюючи спроби кожні `RETRY_INTERVAL_MS` протягом `wait_ms` (0 — одна спроба).

        Помилки з'єднання до кінця очікування не фатальні (БД може ще стартувати). Наприкінці: lock зайнятий →
        `InstanceLockHeld` з власниками (і в лог), БД недоступна → `InstanceLockUnavailable`. З `takeover` один раз:
        підтвердження людини → повторна перевірка, що власник той самий → `audit_log` → `pg_terminate_backend`
        власника; після підтвердження очікування відраховується заново. Будь-яка невдача чи скасування закриває
        з'єднання: lock, який сервер міг видати в останню мить, не лишається за процесом. Викликається один раз на
        екземпляр.
        """
        if wait_ms < 0:
            raise ValueError("wait_ms не може бути від'ємним")
        if self._used:
            raise RuntimeError("acquire() вже викликався — для нового володіння потрібен новий процес (звірка, §11.1)")
        self._used = True
        started_ms = clock.monotonic_ms()
        deadline_ms = started_ms + wait_ms
        attempts = 0
        busy = False
        holders: tuple[LockHolder, ...] = ()
        last_error: str | None = None
        logged_busy = False
        takeover_pending = takeover is not None
        try:
            while True:
                attempts += 1
                try:
                    conn = await self._ensure_connection()
                    if await self._try_lock(conn):
                        self._held = True
                        self._session_pid = conn.get_server_pid()
                        self._log.info(
                            "instance_lock_acquired",
                            session_pid=self._session_pid,
                            attempts=attempts,
                            waited_ms=clock.monotonic_ms() - started_ms,
                            keepalive_timeout_s=KEEPALIVE_TIMEOUT_S,
                        )
                        return
                    busy, last_error = True, None
                    holders = await self._holders(conn)
                    if not logged_busy:
                        logged_busy = True
                        self._log.warning("instance_lock_busy", holders=[h.as_log() for h in holders], wait_ms=wait_ms)
                    if takeover is not None and takeover_pending and holders:
                        takeover_pending = False
                        await self._take_over(conn, holders, takeover)
                        deadline_ms = max(deadline_ms, clock.monotonic_ms() + wait_ms)
                        continue
                except _DB_ERRORS as exc:
                    error = type(exc).__name__
                    if error != last_error:
                        self._log.warning("instance_lock_db_error", error_type=error, error=str(exc), attempts=attempts)
                    busy, last_error = False, error
                    self._drop_connection()
                remaining_ms = deadline_ms - clock.monotonic_ms()
                if remaining_ms <= 0:
                    break
                await self._sleep(min(RETRY_INTERVAL_MS, remaining_ms) / 1000)
        except BaseException:
            self._drop_connection()
            raise
        self._drop_connection()
        waited_ms = clock.monotonic_ms() - started_ms
        if busy:
            self._log.error(
                "instance_lock_held_by_other",
                holders=[h.as_log() for h in holders],
                attempts=attempts,
                waited_ms=waited_ms,
            )
            raise InstanceLockHeld(holders)
        self._log.error("instance_lock_db_unavailable", last_error=last_error, attempts=attempts, waited_ms=waited_ms)
        raise InstanceLockUnavailable(last_error)

    async def watch(
        self, *, interval_ms: int = HEARTBEAT_INTERVAL_MS, timeout_ms: int = HEARTBEAT_TIMEOUT_MS
    ) -> NoReturn:
        """Стежити за lock, доки його не втрачено; тоді — CRITICAL у лог і `InstanceLockLost`.

        Закриття з'єднання помічається одразу (listener asyncpg), решта — heartbeat кожні `interval_ms`:
        запит «чи моя сесія тримає lock» з таймаутом `timeout_ms`. Помилка, таймаут чи `false` = втрата.
        Після втрати `held = False`, але з'єднання **не** закривається: при таймауті heartbeat lock на сервері може
        бути ще нашим, і віддавати його можна лише після зупинки роботи — це робить `release()` (`run_exclusive`).
        """
        while True:
            conn = self._conn
            if conn is None or not self._held:
                raise InstanceLockLost(self._lost_reason or "not_held")
            if self._closed.is_set():
                self._lose("connection_closed")
            try:
                await asyncio.wait_for(self._closed.wait(), interval_ms / 1000)
            except TimeoutError:
                pass
            else:
                self._lose("connection_closed")
            try:
                still_held = await asyncio.wait_for(conn.fetchval(_HELD_SQL, *self._key), timeout_ms / 1000)
            except TimeoutError:
                self._lose("heartbeat_timeout")
            except _DB_ERRORS as exc:
                self._lose(f"heartbeat_failed:{type(exc).__name__}")
            if still_held is not True:
                self._lose("lock_not_held")

    async def release(self) -> None:
        """Звільнити lock закриттям сесії. Ідемпотентно; власне закриття не вважається втратою."""
        conn, self._conn = self._conn, None
        was_held = self._held
        self._held = False
        self._session_pid = None
        if conn is None:
            return
        try:
            if not conn.is_closed():
                await asyncio.wait_for(conn.close(), _CLOSE_TIMEOUT_S)
        except _DB_ERRORS as exc:
            self._log.warning("instance_lock_close_failed", error_type=type(exc).__name__)
        finally:
            if not conn.is_closed():
                conn.terminate()
        if was_held:
            self._log.info("instance_lock_released")

    # --- внутрішнє ---

    async def _ensure_connection(self) -> LockConnection:
        if self._conn is not None and not self._conn.is_closed():
            return self._conn
        self._drop_connection()
        conn = await self._connect(self._url, server_settings=lock_server_settings(prefix=self._prefix))
        self._closed = asyncio.Event()
        self._conn = conn
        conn.add_termination_listener(self._on_connection_closed)
        return conn

    def _on_connection_closed(self, conn: object) -> None:
        # Listener старого з'єднання (замінене, звільнене) ігнорується: `_conn` уже інше або None.
        if conn is self._conn:
            self._closed.set()

    def _drop_connection(self) -> None:
        conn, self._conn = self._conn, None
        self._held = False
        self._session_pid = None
        if conn is not None and not conn.is_closed():
            conn.terminate()

    async def _try_lock(self, conn: LockConnection) -> bool:
        result = await asyncio.wait_for(conn.fetchval(_TRY_LOCK_SQL, *self._key), _QUERY_TIMEOUT_S)
        if not isinstance(result, bool):
            raise TypeError(f"pg_try_advisory_lock повернув {result!r}")
        return result

    async def _holders(self, conn: LockConnection) -> tuple[LockHolder, ...]:
        rows = await asyncio.wait_for(conn.fetch(_HOLDERS_SQL, *self._key), _QUERY_TIMEOUT_S)
        return tuple(_holder(row) for row in rows)

    async def _take_over(self, conn: LockConnection, holders: tuple[LockHolder, ...], confirm: TakeoverConfirm) -> None:
        described = [h.as_log() for h in holders]
        if not await confirm(holders):
            self._log.warning("instance_lock_takeover_refused", holders=described)
            raise TakeoverRefused("not_confirmed")
        # Людина могла думати хвилини: завершувати можна лише тих власників, яких вона бачила.
        current = await self._holders(conn)
        if not current:
            self._log.info("instance_lock_holder_gone", holders=described)
            return
        if {_identity(h) for h in current} != {_identity(h) for h in holders}:
            self._log.warning(
                "instance_lock_holder_changed", confirmed=described, current=[h.as_log() for h in current]
            )
            raise TakeoverRefused("holder_changed")
        request = {"lock": list(self._key), "client_os_pid": os.getpid(), "holders": described}
        try:
            await asyncio.wait_for(
                conn.execute(
                    _AUDIT_SQL,
                    _TAKEOVER_ACTOR,
                    AUDIT_ACTION_FORCE_TAKEOVER,
                    _TAKEOVER_TARGET,
                    _TAKEOVER_REASON,
                    json.dumps(request, ensure_ascii=False),
                    clock.now_ms(),
                ),
                _QUERY_TIMEOUT_S,
            )
        except _DB_ERRORS as exc:
            self._log.error("instance_lock_takeover_audit_failed", error_type=type(exc).__name__, error=str(exc))
            raise TakeoverRefused("audit_failed") from exc
        self._log.warning("instance_lock_force_takeover", holders=described)
        for holder in holders:
            # Завершується лише сесія, що досі тримає lock і стартувала тоді ж (pid міг бути перевикористаний).
            terminated = await asyncio.wait_for(
                conn.fetchval(
                    _TERMINATE_SQL, *self._key, holder.session_pid, holder.backend_start_ms, TERMINATE_WAIT_MS
                ),
                TERMINATE_WAIT_MS / 1000 + _QUERY_TIMEOUT_S,
            )
            self._log.warning(
                "instance_lock_holder_terminated",
                session_pid=holder.session_pid,
                terminated=terminated is True,
                still_holder=terminated is not None,
            )

    def _mark_lost(self, reason: str) -> None:
        """Lock більше не вважається нашим: нові входи заборонені. З'єднання закриває лише `release()`."""
        self._log.critical("instance_lock_lost", reason=reason, session_pid=self._session_pid)
        self._lost_reason = reason
        self._held = False

    def _lose(self, reason: str) -> NoReturn:
        self._mark_lost(reason)
        raise InstanceLockLost(reason)


def _identity(holder: LockHolder) -> tuple[int, int | None]:
    return holder.session_pid, holder.backend_start_ms


async def run_exclusive(
    lock: InstanceLock,
    body: Callable[[], Coroutine[Any, Any, None]],
    *,
    on_stuck: Callable[[str], object],
    heartbeat_interval_ms: int = HEARTBEAT_INTERVAL_MS,
    heartbeat_timeout_ms: int = HEARTBEAT_TIMEOUT_MS,
    stop_timeout_ms: int = BODY_STOP_TIMEOUT_MS,
) -> None:
    """Виконати `body`, поки lock наш, під watchdog-ом (`lock.watch()`). Lock звільняє викликач (`release()`).

    - `body` завершився → watchdog зупиняється; помилка `body` (зокрема `ExceptionGroup`) повертається як є;
    - lock втрачено → `body` скасовується і дочікується, лише потім `InstanceLockLost` (з'єднання lock ще відкрите,
      тож lock, якщо він на сервері ще наш, не дістанеться іншому процесу, поки робота не зупинилась);
    - зовнішнє скасування (сигнал завершення) → те саме, потім `CancelledError`.

    Контракт `body` (T0.7, етап 5): скасування обробляється не довше `stop_timeout_ms` — без необмежених
    `asyncio.shield` і без ковтання `CancelledError`; `body` володіє всіма своїми задачами (`TaskGroup`), а відірвані
    обробники викликають `lock.ensure_held()` перед дією, що збільшує експозицію. Не зупинився вчасно →
    `on_stuck("lock_lost" | "shutdown")`: у процесі — аварійне завершення (`os._exit`), бо працювати без lock не можна.
    """
    lock.ensure_held()
    work = asyncio.create_task(body(), name="instance-lock-body")
    watchdog = asyncio.create_task(
        lock.watch(interval_ms=heartbeat_interval_ms, timeout_ms=heartbeat_timeout_ms),
        name="instance-lock-watchdog",
    )
    try:
        await asyncio.wait((work, watchdog), return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        # Зовнішнє скасування (сигнал): lock ще наш — зупинити роботу, потім забрати підсумок watchdog-а.
        watchdog.cancel()
        await _stop_body(lock, work, stop_timeout_ms, on_stuck, "shutdown")
        lost = await _watchdog_outcome(lock, watchdog)
        if lost is not None:
            raise lost from None
        raise
    if watchdog.done():
        # Втрата lock (або збій самого watchdog-а): позначити втрату, зупинити роботу, лише потім віддати виняток.
        # Сигнал під час зупинки її не скорочує (`_stop_body`), а втрата lock важливіша за сигнал.
        lost = await _watchdog_outcome(lock, watchdog)
        await _stop_body(lock, work, stop_timeout_ms, on_stuck, "lock_lost")
        raise lost if lost is not None else InstanceLockLost("watchdog_stopped")
    watchdog.cancel()
    await _watchdog_outcome(lock, watchdog)
    work.result()


async def _watchdog_outcome(lock: InstanceLock, watchdog: asyncio.Task[Any]) -> InstanceLockLost | None:
    """Дочекатися watchdog-а (скасованого чи завершеного) і повернути втрату lock, якщо вона була.

    Збій самого watchdog-а (виняток не `InstanceLockLost`) теж означає, що за lock ніхто не стежить: lock позначається
    втраченим (`watchdog_failed:<тип>`), щоб `held` / `ensure_held()` одразу заборонили нові входи.
    """
    await asyncio.wait((watchdog,))
    if watchdog.cancelled():
        return None
    error = watchdog.exception()
    if error is None or isinstance(error, InstanceLockLost):
        return error
    reason = f"watchdog_failed:{type(error).__name__}"
    lock._log.error("instance_lock_watchdog_failed", error_type=type(error).__name__, exc_info=error)
    lock._mark_lost(reason)
    lost = InstanceLockLost(reason)
    lost.__cause__ = error
    return lost


async def _stop_body(
    lock: InstanceLock, work: asyncio.Task[None], stop_timeout_ms: int, on_stuck: Callable[[str], object], reason: str
) -> None:
    """Скасувати роботу й чекати її до `stop_timeout_ms`; повторне зовнішнє скасування дедлайн не скорочує."""
    if not work.done():
        work.cancel()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + stop_timeout_ms / 1000
        while not work.done() and (remaining := deadline - loop.time()) > 0:
            try:
                await asyncio.wait((work,), timeout=remaining)
            except asyncio.CancelledError:
                # Сигнал посеред зупинки: lock не можна віддати, поки робота жива, — чекаємо далі до дедлайну.
                lock._log.warning("instance_lock_cancel_during_stop", reason=reason)
        if not work.done():
            lock._log.critical("instance_lock_body_stuck", reason=reason, stop_timeout_ms=stop_timeout_ms)
            on_stuck(reason)
            return
    if not work.cancelled() and (error := work.exception()) is not None:
        # Помилка роботи під час зупинки не повинна загубитися за InstanceLockLost / CancelledError.
        lock._log.error("instance_lock_body_failed", reason=reason, error_type=type(error).__name__, exc_info=error)
