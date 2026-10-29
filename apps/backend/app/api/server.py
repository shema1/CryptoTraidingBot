"""Запуск локального API в торговому процесі (T0.7): uvicorn як робота під single-instance lock (T0.6).

`serve_api()` — `body` для `run_exclusive` (`app/__main__.py`) і виконує його контракт:
- сокет — лише `API_BIND_HOST` = 127.0.0.1 (бінд робимо самі: помилка → `ApiBindError`, а не `sys.exit` uvicorn);
- сигнали SIGTERM/SIGINT обробляє `_run` у `app/__main__.py`: `capture_signals()` uvicorn вимкнено
  (він ставить `signal.signal` поверх обробників циклу подій і після зупинки повторно піднімає сигнал);
- `log_config=None`: логери uvicorn поширюють записи до root-хендлерів із маскуванням (`core/logging.py`);
  access-лог вимкнено (`api_request` на рівні DEBUG пише `LocalOriginGuard`);
- скасування (сигнал або втрата lock) → `should_exit` → graceful shutdown (`GRACEFUL_SHUTDOWN_S`), потім примусово;
  разом із закриттям пулу БД — у межах `BODY_STOP_TIMEOUT_MS` (5 с). `body` не завершується раніше за сервер і задачі
  запитів (`_stop`); якщо вони не зупинились — `run_exclusive` аварійно завершує процес.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import socket
from collections.abc import Iterator
from typing import Final

import uvicorn
from fastapi import FastAPI
from sqlalchemy import text

from app.api.app import create_app as create_app
from app.api.audit import AuditWriter
from app.api.context import ApiContext, LockState
from app.api.idempotency import DbIdempotencyStore
from app.core.config import API_BIND_HOST, Settings
from app.core.db import create_engine, session_factory
from app.core.logging import get_logger

_log = get_logger("app.api")

# Скільки uvicorn чекає завершення поточних запитів після `should_exit`, перш ніж скасувати їх.
GRACEFUL_SHUTDOWN_S: Final = 2
# Скільки чекати зупинки сервера після `should_exit` (graceful + такт циклу uvicorn 0,1 с + запас).
SERVER_STOP_TIMEOUT_MS: Final = 3000
# Скільки чекати на задачу сервера після примусового скасування.
SERVER_FORCE_STOP_MS: Final = 500
# Закриття пулу з'єднань БД при зупинці.
DISPOSE_TIMEOUT_MS: Final = 1000
LISTEN_BACKLOG: Final = 128
# Пул БД API: таймаути, щоб запит не висів, коли БД недоступна (asyncpg за замовчуванням чекає 60 с).
DB_CONNECT_TIMEOUT_S: Final = 5
DB_COMMAND_TIMEOUT_S: Final = 10
DB_POOL_TIMEOUT_S: Final = 5


class ApiBindError(Exception):
    """Не вдалося зайняти `API_BIND_HOST:API_PORT` (порт зайнятий, немає прав) — процес не стартує."""

    def __init__(self, port: int, reason: str) -> None:
        super().__init__(f"не вдалося зайняти {API_BIND_HOST}:{port}: {reason}")
        self.port = port
        self.reason = reason


class ApiStoppedUnexpectedly(Exception):
    """Сервер API зупинився без сигналу — процес завершується з помилкою (служба перезапустить, ТЗ §12)."""


def bind_api_socket(port: int) -> socket.socket:
    """Слухаючий TCP-сокет лише на `API_BIND_HOST` (IPv4 loopback). `port = 0` — ефемерний (тести)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Швидкий рестарт після TIME_WAIT; другий слухач на тому самому порту все одно не займе його.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((API_BIND_HOST, port))
        sock.listen(LISTEN_BACKLOG)
        sock.setblocking(False)
    except OSError as exc:
        sock.close()
        reason = errno.errorcode.get(exc.errno or 0, type(exc).__name__)
        raise ApiBindError(port, reason) from exc
    return sock


class _Server(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        # Сигнали обробляє `app/__main__.py` (`loop.add_signal_handler` → скасування роботи під lock).
        yield


def uvicorn_config(app: FastAPI, settings: Settings) -> uvicorn.Config:
    return uvicorn.Config(
        app,
        host=API_BIND_HOST,
        port=settings.api_port,
        log_config=None,
        access_log=False,
        proxy_headers=False,
        server_header=False,
        lifespan="off",
        ws="none",
        interface="asgi3",
        timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_S,
    )


async def serve_api(settings: Settings, lock: LockState, *, sock: socket.socket | None = None) -> None:
    """Обслуговувати API до скасування. `sock` — уже зайнятий сокет (тести); інакше — `API_PORT`."""
    try:
        if settings.api_token is None:
            raise ValueError("API_TOKEN не задано: локальний API без токена не запускається")
        url = settings.database_url()
        listener = sock if sock is not None else bind_api_socket(settings.api_port)
    except BaseException:
        if sock is not None:
            sock.close()
        raise
    try:
        engine = create_engine(
            url,
            pool_timeout=DB_POOL_TIMEOUT_S,
            connect_args={"timeout": DB_CONNECT_TIMEOUT_S, "command_timeout": DB_COMMAND_TIMEOUT_S},
        )
    except BaseException:
        listener.close()
        raise
    try:

        async def db_ping() -> None:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

        ctx = ApiContext(
            settings=settings,
            lock=lock,
            db_ping=db_ping,
            idempotency=DbIdempotencyStore(session_factory(engine)),
            audit=AuditWriter.from_settings(settings),
        )
        server = _Server(uvicorn_config(create_app(ctx), settings))
        _log.info(
            "api_starting",
            host=API_BIND_HOST,
            port=listener.getsockname()[1],
            allowed_origins=list(settings.api_allowed_origins),
            instance_id=ctx.instance_id,
        )
        await _run_server(server, listener, lock)
    finally:
        listener.close()
        try:
            async with asyncio.timeout(DISPOSE_TIMEOUT_MS / 1000):
                await engine.dispose()
        except Exception as exc:  # noqa: BLE001 — зупинка не повинна зависнути чи впасти через закриття пулу
            _log.warning("api_engine_dispose_failed", error_type=type(exc).__name__)


async def _run_server(server: uvicorn.Server, listener: socket.socket, lock: LockState) -> None:
    serving = asyncio.create_task(server.serve(sockets=[listener]), name="api-server")
    try:
        # `asyncio.wait` не скасовує `serving`, коли скасовують нас: зупинка — керована, нижче.
        await asyncio.wait((serving,))
    except asyncio.CancelledError:
        await _stop(server, serving, lock)
        raise
    error = serving.exception()
    if error is not None:
        raise error
    raise ApiStoppedUnexpectedly("сервер API зупинився без сигналу")


async def _stop(server: uvicorn.Server, serving: asyncio.Task[None], lock: LockState) -> None:
    """Керована зупинка: `body` не повертається, поки живі сервер **і** задачі запитів.

    uvicorn після `timeout_graceful_shutdown` лише скасовує задачі запитів і не чекає їх; обробник, що дописує щось у
    `except CancelledError`, пережив би `serve_api` і працював би вже після звільнення lock (контракт `run_exclusive`,
    T0.6). Тому після сервера дочікуємось і `server_state.tasks`. Не встигли за бюджет → CRITICAL і очікування без
    дедлайну: дедлайн тримає `run_exclusive` (`BODY_STOP_TIMEOUT_MS` → `on_stuck` → аварійний вихід процесу).
    Повторне скасування дедлайни не скорочує (як `_stop_body` у T0.6).
    """
    server.should_exit = True
    if not lock.held:
        # Lock втрачено: graceful-очікування дало б обробникам ще `GRACEFUL_SHUTDOWN_S` працювати без lock. Скасовуємо
        # їх самі одразу, не покладаючись на uvicorn (з Python 3.12 його `shutdown()` чекає закриття з'єднань).
        _log.warning("api_stop_without_lock")
        server.force_exit = True
        for task in server.server_state.tasks:
            task.cancel()
    if not await _wait_done({serving}, SERVER_STOP_TIMEOUT_MS):
        _log.error("api_stop_timeout", timeout_ms=SERVER_STOP_TIMEOUT_MS)
        server.force_exit = True
        serving.cancel()
    requests = set(server.server_state.tasks)
    for task in requests:
        # Уже скасовані uvicorn-ом не скасовуємо вдруге: це перервало б їхнє прибирання (напр. запис у БД).
        if task.cancelling() == 0:
            task.cancel()
    pending = {serving, *requests}
    if not await _wait_done(pending, SERVER_FORCE_STOP_MS):
        stuck = [t for t in pending if not t.done()]
        _log.critical(
            "api_server_stuck", timeout_ms=SERVER_FORCE_STOP_MS, server_alive=not serving.done(), requests=len(stuck)
        )
        await _wait_done(pending, None)
    if not serving.cancelled() and (error := serving.exception()) is not None:
        _log.error("api_server_failed_on_stop", error_type=type(error).__name__, exc_info=error)
    _log.info("api_stopped")


async def _wait_done(tasks: set[asyncio.Task[None]], timeout_ms: int | None) -> bool:
    """Чекати завершення всіх `tasks` до `timeout_ms` (`None` — без дедлайну); скасування нас — не скорочує."""
    loop = asyncio.get_running_loop()
    deadline = None if timeout_ms is None else loop.time() + timeout_ms / 1000
    while not all(t.done() for t in tasks):
        remaining = None if deadline is None else deadline - loop.time()
        if remaining is not None and remaining <= 0:
            break
        try:
            await asyncio.wait(tasks, timeout=remaining)
        except asyncio.CancelledError:
            _log.warning("api_cancel_during_stop")
    return all(t.done() for t in tasks)
