"""Сервер API в торговому процесі (T0.7): реальний uvicorn на loopback-сокеті з ефемерним портом.

Перевіряється інтеграція з `run_exclusive` (T0.6): бінд лише 127.0.0.1, зайнятий порт → `ApiBindError`, скасування
зупиняє сервер у межах `BODY_STOP_TIMEOUT_MS`, сигнали лишаються за `app/__main__.py`, логи uvicorn ідуть через
маскування. БД не потрібна: `POSTGRES_PORT` вказує на закритий порт, тож health — 503 з `database: false`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import socket
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from pydantic import SecretStr
from structlog.testing import capture_logs

from app.api import server as api_server
from app.api.server import ApiBindError, bind_api_socket, serve_api
from app.core.config import API_BIND_HOST, Settings
from app.core.instance_lock import BODY_STOP_TIMEOUT_MS
from tests.api.fakes import AUTH, FakeLockState, make_settings

pytestmark = pytest.mark.asyncio


def _closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
    return port


def _settings_for(sock: socket.socket) -> Settings:
    return make_settings(
        api_port=sock.getsockname()[1],
        postgres_password=SecretStr("unit-test-only"),
        postgres_port=_closed_port(),
    )


@contextlib.asynccontextmanager
async def _running(lock: FakeLockState | None = None) -> AsyncIterator[tuple[asyncio.Task[None], Settings]]:
    sock = bind_api_socket(0)
    settings = _settings_for(sock)
    task = asyncio.create_task(serve_api(settings, lock or FakeLockState(), sock=sock))
    try:
        yield task, settings
    finally:
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _get(settings: Settings, path: str = "/system/health", **headers: str) -> httpx.Response:
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{settings.api_port}", timeout=10) as client:
        return await client.get(path, headers={**AUTH, **headers})


async def test_socket_is_bound_to_loopback_only() -> None:
    sock = bind_api_socket(0)
    try:
        host, port = sock.getsockname()
        assert host == API_BIND_HOST == "127.0.0.1"
        assert port > 0
        assert sock.family == socket.AF_INET
    finally:
        sock.close()


async def test_busy_port_is_bind_error() -> None:
    first = bind_api_socket(0)
    try:
        with pytest.raises(ApiBindError) as exc:
            bind_api_socket(first.getsockname()[1])
        assert exc.value.port == first.getsockname()[1]
    finally:
        first.close()


async def test_serves_health_over_tcp_with_security() -> None:
    async with _running() as (_, settings):
        ok = await _get(settings)
        assert ok.status_code == 503  # БД недоступна в цьому тесті
        assert ok.json()["checks"] == {"instance_lock": True, "database": False}
        assert ok.headers["cache-control"] == "no-store"
        assert "server" not in ok.headers  # без server_header

        no_token = await _get(settings, Authorization="")
        assert no_token.status_code == 401

        rebinding = await _get(settings, Host=f"evil.com:{settings.api_port}")
        assert rebinding.status_code == 403


async def test_cancellation_stops_server_within_deadline() -> None:
    async with _running() as (task, settings):
        assert (await _get(settings)).status_code == 503
        loop = asyncio.get_running_loop()
        started = loop.time()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (loop.time() - started) * 1000 < BODY_STOP_TIMEOUT_MS
        with pytest.raises(httpx.ConnectError):
            await _get(settings)


async def test_cancellation_with_open_keepalive_connection_is_bounded() -> None:
    async with (
        _running() as (task, settings),
        httpx.AsyncClient(base_url=f"http://127.0.0.1:{settings.api_port}", timeout=10) as client,
    ):
        assert (await client.get("/system/health", headers=AUTH)).status_code == 503
        loop = asyncio.get_running_loop()
        started = loop.time()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (loop.time() - started) * 1000 < BODY_STOP_TIMEOUT_MS


async def test_signal_handlers_are_not_taken_over() -> None:
    before = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    async with _running() as (_, settings):
        await _get(settings)
        during = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    assert during == before


async def test_uvicorn_logs_go_through_root_handlers() -> None:
    async with _running() as (_, settings):
        await _get(settings)
        for name in ("uvicorn", "uvicorn.error"):
            logger = logging.getLogger(name)
            assert logger.handlers == [], name
            assert logger.propagate is True, name
        # Access-лог вимкнено: без хендлерів у ланцюжку uvicorn його не пише взагалі (`hasHandlers()`).
        access = logging.getLogger("uvicorn.access")
        assert access.handlers == []
        assert access.hasHandlers() is False


async def test_uvicorn_config_is_hardened(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    original = api_server.uvicorn_config

    def spy(app: Any, settings: Settings) -> Any:
        config = original(app, settings)
        captured["config"] = config
        return config

    monkeypatch.setattr(api_server, "uvicorn_config", spy)
    async with _running() as (_, settings):
        await _get(settings)
    config = captured["config"]
    assert config.log_config is None
    assert config.access_log is False
    assert config.proxy_headers is False
    assert config.server_header is False
    assert config.lifespan == "off"
    assert config.ws == "none"
    assert config.host == "127.0.0.1"
    assert config.timeout_graceful_shutdown == api_server.GRACEFUL_SHUTDOWN_S
    # Рев'ю T0.7: ліміт з'єднань дав би чужій сторінці відрізати UI (і Emergency Stop) — 503 до застосунку.
    assert config.limit_concurrency is None
    # Зупинка (graceful + примусова) і закриття пулу вкладаються в дедлайн роботи під lock (T0.6).
    assert api_server.GRACEFUL_SHUTDOWN_S * 1000 < api_server.SERVER_STOP_TIMEOUT_MS
    stop_budget_ms = api_server.SERVER_STOP_TIMEOUT_MS + api_server.SERVER_FORCE_STOP_MS + api_server.DISPOSE_TIMEOUT_MS
    assert stop_budget_ms < BODY_STOP_TIMEOUT_MS


async def test_server_without_token_does_not_start() -> None:
    sock = bind_api_socket(0)
    settings = _settings_for(sock).model_copy(update={"api_token": None})
    with pytest.raises(ValueError, match="API_TOKEN"):
        await serve_api(settings, FakeLockState(), sock=sock)
    assert sock.fileno() == -1  # сокет закрито


# ---------------------------------------------------------------- запити, що виконуються під час зупинки (рев'ю T0.7)


def _with_lingering_route(monkeypatch: pytest.MonkeyPatch, state: dict[str, Any]) -> None:
    """Маршрут, що після скасування ще працює (cleanup), доки тест не відпустить `release`."""
    original = api_server.create_app

    def create_app(ctx: Any) -> Any:
        app = original(ctx)

        @app.get("/test/linger")
        async def linger() -> None:
            state["entered"].set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                state["cleanup_at"] = asyncio.get_running_loop().time()
                state["cleanup"].set()
                await state["release"].wait()
                state["order"].append("handler_done")
                raise

        return app

    monkeypatch.setattr(api_server, "create_app", create_app)
    monkeypatch.setattr(api_server, "GRACEFUL_SHUTDOWN_S", 1)


def _state() -> dict[str, Any]:
    return {"entered": asyncio.Event(), "cleanup": asyncio.Event(), "release": asyncio.Event(), "order": []}


async def _start_linger(settings: Settings) -> asyncio.Task[Any]:
    async def call() -> None:
        with contextlib.suppress(httpx.HTTPError):
            await _get(settings, "/test/linger")

    return asyncio.create_task(call())


async def test_serve_api_waits_for_request_cleanup_before_returning(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _state()
    _with_lingering_route(monkeypatch, state)
    async with _running() as (task, settings):
        request = await _start_linger(settings)
        await state["entered"].wait()
        task.cancel()
        await state["cleanup"].wait()
        done, _ = await asyncio.wait({task}, timeout=0.2)
        assert done == set()  # обробник ще працює — lock не можна віддавати
        state["release"].set()
        with pytest.raises(asyncio.CancelledError):
            await task
        state["order"].append("serve_done")
        await request
    assert state["order"] == ["handler_done", "serve_done"]


async def test_stuck_request_keeps_body_alive_and_is_critical(monkeypatch: pytest.MonkeyPatch) -> None:
    # Дедлайн тримає run_exclusive (on_stuck → os._exit); serve_api сам не повертається раніше за обробник.
    state = _state()
    _with_lingering_route(monkeypatch, state)
    monkeypatch.setattr(api_server, "SERVER_FORCE_STOP_MS", 50)
    with capture_logs() as logs:
        async with _running() as (task, settings):
            request = await _start_linger(settings)
            await state["entered"].wait()
            task.cancel()
            await state["cleanup"].wait()
            done, _ = await asyncio.wait({task}, timeout=0.5)
            assert done == set()
            state["release"].set()
            with pytest.raises(asyncio.CancelledError):
                await task
            await request
    stuck = [e for e in logs if e["event"] == "api_server_stuck"]
    assert len(stuck) == 1
    assert stuck[0]["log_level"] == "critical"
    assert stuck[0]["requests"] == 1


async def test_server_stopping_without_signal_is_an_error() -> None:
    class Stops:
        server_state = type("S", (), {"tasks": set()})()

        async def serve(self, sockets: list[socket.socket]) -> None:
            return None

    sock = bind_api_socket(0)
    try:
        with pytest.raises(api_server.ApiStoppedUnexpectedly):
            await api_server._run_server(Stops(), sock, FakeLockState())  # type: ignore[arg-type]
    finally:
        sock.close()


async def test_lost_lock_stops_requests_without_graceful_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    # Рев'ю T0.7: після втрати lock обробник не повинен ще GRACEFUL_SHUTDOWN_S працювати без нього.
    state = _state()
    state["release"].set()  # прибирання — одразу
    _with_lingering_route(monkeypatch, state)
    monkeypatch.setattr(api_server, "GRACEFUL_SHUTDOWN_S", 30)
    lock = FakeLockState()
    async with _running(lock) as (task, settings):
        request = await _start_linger(settings)
        await state["entered"].wait()
        lock.held = False
        loop = asyncio.get_running_loop()
        started = loop.time()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert loop.time() - started < 2  # не 30 с graceful
        assert state["cleanup_at"] - started < 0.05  # скасовано одразу, без такту uvicorn (і на Python 3.12+)
        await request
    assert state["order"] == ["handler_done"]


async def test_rejected_requests_do_not_keep_connections_open() -> None:
    async with _running() as (_, settings):
        response = await _get(settings, Host=f"evil.com:{settings.api_port}")
        assert response.status_code == 403
        assert response.headers["connection"] == "close"
        unauthorized = await _get(settings, Authorization="")
        assert unauthorized.headers["connection"] == "close"
