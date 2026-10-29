"""Фейки й тестові маршрути для unit-тестів API (T0.7). Продакшн-коду тут немає."""

from __future__ import annotations

import asyncio
import secrets
from collections import Counter
from dataclasses import dataclass, field, replace
from typing import Any

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse
from pydantic import SecretStr

from app.api.app import create_app
from app.api.audit import AuditWriter
from app.api.context import ApiContext
from app.api.idempotency import IdempotencyRecord, IdempotencyStatus, IdempotencyStore, StoredResponse
from app.core.config import Settings
from app.core.instance_lock import InstanceLockLost

# Токен генерується на кожен запуск тестів: у коді немає жодного «секрету»-літерала.
API_TOKEN = secrets.token_urlsafe(32)
AUTH = {"Authorization": f"Bearer {API_TOKEN}"}
UI_ORIGIN = "http://127.0.0.1:5173"
API_PORT = 8000
BASE_URL = f"http://127.0.0.1:{API_PORT}"


def make_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "api_token": SecretStr(API_TOKEN),
        "api_port": API_PORT,
        "api_allowed_origins": (UI_ORIGIN,),
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


@dataclass
class FakeLockState:
    held: bool = True

    def ensure_held(self) -> None:
        if not self.held:
            raise InstanceLockLost("test_lost")


@dataclass
class FakeDbPing:
    error: BaseException | None = None
    hang: bool = False
    calls: int = 0

    async def __call__(self) -> None:
        self.calls += 1
        if self.hang:
            await asyncio.Event().wait()
        if self.error is not None:
            raise self.error


@dataclass
class MemoryIdempotencyStore:
    """Сховище в пам'яті з тією самою семантикою, що й `DbIdempotencyStore`, і з ін'єкцією збоїв."""

    records: dict[str, IdempotencyRecord] = field(default_factory=dict)
    begin_error: BaseException | None = None
    complete_error: BaseException | None = None
    fail_error: BaseException | None = None
    calls: Counter[str] = field(default_factory=Counter)

    async def begin(self, record: IdempotencyRecord) -> IdempotencyRecord | None:
        self.calls["begin"] += 1
        if self.begin_error is not None:
            raise self.begin_error
        existing = self.records.get(record.key)
        if existing is not None:
            return existing
        self.records[record.key] = record
        return None

    async def complete(self, key: str, owner: str, response: StoredResponse, *, now_ms: int) -> bool:
        self.calls["complete"] += 1
        if self.complete_error is not None:
            raise self.complete_error
        return self._finish(key, owner, IdempotencyStatus.COMPLETED, response)

    async def fail(self, key: str, owner: str, *, now_ms: int) -> bool:
        self.calls["fail"] += 1
        if self.fail_error is not None:
            raise self.fail_error
        return self._finish(key, owner, IdempotencyStatus.FAILED, None)

    def _finish(self, key: str, owner: str, status: IdempotencyStatus, response: StoredResponse | None) -> bool:
        current = self.records.get(key)
        if current is None or current.owner != owner or current.status is not IdempotencyStatus.IN_PROGRESS:
            return False
        self.records[key] = replace(current, status=status, response=response)
        return True


@dataclass
class Routes:
    """Лічильники викликів тестових обробників і «ворота» для конкурентних тестів."""

    calls: Counter[str] = field(default_factory=Counter)
    keys: list[str | None] = field(default_factory=list)
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)


def make_app(
    *,
    store: IdempotencyStore | None = None,
    lock: FakeLockState | None = None,
    db_ping: FakeDbPing | None = None,
    settings: Settings | None = None,
    instance_id: str = "test-instance",
) -> FastAPI:
    settings = settings or make_settings()
    ctx = ApiContext(
        settings=settings,
        lock=lock or FakeLockState(),
        db_ping=db_ping or FakeDbPing(),
        idempotency=store or MemoryIdempotencyStore(),
        audit=AuditWriter.from_settings(settings),
        instance_id=instance_id,
    )
    app = create_app(ctx)
    routes = Routes()
    app.state.routes = routes

    @app.post("/test/actions", status_code=201)
    async def create_action(request: Request) -> dict[str, Any]:
        routes.calls["actions"] += 1
        routes.keys.append(getattr(request.state, "idempotency_key", None))
        return {"n": routes.calls["actions"], "echo": await request.json()}

    @app.delete("/test/actions/{action_id}")
    async def delete_action(action_id: int) -> dict[str, int]:
        routes.calls["delete"] += 1
        return {"deleted": action_id}

    @app.post("/test/fail")
    async def fail() -> None:
        routes.calls["fail"] += 1
        raise RuntimeError("handler failed")

    @app.post("/test/conflict")
    async def conflict() -> JSONResponse:
        routes.calls["conflict"] += 1
        return JSONResponse({"error": {"code": "TEST_CONFLICT", "message": "конфлікт"}}, status_code=409)

    @app.post("/test/slow")
    async def slow() -> dict[str, bool]:
        routes.calls["slow"] += 1
        routes.entered.set()
        await routes.release.wait()
        return {"done": True}

    @app.post("/test/big")
    async def big() -> JSONResponse:
        routes.calls["big"] += 1
        return JSONResponse({"blob": "x" * (2 * 1024 * 1024)})

    @app.get("/test/read")
    async def read() -> dict[str, int]:
        routes.calls["read"] += 1
        return {"n": routes.calls["read"]}

    @app.websocket("/test/ws")
    async def ws(websocket: WebSocket) -> None:
        routes.calls["ws"] += 1
        await websocket.accept()
        await websocket.send_json({"hello": True})
        await websocket.close()

    return app


def routes_of(app: FastAPI) -> Routes:
    routes = app.state.routes
    assert isinstance(routes, Routes)
    return routes


@dataclass
class RawResponse:
    status: int
    headers: list[tuple[bytes, bytes]]
    body: bytes

    def header(self, name: str) -> str | None:
        values = [v.decode("latin-1") for k, v in self.headers if k.decode("latin-1").lower() == name.lower()]
        return values[0] if values else None


async def asgi_call(
    app: Any,
    *,
    method: str = "GET",
    path: str = "/system/health",
    headers: list[tuple[bytes, bytes]] | None = None,
    body: bytes = b"",
    client: tuple[str, int] | None = ("127.0.0.1", 50000),
) -> RawResponse:
    """Сирий ASGI-запит: точний контроль над заголовками (відсутній чи дубльований Host, два Authorization)."""
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": headers or [],
        "client": client,
        "server": ("127.0.0.1", API_PORT),
    }
    sent = False

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    status = 0
    response_headers: list[tuple[bytes, bytes]] = []
    chunks: list[bytes] = []

    async def send(message: dict[str, Any]) -> None:
        nonlocal status, response_headers
        if message["type"] == "http.response.start":
            status = message["status"]
            response_headers = list(message.get("headers", []))
        elif message["type"] == "http.response.body":
            chunks.append(message.get("body", b""))

    await app(scope, receive, send)
    return RawResponse(status=status, headers=response_headers, body=b"".join(chunks))
