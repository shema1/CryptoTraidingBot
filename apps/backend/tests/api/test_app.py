"""Каркас застосунку (T0.7): OpenAPI (токен, `Idempotency-Key`), формат помилок, залежність `require_instance_lock`."""

from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.api.app import create_app
from app.api.context import require_instance_lock
from app.api.errors import ErrorCode
from tests.api.fakes import AUTH, BASE_URL, FakeLockState, make_app, routes_of

KEY = {"Idempotency-Key": "key-0000000000000001"}


@pytest.fixture
def schema(api: FastAPI) -> dict[str, Any]:
    return api.openapi()


def test_openapi_declares_bearer_auth_globally(schema: dict[str, Any]) -> None:
    assert schema["components"]["securitySchemes"]["bearerAuth"] == {"type": "http", "scheme": "bearer"}
    assert schema["security"] == [{"bearerAuth": []}]


def test_openapi_mutations_require_idempotency_key(schema: dict[str, Any]) -> None:
    mutating = {"post", "put", "patch", "delete"}
    seen = 0
    for path, operations in schema["paths"].items():
        for method, operation in operations.items():
            headers = [p for p in operation.get("parameters", []) if p["in"] == "header"]
            names = [p["name"] for p in headers]
            if method in mutating:
                seen += 1
                assert names.count("Idempotency-Key") == 1, (path, method)
                param = next(p for p in headers if p["name"] == "Idempotency-Key")
                assert param["required"] is True
                assert param["schema"]["minLength"] == 16
                assert param["schema"]["maxLength"] == 128
            else:
                assert "Idempotency-Key" not in names, (path, method)
    assert seen >= 5  # тестові маршрути POST/DELETE


def test_openapi_documents_health_and_error_schema(schema: dict[str, Any]) -> None:
    health = schema["paths"]["/system/health"]["get"]
    assert set(health["responses"]) >= {"200", "401", "403", "503"}
    assert "ErrorResponse" in schema["components"]["schemas"]
    assert "HealthResponse" in schema["components"]["schemas"]


def test_openapi_can_be_built_offline_without_routes_of_tests() -> None:
    # T6.4: типи для UI генеруються з create_app(...).openapi() без запуску сервера.
    app = make_app()
    schema = app.openapi()
    assert schema["openapi"].startswith("3.")
    assert "/system/health" in schema["paths"]


def test_error_codes_are_unique_upper_snake() -> None:
    values = [code.value for code in ErrorCode]
    assert len(values) == len(set(values))
    assert all(v.isupper() and " " not in v for v in values)


def test_unhandled_exception_is_generic_500_without_details(api: FastAPI, client: TestClient) -> None:
    response = client.post("/test/fail", headers={**AUTH, **KEY})
    assert response.status_code == 500
    assert response.json() == {"error": {"code": "INTERNAL_ERROR", "message": "Внутрішня помилка сервера"}}
    assert "handler failed" not in response.text
    assert routes_of(api).calls["fail"] == 1


def test_method_not_allowed_has_error_code(client: TestClient) -> None:
    response = client.put("/system/health", headers={**AUTH, **KEY})
    assert response.status_code == 405
    assert response.json()["error"]["code"] == "METHOD_NOT_ALLOWED"


def _app_with_exposure_endpoint(lock: FakeLockState) -> FastAPI:
    app = make_app(lock=lock)

    @app.post("/test/enter", dependencies=[Depends(require_instance_lock)])
    async def enter() -> dict[str, bool]:
        routes_of(app).calls["enter"] += 1
        return {"ok": True}

    return app


def test_exposure_endpoint_runs_while_lock_is_held() -> None:
    app = _app_with_exposure_endpoint(FakeLockState(held=True))
    with TestClient(app, base_url=BASE_URL) as client:
        assert client.post("/test/enter", headers={**AUTH, **KEY}).status_code == 200
    assert routes_of(app).calls["enter"] == 1


def test_exposure_endpoint_refused_after_lock_lost() -> None:
    app = _app_with_exposure_endpoint(FakeLockState(held=False))
    with TestClient(app, base_url=BASE_URL) as client:
        response = client.post("/test/enter", headers={**AUTH, **KEY})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "INSTANCE_LOCK_LOST"
    assert routes_of(app).calls["enter"] == 0


def test_create_app_requires_api_token() -> None:
    from app.api.audit import AuditWriter
    from app.api.context import ApiContext
    from tests.api.fakes import FakeDbPing, MemoryIdempotencyStore, make_settings

    settings = make_settings(api_token=None)
    ctx = ApiContext(
        settings=settings,
        lock=FakeLockState(),
        db_ping=FakeDbPing(),
        idempotency=MemoryIdempotencyStore(),
        audit=AuditWriter.from_settings(settings),
    )
    with pytest.raises(ValueError, match="API_TOKEN"):
        create_app(ctx)


def test_unhandled_500_is_readable_by_ui_and_not_cached(client: TestClient) -> None:
    # Рев'ю T0.7: 500 від зовнішнього ServerErrorMiddleware не мав CORS — UI бачив «мережеву помилку».
    from tests.api.fakes import UI_ORIGIN

    response = client.post("/test/fail", headers={**AUTH, **KEY, "Origin": UI_ORIGIN})
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "INTERNAL_ERROR"
    assert response.headers["access-control-allow-origin"] == UI_ORIGIN
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"


def test_unhandled_exception_is_logged_once(client: TestClient) -> None:
    from structlog.testing import capture_logs

    with capture_logs() as logs:
        client.post("/test/fail", headers={**AUTH, **KEY})
    failures = [e for e in logs if e["event"] == "api_unhandled_exception"]
    assert len(failures) == 1
    assert failures[0]["error_type"] == "RuntimeError"


@pytest.mark.asyncio
async def test_catcher_after_response_started_does_not_send_second_start() -> None:
    from structlog.testing import capture_logs

    from app.api.errors import CatchUnhandled

    async def broken(scope: Any, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        raise RuntimeError("after start")

    sent: list[MutableMapping[str, Any]] = []

    async def send(message: MutableMapping[str, Any]) -> None:
        sent.append(message)

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    with capture_logs() as logs:
        await CatchUnhandled(broken)({"type": "http", "method": "GET", "path": "/x"}, receive, send)
    assert [m["type"] for m in sent] == ["http.response.start"]
    (event,) = [e for e in logs if e["event"] == "api_unhandled_exception"]
    assert event["response_started"] is True
