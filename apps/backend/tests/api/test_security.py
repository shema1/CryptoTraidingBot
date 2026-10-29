"""Безпека локального API (ТЗ §14, T0.7 «Готово, коли»): `Host`/`Origin` → 403, без токена → 401, суворий CORS.

Браузер будь-якої сторінки може звертатися до 127.0.0.1, тож захист не покладається на «локальність»:
DNS rebinding відсікає перевірка `Host`, CSRF — bearer-токен і перевірка `Origin`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from structlog.testing import capture_logs

from app.core.clock import FakeClock
from app.core.logging import configure_logging, shutdown_logging
from tests.api.fakes import API_TOKEN, AUTH, UI_ORIGIN, asgi_call, make_settings, routes_of

IDEMPOTENCY = {"Idempotency-Key": "key-0000000000000001"}


def _code(response: Any) -> str:
    return str(response.json()["error"]["code"])


# ---------------------------------------------------------------- щасливий шлях


def test_ui_origin_with_token_is_allowed_with_strict_cors(client: TestClient) -> None:
    response = client.get("/system/health", headers={**AUTH, "Origin": UI_ORIGIN})
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == UI_ORIGIN
    assert "access-control-allow-credentials" not in response.headers
    assert "Origin" in response.headers["vary"]


def test_non_browser_client_without_origin_is_allowed(client: TestClient) -> None:
    response = client.get("/system/health", headers=AUTH)
    assert response.status_code == 200
    assert "access-control-allow-origin" not in response.headers


@pytest.mark.parametrize("host", ["127.0.0.1:8000", "localhost:8000", "LOCALHOST:8000"])
def test_loopback_host_with_api_port_is_allowed(client: TestClient, host: str) -> None:
    assert client.get("/system/health", headers={**AUTH, "Host": host}).status_code == 200


# ---------------------------------------------------------------- Host → 403 (DNS rebinding)


@pytest.mark.parametrize(
    "host",
    [
        "evil.com:8000",  # rebinding: evil.com → 127.0.0.1, браузер шле Host атакувальника
        "evil.com",
        "127.0.0.1.nip.io:8000",
        "127.0.0.1:9999",  # інший порт
        "localhost:8001",
        "127.0.0.1",  # без порту
        "0.0.0.0:8000",
        "[::1]:8000",  # API не слухає ::1
        "localhost.:8000",
        "",
    ],
)
def test_foreign_host_is_forbidden_even_with_token(client: TestClient, host: str) -> None:
    response = client.get("/system/health", headers={**AUTH, "Host": host})
    assert response.status_code == 403
    assert _code(response) == "HOST_NOT_ALLOWED"


@pytest.mark.asyncio
async def test_missing_host_is_forbidden(api: FastAPI) -> None:
    response = await asgi_call(api, headers=[(b"authorization", f"Bearer {API_TOKEN}".encode())])
    assert response.status == 403
    assert json.loads(response.body)["error"]["code"] == "HOST_NOT_ALLOWED"


@pytest.mark.asyncio
async def test_duplicate_host_is_forbidden(api: FastAPI) -> None:
    headers = [
        (b"host", b"127.0.0.1:8000"),
        (b"host", b"evil.com:8000"),
        (b"authorization", f"Bearer {API_TOKEN}".encode()),
    ]
    response = await asgi_call(api, headers=headers)
    assert response.status == 403
    assert json.loads(response.body)["error"]["code"] == "HOST_NOT_ALLOWED"


def test_foreign_host_is_checked_before_token(client: TestClient) -> None:
    response = client.get("/system/health", headers={"Host": "evil.com:8000"})
    assert response.status_code == 403
    assert _code(response) == "HOST_NOT_ALLOWED"


# ---------------------------------------------------------------- Origin → 403 (CSRF)


FOREIGN_ORIGINS = [
    "http://evil.com",
    "null",  # sandbox-iframe, data:, file:
    "http://127.0.0.1:5174",
    "http://localhost:5173",  # не в allowlist за замовчуванням
    "https://127.0.0.1:5173",
    "http://127.0.0.1:5173/",
    "chrome-extension://abcdefghijklmnop",
    "",
]


@pytest.mark.parametrize("origin", FOREIGN_ORIGINS)
def test_foreign_origin_is_forbidden_even_with_token(client: TestClient, origin: str) -> None:
    response = client.get("/system/health", headers={**AUTH, "Origin": origin})
    assert response.status_code == 403
    assert _code(response) == "ORIGIN_NOT_ALLOWED"
    assert "access-control-allow-origin" not in response.headers


@pytest.mark.parametrize("origin", FOREIGN_ORIGINS)
def test_foreign_origin_mutation_is_forbidden_and_not_executed(api: FastAPI, client: TestClient, origin: str) -> None:
    response = client.post("/test/actions", json={"a": 1}, headers={**AUTH, **IDEMPOTENCY, "Origin": origin})
    assert response.status_code == 403
    assert routes_of(api).calls["actions"] == 0


@pytest.mark.parametrize("origin", ["http://evil.com", "null"])
def test_foreign_origin_preflight_is_forbidden(client: TestClient, origin: str) -> None:
    response = client.options(
        "/test/actions",
        headers={"Origin": origin, "Access-Control-Request-Method": "POST"},
    )
    assert response.status_code == 403
    assert _code(response) == "ORIGIN_NOT_ALLOWED"
    assert "access-control-allow-origin" not in response.headers


@pytest.mark.asyncio
async def test_duplicate_origin_is_forbidden(api: FastAPI) -> None:
    headers = [
        (b"host", b"127.0.0.1:8000"),
        (b"origin", UI_ORIGIN.encode()),
        (b"origin", b"http://evil.com"),
        (b"authorization", f"Bearer {API_TOKEN}".encode()),
    ]
    response = await asgi_call(api, headers=headers)
    assert response.status == 403
    assert json.loads(response.body)["error"]["code"] == "ORIGIN_NOT_ALLOWED"


@pytest.mark.parametrize("site", ["cross-site", "same-site"])
def test_cross_site_browser_request_without_origin_is_forbidden(client: TestClient, site: str) -> None:
    # Напр. <img src="http://127.0.0.1:8000/..."> з іншого сайту: no-cors GET без Origin.
    response = client.get("/system/health", headers={**AUTH, "Sec-Fetch-Site": site})
    assert response.status_code == 403
    assert _code(response) == "ORIGIN_NOT_ALLOWED"


@pytest.mark.parametrize("site", ["same-origin", "none"])
def test_same_origin_or_typed_url_without_origin_is_allowed(client: TestClient, site: str) -> None:
    assert client.get("/system/health", headers={**AUTH, "Sec-Fetch-Site": site}).status_code == 200


def test_configured_extra_origin_is_allowed() -> None:
    from tests.api.fakes import make_app

    app = make_app(settings=make_settings(api_allowed_origins=(UI_ORIGIN, "http://localhost:4173")))
    with TestClient(app, base_url="http://127.0.0.1:8000") as client:
        response = client.get("/system/health", headers={**AUTH, "Origin": "http://localhost:4173"})
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:4173"


# ---------------------------------------------------------------- токен → 401


def test_request_without_token_is_unauthorized(client: TestClient) -> None:
    response = client.get("/system/health")
    assert response.status_code == 401
    assert _code(response) == "AUTH_REQUIRED"
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.parametrize(
    "authorization",
    [
        "Bearer wrong-token-0000000000000000000000000000",
        f"Bearer {API_TOKEN}x",
        f"Bearer {API_TOKEN[:-1]}",
        f"Basic {API_TOKEN}",
        API_TOKEN,
        "Bearer",
        "Bearer ",
        f"Bearer {API_TOKEN} extra",
        f"Token {API_TOKEN}",
    ],
)
def test_invalid_token_is_unauthorized(client: TestClient, authorization: str) -> None:
    response = client.get("/system/health", headers={"Authorization": authorization})
    assert response.status_code == 401
    assert _code(response) == "AUTH_INVALID"
    assert response.headers["www-authenticate"] == "Bearer"


def test_bearer_scheme_is_case_insensitive(client: TestClient) -> None:
    assert client.get("/system/health", headers={"Authorization": f"bearer {API_TOKEN}"}).status_code == 200


def test_token_in_query_string_is_not_accepted(client: TestClient) -> None:
    response = client.get(f"/system/health?token={API_TOKEN}&access_token={API_TOKEN}")
    assert response.status_code == 401
    assert _code(response) == "AUTH_REQUIRED"


@pytest.mark.asyncio
async def test_duplicate_authorization_is_unauthorized(api: FastAPI) -> None:
    headers = [
        (b"host", b"127.0.0.1:8000"),
        (b"authorization", f"Bearer {API_TOKEN}".encode()),
        (b"authorization", b"Bearer other"),
    ]
    response = await asgi_call(api, headers=headers)
    assert response.status == 401
    assert json.loads(response.body)["error"]["code"] == "AUTH_INVALID"


def test_unauthorized_mutation_is_not_executed(api: FastAPI, client: TestClient) -> None:
    response = client.post("/test/actions", json={"a": 1}, headers=IDEMPOTENCY)
    assert response.status_code == 401
    assert routes_of(api).calls["actions"] == 0


def test_unauthorized_response_to_ui_origin_is_readable_by_ui(client: TestClient) -> None:
    response = client.get("/system/health", headers={"Origin": UI_ORIGIN})
    assert response.status_code == 401
    assert response.headers["access-control-allow-origin"] == UI_ORIGIN


# ---------------------------------------------------------------- CORS preflight


def test_preflight_from_ui_origin_needs_no_token(client: TestClient) -> None:
    response = client.options(
        "/test/actions",
        headers={
            "Origin": UI_ORIGIN,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "authorization, content-type, idempotency-key",
        },
    )
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == UI_ORIGIN
    allowed_headers = {h.strip().lower() for h in response.headers["access-control-allow-headers"].split(",")}
    assert {"authorization", "content-type", "idempotency-key"} <= allowed_headers
    allowed_methods = {m.strip() for m in response.headers["access-control-allow-methods"].split(",")}
    assert allowed_methods == {"GET", "POST", "PUT", "PATCH", "DELETE"}
    assert "access-control-allow-credentials" not in response.headers


def test_preflight_for_unlisted_header_is_refused(client: TestClient) -> None:
    response = client.options(
        "/test/actions",
        headers={
            "Origin": UI_ORIGIN,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "x-evil",
        },
    )
    assert response.status_code == 400


def test_preflight_for_private_network_access_is_refused(client: TestClient) -> None:
    response = client.options(
        "/test/actions",
        headers={
            "Origin": UI_ORIGIN,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Private-Network": "true",
        },
    )
    assert response.status_code == 400
    assert "access-control-allow-private-network" not in response.headers


def test_plain_options_without_preflight_headers_needs_token(client: TestClient) -> None:
    assert client.options("/system/health").status_code == 401


def test_replayed_marker_is_exposed_to_ui(client: TestClient) -> None:
    response = client.get("/system/health", headers={**AUTH, "Origin": UI_ORIGIN})
    assert response.headers["access-control-expose-headers"] == "Idempotency-Replayed"


# ---------------------------------------------------------------- документація API і заголовки


def test_openapi_requires_token(client: TestClient) -> None:
    assert client.get("/openapi.json").status_code == 401
    assert client.get("/openapi.json", headers=AUTH).status_code == 200


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/docs/oauth2-redirect"])
def test_interactive_docs_are_disabled(client: TestClient, path: str) -> None:
    # Swagger UI / ReDoc вантажать JS з CDN в origin API.
    assert client.get(path, headers=AUTH).status_code == 404


@pytest.mark.parametrize(
    ("headers", "status"),
    [(AUTH, 200), ({}, 401), ({**AUTH, "Host": "evil.com:8000"}, 403), ({**AUTH, "Origin": "http://evil.com"}, 403)],
)
def test_every_response_is_not_cached_and_not_sniffed(client: TestClient, headers: dict[str, str], status: int) -> None:
    response = client.get("/system/health", headers=headers)
    assert response.status_code == status
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"


def test_unknown_path_is_404_with_error_code(client: TestClient) -> None:
    response = client.get("/nope", headers=AUTH)
    assert response.status_code == 404
    assert _code(response) == "NOT_FOUND"


def test_unknown_path_without_token_is_401_not_404(client: TestClient) -> None:
    # Без токена не можна дізнатися, які шляхи існують.
    assert client.get("/nope").status_code == 401


# ---------------------------------------------------------------- WebSocket (CSWSH)

# TestClient для WebSocket не бере Host з base_url — задаємо явно.
WS_HOST = {"Host": "127.0.0.1:8000"}


@pytest.mark.parametrize(
    ("headers", "reason"),
    [
        ({**WS_HOST, **AUTH, "Origin": "http://evil.com"}, "ORIGIN_NOT_ALLOWED"),
        ({**AUTH, "Host": "evil.com:8000", "Origin": UI_ORIGIN}, "HOST_NOT_ALLOWED"),
        ({**WS_HOST, "Origin": UI_ORIGIN}, "AUTH_REQUIRED"),
        ({**WS_HOST, "Origin": UI_ORIGIN, "Authorization": "Bearer wrong"}, "AUTH_INVALID"),
    ],
)
def test_websocket_rejected_before_accept(
    api: FastAPI, client: TestClient, headers: dict[str, str], reason: str
) -> None:
    with (
        capture_logs() as logs,
        pytest.raises(WebSocketDisconnect) as exc,
        client.websocket_connect("/test/ws", headers=headers),
    ):
        pass
    assert exc.value.code == 1008
    assert [e["reason"] for e in logs if e["event"] == "api_request_rejected"] == [reason]
    assert routes_of(api).calls["ws"] == 0


def test_websocket_from_ui_with_token_is_accepted(api: FastAPI, client: TestClient) -> None:
    with client.websocket_connect("/test/ws", headers={**WS_HOST, **AUTH, "Origin": UI_ORIGIN}) as ws:
        assert ws.receive_json() == {"hello": True}
    assert routes_of(api).calls["ws"] == 1


# ---------------------------------------------------------------- логи


def test_rejections_are_logged_without_credentials(tmp_path: Path, client: TestClient) -> None:
    configure_logging(make_settings(log_dir=tmp_path, log_console=False), process="backend")
    wrong = "wrong-but-plausible-token-1234567890abcdef"
    try:
        client.get("/system/health", headers={"Authorization": f"Bearer {wrong}"})
        client.get("/system/health", headers={**AUTH, "Origin": "http://evil.com"})
        client.get("/system/health", headers={**AUTH, "Host": "evil.com:8000"})
    finally:
        shutdown_logging()
    text = "".join(p.read_text(encoding="utf-8") for p in (tmp_path / "backend").glob("*.log"))
    events = [json.loads(line) for line in text.splitlines() if line.strip()]
    reasons = {e.get("reason") for e in events if e["event"] == "api_request_rejected"}
    assert reasons == {"AUTH_INVALID", "ORIGIN_NOT_ALLOWED", "HOST_NOT_ALLOWED"}
    assert wrong not in text
    assert API_TOKEN not in text


def test_rejection_logs_are_rate_limited_and_summarized(client: TestClient, fake_clock: FakeClock) -> None:
    # Рев'ю T0.7: сторінка в браузері може слати відхилені запити в циклі — лог не повинен рости без меж.
    from app.api.security import REJECTION_LOG_BURST, REJECTION_LOG_WINDOW_MS

    with capture_logs() as logs:
        for _ in range(REJECTION_LOG_BURST + 5):
            client.get("/system/health", headers={"Origin": "http://evil.com"})
        client.get("/system/health")  # 401 у тому самому вікні — теж придушено
        fake_clock.advance(REJECTION_LOG_WINDOW_MS)
        client.get("/system/health", headers={"Host": "evil.com:8000"})
    rejected = [e for e in logs if e["event"] == "api_request_rejected"]
    assert len(rejected) == REJECTION_LOG_BURST + 1
    assert rejected[-1]["reason"] == "HOST_NOT_ALLOWED"
    (summary,) = [e for e in logs if e["event"] == "api_request_rejected_suppressed"]
    assert summary["counts"] == {"ORIGIN_NOT_ALLOWED": 5, "AUTH_REQUIRED": 1}


@pytest.mark.asyncio
async def test_missing_client_address_is_rejected_not_crashed(api: FastAPI) -> None:
    # ASGI дозволяє client=None (напр. Unix-сокет): відмова 403, а не 500 із TypeError.
    response = await asgi_call(api, headers=[(b"host", b"evil.com:8000")], client=None)
    assert response.status == 403


def test_suppressed_summary_is_written_on_next_allowed_request(client: TestClient, fake_clock: FakeClock) -> None:
    # Рев'ю T0.7: флуд припинився — підсумок не повинен чекати наступної відмови.
    from app.api.security import REJECTION_LOG_BURST, REJECTION_LOG_WINDOW_MS

    with capture_logs() as logs:
        for _ in range(REJECTION_LOG_BURST + 3):
            client.get("/system/health", headers={"Origin": "http://evil.com"})
        fake_clock.advance(REJECTION_LOG_WINDOW_MS)
        assert client.get("/system/health", headers=AUTH).status_code == 200
    (summary,) = [e for e in logs if e["event"] == "api_request_rejected_suppressed"]
    assert summary["counts"] == {"ORIGIN_NOT_ALLOWED": 3}
