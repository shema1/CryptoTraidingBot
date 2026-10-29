"""`Idempotency-Key` для змінюючих запитів (ТЗ §16, T0.7 «Готово, коли»): повтор не виконує дію двічі.

Сховище — фейкове в пам'яті (`tests/api/fakes.py`); та сама семантика на реальній БД —
`tests/db/test_api_idempotency_db.py`.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.idempotency import (
    MAX_REQUEST_BODY_BYTES,
    IdempotencyRecord,
    IdempotencyStatus,
    request_fingerprint,
)
from app.core.logging import configure_logging, shutdown_logging
from tests.api.fakes import AUTH, BASE_URL, MemoryIdempotencyStore, asgi_call, make_settings, routes_of

KEY = "6f1c2a9e-4b7d-4c1e-9a0b-1d2e3f405162"
OTHER_KEY = "0a1b2c3d-4e5f-4061-8293-a4b5c6d7e8f9"


def _headers(key: str = KEY) -> dict[str, str]:
    return {**AUTH, "Idempotency-Key": key}


def _code(response: httpx.Response) -> str:
    return str(response.json()["error"]["code"])


# ---------------------------------------------------------------- обов'язковий і коректний ключ


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_mutation_without_key_is_rejected_and_not_executed(
    api: FastAPI, client: TestClient, store: MemoryIdempotencyStore, method: str
) -> None:
    response = client.request(method, "/test/actions", json={"a": 1}, headers=AUTH)
    assert response.status_code == 400
    assert _code(response) == "IDEMPOTENCY_KEY_REQUIRED"
    assert routes_of(api).calls["actions"] == 0
    assert store.calls["begin"] == 0


@pytest.mark.parametrize(
    "key",
    [
        "short-key",  # < 16
        "x" * 129,  # > 128
        "key with spaces 123",
        "ключ-кирилицею-0001",
        "key/with/slash/0001",
        "",
    ],
)
def test_malformed_key_is_rejected_and_not_executed(api: FastAPI, client: TestClient, key: str) -> None:
    # Байти UTF-8: httpx не надсилає не-ASCII рядок у заголовку, а браузер/скрипт — може.
    response = client.post("/test/actions", json={"a": 1}, headers={**AUTH, "Idempotency-Key": key.encode()})
    assert response.status_code == 400
    assert _code(response) == "IDEMPOTENCY_KEY_INVALID"
    assert routes_of(api).calls["actions"] == 0


@pytest.mark.asyncio
async def test_two_key_headers_are_rejected(api: FastAPI) -> None:
    from tests.api.fakes import API_TOKEN

    headers = [
        (b"host", b"127.0.0.1:8000"),
        (b"authorization", f"Bearer {API_TOKEN}".encode()),
        (b"idempotency-key", KEY.encode()),
        (b"idempotency-key", OTHER_KEY.encode()),
        (b"content-type", b"application/json"),
    ]
    response = await asgi_call(api, method="POST", path="/test/actions", headers=headers, body=b"{}")
    assert response.status == 400
    assert json.loads(response.body)["error"]["code"] == "IDEMPOTENCY_KEY_INVALID"
    assert routes_of(api).calls["actions"] == 0


def test_read_requests_need_no_key(api: FastAPI, client: TestClient, store: MemoryIdempotencyStore) -> None:
    assert client.get("/test/read", headers=AUTH).status_code == 200
    assert client.get("/test/read", headers=AUTH).json() == {"n": 2}
    assert store.calls["begin"] == 0


# ---------------------------------------------------------------- повтор не виконує дію двічі


def test_retry_with_same_key_does_not_execute_twice(api: FastAPI, client: TestClient) -> None:
    first = client.post("/test/actions", json={"qty": "0.001"}, headers=_headers())
    second = client.post("/test/actions", json={"qty": "0.001"}, headers=_headers())

    assert routes_of(api).calls["actions"] == 1
    assert first.status_code == second.status_code == 201
    assert first.content == second.content == b'{"n":1,"echo":{"qty":"0.001"}}'
    assert first.headers["content-type"] == second.headers["content-type"]
    assert "idempotency-replayed" not in first.headers
    assert second.headers["idempotency-replayed"] == "true"


def test_many_retries_execute_once(api: FastAPI, client: TestClient) -> None:
    responses = [client.post("/test/actions", json={"a": 1}, headers=_headers()) for _ in range(5)]
    assert routes_of(api).calls["actions"] == 1
    assert {r.content for r in responses} == {responses[0].content}


def test_different_keys_execute_separately(api: FastAPI, client: TestClient) -> None:
    client.post("/test/actions", json={"a": 1}, headers=_headers(KEY))
    client.post("/test/actions", json={"a": 1}, headers=_headers(OTHER_KEY))
    assert routes_of(api).calls["actions"] == 2


def test_handler_sees_key_for_audit(api: FastAPI, client: TestClient) -> None:
    client.post("/test/actions", json={"a": 1}, headers=_headers())
    assert routes_of(api).keys == [KEY]


def test_body_is_passed_to_handler_unchanged(client: TestClient) -> None:
    payload = {"symbol": "BTC/USDT:USDT", "qty": "0.001", "nested": {"list": [1, 2, 3]}}
    response = client.post("/test/actions", json=payload, headers=_headers())
    assert response.json()["echo"] == payload


def test_completed_record_stores_response(client: TestClient, store: MemoryIdempotencyStore) -> None:
    client.post("/test/actions", json={"a": 1}, headers=_headers())
    record = store.records[KEY]
    assert record.status is IdempotencyStatus.COMPLETED
    assert record.method == "POST"
    assert record.path == "/test/actions"
    assert record.owner == "test-instance"
    assert record.response is not None
    assert record.response.status == 201
    assert record.response.body == b'{"n":1,"echo":{"a":1}}'
    assert ("content-type", "application/json") in record.response.headers
    assert all(name != "content-length" for name, _ in record.response.headers)


def test_handler_error_response_is_replayed_not_reexecuted(api: FastAPI, client: TestClient) -> None:
    first = client.post("/test/conflict", headers=_headers())
    second = client.post("/test/conflict", headers=_headers())
    assert first.status_code == second.status_code == 409
    assert second.json() == first.json()
    assert routes_of(api).calls["conflict"] == 1


def test_validation_error_is_replayed(api: FastAPI, client: TestClient) -> None:
    first = client.delete("/test/actions/not-an-int", headers=_headers())
    second = client.delete("/test/actions/not-an-int", headers=_headers())
    assert first.status_code == second.status_code == 422
    assert _code(first) == "VALIDATION_ERROR"
    assert second.content == first.content
    assert second.headers["idempotency-replayed"] == "true"


# ---------------------------------------------------------------- той самий ключ — інший запит → 422


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("POST", "/test/actions", {"qty": "0.002"}),  # інше тіло
        ("POST", "/test/conflict", {"qty": "0.001"}),  # інший шлях
        ("DELETE", "/test/actions/1", None),  # інший метод і шлях
    ],
)
def test_key_reused_for_different_request_is_rejected(
    api: FastAPI, client: TestClient, method: str, path: str, body: object
) -> None:
    client.post("/test/actions", json={"qty": "0.001"}, headers=_headers())
    response = client.request(method, path, json=body, headers=_headers())
    assert response.status_code == 422
    assert _code(response) == "IDEMPOTENCY_KEY_REUSED"
    calls = routes_of(api).calls
    assert (calls["actions"], calls["conflict"], calls["delete"]) == (1, 0, 0)


def test_query_string_is_part_of_the_request(api: FastAPI, client: TestClient) -> None:
    client.post("/test/actions?dry=1", json={"a": 1}, headers=_headers())
    response = client.post("/test/actions?dry=0", json={"a": 1}, headers=_headers())
    assert response.status_code == 422
    assert routes_of(api).calls["actions"] == 1


def test_fingerprint_separates_fields() -> None:
    base = request_fingerprint("POST", "/a", b"", b"b")
    assert request_fingerprint("POST", "/a", b"", b"b") == base
    assert len(base) == 64
    # Межі полів не зсуваються: "/a" + "b" ≠ "/ab" + "".
    assert request_fingerprint("POST", "/ab", b"", b"") != base
    assert request_fingerprint("POST", "/a", b"b", b"") != base
    assert request_fingerprint("PUT", "/a", b"", b"b") != base


# ---------------------------------------------------------------- паралельний повтор, невідомий результат → 409


@pytest.mark.asyncio
async def test_concurrent_retry_while_in_progress_is_conflict(api: FastAPI) -> None:
    routes = routes_of(api)
    transport = httpx.ASGITransport(app=api)
    async with httpx.AsyncClient(transport=transport, base_url=BASE_URL) as client:
        first = asyncio.create_task(client.post("/test/slow", headers=_headers()))
        await routes.entered.wait()
        second = await client.post("/test/slow", headers=_headers())
        assert second.status_code == 409
        assert _code(second) == "IDEMPOTENCY_REQUEST_IN_PROGRESS"
        routes.release.set()
        assert (await first).status_code == 200
        third = await client.post("/test/slow", headers=_headers())
    assert third.status_code == 200
    assert third.headers["idempotency-replayed"] == "true"
    assert routes.calls["slow"] == 1


def test_failed_handler_is_never_retried_with_same_key(
    api: FastAPI, client: TestClient, store: MemoryIdempotencyStore
) -> None:
    first = client.post("/test/fail", headers=_headers())
    assert first.status_code == 500
    assert _code(first) == "INTERNAL_ERROR"
    assert store.records[KEY].status is IdempotencyStatus.FAILED

    second = client.post("/test/fail", headers=_headers())
    assert second.status_code == 409
    assert _code(second) == "IDEMPOTENCY_RESULT_UNKNOWN"
    assert routes_of(api).calls["fail"] == 1


def test_in_progress_record_of_previous_process_is_unknown(
    api: FastAPI, client: TestClient, store: MemoryIdempotencyStore
) -> None:
    # Процес упав (kill -9) посеред обробки: рядок лишився IN_PROGRESS з іншим власником.
    hash_ = request_fingerprint("POST", "/test/actions", b"", b'{"a":1}')
    store.records[KEY] = IdempotencyRecord(
        key=KEY,
        method="POST",
        path="/test/actions",
        request_hash=hash_,
        owner="previous-process",
        status=IdempotencyStatus.IN_PROGRESS,
        created_ms=0,
        response=None,
    )
    response = client.post(
        "/test/actions", content=b'{"a":1}', headers={**_headers(), "Content-Type": "application/json"}
    )
    assert response.status_code == 409
    assert _code(response) == "IDEMPOTENCY_RESULT_UNKNOWN"
    assert routes_of(api).calls["actions"] == 0


def test_response_too_large_to_store_is_marked_failed(
    api: FastAPI, client: TestClient, store: MemoryIdempotencyStore
) -> None:
    first = client.post("/test/big", headers=_headers())
    assert first.status_code == 200  # клієнт отримав відповідь повністю
    assert len(first.content) > 2 * 1024 * 1024
    assert store.records[KEY].status is IdempotencyStatus.FAILED
    second = client.post("/test/big", headers=_headers())
    assert second.status_code == 409
    assert _code(second) == "IDEMPOTENCY_RESULT_UNKNOWN"
    assert routes_of(api).calls["big"] == 1


# ---------------------------------------------------------------- збої сховища: fail-closed


def test_store_unavailable_before_execution_is_503_and_not_executed(
    api: FastAPI, client: TestClient, store: MemoryIdempotencyStore
) -> None:
    store.begin_error = ConnectionError("db down")
    response = client.post("/test/actions", json={"a": 1}, headers=_headers())
    assert response.status_code == 503
    assert _code(response) == "IDEMPOTENCY_STORE_UNAVAILABLE"
    assert routes_of(api).calls["actions"] == 0


def test_result_not_stored_still_answers_and_retry_is_not_reexecuted(
    api: FastAPI, client: TestClient, store: MemoryIdempotencyStore
) -> None:
    store.complete_error = ConnectionError("db down after action")
    first = client.post("/test/actions", json={"a": 1}, headers=_headers())
    assert first.status_code == 201
    assert store.records[KEY].status is IdempotencyStatus.IN_PROGRESS

    store.complete_error = None
    second = client.post("/test/actions", json={"a": 1}, headers=_headers())
    assert second.status_code == 409
    # Рев'ю T0.7: обробник уже не виконується — це «невідомо», а не «ще виконується».
    assert _code(second) == "IDEMPOTENCY_RESULT_UNKNOWN"
    assert routes_of(api).calls["actions"] == 1


def test_failure_marking_error_keeps_original_500(
    api: FastAPI, client: TestClient, store: MemoryIdempotencyStore
) -> None:
    store.fail_error = ConnectionError("db down")
    assert client.post("/test/fail", headers=_headers()).status_code == 500
    assert client.post("/test/fail", headers=_headers()).status_code == 409
    assert routes_of(api).calls["fail"] == 1


# ---------------------------------------------------------------- тіло запиту


def test_body_over_limit_is_rejected_before_execution(
    api: FastAPI, client: TestClient, store: MemoryIdempotencyStore
) -> None:
    body = b'"' + b"x" * MAX_REQUEST_BODY_BYTES + b'"'
    response = client.post("/test/actions", content=body, headers={**_headers(), "Content-Type": "application/json"})
    assert response.status_code == 413
    assert _code(response) == "REQUEST_TOO_LARGE"
    assert routes_of(api).calls["actions"] == 0
    assert store.calls["begin"] == 0


@pytest.mark.asyncio
async def test_declared_content_length_over_limit_is_rejected_without_reading(api: FastAPI) -> None:
    from tests.api.fakes import API_TOKEN

    headers = [
        (b"host", b"127.0.0.1:8000"),
        (b"authorization", f"Bearer {API_TOKEN}".encode()),
        (b"idempotency-key", KEY.encode()),
        (b"content-length", str(MAX_REQUEST_BODY_BYTES + 1).encode()),
    ]
    response = await asgi_call(api, method="POST", path="/test/actions", headers=headers)
    assert response.status == 413
    assert routes_of(api).calls["actions"] == 0


# ---------------------------------------------------------------- порядок: автентифікація до ідемпотентності


def test_unauthenticated_request_does_not_burn_the_key(
    api: FastAPI, client: TestClient, store: MemoryIdempotencyStore
) -> None:
    response = client.post("/test/actions", json={"a": 1}, headers={"Idempotency-Key": KEY})
    assert response.status_code == 401
    assert store.calls["begin"] == 0
    assert client.post("/test/actions", json={"a": 1}, headers=_headers()).status_code == 201
    assert routes_of(api).calls["actions"] == 1


# ---------------------------------------------------------------- логи


def test_replay_and_failures_are_logged(tmp_path: Path, client: TestClient, store: MemoryIdempotencyStore) -> None:
    configure_logging(make_settings(log_dir=tmp_path, log_console=False), process="backend")
    try:
        client.post("/test/actions", json={"a": 1}, headers=_headers())
        client.post("/test/actions", json={"a": 1}, headers=_headers())
        store.complete_error = ConnectionError("down")
        client.post("/test/actions", json={"a": 1}, headers=_headers(OTHER_KEY))
    finally:
        shutdown_logging()
    events = [
        json.loads(line)
        for line in (tmp_path / "backend" / "app.log").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    names = [e["event"] for e in events]
    assert "idempotency_replayed" in names
    stored_failed = next(e for e in events if e["event"] == "idempotency_result_not_stored")
    assert stored_failed["level"] == "error"
    assert stored_failed["idempotency_key"] == OTHER_KEY


# ---------------------------------------------------------------- рев'ю T0.7


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["TRACE", "post", "CONNECT", "PROPFIND"])
async def test_unknown_non_safe_method_is_refused_before_handler(
    api: FastAPI, store: MemoryIdempotencyStore, method: str
) -> None:
    from tests.api.fakes import API_TOKEN

    headers = [
        (b"host", b"127.0.0.1:8000"),
        (b"authorization", f"Bearer {API_TOKEN}".encode()),
        (b"idempotency-key", KEY.encode()),
    ]
    response = await asgi_call(api, method=method, path="/test/actions", headers=headers, body=b"{}")
    assert response.status == 405
    assert json.loads(response.body)["error"]["code"] == "METHOD_NOT_ALLOWED"
    assert routes_of(api).calls["actions"] == 0
    assert store.calls["begin"] == 0


@pytest.mark.asyncio
async def test_cancelled_handler_leaves_unknown_result_never_reexecuted(
    api: FastAPI, store: MemoryIdempotencyStore
) -> None:
    # Зупинка процесу посеред обробки: запис лишається IN_PROGRESS; повтор — «невідомо», і в цьому процесі, і в новому.
    from tests.api.fakes import make_app

    routes = routes_of(api)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api), base_url=BASE_URL) as client:
        request = asyncio.create_task(client.post("/test/slow", headers=_headers()))
        await routes.entered.wait()
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert store.records[KEY].status is IdempotencyStatus.IN_PROGRESS
        same_process = await client.post("/test/slow", headers=_headers())
    assert same_process.status_code == 409
    assert _code(same_process) == "IDEMPOTENCY_RESULT_UNKNOWN"

    restarted = make_app(store=store, instance_id="next-process")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted), base_url=BASE_URL) as client:
        after_restart = await client.post("/test/slow", headers=_headers())
    assert after_restart.status_code == 409
    assert _code(after_restart) == "IDEMPOTENCY_RESULT_UNKNOWN"
    assert routes.calls["slow"] == 1
    assert routes_of(restarted).calls["slow"] == 0


def test_key_bounds_have_single_source() -> None:
    from app.api import idempotency, models

    assert (idempotency.KEY_MIN_LEN, idempotency.KEY_MAX_LEN) == (16, 128)
    assert models.IDEMPOTENCY_KEY_RE == "^[A-Za-z0-9_.:-]{16,128}$"


@pytest.mark.asyncio
async def test_retry_between_commit_and_begin_return_is_in_progress(
    api: FastAPI, store: MemoryIdempotencyStore
) -> None:
    # Рев'ю T0.7: переможець уже закомітив запис, але `begin()` ще не повернувся (прибирання сесії) — повтор має
    # отримати «ще виконується», а не «невідомо» (інакше людина повторила б дію з новим ключем).
    committed = asyncio.Event()
    release = asyncio.Event()
    original_begin = store.begin

    async def slow_begin(record: IdempotencyRecord) -> IdempotencyRecord | None:
        result = await original_begin(record)
        if result is None:
            committed.set()
            await release.wait()
        return result

    store.begin = slow_begin  # type: ignore[method-assign]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api), base_url=BASE_URL) as client:
        first = asyncio.create_task(client.post("/test/actions", json={"a": 1}, headers=_headers()))
        await committed.wait()
        retry = await client.post("/test/actions", json={"a": 1}, headers=_headers())
        release.set()
        assert (await first).status_code == 201
    assert retry.status_code == 409
    assert _code(retry) == "IDEMPOTENCY_REQUEST_IN_PROGRESS"
    assert routes_of(api).calls["actions"] == 1
