"""Сховище `Idempotency-Key` у БД (T0.7, рішення 0007): таблиця `api_idempotency_keys` і `DbIdempotencyStore`.

Окремі з'єднання з реальними комітами (як у `test_concurrency.py`): ключі унікальні для кожного тесту, тож дані
не заважають іншим тестам і лишаються в `<db>_test` до наступної сесії (`migrated_db` починає з `downgrade base`).
Обмеження перевіряються в транзакції з rollback (`conn`).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, insert, select, update
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.api.idempotency import (
    DbIdempotencyStore,
    IdempotencyRecord,
    IdempotencyStatus,
    StoredResponse,
    request_fingerprint,
)
from app.api.models import ApiIdempotencyKey
from app.core.db import session_factory
from tests.api.fakes import AUTH, BASE_URL, make_app, routes_of
from tests.db.dbutil import make_engine
from tests.db.factories import expect_violation

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

T0 = 1_767_225_600_000
RESPONSE = StoredResponse(
    status=201, headers=(("content-type", "application/json"), ("location", "/bots/1")), body=b'{"id":1}'
)


@pytest_asyncio.fixture
async def engine(migrated_db: URL) -> AsyncIterator[AsyncEngine]:
    engine = make_engine(migrated_db)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def store(engine: AsyncEngine) -> DbIdempotencyStore:
    return DbIdempotencyStore(session_factory(engine))


def _key() -> str:
    return str(uuid.uuid4())


def _record(key: str, owner: str = "proc-a", body: bytes = b"{}") -> IdempotencyRecord:
    return IdempotencyRecord(
        key=key,
        method="POST",
        path="/bots",
        request_hash=request_fingerprint("POST", "/bots", b"", body),
        owner=owner,
        status=IdempotencyStatus.IN_PROGRESS,
        created_ms=T0,
        response=None,
    )


async def test_begin_inserts_once_and_returns_existing(store: DbIdempotencyStore) -> None:
    key = _key()
    assert await store.begin(_record(key)) is None
    existing = await store.begin(_record(key, owner="proc-b", body=b'{"other":1}'))
    assert existing == _record(key)  # первинний запис без змін


async def test_concurrent_begin_has_exactly_one_winner(store: DbIdempotencyStore) -> None:
    key = _key()
    results = await asyncio.gather(*(store.begin(_record(key, owner=f"proc-{i}")) for i in range(10)))
    winners = [r for r in results if r is None]
    assert len(winners) == 1
    owners = {r.owner for r in results if r is not None}
    assert len(owners) == 1  # усі інші бачать того самого переможця


async def test_complete_stores_response_and_is_final(store: DbIdempotencyStore) -> None:
    key = _key()
    await store.begin(_record(key))
    assert await store.complete(key, "proc-a", RESPONSE, now_ms=T0 + 5) is True
    assert await store.complete(key, "proc-a", RESPONSE, now_ms=T0 + 6) is False
    assert await store.fail(key, "proc-a", now_ms=T0 + 7) is False

    stored = await store.begin(_record(key))
    assert stored is not None
    assert stored.status is IdempotencyStatus.COMPLETED
    assert stored.response == RESPONSE


async def test_fail_is_final(store: DbIdempotencyStore) -> None:
    key = _key()
    await store.begin(_record(key))
    assert await store.fail(key, "proc-a", now_ms=T0 + 5) is True
    assert await store.complete(key, "proc-a", RESPONSE, now_ms=T0 + 6) is False
    stored = await store.begin(_record(key))
    assert stored is not None
    assert stored.status is IdempotencyStatus.FAILED
    assert stored.response is None


async def test_other_owner_cannot_finish(store: DbIdempotencyStore) -> None:
    key = _key()
    await store.begin(_record(key, owner="proc-a"))
    assert await store.complete(key, "proc-b", RESPONSE, now_ms=T0) is False
    assert await store.fail(key, "proc-b", now_ms=T0) is False
    stored = await store.begin(_record(key))
    assert stored is not None
    assert stored.status is IdempotencyStatus.IN_PROGRESS


async def test_finishing_unknown_key_is_false(store: DbIdempotencyStore) -> None:
    assert await store.complete(_key(), "proc-a", RESPONSE, now_ms=T0) is False


async def test_end_to_end_retry_executes_once_on_real_db(store: DbIdempotencyStore, engine: AsyncEngine) -> None:
    app = make_app(store=store)
    key = _key()
    headers = {**AUTH, "Idempotency-Key": key}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as client:
        first = await client.post("/test/actions", json={"qty": "0.001"}, headers=headers)
        second = await client.post("/test/actions", json={"qty": "0.001"}, headers=headers)
        reused = await client.post("/test/actions", json={"qty": "0.002"}, headers=headers)
    assert routes_of(app).calls["actions"] == 1
    assert (first.status_code, second.status_code, reused.status_code) == (201, 201, 422)
    assert second.content == first.content
    assert second.headers["idempotency-replayed"] == "true"

    async with engine.connect() as conn:
        row = (await conn.execute(select(ApiIdempotencyKey).where(ApiIdempotencyKey.idempotency_key == key))).one()
    assert row.status == "COMPLETED"
    assert row.response_status == 201
    assert row.owner == "test-instance"


# ---------------------------------------------------------------- обмеження таблиці


def _values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "idempotency_key": _key(),
        "method": "POST",
        "path": "/bots",
        "request_hash": "a" * 64,
        "owner": "proc-a",
        "status": "IN_PROGRESS",
        "created_ms": T0,
    }
    values.update(overrides)
    return values


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"idempotency_key": "short"}, "ck_api_idempotency_keys_key_format"),
        ({"idempotency_key": "k" * 129}, "ck_api_idempotency_keys_key_format"),
        ({"idempotency_key": "has space 000000000"}, "ck_api_idempotency_keys_key_format"),
        ({"method": "GET"}, "ck_api_idempotency_keys_method"),
        ({"request_hash": "A" * 64}, "ck_api_idempotency_keys_request_hash_format"),
        ({"request_hash": "a" * 63}, "ck_api_idempotency_keys_request_hash_format"),
        ({"owner": ""}, "ck_api_idempotency_keys_owner_not_empty"),
        ({"status": "DONE", "completed_ms": T0}, "ck_api_idempotency_keys_status"),
        ({"completed_ms": T0}, "ck_api_idempotency_keys_completed_ms_iff_finished"),
        ({"status": "FAILED"}, "ck_api_idempotency_keys_completed_ms_iff_finished"),
        (
            {"status": "COMPLETED", "completed_ms": T0},
            "ck_api_idempotency_keys_response_iff_completed",
        ),
        (
            {
                "status": "FAILED",
                "completed_ms": T0,
                "response_status": 200,
                "response_headers": [],
                "response_body": b"",
            },
            "ck_api_idempotency_keys_response_iff_completed",
        ),
        (
            {
                "status": "COMPLETED",
                "completed_ms": T0,
                "response_status": 99,
                "response_headers": [],
                "response_body": b"",
            },
            "ck_api_idempotency_keys_response_status_range",
        ),
    ],
)
async def test_check_constraints(conn: AsyncConnection, overrides: dict[str, object], constraint: str) -> None:
    await expect_violation(conn, insert(ApiIdempotencyKey).values(_values(**overrides)), constraint)


async def test_primary_key_is_the_idempotency_key(conn: AsyncConnection) -> None:
    values = _values()
    await conn.execute(insert(ApiIdempotencyKey).values(values))
    await expect_violation(
        conn, insert(ApiIdempotencyKey).values({**values, "owner": "proc-b"}), "pk_api_idempotency_keys"
    )


async def test_valid_rows_are_accepted(conn: AsyncConnection) -> None:
    await conn.execute(insert(ApiIdempotencyKey).values(_values()))
    await conn.execute(insert(ApiIdempotencyKey).values(_values(status="FAILED", completed_ms=T0)))
    completed = _values(
        status="COMPLETED", completed_ms=T0, response_status=204, response_headers=[], response_body=b""
    )
    await conn.execute(insert(ApiIdempotencyKey).values(completed))


# ---------------------------------------------------------------- тригер ctb_api_idempotency_guard (рев'ю T0.7)


def _completed(**overrides: object) -> dict[str, object]:
    return _values(
        status="COMPLETED", completed_ms=T0, response_status=201, response_headers=[], response_body=b"{}", **overrides
    )


@pytest.mark.parametrize(
    "values",
    [
        {"status": "FAILED", "response_status": None, "response_headers": None, "response_body": None},
        {
            "status": "IN_PROGRESS",
            "completed_ms": None,
            "response_status": None,
            "response_headers": None,
            "response_body": None,
        },
        {"response_body": b"other"},
        {"response_status": 500},
    ],
)
async def test_completed_record_is_final(conn: AsyncConnection, values: dict[str, object]) -> None:
    row = _completed()
    await conn.execute(insert(ApiIdempotencyKey).values(row))
    stmt = update(ApiIdempotencyKey).where(ApiIdempotencyKey.idempotency_key == row["idempotency_key"]).values(values)
    await expect_violation(conn, stmt, "no transitions from final status COMPLETED")


async def test_failed_record_is_final(conn: AsyncConnection) -> None:
    row = _values(status="FAILED", completed_ms=T0)
    await conn.execute(insert(ApiIdempotencyKey).values(row))
    stmt = (
        update(ApiIdempotencyKey)
        .where(ApiIdempotencyKey.idempotency_key == row["idempotency_key"])
        .values(status="COMPLETED", response_status=200, response_headers=[], response_body=b"")
    )
    await expect_violation(conn, stmt, "no transitions from final status FAILED")


@pytest.mark.parametrize(
    "values",
    [
        {"idempotency_key": "another-key-0000000001"},
        {"method": "PUT"},
        {"path": "/other"},
        {"request_hash": "b" * 64},
        {"owner": "proc-b"},
        {"created_ms": T0 + 1},
    ],
)
async def test_request_identity_is_immutable(conn: AsyncConnection, values: dict[str, object]) -> None:
    row = _values()
    await conn.execute(insert(ApiIdempotencyKey).values(row))
    stmt = update(ApiIdempotencyKey).where(ApiIdempotencyKey.idempotency_key == row["idempotency_key"]).values(values)
    await expect_violation(conn, stmt, "request identity is immutable")


async def test_in_progress_can_finish_and_rows_can_be_deleted(conn: AsyncConnection) -> None:
    row = _values()
    await conn.execute(insert(ApiIdempotencyKey).values(row))
    key = ApiIdempotencyKey.idempotency_key == row["idempotency_key"]
    await conn.execute(update(ApiIdempotencyKey).where(key).values(status="FAILED", completed_ms=T0))
    await conn.execute(delete(ApiIdempotencyKey).where(key))
