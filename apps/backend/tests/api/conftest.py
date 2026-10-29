"""Фікстури unit-тестів локального API (T0.7): застосунок без БД — фейкові сховище ідемпотентності, lock і ping БД.

Семантика сховища в БД перевіряється окремо (`tests/db/test_api_idempotency_db.py`).
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.api.fakes import (
    BASE_URL,
    FakeDbPing,
    FakeLockState,
    MemoryIdempotencyStore,
    make_app,
)


@pytest.fixture
def store() -> MemoryIdempotencyStore:
    return MemoryIdempotencyStore()


@pytest.fixture
def lock_state() -> FakeLockState:
    return FakeLockState()


@pytest.fixture
def db_ping() -> FakeDbPing:
    return FakeDbPing()


@pytest.fixture
def api(store: MemoryIdempotencyStore, lock_state: FakeLockState, db_ping: FakeDbPing) -> FastAPI:
    return make_app(store=store, lock=lock_state, db_ping=db_ping)


@pytest.fixture
def client(api: FastAPI) -> Iterator[TestClient]:
    # Host = 127.0.0.1:8000 (base_url); 500 повертається відповіддю, а не винятком у тесті.
    with TestClient(api, base_url=BASE_URL, raise_server_exceptions=False) as test_client:
        yield test_client
