"""`GET /system/health` (ТЗ §16, T0.7): стан lock торгового процесу і доступність БД; деградація → 503."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.api import system
from app.core.clock import FakeClock
from tests.api.fakes import AUTH, FakeDbPing, FakeLockState
from tests.conftest import FAKE_NOW_MS


def test_healthy(client: TestClient, db_ping: FakeDbPing, fake_clock: FakeClock) -> None:
    fake_clock.advance(1500)
    response = client.get("/system/health", headers=AUTH)
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "time_ms": FAKE_NOW_MS + 1500,
        "uptime_ms": 1500,
        "checks": {"instance_lock": True, "database": True},
    }
    assert db_ping.calls == 1


def test_lost_instance_lock_is_degraded(client: TestClient, lock_state: FakeLockState) -> None:
    lock_state.held = False
    response = client.get("/system/health", headers=AUTH)
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "degraded"
    assert body["checks"] == {"instance_lock": False, "database": True}


def test_database_error_is_degraded(client: TestClient, db_ping: FakeDbPing) -> None:
    db_ping.error = ConnectionRefusedError("db down")
    response = client.get("/system/health", headers=AUTH)
    assert response.status_code == 503
    assert response.json()["checks"] == {"instance_lock": True, "database": False}
    assert "db down" not in response.text  # текст помилки не віддається назовні


def test_hanging_database_is_bounded(client: TestClient, db_ping: FakeDbPing, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(system, "HEALTH_DB_TIMEOUT_MS", 20)
    db_ping.hang = True
    response = client.get("/system/health", headers=AUTH)
    assert response.status_code == 503
    assert response.json()["checks"]["database"] is False


def test_health_db_timeout_default() -> None:
    assert system.HEALTH_DB_TIMEOUT_MS == 2000
