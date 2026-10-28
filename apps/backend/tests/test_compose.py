"""Статична перевірка docker-compose.yml (T0.2, ТЗ §12.2, AGENTS.md інваріант 13).

Живу перевірку (healthy, порт, read-only роль) виконує `scripts/check_db.sh`.
"""

from __future__ import annotations

from typing import Any

import pytest
import yaml

from app.core.config import REPO_ROOT

COMPOSE = REPO_ROOT / "docker-compose.yml"
INIT_DIR = REPO_ROOT / "infra" / "db" / "init"


@pytest.fixture(scope="module")
def service() -> dict[str, Any]:
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert set(data["services"]) == {"timescaledb"}, "у compose лише TimescaleDB (ТЗ §2)"
    service: dict[str, Any] = data["services"]["timescaledb"]
    return service


def test_ports_published_on_loopback_only(service: dict[str, Any]) -> None:
    ports = service["ports"]
    assert ports, "порт БД має бути опублікований для локального бекенду"
    assert all(str(p).startswith("127.0.0.1:") for p in ports), f"порт не на 127.0.0.1: {ports}"


def test_restart_policy(service: dict[str, Any]) -> None:
    assert service["restart"] == "unless-stopped"


def test_healthcheck_uses_pg_isready(service: dict[str, Any]) -> None:
    assert "pg_isready" in " ".join(service["healthcheck"]["test"])


def test_data_lives_in_named_volume(service: dict[str, Any]) -> None:
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    mounts = [v for v in service["volumes"] if "/var/lib/postgresql/data" in v]
    assert len(mounts) == 1
    volume_name = mounts[0].split(":")[0]
    assert volume_name in data["volumes"], "дані БД мають лежати в іменованому volume, а не в bind mount"


def test_image_tag_is_pinned(service: dict[str, Any]) -> None:
    tag = service["image"].split(":", 1)[1]
    assert not tag.startswith("latest"), "версію образу потрібно закріпити"


def test_passwords_are_required_not_defaulted(service: dict[str, Any]) -> None:
    env = service["environment"]
    for name in ("POSTGRES_PASSWORD", "POSTGRES_READONLY_PASSWORD"):
        assert f"${{{name}:?" in env[name], f"{name} має бути обов'язковим (без default)"


def test_compose_variables_are_documented_in_env_example() -> None:
    """Усі POSTGRES_* змінні compose є в `Settings` (а отже, і в .env.example — це перевіряє test_env_example)."""
    from app.core.config import Settings

    text = COMPOSE.read_text(encoding="utf-8")
    fields = {name.upper() for name in Settings.model_fields}
    for var in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_READONLY_PASSWORD"):
        assert var in text
        assert var in fields


def test_init_script_defines_readonly_role_and_test_db() -> None:
    script = (INIT_DIR / "01-roles-and-test-db.sh").read_text(encoding="utf-8")
    assert "ctb_readonly" in script
    assert "default_transaction_read_only" in script
    assert "_test" in script
