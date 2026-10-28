"""Конфіг `Settings`: консервативні defaults, валідація, секрети як `SecretStr` (ТЗ §14, T0.3)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import pytest
from pydantic import BaseModel, Field, SecretBytes, SecretStr, ValidationError

from app.core.config import REPO_ROOT, Settings

DB_PASSWORD = "db-Pa55word-for-tests"
RO_PASSWORD = "ro-Pa55word-for-tests"


def _settings(**kwargs: object) -> Settings:
    return Settings(_env_file=None, **kwargs)  # type: ignore[arg-type]


def test_defaults() -> None:
    settings = _settings()
    assert settings.log_level == "INFO"
    assert settings.log_dir == REPO_ROOT / "logs"
    assert settings.log_max_bytes == 50 * 1024 * 1024
    assert settings.log_backup_count == 10
    assert settings.log_console is True
    assert settings.postgres_password is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("log_max_bytes", 0),
        ("log_max_bytes", 1024 * 1024 - 1),
        ("log_max_bytes", 1024 * 1024 * 1024 + 1),
        ("log_backup_count", 0),
        ("log_backup_count", 101),
        ("log_level", "TRACE"),
    ],
)
def test_out_of_range_values_are_rejected(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        _settings(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("log_max_bytes", 1024 * 1024),
        ("log_max_bytes", 1024 * 1024 * 1024),
        ("log_backup_count", 1),
        ("log_backup_count", 100),
    ],
)
def test_boundary_values_are_accepted(field: str, value: int) -> None:
    assert getattr(_settings(**{field: value}), field) == value


def test_values_are_read_from_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LOG_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_MAX_BYTES", str(2 * 1024 * 1024))
    monkeypatch.setenv("LOG_BACKUP_COUNT", "3")
    monkeypatch.setenv("LOG_CONSOLE", "false")
    settings = _settings()
    assert settings.log_dir == tmp_path
    assert settings.log_max_bytes == 2 * 1024 * 1024
    assert settings.log_backup_count == 3
    assert settings.log_console is False


def test_relative_log_dir_is_resolved_from_repo_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_DIR", "var/logs")
    assert _settings().log_dir == REPO_ROOT / "var" / "logs"


def test_settings_are_immutable() -> None:
    settings = _settings()
    with pytest.raises(ValidationError):
        settings.log_level = "DEBUG"  # type: ignore[misc]  # перевіряємо саме заборону


def test_secret_values_returns_only_set_secrets() -> None:
    assert _settings().secret_values() == []
    settings = _settings(postgres_password=SecretStr(DB_PASSWORD), postgres_readonly_password=SecretStr(RO_PASSWORD))
    assert sorted(settings.secret_values()) == sorted([DB_PASSWORD, RO_PASSWORD])


def test_secrets_are_hidden_in_repr_str_and_dump() -> None:
    settings = _settings(postgres_password=SecretStr(DB_PASSWORD))
    assert DB_PASSWORD not in repr(settings)
    assert DB_PASSWORD not in str(settings)
    assert DB_PASSWORD not in settings.model_dump_json()


class _Keys(BaseModel):
    api_key: SecretStr
    api_secret: SecretStr


class _NestedSettings(Settings):
    keys: _Keys | None = None
    blobs: list[SecretStr] = Field(default_factory=list)
    by_name: dict[str, SecretStr] = Field(default_factory=dict)
    raw: SecretBytes | None = None


def test_secret_values_walks_nested_models_and_collections() -> None:
    settings = _NestedSettings(  # type: ignore[call-arg]
        _env_file=None,
        keys=_Keys(api_key=SecretStr("nested-key-111"), api_secret=SecretStr("nested-secret-222")),
        blobs=[SecretStr("list-secret-333")],
        by_name={"a": SecretStr("dict-secret-444")},
        raw=SecretBytes(b"bytes-secret-555"),
    )
    assert sorted(settings.secret_values()) == sorted(
        ["nested-key-111", "nested-secret-222", "list-secret-333", "dict-secret-444", "bytes-secret-555"]
    )


class _StrictSecretSettings(Settings):
    token: Annotated[SecretStr, Field(min_length=40)] | None = None


def test_validation_error_does_not_echo_input(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TOKEN", "short-real-secret-ABC")
    with pytest.raises(ValidationError) as info:
        _StrictSecretSettings(_env_file=None)  # type: ignore[call-arg]
    assert "short-real-secret-ABC" not in str(info.value)
    assert "short-real-secret-ABC" not in repr(info.value)
