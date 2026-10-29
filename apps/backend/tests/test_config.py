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
    assert settings.lock_wait_s == 60


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("log_max_bytes", 0),
        ("log_max_bytes", 1024 * 1024 - 1),
        ("log_max_bytes", 1024 * 1024 * 1024 + 1),
        ("log_backup_count", 0),
        ("log_backup_count", 101),
        ("log_level", "TRACE"),
        ("lock_wait_s", -1),
        ("lock_wait_s", 601),
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
        ("lock_wait_s", 0),
        ("lock_wait_s", 600),
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


# --- База даних (T0.4) ---


@pytest.mark.parametrize("host", ["10.0.0.5", "db.example.com", "0.0.0.0", ""])  # noqa: S104 — перевіряємо відмову
def test_postgres_host_must_be_loopback(host: str) -> None:
    with pytest.raises(ValidationError):
        _settings(postgres_host=host)


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "::1"])
def test_postgres_loopback_hosts_are_accepted(host: str) -> None:
    assert _settings(postgres_host=host).postgres_host == host


@pytest.mark.parametrize("port", [0, 65536])
def test_postgres_port_bounds(port: int) -> None:
    with pytest.raises(ValidationError):
        _settings(postgres_port=port)


def test_database_url_main_and_test() -> None:
    settings = _settings(postgres_password=DB_PASSWORD, postgres_db="ctb", postgres_port=5433)
    main = settings.database_url()
    test = settings.database_url(test=True)
    assert (main.drivername, main.host, main.port, main.database) == ("postgresql+asyncpg", "127.0.0.1", 5433, "ctb")
    assert test.database == "ctb_test"
    assert main.password == DB_PASSWORD


def test_database_url_hides_password() -> None:
    url = _settings(postgres_password=DB_PASSWORD).database_url()
    assert DB_PASSWORD not in repr(url)
    assert DB_PASSWORD not in str(url)


def test_database_url_requires_password() -> None:
    with pytest.raises(ValueError, match="POSTGRES_PASSWORD"):
        _settings().database_url()


# ---------------------------------------------------------------- локальний API (T0.7)

API_TOKEN = "t0k3n-for-config-tests_0123456789-ABC"  # 37 символів token68


def test_api_defaults_are_conservative() -> None:
    settings = _settings()
    assert settings.api_port == 8000
    assert settings.api_token is None  # без токена торговий процес не стартує (app/__main__.py)
    assert settings.api_allowed_origins == ("http://127.0.0.1:5173",)


def test_api_bind_host_is_loopback_constant() -> None:
    from app.core.config import API_BIND_HOST

    assert API_BIND_HOST == "127.0.0.1"
    assert "api_host" not in Settings.model_fields


@pytest.mark.parametrize("port", [0, 80, 1023, 65536])
def test_api_port_bounds(port: int) -> None:
    with pytest.raises(ValidationError):
        _settings(api_port=port)


@pytest.mark.parametrize("port", [1024, 65535])
def test_api_port_boundaries_are_accepted(port: int) -> None:
    assert _settings(api_port=port).api_port == port


def test_api_token_is_secret_and_masked_in_logs() -> None:
    settings = _settings(api_token=API_TOKEN)
    assert isinstance(settings.api_token, SecretStr)
    assert API_TOKEN not in repr(settings)
    assert API_TOKEN in settings.secret_values()


@pytest.mark.parametrize(
    "token",
    [
        "x" * 31,  # коротший за API_TOKEN_MIN_LEN
        "token with spaces 0123456789abcdef",
        'token"quote0123456789abcdefghijklmn',
        "токен-кирилицею-0123456789abcdefghij",
        "=" + "a" * 40,  # token68: «=» лише в кінці
        "a" * 32,  # рев'ю T0.7: довжина є, ентропії немає
        "12" * 20,
        "abcdefghi" * 4,  # 9 різних символів
    ],
)
def test_weak_or_malformed_api_token_is_rejected_without_echo(token: str) -> None:
    with pytest.raises(ValidationError) as exc:
        _settings(api_token=token)
    assert token not in str(exc.value)
    assert "API_TOKEN" in str(exc.value)


@pytest.mark.parametrize("token", ["Az09._~+/-" * 4 + "==", API_TOKEN])
def test_valid_api_tokens_are_accepted(token: str) -> None:
    settings = _settings(api_token=token)
    assert settings.api_token is not None
    assert settings.api_token.get_secret_value() == token


def test_api_origins_from_environment_are_split_normalized_and_deduplicated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_ALLOWED_ORIGINS", " http://LOCALHOST:5173 ,http://127.0.0.1:4173,http://localhost:5173, ")
    assert Settings(_env_file=None).api_allowed_origins == ("http://localhost:5173", "http://127.0.0.1:4173")


def test_empty_api_origins_variable_means_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_ALLOWED_ORIGINS", "")
    assert Settings(_env_file=None).api_allowed_origins == ("http://127.0.0.1:5173",)


def test_api_origins_may_be_empty_list(monkeypatch: pytest.MonkeyPatch) -> None:
    # Порожній allowlist: браузер не може звертатися до API (лише curl/скрипти без Origin).
    monkeypatch.setenv("API_ALLOWED_ORIGINS", " , ")
    assert Settings(_env_file=None).api_allowed_origins == ()


@pytest.mark.parametrize(
    "origin",
    [
        "*",
        "null",
        "http://evil.com:80",
        "http://evil.com",
        "http://192.168.1.10:5173",
        "http://0.0.0.0:5173",
        "http://127.0.0.1",  # без порту
        "http://127.0.0.1:5173/",  # зі шляхом
        "http://127.0.0.1:5173/app",
        "http://127.0.0.1:05173",
        "http://127.0.0.1:0",
        "http://127.0.0.1:70000",
        "ftp://127.0.0.1:21",
        "http://user@127.0.0.1:5173",
        "http://127.0.0.1.nip.io:5173",
        "http://[::1]",
    ],
)
def test_non_loopback_or_malformed_origin_is_rejected(origin: str) -> None:
    with pytest.raises(ValidationError, match="API_ALLOWED_ORIGINS"):
        _settings(api_allowed_origins=(origin,))


@pytest.mark.parametrize(
    "origin", ["http://127.0.0.1:5173", "http://localhost:4173", "https://localhost:8443", "http://[::1]:5173"]
)
def test_loopback_origins_are_accepted(origin: str) -> None:
    assert _settings(api_allowed_origins=(origin,)).api_allowed_origins == (origin,)


def test_rate_limit_shares_default_to_the_spec() -> None:
    settings = _settings()
    assert (
        settings.rate_limit_share_trading_pct,
        settings.rate_limit_share_worker_pct,
        settings.rate_limit_share_collector_pct,
    ) == (50, 30, 20)


def test_rate_limit_shares_must_sum_to_100() -> None:
    with pytest.raises(ValidationError, match="100"):
        _settings(
            rate_limit_share_trading_pct=40,
            rate_limit_share_worker_pct=40,
            rate_limit_share_collector_pct=40,
        )


def test_rate_limit_shares_are_read_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RATE_LIMIT_SHARE_TRADING_PCT", "40")
    monkeypatch.setenv("RATE_LIMIT_SHARE_WORKER_PCT", "40")
    monkeypatch.setenv("RATE_LIMIT_SHARE_COLLECTOR_PCT", "20")
    assert _settings().rate_limit_share_trading_pct == 40


def test_rate_limit_role_is_not_a_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    # `.env` спільний для торгового процесу, воркера й колектора — роль задає точка входу процесу.
    monkeypatch.setenv("RATE_LIMIT_ROLE", "collector")
    assert "rate_limit_role" not in Settings.model_fields
    assert not hasattr(_settings(), "rate_limit_role")
