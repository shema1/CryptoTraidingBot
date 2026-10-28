"""Конфігурація бекенду з `.env` у корені репозиторію (ТЗ §14).

Правила:
- кожне поле `Settings` має рядок у `.env.example` (без значення) — це перевіряє `tests/test_env_example.py`;
- секрети — лише `SecretStr` (у `repr`/`str`/дампі вони приховані; логер додатково маскує їхні значення,
  `core/logging.py`);
- значення за замовчуванням консервативні, межі задані тут (`Field`), а документація посилається на них;
- `Settings` незмінний після створення (`frozen`).
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field, SecretBytes, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

# app/core/config.py -> core -> app -> backend -> apps -> корінь репозиторію
REPO_ROOT = Path(__file__).resolve().parents[4]
ENV_FILE = REPO_ROOT / ".env"

LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
TEST_DB_SUFFIX = "_test"

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]

_MIB = 1024 * 1024


class Settings(BaseSettings):
    """Налаштування процесу. Ім'я змінної оточення = назва поля у верхньому регістрі."""

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        env_prefix="",
        case_sensitive=False,
        # Порожнє значення (`LOG_LEVEL=` як у .env.example) означає «взяти default», а не помилку.
        env_ignore_empty=True,
        # У `.env` лежать і змінні інших процесів/задач (ключі біржі, T3.6).
        extra="ignore",
        frozen=True,
        validate_default=True,
        # Settings створюється до configure_logging: текст ValidationError не повинен містити введене значення.
        hide_input_in_errors=True,
    )

    # --- Логування (core/logging.py, ТЗ §12) ---
    log_level: LogLevel = "INFO"
    # Тека логів; відносний шлях рахується від кореня репозиторію. Кожен процес пише у власну підтеку.
    log_dir: Path = REPO_ROOT / "logs"
    # Розмір одного файла до ротації, байти (1 MiB … 1 GiB).
    log_max_bytes: Annotated[int, Field(ge=_MIB, le=1024 * _MIB)] = 50 * _MIB
    # Скільки ротованих копій зберігати на кожен файл (1 … 100).
    log_backup_count: Annotated[int, Field(ge=1, le=100)] = 10
    # Дублювати логи в stderr (JSON). Під launchd/systemd можна вимкнути, щоб stderr-файл не ріс без ротації.
    log_console: bool = True

    # --- База даних (docker-compose.yml, T0.2; DSN — `database_url()`, T0.4). ---
    # Лише loopback (AGENTS.md, інваріант 13): БД працює на тому ж комп'ютері, що й бекенд.
    postgres_host: str = "127.0.0.1"
    # Порт БД (1 … 65535).
    postgres_port: Annotated[int, Field(ge=1, le=65535)] = 5432
    postgres_db: str = "ctb"
    postgres_user: str = "ctb"
    postgres_password: SecretStr | None = None
    # Пароль read-only ролі ctb_readonly (MCP `postgres-readonly`); застосовується лише при першій ініціалізації volume.
    postgres_readonly_password: SecretStr | None = None

    @field_validator("log_dir")
    @classmethod
    def _resolve_log_dir(cls, value: Path) -> Path:
        value = value.expanduser()
        return value if value.is_absolute() else REPO_ROOT / value

    @field_validator("postgres_host")
    @classmethod
    def _loopback_only(cls, value: str) -> str:
        if value not in LOOPBACK_HOSTS:
            raise ValueError(f"POSTGRES_HOST має бути loopback-адресою: {sorted(LOOPBACK_HOSTS)}")
        return value

    @property
    def postgres_test_db(self) -> str:
        """Окрема БД для integration-тестів (створюється init-скриптом, T0.2)."""
        return f"{self.postgres_db}{TEST_DB_SUFFIX}"

    def database_url(self, *, test: bool = False) -> URL:
        """DSN для SQLAlchemy (`postgresql+asyncpg`). У `repr`/`str` пароль прихований (`***`).

        `test=True` — тестова БД `<POSTGRES_DB>_test`. Без `POSTGRES_PASSWORD` — помилка, а не порожній пароль.
        """
        if self.postgres_password is None:
            raise ValueError("POSTGRES_PASSWORD не задано в .env")
        return URL.create(
            drivername="postgresql+asyncpg",
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_test_db if test else self.postgres_db,
        )

    def secret_values(self) -> list[str]:
        """Відкриті значення всіх заданих `SecretStr`/`SecretBytes` — лише для реєстрації в маскері логів.

        Обходить і вкладені моделі та колекції (напр. набір ключів біржі як окрема модель, T3.6).
        """
        values: list[str] = []
        _collect_secrets(self, values, 0)
        return values


def _collect_secrets(value: object, out: list[str], depth: int) -> None:
    if depth > 10:
        return
    if isinstance(value, SecretStr):
        out.append(value.get_secret_value())
    elif isinstance(value, SecretBytes):
        out.append(value.get_secret_value().decode("utf-8", errors="replace"))
    elif isinstance(value, BaseModel):
        for name in type(value).model_fields:
            _collect_secrets(getattr(value, name), out, depth + 1)
    elif isinstance(value, Mapping):
        for item in value.values():
            _collect_secrets(item, out, depth + 1)
    elif isinstance(value, list | tuple | set | frozenset):
        for item in value:
            _collect_secrets(item, out, depth + 1)
