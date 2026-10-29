"""Конфігурація бекенду з `.env` у корені репозиторію (ТЗ §14).

Правила:
- кожне поле `Settings` має рядок у `.env.example` (без значення) — це перевіряє `tests/test_env_example.py`;
- секрети — лише `SecretStr` (у `repr`/`str`/дампі вони приховані; логер додатково маскує їхні значення,
  `core/logging.py`);
- значення за замовчуванням консервативні, межі задані тут (`Field`), а документація посилається на них;
- `Settings` незмінний після створення (`frozen`).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Final, Literal

from pydantic import BaseModel, Field, SecretBytes, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from sqlalchemy.engine import URL

from app.core.db import SYMBOL_RE
from app.exchange.rate_limit import (
    DEFAULT_SHARE_COLLECTOR_PCT,
    DEFAULT_SHARE_TRADING_PCT,
    DEFAULT_SHARE_WORKER_PCT,
)

# app/core/config.py -> core -> app -> backend -> apps -> корінь репозиторію
REPO_ROOT = Path(__file__).resolve().parents[4]
ENV_FILE = REPO_ROOT / ".env"

LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
TEST_DB_SUFFIX = "_test"

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]

# Локальний API слухає лише цю адресу (ТЗ §14, AGENTS.md, інваріант 13); не конфігурується.
API_BIND_HOST: Final = "127.0.0.1"
# Bearer-токен UI: не коротший за 32 символи, алфавіт token68 (RFC 6750) — без пробілів і лапок у заголовку.
API_TOKEN_MIN_LEN: Final = 32
# Захист від очевидно слабких значень (`aaaa…`, `1212…`): згенерований токен має десятки різних символів.
API_TOKEN_MIN_DISTINCT: Final = 10
_API_TOKEN_RE: Final = re.compile(r"[A-Za-z0-9._~+/-]+=*")
# Origin UI: лише loopback, явний порт без провідних нулів, без шляху (так його надсилає браузер).
_LOOPBACK_ORIGIN_RE: Final = re.compile(r"(?:http|https)://(?:127\.0\.0\.1|localhost|\[::1\]):[1-9][0-9]{0,4}")

_MIB = 1024 * 1024
# 1 година в мс — крок свічок backfill (ТЗ §4.4). Має збігатися з `Timeframe.H1.duration_ms` (тест).
H1_MS: Final = 3_600_000
# ТЗ §5А.6: фіксований список за ліквідністю на початок історії, 5–8 символів. Торговий символ на старті — BTC.
DEFAULT_TRADING_SYMBOL: Final = "BTC/USDT:USDT"
DEFAULT_RESEARCH_SYMBOLS: Final = (
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
    "BNB/USDT:USDT",
    "DOGE/USDT:USDT",
    "ADA/USDT:USDT",
    "LTC/USDT:USDT",
)
RESEARCH_SYMBOLS_MIN: Final = 5
RESEARCH_SYMBOLS_MAX: Final = 8
# 2020-01-01T00:00:00Z — раніше за лістинг USDT-perpetual Bybit backfill не ходить.
DEFAULT_BACKFILL_START_MS: Final = 1_577_836_800_000
DEFAULT_BACKFILL_INTERVAL_S: Final = 600
BACKFILL_INTERVAL_MIN_S: Final = 60
BACKFILL_INTERVAL_MAX_S: Final = 86_400
_SYMBOL: Final = re.compile(SYMBOL_RE)


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

    # --- Single-instance lock торгового процесу (core/instance_lock.py, ТЗ §6.4, T0.6) ---
    # Скільки секунд при старті повторювати спроби взяти lock (0 … 600; 0 — одна спроба), потім — вихід.
    lock_wait_s: Annotated[int, Field(ge=0, le=600)] = 60

    # --- Локальний API торгового процесу (app/api, ТЗ §14, §16, T0.7). Адреса — лише API_BIND_HOST. ---
    # Порт API (1024 … 65535).
    api_port: Annotated[int, Field(ge=1024, le=65535)] = 8000
    # Bearer-токен UI (≥ API_TOKEN_MIN_LEN символів token68). Без нього торговий процес не стартує.
    api_token: SecretStr | None = None
    # Origins UI, яким дозволено звертатися до API (перевірка `Origin` і CORS); у `.env` — через кому.
    api_allowed_origins: Annotated[tuple[str, ...], NoDecode] = ("http://127.0.0.1:5173",)

    # --- Частки спільного IP-ліміту Bybit (app/exchange/rate_limit.py, ТЗ §6.5). Сума трьох — рівно 100. ---
    # Роль процесу тут не задається: `.env` спільний, роль передає точка входу (`RateLimiter.from_settings`).
    # Частка торгового процесу, %: 1 … 98 (за замовчуванням 50).
    rate_limit_share_trading_pct: Annotated[int, Field(ge=1, le=98)] = DEFAULT_SHARE_TRADING_PCT
    # Частка воркера, %: 1 … 98 (за замовчуванням 30).
    rate_limit_share_worker_pct: Annotated[int, Field(ge=1, le=98)] = DEFAULT_SHARE_WORKER_PCT
    # Частка колектора, %: 1 … 98 (за замовчуванням 20).
    rate_limit_share_collector_pct: Annotated[int, Field(ge=1, le=98)] = DEFAULT_SHARE_COLLECTOR_PCT

    # --- Backfill свічок 1h і funding (app/data/backfill.py, ТЗ §4.1, §4.4, T1.4) ---
    # Торговий символ. Свічки качаються і для нього, навіть якщо його немає в `research_symbols`.
    trading_symbol: str = DEFAULT_TRADING_SYMBOL
    # 5–8 USDT-perpetual для навчання (ТЗ §5А.6). У `.env` — через кому, канонічний формат CCXT.
    research_symbols: Annotated[tuple[str, ...], NoDecode] = DEFAULT_RESEARCH_SYMBOLS
    # Початок історії, мс UTC, кратний 1h. Backfill не запитує дані раніше за цю мить.
    backfill_start_ms: Annotated[int, Field(ge=0)] = DEFAULT_BACKFILL_START_MS
    # Як часто повторювати інкрементальне докачування, секунди (60 … 86400).
    backfill_interval_s: Annotated[int, Field(ge=BACKFILL_INTERVAL_MIN_S, le=BACKFILL_INTERVAL_MAX_S)] = (
        DEFAULT_BACKFILL_INTERVAL_S
    )

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

    @field_validator("api_token")
    @classmethod
    def _strong_api_token(cls, value: SecretStr | None) -> SecretStr | None:
        if value is None:
            return None
        token = value.get_secret_value()
        if (
            len(token) < API_TOKEN_MIN_LEN
            or _API_TOKEN_RE.fullmatch(token) is None
            or len(set(token)) < API_TOKEN_MIN_DISTINCT
        ):
            # Значення в тексті помилки не повторюється (і `hide_input_in_errors`).
            raise ValueError(
                f"API_TOKEN: щонайменше {API_TOKEN_MIN_LEN} символів [A-Za-z0-9._~+/-] (token68), "
                f"з них ≥ {API_TOKEN_MIN_DISTINCT} різних — згенеруйте: secrets.token_urlsafe(32)"
            )
        return value

    @field_validator("api_allowed_origins", mode="before")
    @classmethod
    def _split_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return tuple(item.strip() for item in value.split(",") if item.strip())
        return value

    @field_validator("api_allowed_origins")
    @classmethod
    def _loopback_origins(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        origins: list[str] = []
        for origin in value:
            normalized = origin.strip().lower()
            port = normalized.rsplit(":", 1)[-1]
            if _LOOPBACK_ORIGIN_RE.fullmatch(normalized) is None or not 1 <= int(port) <= 65535:
                raise ValueError(
                    "API_ALLOWED_ORIGINS: лише http(s)://127.0.0.1|localhost|[::1]:<порт> без шляху, через кому"
                )
            if normalized not in origins:
                origins.append(normalized)
        return tuple(origins)

    @field_validator("trading_symbol")
    @classmethod
    def _trading_symbol(cls, value: str) -> str:
        if _SYMBOL.fullmatch(value) is None:
            raise ValueError("TRADING_SYMBOL: канонічний формат CCXT, наприклад BTC/USDT:USDT")
        return value

    @field_validator("research_symbols", mode="before")
    @classmethod
    def _split_research_symbols(cls, value: object) -> object:
        if isinstance(value, str):
            return tuple(part.strip() for part in value.split(",") if part.strip())
        return value

    @field_validator("research_symbols")
    @classmethod
    def _research_symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not RESEARCH_SYMBOLS_MIN <= len(value) <= RESEARCH_SYMBOLS_MAX:
            raise ValueError(
                f"RESEARCH_SYMBOLS: від {RESEARCH_SYMBOLS_MIN} до {RESEARCH_SYMBOLS_MAX} символів (зараз {len(value)})"
            )
        if len(set(value)) != len(value):
            raise ValueError("RESEARCH_SYMBOLS: дублікати")
        for symbol in value:
            if _SYMBOL.fullmatch(symbol) is None:
                raise ValueError("RESEARCH_SYMBOLS: канонічний формат CCXT, наприклад BTC/USDT:USDT")
        return value

    @field_validator("backfill_start_ms")
    @classmethod
    def _backfill_start_aligned(cls, value: int) -> int:
        if value % H1_MS != 0:
            raise ValueError("BACKFILL_START_MS має бути вирівняний за 1h (кратний 3600000)")
        return value

    @model_validator(mode="after")
    def _rate_limit_shares_cover_the_ip(self) -> Settings:
        total = (
            self.rate_limit_share_trading_pct + self.rate_limit_share_worker_pct + self.rate_limit_share_collector_pct
        )
        if total != 100:
            raise ValueError(
                "RATE_LIMIT_SHARE_TRADING_PCT, RATE_LIMIT_SHARE_WORKER_PCT і "
                f"RATE_LIMIT_SHARE_COLLECTOR_PCT мають давати в сумі 100 (зараз {total})"
            )
        return self

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
