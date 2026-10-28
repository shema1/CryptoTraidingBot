"""Конфігурація бекенду з `.env` у корені репозиторію (ТЗ §14).

Каркас (T0.1): лише базове поле. Повний конфіг, валідація та `SecretStr`-поля — задача T0.3.
Правило: кожне поле `Settings` має рядок у `.env.example` (без значення) — це перевіряє
`tests/test_env_example.py`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# app/core/config.py -> core -> app -> backend -> apps -> корінь репозиторію
REPO_ROOT = Path(__file__).resolve().parents[4]
ENV_FILE = REPO_ROOT / ".env"

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]


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
    )

    log_level: LogLevel = "INFO"

    # База даних (docker-compose.yml, T0.2). Паролі обов'язкові для `docker compose up`; повний DSN — T0.4.
    postgres_db: str = "ctb"
    postgres_user: str = "ctb"
    postgres_password: SecretStr | None = None
    # Пароль read-only ролі ctb_readonly (MCP `postgres-readonly`); застосовується лише при першій ініціалізації volume.
    postgres_readonly_password: SecretStr | None = None
