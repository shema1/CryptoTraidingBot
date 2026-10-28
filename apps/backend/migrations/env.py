"""Alembic environment (T0.4): async-двигун asyncpg, DSN з `Settings`, метадані — `app.core.db.load_models()`.

- CLI: `uv run alembic upgrade head` — основна БД; `uv run alembic -x db=test upgrade head` — тестова `<db>_test`.
- Програмно (тести): передати готове з'єднання в `config.attributes["connection"]` — тоді DSN не потрібен.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import Settings
from app.core.db import load_models

config = context.config
target_metadata = load_models()


def _include_object(obj: object, name: str | None, type_: str, reflected: bool, compare_to: object) -> bool:
    # Службові об'єкти TimescaleDB живуть у власних схемах і в порівняння не потрапляють; у public — лише наші.
    return not (type_ == "table" and reflected and compare_to is None and name is not None and name.startswith("_"))


def _configure(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
        include_object=_include_object,
        transaction_per_migration=True,
    )


def _run_sync(connection: Connection) -> None:
    _configure(connection)
    with context.begin_transaction():
        context.run_migrations()


async def _run_async() -> None:
    db = context.get_x_argument(as_dictionary=True).get("db", "main")
    if db not in {"main", "test"}:
        raise ValueError(f"-x db=main|test, отримано {db!r}")
    url = Settings().database_url(test=db == "test")
    engine = create_async_engine(url, poolclass=pool.NullPool)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(_run_sync)
    finally:
        await engine.dispose()


def run_migrations_online() -> None:
    connection = config.attributes.get("connection")
    if connection is not None:
        _run_sync(connection)
        return
    if config.config_file_name is not None:
        fileConfig(config.config_file_name, disable_existing_loggers=False)
    asyncio.run(_run_async())


if context.is_offline_mode():
    raise RuntimeError("offline-режим (--sql) не підтримується: міграції містять функції TimescaleDB і тригери")
run_migrations_online()
