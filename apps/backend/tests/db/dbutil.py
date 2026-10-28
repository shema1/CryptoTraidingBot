"""Помічники integration-тестів БД (T0.4): engine без пулу, запуск Alembic на з'єднанні."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import pool
from sqlalchemy.engine import URL, Connection
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

BACKEND_DIR = Path(__file__).resolve().parents[2]


def alembic_config(connection: Connection) -> Config:
    cfg = Config(str(BACKEND_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(BACKEND_DIR / "migrations"))
    cfg.attributes["connection"] = connection
    return cfg


def make_engine(url: URL) -> AsyncEngine:
    return create_async_engine(url, poolclass=pool.NullPool)


async def run_alembic(url: URL, action: str, revision: str) -> None:
    """`command.upgrade/downgrade` на з'єднанні з тестовою БД (в окремій транзакції на ревізію)."""
    engine = make_engine(url)
    try:
        async with engine.connect() as conn:

            def _run(sync_conn: Connection) -> None:
                getattr(command, action)(alembic_config(sync_conn), revision)

            await conn.run_sync(_run)
            await conn.commit()
    finally:
        await engine.dispose()
