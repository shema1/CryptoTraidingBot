"""Фікстури integration-тестів схеми (T0.4). Працюють лише з тестовою БД `<POSTGRES_DB>_test`.

Без запущеної БД тести пропускаються з причиною; `CTB_REQUIRE_DB=1` перетворює пропуск на помилку.
Кожен тест працює у власній транзакції, яка відкочується (дані між тестами не протікають).
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncConnection

from app.core.config import TEST_DB_SUFFIX, Settings
from tests.db.dbutil import make_engine, run_alembic


def _require_db() -> bool:
    return os.environ.get("CTB_REQUIRE_DB") == "1"


@pytest.fixture(scope="session")
def test_db_url() -> URL:
    try:
        url = Settings().database_url(test=True)
    except ValueError as exc:  # немає POSTGRES_PASSWORD
        if _require_db():
            raise
        pytest.skip(f"тестова БД не налаштована: {exc}")
    # Тести роблять downgrade до base — лише на тестовій БД.
    assert url.database is not None
    assert url.database.endswith(TEST_DB_SUFFIX), f"integration-тести лише на *{TEST_DB_SUFFIX}: {url.database}"

    async def _ping() -> None:
        engine = make_engine(url)
        try:
            async with engine.connect():
                pass
        finally:
            await engine.dispose()

    try:
        asyncio.run(_ping())
    except (OSError, ConnectionError) as exc:
        if _require_db():
            raise
        pytest.skip(f"TimescaleDB недоступна ({type(exc).__name__}): docker compose up -d timescaledb")
    return url


@pytest.fixture(scope="session")
def migrated_db(test_db_url: URL) -> URL:
    """Тестова БД на `head`, починаючи з порожньої схеми."""
    asyncio.run(run_alembic(test_db_url, "downgrade", "base"))
    asyncio.run(run_alembic(test_db_url, "upgrade", "head"))
    return test_db_url


@pytest_asyncio.fixture
async def conn(migrated_db: URL) -> AsyncIterator[AsyncConnection]:
    """З'єднання з відкритою транзакцією; після тесту — rollback."""
    engine = make_engine(migrated_db)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                yield connection
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()
