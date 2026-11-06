"""Фікстури integration-тестів схеми (T0.4). Працюють лише з тестовою БД `<POSTGRES_DB>_test`.

Без запущеної БД тести пропускаються з причиною; `CTB_REQUIRE_DB=1` перетворює пропуск на помилку.
Кожен тест працює у власній транзакції, яка відкочується (дані між тестами не протікають).

Тестова БД одна на комп'ютер, а `migrated_db` на старті робить `downgrade base` → `upgrade head`: другий
одночасний прогін pytest зніс би схему першому посеред тестів (T0.8). Тому сесія тримає міжпроцесне
блокування файла `ctb-test-db-<назва БД>.lock` у тимчасовій теці ОС: другий прогін чекає, доки перший завершиться.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import IO

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


def db_lock_path(database: str) -> Path:
    """Файл міжпроцесного блокування тестової БД (один на назву БД)."""
    return Path(tempfile.gettempdir()) / f"ctb-test-db-{database}.lock"


# Блокування тримається до кінця процесу pytest. Фікстури `tests/db/conftest.py` реекспортують `tests/{ml,data,jobs}`,
# і pytest створює окремий екземпляр сесійної фікстури на кожен conftest, а сам модуль буває завантажений двічі
# (як conftest і як `tests.db.conftest`), тож кеш у модулі не спільний. Другий `flock` того самого файла з нового
# дескриптора в тому самому процесі чекав би сам на себе. Тому власник пише в файл свій PID: збіг — блокування вже
# в цього процесу (інший прогін, взявши блокування, перезаписав би PID своїм).
_HELD: dict[str, IO[str]] = {}


def hold_test_db_lock(database: str) -> IO[str] | None:
    """Ексклюзивне блокування тестової БД до кінця процесу. Чекає, поки інший прогін його відпустить.

    Повторний виклик у тому самому процесі не чекає. `None` — блокування вже в цього процесу (з іншої копії модуля)
    або ОС без `fcntl` (нативна Windows; проєкт запускається на macOS і WSL2). Відпускає його ОС, коли процес pytest
    завершується.
    """
    if sys.platform == "win32":
        return None
    held = _HELD.get(database)
    if held is not None:
        return held
    import fcntl

    path = db_lock_path(database)
    me = str(os.getpid())
    if path.exists() and path.read_text(encoding="utf-8").strip() == me:
        return None
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.stderr.write(f"\nТестова БД {database} зайнята іншим прогоном pytest — чекаю, поки він завершиться…\n")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    handle.seek(0)
    handle.truncate()
    handle.write(me)
    handle.flush()
    _HELD[database] = handle
    return handle


@pytest.fixture(scope="session")
def test_db_lock(test_db_url: URL) -> IO[str] | None:
    """Блокування на весь прогін: міграції й тести одного прогону не перетинаються з іншим прогоном."""
    assert test_db_url.database is not None
    return hold_test_db_lock(test_db_url.database)


@pytest.fixture(scope="session")
def migrated_db(test_db_url: URL, test_db_lock: IO[str] | None) -> URL:
    """Тестова БД на `head`, починаючи з порожньої схеми. Лише під блокуванням `test_db_lock`."""
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
