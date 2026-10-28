"""Конкурентна зупинка бота й вставка позиції/ордера (T0.4, рев'ю): тригери не мають гонки «перевірка → дія».

Два окремих з'єднання з реальними комітами. Дані лишаються в `<db>_test` до наступної сесії тестів
(`migrated_db` починає з `downgrade base`); акаунти й боти щоразу нові, тож тести не заважають одне одному.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import insert, text, update
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.trading.models import Bot, Order, Position
from tests.db.dbutil import make_engine
from tests.db.factories import make_account, make_bot, order_values, position_values

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# Скільки разів опитати pg_stat_activity, чекаючи, поки друга сесія стане в чергу на блокування.
_MAX_POLLS = 2000


@pytest_asyncio.fixture
async def engine(migrated_db: URL) -> AsyncIterator[AsyncEngine]:
    engine = make_engine(migrated_db)
    try:
        yield engine
    finally:
        await engine.dispose()


async def _committed_bot(engine: AsyncEngine) -> tuple[uuid.UUID, uuid.UUID]:
    async with engine.begin() as conn:
        account = await make_account(conn)
        bot = await make_bot(conn, account, status="RUNNING")
    return account, bot


async def _wait_until_blocked(observer: AsyncConnection, pid: int) -> None:
    """Опитує, доки сесія `pid` не чекає на блокування (детерміновано, без sleep)."""
    for _ in range(_MAX_POLLS):
        waiting = await observer.scalar(
            text("SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = :pid"), {"pid": pid}
        )
        if waiting:
            return
    pytest.fail(f"сесія {pid} так і не стала чекати на блокування")


async def _pid(conn: AsyncConnection) -> int:
    pid = await conn.scalar(text("SELECT pg_backend_pid()"))
    assert isinstance(pid, int)
    return pid


async def test_stop_waits_for_uncommitted_position_and_then_refuses(engine: AsyncEngine) -> None:
    """T1 вставляє позицію (не закомічено) → T2 зупиняє бота, чекає на T1 → після коміту T1 зупинка відхиляється."""
    account, bot = await _committed_bot(engine)
    async with engine.connect() as t1, engine.connect() as t2, engine.connect() as observer:
        await t1.begin()
        await t1.execute(insert(Position).values(position_values(bot, account)))

        await t2.begin()
        stop = asyncio.create_task(t2.execute(update(Bot).where(Bot.id == bot).values(status="STOPPED")))
        await _wait_until_blocked(observer, await _pid(t2))
        await t1.commit()

        with pytest.raises(IntegrityError, match="cannot stop bot"):
            await stop
        await t2.rollback()


async def test_entry_waits_for_uncommitted_stop_and_then_refuses(engine: AsyncEngine) -> None:
    """T2 зупиняє бота (не закомічено) → T1 вставляє вхід, чекає на T2 → після коміту T2 вхід відхиляється."""
    account, bot = await _committed_bot(engine)
    async with engine.connect() as t1, engine.connect() as t2, engine.connect() as observer:
        await t2.begin()
        await t2.execute(update(Bot).where(Bot.id == bot).values(status="STOPPED"))

        await t1.begin()
        entry = asyncio.create_task(t1.execute(insert(Order).values(order_values(account, "demo", bot))))
        await _wait_until_blocked(observer, await _pid(t1))
        await t2.commit()

        with pytest.raises(IntegrityError, match="is STOPPED"):
            await entry
        await t1.rollback()


async def test_stop_waits_for_uncommitted_submitting_order(engine: AsyncEngine) -> None:
    """Write-ahead SUBMITTING паралельно із зупинкою: зупинка бачить нетермінальний ордер і відхиляється."""
    account, bot = await _committed_bot(engine)
    async with engine.connect() as t1, engine.connect() as t2, engine.connect() as observer:
        await t1.begin()
        await t1.execute(insert(Order).values(order_values(account, "demo", bot, state="SUBMITTING")))

        await t2.begin()
        stop = asyncio.create_task(t2.execute(update(Bot).where(Bot.id == bot).values(status="STOPPED")))
        await _wait_until_blocked(observer, await _pid(t2))
        await t1.commit()

        with pytest.raises(IntegrityError, match="non-terminal orders"):
            await stop
        await t2.rollback()
