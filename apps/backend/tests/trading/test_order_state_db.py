"""Переходи ордера в БД (ТЗ §7.1–7.2, T3.1): OCC, подія в тій самій транзакції, write-ahead `SUBMITTING`."""

from __future__ import annotations

import asyncio
import re
import uuid
from collections import deque
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker

from app.exchange.types import CLIENT_ORDER_ID_RE
from app.trading.models import Order, OrderEvent
from app.trading.order_state import (
    InvariantViolation,
    NewOrder,
    OrderConflict,
    OrderRef,
    begin_submission,
    create_order,
    transition_order,
)
from tests.db.dbutil import make_engine
from tests.db.factories import SYMBOL, make_account, make_bot
from tests.trading.test_order_state import EXCHANGE_ID_EDGES, SPEC

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_MAX_POLLS = 2000


def _order(account: uuid.UUID, bot: uuid.UUID) -> NewOrder:
    # Ручний тестовий вхід у Demo (ТЗ §16): без сигналу; інваріанти походження тримає БД.
    return NewOrder(
        account_id=account,
        mode="demo",
        bot_id=bot,
        origin="MANUAL_TEST",
        symbol=SYMBOL,
        side="BUY",
        order_type="MARKET",
        reduce_only=False,
        qty=Decimal("0.010"),
    )


def _path_to(state: str) -> list[tuple[str, str, str]]:
    """Найкоротший ланцюжок ребер `(from, to, джерело)` від `CREATED` до `state` з дозволеним джерелом."""
    queue: deque[tuple[str, list[tuple[str, str, str]]]] = deque([("CREATED", [])])
    seen = {"CREATED"}
    while queue:
        current, path = queue.popleft()
        if current == state:
            return path
        for (origin, target), sources in sorted(SPEC.items()):
            if origin == current and target not in seen:
                seen.add(target)
                queue.append((target, [*path, (origin, target, sorted(sources)[0])]))
    raise AssertionError(f"стан {state} недосяжний")


def _kwargs(origin: str, target: str) -> dict[str, Any]:
    """Обов'язкові поля переходу: код відхилення для `REJECTED`, id ордера біржі для першого факту біржі."""
    kwargs: dict[str, Any] = {}
    if target == "REJECTED":
        kwargs["reject_code"] = "10001"
    if (origin, target) in EXCHANGE_ID_EDGES:
        kwargs["exchange_order_id"] = f"ex-{uuid.uuid4().hex}"
    return kwargs


async def _walk(session: AsyncSession, ref: OrderRef, state: str) -> OrderRef:
    for origin, target, source in _path_to(state):
        ref = await transition_order(session, ref, target, source=source, **_kwargs(origin, target))
    return ref


async def _events(session: AsyncSession, order_id: uuid.UUID) -> list[OrderEvent]:
    stmt = select(OrderEvent).where(OrderEvent.order_id == order_id).order_by(OrderEvent.id)
    return list((await session.execute(stmt)).scalars())


async def test_create_order_writes_created_with_a_fresh_bot_id_and_one_event(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        first = await create_order(session, _order(account, bot), host_id="k7")
        second = await create_order(session, _order(account, bot), host_id="k7")
        assert first.state == "CREATED"
        assert first.client_order_id is not None
        assert first.version == 1
        assert re.fullmatch(CLIENT_ORDER_ID_RE, first.client_order_id)
        assert len(first.client_order_id) == 36
        assert first.client_order_id.startswith("tbk7")
        assert first.client_order_id != second.client_order_id
        events = await _events(session, first.id)
        assert [(event.from_state, event.to_state, event.source) for event in events] == [(None, "CREATED", "LOCAL")]


@pytest.mark.parametrize(
    ("edge", "source"), [(edge, source) for edge, sources in sorted(SPEC.items()) for source in sorted(sources)]
)
async def test_every_allowed_transition_writes_exactly_one_event(
    conn: AsyncConnection, edge: tuple[str, str], source: str
) -> None:
    origin, target = edge
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await _walk(session, await create_order(session, _order(account, bot), host_id="aa"), origin)
        assert ref.state == origin
        before = len(await _events(session, ref.id))
        kwargs = _kwargs(origin, target)
        if source == "LOCAL":
            kwargs.pop("exchange_order_id", None)
        moved = await transition_order(session, ref, target, source=source, reason="test", **kwargs)
        assert (moved.state, moved.version) == (target, ref.version + 1)
        events = await _events(session, ref.id)
        assert len(events) == before + 1
        last = events[-1]
        assert (last.from_state, last.to_state, last.source, last.reason) == (origin, target, source, "test")
        stored = await session.get(Order, ref.id, populate_existing=True)
        assert stored is not None
        assert (stored.state, stored.version) == (target, moved.version)


async def test_forbidden_transition_or_wrong_source_changes_nothing(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await create_order(session, _order(account, bot), host_id="aa")
        with pytest.raises(InvariantViolation):
            await transition_order(session, ref, "FILLED", source="WS")
        ref = await transition_order(session, ref, "SUBMITTING", source="LOCAL")
        # Факт біржі з локального джерела — порушення: біржа — джерело істини для фактів.
        with pytest.raises(InvariantViolation, match="джерело"):
            await transition_order(session, ref, "OPEN", source="LOCAL")
        stored = await session.get(Order, ref.id, populate_existing=True)
        assert stored is not None
        assert (stored.state, stored.version) == ("SUBMITTING", ref.version)
        assert len(await _events(session, ref.id)) == 2


async def test_terminal_state_has_no_way_out(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await _walk(session, await create_order(session, _order(account, bot), host_id="aa"), "FILLED")
        for target in ("OPEN", "CANCELED", "PENDING_CANCEL", "SUBMITTING"):
            with pytest.raises(InvariantViolation):
                await transition_order(session, ref, target, source="WS")


async def test_stale_version_is_a_conflict_not_a_write(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        created = await create_order(session, _order(account, bot), host_id="aa")
        submitting = await transition_order(session, created, "SUBMITTING", source="LOCAL")
        stale = OrderRef(id=created.id, client_order_id=created.client_order_id, state="SUBMITTING", version=1)
        with pytest.raises(OrderConflict) as raised:
            await transition_order(session, stale, "OPEN", source="REST", exchange_order_id="ex-stale")
        assert (raised.value.state, raised.value.version) == ("SUBMITTING", submitting.version)
        assert len(await _events(session, created.id)) == 2


async def test_rejected_needs_a_code_and_only_rejected_takes_one(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await transition_order(
            session, await create_order(session, _order(account, bot), host_id="aa"), "SUBMITTING", source="LOCAL"
        )
        with pytest.raises(ValueError, match="reject_code"):
            await transition_order(session, ref, "REJECTED", source="REST")
        with pytest.raises(ValueError, match="reject_code"):
            await transition_order(session, ref, "OPEN", source="REST", reject_code="10001")
        rejected = await transition_order(
            session, ref, "REJECTED", source="REST", reject_code="110007", reject_reason="insufficient balance"
        )
        stored = await session.get(Order, rejected.id, populate_existing=True)
        assert stored is not None
        assert (stored.state, stored.reject_code, stored.reject_reason) == (
            "REJECTED",
            "110007",
            "insufficient balance",
        )


async def test_exchange_order_id_is_stored_with_the_acceptance(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await create_order(session, _order(account, bot), host_id="aa")
        ref = await transition_order(session, ref, "SUBMITTING", source="LOCAL")
        ref = await transition_order(session, ref, "OPEN", source="REST", exchange_order_id="ex-1")
        stored = await session.get(Order, ref.id, populate_existing=True)
        assert stored is not None
        assert stored.exchange_order_id == "ex-1"


async def test_a_ref_with_the_right_version_but_another_state_conflicts(conn: AsyncConnection) -> None:
    """Рев'ю T3.1: умова `state` у WHERE — підроблений стан з правильною версією не дає недозволеного ребра."""
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await transition_order(
            session, await create_order(session, _order(account, bot), host_id="aa"), "SUBMITTING", source="LOCAL"
        )
        forged = OrderRef(id=ref.id, client_order_id=ref.client_order_id, state="OPEN", version=ref.version)
        with pytest.raises(OrderConflict) as raised:
            await transition_order(session, forged, "FILLED", source="WS")
        assert raised.value.state == "SUBMITTING"
        assert [event.to_state for event in await _events(session, ref.id)] == ["CREATED", "SUBMITTING"]


async def test_a_ref_with_another_client_order_id_is_refused(conn: AsyncConnection) -> None:
    """Рев'ю T3.1: write-ahead для id A і відправка з id B — порушення інваріанта 7; ref звіряється з БД."""
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await create_order(session, _order(account, bot), host_id="aa")
        other = await create_order(session, _order(account, bot), host_id="aa")
        mixed = OrderRef(id=ref.id, client_order_id=other.client_order_id, state="CREATED", version=ref.version)
        with pytest.raises(ValueError, match="clientOrderId"):
            await transition_order(session, mixed, "SUBMITTING", source="LOCAL")
        stored = await session.get(Order, ref.id, populate_existing=True)
        assert stored is not None
        assert stored.state == "CREATED"


async def test_a_failed_event_insert_rolls_the_state_back(
    conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Рев'ю T3.1: стан і подія — одне ціле. Падіння запису події не лишає нового стану без події."""
    from app.trading import order_state

    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await create_order(session, _order(account, bot), host_id="aa")

        async def broken(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("подія не записалась")

        monkeypatch.setattr(order_state, "_insert_event", broken)
        with pytest.raises(RuntimeError, match="подія"):
            await transition_order(session, ref, "SUBMITTING", source="LOCAL")
        monkeypatch.undo()
        stored = await session.get(Order, ref.id, populate_existing=True)
        assert stored is not None
        assert (stored.state, stored.version) == ("CREATED", 1)
        # Сесія жива: той самий перехід далі проходить.
        moved = await transition_order(session, ref, "SUBMITTING", source="LOCAL")
        assert moved.version == 2


async def test_payload_is_checked_before_any_write_and_decimals_become_strings(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await create_order(session, _order(account, bot), host_id="aa")
        with pytest.raises(ValueError, match="payload"):
            await transition_order(session, ref, "SUBMITTING", source="LOCAL", payload={"raw": object()})
        stored = await session.get(Order, ref.id, populate_existing=True)
        assert stored is not None
        assert stored.state == "CREATED"
        moved = await transition_order(
            session, ref, "SUBMITTING", source="LOCAL", payload={"qty": Decimal("0.010"), "nested": [Decimal("1.5")]}
        )
        event = (await _events(session, moved.id))[-1]
        assert event.payload == {"qty": "0.010", "nested": ["1.5"]}


async def test_exchange_order_id_rules(conn: AsyncConnection) -> None:
    """Перший факт біржі — з id ордера біржі; id — лише з подією біржі; змінити записаний id не дасть тригер БД."""
    from sqlalchemy.exc import IntegrityError

    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await transition_order(
            session, await create_order(session, _order(account, bot), host_id="aa"), "SUBMITTING", source="LOCAL"
        )
        with pytest.raises(ValueError, match="exchange_order_id"):
            await transition_order(session, ref, "OPEN", source="REST")
        with pytest.raises(ValueError, match="exchange_order_id"):
            await transition_order(session, ref, "SUBMISSION_UNKNOWN", source="LOCAL", exchange_order_id="ex-x")
        with pytest.raises(ValueError, match="exchange_order_id"):
            await transition_order(session, ref, "OPEN", source="REST", exchange_order_id="")
        opened = await transition_order(session, ref, "OPEN", source="REST", exchange_order_id="ex-a")
        with pytest.raises(IntegrityError, match="immutable"):
            await transition_order(session, opened, "PARTIALLY_FILLED", source="WS", exchange_order_id="ex-b")
        stored = await session.get(Order, ref.id, populate_existing=True)
        assert stored is not None
        assert (stored.state, stored.exchange_order_id) == ("OPEN", "ex-a")


async def test_reject_reason_without_rejected_and_a_missing_order_are_refused(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        ref = await create_order(session, _order(account, bot), host_id="aa")
        with pytest.raises(ValueError, match="reject_reason"):
            await transition_order(session, ref, "SUBMITTING", source="LOCAL", reject_reason="x")
        ghost = OrderRef(id=uuid.uuid4(), client_order_id=ref.client_order_id, state="CREATED", version=1)
        with pytest.raises(LookupError):
            await transition_order(session, ghost, "SUBMITTING", source="LOCAL")


@pytest.mark.parametrize(
    "change",
    [
        {"qty": 0.01},
        {"qty": Decimal(0)},
        {"qty": Decimal("-0.01")},
        {"qty": Decimal("NaN")},
        {"price": 65000.0},
        {"sl_price": Decimal(0)},
        {"tp_price": Decimal("Infinity")},
    ],
)
async def test_create_order_refuses_non_decimal_or_non_positive_numbers(
    conn: AsyncConnection, change: dict[str, Any]
) -> None:
    import dataclasses

    account = await make_account(conn)
    bot = await make_bot(conn, account)
    async with AsyncSession(bind=conn) as session:
        with pytest.raises(ValueError, match="Decimal"):
            await create_order(session, dataclasses.replace(_order(account, bot), **change), host_id="aa")
        with pytest.raises(ValueError, match="NewOrder"):
            await create_order(session, object(), host_id="aa")  # type: ignore[arg-type]


# --- Справжні коміти: write-ahead і конкурентний апдейт (окремі з'єднання) ---


@pytest_asyncio.fixture
async def engine(migrated_db: URL) -> AsyncIterator[AsyncEngine]:
    engine = make_engine(migrated_db)
    try:
        yield engine
    finally:
        await engine.dispose()


async def _committed_order(sessions: async_sessionmaker[AsyncSession], engine: AsyncEngine) -> OrderRef:
    async with engine.begin() as conn:
        account = await make_account(conn)
        bot = await make_bot(conn, account)
    async with sessions() as session:
        ref = await create_order(session, _order(account, bot), host_id="aa")
        await session.commit()
    return ref


async def test_submitting_is_committed_before_begin_submission_returns(engine: AsyncEngine) -> None:
    """Write-ahead (ТЗ §7.2): коли `begin_submission` повернувся, `SUBMITTING` уже видно іншому з'єднанню."""
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    ref = await _committed_order(sessions, engine)
    submitting = await begin_submission(sessions, ref)
    assert (submitting.state, submitting.version) == ("SUBMITTING", ref.version + 1)
    async with engine.connect() as observer:
        row = (
            await observer.execute(
                select(Order.state, Order.submitted_at_ms, Order.client_order_id).where(Order.id == ref.id)
            )
        ).one()
        events = await observer.scalar(
            select(func.count()).select_from(OrderEvent).where(OrderEvent.order_id == ref.id)
        )
    assert row.state == "SUBMITTING"
    assert row.submitted_at_ms is not None
    assert row.client_order_id == ref.client_order_id
    assert events == 2
    # Повторна відправка того самого ордера — конфлікт версії, і жодної зайвої події.
    with pytest.raises(OrderConflict):
        await begin_submission(sessions, ref)
    with pytest.raises(ValueError, match="clientOrderId"):
        await begin_submission(sessions, OrderRef(id=ref.id, client_order_id=None, state="CREATED", version=1))
    async with engine.connect() as observer:
        after = await observer.scalar(select(func.count()).select_from(OrderEvent).where(OrderEvent.order_id == ref.id))
    assert after == 2


async def _wait_until_blocked(observer: AsyncConnection, pid: int) -> None:
    for _ in range(_MAX_POLLS):
        # Статистика кешується на транзакцію спостерігача: без очищення 2000 опитувань — одна перевірка (рев'ю T3.1).
        await observer.execute(text("SELECT pg_stat_clear_snapshot()"))
        waiting = await observer.scalar(
            text("SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = :pid"), {"pid": pid}
        )
        if waiting:
            return
    pytest.fail(f"сесія {pid} так і не стала чекати на блокування")


async def test_two_writers_with_one_version_one_wins_the_other_conflicts(engine: AsyncEngine) -> None:
    """OCC: виконання (WS) і запит на скасування (LOCAL) з тієї самої версії — один перехід, одна подія."""
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    ref = await _committed_order(sessions, engine)
    ref = await begin_submission(sessions, ref)
    async with sessions() as session:
        ref = await transition_order(session, ref, "OPEN", source="REST", exchange_order_id=f"ex-{uuid.uuid4().hex}")
        await session.commit()
    async with sessions() as first, sessions() as second, engine.connect() as observer:
        filled = await transition_order(first, ref, "FILLED", source="WS")
        pid = (await second.execute(text("SELECT pg_backend_pid()"))).scalar_one()
        cancel = asyncio.create_task(transition_order(second, ref, "PENDING_CANCEL", source="LOCAL"))
        await _wait_until_blocked(observer, pid)
        await first.commit()
        with pytest.raises(OrderConflict) as raised:
            await cancel
        await second.rollback()
    assert (raised.value.state, raised.value.version) == ("FILLED", filled.version)
    async with engine.connect() as check:
        targets = (
            await check.execute(
                select(OrderEvent.to_state).where(OrderEvent.order_id == ref.id).order_by(OrderEvent.id)
            )
        ).scalars()
        assert list(targets) == ["CREATED", "SUBMITTING", "OPEN", "FILLED"]
