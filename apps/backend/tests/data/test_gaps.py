"""Прогалини (T1.5): дірки сітки без інтерполяції, межі вікна [start, end), запис у data_gaps."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any, cast

import pytest
from hypothesis import given
from hypothesis import strategies as st
from sqlalchemy import delete, event, func, insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, UOWTransaction
from structlog.testing import capture_logs

from app.core.clock import FakeClock
from app.core.config import H1_MS
from app.data.backfill import backfill_symbol, sync_market_data
from app.data.gaps import (
    CANDLE_SETTLE_MS,
    CANDLE_SOURCE,
    CLOCK_DRIFT_LIMIT_MS,
    GapRange,
    GapSync,
    detect_candle_gaps,
    find_gaps,
    has_open_gap,
    is_open_gap_conflict,
    open_gaps_in_window,
    plan_open_gaps,
    settled_end_ms,
    window_has_gap,
)
from app.data.models import OPEN_GAP_INDEX, DataGap
from app.data.models import Candle as StoredCandle
from app.exchange.bybit import KLINE_FINALITY_MARGIN_MS
from app.exchange.errors import ExchangeTimeout
from app.exchange.fake import Fault
from tests.exchange.factories import FAKE_NOW_MS, OTHER_SYMBOL, SYMBOL, hourly_candle, make_fake

HOUR = H1_MS
# Шість закритих слотів до свічки, що формується на FAKE_NOW_MS (межа кратна 1h).
GRID = FAKE_NOW_MS - 6 * HOUR


def _sessions(conn: AsyncConnection) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")


def _candle(open_ms: int, *, symbol: str = SYMBOL) -> dict[str, Any]:
    return {
        "exchange": "bybit",
        "symbol": symbol,
        "timeframe": "1h",
        "open_time_ms": open_ms,
        "open": Decimal("100"),
        "high": Decimal("110"),
        "low": Decimal("90"),
        "close": Decimal("105"),
        "volume": Decimal("1"),
        "is_closed": True,
    }


async def _store(conn: AsyncConnection, opens: list[int], *, symbol: str = SYMBOL) -> None:
    if opens:
        await conn.execute(insert(StoredCandle), [_candle(open_ms, symbol=symbol) for open_ms in opens])


async def _opens(conn: AsyncConnection) -> list[int]:
    rows = await conn.execute(select(StoredCandle.open_time_ms).order_by(StoredCandle.open_time_ms))
    return list(rows.scalars().all())


async def _open_gaps(conn: AsyncConnection, *, symbol: str = SYMBOL) -> list[tuple[int, int, int]]:
    """(id, from_ms, to_ms) відкритих прогалин свічок символу."""
    rows = await conn.execute(
        select(DataGap.id, DataGap.from_ms, DataGap.to_ms)
        .where(DataGap.source == CANDLE_SOURCE, DataGap.symbol == symbol, DataGap.resolved_at_ms.is_(None))
        .order_by(DataGap.from_ms)
    )
    return [(gap_id, from_ms, to_ms) for gap_id, from_ms, to_ms in rows.all()]


async def _detect(conn: AsyncConnection, *, start_ms: int = GRID, end_ms: int = FAKE_NOW_MS) -> GapSync:
    async with _sessions(conn)() as session, session.begin():
        return await detect_candle_gaps(session, exchange="bybit", symbol=SYMBOL, start_ms=start_ms, end_ms=end_ms)


async def _gap_times(conn: AsyncConnection, gap_id: int) -> tuple[int, int | None]:
    """(detected_at_ms, resolved_at_ms) рядка прогалини."""
    row = (await conn.execute(select(DataGap.detected_at_ms, DataGap.resolved_at_ms).where(DataGap.id == gap_id))).one()
    return row[0], row[1]


def _gap_events(logs: Sequence[Mapping[str, Any]]) -> list[tuple[str, str]]:
    return [(entry["event"], entry["log_level"]) for entry in logs if str(entry["event"]).startswith("data_gap")]


def _racing_sessions(conn: AsyncConnection, *, symbol: str, races: int) -> async_sessionmaker[AsyncSession]:
    """Сесії, де «інший процес» вставляє ту саму відкриту прогалину між читанням рядків і записом детектора.

    Вставка йде в тій самій транзакції, що й спроба, тож відкат спроби прибирає й її: тест перевіряє реакцію на
    зіткнення (`uq_data_gaps_open`), а не те, що лишилось від суперника.
    """
    left = [races]

    class RacingSession(Session):
        pass

    def before_flush(session: Session, _flush: UOWTransaction, _instances: object) -> None:
        if left[0] == 0:
            return
        for pending in session.new:
            if isinstance(pending, DataGap) and pending.symbol == symbol:
                left[0] -= 1
                session.connection().execute(
                    insert(DataGap).values(
                        source=pending.source,
                        exchange=pending.exchange,
                        symbol=pending.symbol,
                        timeframe=pending.timeframe,
                        from_ms=pending.from_ms,
                        to_ms=pending.to_ms,
                        detected_at_ms=pending.detected_at_ms,
                    )
                )
                return

    event.listen(RacingSession, "before_flush", before_flush)
    return async_sessionmaker(
        bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint", sync_session_class=RacingSession
    )


def test_find_gaps_merges_a_run_and_ignores_off_grid_and_outside_points() -> None:
    step = HOUR
    # Слот 0 є, слот H пропущено (мітка +1 мс його не закриває), слот 2H є. Мітка на end — уже поза сіткою.
    assert find_gaps([step * 2, 1, 0, step * 3], start_ms=0, end_ms=step * 3, step_ms=step) == (
        GapRange(step, step * 2),
    )
    # 0 — до start, step*3 — рівно end: жодна не закриває слот і не розширює сітку.
    assert find_gaps([0, step, step * 3], start_ms=step, end_ms=step * 3, step_ms=step) == (
        GapRange(step * 2, step * 3),
    )
    assert find_gaps([], start_ms=0, end_ms=0, step_ms=step) == ()
    assert find_gaps([], start_ms=0, end_ms=step * 2, step_ms=step) == (GapRange(0, step * 2),)


def test_find_gaps_rejects_a_broken_grid() -> None:
    with pytest.raises(ValueError, match="вирівнян"):
        find_gaps([], start_ms=1, end_ms=HOUR + 1, step_ms=HOUR)
    with pytest.raises(ValueError, match="раніше"):
        find_gaps([], start_ms=HOUR, end_ms=0, step_ms=HOUR)
    with pytest.raises(ValueError, match="step_ms"):
        find_gaps([], start_ms=0, end_ms=HOUR, step_ms=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="present_ms"):
        find_gaps([True, -1], start_ms=0, end_ms=HOUR, step_ms=HOUR)  # type: ignore[list-item]


def test_window_touching_a_gap_at_either_edge_is_not_an_intersection() -> None:
    gap = GapRange(HOUR, 2 * HOUR)
    assert window_has_gap([gap], start_ms=HOUR, end_ms=2 * HOUR) is True
    assert window_has_gap([gap], start_ms=0, end_ms=HOUR) is False
    assert window_has_gap([gap], start_ms=2 * HOUR, end_ms=3 * HOUR) is False
    assert window_has_gap([gap], start_ms=HOUR - 1, end_ms=HOUR + 1) is True
    assert window_has_gap([gap], start_ms=2 * HOUR - 1, end_ms=2 * HOUR + 1) is True
    assert window_has_gap([gap], start_ms=HOUR, end_ms=HOUR) is False
    assert window_has_gap([], start_ms=0, end_ms=HOUR) is False


def test_plan_keeps_the_outside_and_grows_or_splits_the_scanned_part() -> None:
    step = HOUR
    existing = (GapRange(0, 4 * step),)
    # Скан усередині дірки, дірка там лишилась — один інтервал, та сама from_ms.
    assert plan_open_gaps(existing, (GapRange(step, 3 * step),), scan_start_ms=step, scan_end_ms=3 * step) == existing
    # Середину докачано — дірка розпадається, хвіст поза сканом зберігається.
    assert plan_open_gaps(existing, (), scan_start_ms=step, scan_end_ms=3 * step) == (
        GapRange(0, step),
        GapRange(3 * step, 4 * step),
    )
    # Дірка виросла вперед за межу скану: зливається в один інтервал.
    assert plan_open_gaps(
        (GapRange(0, step),), (GapRange(step, 3 * step),), scan_start_ms=step, scan_end_ms=3 * step
    ) == (GapRange(0, 3 * step),)
    # Порожній скан нічого не закриває.
    assert plan_open_gaps(existing, (), scan_start_ms=step, scan_end_ms=step) == existing
    with pytest.raises(ValueError, match="поза межами"):
        plan_open_gaps((), (GapRange(0, step),), scan_start_ms=step, scan_end_ms=2 * step)


@given(
    n_slots=st.integers(min_value=0, max_value=48),
    missing=st.lists(st.integers(min_value=0, max_value=47), max_size=16, unique=True),
)
def test_gaps_are_exactly_the_missing_slots(n_slots: int, missing: list[int]) -> None:
    """Кожен слот вікна: пропуск на його межі входить у дірку, сусідній слот — ні. Точок не додається."""
    missing_set = {slot for slot in missing if slot < n_slots}
    present = [slot * HOUR for slot in range(n_slots) if slot not in missing_set]
    gaps = find_gaps(present, start_ms=0, end_ms=n_slots * HOUR, step_ms=HOUR)
    covered: list[int] = []
    for gap in gaps:
        covered.extend(range(gap.from_ms, gap.to_ms, HOUR))
    assert covered == [slot * HOUR for slot in range(n_slots) if slot in missing_set]
    for slot in range(n_slots):
        start = slot * HOUR
        assert window_has_gap(gaps, start_ms=start, end_ms=start + HOUR) is (slot in missing_set)
    assert window_has_gap(gaps, start_ms=n_slots * HOUR, end_ms=(n_slots + 1) * HOUR) is False


@pytest.mark.asyncio
async def test_unknown_series_is_rejected_before_the_database() -> None:
    session = cast(AsyncSession, None)
    with pytest.raises(ValueError, match="джерело"):
        await has_open_gap(
            session,
            source="mark_index",
            exchange="bybit",
            symbol=SYMBOL,
            timeframe="1h",
            start_ms=0,
            end_ms=HOUR,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_edge_slots_of_the_window_are_gaps_and_the_touching_window_is_not(conn: AsyncConnection) -> None:
    """Пропуск першого і останнього слота сітки. Вікно рівно між дірками їх не перетинає."""
    # Пропуски: GRID (початок), GRID+3H (середина), GRID+5H (останній слот перед FAKE_NOW).
    await _store(conn, [GRID + HOUR, GRID + 2 * HOUR, GRID + 4 * HOUR])
    before = await _opens(conn)

    with capture_logs() as logs:
        await _detect(conn)

    assert await _opens(conn) == before
    found = await _open_gaps(conn)
    assert [item[1:] for item in found] == [
        (GRID, GRID + HOUR),
        (GRID + 3 * HOUR, GRID + 4 * HOUR),
        (GRID + 5 * HOUR, FAKE_NOW_MS),
    ]
    assert [entry["event"] for entry in logs if entry["event"] == "data_gap"] == ["data_gap", "data_gap", "data_gap"]

    async with _sessions(conn)() as session, session.begin():
        assert (
            await has_open_gap(
                session,
                source=CANDLE_SOURCE,
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="1h",
                start_ms=GRID + HOUR,
                end_ms=GRID + 3 * HOUR,
            )
            is False
        )
        assert (
            await has_open_gap(
                session,
                source=CANDLE_SOURCE,
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="1h",
                start_ms=GRID + 4 * HOUR,
                end_ms=GRID + 5 * HOUR,
            )
            is False
        )
        assert (
            await has_open_gap(
                session,
                source=CANDLE_SOURCE,
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="1h",
                start_ms=GRID,
                end_ms=GRID + HOUR,
            )
            is True
        )
        assert (
            await has_open_gap(
                session,
                source=CANDLE_SOURCE,
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="1h",
                start_ms=GRID + 5 * HOUR,
                end_ms=FAKE_NOW_MS,
            )
            is True
        )
        # Остання мілісекунда першої дірки ще входить у вікно; перша мілісекунда після неї — ні.
        assert await open_gaps_in_window(
            session,
            source=CANDLE_SOURCE,
            exchange="bybit",
            symbol=SYMBOL,
            timeframe="1h",
            start_ms=GRID + HOUR - 1,
            end_ms=GRID + HOUR,
        ) == (GapRange(GRID, GRID + HOUR),)
        assert (
            await open_gaps_in_window(
                session,
                source=CANDLE_SOURCE,
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="1h",
                start_ms=GRID + HOUR,
                end_ms=GRID + HOUR + 1,
            )
            == ()
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_repeat_is_silent_and_a_filled_hole_can_reopen(conn: AsyncConnection, fake_clock: FakeClock) -> None:
    await _detect(conn, start_ms=GRID, end_ms=GRID + HOUR)
    [(gap_id, from_ms, to_ms)] = await _open_gaps(conn)
    assert (from_ms, to_ms) == (GRID, GRID + HOUR)

    with capture_logs() as logs:
        await _detect(conn, start_ms=GRID, end_ms=GRID + HOUR)
    assert await _open_gaps(conn) == [(gap_id, GRID, GRID + HOUR)]
    assert [entry["event"] for entry in logs if str(entry["event"]).startswith("data_gap")] == []

    await _detect(conn, start_ms=GRID, end_ms=GRID + 2 * HOUR)
    assert await _open_gaps(conn) == [(gap_id, GRID, GRID + 2 * HOUR)]

    await _store(conn, [GRID + HOUR])
    await _detect(conn, start_ms=GRID, end_ms=GRID + 2 * HOUR)
    assert await _open_gaps(conn) == [(gap_id, GRID, GRID + HOUR)]

    fake_clock.advance(1)
    await _store(conn, [GRID])
    await _detect(conn, start_ms=GRID, end_ms=GRID + 2 * HOUR)
    assert await _open_gaps(conn) == []
    resolved_at = (await conn.execute(select(DataGap.resolved_at_ms).where(DataGap.id == gap_id))).scalar_one()
    assert resolved_at == FAKE_NOW_MS + 1

    await conn.execute(delete(StoredCandle).where(StoredCandle.open_time_ms == GRID))
    await _detect(conn, start_ms=GRID, end_ms=GRID + 2 * HOUR)
    [(new_id, new_from, new_to)] = await _open_gaps(conn)
    assert new_id != gap_id
    assert (new_from, new_to) == (GRID, GRID + HOUR)
    total = (await conn.execute(select(func.count()).select_from(DataGap))).scalar_one()
    assert total == 2
    async with _sessions(conn)() as session, session.begin():
        assert (
            await has_open_gap(
                session,
                source=CANDLE_SOURCE,
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="1h",
                start_ms=GRID,
                end_ms=GRID + HOUR,
            )
            is True
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_scan_does_not_close_a_gap_outside_it_or_another_source(conn: AsyncConnection) -> None:
    await _store(conn, [GRID + 2 * HOUR, GRID + 3 * HOUR, GRID + 4 * HOUR, GRID + 5 * HOUR])
    await _detect(conn)
    assert [item[1:] for item in await _open_gaps(conn)] == [(GRID, GRID + 2 * HOUR)]

    await _detect(conn, start_ms=GRID + 2 * HOUR, end_ms=FAKE_NOW_MS)
    assert [item[1:] for item in await _open_gaps(conn)] == [(GRID, GRID + 2 * HOUR)]

    await conn.execute(
        insert(DataGap).values(
            source="open_interest",
            exchange="bybit",
            symbol=SYMBOL,
            timeframe="5min",
            from_ms=GRID,
            to_ms=GRID + HOUR,
            detected_at_ms=FAKE_NOW_MS,
        )
    )
    await _detect(conn)
    async with _sessions(conn)() as session, session.begin():
        assert (
            await has_open_gap(
                session,
                source="open_interest",
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="5min",
                start_ms=GRID,
                end_ms=GRID + HOUR,
            )
            is True
        )
        assert (
            await has_open_gap(
                session,
                source=CANDLE_SOURCE,
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="5min",
                start_ms=GRID,
                end_ms=GRID + HOUR,
            )
            is False
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_backfill_records_edge_holes_without_inventing_candles(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    """Біржа не віддала перший і останній слот. Backfill їх не добудовує, а детектор записує обидві дірки."""
    start = FAKE_NOW_MS - 4 * HOUR
    # Останній слот уже поза запасом остаточності: його відсутність — дірка, а не свічка, що ще не віддана.
    fake_clock.set_now(FAKE_NOW_MS + CANDLE_SETTLE_MS)
    fake = make_fake()
    fake.add_candles(
        [hourly_candle(start + HOUR, "100", symbol=SYMBOL), hourly_candle(start + 2 * HOUR, "110", symbol=SYMBOL)]
    )
    sessions = _sessions(conn)

    await backfill_symbol(sessions, fake, symbol=SYMBOL, start_ms=start)
    await backfill_symbol(sessions, fake, symbol=SYMBOL, start_ms=start)

    assert await _opens(conn) == [start + HOUR, start + 2 * HOUR]
    assert [item[1:] for item in await _open_gaps(conn)] == [(start, start + HOUR), (start + 3 * HOUR, FAKE_NOW_MS)]
    async with sessions() as session, session.begin():
        assert (
            await has_open_gap(
                session,
                source=CANDLE_SOURCE,
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="1h",
                start_ms=start + HOUR,
                end_ms=start + 3 * HOUR,
            )
            is False
        )
        assert (
            await has_open_gap(
                session,
                source=CANDLE_SOURCE,
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="1h",
                start_ms=start,
                end_ms=start + HOUR,
            )
            is True
        )
        assert (
            await has_open_gap(
                session,
                source=CANDLE_SOURCE,
                exchange="bybit",
                symbol=SYMBOL,
                timeframe="1h",
                start_ms=start + 3 * HOUR,
                end_ms=FAKE_NOW_MS,
            )
            is True
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_caught_up_series_still_records_a_hole_behind_the_cursor(conn: AsyncConnection) -> None:
    """Курсор backfill — max(час), тож дірку позаду він не качає. Прохід розкладу все одно її записує."""
    start = FAKE_NOW_MS - 4 * HOUR
    present = [start, start + HOUR, start + 3 * HOUR]
    await _store(conn, present)
    fake = make_fake()
    fake.add_candles([hourly_candle(open_ms, "100", symbol=SYMBOL) for open_ms in present])

    result = await sync_market_data(_sessions(conn), fake, symbols=(SYMBOL,), start_ms=start)
    assert result.inline.candles_inserted == 0
    assert await _opens(conn) == present
    assert [item[1:] for item in await _open_gaps(conn)] == [(start + 2 * HOUR, start + 3 * HOUR)]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_failed_candle_fetch_still_records_the_hole_already_in_the_database(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    start = FAKE_NOW_MS - 2 * HOUR
    fake_clock.set_now(FAKE_NOW_MS + CANDLE_SETTLE_MS)
    await _store(conn, [start])
    fake = make_fake()
    fake.fail_next("fetch_candles", Fault.timeout())

    with pytest.raises(ExchangeTimeout):
        await backfill_symbol(_sessions(conn), fake, symbol=SYMBOL, start_ms=start, funding=False)

    assert await _opens(conn) == [start]
    assert [item[1:] for item in await _open_gaps(conn)] == [(start + HOUR, FAKE_NOW_MS)]


def test_the_last_slot_is_checked_only_after_finality_margin_and_clock_drift() -> None:
    """Адаптер віддає свічку через `KLINE_FINALITY_MARGIN_MS` за часом біржі, годинник може відставати до 1 с."""
    assert CLOCK_DRIFT_LIMIT_MS == 1000
    assert CANDLE_SETTLE_MS == KLINE_FINALITY_MARGIN_MS + CLOCK_DRIFT_LIMIT_MS
    assert settled_end_ms(FAKE_NOW_MS) == FAKE_NOW_MS - HOUR
    assert settled_end_ms(FAKE_NOW_MS + CANDLE_SETTLE_MS - 1) == FAKE_NOW_MS - HOUR
    assert settled_end_ms(FAKE_NOW_MS + CANDLE_SETTLE_MS) == FAKE_NOW_MS
    assert settled_end_ms(FAKE_NOW_MS + HOUR - 1) == FAKE_NOW_MS
    assert settled_end_ms(0) == 0
    with pytest.raises(ValueError, match="current_ms"):
        settled_end_ms(-1)


def test_only_the_open_gap_unique_index_is_a_race() -> None:
    class _Cause(Exception):
        def __init__(self, constraint_name: str) -> None:
            super().__init__(constraint_name)
            self.constraint_name = constraint_name

    def integrity(sqlstate: str, constraint_name: str) -> IntegrityError:
        orig = Exception("db")
        orig.sqlstate = sqlstate  # type: ignore[attr-defined]
        orig.__cause__ = _Cause(constraint_name)
        return IntegrityError("INSERT INTO data_gaps", None, orig)

    assert is_open_gap_conflict(integrity("23505", OPEN_GAP_INDEX)) is True
    assert is_open_gap_conflict(integrity("23505", "pk_data_gaps")) is False
    assert is_open_gap_conflict(integrity("23514", OPEN_GAP_INDEX)) is False
    assert is_open_gap_conflict(IntegrityError("INSERT", None, Exception("db"))) is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_candle_inside_the_finality_margin_is_not_a_gap_yet(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    """Біржа на 300 мс позаду локального годинника: о межі + 200 мс остання свічка ще не віддана — це не дірка."""
    start = FAKE_NOW_MS - 3 * HOUR
    fake = make_fake(server_time_offset_ms=-300)
    fake.add_candles([hourly_candle(start + slot * HOUR, "100", symbol=SYMBOL) for slot in range(3)])
    sessions = _sessions(conn)

    fake_clock.set_now(FAKE_NOW_MS + 200)
    with capture_logs() as logs:
        await backfill_symbol(sessions, fake, symbol=SYMBOL, start_ms=start, funding=False)
    assert await _opens(conn) == [start, start + HOUR]
    assert await _open_gaps(conn) == []
    assert _gap_events(logs) == []

    fake_clock.set_now(FAKE_NOW_MS + CANDLE_SETTLE_MS)
    with capture_logs() as logs:
        await backfill_symbol(sessions, fake, symbol=SYMBOL, start_ms=start, funding=False)
    assert await _opens(conn) == [start, start + HOUR, start + 2 * HOUR]
    assert await _open_gaps(conn) == []
    assert _gap_events(logs) == []
    assert (await conn.execute(select(func.count()).select_from(DataGap))).scalar_one() == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_clock_step_back_still_resolves_a_filled_gap(conn: AsyncConnection, fake_clock: FakeClock) -> None:
    """Годинник відкотився (T1.6): закриття не раніше виявлення, CHECK не падає, прохід не зупиняється."""
    await _store(conn, [GRID, GRID + 2 * HOUR, GRID + 3 * HOUR, GRID + 4 * HOUR, GRID + 5 * HOUR])
    await _detect(conn)
    [(gap_id, _from, _to)] = await _open_gaps(conn)
    await _store(conn, [GRID + HOUR])

    fake_clock.set_now(FAKE_NOW_MS - 2_000)
    sync = await _detect(conn)

    assert sync.resolved == (GapRange(GRID + HOUR, GRID + 2 * HOUR),)
    assert await _open_gaps(conn) == []
    assert await _gap_times(conn, gap_id) == (FAKE_NOW_MS, FAKE_NOW_MS)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_gap_filled_from_its_start_keeps_its_row_and_is_not_new(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    """Воркер качає історію з початку: дірка коротшає зліва. Це той самий рядок і `info`, а не новий `data_gap`."""
    await _store(conn, [GRID + 3 * HOUR, GRID + 4 * HOUR, GRID + 5 * HOUR])
    await _detect(conn)
    [(gap_id, _from, _to)] = await _open_gaps(conn)

    fake_clock.advance(HOUR)
    await _store(conn, [GRID])
    with capture_logs() as logs:
        sync = await _detect(conn)

    assert await _open_gaps(conn) == [(gap_id, GRID + HOUR, GRID + 3 * HOUR)]
    assert await _gap_times(conn, gap_id) == (FAKE_NOW_MS, None)
    assert sync.opened == ()
    assert sync.shrunk == (GapRange(GRID + HOUR, GRID + 3 * HOUR),)
    assert _gap_events(logs) == [("data_gap_shrunk", "info")]
    [shrunk] = logs
    assert (shrunk["previous_from_ms"], shrunk["previous_to_ms"]) == (GRID, GRID + 3 * HOUR)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_gap_split_in_the_middle_keeps_its_detection_time(conn: AsyncConnection, fake_clock: FakeClock) -> None:
    await _store(conn, [GRID + 4 * HOUR, GRID + 5 * HOUR])
    await _detect(conn)
    [(gap_id, _from, _to)] = await _open_gaps(conn)

    fake_clock.advance(HOUR)
    await _store(conn, [GRID + 2 * HOUR])
    with capture_logs() as logs:
        sync = await _detect(conn)

    found = await _open_gaps(conn)
    assert [item[1:] for item in found] == [(GRID, GRID + 2 * HOUR), (GRID + 3 * HOUR, GRID + 4 * HOUR)]
    assert found[0][0] == gap_id
    assert [await _gap_times(conn, item[0]) for item in found] == [(FAKE_NOW_MS, None), (FAKE_NOW_MS, None)]
    assert sync.opened == ()
    assert sync.shrunk == (GapRange(GRID, GRID + 2 * HOUR), GapRange(GRID + 3 * HOUR, GRID + 4 * HOUR))
    assert _gap_events(logs) == [("data_gap_shrunk", "info"), ("data_gap_shrunk", "info")]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_lost_candle_between_two_gaps_joins_them_and_warns(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    """Зник слот між двома дірками — нові пропущені дані: `warning`; другий рядок закривається як злитий."""
    await _store(conn, [GRID + HOUR, GRID + 3 * HOUR, GRID + 4 * HOUR, GRID + 5 * HOUR])
    await _detect(conn)
    [(first_id, _f1, _t1), (second_id, _f2, _t2)] = await _open_gaps(conn)

    fake_clock.advance(HOUR)
    await conn.execute(delete(StoredCandle).where(StoredCandle.open_time_ms == GRID + HOUR))
    with capture_logs() as logs:
        sync = await _detect(conn)

    assert await _open_gaps(conn) == [(first_id, GRID, GRID + 3 * HOUR)]
    assert await _gap_times(conn, second_id) == (FAKE_NOW_MS, FAKE_NOW_MS + HOUR)
    assert sync.extended == (GapRange(GRID, GRID + 3 * HOUR),)
    assert sync.merged == (GapRange(GRID + 2 * HOUR, GRID + 3 * HOUR),)
    assert sync.resolved == ()
    assert _gap_events(logs) == [("data_gap_extended", "warning"), ("data_gap_merged", "info")]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_concurrent_writer_on_the_same_gap_is_retried(conn: AsyncConnection, fake_clock: FakeClock) -> None:
    start = FAKE_NOW_MS - 2 * HOUR
    fake_clock.set_now(FAKE_NOW_MS + CANDLE_SETTLE_MS)
    sessions = _racing_sessions(conn, symbol=SYMBOL, races=1)

    with capture_logs() as logs:
        await backfill_symbol(sessions, make_fake(), symbol=SYMBOL, start_ms=start, funding=False)

    assert [item[1:] for item in await _open_gaps(conn)] == [(start, FAKE_NOW_MS)]
    assert _gap_events(logs) == [("data_gap_sync_conflict", "warning"), ("data_gap", "warning")]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_persistent_conflict_skips_the_series_not_the_pass(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    """Обидві спроби зіткнулись: ряд щойно звів інший процес. Решта символів і funding докачуються далі."""
    start = FAKE_NOW_MS - 2 * HOUR
    fake_clock.set_now(FAKE_NOW_MS + CANDLE_SETTLE_MS)
    sessions = _racing_sessions(conn, symbol=SYMBOL, races=100)

    with capture_logs() as logs:
        result = await sync_market_data(sessions, make_fake(), symbols=(SYMBOL, OTHER_SYMBOL), start_ms=start)

    assert result.failed_symbols == ()
    assert await _open_gaps(conn) == []
    assert [item[1:] for item in await _open_gaps(conn, symbol=OTHER_SYMBOL)] == [(start, FAKE_NOW_MS)]
    conflicts = [entry for entry in logs if entry["event"] == "data_gap_sync_conflict"]
    assert [(entry["symbol"], entry["attempt"]) for entry in conflicts] == [(SYMBOL, 1), (SYMBOL, 2)]
