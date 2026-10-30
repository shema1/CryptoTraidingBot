"""Backfill деривативів 1h (T1.9): повтор не змінює рядки, дірка лишається діркою, довга історія — задача воркера."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import TypeVar

import pytest
from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from app.core.clock import FakeClock
from app.data.derivatives import (
    DERIVATIVES_JOB_PRIORITY,
    JOB_KIND_DERIVATIVES,
    DerivativesBackfillIncomplete,
    DerivativesStats,
    DerivativesSync,
    derivative_symbols,
    derivatives_job_params,
    parse_derivatives_job_params,
    run_derivatives_job,
    sync_derivatives,
)
from app.data.models import DataGap
from app.data.models import LongShortRatio as StoredRatio
from app.data.models import MarkIndexCandle as StoredMark
from app.data.models import OpenInterest as StoredOpenInterest
from app.data.models import PremiumIndex as StoredPremium
from app.exchange.errors import ExchangeTimeout
from app.exchange.models import Instrument as StoredInstrument
from app.exchange.types import (
    DerivativePriceType,
    LongShortRatio,
    MarkIndexCandle,
    OpenInterest,
    PremiumIndex,
    Timeframe,
)
from app.jobs.models import Job
from tests.exchange.factories import FAKE_NOW_MS, OTHER_SYMBOL, SYMBOL

HOUR = Timeframe.H1.duration_ms
_Row = TypeVar("_Row", OpenInterest, PremiumIndex, MarkIndexCandle, LongShortRatio)
START = FAKE_NOW_MS - 5 * HOUR


def _sessions(conn: AsyncConnection) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")


def _oi(ts: int, symbol: str = SYMBOL, value: str = "10") -> OpenInterest:
    return OpenInterest(symbol=symbol, timeframe=Timeframe.H1, ts_ms=ts, open_interest=Decimal(value))


def _premium(ts: int, symbol: str = SYMBOL, close: str = "-0.0001") -> PremiumIndex:
    price = Decimal(close)
    return PremiumIndex(symbol=symbol, timeframe=Timeframe.H1, ts_ms=ts, open=price, high=price, low=price, close=price)


def _mark(ts: int, kind: DerivativePriceType, symbol: str = SYMBOL, close: str = "100") -> MarkIndexCandle:
    price = Decimal(close)
    return MarkIndexCandle(
        symbol=symbol, timeframe=Timeframe.H1, price_type=kind, ts_ms=ts, open=price, high=price, low=price, close=price
    )


def _ratio(ts: int, symbol: str = SYMBOL, buy: str = "0.55") -> LongShortRatio:
    bought = Decimal(buy)
    return LongShortRatio(
        symbol=symbol, timeframe=Timeframe.H1, ts_ms=ts, buy_ratio=bought, sell_ratio=Decimal(1) - bought
    )


class Book:
    """Біржа без мережі: віддає точки діапазону або `ExchangeTimeout` для символу з `fail`."""

    def __init__(self) -> None:
        self.oi: list[OpenInterest] = []
        self.premium: list[PremiumIndex] = []
        self.mark: list[MarkIndexCandle] = []
        self.index: list[MarkIndexCandle] = []
        self.ratio: list[LongShortRatio] = []
        self.fail: set[str] = set()
        self.calls: list[str] = []

    async def fetch_open_interest(
        self, *, symbol: str, timeframe: Timeframe, start_ms: int, end_ms: int | None = None, limit: int | None = None
    ) -> list[OpenInterest]:
        return self._take("open_interest", symbol, start_ms, end_ms, self.oi)

    async def fetch_premium_index(
        self, *, symbol: str, timeframe: Timeframe, start_ms: int, end_ms: int | None = None, limit: int | None = None
    ) -> list[PremiumIndex]:
        return self._take("premium_index", symbol, start_ms, end_ms, self.premium)

    async def fetch_mark_price_candles(
        self, *, symbol: str, timeframe: Timeframe, start_ms: int, end_ms: int | None = None, limit: int | None = None
    ) -> list[MarkIndexCandle]:
        return self._take("mark_candles", symbol, start_ms, end_ms, self.mark)

    async def fetch_index_price_candles(
        self, *, symbol: str, timeframe: Timeframe, start_ms: int, end_ms: int | None = None, limit: int | None = None
    ) -> list[MarkIndexCandle]:
        return self._take("index_candles", symbol, start_ms, end_ms, self.index)

    async def fetch_long_short_ratio(
        self, *, symbol: str, timeframe: Timeframe, start_ms: int, end_ms: int | None = None, limit: int | None = None
    ) -> list[LongShortRatio]:
        return self._take("long_short", symbol, start_ms, end_ms, self.ratio)

    def _take(self, name: str, symbol: str, start_ms: int, end_ms: int | None, rows: Sequence[_Row]) -> list[_Row]:
        del name
        self.calls.append(symbol)
        if symbol in self.fail:
            raise ExchangeTimeout(symbol)
        end = 10**18 if end_ms is None else end_ms
        return [row for row in rows if row.symbol == symbol and start_ms <= row.ts_ms < end]


def _fill(book: Book, times: list[int], symbol: str, *, oi: str = "10") -> None:
    book.oi.extend(_oi(ts, symbol, oi) for ts in times)
    book.premium.extend(_premium(ts, symbol) for ts in times)
    book.mark.extend(_mark(ts, DerivativePriceType.MARK, symbol) for ts in times)
    book.index.extend(_mark(ts, DerivativePriceType.INDEX, symbol, "90") for ts in times)
    book.ratio.extend(_ratio(ts, symbol) for ts in times)


async def _counts(conn: AsyncConnection) -> dict[str, int]:
    tables = {
        "open_interest": StoredOpenInterest,
        "premium_index": StoredPremium,
        "mark_index_candles": StoredMark,
        "long_short_ratio": StoredRatio,
    }
    found: dict[str, int] = {}
    for name, table in tables.items():
        found[name] = (await conn.execute(select(func.count()).select_from(table))).scalar_one()
    return found


async def _open_gaps(conn: AsyncConnection, source: str) -> list[tuple[str, int, int]]:
    rows = await conn.execute(
        select(DataGap.symbol, DataGap.from_ms, DataGap.to_ms)
        .where(DataGap.source == source, DataGap.resolved_at_ms.is_(None))
        .order_by(DataGap.symbol, DataGap.from_ms)
    )
    return [(symbol, start, end) for symbol, start, end in rows.all()]


def test_job_params_reject_anything_but_1h_without_duplicates() -> None:
    params = derivatives_job_params((SYMBOL,), START)
    assert parse_derivatives_job_params(params) == ((SYMBOL,), START)
    assert derivative_symbols((OTHER_SYMBOL, SYMBOL, OTHER_SYMBOL)) == (OTHER_SYMBOL, SYMBOL)
    with pytest.raises(ValueError, match="1h"):
        parse_derivatives_job_params({**params, "interval": "15m"})
    with pytest.raises(ValueError, match="дублікати"):
        parse_derivatives_job_params({**params, "symbols": [SYMBOL, SYMBOL]})


@pytest.mark.integration
@pytest.mark.asyncio
async def test_repeat_does_not_change_rows_and_a_hole_stays_one_gap(conn: AsyncConnection) -> None:
    present = [FAKE_NOW_MS - 5 * HOUR, FAKE_NOW_MS - 4 * HOUR, FAKE_NOW_MS - 2 * HOUR]
    hole = FAKE_NOW_MS - 3 * HOUR
    book = Book()
    _fill(book, [*present, FAKE_NOW_MS], SYMBOL)
    sessions = _sessions(conn)

    first = await sync_derivatives(sessions, book, symbols=(SYMBOL,), start_ms=START)
    assert first.job_id is None
    assert first.inline == DerivativesStats(
        open_interest=3, premium_index=3, mark_candles=3, index_candles=3, long_short=3
    )
    stored = await _counts(conn)
    assert stored == {"open_interest": 3, "premium_index": 3, "mark_index_candles": 6, "long_short_ratio": 3}
    assert await _open_gaps(conn, "open_interest") == [(SYMBOL, hole, hole + HOUR)]
    premium = (await conn.execute(select(StoredPremium.close).where(StoredPremium.ts_ms == present[0]))).scalar_one()
    assert premium == Decimal("-0.0001")
    # Година, що ще формується за локальним годинником, у таблицю не потрапляє.
    assert (
        await conn.execute(
            select(func.count()).select_from(StoredOpenInterest).where(StoredOpenInterest.ts_ms == FAKE_NOW_MS)
        )
    ).scalar_one() == 0

    for point in book.oi:
        if point.ts_ms == present[0]:
            book.oi[book.oi.index(point)] = _oi(present[0], SYMBOL, "1")
    second = await sync_derivatives(sessions, book, symbols=(SYMBOL,), start_ms=START)
    assert second.inline == DerivativesStats()
    assert await _counts(conn) == stored
    kept = (
        await conn.execute(select(StoredOpenInterest.open_interest).where(StoredOpenInterest.ts_ms == present[0]))
    ).scalar_one()
    assert kept == Decimal("10")
    assert await _open_gaps(conn, "open_interest") == [(SYMBOL, hole, hole + HOUR)]
    assert await _open_gaps(conn, "premium_index") == [(SYMBOL, hole, hole + HOUR)]
    assert await _open_gaps(conn, "mark_candles") == [(SYMBOL, hole, hole + HOUR)]
    assert await _open_gaps(conn, "index_candles") == [(SYMBOL, hole, hole + HOUR)]
    assert await _open_gaps(conn, "long_short") == [(SYMBOL, hole, hole + HOUR)]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_premium_with_the_previous_close_as_open_is_stored_without_a_gap(conn: AsyncConnection) -> None:
    times = [FAKE_NOW_MS - 3 * HOUR, FAKE_NOW_MS - 2 * HOUR, FAKE_NOW_MS - HOUR]
    book = Book()
    _fill(book, times, SYMBOL)
    # Як на Testnet: open — close попередньої години, тож лежить поза [low, high] цієї години.
    book.premium = [
        PremiumIndex(
            symbol=SYMBOL,
            timeframe=Timeframe.H1,
            ts_ms=ts,
            open=Decimal("0.0006"),
            high=Decimal("0.0005"),
            low=Decimal("0.0004"),
            close=Decimal("0.0004"),
        )
        for ts in times
    ]

    result = await sync_derivatives(_sessions(conn), book, symbols=(SYMBOL,), start_ms=times[0])

    assert result.inline.premium_index == 3
    stored = (await conn.execute(select(StoredPremium.ts_ms, StoredPremium.open).order_by(StoredPremium.ts_ms))).all()
    assert [(ts, value) for ts, value in stored] == [(ts, Decimal("0.0006")) for ts in times]
    assert await _open_gaps(conn, "premium_index") == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hours_before_listing_are_not_a_gap(conn: AsyncConnection) -> None:
    launch = FAKE_NOW_MS - 4 * HOUR
    await conn.execute(
        insert(StoredInstrument).values(
            exchange="bybit",
            mode="demo",
            symbol=SYMBOL,
            tick_size=Decimal("0.1"),
            qty_step=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=Decimal("100"),
            launch_time_ms=launch,
            status="Trading",
            updated_at_ms=FAKE_NOW_MS,
        )
    )
    book = Book()
    _fill(book, [launch, launch + HOUR, launch + 2 * HOUR], SYMBOL)
    await sync_derivatives(_sessions(conn), book, symbols=(SYMBOL,), start_ms=launch - 8 * HOUR)
    assert await _open_gaps(conn, "open_interest") == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_long_history_is_one_job_and_the_second_run_inserts_nothing(conn: AsyncConnection) -> None:
    floor = FAKE_NOW_MS - 400 * HOUR
    book = Book()
    _fill(book, [floor, floor + HOUR, FAKE_NOW_MS - 2 * HOUR], SYMBOL)
    sessions = _sessions(conn)

    planned = await sync_derivatives(sessions, book, symbols=(SYMBOL,), start_ms=floor)
    assert planned.inline == DerivativesStats()
    assert planned.job_symbols == (SYMBOL,)
    assert book.calls == []
    assert await _counts(conn) == {
        "open_interest": 0,
        "premium_index": 0,
        "mark_index_candles": 0,
        "long_short_ratio": 0,
    }
    jobs = (await conn.execute(select(Job.kind, Job.status, Job.priority, Job.params, Job.dedup_key))).all()
    assert len(jobs) == 1
    assert jobs[0].kind == JOB_KIND_DERIVATIVES
    assert jobs[0].status == "QUEUED"
    assert jobs[0].priority == DERIVATIVES_JOB_PRIORITY
    assert jobs[0].dedup_key == "backfill_derivatives"
    assert jobs[0].params == {"symbols": [SYMBOL], "interval": "1h", "start_ms": floor}

    again = await sync_derivatives(sessions, book, symbols=(SYMBOL,), start_ms=floor)
    assert again.job_id == planned.job_id

    loaded = await run_derivatives_job(sessions, book, jobs[0].params)
    assert loaded.open_interest == 3
    assert (await _counts(conn))["open_interest"] == 3
    book.oi = [_oi(floor, SYMBOL, "1") if point.ts_ms == floor else point for point in book.oi]
    reloaded = await run_derivatives_job(sessions, book, jobs[0].params)
    assert reloaded == DerivativesStats()
    kept = (
        await conn.execute(select(StoredOpenInterest.open_interest).where(StoredOpenInterest.ts_ms == floor))
    ).scalar_one()
    assert kept == Decimal("10")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_one_symbol_timeout_does_not_stop_the_other(conn: AsyncConnection) -> None:
    book = Book()
    book.fail.add(SYMBOL)
    _fill(book, [FAKE_NOW_MS - 2 * HOUR], OTHER_SYMBOL)
    result = await sync_derivatives(
        _sessions(conn), book, symbols=(SYMBOL, OTHER_SYMBOL), start_ms=FAKE_NOW_MS - 2 * HOUR
    )
    assert result.failed_symbols == (SYMBOL,)
    assert result.inline.open_interest == 1
    assert (await _counts(conn))["open_interest"] == 1


@pytest.mark.asyncio
async def test_a_failed_derivatives_pass_waits_out_the_backoff(fake_clock: FakeClock) -> None:
    def broken_db() -> AsyncSession:
        raise ConnectionError("БД недоступна")

    syncer = DerivativesSync(
        broken_db,  # type: ignore[arg-type]
        Book(),
        symbols=(SYMBOL,),
        start_ms=START,
        interval_ms=60_000,
        jitter=lambda _low, high: high,
    )
    with pytest.raises(ConnectionError):
        await syncer.run_if_due()
    assert await syncer.run_if_due() is None
    fake_clock.advance(29_999)
    assert await syncer.run_if_due() is None
    fake_clock.advance(1)
    with pytest.raises(ConnectionError):
        await syncer.run_if_due()


def test_incomplete_carries_the_rows_already_stored() -> None:
    stats = DerivativesStats(open_interest=2)
    exc = DerivativesBackfillIncomplete((SYMBOL,), stats)
    assert exc.failed_symbols == (SYMBOL,)
    assert exc.stats.open_interest == 2
