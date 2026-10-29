"""Backfill свічок 1h і funding (T1.4): лише закриті, повтор не змінює рядки, довга історія — задача воркера."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from itertools import pairwise
from typing import cast

import pytest
from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from app.core.clock import FakeClock
from app.core.config import DEFAULT_BACKFILL_START_MS, H1_MS, Settings
from app.data.backfill import (
    FUNDING_WINDOW_MS,
    SYNC_RETRY_BASE_MS,
    BackfillIncomplete,
    BackfillStats,
    MarketDataSync,
    SessionFactory,
    backfill_is_due,
    backfill_job_params,
    backfill_symbol,
    backfill_symbols,
    candles_closed_at,
    forming_open_ms,
    funding_known_at,
    parse_backfill_job_params,
    run_backfill_job,
    sync_market_data,
)
from app.data.models import Candle as StoredCandle
from app.data.models import FundingRate as StoredFunding
from app.exchange.errors import ExchangeTimeout
from app.exchange.fake import FakeExchangeAdapter, Fault
from app.exchange.types import Candle, FundingRate, Timeframe
from app.jobs.models import Job
from tests.exchange.factories import FAKE_NOW_MS, OTHER_SYMBOL, SYMBOL, hourly_candle, make_fake
from tests.exchange.factories import H1_MS as HOUR_MS

START = FAKE_NOW_MS - 5 * HOUR_MS
# Фейк не знає цього інструмента й відповідає `RequestRejected` — як біржа на делістингований символ.
UNKNOWN_SYMBOL = "SOL/USDT:USDT"


def _settings(**kwargs: object) -> Settings:
    return Settings(_env_file=None, **kwargs)  # type: ignore[arg-type]


def _sessions(conn: AsyncConnection) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")


async def _candle_opens(conn: AsyncConnection) -> list[tuple[str, int]]:
    rows = await conn.execute(
        select(StoredCandle.symbol, StoredCandle.open_time_ms).order_by(StoredCandle.symbol, StoredCandle.open_time_ms)
    )
    return [(symbol, open_ms) for symbol, open_ms in rows.all()]


async def _closes(conn: AsyncConnection, symbol: str) -> dict[int, Decimal]:
    rows = await conn.execute(
        select(StoredCandle.open_time_ms, StoredCandle.close).where(StoredCandle.symbol == symbol)
    )
    found: dict[int, Decimal] = {}
    for open_ms, close in rows.all():
        found[open_ms] = close
    return found


async def _funding(conn: AsyncConnection) -> list[tuple[str, int, Decimal]]:
    rows = await conn.execute(
        select(StoredFunding.symbol, StoredFunding.funding_time_ms, StoredFunding.rate).order_by(
            StoredFunding.symbol, StoredFunding.funding_time_ms
        )
    )
    return [(symbol, ts, rate) for symbol, ts, rate in rows.all()]


async def _funding_count(conn: AsyncConnection) -> int:
    return (await conn.execute(select(func.count()).select_from(StoredFunding))).scalar_one()


async def _store_recent(conn: AsyncConnection, symbol: str, *, candle_open_ms: int = FAKE_NOW_MS - HOUR_MS) -> None:
    """Свіжий ряд символу в БД: свічка `candle_open_ms` і funding на `FAKE_NOW_MS` (за замовчуванням — наздогнав)."""
    await conn.execute(
        insert(StoredCandle).values(
            exchange="bybit",
            symbol=symbol,
            timeframe="1h",
            open_time_ms=candle_open_ms,
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("90"),
            close=Decimal("105"),
            volume=Decimal("1"),
            is_closed=True,
        )
    )
    await conn.execute(
        insert(StoredFunding).values(
            exchange="bybit", symbol=symbol, funding_time_ms=FAKE_NOW_MS, rate=Decimal("0.0001")
        )
    )


def _hourly_funding(first_ms: int, count: int) -> list[FundingRate]:
    return [
        FundingRate(symbol=SYMBOL, funding_time_ms=first_ms + i * HOUR_MS, rate=Decimal("0.0001")) for i in range(count)
    ]


class _Flaky:
    """Фейкова біржа, що падає `ExchangeTimeout` там, де `fails(method, symbol, n)` (n — номер виклику методу з 1)."""

    def __init__(self, fake: FakeExchangeAdapter, fails: Callable[[str, str, int], bool]) -> None:
        self._fake = fake
        self._fails = fails
        self._counts: dict[str, int] = {}

    def _check(self, method: str, symbol: str) -> None:
        self._counts[method] = self._counts.get(method, 0) + 1
        if self._fails(method, symbol, self._counts[method]):
            raise ExchangeTimeout(f"{method} {symbol}")

    async def fetch_candles(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[Candle]:
        self._check("fetch_candles", symbol)
        return await self._fake.fetch_candles(
            symbol=symbol, timeframe=timeframe, start_ms=start_ms, end_ms=end_ms, limit=limit
        )

    async def fetch_funding_history(
        self, *, symbol: str, start_ms: int, end_ms: int | None = None
    ) -> list[FundingRate]:
        self._check("fetch_funding_history", symbol)
        return await self._fake.fetch_funding_history(symbol=symbol, start_ms=start_ms, end_ms=end_ms)


def test_h1_constant_matches_timeframe() -> None:
    assert H1_MS == Timeframe.H1.duration_ms == HOUR_MS


def test_forming_open_is_the_candle_still_in_progress() -> None:
    assert forming_open_ms(FAKE_NOW_MS) == FAKE_NOW_MS
    assert forming_open_ms(FAKE_NOW_MS + 1) == FAKE_NOW_MS
    assert forming_open_ms(FAKE_NOW_MS + HOUR_MS - 1) == FAKE_NOW_MS
    assert forming_open_ms(FAKE_NOW_MS + HOUR_MS) == FAKE_NOW_MS + HOUR_MS


def test_only_candles_closed_on_the_local_clock_are_kept() -> None:
    closed = hourly_candle(FAKE_NOW_MS - HOUR_MS, "100")
    forming = hourly_candle(FAKE_NOW_MS, "110")
    assert candles_closed_at([forming, closed], now=FAKE_NOW_MS) == [closed]
    assert candles_closed_at([closed], now=closed.close_time_ms) == [closed]
    assert candles_closed_at([closed], now=closed.close_time_ms - 1) == []


def test_future_funding_is_not_known_yet() -> None:
    past = FundingRate(symbol=SYMBOL, funding_time_ms=FAKE_NOW_MS - HOUR_MS, rate=Decimal("-0.0001"))
    now = FundingRate(symbol=SYMBOL, funding_time_ms=FAKE_NOW_MS, rate=Decimal("0.0001"))
    future = FundingRate(symbol=SYMBOL, funding_time_ms=FAKE_NOW_MS + 1, rate=Decimal("0.01"))
    assert funding_known_at([future, past, now], now=FAKE_NOW_MS, start_ms=FAKE_NOW_MS - HOUR_MS) == [past, now]


def test_backfill_symbols_put_trading_symbol_first_without_duplicates() -> None:
    assert backfill_symbols(trading_symbol=SYMBOL, research_symbols=(OTHER_SYMBOL, SYMBOL)) == (SYMBOL, OTHER_SYMBOL)
    assert backfill_symbols(trading_symbol="DOGE/USDT:USDT", research_symbols=(SYMBOL, OTHER_SYMBOL)) == (
        "DOGE/USDT:USDT",
        SYMBOL,
        OTHER_SYMBOL,
    )


def test_backfill_is_due_on_startup_and_after_the_interval() -> None:
    assert backfill_is_due(last_run_ms=None, now=FAKE_NOW_MS, interval_ms=HOUR_MS) is True
    assert backfill_is_due(last_run_ms=FAKE_NOW_MS, now=FAKE_NOW_MS, interval_ms=HOUR_MS) is False
    assert backfill_is_due(last_run_ms=FAKE_NOW_MS, now=FAKE_NOW_MS + HOUR_MS - 1, interval_ms=HOUR_MS) is False
    assert backfill_is_due(last_run_ms=FAKE_NOW_MS, now=FAKE_NOW_MS + HOUR_MS, interval_ms=HOUR_MS) is True
    # Стрибок годинника назад не запускає докачування вдруге.
    assert backfill_is_due(last_run_ms=FAKE_NOW_MS, now=FAKE_NOW_MS - 1, interval_ms=HOUR_MS) is False


@pytest.mark.parametrize(
    ("params", "match"),
    [
        ({"symbols": [SYMBOL], "timeframe": "1h"}, "очікуються"),
        ({"symbols": [SYMBOL], "timeframe": "1h", "start_ms": FAKE_NOW_MS, "extra": 1}, "очікуються"),
        ({"symbols": [SYMBOL], "timeframe": "15m", "start_ms": FAKE_NOW_MS}, "1h"),
        ({"symbols": [SYMBOL, SYMBOL], "timeframe": "1h", "start_ms": FAKE_NOW_MS}, "дублікати"),
        ({"symbols": ["btc/usdt:usdt"], "timeframe": "1h", "start_ms": FAKE_NOW_MS}, "канонічному"),
        ({"symbols": [SYMBOL], "timeframe": "1h", "start_ms": FAKE_NOW_MS + 1}, "1h"),
        ({"symbols": [], "timeframe": "1h", "start_ms": FAKE_NOW_MS}, "непорожнім"),
    ],
)
def test_job_params_reject_anything_except_hourly_canonical_symbols(params: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        parse_backfill_job_params(params)


def test_job_params_round_trip() -> None:
    assert parse_backfill_job_params({"symbols": [SYMBOL], "timeframe": "1h", "start_ms": FAKE_NOW_MS}) == (
        (SYMBOL,),
        FAKE_NOW_MS,
    )


def test_from_settings_uses_the_configured_universe_and_interval() -> None:
    settings = _settings()
    syncer = MarketDataSync.from_settings(settings, cast(SessionFactory, None), make_fake())
    assert syncer._symbols[0] == "BTC/USDT:USDT"
    assert syncer._symbols == backfill_symbols(
        trading_symbol=settings.trading_symbol, research_symbols=settings.research_symbols
    )
    assert len(syncer._symbols) == 8
    assert syncer._start_ms == DEFAULT_BACKFILL_START_MS
    assert syncer._interval_ms == settings.backfill_interval_s * 1000


@pytest.mark.integration
@pytest.mark.asyncio
async def test_forming_candle_stays_out_until_the_local_hour_closes(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    """Біржа вже вважає свічку закритою (її годинник попереду), локальний — ще ні. У БД її немає."""
    fake = make_fake(server_time_offset_ms=HOUR_MS)
    fake.add_candles(
        [
            hourly_candle(FAKE_NOW_MS - 2 * HOUR_MS, "100", symbol=SYMBOL),
            hourly_candle(FAKE_NOW_MS - HOUR_MS, "110", symbol=SYMBOL),
            hourly_candle(FAKE_NOW_MS, "120", symbol=SYMBOL),
        ]
    )
    sessions = _sessions(conn)

    async def run() -> None:
        await backfill_symbol(sessions, fake, symbol=SYMBOL, start_ms=FAKE_NOW_MS - 2 * HOUR_MS)

    await run()
    await run()
    assert await _candle_opens(conn) == [(SYMBOL, FAKE_NOW_MS - 2 * HOUR_MS), (SYMBOL, FAKE_NOW_MS - HOUR_MS)]

    fake_clock.advance(30 * 60_000)
    await run()
    assert await _candle_opens(conn) == [(SYMBOL, FAKE_NOW_MS - 2 * HOUR_MS), (SYMBOL, FAKE_NOW_MS - HOUR_MS)]

    fake_clock.advance(30 * 60_000)
    await run()
    assert await _candle_opens(conn) == [
        (SYMBOL, FAKE_NOW_MS - 2 * HOUR_MS),
        (SYMBOL, FAKE_NOW_MS - HOUR_MS),
        (SYMBOL, FAKE_NOW_MS),
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_repeated_backfill_does_not_change_rows_or_fill_gaps(conn: AsyncConnection) -> None:
    fake = make_fake()
    present = [FAKE_NOW_MS - 5 * HOUR_MS, FAKE_NOW_MS - 4 * HOUR_MS, FAKE_NOW_MS - 2 * HOUR_MS]
    fake.add_candles([hourly_candle(open_ms, "200", symbol=SYMBOL) for open_ms in present])
    fake.add_candles([hourly_candle(open_ms, "300", symbol=OTHER_SYMBOL) for open_ms in present[:2]])
    sessions = _sessions(conn)

    first = await sync_market_data(sessions, fake, symbols=(SYMBOL, OTHER_SYMBOL), start_ms=START)
    assert first.job_id is None
    assert await _candle_opens(conn) == [
        (SYMBOL, present[0]),
        (SYMBOL, present[1]),
        (SYMBOL, present[2]),
        (OTHER_SYMBOL, present[0]),
        (OTHER_SYMBOL, present[1]),
    ]
    before = await _closes(conn, SYMBOL)
    fake.add_candles([hourly_candle(present[0], "999", symbol=SYMBOL)])

    second = await sync_market_data(sessions, fake, symbols=(SYMBOL, OTHER_SYMBOL), start_ms=START)
    assert second.inline == BackfillStats()
    assert await _closes(conn, SYMBOL) == before
    assert await _candle_opens(conn) == [
        (SYMBOL, present[0]),
        (SYMBOL, present[1]),
        (SYMBOL, present[2]),
        (OTHER_SYMBOL, present[0]),
        (OTHER_SYMBOL, present[1]),
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_backfill_pages_and_keeps_a_committed_page_when_the_next_fetch_fails(conn: AsyncConnection) -> None:
    fake = make_fake()
    opens = [FAKE_NOW_MS - hour * HOUR_MS for hour in range(5, 0, -1)]
    fake.add_candles([hourly_candle(open_ms, "40") for open_ms in opens])
    sessions = _sessions(conn)

    flaky = _Flaky(fake, lambda method, _symbol, n: method == "fetch_candles" and n == 2)
    with pytest.raises(ExchangeTimeout):
        await backfill_symbol(sessions, flaky, symbol=SYMBOL, start_ms=START, fetch_limit=2)
    assert [open_ms for _, open_ms in await _candle_opens(conn)] == opens[:2]

    await backfill_symbol(sessions, fake, symbol=SYMBOL, start_ms=START, fetch_limit=2)
    assert [open_ms for _, open_ms in await _candle_opens(conn)] == opens
    # Одна успішна сторінка до збою і дві сторінки добору решти (2+1).
    assert len(fake.calls_to("fetch_candles")) == 3

    again = await backfill_symbol(sessions, fake, symbol=SYMBOL, start_ms=START, fetch_limit=2)
    assert again.candles_inserted == 0
    assert [open_ms for _, open_ms in await _candle_opens(conn)] == opens


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetch_error_inserts_nothing(conn: AsyncConnection) -> None:
    fake = make_fake()
    fake.add_candles([hourly_candle(FAKE_NOW_MS - HOUR_MS, "50")])
    fake.fail_next("fetch_candles", Fault.timeout())
    with pytest.raises(ExchangeTimeout):
        await backfill_symbol(_sessions(conn), fake, symbol=SYMBOL, start_ms=FAKE_NOW_MS - HOUR_MS)
    assert await _candle_opens(conn) == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_funding_is_stored_once_and_future_rates_are_skipped(conn: AsyncConnection) -> None:
    fake = make_fake()
    past = FundingRate(symbol=SYMBOL, funding_time_ms=FAKE_NOW_MS - 8 * HOUR_MS, rate=Decimal("0.0001"))
    at_now = FundingRate(symbol=SYMBOL, funding_time_ms=FAKE_NOW_MS, rate=Decimal("-0.0003"))
    future = FundingRate(symbol=SYMBOL, funding_time_ms=FAKE_NOW_MS + 8 * HOUR_MS, rate=Decimal("0.01"))
    other = FundingRate(symbol=OTHER_SYMBOL, funding_time_ms=FAKE_NOW_MS, rate=Decimal("0.0002"))
    fake.add_funding_rates([past, at_now, future, other])
    sessions = _sessions(conn)
    start = FAKE_NOW_MS - 8 * HOUR_MS

    first = await sync_market_data(sessions, fake, symbols=(SYMBOL, OTHER_SYMBOL), start_ms=start)
    assert first.job_id is None
    stored_funding = [
        (SYMBOL, past.funding_time_ms, Decimal("0.0001")),
        (SYMBOL, FAKE_NOW_MS, Decimal("-0.0003")),
        (OTHER_SYMBOL, FAKE_NOW_MS, Decimal("0.0002")),
    ]
    assert await _funding(conn) == stored_funding
    fake.add_funding_rates([FundingRate(symbol=SYMBOL, funding_time_ms=past.funding_time_ms, rate=Decimal("0.5"))])
    second = await sync_market_data(sessions, fake, symbols=(SYMBOL, OTHER_SYMBOL), start_ms=start)
    assert second.inline.funding_inserted == 0
    assert await _funding(conn) == stored_funding


@pytest.mark.integration
@pytest.mark.asyncio
async def test_long_history_is_one_worker_job_and_a_second_run_is_a_no_op(conn: AsyncConnection) -> None:
    floor = FAKE_NOW_MS - 1000 * HOUR_MS
    fake = make_fake()
    early = [floor, floor + HOUR_MS]
    recent = FAKE_NOW_MS - HOUR_MS
    fake.add_candles([hourly_candle(open_ms, "60") for open_ms in (*early, recent)])
    sessions = _sessions(conn)

    planned = await sync_market_data(sessions, fake, symbols=(SYMBOL,), start_ms=floor, inline_max_bars=168)
    assert planned.inline == BackfillStats()
    assert planned.job_symbols == (SYMBOL,)
    assert fake.calls_to("fetch_candles") == []
    assert fake.calls_to("fetch_funding_history") == []
    assert await _candle_opens(conn) == []

    again = await sync_market_data(sessions, fake, symbols=(SYMBOL,), start_ms=floor, inline_max_bars=168)
    assert again.job_id == planned.job_id
    jobs = (await conn.execute(select(Job.id, Job.kind, Job.status, Job.params))).all()
    assert len(jobs) == 1
    assert jobs[0].kind == "backfill_candles"
    assert jobs[0].status == "QUEUED"
    assert jobs[0].params == {"symbols": [SYMBOL], "timeframe": "1h", "start_ms": floor}

    loaded = await run_backfill_job(sessions, fake, jobs[0].params)
    assert loaded.candles_inserted == 3
    assert [open_ms for _, open_ms in await _candle_opens(conn)] == [*early, recent]
    fake.add_candles([hourly_candle(early[0], "999")])
    reloaded = await run_backfill_job(sessions, fake, jobs[0].params)
    assert reloaded.candles_inserted == 0
    assert (await _closes(conn, SYMBOL))[early[0]] == Decimal("60")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_caught_up_symbol_is_not_queued_with_one_that_needs_history(conn: AsyncConnection) -> None:
    floor = FAKE_NOW_MS - 1000 * HOUR_MS
    await _store_recent(conn, SYMBOL)
    fake = make_fake()
    fake.add_candles([hourly_candle(floor, "70", symbol=OTHER_SYMBOL)])
    result = await sync_market_data(
        _sessions(conn), fake, symbols=(SYMBOL, OTHER_SYMBOL), start_ms=floor, inline_max_bars=168
    )
    assert result.job_symbols == (OTHER_SYMBOL,)
    assert result.inline == BackfillStats()
    assert fake.calls == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_schedule_runs_at_startup_then_waits_and_picks_up_the_closed_hour(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    fake = make_fake()
    fake.add_candles(
        [
            hourly_candle(FAKE_NOW_MS - HOUR_MS, "80"),
            hourly_candle(FAKE_NOW_MS, "81"),
        ]
    )
    syncer = MarketDataSync(
        _sessions(conn),
        fake,
        symbols=(SYMBOL,),
        start_ms=FAKE_NOW_MS - HOUR_MS,
        interval_ms=HOUR_MS,
    )
    first = await syncer.run_if_due()
    assert first is not None
    assert [open_ms for _, open_ms in await _candle_opens(conn)] == [FAKE_NOW_MS - HOUR_MS]

    fake_clock.advance(HOUR_MS - 1)
    assert await syncer.run_if_due() is None
    assert [open_ms for _, open_ms in await _candle_opens(conn)] == [FAKE_NOW_MS - HOUR_MS]

    fake_clock.advance(1)
    second = await syncer.run_if_due()
    assert second is not None
    assert [open_ms for _, open_ms in await _candle_opens(conn)] == [FAKE_NOW_MS - HOUR_MS, FAKE_NOW_MS]


# --- Виправлення після рев'ю 2026-10-29 ---


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dense_funding_history_is_fetched_and_committed_by_windows(conn: AsyncConnection) -> None:
    """Понад 8191 запис funding за прохід (asyncpg: ≤ 32767 параметрів на запит) не валить backfill."""
    total = 9000
    first = FAKE_NOW_MS - (total - 1) * HOUR_MS
    fake = make_fake()
    fake.add_funding_rates(_hourly_funding(first, total))
    sessions = _sessions(conn)

    stats = await backfill_symbol(sessions, fake, symbol=SYMBOL, start_ms=first, candles=False)
    assert stats.funding_inserted == total
    assert await _funding_count(conn) == total
    windows = [
        (cast(int, call.args["start_ms"]), cast(int, call.args["end_ms"]))
        for call in fake.calls_to("fetch_funding_history")
    ]
    assert windows[0][0] == first
    assert windows[-1][1] == FAKE_NOW_MS + 1
    assert all(end - start <= FUNDING_WINDOW_MS for start, end in windows)
    # Вікна йдуть впритул: між ними немає непрочитаних мілісекунд.
    assert all(left[1] == right[0] for left, right in pairwise(windows))

    again = await backfill_symbol(sessions, fake, symbol=SYMBOL, start_ms=first, candles=False)
    assert again.funding_inserted == 0
    assert await _funding_count(conn) == total


@pytest.mark.integration
@pytest.mark.asyncio
async def test_one_large_funding_window_is_inserted_in_chunks(conn: AsyncConnection) -> None:
    total = 9000
    first = FAKE_NOW_MS - (total - 1) * HOUR_MS
    fake = make_fake()
    fake.add_funding_rates(_hourly_funding(first, total))

    stats = await backfill_symbol(
        _sessions(conn), fake, symbol=SYMBOL, start_ms=first, candles=False, funding_window_ms=(total + 1) * HOUR_MS
    )
    assert len(fake.calls_to("fetch_funding_history")) == 1
    assert stats.funding_inserted == total
    assert await _funding_count(conn) == total


@pytest.mark.integration
@pytest.mark.asyncio
async def test_funding_failure_keeps_the_windows_committed_before_it(conn: AsyncConnection) -> None:
    eight_hours = 8 * HOUR_MS
    first = FAKE_NOW_MS - 6 * 24 * HOUR_MS
    times = list(range(first, FAKE_NOW_MS + 1, eight_hours))
    fake = make_fake()
    fake.add_funding_rates(FundingRate(symbol=SYMBOL, funding_time_ms=ts, rate=Decimal("0.0001")) for ts in times)
    sessions = _sessions(conn)
    window = 2 * 24 * HOUR_MS

    flaky = _Flaky(fake, lambda method, _symbol, n: method == "fetch_funding_history" and n == 2)
    with pytest.raises(ExchangeTimeout):
        await backfill_symbol(sessions, flaky, symbol=SYMBOL, start_ms=first, candles=False, funding_window_ms=window)
    assert [ts for _, ts, _ in await _funding(conn)] == [ts for ts in times if ts < first + window]

    await backfill_symbol(sessions, fake, symbol=SYMBOL, start_ms=first, candles=False, funding_window_ms=window)
    assert [ts for _, ts, _ in await _funding(conn)] == times


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_symbol_the_exchange_rejects_does_not_stop_the_others(conn: AsyncConnection) -> None:
    fake = make_fake()
    fake.add_candles([hourly_candle(FAKE_NOW_MS - HOUR_MS, "90", symbol=OTHER_SYMBOL)])

    result = await sync_market_data(
        _sessions(conn), fake, symbols=(UNKNOWN_SYMBOL, OTHER_SYMBOL), start_ms=FAKE_NOW_MS - HOUR_MS
    )
    assert result.failed_symbols == (UNKNOWN_SYMBOL,)
    assert result.inline.candles_inserted == 1
    assert await _candle_opens(conn) == [(OTHER_SYMBOL, FAKE_NOW_MS - HOUR_MS)]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_job_is_queued_even_when_an_inline_symbol_fails(conn: AsyncConnection) -> None:
    floor = FAKE_NOW_MS - 1000 * HOUR_MS
    # Свічка й funding майже свіжі → ряд докачується в процесі і падає; OTHER без історії → задача воркера.
    await _store_recent(conn, UNKNOWN_SYMBOL, candle_open_ms=FAKE_NOW_MS - 3 * HOUR_MS)
    fake = make_fake()

    result = await sync_market_data(
        _sessions(conn), fake, symbols=(UNKNOWN_SYMBOL, OTHER_SYMBOL), start_ms=floor, inline_max_bars=168
    )
    assert result.failed_symbols == (UNKNOWN_SYMBOL,)
    assert result.job_symbols == (OTHER_SYMBOL,)
    assert result.job_id is not None
    jobs = (await conn.execute(select(Job.id, Job.status, Job.params))).all()
    assert [(job.id, job.status, job.params["symbols"]) for job in jobs] == [(result.job_id, "QUEUED", [OTHER_SYMBOL])]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_job_loads_the_other_symbols_then_reports_the_failed_one(conn: AsyncConnection) -> None:
    fake = make_fake()
    fake.add_candles([hourly_candle(FAKE_NOW_MS - HOUR_MS, "95", symbol=OTHER_SYMBOL)])
    params = backfill_job_params((UNKNOWN_SYMBOL, OTHER_SYMBOL), FAKE_NOW_MS - HOUR_MS)

    with pytest.raises(BackfillIncomplete, match="SOL/USDT:USDT") as caught:
        await run_backfill_job(_sessions(conn), fake, params)
    assert caught.value.failed_symbols == (UNKNOWN_SYMBOL,)
    assert caught.value.stats.candles_inserted == 1
    assert await _candle_opens(conn) == [(OTHER_SYMBOL, FAKE_NOW_MS - HOUR_MS)]


@pytest.mark.asyncio
async def test_a_failed_pass_is_retried_after_a_growing_backoff_not_on_every_call(fake_clock: FakeClock) -> None:
    jitter_ranges: list[tuple[int, int]] = []

    def jitter(low: int, high: int) -> int:
        jitter_ranges.append((low, high))
        return high

    def broken_db() -> AsyncSession:
        raise ConnectionError("БД недоступна")

    base = SYNC_RETRY_BASE_MS
    syncer = MarketDataSync(
        cast(SessionFactory, broken_db),
        make_fake(),
        symbols=(SYMBOL,),
        start_ms=START,
        interval_ms=4 * base,
        jitter=jitter,
    )
    with pytest.raises(ConnectionError):
        await syncer.run_if_due()
    # Повтор — не на кожному виклику, а після паузи, що росте до інтервалу розкладу.
    for delay in (base, 2 * base, 4 * base, 4 * base):
        assert await syncer.run_if_due() is None
        fake_clock.advance(delay - 1)
        assert await syncer.run_if_due() is None
        fake_clock.advance(1)
        with pytest.raises(ConnectionError):
            await syncer.run_if_due()
    assert jitter_ranges == [
        (base // 2, base),
        (base, 2 * base),
        (2 * base, 4 * base),
        (2 * base, 4 * base),
        (2 * base, 4 * base),
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_incomplete_pass_is_retried_early_and_a_clean_pass_restores_the_interval(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    fake = make_fake()
    fake.add_candles([hourly_candle(FAKE_NOW_MS - HOUR_MS, "85")])
    fake.fail_next("fetch_candles", Fault.timeout())
    syncer = MarketDataSync(
        _sessions(conn),
        fake,
        symbols=(SYMBOL,),
        start_ms=FAKE_NOW_MS - HOUR_MS,
        interval_ms=HOUR_MS,
        jitter=lambda _low, high: high,
    )
    first = await syncer.run_if_due()
    assert first is not None
    assert first.failed_symbols == (SYMBOL,)
    assert await _candle_opens(conn) == []

    fake_clock.advance(SYNC_RETRY_BASE_MS - 1)
    assert await syncer.run_if_due() is None
    fake_clock.advance(1)
    retried = await syncer.run_if_due()
    assert retried is not None
    assert retried.failed_symbols == ()
    assert await _candle_opens(conn) == [(SYMBOL, FAKE_NOW_MS - HOUR_MS)]

    fake_clock.advance(HOUR_MS - 1)
    assert await syncer.run_if_due() is None
    fake_clock.advance(1)
    assert await syncer.run_if_due() is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_schedule_follows_the_monotonic_clock_not_wall_jumps(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    syncer = MarketDataSync(
        _sessions(conn), make_fake(), symbols=(SYMBOL,), start_ms=FAKE_NOW_MS - HOUR_MS, interval_ms=HOUR_MS
    )
    assert await syncer.run_if_due() is not None
    # Системний годинник стрибнув уперед: інтервал за монотонним ще не минув.
    fake_clock.jump_wall(2 * HOUR_MS)
    assert await syncer.run_if_due() is None
    # І назад (NTP виправив годинник, що поспішав): розклад не зупиняється на добу.
    fake_clock.jump_wall(-26 * HOUR_MS)
    fake_clock.advance(HOUR_MS)
    assert await syncer.run_if_due() is not None
