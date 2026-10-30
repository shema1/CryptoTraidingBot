"""Bybit: open interest, premium, mark/index і long/short на записаних відповідях (T1.9).

Мережі немає. Форми тіл — документація v5 і відповіді Testnet (2026-10-30): список від новіших,
`start`/`end` включні, курсор percent-encoded, незакритий інтервал входить у список.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

import pytest

from app.core.clock import FakeClock
from app.exchange.bybit import (
    OI_PAGE,
    PATH_ACCOUNT_RATIO,
    PATH_INDEX_KLINE,
    PATH_MARK_KLINE,
    PATH_OPEN_INTEREST,
    PATH_PREMIUM_KLINE,
    BybitRawResponse,
)
from app.exchange.errors import ExchangeUnavailable, MalformedResponse
from app.exchange.rate_limit import EndpointGroup, RequestPriority
from app.exchange.types import DerivativePriceType, Timeframe
from tests.exchange.test_bybit_public import STEP, SYMBOL, _ok, _open, _raw

HOUR = STEP


def _oi_row(ts: int, value: str = "10.5") -> dict[str, str]:
    return {"openInterest": value, "singleOpenInterest": "1", "timestamp": str(ts)}


def _oi_body(
    rows: list[dict[str, str]], *, cursor: str = "", time_ms: int, symbol: str = "BTCUSDT"
) -> dict[str, object]:
    return _ok(
        {"category": "linear", "symbol": symbol, "nextPageCursor": cursor, "list": rows},
        time_ms=time_ms,
    )


def _ratio_row(ts: int, buy: str = "0.6", sell: str = "0.4") -> dict[str, str]:
    return {"symbol": "BTCUSDT", "buyRatio": buy, "sellRatio": sell, "timestamp": str(ts)}


def _ratio_body(
    rows: list[dict[str, str]], *, cursor: str = "", time_ms: int, category: str | None = None
) -> dict[str, object]:
    result: dict[str, object] = {"symbol": "BTCUSDT", "nextPageCursor": cursor, "list": rows}
    if category is not None:
        result["category"] = category
    return _ok(result, time_ms=time_ms)


def _price_row(ts: int, prices: tuple[str, str, str, str]) -> list[str]:
    open_, high, low, close = prices
    return [str(ts), open_, high, low, close]


def _price_body(rows: list[list[str]], *, time_ms: int, symbol: str = "BTCUSDT") -> dict[str, object]:
    return _ok({"category": "linear", "symbol": symbol, "list": rows}, time_ms=time_ms)


def _closed_at(*opens: int) -> int:
    """Час біржі, коли кожна з цих свічок уже за запасом остаточності."""
    return max(opens) + HOUR + 500


@pytest.mark.asyncio
async def test_open_interest_is_ascending_closed_and_uses_data_priority(fake_clock: FakeClock) -> None:
    server = _closed_at(0, HOUR)
    forming = 2 * HOUR

    def handler(path: str, query: Mapping[str, str]) -> BybitRawResponse:
        assert path == PATH_OPEN_INTEREST
        assert query["category"] == "linear"
        assert query["symbol"] == "BTCUSDT"
        assert query["intervalTime"] == "1h"
        assert query["startTime"] == "0"
        assert query["endTime"] == str(3 * HOUR - 1)
        assert query["limit"] == str(OI_PAGE)
        rows = [_oi_row(forming, "3"), _oi_row(HOUR, "2"), _oi_row(0, "1")]
        return _raw(_oi_body(rows, time_ms=server))

    async with _open(fake_clock, handler) as (adapter, limiter, _client):
        points = await adapter.fetch_open_interest(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=3 * HOUR)
    assert [point.ts_ms for point in points] == [0, HOUR]
    assert points[0].open_interest == Decimal("1")
    assert type(points[0].open_interest) is Decimal
    assert limiter.priorities == [RequestPriority.DATA]
    assert limiter.groups == [EndpointGroup.MARKET]
    assert limiter.retries == [True]


@pytest.mark.asyncio
async def test_open_interest_follows_a_percent_encoded_cursor(
    fake_clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("app.exchange.bybit.OI_PAGE", 2)
    server = _closed_at(0, HOUR)
    encoded = "lastid%3D9%26lasttime%3D1"

    def handler(_path: str, query: Mapping[str, str]) -> BybitRawResponse:
        if query["startTime"] != "0":
            return _raw(_oi_body([], time_ms=server))
        if "cursor" not in query:
            return _raw(_oi_body([_oi_row(HOUR, "2"), _oi_row(HOUR, "2")], cursor=encoded, time_ms=server))
        assert query["cursor"] == "lastid=9&lasttime=1"
        return _raw(_oi_body([_oi_row(0, "1")], time_ms=server))

    async with _open(fake_clock, handler) as (adapter, _limiter, client):
        points = await adapter.fetch_open_interest(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=2 * HOUR)
    assert [(point.ts_ms, point.open_interest) for point in points] == [(0, Decimal("1")), (HOUR, Decimal("2"))]
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_repeated_cursor_is_a_malformed_response(fake_clock: FakeClock, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.exchange.bybit.OI_PAGE", 2)

    def handler(_path: str, query: Mapping[str, str]) -> BybitRawResponse:
        return _raw(_oi_body([_oi_row(HOUR), _oi_row(HOUR)], cursor="again", time_ms=_closed_at(HOUR)))

    async with _open(fake_clock, handler) as (adapter, _, _client):
        with pytest.raises(MalformedResponse, match="повторився"):
            await adapter.fetch_open_interest(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=2 * HOUR)


@pytest.mark.asyncio
async def test_cursor_page_cap_stops_a_cursor_that_never_ends(
    fake_clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("app.exchange.bybit.OI_PAGE", 1)
    pages = 0

    def handler(_path: str, _query: Mapping[str, str]) -> BybitRawResponse:
        nonlocal pages
        pages += 1
        return _raw(_oi_body([_oi_row(1, "1")], cursor=f"c{pages}", time_ms=10 * HOUR))

    async with _open(fake_clock, handler) as (adapter, _, _client):
        with pytest.raises(ExchangeUnavailable, match="сторінок"):
            await adapter.fetch_open_interest(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=HOUR)
    assert pages == 8


@pytest.mark.asyncio
async def test_empty_open_interest_window_is_an_empty_list(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, lambda _path, _query: _raw(_oi_body([], time_ms=HOUR))) as (adapter, _, client):
        assert await adapter.fetch_open_interest(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=HOUR) == []
    assert len(client.calls) == 1
    async with _open(fake_clock, lambda _path, _query: _raw(_oi_body([_oi_row(0)], time_ms=HOUR))) as (
        adapter,
        _,
        client,
    ):
        assert await adapter.fetch_open_interest(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=0) == []
    assert client.calls == []


@pytest.mark.asyncio
async def test_open_interest_rejects_a_bad_page_and_an_unsupported_interval(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, lambda _p, _q: _raw(_oi_body([_oi_row(0)], time_ms=HOUR, symbol="ETHUSDT"))) as (
        adapter,
        _,
        client,
    ):
        with pytest.raises(MalformedResponse, match="symbol"):
            await adapter.fetch_open_interest(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=HOUR)
    assert len(client.calls) == 1

    async with _open(fake_clock, lambda _p, _q: _raw(_oi_body([_oi_row(0, "nope")], time_ms=_closed_at(0)))) as (
        adapter,
        _,
        client,
    ):
        with pytest.raises(MalformedResponse, match="openInterest"):
            await adapter.fetch_open_interest(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=HOUR)

    client_calls: list[object] = []

    def handler(path: str, query: Mapping[str, str]) -> BybitRawResponse:
        client_calls.append((path, dict(query)))
        return _raw(_oi_body([], time_ms=0))

    async with _open(fake_clock, handler) as (adapter, _, _client):
        with pytest.raises(ValueError, match="1m"):
            await adapter.fetch_open_interest(symbol=SYMBOL, timeframe=Timeframe.M1, start_ms=0, end_ms=HOUR)
    assert client_calls == []


@pytest.mark.asyncio
async def test_conflicting_open_interest_at_one_timestamp_is_malformed(fake_clock: FakeClock) -> None:
    rows = [_oi_row(0, "2"), _oi_row(0, "1")]

    async with _open(fake_clock, lambda _p, _q: _raw(_oi_body(rows, time_ms=_closed_at(0)))) as (adapter, _, _c):
        with pytest.raises(MalformedResponse, match="суперечливі"):
            await adapter.fetch_open_interest(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=HOUR)


@pytest.mark.asyncio
async def test_premium_keeps_negatives_and_a_carried_open_but_skips_a_broken_range(fake_clock: FakeClock) -> None:
    good = _price_row(0, ("-0.0002", "-0.0001", "-0.0003", "-0.0002"))
    # Testnet BTCUSDT 2024-06-01 02:00: open — close попередньої години, вище за high цієї. Це не збій.
    carried = _price_row(HOUR, ("0.00059532", "0.0004741", "0.00044398", "0.00044398"))
    broken = _price_row(2 * HOUR, ("0.1", "0.0", "-0.1", "0.2"))  # close > high
    seen: dict[str, str] = {}

    def handler(path: str, query: Mapping[str, str]) -> BybitRawResponse:
        assert path == PATH_PREMIUM_KLINE
        seen.update(query)
        return _raw(_price_body([broken, carried, good], time_ms=_closed_at(0, HOUR, 2 * HOUR)))

    async with _open(fake_clock, handler) as (adapter, limiter, _client):
        points = await adapter.fetch_premium_index(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=3 * HOUR)
    assert [point.ts_ms for point in points] == [0, HOUR]
    assert points[0].high == Decimal("-0.0001")
    assert points[0].low == Decimal("-0.0003")
    assert (points[1].open, points[1].high, points[1].low, points[1].close) == (
        Decimal("0.00059532"),
        Decimal("0.0004741"),
        Decimal("0.00044398"),
        Decimal("0.00044398"),
    )
    assert seen["interval"] == "60"
    assert seen["start"] == "0"
    assert seen["end"] == str(3 * HOUR - 1)
    assert limiter.priorities == [RequestPriority.DATA]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "kind"),
    [
        ("fetch_mark_price_candles", PATH_MARK_KLINE, DerivativePriceType.MARK),
        ("fetch_index_price_candles", PATH_INDEX_KLINE, DerivativePriceType.INDEX),
    ],
)
async def test_mark_and_index_drop_a_non_positive_price(
    fake_clock: FakeClock, method: str, path: str, kind: DerivativePriceType
) -> None:
    good = _price_row(0, ("10", "12", "9", "11"))
    zero = _price_row(HOUR, ("0", "1", "0", "1"))

    def handler(seen_path: str, _query: Mapping[str, str]) -> BybitRawResponse:
        assert seen_path == path
        return _raw(_price_body([zero, good], time_ms=_closed_at(0, HOUR)))

    async with _open(fake_clock, handler) as (adapter, _, _client):
        points = await getattr(adapter, method)(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=2 * HOUR)
    assert [point.ts_ms for point in points] == [0]
    assert points[0].price_type is kind
    assert points[0].close == Decimal("11")


@pytest.mark.asyncio
async def test_long_short_uses_period_1h_and_tolerates_a_missing_category(fake_clock: FakeClock) -> None:
    def handler(path: str, query: Mapping[str, str]) -> BybitRawResponse:
        assert path == PATH_ACCOUNT_RATIO
        assert query["period"] == "1h"
        assert query["limit"] == "500"
        assert query["category"] == "linear"
        return _raw(_ratio_body([_ratio_row(0)], time_ms=_closed_at(0)))

    async with _open(fake_clock, handler) as (adapter, limiter, _client):
        points = await adapter.fetch_long_short_ratio(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=HOUR)
    assert points[0].buy_ratio == Decimal("0.6")
    assert points[0].sell_ratio == Decimal("0.4")
    assert limiter.priorities == [RequestPriority.DATA]

    async with _open(fake_clock, lambda _p, _q: _raw(_ratio_body([], time_ms=0, category="inverse"))) as (
        adapter,
        _,
        _c,
    ):
        with pytest.raises(MalformedResponse, match="linear"):
            await adapter.fetch_long_short_ratio(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=HOUR)

    async with _open(fake_clock, lambda _p, _q: _raw(_ok({"list": []}, time_ms=0))) as (adapter, _, _c):
        with pytest.raises(ValueError, match="інтервал"):
            await adapter.fetch_long_short_ratio(symbol=SYMBOL, timeframe=Timeframe.M1, start_ms=0, end_ms=HOUR)
