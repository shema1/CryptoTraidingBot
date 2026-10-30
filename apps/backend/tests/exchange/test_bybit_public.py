"""Bybit: публічні дані та `fetch_time` на записаних відповідях (T1.3).

Мережі немає. Форми тіл — з документації v5 і відповідей Testnet (2026-10-29): ціни рядками,
kline і funding від новіших до старіших, `start`/`end` включні.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import TypeVar

import pytest

from app.core.clock import FakeClock
from app.exchange.bybit import (
    FUNDING_PAGE,
    KLINE_FINALITY_MARGIN_MS,
    KLINE_PAGE,
    PATH_INSTRUMENTS,
    PATH_KLINE,
    PATH_TIME,
    BybitAdapter,
    BybitPublicClient,
    BybitRawResponse,
    ExchangeMode,
)
from app.exchange.errors import (
    AdapterConfigError,
    ExchangeOutcomeUnknown,
    ExchangeTimeout,
    ExchangeUnavailable,
    MalformedResponse,
)
from app.exchange.rate_limit import (
    EndpointGroup,
    Jitter,
    LimitSnapshot,
    ProcessRole,
    RateLimitConfig,
    RateLimiter,
    RequestPriority,
    Sleep,
)
from app.exchange.types import Timeframe

SERVER = 10 * Timeframe.H1.duration_ms
STEP = Timeframe.H1.duration_ms
SYMBOL = "BTC/USDT:USDT"

# Інтервали Bybit v5 для наших таймфреймів (документація /v5/market/kline).
_INTERVALS: dict[Timeframe, str] = {
    Timeframe.M1: "1",
    Timeframe.M5: "5",
    Timeframe.M15: "15",
    Timeframe.M30: "30",
    Timeframe.H1: "60",
    Timeframe.H2: "120",
    Timeframe.H4: "240",
    Timeframe.H6: "360",
    Timeframe.H12: "720",
    Timeframe.D1: "D",
}


def _sleep_for(clock: FakeClock) -> Sleep:
    async def sleep(ms: int) -> None:
        if ms > 0:
            clock.advance(ms)
        await asyncio.sleep(0)

    return sleep


def _jitter(low: int, _high: int) -> int:
    return low


_JITTER: Jitter = _jitter
_R = TypeVar("_R")


class SpyLimiter(RateLimiter):
    """Журнал пріоритету й заголовків поверх справжньої черги (час зрушує `FakeClock`)."""

    def __init__(self, clock: FakeClock) -> None:
        super().__init__(
            RateLimitConfig(
                role=ProcessRole.COLLECTOR,
                share_pct=50,
                ip_requests=10_000,
                backoff_base_ms=1,
                backoff_cap_ms=10,
                ip_ban_ms=10,
            ),
            sleeper=_sleep_for(clock),
            jitter=_JITTER,
        )
        self.priorities: list[RequestPriority] = []
        self.groups: list[EndpointGroup] = []
        self.retries: list[bool] = []
        self.observed: list[tuple[EndpointGroup, LimitSnapshot]] = []

    async def call(
        self,
        *,
        priority: RequestPriority,
        group: EndpointGroup,
        operation: Callable[[], Awaitable[_R]],
        retry: bool,
        weight: int = 1,
    ) -> _R:
        self.priorities.append(priority)
        self.groups.append(group)
        self.retries.append(retry)
        return await super().call(priority=priority, group=group, operation=operation, retry=retry, weight=weight)

    def observe(self, group: EndpointGroup, snapshot: LimitSnapshot) -> None:
        self.observed.append((group, snapshot))
        super().observe(group, snapshot)


class ScriptedClient:
    """Клієнт без мережі: відповідь залежить від шляху й query."""

    def __init__(self, handler: Callable[[str, Mapping[str, str]], BybitRawResponse]) -> None:
        self._handler = handler
        self.calls: list[tuple[str, dict[str, str]]] = []
        self.public_origin = "https://api-testnet.example"
        self.closed = False

    async def get(self, path: str, query: Mapping[str, str]) -> BybitRawResponse:
        recorded = dict(query)
        self.calls.append((path, recorded))
        return self._handler(path, recorded)

    async def aclose(self) -> None:
        self.closed = True


def _raw(body: object, *, status: int = 200, headers: dict[str, str] | None = None) -> BybitRawResponse:
    return BybitRawResponse(status=status, body=body, headers={} if headers is None else headers)


def _ok(result: dict[str, object], *, time_ms: int = SERVER) -> dict[str, object]:
    return {"retCode": 0, "retMsg": "OK", "result": result, "retExtInfo": {}, "time": time_ms}


def _time_body(*, time_ms: int = SERVER, nano_ms: int | None = None) -> dict[str, object]:
    point = time_ms if nano_ms is None else nano_ms
    return _ok({"timeSecond": str(time_ms // 1000), "timeNano": str(point * 1_000_000)}, time_ms=time_ms)


def _kline_row(open_ms: int, *, close: str = "11") -> list[str]:
    return [str(open_ms), "10", "12", "9", close, "1.5", "100"]


def _kline_body(opens: list[int], *, time_ms: int = SERVER, symbol: str = "BTCUSDT") -> dict[str, object]:
    return _ok(
        {"category": "linear", "symbol": symbol, "list": [_kline_row(open_ms) for open_ms in opens]},
        time_ms=time_ms,
    )


def _perp(
    *,
    symbol: str = "BTCUSDT",
    base: str = "BTC",
    quote: str = "USDT",
    settle: str = "USDT",
    contract_type: str = "LinearPerpetual",
    tick: str = "0.10",
    max_order: str = "1190.000",
    max_market: str = "500.000",
    status: str = "Trading",
    launch: str | None = "1585123200000",
) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": symbol,
        "contractType": contract_type,
        "status": status,
        "baseCoin": base,
        "quoteCoin": quote,
        "settleCoin": settle,
        "priceFilter": {"tickSize": tick, "minPrice": "0.10", "maxPrice": "1999999.80"},
        "lotSizeFilter": {
            "maxOrderQty": max_order,
            "maxMktOrderQty": max_market,
            "minOrderQty": "0.001",
            "qtyStep": "0.001",
            "minNotionalValue": "5",
        },
        "leverageFilter": {"minLeverage": "1", "maxLeverage": "150.00", "leverageStep": "0.01"},
    }
    if launch is not None:
        row["launchTime"] = launch
    return row


def _instruments(rows: list[dict[str, object]], *, cursor: str = "") -> dict[str, object]:
    return _ok({"category": "linear", "nextPageCursor": cursor, "list": rows})


def _fund(ts: int, rate: str = "0.0001") -> dict[str, str]:
    return {"symbol": "BTCUSDT", "fundingRate": rate, "fundingRateTimestamp": str(ts)}


def _funding_body(rows: list[dict[str, str]]) -> dict[str, object]:
    return _ok({"category": "linear", "list": rows})


def _only(body: dict[str, object]) -> Callable[[str, Mapping[str, str]], BybitRawResponse]:
    def handler(_path: str, _query: Mapping[str, str]) -> BybitRawResponse:
        return _raw(body)

    return handler


@asynccontextmanager
async def _open(
    clock: FakeClock,
    handler: Callable[[str, Mapping[str, str]], BybitRawResponse],
    *,
    read_attempts: int = 3,
) -> AsyncIterator[tuple[BybitAdapter, SpyLimiter, ScriptedClient]]:
    client = ScriptedClient(handler)
    limiter = SpyLimiter(clock)
    adapter = BybitAdapter(
        mode=ExchangeMode.TESTNET,
        limiter=limiter,
        client=client,
        sleeper=_sleep_for(clock),
        jitter=_JITTER,
        read_attempts=read_attempts,
    )
    try:
        yield adapter, limiter, client
    finally:
        await adapter.aclose()
        await limiter.aclose()


def test_non_linear_category_refuses_before_any_request(fake_clock: FakeClock) -> None:
    client = ScriptedClient(_only(_time_body()))
    with pytest.raises(AdapterConfigError):
        BybitAdapter(mode=ExchangeMode.TESTNET, limiter=SpyLimiter(fake_clock), category="spot", client=client)
    assert client.calls == []


def test_mode_must_be_enum(fake_clock: FakeClock) -> None:
    with pytest.raises(AdapterConfigError):
        BybitAdapter(mode="testnet", limiter=SpyLimiter(fake_clock), client=ScriptedClient(_only(_time_body())))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_fetch_time_uses_envelope_time_checked_against_time_nano(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(_time_body())) as (adapter, limiter, client):
        assert await adapter.fetch_time() == SERVER
    assert client.calls == [(PATH_TIME, {})]
    assert limiter.priorities == [RequestPriority.RECONCILE]
    assert limiter.groups == [EndpointGroup.MARKET]
    assert limiter.retries == [True]


@pytest.mark.asyncio
async def test_fetch_time_accepts_string_ret_code_and_one_ms_skew(fake_clock: FakeClock) -> None:
    body = _time_body(time_ms=SERVER, nano_ms=SERVER + 1)
    body["retCode"] = "0"

    async with _open(fake_clock, _only(body)) as (adapter, _, _client):
        assert await adapter.fetch_time() == SERVER


@pytest.mark.asyncio
async def test_fetch_time_rejects_time_nano_skew_over_one_ms(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(_time_body(time_ms=SERVER, nano_ms=SERVER + 2))) as (adapter, _, client):
        with pytest.raises(MalformedResponse):
            await adapter.fetch_time()
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_candles_are_closed_ascending_and_do_not_fill_gaps(fake_clock: FakeClock) -> None:
    # Найновіша в тілі — та, що ще формується (close > time відповіді). Пропуск STEP не заповнюється.
    opens = [3 * STEP, 2 * STEP, 0]
    seen: dict[str, str] = {}

    def handler(path: str, query: Mapping[str, str]) -> BybitRawResponse:
        assert path == PATH_KLINE
        seen.update(query)
        return _raw(_kline_body(opens, time_ms=3 * STEP + KLINE_FINALITY_MARGIN_MS))

    async with _open(fake_clock, handler) as (adapter, limiter, _client):
        candles = await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=4 * STEP)
    assert [c.open_time_ms for c in candles] == [0, 2 * STEP]
    assert candles[0].close == Decimal("11")
    assert candles[0].volume == Decimal("1.5")
    assert type(candles[0].open) is Decimal
    assert seen["category"] == "linear"
    assert seen["symbol"] == "BTCUSDT"
    assert seen["interval"] == "60"
    assert seen["start"] == "0"
    assert seen["end"] == str(4 * STEP - 1)
    assert limiter.priorities == [RequestPriority.DATA]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("after_close_ms", "returned"), [(KLINE_FINALITY_MARGIN_MS - 1, False), (KLINE_FINALITY_MARGIN_MS, True)]
)
async def test_just_closed_candle_waits_for_the_finality_margin(
    fake_clock: FakeClock, after_close_ms: int, returned: bool
) -> None:
    """Свічка, закрита менше ніж `KLINE_FINALITY_MARGIN_MS` тому за часом біржі, ще не остаточна і не віддається."""

    def handler(_path: str, _query: Mapping[str, str]) -> BybitRawResponse:
        return _raw(_kline_body([STEP, 0], time_ms=2 * STEP + after_close_ms))

    async with _open(fake_clock, handler) as (adapter, _, _client):
        candles = await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=2 * STEP)
    assert [c.open_time_ms for c in candles] == ([0, STEP] if returned else [0])


def test_finality_margin_is_the_owner_decision() -> None:
    """500 мс — рішення власника (T1.3, 2026-10-29): бюджет ТЗ §5.5 (ціль < 1 с, `max_signal_age_ms` 5 с)."""
    assert KLINE_FINALITY_MARGIN_MS == 500


@pytest.mark.asyncio
async def test_candle_end_is_exclusive_and_limit_keeps_the_earliest(fake_clock: FakeClock) -> None:
    def handler(_path: str, _query: Mapping[str, str]) -> BybitRawResponse:
        return _raw(_kline_body([2 * STEP, STEP, 0]))

    async with _open(fake_clock, handler) as (adapter, _, _client):
        bounded = await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=2 * STEP)
        limited = await adapter.fetch_candles(
            symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=3 * STEP, limit=1
        )
    assert [c.open_time_ms for c in bounded] == [0, STEP]
    assert [c.open_time_ms for c in limited] == [0]


@pytest.mark.asyncio
async def test_open_end_uses_fetch_time_and_not_the_local_clock(fake_clock: FakeClock) -> None:
    def handler(path: str, query: Mapping[str, str]) -> BybitRawResponse:
        if path == PATH_TIME:
            return _raw(_time_body(time_ms=2 * STEP))
        assert query["end"] == str(2 * STEP - 1)
        return _raw(_kline_body([STEP, 0], time_ms=2 * STEP + KLINE_FINALITY_MARGIN_MS))

    async with _open(fake_clock, handler) as (adapter, limiter, _client):
        candles = await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0)
    assert [c.open_time_ms for c in candles] == [0, STEP]
    assert limiter.priorities == [RequestPriority.RECONCILE, RequestPriority.DATA]


@pytest.mark.asyncio
async def test_empty_candle_range_does_not_call_the_exchange(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(_kline_body([0]))) as (adapter, _, client):
        assert await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=STEP, end_ms=STEP) == []
    assert client.calls == []


@pytest.mark.asyncio
async def test_kline_pages_walk_forward_and_a_truncated_page_is_split(fake_clock: FakeClock) -> None:
    page = KLINE_PAGE * STEP

    def handler(_path: str, query: Mapping[str, str]) -> BybitRawResponse:
        start = int(query["start"])
        end = int(query["end"]) + 1
        if start == 0 and end == page:
            # Повна сторінка, але один рядок поза вікном: біржа обрізала найстаріші.
            opens = [i * STEP for i in range(1, KLINE_PAGE)]
            opens.append(page)
            return _raw(_kline_body(opens, time_ms=page + 2 * STEP))
        if start == 0 and end == page // 2:
            return _raw(_kline_body([0], time_ms=page))
        if start == page // 2 and end == page:
            return _raw(_kline_body([page // 2], time_ms=page))
        if start == page:
            return _raw(_kline_body([page], time_ms=page + 2 * STEP))
        raise AssertionError(query)

    async with _open(fake_clock, handler) as (adapter, _, client):
        candles = await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=page + STEP)
    assert [c.open_time_ms for c in candles] == [0, page // 2, page]
    assert len(client.calls) == 4


@pytest.mark.asyncio
async def test_unaligned_end_keeps_a_closed_candle_opened_before_it(fake_clock: FakeClock) -> None:
    """`[start, end)` — за `open_time_ms`. Вікно коротше за таймфрейм усе одно запитується."""
    seen: list[dict[str, str]] = []

    def handler(_path: str, query: Mapping[str, str]) -> BybitRawResponse:
        seen.append(dict(query))
        return _raw(_kline_body([0]))

    async with _open(fake_clock, handler) as (adapter, _, _client):
        candles = await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=STEP // 2)
    assert [c.open_time_ms for c in candles] == [0]
    assert [(q["start"], q["end"]) for q in seen] == [("0", str(STEP // 2 - 1))]


@pytest.mark.asyncio
async def test_unaligned_end_after_a_full_page_keeps_the_tail_candle(fake_clock: FakeClock) -> None:
    page = KLINE_PAGE * STEP
    end = page + STEP // 2

    def handler(_path: str, query: Mapping[str, str]) -> BybitRawResponse:
        start = int(query["start"])
        if start == 0:
            return _raw(_kline_body([i * STEP for i in reversed(range(KLINE_PAGE))], time_ms=page + 2 * STEP))
        assert (start, int(query["end"])) == (page, end - 1)
        return _raw(_kline_body([page], time_ms=page + 2 * STEP))

    async with _open(fake_clock, handler) as (adapter, _, client):
        candles = await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=end)
    assert len(candles) == KLINE_PAGE + 1
    assert candles[-1].open_time_ms == page
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_conflicting_candle_and_float_price_are_malformed(fake_clock: FakeClock) -> None:
    conflict = _kline_body([0, 0])
    rows = conflict["result"]["list"]  # type: ignore[index]
    assert isinstance(rows, list)
    rows[1] = _kline_row(0, close="10")
    async with _open(fake_clock, _only(conflict)) as (adapter, _, client):
        with pytest.raises(MalformedResponse):
            await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=STEP)
    assert len(client.calls) == 1

    numeric = _kline_body([0])
    numeric_rows = numeric["result"]["list"]  # type: ignore[index]
    assert isinstance(numeric_rows, list)
    numeric_rows[0] = [str(0), 10, "12", "9", "11", "1.5", "100"]
    async with _open(fake_clock, _only(numeric)) as (adapter, _, client):
        with pytest.raises(MalformedResponse):
            await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=STEP)
    assert len(client.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("timeframe", "interval"), list(_INTERVALS.items()))
async def test_each_timeframe_uses_the_documented_bybit_interval(
    fake_clock: FakeClock, timeframe: Timeframe, interval: str
) -> None:
    step = timeframe.duration_ms
    seen: dict[str, str] = {}

    def handler(_path: str, query: Mapping[str, str]) -> BybitRawResponse:
        seen.update(query)
        return _raw(_kline_body([0], time_ms=step + KLINE_FINALITY_MARGIN_MS))

    async with _open(fake_clock, handler) as (adapter, _, _client):
        candles = await adapter.fetch_candles(symbol=SYMBOL, timeframe=timeframe, start_ms=0, end_ms=step)
    assert seen["interval"] == interval
    assert [c.open_time_ms for c in candles] == [0]


@pytest.mark.asyncio
async def test_invalid_symbol_and_mismatched_settle_do_not_call(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(_kline_body([0]))) as (adapter, _, client):
        with pytest.raises(ValueError, match="канонічним"):
            await adapter.fetch_candles(symbol="btc/usdt:usdt", timeframe=Timeframe.H1, start_ms=0, end_ms=STEP)
        with pytest.raises(ValueError, match="quote і settle"):
            await adapter.fetch_funding_history(symbol="BTC/USDT:BTC", start_ms=0, end_ms=1)
    assert client.calls == []


@pytest.mark.asyncio
async def test_unknown_ret_code_is_not_an_empty_result_and_is_not_retried(fake_clock: FakeClock) -> None:
    body = {
        "retCode": 10001,
        "retMsg": "params error: symbol invalid",
        "result": {"category": "", "list": []},
        "time": SERVER,
    }
    async with _open(fake_clock, _only(body)) as (adapter, _, client):
        with pytest.raises(ExchangeOutcomeUnknown):
            await adapter.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0, end_ms=STEP)
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_server_error_is_retried_and_timeout_stops_after_the_attempt_budget(fake_clock: FakeClock) -> None:
    attempts = {"n": 0}

    def recover(_path: str, _query: Mapping[str, str]) -> BybitRawResponse:
        attempts["n"] += 1
        if attempts["n"] == 1:
            return _raw({"retCode": 0, "retMsg": "OK", "result": {}, "time": SERVER}, status=500)
        return _raw(_time_body())

    async with _open(fake_clock, recover) as (adapter, _, _client):
        assert await adapter.fetch_time() == SERVER
    assert attempts["n"] == 2

    def explode(_path: str, _query: Mapping[str, str]) -> BybitRawResponse:
        raise ExchangeTimeout(PATH_TIME)

    async with _open(fake_clock, explode) as (adapter, limiter, client):
        with pytest.raises(ExchangeTimeout):
            await adapter.fetch_time()
    assert len(client.calls) == 3
    assert limiter.priorities == [RequestPriority.RECONCILE] * 3


@pytest.mark.asyncio
async def test_rate_limit_pauses_and_retries_and_headers_tighten_the_bucket(fake_clock: FakeClock) -> None:
    calls = {"n": 0}
    headers = {
        "X-Bapi-Limit": "100",
        "X-Bapi-Limit-Status": "90",
        "X-Bapi-Limit-Reset-Timestamp": str(SERVER),
    }

    def handler(_path: str, _query: Mapping[str, str]) -> BybitRawResponse:
        calls["n"] += 1
        if calls["n"] == 1:
            return _raw(
                {"retCode": 10006, "retMsg": "Too many visits!", "result": {}, "time": SERVER},
                status=429,
                headers=headers,
            )
        return _raw(_time_body(), headers=headers)

    async with _open(fake_clock, handler) as (adapter, limiter, _client):
        assert await adapter.fetch_time() == SERVER
    assert calls["n"] == 2
    assert limiter.observed[0][0] is EndpointGroup.MARKET
    assert limiter.observed[0][1].remaining == 90


@pytest.mark.asyncio
async def test_instruments_keep_usdt_perpetuals_and_skip_the_rest(fake_clock: FakeClock) -> None:
    rows: list[dict[str, object]] = [
        _perp(),
        {"contractType": "LinearFutures"},
        {
            "contractType": "LinearPerpetual",
            "symbol": "BTCPERP",
            "baseCoin": "BTC",
            "quoteCoin": "USDC",
            "settleCoin": "USDC",
        },
        _perp(symbol="ETHUSDT", base="ETH"),
        _perp(symbol="SOLUSDT", base="SOL", contract_type="Perpetual"),
    ]

    async with _open(fake_clock, _only(_instruments(rows))) as (adapter, limiter, client):
        instruments = await adapter.fetch_instruments()
        one = await adapter.fetch_instruments(symbols=["NOSUCHCOIN/USDT:USDT", SYMBOL])
        assert await adapter.fetch_instruments(symbols=[]) == []
    symbols = [item.symbol for item in instruments]
    assert symbols == ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]
    btc = instruments[0]
    assert btc.tick_size == Decimal("0.10")
    assert btc.qty_step == Decimal("0.001")
    assert btc.min_qty == Decimal("0.001")
    assert btc.min_notional == Decimal("5")
    assert btc.max_qty == Decimal("500")
    assert btc.max_leverage == Decimal("150.00")
    assert btc.launch_time_ms == 1_585_123_200_000
    assert btc.status == "Trading"
    assert [item.symbol for item in one] == [SYMBOL]
    assert client.calls[-1] == (PATH_INSTRUMENTS, {}) or client.calls[0][0] == PATH_INSTRUMENTS
    assert all(query.get("category") == "linear" for _path, query in client.calls if _path == PATH_INSTRUMENTS)
    assert limiter.priorities[0] is RequestPriority.DATA
    # Порожній список символів мережу не чіпає: три виклики fetch, два з них з HTTP.
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_instrument_cursor_is_decoded_once_before_the_next_request(fake_clock: FakeClock) -> None:
    pages = [
        _instruments([_perp()], cursor="first%3DETHUSDT%26last%3DETHUSDT"),
        _instruments([_perp(symbol="ETHUSDT", base="ETH")], cursor=""),
    ]

    def handler(_path: str, _query: Mapping[str, str]) -> BybitRawResponse:
        return _raw(pages.pop(0))

    async with _open(fake_clock, handler) as (adapter, _, client):
        instruments = await adapter.fetch_instruments()
    assert [item.symbol for item in instruments] == ["BTC/USDT:USDT", "ETH/USDT:USDT"]
    assert client.calls[1][1]["cursor"] == "first=ETHUSDT&last=ETHUSDT"


@pytest.mark.asyncio
async def test_broken_perpetual_filter_fails_the_whole_instruments_response(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(_instruments([_perp(tick="0")]))) as (adapter, _, client):
        with pytest.raises(MalformedResponse):
            await adapter.fetch_instruments(symbols=[SYMBOL])
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_instrument_without_launch_time_fails_the_response(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(_instruments([_perp(launch=None)]))) as (adapter, _, _client):
        with pytest.raises(MalformedResponse):
            await adapter.fetch_instruments()


@pytest.mark.asyncio
async def test_instrument_status_is_kept_even_when_not_trading(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(_instruments([_perp(status="PreLaunch")]))) as (adapter, _, _client):
        instruments = await adapter.fetch_instruments()
    assert [item.status for item in instruments] == ["PreLaunch"]


@pytest.mark.asyncio
async def test_funding_is_ascending_end_exclusive_and_a_full_page_is_split(fake_clock: FakeClock) -> None:
    def handler(_path: str, query: Mapping[str, str]) -> BybitRawResponse:
        start = int(query["startTime"])
        end = int(query["endTime"]) + 1
        assert query["category"] == "linear"
        assert query["symbol"] == "BTCUSDT"
        if end - start >= 1_000:
            return _raw(_funding_body([_fund(start)] * FUNDING_PAGE))
        if start == 0:
            return _raw(_funding_body([_fund(400, "-0.00199027"), _fund(100), _fund(1_000)]))
        return _raw(_funding_body([_fund(start, "0.0002")]))

    async with _open(fake_clock, handler) as (adapter, _, _client):
        rows = await adapter.fetch_funding_history(symbol=SYMBOL, start_ms=0, end_ms=1_000)
    assert [(row.funding_time_ms, row.rate) for row in rows] == [
        (100, Decimal("0.0001")),
        (400, Decimal("-0.00199027")),
        (500, Decimal("0.0002")),
    ]


@pytest.mark.asyncio
async def test_conflicting_funding_rate_is_malformed(fake_clock: FakeClock) -> None:
    body = _funding_body([_fund(100, "0.0001"), _fund(100, "0.0002")])
    async with _open(fake_clock, _only(body)) as (adapter, _, _client):
        with pytest.raises(MalformedResponse):
            await adapter.fetch_funding_history(symbol=SYMBOL, start_ms=0, end_ms=1_000)


@pytest.mark.asyncio
async def test_closed_adapter_does_not_call(fake_clock: FakeClock) -> None:
    client = ScriptedClient(_only(_time_body()))
    limiter = SpyLimiter(fake_clock)
    adapter = BybitAdapter(
        mode=ExchangeMode.TESTNET,
        limiter=limiter,
        client=client,
        sleeper=_sleep_for(fake_clock),
        jitter=_JITTER,
    )
    await adapter.aclose()
    await adapter.aclose()
    with pytest.raises(ExchangeUnavailable):
        await adapter.fetch_time()
    assert client.calls == []
    await limiter.aclose()


@pytest.mark.asyncio
async def test_public_origin_is_mainnet_for_demo_and_live_and_testnet_host_for_testnet(
    fake_clock: FakeClock,
) -> None:
    opened: list[tuple[BybitAdapter, SpyLimiter]] = []
    try:
        origins: dict[ExchangeMode, str] = {}
        for mode in (ExchangeMode.DEMO, ExchangeMode.LIVE, ExchangeMode.TESTNET):
            limiter = SpyLimiter(fake_clock)
            adapter = BybitAdapter(
                mode=mode,
                limiter=limiter,
                sleeper=_sleep_for(fake_clock),
                jitter=_JITTER,
            )
            opened.append((adapter, limiter))
            origins[mode] = adapter.public_origin
        assert origins[ExchangeMode.DEMO] == origins[ExchangeMode.LIVE]
        assert "testnet" not in origins[ExchangeMode.DEMO]
        assert "demo" not in origins[ExchangeMode.DEMO]
        assert origins[ExchangeMode.DEMO].startswith("https://")
        assert "testnet" in origins[ExchangeMode.TESTNET]
        assert origins[ExchangeMode.TESTNET] != origins[ExchangeMode.LIVE]
    finally:
        for adapter, limiter in opened:
            await adapter.aclose()
            await limiter.aclose()


def test_scripted_client_satisfies_the_protocol() -> None:
    client: BybitPublicClient = ScriptedClient(_only(_time_body()))
    assert client.public_origin.startswith("https://")
