"""Приватні методи `BybitAdapter` на записаних відповідях (T3.2): параметри, розбір, помилки, ліміти.

Транспорт — `ScriptedPrivateClient` (без мережі й без CCXT; підпис і URL — `test_bybit_private_ccxt.py`).
Тіла відповідей — за документацією Bybit v5 (context7, 2026-11-10).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from decimal import Decimal

import pytest
from structlog.testing import capture_logs

from app.core.clock import FakeClock
from app.exchange.bybit import EXECUTION_WINDOW_MS, BybitAdapter, BybitRawResponse, ExchangeMode
from app.exchange.bybit_codes import (
    PATH_ACCOUNT_INFO,
    PATH_EXECUTION_LIST,
    PATH_ORDER_HISTORY,
    PATH_ORDER_REALTIME,
    PATH_POSITION_LIST,
    PATH_WALLET_BALANCE,
)
from app.exchange.errors import (
    AdapterConfigError,
    ExchangeAuthError,
    ExchangeOutcomeUnknown,
    ExchangeUnavailable,
    MalformedResponse,
    RequestRejected,
)
from app.exchange.rate_limit import EndpointGroup, ProcessRole, RequestPriority
from app.exchange.types import (
    ExchangeOrderStatus,
    ExecType,
    MarginMode,
    OrderType,
    PositionSide,
    Side,
    StopOrderType,
    TimeInForce,
)
from tests.exchange.factories import FAKE_NOW_MS, VALID_CLIENT_ORDER_ID
from tests.exchange.test_bybit_public import _JITTER, SERVER, ScriptedClient, SpyLimiter, _ok, _raw, _sleep_for

Handler = Callable[[str, dict[str, str | bool]], BybitRawResponse]
DAY = 24 * 3_600_000


class ScriptedPrivateClient:
    """Приватний транспорт без мережі: відповідь залежить від шляху й параметрів."""

    def __init__(self, handler: Handler) -> None:
        self._handler = handler
        self.calls: list[tuple[str, dict[str, str | bool]]] = []
        self.private_origin = "https://api-testnet.example"
        self.closed = False

    async def request(self, path: str, params: Mapping[str, str | bool]) -> BybitRawResponse:
        recorded = dict(params)
        self.calls.append((path, recorded))
        return self._handler(path, recorded)

    async def aclose(self) -> None:
        self.closed = True


def _no_public(_path: str, _query: Mapping[str, str]) -> BybitRawResponse:
    raise AssertionError("приватний тест не ходить у публічний API")


@asynccontextmanager
async def _open(
    clock: FakeClock,
    handler: Handler,
    *,
    read_attempts: int = 3,
    on_recv_window_rejected: Callable[[], None] | None = None,
) -> AsyncIterator[tuple[BybitAdapter, SpyLimiter, ScriptedPrivateClient]]:
    private = ScriptedPrivateClient(handler)
    limiter = SpyLimiter(clock, role=ProcessRole.TRADING)
    adapter = BybitAdapter(
        mode=ExchangeMode.TESTNET,
        limiter=limiter,
        client=ScriptedClient(_no_public),
        private_client=private,
        sleeper=_sleep_for(clock),
        jitter=_JITTER,
        read_attempts=read_attempts,
        on_recv_window_rejected=on_recv_window_rejected,
    )
    try:
        yield adapter, limiter, private
    finally:
        await adapter.aclose()
        await limiter.aclose()


def _err(code: int, message: str = "error") -> dict[str, object]:
    return {"retCode": code, "retMsg": message, "result": {}, "retExtInfo": {}, "time": SERVER}


def _list(rows: list[dict[str, object]], cursor: str = "") -> dict[str, object]:
    return _ok({"category": "linear", "list": rows, "nextPageCursor": cursor})


def _account(margin: str = "ISOLATED_MARGIN", status: int = 5) -> dict[str, object]:
    return _ok({"marginMode": margin, "unifiedMarginStatus": status, "updatedTime": str(FAKE_NOW_MS)})


def _position_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "positionIdx": 0,
        "symbol": "BTCUSDT",
        "side": "Buy",
        "size": "0.010",
        "avgPrice": "60000",
        "markPrice": "60100",
        "liqPrice": "50000",
        "leverage": "5",
        "stopLoss": "58000",
        "takeProfit": "0",
        "unrealisedPnl": "1.5",
        "tradeMode": 0,
        "updatedTime": str(FAKE_NOW_MS),
    }
    row.update(overrides)
    return row


def _order_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "orderId": "ex-1",
        "orderLinkId": VALID_CLIENT_ORDER_ID,
        "symbol": "BTCUSDT",
        "side": "Buy",
        "orderType": "Limit",
        "orderStatus": "New",
        "qty": "0.010",
        "cumExecQty": "0",
        "avgPrice": "",
        "price": "30000",
        "triggerPrice": "",
        "stopOrderType": "",
        "timeInForce": "GTC",
        "reduceOnly": False,
        "rejectReason": "EC_NoError",
        "createdTime": str(FAKE_NOW_MS),
        "updatedTime": str(FAKE_NOW_MS + 1),
    }
    row.update(overrides)
    return row


def _exec_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "execId": "e-1",
        "symbol": "BTCUSDT",
        "orderId": "ex-1",
        "orderLinkId": VALID_CLIENT_ORDER_ID,
        "side": "Buy",
        "execPrice": "60000",
        "execQty": "0.010",
        "execFee": "0.33",
        "feeCurrency": "USDT",
        "isMaker": False,
        "execType": "Trade",
        "execTime": str(FAKE_NOW_MS),
    }
    row.update(overrides)
    return row


def _wallet(coins: list[dict[str, object]]) -> dict[str, object]:
    return _ok({"list": [{"accountType": "UNIFIED", "coin": coins}]})


def _usdt(**overrides: object) -> dict[str, object]:
    coin: dict[str, object] = {
        "coin": "USDT",
        "walletBalance": "1000",
        "equity": "1010",
        "unrealisedPnl": "10",
        "totalPositionIM": "100",
        "totalOrderIM": "50",
        "locked": "5",
        "bonus": "",
    }
    coin.update(overrides)
    return coin


# --- Ключі ---


@pytest.mark.asyncio
async def test_without_keys_private_methods_refuse_before_the_network(fake_clock: FakeClock) -> None:
    limiter = SpyLimiter(fake_clock, role=ProcessRole.TRADING)
    adapter = BybitAdapter(mode=ExchangeMode.TESTNET, limiter=limiter, client=ScriptedClient(_no_public))
    try:
        assert adapter.private_origin is None
        with pytest.raises(AdapterConfigError, match="ключі"):
            await adapter.fetch_balance(priority=RequestPriority.RECONCILE)
        assert limiter.groups == []
    finally:
        await adapter.aclose()
        await limiter.aclose()


# --- Баланс ---


@pytest.mark.asyncio
async def test_balance_is_unified_usdt_with_isolated_available_margin(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, lambda _p, _q: _raw(_wallet([_usdt()]))) as (adapter, limiter, private):
        balance = await adapter.fetch_balance(priority=RequestPriority.PROTECTION)
    assert private.calls == [(PATH_WALLET_BALANCE, {"accountType": "UNIFIED", "coin": "USDT"})]
    assert (limiter.groups, limiter.priorities, limiter.retries) == (
        [EndpointGroup.WALLET_BALANCE],
        [RequestPriority.PROTECTION],
        [True],
    )
    assert balance.coin == "USDT"
    assert (balance.wallet_balance, balance.equity, balance.unrealized_pnl) == (
        Decimal(1000),
        Decimal(1010),
        Decimal(10),
    )
    # walletBalance − totalPositionIM − totalOrderIM − locked − bonus (порожнє — нуль).
    assert balance.available_balance == Decimal(845)


@pytest.mark.asyncio
async def test_balance_without_usdt_is_zero_and_without_unified_is_malformed(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, lambda _p, _q: _raw(_wallet([]))) as (adapter, _limiter, _private):
        balance = await adapter.fetch_balance(priority=RequestPriority.RECONCILE)
    assert balance.wallet_balance == balance.available_balance == Decimal(0)
    body = _ok({"list": [{"accountType": "CONTRACT", "coin": []}]})
    async with _open(fake_clock, lambda _p, _q: _raw(body)) as (adapter, _limiter, _private):
        with pytest.raises(MalformedResponse, match="UNIFIED"):
            await adapter.fetch_balance(priority=RequestPriority.RECONCILE)


# --- Позиції ---


def _positions_handler(rows: list[dict[str, object]], *, margin: str = "ISOLATED_MARGIN") -> Handler:
    def handler(path: str, _params: dict[str, str | bool]) -> BybitRawResponse:
        if path == PATH_ACCOUNT_INFO:
            return _raw(_account(margin))
        assert path == PATH_POSITION_LIST
        return _raw(_list(rows))

    return handler


@pytest.mark.asyncio
async def test_positions_parse_skip_zero_size_and_take_margin_mode_from_the_account(fake_clock: FakeClock) -> None:
    rows = [
        _position_row(symbol="ETHUSDT", side="Sell", size="0.5", avgPrice="3000", markPrice="2990", liqPrice=""),
        _position_row(),
        _position_row(symbol="SOLUSDT", side="", size="0", avgPrice="0", markPrice="150"),
    ]
    async with _open(fake_clock, _positions_handler(rows)) as (adapter, limiter, private):
        positions = await adapter.fetch_positions(priority=RequestPriority.PROTECTION)
    assert [path for path, _ in private.calls] == [PATH_ACCOUNT_INFO, PATH_POSITION_LIST]
    assert private.calls[1][1] == {"category": "linear", "limit": "200", "settleCoin": "USDT"}
    assert limiter.groups == [EndpointGroup.ACCOUNT_INFO, EndpointGroup.POSITION_LIST]
    assert [position.symbol for position in positions] == ["BTC/USDT:USDT", "ETH/USDT:USDT"]
    btc, eth = positions
    assert (btc.side, btc.qty, btc.entry_price, btc.leverage) == (
        PositionSide.LONG,
        Decimal("0.010"),
        Decimal(60000),
        Decimal(5),
    )
    assert (btc.stop_loss, btc.take_profit, btc.liq_price) == (Decimal(58000), None, Decimal(50000))
    assert (eth.side, eth.liq_price) == (PositionSide.SHORT, None)
    assert {position.margin_mode for position in positions} == {MarginMode.ISOLATED}


@pytest.mark.asyncio
async def test_positions_by_symbol_query_each_symbol_and_report_cross_margin(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _positions_handler([_position_row()], margin="REGULAR_MARGIN")) as (
        adapter,
        _limiter,
        private,
    ):
        positions = await adapter.fetch_positions(priority=RequestPriority.RECONCILE, symbols=["BTC/USDT:USDT"])
    assert private.calls[1] == (PATH_POSITION_LIST, {"category": "linear", "limit": "200", "symbol": "BTCUSDT"})
    assert positions[0].margin_mode is MarginMode.CROSS


@pytest.mark.asyncio
async def test_hedge_mode_position_is_a_config_error_not_a_skip(fake_clock: FakeClock) -> None:
    rows = [_position_row(positionIdx=1), _position_row(positionIdx=2, side="Sell")]
    async with _open(fake_clock, _positions_handler(rows)) as (adapter, _limiter, _private):
        with pytest.raises(AdapterConfigError, match="hedge"):
            await adapter.fetch_positions(priority=RequestPriority.PROTECTION)


@pytest.mark.asyncio
async def test_position_cursor_is_followed_to_the_end(fake_clock: FakeClock) -> None:
    seen: list[str | bool | None] = []

    def handler(path: str, params: dict[str, str | bool]) -> BybitRawResponse:
        if path == PATH_ACCOUNT_INFO:
            return _raw(_account())
        seen.append(params.get("cursor"))
        if "cursor" not in params:
            return _raw(_list([_position_row()], cursor="page%3D2"))
        return _raw(_list([_position_row(symbol="ETHUSDT", avgPrice="3000", markPrice="3000")]))

    async with _open(fake_clock, handler) as (adapter, _limiter, _private):
        positions = await adapter.fetch_positions(priority=RequestPriority.RECONCILE)
    # Курсор біржі (percent-encoded) адаптер тримає декодованим; закодує його клієнт — рівно один раз.
    assert seen == [None, "page=2"]
    assert len(positions) == 2


# --- Ордери ---


@pytest.mark.asyncio
async def test_open_orders_query_realtime_open_only_and_parse_every_field(fake_clock: FakeClock) -> None:
    rows = [
        _order_row(orderId="ex-2", createdTime=str(FAKE_NOW_MS + 5)),
        _order_row(
            orderId="ex-3",
            orderLinkId="",
            orderType="Market",
            side="Sell",
            price="59000",
            reduceOnly=True,
            stopOrderType="StopLoss",
            triggerPrice="58000",
            orderStatus="Untriggered",
            qty="0",
            timeInForce="IOC",
        ),
    ]
    async with _open(fake_clock, lambda _p, _q: _raw(_list(rows))) as (adapter, limiter, private):
        orders = await adapter.fetch_open_orders(priority=RequestPriority.RECONCILE)
    assert private.calls == [
        (PATH_ORDER_REALTIME, {"category": "linear", "openOnly": "0", "limit": "50", "settleCoin": "USDT"})
    ]
    assert limiter.groups == [EndpointGroup.ORDER_REALTIME]
    assert [order.exchange_order_id for order in orders] == ["ex-3", "ex-2"]
    stop, limit = orders
    assert (limit.order_type, limit.price, limit.status, limit.time_in_force) == (
        OrderType.LIMIT,
        Decimal(30000),
        ExchangeOrderStatus.NEW,
        TimeInForce.GTC,
    )
    assert (limit.client_order_id, limit.reject_reason, limit.avg_fill_price) == (VALID_CLIENT_ORDER_ID, None, None)
    # Службову ціну ринкового ордера домен не бере; умовний SL — з тригером і нульовою кількістю.
    assert (stop.side, stop.price, stop.client_order_id, stop.reduce_only) == (Side.SELL, None, None, True)
    assert (stop.stop_order_type, stop.trigger_price, stop.status) == (
        StopOrderType.STOP_LOSS,
        Decimal(58000),
        ExchangeOrderStatus.UNTRIGGERED,
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("New", ExchangeOrderStatus.NEW),
        ("Created", ExchangeOrderStatus.NEW),
        ("PartiallyFilled", ExchangeOrderStatus.PARTIALLY_FILLED),
        ("Untriggered", ExchangeOrderStatus.UNTRIGGERED),
        ("Triggered", ExchangeOrderStatus.TRIGGERED),
        ("Filled", ExchangeOrderStatus.FILLED),
        ("Cancelled", ExchangeOrderStatus.CANCELED),
        ("PartiallyFilledCanceled", ExchangeOrderStatus.CANCELED),
        ("Rejected", ExchangeOrderStatus.REJECTED),
        ("Deactivated", ExchangeOrderStatus.DEACTIVATED),
        ("SomethingNew", ExchangeOrderStatus.UNKNOWN),
    ],
)
def test_order_status_mapping(raw: str, expected: ExchangeOrderStatus) -> None:
    from app.exchange.bybit_codes import order_status

    assert order_status(raw) is expected


def _by_link(realtime: list[dict[str, object]], history: list[dict[str, object]]) -> Handler:
    def handler(path: str, params: dict[str, str | bool]) -> BybitRawResponse:
        assert params == {"category": "linear", "symbol": "BTCUSDT", "orderLinkId": VALID_CLIENT_ORDER_ID}
        return _raw(_list(realtime if path == PATH_ORDER_REALTIME else history))

    return handler


@pytest.mark.asyncio
async def test_order_by_client_id_found_in_realtime_does_not_ask_history(fake_clock: FakeClock) -> None:
    row = _order_row(orderStatus="Cancelled", cumExecQty="0.004", avgPrice="30000.5")
    async with _open(fake_clock, _by_link([row], [])) as (adapter, limiter, private):
        order = await adapter.fetch_order_by_client_id(
            priority=RequestPriority.RECONCILE, symbol="BTC/USDT:USDT", client_order_id=VALID_CLIENT_ORDER_ID
        )
    assert [path for path, _ in private.calls] == [PATH_ORDER_REALTIME]
    assert limiter.groups == [EndpointGroup.ORDER_REALTIME]
    assert order is not None
    # Частковий fill і скасування залишку в деривативах — `Cancelled` з виконаною кількістю (рішення 0027).
    assert (order.status, order.filled_qty, order.avg_fill_price) == (
        ExchangeOrderStatus.CANCELED,
        Decimal("0.004"),
        Decimal("30000.5"),
    )


@pytest.mark.asyncio
async def test_order_by_client_id_falls_back_to_history_and_none_only_after_both(fake_clock: FakeClock) -> None:
    async with _open(
        fake_clock, _by_link([], [_order_row(orderStatus="Filled", cumExecQty="0.010", avgPrice="1")])
    ) as (
        adapter,
        limiter,
        _private,
    ):
        found = await adapter.fetch_order_by_client_id(
            priority=RequestPriority.RECONCILE, symbol="BTC/USDT:USDT", client_order_id=VALID_CLIENT_ORDER_ID
        )
    assert found is not None
    assert found.status is ExchangeOrderStatus.FILLED
    assert limiter.groups == [EndpointGroup.ORDER_REALTIME, EndpointGroup.ORDER_HISTORY]
    async with _open(fake_clock, _by_link([], [])) as (adapter, _limiter, private):
        missing = await adapter.fetch_order_by_client_id(
            priority=RequestPriority.RECONCILE, symbol="BTC/USDT:USDT", client_order_id=VALID_CLIENT_ORDER_ID
        )
    assert missing is None
    assert [path for path, _ in private.calls] == [PATH_ORDER_REALTIME, PATH_ORDER_HISTORY]


@pytest.mark.asyncio
async def test_two_orders_with_one_link_id_are_malformed_not_a_guess(fake_clock: FakeClock) -> None:
    rows = [_order_row(), _order_row(orderId="ex-other")]
    async with _open(fake_clock, _by_link(rows, [])) as (adapter, _limiter, _private):
        with pytest.raises(MalformedResponse, match="orderLinkId"):
            await adapter.fetch_order_by_client_id(
                priority=RequestPriority.RECONCILE, symbol="BTC/USDT:USDT", client_order_id=VALID_CLIENT_ORDER_ID
            )


@pytest.mark.asyncio
async def test_filled_order_without_average_price_is_malformed(fake_clock: FakeClock) -> None:
    row = _order_row(orderStatus="Filled", cumExecQty="0.010", avgPrice="")
    async with _open(fake_clock, _by_link([row], [])) as (adapter, _limiter, _private):
        with pytest.raises(MalformedResponse):
            await adapter.fetch_order_by_client_id(
                priority=RequestPriority.RECONCILE, symbol="BTC/USDT:USDT", client_order_id=VALID_CLIENT_ORDER_ID
            )


# --- Виконання ---


@pytest.mark.asyncio
async def test_executions_split_into_seven_day_windows_skip_funding_and_sort(fake_clock: FakeClock) -> None:
    start = FAKE_NOW_MS - 10 * DAY
    windows: list[tuple[str | bool, str | bool]] = []

    def handler(path: str, params: dict[str, str | bool]) -> BybitRawResponse:
        assert path == PATH_EXECUTION_LIST
        windows.append((params["startTime"], params["endTime"]))
        if params["startTime"] == str(start):
            return _raw(
                _list(
                    [
                        _exec_row(execId="e-2", execTime=str(start + 2)),
                        _exec_row(execId="f-1", execType="Funding", execTime=str(start + 3)),
                        _exec_row(execId="e-1", execTime=str(start + 1), execFee="-0.01", isMaker=True),
                    ]
                )
            )
        # Той самий fill у сусідньому вікні — не дубль, а один запис.
        return _raw(_list([_exec_row(execId="e-1", execTime=str(start + 1), execFee="-0.01", isMaker=True)]))

    async with _open(fake_clock, handler) as (adapter, limiter, _private):
        fills = await adapter.fetch_executions(
            priority=RequestPriority.RECONCILE, start_ms=start, end_ms=FAKE_NOW_MS, symbol="BTC/USDT:USDT"
        )
    assert windows == [
        (str(start), str(start + EXECUTION_WINDOW_MS - 1)),
        (str(start + EXECUTION_WINDOW_MS), str(FAKE_NOW_MS - 1)),
    ]
    assert limiter.groups == [EndpointGroup.EXECUTION_LIST] * 2
    assert [fill.exec_id for fill in fills] == ["e-1", "e-2"]
    assert (fills[0].fee, fills[0].is_maker, fills[0].exec_type) == (Decimal("-0.01"), True, ExecType.TRADE)


@pytest.mark.asyncio
async def test_unknown_exec_type_and_conflicting_duplicate_are_malformed(fake_clock: FakeClock) -> None:
    start = FAKE_NOW_MS - DAY
    unknown = _list([_exec_row(execType="BlockTrade")])
    async with _open(fake_clock, lambda _p, _q: _raw(unknown)) as (adapter, _limiter, _private):
        with pytest.raises(MalformedResponse, match="execType"):
            await adapter.fetch_executions(priority=RequestPriority.RECONCILE, start_ms=start, end_ms=FAKE_NOW_MS)
    conflict = _list([_exec_row(execTime=str(start + 1)), _exec_row(execTime=str(start + 1), execQty="0.020")])
    async with _open(fake_clock, lambda _p, _q: _raw(conflict)) as (adapter, _limiter, _private):
        with pytest.raises(MalformedResponse, match="execId"):
            await adapter.fetch_executions(priority=RequestPriority.RECONCILE, start_ms=start, end_ms=FAKE_NOW_MS)
    async with _open(fake_clock, lambda _p, _q: _raw(conflict)) as (adapter, _limiter, private):
        with pytest.raises(ValueError, match="end_ms"):
            await adapter.fetch_executions(priority=RequestPriority.RECONCILE, start_ms=start, end_ms=start)
        assert private.calls == []


# --- Помилки й повтори ---


@pytest.mark.asyncio
async def test_parameter_error_on_read_is_rejected_and_not_retried(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, lambda _p, _q: _raw(_err(10001, "params error"))) as (adapter, _limiter, private):
        with pytest.raises(RequestRejected) as raised:
            await adapter.fetch_balance(priority=RequestPriority.RECONCILE)
    assert raised.value.code == "10001"
    assert len(private.calls) == 1


@pytest.mark.asyncio
async def test_server_error_code_is_retried_on_read_then_raised(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, lambda _p, _q: _raw(_err(10016, "service restarting")), read_attempts=3) as (
        adapter,
        _limiter,
        private,
    ):
        with pytest.raises(ExchangeUnavailable):
            await adapter.fetch_balance(priority=RequestPriority.RECONCILE)
    assert len(private.calls) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "status", "error"),
    [
        (_err(10003, "invalid api key"), 200, ExchangeAuthError),
        (_err(10005, "permission denied"), 200, ExchangeAuthError),
        (_err(10017, "route not found"), 200, AdapterConfigError),
        (_err(99999, "never heard of it"), 200, ExchangeOutcomeUnknown),
        ({"retMsg": "no code"}, 200, MalformedResponse),
        ("Unauthorized", 401, ExchangeAuthError),
        ("Not Found", 404, AdapterConfigError),
        ("Teapot", 418, ExchangeOutcomeUnknown),
    ],
)
async def test_read_errors_map_without_retry(
    fake_clock: FakeClock, body: object, status: int, error: type[Exception]
) -> None:
    async with _open(fake_clock, lambda _p, _q: _raw(body, status=status)) as (adapter, _limiter, private):
        with pytest.raises(error):
            await adapter.fetch_balance(priority=RequestPriority.RECONCILE)
    assert len(private.calls) == 1


@pytest.mark.asyncio
async def test_recv_window_rejection_asks_for_a_clock_recheck(fake_clock: FakeClock) -> None:
    rechecks: list[str] = []
    async with _open(
        fake_clock,
        lambda _p, _q: _raw(_err(10002, "invalid request, recv_window")),
        on_recv_window_rejected=lambda: rechecks.append("x"),
    ) as (adapter, _limiter, _private):
        with pytest.raises(ExchangeOutcomeUnknown, match="10002"):
            await adapter.fetch_balance(priority=RequestPriority.RECONCILE)
    assert rechecks == ["x"]

    def broken() -> None:
        raise RuntimeError("guard down")

    async with _open(fake_clock, lambda _p, _q: _raw(_err(10002)), on_recv_window_rejected=broken) as (
        adapter,
        _limiter,
        _private,
    ):
        with capture_logs() as logs, pytest.raises(ExchangeOutcomeUnknown):
            await adapter.fetch_balance(priority=RequestPriority.RECONCILE)
    assert any(entry["event"] == "bybit_recv_window_callback_failed" for entry in logs)


@pytest.mark.asyncio
async def test_rate_limit_headers_tighten_the_private_group(fake_clock: FakeClock) -> None:
    headers = {
        "X-Bapi-Limit-Status": "7",
        "X-Bapi-Limit": "50",
        "X-Bapi-Limit-Reset-Timestamp": str(FAKE_NOW_MS + 1000),
    }
    async with _open(fake_clock, lambda _p, _q: _raw(_wallet([_usdt()]), headers=headers)) as (adapter, limiter, _p):
        await adapter.fetch_balance(priority=RequestPriority.RECONCILE)
    assert [group for group, _snapshot in limiter.observed] == [EndpointGroup.WALLET_BALANCE]


# --- Стартова перевірка акаунта ---


def _verify_handler(*, margin: str = "ISOLATED_MARGIN", status: int = 5, index: int = 0) -> Handler:
    def handler(path: str, _params: dict[str, str | bool]) -> BybitRawResponse:
        if path == PATH_ACCOUNT_INFO:
            return _raw(_account(margin, status))
        return _raw(_list([_position_row(positionIdx=index, size="0", side="", avgPrice="0")]))

    return handler


@pytest.mark.asyncio
async def test_account_config_passes_for_uta2_isolated_one_way(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _verify_handler()) as (adapter, limiter, private):
        await adapter.verify_account_config(["BTC/USDT:USDT"])
    assert [path for path, _ in private.calls] == [PATH_ACCOUNT_INFO, PATH_POSITION_LIST]
    assert set(limiter.priorities) == {RequestPriority.RECONCILE}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"status": 1}, "Unified"),
        ({"status": 3}, "Unified"),
        ({"margin": "REGULAR_MARGIN"}, "ISOLATED"),
        ({"margin": "PORTFOLIO_MARGIN"}, "ISOLATED"),
        ({"index": 1}, "hedge"),
    ],
)
async def test_account_config_refuses_everything_else(
    fake_clock: FakeClock, kwargs: dict[str, object], match: str
) -> None:
    async with _open(fake_clock, _verify_handler(**kwargs)) as (adapter, _limiter, _private):  # type: ignore[arg-type]
        with pytest.raises(AdapterConfigError, match=match):
            await adapter.verify_account_config(["BTC/USDT:USDT"])


@pytest.mark.asyncio
async def test_unknown_margin_mode_is_malformed_not_guessed(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _verify_handler(margin="SOMETHING")) as (adapter, _limiter, _private):
        with pytest.raises(MalformedResponse, match="marginMode"):
            await adapter.verify_account_config(["BTC/USDT:USDT"])
