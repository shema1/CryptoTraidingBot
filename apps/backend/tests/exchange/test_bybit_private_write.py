"""Методи запису `BybitAdapter` на записаних відповідях (T3.2, коміт 2): тіло запиту, групи лімітера, коди, повтори.

Правило запису: адаптер не повторює жодного запиту, що змінює стан (ТЗ §7.2; `adapter.py`), — ні після таймауту, ні
після збою сервера. `place_order` і `cancel_order` ще й не повторюються після ліміту (`retry=False`); ідемпотентні
`set_*` лімітер може повторити (`retry=True`). Тіла відповідей — за документацією Bybit v5 (context7, 2026-11-11).
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest

from app.core.clock import FakeClock
from app.exchange.bybit import BybitAdapter, BybitRawResponse
from app.exchange.bybit_codes import (
    PATH_ACCOUNT_INFO,
    PATH_ORDER_CANCEL,
    PATH_ORDER_CREATE,
    PATH_POSITION_LIST,
    PATH_SET_LEVERAGE,
    PATH_SET_MARGIN_MODE,
    PATH_TRADING_STOP,
)
from app.exchange.errors import (
    POSITION_SIDE_MISMATCH_CODE,
    ZERO_POSITION_CODE,
    AdapterConfigError,
    DuplicateClientOrderId,
    ExchangeError,
    ExchangeOutcomeUnknown,
    ExchangeTimeout,
    ExchangeUnavailable,
    InsufficientMargin,
    MalformedResponse,
    OrderNotActive,
    OrderRejected,
    RateLimited,
    RequestRejected,
    is_definite_rejection,
)
from app.exchange.rate_limit import EndpointGroup, RequestPriority
from app.exchange.types import (
    MarginMode,
    OrderAck,
    OrderType,
    PlaceOrderRequest,
    PositionProtection,
    PositionSide,
    Side,
    TimeInForce,
    TriggerBy,
)
from tests.exchange.factories import VALID_CLIENT_ORDER_ID
from tests.exchange.test_bybit_private import Params, _account, _err, _list, _open, _position_row
from tests.exchange.test_bybit_public import _ok, _raw

_SYMBOL = "BTC/USDT:USDT"


def _ack(order_id: str = "ex-9", link_id: str = VALID_CLIENT_ORDER_ID) -> dict[str, object]:
    return _ok({"orderId": order_id, "orderLinkId": link_id})


def _only(path: str, body: dict[str, object] | str, *, status: int = 200) -> Callable[[str, Params], BybitRawResponse]:
    def handler(seen: str, _params: Params) -> BybitRawResponse:
        assert seen == path
        return _raw(body, status=status)

    return handler


def _limit_entry(**overrides: object) -> PlaceOrderRequest:
    fields: dict[str, object] = {
        "symbol": _SYMBOL,
        "client_order_id": VALID_CLIENT_ORDER_ID,
        "side": Side.BUY,
        "order_type": OrderType.LIMIT,
        "qty": Decimal("0.010"),
        "reduce_only": False,
        "price": Decimal("60000"),
        "stop_loss": Decimal("58000"),
        "take_profit": Decimal("64000.5"),
    }
    fields.update(overrides)
    return PlaceOrderRequest.model_validate(fields)


def _market_exit() -> PlaceOrderRequest:
    return PlaceOrderRequest(
        symbol=_SYMBOL,
        client_order_id=VALID_CLIENT_ORDER_ID,
        side=Side.SELL,
        order_type=OrderType.MARKET,
        qty=Decimal("0.010"),
        reduce_only=True,
    )


def _protection(**overrides: object) -> PositionProtection:
    fields: dict[str, object] = {
        "symbol": _SYMBOL,
        "position_side": PositionSide.LONG,
        "stop_loss": Decimal("59000"),
        "take_profit": Decimal("64000"),
    }
    fields.update(overrides)
    return PositionProtection.model_validate(fields)


# --- place_order ---


@pytest.mark.asyncio
async def test_limit_entry_carries_native_full_mode_sl_tp_and_returns_the_ack(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CREATE, _ack())) as (adapter, limiter, private):
        ack = await adapter.place_order(_limit_entry(), priority=RequestPriority.DATA)
    assert ack == OrderAck(client_order_id=VALID_CLIENT_ORDER_ID, exchange_order_id="ex-9")
    assert private.calls == [
        (
            PATH_ORDER_CREATE,
            {
                "category": "linear",
                "symbol": "BTCUSDT",
                "side": "Buy",
                "orderType": "Limit",
                "qty": "0.010",
                "price": "60000",
                "timeInForce": "GTC",
                "orderLinkId": VALID_CLIENT_ORDER_ID,
                "reduceOnly": False,
                "positionIdx": 0,
                "tpslMode": "Full",
                "stopLoss": "58000",
                "slTriggerBy": "MarkPrice",
                "slOrderType": "Market",
                "takeProfit": "64000.5",
                "tpTriggerBy": "LastPrice",
                "tpOrderType": "Market",
            },
        )
    ]
    assert limiter.groups == [EndpointGroup.ORDER_CREATE]
    assert limiter.priorities == [RequestPriority.DATA]
    assert limiter.retries == [False]


@pytest.mark.asyncio
async def test_market_reduce_only_exit_has_no_price_time_in_force_or_protection(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CREATE, _ack())) as (adapter, limiter, private):
        await adapter.place_order(_market_exit(), priority=RequestPriority.PROTECTION)
    assert private.calls == [
        (
            PATH_ORDER_CREATE,
            {
                "category": "linear",
                "symbol": "BTCUSDT",
                "side": "Sell",
                "orderType": "Market",
                "qty": "0.010",
                "orderLinkId": VALID_CLIENT_ORDER_ID,
                "reduceOnly": True,
                "positionIdx": 0,
            },
        )
    ]
    assert limiter.priorities == [RequestPriority.PROTECTION]


@pytest.mark.asyncio
async def test_explicit_time_in_force_trigger_types_and_plain_decimals(fake_clock: FakeClock) -> None:
    request = _limit_entry(
        qty=Decimal("1E+1"),
        price=Decimal("6.0E+4"),
        time_in_force=TimeInForce.IOC,
        stop_loss=Decimal("5.8E+4"),
        take_profit=None,
        sl_trigger_by=TriggerBy.INDEX_PRICE,
    )
    async with _open(fake_clock, _only(PATH_ORDER_CREATE, _ack())) as (adapter, _limiter, private):
        await adapter.place_order(request, priority=RequestPriority.DATA)
    [(_path, body)] = private.calls
    assert body["qty"] == "10"
    assert body["price"] == "60000"
    assert body["timeInForce"] == "IOC"
    assert body["stopLoss"] == "58000"
    assert body["slTriggerBy"] == "IndexPrice"
    assert "takeProfit" not in body
    assert "tpTriggerBy" not in body
    assert "tpOrderType" not in body


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        _raw(_err(10016, "internal error")),
        _raw("Bad Gateway", status=502),
        _raw(_err(110079, "order is processing")),
    ],
)
async def test_place_order_is_never_retried_after_a_server_failure(
    fake_clock: FakeClock, failure: BybitRawResponse
) -> None:
    async with _open(fake_clock, lambda _p, _q: failure) as (adapter, _limiter, private):
        with pytest.raises(ExchangeUnavailable):
            await adapter.place_order(_limit_entry(), priority=RequestPriority.DATA)
    assert len(private.calls) == 1


@pytest.mark.asyncio
async def test_place_order_is_never_retried_after_a_timeout(fake_clock: FakeClock) -> None:
    def handler(_path: str, _params: Params) -> BybitRawResponse:
        raise ExchangeTimeout(PATH_ORDER_CREATE)

    async with _open(fake_clock, handler) as (adapter, _limiter, private):
        with pytest.raises(ExchangeTimeout):
            await adapter.place_order(_limit_entry(), priority=RequestPriority.DATA)
    assert len(private.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "error"),
    [
        (110007, InsufficientMargin),
        (110094, OrderRejected),
        (10001, OrderRejected),
        (110001, ExchangeOutcomeUnknown),  # «ордер не існує» — код скасування, не відмова створення
        (99999, ExchangeOutcomeUnknown),
    ],
)
async def test_place_order_codes(fake_clock: FakeClock, code: int, error: type[Exception]) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CREATE, _err(code))) as (adapter, _limiter, private):
        with pytest.raises(error) as caught:
            await adapter.place_order(_limit_entry(), priority=RequestPriority.DATA)
    assert type(caught.value) is error
    assert len(private.calls) == 1


@pytest.mark.asyncio
async def test_duplicate_link_id_is_not_a_rejection(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CREATE, _err(110072, "OrderLinkedID is duplicate"))) as (
        adapter,
        _limiter,
        _private,
    ):
        with pytest.raises(DuplicateClientOrderId) as caught:
            await adapter.place_order(_limit_entry(), priority=RequestPriority.DATA)
    assert caught.value.client_order_id == VALID_CLIENT_ORDER_ID


@pytest.mark.asyncio
async def test_recv_window_rejection_on_place_is_unknown_and_asks_for_a_clock_recheck(fake_clock: FakeClock) -> None:
    rechecks: list[None] = []
    async with _open(
        fake_clock,
        _only(PATH_ORDER_CREATE, _err(10002, "invalid request, please check your server timestamp")),
        on_recv_window_rejected=lambda: rechecks.append(None),
    ) as (adapter, _limiter, private):
        with pytest.raises(ExchangeOutcomeUnknown) as caught:
            await adapter.place_order(_limit_entry(), priority=RequestPriority.DATA)
    assert type(caught.value) is ExchangeOutcomeUnknown
    assert rechecks == [None]
    assert len(private.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        {"orderId": "ex-9", "orderLinkId": "tbAAotherorder00000000000000000000000"},
        {"orderId": "", "orderLinkId": VALID_CLIENT_ORDER_ID},
        {"orderLinkId": VALID_CLIENT_ORDER_ID},
        {},
    ],
)
async def test_ack_without_our_link_id_or_order_id_is_malformed(
    fake_clock: FakeClock, result: dict[str, object]
) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CREATE, _ok(result))) as (adapter, _limiter, _private):
        with pytest.raises(MalformedResponse):
            await adapter.place_order(_limit_entry(), priority=RequestPriority.DATA)


@pytest.mark.asyncio
async def test_place_order_refuses_a_non_request_before_the_network(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CREATE, _ack())) as (adapter, _limiter, private):
        with pytest.raises(TypeError):
            await adapter.place_order({"symbol": _SYMBOL}, priority=RequestPriority.DATA)  # type: ignore[arg-type]
    assert private.calls == []


# --- cancel_order ---


@pytest.mark.asyncio
async def test_cancel_by_client_id(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CANCEL, _ack())) as (adapter, limiter, private):
        await adapter.cancel_order(
            priority=RequestPriority.PROTECTION, symbol=_SYMBOL, client_order_id=VALID_CLIENT_ORDER_ID
        )
    assert private.calls == [
        (PATH_ORDER_CANCEL, {"category": "linear", "symbol": "BTCUSDT", "orderLinkId": VALID_CLIENT_ORDER_ID})
    ]
    assert limiter.groups == [EndpointGroup.ORDER_CANCEL]
    assert limiter.priorities == [RequestPriority.PROTECTION]
    assert limiter.retries == [False]


@pytest.mark.asyncio
async def test_cancel_by_exchange_id(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CANCEL, _ack(link_id=""))) as (adapter, _limiter, private):
        await adapter.cancel_order(priority=RequestPriority.RECONCILE, symbol=_SYMBOL, exchange_order_id="ex-9")
    assert private.calls == [(PATH_ORDER_CANCEL, {"category": "linear", "symbol": "BTCUSDT", "orderId": "ex-9"})]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ids",
    [{}, {"client_order_id": VALID_CLIENT_ORDER_ID, "exchange_order_id": "ex-9"}, {"exchange_order_id": ""}],
)
async def test_cancel_needs_exactly_one_identifier_before_the_network(
    fake_clock: FakeClock, ids: dict[str, str]
) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CANCEL, _ack())) as (adapter, _limiter, private):
        with pytest.raises(ValueError, match="ідентифікатор"):
            await adapter.cancel_order(priority=RequestPriority.RECONCILE, symbol=_SYMBOL, **ids)
    assert private.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [110001, 110008, 110010])
async def test_cancel_of_an_inactive_order(fake_clock: FakeClock, code: int) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CANCEL, _err(code, "order not exists or too late to cancel"))) as (
        adapter,
        _limiter,
        _private,
    ):
        with pytest.raises(OrderNotActive):
            await adapter.cancel_order(
                priority=RequestPriority.RECONCILE, symbol=_SYMBOL, client_order_id=VALID_CLIENT_ORDER_ID
            )


@pytest.mark.asyncio
async def test_cancel_is_not_retried_and_a_foreign_ack_is_malformed(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CANCEL, _err(10016))) as (adapter, _limiter, private):
        with pytest.raises(ExchangeUnavailable):
            await adapter.cancel_order(priority=RequestPriority.RECONCILE, symbol=_SYMBOL, exchange_order_id="ex-9")
    assert len(private.calls) == 1
    async with _open(fake_clock, _only(PATH_ORDER_CANCEL, _ack(order_id="ex-other"))) as (adapter, _limiter, _private):
        with pytest.raises(MalformedResponse):
            await adapter.cancel_order(priority=RequestPriority.RECONCILE, symbol=_SYMBOL, exchange_order_id="ex-9")


# --- set_leverage ---


@pytest.mark.asyncio
async def test_set_leverage_sets_both_sides_to_the_same_value(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_SET_LEVERAGE, _ok({}))) as (adapter, limiter, private):
        await adapter.set_leverage(priority=RequestPriority.DATA, symbol=_SYMBOL, leverage=Decimal("2.50"))
    assert private.calls == [
        (PATH_SET_LEVERAGE, {"category": "linear", "symbol": "BTCUSDT", "buyLeverage": "2.50", "sellLeverage": "2.50"})
    ]
    assert limiter.groups == [EndpointGroup.SET_LEVERAGE]
    assert limiter.retries == [True]


@pytest.mark.asyncio
async def test_same_leverage_is_success(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_SET_LEVERAGE, _err(110043, "Set leverage has not been modified."))) as (
        adapter,
        _limiter,
        private,
    ):
        await adapter.set_leverage(priority=RequestPriority.DATA, symbol=_SYMBOL, leverage=Decimal("5"))
    assert len(private.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("leverage", [Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity"), 5, 5.0])
async def test_set_leverage_refuses_invalid_values_before_the_network(fake_clock: FakeClock, leverage: object) -> None:
    async with _open(fake_clock, _only(PATH_SET_LEVERAGE, _ok({}))) as (adapter, _limiter, private):
        with pytest.raises(ValueError, match="leverage"):
            await adapter.set_leverage(
                priority=RequestPriority.DATA,
                symbol=_SYMBOL,
                leverage=leverage,  # type: ignore[arg-type]
            )
    assert private.calls == []


@pytest.mark.asyncio
async def test_set_leverage_rejection_and_no_retry_after_a_server_failure(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_SET_LEVERAGE, _err(110013, "cannot set leverage"))) as (
        adapter,
        _limiter,
        _private,
    ):
        with pytest.raises(RequestRejected):
            await adapter.set_leverage(priority=RequestPriority.DATA, symbol=_SYMBOL, leverage=Decimal("5"))
    async with _open(fake_clock, _only(PATH_SET_LEVERAGE, _err(10016))) as (adapter, _limiter, private):
        with pytest.raises(ExchangeUnavailable):
            await adapter.set_leverage(priority=RequestPriority.DATA, symbol=_SYMBOL, leverage=Decimal("5"))
    assert len(private.calls) == 1


# --- set_margin_mode ---


def _margin_handler(
    margin: str, answer: dict[str, object]
) -> tuple[Callable[[str, Params], BybitRawResponse], list[str]]:
    paths: list[str] = []

    def handler(path: str, _params: Params) -> BybitRawResponse:
        paths.append(path)
        if path == PATH_ACCOUNT_INFO:
            return _raw(_account(margin))
        assert path == PATH_SET_MARGIN_MODE
        return _raw(answer)

    return handler, paths


@pytest.mark.asyncio
async def test_margin_mode_already_isolated_is_success_without_a_write(fake_clock: FakeClock) -> None:
    handler, paths = _margin_handler("ISOLATED_MARGIN", _ok({"reasons": []}))
    async with _open(fake_clock, handler) as (adapter, limiter, _private):
        await adapter.set_margin_mode(priority=RequestPriority.RECONCILE, symbol=_SYMBOL, mode=MarginMode.ISOLATED)
    assert paths == [PATH_ACCOUNT_INFO]
    assert limiter.groups == [EndpointGroup.ACCOUNT_INFO]


@pytest.mark.asyncio
async def test_margin_mode_switches_the_account_to_isolated(fake_clock: FakeClock) -> None:
    handler, paths = _margin_handler("REGULAR_MARGIN", _ok({"reasons": []}))
    async with _open(fake_clock, handler) as (adapter, limiter, private):
        await adapter.set_margin_mode(priority=RequestPriority.RECONCILE, symbol=_SYMBOL, mode=MarginMode.ISOLATED)
    assert paths == [PATH_ACCOUNT_INFO, PATH_SET_MARGIN_MODE]
    assert private.calls[1] == (PATH_SET_MARGIN_MODE, {"setMarginMode": "ISOLATED_MARGIN"})
    assert limiter.groups == [EndpointGroup.ACCOUNT_INFO, EndpointGroup.SET_MARGIN_MODE]
    assert limiter.priorities == [RequestPriority.RECONCILE, RequestPriority.RECONCILE]


@pytest.mark.asyncio
async def test_margin_mode_failure_reports_the_reasons(fake_clock: FakeClock) -> None:
    failure = {
        "retCode": 3400045,
        "retMsg": "Set margin mode failed",
        "result": {"reasons": [{"reasonCode": "3400000", "reasonMsg": "Equity needs to be at least 1000 USDC"}]},
    }
    handler, _paths = _margin_handler("REGULAR_MARGIN", failure)
    async with _open(fake_clock, handler) as (adapter, _limiter, _private):
        with pytest.raises(RequestRejected) as caught:
            await adapter.set_margin_mode(priority=RequestPriority.RECONCILE, symbol=_SYMBOL, mode=MarginMode.ISOLATED)
    assert caught.value.code == "3400045"
    assert "3400000" in caught.value.reason
    assert "Equity needs to be at least 1000 USDC" in caught.value.reason


@pytest.mark.asyncio
async def test_margin_mode_success_code_with_reasons_is_malformed(fake_clock: FakeClock) -> None:
    contradiction = _ok({"reasons": [{"reasonCode": "3400000", "reasonMsg": "not enough equity"}]})
    handler, _paths = _margin_handler("REGULAR_MARGIN", contradiction)
    async with _open(fake_clock, handler) as (adapter, _limiter, _private):
        with pytest.raises(MalformedResponse):
            await adapter.set_margin_mode(priority=RequestPriority.RECONCILE, symbol=_SYMBOL, mode=MarginMode.ISOLATED)


@pytest.mark.asyncio
async def test_cross_margin_is_refused_before_the_network(fake_clock: FakeClock) -> None:
    """Підтримується лише isolated (ТЗ §0, §9.1); на UTA перемикання — на весь акаунт, тож крос не вмикається."""
    handler, paths = _margin_handler("ISOLATED_MARGIN", _ok({"reasons": []}))
    async with _open(fake_clock, handler) as (adapter, _limiter, _private):
        with pytest.raises(AdapterConfigError):
            await adapter.set_margin_mode(priority=RequestPriority.RECONCILE, symbol=_SYMBOL, mode=MarginMode.CROSS)
    assert paths == []


# --- set_position_protection ---


def _protection_handler(
    rows: list[dict[str, object]], answer: dict[str, object] | None = None
) -> Callable[[str, Params], BybitRawResponse]:
    def handler(path: str, _params: Params) -> BybitRawResponse:
        if path == PATH_POSITION_LIST:
            return _raw(_list(rows))
        assert path == PATH_TRADING_STOP
        return _raw(_ok({}) if answer is None else answer)

    return handler


@pytest.mark.asyncio
async def test_protection_already_in_place_is_success_without_a_write(fake_clock: FakeClock) -> None:
    rows = [_position_row(stopLoss="59000.00", takeProfit="64000")]
    async with _open(fake_clock, _protection_handler(rows)) as (adapter, limiter, private):
        await adapter.set_position_protection(_protection())
    assert [path for path, _params in private.calls] == [PATH_POSITION_LIST]
    assert private.calls[0][1] == {"category": "linear", "limit": "200", "symbol": "BTCUSDT"}
    assert limiter.priorities == [RequestPriority.PROTECTION]
    assert limiter.groups == [EndpointGroup.POSITION_LIST]


@pytest.mark.asyncio
async def test_new_stop_loss_is_sent_in_full_mode_with_mark_trigger(fake_clock: FakeClock) -> None:
    rows = [_position_row(stopLoss="58000", takeProfit="64000")]
    async with _open(fake_clock, _protection_handler(rows)) as (adapter, limiter, private):
        await adapter.set_position_protection(_protection())
    assert private.calls[1] == (
        PATH_TRADING_STOP,
        {
            "category": "linear",
            "symbol": "BTCUSDT",
            "tpslMode": "Full",
            "positionIdx": 0,
            "stopLoss": "59000",
            "slTriggerBy": "MarkPrice",
            "slOrderType": "Market",
            "takeProfit": "64000",
            "tpTriggerBy": "LastPrice",
            "tpOrderType": "Market",
        },
    )
    assert limiter.groups == [EndpointGroup.POSITION_LIST, EndpointGroup.TRADING_STOP]
    assert limiter.priorities == [RequestPriority.PROTECTION, RequestPriority.PROTECTION]
    assert limiter.retries == [True, True]


@pytest.mark.asyncio
async def test_without_take_profit_the_tp_is_neither_sent_nor_compared(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _protection_handler([_position_row(stopLoss="59000", takeProfit="70000")])) as (
        adapter,
        _limiter,
        private,
    ):
        await adapter.set_position_protection(_protection(take_profit=None))
    assert len(private.calls) == 1
    async with _open(fake_clock, _protection_handler([_position_row(stopLoss="58000", takeProfit="70000")])) as (
        adapter,
        _limiter,
        private,
    ):
        await adapter.set_position_protection(_protection(take_profit=None))
    [_read, (path, body)] = private.calls
    assert path == PATH_TRADING_STOP
    assert "takeProfit" not in body
    assert "tpTriggerBy" not in body
    assert body["stopLoss"] == "59000"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "code"),
    [
        ([], ZERO_POSITION_CODE),
        ([_position_row(size="0", side="", stopLoss="", takeProfit="")], ZERO_POSITION_CODE),
        ([_position_row(side="Sell")], POSITION_SIDE_MISMATCH_CODE),
    ],
)
async def test_protection_without_a_matching_position_is_rejected_without_a_write(
    fake_clock: FakeClock, rows: list[dict[str, object]], code: str
) -> None:
    async with _open(fake_clock, _protection_handler(rows)) as (adapter, _limiter, private):
        with pytest.raises(RequestRejected) as caught:
            await adapter.set_position_protection(_protection())
    assert caught.value.code == code
    assert [path for path, _params in private.calls] == [PATH_POSITION_LIST]


@pytest.mark.asyncio
async def test_hedge_mode_row_is_a_config_error(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _protection_handler([_position_row(positionIdx=1)])) as (adapter, _limiter, private):
        with pytest.raises(AdapterConfigError):
            await adapter.set_position_protection(_protection())
    assert [path for path, _params in private.calls] == [PATH_POSITION_LIST]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "error"),
    [
        (_err(10001, "StopLoss for Buy position should lower than base_price"), RequestRejected),
        (_err(34040, "not modified"), ExchangeOutcomeUnknown),  # TODO(verify-bybit): у документації v5 коду немає
        (_err(10016, "internal error"), ExchangeUnavailable),
    ],
)
async def test_trading_stop_answers(fake_clock: FakeClock, answer: dict[str, object], error: type[Exception]) -> None:
    rows = [_position_row(stopLoss="58000")]
    async with _open(fake_clock, _protection_handler(rows, answer)) as (adapter, _limiter, private):
        with pytest.raises(error) as caught:
            await adapter.set_position_protection(_protection())
    assert type(caught.value) is error
    # Запис не повторюється: одне читання й одна спроба запису.
    assert [path for path, _params in private.calls] == [PATH_POSITION_LIST, PATH_TRADING_STOP]


@pytest.mark.asyncio
async def test_protection_read_is_retried_but_the_write_is_not(fake_clock: FakeClock) -> None:
    reads = 0

    def handler(path: str, _params: Params) -> BybitRawResponse:
        nonlocal reads
        if path == PATH_POSITION_LIST:
            reads += 1
            if reads == 1:
                raise ExchangeTimeout(path)
            return _raw(_list([_position_row(stopLoss="58000")]))
        raise ExchangeTimeout(path)

    async with _open(fake_clock, handler) as (adapter, _limiter, private):
        with pytest.raises(ExchangeTimeout):
            await adapter.set_position_protection(_protection())
    assert [path for path, _params in private.calls] == [PATH_POSITION_LIST, PATH_POSITION_LIST, PATH_TRADING_STOP]


# --- приватний потік ---


@pytest.mark.asyncio
async def test_private_stream_is_refused_explicitly_until_t3_5(fake_clock: FakeClock) -> None:
    async with _open(fake_clock, _only(PATH_ORDER_CREATE, _ack())) as (adapter, _limiter, private):
        stream = adapter.subscribe_private_stream()
        with pytest.raises(AdapterConfigError, match=r"T3\.5"):
            await anext(stream)
    assert private.calls == []


def test_bybit_adapter_satisfies_the_exchange_adapter_protocol() -> None:
    from app.exchange.adapter import ExchangeAdapter

    def _as_adapter(adapter: ExchangeAdapter) -> ExchangeAdapter:
        return adapter

    assert callable(_as_adapter)
    # Статична перевірка: mypy відхилить цей рядок, якщо `BybitAdapter` не реалізує протокол повністю.
    _check: Callable[[BybitAdapter], ExchangeAdapter] = _as_adapter
    assert _check is _as_adapter


# --- Знахідки рев'ю: ліміт, не-200, гілки захисту ---


_LIMITED = [
    _raw(_err(10006, "Too many visits")),
    _raw("Too Many Requests", status=429),
    _raw("Access too frequent. Please try again in 5 minutes.", status=403),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("limited", _LIMITED)
async def test_place_and_cancel_are_not_repeated_after_a_rate_limit(
    fake_clock: FakeClock, limited: BybitRawResponse
) -> None:
    async with _open(fake_clock, lambda _p, _q: limited) as (adapter, _limiter, private):
        with pytest.raises(RateLimited):
            await adapter.place_order(_limit_entry(), priority=RequestPriority.DATA)
        with pytest.raises(RateLimited):
            await adapter.cancel_order(priority=RequestPriority.PROTECTION, symbol=_SYMBOL, exchange_order_id="ex-9")
    assert [path for path, _params in private.calls] == [PATH_ORDER_CREATE, PATH_ORDER_CANCEL]


@pytest.mark.asyncio
async def test_idempotent_writes_are_repeated_by_the_limiter_after_a_rate_limit(fake_clock: FakeClock) -> None:
    answers = {PATH_TRADING_STOP: [_raw(_err(10006, "Too many visits")), _raw(_ok({}))]}
    answers[PATH_SET_LEVERAGE] = [_raw(_err(10006, "Too many visits")), _raw(_ok({}))]

    def handler(path: str, _params: Params) -> BybitRawResponse:
        if path == PATH_POSITION_LIST:
            return _raw(_list([_position_row(stopLoss="58000")]))
        return answers[path].pop(0)

    async with _open(fake_clock, handler) as (adapter, _limiter, private):
        await adapter.set_position_protection(_protection())
        await adapter.set_leverage(priority=RequestPriority.DATA, symbol=_SYMBOL, leverage=Decimal("5"))
    assert [path for path, _params in private.calls] == [
        PATH_POSITION_LIST,
        PATH_TRADING_STOP,
        PATH_TRADING_STOP,
        PATH_SET_LEVERAGE,
        PATH_SET_LEVERAGE,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "answer",
    [
        _raw("Bad Request", status=400),
        _raw("Unauthorized", status=401),
        _raw("Forbidden", status=403),
        _raw("Not Found", status=404),
        _raw("<html>maintenance</html>"),
        _raw({"retCode": 0, "retMsg": "OK", "time": 1}),
        _raw({"retCode": 0, "retMsg": "OK", "result": {"orderId": "ex-9", "orderLinkId": VALID_CLIENT_ORDER_ID}}),
        _raw({"retMsg": "no code"}),
    ],
)
async def test_place_order_never_reads_a_non_answer_as_a_definite_rejection(
    fake_clock: FakeClock, answer: BybitRawResponse
) -> None:
    async with _open(fake_clock, lambda _p, _q: answer) as (adapter, _limiter, private):
        with pytest.raises(ExchangeError) as caught:
            await adapter.place_order(_limit_entry(), priority=RequestPriority.DATA)
    assert not is_definite_rejection(caught.value)
    assert len(private.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "request_fields"),
    [
        ({"stopLoss": "59000", "takeProfit": "63000"}, {}),  # SL той самий, TP інший
        ({"stopLoss": "59000", "takeProfit": ""}, {}),  # TP запитано, на позиції його немає
        ({"stopLoss": "", "takeProfit": "64000"}, {}),  # SL немає зовсім
    ],
)
async def test_protection_is_written_when_anything_requested_differs(
    fake_clock: FakeClock, row: dict[str, object], request_fields: dict[str, object]
) -> None:
    async with _open(fake_clock, _protection_handler([_position_row(**row)])) as (adapter, _limiter, private):
        await adapter.set_position_protection(_protection(**request_fields))
    assert [path for path, _params in private.calls] == [PATH_POSITION_LIST, PATH_TRADING_STOP]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        [_position_row(), _position_row(side="Sell")],  # дві ненульові позиції one-way одного символу
        [_position_row(symbol="ETHUSDT")],  # чужий символ
        [_position_row(size="-0.010")],
    ],
)
async def test_protection_does_not_guess_which_position_it_protects(
    fake_clock: FakeClock, rows: list[dict[str, object]]
) -> None:
    async with _open(fake_clock, _protection_handler(rows)) as (adapter, _limiter, private):
        with pytest.raises(MalformedResponse):
            await adapter.set_position_protection(_protection())
    assert [path for path, _params in private.calls] == [PATH_POSITION_LIST]
