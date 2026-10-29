"""Доменні типи біржового шару (`app/exchange/types.py`, T1.1; ТЗ §3, §4.3, §7.2, §9.1).

Перевіряємо, що помилки нормалізації (float замість Decimal, секунди замість мс, сирий рядок замість enum) і
внутрішньо неузгоджені факти біржі не проходять межу адаптера, а запити з SL/TP не з того боку не можна створити.
"""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError
from sqlalchemy import CheckConstraint, Table

from app.exchange import types
from app.exchange.types import (
    CLIENT_ORDER_ID_LEN,
    CLIENT_ORDER_ID_RE,
    Balance,
    Candle,
    ClientOrderIdOrigin,
    ExchangeOrderStatus,
    Fill,
    Instrument,
    MarginMode,
    Order,
    OrderType,
    PlaceOrderRequest,
    Position,
    PositionProtection,
    PositionSide,
    PositionUpdate,
    Side,
    StopOrderType,
    Timeframe,
    TimeInForce,
    TriggerBy,
    client_order_host_id,
    client_order_id_origin,
    new_client_order_id,
)
from app.trading import models as trading_models
from tests.exchange.factories import FAKE_NOW_MS, SYMBOL, VALID_CLIENT_ORDER_ID

BACKEND_DIR = Path(__file__).resolve().parents[2]
D = Decimal


# ---------------------------------------------------------------- базові словники валідних моделей


def _candle_kwargs(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "symbol": SYMBOL,
        "timeframe": Timeframe.H1,
        "open_time_ms": FAKE_NOW_MS,
        "open": D("100"),
        "high": D("110"),
        "low": D("90"),
        "close": D("105"),
        "volume": D("1"),
    }
    return {**base, **over}


def _instrument_kwargs(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "symbol": SYMBOL,
        "tick_size": D("0.1"),
        "qty_step": D("0.001"),
        "min_qty": D("0.001"),
        "min_notional": D("5"),
        "max_qty": D("100"),
        "max_leverage": D("100"),
        "status": "Trading",
    }
    return {**base, **over}


def _order_kwargs(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "symbol": SYMBOL,
        "exchange_order_id": "1001",
        "client_order_id": None,
        "side": Side.BUY,
        "order_type": OrderType.LIMIT,
        "status": ExchangeOrderStatus.NEW,
        "qty": D("0.010"),
        "filled_qty": D("0"),
        "avg_fill_price": None,
        "price": D("59000"),
        "time_in_force": TimeInForce.GTC,
        "reduce_only": False,
        "created_at_ms": FAKE_NOW_MS,
        "updated_at_ms": FAKE_NOW_MS,
    }
    return {**base, **over}


def _fill_kwargs(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "exec_id": "e-1",
        "symbol": SYMBOL,
        "exchange_order_id": "1001",
        "client_order_id": None,
        "side": Side.BUY,
        "price": D("60000"),
        "qty": D("0.01"),
        "fee": D("0.33"),
        "fee_currency": "USDT",
        "is_maker": False,
        "exec_type": types.ExecType.TRADE,
        "ts_ms": FAKE_NOW_MS,
    }
    return {**base, **over}


def _position_kwargs(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "symbol": SYMBOL,
        "side": PositionSide.LONG,
        "qty": D("0.01"),
        "entry_price": D("60000"),
        "mark_price": D("60000"),
        "liq_price": None,
        "leverage": D("1"),
        "margin_mode": MarginMode.ISOLATED,
        "stop_loss": None,
        "take_profit": None,
        "unrealized_pnl": D("0"),
        "updated_at_ms": FAKE_NOW_MS,
    }
    return {**base, **over}


def _balance_kwargs(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "coin": "USDT",
        "wallet_balance": D("10000"),
        "equity": D("10000"),
        "available_balance": D("10000"),
        "unrealized_pnl": D("0"),
    }
    return {**base, **over}


def _request_kwargs(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "symbol": SYMBOL,
        "client_order_id": VALID_CLIENT_ORDER_ID,
        "side": Side.BUY,
        "order_type": OrderType.MARKET,
        "qty": D("0.01"),
        "reduce_only": False,
    }
    return {**base, **over}


def _protection_kwargs(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {"symbol": SYMBOL, "position_side": PositionSide.LONG, "stop_loss": D("58000")}
    return {**base, **over}


# ---------------------------------------------------------------- strict: Decimal, мс, enum


@pytest.mark.parametrize("bad", [1.0, 1, "1"], ids=["float", "int", "str"])
@pytest.mark.parametrize(
    ("model", "kwargs", "field"),
    [
        (Candle, _candle_kwargs, "close"),
        (Order, _order_kwargs, "qty"),
        (Fill, _fill_kwargs, "fee"),
        (Position, _position_kwargs, "entry_price"),
        (Balance, _balance_kwargs, "equity"),
        (PlaceOrderRequest, _request_kwargs, "qty"),
    ],
)
def test_decimal_fields_reject_non_decimal(model: type[Any], kwargs: Any, field: str, bad: object) -> None:
    with pytest.raises(ValidationError) as exc:
        model.model_validate(kwargs(**{field: bad}))
    assert [e["loc"] for e in exc.value.errors()] == [(field,)]


@pytest.mark.parametrize("bad", ["NaN", "-NaN", "sNaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize(
    ("model", "kwargs", "field"),
    [
        # Поля без обмежень (gt/ge): NaN/Infinity мають відхилятися самі по собі, а не через `gt=0`.
        (Balance, _balance_kwargs, "equity"),
        (Balance, _balance_kwargs, "wallet_balance"),
        (Fill, _fill_kwargs, "fee"),
        (Position, _position_kwargs, "unrealized_pnl"),
        (
            types.FundingRate,
            lambda **o: {"symbol": SYMBOL, "funding_time_ms": FAKE_NOW_MS, "rate": D("0"), **o},
            "rate",
        ),
        (Candle, _candle_kwargs, "close"),
    ],
)
def test_decimal_fields_reject_nan_and_infinity(model: type[Any], kwargs: Any, field: str, bad: str) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(kwargs(**{field: D(bad)}))


def test_unconstrained_decimals_accept_negative_values() -> None:
    # Від'ємна equity/PnL і rebate мейкера (від'ємна комісія) — легальні факти біржі.
    balance = Balance.model_validate(_balance_kwargs(equity=D("-5.5"), unrealized_pnl=D("-15.5")))
    assert balance.equity == D("-5.5")
    fill = Fill.model_validate(_fill_kwargs(fee=D("-0.12")))
    assert fill.fee == D("-0.12")


@pytest.mark.parametrize("bad", [True, 1.0, float(FAKE_NOW_MS), -1, str(FAKE_NOW_MS)])
@pytest.mark.parametrize(
    ("model", "kwargs", "field"),
    [
        (Candle, _candle_kwargs, "open_time_ms"),
        (Order, _order_kwargs, "created_at_ms"),
        (Fill, _fill_kwargs, "ts_ms"),
        (Position, _position_kwargs, "updated_at_ms"),
    ],
)
def test_millisecond_fields_reject_bool_float_negative_and_str(
    model: type[Any], kwargs: Any, field: str, bad: object
) -> None:
    with pytest.raises(ValidationError) as exc:
        model.model_validate(kwargs(**{field: bad}))
    assert [e["loc"] for e in exc.value.errors()] == [(field,)]


def test_stream_connected_rejects_bool_timestamp() -> None:
    with pytest.raises(ValidationError):
        types.StreamConnected.model_validate({"connected_at_ms": True})


@pytest.mark.parametrize(
    ("model", "kwargs", "field", "raw"),
    [
        (PlaceOrderRequest, _request_kwargs, "side", "BUY"),
        (PlaceOrderRequest, _request_kwargs, "order_type", "MARKET"),
        (Candle, _candle_kwargs, "timeframe", "1h"),
        (Order, _order_kwargs, "status", "NEW"),
        (Position, _position_kwargs, "margin_mode", "ISOLATED"),
        (PositionProtection, _protection_kwargs, "sl_trigger_by", "MARK_PRICE"),
    ],
)
def test_enum_fields_reject_raw_strings(model: type[Any], kwargs: Any, field: str, raw: str) -> None:
    with pytest.raises(ValidationError) as exc:
        model.model_validate(kwargs(**{field: raw}))
    assert [e["loc"] for e in exc.value.errors()] == [(field,)]


def test_models_are_frozen() -> None:
    balance = Balance.model_validate(_balance_kwargs())
    with pytest.raises(ValidationError):
        balance.equity = D("1")  # type: ignore[misc]
    assert balance.equity == D("10000")


@pytest.mark.parametrize(
    ("model", "kwargs"),
    [
        (Candle, _candle_kwargs),
        (Order, _order_kwargs),
        (Fill, _fill_kwargs),
        (Position, _position_kwargs),
        (Balance, _balance_kwargs),
        (PlaceOrderRequest, _request_kwargs),
        (PositionProtection, _protection_kwargs),
    ],
)
def test_models_forbid_extra_fields(model: type[Any], kwargs: Any) -> None:
    with pytest.raises(ValidationError) as exc:
        model.model_validate(kwargs(category="linear"))
    assert [e["type"] for e in exc.value.errors()] == ["extra_forbidden"]


@pytest.mark.parametrize("symbol", ["BTCUSDT", "btc/usdt:usdt", "BTC/USDT", "BTC/USDT:USDT\n", ""])
def test_symbol_must_be_canonical_ccxt_format(symbol: str) -> None:
    with pytest.raises(ValidationError):
        Candle.model_validate(_candle_kwargs(symbol=symbol))


# ---------------------------------------------------------------- Timeframe / Candle

EXPECTED_DURATIONS_MS = {
    Timeframe.M1: 60_000,
    Timeframe.M5: 300_000,
    Timeframe.M15: 900_000,
    Timeframe.M30: 1_800_000,
    Timeframe.H1: 3_600_000,
    Timeframe.H2: 7_200_000,
    Timeframe.H4: 14_400_000,
    Timeframe.H6: 21_600_000,
    Timeframe.H12: 43_200_000,
    Timeframe.D1: 86_400_000,
}


def test_expected_durations_cover_every_timeframe() -> None:
    assert set(EXPECTED_DURATIONS_MS) == set(Timeframe)


@pytest.mark.parametrize(("timeframe", "duration"), list(EXPECTED_DURATIONS_MS.items()))
def test_timeframe_duration_ms(timeframe: Timeframe, duration: int) -> None:
    assert timeframe.duration_ms == duration
    assert type(timeframe.duration_ms) is int


def test_valid_candle_is_accepted_and_close_time_is_open_plus_duration() -> None:
    candle = Candle.model_validate(_candle_kwargs())
    assert candle.close == D("105")
    # Свічка — напівінтервал [open, open + tf): закрита, коли open + tf ≤ now (контракт fetch_candles).
    assert candle.close_time_ms == FAKE_NOW_MS + 3_600_000


def test_flat_candle_with_all_prices_equal_is_accepted() -> None:
    candle = Candle.model_validate(_candle_kwargs(open=D("100"), high=D("100"), low=D("100"), close=D("100")))
    assert candle.high == candle.low == D("100")


@pytest.mark.parametrize(
    "over",
    [
        {"high": D("99")},  # high < open
        {"high": D("104"), "open": D("100")},  # high < close
        {"low": D("101")},  # low > open
        {"open": D("108"), "close": D("95"), "low": D("96")},  # low > close
        {"high": D("89"), "open": D("89"), "close": D("89")},  # high < low
    ],
    ids=["high<open", "high<close", "low>open", "low>close", "high<low"],
)
def test_candle_rejects_inconsistent_ohlc(over: dict[str, Decimal]) -> None:
    with pytest.raises(ValidationError):
        Candle.model_validate(_candle_kwargs(**over))


@pytest.mark.parametrize(
    ("timeframe", "offset_ms"),
    [
        (Timeframe.M1, 1),
        (Timeframe.M5, 60_000),
        (Timeframe.H1, 1_800_000),
        (Timeframe.H4, 3_600_000),
        (Timeframe.D1, 3_600_000),
    ],
)
def test_candle_open_time_must_be_aligned_to_timeframe(timeframe: Timeframe, offset_ms: int) -> None:
    with pytest.raises(ValidationError):
        Candle.model_validate(_candle_kwargs(timeframe=timeframe, open_time_ms=FAKE_NOW_MS + offset_ms))


@pytest.mark.parametrize("timeframe", list(Timeframe))
def test_candle_aligned_open_time_is_accepted_for_every_timeframe(timeframe: Timeframe) -> None:
    # FAKE_NOW_MS — опівніч UTC, тож кратний кожному таймфрейму; + 1 тривалість теж вирівняна.
    open_time = FAKE_NOW_MS + EXPECTED_DURATIONS_MS[timeframe]
    candle = Candle.model_validate(_candle_kwargs(timeframe=timeframe, open_time_ms=open_time))
    assert candle.close_time_ms == open_time + EXPECTED_DURATIONS_MS[timeframe]


# ---------------------------------------------------------------- Instrument


def test_instrument_rejects_max_qty_below_min_qty() -> None:
    with pytest.raises(ValidationError):
        Instrument.model_validate(_instrument_kwargs(min_qty=D("0.01"), max_qty=D("0.005")))


def test_instrument_accepts_max_qty_equal_to_min_qty_and_none() -> None:
    equal = Instrument.model_validate(_instrument_kwargs(min_qty=D("0.01"), max_qty=D("0.01")))
    assert equal.max_qty == D("0.01")
    unbounded = Instrument.model_validate(_instrument_kwargs(max_qty=None))
    assert unbounded.max_qty is None


@pytest.mark.parametrize(
    "over",
    [{"tick_size": D("0")}, {"qty_step": D("0")}, {"min_qty": D("0")}, {"max_leverage": D("0.5")}, {"status": ""}],
)
def test_instrument_rejects_non_positive_rules(over: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Instrument.model_validate(_instrument_kwargs(**over))


# ---------------------------------------------------------------- Order (факт біржі)


def test_new_order_with_zero_fill_is_accepted() -> None:
    order = Order.model_validate(_order_kwargs())
    assert order.status is ExchangeOrderStatus.NEW
    assert order.filled_qty == D("0")


def test_order_accepts_foreign_or_manual_client_order_id() -> None:
    # Факти біржі не фільтруються за форматом id: ручні й чужі ордери має побачити звірка (ТЗ §11.4).
    order = Order.model_validate(_order_kwargs(client_order_id="manual-1"))
    assert order.client_order_id == "manual-1"


def test_order_rejects_empty_client_order_id() -> None:
    with pytest.raises(ValidationError):
        Order.model_validate(_order_kwargs(client_order_id=""))


@pytest.mark.parametrize(
    "over",
    [
        # filled > qty
        {"status": ExchangeOrderStatus.PARTIALLY_FILLED, "filled_qty": D("0.011"), "avg_fill_price": D("59000")},
        # FILLED з неповним виконанням
        {"status": ExchangeOrderStatus.FILLED, "filled_qty": D("0.009"), "avg_fill_price": D("59000")},
        # FILLED без виконання
        {"status": ExchangeOrderStatus.FILLED},
        # NEW з виконанням
        {"status": ExchangeOrderStatus.NEW, "filled_qty": D("0.001"), "avg_fill_price": D("59000")},
        # UNTRIGGERED з виконанням
        {
            "status": ExchangeOrderStatus.UNTRIGGERED,
            "filled_qty": D("0.001"),
            "avg_fill_price": D("59000"),
            "stop_order_type": StopOrderType.STOP_LOSS,
            "trigger_price": D("58000"),
        },
        # PARTIALLY_FILLED без виконання
        {"status": ExchangeOrderStatus.PARTIALLY_FILLED},
        # PARTIALLY_FILLED з повним виконанням
        {"status": ExchangeOrderStatus.PARTIALLY_FILLED, "filled_qty": D("0.010"), "avg_fill_price": D("59000")},
        # avg_fill_price без виконання
        {"avg_fill_price": D("59000")},
        # виконання без avg_fill_price
        {"status": ExchangeOrderStatus.CANCELED, "filled_qty": D("0.004")},
        # qty = 0 у звичайного (не умовного) ордера
        {"qty": D("0")},
    ],
    ids=[
        "filled>qty",
        "FILLED-partial",
        "FILLED-zero",
        "NEW-filled",
        "UNTRIGGERED-filled",
        "PARTIALLY_FILLED-zero",
        "PARTIALLY_FILLED-full",
        "avg-without-fill",
        "fill-without-avg",
        "qty0-not-conditional",
    ],
)
def test_order_rejects_inconsistent_fill_state(over: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Order.model_validate(_order_kwargs(**over))


def test_filled_order_with_full_fill_is_accepted() -> None:
    order = Order.model_validate(
        _order_kwargs(status=ExchangeOrderStatus.FILLED, filled_qty=D("0.010"), avg_fill_price=D("59000"))
    )
    assert order.filled_qty == order.qty == D("0.010")


def test_partially_filled_order_strictly_between_zero_and_qty_is_accepted() -> None:
    order = Order.model_validate(
        _order_kwargs(status=ExchangeOrderStatus.PARTIALLY_FILLED, filled_qty=D("0.004"), avg_fill_price=D("59000"))
    )
    assert order.filled_qty == D("0.004")


def test_canceled_order_with_partial_fill_is_accepted() -> None:
    # ТЗ §7.1: частковий fill + скасування залишку = CANCELED з filled_qty > 0.
    order = Order.model_validate(
        _order_kwargs(status=ExchangeOrderStatus.CANCELED, filled_qty=D("0.004"), avg_fill_price=D("59000"))
    )
    assert order.status is ExchangeOrderStatus.CANCELED
    assert order.filled_qty == D("0.004")


def test_canceled_order_without_fill_is_accepted() -> None:
    order = Order.model_validate(_order_kwargs(status=ExchangeOrderStatus.CANCELED))
    assert order.filled_qty == D("0")
    assert order.avg_fill_price is None


def test_conditional_order_may_have_zero_qty() -> None:
    order = Order.model_validate(
        _order_kwargs(
            status=ExchangeOrderStatus.UNTRIGGERED,
            qty=D("0"),
            order_type=OrderType.MARKET,
            price=None,
            stop_order_type=StopOrderType.STOP_LOSS,
            trigger_price=D("58000"),
        )
    )
    assert order.qty == D("0")
    assert order.stop_order_type is StopOrderType.STOP_LOSS


@pytest.mark.parametrize(
    ("status", "terminal"),
    [
        (ExchangeOrderStatus.NEW, False),
        (ExchangeOrderStatus.PARTIALLY_FILLED, False),
        (ExchangeOrderStatus.UNTRIGGERED, False),
        # Bybit відносить Triggered до закритих, але ордер одразу стає New і ще може виконатися — не термінальний.
        (ExchangeOrderStatus.TRIGGERED, False),
        # Невідомий статус — fail-safe: вважаємо, що ордер ще може виконатися.
        (ExchangeOrderStatus.UNKNOWN, False),
        (ExchangeOrderStatus.FILLED, True),
        (ExchangeOrderStatus.CANCELED, True),
        (ExchangeOrderStatus.REJECTED, True),
        (ExchangeOrderStatus.DEACTIVATED, True),
    ],
)
def test_order_status_terminality(status: ExchangeOrderStatus, terminal: bool) -> None:
    assert status.is_terminal is terminal


def test_terminality_test_covers_every_status() -> None:
    covered = {
        ExchangeOrderStatus.NEW,
        ExchangeOrderStatus.PARTIALLY_FILLED,
        ExchangeOrderStatus.UNTRIGGERED,
        ExchangeOrderStatus.TRIGGERED,
        ExchangeOrderStatus.UNKNOWN,
        ExchangeOrderStatus.FILLED,
        ExchangeOrderStatus.CANCELED,
        ExchangeOrderStatus.REJECTED,
        ExchangeOrderStatus.DEACTIVATED,
    }
    assert covered == set(ExchangeOrderStatus)


def test_unknown_status_order_keeps_fill_data_without_status_invariants() -> None:
    """Статус, якого адаптер не знає, не «засліплює» весь список ордерів (`MalformedResponse`) — і не губить fills."""
    order = Order.model_validate(
        _order_kwargs(status=ExchangeOrderStatus.UNKNOWN, filled_qty=D("0.004"), avg_fill_price=D("59000"))
    )
    assert (order.status, order.filled_qty, order.status.is_terminal) == (
        ExchangeOrderStatus.UNKNOWN,
        D("0.004"),
        False,
    )


def test_unknown_status_order_still_rejects_overfill() -> None:
    with pytest.raises(ValidationError):
        Order.model_validate(
            _order_kwargs(status=ExchangeOrderStatus.UNKNOWN, filled_qty=D("0.011"), avg_fill_price=D("59000"))
        )


# ---------------------------------------------------------------- PositionUpdate


def test_position_update_rejects_symbol_mismatch() -> None:
    position = Position.model_validate(_position_kwargs())
    with pytest.raises(ValidationError):
        PositionUpdate(symbol="ETH/USDT:USDT", position=position)


def test_position_update_accepts_matching_symbol_and_closed_position() -> None:
    position = Position.model_validate(_position_kwargs())
    assert PositionUpdate(symbol=SYMBOL, position=position).position == position
    assert PositionUpdate(symbol=SYMBOL, position=None).position is None


# ---------------------------------------------------------------- PlaceOrderRequest: clientOrderId


def test_request_accepts_our_client_order_id_format() -> None:
    request = PlaceOrderRequest.model_validate(_request_kwargs())
    assert request.client_order_id == VALID_CLIENT_ORDER_ID
    assert len(VALID_CLIENT_ORDER_ID) == CLIENT_ORDER_ID_LEN == 36


@pytest.mark.parametrize(
    "bad",
    [
        VALID_CLIENT_ORDER_ID[:-1],  # 35 символів
        VALID_CLIENT_ORDER_ID + "0",  # 37 символів
        VALID_CLIENT_ORDER_ID.upper(),  # великі літери
        "tbAA" + VALID_CLIENT_ORDER_ID[4:],  # великі літери в HOST_ID
        VALID_CLIENT_ORDER_ID[:-1] + "A",  # велика літера в hex
        "xx" + VALID_CLIENT_ORDER_ID[2:],  # інший префікс
        VALID_CLIENT_ORDER_ID + "\n",  # `$` у Python re пропускає кінцевий \n
        VALID_CLIENT_ORDER_ID[:-1] + "\n",
        "tba-" + VALID_CLIENT_ORDER_ID[4:],  # дефіс у HOST_ID
        "tbaa" + "0123456789abcdef-123456789abcdef",  # uuid з дефісом
        "",
    ],
    ids=[
        "35",
        "37",
        "upper",
        "upper-host",
        "upper-hex",
        "prefix",
        "newline",
        "newline-35",
        "dash-host",
        "dash",
        "empty",
    ],
)
def test_request_rejects_foreign_client_order_id_format(bad: str) -> None:
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(_request_kwargs(client_order_id=bad))


# ---------------------------------------------------------------- PlaceOrderRequest: тип, ціна, TIF, reduceOnly


def test_limit_requires_price() -> None:
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(_request_kwargs(order_type=OrderType.LIMIT))


def test_market_rejects_price() -> None:
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(_request_kwargs(price=D("60000")))


@pytest.mark.parametrize("tif", list(TimeInForce))
def test_market_rejects_time_in_force(tif: TimeInForce) -> None:
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(_request_kwargs(time_in_force=tif))


@pytest.mark.parametrize("tif", [None, *TimeInForce])
def test_limit_with_price_accepts_any_time_in_force(tif: TimeInForce | None) -> None:
    request = PlaceOrderRequest.model_validate(
        _request_kwargs(order_type=OrderType.LIMIT, price=D("59000"), time_in_force=tif)
    )
    assert request.price == D("59000")
    assert request.time_in_force == tif


@pytest.mark.parametrize("over", [{"stop_loss": D("61000")}, {"take_profit": D("58000")}])
def test_reduce_only_rejects_stop_loss_and_take_profit(over: dict[str, Decimal]) -> None:
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(_request_kwargs(side=Side.SELL, reduce_only=True, **over))


def test_reduce_only_without_protection_is_accepted() -> None:
    request = PlaceOrderRequest.model_validate(_request_kwargs(side=Side.SELL, reduce_only=True))
    assert request.reduce_only is True
    assert request.stop_loss is None
    assert request.take_profit is None


def test_request_trigger_by_defaults() -> None:
    request = PlaceOrderRequest.model_validate(_request_kwargs())
    assert request.sl_trigger_by is TriggerBy.MARK_PRICE
    assert request.tp_trigger_by is TriggerBy.LAST_PRICE


# ---------------------------------------------------------------- PlaceOrderRequest: орієнтація SL/TP


@pytest.mark.parametrize(
    ("side", "sl", "tp"),
    [(Side.BUY, "58000", "63000"), (Side.SELL, "63000", "58000")],
)
def test_market_with_correct_sl_tp_orientation_is_accepted(side: Side, sl: str, tp: str) -> None:
    request = PlaceOrderRequest.model_validate(_request_kwargs(side=side, stop_loss=D(sl), take_profit=D(tp)))
    assert request.stop_loss == D(sl)
    assert request.take_profit == D(tp)


@pytest.mark.parametrize(
    ("side", "sl", "tp"),
    [
        (Side.BUY, "63000", "58000"),  # дзеркально для LONG
        (Side.BUY, "60000", "60000"),  # рівність
        (Side.SELL, "58000", "63000"),  # помилка знаку для SHORT
        (Side.SELL, "60000", "60000"),
    ],
)
def test_market_with_wrong_sl_tp_orientation_is_rejected(side: Side, sl: str, tp: str) -> None:
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(_request_kwargs(side=side, stop_loss=D(sl), take_profit=D(tp)))


@pytest.mark.parametrize(
    ("side", "sl", "tp"),
    [
        (Side.BUY, "58000", "63000"),
        (Side.BUY, "58000", None),
        (Side.BUY, None, "63000"),
        (Side.SELL, "63000", "58000"),
        (Side.SELL, "63000", None),
        (Side.SELL, None, "58000"),
    ],
)
def test_limit_with_sl_tp_on_correct_side_of_price_is_accepted(side: Side, sl: str | None, tp: str | None) -> None:
    request = PlaceOrderRequest.model_validate(
        _request_kwargs(
            side=side,
            order_type=OrderType.LIMIT,
            price=D("60000"),
            stop_loss=D(sl) if sl else None,
            take_profit=D(tp) if tp else None,
        )
    )
    assert request.stop_loss == (D(sl) if sl else None)
    assert request.take_profit == (D(tp) if tp else None)


@pytest.mark.parametrize(
    ("side", "sl", "tp"),
    [
        (Side.BUY, "61000", "63000"),  # SL вище ціни входу LONG
        (Side.BUY, "58000", "59000"),  # TP нижче ціни входу LONG
        (Side.BUY, "60000", "63000"),  # SL = ціна
        (Side.BUY, "58000", "60000"),  # TP = ціна
        (Side.BUY, "61000", None),
        (Side.BUY, None, "59000"),
        (Side.SELL, "59000", "58000"),  # SL нижче ціни входу SHORT
        (Side.SELL, "63000", "61000"),  # TP вище ціни входу SHORT
        (Side.SELL, "60000", "58000"),
        (Side.SELL, "63000", "60000"),
        (Side.SELL, "59000", None),
        (Side.SELL, None, "61000"),
    ],
)
def test_limit_with_sl_tp_on_wrong_side_of_price_is_rejected(side: Side, sl: str | None, tp: str | None) -> None:
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(
            _request_kwargs(
                side=side,
                order_type=OrderType.LIMIT,
                price=D("60000"),
                stop_loss=D(sl) if sl else None,
                take_profit=D(tp) if tp else None,
            )
        )


_three_prices = st.lists(st.integers(min_value=1, max_value=10**9), min_size=3, max_size=3, unique=True).map(
    lambda xs: [D(str(x)) * D("0.1") for x in sorted(xs)]
)


@given(prices=_three_prices)
def test_sl_tp_orientation_property(prices: list[Decimal]) -> None:
    low, mid, high = prices
    # BUY: SL < price < TP приймається, дзеркальне — ні.
    buy_ok = PlaceOrderRequest.model_validate(
        _request_kwargs(side=Side.BUY, order_type=OrderType.LIMIT, price=mid, stop_loss=low, take_profit=high)
    )
    assert (buy_ok.stop_loss, buy_ok.price, buy_ok.take_profit) == (low, mid, high)
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(
            _request_kwargs(side=Side.BUY, order_type=OrderType.LIMIT, price=mid, stop_loss=high, take_profit=low)
        )
    # SELL: TP < price < SL приймається, дзеркальне — ні.
    sell_ok = PlaceOrderRequest.model_validate(
        _request_kwargs(side=Side.SELL, order_type=OrderType.LIMIT, price=mid, stop_loss=high, take_profit=low)
    )
    assert (sell_ok.stop_loss, sell_ok.price, sell_ok.take_profit) == (high, mid, low)
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(
            _request_kwargs(side=Side.SELL, order_type=OrderType.LIMIT, price=mid, stop_loss=low, take_profit=high)
        )
    # MARKET: лише SL проти TP.
    assert (
        PlaceOrderRequest.model_validate(_request_kwargs(side=Side.BUY, stop_loss=low, take_profit=high)).stop_loss
        == low
    )
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(_request_kwargs(side=Side.BUY, stop_loss=high, take_profit=low))
    with pytest.raises(ValidationError):
        PlaceOrderRequest.model_validate(_request_kwargs(side=Side.SELL, stop_loss=low, take_profit=high))


# ---------------------------------------------------------------- PositionProtection


def test_protection_requires_stop_loss() -> None:
    kwargs = _protection_kwargs()
    del kwargs["stop_loss"]
    with pytest.raises(ValidationError):
        PositionProtection.model_validate(kwargs)


@pytest.mark.parametrize("bad", ["0", "-1"])
def test_protection_rejects_non_positive_stop_loss(bad: str) -> None:
    with pytest.raises(ValidationError):
        PositionProtection.model_validate(_protection_kwargs(stop_loss=D(bad)))


def test_protection_rejects_none_stop_loss() -> None:
    # Зняти SL через доменний запит неможливо за побудовою (ТЗ §9.2).
    with pytest.raises(ValidationError):
        PositionProtection.model_validate(_protection_kwargs(stop_loss=None))


@pytest.mark.parametrize(
    ("side", "sl", "tp"),
    [(PositionSide.LONG, "58000", "63000"), (PositionSide.SHORT, "63000", "58000"), (PositionSide.LONG, "58000", None)],
)
def test_protection_with_correct_orientation_is_accepted(side: PositionSide, sl: str, tp: str | None) -> None:
    protection = PositionProtection.model_validate(
        _protection_kwargs(position_side=side, stop_loss=D(sl), take_profit=D(tp) if tp else None)
    )
    assert protection.stop_loss == D(sl)
    assert protection.take_profit == (D(tp) if tp else None)


@pytest.mark.parametrize(
    ("side", "sl", "tp"),
    [
        (PositionSide.LONG, "63000", "58000"),
        (PositionSide.LONG, "60000", "60000"),
        (PositionSide.SHORT, "58000", "63000"),
        (PositionSide.SHORT, "60000", "60000"),
    ],
)
def test_protection_with_wrong_orientation_is_rejected(side: PositionSide, sl: str, tp: str) -> None:
    with pytest.raises(ValidationError):
        PositionProtection.model_validate(_protection_kwargs(position_side=side, stop_loss=D(sl), take_profit=D(tp)))


def test_protection_trigger_by_defaults() -> None:
    protection = PositionProtection.model_validate(_protection_kwargs())
    assert protection.sl_trigger_by is TriggerBy.MARK_PRICE
    assert protection.tp_trigger_by is TriggerBy.LAST_PRICE


# ---------------------------------------------------------------- new_client_order_id / client_order_host_id


def test_new_client_order_id_matches_format() -> None:
    value = new_client_order_id("a1")
    assert re.fullmatch(CLIENT_ORDER_ID_RE, value) is not None
    assert len(value) == CLIENT_ORDER_ID_LEN
    assert value.startswith("tba1")
    assert client_order_host_id(value) == "a1"


def test_new_client_order_id_is_unique_per_call() -> None:
    ids = {new_client_order_id("aa") for _ in range(1_000)}
    assert len(ids) == 1_000


@pytest.mark.parametrize("bad", ["a", "abc", "AA", "Aa", "a-", "", "a\n", "а1"])
def test_new_client_order_id_rejects_bad_host_id(bad: str) -> None:
    with pytest.raises(ValueError, match=r".+"):
        new_client_order_id(bad)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (VALID_CLIENT_ORDER_ID, "aa"),
        ("tbzz" + VALID_CLIENT_ORDER_ID[4:], "zz"),
        (None, None),
        ("", None),
        ("manual-1", None),
        (VALID_CLIENT_ORDER_ID[:-1], None),
        (VALID_CLIENT_ORDER_ID + "0", None),
        (VALID_CLIENT_ORDER_ID.upper(), None),
        (VALID_CLIENT_ORDER_ID + "\n", None),
        ("xx" + VALID_CLIENT_ORDER_ID[2:], None),
    ],
)
def test_client_order_host_id(value: str | None, expected: str | None) -> None:
    assert client_order_host_id(value) == expected


# ---------------------------------------------------------------- єдине джерело формату clientOrderId


def test_trading_models_use_the_same_client_order_id_regex() -> None:
    assert vars(trading_models)["CLIENT_ORDER_ID_RE"] is CLIENT_ORDER_ID_RE
    table = trading_models.Order.__table__
    assert isinstance(table, Table)
    checks = [
        c
        for c in table.constraints
        if isinstance(c, CheckConstraint) and c.name is not None and str(c.name).endswith("client_order_id_format")
    ]
    assert len(checks) == 1
    assert str(checks[0].sqltext) == f"client_order_id ~ '{CLIENT_ORDER_ID_RE}'"


def test_initial_migration_uses_the_same_client_order_id_regex() -> None:
    migration = (BACKEND_DIR / "migrations" / "versions" / "0001_initial_schema.py").read_text(encoding="utf-8")
    assert f"\"client_order_id ~ '{CLIENT_ORDER_ID_RE}'\"" in migration


# ---------------------------------------------------------------- класифікація clientOrderId (ТЗ §6.4, §11.4)

_MALFORMED = ClientOrderIdOrigin.BOT_PREFIX_MALFORMED


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (VALID_CLIENT_ORDER_ID, ClientOrderIdOrigin.BOT),
        ("tbzz" + VALID_CLIENT_ORDER_ID[4:], ClientOrderIdOrigin.BOT),
        # Префікс `tb`, але формат не наш: звірка трактує як ордер іншого комп'ютера (пауза), а не як ручний.
        ("tbzz" + VALID_CLIENT_ORDER_ID[4:-1], _MALFORMED),
        ("tbZZ" + VALID_CLIENT_ORDER_ID[4:], _MALFORMED),
        ("tbzz-" + VALID_CLIENT_ORDER_ID[5:], _MALFORMED),
        (VALID_CLIENT_ORDER_ID + "0", _MALFORMED),
        (VALID_CLIENT_ORDER_ID + "\n", _MALFORMED),
        (VALID_CLIENT_ORDER_ID.upper(), _MALFORMED),
        ("Tb-manual", _MALFORMED),
        ("tb", _MALFORMED),
        (None, ClientOrderIdOrigin.NOT_BOT),
        ("", ClientOrderIdOrigin.NOT_BOT),
        ("manual-1", ClientOrderIdOrigin.NOT_BOT),
        ("xx" + VALID_CLIENT_ORDER_ID[2:], ClientOrderIdOrigin.NOT_BOT),
        (" " + VALID_CLIENT_ORDER_ID, ClientOrderIdOrigin.NOT_BOT),
    ],
)
def test_client_order_id_origin(value: str | None, expected: ClientOrderIdOrigin) -> None:
    assert client_order_id_origin(value) is expected


@pytest.mark.parametrize(
    "value",
    ["tbzz" + VALID_CLIENT_ORDER_ID[4:-1], "tbZZ" + VALID_CLIENT_ORDER_ID[4:], VALID_CLIENT_ORDER_ID.upper(), "tb"],
)
def test_malformed_bot_prefix_has_no_host_but_is_not_treated_as_manual(value: str) -> None:
    """`client_order_host_id` → None ще не означає «ручний ордер»: рішення звірки — лише за `client_order_id_origin`."""
    assert client_order_host_id(value) is None
    assert client_order_id_origin(value) is ClientOrderIdOrigin.BOT_PREFIX_MALFORMED


@given(host=st.from_regex(r"[a-z0-9]{2}", fullmatch=True))
def test_generated_ids_are_classified_as_bot_with_their_host(host: str) -> None:
    value = new_client_order_id(host)
    assert client_order_id_origin(value) is ClientOrderIdOrigin.BOT
    assert client_order_host_id(value) == host
