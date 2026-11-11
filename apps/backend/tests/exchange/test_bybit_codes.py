"""Мапінг кодів і статусів Bybit v5 (`app/exchange/bybit_codes.py`, T3.2).

Таблиця записана тут літералом, а не зібрана з констант модуля: тест має впасти, якщо хтось додасть код у перелік
відмов або перенесе його в іншу операцію. Кожна операція перевіряється на кожному відомому коді: код, якого немає в
переліку саме цієї операції, — `ExchangeOutcomeUnknown` (allowlist, `app/exchange/errors.py`).
"""

from __future__ import annotations

from typing import get_args

import pytest

from app.exchange.bybit_codes import (
    MARGIN_MODE_REQUEST,
    Operation,
    error_for,
    exec_type,
    margin_mode,
    order_status,
    stop_order_type,
)
from app.exchange.errors import (
    AdapterConfigError,
    DuplicateClientOrderId,
    ExchangeAuthError,
    ExchangeError,
    ExchangeOutcomeUnknown,
    ExchangeUnavailable,
    InsufficientMargin,
    MalformedResponse,
    OrderNotActive,
    OrderRejected,
    RateLimited,
    RequestRejected,
    is_definite_rejection,
)
from app.exchange.types import ExchangeOrderStatus, ExecType, MarginMode, StopOrderType

OPERATIONS: tuple[Operation, ...] = ("read", "place", "cancel", "leverage", "protection", "margin")

# Коди, однакові для всіх операцій.
_COMMON: dict[str, type[ExchangeError]] = {
    "10003": ExchangeAuthError,
    "10004": ExchangeAuthError,
    "10005": ExchangeAuthError,
    "10010": ExchangeAuthError,
    "10017": AdapterConfigError,
    "10006": RateLimited,
    "10000": ExchangeUnavailable,
    "10016": ExchangeUnavailable,
    "110079": ExchangeUnavailable,
    "-1": ExchangeUnavailable,
    "429": ExchangeUnavailable,
    "10002": ExchangeOutcomeUnknown,
}

# Коди конкретної операції; `None` — «не змінено», тобто успіх.
_BY_OPERATION: dict[Operation, dict[str, type[ExchangeError] | None]] = {
    "read": {"10001": RequestRejected},
    "place": {
        "10001": OrderRejected,
        "10027": OrderRejected,
        "10029": OrderRejected,
        "110003": OrderRejected,
        "110017": OrderRejected,
        "110020": OrderRejected,
        "110094": OrderRejected,
        "110004": InsufficientMargin,
        "110007": InsufficientMargin,
        "110012": InsufficientMargin,
        "110072": DuplicateClientOrderId,
    },
    "cancel": {
        "10001": RequestRejected,
        "110001": OrderNotActive,
        "110008": OrderNotActive,
        "110010": OrderNotActive,
    },
    "leverage": {"10001": RequestRejected, "110013": RequestRejected, "110043": None},
    "protection": {"10001": RequestRejected},
    "margin": {"10001": RequestRejected, "110073": RequestRejected, "3400045": RequestRejected},
}

_ALL_CODES = sorted({*_COMMON, *(code for table in _BY_OPERATION.values() for code in table), "99999", "34040"})


def _expected(op: Operation, code: str) -> type[ExchangeError] | None:
    table = _BY_OPERATION[op]
    if code in table:
        return table[code]
    return _COMMON.get(code, ExchangeOutcomeUnknown)


def test_operation_literal_matches_the_table() -> None:
    assert set(get_args(Operation)) == set(OPERATIONS) == set(_BY_OPERATION)


@pytest.mark.parametrize("op", OPERATIONS)
@pytest.mark.parametrize("code", _ALL_CODES)
def test_every_code_maps_exactly_as_documented_for_every_operation(op: Operation, code: str) -> None:
    error = error_for(op, code, "message from exchange", client_order_id="tbH1abc")
    expected = _expected(op, code)
    if expected is None:
        assert error is None
        return
    assert type(error) is expected
    # Лише відмова з переліку означає «точно не виконано»; решта — невизначеність для машини станів.
    assert is_definite_rejection(error) is issubclass(expected, RequestRejected)


def test_rejection_keeps_the_exchange_code_and_message() -> None:
    error = error_for("place", "110094", "Order does not meet minimum order value 5USDT")
    assert type(error) is OrderRejected
    assert error.code == "110094"
    assert error.reason == "Order does not meet minimum order value 5USDT"


def test_duplicate_client_order_id_carries_the_id_and_without_it_is_unknown() -> None:
    error = error_for("place", "110072", "OrderLinkedID is duplicate", client_order_id="tbH1abc")
    assert type(error) is DuplicateClientOrderId
    assert error.client_order_id == "tbH1abc"
    # Без id викликач не зможе розв'язати дубль пошуком, тож це просто невідомий результат, не відмова.
    assert type(error_for("place", "110072", "OrderLinkedID is duplicate")) is ExchangeOutcomeUnknown


def test_success_code_is_not_an_error() -> None:
    with pytest.raises(ValueError, match="retCode 0"):
        error_for("read", "0", "OK")


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
        ("Active", ExchangeOrderStatus.UNKNOWN),
        ("", ExchangeOrderStatus.UNKNOWN),
        ("cancelled", ExchangeOrderStatus.UNKNOWN),
    ],
)
def test_order_status(raw: str, expected: ExchangeOrderStatus) -> None:
    assert order_status(raw) is expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", None),
        ("UNKNOWN", None),
        ("TakeProfit", StopOrderType.TAKE_PROFIT),
        ("PartialTakeProfit", StopOrderType.TAKE_PROFIT),
        ("StopLoss", StopOrderType.STOP_LOSS),
        ("PartialStopLoss", StopOrderType.STOP_LOSS),
        ("TrailingStop", StopOrderType.TRAILING_STOP),
        ("Stop", StopOrderType.CONDITIONAL),
        ("tpslOrder", StopOrderType.OTHER),
        ("OcoOrder", StopOrderType.OTHER),
    ],
)
def test_stop_order_type(raw: str, expected: StopOrderType | None) -> None:
    assert stop_order_type(raw) is expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Trade", ExecType.TRADE),
        ("AdlTrade", ExecType.ADL_TRADE),
        ("BustTrade", ExecType.BUST_TRADE),
        ("Delivery", ExecType.DELIVERY),
        ("Funding", None),
    ],
)
def test_exec_type(raw: str, expected: ExecType | None) -> None:
    assert exec_type(raw) is expected


@pytest.mark.parametrize("raw", ["Settle", "trade", ""])
def test_unknown_exec_type_is_malformed_not_skipped(raw: str) -> None:
    with pytest.raises(MalformedResponse):
        exec_type(raw)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("ISOLATED_MARGIN", MarginMode.ISOLATED),
        ("REGULAR_MARGIN", MarginMode.CROSS),
        ("PORTFOLIO_MARGIN", MarginMode.CROSS),
    ],
)
def test_margin_mode(raw: str, expected: MarginMode) -> None:
    assert margin_mode(raw) is expected


@pytest.mark.parametrize("raw", ["", "isolated", "ISOLATED"])
def test_unknown_margin_mode_is_malformed(raw: str) -> None:
    with pytest.raises(MalformedResponse):
        margin_mode(raw)


def test_margin_mode_request_values() -> None:
    assert dict(MARGIN_MODE_REQUEST) == {MarginMode.ISOLATED: "ISOLATED_MARGIN", MarginMode.CROSS: "REGULAR_MARGIN"}
