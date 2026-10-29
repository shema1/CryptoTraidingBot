"""Ієрархія доменних винятків біржі та `is_definite_rejection` (`app/exchange/errors.py`, T1.1; ТЗ §7.1, §7.2).

Головна властивість: ордер вважається точно не створеним **лише** за явної відмови біржі. Таймаут, 5xx, rate limit,
дубль `clientOrderId`, помилка авторизації чи будь-який сторонній виняток — це `SUBMISSION_UNKNOWN` (fail-closed).
"""

from __future__ import annotations

import asyncio

import pytest

from app.exchange.errors import (
    AdapterConfigError,
    DuplicateClientOrderId,
    ExchangeAuthError,
    ExchangeError,
    ExchangeOutcomeUnknown,
    ExchangeTimeout,
    ExchangeUnavailable,
    InsufficientMargin,
    MalformedResponse,
    OrderNotActive,
    OrderRejected,
    PrivateStreamLost,
    RateLimited,
    RequestRejected,
    is_definite_rejection,
)

# ---------------------------------------------------------------- ієрархія


@pytest.mark.parametrize(
    ("cls", "parent"),
    [
        (ExchangeError, Exception),
        (ExchangeOutcomeUnknown, ExchangeError),
        (ExchangeTimeout, ExchangeOutcomeUnknown),
        (ExchangeUnavailable, ExchangeOutcomeUnknown),
        (PrivateStreamLost, ExchangeUnavailable),
        (MalformedResponse, ExchangeOutcomeUnknown),
        (RateLimited, ExchangeError),
        (DuplicateClientOrderId, ExchangeError),
        (RequestRejected, ExchangeError),
        (OrderRejected, RequestRejected),
        (InsufficientMargin, OrderRejected),
        (OrderNotActive, ExchangeError),
        (ExchangeAuthError, ExchangeError),
        (AdapterConfigError, ExchangeError),
    ],
)
def test_direct_parent(cls: type[BaseException], parent: type[BaseException]) -> None:
    assert cls.__bases__ == (parent,)


@pytest.mark.parametrize(
    "cls",
    [
        RateLimited,
        DuplicateClientOrderId,
        OrderNotActive,
        ExchangeAuthError,
        AdapterConfigError,
        ExchangeOutcomeUnknown,
    ],
)
def test_not_a_rejection_by_inheritance(cls: type[BaseException]) -> None:
    # Дубль id і rate limit НЕ є відмовою: ордер міг бути створений попередньою відправкою.
    assert issubclass(cls, RequestRejected) is False


def test_private_stream_lost_means_outcome_unknown() -> None:
    assert issubclass(PrivateStreamLost, ExchangeOutcomeUnknown) is True
    assert issubclass(PrivateStreamLost, RequestRejected) is False


@pytest.mark.parametrize("cls", [RequestRejected, OrderRejected, InsufficientMargin])
def test_rejections_are_not_outcome_unknown(cls: type[BaseException]) -> None:
    assert issubclass(cls, ExchangeOutcomeUnknown) is False


# ---------------------------------------------------------------- атрибути


@pytest.mark.parametrize("cls", [RequestRejected, OrderRejected, InsufficientMargin])
def test_rejection_keeps_code_and_reason(cls: type[RequestRejected]) -> None:
    exc = cls("110007", "ab not enough for new order")
    assert exc.code == "110007"
    assert exc.reason == "ab not enough for new order"
    assert str(exc) == "110007: ab not enough for new order"


def test_duplicate_client_order_id_keeps_id() -> None:
    exc = DuplicateClientOrderId("tbaa0123456789abcdef0123456789abcdef")
    assert exc.client_order_id == "tbaa0123456789abcdef0123456789abcdef"
    assert "tbaa0123456789abcdef0123456789abcdef" in str(exc)


@pytest.mark.parametrize("value", [None, 0, 1_500])
def test_rate_limited_accepts_none_or_non_negative_int_ms(value: int | None) -> None:
    exc = RateLimited(retry_after_ms=value)
    assert exc.retry_after_ms == value
    assert exc.ip_ban is False


def test_rate_limited_ip_ban_flag() -> None:
    exc = RateLimited("ip banned", ip_ban=True)
    assert exc.ip_ban is True


@pytest.mark.parametrize("bad", [-1, True, False, 1.5, 1000.0, "1000"])
def test_rate_limited_rejects_bad_retry_after(bad: object) -> None:
    with pytest.raises(ValueError, match="retry_after_ms"):
        RateLimited(retry_after_ms=bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [0, 1, "yes", None])
def test_rate_limited_rejects_non_bool_ip_ban(bad: object) -> None:
    with pytest.raises(ValueError, match="ip_ban"):
        RateLimited(ip_ban=bad)  # type: ignore[arg-type]


# ---------------------------------------------------------------- is_definite_rejection

DEFINITE_REJECTIONS: list[BaseException] = [
    RequestRejected("10001", "params error"),
    OrderRejected("110017", "reduce-only rule not satisfied"),
    InsufficientMargin("110007", "insufficient balance"),
]

NOT_REJECTIONS: list[BaseException] = [
    ExchangeError("x"),
    ExchangeOutcomeUnknown("x"),
    ExchangeTimeout("x"),
    ExchangeUnavailable("x"),
    PrivateStreamLost("x"),
    MalformedResponse("x"),
    RateLimited(retry_after_ms=1_000),
    DuplicateClientOrderId("tbaa0123456789abcdef0123456789abcdef"),
    OrderNotActive("x"),
    ExchangeAuthError("x"),
    AdapterConfigError("x"),
    ValueError("x"),
    Exception("x"),
    TimeoutError("x"),
    ConnectionResetError("x"),
    asyncio.CancelledError(),
    KeyboardInterrupt(),
]


@pytest.mark.parametrize("exc", DEFINITE_REJECTIONS, ids=lambda e: type(e).__name__)
def test_definite_rejection_is_true_only_for_request_rejected_family(exc: BaseException) -> None:
    assert is_definite_rejection(exc) is True


@pytest.mark.parametrize("exc", NOT_REJECTIONS, ids=lambda e: type(e).__name__)
def test_everything_else_is_not_a_definite_rejection(exc: BaseException) -> None:
    # Fail-closed: усе, що не явна відмова біржі, — SUBMISSION_UNKNOWN (ТЗ §7.2).
    assert is_definite_rejection(exc) is False


def test_every_exception_class_is_classified() -> None:
    """Новий клас в errors.py має потрапити в один із двох списків вище (інакше класифікацію не перевірено)."""
    from app.exchange import errors

    declared = {
        obj
        for obj in vars(errors).values()
        if isinstance(obj, type) and issubclass(obj, BaseException) and obj.__module__ == errors.__name__
    }
    covered = {type(e) for e in DEFINITE_REJECTIONS + NOT_REJECTIONS if type(e).__module__ == errors.__name__}
    assert declared == covered
