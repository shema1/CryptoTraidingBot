"""Точна десяткова арифметика ризику.

Ділення, з якого виходить кількість, округлюється вниз: частка в контексті Decimal
інакше може стати трохи більшою за точну і після кроку лота перевищити бюджет ризику.

Публічні функції ризику рахують у власному контексті (`fixed_context`), а не в контексті
потоку: CCXT у торговому процесі міняє його (`ROUND_HALF_UP`, пастка `Underflow`), а
воркер бектесту — ні. Без цього той самий вхід міг би дати різний розмір у двох процесах.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from decimal import (
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    DivisionByZero,
    InvalidOperation,
    Overflow,
    localcontext,
)
from typing import ParamSpec, TypeVar

from pydantic import BaseModel, ConfigDict

_PREC = 50
# Типовий контекст Python, зафіксований явно: його не змінить ні CCXT, ні інший код потоку.
_RISK_CONTEXT = Context(prec=28, rounding=ROUND_HALF_EVEN, traps=[InvalidOperation, DivisionByZero, Overflow])

_P = ParamSpec("_P")
_R = TypeVar("_R")


def fixed_context(func: Callable[_P, _R]) -> Callable[_P, _R]:
    """Виконати `func` у `_RISK_CONTEXT`, незалежно від контексту Decimal потоку."""

    @functools.wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        with localcontext(_RISK_CONTEXT):
            return func(*args, **kwargs)

    return wrapper


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)


def require_decimal(
    name: str,
    value: object,
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise ValueError(f"{name} має бути скінченний Decimal")
    if positive and value <= 0:
        raise ValueError(f"{name} має бути > 0")
    if non_negative and value < 0:
        raise ValueError(f"{name} має бути ≥ 0")
    return value


def div_down(numerator: Decimal, denominator: Decimal) -> Decimal:
    """`numerator / denominator`, округлення вниз. `denominator` > 0."""
    if denominator <= 0:
        raise ValueError("дільник має бути > 0")
    with localcontext(Context(prec=_PREC, rounding=ROUND_DOWN)):
        return numerator / denominator


def div_up(numerator: Decimal, denominator: Decimal) -> Decimal:
    """`numerator / denominator`, округлення вгору. `denominator` > 0."""
    if denominator <= 0:
        raise ValueError("дільник має бути > 0")
    with localcontext(Context(prec=_PREC, rounding=ROUND_CEILING)):
        return numerator / denominator


def mul_div(value: Decimal, numerator: Decimal, denominator: Decimal, *, rounding: str) -> Decimal:
    """`value * numerator / denominator` у розширеній точності із заданим округленням."""
    if denominator <= 0:
        raise ValueError("дільник має бути > 0")
    with localcontext(Context(prec=_PREC, rounding=rounding)):
        return value * numerator / denominator
