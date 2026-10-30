"""Округлення ціни й кількості (ТЗ §8.3, T1.7).

Кількість: кратна кроку, не більша за вхід, і ніколи не додатна менша за `minQty` — тоді `0` і причина.
Ціна: кратна тіку; напрям залежить від ролі і не збільшує ризик.
"""

from __future__ import annotations

from decimal import Context, Decimal, localcontext

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.exchange.rounding import (
    EntryQty,
    PriceRole,
    QtyRejectReason,
    ceil_to_step,
    floor_to_step,
    round_entry_qty,
    round_price,
)
from app.exchange.types import Instrument

_FLOOR = {PriceRole.LIMIT_BUY, PriceRole.STOP_SHORT, PriceRole.TAKE_PROFIT_LONG}


def _instrument(
    *,
    step: Decimal = Decimal("0.001"),
    min_qty: Decimal = Decimal("0.001"),
    min_notional: Decimal = Decimal("5"),
    max_qty: Decimal | None = Decimal("100"),
) -> Instrument:
    return Instrument(
        symbol="BTC/USDT:USDT",
        tick_size=Decimal("0.1"),
        qty_step=step,
        min_qty=min_qty,
        min_notional=min_notional,
        max_qty=max_qty,
        max_leverage=Decimal("1"),
        launch_time_ms=0,
        status="Trading",
    )


def test_examples_floor_qty_and_reject_below_minimums() -> None:
    assert floor_to_step(Decimal("1.239"), Decimal("0.01")) == Decimal("1.23")
    assert floor_to_step(Decimal("1.20"), Decimal("0.01")) == Decimal("1.20")
    assert floor_to_step(Decimal("0.009"), Decimal("0.01")) == Decimal("0")
    below_min = round_entry_qty(Decimal("0.005"), _instrument(min_qty=Decimal("0.01")), price=Decimal("100000"))
    assert below_min == EntryQty(Decimal(0), QtyRejectReason.BELOW_MIN_QTY)
    assert round_entry_qty(Decimal("0.0009"), _instrument(), price=Decimal("100")) == EntryQty(
        Decimal(0), QtyRejectReason.ZERO_AFTER_ROUND
    )
    # Крок 0.01, мінімум 0.015 не кратний: 0.01 не повертається — це менше за мінімум.
    odd_min = round_entry_qty(
        Decimal("0.019"), _instrument(step=Decimal("0.01"), min_qty=Decimal("0.015")), price=Decimal("1000")
    )
    assert odd_min == EntryQty(Decimal(0), QtyRejectReason.BELOW_MIN_QTY)
    assert round_entry_qty(
        Decimal("0.02"), _instrument(step=Decimal("0.01"), min_qty=Decimal("0.015")), price=Decimal("1000")
    ) == EntryQty(Decimal("0.02"), None)
    cheap = round_entry_qty(Decimal("0.01"), _instrument(), price=Decimal("100"))
    assert cheap == EntryQty(Decimal(0), QtyRejectReason.BELOW_MIN_NOTIONAL)
    capped = round_entry_qty(
        Decimal("5"),
        _instrument(step=Decimal("0.1"), min_qty=Decimal("0.1"), max_qty=Decimal("1.5")),
        price=Decimal("100"),
    )
    assert capped == EntryQty(Decimal("1.5"), None)


def test_price_examples_move_toward_less_risk() -> None:
    tick = Decimal("0.1")
    assert round_price(Decimal("100.19"), tick, PriceRole.LIMIT_BUY) == Decimal("100.1")
    assert round_price(Decimal("100.11"), tick, PriceRole.LIMIT_SELL) == Decimal("100.2")
    assert round_price(Decimal("100.11"), tick, PriceRole.STOP_LONG) == Decimal("100.2")
    assert round_price(Decimal("100.19"), tick, PriceRole.STOP_SHORT) == Decimal("100.1")
    assert round_price(Decimal("100.19"), tick, PriceRole.TAKE_PROFIT_LONG) == Decimal("100.1")
    assert round_price(Decimal("100.11"), tick, PriceRole.TAKE_PROFIT_SHORT) == Decimal("100.2")
    assert round_price(Decimal("100.1"), tick, PriceRole.STOP_LONG) == Decimal("100.1")
    with pytest.raises(ValueError, match="тік"):
        round_price(Decimal("0.05"), tick, PriceRole.LIMIT_BUY)


@settings(max_examples=150)
@given(
    qty=st.integers(min_value=0, max_value=20_000),
    step_units=st.integers(min_value=1, max_value=50),
    scale=st.integers(min_value=0, max_value=4),
    min_units=st.integers(min_value=1, max_value=20),
    extra_units=st.integers(min_value=0, max_value=80),
    price_units=st.integers(min_value=1, max_value=5_000),
    notional_units=st.integers(min_value=0, max_value=50),
    unbounded=st.booleans(),
)
def test_entry_qty_is_a_step_multiple_not_above_the_input_and_never_a_short_lot(
    qty: int,
    step_units: int,
    scale: int,
    min_units: int,
    extra_units: int,
    price_units: int,
    notional_units: int,
    unbounded: bool,
) -> None:
    step = Decimal(step_units).scaleb(-scale)
    min_qty = step * min_units
    max_qty = None if unbounded else step * (min_units + extra_units)
    price = Decimal(price_units).scaleb(-2)
    asked = Decimal(qty).scaleb(-scale)
    result = round_entry_qty(
        asked,
        _instrument(step=step, min_qty=min_qty, min_notional=Decimal(notional_units), max_qty=max_qty),
        price=price,
    )
    assert result.qty <= asked
    assert result.qty % step == 0
    assert result.qty == 0 or result.qty >= min_qty
    if result.qty == 0:
        assert result.reason is not None
    else:
        assert result.reason is None
        assert result.qty * price >= Decimal(notional_units)
        if max_qty is not None:
            assert result.qty <= max_qty


@given(
    price_units=st.integers(min_value=1, max_value=100_000),
    tick_units=st.integers(min_value=1, max_value=100),
    scale=st.integers(min_value=0, max_value=4),
    role=st.sampled_from(tuple(PriceRole)),
)
def test_rounded_price_is_a_tick_multiple_and_does_not_increase_risk(
    price_units: int, tick_units: int, scale: int, role: PriceRole
) -> None:
    tick = Decimal(tick_units).scaleb(-scale)
    price = Decimal(price_units).scaleb(-scale)
    if role in _FLOOR and price < tick:
        with pytest.raises(ValueError, match="тік"):
            round_price(price, tick, role)
        return
    rounded = round_price(price, tick, role)
    assert rounded > 0
    assert rounded % tick == 0
    if role in _FLOOR:
        assert rounded <= price
    else:
        assert rounded >= price
    if price % tick == 0:
        assert rounded == price


def test_ceil_and_floor_of_an_exact_multiple_stay_put() -> None:
    assert ceil_to_step(Decimal("0.3"), Decimal("0.1")) == Decimal("0.3")
    assert floor_to_step(Decimal("0.3"), Decimal("0.1")) == Decimal("0.3")


def test_floor_does_not_round_up_when_the_quotient_exceeds_decimal_precision() -> None:
    # 28 значущих цифр, тік 0.5: частка має 29 цифр і в контексті Decimal округлилася б до 20.
    just_below = Decimal("9.999999999999999999999999999")
    assert floor_to_step(just_below, Decimal("0.5")) == Decimal("9.5")
    assert round_price(just_below, Decimal("0.5"), PriceRole.LIMIT_BUY) == Decimal("9.5")
    assert ceil_to_step(Decimal("10.00000000000000000000000001"), Decimal("0.5")) == Decimal("10.5")
    assert round_entry_qty(
        just_below,
        _instrument(step=Decimal("0.5"), min_qty=Decimal("0.5"), max_qty=None),
        price=Decimal("100"),
    ) == EntryQty(Decimal("9.5"), None)


@st.composite
def _value_and_step(draw: st.DrawFn) -> tuple[Decimal, Decimal]:
    """Крок 1, 5 або 25 × 10^-k; значення довільне або на відстані 10^-n від кратного кроку."""
    step = Decimal(draw(st.sampled_from((1, 5, 25)))).scaleb(-draw(st.integers(min_value=0, max_value=8)))
    if draw(st.booleans()):
        value = draw(st.decimals(min_value=0, max_value=10**9, allow_nan=False, allow_infinity=False, places=None))
        return value, step
    multiple = step * draw(st.integers(min_value=1, max_value=10**8))
    offset = Decimal(draw(st.sampled_from((-1, 1)))).scaleb(-draw(st.integers(min_value=10, max_value=32)))
    # Точна сума: вхід може мати більше 28 значущих цифр, як рядок з біржі чи результат ділення.
    with localcontext(Context(prec=100)):
        return multiple + offset, step


@settings(max_examples=300)
@given(case=_value_and_step())
def test_step_rounding_is_exact_for_full_precision_inputs(case: tuple[Decimal, Decimal]) -> None:
    """Вхід з T2.2 — результат ділення з 28 значущими цифрами: округлення не може вийти за вхід."""
    value, step = case
    floored = floor_to_step(value, step)
    ceiled = ceil_to_step(value, step)
    # Перевірки — у точному контексті: різниця з 28-значним входом у стандартному теж округлилася б.
    with localcontext(Context(prec=100)):
        assert floored <= value <= ceiled
        assert floored % step == 0
        assert ceiled % step == 0
        assert value - floored < step
        assert ceiled - value < step
