"""Модель витрат (ТЗ §5.2.2, T2.2)."""

from __future__ import annotations

from decimal import ROUND_UP, Context, Decimal, Inexact, Underflow, localcontext

import pytest

from app.exchange.types import PositionSide
from app.risk.costs import (
    DEFAULT_MAX_SIGNAL_AGE_MS,
    CostModel,
    FundingEvent,
    SlippageLeg,
    entry_is_stale,
    funding_over_hold,
    funding_payment,
)

_LONG = PositionSide.LONG
_SHORT = PositionSide.SHORT


def test_defaults_match_the_spec_example() -> None:
    costs = CostModel()
    assert costs.taker_fee_rate == Decimal("0.00055")
    assert costs.maker_fee_rate == costs.taker_fee_rate
    assert costs.slippage_entry_atr == Decimal("0.05")
    assert costs.stop_slippage_atr == Decimal("0.10")
    assert costs.slippage_tp_atr == Decimal("0.05")
    assert costs.slippage_entry_bps == Decimal(0)
    assert costs.stop_slippage_bps == Decimal(0)


def test_explicit_stop_slippage_is_not_doubled() -> None:
    costs = CostModel(slippage_sl_atr=Decimal("0.01"), slippage_sl_bps=Decimal(3))
    assert costs.stop_slippage_atr == Decimal("0.01")
    assert costs.stop_slippage_bps == Decimal(3)


def test_slippage_is_bps_plus_atr_fraction() -> None:
    costs = CostModel(slippage_entry_bps=Decimal(10), slippage_entry_atr=Decimal("0.05"))
    # 100 * 10/10000 + 2 * 0.05 = 0.1 + 0.1
    assert costs.slippage(price=Decimal(100), atr=Decimal(2), leg=SlippageLeg.ENTRY) == Decimal("0.2")
    assert costs.slippage(price=Decimal(100), atr=Decimal(2), leg=SlippageLeg.STOP) == Decimal("0.4")


def test_execution_moves_against_the_trade_and_charges_taker_on_the_exit() -> None:
    costs = CostModel()
    long = costs.execution(side=_LONG, entry=Decimal(100), stop=Decimal(99), take_profit=Decimal(102), atr=Decimal(2))
    assert long is not None
    assert long.entry_fill == Decimal("100.1")
    assert long.stop_fill == Decimal("98.8")
    assert long.take_profit_fill == Decimal("101.9")
    assert long.loss_per_unit == Decimal("1.409395")
    # Ціль 1.8 мінус комісії входу і виходу: 1.8 - 0.00055*(100.1+101.9) = 1.8 - 0.1111
    assert long.tp_net_per_unit == Decimal("1.6889")

    short = costs.execution(side=_SHORT, entry=Decimal(100), stop=Decimal(101), take_profit=Decimal(98), atr=Decimal(2))
    assert short is not None
    assert short.entry_fill == Decimal("99.9")
    assert short.stop_fill == Decimal("101.2")
    assert short.take_profit_fill == Decimal("98.1")
    assert short.loss_per_unit == Decimal("1.410605")


def test_maker_entry_does_not_discount_the_stop_exit() -> None:
    costs = CostModel(maker_fee_rate=Decimal("0.0001"))
    made = costs.execution(
        side=_LONG, entry=Decimal(100), stop=Decimal(99), take_profit=None, atr=Decimal(0), entry_is_maker=True
    )
    taken = costs.execution(
        side=_LONG, entry=Decimal(100), stop=Decimal(99), take_profit=None, atr=Decimal(0), entry_is_maker=False
    )
    assert made is not None
    assert taken is not None
    assert made.loss_per_unit < taken.loss_per_unit
    assert taken.loss_per_unit - made.loss_per_unit == (costs.taker_fee_rate - costs.maker_fee_rate) * made.entry_fill


def test_non_positive_fill_or_a_stop_on_the_wrong_side_is_not_an_execution() -> None:
    costs = CostModel(slippage_entry_atr=Decimal(1), slippage_sl_atr=Decimal(0))
    # Шорт: вхід 1 мінус slippage 1·ATR не лишає додатної ціни.
    assert costs.execution(side=_SHORT, entry=Decimal(1), stop=Decimal(2), take_profit=None, atr=Decimal(1)) is None
    assert costs.execution(side=_LONG, entry=Decimal(100), stop=Decimal(101), take_profit=None, atr=Decimal(0)) is None


def test_scaled_doubles_fees_and_slippage_but_refuses_to_shrink_them() -> None:
    doubled = CostModel().scaled(Decimal(2))
    assert doubled.taker_fee_rate == Decimal("0.00110")
    assert doubled.slippage_entry_atr == Decimal("0.10")
    assert doubled.stop_slippage_atr == Decimal("0.20")
    assert doubled.slippage_tp_atr == Decimal("0.10")
    with pytest.raises(ValueError, match="менший за 1"):
        CostModel().scaled(Decimal("0.5"))


def test_scaling_keeps_an_explicit_stop_ratio() -> None:
    costs = CostModel(slippage_sl_atr=Decimal("0.07")).scaled(Decimal(2))
    assert costs.stop_slippage_atr == Decimal("0.14")
    assert costs.slippage_entry_atr == Decimal("0.10")


def test_funding_sign_and_the_open_interval() -> None:
    rate = Decimal("0.0001")
    qty = Decimal(2)
    mark = Decimal(100)
    assert funding_payment(rate=rate, qty=qty, mark_price=mark, side=_LONG) == Decimal("-0.02")
    assert funding_payment(rate=rate, qty=qty, mark_price=mark, side=_SHORT) == Decimal("0.02")
    assert funding_payment(rate=Decimal(0), qty=qty, mark_price=mark, side=_LONG) == Decimal(0)

    events = [
        FundingEvent(time_ms=1_000, rate=rate, mark_price=mark),
        FundingEvent(time_ms=2_000, rate=Decimal("0.0002"), mark_price=mark),
        FundingEvent(time_ms=3_000, rate=Decimal("0.0003"), mark_price=mark),
    ]
    # (1000, 3000]: подія 1000 — до виконання входу, 3000 — позиція ще відкрита.
    held = funding_over_hold(events, side=_LONG, qty=qty, opened_at_ms=1_000, closed_at_ms=3_000)
    assert held == Decimal("-0.10")
    still_open = funding_over_hold(events, side=_SHORT, qty=qty, opened_at_ms=2_000, closed_at_ms=None)
    assert still_open == Decimal("0.06")


def test_funding_on_the_entry_candle_open_is_not_paid_and_on_the_exit_candle_open_is() -> None:
    # Мітки funding — рівно на відкритті години. Вхід за open[t] виконується вже після мітки,
    # а вихід за open[t + horizon] (або SL у свічці, що почалась з мітки) — коли позиція ще відкрита.
    hour = 3_600_000
    entry_open = 8 * hour
    exit_open = 16 * hour
    events = [
        FundingEvent(time_ms=entry_open, rate=Decimal("0.0001"), mark_price=Decimal(100)),
        FundingEvent(time_ms=exit_open, rate=Decimal("0.0002"), mark_price=Decimal(100)),
    ]
    paid = funding_over_hold(events, side=_LONG, qty=Decimal(1), opened_at_ms=entry_open, closed_at_ms=exit_open)
    assert paid == Decimal("-0.02")
    same_candle = funding_over_hold(
        events, side=_LONG, qty=Decimal(1), opened_at_ms=entry_open, closed_at_ms=entry_open
    )
    assert same_candle == Decimal(0)
    # Фактичний час fill (через кілька секунд після відкриття свічки) дає той самий результат.
    filled = funding_over_hold(
        events, side=_LONG, qty=Decimal(1), opened_at_ms=entry_open + 2_000, closed_at_ms=exit_open + 1_500
    )
    assert filled == paid


def test_execution_does_not_depend_on_the_ambient_decimal_context() -> None:
    # CCXT ставить контексту потоку ROUND_HALF_UP і пастку Underflow; ризик від цього не залежить.
    costs = CostModel(slippage_entry_bps=Decimal(3))
    args = {
        "side": _LONG,
        "entry": Decimal("100.37"),
        "stop": Decimal("99.11"),
        "take_profit": Decimal("103.3"),
        "atr": Decimal("1.7"),
    }
    reference = costs.execution(**args)  # type: ignore[arg-type]
    with localcontext(Context(prec=5, rounding=ROUND_UP, traps=[Underflow, Inexact])):
        hostile = costs.execution(**args)  # type: ignore[arg-type]
    assert hostile == reference


def test_entry_delay_blocks_only_past_the_limit_and_a_negative_delay() -> None:
    opened = 1_000_000
    assert entry_is_stale(computed_at_ms=opened + DEFAULT_MAX_SIGNAL_AGE_MS, entry_candle_open_time_ms=opened) is False
    assert entry_is_stale(computed_at_ms=opened + DEFAULT_MAX_SIGNAL_AGE_MS + 1, entry_candle_open_time_ms=opened)
    assert entry_is_stale(computed_at_ms=opened - 1, entry_candle_open_time_ms=opened)
    with pytest.raises(ValueError, match="max_signal_age_ms"):
        entry_is_stale(computed_at_ms=opened, entry_candle_open_time_ms=opened, max_signal_age_ms=-1)
