"""Симулятор угоди, мітки і ваги унікальності (ТЗ §5.2.1, §5.1, §5.2.4, T2.3)."""

from __future__ import annotations

import math
from decimal import Decimal

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from app.exchange.types import PositionSide
from app.ml.features import (
    FEATURE_LOOKBACK_BARS,
    HOUR_MS,
    CandleBar,
    SymbolData,
    compute_features,
    wilder_atr,
)
from app.ml.trade_sim import (
    DirectClass,
    EntryCandidate,
    ExitReason,
    IncompletePath,
    LabelSpan,
    MarkSeries,
    NoAtr,
    SimCandle,
    SimulatedTrade,
    SkipReason,
    StopPriceSource,
    TargetParams,
    UnusableExit,
    atr_before_entry,
    label_direct,
    label_meta,
    label_span,
    label_span_direct,
    sample_weights,
    simulate_trade,
    uniqueness_weights,
)
from app.risk.costs import CostModel, FundingEvent
from app.risk.limits import check_time_stop

HOUR = HOUR_MS
SYMBOL = "BTC/USDT:USDT"
_LONG = PositionSide.LONG
_SHORT = PositionSide.SHORT
_ATR = Decimal(2)


def _d(value: int | str | Decimal) -> Decimal:
    if type(value) is Decimal:
        return value
    return Decimal(value)


def _candle(
    index: int,
    open_: int | str | Decimal,
    high: int | str | Decimal,
    low: int | str | Decimal,
    close: int | str | Decimal,
) -> SimCandle:
    return SimCandle(open_time_ms=index * HOUR, open=_d(open_), high=_d(high), low=_d(low), close=_d(close))


def _flat(count: int, price: int = 100) -> tuple[SimCandle, ...]:
    return tuple(_candle(index, price, price, price, price) for index in range(count))


def _pad(candles: tuple[SimCandle, ...], *, index: int, horizon: int) -> tuple[SimCandle, ...]:
    """Дотягнути суцільний хвіст до свічки time-stop, не чіпаючи вже задані бари."""
    need = index + horizon + 1
    if len(candles) >= need:
        return candles
    last = candles[-1]
    out = list(candles)
    price = last.close
    for step in range(1, need - len(candles) + 1):
        out.append(
            SimCandle(
                open_time_ms=last.open_time_ms + step * HOUR,
                open=price,
                high=price,
                low=price,
                close=price,
            )
        )
    return tuple(out)


def _params(horizon: int = 3, tp: str = "2", sl: str = "1") -> TargetParams:
    return TargetParams(tp_atr_mult=Decimal(tp), sl_atr_mult=Decimal(sl), horizon_bars=horizon)


def _zero_slip() -> CostModel:
    return CostModel(
        taker_fee_rate=Decimal("0.001"),
        maker_fee_rate=Decimal("0.0002"),
        slippage_entry_atr=Decimal(0),
        slippage_entry_bps=Decimal(0),
        slippage_sl_atr=Decimal(0),
        slippage_sl_bps=Decimal(0),
        slippage_tp_atr=Decimal(0),
        slippage_tp_bps=Decimal(0),
    )


def _sim(
    candles: tuple[SimCandle, ...],
    *,
    index: int = 0,
    side: PositionSide = _LONG,
    horizon: int = 3,
    tp: str = "2",
    sl: str = "1",
    mark: tuple[SimCandle, ...] | MarkSeries | None = None,
    funding: tuple[FundingEvent, ...] = (),
    costs: CostModel | None = None,
    atr: Decimal = _ATR,
    maker: bool = False,
    pad: bool = True,
) -> SimulatedTrade:
    series = _pad(candles, index=index, horizon=horizon) if pad else candles
    return simulate_trade(
        series,
        entry_index=index,
        side=side,
        atr=atr,
        params=_params(horizon, tp, sl),
        costs=_zero_slip() if costs is None else costs,
        mark_candles=mark,
        funding=funding,
        entry_is_maker=maker,
    )


def test_target_defaults_match_the_spec() -> None:
    params = TargetParams()
    assert params.tp_atr_mult == Decimal("2")
    assert params.sl_atr_mult == Decimal("1")
    assert params.horizon_bars == 24


def test_long_take_profit_on_the_entry_candle_still_needs_the_horizon_path() -> None:
    # stop 98, target 104. Вихід на свічці входу, але без хвоста до time-stop мітки немає:
    # інакше біля кінця ряду лишались би лише ранні TP.
    hit = _candle(0, 100, 105, "99.5", 104)
    with pytest.raises(IncompletePath):
        _sim((hit,), horizon=3, pad=False)
    trade = _sim((hit,), horizon=3)
    assert trade.exit_reason is ExitReason.TAKE_PROFIT
    assert trade.exit_index == 0
    assert trade.meta_label == 1
    assert trade.reference_entry == Decimal(100)
    assert trade.reference_stop == Decimal(98)
    assert trade.reference_take_profit == Decimal(104)
    assert trade.reference_exit == Decimal(104)
    assert trade.stop_price_source is StopPriceSource.LAST


def test_stop_level_comes_from_the_open_not_the_wick() -> None:
    # close 90 і low 90 пробивають стоп, але рівень лишається open − SL·ATR, не low.
    trade = _sim((_candle(0, 100, 101, 90, 90),))
    assert trade.exit_reason is ExitReason.STOP_LOSS
    assert trade.reference_entry == Decimal(100)
    assert trade.reference_stop == Decimal(98)
    assert trade.reference_exit == Decimal(98)
    assert trade.meta_label == 0


def test_same_candle_stop_and_target_is_a_stop() -> None:
    trade = _sim((_candle(0, 100, 110, 90, 100),))
    assert trade.exit_reason is ExitReason.STOP_LOSS
    assert trade.reference_exit == Decimal(98)
    assert trade.exit_fill == Decimal(98)


def test_stop_gap_slippage_is_applied_to_the_open() -> None:
    costs = CostModel(
        slippage_entry_atr=Decimal(0),
        slippage_entry_bps=Decimal(0),
        slippage_sl_atr=Decimal("0.1"),
        slippage_sl_bps=Decimal(0),
        slippage_tp_atr=Decimal(0),
        slippage_tp_bps=Decimal(0),
    )
    long = _sim(
        (_candle(0, 100, 101, 99, 100), _candle(1, 97, "97.5", 96, 97)),
        horizon=2,
        costs=costs,
    )
    short = _sim(
        (_candle(0, 100, 101, 99, 100), _candle(1, 104, 106, 103, 105)),
        side=_SHORT,
        horizon=2,
        costs=costs,
    )
    # slip = atr · 0.1 = 0.2, від open гепу, не від рівня стопа.
    assert long.reference_exit == Decimal(97)
    assert long.exit_fill == Decimal("96.8")
    assert short.reference_exit == Decimal(104)
    assert short.exit_fill == Decimal("104.2")


def test_stop_gap_exits_at_the_open_and_is_worse_than_the_level() -> None:
    candles = (
        _candle(0, 100, 101, 99, 100),
        _candle(1, 97, "97.5", 96, 97),
    )
    trade = _sim(candles, horizon=2)
    assert trade.exit_reason is ExitReason.STOP_LOSS
    assert trade.exit_index == 1
    assert trade.reference_exit == Decimal(97)
    assert trade.exit_fill == Decimal(97)
    # Той самий вхід, якби вийшли рівно по 98: gross −2 і комісія виходу з 98.
    at_level = Decimal("-2.198")
    assert trade.net_per_unit < at_level
    assert trade.loss_per_unit == Decimal("2.198")


def test_target_gap_keeps_the_target_price() -> None:
    candles = (
        _candle(0, 100, 101, 99, 100),
        _candle(1, 110, 112, 109, 111),
    )
    trade = _sim(candles, horizon=2)
    assert trade.exit_reason is ExitReason.TAKE_PROFIT
    assert trade.reference_exit == Decimal(104)
    assert trade.exit_fill == Decimal(104)


def test_wick_through_the_stop_wins_over_an_open_through_the_target() -> None:
    candles = (
        _candle(0, 100, 101, 99, 100),
        _candle(1, 110, 112, 90, 100),
    )
    trade = _sim(candles, horizon=2)
    assert trade.exit_reason is ExitReason.STOP_LOSS
    assert trade.reference_exit == Decimal(98)


def test_time_stop_uses_the_open_and_ignores_the_candle_wick() -> None:
    candles = (
        _candle(0, 100, 101, 99, 100),
        _candle(1, 100, 101, 99, 100),
        _candle(2, 101, 120, 100, 110),
    )
    trade = _sim(candles, horizon=2)
    assert trade.exit_reason is ExitReason.TIME_STOP
    assert trade.exit_index == 2
    assert trade.reference_exit == Decimal(101)
    assert trade.exit_fill == Decimal(101)
    assert trade.meta_label == 0
    assert check_time_stop(opened_at_ms=0, now_ms=2 * HOUR, max_hold_bars=2, bar_ms=HOUR).close is True
    assert check_time_stop(opened_at_ms=0, now_ms=HOUR, max_hold_bars=2, bar_ms=HOUR).close is False


def test_a_hit_on_the_last_in_horizon_bar_is_not_a_time_stop() -> None:
    candles = (
        _candle(0, 100, 101, 99, 100),
        _candle(1, 100, 105, 99, 104),
        _candle(2, 90, 91, 89, 90),
    )
    trade = _sim(candles, horizon=2)
    assert trade.exit_reason is ExitReason.TAKE_PROFIT
    assert trade.exit_index == 1
    assert trade.reference_exit == Decimal(104)


def test_time_stop_slippage_uses_the_entry_leg_against_the_close() -> None:
    costs = CostModel(
        slippage_entry_atr=Decimal("0.05"),
        slippage_entry_bps=Decimal(0),
        slippage_sl_atr=Decimal("0.5"),
        slippage_sl_bps=Decimal(0),
        slippage_tp_atr=Decimal(0),
        slippage_tp_bps=Decimal(0),
    )
    candles = _flat(2)
    long = _sim(candles, horizon=1, costs=costs)
    short = _sim(candles, side=_SHORT, horizon=1, costs=costs)
    # Вхід: 100 + 2·0.05 = 100.1. Вихід market: 100 ∓ 0.1, а не стоповий зсув 2·0.5.
    assert long.entry_fill == Decimal("100.1")
    assert long.exit_fill == Decimal("99.9")
    assert short.entry_fill == Decimal("99.9")
    assert short.exit_fill == Decimal("100.1")


def test_short_mirrors_the_stop_the_target_and_the_gap() -> None:
    target = _sim((_candle(0, 100, "100.5", 95, 96),), side=_SHORT)
    assert target.exit_reason is ExitReason.TAKE_PROFIT
    assert target.reference_stop == Decimal(102)
    assert target.reference_take_profit == Decimal(96)
    assert target.reference_exit == Decimal(96)

    stopped = _sim((_candle(0, 100, 103, 99, 102),), side=_SHORT)
    assert stopped.exit_reason is ExitReason.STOP_LOSS
    assert stopped.reference_exit == Decimal(102)

    both = _sim((_candle(0, 100, 110, 90, 100),), side=_SHORT)
    assert both.exit_reason is ExitReason.STOP_LOSS
    assert both.reference_exit == Decimal(102)

    gap = _sim(
        (
            _candle(0, 100, 101, 99, 100),
            _candle(1, 104, 106, 103, 105),
        ),
        side=_SHORT,
        horizon=2,
    )
    assert gap.exit_reason is ExitReason.STOP_LOSS
    assert gap.reference_exit == Decimal(104)
    assert gap.net_per_unit < Decimal("-2.202")


def test_mark_stop_beats_a_last_price_target_in_the_same_candle() -> None:
    last = (_candle(0, 100, 105, "99.5", 104), _candle(1, 100, 100, 100, 100))
    mark = (
        _candle(0, 100, "100.2", 97, 99),
        _candle(1, 100, "100.2", "99.8", 100),
    )
    trade = _sim(last, horizon=2, mark=mark)
    assert trade.stop_price_source is StopPriceSource.MARK
    assert trade.exit_reason is ExitReason.STOP_LOSS
    assert trade.reference_exit == Decimal(98)


def test_a_mark_gap_exits_at_the_mark_open() -> None:
    last = (_candle(0, 100, 101, 99, 100), _candle(1, 100, 101, 99, 100))
    mark = (_candle(0, 100, 101, 99, 100), _candle(1, 97, 98, 96, 97))
    trade = _sim(last, horizon=2, mark=mark)
    assert trade.stop_price_source is StopPriceSource.MARK
    assert trade.reference_exit == Decimal(97)


def test_a_mark_gap_fills_at_the_worse_of_the_mark_and_last_opens() -> None:
    # SL тригериться за mark, а market-вихід виконується за last: гірший з двох open, не кращий.
    mark = (_candle(0, 100, 101, 99, 100), _candle(1, 97, 98, 96, 97))
    long = _sim((_candle(0, 100, 101, 99, 100), _candle(1, 90, 91, 89, 90)), horizon=2, mark=mark)
    assert long.exit_reason is ExitReason.STOP_LOSS
    assert long.reference_exit == Decimal(90)

    short_mark = (_candle(0, 100, 101, 99, 100), _candle(1, 104, 105, 103, 104))
    short = _sim(
        (_candle(0, 100, 101, 99, 100), _candle(1, 106, 107, 105, 106)), side=_SHORT, horizon=2, mark=short_mark
    )
    assert short.exit_reason is ExitReason.STOP_LOSS
    assert short.reference_exit == Decimal(106)
    calmer = _sim(
        (_candle(0, 100, 101, 99, 100), _candle(1, 101, 102, 100, 101)), side=_SHORT, horizon=2, mark=short_mark
    )
    assert calmer.reference_exit == Decimal(104)


def test_a_short_stop_by_mark_beats_a_last_price_target() -> None:
    last = (_candle(0, 100, 101, 95, 96), _candle(1, 96, 97, 95, 96))
    mark = (_candle(0, 100, 103, "99.8", 100), _candle(1, 100, "100.2", "99.8", 100))
    trade = _sim(last, side=_SHORT, horizon=2, mark=mark)
    assert trade.stop_price_source is StopPriceSource.MARK
    assert trade.exit_reason is ExitReason.STOP_LOSS
    assert trade.reference_exit == Decimal(102)


def test_a_missing_mark_in_the_horizon_falls_back_for_the_whole_trade() -> None:
    last = (_candle(0, 100, 105, "99.5", 104), _candle(1, 100, 100, 100, 100))
    mark = (_candle(0, 100, "100.2", 97, 99),)
    trade = _sim(last, horizon=2, mark=mark)
    assert trade.stop_price_source is StopPriceSource.LAST
    assert trade.exit_reason is ExitReason.TAKE_PROFIT


def test_the_time_stop_candle_does_not_need_a_mark() -> None:
    last = (_candle(0, 100, 101, 99, 100), _candle(1, 100, 130, 99, 120))
    mark = (_candle(0, 100, 101, 99, 100),)
    trade = _sim(last, horizon=1, mark=mark)
    assert trade.stop_price_source is StopPriceSource.MARK
    assert trade.exit_reason is ExitReason.TIME_STOP
    assert trade.reference_exit == Decimal(100)


def test_an_early_exit_ignores_later_candles() -> None:
    hit = _candle(0, 100, 105, "99.5", 104)
    quiet = _candle(1, 100, 101, 99, 100)
    wild = _candle(1, 90, 91, 80, 85)
    assert _sim((hit, quiet), horizon=3) == _sim((hit, wild), horizon=3)


def test_a_gap_or_a_short_series_has_no_fabricated_exit() -> None:
    gapped = (_candle(0, 100, 101, 99, 100), _candle(2, 100, 101, 99, 100))
    with pytest.raises(IncompletePath):
        _sim(gapped, horizon=3, pad=False)
    with pytest.raises(IncompletePath):
        _sim(_flat(1), horizon=2, pad=False)
    # Ранній TP перед діркою теж без мітки: вибірка не залежить від того, чи бар'єр уже спрацював.
    hit_then_gap = (_candle(0, 100, 105, "99.5", 104), _candle(2, 100, 101, 99, 100))
    with pytest.raises(IncompletePath):
        _sim(hit_then_gap, horizon=3, pad=False)


def test_a_gap_after_the_time_stop_candle_does_not_cancel_the_label() -> None:
    trade = _sim((*_flat(2), _candle(3, 100, 101, 99, 100)), horizon=1, pad=False)
    assert trade.exit_reason is ExitReason.TIME_STOP
    assert trade.exit_index == 1


_QUIET = (_candle(0, 100, 101, 99, 100), _candle(1, 100, 101, 99, 100))


def test_a_time_stop_candle_opening_through_the_target_is_a_target() -> None:
    # Біржовий TP спрацьовує на гепі раніше за market-закриття бота: мітка 1 і ціна TP, не кращий open.
    long = _sim((*_QUIET, _candle(2, 110, 111, 109, 110)), horizon=2, pad=False)
    assert long.exit_reason is ExitReason.TAKE_PROFIT
    assert long.exit_index == 2
    assert long.exit_open_time_ms == 2 * HOUR
    assert long.reference_exit == Decimal(104)
    assert long.exit_fill == Decimal(104)
    assert long.meta_label == 1

    short = _sim((*_QUIET, _candle(2, 90, 91, 89, 90)), side=_SHORT, horizon=2, pad=False)
    assert short.exit_reason is ExitReason.TAKE_PROFIT
    assert short.reference_exit == Decimal(96)
    assert short.exit_fill == Decimal(96)
    assert short.meta_label == 1

    costs = CostModel()
    priced = costs.execution(side=_LONG, entry=Decimal(100), stop=Decimal(98), take_profit=Decimal(104), atr=_ATR)
    assert priced is not None
    default = _sim((*_QUIET, _candle(2, 110, 111, 109, 110)), horizon=2, pad=False, costs=costs)
    assert default.exit_fill == priced.take_profit_fill
    assert default.net_per_unit == priced.tp_net_per_unit


def test_a_time_stop_candle_opening_through_the_stop_is_a_stop_with_stop_slippage() -> None:
    costs = CostModel(
        slippage_entry_atr=Decimal("0.05"),
        slippage_entry_bps=Decimal(0),
        slippage_sl_atr=Decimal("0.5"),
        slippage_sl_bps=Decimal(0),
        slippage_tp_atr=Decimal(0),
        slippage_tp_bps=Decimal(0),
    )
    long = _sim((*_QUIET, _candle(2, 90, 91, 89, 90)), horizon=2, pad=False, costs=costs)
    assert long.exit_reason is ExitReason.STOP_LOSS
    assert long.exit_index == 2
    assert long.reference_exit == Decimal(90)
    # Нога STOP: 2·0.5 = 1 від open гепу, а не ринкова 2·0.05 = 0.1.
    assert long.exit_fill == Decimal(89)
    assert long.meta_label == 0

    short = _sim((*_QUIET, _candle(2, 110, 111, 109, 110)), side=_SHORT, horizon=2, pad=False, costs=costs)
    assert short.exit_reason is ExitReason.STOP_LOSS
    assert short.reference_exit == Decimal(110)
    assert short.exit_fill == Decimal(111)

    # Відкриття рівно на рівні — звичайний SL з ціною виконання з CostModel.execution.
    at_level = _sim((*_QUIET, _candle(2, 98, 99, 97, 98)), horizon=2, pad=False, costs=costs)
    priced = costs.execution(side=_LONG, entry=Decimal(100), stop=Decimal(98), take_profit=Decimal(104), atr=_ATR)
    assert priced is not None
    assert at_level.exit_reason is ExitReason.STOP_LOSS
    assert at_level.reference_exit == Decimal(98)
    assert at_level.exit_fill == priced.stop_fill
    assert at_level.net_per_unit == -priced.loss_per_unit


def test_the_time_stop_open_checks_the_stop_by_mark() -> None:
    last = (*_QUIET, _candle(2, 97, 98, 96, 97))
    calm_mark = (*_QUIET, _candle(2, 99, 100, "98.5", 99))
    # last відкрився за стопом, mark — ні: біржовий SL не спрацював, бот закриває по time-stop.
    held = _sim(last, horizon=2, mark=calm_mark, pad=False)
    assert held.stop_price_source is StopPriceSource.MARK
    assert held.exit_reason is ExitReason.TIME_STOP
    assert held.reference_exit == Decimal(97)

    gapped_mark = (*_QUIET, _candle(2, 96, 97, 95, 96))
    stopped = _sim(last, horizon=2, mark=gapped_mark, pad=False)
    assert stopped.stop_price_source is StopPriceSource.MARK
    assert stopped.exit_reason is ExitReason.STOP_LOSS
    assert stopped.reference_exit == Decimal(96)

    # Mark свічки time-stop немає: open перевіряється за last, і вихід, вирішений так, — fallback у журналі.
    fallback = _sim(last, horizon=2, mark=_QUIET, pad=False)
    assert fallback.stop_price_source is StopPriceSource.LAST
    assert fallback.exit_reason is ExitReason.STOP_LOSS
    assert fallback.reference_exit == Decimal(97)
    target = _sim((*_QUIET, _candle(2, 110, 111, 109, 110)), horizon=2, mark=_QUIET, pad=False)
    assert target.exit_reason is ExitReason.TAKE_PROFIT
    assert target.stop_price_source is StopPriceSource.LAST
    # Без гепу на цьому open мітка від mark не залежить: джерело лишається MARK.
    quiet = _sim((*_QUIET, _candle(2, 100, 101, 99, 100)), horizon=2, mark=_QUIET, pad=False)
    assert quiet.exit_reason is ExitReason.TIME_STOP
    assert quiet.stop_price_source is StopPriceSource.MARK


def test_a_stop_gap_beats_a_target_gap_on_the_time_stop_open() -> None:
    last = (*_QUIET, _candle(2, 110, 111, 109, 110))
    mark = (*_QUIET, _candle(2, 97, 98, 96, 97))
    trade = _sim(last, horizon=2, mark=mark, pad=False)
    assert trade.exit_reason is ExitReason.STOP_LOSS
    assert trade.reference_exit == Decimal(97)
    assert trade.meta_label == 0


def test_default_costs_match_execution_for_a_stop_and_a_target() -> None:
    costs = CostModel()
    stopped = _sim((_candle(0, 100, 101, 97, 99),), costs=costs)
    priced_stop = costs.execution(side=_LONG, entry=Decimal(100), stop=Decimal(98), take_profit=Decimal(104), atr=_ATR)
    assert priced_stop is not None
    assert stopped.entry_fill == priced_stop.entry_fill
    assert stopped.exit_fill == priced_stop.stop_fill
    assert stopped.funding == Decimal(0)
    assert stopped.net_per_unit == -priced_stop.loss_per_unit

    target = _sim((_candle(0, 100, 105, 99, 104),), costs=costs)
    priced_tp = costs.execution(side=_LONG, entry=Decimal(100), stop=Decimal(98), take_profit=Decimal(104), atr=_ATR)
    assert priced_tp is not None
    assert priced_tp.tp_net_per_unit is not None
    assert target.exit_fill == priced_tp.take_profit_fill
    assert target.net_per_unit == priced_tp.tp_net_per_unit


def test_a_maker_entry_is_rejected_until_limit_fills_exist() -> None:
    with pytest.raises(ValueError, match="market-вхід"):
        _sim((_candle(0, 100, 101, 97, 99),), maker=True)


def test_funding_is_charged_on_the_exit_open_and_not_on_the_entry_open() -> None:
    candles = (
        _candle(0, 100, 101, 99, 100),
        _candle(1, 100, 101, 97, 99),
    )
    events = (
        FundingEvent(time_ms=0, rate=Decimal("0.01"), mark_price=Decimal(50)),
        FundingEvent(time_ms=HOUR, rate=Decimal("0.01"), mark_price=Decimal(50)),
        FundingEvent(time_ms=HOUR + 1, rate=Decimal("0.01"), mark_price=Decimal(50)),
    )
    trade = _sim(candles, horizon=2, funding=events)
    assert trade.exit_open_time_ms == HOUR
    assert trade.funding == Decimal("-0.5")
    assert trade.net_per_unit == Decimal("-2.198") + trade.funding


def test_a_short_receives_positive_funding() -> None:
    candles = (
        _candle(0, 100, 101, 99, 100),
        _candle(1, 100, 103, 99, 102),
    )
    events = (
        FundingEvent(time_ms=0, rate=Decimal("0.01"), mark_price=Decimal(50)),
        FundingEvent(time_ms=HOUR, rate=Decimal("0.01"), mark_price=Decimal(50)),
        FundingEvent(time_ms=HOUR + 1, rate=Decimal("0.01"), mark_price=Decimal(50)),
    )
    trade = _sim(candles, side=_SHORT, horizon=2, funding=events)
    assert trade.exit_reason is ExitReason.STOP_LOSS
    assert trade.exit_open_time_ms == HOUR
    assert trade.funding == Decimal("0.5")
    assert trade.net_per_unit == Decimal("-2.202") + trade.funding


def test_a_crash_below_the_slippage_is_a_total_loss_not_a_skip() -> None:
    # Ціна виконання після slippage не може бути ≤ 0, але угода від цього не зникає: інакше з вибірки
    # випадали б саме найгірші збитки. Вихід — 0, тобто вся вартість позиції.
    costs = CostModel()
    crash = (_candle(0, 100, 101, 99, 100), _candle(1, "0.1", "0.1", "0.05", "0.1"))
    stopped = _sim(crash, horizon=2, costs=costs)
    assert stopped.exit_reason is ExitReason.STOP_LOSS
    assert stopped.reference_exit == Decimal("0.1")
    assert stopped.exit_fill == Decimal(0)
    assert stopped.net_per_unit == -stopped.entry_fill - costs.taker_fee_rate * stopped.entry_fill
    assert stopped.meta_label == 0

    labeled = label_meta(
        _pad(crash, index=0, horizon=2),
        (EntryCandidate(entry_index=0, side=_LONG, symbol=SYMBOL),),
        params=_params(2),
        costs=costs,
        atr_by_index={0: _ATR},
    )
    assert labeled.skipped == ()
    assert [sample.label for sample in labeled.samples] == [0]

    # Обвал last на open свічки time-stop при спокійному mark: SL не спрацював, time-stop закриває по 0.
    calm = (*_QUIET, _candle(2, 100, 101, 99, 100))
    held = _sim((*_QUIET, _candle(2, "0.05", "0.05", "0.05", "0.05")), horizon=2, mark=calm, pad=False, costs=costs)
    assert held.exit_reason is ExitReason.TIME_STOP
    assert held.exit_fill == Decimal(0)


def test_meta_labels_follow_the_candidates_and_a_skip_is_not_a_zero() -> None:
    candles = _pad(
        (
            _candle(0, 100, 105, "99.5", 104),
            _candle(1, 100, 101, 99, 100),
        ),
        index=1,
        horizon=1,
    )
    labeled = label_meta(
        candles,
        (
            EntryCandidate(entry_index=0, side=_LONG, symbol=SYMBOL),
            EntryCandidate(entry_index=0, side=_SHORT, symbol=SYMBOL),
            EntryCandidate(entry_index=1, side=_LONG, symbol=SYMBOL),
            EntryCandidate(entry_index=3, side=_LONG, symbol=SYMBOL),
        ),
        params=_params(1),
        costs=_zero_slip(),
        atr_by_index={0: _ATR},
    )
    assert [sample.label for sample in labeled.samples] == [1, 0]
    assert labeled.samples[0].trade.side is _LONG
    assert labeled.samples[1].trade.side is _SHORT
    assert [(item.entry_index, item.reason) for item in labeled.skipped] == [
        (1, SkipReason.NO_ATR),
        (3, SkipReason.INCOMPLETE_PATH),
    ]


def test_direct_labels_up_down_and_flat() -> None:
    up = label_direct(
        _pad((_candle(0, 100, 105, "99.5", 104),), index=0, horizon=3),
        symbol=SYMBOL,
        entry_indexes=(0,),
        params=_params(3),
        costs=_zero_slip(),
        atr_by_index={0: _ATR},
    )
    assert up.samples[0].label is DirectClass.LONG
    assert up.samples[0].both_reached_tp is False
    assert up.samples[0].short_trade.meta_label == 0

    down = label_direct(
        _pad((_candle(0, 100, "100.5", 95, 96),), index=0, horizon=3),
        symbol=SYMBOL,
        entry_indexes=(0,),
        params=_params(3),
        costs=_zero_slip(),
        atr_by_index={0: _ATR},
    )
    assert down.samples[0].label is DirectClass.SHORT
    assert down.samples[0].long_trade.meta_label == 0

    flat = label_direct(
        _flat(2),
        symbol=SYMBOL,
        entry_indexes=(0,),
        params=_params(1),
        costs=_zero_slip(),
        atr_by_index={0: _ATR},
    )
    assert flat.samples[0].label is DirectClass.FLAT
    assert flat.samples[0].both_reached_tp is False
    assert flat.samples[0].long_trade.exit_reason is ExitReason.TIME_STOP
    assert flat.samples[0].short_trade.exit_reason is ExitReason.TIME_STOP


def test_both_targets_when_the_target_is_closer_than_the_stop_become_flat() -> None:
    # tp 1 ATR, sl 2 ATR: спершу вниз до SHORT TP, потім вгору до LONG TP. Жоден SL не зачеплено.
    candles = _pad(
        (
            _candle(0, 100, "100.5", "97.5", 99),
            _candle(1, 100, 103, "99.5", 102),
        ),
        index=0,
        horizon=2,
    )
    labeled = label_direct(
        candles,
        symbol=SYMBOL,
        entry_indexes=(0,),
        params=_params(2, tp="1", sl="2"),
        costs=_zero_slip(),
        atr_by_index={0: _ATR},
    )
    sample = labeled.samples[0]
    assert sample.long_trade.meta_label == 1
    assert sample.short_trade.meta_label == 1
    assert sample.label is DirectClass.FLAT
    assert sample.both_reached_tp is True


def test_both_targets_when_mark_and_last_diverge_become_flat() -> None:
    last = _pad(
        (
            _candle(0, 100, 105, "99.5", 104),
            _candle(1, 100, "100.5", 95, 96),
        ),
        index=0,
        horizon=2,
    )
    mark = (
        _candle(0, 100, "100.2", "99.8", 100),
        _candle(1, 100, "100.2", "99.8", 100),
    )
    labeled = label_direct(
        last,
        symbol=SYMBOL,
        entry_indexes=(0,),
        params=_params(2),
        costs=_zero_slip(),
        mark_candles=mark,
        atr_by_index={0: _ATR},
    )
    sample = labeled.samples[0]
    assert sample.long_trade.stop_price_source is StopPriceSource.MARK
    assert sample.long_trade.meta_label == 1
    assert sample.short_trade.meta_label == 1
    assert sample.label is DirectClass.FLAT
    assert sample.both_reached_tp is True


@given(data=st.data())
@settings(max_examples=40, deadline=None)
def test_long_and_short_never_both_win_on_the_same_candles(data: st.DataObject) -> None:
    horizon = data.draw(st.integers(min_value=1, max_value=4))
    tp_units = data.draw(st.integers(min_value=1, max_value=3))
    atr = data.draw(st.sampled_from((Decimal(1), Decimal("1.5"), Decimal(2))))
    count = horizon + 1
    price = 100
    bars: list[SimCandle] = []
    for index in range(count):
        up = data.draw(st.integers(min_value=0, max_value=30))
        down = data.draw(st.integers(min_value=0, max_value=30))
        delta = data.draw(st.integers(min_value=-15, max_value=15))
        open_ = Decimal(price)
        close = open_ + Decimal(delta)
        high = max(open_, close) + Decimal(up)
        low = min(open_, close) - Decimal(down)
        if low <= 0 or close <= 0:
            close = open_
            high = max(open_, close) + Decimal(up)
            low = min(open_, close) - Decimal(down)
        if low <= 0:
            low = Decimal(1)
            open_ = max(open_, low)
            close = max(close, low)
            high = max(high, open_, close)
        bars.append(_candle(index, open_, high, low, close))
        price = max(int(close), 40)
    labeled = label_direct(
        tuple(bars),
        symbol=SYMBOL,
        entry_indexes=(0,),
        params=_params(horizon, tp=str(tp_units), sl="1"),
        costs=_zero_slip(),
        atr_by_index={0: atr},
    )
    assert len(labeled.samples) == 1
    for sample in labeled.samples:
        assert not (sample.long_trade.meta_label == 1 and sample.short_trade.meta_label == 1)
        assert sample.label in {DirectClass.LONG, DirectClass.SHORT, DirectClass.FLAT}


@given(data=st.data())
@settings(max_examples=200, deadline=None)
def test_long_and_short_never_both_win_with_gapped_opens(data: st.DataObject) -> None:
    # Кожен open незалежний від попереднього close: геп можливий на будь-якій свічці, і на свічці time-stop теж.
    horizon = data.draw(st.integers(min_value=1, max_value=4))
    tp_units = data.draw(st.integers(min_value=1, max_value=3))
    atr = data.draw(st.sampled_from((Decimal(1), Decimal("1.5"), Decimal(2))))
    bars: list[SimCandle] = []
    for index in range(horizon + 1):
        open_ = Decimal(data.draw(st.integers(min_value=90, max_value=110)))
        close = Decimal(data.draw(st.integers(min_value=90, max_value=110)))
        high = max(open_, close) + Decimal(data.draw(st.integers(min_value=0, max_value=6)))
        low = min(open_, close) - Decimal(data.draw(st.integers(min_value=0, max_value=6)))
        bars.append(_candle(index, open_, high, low, close))
    labeled = label_direct(
        tuple(bars),
        symbol=SYMBOL,
        entry_indexes=(0,),
        params=_params(horizon, tp=str(tp_units), sl="1"),
        costs=_zero_slip(),
        atr_by_index={0: atr},
    )
    assert len(labeled.samples) == 1
    sample = labeled.samples[0]
    assert sample.both_reached_tp is False
    assert not (sample.long_trade.meta_label == 1 and sample.short_trade.meta_label == 1)


def _random_bar(data: st.DataObject, index: int) -> SimCandle:
    open_ = Decimal(data.draw(st.integers(min_value=90, max_value=110)))
    close = Decimal(data.draw(st.integers(min_value=90, max_value=110)))
    high = max(open_, close) + Decimal(data.draw(st.integers(min_value=0, max_value=6)))
    low = min(open_, close) - Decimal(data.draw(st.integers(min_value=0, max_value=6)))
    return _candle(index, open_, high, low, close)


@given(data=st.data())
@settings(max_examples=300, deadline=None)
def test_exits_are_never_better_than_the_barrier_rules(data: st.DataObject) -> None:
    # Песимізм симулятора: SL не кращий за рівень, TP рівно за ціною TP, time-stop не за TP.
    horizon = data.draw(st.integers(min_value=1, max_value=4))
    side = data.draw(st.sampled_from((_LONG, _SHORT)))
    with_mark = data.draw(st.booleans())
    last = tuple(_random_bar(data, index) for index in range(horizon + 1))
    mark = tuple(_random_bar(data, index) for index in range(horizon + 1)) if with_mark else None
    trade = simulate_trade(
        last,
        entry_index=0,
        side=side,
        atr=_ATR,
        params=_params(horizon),
        costs=_zero_slip(),
        mark_candles=mark,
    )
    long = side is _LONG
    if trade.exit_reason is ExitReason.STOP_LOSS:
        assert trade.reference_exit <= trade.reference_stop if long else trade.reference_exit >= trade.reference_stop
        assert trade.net_per_unit <= -trade.loss_per_unit
    elif trade.exit_reason is ExitReason.TAKE_PROFIT:
        assert trade.reference_exit == trade.reference_take_profit
        priced = _zero_slip().execution(
            side=side,
            entry=trade.reference_entry,
            stop=trade.reference_stop,
            take_profit=trade.reference_take_profit,
            atr=_ATR,
        )
        assert priced is not None
        assert trade.net_per_unit == priced.tp_net_per_unit
    else:
        assert trade.exit_index == horizon
        assert (
            trade.reference_exit < trade.reference_take_profit
            if long
            else trade.reference_exit > trade.reference_take_profit
        )
        if mark is None:
            # Без mark SL і time-stop дивляться на ті самі ціни: time-stop не буває за рівнем SL.
            assert trade.reference_exit > trade.reference_stop if long else trade.reference_exit < trade.reference_stop


def test_atr_matches_features_and_ignores_the_entry_candle() -> None:
    count = FEATURE_LOOKBACK_BARS + 2
    rng = np.random.default_rng(0)
    price = 100.0
    bars: list[CandleBar] = []
    for index in range(count):
        open_ = price
        close = float(price * math.exp(float(rng.normal(0.0, 0.01))))
        high = max(open_, close) * (1.0 + float(rng.uniform(0.0, 0.004)))
        low = min(open_, close) * (1.0 - float(rng.uniform(0.0, 0.004)))
        bars.append(CandleBar(index * HOUR, open_, high, low, close, 1.0))
        price = close
    series = tuple(bars)
    sim = tuple(
        SimCandle(
            open_time_ms=bar.open_time_ms,
            open=Decimal(repr(bar.open)),
            high=Decimal(repr(bar.high)),
            low=Decimal(repr(bar.low)),
            close=Decimal(repr(bar.close)),
        )
        for bar in series
    )
    entry = FEATURE_LOOKBACK_BARS
    atr = atr_before_entry(sim, entry)
    window = series[:entry]
    expected = wilder_atr(
        np.array([bar.high for bar in window], dtype=np.float64),
        np.array([bar.low for bar in window], dtype=np.float64),
        np.array([bar.close for bar in window], dtype=np.float64),
    )
    assert float(atr) == expected
    rows = compute_features({SYMBOL: SymbolData(candles=series)}, enabled_sources=())
    row = entry - 1
    assert int(rows.entry_open_time_ms[row]) == series[entry].open_time_ms
    assert bool(rows.valid[row])
    column = rows.columns.index("atr_pct")
    assert math.isclose(float(atr), float(rows.values[row, column]) * series[row].close, rel_tol=1e-9, abs_tol=1e-12)

    bumped = list(sim)
    entry_bar = bumped[entry]
    bumped[entry] = SimCandle(
        open_time_ms=entry_bar.open_time_ms,
        open=entry_bar.open + 1,
        high=entry_bar.high + 1,
        low=entry_bar.low + 1,
        close=entry_bar.close + 1,
    )
    assert atr_before_entry(tuple(bumped), entry) == atr
    previous = bumped[entry - 1]
    bumped[entry - 1] = SimCandle(
        open_time_ms=previous.open_time_ms,
        open=previous.open,
        high=previous.high + 50,
        low=previous.low,
        close=previous.close,
    )
    assert atr_before_entry(tuple(bumped), entry) != atr


def test_atr_needs_a_contiguous_feature_window() -> None:
    with pytest.raises(NoAtr):
        atr_before_entry(_flat(10), 5)
    sim = list(_flat(FEATURE_LOOKBACK_BARS + 1))
    jumped = sim[10]
    sim[10] = SimCandle(
        open_time_ms=jumped.open_time_ms + HOUR,
        open=jumped.open,
        high=jumped.high,
        low=jumped.low,
        close=jumped.close,
    )
    with pytest.raises(NoAtr):
        atr_before_entry(tuple(sim), FEATURE_LOOKBACK_BARS)


def test_uniqueness_on_a_known_overlap() -> None:
    spans = (
        LabelSpan(symbol="A", entry_open_time_ms=0, exit_open_time_ms=2, bar_ms=1),
        LabelSpan(symbol="A", entry_open_time_ms=1, exit_open_time_ms=2, bar_ms=1),
        LabelSpan(symbol="A", entry_open_time_ms=2, exit_open_time_ms=2, bar_ms=1),
    )
    assert uniqueness_weights(spans) == (
        Decimal(11) / Decimal(18),
        Decimal(5) / Decimal(12),
        Decimal(1) / Decimal(3),
    )


def test_uniqueness_does_not_cross_symbols() -> None:
    spans = (
        LabelSpan(symbol="A", entry_open_time_ms=0, exit_open_time_ms=2, bar_ms=1),
        LabelSpan(symbol="B", entry_open_time_ms=0, exit_open_time_ms=2, bar_ms=1),
    )
    assert uniqueness_weights(spans) == (Decimal(1), Decimal(1))


def test_uniqueness_follows_the_trade_lifetime_not_the_full_horizon() -> None:
    early = _sim((_candle(0, 100, 105, "99.5", 104),), horizon=3)
    held = _sim(_flat(4), horizon=3)
    spans = (label_span("S", early), label_span("S", held))
    # Ранній TP живе один бар і ділить його з угодою, що дійшла до time-stop.
    assert early.exit_index == 0
    assert held.exit_index == 3
    assert uniqueness_weights(spans) == (Decimal("0.5"), Decimal(7) / Decimal(8))


def test_sample_weight_is_uniqueness_times_the_global_class_balance() -> None:
    separate = (
        LabelSpan(symbol="A", entry_open_time_ms=0, exit_open_time_ms=0, bar_ms=1),
        LabelSpan(symbol="A", entry_open_time_ms=10, exit_open_time_ms=10, bar_ms=1),
        LabelSpan(symbol="B", entry_open_time_ms=0, exit_open_time_ms=0, bar_ms=1),
    )
    # Унікальність 1. Класи на всьому наборі: дві «1» і одна «0» → 3/4 і 3/2, не 1 всередині символу A.
    assert sample_weights(separate, (1, 1, 0)) == (Decimal("0.75"), Decimal("0.75"), Decimal("1.5"))

    overlap = (
        LabelSpan(symbol="A", entry_open_time_ms=0, exit_open_time_ms=2, bar_ms=1),
        LabelSpan(symbol="A", entry_open_time_ms=1, exit_open_time_ms=2, bar_ms=1),
        LabelSpan(symbol="A", entry_open_time_ms=2, exit_open_time_ms=2, bar_ms=1),
    )
    assert sample_weights(overlap, (1, 1, 0)) == (
        Decimal(11) / Decimal(24),
        Decimal(5) / Decimal(16),
        Decimal("0.5"),
    )
    assert sample_weights((), ()) == ()
    with pytest.raises(ValueError, match="порівну"):
        sample_weights(separate[:1], ())


def test_a_negative_barrier_or_a_non_positive_fill_is_not_labeled() -> None:
    with pytest.raises(UnusableExit):
        _sim((_candle(0, 1, 2, 1, 1),), atr=Decimal(2))
    costs = CostModel(
        slippage_entry_atr=Decimal(0),
        slippage_entry_bps=Decimal(0),
        slippage_sl_atr=Decimal(50),
        slippage_sl_bps=Decimal(0),
        slippage_tp_atr=Decimal(0),
        slippage_tp_bps=Decimal(0),
    )
    with pytest.raises(UnusableExit):
        _sim(_flat(1), costs=costs)
    skipped = label_meta(
        _flat(2),
        (EntryCandidate(entry_index=0, side=_LONG, symbol=SYMBOL),),
        params=_params(1),
        costs=costs,
        atr_by_index={0: _ATR},
    )
    assert skipped.samples == ()
    assert skipped.skipped[0].reason is SkipReason.UNUSABLE_EXIT


def test_a_mark_gap_on_the_entry_candle_uses_the_mark_open() -> None:
    last = (_candle(0, 100, 101, 99, 100), _candle(1, 100, 101, 99, 100))
    mark = (_candle(0, 97, 98, 96, 97),)
    trade = _sim(last, horizon=1, mark=mark, pad=False)
    assert trade.stop_price_source is StopPriceSource.MARK
    assert trade.reference_entry == Decimal(100)
    assert trade.reference_exit == Decimal(97)
    assert trade.exit_index == 0


def test_direct_span_runs_to_the_later_exit() -> None:
    labeled = label_direct(
        _pad(
            (
                _candle(0, 100, 105, "99.5", 104),
                _candle(1, 100, "100.5", 95, 96),
            ),
            index=0,
            horizon=2,
        ),
        symbol=SYMBOL,
        entry_indexes=(0,),
        params=_params(2),
        costs=_zero_slip(),
        mark_candles=(
            _candle(0, 100, "100.2", "99.8", 100),
            _candle(1, 100, "100.2", "99.8", 100),
        ),
        atr_by_index={0: _ATR},
    )
    sample = labeled.samples[0]
    assert sample.long_trade.exit_index == 0
    assert sample.short_trade.exit_index == 1
    span = label_span_direct(SYMBOL, sample)
    assert span.entry_open_time_ms == 0
    assert span.exit_open_time_ms == HOUR


def test_label_meta_without_an_atr_map_uses_atr_before_entry() -> None:
    count = FEATURE_LOOKBACK_BARS + 25
    rng = np.random.default_rng(1)
    price = 100.0
    bars: list[SimCandle] = []
    for index in range(count):
        open_ = price
        close = float(price * math.exp(float(rng.normal(0.0, 0.002))))
        high = max(open_, close) * 1.001
        low = min(open_, close) * 0.999
        bars.append(
            SimCandle(
                open_time_ms=index * HOUR,
                open=Decimal(repr(open_)),
                high=Decimal(repr(high)),
                low=Decimal(repr(low)),
                close=Decimal(repr(close)),
            )
        )
        price = close
    series = tuple(bars)
    entry = FEATURE_LOOKBACK_BARS
    labeled = label_meta(
        series,
        (EntryCandidate(entry_index=entry, side=_LONG, symbol=SYMBOL),),
        params=_params(24),
        costs=_zero_slip(),
    )
    expected = simulate_trade(
        series,
        entry_index=entry,
        side=_LONG,
        atr=atr_before_entry(series, entry),
        params=_params(24),
        costs=_zero_slip(),
    )
    assert len(labeled.samples) == 1
    assert labeled.samples[0].trade == expected


def test_bad_candles_and_duplicate_marks_are_rejected() -> None:
    with pytest.raises(ValidationError):
        SimCandle(open_time_ms=0, open=1.0, high=1.0, low=1.0, close=1.0)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="дубль"):
        _sim(_flat(1), mark=(_candle(0, 100, 100, 100, 100), _candle(0, 100, 101, 99, 100)))


def test_a_mark_series_built_once_gives_the_same_labels() -> None:
    last = _pad(
        (
            _candle(0, 100, 105, "99.5", 104),
            _candle(1, 100, "100.5", 95, 96),
        ),
        index=0,
        horizon=2,
    )
    raw = (
        _candle(0, 100, "100.2", 97, 99),
        _candle(1, 100, "100.2", "99.8", 100),
    )
    series = MarkSeries(raw)
    assert len(series) == 2
    assert series.get(0) == raw[0]
    assert series.get(2 * HOUR) is None
    assert _sim(last, horizon=2, mark=series) == _sim(last, horizon=2, mark=raw)

    def direct(mark: tuple[SimCandle, ...] | MarkSeries) -> object:
        return label_direct(
            last,
            symbol=SYMBOL,
            entry_indexes=(0,),
            params=_params(2),
            costs=_zero_slip(),
            mark_candles=mark,
            atr_by_index={0: _ATR},
        )

    def meta(mark: tuple[SimCandle, ...] | MarkSeries) -> object:
        return label_meta(
            last,
            (EntryCandidate(entry_index=0, side=_SHORT, symbol=SYMBOL),),
            params=_params(2),
            costs=_zero_slip(),
            mark_candles=mark,
            atr_by_index={0: _ATR},
        )

    assert direct(series) == direct(raw)
    assert meta(series) == meta(raw)

    with pytest.raises(ValueError, match="дубль"):
        MarkSeries((raw[0], raw[0]))
    with pytest.raises(ValueError, match="SimCandle"):
        MarkSeries((object(),))  # type: ignore[arg-type]
    assert len(MarkSeries(())) == 0


def test_strict_path_rejects_a_gap_after_an_early_target_and_the_loose_path_keeps_it() -> None:
    hit = _candle(0, 100, 105, "99.5", 104)
    later = _candle(2, 100, 101, 99, 100)
    with pytest.raises(IncompletePath) as caught:
        simulate_trade(
            (hit, later),
            entry_index=0,
            side=_LONG,
            atr=_ATR,
            params=_params(3),
            costs=_zero_slip(),
        )
    assert caught.value.last_index is None
    trade = simulate_trade(
        (hit, later),
        entry_index=0,
        side=_LONG,
        atr=_ATR,
        params=_params(3),
        costs=_zero_slip(),
        strict_path=False,
    )
    assert trade.exit_reason is ExitReason.TAKE_PROFIT
    assert trade.exit_index == 0
    complete = _pad((hit,), index=0, horizon=3)
    assert simulate_trade(
        complete,
        entry_index=0,
        side=_LONG,
        atr=_ATR,
        params=_params(3),
        costs=_zero_slip(),
        strict_path=False,
    ) == simulate_trade(
        complete,
        entry_index=0,
        side=_LONG,
        atr=_ATR,
        params=_params(3),
        costs=_zero_slip(),
    )


def test_loose_path_names_the_last_bar_when_no_barrier_fired() -> None:
    bars = tuple(_candle(index, 100, 101, 99, 100) for index in (0, 1, 3))
    with pytest.raises(IncompletePath) as caught:
        simulate_trade(
            bars,
            entry_index=0,
            side=_LONG,
            atr=_ATR,
            params=_params(4),
            costs=_zero_slip(),
            strict_path=False,
        )
    assert caught.value.last_index == 1


def test_broken_path_is_the_stop_budget_not_a_skipped_trade() -> None:
    from app.ml.trade_sim import close_broken_path

    bars = tuple(_candle(index, 100, 101, 99, 100) for index in range(2))
    trade = close_broken_path(
        bars,
        entry_index=0,
        last_index=1,
        side=_LONG,
        atr=_ATR,
        params=_params(4),
        costs=_zero_slip(),
    )
    assert trade.exit_reason is ExitReason.DATA_GAP
    assert trade.exit_index == 1
    assert trade.net_per_unit == -trade.loss_per_unit
    assert trade.meta_label == 0
    with pytest.raises(ValueError, match="не суцільний"):
        close_broken_path(
            tuple(_candle(index, 100, 101, 99, 100) for index in (0, 2)),
            entry_index=0,
            last_index=1,
            side=_LONG,
            atr=_ATR,
            params=_params(4),
            costs=_zero_slip(),
        )


def test_market_close_of_an_open_trade_takes_the_given_price_and_market_costs() -> None:
    from app.ml.trade_sim import close_at_market

    bars = _pad((_candle(0, 100, 101, 99, 100), _candle(1, 100, "100.5", "98.5", 99)), index=0, horizon=3)
    trade = simulate_trade(bars, entry_index=0, side=_LONG, atr=_ATR, params=_params(3), costs=_zero_slip())
    assert trade.exit_reason is ExitReason.TIME_STOP
    closed = close_at_market(bars, trade, exit_index=1, reference_exit=Decimal("98.5"), costs=_zero_slip())
    assert closed.exit_reason is ExitReason.EMERGENCY_STOP
    assert closed.exit_index == 1
    assert closed.exit_open_time_ms == bars[1].open_time_ms
    assert closed.reference_exit == Decimal("98.5")
    assert closed.exit_fill == Decimal("98.5")
    # −1,5 − 0,1 − 0,0985
    assert closed.net_per_unit == Decimal("-1.6985")
    assert closed.loss_per_unit == trade.loss_per_unit
    assert closed.stop_price_source is trade.stop_price_source
    assert closed.meta_label == 0

    # Market-вихід: slippage ноги входу (10 bps) проти закриття, як у time-stop.
    slipped = CostModel(
        taker_fee_rate=Decimal("0.001"),
        maker_fee_rate=Decimal("0.0002"),
        slippage_entry_atr=Decimal(0),
        slippage_entry_bps=Decimal(10),
        slippage_sl_atr=Decimal(0),
        slippage_sl_bps=Decimal(0),
        slippage_tp_atr=Decimal(0),
        slippage_tp_bps=Decimal(0),
    )
    costly = simulate_trade(bars, entry_index=0, side=_LONG, atr=_ATR, params=_params(3), costs=slipped)
    forced = close_at_market(bars, costly, exit_index=1, reference_exit=Decimal("98.5"), costs=slipped)
    assert forced.exit_fill == Decimal("98.4015")
    # 98,4015 − 100,1 − 0,1001 − 0,0984015
    assert forced.net_per_unit == Decimal("-1.8970015")


def test_market_close_rejects_a_bar_the_exchange_already_closed_and_foreign_inputs() -> None:
    from app.ml.trade_sim import close_at_market, close_broken_path

    bars = _pad((_candle(0, 100, 101, 99, 100), _candle(1, 100, "100.5", 97, 99)), index=0, horizon=3)
    stopped = simulate_trade(bars, entry_index=0, side=_LONG, atr=_ATR, params=_params(3), costs=_zero_slip())
    assert stopped.exit_reason is ExitReason.STOP_LOSS
    with pytest.raises(ValueError, match="вже закрита"):
        close_at_market(bars, stopped, exit_index=1, reference_exit=Decimal(99), costs=_zero_slip())
    with pytest.raises(ValueError, match="поза свічкою"):
        close_at_market(bars, stopped, exit_index=0, reference_exit=Decimal("98.9"), costs=_zero_slip())
    with pytest.raises(ValueError, match="модель витрат"):
        close_at_market(bars, stopped, exit_index=0, reference_exit=Decimal(99), costs=CostModel())
    with pytest.raises(ValueError, match="exit_index"):
        close_at_market(bars, stopped, exit_index=2, reference_exit=Decimal(99), costs=_zero_slip())
    broken = close_broken_path(
        bars, entry_index=0, last_index=0, side=_LONG, atr=_ATR, params=_params(3), costs=_zero_slip()
    )
    with pytest.raises(ValueError, match="DATA_GAP"):
        close_at_market(bars, broken, exit_index=0, reference_exit=Decimal(99), costs=_zero_slip())
