"""Бектест: ті самі угоди, розмір і ліміти, що live (ТЗ §5.2.3, §6.6, T2.4)."""

from __future__ import annotations

import re
from decimal import ROUND_DOWN, Decimal

import pytest

from app.exchange.types import PositionSide
from app.ml.backtest import (
    BacktestReport,
    Cashflow,
    EntrySignal,
    FundingCoverageError,
    run_backtest,
)
from app.ml.features import FEATURE_LOOKBACK_BARS, HOUR_MS
from app.ml.trade_sim import ExitReason, SimCandle, StopPriceSource, TargetParams
from app.risk.costs import CostModel, FundingEvent
from app.risk.limits import adjust_equity_anchors
from app.risk.numbers import div_down
from app.risk.reasons import RiskReason
from app.trading.models import REASON_CODE_RE
from tests.risk.conftest import instrument, limits

_FEE = Decimal("0.001")
_STEP = Decimal("0.001")
_LONG = PositionSide.LONG
_SHORT = PositionSide.SHORT
_START = Decimal(10000)
_MMR = Decimal("0.005")
_HOUR = HOUR_MS


def _costs(**overrides: Decimal) -> CostModel:
    data: dict[str, Decimal] = {
        "taker_fee_rate": _FEE,
        "maker_fee_rate": _FEE,
        "slippage_entry_atr": Decimal(0),
        "slippage_entry_bps": Decimal(0),
        "slippage_sl_atr": Decimal(0),
        "slippage_sl_bps": Decimal(0),
        "slippage_tp_atr": Decimal(0),
        "slippage_tp_bps": Decimal(0),
    }
    data.update(overrides)
    return CostModel(**data)


def _bar(index: int, open_: str, high: str, low: str, close: str) -> SimCandle:
    return SimCandle(
        open_time_ms=index * _HOUR,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
    )


def _flat(count: int) -> list[SimCandle]:
    """Діапазон 2 на кожній свічці: Wilder ATR сходиться до 2."""
    return [_bar(index, "100", "101", "99", "100") for index in range(count)]


def _qty(equity: Decimal, loss: Decimal) -> Decimal:
    raw = div_down(equity * Decimal("0.005"), loss)
    return (raw / _STEP).to_integral_value(ROUND_DOWN) * _STEP


def _run(
    candles: list[SimCandle],
    signals: tuple[EntrySignal, ...],
    *,
    seed: int = 7,
    costs: CostModel | None = None,
    risk: object | None = None,
    factor: Decimal = Decimal(1),
    marks: tuple[SimCandle, ...] | None = None,
    funding: tuple[FundingEvent, ...] = (),
    cashflows: tuple[Cashflow, ...] = (),
    others: int = 0,
    interval: int = 8 * _HOUR,
    equity: Decimal = _START,
) -> BacktestReport:
    return run_backtest(
        candles,
        signals,
        symbol="BTC/USDT:USDT",
        instrument=instrument(),
        limits=limits() if risk is None else risk,  # type: ignore[arg-type]
        initial_equity=equity,
        maintenance_margin_rate=_MMR,
        seed=seed,
        costs=_costs() if costs is None else costs,
        params=TargetParams(tp_atr_mult=Decimal(2), sl_atr_mult=Decimal(1), horizon_bars=2),
        mark_candles=marks,
        funding=funding,
        cashflows=cashflows,
        other_open_positions=others,
        cost_factor=factor,
        funding_interval_ms=interval,
    )


def test_four_trades_match_the_hand_calculation_and_the_same_seed_repeats() -> None:
    # Лонг: стоп 98, ціль 104, ATR = 2. Комісія 0,1%, slippage 0.
    # Ціль: чистий 4 − 0,1 − 0,104 = 3,796; бюджет стопа 2 + 0,1 + 0,098 = 2,198.
    # Стоп: чистий = −2,198, R = −1. Time-stop шорта на 100: чистий −0,2, бюджет 2,202.
    loss_long = Decimal("2.198")
    win = Decimal("3.796")
    loss_short = Decimal("2.202")
    flat_exit = Decimal("-0.2")

    series = _flat(2604)
    series[1300] = _bar(1300, "100", "105", "99.5", "104")
    series[2601] = _bar(2601, "100", "105", "99.5", "104")
    chained = _run(
        series,
        (
            EntrySignal(entry_index=1300, side=_LONG),
            EntrySignal(entry_index=2601, side=_LONG),
        ),
    )
    qty1 = _qty(_START, loss_long)
    net1 = win * qty1
    qty2 = _qty(_START + net1, loss_long)
    net2 = win * qty2
    assert chained.trades[0].qty == qty1
    assert chained.trades[0].net_usd == net1
    assert chained.trades[0].r_multiple == win / loss_long
    assert chained.trades[0].exit_reason is ExitReason.TAKE_PROFIT
    assert chained.trades[1].qty == qty2
    assert chained.trades[1].net_usd == net2
    assert chained.trades[1].r_multiple == win / loss_long
    assert chained.metrics.ending_equity == _START + net1 + net2
    assert chained.metrics.total_r == (win / loss_long) * 2
    assert chained.metrics.profit_factor is None
    assert chained.metrics.n_trades == 2

    stopped = _flat(1303)
    stopped[1300] = _bar(1300, "100", "101", "97", "99")
    stop_report = _run(stopped, (EntrySignal(entry_index=1300, side=_LONG),))
    assert stop_report.trades[0].qty == qty1
    assert stop_report.trades[0].net_usd == -loss_long * qty1
    assert stop_report.trades[0].r_multiple == Decimal(-1)
    assert stop_report.trades[0].exit_reason is ExitReason.STOP_LOSS

    faded = _run(_flat(1303), (EntrySignal(entry_index=1300, side=_SHORT),))
    short_qty = _qty(_START, loss_short)
    assert faded.trades[0].qty == short_qty
    assert faded.trades[0].net_usd == flat_exit * short_qty
    assert faded.trades[0].r_multiple == flat_exit / loss_short
    assert faded.trades[0].exit_reason is ExitReason.TIME_STOP

    held = _run(_flat(1303), (EntrySignal(entry_index=1300, side=_LONG),))
    assert held.trades[0].net_usd == flat_exit * qty1
    assert held.trades[0].r_multiple == flat_exit / loss_long
    assert held.trades[0].exit_reason is ExitReason.TIME_STOP
    assert held.metrics.hit_rate == Decimal(0)

    again = _run(
        series,
        (
            EntrySignal(entry_index=1300, side=_LONG),
            EntrySignal(entry_index=2601, side=_LONG),
        ),
        seed=7,
    )
    assert again == chained
    other_seed = _run(
        series,
        (
            EntrySignal(entry_index=1300, side=_LONG),
            EntrySignal(entry_index=2601, side=_LONG),
        ),
        seed=99,
    )
    assert other_seed.trades == chained.trades
    assert other_seed.seed == 99


def test_entry_size_ignores_the_rest_of_the_entry_bar_and_bars_after_the_exit() -> None:
    base = _flat(1303)
    base[1300] = _bar(1300, "100", "105", "99.5", "104")
    wider = list(base)
    wider[1300] = _bar(1300, "100", "120", "99.5", "110")
    wider[1302] = _bar(1302, "100", "140", "50", "100")
    signal = (EntrySignal(entry_index=1300, side=_LONG),)
    first = _run(base, signal)
    second = _run(wider, signal)
    assert first.trades[0].qty == second.trades[0].qty
    assert first.trades[0].net_usd == second.trades[0].net_usd
    assert first.trades[0].exit_reason is ExitReason.TAKE_PROFIT


def test_open_position_cooldown_and_daily_loss_block_the_next_entry() -> None:
    # Горизонт 4, щоб перша угода ще була відкрита на наступному барі.
    candles = _flat(FEATURE_LOOKBACK_BARS + 30)
    candles[1300] = _bar(1300, "100", "101", "99.5", "100")
    report = run_backtest(
        candles,
        (
            EntrySignal(entry_index=1300, side=_LONG),
            EntrySignal(entry_index=1301, side=_LONG),
        ),
        symbol="BTC/USDT:USDT",
        instrument=instrument(),
        limits=limits(),
        initial_equity=_START,
        maintenance_margin_rate=_MMR,
        seed=1,
        costs=_costs(),
        params=TargetParams(horizon_bars=4, tp_atr_mult=Decimal(2), sl_atr_mult=Decimal(1)),
        funding=(FundingEvent(time_ms=1304 * _HOUR, rate=Decimal(0), mark_price=Decimal(100)),),
    )
    assert report.metrics.n_trades == 1
    assert report.decisions[1].decision == "SKIP"
    assert report.decisions[1].reason_code == RiskReason.RISK_MAX_POSITIONS.value
    assert report.trades[0].exit_reason is ExitReason.TIME_STOP

    stopped = _flat(FEATURE_LOOKBACK_BARS + 8)
    stopped[1300] = _bar(1300, "100", "101", "97", "99")
    cooling = _run(
        stopped,
        (
            EntrySignal(entry_index=1300, side=_LONG),
            EntrySignal(entry_index=1302, side=_LONG),
            EntrySignal(entry_index=1304, side=_LONG),
        ),
        risk=limits(max_consecutive_losses=1, cooldown_bars=4),
    )
    reasons = [row.reason_code for row in cooling.decisions if row.decision == "SKIP"]
    assert reasons == [RiskReason.RISK_COOLDOWN.value]
    assert cooling.metrics.n_trades == 2
    assert cooling.trades[1].entry_index == 1304

    # Вхід 1324 з горизонтом 2 потребує свічки 1326 (time-stop), тож ряд до 1326 включно.
    day = _flat(FEATURE_LOOKBACK_BARS + 27)
    day[1300] = _bar(1300, "100", "101", "97", "99")
    paused = _run(
        day,
        (
            EntrySignal(entry_index=1300, side=_LONG),
            EntrySignal(entry_index=1302, side=_LONG),
            EntrySignal(entry_index=1324, side=_LONG),
        ),
        risk=limits(daily_loss_limit_pct=Decimal("0.001")),
    )
    skip = [row for row in paused.decisions if row.decision == "SKIP"]
    assert [row.reason_code for row in skip] == [RiskReason.RISK_DAILY_LOSS.value]
    assert paused.metrics.n_trades == 2
    assert paused.trades[1].entry_index == 1324
    assert paused.trades[1].exit_reason is ExitReason.TIME_STOP
    assert paused.metrics.n_incomplete == 0
    assert paused.metrics.halted is False


def test_drawdown_uses_the_new_peak_and_a_wick_and_stays_halted_next_day() -> None:
    series = _flat(FEATURE_LOOKBACK_BARS + 8)
    series[1300] = _bar(1300, "100", "105", "99.5", "104")
    series[1303] = _bar(1303, "100", "101", "97", "99")
    # Виграш піднімає пік. Повний стоп від нового піку — близько 0,5%, ліміт 0,4%.
    # Від старого піку 10 000 капітал лишається вищим: без оновлення піку третього входу не було б.
    report = _run(
        series,
        (
            EntrySignal(entry_index=1300, side=_LONG),
            EntrySignal(entry_index=1303, side=_LONG),
            EntrySignal(entry_index=1305, side=_LONG),
        ),
        risk=limits(max_drawdown_pct=Decimal("0.004")),
    )
    assert report.metrics.n_trades == 2
    assert report.metrics.halted is True
    assert report.metrics.peak_equity > _START
    assert report.decisions[-1].reason_code == RiskReason.RISK_MAX_DRAWDOWN.value

    wick = _flat(FEATURE_LOOKBACK_BARS + 30)
    wick[1300] = _bar(1300, "100", "101", "99.9", "100")
    wick[1301] = _bar(1301, "100", "101", "99", "100")
    halted = run_backtest(
        wick,
        (
            EntrySignal(entry_index=1300, side=_LONG),
            EntrySignal(entry_index=1324, side=_LONG),
        ),
        symbol="BTC/USDT:USDT",
        instrument=instrument(),
        limits=limits(max_drawdown_pct=Decimal("0.001")),
        initial_equity=_START,
        maintenance_margin_rate=_MMR,
        seed=1,
        costs=_costs(),
        params=TargetParams(horizon_bars=4, tp_atr_mult=Decimal(2), sl_atr_mult=Decimal(1)),
        funding=(FundingEvent(time_ms=1304 * _HOUR, rate=Decimal(0), mark_price=Decimal(100)),),
    )
    # Тінь 99 на другій свічці зупиняє рахунок, і Emergency Stop закриває позицію до time-stop.
    assert halted.trades[0].exit_reason is ExitReason.EMERGENCY_STOP
    assert halted.trades[0].exit_index == 1301
    assert halted.metrics.halted is True
    assert halted.decisions[-1].reason_code == RiskReason.RISK_MAX_DRAWDOWN.value
    assert halted.metrics.max_drawdown_pct >= Decimal("0.001")


def test_take_profit_wick_against_the_position_halts_like_a_bar_without_an_exit() -> None:
    """Тінь майже до стопа на свічці TP — просадка, зупинка і Emergency Stop, як на свічці без виходу.

    Порядок тіні й TP у свічці невідомий: песимістично тінь перша, тож позиція закривається market
    за 98,1 до тейку: −1,9 − 0,1 − 0,0981 = −2,0981 на одиницю.
    """
    candles = _flat(FEATURE_LOOKBACK_BARS + 8)
    candles[1300] = _bar(1300, "100", "105", "98.1", "104")
    report = _run(
        candles,
        (
            EntrySignal(entry_index=1300, side=_LONG),
            EntrySignal(entry_index=1305, side=_LONG),
        ),
        risk=limits(max_drawdown_pct=Decimal("0.004")),
    )
    assert report.trades[0].exit_reason is ExitReason.EMERGENCY_STOP
    assert report.trades[0].net_usd == Decimal("-2.0981") * _qty(_START, Decimal("2.198"))
    assert report.metrics.n_trades == 1
    assert report.metrics.halted is True
    assert report.metrics.max_drawdown_pct >= Decimal("0.004")
    assert report.decisions[-1].reason_code == RiskReason.RISK_MAX_DRAWDOWN.value


def test_a_hole_while_the_trade_is_open_is_booked_not_dropped() -> None:
    bars = [bar for bar in _flat(FEATURE_LOOKBACK_BARS + 6) if bar.open_time_ms != 1302 * _HOUR]
    entry = next(index for index, bar in enumerate(bars) if bar.open_time_ms == 1300 * _HOUR)
    report = run_backtest(
        bars,
        (EntrySignal(entry_index=entry, side=_LONG),),
        symbol="BTC/USDT:USDT",
        instrument=instrument(),
        limits=limits(),
        initial_equity=_START,
        maintenance_margin_rate=_MMR,
        seed=1,
        costs=_costs(),
        params=TargetParams(horizon_bars=4, tp_atr_mult=Decimal(2), sl_atr_mult=Decimal(1)),
    )
    assert report.metrics.n_trades == 1
    assert report.metrics.n_incomplete == 1
    trade = report.trades[0]
    assert trade.exit_reason is ExitReason.DATA_GAP
    assert trade.r_multiple == Decimal(-1)
    assert report.metrics.ending_equity == _START + trade.net_usd
    assert report.metrics.ending_equity < _START


def test_unusable_barrier_and_stale_signal_are_counted_and_do_not_open() -> None:
    wide = [_bar(index, "3", "5", "1", "3") for index in range(FEATURE_LOOKBACK_BARS + 3)]
    unusable = _run(wide, (EntrySignal(entry_index=FEATURE_LOOKBACK_BARS, side=_LONG),))
    assert unusable.metrics.n_trades == 0
    assert unusable.metrics.n_unusable == 1
    assert unusable.metrics.ending_equity == _START
    assert unusable.decisions[0].reason_code == "UNUSABLE_EXIT"

    fresh = _flat(FEATURE_LOOKBACK_BARS + 3)
    opened = fresh[FEATURE_LOOKBACK_BARS].open_time_ms
    stale = _run(
        fresh,
        (EntrySignal(entry_index=FEATURE_LOOKBACK_BARS, side=_LONG, computed_at_ms=opened + 5001),),
    )
    assert stale.metrics.n_trades == 0
    assert stale.decisions[0].reason_code == "SIGNAL_STALE"
    assert stale.decisions[0].stage == "SIGNAL"
    on_time = _run(
        fresh,
        (EntrySignal(entry_index=FEATURE_LOOKBACK_BARS, side=_LONG, computed_at_ms=opened + 5000),),
    )
    assert on_time.metrics.n_trades == 1


def test_slippage_above_the_live_cap_cancels_the_entry() -> None:
    report = _run(
        _flat(FEATURE_LOOKBACK_BARS + 3),
        (EntrySignal(entry_index=FEATURE_LOOKBACK_BARS, side=_LONG),),
        costs=_costs(slippage_entry_bps=Decimal(20)),
    )
    assert report.metrics.n_trades == 0
    assert report.decisions[0].reason_code == RiskReason.RISK_MAX_SLIPPAGE.value


def test_doubled_costs_hit_the_execution_not_the_size() -> None:
    # Стрес — виконання гірше за модель: розмір і бюджет стопа з базових витрат, як у live.
    # Стоп: 2 + 0,2 + 0,196 = 2,396 на одиницю проти бюджету 2,198. Ціль: 4 − 0,2 − 0,208 = 3,592.
    budget = Decimal("2.198")
    qty = _qty(_START, budget)
    entry = FEATURE_LOOKBACK_BARS
    signal = (EntrySignal(entry_index=entry, side=_LONG),)

    stopped = _flat(entry + 3)
    stopped[entry] = _bar(entry, "100", "101", "97", "99")
    base = _run(stopped, signal)
    stress = _run(stopped, signal, factor=Decimal(2))
    assert stress.cost_factor == Decimal(2)
    assert base.trades[0].r_multiple == Decimal(-1)
    assert stress.trades[0].qty == base.trades[0].qty == qty
    assert stress.trades[0].loss_per_unit == budget
    assert stress.trades[0].net_usd == Decimal("-2.396") * qty
    assert stress.trades[0].r_multiple == Decimal("-2.396") / budget

    target = _flat(entry + 3)
    target[entry] = _bar(entry, "100", "105", "99.5", "104")
    won = _run(target, signal, factor=Decimal(2))
    assert won.trades[0].exit_reason is ExitReason.TAKE_PROFIT
    assert won.trades[0].qty == qty
    assert won.trades[0].net_usd == Decimal("3.592") * qty
    assert won.trades[0].r_multiple == Decimal("3.592") / budget


def test_stress_keeps_the_entries_that_the_live_slippage_cap_allowed() -> None:
    # Slippage входу 10 bps проходить ліміт 15; подвоєний — 20 bps. Стрес не викидає ці входи.
    candles = _flat(FEATURE_LOOKBACK_BARS + 3)
    signal = (EntrySignal(entry_index=FEATURE_LOOKBACK_BARS, side=_LONG),)
    costs = _costs(slippage_entry_bps=Decimal(10))
    base = _run(candles, signal, costs=costs)
    stress = _run(candles, signal, costs=costs, factor=Decimal(2))
    assert base.metrics.n_trades == stress.metrics.n_trades == 1
    assert [(row.decision, row.reason_code) for row in stress.decisions] == [
        (row.decision, row.reason_code) for row in base.decisions
    ]
    assert stress.trades[0].qty == base.trades[0].qty
    assert stress.trades[0].net_usd < base.trades[0].net_usd


def test_a_signal_without_horizon_candles_is_a_run_error() -> None:
    # Горизонт 2: останній дозволений вхід — за дві свічки до кінця ряду. Решта — помилка прогону,
    # а не DATA_GAP на −1R. Рішення за індексом: ранній TP біля кінця ряду помилку не знімає.
    candles = _flat(FEATURE_LOOKBACK_BARS + 3)
    last_ok = len(candles) - 3
    report = _run(candles, (EntrySignal(entry_index=last_ok, side=_LONG),))
    assert report.trades[0].exit_reason is ExitReason.TIME_STOP
    assert report.trades[0].exit_index == len(candles) - 1
    assert report.metrics.n_incomplete == 0

    early = list(candles)
    early[last_ok + 1] = _bar(last_ok + 1, "100", "105", "99.5", "104")
    with pytest.raises(ValueError, match="horizon_bars"):
        _run(early, (EntrySignal(entry_index=last_ok + 1, side=_LONG),))
    with pytest.raises(ValueError, match="horizon_bars"):
        _run(candles, (EntrySignal(entry_index=len(candles) - 1, side=_SHORT),))


def test_halt_with_an_open_position_closes_it_at_market() -> None:
    # ТЗ §8.5, §10.3: HALTED запускає Emergency Stop — позиція закривається market, а не чекає TP.
    # Тінь 98,05 (стоп 98 не зачеплено) дає просадку ≈ 0,47% ≥ 0,3%. Вихід за цією ціною:
    # −1,95 − 0,1 − 0,09805 = −2,14805 на одиницю.
    entry = FEATURE_LOOKBACK_BARS
    risk = limits(max_drawdown_pct=Decimal("0.003"))
    qty = _qty(_START, Decimal("2.198"))

    wick = _flat(entry + 8)
    wick[entry + 1] = _bar(entry + 1, "100", "100.5", "98.05", "99")
    report = _run(
        wick,
        (EntrySignal(entry_index=entry, side=_LONG), EntrySignal(entry_index=entry + 4, side=_LONG)),
        risk=risk,
    )
    trade = report.trades[0]
    assert trade.exit_reason is ExitReason.EMERGENCY_STOP
    assert trade.exit_index == entry + 1
    assert trade.net_usd == Decimal("-2.14805") * qty
    assert report.metrics.halted is True
    assert [row.reason_code for row in report.decisions if row.decision == "CLOSE"] == ["EMERGENCY_STOP"]
    assert report.decisions[-1].reason_code == RiskReason.RISK_MAX_DRAWDOWN.value

    # Шорт дзеркально: стоп 102, тінь 101,95. −1,95 − 0,1 − 0,10195 = −2,15195.
    up = _flat(entry + 8)
    up[entry + 1] = _bar(entry + 1, "100", "101.95", "99.5", "100.5")
    short = _run(up, (EntrySignal(entry_index=entry, side=_SHORT),), risk=risk)
    assert short.trades[0].exit_reason is ExitReason.EMERGENCY_STOP
    assert short.trades[0].net_usd == Decimal("-2.15195") * _qty(_START, Decimal("2.202"))

    # Просадка вже на open: закриття за open 98,05, а не за нижчою тінню 98,01.
    gap = _flat(entry + 8)
    gap[entry + 1] = _bar(entry + 1, "98.05", "98.5", "98.01", "98.2")
    opened = _run(
        gap,
        (EntrySignal(entry_index=entry, side=_LONG), EntrySignal(entry_index=entry + 1, side=_LONG)),
        risk=risk,
    )
    assert opened.trades[0].exit_reason is ExitReason.EMERGENCY_STOP
    assert opened.trades[0].net_usd == Decimal("-2.14805") * qty
    assert opened.decisions[-1].entry_index == entry + 1
    assert opened.decisions[-1].reason_code == RiskReason.RISK_MAX_DRAWDOWN.value

    # Стоп у тій самій свічці лишається стопом: біржовий SL за рівнем не гірший за market-закриття.
    stop = _flat(entry + 8)
    stop[entry + 1] = _bar(entry + 1, "100", "100.5", "97", "99")
    stopped = _run(stop, (EntrySignal(entry_index=entry, side=_LONG),), risk=risk)
    assert stopped.trades[0].exit_reason is ExitReason.STOP_LOSS
    assert stopped.metrics.halted is True


def test_a_take_profit_gap_is_known_at_the_open() -> None:
    # Свічка відкрилась вище TP 104: біржовий TP виконано на open, тінь після нього позицію не застала.
    # Сигнал цієї свічки входить, як після гепу через SL.
    entry = FEATURE_LOOKBACK_BARS
    candles = _flat(entry + 8)
    candles[entry + 1] = _bar(entry + 1, "104.5", "105", "98.5", "100")
    report = _run(
        candles,
        (EntrySignal(entry_index=entry, side=_LONG), EntrySignal(entry_index=entry + 1, side=_LONG)),
        risk=limits(max_drawdown_pct=Decimal("0.01")),
    )
    assert report.trades[0].exit_reason is ExitReason.TAKE_PROFIT
    assert report.trades[0].net_usd == Decimal("3.796") * _qty(_START, Decimal("2.198"))
    assert report.metrics.halted is False
    assert [row.decision for row in report.decisions if row.entry_index == entry + 1 and row.stage == "RISK"] == [
        "ENTER"
    ]
    assert report.trades[1].entry_index == entry + 1


def test_missing_funding_is_a_cost_and_a_zero_history_that_covers_the_hold_is_not() -> None:
    candles = _flat(FEATURE_LOOKBACK_BARS + 4)
    entry = FEATURE_LOOKBACK_BARS
    opened = candles[entry].open_time_ms
    # Інтервал 1 год: time-stop через 2 год перетинає дві мітки.
    covered = tuple(
        FundingEvent(time_ms=opened + step * _HOUR, rate=Decimal(0), mark_price=Decimal(100)) for step in (1, 2)
    )
    clean = _run(candles, (EntrySignal(entry_index=entry, side=_LONG),), funding=covered, interval=_HOUR)
    assert clean.trades[0].funding_gap is False
    assert clean.trades[0].funding_penalty_usd == 0

    priced = (FundingEvent(time_ms=opened - 8 * _HOUR, rate=Decimal("0.0001"), mark_price=Decimal(100)),)
    gapped = _run(candles, (EntrySignal(entry_index=entry, side=_LONG),), funding=priced, interval=_HOUR)
    assert gapped.trades[0].funding_gap is True
    assert gapped.trades[0].funding_penalty_usd < 0
    assert gapped.metrics.n_funding_gaps == 1
    assert gapped.trades[0].net_usd < clean.trades[0].net_usd
    later = (
        *priced,
        FundingEvent(time_ms=opened + 10 * _HOUR, rate=Decimal("0.01"), mark_price=Decimal(100)),
    )
    extended = _run(candles, (EntrySignal(entry_index=entry, side=_LONG),), funding=later, interval=_HOUR)
    assert extended.trades[0].funding_penalty_usd == gapped.trades[0].funding_penalty_usd

    with pytest.raises(FundingCoverageError):
        _run(candles, (EntrySignal(entry_index=entry, side=_LONG),), interval=_HOUR)
    future_only = (FundingEvent(time_ms=opened + 10 * _HOUR, rate=Decimal("0.01"), mark_price=Decimal(100)),)
    with pytest.raises(FundingCoverageError):
        _run(candles, (EntrySignal(entry_index=entry, side=_LONG),), funding=future_only, interval=_HOUR)


def test_mark_stop_is_used_and_account_positions_block_before_the_bot_opens() -> None:
    candles = _flat(FEATURE_LOOKBACK_BARS + 3)
    entry = FEATURE_LOOKBACK_BARS
    marks = tuple(candles)
    # Mark пробиває стоп 98, last — ні.
    marked = list(marks)
    marked[entry] = _bar(entry, "100", "101", "97", "100")
    report = _run(candles, (EntrySignal(entry_index=entry, side=_LONG),), marks=tuple(marked))
    assert report.trades[0].exit_reason is ExitReason.STOP_LOSS
    assert report.trades[0].stop_price_source is StopPriceSource.MARK

    blocked = _run(
        candles,
        (EntrySignal(entry_index=entry, side=_LONG),),
        others=3,
    )
    assert blocked.metrics.n_trades == 0
    assert blocked.decisions[0].reason_code == RiskReason.RISK_MAX_POSITIONS.value


def test_deposit_scales_the_peak_instead_of_erasing_drawdown() -> None:
    candles = _flat(FEATURE_LOOKBACK_BARS + 2)
    deposit = Decimal(500)
    report = _run(candles, (), cashflows=(Cashflow(time_ms=0, amount=deposit),))
    anchors = adjust_equity_anchors(
        equity_after=_START + deposit,
        cashflow=deposit,
        peak_equity=_START,
        day_start_equity=_START,
    )
    assert report.metrics.ending_equity == _START + deposit
    assert report.metrics.peak_equity == anchors.peak_equity
    assert report.metrics.n_trades == 0
    assert report.metrics.sharpe is None


def test_decision_codes_match_the_decision_log_check() -> None:
    pattern = re.compile(REASON_CODE_RE)
    report = _run(
        _flat(FEATURE_LOOKBACK_BARS + 3),
        (EntrySignal(entry_index=FEATURE_LOOKBACK_BARS, side=_SHORT),),
    )
    assert report.decisions[0].decision == "ENTER"
    assert report.decisions[0].reason_code is None
    close = report.decisions[1]
    assert close.decision == "CLOSE"
    assert close.reason_code is not None
    assert pattern.fullmatch(close.reason_code)
