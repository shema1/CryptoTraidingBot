"""Калібрування і правило очікуваної вигоди (ТЗ §5.5.1, §5А.8, T2.9)."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal, localcontext

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.exchange.types import PositionSide
from app.ml.decision import (
    CONFIDENCE_GRID,
    DEFAULT_MIN_CONFIDENCE,
    DEFAULT_MIN_EDGE_ATR,
    EDGE_GRID,
    ENTER,
    REASON_CONFIDENCE_BELOW_MIN,
    REASON_EDGE_BELOW_MIN,
    REASON_UNUSABLE_EXIT,
    RELIABILITY_BIN_COUNT,
    SKIP,
    THRESHOLD_CRITERION,
    CalibrationError,
    DecisionRule,
    OosObservation,
    ProbabilityCalibrator,
    decide_entry,
    expected_edge_atr,
    expected_value,
    fit_decision,
    judge_entry,
    reliability_diagram,
    select_thresholds,
)
from app.ml.features import HOUR_MS
from app.ml.trade_sim import ExitReason, FundingEvent, SimCandle, TargetParams, simulate_trade
from app.ml.validation import (
    MIN_TRADES_PER_FOLD,
    Fold,
    ResearchSplit,
    SplitLeakError,
    SplitPurpose,
    TradeCount,
    build_split,
    is_test_row,
    rows_for,
)
from app.ml.validation import (
    SampleRow as ValidationRow,
)
from app.risk.costs import CostModel

BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
HOUR = HOUR_MS
_LONG = PositionSide.LONG
_SHORT = PositionSide.SHORT


def _count(n: int = MIN_TRADES_PER_FOLD) -> TradeCount:
    def count(_start: int, _end: int) -> int:
        return n

    return count


def _split() -> ResearchSplit:
    return build_split(
        data_from_ms=0,
        data_to_ms=500 * HOUR,
        horizon_bars=4,
        count_trades=_count(),
        min_train_ms=48 * HOUR,
        test_segment_ms=24 * HOUR,
        test_segment_extended_ms=48 * HOUR,
    )


def _hours(fold: Fold) -> list[int]:
    return list(range(fold.test_start_ms // HOUR, fold.test_end_ms // HOUR))


def _obs(
    fold_index: int,
    hour: int,
    *,
    label: int | None,
    probability: str,
    symbol: str = BTC,
    side: PositionSide = _LONG,
    entry: str = "100",
    atr: str = "10",
) -> OosObservation:
    return OosObservation(
        symbol=symbol,
        time_ms=hour * HOUR,
        index=hour,
        fold_index=fold_index,
        side=side,
        raw_probability=Decimal(probability),
        true_label=label,
        entry=Decimal(entry),
        atr=Decimal(atr),
    )


def _separable(split: ResearchSplit, n_folds: int = 2) -> list[OosObservation]:
    rows: list[OosObservation] = []
    for fold in split.folds[:n_folds]:
        hours = _hours(fold)
        half = len(hours) // 2
        for hour in hours[:half]:
            rows.append(_obs(fold.index, hour, label=0, probability="0.1"))
        for hour in hours[half:]:
            rows.append(_obs(fold.index, hour, label=1, probability="0.9"))
    return rows


def _candle(index: int, open_: str, high: str, low: str, close: str) -> SimCandle:
    return SimCandle(
        open_time_ms=index * HOUR,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
    )


def test_defaults_match_the_spec_and_sit_on_the_grid() -> None:
    assert Decimal("0.10") == DEFAULT_MIN_EDGE_ATR
    assert Decimal("0.40") == DEFAULT_MIN_CONFIDENCE
    assert DEFAULT_MIN_EDGE_ATR in EDGE_GRID
    assert DEFAULT_MIN_CONFIDENCE in CONFIDENCE_GRID
    assert tuple(sorted(EDGE_GRID)) == EDGE_GRID
    assert tuple(sorted(CONFIDENCE_GRID)) == CONFIDENCE_GRID
    assert DecisionRule().metadata() == {"min_edge_atr": "0.10", "min_confidence": "0.40"}
    assert THRESHOLD_CRITERION == "median_fold_edge_atr"


def test_expected_value_at_the_boundaries() -> None:
    assert expected_value(probability=Decimal(0), tp_net=Decimal("3"), sl_net=Decimal("2")) == Decimal("-2")
    assert expected_value(probability=Decimal(1), tp_net=Decimal("3"), sl_net=Decimal("2")) == Decimal("3")
    # p = SL / (TP + SL) = 2/5: виграш і програш у середньому гасяться.
    assert expected_value(probability=Decimal(2) / Decimal(5), tp_net=Decimal("3"), sl_net=Decimal("2")) == Decimal(0)
    assert expected_value(probability=Decimal(1), tp_net=Decimal("-1"), sl_net=Decimal("2")) == Decimal("-1")
    assert expected_value(probability=Decimal("0.5"), tp_net=Decimal("2"), sl_net=Decimal("1")) == Decimal("0.5")


def test_expected_value_rejects_a_bad_probability_or_stop() -> None:
    with pytest.raises(ValueError, match="ймовірність"):
        expected_value(probability=Decimal("1.1"), tp_net=Decimal("1"), sl_net=Decimal("1"))
    with pytest.raises(ValueError, match="ймовірність"):
        expected_value(probability=Decimal("-0.01"), tp_net=Decimal("1"), sl_net=Decimal("1"))
    with pytest.raises(ValueError, match="sl_net"):
        expected_value(probability=Decimal("0.5"), tp_net=Decimal("1"), sl_net=Decimal(0))


@given(
    probability=st.decimals(min_value=0, max_value=1, allow_nan=False, allow_infinity=False, places=4),
    tp_net=st.decimals(min_value="-5", max_value="5", allow_nan=False, allow_infinity=False, places=4),
    sl_net=st.decimals(min_value="0.0001", max_value="5", allow_nan=False, allow_infinity=False, places=4),
)
@settings(max_examples=60, deadline=None)
def test_expected_value_is_the_mixture_of_target_and_stop(
    probability: Decimal,
    tp_net: Decimal,
    sl_net: Decimal,
) -> None:
    assert expected_value(probability=probability, tp_net=tp_net, sl_net=sl_net) == (
        probability * tp_net - (Decimal(1) - probability) * sl_net
    )


def test_edge_uses_cost_model_for_both_sides_and_ignores_the_horizon() -> None:
    entry = Decimal("100")
    atr = Decimal("1")
    base = TargetParams()
    cheap = CostModel()
    costly = CostModel(slippage_entry_atr=Decimal("0.25"), slippage_tp_atr=Decimal("0.25"))
    wider_stop = CostModel(slippage_sl_atr=Decimal("0.5"))
    long_edge = expected_edge_atr(probability=Decimal(1), side=_LONG, entry=entry, atr=atr, costs=cheap, params=base)
    short_edge = expected_edge_atr(probability=Decimal(1), side=_SHORT, entry=entry, atr=atr, costs=cheap, params=base)
    assert long_edge > 0
    assert short_edge > 0
    assert (
        expected_edge_atr(probability=Decimal(1), side=_LONG, entry=entry, atr=atr, costs=costly, params=base)
        < long_edge
    )
    assert expected_edge_atr(
        probability=Decimal(0), side=_LONG, entry=entry, atr=atr, costs=wider_stop, params=base
    ) < expected_edge_atr(probability=Decimal(0), side=_LONG, entry=entry, atr=atr, costs=cheap, params=base)
    assert expected_edge_atr(
        probability=Decimal("0.5"),
        side=_LONG,
        entry=entry,
        atr=atr,
        costs=cheap,
        params=TargetParams(horizon_bars=3),
    ) == expected_edge_atr(
        probability=Decimal("0.5"),
        side=_LONG,
        entry=entry,
        atr=atr,
        costs=cheap,
        params=TargetParams(horizon_bars=48),
    )


def test_time_stop_and_funding_do_not_replace_the_stop_budget() -> None:
    costs = CostModel()
    params = TargetParams(tp_atr_mult=Decimal("2"), sl_atr_mult=Decimal("1"), horizon_bars=2)
    trade = simulate_trade(
        (
            _candle(0, "100", "101", "99", "100"),
            _candle(1, "100", "101", "99", "100"),
            _candle(2, "101", "120", "100", "110"),
        ),
        entry_index=0,
        side=_LONG,
        atr=Decimal("2"),
        params=params,
        costs=costs,
        funding=(FundingEvent(time_ms=HOUR, rate=Decimal("0.01"), mark_price=Decimal("100")),),
    )
    assert trade.exit_reason is ExitReason.TIME_STOP
    assert trade.meta_label == 0
    assert trade.funding != 0
    edge = expected_edge_atr(
        probability=Decimal(0),
        side=_LONG,
        entry=trade.reference_entry,
        atr=trade.atr,
        costs=costs,
        params=params,
    )
    assert edge == -trade.loss_per_unit / trade.atr
    assert edge != trade.net_per_unit / trade.atr


def test_judge_includes_both_bounds_and_names_the_first_failure() -> None:
    rule = DecisionRule()
    edge = Decimal("0.10")
    probability = Decimal("0.40")
    entered = judge_entry(probability=probability, expected_edge_atr=edge, rule=rule)
    assert entered.decision == ENTER
    assert entered.reason is None
    assert entered.expected_edge_atr == edge
    below_confidence = judge_entry(probability=probability - Decimal("0.0000000001"), expected_edge_atr=edge, rule=rule)
    assert below_confidence.decision == SKIP
    assert below_confidence.reason == REASON_CONFIDENCE_BELOW_MIN
    assert below_confidence.expected_edge_atr == edge
    below_edge = judge_entry(probability=probability, expected_edge_atr=edge - Decimal("0.0000000001"), rule=rule)
    assert below_edge.decision == SKIP
    assert below_edge.reason == REASON_EDGE_BELOW_MIN
    both = judge_entry(probability=Decimal("0.1"), expected_edge_atr=Decimal("-1"), rule=rule)
    assert both.reason == REASON_CONFIDENCE_BELOW_MIN
    unusable = judge_entry(probability=probability, expected_edge_atr=None, rule=rule)
    assert unusable.decision == SKIP
    assert unusable.reason == REASON_UNUSABLE_EXIT
    assert unusable.expected_edge_atr is None


@given(
    probability=st.decimals(min_value=0, max_value=1, allow_nan=False, allow_infinity=False, places=3),
    edge=st.decimals(min_value="-2", max_value="2", allow_nan=False, allow_infinity=False, places=3),
    min_confidence=st.decimals(min_value=0, max_value=1, allow_nan=False, allow_infinity=False, places=3),
    min_edge=st.decimals(min_value=0, max_value="1", allow_nan=False, allow_infinity=False, places=3),
)
@settings(max_examples=50, deadline=None)
def test_entry_requires_both_thresholds(
    probability: Decimal,
    edge: Decimal,
    min_confidence: Decimal,
    min_edge: Decimal,
) -> None:
    rule = DecisionRule(min_edge_atr=min_edge, min_confidence=min_confidence)
    decision = judge_entry(probability=probability, expected_edge_atr=edge, rule=rule)
    if probability >= min_confidence and edge >= min_edge:
        assert decision.decision == ENTER
        assert decision.reason is None
    else:
        assert decision.decision == SKIP
        assert decision.reason is not None


def test_reliability_bins_are_equal_and_the_last_one_includes_one() -> None:
    diagram = reliability_diagram(
        [Decimal("0"), Decimal("0.1"), Decimal("1")],
        [0, 1, 1],
    )
    assert len(diagram.bins) == RELIABILITY_BIN_COUNT
    assert diagram.bins[0].lower == Decimal(0)
    assert diagram.bins[0].n == 1
    assert diagram.bins[0].mean_predicted == Decimal(0)
    assert diagram.bins[0].fraction_positive == Decimal(0)
    assert diagram.bins[1].lower == Decimal("0.1")
    assert diagram.bins[1].n == 1
    assert diagram.bins[1].fraction_positive == Decimal(1)
    assert diagram.bins[-1].upper == Decimal(1)
    assert diagram.bins[-1].n == 1
    assert diagram.bins[-1].mean_predicted == Decimal(1)
    assert diagram.bins[-1].fraction_positive == Decimal(1)
    assert diagram.bins[2].n == 0
    assert diagram.bins[2].mean_predicted is None
    assert diagram.bins[2].fraction_positive is None


def test_thresholds_take_only_winners_and_ties_break_toward_the_stricter_rule() -> None:
    split = _split()
    rows: list[OosObservation] = []
    calibrated: dict[tuple[str, int], Decimal] = {}
    for fold in split.folds[:2]:
        hours = _hours(fold)
        winner = _obs(fold.index, hours[0], label=1, probability="1")
        loser = _obs(fold.index, hours[1], label=0, probability="0")
        rows.extend((winner, loser))
        calibrated[(winner.symbol, winner.time_ms)] = Decimal(1)
        calibrated[(loser.symbol, loser.time_ms)] = Decimal(0)
    selection = select_thresholds(rows, split, calibrated, costs=CostModel(), params=TargetParams())
    assert selection.rule == DecisionRule(min_edge_atr=EDGE_GRID[-1], min_confidence=CONFIDENCE_GRID[-1])
    assert selection.n_taken == 2
    assert selection.n_folds == 2
    assert selection.beats_abstain is True
    assert selection.criterion == THRESHOLD_CRITERION
    assert selection.median_fold_edge_atr > 0
    taken = 0
    for row in rows:
        probability = calibrated[(row.symbol, row.time_ms)]
        edge = expected_edge_atr(
            probability=probability,
            side=row.side,
            entry=row.entry,
            atr=row.atr,
            costs=CostModel(),
            params=TargetParams(),
        )
        if judge_entry(probability=probability, expected_edge_atr=edge, rule=selection.rule).decision == ENTER:
            taken += 1
    assert taken == selection.n_taken


def test_thresholds_use_the_calibrated_probability_not_the_raw_score() -> None:
    split = _split()
    hour = _hours(split.folds[0])[0]
    # Сира 0.1 виглядає як відмова, сира 0.9 — як вхід. Калібрування каже навпаки.
    taken = _obs(0, hour, label=1, probability="0.1")
    skipped = _obs(0, _hours(split.folds[0])[1], label=0, probability="0.9")
    calibrated = {
        (taken.symbol, taken.time_ms): Decimal(1),
        (skipped.symbol, skipped.time_ms): Decimal("0.1"),
    }
    selection = select_thresholds((taken, skipped), split, calibrated, costs=CostModel(), params=TargetParams())
    assert selection.n_taken == 1
    assert selection.median_fold_edge_atr > 0


def test_a_label_of_zero_costs_the_full_stop_in_the_threshold_sum() -> None:
    split = _split()
    costs = CostModel()
    params = TargetParams(tp_atr_mult=Decimal("2"), sl_atr_mult=Decimal("1"), horizon_bars=2)
    trade = simulate_trade(
        (
            _candle(0, "100", "101", "99", "100"),
            _candle(1, "100", "101", "99", "100"),
            _candle(2, "101", "120", "100", "110"),
        ),
        entry_index=0,
        side=_LONG,
        atr=Decimal("2"),
        params=params,
        costs=costs,
        funding=(FundingEvent(time_ms=HOUR, rate=Decimal("0.01"), mark_price=Decimal("100")),),
    )
    hour = _hours(split.folds[0])[0]
    row = OosObservation(
        symbol=BTC,
        time_ms=hour * HOUR,
        index=hour,
        fold_index=0,
        side=_LONG,
        raw_probability=Decimal("0.2"),
        true_label=0,
        entry=trade.reference_entry,
        atr=trade.atr,
    )
    selection = select_thresholds(
        (row,),
        split,
        {(row.symbol, row.time_ms): Decimal(1)},
        costs=costs,
        params=params,
    )
    assert trade.meta_label == 0
    assert selection.n_taken == 1
    assert selection.median_fold_edge_atr == -trade.loss_per_unit / trade.atr
    assert selection.median_fold_edge_atr != trade.net_per_unit / trade.atr
    assert selection.beats_abstain is False


def test_low_probability_abstains_and_an_unusable_barrier_is_not_a_trade() -> None:
    split = _split()
    hours = _hours(split.folds[0])
    quiet = _obs(0, hours[0], label=1, probability="0.2")
    broken = _obs(0, hours[1], label=1, probability="1", entry="1", atr="10")
    selection = select_thresholds(
        (quiet, broken),
        split,
        {
            (quiet.symbol, quiet.time_ms): Decimal("0.2"),
            (broken.symbol, broken.time_ms): Decimal(1),
        },
        costs=CostModel(),
        params=TargetParams(),
    )
    assert selection.n_taken == 0
    assert selection.median_fold_edge_atr == Decimal(0)
    assert selection.beats_abstain is False


def test_threshold_selection_rejects_holdout_and_a_missing_label() -> None:
    split = _split()
    holdout_hour = split.holdout_from_ms // HOUR
    holdout = _obs(0, holdout_hour, label=1, probability="0.9")
    with pytest.raises(SplitLeakError, match="hold-out"):
        select_thresholds(
            (holdout,),
            split,
            {(holdout.symbol, holdout.time_ms): Decimal("0.9")},
            costs=CostModel(),
            params=TargetParams(),
        )
    hour = _hours(split.folds[0])[0]
    unlabeled = _obs(0, hour, label=None, probability="0.5")
    with pytest.raises(ValueError, match="мітки"):
        select_thresholds(
            (unlabeled,),
            split,
            {(unlabeled.symbol, unlabeled.time_ms): Decimal("0.5")},
            costs=CostModel(),
            params=TargetParams(),
        )


def test_isotonic_out_of_fold_probability_is_not_fit_on_the_same_fold() -> None:
    split = _split()
    rows: list[OosObservation] = []
    for fold in split.folds[:2]:
        hours = _hours(fold)
        half = len(hours) // 2
        for hour in hours[:half]:
            rows.append(_obs(fold.index, hour, label=0, probability="0.2"))
        for hour in hours[half:]:
            rows.append(_obs(fold.index, hour, label=1, probability="0.8"))
    # Другий символ у фолді 1 робить частку міток на p=0.2 рівно 1/2. Фолд 0 цього не бачить.
    for hour in _hours(split.folds[1])[: len(_hours(split.folds[1])) // 2]:
        rows.append(_obs(1, hour, label=1, probability="0.2", symbol=ETH))
    fit = fit_decision(rows, split, costs=CostModel(), params=TargetParams(), method="isotonic")
    probe = next(
        row for row in rows if row.fold_index == 0 and row.symbol == BTC and row.raw_probability == Decimal("0.2")
    )
    assert fit.oof_probability[(probe.symbol, probe.time_ms)] == Decimal("0.5")
    assert fit.method == "isotonic"
    assert fit.calibrator.estimator.increasing is True
    assert fit.calibrator.estimator.out_of_bounds == "clip"


def test_separable_walk_forward_calibrates_and_decides_with_the_same_function() -> None:
    split = _split()
    rows = _separable(split)
    fit = fit_decision(rows, split, costs=CostModel(), params=TargetParams(), method="isotonic")
    assert fit.oof_brier == Decimal(0)
    assert fit.n_labeled == len(rows)
    assert fit.n_unlabeled == 0
    assert fit.calibrator.calibrate(Decimal("0.9")) == Decimal(1)
    assert fit.calibrator.calibrate(Decimal("0.1")) == Decimal(0)
    assert sum(bucket.n for bucket in fit.diagram.bins) == fit.n_labeled
    assert fit.diagram.bins[0].fraction_positive == Decimal(0)
    assert fit.diagram.bins[-1].fraction_positive == Decimal(1)
    entered = decide_entry(
        raw_probability=Decimal("0.9"),
        side=_LONG,
        entry=Decimal("100"),
        atr=Decimal("10"),
        costs=CostModel(),
        params=TargetParams(),
        calibrator=fit.calibrator,
        rule=fit.rule,
    )
    skipped = decide_entry(
        raw_probability=Decimal("0.1"),
        side=_LONG,
        entry=Decimal("100"),
        atr=Decimal("10"),
        costs=CostModel(),
        params=TargetParams(),
        calibrator=fit.calibrator,
        rule=fit.rule,
    )
    calibrated = fit.calibrator.calibrate(Decimal("0.9"))
    assert entered.decision == ENTER
    assert entered.probability == calibrated
    assert entered.expected_edge_atr == expected_edge_atr(
        probability=calibrated,
        side=_LONG,
        entry=Decimal("100"),
        atr=Decimal("10"),
        costs=CostModel(),
        params=TargetParams(),
    )
    assert skipped.decision == SKIP
    assert skipped.reason == REASON_CONFIDENCE_BELOW_MIN
    broken = decide_entry(
        raw_probability=Decimal("0.9"),
        side=_LONG,
        entry=Decimal("1"),
        atr=Decimal("10"),
        costs=CostModel(),
        params=TargetParams(),
        calibrator=fit.calibrator,
        rule=fit.rule,
    )
    assert broken.reason == REASON_UNUSABLE_EXIT
    assert broken.expected_edge_atr is None
    again = fit_decision(list(reversed(rows)), split, costs=CostModel(), params=TargetParams(), method="isotonic")
    assert again.calibrator.calibrate(Decimal("0.8")) == fit.calibrator.calibrate(Decimal("0.8"))


def test_platt_does_not_rebalance_classes_and_the_method_can_be_chosen() -> None:
    split = _split()
    rows = _separable(split)
    platt = fit_decision(rows, split, costs=CostModel(), params=TargetParams(), method="platt")
    assert platt.method == "platt"
    assert platt.calibrator.estimator.class_weight is None
    assert platt.calibrator.calibrate(Decimal("0.9")) >= Decimal("0.7")
    assert platt.calibrator.calibrate(Decimal("0.1")) <= Decimal("0.3")
    chosen = fit_decision(rows, split, costs=CostModel(), params=TargetParams())
    assert chosen.method in {"isotonic", "platt"}


class _FixedScore:
    """Оцінювач з однією відповіддю: на 11–12 знаках рівно половина кроку округлення."""

    def predict(self, raw: np.ndarray) -> np.ndarray:
        return np.full(len(raw), 0.12345678905)


def test_calibrated_probability_does_not_depend_on_the_thread_decimal_context() -> None:
    # CCXT лишає потоку HALF_UP. OOF-імовірності, Brier і поріг мають округлюватись так само, як live.
    calibrator = ProbabilityCalibrator(method="isotonic", estimator=_FixedScore())
    expected = calibrator.calibrate(Decimal("0.5"))
    assert expected == Decimal("0.1234567890")
    with localcontext() as ctx:
        ctx.rounding = ROUND_HALF_UP
        assert Decimal("0.123456789050").quantize(Decimal("0.0000000001")) != expected
        assert calibrator.calibrate(Decimal("0.5")) == expected


def test_calibration_and_thresholds_do_not_see_holdout_train_or_the_wrong_fold() -> None:
    split = _split()
    rows = _separable(split)
    fit = fit_decision(rows, split, costs=CostModel(), params=TargetParams(), method="isotonic")
    # Train з перевернутою міткою зіпсував би isotonic, якби потрапив у навчання.
    train = _obs(0, 10, label=0, probability="0.9")
    train_row = ValidationRow(symbol=BTC, time_ms=train.time_ms, index=train.index)
    assert train_row not in rows_for([train_row], split, SplitPurpose.CALIBRATION)
    assert is_test_row(train_row, split, 0) is False
    with_train = fit_decision([*rows, train], split, costs=CostModel(), params=TargetParams(), method="isotonic")
    assert with_train.n_labeled == fit.n_labeled
    assert with_train.calibrator.calibrate(Decimal("0.9")) == fit.calibrator.calibrate(Decimal("0.9"))
    unlabeled = _obs(0, _hours(split.folds[0])[0], label=None, probability="0.1", symbol=ETH)
    with_gap = fit_decision([*rows, unlabeled], split, costs=CostModel(), params=TargetParams(), method="isotonic")
    assert with_gap.n_unlabeled == 1
    assert with_gap.n_labeled == fit.n_labeled
    assert with_gap.calibrator.calibrate(Decimal("0.9")) == fit.calibrator.calibrate(Decimal("0.9"))
    assert sum(bucket.n for bucket in with_gap.diagram.bins) == with_gap.n_labeled
    holdout = _obs(0, split.holdout_from_ms // HOUR, label=1, probability="0.9")
    reserve = _obs(0, split.decision_end_ms // HOUR, label=1, probability="0.9", symbol=ETH)
    for poisoned in (holdout, reserve):
        with pytest.raises(SplitLeakError, match="hold-out"):
            fit_decision([*rows, poisoned], split, costs=CostModel(), params=TargetParams(), method="isotonic")
    wrong_fold = _obs(1, _hours(split.folds[0])[0], label=1, probability="0.9", symbol=ETH)
    with pytest.raises(SplitLeakError, match="фолду"):
        fit_decision([*rows, wrong_fold], split, costs=CostModel(), params=TargetParams(), method="isotonic")


def test_one_fold_or_a_single_class_in_the_complement_is_rejected() -> None:
    split = _split()
    with pytest.raises(CalibrationError, match="два фолди"):
        fit_decision(_separable(split, n_folds=1), split, costs=CostModel(), params=TargetParams())
    rows = _separable(split, n_folds=1)
    for hour in _hours(split.folds[1]):
        rows.append(_obs(1, hour, label=1, probability="0.9"))
    with pytest.raises(CalibrationError, match="обидва класи"):
        fit_decision(rows, split, costs=CostModel(), params=TargetParams())
    with pytest.raises(CalibrationError):
        fit_decision((), split, costs=CostModel(), params=TargetParams())


def test_duplicate_rows_are_rejected() -> None:
    split = _split()
    hour = _hours(split.folds[0])[0]
    row = _obs(0, hour, label=1, probability="0.9")
    with pytest.raises(ValueError, match="повтор"):
        fit_decision((row, row), split, costs=CostModel(), params=TargetParams())
