"""Блочний bootstrap, DSR, PBO, коридор і умови Gate G1 (ТЗ §5.2.5, §5.7, §5А.10)."""

from __future__ import annotations

import tracemalloc
from decimal import Decimal
from itertools import combinations

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import app.ml.statistics as ml_statistics
import app.ml.validation as ml_validation
from app.exchange.types import PositionSide
from app.ml.backtest import BacktestTrade
from app.ml.features import HOUR_MS
from app.ml.statistics import (
    WEEK_MS,
    BlockBootstrapPf,
    FoldOutcome,
    GateDecision,
    GateThresholds,
    _norm_cdf,
    _norm_ppf,
    block_bootstrap_pf,
    corridor_payload,
    deflated_sharpe_ratio,
    evaluate_gate,
    fold_outcomes_from_metrics,
    live_corridor,
    loosened_thresholds,
    loss_streak,
    parse_oos_results,
    probability_of_backtest_overfitting,
    r_profit_factor,
    return_moments,
)
from app.ml.trade_sim import ExitReason, StopPriceSource

_REPLICATES = 20


def _trade(
    entry_ms: int,
    r_multiple: Decimal,
    *,
    reason: ExitReason = ExitReason.TAKE_PROFIT,
    funding_gap: bool = False,
    entry_index: int = 0,
) -> BacktestTrade:
    return BacktestTrade(
        side=PositionSide.LONG,
        entry_index=entry_index,
        exit_index=entry_index + 1,
        entry_open_time_ms=entry_ms,
        exit_open_time_ms=entry_ms + HOUR_MS,
        exit_reason=reason,
        stop_price_source=StopPriceSource.MARK,
        qty=Decimal("1"),
        net_usd=r_multiple,
        loss_per_unit=Decimal("1"),
        r_multiple=r_multiple,
        risk_usd=Decimal("1"),
        funding_penalty_usd=Decimal("-0.1") if funding_gap else Decimal("0"),
        funding_gap=funding_gap,
    )


def _decimals(values: list[float], places: int = 6) -> tuple[Decimal, ...]:
    return tuple(Decimal(f"{value:.{places}f}") for value in values)


def _fold(
    net: str,
    pf: str | None,
    *,
    n_trades: int = 20,
    below: bool = False,
    cost: str = "1",
) -> FoldOutcome:
    return FoldOutcome(
        n_trades=n_trades,
        total_net_usd=Decimal(net),
        profit_factor=None if pf is None else Decimal(pf),
        below_min_trades=below,
        cost_factor=Decimal(cost),
    )


def _passing_folds(count: int = 5) -> tuple[FoldOutcome, ...]:
    return tuple(_fold("10", "1.20") for _ in range(count))


def _passing_stress(count: int = 5) -> tuple[FoldOutcome, ...]:
    return tuple(_fold("4", "1.05", cost="2") for _ in range(count))


def _gate(
    *,
    label_type: str = "meta",
    folds: tuple[FoldOutcome, ...] | None = None,
    stress: tuple[FoldOutcome, ...] | None = None,
    dsr: Decimal | None = Decimal("0.95"),
    pbo: Decimal | None = Decimal("0.49"),
    n_trials: int = 4,
    unfiltered_total_net: Decimal | None = Decimal("0"),
    thresholds: GateThresholds | None = None,
) -> GateDecision:
    return evaluate_gate(
        label_type=label_type,
        folds=_passing_folds() if folds is None else folds,
        stress=_passing_stress() if stress is None else stress,
        dsr=dsr,
        pbo=pbo,
        n_trials=n_trials,
        unfiltered_total_net=unfiltered_total_net,
        thresholds=thresholds,
    )


def test_profit_factor_uses_r_and_is_undefined_without_a_loss() -> None:
    assert r_profit_factor([Decimal("1"), Decimal("-0.5"), Decimal("0")]) == Decimal("2")
    assert r_profit_factor([Decimal("1"), Decimal("2")]) is None
    assert r_profit_factor([Decimal("0"), Decimal("0")]) is None


def test_zero_r_does_not_extend_or_reset_the_loss_streak() -> None:
    assert loss_streak([Decimal("-1"), Decimal("0"), Decimal("-1")]) == 2
    assert loss_streak([Decimal("-1"), Decimal("1"), Decimal("-1")]) == 1
    assert loss_streak([Decimal("1"), Decimal("-1"), Decimal("-1"), Decimal("-1")]) == 3


def test_block_bootstrap_keeps_gap_stop_and_funding_trades() -> None:
    trades = (
        _trade(0, Decimal("1")),
        _trade(HOUR_MS, Decimal("-1"), reason=ExitReason.DATA_GAP),
        _trade(2 * HOUR_MS, Decimal("-1"), reason=ExitReason.EMERGENCY_STOP),
        _trade(3 * HOUR_MS, Decimal("1"), funding_gap=True),
    )
    result = block_bootstrap_pf(trades, n_replicates=_REPLICATES, seed=1)
    # One week: every resample is the same path. Dropping the forced exits would leave PF undefined.
    assert result.n_blocks == 1
    assert result.observed == Decimal("1")
    assert result.lower == result.observed
    assert result.upper == result.observed
    assert result.lower_above(Decimal("1")) is False


def test_week_bucket_is_seven_days_and_the_seed_does_not_invent_returns() -> None:
    same_week = (
        _trade(0, Decimal("3")),
        _trade(WEEK_MS - 1, Decimal("-1")),
    )
    collapsed = block_bootstrap_pf(same_week, n_replicates=_REPLICATES, seed=4)
    assert collapsed.n_blocks == 1
    assert collapsed.lower == collapsed.upper == Decimal("3")

    two_weeks = (same_week[0], _trade(WEEK_MS, Decimal("-1")))
    spread = block_bootstrap_pf(two_weeks, n_replicates=4000, seed=4)
    assert spread.n_blocks == 2
    assert spread.observed == Decimal("3")
    assert spread.lower == Decimal("0")
    assert spread.upper_no_losses is True
    assert spread.lower_above(Decimal("1")) is False

    np.random.seed(12345)
    again = block_bootstrap_pf(two_weeks, n_replicates=4000, seed=4)
    assert again == spread


def test_corridor_follows_entry_order_and_the_streak_is_not_resampled() -> None:
    trades = (
        _trade(10, Decimal("5"), entry_index=2),
        _trade(10, Decimal("-2"), entry_index=0),
        _trade(10, Decimal("-1"), entry_index=1),
        _trade(WEEK_MS, Decimal("1")),
    )
    first = live_corridor(trades, n_replicates=_REPLICATES, seed=1)
    second = live_corridor(trades, n_replicates=_REPLICATES, seed=2)
    assert first.n_trades == (1, 2, 3, 4)
    assert first.p5_cum_r[0] == Decimal("-2")
    assert first.max_loss_streak == second.max_loss_streak == 2
    assert first.p5_cum_r != second.p5_cum_r
    payload = corridor_payload(first)
    assert payload["n_trades"] == [1, 2, 3, 4]
    assert payload["max_loss_streak"] == 2
    assert all(type(item) is str for item in payload["p5_cum_r"])


@given(
    st.lists(
        st.decimals(
            min_value=Decimal("-3"),
            max_value=Decimal("3"),
            places=2,
            allow_nan=False,
            allow_infinity=False,
        ),
        min_size=1,
        max_size=6,
    )
)
@settings(max_examples=40, deadline=None)
def test_one_week_is_copied_not_redrawn(values: list[Decimal]) -> None:
    trades = tuple(_trade(index, value, entry_index=index) for index, value in enumerate(values))
    corridor = live_corridor(trades, n_replicates=_REPLICATES, seed=1)
    total = Decimal(0)
    for index, value in enumerate(values):
        total += value
        assert corridor.p5_cum_r[index] == total
    assert corridor.max_loss_streak == loss_streak(values)
    if any(value < 0 for value in values):
        interval = block_bootstrap_pf(trades, n_replicates=_REPLICATES, seed=1)
        assert interval.lower == interval.upper == r_profit_factor(values)


def test_symmetric_trades_have_deflated_sharpe_one_half() -> None:
    # Per-trade Sharpe is 0, so DSR is Φ(0). The annualized bar Sharpe of the backtest is not an input.
    series = (Decimal("1"), Decimal("-1"), Decimal("1"), Decimal("-1"))
    assert deflated_sharpe_ratio(series, trial_returns=(series,), n_trials=1) == Decimal("0.5")


def test_more_attempts_lower_the_deflated_sharpe_and_a_constant_series_is_undefined() -> None:
    positive = (Decimal("1"), Decimal("-0.2")) * 20
    alone = deflated_sharpe_ratio(positive, trial_returns=(positive,), n_trials=1)
    noise = tuple(_decimals(list(np.random.default_rng(2).normal(0, 1, size=40))) for _ in range(20))
    crowded = deflated_sharpe_ratio(positive, trial_returns=(positive, *noise), n_trials=21)
    assert alone is not None
    assert crowded is not None
    assert alone > crowded
    empty = (positive, ())
    with_gap = deflated_sharpe_ratio(positive, trial_returns=empty, n_trials=2)
    assert with_gap is not None
    assert with_gap < alone
    assert deflated_sharpe_ratio((Decimal("1"),) * 8, trial_returns=((Decimal("1"),) * 8,), n_trials=1) is None
    # Дві однакові оцінки Sharpe дають нульову дисперсію між спробами: порожня невдача DSR не знижує.
    copied = deflated_sharpe_ratio(positive, trial_returns=(positive, positive, ()), n_trials=3)
    assert copied == alone
    with pytest.raises(ValueError, match="кількості рядів"):
        deflated_sharpe_ratio(positive, trial_returns=(positive,), n_trials=3)


def test_random_trials_deflate_and_a_real_edge_does_not() -> None:
    rng = np.random.default_rng(7)
    random_trials = tuple(_decimals(list(rng.normal(0, 1, size=80))) for _ in range(30))
    best = max(random_trials, key=lambda series: _plain_sharpe(series))
    random_dsr = deflated_sharpe_ratio(best, trial_returns=random_trials, n_trials=len(random_trials))
    assert random_dsr is not None
    assert random_dsr < Decimal("0.95")

    edge = _decimals(list(rng.normal(0.35, 1, size=400)))
    others = tuple(_decimals(list(rng.normal(0, 1, size=400))) for _ in range(5))
    edge_dsr = deflated_sharpe_ratio(edge, trial_returns=(edge, *others), n_trials=6)
    assert edge_dsr is not None
    assert edge_dsr >= Decimal("0.95")


def test_normal_moments_use_raw_kurtosis() -> None:
    sample = _decimals(list(np.random.default_rng(3).normal(0, 1, size=8000)), places=4)
    moments = return_moments(sample)
    assert moments is not None
    assert Decimal("2") < moments.kurtosis < Decimal("4")
    skewed = return_moments((Decimal("1"), Decimal("1"), Decimal("1"), Decimal("8")))
    assert skewed is not None
    assert skewed.skewness > 0


def test_normal_quantile_matches_a_known_value() -> None:
    assert abs(_norm_ppf(0.975) - 1.959963984540054) < 1e-8
    assert abs(_norm_ppf(0.025) + 1.959963984540054) < 1e-6
    assert abs(_norm_ppf(0.5)) < 1e-9
    assert _norm_ppf(0.001) < 0
    assert _norm_cdf(0.0) == 0.5


def test_pbo_of_a_half_sample_reversal_is_one_and_noise_is_near_one_half() -> None:
    overfit = probability_of_backtest_overfitting(
        ((Decimal("1"), Decimal("1"), Decimal("-1"), Decimal("-1")), (Decimal("0"),) * 4),
        candidate_index=0,
        n_blocks=2,
    )
    assert overfit == Decimal("1")

    estimates: list[Decimal] = []
    for seed in range(8):
        rng = np.random.default_rng(seed)
        noise = tuple(_decimals(list(rng.normal(0, 1, size=16 * 16))) for _ in range(10))
        estimate = probability_of_backtest_overfitting(noise, candidate_index=0)
        assert estimate is not None
        estimates.append(estimate)
    random_pbo = sum(estimates, Decimal(0)) / Decimal(len(estimates))
    assert Decimal("0.45") < random_pbo < Decimal("0.65")

    rng = np.random.default_rng(11)
    edge = _decimals(list(rng.normal(0.8, 1, size=16 * 12)))
    rest = tuple(_decimals(list(rng.normal(0, 1, size=16 * 12))) for _ in range(5))
    skilled = probability_of_backtest_overfitting((edge, *rest), candidate_index=0)
    assert skilled is not None
    assert skilled < Decimal("0.2")
    assert probability_of_backtest_overfitting((edge,), candidate_index=0) is None


def _reference_pbo(columns: list[tuple[Decimal, ...]], n_blocks: int) -> Decimal:
    """CSCV напряму: склеєні блоки половини, Sharpe через std(ddof=1), середній ранг на OOS."""
    matrix = np.column_stack([np.asarray([float(value) for value in column]) for column in columns])
    size = matrix.shape[0] // n_blocks
    parts = [matrix[index * size : (index + 1) * size] for index in range(n_blocks)]
    n_strategies = matrix.shape[1]
    bad = 0
    total = 0
    for chosen in combinations(range(n_blocks), n_blocks // 2):
        inside = np.vstack([parts[index] for index in chosen])
        outside = np.vstack([parts[index] for index in range(n_blocks) if index not in chosen])
        in_sharpe = inside.mean(axis=0) / inside.std(axis=0, ddof=1)
        out_sharpe = outside.mean(axis=0) / outside.std(axis=0, ddof=1)
        mine = out_sharpe[int(np.argmax(in_sharpe))]
        below = sum(1 for value in out_sharpe if value < mine)
        tied = sum(1 for value in out_sharpe if value == mine)
        if (below + (tied + 1) / 2) / (n_strategies + 1) <= 0.5:
            bad += 1
        total += 1
    return Decimal(f"{bad / total:.12f}")


def test_pbo_matches_a_direct_cscv_with_ties(monkeypatch: pytest.MonkeyPatch) -> None:
    rng = np.random.default_rng(17)
    columns = [_decimals(list(rng.normal(0.1 * index, 1, size=12 * 5)), places=4) for index in range(6)]
    # Копія дає нічию і на in-sample, і на out-of-sample.
    columns.append(columns[2])
    expected = _reference_pbo(columns, 12)
    assert probability_of_backtest_overfitting(tuple(columns), candidate_index=0, n_blocks=12) == expected
    # Порція з одного розбиття: межі порцій не змінюють результат.
    monkeypatch.setattr(ml_statistics, "_CSCV_CELLS", 1)
    assert probability_of_backtest_overfitting(tuple(columns), candidate_index=0, n_blocks=12) == expected


def test_pbo_memory_does_not_grow_with_the_number_of_splits() -> None:
    # 12870 розбиттів × 8 блоків × 200 спроб у float64 одним масивом — 165 МБ.
    rng = np.random.default_rng(19)
    trials = tuple(_decimals(list(rng.normal(0, 1, size=16 * 20)), places=4) for _ in range(200))
    tracemalloc.start()
    try:
        estimate = probability_of_backtest_overfitting(trials, candidate_index=0)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert estimate is not None
    assert peak < 32 * 1024 * 1024


def test_no_public_opening_bypasses_the_gate() -> None:
    # Відкриття hold-out без G1 — лише приватний крок `open_holdout_after_gate` (ТЗ §5А.10).
    assert not hasattr(ml_validation, "open_holdout")
    assert not hasattr(ml_statistics, "open_holdout")
    assert callable(ml_statistics.open_holdout_after_gate)


def test_gate_thresholds_keep_the_doubled_cost_stress_and_name_looser_values() -> None:
    with pytest.raises(ValueError, match="подвоєння"):
        GateThresholds(stress_cost_factor=Decimal("1.5"))
    assert GateThresholds(stress_cost_factor=Decimal("3")).stress_cost_factor == Decimal("3")
    # Базові фолди — завжди звичайні витрати; поріг для них не налаштовується.
    with pytest.raises(ValueError, match="Extra inputs"):
        GateThresholds(base_cost_factor=Decimal("2"))  # type: ignore[call-arg]
    assert loosened_thresholds(GateThresholds()) == ()
    stricter = GateThresholds(dsr_min=Decimal("0.99"), pbo_max=Decimal("0.3"), median_pf_min=Decimal("1.3"))
    assert loosened_thresholds(stricter) == ()
    looser = GateThresholds(min_oos_trades=50, dsr_min=Decimal("0.5"), pbo_max=Decimal("0.6"))
    assert loosened_thresholds(looser) == ("min_oos_trades", "dsr_min", "pbo_max")


def test_journal_r_strings_reject_floats() -> None:
    assert parse_oos_results(["-1.5", "0.25"]) == (Decimal("-1.5"), Decimal("0.25"))
    with pytest.raises(ValueError, match="рядок Decimal"):
        parse_oos_results([0.1])  # type: ignore[list-item]
    parsed = fold_outcomes_from_metrics(
        {
            "folds": [
                {
                    "below_min_trades": False,
                    "cost_factor": "2",
                    "trading": {"metrics": {"n_trades": 3, "total_net_usd": "-1.5", "profit_factor": "1.25"}},
                }
            ]
        }
    )
    assert parsed[0].profit_factor == Decimal("1.25")
    assert parsed[0].cost_factor == Decimal("2")
    with pytest.raises(ValueError, match="рядок Decimal"):
        fold_outcomes_from_metrics(
            {
                "folds": [
                    {
                        "below_min_trades": False,
                        "cost_factor": 1,
                        "trading": {"metrics": {"n_trades": 3, "total_net_usd": "1", "profit_factor": None}},
                    }
                ]
            }
        )


def test_gate_passes_on_the_boundary_and_names_every_miss() -> None:
    assert _gate().passed is True
    assert _gate(dsr=Decimal("0.95")).passed is True
    assert _gate(pbo=Decimal("0.5")).passed is False
    assert _gate(dsr=Decimal("0.949")).reasons == ("G1_DSR",)
    even = (_fold("1", "1.14", n_trades=50), _fold("1", "1.16", n_trades=50))
    even_stress = tuple(_fold("1", "1.05", n_trades=50, cost="2") for _ in range(2))
    assert _gate(folds=even, stress=even_stress).passed is True
    below = (_fold("1", "1.10", n_trades=50), _fold("1", "1.16", n_trades=50))
    assert "G1_MEDIAN_PF" in _gate(folds=below, stress=even_stress).reasons
    no_loss = tuple(_fold("3", None) for _ in range(5))
    assert _gate(folds=no_loss).median_pf_unbounded is True
    assert _gate(folds=no_loss).passed is True

    ten = tuple(_fold("1", "1.20") if index < 7 else _fold("-1", "1.20") for index in range(10))
    assert _gate(folds=ten, stress=_passing_stress(10)).passed is True
    six = tuple(_fold("1", "1.20") if index < 6 else _fold("-1", "1.20") for index in range(10))
    assert "G1_FOLD_SHARE" in _gate(folds=six, stress=_passing_stress(10)).reasons

    short = tuple(_fold("10", "1.20", n_trades=19) for _ in range(5))
    missed = _gate(folds=short, stress=tuple(_fold("4", "1.05", n_trades=19, cost="2") for _ in range(5)))
    assert "G1_MIN_TRADES_FOLD" in missed.reasons
    assert "G1_MIN_TRADES_TOTAL" in missed.reasons
    flagged = (_fold("10", "1.20", below=True), *_passing_folds(4))
    assert "G1_MIN_TRADES_FOLD" in _gate(folds=flagged).reasons

    assert "G1_STRESS" in _gate(stress=_passing_stress(4)).reasons
    weak_stress = tuple(_fold("1", "0.99", cost="2") for _ in range(5))
    assert "G1_STRESS" in _gate(stress=weak_stress).reasons
    flat_stress = tuple(_fold("0", "1.05", cost="2") for _ in range(5))
    assert "G1_STRESS" in _gate(stress=flat_stress).reasons
    assert "G1_FILTER" in _gate(unfiltered_total_net=Decimal("50")).reasons
    assert "G1_FILTER" in _gate(unfiltered_total_net=None).reasons
    assert _gate(label_type="rule_only", unfiltered_total_net=Decimal("999")).passed is True
    assert _gate(label_type="direct", unfiltered_total_net=None).passed is True
    assert "G1_COST_FACTOR" in _gate(folds=tuple(_fold("10", "1.20", cost="2") for _ in range(5))).reasons
    assert _gate(dsr=None, pbo=None).reasons == ("G1_DSR", "G1_PBO")


def test_gate_thresholds_reject_a_looser_fold_minimum_than_the_caller_sets() -> None:
    loose = GateThresholds(min_trades_per_fold=1, min_oos_trades=5)
    folds = tuple(_fold("2", "1.20", n_trades=5) for _ in range(2))
    stress = tuple(_fold("1", "1.10", n_trades=5, cost="2") for _ in range(2))
    assert (
        evaluate_gate(
            label_type="rule_only",
            folds=folds,
            stress=stress,
            dsr=Decimal("0.99"),
            pbo=Decimal("0.1"),
            n_trials=2,
            thresholds=loose,
        ).passed
        is True
    )


def _plain_sharpe(series: tuple[Decimal, ...]) -> float:
    values = np.asarray([float(item) for item in series], dtype=np.float64)
    return float(values.mean() / values.std(ddof=1))


def test_bootstrap_result_is_a_model_instance() -> None:
    # Keeps the import of the result type honest for the one-week equality checks above.
    assert isinstance(
        block_bootstrap_pf((_trade(0, Decimal("-1")), _trade(1, Decimal("2"))), n_replicates=_REPLICATES, seed=0),
        BlockBootstrapPf,
    )
