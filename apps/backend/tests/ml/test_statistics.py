"""Блочний bootstrap, DSR, PBO, коридор і умови Gate G1 (ТЗ §5.2.5, §5.7, §5А.10)."""

from __future__ import annotations

import math
import tracemalloc
from decimal import Decimal
from itertools import combinations
from statistics import NormalDist

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import app.ml.statistics as ml_statistics
import app.ml.validation as ml_validation
from app.exchange.types import PositionSide
from app.ml.backtest import BacktestDecision, BacktestMetrics, BacktestReport, BacktestTrade
from app.ml.features import HOUR_MS
from app.ml.statistics import (
    DAY_MS,
    DSR_SPREAD_MIN_TRADES,
    WEEK_MS,
    BlockBootstrapPf,
    FoldOutcome,
    GateDecision,
    GateThresholds,
    TrialSeries,
    _norm_cdf,
    _norm_ppf,
    _PathPf,
    _pf_percentile,
    _psr,
    _selection_benchmark,
    block_bootstrap_pf,
    corridor_payload,
    deflated_sharpe,
    deflated_sharpe_ratio,
    evaluate_gate,
    fold_outcome_from_report,
    fold_outcomes_from_metrics,
    live_corridor,
    loosened_thresholds,
    loss_streak,
    parse_oos_results,
    pbo_competitor,
    pbo_estimate,
    probability_of_backtest_overfitting,
    r_profit_factor,
    return_moments,
    trial_series_from_journal,
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
    n_signals: int | None = None,
    below: bool = False,
    cost: str = "1",
) -> FoldOutcome:
    # За замовчуванням п'ять сигналів фолду відкинуто лімітами: сигналів більше, ніж угод.
    return FoldOutcome(
        n_trades=n_trades,
        n_signals=n_trades + 5 if n_signals is None else n_signals,
        total_net_usd=Decimal(net),
        profit_factor=None if pf is None else Decimal(pf),
        below_min_trades=below,
        cost_factor=Decimal(cost),
    )


def _daily(values: tuple[Decimal, ...], *, days: int | None = None) -> TrialSeries:
    """Одна угода на добу з першої доби; відрізок walk-forward — `days` діб (за замовчуванням — скільки угод)."""
    span = len(values) if days is None else days
    return TrialSeries(
        returns=values,
        entry_ms=tuple(index * DAY_MS for index in range(len(values))),
        covered_from_ms=0,
        covered_to_ms=span * DAY_MS,
    )


def _decision(entry_index: int, decision: str) -> BacktestDecision:
    return BacktestDecision(
        ts_ms=entry_index * HOUR_MS,
        entry_index=entry_index,
        stage="RISK" if decision != "CLOSE" else "RESULT",
        decision=decision,
        reason_code={"ENTER": None, "SKIP": "RISK_COOLDOWN", "CLOSE": "TAKE_PROFIT"}[decision],
        side=PositionSide.LONG,
    )


def _report(trades: tuple[BacktestTrade, ...], decisions: tuple[BacktestDecision, ...]) -> BacktestReport:
    total = sum((trade.net_usd for trade in trades), Decimal(0))
    profit = sum((trade.net_usd for trade in trades if trade.net_usd > 0), Decimal(0))
    loss = sum((-trade.net_usd for trade in trades if trade.net_usd < 0), Decimal(0))
    return BacktestReport(
        symbol="BTC/USDT:USDT",
        seed=1,
        cost_factor=Decimal("2"),
        trades=trades,
        decisions=decisions,
        metrics=BacktestMetrics(
            n_trades=len(trades),
            total_r=sum((trade.r_multiple for trade in trades), Decimal(0)),
            total_net_usd=total,
            profit_factor=None if loss == 0 else profit / loss,
            max_drawdown_pct=Decimal("1"),
            hit_rate=None,
            sharpe=None,
            max_consecutive_losses=1,
            n_incomplete=0,
            n_unusable=0,
            n_funding_gaps=0,
            ending_equity=Decimal("100"),
            peak_equity=Decimal("100"),
            halted=False,
        ),
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
    # Копія кандидата дає нульовий розкид між спробами, але розкид не буває меншим за 1/(T−1):
    # N і далі знижує DSR, копією його не вимкнути.
    long_positive = (Decimal("1"), Decimal("-0.2")) * 50
    long_alone = deflated_sharpe_ratio(long_positive, trial_returns=(long_positive,), n_trials=1)
    copied = deflated_sharpe(long_positive, trial_returns=(long_positive, long_positive, ()), n_trials=3)
    assert long_alone is not None
    assert copied.dsr is not None
    assert copied.dsr < long_alone
    assert copied.spread_floor is True
    floor_only = deflated_sharpe(long_positive, trial_returns=(long_positive, ()), n_trials=2)
    assert floor_only.spread_floor is True
    with pytest.raises(ValueError, match="кількості рядів"):
        deflated_sharpe_ratio(positive, trial_returns=(positive,), n_trials=3)
    with pytest.raises(ValueError, match="variance_min_trades"):
        deflated_sharpe_ratio(positive, trial_returns=(positive,), n_trials=1, variance_min_trades=1)


def test_short_trials_count_in_n_but_do_not_set_the_spread() -> None:
    rng = np.random.default_rng(5)
    edge = _decimals(list(rng.normal(0.35, 1, size=400)), places=4)
    others = tuple(_decimals(list(rng.normal(0, 1, size=400)), places=4) for _ in range(20))
    # Обрізана спроба з трьох виграшів: Sharpe ≈ 77 на угоду.
    short = (Decimal("1.95"), Decimal("2.0"), Decimal("1.98"))
    base = deflated_sharpe_ratio(edge, trial_returns=(edge, *others), n_trials=21)
    crowded = deflated_sharpe_ratio(edge, trial_returns=(edge, *others, short), n_trials=22)
    assert base is not None
    assert crowded is not None
    # Спроба лишається в N (DSR трохи нижчий), але розкид між спробами не роздуває.
    assert Decimal("0.95") <= crowded < base
    # Якби розкид брав і три угоди, одна така спроба назавжди обвалила б DSR лінії.
    collapsed = deflated_sharpe_ratio(edge, trial_returns=(edge, *others, short), n_trials=22, variance_min_trades=2)
    assert collapsed is not None
    assert collapsed < Decimal("0.05")
    # Деталі для запису відкриття: розкид — з 21 ряду, коротка спроба лише в N.
    estimate = deflated_sharpe(edge, trial_returns=(edge, *others, short), n_trials=22)
    assert estimate.dsr == crowded
    assert estimate.n_trials == 22
    assert estimate.n_spread == 21
    assert estimate.benchmark_sharpe is not None
    assert estimate.benchmark_sharpe > 0
    # Поріг розкиду — константа, а не `min_oos_trades` G1: суворіший поріг G1 не має звужувати розкид.
    assert DSR_SPREAD_MIN_TRADES == 30
    assert "dsr_spread_min_trades" not in GateThresholds.model_fields


def test_copies_and_short_trials_cannot_switch_off_the_trial_count() -> None:
    rng = np.random.default_rng(1)
    candidate = _decimals(list(rng.normal(0.25, 1, size=400)), places=4)
    # 98 спроб по 20 угод (коротші за поріг розкиду) і копія кандидата: без нижньої межі розкид — 0.
    short = tuple(_decimals(list(rng.normal(0, 1, size=20)), places=4) for _ in range(98))
    gamed = deflated_sharpe(candidate, trial_returns=(candidate, candidate, *short), n_trials=100)
    assert gamed.spread_floor is True
    assert gamed.dsr is not None
    # З N = 100 і Sharpe ≈ 0.25 на угоду G1 не проходить: поріг — очікуваний максимум 100 спроб.
    assert gamed.dsr < Decimal("0.95")
    alone = deflated_sharpe_ratio(candidate, trial_returns=(candidate,), n_trials=1)
    assert alone is not None
    assert alone > Decimal("0.99")
    # Поріг рядів для розкиду: короткий ряд (20 угод) не входить, ряд з 30 — входить.
    thirty = _decimals(list(rng.normal(0, 1, size=30)), places=4)
    counted = deflated_sharpe(candidate, trial_returns=(candidate, thirty, short[0]), n_trials=3)
    assert counted.n_spread == 2


def test_deflated_sharpe_matches_the_paper_example() -> None:
    # Bailey & López de Prado (2014), числовий приклад: річний SR 2.5 за 1250 днів, N = 100,
    # V[SR] = 1/2 річних, асиметрія −3, сирий ексцес 10 → SR₀ ≈ 0.1132 на день, DSR ≈ 0.9004.
    daily = 2.5 / math.sqrt(250)
    benchmark = _selection_benchmark(100, 0.5 / 250)
    assert abs(benchmark - 0.1132) < 5e-5
    probability = _psr(daily, benchmark, 1250, -3.0, 10.0)
    assert probability is not None
    assert abs(probability - 0.9004) < 5e-5
    # Незалежні Φ і Φ⁻¹ зі stdlib дають той самий поріг.
    normal = NormalDist()
    euler = 0.5772156649015329
    expected = math.sqrt(0.5 / 250) * (
        (1 - euler) * normal.inv_cdf(1 - 1 / 100) + euler * normal.inv_cdf(1 - 1 / (100 * math.e))
    )
    assert abs(benchmark - expected) < 1e-8
    # Знаки поправок: права асиметрія підвищує PSR, товсті хвости знижують.
    right = _psr(0.1, 0.0, 400, 1.0, 3.0)
    left = _psr(0.1, 0.0, 400, -1.0, 3.0)
    thin = _psr(0.1, 0.0, 400, 0.0, 3.0)
    fat = _psr(0.1, 0.0, 400, 0.0, 9.0)
    assert right is not None
    assert left is not None
    assert thin is not None
    assert fat is not None
    assert right > left
    assert thin > fat


def test_pf_lower_bound_takes_the_finite_neighbour() -> None:
    # 5% шляхів зі збитками: нижня межа на стику скінченного і нескінченного — скінченна, не «без збитків».
    samples = [_PathPf(Decimal("0.5"), False)] * 50 + [_PathPf(None, True)] * 950
    assert _pf_percentile(samples, Decimal("0.05"), lower=True) == _PathPf(Decimal("0.5"), False)
    assert _pf_percentile(samples, Decimal("0.05"), lower=False) == _PathPf(None, True)
    assert _pf_percentile(samples, Decimal("0.95"), lower=False) == _PathPf(None, True)


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
        (_daily((Decimal("1"), Decimal("1"), Decimal("-1"), Decimal("-1"))), _daily((Decimal("0"),) * 4)),
        candidate_index=0,
        n_blocks=2,
    )
    assert overfit == Decimal("1")

    estimates: list[Decimal] = []
    for seed in range(8):
        rng = np.random.default_rng(seed)
        noise = tuple(_daily(_decimals(list(rng.normal(0, 1, size=16 * 16)))) for _ in range(10))
        estimate = probability_of_backtest_overfitting(noise, candidate_index=0)
        assert estimate is not None
        estimates.append(estimate)
    random_pbo = sum(estimates, Decimal(0)) / Decimal(len(estimates))
    assert Decimal("0.45") < random_pbo < Decimal("0.65")

    rng = np.random.default_rng(11)
    edge = _daily(_decimals(list(rng.normal(0.8, 1, size=16 * 12))))
    rest = tuple(_daily(_decimals(list(rng.normal(0, 1, size=16 * 12)))) for _ in range(5))
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
    trials = tuple(_daily(column) for column in columns)
    assert probability_of_backtest_overfitting(trials, candidate_index=0, n_blocks=12) == expected
    # Порція з одного розбиття: межі порцій не змінюють результат.
    monkeypatch.setattr(ml_statistics, "_CSCV_CELLS", 1)
    assert probability_of_backtest_overfitting(trials, candidate_index=0, n_blocks=12) == expected


def test_pbo_memory_does_not_grow_with_the_number_of_splits() -> None:
    # 12870 розбиттів × 8 блоків × 200 спроб у float64 одним масивом — 165 МБ.
    rng = np.random.default_rng(19)
    trials = tuple(_daily(_decimals(list(rng.normal(0, 1, size=16 * 20)), places=4)) for _ in range(200))
    tracemalloc.start()
    try:
        estimate = probability_of_backtest_overfitting(trials, candidate_index=0)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert estimate is not None
    assert peak < 32 * 1024 * 1024


def test_masked_baseline_does_not_change_who_competes_in_pbo() -> None:
    """Збитковий ряд на весь відрізок змінює склад матриці, доки його не вимкнуто (рішення 0016)."""
    days = 16 * 4
    good = _daily(_decimals([0.4] * days, places=1))
    other = _daily(_decimals([0.0] * days, places=1))
    loser = _daily(_decimals([-1.0] * days, places=1))
    plain = pbo_estimate((good, other), candidate_index=0)
    leaked = pbo_estimate((good, other, loser), candidate_index=0)
    masked = pbo_estimate((good, other, pbo_competitor(loser, compete=False)), candidate_index=0)
    assert leaked.n_strategies == 3
    assert plain.n_strategies == 2
    assert masked.n_strategies == plain.n_strategies
    assert masked.pbo == plain.pbo
    assert masked.n_splits == plain.n_splits


def test_pbo_rows_are_days_and_a_sparse_trial_still_competes() -> None:
    rng = np.random.default_rng(23)
    days = 16 * 6
    dense = _daily(_decimals(list(rng.normal(0.2, 1, size=days)), places=4))
    noise = tuple(_daily(_decimals(list(rng.normal(0, 1, size=days)), places=4)) for _ in range(4))
    # Рідка спроба: 30 угод у випадкові дні (дві — в одну добу) на тому самому відрізку walk-forward.
    picked = sorted(int(day) for day in rng.choice(days, size=29, replace=False))
    picked = sorted([*picked, picked[10]])
    sparse_r = _decimals(list(rng.normal(0.1, 1, size=len(picked))), places=4)
    sparse = TrialSeries(
        returns=sparse_r,
        entry_ms=tuple(day * DAY_MS + 5 * HOUR_MS for day in picked),
        covered_from_ms=0,
        covered_to_ms=days * DAY_MS,
    )
    per_day = [Decimal(0)] * days
    for day, value in zip(picked, sparse_r, strict=True):
        per_day[day] += value
    expected = _reference_pbo([dense.returns, tuple(per_day), *(item.returns for item in noise)], 16)
    trials = (dense, sparse, *noise)
    assert probability_of_backtest_overfitting(trials, candidate_index=0) == expected
    # Спільний відрізок: рядок — доба, тож який ряд кандидат, не важить.
    assert probability_of_backtest_overfitting(trials, candidate_index=1) == expected


def _covering(values: tuple[Decimal, ...], *, from_day: int) -> TrialSeries:
    """Одна угода на добу з доби `from_day`; відрізок — від неї до останньої угоди включно."""
    return TrialSeries(
        returns=values,
        entry_ms=tuple((from_day + index) * DAY_MS for index in range(len(values))),
        covered_from_ms=from_day * DAY_MS,
        covered_to_ms=(from_day + len(values)) * DAY_MS,
    )


def _sample_sharpe(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    if variance <= 0:
        return math.inf if mean > 0 else (-math.inf if mean < 0 else 0.0)
    return mean / math.sqrt(variance)


def _reference_pbo_with_gaps(columns: list[list[float | None]], n_blocks: int) -> Decimal | None:
    """CSCV з пропусками напряму: стратегія змагається в розбитті, лише якщо має ≥ 2 доби в обох половинах."""
    size = len(columns[0]) // n_blocks
    bad = 0
    total = 0
    for chosen in combinations(range(n_blocks), n_blocks // 2):
        inside = [row for block in chosen for row in range(block * size, (block + 1) * size)]
        outside = [
            row for block in range(n_blocks) if block not in chosen for row in range(block * size, (block + 1) * size)
        ]
        scores: dict[int, tuple[float, float]] = {}
        for index, column in enumerate(columns):
            in_sharpe = _sample_sharpe([value for row in inside if (value := column[row]) is not None])
            out_sharpe = _sample_sharpe([value for row in outside if (value := column[row]) is not None])
            if in_sharpe is not None and out_sharpe is not None:
                scores[index] = (in_sharpe, out_sharpe)
        if len(scores) < 2:
            continue
        best = max(scores, key=lambda index: scores[index][0])
        mine = scores[best][1]
        below = sum(1 for _in, out in scores.values() if out < mine)
        tied = sum(1 for _in, out in scores.values() if out == mine)
        if (below + (tied + 1) / 2) / (len(scores) + 1) <= 0.5:
            bad += 1
        total += 1
    return None if total == 0 else Decimal(f"{bad / total:.12f}")


def _gapped(series: TrialSeries, days: int) -> list[float | None]:
    """Добовий ряд на відрізку [0, days): доба поза відрізком спроби — пропуск."""
    assert series.covered_from_ms is not None
    assert series.covered_to_ms is not None
    out: list[float | None] = []
    for day in range(days):
        inside = series.covered_from_ms <= day * DAY_MS and (day + 1) * DAY_MS <= series.covered_to_ms
        total = sum(
            (value for value, entry in zip(series.returns, series.entry_ms, strict=True) if entry // DAY_MS == day),
            Decimal(0),
        )
        out.append(float(total) if inside else None)
    return out


def test_pbo_with_gaps_scores_every_trial_on_the_days_it_covered(monkeypatch: pytest.MonkeyPatch) -> None:
    rng = np.random.default_rng(31)
    candidate = _daily(_decimals(list(rng.normal(0.3, 1, size=64)), places=4))
    # Суперники з різною геометрією: зсунутий на 5 діб, лише перша половина, лише 3 доби.
    shifted = _covering(_decimals(list(rng.normal(0, 1, size=64)), places=4), from_day=5)
    first_half = _covering(_decimals(list(rng.normal(0.2, 1, size=32)), places=4), from_day=0)
    tiny = _covering(_decimals(list(rng.normal(3, 1, size=3)), places=4), from_day=40)
    trials = (candidate, shifted, first_half, tiny)
    expected = _reference_pbo_with_gaps([_gapped(item, 64) for item in trials], 8)
    estimate = pbo_estimate(trials, candidate_index=0, n_blocks=8)
    assert estimate.pbo == expected
    # Три доби — ніде немає двох діб в обох половинах: у жодному розбитті не змагається.
    assert estimate.n_strategies == 3
    assert (estimate.span_from_ms, estimate.span_to_ms) == (0, 64 * DAY_MS)
    assert estimate.n_splits == 70
    # Порції по одному розбиттю не змінюють результат.
    monkeypatch.setattr(ml_statistics, "_CSCV_CELLS", 1)
    assert pbo_estimate(trials, candidate_index=0, n_blocks=8) == estimate


def test_pbo_window_is_the_candidate_walk_forward() -> None:
    rng = np.random.default_rng(29)
    whole = _daily(_decimals(list(rng.normal(0, 1, size=64)), places=4))
    # Старий експеримент (до зсуву hold-out) покрив лише перші 32 доби: він змагається на них,
    # а відрізок PBO — увесь walk-forward кандидата, з новими даними.
    older = _covering(whole.returns[:32], from_day=0)
    moved = pbo_estimate((whole, older), candidate_index=0)
    assert moved.pbo is not None
    assert (moved.n_strategies, moved.span_from_ms, moved.span_to_ms) == (2, 0, 64 * DAY_MS)
    # Допоміжна спроба з коротшим відрізком не звужує вікно і не виключає суперників.
    helper = _covering(whole.returns[40:56], from_day=40)
    with_helper = pbo_estimate((whole, older, helper), candidate_index=0)
    assert (with_helper.span_from_ms, with_helper.span_to_ms) == (0, 64 * DAY_MS)
    assert with_helper.n_strategies == 3
    # Обрізана спроба без останнього фолду змагається на тому, що покрила.
    pruned = _covering(whole.returns[:60], from_day=0)
    assert pbo_estimate((whole, pruned), candidate_index=0).n_strategies == 2
    # Спроба без відрізка не змагається; кандидат без суперника — жодного розбиття, PBO None.
    empty = TrialSeries(returns=(), entry_ms=(), covered_from_ms=None, covered_to_ms=None)
    alone = pbo_estimate((whole, empty), candidate_index=0)
    assert (alone.pbo, alone.n_strategies, alone.n_splits) == (None, 0, 0)
    assert (alone.span_from_ms, alone.span_to_ms) == (0, 64 * DAY_MS)
    # Кандидат без відрізка або з відрізком коротшим за 2 доби на блок — PBO не рахується.
    nothing = pbo_estimate((empty, whole), candidate_index=0)
    assert (nothing.pbo, nothing.n_strategies, nothing.span_from_ms, nothing.span_to_ms) == (None, 0, None, None)
    short = pbo_estimate((_covering(whole.returns[:20], from_day=0), whole), candidate_index=0)
    assert (short.pbo, short.span_from_ms) == (None, None)


def test_trial_series_from_the_journal_needs_entry_times() -> None:
    folds = {
        "folds": [
            {"test_start_ms": 0, "test_end_ms": 2 * DAY_MS},
            {"test_start_ms": 2 * DAY_MS, "test_end_ms": 5 * DAY_MS},
        ]
    }
    series = trial_series_from_journal(["1.5", "-1"], [HOUR_MS, 3 * DAY_MS], folds)
    assert series.returns == (Decimal("1.5"), Decimal("-1"))
    assert series.entry_ms == (HOUR_MS, 3 * DAY_MS)
    assert (series.covered_from_ms, series.covered_to_ms) == (0, 5 * DAY_MS)
    empty = trial_series_from_journal([], [], {"folds": []})
    assert (empty.covered_from_ms, empty.covered_to_ms) == (None, None)
    # Рядок без часу угод (до міграції 0005) у PBO не входить мовчки: відмова.
    with pytest.raises(ValueError, match="час"):
        trial_series_from_journal(["1"], [], folds)
    with pytest.raises(ValueError, match="поза"):
        TrialSeries(returns=(Decimal("1"),), entry_ms=(6 * DAY_MS,), covered_from_ms=0, covered_to_ms=5 * DAY_MS)
    with pytest.raises(ValueError, match="порядку"):
        TrialSeries(
            returns=(Decimal("1"), Decimal("1")),
            entry_ms=(DAY_MS, 0),
            covered_from_ms=0,
            covered_to_ms=5 * DAY_MS,
        )


def test_stress_compares_signals_not_executed_trades() -> None:
    # Подвоєні витрати можуть увімкнути cooldown і пропустити вхід: угод менше, сигнали ті самі.
    fewer = tuple(_fold("4", "1.05", n_trades=18, n_signals=25, cost="2") for _ in range(5))
    assert _gate(stress=fewer).passed is True
    other = tuple(_fold("4", "1.05", n_signals=24, cost="2") for _ in range(5))
    assert _gate(stress=other).reasons == ("G1_STRESS",)
    with pytest.raises(ValueError, match="n_signals"):
        _fold("1", "1.1", n_trades=5, n_signals=4)


def test_fold_outcome_from_a_report_counts_every_signal() -> None:
    trades = (_trade(0, Decimal("2")), _trade(3 * HOUR_MS, Decimal("-1"), entry_index=3))
    decisions = (
        _decision(0, "ENTER"),
        _decision(1, "SKIP"),
        _decision(1, "CLOSE"),
        _decision(3, "ENTER"),
        _decision(4, "SKIP"),
        _decision(4, "CLOSE"),
    )
    report = _report(trades, decisions)
    assert report.n_signals == 4
    outcome = fold_outcome_from_report(report, below_min_trades=False)
    assert outcome == FoldOutcome(
        n_trades=2,
        n_signals=4,
        total_net_usd=Decimal("1"),
        profit_factor=Decimal("2"),
        below_min_trades=False,
        cost_factor=Decimal("2"),
    )


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
                    "trading": {
                        "n_signals": 4,
                        "metrics": {"n_trades": 3, "total_net_usd": "-1.5", "profit_factor": "1.25"},
                    },
                }
            ]
        }
    )
    assert parsed[0].profit_factor == Decimal("1.25")
    assert parsed[0].cost_factor == Decimal("2")
    assert parsed[0].n_signals == 4
    with pytest.raises(ValueError, match="n_signals"):
        fold_outcomes_from_metrics(
            {
                "folds": [
                    {
                        "below_min_trades": False,
                        "cost_factor": "1",
                        "trading": {"metrics": {"n_trades": 3, "total_net_usd": "1", "profit_factor": None}},
                    }
                ]
            }
        )
    with pytest.raises(ValueError, match="рядок Decimal"):
        fold_outcomes_from_metrics(
            {
                "folds": [
                    {
                        "below_min_trades": False,
                        "cost_factor": 1,
                        "trading": {
                            "n_signals": 3,
                            "metrics": {"n_trades": 3, "total_net_usd": "1", "profit_factor": None},
                        },
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


def test_gate_uses_the_thresholds_the_caller_passes() -> None:
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
