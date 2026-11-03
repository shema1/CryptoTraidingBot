"""Бінарна meta-модель: ваги лише на train, фільтр через бектест, перемішані мітки без сигналу (T2.8)."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.ml.features import (
    HOUR_MS,
    CandleBar,
    SymbolData,
    TimedValue,
    feature_schema_hash,
)
from app.ml.meta_model import (
    META_MIN_PROBABILITY,
    SIDE_COLUMN,
    MetaRun,
    MetaTrainError,
    keep,
    lgbm_journal_params,
    probability_gap,
    run_meta_experiment,
    train_meta,
)
from app.ml.models import Experiment, ExperimentTrial
from app.ml.statistics import GateRejected, evaluate_gate, fold_outcomes_from_metrics, open_holdout_after_gate
from app.ml.strategy import (
    ALWAYS_FLAT,
    ALWAYS_LONG,
    BREAKOUT,
    RANDOM_ENTRIES,
    StrategySpec,
    SymbolHistory,
    compare_strategies,
    is_reference_baseline,
    primary_candidates,
)
from app.ml.trade_sim import LabelSpan, SimCandle, SkipReason, TargetParams, label_meta, sample_weights
from app.ml.validation import ResearchSplit, build_split
from app.risk.costs import CostModel, FundingEvent
from app.risk.limits import RiskLimits
from tests.db.factories import make_model
from tests.risk.conftest import instrument, limits

BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
HOUR = HOUR_MS
_FIRST_ENTRY = 1321
_SPACING = 40
_EQUITY = Decimal(10000)
_MARGIN = Decimal("0.005")
_SEED = 7


def _bar(index: int, open_: str, high: str, low: str, close: str) -> SimCandle:
    return SimCandle(
        open_time_ms=index * HOUR,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
    )


def _flat(count: int) -> list[SimCandle]:
    return [_bar(index, "100", "101", "99", "100") for index in range(count)]


def _paint(candles: list[SimCandle], setups: int, *, good: Callable[[int], bool]) -> None:
    for step in range(setups):
        decision = _FIRST_ENTRY - 1 + step * _SPACING
        if good(step):
            candles[decision] = _bar(decision, "100", "102", "99.5", "101.5")
            candles[decision + 1] = _bar(decision + 1, "100", "110", "99.5", "100")
        else:
            candles[decision] = _bar(decision, "100", "130", "99", "128")
            candles[decision + 1] = _bar(decision + 1, "100", "128.5", "90", "100")


def _entry_index(step: int) -> int:
    return _FIRST_ENTRY + step * _SPACING


def _zero_costs() -> CostModel:
    zero = Decimal(0)
    return CostModel(
        taker_fee_rate=Decimal("0.00055"),
        maker_fee_rate=Decimal("0.00055"),
        slippage_entry_atr=zero,
        slippage_entry_bps=zero,
        slippage_sl_atr=zero,
        slippage_sl_bps=zero,
        slippage_tp_atr=zero,
        slippage_tp_bps=zero,
    )


def _wide_limits() -> RiskLimits:
    # Синтетичний ряд не має впертися в денний ліміт раніше, ніж фільтр встигне відсіяти угоду.
    return limits(
        daily_loss_limit_pct=Decimal("0.5"),
        max_drawdown_pct=Decimal("0.5"),
        max_consecutive_losses=100,
        cooldown_bars=1,
        max_trades_per_day=100,
    )


def _history(symbol: str, candles: list[SimCandle]) -> SymbolHistory:
    funding = (FundingEvent(time_ms=0, rate=Decimal(0), mark_price=Decimal(100)),)
    return SymbolHistory(
        symbol=symbol,
        candles=tuple(candles),
        instrument=instrument(symbol=symbol),
        funding=funding,
    )


def _split(count: int, *, first_test: int, segment: int) -> ResearchSplit:
    split = build_split(
        data_from_ms=0,
        data_to_ms=count * HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        min_train_ms=first_test * HOUR,
        test_segment_ms=segment * HOUR,
        test_segment_extended_ms=(segment + _SPACING) * HOUR,
        min_trades_per_fold=1,
    )
    assert len(split.folds) == 1
    return split


def _train(
    books: Sequence[SymbolHistory],
    split: ResearchSplit,
    *,
    shuffled_labels: bool = False,
    min_probability: Decimal = META_MIN_PROBABILITY,
    feature_data: dict[str, SymbolData] | None = None,
) -> MetaRun:
    return train_meta(
        books,
        spec=BREAKOUT,
        split=split,
        trading_symbol=BTC,
        limits=_wide_limits(),
        initial_equity=_EQUITY,
        maintenance_margin_rate=_MARGIN,
        seed=_SEED,
        costs=_zero_costs(),
        params=TargetParams(horizon_bars=4),
        feature_data=feature_data,
        min_probability=min_probability,
        shuffled_labels=shuffled_labels,
    )


def _learning_books() -> tuple[list[SimCandle], tuple[SymbolHistory, ...], ResearchSplit]:
    count = 5800
    candles = _flat(count)
    _paint(candles, 80, good=lambda step: step % 2 == 0)
    split = _split(count, first_test=_entry_index(60), segment=20 * _SPACING)
    return candles, (_history(BTC, candles),), split


def test_recipe_is_the_cpu_binary_default_and_not_a_search() -> None:
    params = lgbm_journal_params()
    assert params["boosting_type"] == "gbdt"
    assert params["num_leaves"] == 31
    assert params["learning_rate"] == Decimal("0.1")
    assert params["n_estimators"] == 100
    assert params["min_child_samples"] == 20
    assert params["class_weight"] is None
    assert params["n_jobs"] == 1
    assert params["deterministic"] is True
    assert "device" not in params
    assert keep(META_MIN_PROBABILITY)
    assert keep(Decimal("0.5"))
    assert not keep(Decimal("0.4999999999"))
    assert keep(Decimal(0), min_probability=Decimal(0))
    with pytest.raises(ValueError, match="поріг"):
        keep(Decimal("0.5"), min_probability=Decimal("1.1"))


def test_only_a_fixed_primary_candidate_can_be_trained() -> None:
    candles = _flat(80)
    books = (_history(BTC, candles),)
    split = build_split(
        data_from_ms=0,
        data_to_ms=80 * HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        min_train_ms=40 * HOUR,
        test_segment_ms=8 * HOUR,
        test_segment_extended_ms=16 * HOUR,
        min_trades_per_fold=1,
    )
    custom = StrategySpec(name="ema_cross", fast=10, slow=50)
    for spec in (ALWAYS_FLAT, ALWAYS_LONG, RANDOM_ENTRIES, custom):
        with pytest.raises(ValueError, match="кандидаті"):
            train_meta(
                books,
                spec=spec,
                split=split,
                trading_symbol=BTC,
                limits=_wide_limits(),
                initial_equity=_EQUITY,
                maintenance_margin_rate=_MARGIN,
                seed=_SEED,
            )


def test_sample_weights_are_the_train_balance_and_embargo_holds_for_every_symbol() -> None:
    count = 2700
    candles = _flat(count)
    _paint(candles, 20, good=lambda step: step % 4 == 0)
    books = (_history(BTC, candles), _history(ETH, candles))
    split = _split(count, first_test=_entry_index(16), segment=4 * _SPACING)
    run = _train(books, split)
    fold = run.folds[0]
    window = split.folds[0]
    assert fold.model.estimator.get_params()["class_weight"] is None
    assert fold.model.estimator.get_params()["device"] == "cpu"
    assert fold.model.columns[-1] == SIDE_COLUMN
    assert SIDE_COLUMN not in fold.model.feature_columns
    assert fold.model.schema_hash == feature_schema_hash(fold.model.feature_columns)
    spans = tuple(
        LabelSpan(symbol=row.symbol, entry_open_time_ms=row.time_ms, exit_open_time_ms=row.exit_open_time_ms)
        for row in fold.fitted
    )
    expected = sample_weights(spans, [row.fit_label for row in fold.fitted])
    assert [row.weight for row in fold.fitted] == list(expected)
    assert set(expected) == {Decimal(2), Decimal(2) / Decimal(3)}
    symbols = {row.symbol for row in fold.fitted}
    assert symbols == {BTC, ETH}
    for fitted in fold.fitted:
        assert fitted.fit_label == fitted.true_label
        assert fitted.time_ms < window.embargo_start_ms
        assert fitted.weight > 0
    for scored in fold.scored:
        assert window.test_start_ms <= scored.time_ms < window.test_end_ms
        assert scored.time_ms < split.holdout_from_ms


def test_threshold_zero_matches_the_unfiltered_rule() -> None:
    count = 2700
    candles = _flat(count)
    _paint(candles, 20, good=lambda step: step % 4 == 0)
    books = (_history(BTC, candles),)
    split = _split(count, first_test=_entry_index(16), segment=4 * _SPACING)
    run = _train(books, split, min_probability=Decimal(0))
    comparison = compare_strategies(
        books,
        split=split,
        trading_symbol=BTC,
        limits=_wide_limits(),
        initial_equity=_EQUITY,
        maintenance_margin_rate=_MARGIN,
        seed=_SEED,
        costs=_zero_costs(),
        params=TargetParams(horizon_bars=4),
        specs=(BREAKOUT,),
    )
    rule = next(item for item in comparison.variants if item.spec == BREAKOUT)
    rule_net = sum((score.trading_net_usd for score in rule.scores), Decimal(0))
    assert run.unfiltered_total_net == rule_net
    assert run.filtered_total_net == rule_net
    assert all(row.taken for row in run.folds[0].scored)
    assert run.scored_unfiltered_total_net == rule_net
    assert run.folds[0].n_unscored_signals == 0
    # Модель, що нічого не відсіяла, правило не перевершує: потрібна строга перевага.
    assert not run.beats_unfiltered
    trading = next(report for report in run.folds[0].record.reports if report.symbol == BTC)
    assert trading.n_signals == len(run.folds[0].scored)
    assert run.folds[0].stress.n_signals == trading.n_signals
    assert run.folds[0].stress.cost_factor == Decimal(2)


def test_real_labels_separate_and_beat_the_unfiltered_rule() -> None:
    _candles, books, split = _learning_books()
    run = _train(books, split)
    again = _train(books, split)
    fold = run.folds[0]
    gap = probability_gap(fold.scored)
    assert gap is not None
    assert gap > Decimal("0.5")
    assert run.beats_unfiltered
    assert run.filtered_total_net > run.unfiltered_total_net
    assert run.filtered_total_net > run.scored_unfiltered_total_net
    assert [row.true_label for row in fold.scored if row.taken] == [1] * sum(row.taken for row in fold.scored)
    assert fold.stress.n_signals == next(report.n_signals for report in fold.record.reports if report.symbol == BTC)
    assert fold.model.estimator.get_params()["random_state"] == _SEED
    assert [row.probability for row in again.folds[0].scored] == [row.probability for row in fold.scored]
    for row in fold.fitted:
        step = (row.time_ms // HOUR - _FIRST_ENTRY) // _SPACING
        assert row.true_label == (0 if step % 2 else 1)
        assert row.fit_label == row.true_label
    assert not is_reference_baseline(run.trial_params())


def test_shuffled_train_labels_leave_no_probability_gap() -> None:
    _candles, books, split = _learning_books()
    run = _train(books, split, shuffled_labels=True)
    fold = run.folds[0]
    gap = probability_gap(fold.scored)
    assert gap is not None
    assert abs(gap) < Decimal("0.15")
    assert any(row.fit_label != row.true_label for row in fold.fitted)
    for row in fold.scored:
        step = (row.entry_index - _FIRST_ENTRY) // _SPACING
        assert row.true_label == (0 if step % 2 else 1)


def test_holdout_prices_do_not_change_walk_forward_scores() -> None:
    candles, books, split = _learning_books()
    run = _train(books, split)
    mutated = list(candles)
    start = split.holdout_from_ms // HOUR
    for index in range(start, start + 5):
        mutated[index] = _bar(index, "100", "500", "50", "400")
    second = _train((_history(BTC, mutated),), split)
    assert [row.probability for row in second.folds[0].scored] == [row.probability for row in run.folds[0].scored]
    assert [row.time_ms for row in second.folds[0].fitted] == [row.time_ms for row in run.folds[0].fitted]
    assert all(row.time_ms < split.holdout_from_ms for row in run.folds[0].fitted)
    assert all(row.time_ms < split.holdout_from_ms for row in run.folds[0].scored)
    # Те, що G1 прочитає зі спроби (фолди, звіти правила і стресу), від цін hold-out теж не залежить.
    assert second.folds[0].record == run.folds[0].record
    assert second.trial_params()["comparison"] == run.trial_params()["comparison"]


def test_open_interest_column_follows_train_coverage_only() -> None:
    count = 2000
    candles = _flat(count)
    _paint(candles, 8, good=lambda step: step % 2 == 0)
    history = _history(BTC, candles)
    split = build_split(
        data_from_ms=0,
        data_to_ms=count * HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        min_train_ms=1400 * HOUR,
        test_segment_ms=48 * HOUR,
        test_segment_extended_ms=96 * HOUR,
        min_trades_per_fold=1,
    )
    assert len(split.folds) >= 2
    embargo = split.folds[0].embargo_start_ms // HOUR
    bars = tuple(
        CandleBar(
            open_time_ms=bar.open_time_ms,
            open=float(bar.open),
            high=float(bar.high),
            low=float(bar.low),
            close=float(bar.close),
            volume=1.0,
        )
        for bar in candles
        if bar.open_time_ms < split.holdout_from_ms
    )
    interest = tuple(TimedValue(ts_ms=step * HOUR, value=float(100 + step)) for step in range(embargo))
    feature_data = {BTC: SymbolData(candles=bars, open_interest=interest)}
    run = train_meta(
        (history,),
        spec=BREAKOUT,
        split=split,
        trading_symbol=BTC,
        limits=_wide_limits(),
        initial_equity=_EQUITY,
        maintenance_margin_rate=_MARGIN,
        seed=_SEED,
        costs=_zero_costs(),
        params=TargetParams(horizon_bars=4),
        feature_data=feature_data,
    )
    assert "oi_chg_24" in run.folds[0].model.feature_columns
    assert "oi_chg_24" not in run.folds[1].model.feature_columns


def test_a_skipped_path_is_not_stored_as_a_zero_label() -> None:
    count = 2200
    candles = _flat(count)
    _paint(candles, 8, good=lambda step: step % 2 == 0)
    skipped_entry = _entry_index(7)
    del candles[skipped_entry + 2]
    history = _history(BTC, candles)
    split = _split(count, first_test=_entry_index(8), segment=2 * _SPACING)
    candidates = primary_candidates(BREAKOUT, history.candles, symbol=BTC, seed=_SEED)
    assert any(candidate.entry_index == skipped_entry for candidate in candidates)
    labeled = label_meta(
        history.candles,
        candidates,
        params=TargetParams(horizon_bars=4),
        costs=_zero_costs(),
        funding=history.funding,
    )
    assert any(item.entry_index == skipped_entry for item in labeled.skipped)
    assert all(sample.trade.entry_index != skipped_entry for sample in labeled.samples)
    run = _train((history,), split)
    skipped_time = skipped_entry * HOUR
    assert all(row.time_ms != skipped_time for row in run.folds[0].fitted)
    assert all(row.true_label in (0, 1) for row in run.folds[0].fitted)


def test_a_gap_after_a_test_entry_does_not_drop_the_signal_from_the_filter() -> None:
    # Дірка після входу — майбутнє. Фільтр не має її знати: з порогом 0 він торгує те саме, що правило.
    count = 2700
    candles = _flat(count)
    _paint(candles, 20, good=lambda step: step % 4 == 0)
    split = _split(count, first_test=_entry_index(16), segment=4 * _SPACING)
    gap_entry = _entry_index(17)
    window = split.folds[0]
    assert window.test_start_ms <= gap_entry * HOUR < window.test_end_ms
    del candles[gap_entry + 2]
    history = _history(BTC, candles)
    candidates = primary_candidates(BREAKOUT, history.candles, symbol=BTC, seed=_SEED)
    labeled = label_meta(
        history.candles,
        candidates,
        params=TargetParams(horizon_bars=4),
        costs=_zero_costs(),
        funding=history.funding,
    )
    assert any(item.entry_index == gap_entry and item.reason is SkipReason.INCOMPLETE_PATH for item in labeled.skipped)
    run = _train((history,), split, min_probability=Decimal(0))
    fold = run.folds[0]
    gap_row = next(row for row in fold.scored if row.entry_index == gap_entry)
    assert gap_row.true_label is None
    assert gap_row.taken
    assert all(row.time_ms != gap_entry * HOUR for row in fold.fitted)
    assert run.filtered_total_net == run.unfiltered_total_net
    trading = next(report for report in fold.record.reports if report.symbol == BTC)
    assert [trade.entry_open_time_ms for trade in trading.trades].count(gap_entry * HOUR) == 1
    assert fold.stress.n_signals == trading.n_signals
    labeled_rows = [row for row in fold.scored if row.true_label is not None]
    assert probability_gap(fold.scored) == probability_gap(labeled_rows)


def test_signals_the_model_cannot_score_do_not_count_for_the_filter() -> None:
    # Дірка OI робить фічі невалідними. Модель цих сигналів не бачить, правило їх торгує (рішення 0017).
    count = 2700
    candles = _flat(count)
    _paint(candles, 20, good=lambda step: step % 4 == 0)
    split = _split(count, first_test=_entry_index(16), segment=4 * _SPACING)
    bars = tuple(
        CandleBar(
            open_time_ms=bar.open_time_ms,
            open=float(bar.open),
            high=float(bar.high),
            low=float(bar.low),
            close=float(bar.close),
            volume=1.0,
        )
        for bar in candles
        if bar.open_time_ms < split.holdout_from_ms
    )
    hole = range(_entry_index(17) - 30, _entry_index(19) + 1)
    interest = tuple(
        TimedValue(ts_ms=step * HOUR, value=float(100 + step % 7)) for step in range(count) if step not in hole
    )
    books = (_history(BTC, candles),)
    feature_data = {BTC: SymbolData(candles=bars, open_interest=interest)}
    run = _train(books, split, min_probability=Decimal(0), feature_data=feature_data)
    fold = run.folds[0]
    assert "oi_chg_24" in fold.model.feature_columns
    assert [row.entry_index for row in fold.scored] == [_entry_index(16)]
    assert fold.n_unscored_signals == 3
    assert run.filtered_total_net > run.unfiltered_total_net
    assert run.filtered_total_net == run.scored_unfiltered_total_net
    assert not run.beats_unfiltered
    assert run.trial_params()["comparison"]["n_unscored_signals"] == [3]
    # Які сигнали оцінені — вирішують лише фічі, не поріг і не мітки.
    for other in (
        _train(books, split, min_probability=Decimal(1), feature_data=feature_data),
        _train(books, split, shuffled_labels=True, feature_data=feature_data),
    ):
        assert [row.entry_index for row in other.folds[0].scored] == [_entry_index(16)]
        assert other.folds[0].n_unscored_signals == 3
        assert other.scored_unfiltered_total_net == run.scored_unfiltered_total_net
        assert other.unfiltered_total_net == run.unfiltered_total_net


@pytest.mark.integration
@pytest.mark.asyncio
async def test_experiment_records_one_trial_and_not_the_stress_run(conn: AsyncConnection) -> None:
    count = 2700
    candles = _flat(count)
    _paint(candles, 20, good=lambda step: step % 4 == 0)
    books = (_history(BTC, candles),)
    split = _split(count, first_test=_entry_index(16), segment=4 * _SPACING)
    async with AsyncSession(bind=conn) as session:
        recorded = await run_meta_experiment(
            session,
            books,
            spec=BREAKOUT,
            split=split,
            trading_symbol=BTC,
            limits=_wide_limits(),
            initial_equity=_EQUITY,
            maintenance_margin_rate=_MARGIN,
            seed=_SEED,
            costs=_zero_costs(),
            params=TargetParams(horizon_bars=4),
        )
        experiment = (
            await session.execute(select(Experiment).where(Experiment.id == recorded.experiment_id))
        ).scalar_one()
        trials = (
            (
                await session.execute(
                    select(ExperimentTrial).where(ExperimentTrial.experiment_id == recorded.experiment_id)
                )
            )
            .scalars()
            .all()
        )
        # G1 читає з цієї спроби ті самі числа правила і стрес, що порахував прогін (рішення 0017).
        model = await make_model(conn, experiment_id=recorded.experiment_id)
        with pytest.raises(GateRejected) as caught:
            await open_holdout_after_gate(
                session,
                experiment_id=recorded.experiment_id,
                trial_id=recorded.trial.trial_id,
                model_version=model,
                stress=recorded.run.stress,
            )
        decision = caught.value.decision
        expected = evaluate_gate(
            label_type="meta",
            folds=fold_outcomes_from_metrics(trials[0].metrics),
            stress=recorded.run.stress,
            dsr=decision.dsr,
            pbo=decision.pbo,
            n_trials=decision.n_trials,
            unfiltered_total_net=recorded.run.unfiltered_total_net,
            scored_unfiltered_total_net=recorded.run.scored_unfiltered_total_net,
        )
        assert decision.reasons == expected.reasons
        assert ("G1_FILTER" in decision.reasons) is not recorded.run.beats_unfiltered
    assert experiment.label_type == "meta"
    assert experiment.primary_strategy == BREAKOUT.metadata()
    assert len(trials) == 1
    assert trials[0].state == "COMPLETE"
    assert trials[0].params["role"] == "meta"
    assert trials[0].params["device"] == "cpu"
    assert trials[0].params["comparison"]["beats_unfiltered"] == recorded.run.beats_unfiltered
    comparison = trials[0].params["comparison"]
    assert comparison["scored_unfiltered_total_net"] == format(recorded.run.scored_unfiltered_total_net, "f")
    assert comparison["n_unscored_signals"] == [0]
    assert trials[0].params["comparison"]["filtered_total_net"] == format(recorded.run.filtered_total_net, "f")
    assert "stress" not in trials[0].params
    runs = trials[0].metrics["folds"][0]["meta"]
    assert runs["unfiltered"]["metrics"]["total_net_usd"] == format(recorded.run.unfiltered_total_net, "f")
    assert runs["scored_unfiltered"]["metrics"]["total_net_usd"] == format(
        recorded.run.scored_unfiltered_total_net, "f"
    )
    assert runs["stress"]["n_signals"] == recorded.run.folds[0].stress.n_signals
    assert runs["stress"]["cost_factor"] == "2"
    assert not is_reference_baseline(trials[0].params)
    trading = next(report for report in recorded.run.folds[0].record.reports if report.symbol == BTC)
    assert recorded.run.folds[0].stress.n_signals == trading.n_signals
    assert recorded.run.folds[0].stress.cost_factor == Decimal(2)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gate_accepts_what_a_real_run_wrote_and_nothing_else(conn: AsyncConnection) -> None:
    # Фільтр, що справді кращий: G1 бере його числа і стрес зі спроби і не відмовляє за ними (рішення 0017).
    _candles, books, split = _learning_books()
    async with AsyncSession(bind=conn) as session:
        recorded = await run_meta_experiment(
            session,
            books,
            spec=BREAKOUT,
            split=split,
            trading_symbol=BTC,
            limits=_wide_limits(),
            initial_equity=_EQUITY,
            maintenance_margin_rate=_MARGIN,
            seed=_SEED,
            costs=_zero_costs(),
            params=TargetParams(horizon_bars=4),
        )
        assert recorded.run.beats_unfiltered
        model = await make_model(conn, experiment_id=recorded.experiment_id)
        with pytest.raises(GateRejected) as caught:
            await open_holdout_after_gate(
                session,
                experiment_id=recorded.experiment_id,
                trial_id=recorded.trial.trial_id,
                model_version=model,
                stress=recorded.run.stress,
            )
        reasons = caught.value.decision.reasons
        assert "G1_FILTER" not in reasons
        assert "G1_STRESS" not in reasons
        # Без стресу від викликача G1 бере записаний і вирішує так само.
        with pytest.raises(GateRejected) as journal_only:
            await open_holdout_after_gate(
                session,
                experiment_id=recorded.experiment_id,
                trial_id=recorded.trial.trial_id,
                model_version=model,
            )
        assert journal_only.value.decision.reasons == reasons
        # Стрес, що пройшов би сам по собі, але не з цього прогону.
        forged = tuple(
            item.model_copy(update={"total_net_usd": item.total_net_usd + 1}) for item in recorded.run.stress
        )
        with pytest.raises(GateRejected) as tampered:
            await open_holdout_after_gate(
                session,
                experiment_id=recorded.experiment_id,
                trial_id=recorded.trial.trial_id,
                model_version=model,
                stress=forged,
            )
        assert "G1_STRESS" in tampered.value.decision.reasons


@pytest.mark.integration
@pytest.mark.asyncio
async def test_one_class_train_is_recorded_as_failed(conn: AsyncConnection) -> None:
    count = 2200
    candles = _flat(count)
    _paint(candles, 8, good=lambda _step: True)
    books = (_history(BTC, candles),)
    split = _split(count, first_test=_entry_index(8), segment=2 * _SPACING)
    async with AsyncSession(bind=conn) as session:
        with pytest.raises(MetaTrainError, match="один клас") as caught:
            await run_meta_experiment(
                session,
                books,
                spec=BREAKOUT,
                split=split,
                trading_symbol=BTC,
                limits=_wide_limits(),
                initial_equity=_EQUITY,
                maintenance_margin_rate=_MARGIN,
                seed=_SEED,
                costs=_zero_costs(),
                params=TargetParams(horizon_bars=4),
            )
        assert caught.value.experiment_id is not None
        trials = (
            (
                await session.execute(
                    select(ExperimentTrial).where(ExperimentTrial.experiment_id == caught.value.experiment_id)
                )
            )
            .scalars()
            .all()
        )
        line_count = (
            await session.execute(
                select(func.count())
                .select_from(ExperimentTrial)
                .join(Experiment, Experiment.id == ExperimentTrial.experiment_id)
                .where(Experiment.research_line == BTC)
            )
        ).scalar_one()
    assert len(trials) == 1
    assert trials[0].state == "FAILED"
    assert trials[0].oos_results == []
    assert int(line_count) == 1
