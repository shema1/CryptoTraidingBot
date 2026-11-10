"""Пряме 3-класове прогнозування, ітерація 2 (T2.13): рецепт, рядки для правила входу, журнал, G1, гейт.

Синтетичний ряд: установки через кожні 8 свічок — `long`, `short`, `flat` і дві «приманки» (`lure_*`):
приманка має ту саму свічку перед входом, що й відповідна установка, але вхід б'є SL обох сторін. Тож
фічі моделі не розрізняють установку й приманку, а класи — так. Решта ряду — дрібний детермінований шум.

Тести перевіряють механіку конвеєра (чесність, журнал, G1, гейт), а не точність моделі. Правило входу
й перехресна оцінка — на сценарних рядках (`_scripted_run`): навчена модель на цих даних майже ідеальна
й дає лише виграшні прогнози, тож калібратору не з чого вчитись.
"""

from __future__ import annotations

import dataclasses
import random
from collections.abc import Callable, Sequence
from decimal import Decimal
from typing import Any

import numpy as np
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from app.exchange.types import PositionSide
from app.ml import direct_model
from app.ml.artifacts import decision_payload
from app.ml.decision import ENTRY_EVALUATION_PAST_FOLDS, OosObservation
from app.ml.direct_model import (
    CLASS_LABELS,
    PAST_FOLDS_MIN,
    DirectFold,
    DirectFoldModel,
    DirectRun,
    DirectTrainError,
    _bars,
    _Columns,
    _lookup,
    _row_vector,
    _test_observations,
    _winner,
    direct_probability_gap,
    direct_recipe,
    direct_trial_params,
    evaluate_direct,
    run_direct_experiment,
    train_direct,
)
from app.ml.features import HOUR_MS
from app.ml.meta_model import _Backtest, _feature_series, _features_for_fold, lgbm_journal_params
from app.ml.models import Experiment, ExperimentTrial, HoldoutOpening
from app.ml.research_cycle import (
    CHANGE_LINE_OR_TIMEFRAME,
    ITERATION_TWO,
    WAIT_FOR_HUMAN_HOLDOUT,
    IterationOneReport,
    iteration_two_verdict,
    run_iteration_two,
)
from app.ml.statistics import (
    G1_ENTRY_RULE,
    G1_FILTER,
    G1_STRESS,
    FoldOutcome,
    evaluate_gate,
    preview_gate_g1,
)
from app.ml.strategy import BREAKOUT, PrimarySelection, SymbolHistory
from app.ml.trade_sim import SimCandle, TargetParams
from app.ml.validation import (
    DIRECT_STAGE,
    RecordedTrial,
    ResearchSplit,
    SampleRow,
    SplitPurpose,
    build_split,
    create_experiment,
    label_diagnostics,
    record_trial,
    rows_for,
)
from tests.ml.test_meta import (
    _EQUITY,
    _FIRST_ENTRY,
    _MARGIN,
    _SEED,
    BTC,
    ETH,
    _bar,
    _entry_index,
    _history,
    _wide_limits,
    _zero_costs,
)

_TARGET = TargetParams(horizon_bars=4)
# Установки через 8 свічок: за 400 годин тесту близько п'ятдесяти установок на фолд.
_STEP = 8
_SETUPS = 500
_PATTERNS = ("long", "short", "flat", "lure_long", "lure_short")


def _pattern_at(index: int) -> str | None:
    """Установка, що починається на свічці входу `index`, або `None`."""
    offset = index - _FIRST_ENTRY
    if offset < 0 or offset % _STEP != 0 or offset // _STEP >= _SETUPS:
        return None
    return _PATTERNS[(offset // _STEP) % len(_PATTERNS)]


def _paint_setup(candles: list[SimCandle], decision: int, pattern: str) -> None:
    """Свічка перед входом і свічка входу. Ціни від відкриття 100, ATR близько 1 (TP ±2, SL ∓1)."""
    entry = decision + 1
    if pattern == "long":
        candles[decision] = _bar(decision, "100", "102", "99.5", "101.5")
        candles[entry] = _bar(entry, "100", "110", "99.5", "100")
    elif pattern == "short":
        candles[decision] = _bar(decision, "100", "100.5", "98", "98.5")
        candles[entry] = _bar(entry, "100", "100.5", "90", "92")
    elif pattern == "lure_long":
        candles[decision] = _bar(decision, "100", "102", "99.5", "101.5")
        candles[entry] = _bar(entry, "100", "103", "97", "100")
    elif pattern == "lure_short":
        candles[decision] = _bar(decision, "100", "100.5", "98", "98.5")
        candles[entry] = _bar(entry, "100", "101.5", "99.5", "101")


def _noisy_flat(count: int) -> list[SimCandle]:
    """Ряд без установок: відкриття 100 і дрібний детермінований шум (фічі флетових свічок різні)."""
    rng = random.Random(_SEED)  # noqa: S311 — детерміновані тестові дані, не криптографія
    candles: list[SimCandle] = []
    for index in range(count):
        up = Decimal(rng.randint(0, 100)) / 100
        down = Decimal(rng.randint(0, 100)) / 100
        drift = Decimal(rng.randint(-50, 50)) / 100
        high = Decimal(100) + max(up, drift, Decimal(0))
        low = Decimal(100) - max(down, -drift, Decimal(0))
        candles.append(_bar(index, "100", str(high), str(low), str(Decimal(100) + drift)))
    return candles


def _build(
    count: int,
    patterns: Sequence[str],
    *,
    noise: bool = True,
) -> tuple[tuple[SymbolHistory, ...], ResearchSplit]:
    candles = _noisy_flat(count) if noise else [_bar(index, "100", "101", "99", "100") for index in range(count)]
    for step, pattern in enumerate(patterns):
        _paint_setup(candles, _FIRST_ENTRY - 1 + step * _STEP, pattern)
    split = build_split(
        data_from_ms=0,
        data_to_ms=count * HOUR_MS,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        min_train_ms=_entry_index(60) * HOUR_MS,
        test_segment_ms=400 * HOUR_MS,
        test_segment_extended_ms=440 * HOUR_MS,
        min_trades_per_fold=1,
    )
    return (_history(BTC, candles),), split


def _three_folds(
    patterns: Sequence[str] | None = None,
    *,
    noise: bool = True,
) -> tuple[tuple[SymbolHistory, ...], ResearchSplit]:
    chosen = patterns if patterns is not None else [_PATTERNS[step % len(_PATTERNS)] for step in range(_SETUPS)]
    books, split = _build(6400, chosen, noise=noise)
    assert len(split.folds) == 3
    return books, split


def _four_folds() -> tuple[tuple[SymbolHistory, ...], ResearchSplit]:
    """Чотири фолди: два прогрівні й два оцінені — правило фолду 2 має пізніший фолд, який не сміє бачити."""
    books, split = _build(6700, [_PATTERNS[step % len(_PATTERNS)] for step in range(_SETUPS)])
    assert len(split.folds) == 4
    return books, split


def _two_folds() -> tuple[tuple[SymbolHistory, ...], ResearchSplit]:
    books, split = _build(5800, [_PATTERNS[step % len(_PATTERNS)] for step in range(_SETUPS)])
    assert len(split.folds) == 2
    return books, split


def _options(split: ResearchSplit) -> dict[str, Any]:
    return {
        "split": split,
        "trading_symbol": BTC,
        "limits": _wide_limits(),
        "initial_equity": _EQUITY,
        "maintenance_margin_rate": _MARGIN,
        "seed": _SEED,
        "costs": _zero_costs(),
        "params": _TARGET,
    }


def _train(books: tuple[SymbolHistory, ...], split: ResearchSplit, **overrides: Any) -> DirectRun:
    return train_direct(books, **{**_options(split), **overrides})


def _scripted_run(books: tuple[SymbolHistory, ...], split: ResearchSplit) -> DirectRun:
    """Прогін без навчання: у кожній установці тестового вікна рядок тієї сторони, що її показує установка.

    Ймовірність — 0.70 для справжньої установки і 0.55 для приманки, мітка — з бектесту цієї свічки.
    Приманки дають програшні рядки, тож калібратор і поріг мають з чого вчитись.
    """
    bars = _bars(books[0], split, _TARGET, _zero_costs())
    folds: list[DirectFold] = []
    for fold in split.folds:
        observations: list[OosObservation] = []
        for bar in bars:
            pattern = _pattern_at(bar.row.index)
            if pattern in (None, "flat") or bar.sample is None or bar.atr is None:
                continue
            if not fold.test_start_ms <= bar.row.time_ms < fold.test_end_ms:
                continue
            side = PositionSide.LONG if pattern in ("long", "lure_long") else PositionSide.SHORT
            trade = bar.sample.long_trade if side is PositionSide.LONG else bar.sample.short_trade
            observations.append(
                OosObservation(
                    symbol=bar.row.symbol,
                    time_ms=bar.row.time_ms,
                    index=bar.row.index,
                    fold_index=fold.index,
                    side=side,
                    raw_probability=Decimal("0.55") if pattern.startswith("lure") else Decimal("0.70"),
                    true_label=trade.meta_label,
                    entry=bar.open,
                    atr=bar.atr,
                )
            )
        folds.append(
            DirectFold(
                fold_index=fold.index,
                model=DirectFoldModel(
                    fold_index=fold.index,
                    feature_columns=(),
                    schema_hash="sha256:scripted",
                    preprocessing_version="scripted",
                    estimator=None,
                ),
                observations=tuple(observations),
                n_flat=0,
                train_labels=label_diagnostics(()),
                test_labels=label_diagnostics(()),
            )
        )
    common = _Backtest(
        limits=_wide_limits(),
        initial_equity=_EQUITY,
        maintenance_margin_rate=_MARGIN,
        seed=_SEED,
        costs=_zero_costs(),
        params=_TARGET,
    )
    return DirectRun(
        split=split,
        seed=_SEED,
        trading_symbol=BTC,
        shuffled_labels=False,
        common=common,
        folds=tuple(folds),
    )


def _observation_keys(run: DirectRun) -> list[tuple[int, str, int, PositionSide, Decimal]]:
    return [
        (fold.fold_index, row.symbol, row.time_ms, row.side, row.raw_probability)
        for fold in run.folds
        for row in fold.observations
    ]


class _FixedProbabilities:
    """Заглушка моделі: повертає задані ймовірності класів у порядку рядків матриці."""

    classes_ = np.array([0, 1, 2])

    def __init__(self, rows: Sequence[Sequence[float]]) -> None:
        self._rows = np.array(rows, dtype=np.float64)

    def predict_proba(self, matrix: np.ndarray) -> np.ndarray:
        assert len(matrix) == len(self._rows)
        return self._rows


def _sessions(conn: AsyncConnection) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")


def _iteration_one(next_step: str, *, hold_out_opened: bool = False) -> IterationOneReport:
    return IterationOneReport(
        selection=PrimarySelection(chosen=(BREAKOUT,), beats_flat=(True,)),
        comparison_trials=(),
        rules=(),
        next_step=next_step,
        hold_out_opened=hold_out_opened,
    )


def test_recipe_is_the_journal_lightgbm_with_three_classes_and_no_search() -> None:
    recipe = direct_recipe(shuffled_labels=False)
    assert recipe["stage"] == DIRECT_STAGE
    assert recipe["role"] == "direct"
    assert recipe["objective"] == "multiclass"
    assert recipe["classes"] == ["FLAT", "LONG", "SHORT"]
    assert recipe["lgbm"] == lgbm_journal_params()
    # Без ознак meta: інакше G1 сприйняв би спробу як meta-модель і вимагав би фільтра.
    assert "comparison" not in recipe
    with pytest.raises(ValueError, match="shuffled_labels"):
        direct_recipe(shuffled_labels=1)  # type: ignore[arg-type]


def test_a_tie_is_no_trade_and_a_unique_maximum_is_the_class() -> None:
    assert _winner((Decimal("0.4"), Decimal("0.4"), Decimal("0.2"))) is None
    assert _winner((Decimal("0.5"), Decimal("0.3"), Decimal("0.2"))) == 0
    assert _winner((Decimal("0.2"), Decimal("0.5"), Decimal("0.3"))) == 1
    assert _winner((Decimal("0.2"), Decimal("0.3"), Decimal("0.5"))) == 2
    assert [label.value for label in CLASS_LABELS] == ["FLAT", "LONG", "SHORT"]


def test_entry_row_takes_the_predicted_side_its_probability_and_that_sides_trade_label() -> None:
    books, split = _three_folds()
    bars = _bars(books[0], split, _TARGET, _zero_costs())
    series = _feature_series(books, None, split.holdout_from_ms)
    features = _features_for_fold(series, split, 0, {})
    columns = _Columns(features=features, lookup=_lookup(features))
    test_keys = {
        (row.symbol, row.index) for row in rows_for([bar.row for bar in bars], split, SplitPurpose.TEST, fold_index=0)
    }
    probe = [
        bar
        for bar in bars
        if (bar.row.symbol, bar.row.index) in test_keys
        and bar.atr is not None
        and _row_vector(columns, bar.row) is not None
    ][:12]
    assert len(probe) == 12
    # Цикл із чотирьох випадків: LONG, FLAT, SHORT і нічия FLAT/LONG (без угоди).
    cycle = [(0.20, 0.60, 0.20), (0.70, 0.20, 0.10), (0.30, 0.10, 0.60), (0.40, 0.40, 0.20)]
    model = DirectFoldModel(
        fold_index=0,
        feature_columns=features.columns,
        schema_hash=features.schema_hash,
        preprocessing_version=features.preprocessing_version,
        estimator=_FixedProbabilities([cycle[index % 4] for index in range(len(probe))]),
    )
    observations, n_flat = _test_observations(model, probe, columns, fold_index=0)
    expected: list[tuple[PositionSide, Decimal, int | None]] = []
    for index, bar in enumerate(probe):
        case = index % 4
        if case in (1, 3):  # FLAT і нічия — без угоди
            continue
        # Ймовірність вибраної сторони: 0.60 у кожному з двох випадків (стовпці LONG і SHORT циклу).
        side = PositionSide.LONG if case == 0 else PositionSide.SHORT
        label: int | None = None
        if bar.sample is not None:
            label = (bar.sample.long_trade if side is PositionSide.LONG else bar.sample.short_trade).meta_label
        expected.append((side, Decimal("0.60"), label))
    assert n_flat == 3
    assert [(item.side, item.raw_probability, item.true_label) for item in observations] == expected
    assert all(item.fold_index == 0 for item in observations)


def test_observations_are_test_rows_of_their_own_fold_and_never_touch_the_holdout() -> None:
    books, split = _three_folds()
    run = _train(books, split)
    for fold in run.folds:
        window = split.folds[fold.fold_index]
        assert all(window.test_start_ms <= row.time_ms < window.test_end_ms for row in fold.observations)
        assert all(row.time_ms < split.holdout_from_ms for row in fold.observations)
        assert all(row.true_label in (0, 1, None) for row in fold.observations)


def test_holdout_prices_never_reach_the_direct_model() -> None:
    """Зміна цін hold-out не змінює жодного рядка, ймовірності, мітки чи схеми фолдів (ТЗ §5А.10)."""
    books, split = _three_folds()
    before = _train(books, split)
    candles = list(books[0].candles)
    for index in range(split.holdout_from_ms // HOUR_MS, len(candles)):
        candles[index] = _bar(index, "100", "900", "5", "700")
    after = _train((_history(BTC, candles),), split)
    assert _observation_keys(before) == _observation_keys(after)
    assert [fold.train_labels for fold in before.folds] == [fold.train_labels for fold in after.folds]
    assert [fold.test_labels for fold in before.folds] == [fold.test_labels for fold in after.folds]
    assert [fold.model.schema_hash for fold in before.folds] == [fold.model.schema_hash for fold in after.folds]


def test_a_training_window_without_a_short_label_is_refused() -> None:
    books, split = _three_folds(["long", "flat", "lure_long"] * 170, noise=False)
    with pytest.raises(DirectTrainError, match="немає класу SHORT"):
        _train(books, split)


def test_the_target_must_match_the_split_horizon() -> None:
    books, split = _three_folds()
    with pytest.raises(ValueError, match="горизонт"):
        _train(books, split, params=TargetParams(horizon_bars=3))


def test_scripted_rule_has_an_entry_rule_per_fold_and_a_stress_per_fold() -> None:
    books, split = _three_folds()
    run = _scripted_run(books, split)
    evaluation = evaluate_direct(run, books)
    assert evaluation.usable, (evaluation.decision_error, evaluation.cross_error)
    assert len(evaluation.rules) == len(split.folds)
    assert len(evaluation.records) == len(split.folds)
    # Рішення 0022: фолди без минулих для калібрування (`< PAST_FOLDS_MIN`) правила не мають.
    assert [rule is None for rule in evaluation.rules] == [index < PAST_FOLDS_MIN for index in range(len(split.folds))]
    for record in evaluation.records:
        assert record.direct is not None
        assert record.direct.stress.cost_factor == Decimal(2)


def test_two_folds_leave_no_entry_rule_so_no_trade_is_taken() -> None:
    books, split = _two_folds()
    run = _scripted_run(books, split)
    evaluation = evaluate_direct(run, books)
    assert evaluation.usable is False
    assert evaluation.rules == ()
    assert evaluation.cross_error is not None
    for record in evaluation.records:
        assert all(report.metrics.n_trades == 0 for report in record.reports)
    params = direct_trial_params(run, evaluation)
    assert "entry_evaluation" not in params
    assert "cross_error" in params


def test_trial_params_carry_the_entry_rule_the_schema_and_no_meta_markers() -> None:
    books, split = _three_folds()
    run = _scripted_run(books, split)
    evaluation = evaluate_direct(run, books)
    params = direct_trial_params(run, evaluation)
    assert params["stage"] == DIRECT_STAGE
    assert params["role"] == "direct"
    assert "comparison" not in params
    assert params["entry_evaluation"] == ENTRY_EVALUATION_PAST_FOLDS
    assert evaluation.decision is not None
    assert params["decision"] == decision_payload(evaluation.decision.rule, evaluation.decision.calibrator)
    assert len(params["entry_rules"]) == len(split.folds)
    assert params["feature_schema_hash"] == "sha256:scripted"


def test_g1_for_direct_has_no_filter_condition_and_meta_still_needs_one() -> None:
    # Чотири фолди — мінімум оцінених фолдів G1 (рішення 0024).
    folds = tuple(
        FoldOutcome(
            n_trades=40,
            n_signals=60,
            total_net_usd=Decimal(100),
            profit_factor=Decimal("1.5"),
            below_min_trades=False,
            cost_factor=Decimal(1),
        )
        for _ in range(4)
    )
    stress = tuple(item.model_copy(update={"cost_factor": Decimal(2), "total_net_usd": Decimal(10)}) for item in folds)
    direct = evaluate_gate(
        label_type="direct",
        folds=folds,
        stress=stress,
        dsr=Decimal("0.99"),
        pbo=Decimal("0.1"),
        n_trials=5,
    )
    assert G1_FILTER not in direct.reasons
    assert direct.passed
    meta = evaluate_gate(
        label_type="meta",
        folds=folds,
        stress=stress,
        dsr=Decimal("0.99"),
        pbo=Decimal("0.1"),
        n_trials=5,
    )
    assert G1_FILTER in meta.reasons


@pytest.mark.asyncio
async def test_iteration_two_refuses_before_any_write_when_iteration_one_passed_or_opened() -> None:
    books, split = _two_folds()
    for after in (
        _iteration_one(WAIT_FOR_HUMAN_HOLDOUT),
        _iteration_one(ITERATION_TWO, hold_out_opened=True),
    ):
        with pytest.raises(ValueError, match="після ітерації 1"):
            await run_iteration_two(
                async_sessionmaker(),
                books,
                after=after,
                split=split,
                trading_symbol=BTC,
                limits=_wide_limits(),
                initial_equity=_EQUITY,
                maintenance_margin_rate=_MARGIN,
                seed=_SEED,
            )


def test_iteration_two_verdict_waits_for_the_human_or_changes_the_line() -> None:
    assert iteration_two_verdict(True) == WAIT_FOR_HUMAN_HOLDOUT
    assert iteration_two_verdict(False) == CHANGE_LINE_OR_TIMEFRAME


@pytest.mark.integration
@pytest.mark.asyncio
async def test_journal_stores_a_scripted_rule_and_g1_reads_the_stress_from_it(conn: AsyncConnection) -> None:
    books, split = _three_folds()
    run = _scripted_run(books, split)
    evaluation = evaluate_direct(run, books)
    async with AsyncSession(bind=conn) as session:
        experiment_id = await create_experiment(
            session,
            split=split,
            research_line=BTC,
            symbols=(BTC,),
            label_type="direct",
            primary_strategy={},
            params=direct_recipe(shuffled_labels=False),
            seed=_SEED,
        )
        trial = await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="COMPLETE",
            params=direct_trial_params(run, evaluation),
            folds=evaluation.records,
        )
        stored = (
            await session.execute(select(ExperimentTrial).where(ExperimentTrial.id == trial.trial_id))
        ).scalar_one()
        preview = await preview_gate_g1(session, experiment_id=experiment_id, trial_id=trial.trial_id)
        forged = tuple(
            FoldOutcome(
                n_trades=0,
                n_signals=0,
                total_net_usd=Decimal(1),
                profit_factor=None,
                below_min_trades=False,
                cost_factor=Decimal(2),
            )
            for _ in split.folds
        )
        refused = await preview_gate_g1(
            session,
            experiment_id=experiment_id,
            trial_id=trial.trial_id,
            stress=forged,
        )
    assert stored.params["entry_evaluation"] == ENTRY_EVALUATION_PAST_FOLDS
    for fold in stored.metrics["folds"]:
        assert Decimal(str(fold["direct"]["stress"]["cost_factor"])) == Decimal(2)
    assert G1_ENTRY_RULE not in preview.decision.reasons
    assert G1_FILTER not in preview.decision.reasons
    # Сценарний прогін не обов'язково проходить економіку стресу; перевіряємо механізм: підмінений стрес
    # додає G1_STRESS і більше нічого не змінює порівняно з записаним стресом журналу.
    assert G1_STRESS in refused.decision.reasons
    assert set(refused.decision.reasons) - {G1_STRESS} == set(preview.decision.reasons) - {G1_STRESS}
    assert refused.decision.passed is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trained_trial_is_a_complete_direct_trial_with_its_schema(conn: AsyncConnection) -> None:
    books, split = _three_folds()
    async with AsyncSession(bind=conn) as session:
        recorded = await run_direct_experiment(session, books, **_options(split))
        experiment = (
            await session.execute(select(Experiment).where(Experiment.id == recorded.experiment_id))
        ).scalar_one()
        trial = (
            await session.execute(select(ExperimentTrial).where(ExperimentTrial.id == recorded.trial.trial_id))
        ).scalar_one()
    assert experiment.label_type == "direct"
    assert experiment.primary_strategy == {}
    assert trial.state == "COMPLETE"
    assert trial.params["stage"] == DIRECT_STAGE
    assert trial.params["feature_schema_hash"] == recorded.run.folds[-1].model.schema_hash
    assert trial.params["feature_schema_hashes"] == [fold.model.schema_hash for fold in recorded.run.folds]
    assert len(trial.oos_results) == len(trial.oos_entry_ms)
    # Правило входу записане тоді і лише тоді, коли його можна було підігнати (fail-closed для G1).
    assert ("entry_evaluation" in trial.params) == recorded.evaluation.usable
    assert ("decision_error" in trial.params or "cross_error" in trial.params) != recorded.evaluation.usable
    for fold in trial.metrics["folds"]:
        assert Decimal(str(fold["direct"]["stress"]["cost_factor"])) == Decimal(2)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_failed_entry_rule_is_still_a_trial_and_the_error_propagates(
    conn: AsyncConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    books, split = _three_folds()

    def _broken(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("правило впало")

    monkeypatch.setattr(direct_model, "evaluate_direct", _broken)
    async with AsyncSession(bind=conn) as session:
        with pytest.raises(RuntimeError, match="правило впало") as raised:
            await run_direct_experiment(session, books, **_options(split))
        await session.commit()
        trials = (await session.execute(select(ExperimentTrial))).scalars().all()
    assert any("experiment_id=" in note for note in raised.value.__notes__)
    failed = [trial for trial in trials if trial.state == "FAILED"]
    assert len(failed) == 1
    assert failed[0].params["failed_at"] == "entry_rule"
    assert failed[0].params["stage"] == DIRECT_STAGE


@pytest.mark.integration
@pytest.mark.asyncio
async def test_a_training_failure_is_a_failed_trial_with_no_folds(conn: AsyncConnection) -> None:
    books, split = _three_folds(["long", "flat", "lure_long"] * 170, noise=False)
    async with AsyncSession(bind=conn) as session:
        with pytest.raises(DirectTrainError) as raised:
            await run_direct_experiment(session, books, **_options(split))
        await session.commit()
        trial = (await session.execute(select(ExperimentTrial))).scalar_one()
    assert raised.value.experiment_id is not None
    assert trial.state == "FAILED"
    assert trial.oos_results == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iteration_two_records_one_trial_and_never_opens_the_holdout(conn: AsyncConnection) -> None:
    books, split = _three_folds()
    sessions = _sessions(conn)
    report = await run_iteration_two(
        sessions,
        books,
        after=_iteration_one(ITERATION_TWO),
        split=split,
        trading_symbol=BTC,
        limits=_wide_limits(),
        initial_equity=_EQUITY,
        maintenance_margin_rate=_MARGIN,
        seed=_SEED,
        costs=_zero_costs(),
        params=_TARGET,
    )
    async with sessions() as session:
        openings = (await session.execute(select(func.count()).select_from(HoldoutOpening))).scalar_one()
        trials = (await session.execute(select(func.count()).select_from(ExperimentTrial))).scalar_one()
    assert openings == 0
    assert trials == 1
    assert report.hold_out_opened is False
    assert report.experiment.spec_name == "direct_3class"
    assert report.next_step == iteration_two_verdict(report.experiment.passed)


def test_fold_rows_are_test_rows_only() -> None:
    """Рядки, які бачить правило входу, — лише тестові рядки свого фолду (`rows_for` TEST)."""
    books, split = _three_folds()
    run = _scripted_run(books, split)
    for fold in run.folds:
        rows = [SampleRow(symbol=row.symbol, time_ms=row.time_ms, index=row.index) for row in fold.observations]
        kept = rows_for(rows, split, SplitPurpose.TEST, fold_index=fold.fold_index)
        assert len(kept) == len(rows)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_any_training_exception_is_a_failed_trial_not_a_lost_attempt(
    conn: AsyncConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Будь-який збій навчання йде в журнал як спроба `FAILED` (N лінії не має зникати), далі — виняток."""
    books, split = _three_folds()

    def _broken(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("навчання впало")

    monkeypatch.setattr(direct_model, "train_direct", _broken)
    async with AsyncSession(bind=conn) as session:
        with pytest.raises(RuntimeError, match="навчання впало") as raised:
            await run_direct_experiment(session, books, **_options(split))
        await session.commit()
        trials = (await session.execute(select(ExperimentTrial))).scalars().all()
    assert any("experiment_id=" in note for note in raised.value.__notes__)
    assert [trial.state for trial in trials] == ["FAILED"]
    assert trials[0].params["failed_at"] == "train"
    assert trials[0].oos_results == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iteration_two_refuses_a_split_other_than_iteration_one(conn: AsyncConnection) -> None:
    """Рев'ю T2.13: ітерація 2 — на тих самих фолдах, що й ітерація 1; інша розбивка — відмова без запису."""
    books_a, split_a = _three_folds()
    _books_b, split_b = _two_folds()
    async with AsyncSession(bind=conn) as session:
        experiment_id = await create_experiment(
            session,
            split=split_a,
            research_line=BTC,
            symbols=(BTC,),
            label_type="direct",
            primary_strategy={},
            params=direct_recipe(shuffled_labels=False),
            seed=_SEED,
        )
        await session.flush()
    after = IterationOneReport(
        selection=PrimarySelection(chosen=(BREAKOUT,), beats_flat=(True,)),
        comparison_trials=(RecordedTrial(experiment_id=experiment_id, trial_id=0, trial_no=1, line_trial_no=1),),
        rules=(),
        next_step=ITERATION_TWO,
    )
    sessions = _sessions(conn)
    with pytest.raises(ValueError, match="розбивка"):
        await run_iteration_two(
            sessions,
            books_a,
            after=after,
            split=split_b,
            trading_symbol=BTC,
            limits=_wide_limits(),
            initial_equity=_EQUITY,
            maintenance_margin_rate=_MARGIN,
            seed=_SEED,
            costs=_zero_costs(),
            params=_TARGET,
        )
    async with sessions() as session:
        trials = (await session.execute(select(func.count()).select_from(ExperimentTrial))).scalar_one()
    assert trials == 0


def test_shuffled_labels_leave_no_probability_gap() -> None:
    """ТЗ §5А.9: на перемішаних мітках прогалина ймовірностей близька до нуля (мітка не була у фічах)."""
    books, split = _three_folds()
    shuffled = _train(books, split, shuffled_labels=True)
    gap = direct_probability_gap(shuffled)
    assert gap is not None
    assert abs(gap) < Decimal("0.05"), gap


def test_probability_gap_is_positive_when_the_probabilities_track_the_labels() -> None:
    """Той самий міжрядковий показник на сценарних рядках: справжні TP мають вищу ймовірність, ніж решта."""
    books, split = _three_folds()
    gap = direct_probability_gap(_scripted_run(books, split))
    assert gap is not None
    assert gap > Decimal("0.1"), gap


def test_two_symbols_share_the_folds_and_the_journal_keeps_both_reports() -> None:
    """Багатосимвольна вибірка: ETH і BTC на тих самих фолдах, стрес — лише торгового символу."""
    btc_books, split = _three_folds()
    books = (btc_books[0], _history(ETH, list(btc_books[0].candles)))
    run = _train(books, split)
    symbols = {row.symbol for fold in run.folds for row in fold.observations}
    assert symbols <= {BTC, ETH}
    for fold in run.folds:
        window = split.folds[fold.fold_index]
        assert all(window.test_start_ms <= row.time_ms < window.test_end_ms for row in fold.observations)
    evaluation = evaluate_direct(run, books)
    for record in evaluation.records:
        assert {report.symbol for report in record.reports} == {BTC, ETH}
        assert record.direct is not None
        assert record.direct.stress.symbol == BTC


@pytest.mark.parametrize("setup", [_three_folds, _four_folds], ids=["3-folds", "4-folds"])
def test_flipping_the_labels_of_a_fold_never_changes_a_rule_fit_before_it(
    setup: Callable[[], tuple[tuple[SymbolHistory, ...], ResearchSplit]],
) -> None:
    """Рішення 0022, 0024: правило фолду k бачить лише фолди `< k`. Мітки фолду j ≥ k його не чіпають; j < k — можуть.

    На трьох фолдах оцінюється лише останній, тож відмінність від старої перехресної оцінки видно лише з чотирма:
    мітки фолду 3 не сміють змінити правило фолду 2.
    """
    books, split = setup()
    base_run = _scripted_run(books, split)
    base = evaluate_direct(base_run, books)
    assert base.usable
    base_payloads = [None if fit is None else decision_payload(fit.rule, fit.calibrator) for fit in base.rules]
    for fold_index in range(len(split.folds)):
        flipped_folds = []
        for fold in base_run.folds:
            if fold.fold_index != fold_index:
                flipped_folds.append(fold)
                continue
            flipped = tuple(
                row.model_copy(update={"true_label": None if row.true_label is None else 1 - row.true_label})
                for row in fold.observations
            )
            flipped_folds.append(dataclasses.replace(fold, observations=flipped))
        flipped_run = dataclasses.replace(base_run, folds=tuple(flipped_folds))
        after = evaluate_direct(flipped_run, books)
        assert after.usable
        after_payloads = [None if fit is None else decision_payload(fit.rule, fit.calibrator) for fit in after.rules]
        for rule_index in range(len(split.folds)):
            if rule_index <= fold_index:
                assert after_payloads[rule_index] == base_payloads[rule_index]
        if any(
            rule_index > fold_index for rule_index in range(len(split.folds)) if base_payloads[rule_index] is not None
        ):
            assert any(
                after_payloads[rule_index] != base_payloads[rule_index]
                for rule_index in range(len(split.folds))
                if rule_index > fold_index and base_payloads[rule_index] is not None
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_journal_keeps_a_report_for_every_symbol_of_the_experiment(conn: AsyncConnection) -> None:
    """Багатосимвольний експеримент direct: журнал несе прогін кожного символу у фолді (`all_symbols`)."""
    btc_books, split = _three_folds()
    books = (btc_books[0], _history(ETH, list(btc_books[0].candles)))
    async with AsyncSession(bind=conn) as session:
        recorded = await run_direct_experiment(session, books, **_options(split))
        trial = (
            await session.execute(select(ExperimentTrial).where(ExperimentTrial.id == recorded.trial.trial_id))
        ).scalar_one()
    assert trial.state == "COMPLETE"
    for fold in trial.metrics["folds"]:
        symbols = {entry["symbol"] for entry in fold["all_symbols"]["per_symbol"]}
        assert symbols == {BTC, ETH}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iteration_two_runs_once_per_folds_target_and_costs(conn: AsyncConnection) -> None:
    """Рев'ю T2.20: повтор ітерації 2 на тих самих фолдах (інший seed) — прихований вибір; відмова без запису."""
    from app.ml.research_cycle import JournalMismatch, direct_runs_on_same_folds
    from app.ml.validation import costs_payload, target_payload

    books, split = _three_folds()
    async with AsyncSession(bind=conn) as session:
        earlier = await create_experiment(
            session,
            split=split,
            research_line=BTC,
            symbols=(BTC,),
            label_type="direct",
            primary_strategy={},
            params={
                **direct_recipe(shuffled_labels=False),
                "target": target_payload(_TARGET),
                "costs": costs_payload(_zero_costs()),
            },
            seed=_SEED + 1,
        )
        await session.flush()
        assert await direct_runs_on_same_folds(
            session, split=split, trading_symbol=BTC, target=_TARGET, costs=_zero_costs()
        ) == (earlier,)
        # Інший таргет — не той самий прогін.
        assert (
            await direct_runs_on_same_folds(
                session,
                split=split,
                trading_symbol=BTC,
                target=TargetParams(horizon_bars=_TARGET.horizon_bars, tp_atr_mult=Decimal(3)),
                costs=_zero_costs(),
            )
            == ()
        )
    sessions = _sessions(conn)
    with pytest.raises(JournalMismatch, match="уже прогнано") as refused:
        await run_iteration_two(
            sessions,
            books,
            after=_iteration_one(ITERATION_TWO),
            split=split,
            trading_symbol=BTC,
            limits=_wide_limits(),
            initial_equity=_EQUITY,
            maintenance_margin_rate=_MARGIN,
            seed=_SEED,
            costs=_zero_costs(),
            params=_TARGET,
        )
    assert refused.value.kind == "repeated"
    async with sessions() as session:
        trials = (await session.execute(select(func.count()).select_from(ExperimentTrial))).scalar_one()
    assert trials == 0
