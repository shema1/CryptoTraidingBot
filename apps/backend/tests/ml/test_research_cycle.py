"""Дослідницький цикл, ітерація 1 (T2.12): правило входу і схема фіч у спробі G1, перегляд без hold-out.

Ітерація 1 — meta-labeling з 1–2 первинними стратегіями на walk-forward. Цикл сам hold-out не відкриває:
він зупиняється на рішенні людини (`open_holdout_after_gate`), навіть коли перегляд G1 пройшов.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from app.ml import meta_model, research_cycle
from app.ml.artifacts import (
    GitState,
    _recipe_matches_trial,
    booster_of,
    build_metadata,
    current_library_versions,
    decision_payload,
    register_model,
    write_artifact,
)
from app.ml.decision import (
    ENTER,
    ENTRY_EVALUATION_PAST_FOLDS,
    ENTRY_RULE_WARMUP_FOLDS,
    CalibrationError,
    decide_entry,
    fit_decision,
    past_fold_pool,
)
from app.ml.features import HOUR_MS
from app.ml.meta_model import (
    META_MIN_PROBABILITY,
    MetaRun,
    decision_observations,
    fit_entry_rule,
    fit_frozen_meta,
    g1_recipe_fields,
    past_fold_entry,
    recipe_trial,
    run_meta_experiment,
)
from app.ml.models import Experiment, ExperimentTrial, HoldoutOpening, ModelArtifact
from app.ml.research_cycle import (
    ITERATION_TWO,
    WAIT_FOR_HUMAN_HOLDOUT,
    IterationOneReport,
    RuleResult,
    iteration_one_verdict,
    run_iteration_one,
)
from app.ml.statistics import (
    G1_ENTRY_RULE,
    G1Preview,
    GateDecision,
    GateRejected,
    GateThresholds,
    entry_rule_evaluated,
    fold_outcomes_from_metrics,
    open_holdout_after_gate,
    preview_gate_g1,
)
from app.ml.strategy import BREAKOUT, PrimarySelection, StrategyComparison, SymbolHistory
from app.ml.trade_sim import TargetParams
from app.ml.validation import (
    ResearchSplit,
    SplitLeakError,
    build_split,
    costs_payload,
    line_trial_count,
    target_payload,
)
from app.risk.costs import CostModel
from tests.db.factories import make_model
from tests.ml.test_meta import (
    _EQUITY,
    _MARGIN,
    _SEED,
    BTC,
    _entry_index,
    _flat,
    _history,
    _learning_books,
    _paint,
    _train,
    _wide_limits,
    _zero_costs,
)


def test_observations_copy_the_scored_rows_of_every_fold() -> None:
    _candles, books, split = _learning_books()
    run = _train(books, split)
    rows = decision_observations(run, books)
    scored = [(fold.fold_index, row) for fold in run.folds for row in fold.scored]
    assert scored, "тест має оцінити хоча б одного кандидата"
    assert len(rows) == len(scored)
    by_key = {(row.symbol, row.time_ms): row for row in rows}
    assert len(by_key) == len(rows)
    candles = books[0].candles
    for fold_index, scored_row in scored:
        observation = by_key[(scored_row.symbol, scored_row.time_ms)]
        assert observation.fold_index == fold_index
        assert observation.index == scored_row.entry_index
        assert observation.side is scored_row.side
        assert observation.raw_probability == scored_row.probability
        assert observation.true_label == scored_row.true_label
        # Ціна входу — open свічки входу; ATR — ATR(t−1) того самого ряду, без майбутніх свічок.
        assert observation.entry == candles[scored_row.entry_index].open
        assert observation.atr > 0


def test_recipe_pins_the_decision_and_the_last_fold_schema() -> None:
    books, split = _two_fold_setup()
    run = _train(books, split)
    target = TargetParams(horizon_bars=4)
    fit = fit_decision(decision_observations(run, books), split, costs=_zero_costs(), params=target)
    fields = g1_recipe_fields(run, fit)
    assert fields["decision"] == decision_payload(fit.rule, fit.calibrator)
    assert fields["feature_schema_hashes"] == [fold.model.schema_hash for fold in run.folds]
    # Схема рецепта — схема останнього фолду (найбільше покриття train). Див. docs/decisions/0019.
    assert fields["feature_schema_hash"] == run.folds[-1].model.schema_hash


def test_missing_labels_leave_no_rule_and_say_why() -> None:
    # Без out-of-sample рядків калібрувати нічого: правило не вигадується, а причина йде в журнал.
    _candles, books, split = _learning_books()
    empty = MetaRun(
        spec=BREAKOUT,
        split=split,
        seed=_SEED,
        trading_symbol=BTC,
        shuffled_labels=False,
        min_probability=META_MIN_PROBABILITY,
        folds=(),
    )
    fit, error = fit_entry_rule(empty, books, split=split, costs=_zero_costs(), params=TargetParams(horizon_bars=4))
    assert fit is None
    assert error


def test_no_passing_rule_moves_on_to_iteration_two() -> None:
    assert iteration_one_verdict(()) == ITERATION_TWO
    assert iteration_one_verdict((False, False)) == ITERATION_TWO
    assert iteration_one_verdict((False, True)) == WAIT_FOR_HUMAN_HOLDOUT


def test_a_passing_rule_still_waits_for_the_human() -> None:
    # Пройти перегляд G1 не означає відкрити hold-out: рішення приймає людина.
    assert iteration_one_verdict((True,)) == WAIT_FOR_HUMAN_HOLDOUT


def _passed_preview() -> G1Preview:
    decision = GateDecision(
        passed=True,
        reasons=(),
        dsr=Decimal("0.99"),
        pbo=Decimal("0.1"),
        n_trials=3,
        n_trades=150,
        median_pf=Decimal("1.4"),
        median_pf_unbounded=False,
        profitable_fold_share=Decimal("0.8"),
    )
    return G1Preview(trial_id=1, decision=decision, dsr=decision.dsr, pbo=decision.pbo, n_trials=3)


def test_rule_without_entry_rule_never_passes_even_when_g1_does() -> None:
    # G1 на walk-forward пройдено, але правила входу немає: допуск його все одно відхилить (fail-closed).
    preview = _passed_preview()
    fitted = RuleResult(spec_name="breakout", experiment_id="e", trial_id=1, decision_error=None, preview=preview)
    missing = RuleResult(
        spec_name="breakout",
        experiment_id="e",
        trial_id=1,
        decision_error="калібрування потребує щонайменше два фолди walk-forward",
        preview=preview,
    )
    assert fitted.passed is True
    assert missing.passed is False
    assert iteration_one_verdict((missing.passed,)) == ITERATION_TWO


def test_rule_passing_only_loosened_thresholds_does_not_wait_for_the_holdout() -> None:
    # CANDIDATE вимагає порогів ТЗ: відкрити hold-out під послаблені пороги — витратити відкриття даремно.
    loosened = dataclasses.replace(_passed_preview(), loosened=("dsr_min",))
    rule = RuleResult(spec_name="breakout", experiment_id="e", trial_id=1, decision_error=None, preview=loosened)
    assert rule.passed is False
    assert iteration_one_verdict((rule.passed,)) == ITERATION_TWO


def _two_fold_setup() -> tuple[tuple[SymbolHistory, ...], ResearchSplit]:
    """Два фолди walk-forward: калібрування вимагає щонайменше два (`decision.py`). Сигнали чергуються TP/SL."""
    count = 5800
    candles = _flat(count)
    _paint(candles, 80, good=lambda step: step % 2 == 0)
    books = (_history(BTC, candles),)
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
    assert len(split.folds) == 2
    return books, split


def _three_fold_setup() -> tuple[tuple[SymbolHistory, ...], ResearchSplit]:
    """Три фолди: два прогрівні й один, чиє правило входу калібрується на двох минулих (рішення 0024)."""
    count = 6400
    candles = _flat(count)
    _paint(candles, 100, good=lambda step: step % 2 == 0)
    books = (_history(BTC, candles),)
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
    assert len(split.folds) == 3
    return books, split


def test_past_fold_entry_takes_what_decide_entry_takes_with_a_rule_from_past_folds() -> None:
    # G1 має бачити правило рецепта (`decide_entry`), а не keep(0.5). Правило фолду k — лише з фолдів < k
    # (рішення 0024): фолди 0 і 1 — прогрів калібрування, без правила і без угод.
    books, split = _three_fold_setup()
    run = _train(books, split)
    evaluated = past_fold_entry(run, books)
    rows = decision_observations(run, books)
    target = TargetParams(horizon_bars=4)
    assert len(evaluated.entry_rules) == len(run.folds)
    for fold, before, fit in zip(evaluated.folds, run.folds, evaluated.entry_rules, strict=True):
        assert [(row.symbol, row.time_ms) for row in fold.scored] == [
            (row.symbol, row.time_ms) for row in before.scored
        ]
        assert fold.record.meta is not None
        assert before.record.meta is not None
        # Правило без фільтра від рішення не залежить і лишається як було — і на прогріві теж.
        assert fold.record.meta.unfiltered == before.record.meta.unfiltered
        assert fold.record.meta.scored_unfiltered == before.record.meta.scored_unfiltered
        if fold.fold_index < ENTRY_RULE_WARMUP_FOLDS:
            assert fit is None
            assert not any(row.taken for row in fold.scored)
            assert all(report.metrics.n_trades == 0 for report in fold.record.reports)
            assert fold.stress.n_signals == 0
            continue
        assert fit is not None
        pool = past_fold_pool(rows, split, fold.fold_index)
        assert {row.fold_index for row in pool} <= set(range(fold.fold_index))
        expected = fit_decision(pool, split, costs=_zero_costs(), params=target)
        assert fit.rule == expected.rule
        assert decision_payload(fit.rule, fit.calibrator) == decision_payload(expected.rule, expected.calibrator)
        own = {(row.symbol, row.time_ms): row for row in rows if row.fold_index == fold.fold_index}
        for row in fold.scored:
            observation = own[(row.symbol, row.time_ms)]
            verdict = decide_entry(
                raw_probability=observation.raw_probability,
                side=observation.side,
                entry=observation.entry,
                atr=observation.atr,
                costs=_zero_costs(),
                params=target,
                calibrator=fit.calibrator,
                rule=fit.rule,
            )
            assert row.taken is (verdict.decision == ENTER)
            assert row.probability == observation.raw_probability
        # Стрес — ті самі сигнали, що прийняв фільтр.
        assert fold.stress.n_signals == sum(1 for row in fold.scored if row.symbol == BTC and row.taken)


def _flip_labels(run: MetaRun, fold_index: int) -> MetaRun:
    folds = list(run.folds)
    target = folds[fold_index]
    folds[fold_index] = dataclasses.replace(
        target,
        scored=tuple(
            row.model_copy(update={"true_label": None if row.true_label is None else 1 - row.true_label})
            for row in target.scored
        ),
    )
    return dataclasses.replace(run, folds=tuple(folds))


def _four_fold_setup() -> tuple[tuple[SymbolHistory, ...], ResearchSplit]:
    """Чотири фолди: два прогрівні, правило фолду 2 має пізніший фолд 3, якого не сміє бачити."""
    count = 6700
    candles = _flat(count)
    _paint(candles, 100, good=lambda step: step % 2 == 0)
    books = (_history(BTC, candles),)
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
    assert len(split.folds) == 4
    return books, split


def test_a_rule_ignores_every_later_fold() -> None:
    """Відмінність від старої перехресної оцінки (0020): мітки фолду 3 не змінюють правило фолду 2 (рішення 0024)."""
    books, split = _four_fold_setup()
    run = _train(books, split)
    base = past_fold_entry(run, books).entry_rules
    later = past_fold_entry(_flip_labels(run, 3), books).entry_rules
    assert base[2] is not None
    assert later[2] is not None
    assert decision_payload(later[2].rule, later[2].calibrator) == decision_payload(base[2].rule, base[2].calibrator)
    # Тест не порожній: правило фолду 3 підігнане й на фолді 2, тож мітки фолду 2 його змінюють.
    earlier = past_fold_entry(_flip_labels(run, 2), books).entry_rules
    assert base[3] is not None
    assert earlier[3] is not None
    assert decision_payload(earlier[3].rule, earlier[3].calibrator) != decision_payload(
        base[3].rule, base[3].calibrator
    )


def test_past_fold_rule_never_sees_its_own_or_later_fold_labels() -> None:
    # Ні прямого, ні опосередкованого витоку з майбутнього: мітки фолду ≥ k у правило k не входять.
    books, split = _three_fold_setup()
    run = _train(books, split)
    last = len(run.folds) - 1
    before = past_fold_entry(run, books).entry_rules[last]
    after_own = past_fold_entry(_flip_labels(run, last), books).entry_rules[last]
    assert before is not None
    assert after_own is not None
    assert decision_payload(after_own.rule, after_own.calibrator) == decision_payload(before.rule, before.calibrator)
    # Тест не порожній: мітки минулого фолду правило змінюють.
    after_past = past_fold_entry(_flip_labels(run, last - 1), books).entry_rules[last]
    assert after_past is not None
    assert decision_payload(after_past.rule, after_past.calibrator) != decision_payload(before.rule, before.calibrator)


def test_past_fold_entry_needs_three_folds() -> None:
    # Два фолди — обидва прогрівні: оцінювати правило нема на чому.
    books, split = _two_fold_setup()
    run = _train(books, split)
    with pytest.raises(CalibrationError, match="минулих фолдів"):
        past_fold_entry(run, books)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trial_stores_the_entry_rule_and_the_feature_schema(conn: AsyncConnection) -> None:
    books, split = _three_fold_setup()
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
        trial = (
            await session.execute(select(ExperimentTrial).where(ExperimentTrial.id == recorded.trial.trial_id))
        ).scalar_one()
    assert recorded.decision is not None
    assert trial.params["decision"] == decision_payload(recorded.decision.rule, recorded.decision.calibrator)
    assert trial.params["feature_schema_hash"] == recorded.run.folds[-1].model.schema_hash


@pytest.mark.integration
@pytest.mark.asyncio
async def test_preview_matches_the_refusal_and_writes_nothing(conn: AsyncConnection) -> None:
    books, split = _two_fold_setup()
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
        openings_before = (await session.execute(select(func.count()).select_from(HoldoutOpening))).scalar_one()
        preview = await preview_gate_g1(
            session,
            experiment_id=recorded.experiment_id,
            trial_id=recorded.trial.trial_id,
            stress=recorded.run.stress,
        )
        openings_after = (await session.execute(select(func.count()).select_from(HoldoutOpening))).scalar_one()
        assert openings_before == openings_after
        assert preview.loosened == ()
        loosened = await preview_gate_g1(
            session,
            experiment_id=recorded.experiment_id,
            trial_id=recorded.trial.trial_id,
            stress=recorded.run.stress,
            thresholds=GateThresholds(dsr_min=Decimal("0.5"), pbo_max=Decimal("0.9")),
        )
        # Перегляд називає послаблені пороги так само, як запис відкриття (`loosened_thresholds`).
        assert loosened.loosened == ("dsr_min", "pbo_max")
        model = await make_model(conn, experiment_id=recorded.experiment_id)
        with pytest.raises(GateRejected) as caught:
            await open_holdout_after_gate(
                session,
                experiment_id=recorded.experiment_id,
                trial_id=recorded.trial.trial_id,
                model_version=model,
                stress=recorded.run.stress,
            )
    # Перегляд і відмова відкриття рахують G1 однаково і не лишають слідів у журналі.
    assert preview.decision == caught.value.decision


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iteration_one_never_opens_the_holdout(conn: AsyncConnection) -> None:
    books, split = _two_fold_setup()
    sessions = _sessions(conn)
    report = await _cycle(sessions, books, split)
    async with sessions() as session:
        openings = (await session.execute(select(func.count()).select_from(HoldoutOpening))).scalar_one()
    assert openings == 0
    assert report.hold_out_opened is False
    assert report.selection.chosen
    assert report.next_step == iteration_one_verdict(tuple(item.passed for item in report.rules))


_TARGET = TargetParams(horizon_bars=4)


def _sessions(conn: AsyncConnection) -> async_sessionmaker[AsyncSession]:
    """Кожна сесія — savepoint тестового з'єднання: `commit` фіксує її, закриття без нього — відкочує."""
    return async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")


def _two_rules(_comparison: StrategyComparison) -> PrimarySelection:
    return PrimarySelection(chosen=(BREAKOUT, BREAKOUT), beats_flat=(False, False))


async def _cycle(
    sessions: async_sessionmaker[AsyncSession],
    books: tuple[SymbolHistory, ...],
    split: ResearchSplit,
    *,
    params: TargetParams | None = _TARGET,
) -> IterationOneReport:
    return await run_iteration_one(
        sessions,
        books,
        split=split,
        trading_symbol=BTC,
        limits=_wide_limits(),
        initial_equity=_EQUITY,
        maintenance_margin_rate=_MARGIN,
        seed=_SEED,
        costs=_zero_costs(),
        params=params,
    )


async def _record(session: AsyncSession, books: tuple[SymbolHistory, ...], split: ResearchSplit) -> Any:
    return await run_meta_experiment(
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admission_model_recipe_matches_the_recorded_g1_trial(conn: AsyncConnection, tmp_path: Path) -> None:
    # Головне твердження T2.12: спроба, яку пише `run_meta_experiment`, — рецепт, який допуск приймає.
    # Модель допуску — справжній `fit_frozen_meta` до hold-out з правилом входу і калібратором спроби.
    books, split = _three_fold_setup()
    target = TargetParams(horizon_bars=4)
    version = "lgbm-meta-2026-11-04-001"
    async with AsyncSession(bind=conn) as session:
        recorded = await _record(session, books, split)
        assert recorded.decision is not None
        fit = fit_frozen_meta(
            books, spec=BREAKOUT, split=split, trading_symbol=BTC, seed=_SEED, costs=_zero_costs(), params=target
        )
        meta = build_metadata(
            model_version=version,
            label_type="meta",
            spec=BREAKOUT,
            trade_symbol=BTC,
            train_symbols=(BTC,),
            feature_columns=fit.model.feature_columns,
            target=target,
            costs=_zero_costs(),
            rule=recorded.decision.rule,
            experiment_id=recorded.experiment_id,
            seed=_SEED,
            data_hash="sha256:" + "ef" * 32,
            git=GitState(commit="abc123", dirty=False),
            library_versions=current_library_versions(),
            train_range_ms=(split.data_from_ms, split.holdout_from_ms),
            recipe_lgbm=fit.lgbm,
            coverage_ms=fit.coverage_ms,
            train_rows=fit.n_rows,
            include_holdout=False,
            calibrator_method=recorded.decision.method,
        )
        write_artifact(tmp_path / version, meta, booster=booster_of(fit.model), calibrator=recorded.decision.calibrator)
        evidence = await register_model(session, tmp_path / version)
        model = await session.get(ModelArtifact, evidence)
        trial = await session.get(ExperimentTrial, recorded.trial.trial_id)
    assert model is not None
    assert trial is not None
    assert trial.params["feature_schema_hash"] == model.metadata_json["feature_schema_hash"]
    # Не кидає: правило входу (decision_rule, sha256 калібратора), схема фіч і LightGBM — ті самі.
    _recipe_matches_trial(model.metadata_json, trial)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unfitted_entry_rule_is_recorded_with_its_reason(conn: AsyncConnection) -> None:
    # Один фолд walk-forward: калібрувати нічим. Спроба все одно в журналі, але без `decision`.
    _candles, books, split = _learning_books()
    assert len(split.folds) == 1
    async with AsyncSession(bind=conn) as session:
        recorded = await _record(session, books, split)
        trial = await session.get(ExperimentTrial, recorded.trial.trial_id)
    assert recorded.decision is None
    assert recorded.decision_error == "калібрування потребує щонайменше два фолди walk-forward"
    assert trial is not None
    assert trial.state == "COMPLETE"
    assert "decision" not in trial.params
    assert trial.params["decision_error"] == recorded.decision_error


@pytest.mark.integration
@pytest.mark.asyncio
async def test_failure_after_training_is_still_a_trial(conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch) -> None:
    # Модель уже навчена: збій підгонки правила входу не може сховати спробу з журналу (N росте).
    books, split = _two_fold_setup()

    def leak(*_args: Any, **_kwargs: Any) -> Any:
        raise SplitLeakError("рядок hold-out у вибірці порогу")

    monkeypatch.setattr(meta_model, "fit_entry_rule", leak)
    async with AsyncSession(bind=conn) as session:
        with pytest.raises(SplitLeakError):
            await _record(session, books, split)
        trials = (await session.execute(select(ExperimentTrial))).scalars().all()
        line = await line_trial_count(session, BTC)
    assert line == 1
    assert len(trials) == 1
    assert trials[0].state == "FAILED"
    assert trials[0].params["failed_at"] == "entry_rule"
    assert trials[0].params["error"] == "SplitLeakError: рядок hold-out у вибірці порогу"
    assert "decision" not in trials[0].params


@pytest.mark.integration
@pytest.mark.asyncio
async def test_every_rule_is_previewed_against_the_whole_line(
    conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Перегляд першого правила до запису другого бачив би менше N і не мав би суперника в PBO.
    books, split = _two_fold_setup()
    monkeypatch.setattr(research_cycle, "select_primary", _two_rules)
    sessions = _sessions(conn)
    report = await _cycle(sessions, books, split)
    async with sessions() as session:
        line = await line_trial_count(session, BTC)
    assert len(report.rules) == 2
    assert line == len(report.comparison_trials) + 2
    assert [rule.preview.n_trials for rule in report.rules] == [line, line]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_opening_requires_a_model_version(conn: AsyncConnection) -> None:
    # `None` — це перегляд, а не відкриття: відкриття без моделі відмовляє до будь-якого розрахунку.
    books, split = _two_fold_setup()
    async with AsyncSession(bind=conn) as session:
        recorded = await _record(session, books, split)
        with pytest.raises(ValueError, match="model_version"):
            await open_holdout_after_gate(
                session,
                experiment_id=recorded.experiment_id,
                trial_id=recorded.trial.trial_id,
                model_version=None,  # type: ignore[arg-type]
                stress=recorded.run.stress,
            )
        openings = (await session.execute(select(func.count()).select_from(HoldoutOpening))).scalar_one()
    assert openings == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_comparison_records_the_same_target_and_costs_as_the_meta_runs(conn: AsyncConnection) -> None:
    # Каталог і meta-експерименти — на одному таргеті й витратах, і журнал каталогу їх називає.
    # Без явного таргета обидва беруть горизонт розбивки, а не стандартний `TargetParams()`.
    books, split = _two_fold_setup()
    sessions = _sessions(conn)
    report = await _cycle(sessions, books, split, params=None)
    async with sessions() as session:
        comparison = await session.get(Experiment, report.comparison_trials[0].experiment_id)
        metas = [await session.get(Experiment, rule.experiment_id) for rule in report.rules]
    assert comparison is not None
    expected_target = target_payload(TargetParams(horizon_bars=split.horizon_bars))
    assert comparison.params["target"] == expected_target
    assert comparison.params["costs"] == costs_payload(_zero_costs())
    for meta in metas:
        assert meta is not None
        assert meta.params["target"] == comparison.params["target"]
        assert meta.params["costs"] == comparison.params["costs"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trial_folds_are_the_past_fold_entry_rule(conn: AsyncConnection) -> None:
    # Журнал спроби — угоди правила рецепта, оцінені лише минулими фолдами; G1 читає саме їх (рішення 0024).
    books, split = _three_fold_setup()
    async with AsyncSession(bind=conn) as session:
        recorded = await _record(session, books, split)
        trial = await session.get(ExperimentTrial, recorded.trial.trial_id)
    assert trial is not None
    assert trial.params["entry_evaluation"] == ENTRY_EVALUATION_PAST_FOLDS
    assert [item["fold_index"] for item in trial.params["entry_rules"]] == [0, 1, 2]
    assert len(recorded.run.entry_rules) == 3
    # Правило кожного фолду відтворюване: пороги, sha256 саме того калібратора і фолди, на яких його підігнано.
    for item, fit in zip(trial.params["entry_rules"], recorded.run.entry_rules, strict=True):
        if fit is None:
            assert item == {"fold_index": item["fold_index"], "method": None, "fit_on_folds": []}
            continue
        payload = decision_payload(fit.rule, fit.calibrator)
        assert item["decision_rule"] == payload["decision_rule"]
        assert item["calibrator_sha256"] == payload["calibrator_sha256"]
        assert item["method"] == fit.method
        assert item["fit_on_folds"] == list(range(item["fold_index"]))
        # Кінець міток пулу — строго до тесту фолду, який правило оцінює (purge, рішення 0024).
        assert item["label_end_ms"] < split.folds[item["fold_index"]].test_start_ms
    expected = past_fold_entry(_train(books, split), books)
    assert [fold.record for fold in recorded.run.folds] == [fold.record for fold in expected.folds]
    assert trial.params["comparison"]["filtered_total_net"] == format(expected.filtered_total_net, "f")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_two_folds_leave_no_entry_rule_and_g1_refuses(conn: AsyncConnection) -> None:
    # Без оцінки минулими фолдами G1 бачив би keep(0.5), а не правило рецепта: відмова, а не тихий пропуск.
    books, split = _two_fold_setup()
    async with AsyncSession(bind=conn) as session:
        recorded = await _record(session, books, split)
        trial = await session.get(ExperimentTrial, recorded.trial.trial_id)
        preview = await preview_gate_g1(
            session,
            experiment_id=recorded.experiment_id,
            trial_id=recorded.trial.trial_id,
            stress=recorded.run.stress,
        )
    assert recorded.decision is None
    assert recorded.decision_error is not None
    assert recorded.decision_error.startswith("правило входу минулих фолдів")
    assert trial is not None
    assert "decision" not in trial.params
    assert "entry_evaluation" not in trial.params
    assert G1_ENTRY_RULE in preview.decision.reasons


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cycle_commits_every_trial_even_when_a_later_rule_fails(
    conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Каталог, COMPLETE першого правила і FAILED другого — у журналі навіть після винятку: інакше
    # rollback викликача стер би спроби, які людина вже бачила, і N для DSR було б занижене.
    books, split = _two_fold_setup()
    monkeypatch.setattr(research_cycle, "select_primary", _two_rules)
    calls = 0
    original = meta_model.fit_entry_rule

    def second_fails(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise SplitLeakError("рядок hold-out у вибірці порогу")
        return original(*args, **kwargs)

    monkeypatch.setattr(meta_model, "fit_entry_rule", second_fails)
    sessions = _sessions(conn)
    with pytest.raises(SplitLeakError):
        await _cycle(sessions, books, split)
    async with sessions() as session:
        states = (await session.execute(select(ExperimentTrial.state, ExperimentTrial.params))).all()
        line = await line_trial_count(session, BTC)
    meta_states = sorted(state for state, params in states if params.get("role") == "meta")
    assert meta_states == ["COMPLETE", "FAILED"]
    comparison = [params for _state, params in states if params.get("role") != "meta"]
    assert comparison, "спроби каталогу теж закомічені"
    assert line == len(states)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recorded_folds_come_from_the_entry_rule_not_keep(
    conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    # На синтетиці `decide_entry` і `keep(0.5)` беруть ті самі рядки. Тут правило не бере нічого:
    # якщо журнал пише угоди `keep`, у ньому лишаться угоди — і проводка 0020/0024 зламана.
    books, split = _three_fold_setup()
    kept = _train(books, split)
    assert any(row.taken for fold in kept.folds for row in fold.scored), "keep(0.5) щось бере"
    monkeypatch.setattr(meta_model, "_enters", lambda *_args, **_kwargs: False)
    async with AsyncSession(bind=conn) as session:
        recorded = await _record(session, books, split)
        trial = await session.get(ExperimentTrial, recorded.trial.trial_id)
    assert trial is not None
    assert entry_rule_evaluated(trial.params)
    assert [outcome.n_trades for outcome in fold_outcomes_from_metrics(trial.metrics)] == [0, 0, 0]
    assert all(fold.stress.n_signals == 0 for fold in recorded.run.folds)


def test_recipe_trial_refuses_costs_or_target_other_than_training() -> None:
    # Правило рецепта (`params.decision`) і правила фолдів G1 — з тими самими витратами і таргетом.
    books, split = _three_fold_setup()
    run = _train(books, split)
    with pytest.raises(ValueError, match="витрати"):
        recipe_trial(run, books, split=split, costs=CostModel(), params=TargetParams(horizon_bars=4))
    with pytest.raises(ValueError, match="таргет"):
        recipe_trial(
            run, books, split=split, costs=_zero_costs(), params=TargetParams(horizon_bars=4, tp_atr_mult=Decimal(3))
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iteration_one_is_restored_from_the_journal_for_a_separate_iteration_two(conn: AsyncConnection) -> None:
    """T2.20: ітерація 2 окремим прогоном бере звіт ітерації 1 з журналу — той самий вибір, ті самі спроби."""
    from app.ml.research_cycle import iteration_one_from_journal

    books, split = _two_fold_setup()
    sessions = _sessions(conn)
    report = await _cycle(sessions, books, split)
    async with sessions() as session:
        n_before = await line_trial_count(session, BTC)
        restored = await iteration_one_from_journal(
            session,
            split=split,
            trading_symbol=BTC,
            symbols=_symbols(books),
            seed=_SEED,
            target=_TARGET,
            costs=_zero_costs(),
        )
        n_after = await line_trial_count(session, BTC)
    assert n_after == n_before  # відновлення нічого не пише
    assert restored.selection == report.selection
    assert restored.comparison_trials == report.comparison_trials
    assert [(rule.spec_name, rule.experiment_id, rule.trial_id, rule.decision_error) for rule in restored.rules] == [
        (rule.spec_name, rule.experiment_id, rule.trial_id, rule.decision_error) for rule in report.rules
    ]
    # Журнал між прогонами не змінився: перегляди G1 ті самі.
    assert [rule.preview for rule in restored.rules] == [rule.preview for rule in report.rules]
    assert restored.next_step == report.next_step
    assert restored.hold_out_opened is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restoring_iteration_one_refuses_another_target_or_costs_or_split(conn: AsyncConnection) -> None:
    from app.ml.research_cycle import iteration_one_from_journal

    books, split = _two_fold_setup()
    _other_books, other_split = _three_fold_setup()
    sessions = _sessions(conn)
    await _cycle(sessions, books, split)
    async with sessions() as session:
        with pytest.raises(ValueError, match="немає"):
            await iteration_one_from_journal(
                session,
                split=split,
                trading_symbol=BTC,
                symbols=_symbols(books),
                seed=_SEED,
                target=TargetParams(horizon_bars=4, tp_atr_mult=Decimal(3)),
                costs=_zero_costs(),
            )
        with pytest.raises(ValueError, match="немає"):
            await iteration_one_from_journal(
                session,
                split=split,
                trading_symbol=BTC,
                symbols=_symbols(books),
                seed=_SEED,
                target=_TARGET,
                costs=CostModel(),
            )
        with pytest.raises(ValueError, match="немає"):
            await iteration_one_from_journal(
                session,
                split=other_split,
                trading_symbol=BTC,
                symbols=_symbols(books),
                seed=_SEED,
                target=_TARGET,
                costs=_zero_costs(),
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restoring_iteration_one_refuses_two_runs_of_the_same_variant(conn: AsyncConnection) -> None:
    """Двічі прогнана ітерація 1 на тих самих фолдах — неоднозначно: який прогін брати, вирішує людина."""
    from app.ml.research_cycle import iteration_one_from_journal

    books, split = _two_fold_setup()
    sessions = _sessions(conn)
    await _cycle(sessions, books, split)
    await _cycle(sessions, books, split)
    async with sessions() as session:
        with pytest.raises(ValueError, match="кілька"):  # JournalMismatch("ambiguous")
            await iteration_one_from_journal(
                session,
                split=split,
                trading_symbol=BTC,
                symbols=_symbols(books),
                seed=_SEED,
                target=_TARGET,
                costs=_zero_costs(),
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restored_iteration_one_reports_an_opened_holdout_and_iteration_two_refuses(
    conn: AsyncConnection,
) -> None:
    """Hold-out лінії вже відкривали — відновлений звіт це каже, і ітерація 2 відмовляє до будь-якого запису."""
    from app.ml.research_cycle import iteration_one_from_journal, run_iteration_two

    books, split = _two_fold_setup()
    sessions = _sessions(conn)
    report = await _cycle(sessions, books, split)
    version = await make_model(conn)
    await conn.execute(
        insert(HoldoutOpening).values(
            research_line=BTC,
            holdout_from_ms=split.holdout_from_ms,
            holdout_to_ms=split.holdout_to_ms,
            opening_no=1,
            experiment_id=report.rules[0].experiment_id,
            model_version=version,
        )
    )
    async with sessions() as session:
        restored = await iteration_one_from_journal(
            session,
            split=split,
            trading_symbol=BTC,
            symbols=_symbols(books),
            seed=_SEED,
            target=_TARGET,
            costs=_zero_costs(),
        )
        n_before = await line_trial_count(session, BTC)
    assert restored.hold_out_opened is True
    with pytest.raises(ValueError, match="ітерація 2 лише після"):
        await run_iteration_two(
            sessions,
            books,
            after=restored,
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
        assert await line_trial_count(session, BTC) == n_before


def _symbols(books: tuple[SymbolHistory, ...]) -> tuple[str, ...]:
    return tuple(history.symbol for history in books)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_restoring_iteration_one_refuses_other_symbols_or_seed(conn: AsyncConnection) -> None:
    """Рев'ю T2.20: ітерація 2 — на тих самих символах і seed, що ітерація 1; інакше журнал не про ці дані."""
    from app.ml.research_cycle import JournalMismatch, iteration_one_from_journal

    books, split = _two_fold_setup()
    sessions = _sessions(conn)
    await _cycle(sessions, books, split)
    async with sessions() as session:
        for symbols, seed in (((*_symbols(books), "ETH/USDT:USDT"), _SEED), (_symbols(books), _SEED + 1)):
            with pytest.raises(JournalMismatch) as refused:
                await iteration_one_from_journal(
                    session,
                    split=split,
                    trading_symbol=BTC,
                    symbols=symbols,
                    seed=seed,
                    target=_TARGET,
                    costs=_zero_costs(),
                )
            assert refused.value.kind == "inconsistent"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_optuna_on_the_same_folds_does_not_break_the_restored_iteration_one(conn: AsyncConnection) -> None:
    """Пошук Optuna (`stage = meta_model` + `optuna_search`) на тих самих фолдах — не спроба ітерації 1."""
    from app.ml.meta_model import META_MIN_PROBABILITY, meta_recipe
    from app.ml.research_cycle import iteration_one_from_journal
    from app.ml.validation import create_experiment

    books, split = _two_fold_setup()
    sessions = _sessions(conn)
    report = await _cycle(sessions, books, split)
    recipe = meta_recipe(shuffled_labels=False, min_probability=META_MIN_PROBABILITY)
    recipe["optuna_search"] = {"space": "test"}
    recipe["target"] = target_payload(_TARGET)
    recipe["costs"] = costs_payload(_zero_costs())
    async with sessions() as session:
        await create_experiment(
            session,
            split=split,
            research_line=BTC,
            symbols=_symbols(books),
            label_type="meta",
            primary_strategy=BREAKOUT.metadata(),
            params=recipe,
            seed=_SEED,
        )
        await session.commit()
    async with sessions() as session:
        restored = await iteration_one_from_journal(
            session,
            split=split,
            trading_symbol=BTC,
            symbols=_symbols(books),
            seed=_SEED,
            target=_TARGET,
            costs=_zero_costs(),
        )
    assert [rule.experiment_id for rule in restored.rules] == [rule.experiment_id for rule in report.rules]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unfinished_iteration_one_is_not_restored(conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ітерація 1 впала на другому правилі (спроба `FAILED`): звіту не було — і з журналу його немає."""
    from app.ml.research_cycle import JournalMismatch, iteration_one_from_journal

    books, split = _two_fold_setup()
    monkeypatch.setattr(research_cycle, "select_primary", _two_rules)
    calls = 0
    original = meta_model.fit_entry_rule

    def second_fails(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise SplitLeakError("рядок hold-out у вибірці порогу")
        return original(*args, **kwargs)

    monkeypatch.setattr(meta_model, "fit_entry_rule", second_fails)
    sessions = _sessions(conn)
    with pytest.raises(SplitLeakError):
        await _cycle(sessions, books, split)
    async with sessions() as session:
        with pytest.raises(JournalMismatch, match="COMPLETE") as refused:
            await iteration_one_from_journal(
                session,
                split=split,
                trading_symbol=BTC,
                symbols=_symbols(books),
                seed=_SEED,
                target=_TARGET,
                costs=_zero_costs(),
            )
    assert refused.value.kind == "inconsistent"
