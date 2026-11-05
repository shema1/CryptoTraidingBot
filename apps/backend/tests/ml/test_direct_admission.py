"""Допуск, артефакт, перенавчання і статуси для `label_type=direct` (T2.11-розширення, рішення 0022).

Модель direct — 3 класи на всіх свічках, без первинного правила (`primary_strategy` = `{}`), з калібратором
`calibrator.json`. Тут перевіряються: склад артефакту, прив'язка допуску до спроби G1, перенавчання з грантом
без підбору, перехід у CANDIDATE лише після допуску, і Live не торгує CANDIDATE.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.api.models import AUDIT_ACTION_MODEL_APPROVE, AuditLog
from app.exchange.types import PositionSide
from app.ml.artifacts import (
    _DIRECT_CLASSES,
    CALIBRATOR_FILE,
    ArtifactError,
    GitState,
    ModelNotTradable,
    _lgbm_payload,
    _recipe_matches_trial,
    approve_model,
    booster_of,
    build_metadata,
    current_library_versions,
    decision_payload,
    load_model,
    load_registered_model,
    promote_to_candidate,
    record_holdout_admission,
    register_model,
    require_model_for_start,
    retrain_registered_recipe,
    write_artifact,
)
from app.ml.decision import ENTRY_EVALUATION_PAST_FOLDS, DecisionRule, OosObservation
from app.ml.direct_model import (
    CLASS_LABELS,
    FrozenDirectFit,
    _holdout_signals,
    _past_pool,
    direct_recipe,
    direct_trial_params,
    evaluate_direct,
    fit_frozen_direct,
)
from app.ml.features import HOUR_MS, feature_columns, feature_schema_hash
from app.ml.meta_model import _grant_holdout, lgbm_journal_params
from app.ml.models import Experiment, ExperimentTrial, HoldoutOpening, ModelArtifact
from app.ml.statistics import GateRejected, open_holdout_after_gate
from app.ml.strategy import BREAKOUT
from app.ml.trade_sim import TargetParams
from app.ml.validation import (
    ResearchSplit,
    SplitLeakError,
    _open_holdout,
    costs_payload,
    create_experiment,
    record_trial,
    target_payload,
)
from tests.ml.test_artifacts import (
    backtest_report,
    holdout_trades,
    isotonic_calibrator,
    random_booster,
    walk_forward_trades,
    zero_costs,
)
from tests.ml.test_direct_model import _scripted_run, _three_folds

BTC = "BTC/USDT:USDT"
HOUR = HOUR_MS
SEED = 7
TARGET = TargetParams(horizon_bars=4)
_G1: dict[str, Any] = {
    "gate": "G1",
    "passed": True,
    "n_trials_line": 7,
    "dsr": "0.97",
    "pbo": "0.31",
    "profitable_fold_share": "0.80",
    "loosened": [],
}


@pytest.fixture(autouse=True)
def _fast_corridor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.ml.artifacts.CORRIDOR_REPLICATES", 20)


def test_artifact_class_order_is_the_model_class_order() -> None:
    """`artifacts._DIRECT_CLASSES` дублює `direct_model.CLASS_LABELS` (імпорт дав би цикл). Звірка тут."""
    assert tuple(label.value for label in CLASS_LABELS) == _DIRECT_CLASSES


def test_direct_metadata_has_no_strategy_and_needs_a_calibrator() -> None:
    kwargs: dict[str, Any] = {
        "model_version": "lgbm-direct-2026-11-05-001",
        "label_type": "direct",
        "trade_symbol": BTC,
        "train_symbols": (BTC,),
        "feature_columns": feature_columns(()),
        "target": TARGET,
        "costs": zero_costs(),
        "rule": DecisionRule(),
        "experiment_id": "exp",
        "seed": SEED,
        "data_hash": "sha256:" + "ab" * 32,
        "git": GitState(commit="abc123", dirty=False),
        "library_versions": current_library_versions(),
        "train_range_ms": (0, 100 * HOUR),
        "recipe_lgbm": lgbm_journal_params(),
        "coverage_ms": (0, 100 * HOUR),
        "train_rows": 12,
        "include_holdout": False,
        "calibrator_method": "isotonic",
    }
    meta = build_metadata(spec=None, **kwargs)
    assert meta["primary_strategy"] == {}
    assert meta["algorithm"] == "lightgbm"
    with pytest.raises(ArtifactError, match="первинного правила"):
        build_metadata(spec=BREAKOUT, **kwargs)
    with pytest.raises(ArtifactError, match="калібратор"):
        build_metadata(spec=None, **{**kwargs, "calibrator_method": None})


def test_direct_artifact_roundtrip_and_a_swapped_calibrator_is_refused(tmp_path: Path) -> None:
    columns = feature_columns(())
    meta = build_metadata(
        model_version="lgbm-direct-2026-11-05-002",
        label_type="direct",
        spec=None,
        trade_symbol=BTC,
        train_symbols=(BTC,),
        feature_columns=columns,
        target=TARGET,
        costs=zero_costs(),
        rule=DecisionRule(),
        experiment_id="exp",
        seed=SEED,
        data_hash="sha256:" + "ab" * 32,
        git=GitState(commit="abc123", dirty=False),
        library_versions=current_library_versions(),
        train_range_ms=(0, 100 * HOUR),
        recipe_lgbm=lgbm_journal_params(),
        coverage_ms=(0, 100 * HOUR),
        train_rows=12,
        include_holdout=False,
        calibrator_method="isotonic",
    )
    directory = tmp_path / meta["model_version"]
    write_artifact(directory, meta, booster=random_booster(len(columns)), calibrator=isotonic_calibrator())
    loaded = load_model(directory)
    assert loaded.calibrator is not None
    assert loaded.metadata["reproducibility"]["calibrator_sha256"].startswith("sha256:")
    (directory / CALIBRATOR_FILE).write_text('{"method": "isotonic", "x": []}', encoding="utf-8")
    with pytest.raises(ArtifactError, match="калібратора"):
        load_model(directory)


def test_frozen_direct_fit_refuses_a_grant_of_another_window() -> None:
    books, split = _three_folds()
    wrong = _grant_holdout(1, split.holdout_from_ms + HOUR, split.holdout_to_ms)
    with pytest.raises(SplitLeakError, match="грант не цього hold-out"):
        fit_frozen_direct(books, split=split, trading_symbol=BTC, seed=SEED, costs=zero_costs(), holdout_grant=wrong)


def test_holdout_signals_are_only_for_a_model_without_the_grant() -> None:
    books, split = _three_folds()
    fit = fit_frozen_direct(books, split=split, trading_symbol=BTC, seed=SEED, costs=zero_costs(), params=TARGET)
    granted = dataclasses.replace(fit, include_holdout=True)
    with pytest.raises(ValueError, match="модель допуску"):
        _holdout_signals(granted, None, books, split=split, trading_symbol=BTC, costs=zero_costs(), params=TARGET)  # type: ignore[arg-type]


async def _direct_trial(session: AsyncSession, experiment_id: str, *, schema: str) -> int:
    """COMPLETE-спроба direct з рецептом, схемою, правилом і перехресним (минулими фолдами) оцінюванням."""
    params: dict[str, Any] = {
        **direct_recipe(shuffled_labels=False),
        "lgbm": _lgbm_payload(lgbm_journal_params()),
        "feature_schema_hash": schema,
        "decision": decision_payload(DecisionRule(), isotonic_calibrator()),
        "entry_evaluation": ENTRY_EVALUATION_PAST_FOLDS,
    }
    forward = walk_forward_trades()
    inserted = await session.execute(
        insert(ExperimentTrial)
        .values(
            experiment_id=experiment_id,
            trial_no=1,
            state="COMPLETE",
            params=params,
            oos_results=[format(item.r_multiple, "f") for item in forward],
            oos_entry_ms=[item.entry_open_time_ms for item in forward],
            metrics={},
            created_at_ms=1,
        )
        .returning(ExperimentTrial.id)
    )
    return int(inserted.scalar_one())


async def _direct_experiment(session: AsyncSession, split: ResearchSplit) -> str:
    params = {
        **direct_recipe(shuffled_labels=False),
        "target": target_payload(TargetParams(horizon_bars=split.horizon_bars)),
        "costs": costs_payload(zero_costs()),
    }
    return await create_experiment(
        session,
        split=split,
        research_line=BTC,
        symbols=(BTC,),
        label_type="direct",
        primary_strategy={},
        params=params,
        seed=SEED,
    )


def _direct_meta(version: str, experiment_id: str, fit: FrozenDirectFit, split: ResearchSplit) -> dict:  # type: ignore[type-arg]
    return build_metadata(
        model_version=version,
        label_type="direct",
        spec=None,
        trade_symbol=BTC,
        train_symbols=(BTC,),
        feature_columns=fit.model.feature_columns,
        target=TARGET,
        costs=zero_costs(),
        rule=DecisionRule(),
        experiment_id=experiment_id,
        seed=SEED,
        data_hash="sha256:" + "cd" * 32,
        git=GitState(commit="abc123", dirty=False),
        library_versions=current_library_versions(),
        train_range_ms=(split.data_from_ms, split.holdout_from_ms),
        recipe_lgbm=fit.lgbm,
        coverage_ms=fit.coverage_ms,
        train_rows=fit.n_rows,
        include_holdout=False,
        calibrator_method="isotonic",
    )


async def _audit(session: AsyncSession, target: str) -> int:
    inserted = await session.execute(
        insert(AuditLog)
        .values(actor="user", action=AUDIT_ACTION_MODEL_APPROVE, confirmed=True, target=target)
        .returning(AuditLog.id)
    )
    return int(inserted.scalar_one())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_direct_model_reaches_candidate_only_through_its_holdout_and_never_trades_before_t5(
    conn: AsyncConnection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    books, split = _three_folds()
    monkeypatch.setattr("app.ml.artifacts.read_git_state", lambda _root: GitState(commit="def456", dirty=False))
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        experiment_id = await _direct_experiment(session, split)
        fit = fit_frozen_direct(books, split=split, trading_symbol=BTC, seed=SEED, costs=zero_costs(), params=TARGET)
        trial_id = await _direct_trial(session, experiment_id, schema=fit.model.schema_hash)
        version = "lgbm-direct-2026-11-05-101"
        write_artifact(
            tmp_path / version,
            _direct_meta(version, experiment_id, fit, split),
            booster=booster_of(fit.model),
            calibrator=isotonic_calibrator(),
        )
        evidence = await register_model(session, tmp_path / version)
        row = await session.get(ModelArtifact, evidence)
        assert row is not None
        assert row.label_type == "direct"
        assert row.status == "TRAINED"
        # Без допуску перенавчання не стартує, а CANDIDATE не виникає.
        with pytest.raises(ArtifactError, match="допуск"):
            await retrain_registered_recipe(session, books, validation_version=evidence, root=tmp_path, at_ms=0)

        opening = await _open_holdout(
            session,
            research_line=BTC,
            holdout_from_ms=split.holdout_from_ms,
            holdout_to_ms=split.holdout_to_ms,
            experiment_id=experiment_id,
            model_version=evidence,
            result={**_G1, "trial_id": trial_id},
        )
        report = backtest_report(holdout_trades(start=split.holdout_from_ms), seed=SEED).model_copy(
            update={"symbol": BTC}
        )
        admission = await record_holdout_admission(
            session,
            opening_id=opening.opening_id,
            report=report,
            walk_forward=walk_forward_trades(),
        )
        assert admission.passed, admission.reasons

        retrained = await retrain_registered_recipe(
            session, books, validation_version=evidence, root=tmp_path, at_ms=split.data_to_ms
        )
        retrained_row = await session.get(ModelArtifact, retrained)
        assert retrained_row is not None
        assert retrained_row.status == "TRAINED"
        assert retrained_row.validated_by == evidence
        meta = dict(retrained_row.metadata_json)
        assert meta["recipe"]["include_holdout"] is True
        assert meta["primary_strategy"] == {}
        assert (tmp_path / retrained / CALIBRATOR_FILE).is_file()

        oos = walk_forward_trades() + holdout_trades(start=split.holdout_from_ms)
        monkeypatch.setattr("app.ml.artifacts.read_git_state", lambda _root: GitState(commit="def456", dirty=True))
        dirty = await retrain_registered_recipe(
            session, books, validation_version=evidence, root=tmp_path, at_ms=split.data_to_ms + 1
        )
        assert dirty != retrained
        with pytest.raises(ArtifactError, match="git_dirty"):
            await promote_to_candidate(session, model_version=dirty, oos_trades=oos)

        await promote_to_candidate(session, model_version=retrained, oos_trades=oos)
        session.expire_all()
        candidate = await session.get(ModelArtifact, retrained)
        assert candidate is not None
        assert candidate.status == "CANDIDATE"
        assert candidate.live_corridor is not None
        with pytest.raises(ModelNotTradable):
            await require_model_for_start(session, model_version=retrained, mode="live")
        # До T5.x direct не стає APPROVED і не торгує (рішення 0022), навіть з підтвердженням.
        audit_id = await _audit(session, retrained)
        with pytest.raises(ArtifactError, match="T5"):
            await approve_model(session, model_version=retrained, confirmed_audit_id=audit_id)
        with pytest.raises(ModelNotTradable, match="T5"):
            await require_model_for_start(session, model_version=retrained, mode="live")
        loaded = await load_registered_model(session, retrained)
        assert loaded.db_status == "CANDIDATE"
        assert loaded.calibrator is not None
        opening_row = await session.get(HoldoutOpening, opening.opening_id)
        assert opening_row is not None
        experiment = await session.get(Experiment, experiment_id)
        assert experiment is not None
        assert experiment.label_type == "direct"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_direct_holdout_opens_only_through_g1_and_a_refusal_writes_nothing(
    conn: AsyncConnection, tmp_path: Path
) -> None:
    """Fail-closed для direct знято (рішення 0022): відкриття йде через Gate G1 з моделлю допуску.

    Сценарна спроба з метриками фолдів не проходить G1: фолди 0 і 1 без угод (правило минулих фолдів), тож
    відмова називає `G1_MIN_TRADES_FOLD`. Відмова `GateRejected` без жодного запису в `holdout_openings`.
    Без `model_version` — `ValueError` до будь-якого розрахунку.
    """
    books, split = _three_folds()
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        experiment_id = await _direct_experiment(session, split)
        fit = fit_frozen_direct(books, split=split, trading_symbol=BTC, seed=SEED, costs=zero_costs(), params=TARGET)
        # Спроба з метриками фолдів і правилом входу, як пише журнал (сценарний прогін без навчання).
        run = _scripted_run(books, split)
        evaluation = evaluate_direct(run, books)
        trial = await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="COMPLETE",
            params=direct_trial_params(run, evaluation),
            folds=evaluation.records,
        )
        trial_id = trial.trial_id
        version = "lgbm-direct-2026-11-05-201"
        write_artifact(
            tmp_path / version,
            _direct_meta(version, experiment_id, fit, split),
            booster=booster_of(fit.model),
            calibrator=isotonic_calibrator(),
        )
        model_version = await register_model(session, tmp_path / version)
        with pytest.raises(ValueError, match="model_version"):
            await open_holdout_after_gate(session, experiment_id=experiment_id, trial_id=trial_id, model_version="")
        with pytest.raises(GateRejected) as refused:
            await open_holdout_after_gate(
                session, experiment_id=experiment_id, trial_id=trial_id, model_version=model_version
            )
        assert "G1_MIN_TRADES_FOLD" in refused.value.decision.reasons
        openings = (await session.execute(select(func.count()).select_from(HoldoutOpening))).scalar_one()
    assert openings == 0


def _direct_recipe_meta() -> dict[str, Any]:
    """Метадані допуску direct, які звіряє `_recipe_matches_trial`, без файлів."""
    rule = DecisionRule()
    return {
        "label_type": "direct",
        "primary_strategy": {},
        "decision_rule": rule.metadata(),
        "feature_schema_hash": feature_schema_hash(feature_columns(())),
        "recipe": {"lgbm": _lgbm_payload(lgbm_journal_params())},
        "reproducibility": {
            "calibrator_sha256": decision_payload(rule, isotonic_calibrator())["calibrator_sha256"],
        },
    }


def _direct_recipe_params() -> dict[str, Any]:
    return {
        **direct_recipe(shuffled_labels=False),
        "lgbm": _lgbm_payload(lgbm_journal_params()),
        "feature_schema_hash": feature_schema_hash(feature_columns(())),
        "decision": decision_payload(DecisionRule(), isotonic_calibrator()),
        "entry_evaluation": ENTRY_EVALUATION_PAST_FOLDS,
    }


def test_direct_recipe_matches_its_own_g1_trial() -> None:
    _recipe_matches_trial(_direct_recipe_meta(), SimpleNamespace(params=_direct_recipe_params()))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("stage", "rule"),
        ("role", "meta"),
        ("objective", "binary"),
        ("classes", ["LONG", "SHORT", "FLAT"]),
        ("shuffled_labels", True),
        ("decision", {"decision_rule": {}, "calibrator_sha256": "sha256:" + "0" * 64}),
        ("feature_schema_hash", "sha256:" + "1" * 64),
        ("lgbm", {"n_estimators": 7}),
        ("best_iteration", [7]),
    ],
)
def test_direct_recipe_refuses_a_trial_that_differs_in_any_recipe_field(key: str, value: Any) -> None:
    params = {**_direct_recipe_params(), key: value}
    with pytest.raises(ArtifactError):
        _recipe_matches_trial(_direct_recipe_meta(), SimpleNamespace(params=params))  # type: ignore[arg-type]


def test_direct_trial_with_a_meta_strategy_still_matches_only_by_direct_identity() -> None:
    """`direct` не має первинного правила: поле `strategy` спроби ні на що не впливає, ідентичність — стадія."""
    params = {**_direct_recipe_params(), "strategy": {"name": "ema_cross", "params": {}}}
    _recipe_matches_trial(_direct_recipe_meta(), SimpleNamespace(params=params))  # type: ignore[arg-type]


def test_past_pool_drops_rows_whose_label_reaches_the_next_test_fold() -> None:
    """Purge: рядок минулого фолду, чия мітка (до `horizon_bars` свічок) заходить у тест фолду k, у пул не йде."""
    horizon = 4
    test_start = 10 * HOUR
    split = SimpleNamespace(horizon_bars=horizon, folds=[None] * 3)
    split.folds[2] = SimpleNamespace(test_start_ms=test_start)

    def row(fold: int, time_ms: int) -> OosObservation:
        return OosObservation(
            symbol=BTC,
            time_ms=time_ms,
            index=time_ms // HOUR,
            fold_index=fold,
            side=PositionSide.LONG,
            raw_probability=Decimal("0.6"),
            true_label=1,
            entry=Decimal(100),
            atr=Decimal(1),
        )

    inside = row(1, test_start - horizon * HOUR + HOUR)  # мітка заходить у тест
    ends_before = row(1, test_start - horizon * HOUR)  # мітка закінчується рівно на межі
    later = row(2, test_start - 10 * HOUR)  # фолд ≥ k не входить ніколи
    pool = _past_pool([inside, ends_before, later], split, 2)  # type: ignore[arg-type]
    assert pool == (ends_before,)
