"""Статуси моделі, допуск hold-out, коридор, перенавчання і заборона Live для CANDIDATE (T2.11)."""

from __future__ import annotations

import dataclasses
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.api.models import AuditLog
from app.exchange.types import PositionSide
from app.ml.artifacts import (
    CALIBRATOR_FILE,
    METADATA_FILE,
    MODEL_FILE,
    REASON_DRAWDOWN,
    REASON_FOLD_SHARE,
    ArtifactError,
    GitState,
    ModelNotTradable,
    _lgbm_payload,
    _register_retrained,
    accept_primary,
    approve_model,
    booster_of,
    build_metadata,
    current_library_versions,
    decision_payload,
    load_registered_model,
    promote_to_candidate,
    record_holdout_admission,
    register_model,
    require_model_for_start,
    retire_model,
    retrain_registered_recipe,
    rows_for_opened_holdout,
    write_artifact,
)
from app.ml.backtest import BacktestReport, BacktestTrade
from app.ml.decision import DecisionRule
from app.ml.features import HOUR_MS, feature_columns, feature_schema_hash
from app.ml.meta_model import _grant_holdout, fit_frozen_meta, lgbm_journal_params
from app.ml.models import Experiment, ExperimentTrial, HoldoutOpening, ModelArtifact
from app.ml.statistics import DEFAULT_BOOTSTRAP_REPLICATES
from app.ml.strategy import BREAKOUT, EMA_CROSS, StrategySpec, SymbolHistory
from app.ml.trade_sim import TargetParams
from app.ml.validation import (
    ResearchSplit,
    SampleRow,
    SplitLeakError,
    _open_holdout,
    build_split,
    costs_payload,
    create_experiment,
    target_payload,
)
from app.risk.costs import CostModel
from tests.ml.test_artifacts import (
    backtest_report,
    extra_losers,
    holdout_trades,
    isotonic_calibrator,
    learning_books,
    random_booster,
    walk_forward_trades,
    zero_costs,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
SOL = "SOL/USDT:USDT"
HOUR = HOUR_MS
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
    """Коридор у тестах — 20 реплік замість 1000: число береться з модульної константи, не від викликача."""
    monkeypatch.setattr("app.ml.artifacts.CORRIDOR_REPLICATES", 20)


@dataclasses.dataclass(frozen=True)
class World:
    """Експеримент, спроба G1 у журналі, модель допуску і відкриття hold-out."""

    experiment_id: str
    trial_id: int
    evidence: str
    opening_id: int
    split: ResearchSplit
    symbol: str


async def test_live_does_not_start_on_candidate_and_dirty_never_promotes(conn: AsyncConnection, tmp_path) -> None:  # type: ignore[no-untyped-def]
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-001")
        await _admit(session, world)
        experiment = await session.get(Experiment, world.experiment_id)
        assert experiment is not None
        experiment.n_trials_line = 999
        experiment.dsr = Decimal("0.10")
        experiment.pbo = Decimal("0.90")
        candidate = await _candidate(session, tmp_path, world, version="lgbm-meta-2026-11-03-002", dirty=False)
        dirty = await _candidate(session, tmp_path, world, version="lgbm-meta-2026-11-03-003", dirty=True)
        trades = _oos_trades(world)
        with pytest.raises(ArtifactError, match="git_dirty"):
            await promote_to_candidate(session, model_version=dirty, oos_trades=trades)
        dirty_row = await session.get(ModelArtifact, dirty)
        assert dirty_row is not None
        assert dirty_row.status == "TRAINED"
        assert dirty_row.live_corridor is None

        meta = await promote_to_candidate(session, model_version=candidate, oos_trades=trades)
        session.expire_all()
        row = await session.get(ModelArtifact, candidate)
        assert row is not None
        assert row.status == "CANDIDATE"
        assert row.git_dirty is False
        assert row.live_corridor is not None
        assert set(row.live_corridor) == {"n_trades", "p5_cum_r", "max_loss_streak"}
        assert row.live_corridor["n_trades"] == list(range(1, 111))
        assert all(isinstance(value, str) for value in row.live_corridor["p5_cum_r"])
        # DSR, PBO і N — з відкриття, не зі знімка експерименту (там 999 / 0.10 / 0.90).
        assert meta["n_trials_total"] == 7
        assert meta["metrics"]["dsr"] == "0.97"
        assert meta["metrics"]["pbo"] == "0.31"
        assert meta["metrics"]["g1_trial_id"] == world.trial_id
        assert meta["reproducibility"]["corridor_replicates"] == 20
        corridor = dict(row.live_corridor)
        loaded = await load_registered_model(session, candidate)
        assert loaded.metadata["status"] == "CANDIDATE"
        assert loaded.metadata["live_corridor"] == corridor

        with pytest.raises(ModelNotTradable, match="CANDIDATE"):
            await require_model_for_start(session, model_version=candidate, mode="live")
        for mode in ("demo", "testnet"):
            started = await require_model_for_start(session, model_version=candidate, mode=mode)
            assert started.status == "CANDIDATE"
        for mode in ("demo", "testnet", "live"):
            with pytest.raises(ModelNotTradable, match="TRAINED"):
                await require_model_for_start(session, model_version=world.evidence, mode=mode)

        unconfirmed = await _audit(session, candidate, confirmed=False)
        with pytest.raises(ArtifactError, match=r"model\.approve"):
            await approve_model(session, model_version=candidate, confirmed_audit_id=unconfirmed)
        foreign = await _audit(session, world.evidence, confirmed=True)
        with pytest.raises(ArtifactError, match=r"model\.approve"):
            await approve_model(session, model_version=candidate, confirmed_audit_id=foreign)
        other_action = await _audit(session, candidate, confirmed=True, action="trial.invalidate")
        with pytest.raises(ArtifactError, match=r"model\.approve"):
            await approve_model(session, model_version=candidate, confirmed_audit_id=other_action)
        confirmed = await _audit(session, candidate, confirmed=True)
        await approve_model(session, model_version=candidate, confirmed_audit_id=confirmed)
        session.expire_all()
        approved = await session.get(ModelArtifact, candidate)
        assert approved is not None
        assert approved.status == "APPROVED"
        assert approved.confirmed_audit_id == confirmed
        assert approved.live_corridor == corridor
        for mode in ("demo", "testnet", "live"):
            live = await require_model_for_start(session, model_version=candidate, mode=mode)
            assert live.status == "APPROVED"

        await retire_model(session, model_version=candidate)
        session.expire_all()
        retired = await session.get(ModelArtifact, candidate)
        assert retired is not None
        assert retired.status == "RETIRED"
        assert retired.live_corridor == corridor
        for mode in ("demo", "testnet", "live"):
            with pytest.raises(ModelNotTradable, match="RETIRED"):
                await require_model_for_start(session, model_version=candidate, mode=mode)
        stored = await session.scalar(select(Experiment.n_trials_line).where(Experiment.id == world.experiment_id))
        assert stored == 999


async def test_every_forbidden_transition_is_refused(conn: AsyncConnection, tmp_path) -> None:  # type: ignore[no-untyped-def]
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-031")
        await _admit(session, world)
        model = await _candidate(session, tmp_path, world, version="lgbm-meta-2026-11-03-032", dirty=False)
        trades = _oos_trades(world)
        # Модель допуску не бачила hold-out і в CANDIDATE не йде.
        with pytest.raises(ArtifactError, match="перенавчена"):
            await promote_to_candidate(session, model_version=world.evidence, oos_trades=trades)
        # TRAINED: лише в CANDIDATE.
        approve_audit = await _audit(session, model, confirmed=True)
        with pytest.raises(ArtifactError, match="лише з CANDIDATE"):
            await approve_model(session, model_version=model, confirmed_audit_id=approve_audit)
        with pytest.raises(ArtifactError, match="RETIRED ставиться"):
            await retire_model(session, model_version=model)
        # Коридор — з усіх OOS-угод рецепта, у порядку часу, ті самі час і R, що в журналі й допуску.
        with pytest.raises(ArtifactError, match="усіх out-of-sample"):
            await promote_to_candidate(session, model_version=model, oos_trades=trades[:-1])
        with pytest.raises(ArtifactError, match="порядку часу"):
            await promote_to_candidate(session, model_version=model, oos_trades=trades[::-1])
        widened = (*trades[:-1], trades[-1].model_copy(update={"r_multiple": Decimal("-3")}))
        with pytest.raises(ArtifactError, match="R угод коридора"):
            await promote_to_candidate(session, model_version=model, oos_trades=widened)
        await promote_to_candidate(session, model_version=model, oos_trades=trades)
        # CANDIDATE: другий CANDIDATE (перерахунок коридора) заборонений.
        with pytest.raises(ArtifactError, match="лише TRAINED"):
            await promote_to_candidate(session, model_version=model, oos_trades=trades)
        # Підтвердження, дане ще до CANDIDATE, не рахується: потрібне свіже рішення людини.
        with pytest.raises(ArtifactError, match="старіше"):
            await approve_model(session, model_version=model, confirmed_audit_id=approve_audit)
        approve_audit = await _audit(session, model, confirmed=True)
        await approve_model(session, model_version=model, confirmed_audit_id=approve_audit)
        # APPROVED: ні CANDIDATE, ні APPROVED вдруге.
        with pytest.raises(ArtifactError, match="лише TRAINED"):
            await promote_to_candidate(session, model_version=model, oos_trades=trades)
        with pytest.raises(ArtifactError, match="лише з CANDIDATE"):
            await approve_model(session, model_version=model, confirmed_audit_id=approve_audit)
        await retire_model(session, model_version=model)
        # RETIRED — кінцевий.
        with pytest.raises(ArtifactError, match="лише TRAINED"):
            await promote_to_candidate(session, model_version=model, oos_trades=trades)
        with pytest.raises(ArtifactError, match="лише з CANDIDATE"):
            await approve_model(session, model_version=model, confirmed_audit_id=approve_audit)
        with pytest.raises(ArtifactError, match="RETIRED ставиться"):
            await retire_model(session, model_version=model)


async def test_candidate_requires_a_recorded_holdout_pass(conn: AsyncConnection, tmp_path) -> None:  # type: ignore[no-untyped-def]
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-011")
        candidate = await _candidate(session, tmp_path, world, version="lgbm-meta-2026-11-03-012", dirty=False)
        trades = _oos_trades(world)
        with pytest.raises(ArtifactError, match="hold-out"):
            await promote_to_candidate(session, model_version=candidate, oos_trades=trades)
        failed = await _admit(session, world, losing=True)
        assert failed.passed is False
        with pytest.raises(ArtifactError, match="hold-out"):
            await promote_to_candidate(session, model_version=candidate, oos_trades=trades)
        row = await session.get(ModelArtifact, candidate)
        assert row is not None
        assert row.status == "TRAINED"


async def test_loosened_g1_does_not_give_a_candidate(conn: AsyncConnection, tmp_path) -> None:  # type: ignore[no-untyped-def]
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(
            session, tmp_path, version="lgbm-meta-2026-11-03-071", g1={"dsr": "0.90", "loosened": ["dsr"]}
        )
        assert (await _admit(session, world)).passed is True
        candidate = await _candidate(session, tmp_path, world, version="lgbm-meta-2026-11-03-072", dirty=False)
        with pytest.raises(ArtifactError, match="без послаблень"):
            await promote_to_candidate(session, model_version=candidate, oos_trades=_oos_trades(world))


async def test_admission_is_computed_from_reports_and_written_once(conn: AsyncConnection, tmp_path) -> None:  # type: ignore[no-untyped-def]
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-041")
        start = world.split.holdout_from_ms
        with pytest.raises(ArtifactError, match="спробі G1"):
            await _admit(session, world, walk_forward=walk_forward_trades()[:-1])
        with pytest.raises(ArtifactError, match="seed"):
            await _admit(session, world, seed=2)
        honest = await _admit(session, world)
        assert honest.passed is True
        opening = await session.get(HoldoutOpening, world.opening_id)
        assert opening is not None
        admission = dict(opening.result or {})["admission"]
        assert admission["seed"] == 1
        assert admission["n_replicates"] == DEFAULT_BOOTSTRAP_REPLICATES
        assert admission["max_drawdown_limit_pct"] == "0.10"
        assert admission["profitable_fold_share"] == "0.80"
        assert admission["trial_id"] == world.trial_id
        assert admission["holdout_entry_ms"] == [start + index * HOUR for index in range(30)]
        with pytest.raises(ArtifactError, match="уже записаний"):
            await _admit(session, world)
        not_g1 = await _open(session, world, result={**_G1, "passed": False})
        with pytest.raises(ArtifactError, match="Gate G1"):
            await _admit(session, dataclasses.replace(world, opening_id=not_g1))


async def test_admission_takes_fold_share_from_g1_and_gives_no_second_chance(conn: AsyncConnection, tmp_path) -> None:  # type: ignore[no-untyped-def]
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(
            session, tmp_path, version="lgbm-meta-2026-11-03-081", g1={"profitable_fold_share": "0.60"}
        )
        failed = await _admit(session, world, drawdown=Decimal("0.5"))
        assert failed.passed is False
        assert REASON_FOLD_SHARE in failed.reasons
        assert REASON_DRAWDOWN in failed.reasons
        # Друге відкриття тієї самої моделі й спроби з чесним G1 не дає ще одного допуску.
        second = await _open(session, world)
        with pytest.raises(ArtifactError, match="уже перевірявся"):
            await _admit(session, dataclasses.replace(world, opening_id=second))
        with pytest.raises(ArtifactError, match="невдалий допуск"):
            await retrain_registered_recipe(session, (), validation_version=world.evidence, root=tmp_path)


async def test_the_same_g1_trial_gets_no_second_admission_through_another_model(  # type: ignore[no-untyped-def]
    conn: AsyncConnection, tmp_path
) -> None:
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-121")
        assert (await _admit(session, world, losing=True)).passed is False
        # Інша модель того самого рецепта й тієї самої спроби G1 — друге відкриття, але не другий допуск.
        other_version = "lgbm-meta-2026-11-03-122"
        meta = _meta(
            version=other_version,
            experiment_id=world.experiment_id,
            spec=EMA_CROSS,
            symbol=BTC,
            train_range=(0, world.split.holdout_from_ms),
        )
        write_artifact(
            tmp_path / other_version,
            meta,
            booster=random_booster(len(feature_columns(())) + 1),
            calibrator=isotonic_calibrator(),
        )
        other = dataclasses.replace(world, evidence=await register_model(session, tmp_path / other_version))
        second = await _open(session, other)
        with pytest.raises(ArtifactError, match="уже перевірявся"):
            await _admit(session, dataclasses.replace(other, opening_id=second))


async def test_journal_must_carry_the_target_costs_and_entry_rule_of_the_recipe(  # type: ignore[no-untyped-def]
    conn: AsyncConnection, tmp_path
) -> None:
    other_costs = costs_payload(zero_costs())
    other_rule = decision_payload(DecisionRule(min_edge_atr=Decimal("0.2")), isotonic_calibrator())
    cases: tuple[tuple[str, dict[str, Any], str], ...] = (
        ("BTC/USDT:USDT", {"experiment_extra": {"target": None}}, "таргет"),
        ("ETH/USDT:USDT", {"experiment_extra": {"costs": other_costs}}, "витрати"),
        ("SOL/USDT:USDT", {"trial_extra": {"decision": None}}, "правило входу"),
        ("XRP/USDT:USDT", {"trial_extra": {"decision": other_rule}}, "правило входу"),
        ("ADA/USDT:USDT", {"trial_extra": {"strategy": None}}, "правило моделі допуску"),
        ("DOGE/USDT:USDT", {"trial_extra": {"shuffled_labels": True}}, "перемішаними"),
        ("LTC/USDT:USDT", {"trial_extra": {"feature_schema_hash": None}}, "схема фіч"),
        ("BNB/USDT:USDT", {"trial_extra": {"feature_schema_hash": "sha256:" + "0" * 64}}, "схема фіч"),
    )
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        for index, (symbol, changes, reason) in enumerate(cases):
            world = await _world(
                session, tmp_path, version=f"lgbm-meta-2026-11-03-{130 + index:03d}", symbol=symbol, **changes
            )
            with pytest.raises(ArtifactError, match=reason):
                await _admit(session, world)


async def test_retire_does_not_depend_on_writing_the_metadata_file(  # type: ignore[no-untyped-def]
    conn: AsyncConnection, tmp_path, monkeypatch
) -> None:
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-141")
        await _admit(session, world)
        model = await _candidate(session, tmp_path, world, version="lgbm-meta-2026-11-03-142", dirty=False)
        await promote_to_candidate(session, model_version=model, oos_trades=_oos_trades(world))
        await approve_model(
            session, model_version=model, confirmed_audit_id=await _audit(session, model, confirmed=True)
        )

        def _disk_full(*_args: object, **_kwargs: object) -> None:
            raise OSError("диск заповнений")

        monkeypatch.setattr("app.ml.artifacts._write_json", _disk_full)
        await retire_model(session, model_version=model)
        session.expire_all()
        row = await session.get(ModelArtifact, model)
        assert row is not None
        assert row.status == "RETIRED"
        # Файл лишився APPROVED, але торгівля читає статус з БД.
        on_disk = json.loads((tmp_path / model / METADATA_FILE).read_text(encoding="utf-8"))
        assert on_disk["status"] == "APPROVED"
        loaded = await load_registered_model(session, model)
        assert loaded.db_status == "RETIRED"
        with pytest.raises(ModelNotTradable):
            await require_model_for_start(session, model_version=model, mode="demo")


async def test_status_change_from_another_session_is_seen_not_the_cached_row(  # type: ignore[no-untyped-def]
    conn: AsyncConnection, tmp_path
) -> None:
    async with (
        AsyncSession(bind=conn, expire_on_commit=False) as trading,
        AsyncSession(bind=conn, expire_on_commit=False) as human,
    ):
        world = await _world(trading, tmp_path, version="lgbm-meta-2026-11-03-151")
        await _admit(trading, world)
        model = await _candidate(trading, tmp_path, world, version="lgbm-meta-2026-11-03-152", dirty=False)
        await promote_to_candidate(trading, model_version=model, oos_trades=_oos_trades(world))
        # Торговий процес тримає рядок (сильне посилання: identity map сесії інакше його забуде).
        cached = await require_model_for_start(trading, model_version=model, mode="demo")
        assert cached.status == "CANDIDATE"
        # Людина знімає модель в іншій сесії; торгова сесія тримає рядок CANDIDATE у кеші.
        await retire_model(human, model_version=model)
        with pytest.raises(ModelNotTradable, match="RETIRED"):
            await require_model_for_start(trading, model_version=model, mode="demo")
        assert (await load_registered_model(trading, model)).db_status == "RETIRED"
        assert cached.status == "RETIRED"


async def test_failed_admission_from_another_session_blocks_the_second_opening(  # type: ignore[no-untyped-def]
    conn: AsyncConnection, tmp_path
) -> None:
    async with (
        AsyncSession(bind=conn, expire_on_commit=False) as researcher,
        AsyncSession(bind=conn, expire_on_commit=False) as other,
    ):
        world = await _world(researcher, tmp_path, version="lgbm-meta-2026-11-03-161")
        second = await _open(researcher, world)
        cached = await researcher.get(HoldoutOpening, world.opening_id)
        assert cached is not None
        assert "admission" not in dict(cached.result or {})
        assert (await _admit(other, world, losing=True)).passed is False
        with pytest.raises(ArtifactError, match="уже перевірявся"):
            await _admit(researcher, dataclasses.replace(world, opening_id=second))
        with pytest.raises(ArtifactError, match="невдалий допуск"):
            await retrain_registered_recipe(researcher, (), validation_version=world.evidence, root=tmp_path)


async def test_validation_model_must_precede_the_holdout_and_match_the_g1_recipe(
    conn: AsyncConnection, tmp_path: Path
) -> None:
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        leaky = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-091", train_to=450 * HOUR)
        with pytest.raises(SplitLeakError, match="hold-out"):
            await _admit(session, leaky)
        other_lgbm = {**lgbm_journal_params(), "num_leaves": 15}
        other = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-092", symbol=ETH, trial_lgbm=other_lgbm)
        with pytest.raises(ArtifactError, match="спробі G1"):
            await _admit(session, other)
        stopped = await _world(
            session, tmp_path, version="lgbm-meta-2026-11-03-093", symbol=SOL, trial_extra={"best_iteration": [40, 60]}
        )
        with pytest.raises(ArtifactError, match="ранньою зупинкою"):
            await _admit(session, stopped)
        # Модель, що бачила hold-out, публічно не реєструється: її пише лише перенавчання.
        sneaky = _meta(
            version="lgbm-meta-2026-11-03-094",
            experiment_id=leaky.experiment_id,
            spec=EMA_CROSS,
            symbol=BTC,
            train_range=(0, 500 * HOUR),
            include_holdout=True,
            validated_by=leaky.evidence,
            opening_id=leaky.opening_id,
        )
        write_artifact(
            tmp_path / "lgbm-meta-2026-11-03-094",
            sneaky,
            booster=random_booster(len(feature_columns(())) + 1),
            calibrator=isotonic_calibrator(),
        )
        with pytest.raises(ArtifactError, match="retrain_registered_recipe"):
            await register_model(session, tmp_path / "lgbm-meta-2026-11-03-094")


async def test_holdout_rows_need_a_g1_opening_of_this_experiment(conn: AsyncConnection, tmp_path) -> None:  # type: ignore[no-untyped-def]
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-051")
        foreign = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-052", symbol=ETH)
        sealed = world.split
        rows = (
            SampleRow(symbol=BTC, time_ms=sealed.folds[0].test_start_ms, index=0),
            SampleRow(symbol=BTC, time_ms=sealed.holdout_from_ms - HOUR, index=1),
            SampleRow(symbol=BTC, time_ms=sealed.holdout_from_ms, index=2),
            SampleRow(symbol=BTC, time_ms=sealed.holdout_to_ms - HOUR, index=3),
        )
        with pytest.raises(ArtifactError, match="немає такого відкриття"):
            await rows_for_opened_holdout(session, rows, experiment_id=world.experiment_id, opening_id=999_999)
        rejected = await _open(session, world, result={**_G1, "passed": False})
        with pytest.raises(ArtifactError, match="Gate G1"):
            await rows_for_opened_holdout(session, rows, experiment_id=world.experiment_id, opening_id=rejected)
        visible = await rows_for_opened_holdout(
            session, rows, experiment_id=world.experiment_id, opening_id=world.opening_id
        )
        assert [row.index for row in visible] == [2, 3]
        # Відкриття лінії BTC не відкриває hold-out лінії ETH з тим самим вікном.
        with pytest.raises(SplitLeakError, match="експерименту"):
            await rows_for_opened_holdout(
                session, rows, experiment_id=foreign.experiment_id, opening_id=world.opening_id
            )
        eth_rows = (SampleRow(symbol=ETH, time_ms=sealed.holdout_from_ms, index=9),)
        with pytest.raises(SplitLeakError, match="символу"):
            await rows_for_opened_holdout(
                session, eth_rows, experiment_id=world.experiment_id, opening_id=world.opening_id
            )


async def test_registered_model_must_match_its_files(conn: AsyncConnection, tmp_path) -> None:  # type: ignore[no-untyped-def]
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(session, tmp_path, version="lgbm-meta-2026-11-03-061")
        loaded = await load_registered_model(session, world.evidence)
        assert loaded.db_status == "TRAINED"
        path = loaded.directory / METADATA_FILE
        original = path.read_text(encoding="utf-8")
        # Рецепт у файлі — лише разом із БД.
        tampering: tuple[tuple[str, object], ...] = (
            ("decision_rule", {"min_edge_atr": "0", "min_confidence": "0"}),
            ("cost_model", {**json.loads(original)["cost_model"], "slippage_entry_bps": "123"}),
            ("target_params", {"tp_atr_mult": "3", "sl_atr_mult": "1", "horizon_bars": 4}),
        )
        for key, value in tampering:
            tampered = json.loads(original)
            tampered[key] = value
            path.write_text(json.dumps(tampered), encoding="utf-8")
            with pytest.raises(ArtifactError, match="не збігається з БД"):
                await load_registered_model(session, world.evidence)
        # Статус і коридор у файлі — лише копія: торгівля бере їх з БД.
        forged = json.loads(original)
        forged["status"] = "APPROVED"
        path.write_text(json.dumps(forged), encoding="utf-8")
        reloaded = await load_registered_model(session, world.evidence)
        assert reloaded.db_status == "TRAINED"
        assert reloaded.metadata["status"] == "TRAINED"
        path.write_text(original, encoding="utf-8")
        calibrator = loaded.directory / CALIBRATOR_FILE
        calibrator.write_text(calibrator.read_text(encoding="utf-8").replace("0x", "-0x", 1), encoding="utf-8")
        with pytest.raises(ArtifactError, match="калібратора"):
            await load_registered_model(session, world.evidence)


async def test_retrain_copies_the_recipe_on_all_data_and_only_a_clean_tree_promotes(  # type: ignore[no-untyped-def]
    conn: AsyncConnection, tmp_path, monkeypatch
) -> None:
    def _forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("підбір під час перенавчання")

    monkeypatch.setattr("app.ml.meta_model._fit_search", _forbidden)
    books, split = learning_books()
    target = TargetParams(horizon_bars=4)
    costs = zero_costs()
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world, fit = await _learning_world(session, tmp_path, books, split, version="lgbm-meta-2026-11-03-101")
        evidence_row = await session.get(ModelArtifact, world.evidence)
        assert evidence_row is not None
        evidence_meta = dict(evidence_row.metadata_json)
        # Без допуску перенавчання не стартує.
        with pytest.raises(ArtifactError, match="допуск"):
            await _retrain(session, books, world.evidence, tmp_path)
        await _admit(session, world)
        with pytest.raises(ArtifactError, match="таргет"):
            await _retrain(
                session, books, world.evidence, tmp_path, params=TargetParams(tp_atr_mult=Decimal(3), horizon_bars=4)
            )
        with pytest.raises(ArtifactError, match="витрати"):
            await _retrain(session, books, world.evidence, tmp_path, costs=CostModel())

        monkeypatch.setattr("app.ml.artifacts.read_git_state", lambda _root: GitState(commit="def456", dirty=False))
        seen: list[dict[str, Any]] = []

        def _spy(*args: Any, **kwargs: Any) -> Any:
            seen.append(kwargs)
            return fit_frozen_meta(*args, **kwargs)

        monkeypatch.setattr("app.ml.artifacts.fit_frozen_meta", _spy)
        # Витрати й таргет не передано: навчання бере їх із рецепта, а не дефолтні CostModel() і таргет.
        retrained = await _retrain(session, books, world.evidence, tmp_path, at_ms=split.data_to_ms)
        assert len(seen) == 2
        assert seen[0].get("holdout_grant") is None
        assert seen[1]["holdout_grant"].opening_id == world.opening_id
        for call in seen:
            assert call["costs"] == costs
            assert call["params"] == target
            assert call["seed"] == 7
            assert call["spec"] == BREAKOUT
            assert call["split"] == split
        again = await _retrain(session, books, world.evidence, tmp_path, at_ms=split.data_to_ms)
        assert retrained != again
        row = await session.get(ModelArtifact, retrained)
        assert row is not None
        assert row.status == "TRAINED"
        assert row.validated_by == world.evidence
        assert row.git_commit == "def456"
        assert row.git_dirty is False
        meta = dict(row.metadata_json)
        assert meta["recipe"]["include_holdout"] is True
        assert meta["recipe"]["lgbm"] == evidence_meta["recipe"]["lgbm"]
        assert meta["recipe"]["train_rows"] > fit.n_rows
        assert meta["train_range_ms"] == [split.data_from_ms, split.data_to_ms]
        assert meta["validated_opening_id"] == world.opening_id
        assert meta["reproducibility"]["data_hash"].startswith("sha256:")
        for key in ("decision_rule", "cost_model", "target_params", "feature_schema_hash", "primary_strategy"):
            assert meta[key] == evidence_meta[key]
        evidence_dir = tmp_path / world.evidence
        assert (tmp_path / retrained / CALIBRATOR_FILE).read_bytes() == (evidence_dir / CALIBRATOR_FILE).read_bytes()
        # Той самий рецепт і seed — той самий файл моделі.
        again_row = await session.get(ModelArtifact, again)
        assert again_row is not None
        assert again_row.artifact_sha256 == row.artifact_sha256
        direct = fit_frozen_meta(
            books,
            spec=BREAKOUT,
            split=split,
            trading_symbol=BTC,
            seed=7,
            costs=costs,
            params=target,
            lgbm=fit.lgbm,
            holdout_grant=_grant_holdout(world.opening_id, split.holdout_from_ms, split.holdout_to_ms),
            expected_schema_hash=fit.model.schema_hash,
        )
        assert direct.n_rows == meta["recipe"]["train_rows"]
        reference = tmp_path / "reference.txt"
        booster_of(direct.model).save_model(str(reference))
        assert reference.read_bytes() == (tmp_path / retrained / MODEL_FILE).read_bytes()

        trades = _oos_trades(world)
        await promote_to_candidate(session, model_version=retrained, oos_trades=trades)
        # Брудне дерево не стає чистим артефактом: git модуль читає сам.
        monkeypatch.setattr("app.ml.artifacts.read_git_state", lambda _root: GitState(commit="def456", dirty=True))
        dirty = await _retrain(session, books, world.evidence, tmp_path, at_ms=split.data_to_ms)
        dirty_row = await session.get(ModelArtifact, dirty)
        assert dirty_row is not None
        assert dirty_row.git_dirty is True
        with pytest.raises(ArtifactError, match="git_dirty"):
            await promote_to_candidate(session, model_version=dirty, oos_trades=trades)
        # Перенавчена модель не стає моделлю допуску для наступного перенавчання.
        with pytest.raises(ArtifactError, match="не бачила hold-out"):
            await _retrain(session, books, retrained, tmp_path)


async def test_retrain_refuses_a_validation_model_the_recipe_does_not_reproduce(  # type: ignore[no-untyped-def]
    conn: AsyncConnection, tmp_path
) -> None:
    books, split = learning_books()
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world, _fit = await _learning_world(
            session, tmp_path, books, split, version="lgbm-meta-2026-11-03-111", foreign_booster=True
        )
        await _admit(session, world)
        with pytest.raises(ArtifactError, match="не відтворюється"):
            await _retrain(session, books, world.evidence, tmp_path)
        retrained = await session.scalar(
            select(ModelArtifact.model_version).where(ModelArtifact.validated_by == world.evidence)
        )
        assert retrained is None


async def test_rule_only_retrains_without_a_model_file_and_promotes(  # type: ignore[no-untyped-def]
    conn: AsyncConnection, tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr("app.ml.artifacts.read_git_state", lambda _root: GitState(commit="def456", dirty=False))
    books, _learning_split = learning_books()
    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
        world = await _world(session, tmp_path, version="rule-only-2026-11-03-001", label_type="rule_only")
        decision = await _admit(session, world)
        assert decision.passed is True
        assert decision.unfiltered_net is None
        retrained = await _retrain(session, books, world.evidence, tmp_path, at_ms=world.split.data_to_ms)
        row = await session.get(ModelArtifact, retrained)
        assert row is not None
        assert row.artifact_path is None
        assert not (tmp_path / retrained / MODEL_FILE).exists()
        await promote_to_candidate(session, model_version=retrained, oos_trades=_oos_trades(world))
        loaded = await load_registered_model(session, retrained)
        assert loaded.metadata["status"] == "CANDIDATE"
        versions = loaded.metadata["reproducibility"]["library_versions"]
        assert accept_primary(loaded, PositionSide.SHORT, mode="demo", versions=versions) is PositionSide.SHORT
        with pytest.raises(ModelNotTradable):
            accept_primary(loaded, PositionSide.SHORT, mode="live", versions=versions)


async def _retrain(
    session: AsyncSession,
    books: tuple[SymbolHistory, ...],
    evidence: str,
    root: Path,
    *,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    at_ms: int | None = None,
) -> str:
    return await retrain_registered_recipe(
        session,
        books,
        validation_version=evidence,
        root=root,
        costs=costs,
        params=params,
        at_ms=at_ms,
    )


def _split() -> ResearchSplit:
    return build_split(
        data_from_ms=0,
        data_to_ms=500 * HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        min_train_ms=48 * HOUR,
        test_segment_ms=24 * HOUR,
        test_segment_extended_ms=48 * HOUR,
    )


async def _world(
    session: AsyncSession,
    root: Path,
    *,
    version: str,
    symbol: str = BTC,
    label_type: str = "meta",
    train_to: int | None = None,
    trial_lgbm: dict[str, Any] | None = None,
    trial_extra: dict[str, Any] | None = None,
    g1: dict[str, Any] | None = None,
    experiment_extra: dict[str, Any] | None = None,
) -> World:
    """Експеримент на `_split()`, спроба G1 у журналі, модель допуску до hold-out і відкриття."""
    split = _split()
    experiment_id = await _experiment(
        session, split, symbol=symbol, label_type=label_type, spec=EMA_CROSS, seed=1, overrides=experiment_extra
    )
    trial_id = await _trial(
        session, experiment_id, spec=EMA_CROSS, label_type=label_type, lgbm=trial_lgbm, extra=trial_extra
    )
    meta = _meta(
        version=version,
        experiment_id=experiment_id,
        spec=EMA_CROSS,
        symbol=symbol,
        label_type=label_type,
        train_range=(0, split.holdout_from_ms if train_to is None else train_to),
    )
    meta_only = label_type == "meta"
    write_artifact(
        root / version,
        meta,
        booster=random_booster(len(feature_columns(())) + 1) if meta_only else None,
        calibrator=isotonic_calibrator() if meta_only else None,
    )
    evidence = await register_model(session, root / version)
    world = World(
        experiment_id=experiment_id,
        trial_id=trial_id,
        evidence=evidence,
        opening_id=0,
        split=split,
        symbol=symbol,
    )
    opening = await _open(session, world, result={**_G1, **(g1 or {})})
    return dataclasses.replace(world, opening_id=opening)


async def _learning_world(
    session: AsyncSession,
    root: Path,
    books: tuple[SymbolHistory, ...],
    split: ResearchSplit,
    *,
    version: str,
    foreign_booster: bool = False,
) -> tuple[World, Any]:
    """Світ на синтетичній історії: модель допуску — справжній `fit_frozen_meta` без hold-out."""
    experiment_id = await _experiment(
        session, split, symbol=BTC, label_type="meta", spec=BREAKOUT, seed=7, costs=zero_costs()
    )
    target = TargetParams(horizon_bars=4)
    fit = fit_frozen_meta(
        books, spec=BREAKOUT, split=split, trading_symbol=BTC, seed=7, costs=zero_costs(), params=target
    )
    trial_id = await _trial(
        session,
        experiment_id,
        spec=BREAKOUT,
        label_type="meta",
        extra={"feature_schema_hash": fit.model.schema_hash},
    )
    meta = build_metadata(
        model_version=version,
        label_type="meta",
        spec=BREAKOUT,
        trade_symbol=BTC,
        train_symbols=(BTC,),
        feature_columns=fit.model.feature_columns,
        target=target,
        costs=zero_costs(),
        rule=DecisionRule(),
        experiment_id=experiment_id,
        seed=7,
        data_hash="sha256:" + "ef" * 32,
        git=GitState(commit="abc123", dirty=False),
        library_versions=current_library_versions(),
        train_range_ms=(split.data_from_ms, split.holdout_from_ms),
        recipe_lgbm=fit.lgbm,
        coverage_ms=fit.coverage_ms,
        train_rows=fit.n_rows,
        include_holdout=False,
        calibrator_method="isotonic",
    )
    booster = random_booster(len(fit.model.columns)) if foreign_booster else booster_of(fit.model)
    write_artifact(root / version, meta, booster=booster, calibrator=isotonic_calibrator())
    evidence = await register_model(session, root / version)
    world = World(
        experiment_id=experiment_id, trial_id=trial_id, evidence=evidence, opening_id=0, split=split, symbol=BTC
    )
    opening = await _open(session, world, result=dict(_G1))
    return dataclasses.replace(world, opening_id=opening), fit


async def _experiment(
    session: AsyncSession,
    split: ResearchSplit,
    *,
    symbol: str,
    label_type: str,
    spec: StrategySpec,
    seed: int,
    costs: CostModel | None = None,
    overrides: dict[str, Any] | None = None,
) -> str:
    """Експеримент з таргетом і витратами в журналі, як пишуть `run_meta_experiment` і Optuna.

    `overrides` замінює ключі `params`; `None` прибирає ключ (журнал без таргета чи витрат).
    """
    params: dict[str, Any] = {
        "stage": "meta_model" if label_type == "meta" else "rule",
        "target": target_payload(TargetParams(horizon_bars=split.horizon_bars)),
        "costs": costs_payload(CostModel() if costs is None else costs),
    }
    for key, value in (overrides or {}).items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = value
    return await create_experiment(
        session,
        split=split,
        research_line=symbol,
        symbols=(symbol,),
        label_type=label_type,
        primary_strategy=spec.metadata(),
        params=params,
        seed=seed,
    )


async def _trial(
    session: AsyncSession,
    experiment_id: str,
    *,
    spec: StrategySpec,
    label_type: str,
    lgbm: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> int:
    """Спроба G1 у журналі: 80 угод walk-forward з R = 1 (ті самі, що `walk_forward_trades`).

    Для `meta` — рецепт LightGBM, `shuffled_labels=false` і правило входу (`decision_payload`), як його
    писатиме T2.12. `extra` замінює ключі; `None` прибирає ключ.
    """
    forward = walk_forward_trades()
    params: dict[str, Any] = {"strategy": spec.metadata()}
    if label_type == "meta":
        params["lgbm"] = _lgbm_payload(lgbm_journal_params() if lgbm is None else lgbm)
        params["shuffled_labels"] = False
        params["decision"] = decision_payload(DecisionRule(), isotonic_calibrator())
        params["feature_schema_hash"] = feature_schema_hash(feature_columns(()))
    for key, value in (extra or {}).items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = value
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


def _meta(
    *,
    version: str,
    experiment_id: str,
    spec: StrategySpec,
    symbol: str,
    train_range: tuple[int, int],
    label_type: str = "meta",
    include_holdout: bool = False,
    validated_by: str | None = None,
    opening_id: int | None = None,
    dirty: bool = False,
) -> dict[str, Any]:
    meta_only = label_type == "meta"
    return build_metadata(
        model_version=version,
        label_type=label_type,
        spec=spec,
        trade_symbol=symbol,
        train_symbols=(symbol,),
        feature_columns=feature_columns(()),
        target=TargetParams(horizon_bars=4),
        costs=CostModel(),
        rule=DecisionRule(),
        experiment_id=experiment_id,
        seed=1,
        data_hash="sha256:" + "cd" * 32,
        git=GitState(commit="abc123", dirty=dirty),
        library_versions=current_library_versions(),
        train_range_ms=train_range,
        recipe_lgbm=lgbm_journal_params() if meta_only else None,
        coverage_ms=(0, 396 * HOUR),
        train_rows=12,
        include_holdout=include_holdout,
        calibrator_method="isotonic" if meta_only else None,
        validated_by=validated_by,
        validated_opening_id=opening_id,
    )


async def _candidate(session: AsyncSession, root: Path, world: World, *, version: str, dirty: bool) -> str:
    """Перенавчена модель того самого рецепта, зареєстрована з грантом відкриття (як у перенавчанні)."""
    meta = _meta(
        version=version,
        experiment_id=world.experiment_id,
        spec=EMA_CROSS,
        symbol=world.symbol,
        train_range=(0, world.split.data_to_ms),
        include_holdout=True,
        validated_by=world.evidence,
        opening_id=world.opening_id,
        dirty=dirty,
    )
    write_artifact(
        root / version,
        meta,
        booster=random_booster(len(feature_columns(())) + 1),
        calibrator=isotonic_calibrator(),
    )
    grant = _grant_holdout(world.opening_id, world.split.holdout_from_ms, world.split.holdout_to_ms)
    return await _register_retrained(session, root / version, grant=grant)


async def _open(session: AsyncSession, world: World, *, result: dict[str, Any] | None = None) -> int:
    payload = dict(_G1 if result is None else result)
    payload["trial_id"] = world.trial_id
    opening = await _open_holdout(
        session,
        research_line=world.symbol,
        holdout_from_ms=world.split.holdout_from_ms,
        holdout_to_ms=world.split.holdout_to_ms,
        experiment_id=world.experiment_id,
        model_version=world.evidence,
        result=payload,
    )
    return int(opening.opening_id)


async def _admit(
    session: AsyncSession,
    world: World,
    *,
    losing: bool = False,
    drawdown: Decimal = Decimal("0.04"),
    seed: int | None = None,
    walk_forward: tuple[BacktestTrade, ...] | None = None,
) -> Any:
    """Допуск зі звітів: 30 угод фільтра і правило без фільтра з п'ятьма зайвими збитками."""
    experiment = await session.get(Experiment, world.experiment_id)
    assert experiment is not None
    run_seed = experiment.seed if seed is None else seed
    start = world.split.holdout_from_ms
    filtered = holdout_trades(start=start, loss=Decimal("-5") if losing else Decimal("-1"))
    report = _with_symbol(backtest_report(filtered, drawdown=drawdown, seed=run_seed), world.symbol)
    rule = _with_symbol(backtest_report((*filtered, *extra_losers(start=start)), seed=run_seed), world.symbol)
    meta = experiment.label_type == "meta"
    return await record_holdout_admission(
        session,
        opening_id=world.opening_id,
        report=report,
        walk_forward=walk_forward_trades() if walk_forward is None else walk_forward,
        unfiltered=rule if meta else None,
        scored_unfiltered=rule if meta else None,
    )


def _with_symbol(report: BacktestReport, symbol: str) -> BacktestReport:
    return report.model_copy(update={"symbol": symbol})


def _oos_trades(world: World) -> tuple[BacktestTrade, ...]:
    """Угоди коридора: walk-forward спроби G1 і hold-out допуску, у порядку часу."""
    return walk_forward_trades() + holdout_trades(start=world.split.holdout_from_ms)


async def _audit(session: AsyncSession, target: str, *, confirmed: bool, action: str = "model.approve") -> int:
    inserted = await session.execute(
        insert(AuditLog).values(actor="user", action=action, confirmed=confirmed, target=target).returning(AuditLog.id)
    )
    return int(inserted.scalar_one())
