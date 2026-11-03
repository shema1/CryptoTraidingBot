"""Optuna на синтетичній історії: журнал, hold-out, відновлення, обрізання і скасування (T2.10).

Потрібні тестова БД і LightGBM (libomp). Бюджет тут 1–2 спроби; задача воркера такий бюджет не приймає.
"""

from __future__ import annotations

import asyncio
import importlib
import threading
from decimal import Decimal
from typing import Any

import pytest
from optuna.distributions import BaseDistribution
from optuna.pruners import BasePruner
from optuna.samplers import BaseSampler
from optuna.study import Study
from optuna.trial import FrozenTrial
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from app.ml.features import HOUR_MS
from app.ml.models import Experiment, ExperimentTrial, HoldoutOpening
from app.ml.optuna_search import run_optuna_study
from app.ml.statistics import deflated_sharpe, trial_series_from_journal
from app.ml.strategy import BREAKOUT
from app.ml.trade_sim import SimCandle, TargetParams
from app.ml.validation import ResearchSplit, build_split, line_trial_count


def _lightgbm_loads() -> bool:
    try:
        importlib.import_module("lightgbm")
    except OSError:
        return False
    return True


pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
    pytest.mark.asyncio,
    pytest.mark.skipif(not _lightgbm_loads(), reason="потрібен libomp, щоб імпортувати LightGBM"),
]


class _Pinned(BaseSampler):
    """Точка, яку вміщує короткий синтетичний train: 50 рядків у листі, 200 дерев."""

    def infer_relative_search_space(self, study: Study, trial: FrozenTrial) -> dict[str, BaseDistribution]:
        return {}

    def sample_relative(
        self, study: Study, trial: FrozenTrial, search_space: dict[str, BaseDistribution]
    ) -> dict[str, Any]:
        return {}

    def sample_independent(
        self, study: Study, trial: FrozenTrial, param_name: str, param_distribution: BaseDistribution
    ) -> Any:
        return {
            "num_leaves": 8,
            "max_depth": 3,
            "learning_rate": 0.05,
            "n_estimators": 200,
            "min_child_samples": 50,
            "subsample": 1.0,
            "colsample_bytree": 1.0,
            "reg_lambda": 0.0,
        }[param_name]


class _PruneImmediately(BasePruner):
    def prune(self, study: Study, trial: FrozenTrial) -> bool:
        return trial.last_step is not None and trial.last_step >= 0


def _sessions(conn: AsyncConnection) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")


def _meta() -> Any:
    from tests.ml import test_meta

    return test_meta


def _one_fold() -> tuple[list[SimCandle], ResearchSplit]:
    meta = _meta()
    count = 5557
    candles = meta._flat(count)
    meta._paint(candles, 90, good=lambda step: step % 2 == 0)
    split = build_split(
        data_from_ms=0,
        data_to_ms=count * HOUR_MS,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        min_train_ms=meta._entry_index(70) * HOUR_MS,
        test_segment_ms=8 * meta._SPACING * HOUR_MS,
        test_segment_extended_ms=12 * meta._SPACING * HOUR_MS,
        min_trades_per_fold=1,
    )
    assert len(split.folds) == 1
    return candles, split


def _two_folds() -> tuple[list[SimCandle], ResearchSplit]:
    meta = _meta()
    count = 5057
    candles = meta._flat(count)
    meta._paint(candles, 80, good=lambda step: step % 2 == 0)
    split = build_split(
        data_from_ms=0,
        data_to_ms=count * HOUR_MS,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        min_train_ms=meta._entry_index(60) * HOUR_MS,
        test_segment_ms=4 * meta._SPACING * HOUR_MS,
        test_segment_extended_ms=8 * meta._SPACING * HOUR_MS,
        min_trades_per_fold=1,
    )
    assert len(split.folds) == 2
    return candles, split


async def test_trial_ignores_holdout_and_resume_does_not_duplicate(conn: AsyncConnection) -> None:
    meta = _meta()
    candles, split = _one_fold()
    sessions = _sessions(conn)
    books = (meta._history(meta.BTC, candles),)
    first = await run_optuna_study(
        sessions,
        books,
        spec=BREAKOUT,
        split=split,
        trading_symbol=meta.BTC,
        limits=meta._wide_limits(),
        initial_equity=meta._EQUITY,
        maintenance_margin_rate=meta._MARGIN,
        seed=meta._SEED,
        n_trials=1,
        costs=meta._zero_costs(),
        params=TargetParams(horizon_bars=4),
        trial_bounds=(1, 2),
        sampler=_Pinned(),
    )
    assert first.trials_recorded == 1
    assert first.stopped is False
    async with sessions() as session:
        trial = (
            await session.execute(select(ExperimentTrial).where(ExperimentTrial.experiment_id == first.experiment_id))
        ).scalar_one()
        openings = (await session.execute(select(func.count()).select_from(HoldoutOpening))).scalar_one()
    assert trial.state == "COMPLETE"
    assert openings == 0
    assert trial.oos_entry_ms
    assert all(entry < split.holdout_from_ms for entry in trial.oos_entry_ms)
    mutated = list(candles)
    hold_index = split.holdout_from_ms // HOUR_MS
    mutated[hold_index] = SimCandle(
        open_time_ms=hold_index * HOUR_MS,
        open=Decimal(50),
        high=Decimal(80),
        low=Decimal(40),
        close=Decimal(70),
    )
    again = await run_optuna_study(
        sessions,
        (meta._history(meta.BTC, mutated),),
        spec=BREAKOUT,
        split=split,
        trading_symbol=meta.BTC,
        limits=meta._wide_limits(),
        initial_equity=meta._EQUITY,
        maintenance_margin_rate=meta._MARGIN,
        seed=meta._SEED,
        n_trials=2,
        costs=meta._zero_costs(),
        params=TargetParams(horizon_bars=4),
        experiment_id=first.experiment_id,
        trial_bounds=(1, 2),
        sampler=_Pinned(),
    )
    assert again.experiment_id == first.experiment_id
    assert again.trials_recorded == 1
    async with sessions() as session:
        rows = (
            (
                await session.execute(
                    select(ExperimentTrial)
                    .where(ExperimentTrial.experiment_id == first.experiment_id)
                    .order_by(ExperimentTrial.trial_no)
                )
            )
            .scalars()
            .all()
        )
        counted = await line_trial_count(session, meta.BTC)
    assert [row.params["optuna"]["number"] for row in rows] == [0, 1]
    assert counted == 2
    assert rows[0].oos_results == rows[1].oos_results
    series = [trial_series_from_journal(row.oos_results, row.oos_entry_ms, row.metrics).returns for row in rows]
    estimate = deflated_sharpe(series[0], trial_returns=series, n_trials=counted)
    assert estimate.n_trials == 2


async def test_cancel_during_training_stops_after_the_current_fold(
    conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Скасування не лишає потік учити решту фолдів і не пише перервану спробу."""
    meta = _meta()
    candles, split = _two_folds()
    sessions = _sessions(conn)
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    seen: dict[str, int] = {}

    def gated(train_meta: Any, histories: Any, **kwargs: Any) -> Any:
        loop.call_soon_threadsafe(started.set)
        release.wait(timeout=60)
        try:
            run = train_meta(histories, **kwargs)
            seen["folds"] = len(run.folds)
            return run
        finally:
            finished.set()

    async def train(train_meta: Any, histories: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(gated, train_meta, histories, **kwargs)

    monkeypatch.setattr("app.ml.optuna_search._train", train)
    task = asyncio.create_task(
        run_optuna_study(
            sessions,
            (meta._history(meta.BTC, candles),),
            spec=BREAKOUT,
            split=split,
            trading_symbol=meta.BTC,
            limits=meta._wide_limits(),
            initial_equity=meta._EQUITY,
            maintenance_margin_rate=meta._MARGIN,
            seed=meta._SEED,
            n_trials=1,
            costs=meta._zero_costs(),
            params=TargetParams(horizon_bars=4),
            trial_bounds=(1, 1),
            sampler=_Pinned(),
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # Навчання ще не почалось: зупинка вже стоїть, тож потік довчить лише перший фолд.
    release.set()
    assert await asyncio.to_thread(finished.wait, 120)
    assert len(split.folds) == 2
    assert seen == {"folds": 1}
    async with sessions() as session:
        trials = (await session.execute(select(func.count()).select_from(ExperimentTrial))).scalar_one()
        experiments = (await session.execute(select(func.count()).select_from(Experiment))).scalar_one()
    assert experiments == 1
    assert trials == 0


async def test_pruned_prefix_still_counts_in_the_line(conn: AsyncConnection) -> None:
    meta = _meta()
    candles, split = _two_folds()
    sessions = _sessions(conn)
    result = await run_optuna_study(
        sessions,
        (meta._history(meta.BTC, candles),),
        spec=BREAKOUT,
        split=split,
        trading_symbol=meta.BTC,
        limits=meta._wide_limits(),
        initial_equity=meta._EQUITY,
        maintenance_margin_rate=meta._MARGIN,
        seed=meta._SEED,
        n_trials=1,
        costs=meta._zero_costs(),
        params=TargetParams(horizon_bars=4),
        trial_bounds=(1, 1),
        sampler=_Pinned(),
        pruner=_PruneImmediately(),
    )
    async with sessions() as session:
        trial = (
            await session.execute(select(ExperimentTrial).where(ExperimentTrial.experiment_id == result.experiment_id))
        ).scalar_one()
        experiment = (
            await session.execute(select(Experiment).where(Experiment.id == result.experiment_id))
        ).scalar_one()
        counted = await line_trial_count(session, meta.BTC)
    assert trial.state == "PRUNED"
    assert len(trial.metrics["folds"]) == 1
    assert experiment.primary_strategy == BREAKOUT.metadata()
    assert counted == 1
    series = [trial_series_from_journal(trial.oos_results, trial.oos_entry_ms, trial.metrics).returns]
    estimate = deflated_sharpe(series[0], trial_returns=series, n_trials=counted)
    assert estimate.n_trials == 1
