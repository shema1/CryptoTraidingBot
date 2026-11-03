"""Задача Optuna: черга, скасування, продовження і лічильник лінії без навчання (T2.10)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from app.jobs.models import Job
from app.jobs.worker import (
    ERROR_BAD_PARAMS,
    ERROR_CANCELED,
    ERROR_HANDLER,
    ERROR_UNKNOWN_KIND,
    cancel_job,
    run_worker,
)
from app.ml.models import ExperimentTrial, HoldoutOpening
from app.ml.optuna_search import (
    OptunaInputs,
    OptunaJobRequest,
    OptunaResult,
    enqueue_optuna_job,
    parse_optuna_job,
    resume_optuna_job,
)
from app.ml.statistics import deflated_sharpe, trial_series_from_journal
from app.ml.strategy import BREAKOUT
from app.ml.validation import build_split, create_experiment, line_trial_count, record_trial
from tests.exchange.factories import make_fake

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_HOUR = 3_600_000
BTC = "BTC/USDT:USDT"


def _sessions(conn: AsyncConnection) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")


def _request() -> OptunaJobRequest:
    return parse_optuna_job(
        {
            "strategy": "breakout",
            "n_trials": 100,
            "seed": 7,
            "symbols": [BTC],
            "trading_symbol": BTC,
            "initial_equity": "10000",
            "maintenance_margin_rate": "0.005",
            "min_equity_usd": "1000",
            "max_notional_per_symbol_usd": "5000",
        }
    )


def _split() -> Any:
    return build_split(
        data_from_ms=0,
        data_to_ms=30_000 * _HOUR,
        horizon_bars=24,
        count_trades=lambda _start, _end: 20,
    )


def _worker_sleep(stop: asyncio.Event) -> Any:
    """Перший виклик — heartbeat: він не чіпає БД, доки обробник не завершиться. Наступний зупиняє цикл."""

    state = {"heartbeat": True}

    async def sleep(_seconds: float) -> None:
        if state["heartbeat"]:
            state["heartbeat"] = False
            await asyncio.Event().wait()
        stop.set()

    return sleep


async def test_queued_job_can_be_canceled_and_resumed_without_an_experiment(conn: AsyncConnection) -> None:
    sessions = _sessions(conn)
    async with sessions() as session, session.begin():
        job_id = await enqueue_optuna_job(session, _request())
        again = await enqueue_optuna_job(session, _request())
    assert again == job_id
    async with sessions() as session, session.begin():
        assert await cancel_job(session, job_id)
        job = await session.get(Job, job_id)
        assert job is not None
        assert job.status == "CANCELED"
        assert job.error == ERROR_CANCELED
        resumed = await resume_optuna_job(session, job_id)
    assert resumed != job_id
    async with sessions() as session:
        rows = (await session.execute(select(func.count()).select_from(ExperimentTrial))).scalar_one()
        active = await session.get(Job, resumed)
    assert rows == 0
    assert active is not None
    assert active.status == "QUEUED"
    assert "experiment_id" not in active.params


async def test_empty_history_fails_as_handler_error(conn: AsyncConnection) -> None:
    sessions = _sessions(conn)
    async with sessions() as session, session.begin():
        job_id = await enqueue_optuna_job(session, _request())
    stop = asyncio.Event()
    await run_worker(sessions, make_fake(), stop=stop, sleep=_worker_sleep(stop), poll_ms=1)
    async with sessions() as session:
        job = await session.get(Job, job_id)
    assert job is not None
    assert job.status == "FAILED"
    assert job.error == ERROR_HANDLER
    assert job.error != ERROR_UNKNOWN_KIND
    assert job.result == {"error_type": "OptunaDataError"}


async def test_bad_budget_is_bad_params(conn: AsyncConnection) -> None:
    sessions = _sessions(conn)
    async with sessions() as session, session.begin():
        job = Job(
            kind="optuna_meta",
            priority=300,
            params={**_request_params(), "n_trials": 99},
            dedup_key="optuna:BTC/USDT:USDT",
            progress_message=None,
            result=None,
            error=None,
        )
        session.add(job)
        await session.flush()
        job_id = job.id
    stop = asyncio.Event()
    await run_worker(sessions, make_fake(), stop=stop, sleep=_worker_sleep(stop), poll_ms=1)
    async with sessions() as session:
        stored = await session.get(Job, job_id)
    assert stored is not None
    assert stored.error == ERROR_BAD_PARAMS


async def test_cancel_during_run_keeps_the_trial_and_resume_adds_another(
    conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions = _sessions(conn)
    split = _split()
    seen = {"recorded": 0}

    async def load(_sessions: async_sessionmaker[AsyncSession], request: OptunaJobRequest) -> OptunaInputs:
        return OptunaInputs(histories=(), feature_data={}, split=split)

    async def study(factory: async_sessionmaker[AsyncSession], _histories: object, **kwargs: Any) -> OptunaResult:
        experiment_id = kwargs["experiment_id"]
        if experiment_id is None:
            async with factory() as session, session.begin():
                experiment_id = await create_experiment(
                    session,
                    split=split,
                    research_line=BTC,
                    symbols=(BTC,),
                    label_type="meta",
                    primary_strategy=BREAKOUT.metadata(),
                    params={"stage": "meta_model", "role": "meta"},
                    seed=7,
                )
                await kwargs["on_created"](session, experiment_id)
        number = seen["recorded"]
        async with factory() as session, session.begin():
            await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="FAILED",
                params={
                    "role": "meta",
                    "stage": "meta_model",
                    "strategy": BREAKOUT.metadata(),
                    "lgbm": {"num_leaves": 8},
                    "optuna": {"number": number, "state": "FAILED", "value": None, "steps": []},
                },
                folds=(),
            )
        seen["recorded"] += 1
        if number == 0:
            async with factory() as session, session.begin():
                await cancel_job(session, box["job_id"])
            return OptunaResult(experiment_id=experiment_id, trials_recorded=1, trials_total=1, stopped=True)
        return OptunaResult(experiment_id=experiment_id, trials_recorded=1, trials_total=2, stopped=False)

    monkeypatch.setattr("app.ml.optuna_search.load_optuna_inputs", load)
    monkeypatch.setattr("app.ml.optuna_search.run_optuna_study", study)
    box: dict[str, Any] = {}
    async with sessions() as session, session.begin():
        box["job_id"] = await enqueue_optuna_job(session, _request())
    stop = asyncio.Event()
    await run_worker(sessions, make_fake(), stop=stop, sleep=_worker_sleep(stop), poll_ms=1)
    job_id = box["job_id"]
    async with sessions() as session:
        canceled = await session.get(Job, job_id)
    assert canceled is not None
    assert canceled.status == "CANCELED"
    assert canceled.params["experiment_id"]
    async with sessions() as session, session.begin():
        resumed = await resume_optuna_job(session, job_id)
    stop2 = asyncio.Event()
    await run_worker(sessions, make_fake(), stop=stop2, sleep=_worker_sleep(stop2), poll_ms=1)
    async with sessions() as session:
        counted = await line_trial_count(session, BTC)
        rows = (await session.execute(select(ExperimentTrial).order_by(ExperimentTrial.trial_no))).scalars().all()
        openings = (await session.execute(select(func.count()).select_from(HoldoutOpening))).scalar_one()
        finished = await session.get(Job, resumed)
    assert counted == 2
    assert openings == 0
    assert finished is not None
    assert finished.status == "DONE"
    series = [trial_series_from_journal(row.oos_results, row.oos_entry_ms, row.metrics).returns for row in rows]
    assert all(item == () for item in series)
    estimate = deflated_sharpe(series[0], trial_returns=series, n_trials=counted)
    assert estimate.n_trials == 2


def _request_params() -> dict[str, object]:
    request = _request()
    return {
        "strategy": request.strategy.name,
        "n_trials": request.n_trials,
        "seed": request.seed,
        "symbols": list(request.symbols),
        "trading_symbol": request.trading_symbol,
        "initial_equity": format(request.initial_equity, "f"),
        "maintenance_margin_rate": format(request.maintenance_margin_rate, "f"),
        "min_equity_usd": format(request.min_equity_usd, "f"),
        "max_notional_per_symbol_usd": format(request.max_notional_per_symbol_usd, "f"),
    }
