"""Gate G1 — єдиний шлях до hold-out; знімок DSR і PBO пишеться лише разом із відкриттям (ТЗ §5А.10)."""

from __future__ import annotations

from decimal import Decimal

import numpy as np
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.exchange.types import PositionSide
from app.ml.backtest import BacktestMetrics, BacktestReport, BacktestTrade
from app.ml.models import Experiment, ExperimentTrial, HoldoutOpening
from app.ml.statistics import FoldOutcome, GateRejected, GateThresholds, open_holdout_after_gate
from app.ml.trade_sim import ExitReason, StopPriceSource
from app.ml.validation import (
    HOUR_MS,
    FoldRecord,
    LabelStats,
    ResearchSplit,
    build_split,
    create_experiment,
    record_trial,
)
from tests.db.factories import make_model

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
HOUR = HOUR_MS
_LABELS = LabelStats(n_labels=2, n_last_stop=0, n_both_reached_tp=0)


def _split() -> ResearchSplit:
    return build_split(
        data_from_ms=0,
        data_to_ms=500 * HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 1,
        min_train_ms=48 * HOUR,
        test_segment_ms=24 * HOUR,
        test_segment_extended_ms=48 * HOUR,
        min_trades_per_fold=1,
    )


def _trade(entry_ms: int, r_multiple: Decimal, *, entry_index: int) -> BacktestTrade:
    return BacktestTrade(
        side=PositionSide.LONG,
        entry_index=entry_index,
        exit_index=entry_index + 1,
        entry_open_time_ms=entry_ms,
        exit_open_time_ms=entry_ms + HOUR,
        exit_reason=ExitReason.TAKE_PROFIT,
        stop_price_source=StopPriceSource.MARK,
        qty=Decimal("1"),
        net_usd=r_multiple,
        loss_per_unit=Decimal("1"),
        r_multiple=r_multiple,
        risk_usd=Decimal("1"),
        funding_penalty_usd=Decimal("0"),
        funding_gap=False,
    )


def _report(symbol: str, trades: tuple[BacktestTrade, ...]) -> BacktestReport:
    total = sum((trade.net_usd for trade in trades), Decimal(0))
    wins = sum(1 for trade in trades if trade.net_usd > 0)
    loss = sum((-trade.net_usd for trade in trades if trade.net_usd < 0), Decimal(0))
    profit = sum((trade.net_usd for trade in trades if trade.net_usd > 0), Decimal(0))
    return BacktestReport(
        symbol=symbol,
        seed=7,
        cost_factor=Decimal("1"),
        trades=trades,
        decisions=(),
        metrics=BacktestMetrics(
            n_trades=len(trades),
            total_r=total,
            total_net_usd=total,
            profit_factor=None if loss == 0 else profit / loss,
            max_drawdown_pct=Decimal("1"),
            hit_rate=Decimal(wins) / Decimal(len(trades)),
            sharpe=None,
            max_consecutive_losses=0,
            n_incomplete=0,
            n_unusable=0,
            n_funding_gaps=0,
            ending_equity=Decimal("100"),
            peak_equity=Decimal("100"),
            halted=False,
        ),
    )


def _paths(n_trades: int) -> tuple[tuple[Decimal, ...], tuple[Decimal, ...]]:
    """Фіксований ряд: достатній розкид, щоб PSR лишався визначеним, і перевага над шумом."""
    rng = np.random.default_rng(3)
    strong = tuple(Decimal(f"{value:.4f}") for value in rng.normal(0.6, 1, size=n_trades))
    junk = tuple(Decimal(f"{value:.4f}") for value in rng.normal(0, 1, size=n_trades))
    return strong, junk


def _records(split: ResearchSplit, series: tuple[Decimal, ...]) -> list[FoldRecord]:
    if len(series) != len(split.folds) * 2:
        raise ValueError("ряд має містити дві угоди на фолд")
    records: list[FoldRecord] = []
    for index, fold in enumerate(split.folds):
        pair = series[index * 2 : index * 2 + 2]
        btc = tuple(
            _trade(fold.test_start_ms + offset * HOUR, value, entry_index=offset) for offset, value in enumerate(pair)
        )
        eth = (_trade(fold.test_start_ms, Decimal("9"), entry_index=0),)
        records.append(
            FoldRecord(
                reports=(_report(BTC, btc), _report(ETH, eth)),
                train_labels=_LABELS,
                test_labels=_LABELS,
            )
        )
    return records


def _stress(split: ResearchSplit) -> tuple[FoldOutcome, ...]:
    return tuple(
        FoldOutcome(
            n_trades=2,
            total_net_usd=Decimal("1"),
            profit_factor=Decimal("1.1"),
            below_min_trades=False,
            cost_factor=Decimal("2"),
        )
        for _ in split.folds
    )


async def _line(session: AsyncSession, split: ResearchSplit) -> str:
    return await create_experiment(
        session,
        split=split,
        research_line=BTC,
        symbols=(BTC, ETH),
        label_type="meta",
        primary_strategy={"name": "ema_cross"},
        params={},
        seed=7,
    )


async def _strong_and_junk(session: AsyncSession, split: ResearchSplit) -> tuple[str, int, int]:
    strong_r, junk_r = _paths(len(split.folds) * 2)
    experiment_id = await _line(session, split)
    junk = await record_trial(
        session,
        experiment_id=experiment_id,
        split=split,
        state="COMPLETE",
        params={"name": "junk"},
        folds=_records(split, junk_r),
    )
    strong = await record_trial(
        session,
        experiment_id=experiment_id,
        split=split,
        state="COMPLETE",
        params={"name": "strong"},
        folds=_records(split, strong_r),
    )
    return experiment_id, strong.trial_id, junk.trial_id


async def test_rejected_gate_writes_nothing(conn: AsyncConnection) -> None:
    split = _split()
    strong_r, _junk_r = _paths(len(split.folds) * 2)
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, _junk_id = await _strong_and_junk(session, split)
        stored = await session.get(ExperimentTrial, strong_id)
        assert stored is not None
        assert stored.oos_results == [format(value, "f") for value in strong_r]
        assert "9" not in stored.oos_results
        with pytest.raises(GateRejected) as caught:
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=strong_id,
                model_version=await make_model(conn),
                stress=_stress(split),
                unfiltered_total_net=Decimal("0"),
            )
        decision = caught.value.decision
        assert "G1_MIN_TRADES_FOLD" in decision.reasons
        assert "G1_MIN_TRADES_TOTAL" in decision.reasons
        assert decision.n_trials == 2
        assert decision.dsr is not None
        assert decision.dsr >= Decimal("0.95")
        assert decision.pbo is not None
        assert decision.pbo < Decimal("0.5")
        # Знімок без відкриття не пишеться: інакше експеримент показував би DSR спроби, що hold-out не відкривала.
        experiment = await session.get(Experiment, experiment_id)
        assert experiment is not None
        assert experiment.n_trials_line is None
        assert experiment.dsr is None
        assert experiment.pbo is None
        openings = select(func.count()).select_from(HoldoutOpening).where(HoldoutOpening.research_line == BTC)
        assert (await session.execute(openings)).scalar_one() == 0


async def test_later_rejection_keeps_the_snapshot_of_the_opening(conn: AsyncConnection) -> None:
    split = _split()
    loose = GateThresholds(min_trades_per_fold=1, min_oos_trades=1)
    model = await make_model(conn)
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, junk_id = await _strong_and_junk(session, split)
        opened = await open_holdout_after_gate(
            session,
            experiment_id=experiment_id,
            trial_id=strong_id,
            model_version=model,
            stress=_stress(split),
            unfiltered_total_net=Decimal("0"),
            thresholds=loose,
        )
        with pytest.raises(GateRejected):
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=junk_id,
                model_version=model,
                stress=_stress(split),
                unfiltered_total_net=Decimal("0"),
                thresholds=loose,
            )
        row = (await session.execute(select(HoldoutOpening).where(HoldoutOpening.id == opened.opening_id))).scalar_one()
        assert row.result is not None
        assert row.result["trial_id"] == strong_id
        experiment = await session.get(Experiment, experiment_id)
        assert experiment is not None
        assert experiment.n_trials_line == row.result["n_trials_line"] == 2
        assert experiment.dsr == Decimal(row.result["dsr"])
        assert experiment.pbo == Decimal(row.result["pbo"])
        openings = select(func.count()).select_from(HoldoutOpening).where(HoldoutOpening.research_line == BTC)
        assert (await session.execute(openings)).scalar_one() == 1


async def test_passed_gate_opens_only_this_holdout(conn: AsyncConnection) -> None:
    split = _split()
    model = await make_model(conn)
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, _junk_id = await _strong_and_junk(session, split)
        opened = await open_holdout_after_gate(
            session,
            experiment_id=experiment_id,
            trial_id=strong_id,
            model_version=model,
            stress=_stress(split),
            unfiltered_total_net=Decimal("0"),
            thresholds=GateThresholds(min_trades_per_fold=1, min_oos_trades=1),
        )
        assert opened.opening_no == 1
        row = (await session.execute(select(HoldoutOpening).where(HoldoutOpening.id == opened.opening_id))).scalar_one()
        result = row.result
        assert result is not None
        assert result["gate"] == "G1"
        assert result["passed"] is True
        assert result["trial_id"] == strong_id
        experiment = await session.get(Experiment, experiment_id)
        assert experiment is not None
        assert experiment.n_trials_line == result["n_trials_line"] == 2
        assert experiment.dsr is not None
        assert experiment.dsr >= Decimal("0.95")
        assert experiment.pbo is not None
        assert experiment.pbo < Decimal("0.5")
        assert Decimal(result["dsr"]) == experiment.dsr
        assert Decimal(result["pbo"]) == experiment.pbo
        # Запис відкриття показує, з якими порогами і яким стресом пройдено G1.
        assert result["thresholds"]["dsr_min"] == "0.95"
        assert result["thresholds"]["min_trades_per_fold"] == 1
        assert result["thresholds"]["stress_cost_factor"] == "2"
        assert result["loosened"] == ["min_trades_per_fold", "min_oos_trades"]
        assert result["n_trades"] == len(split.folds) * 2
        assert result["unfiltered_total_net"] == "0"
        assert len(result["stress"]) == len(split.folds)
        assert result["stress"][0] == {
            "n_trades": 2,
            "total_net_usd": "1",
            "profit_factor": "1.1",
            "below_min_trades": False,
            "cost_factor": "2",
        }


async def test_incomplete_or_foreign_trial_does_not_snapshot(conn: AsyncConnection) -> None:
    split = _split()
    async with AsyncSession(bind=conn) as session:
        experiment_id = await _line(session, split)
        failed = await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="FAILED",
            params={"name": "failed"},
        )
        with pytest.raises(ValueError, match="COMPLETE"):
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=failed.trial_id,
                model_version="unused",
                stress=(),
            )
        with pytest.raises(ValueError, match="не належить"):
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=failed.trial_id + 1000,
                model_version="unused",
                stress=(),
            )
        experiment = await session.get(Experiment, experiment_id)
        assert experiment is not None
        assert experiment.n_trials_line is None
        assert experiment.dsr is None
        assert experiment.pbo is None
