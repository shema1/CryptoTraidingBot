"""Gate G1 — єдиний шлях до hold-out; знімок DSR і PBO пишеться лише разом із відкриттям (ТЗ §5А.10)."""

from __future__ import annotations

from decimal import Decimal

import numpy as np
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.api.models import AUDIT_ACTION_TRIAL_INVALIDATE
from app.exchange.types import PositionSide
from app.ml.backtest import BacktestDecision, BacktestMetrics, BacktestReport, BacktestTrade
from app.ml.models import Experiment, ExperimentTrial, HoldoutOpening
from app.ml.statistics import FoldOutcome, GateRejected, GateThresholds, open_holdout_after_gate
from app.ml.strategy import ALWAYS_FLAT
from app.ml.trade_sim import ExitReason, StopPriceSource
from app.ml.validation import (
    HOUR_MS,
    FoldRecord,
    LabelStats,
    ResearchSplit,
    build_split,
    create_experiment,
    invalidate_trial,
    record_trial,
)
from tests.db.factories import make_audit, make_model

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
HOUR = HOUR_MS
_LABELS = LabelStats(n_labels=2, n_last_stop=0, n_both_reached_tp=0)


def _split(*, min_train_ms: int = 48 * HOUR) -> ResearchSplit:
    # 47 добових фолдів: PBO ділить відрізок walk-forward на 16 блоків по добі.
    return build_split(
        data_from_ms=0,
        data_to_ms=1500 * HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 1,
        min_train_ms=min_train_ms,
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


def _decision(trade: BacktestTrade, decision: str) -> BacktestDecision:
    return BacktestDecision(
        ts_ms=trade.entry_open_time_ms,
        entry_index=trade.entry_index,
        stage="RISK",
        decision=decision,
        reason_code=None if decision == "ENTER" else "RISK_COOLDOWN",
        side=trade.side,
    )


def _report(symbol: str, trades: tuple[BacktestTrade, ...]) -> BacktestReport:
    """Кожна угода — рішення ENTER; ще один сигнал фолду відкинуто cooldown."""
    total = sum((trade.net_usd for trade in trades), Decimal(0))
    wins = sum(1 for trade in trades if trade.net_usd > 0)
    loss = sum((-trade.net_usd for trade in trades if trade.net_usd < 0), Decimal(0))
    profit = sum((trade.net_usd for trade in trades if trade.net_usd > 0), Decimal(0))
    return BacktestReport(
        symbol=symbol,
        seed=7,
        cost_factor=Decimal("1"),
        trades=trades,
        decisions=(*(_decision(trade, "ENTER") for trade in trades), _decision(trades[-1], "SKIP")),
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
        # Подвоєні витрати увімкнули cooldown: виконано одну угоду з тих самих трьох сигналів.
        FoldOutcome(
            n_trades=1,
            n_signals=3,
            total_net_usd=Decimal("1"),
            profit_factor=Decimal("1.1"),
            below_min_trades=False,
            cost_factor=Decimal("2"),
        )
        for _ in split.folds
    )


async def _line(session: AsyncSession, split: ResearchSplit, *, research_line: str = BTC) -> str:
    return await create_experiment(
        session,
        split=split,
        research_line=research_line,
        symbols=(BTC, ETH),
        label_type="meta",
        primary_strategy={"name": "ema_cross"},
        params={},
        seed=7,
    )


async def _model(conn: AsyncConnection, experiment_id: str) -> str:
    """Модель рецепта цього експерименту: meta, BTC, 1h — як у `_line`."""
    return await make_model(conn, experiment_id=experiment_id)


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
        assert stored.oos_entry_ms == [fold.test_start_ms + offset * HOUR for fold in split.folds for offset in (0, 1)]
        assert stored.metrics["folds"][0]["trading"]["n_signals"] == 3
        with pytest.raises(GateRejected) as caught:
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=strong_id,
                model_version=await _model(conn, experiment_id),
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
        # Інший поріг угод G1 (тут слабший — 50 замість 100) не змінює розкид Sharpe для DSR: DSR той самий.
        with pytest.raises(GateRejected) as other:
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=strong_id,
                model_version=await _model(conn, experiment_id),
                stress=_stress(split),
                unfiltered_total_net=Decimal("0"),
                thresholds=GateThresholds(min_oos_trades=50),
            )
        assert "G1_MIN_TRADES_TOTAL" not in other.value.decision.reasons
        assert other.value.decision.dsr == decision.dsr
        # Знімок без відкриття не пишеться: інакше експеримент показував би DSR спроби, що hold-out не відкривала.
        experiment = await session.get(Experiment, experiment_id)
        assert experiment is not None
        assert experiment.n_trials_line is None
        assert experiment.dsr is None
        assert experiment.pbo is None
        openings = select(func.count()).select_from(HoldoutOpening).where(HoldoutOpening.research_line == BTC)
        assert (await session.execute(openings)).scalar_one() == 0


async def test_n_counts_every_trial_of_the_line_and_only_it(conn: AsyncConnection) -> None:
    split = _split()
    _strong_r, junk_r = _paths(len(split.folds) * 2)
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, _junk_id = await _strong_and_junk(session, split)
        model = await _model(conn, experiment_id)
        # Невдала і обрізана спроби цього експерименту — у N. Обрізана покрила 3 доби: у жодному
        # розбитті PBO в неї немає двох діб в обох половинах, тож вона не змагається.
        await record_trial(session, experiment_id=experiment_id, split=split, state="FAILED", params={"name": "failed"})
        await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="PRUNED",
            params={"name": "pruned"},
            folds=_records(split, junk_r)[:3],
        )
        # Інший експеримент тієї самої лінії — у N і в PBO.
        sibling = await _line(session, split)
        await record_trial(
            session,
            experiment_id=sibling,
            split=split,
            state="COMPLETE",
            params={"name": "sibling"},
            folds=_records(split, junk_r),
        )
        # Інша лінія (торговий символ ETH) у N цієї лінії не входить.
        other_line = await _line(session, split, research_line=ETH)
        for name in ("eth-1", "eth-2"):
            await record_trial(
                session,
                experiment_id=other_line,
                split=split,
                state="COMPLETE",
                params={"name": name},
                folds=_records(split, junk_r),
            )
        with pytest.raises(GateRejected) as caught:
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=strong_id,
                model_version=model,
                stress=_stress(split),
                unfiltered_total_net=Decimal("0"),
            )
        assert caught.value.decision.n_trials == 5
        opened = await open_holdout_after_gate(
            session,
            experiment_id=experiment_id,
            trial_id=strong_id,
            model_version=model,
            stress=_stress(split),
            unfiltered_total_net=Decimal("0"),
            thresholds=GateThresholds(min_trades_per_fold=1, min_oos_trades=1),
        )
        row = (await session.execute(select(HoldoutOpening).where(HoldoutOpening.id == opened.opening_id))).scalar_one()
        result = row.result
        assert result is not None
        assert result["n_trials_line"] == 5
        # Розкид DSR — з рядів ≥ 30 угод: кандидат, junk і сусідня спроба (по 94); обрізана (6) — ні.
        assert result["dsr_spread_trials"] == 3
        assert result["dsr_spread_floor"] is False
        assert Decimal(result["dsr_benchmark_sharpe"]) > 0
        # PBO: кандидат, junk і спроба сусіднього експерименту; 32 доби з 47 (кратно 16), усі розбиття.
        assert result["pbo_strategies"] == 3
        assert result["pbo_splits"] == 12870
        start = split.folds[0].test_start_ms
        assert (result["pbo_span_from_ms"], result["pbo_span_to_ms"]) == (start, start + 32 * 24 * HOUR)


async def test_shifted_geometry_does_not_shed_rivals(conn: AsyncConnection) -> None:
    old_split = _split()
    # Той самий запечатаний hold-out, але walk-forward починається на добу раніше.
    shifted = _split(min_train_ms=24 * HOUR)
    _old_r, junk_r = _paths(len(old_split.folds) * 2)
    strong_r, _junk = _paths(len(shifted.folds) * 2)
    async with AsyncSession(bind=conn) as session:
        old = await _line(session, old_split)
        for name, series in (("junk", junk_r), ("mirror", tuple(-value for value in junk_r))):
            await record_trial(
                session,
                experiment_id=old,
                split=old_split,
                state="COMPLETE",
                params={"name": name},
                folds=_records(old_split, series),
            )
        moved = await _line(session, shifted)
        model = await _model(conn, moved)
        candidate = await record_trial(
            session,
            experiment_id=moved,
            split=shifted,
            state="COMPLETE",
            params={"name": "strong"},
            folds=_records(shifted, strong_r),
        )
        opened = await open_holdout_after_gate(
            session,
            experiment_id=moved,
            trial_id=candidate.trial_id,
            model_version=model,
            stress=_stress(shifted),
            unfiltered_total_net=Decimal("0"),
            thresholds=GateThresholds(min_trades_per_fold=1, min_oos_trades=1),
        )
        row = (await session.execute(select(HoldoutOpening).where(HoldoutOpening.id == opened.opening_id))).scalar_one()
        result = row.result
        assert result is not None
        # Відрізок PBO — walk-forward кандидата; старі спроби змагаються на добах, які покрили.
        assert result["pbo_strategies"] == 3
        assert result["pbo_span_from_ms"] == shifted.folds[0].test_start_ms


async def test_later_rejection_keeps_the_snapshot_of_the_opening(conn: AsyncConnection) -> None:
    split = _split()
    loose = GateThresholds(min_trades_per_fold=1, min_oos_trades=1)
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, junk_id = await _strong_and_junk(session, split)
        model = await _model(conn, experiment_id)
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
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, _junk_id = await _strong_and_junk(session, split)
        model = await _model(conn, experiment_id)
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
        assert result["invalidated_trials"] == []
        assert len(result["stress"]) == len(split.folds)
        assert result["stress"][0] == {
            "n_trades": 1,
            "n_signals": 3,
            "total_net_usd": "1",
            "profit_factor": "1.1",
            "below_min_trades": False,
            "cost_factor": "2",
        }


async def test_invalidated_trial_counts_in_n_but_not_in_spread_or_pbo(conn: AsyncConnection) -> None:
    split = _split()
    loose = GateThresholds(min_trades_per_fold=1, min_oos_trades=1)
    rng = np.random.default_rng(5)
    leak_r = tuple(Decimal(f"{value:.4f}") for value in rng.normal(3, 1, size=len(split.folds) * 2))
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, _junk_id = await _strong_and_junk(session, split)
        leak = await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="COMPLETE",
            params={"name": "leak"},
            folds=_records(split, leak_r),
        )
        model = await _model(conn, experiment_id)
        # Спроба з витоком (Sharpe ≈ 3 на угоду) роздуває розкид: G1 для сильного кандидата зачинено.
        with pytest.raises(GateRejected) as caught:
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=strong_id,
                model_version=model,
                stress=_stress(split),
                unfiltered_total_net=Decimal("0"),
                thresholds=loose,
            )
        assert "G1_DSR" in caught.value.decision.reasons
        # Позначка — лише з підтвердженим `audit_log` `trial.invalidate` саме для цієї спроби.
        for target, confirmed in ((str(strong_id), True), (str(leak.trial_id), False)):
            wrong = await make_audit(conn, AUDIT_ACTION_TRIAL_INVALIDATE, target=target, confirmed=confirmed)
            with pytest.raises(ValueError, match="audit_log"):
                await invalidate_trial(session, trial_id=leak.trial_id, audit_id=wrong, reason="витік")
        with pytest.raises(ValueError, match="причин"):
            await invalidate_trial(session, trial_id=leak.trial_id, audit_id=wrong, reason=" ")
        with pytest.raises(ValueError, match="немає"):
            await invalidate_trial(session, trial_id=leak.trial_id + 1000, audit_id=wrong, reason="витік")
        audit = await make_audit(conn, AUDIT_ACTION_TRIAL_INVALIDATE, target=str(leak.trial_id))
        await invalidate_trial(session, trial_id=leak.trial_id, audit_id=audit, reason="витік у фічах")
        again = await make_audit(conn, AUDIT_ACTION_TRIAL_INVALIDATE, target=str(leak.trial_id))
        with pytest.raises(ValueError, match="вже"):
            await invalidate_trial(session, trial_id=leak.trial_id, audit_id=again, reason="ще раз")
        # Недійсна спроба не буває кандидатом.
        with pytest.raises(ValueError, match="недійсн"):
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=leak.trial_id,
                model_version=model,
                stress=_stress(split),
                unfiltered_total_net=Decimal("0"),
                thresholds=loose,
            )
        opened = await open_holdout_after_gate(
            session,
            experiment_id=experiment_id,
            trial_id=strong_id,
            model_version=model,
            stress=_stress(split),
            unfiltered_total_net=Decimal("0"),
            thresholds=loose,
        )
        row = (await session.execute(select(HoldoutOpening).where(HoldoutOpening.id == opened.opening_id))).scalar_one()
        result = row.result
        assert result is not None
        # У N — так; у розкиді DSR і PBO — ні; запис відкриття її називає.
        assert result["n_trials_line"] == 3
        assert result["dsr_spread_trials"] == 2
        assert result["pbo_strategies"] == 2
        assert result["invalidated_trials"] == [
            {"trial_id": leak.trial_id, "audit_id": audit, "reason": "витік у фічах"}
        ]


async def test_opening_model_must_belong_to_the_experiment(conn: AsyncConnection) -> None:
    # Інакше meta-модель, відкрита через експеримент, оголошений `direct`, обійшла б порівняння з правилом.
    split = _split()
    loose = GateThresholds(min_trades_per_fold=1, min_oos_trades=1)
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, _junk_id = await _strong_and_junk(session, split)
        sibling = await _line(session, split)
        cases: tuple[tuple[dict[str, object], str], ...] = (
            ({}, "експерименту"),
            ({"experiment_id": sibling}, "експерименту"),
            ({"experiment_id": experiment_id, "label_type": "direct"}, "label_type"),
            ({"experiment_id": experiment_id, "trade_symbol": ETH}, "символ"),
            ({"experiment_id": experiment_id, "timeframe": "4h"}, "таймфрейм"),
        )
        for overrides, message in cases:
            with pytest.raises(ValueError, match=message):
                await open_holdout_after_gate(
                    session,
                    experiment_id=experiment_id,
                    trial_id=strong_id,
                    model_version=await make_model(conn, **overrides),
                    stress=_stress(split),
                    unfiltered_total_net=Decimal("0"),
                    thresholds=loose,
                )
        with pytest.raises(ValueError, match="немає"):
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=strong_id,
                model_version="m-missing",
                stress=_stress(split),
                unfiltered_total_net=Decimal("0"),
                thresholds=loose,
            )
        openings = select(func.count()).select_from(HoldoutOpening).where(HoldoutOpening.research_line == BTC)
        assert (await session.execute(openings)).scalar_one() == 0


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


async def test_reference_baseline_counts_in_n_but_not_in_pbo(conn: AsyncConnection) -> None:
    """«Завжди FLAT» лишається спробою лінії і не змагається в PBO (рішення 0016)."""
    split = _split()
    losses = tuple(Decimal("-1") for _ in range(len(split.folds) * 2))
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, _junk_id = await _strong_and_junk(session, split)
        baseline = await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="COMPLETE",
            params=ALWAYS_FLAT.trial_params(),
            folds=_records(split, losses),
        )
        model = await _model(conn, experiment_id)
        with pytest.raises(ValueError, match="бейзлайн"):
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=baseline.trial_id,
                model_version=model,
                stress=_stress(split),
                unfiltered_total_net=Decimal("0"),
                thresholds=GateThresholds(min_trades_per_fold=1, min_oos_trades=1),
            )
        opened = await open_holdout_after_gate(
            session,
            experiment_id=experiment_id,
            trial_id=strong_id,
            model_version=model,
            stress=_stress(split),
            unfiltered_total_net=Decimal("0"),
            thresholds=GateThresholds(min_trades_per_fold=1, min_oos_trades=1),
        )
        row = (await session.execute(select(HoldoutOpening).where(HoldoutOpening.id == opened.opening_id))).scalar_one()
        result = row.result
        assert result is not None
        assert result["n_trials_line"] == 3
        assert result["baseline_trials"] == [baseline.trial_id]
        assert result["pbo_strategies"] == 2
