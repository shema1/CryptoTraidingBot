"""Gate G1 — єдиний шлях до hold-out; знімок DSR і PBO пишеться лише разом із відкриттям (ТЗ §5А.10)."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal, localcontext

import numpy as np
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.api.models import AUDIT_ACTION_TRIAL_INVALIDATE
from app.exchange.types import PositionSide
from app.ml.backtest import BacktestDecision, BacktestMetrics, BacktestReport, BacktestTrade
from app.ml.decision import ENTRY_EVALUATION_PAST_FOLDS, ENTRY_RULE_WARMUP_FOLDS
from app.ml.models import Experiment, ExperimentTrial, HoldoutOpening
from app.ml.statistics import (
    G1_ENTRY_RULE,
    FoldOutcome,
    GateRejected,
    GateThresholds,
    open_holdout_after_gate,
    preview_gate_g1,
)
from app.ml.strategy import ALWAYS_FLAT
from app.ml.trade_sim import ExitReason, StopPriceSource
from app.ml.validation import (
    HOUR_MS,
    Fold,
    FoldRecord,
    LabelStats,
    MetaFoldRuns,
    ResearchSplit,
    build_split,
    create_experiment,
    invalidate_trial,
    journal_trial_count,
    line_trial_count,
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


def _skip_after(trade: BacktestTrade) -> BacktestDecision:
    """Сигнал на свічці після угоди: `run_backtest` дає одне рішення на сигнал, не два на одну свічку."""
    return BacktestDecision(
        ts_ms=trade.entry_open_time_ms + HOUR,
        entry_index=trade.entry_index + 1,
        stage="RISK",
        decision="SKIP",
        reason_code="RISK_COOLDOWN",
        side=trade.side,
    )


def _report(
    symbol: str,
    trades: tuple[BacktestTrade, ...],
    *,
    cost_factor: Decimal = Decimal("1"),
) -> BacktestReport:
    """Кожна угода — рішення ENTER; ще один сигнал на наступній свічці відкинуто cooldown."""
    if not trades:
        return _empty_report(symbol, cost_factor=cost_factor)
    total = sum((trade.net_usd for trade in trades), Decimal(0))
    wins = sum(1 for trade in trades if trade.net_usd > 0)
    loss = sum((-trade.net_usd for trade in trades if trade.net_usd < 0), Decimal(0))
    profit = sum((trade.net_usd for trade in trades if trade.net_usd > 0), Decimal(0))
    return BacktestReport(
        symbol=symbol,
        seed=7,
        cost_factor=cost_factor,
        trades=trades,
        decisions=(*(_decision(trade, "ENTER") for trade in trades), _skip_after(trades[-1])),
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


def _empty_report(symbol: str, *, cost_factor: Decimal = Decimal("1")) -> BacktestReport:
    """Фолд прогріву правила входу (рішення 0024): фільтр не отримав жодного сигналу, угод немає."""
    return BacktestReport(
        symbol=symbol,
        seed=7,
        cost_factor=cost_factor,
        trades=(),
        decisions=(),
        metrics=BacktestMetrics(
            n_trades=0,
            total_r=Decimal(0),
            total_net_usd=Decimal(0),
            profit_factor=None,
            max_drawdown_pct=Decimal(0),
            hit_rate=None,
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


def _pair(fold: Fold, values: tuple[Decimal, ...]) -> tuple[BacktestTrade, ...]:
    """Угоди на перших годинах тесту фолду: ті самі сигнали, що в торговому прогоні `_records`."""
    return tuple(
        _trade(fold.test_start_ms + offset * HOUR, value, entry_index=offset) for offset, value in enumerate(values)
    )


def _meta_runs(
    fold: Fold,
    pair: tuple[Decimal, ...],
    *,
    scored_shift: Decimal = Decimal("0"),
    warm: bool = False,
    warm_unfiltered: Decimal = Decimal("0"),
) -> MetaFoldRuns:
    """Прогони для G1 (рішення 0017): усе правило дає нуль, стрес — PF 1.1 на тих самих сигналах.

    На прогріві (рішення 0024) правило без фільтра торгує як завжди, а фільтр — ні: стрес без сигналів.
    """
    rule = warm_unfiltered if warm else Decimal("0")
    flat = _report(BTC, _pair(fold, (rule, rule)))
    scored = flat if not scored_shift else _report(BTC, _pair(fold, tuple(value + scored_shift for value in pair)))
    stress = (
        _empty_report(BTC, cost_factor=Decimal("2"))
        if warm
        else _report(BTC, _pair(fold, (Decimal("2.2"), Decimal("-2"))), cost_factor=Decimal("2"))
    )
    return MetaFoldRuns(unfiltered=flat, scored_unfiltered=scored, stress=stress)


def _records(
    split: ResearchSplit,
    series: tuple[Decimal, ...],
    *,
    meta: bool = False,
    scored_shift: Decimal = Decimal("0"),
    warmup: int = 0,
    warm_unfiltered: Decimal = Decimal("0"),
) -> list[FoldRecord]:
    """Дві угоди BTC на фолд з `series`. Перші `warmup` фолдів — прогрів (рішення 0024): угод немає,
    їхні значення ряду не використовуються."""
    if len(series) != len(split.folds) * 2:
        raise ValueError("ряд має містити дві угоди на фолд")
    records: list[FoldRecord] = []
    for index, fold in enumerate(split.folds):
        pair = series[index * 2 : index * 2 + 2]
        warm = index < warmup
        eth = (_trade(fold.test_start_ms, Decimal("9"), entry_index=0),)
        reports = (
            (_empty_report(BTC), _empty_report(ETH)) if warm else (_report(BTC, _pair(fold, pair)), _report(ETH, eth))
        )
        records.append(
            FoldRecord(
                reports=reports,
                train_labels=_LABELS,
                test_labels=_LABELS,
                meta=(
                    _meta_runs(fold, pair, scored_shift=scored_shift, warm=warm, warm_unfiltered=warm_unfiltered)
                    if meta
                    else None
                ),
            )
        )
    return records


def _stress(split: ResearchSplit, *, warmup: int = ENTRY_RULE_WARMUP_FOLDS) -> tuple[FoldOutcome, ...]:
    """Те, що G1 прочитає зі стресу `_meta_runs` кандидата: дві угоди з тих самих трьох сигналів, одна збиткова.

    Фолди прогріву (рішення 0024) — без сигналів. Викликач передає стрес усіх фолдів, як `MetaRun.stress`.
    """
    warm = FoldOutcome(
        n_trades=0,
        n_signals=0,
        total_net_usd=Decimal("0"),
        profit_factor=None,
        below_min_trades=False,
        cost_factor=Decimal("2"),
    )
    hot = FoldOutcome(
        n_trades=2,
        n_signals=3,
        total_net_usd=Decimal("0.2"),
        profit_factor=Decimal("1.1"),
        below_min_trades=False,
        cost_factor=Decimal("2"),
    )
    return tuple(warm if index < warmup else hot for index in range(len(split.folds)))


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


_DECISION = {"decision_rule": {"min_confidence": "0.40", "min_edge_atr": "0.10"}, "calibrator_sha256": None}


def _entry_rules(split: ResearchSplit, *, warmup: int = ENTRY_RULE_WARMUP_FOLDS) -> list[dict[str, object]]:
    """Записи `entry_rules` журналу (рішення 0024): прогрів без правила, далі правило k на фолдах 0…k−1,
    мітки пулу закінчились за годину до тесту фолду k."""
    return [
        {"fold_index": index, "method": None, "fit_on_folds": []}
        if index < warmup
        else {
            "fold_index": index,
            "method": "platt",
            "fit_on_folds": list(range(index)),
            "label_end_ms": fold.test_start_ms - HOUR,
            **_DECISION,
        }
        for index, fold in enumerate(split.folds)
    ]


def _candidate(name: str, split: ResearchSplit) -> dict[str, object]:
    """Параметри кандидата meta: угоди оцінені правилом входу рецепта лише минулими фолдами, правило — у спробі."""
    return {
        "name": name,
        "entry_evaluation": ENTRY_EVALUATION_PAST_FOLDS,
        "entry_rules": _entry_rules(split),
        "decision": _DECISION,
    }


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
        params=_candidate("strong", split),
        folds=_records(split, strong_r, meta=True, warmup=ENTRY_RULE_WARMUP_FOLDS),
    )
    return experiment_id, strong.trial_id, junk.trial_id


async def test_rejected_gate_writes_nothing(conn: AsyncConnection) -> None:
    split = _split()
    strong_r, _junk_r = _paths(len(split.folds) * 2)
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, _junk_id = await _strong_and_junk(session, split)
        stored = await session.get(ExperimentTrial, strong_id)
        assert stored is not None
        # Прогрів (рішення 0024): два перші фолди без угод, ряд — лише оцінені фолди.
        warm = ENTRY_RULE_WARMUP_FOLDS
        assert stored.oos_results == [format(value, "f") for value in strong_r[warm * 2 :]]
        assert "9" not in stored.oos_results
        assert stored.oos_entry_ms == [
            fold.test_start_ms + offset * HOUR for fold in split.folds[warm:] for offset in (0, 1)
        ]
        assert stored.metrics["folds"][0]["trading"]["n_signals"] == 0
        assert stored.metrics["folds"][warm]["trading"]["n_signals"] == 3
        with pytest.raises(GateRejected) as caught:
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=strong_id,
                model_version=await _model(conn, experiment_id),
                stress=_stress(split),
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


async def test_n_counts_every_trial_of_the_journal_across_lines(conn: AsyncConnection) -> None:
    """Рішення 0026: N, розкид DSR і суперники PBO — усі спроби журналу, і іншої лінії теж (вибір символу — вибір).

    До 0026 тест вимагав протилежного (інша лінія в N не входила) — поведінку змінено рішенням, а не послаблено.
    """
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
        # Інша лінія (торговий символ ETH) — теж у N, у розкиді DSR і в PBO (рішення 0026).
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
            )
        assert caught.value.decision.n_trials == 7
        # Лічильник лінії (нумерація спроб) лишається своїм: 5 спроб BTC, 7 — у журналі.
        assert await line_trial_count(session, BTC) == 5
        assert await journal_trial_count(session) == 7
        opened = await open_holdout_after_gate(
            session,
            experiment_id=experiment_id,
            trial_id=strong_id,
            model_version=model,
            stress=_stress(split),
            thresholds=GateThresholds(min_trades_per_fold=1, min_oos_trades=1),
        )
        row = (await session.execute(select(HoldoutOpening).where(HoldoutOpening.id == opened.opening_id))).scalar_one()
        result = row.result
        assert result is not None
        assert result["n_trials_line"] == 7
        # Рішення 0026, рев'ю T2.21: обидві оцінки записані, у G1 — гірша (мінімум DSR, максимум PBO).
        scopes = result["scopes"]
        assert (scopes["line"]["n_trials"], scopes["journal"]["n_trials"]) == (5, 7)
        assert Decimal(result["dsr"]) == min(Decimal(scopes["line"]["dsr"]), Decimal(scopes["journal"]["dsr"]))
        assert Decimal(result["pbo"]) == max(Decimal(scopes["line"]["pbo"]), Decimal(scopes["journal"]["pbo"]))
        assert result["dsr_scope"] in ("line", "journal")
        # Розкид DSR — з рядів ≥ 30 угод з визначеним Sharpe: кандидат (90), junk і сусідня спроба (по 94). Обрізана (6)
        # — ні; дві спроби ETH — ряд ETH з однакових R (Sharpe не визначений), у розкид не входять, хоч і в N.
        assert result["dsr_spread_trials"] == 3
        assert result["dsr_spread_floor"] is False
        assert Decimal(result["dsr_benchmark_sharpe"]) > 0
        # PBO журналу: кандидат, junk, спроба сусіднього експерименту і дві спроби ETH (5); лінії — без ETH (3).
        # Спроби ETH з постійним R тягнуть медіану OOS і знижують PBO журналу — у G1 іде вищий, лінійний (рішення 0026).
        # 32 доби з 45 оцінених (кратно 16); відрізок — від першого оціненого фолду кандидата (рішення 0024).
        assert (scopes["line"]["pbo_strategies"], scopes["journal"]["pbo_strategies"]) == (3, 5)
        assert result["pbo_scope"] == "line"
        assert Decimal(scopes["journal"]["pbo"]) < Decimal(scopes["line"]["pbo"])
        assert result["pbo_strategies"] == 3
        assert result["pbo_splits"] == 12870
        start = split.folds[ENTRY_RULE_WARMUP_FOLDS].test_start_ms
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
            params=_candidate("strong", shifted),
            folds=_records(shifted, strong_r, meta=True, warmup=ENTRY_RULE_WARMUP_FOLDS),
        )
        opened = await open_holdout_after_gate(
            session,
            experiment_id=moved,
            trial_id=candidate.trial_id,
            model_version=model,
            stress=_stress(shifted),
            thresholds=GateThresholds(min_trades_per_fold=1, min_oos_trades=1),
        )
        row = (await session.execute(select(HoldoutOpening).where(HoldoutOpening.id == opened.opening_id))).scalar_one()
        result = row.result
        assert result is not None
        # Відрізок PBO — оцінений walk-forward кандидата (без прогріву, рішення 0024); старі спроби змагаються
        # на добах, які покрили.
        assert result["pbo_strategies"] == 3
        assert result["pbo_span_from_ms"] == shifted.folds[ENTRY_RULE_WARMUP_FOLDS].test_start_ms
        assert result["warmup_folds"] == ENTRY_RULE_WARMUP_FOLDS


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
            thresholds=loose,
        )
        with pytest.raises(GateRejected):
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=junk_id,
                model_version=model,
                stress=_stress(split),
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
            # Для meta стрес можна не передавати: G1 бере його зі звітів спроби.
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
        # Умови фолдів — без двох прогрівних (рішення 0024): угоди й стрес лише оцінених фолдів.
        evaluated = len(split.folds) - ENTRY_RULE_WARMUP_FOLDS
        assert result["warmup_folds"] == ENTRY_RULE_WARMUP_FOLDS
        assert result["n_trades"] == evaluated * 2
        assert result["unfiltered_total_net"] == "0"
        assert result["scored_unfiltered_total_net"] == "0"
        assert result["invalidated_trials"] == []
        assert len(result["stress"]) == evaluated
        assert result["stress"][0] == {
            "n_trades": 2,
            "n_signals": 3,
            "total_net_usd": "0.2",
            "profit_factor": "1.1",
            "below_min_trades": False,
            "cost_factor": "2",
        }


async def test_meta_trial_not_evaluated_by_the_entry_rule_does_not_pass(conn: AsyncConnection) -> None:
    """Ті самі угоди без оцінки правилом входу лише минулими фолдами — G1 відмовляє лише з цієї причини.

    Інакше G1 оцінював би `keep(0.5)` (рішення 0020) або правило, підігнане й на пізніших фолдах (стара
    перехресна оцінка, рішення 0024), а в модель допуску і live пішло б правило `decide_entry`.
    """
    split = _split()
    strong_r, _junk_r = _paths(len(split.folds) * 2)
    loose = GateThresholds(min_trades_per_fold=1, min_oos_trades=1)
    async with AsyncSession(bind=conn) as session:
        experiment_id, _strong_id, _junk_id = await _strong_and_junk(session, split)
        model = await _model(conn, experiment_id)
        for name, params in (
            ("keep", {"name": "keep"}),
            ("no-rule", {"name": "no-rule", "entry_evaluation": ENTRY_EVALUATION_PAST_FOLDS}),
            # Стара перехресна оцінка (0020): калібратор фолду k бачив пізніші фолди.
            ("cross-fold", {"name": "cross-fold", "entry_evaluation": "decision_cross_fold", "decision": _DECISION}),
        ):
            trial = await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="COMPLETE",
                params=params,
                folds=_records(split, strong_r, meta=True),
            )
            with pytest.raises(GateRejected) as caught:
                await open_holdout_after_gate(
                    session,
                    experiment_id=experiment_id,
                    trial_id=trial.trial_id,
                    model_version=model,
                    thresholds=loose,
                )
            assert caught.value.decision.reasons == (G1_ENTRY_RULE,), name
        openings = select(func.count()).select_from(HoldoutOpening).where(HoldoutOpening.research_line == BTC)
        assert (await session.execute(openings)).scalar_one() == 0


async def test_record_trial_does_not_depend_on_the_thread_decimal_context(conn: AsyncConnection) -> None:
    """`run_backtest` рахує суми у фіксованому контексті; звірка журналу — так само, а не в HALF_UP від CCXT."""
    split = _split()
    strong_r, _junk_r = _paths(len(split.folds) * 2)
    records = _records(split, strong_r, meta=True)
    fold = split.folds[0]
    # Точна сума має 29 значущих цифр: HALF_EVEN дає …004, HALF_UP — …005.
    odd = _pair(fold, (Decimal("1000.000000000000000000000004"), Decimal("0.0000000000000000000000005")))
    records[0] = records[0].model_copy(update={"reports": (_report(BTC, odd), records[0].reports[1])})
    async with AsyncSession(bind=conn) as session:
        experiment_id = await _line(session, split)
        with localcontext() as ctx:
            ctx.rounding = ROUND_HALF_UP
            written = await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="COMPLETE",
                params={"name": "half-up"},
                folds=records,
            )
        stored = await session.get(ExperimentTrial, written.trial_id)
        assert stored is not None
        assert stored.metrics["folds"][0]["trading"]["metrics"]["total_net_usd"] == "1000.000000000000000000000004"


async def test_meta_gate_takes_rule_numbers_and_stress_from_the_journal(conn: AsyncConnection) -> None:
    """Викликач не підміняє ні правило без фільтра, ні стрес: G1 рахує їх зі звітів спроби (рішення 0017)."""
    split = _split()
    loose = GateThresholds(min_trades_per_fold=1, min_oos_trades=1)
    strong_r, _junk_r = _paths(len(split.folds) * 2)
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, _junk_id = await _strong_and_junk(session, split)
        model = await _model(conn, experiment_id)
        # Сильніший стрес з тими самими сигналами, але не з цього прогону.
        forged = tuple(item.model_copy(update={"total_net_usd": Decimal("100")}) for item in _stress(split))
        with pytest.raises(GateRejected) as caught:
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=strong_id,
                model_version=model,
                stress=forged,
                thresholds=loose,
            )
        assert "G1_STRESS" in caught.value.decision.reasons
        assert "G1_FILTER" not in caught.value.decision.reasons
        cases: tuple[tuple[str, list[FoldRecord], tuple[str, ...]], ...] = (
            # Без звітів правила і стресу в журналі G1 нема з чим порівнювати.
            ("no-runs", _records(split, strong_r), ("G1_FILTER", "G1_STRESS")),
            # Правило на оцінених сигналах краще за фільтр.
            ("scored-wins", _records(split, strong_r, meta=True, scored_shift=Decimal("1")), ("G1_FILTER",)),
        )
        for name, folds, reasons in cases:
            trial = await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="COMPLETE",
                params={"name": name},
                folds=folds,
            )
            with pytest.raises(GateRejected) as caught:
                await open_holdout_after_gate(
                    session,
                    experiment_id=experiment_id,
                    trial_id=trial.trial_id,
                    model_version=model,
                    thresholds=loose,
                )
            assert set(reasons) <= set(caught.value.decision.reasons), name
        openings = select(func.count()).select_from(HoldoutOpening).where(HoldoutOpening.research_line == BTC)
        assert (await session.execute(openings)).scalar_one() == 0


async def test_meta_runs_are_checked_when_written(conn: AsyncConnection) -> None:
    """Звіти правила і стресу — угоди бектесту торгового символу на сигналах фільтра, а не голі числа."""
    split = _split()
    strong_r, _junk_r = _paths(len(split.folds) * 2)
    good = _records(split, strong_r, meta=True)
    fold = split.folds[0]
    runs = good[0].meta
    assert runs is not None
    flat = _pair(fold, (Decimal("0"), Decimal("0")))
    shifted = (flat[0], _trade(fold.test_start_ms + 2 * HOUR, Decimal("1"), entry_index=2))
    twice = _decision(runs.stress.trades[-1], "SKIP")
    lying = runs.unfiltered.model_copy(
        update={"metrics": runs.unfiltered.metrics.model_copy(update={"total_net_usd": Decimal("-50")})}
    )
    cases: tuple[tuple[dict[str, object], str], ...] = (
        ({"unfiltered": _report(ETH, flat)}, "торгового символу"),
        ({"unfiltered": lying}, "PnL"),
        ({"stress": _report(BTC, _pair(fold, (Decimal("2.2"), Decimal("-2"))))}, "стрес"),
        # Стрес на іншому наборі сигналів: другий сигнал на годину пізніше, ніж у фільтра.
        ({"stress": _report(BTC, shifted, cost_factor=Decimal("2"))}, "сигнал"),
        # Оцінених сигналів менше, ніж узяв фільтр.
        ({"scored_unfiltered": _report(BTC, flat[:1])}, "сигнал"),
        # Два рішення на одній свічці: множина сигналів сховала б повтор.
        ({"stress": runs.stress.model_copy(update={"decisions": (*runs.stress.decisions[:-1], twice)})}, "повтор"),
    )
    async with AsyncSession(bind=conn) as session:
        experiment_id = await _line(session, split)
        for update, message in cases:
            broken = good[0].model_copy(update={"meta": runs.model_copy(update=update)})
            with pytest.raises(ValueError, match=message):
                await record_trial(
                    session,
                    experiment_id=experiment_id,
                    split=split,
                    state="COMPLETE",
                    params={"name": "broken"},
                    folds=[broken, *good[1:]],
                )
        direct = await create_experiment(
            session,
            split=split,
            research_line=BTC,
            symbols=(BTC, ETH),
            label_type="direct",
            primary_strategy={"name": "ema_cross"},
            params={},
            seed=7,
        )
        with pytest.raises(ValueError, match="meta"):
            await record_trial(
                session,
                experiment_id=direct,
                split=split,
                state="COMPLETE",
                params={"name": "meta-runs"},
                folds=good,
            )
        written = await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="COMPLETE",
            params={"name": "good"},
            folds=good,
        )
        stored = await session.get(ExperimentTrial, written.trial_id)
        assert stored is not None
        block = stored.metrics["folds"][0]["meta"]
        assert block["unfiltered"]["metrics"]["total_net_usd"] == "0"
        assert block["stress"]["cost_factor"] == "2"
        assert block["stress"]["n_signals"] == 3


async def test_meta_trial_in_another_experiment_type_is_not_a_candidate(conn: AsyncConnection) -> None:
    """Тип експерименту не вимикає порівняння з правилом: спроба meta в `direct` / `rule_only` — не кандидат."""
    split = _split()
    loose = GateThresholds(min_trades_per_fold=1, min_oos_trades=1)
    strong_r, _junk_r = _paths(len(split.folds) * 2)
    markers: tuple[dict[str, object], ...] = (
        {"name": "comparison", "comparison": {"beats_unfiltered": True}},
        {"name": "role", "role": "meta"},
        {"name": "stage", "stage": "meta_model"},
    )
    async with AsyncSession(bind=conn) as session:
        for label_type in ("direct", "rule_only"):
            experiment_id = await create_experiment(
                session,
                split=split,
                research_line=BTC,
                symbols=(BTC, ETH),
                label_type=label_type,
                primary_strategy={"name": "ema_cross"},
                params={},
                seed=7,
            )
            artifact = {"artifact_path": None} if label_type == "rule_only" else {}
            model = await make_model(conn, experiment_id=experiment_id, label_type=label_type, **artifact)
            for params in markers:
                trial = await record_trial(
                    session,
                    experiment_id=experiment_id,
                    split=split,
                    state="COMPLETE",
                    params=params,
                    folds=_records(split, strong_r),
                )
                with pytest.raises(ValueError, match="meta"):
                    await open_holdout_after_gate(
                        session,
                        experiment_id=experiment_id,
                        trial_id=trial.trial_id,
                        model_version=model,
                        stress=_stress(split),
                        thresholds=loose,
                    )
        openings = select(func.count()).select_from(HoldoutOpening).where(HoldoutOpening.research_line == BTC)
        assert (await session.execute(openings)).scalar_one() == 0


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
                thresholds=loose,
            )
        opened = await open_holdout_after_gate(
            session,
            experiment_id=experiment_id,
            trial_id=strong_id,
            model_version=model,
            stress=_stress(split),
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
                    thresholds=loose,
                )
        with pytest.raises(ValueError, match="немає"):
            await open_holdout_after_gate(
                session,
                experiment_id=experiment_id,
                trial_id=strong_id,
                model_version="m-missing",
                stress=_stress(split),
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
                thresholds=GateThresholds(min_trades_per_fold=1, min_oos_trades=1),
            )
        opened = await open_holdout_after_gate(
            session,
            experiment_id=experiment_id,
            trial_id=strong_id,
            model_version=model,
            stress=_stress(split),
            thresholds=GateThresholds(min_trades_per_fold=1, min_oos_trades=1),
        )
        row = (await session.execute(select(HoldoutOpening).where(HoldoutOpening.id == opened.opening_id))).scalar_one()
        result = row.result
        assert result is not None
        assert result["n_trials_line"] == 3
        assert result["baseline_trials"] == [baseline.trial_id]
        assert result["pbo_strategies"] == 2


async def test_warmup_folds_are_left_out_of_every_fold_condition(conn: AsyncConnection) -> None:
    """Прогрів (рішення 0024): два перші фолди без угод не валять умову «кожен фолд ≥ N угод» і частку фолдів.

    Якби G1 рахував їх, `min_trades_per_fold = 1` відмовив би (`G1_MIN_TRADES_FOLD`), а частка прибуткових
    фолдів упала б нижче за 70% лише через прогрів.
    """
    split = _split()
    async with AsyncSession(bind=conn) as session:
        experiment_id, strong_id, _junk_id = await _strong_and_junk(session, split)
        preview = await preview_gate_g1(
            session,
            experiment_id=experiment_id,
            trial_id=strong_id,
            stress=_stress(split),
            thresholds=GateThresholds(min_trades_per_fold=1, min_oos_trades=1),
        )
    decision = preview.decision
    assert decision.passed, decision.reasons
    assert decision.warmup_folds == ENTRY_RULE_WARMUP_FOLDS
    assert decision.n_trades == (len(split.folds) - ENTRY_RULE_WARMUP_FOLDS) * 2


async def test_a_warmup_the_journal_does_not_prove_is_an_entry_rule_failure(conn: AsyncConnection) -> None:
    """Спроба не може оголосити прогрівом довільні фолди: інакше прибрала б з G1 незручні фолди.

    Прогрів визнається лише як префікс без жодного сигналу, а правило фолду k — рівно на фолдах 0…k−1.
    """
    split = _split()
    strong_r, _junk_r = _paths(len(split.folds) * 2)
    n_folds = len(split.folds)
    loose = GateThresholds(min_trades_per_fold=1, min_oos_trades=1)
    # Прогрів зсунуто на фолди 1–2, а фолд 0 має правило ні на чому.
    first: dict[str, object] = {"fold_index": 0, "method": "platt", "fit_on_folds": [], **_DECISION}
    shifted_rules: list[dict[str, object]] = [first]
    shifted_rules.extend({**item, "fold_index": index + 1} for index, item in enumerate(_entry_rules(split)[:-1]))
    own_fold = _entry_rules(split)
    own_fold[2] = {**own_fold[2], "fit_on_folds": [0, 1, 2]}
    future = _entry_rules(split)
    future[3] = {**future[3], "fit_on_folds": list(range(n_folds))}
    # Фолди пулу минулі, але мітки пулу дочитують open першого бару тесту, який правило оцінює.
    reaching = _entry_rules(split)
    reaching[3] = {**reaching[3], "label_end_ms": split.folds[3].test_start_ms}
    no_pool = _entry_rules(split)
    no_pool[4] = {**no_pool[4], "fit_on_folds": []}
    cases: tuple[tuple[str, list[dict[str, object]], int], ...] = (
        # Прогрів не з початку walk-forward.
        ("shifted", shifted_rules, ENTRY_RULE_WARMUP_FOLDS),
        # Правило фолду бачило власний фолд.
        ("own-fold", own_fold, ENTRY_RULE_WARMUP_FOLDS),
        # Правило фолду бачило пізніші фолди.
        ("future", future, ENTRY_RULE_WARMUP_FOLDS),
        # Мітки пулу дочитують тест фолду.
        ("label-reaches-test", reaching, ENTRY_RULE_WARMUP_FOLDS),
        # Правило без пулу (нема з чого підганяти).
        ("no-pool", no_pool, ENTRY_RULE_WARMUP_FOLDS),
        # Записи кажуть «прогрів», а у фолдах прогріву є угоди.
        ("traded-warmup", _entry_rules(split), 0),
        # Записів менше, ніж фолдів.
        ("short", _entry_rules(split)[:-1], ENTRY_RULE_WARMUP_FOLDS),
    )
    async with AsyncSession(bind=conn) as session:
        experiment_id, _strong_id, _junk_id = await _strong_and_junk(session, split)
        for name, rules, warmup in cases:
            trial = await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="COMPLETE",
                params={
                    "name": name,
                    "entry_evaluation": ENTRY_EVALUATION_PAST_FOLDS,
                    "entry_rules": rules,
                    "decision": _DECISION,
                },
                folds=_records(split, strong_r, meta=True, warmup=warmup),
            )
            preview = await preview_gate_g1(
                session,
                experiment_id=experiment_id,
                trial_id=trial.trial_id,
                thresholds=loose,
            )
            assert G1_ENTRY_RULE in preview.decision.reasons, name
            assert preview.decision.warmup_folds == 0, name
            assert not preview.decision.passed, name


async def test_rule_without_filter_on_warmup_folds_does_not_enter_the_filter_comparison(conn: AsyncConnection) -> None:
    """На прогріві фільтр не торгує, а правило без фільтра торгує (рішення 0024). Його виграш там не має робити
    фільтр «гіршим», а програш — «кращим»: обидві суми — лише по оцінених фолдах."""
    split = _split()
    strong_r, _junk_r = _paths(len(split.folds) * 2)
    loose = GateThresholds(min_trades_per_fold=1, min_oos_trades=1)
    async with AsyncSession(bind=conn) as session:
        experiment_id, _strong_id, _junk_id = await _strong_and_junk(session, split)
        for name, warm_rule in (("warm-wins", Decimal("1000")), ("warm-loses", Decimal("-1000"))):
            trial = await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="COMPLETE",
                params=_candidate(name, split),
                folds=_records(split, strong_r, meta=True, warmup=ENTRY_RULE_WARMUP_FOLDS, warm_unfiltered=warm_rule),
            )
            preview = await preview_gate_g1(
                session,
                experiment_id=experiment_id,
                trial_id=trial.trial_id,
                stress=_stress(split),
                thresholds=loose,
            )
            # Сума правила на прогріві — 4000 або −4000: увійди вона в порівняння, «warm-wins» не пройшов би фільтр.
            assert "G1_FILTER" not in preview.decision.reasons, name
            assert preview.decision.warmup_folds == ENTRY_RULE_WARMUP_FOLDS, name
