"""Журнал спроб і відкриття hold-out (ТЗ §5.2.5, §5А.10, T2.5)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.core.clock import FakeClock, use_clock
from app.exchange.types import PositionSide
from app.ml.backtest import BacktestDecision, BacktestMetrics, BacktestReport, BacktestTrade
from app.ml.models import Experiment, ExperimentTrial, HoldoutOpening
from app.ml.trade_sim import ExitReason, StopPriceSource
from app.ml.validation import (
    HOUR_MS,
    FoldRecord,
    HoldoutLimitError,
    HoldoutNotNewerError,
    LabelStats,
    ResearchSplit,
    SplitLeakError,
    _open_holdout,
    _opening_no,
    build_split,
    create_experiment,
    holdout_was_opened,
    line_trial_count,
    record_trial,
    validation_payload,
)
from tests.db.factories import make_model

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
HOUR = HOUR_MS
T0 = 1_700_000_000_000


def _split(*, horizon_bars: int = 4, data_to_ms: int = 500 * HOUR) -> ResearchSplit:
    return build_split(
        data_from_ms=0,
        data_to_ms=data_to_ms,
        horizon_bars=horizon_bars,
        count_trades=lambda _start, _end: 20,
        min_train_ms=48 * HOUR,
        test_segment_ms=24 * HOUR,
        test_segment_extended_ms=48 * HOUR,
    )


def _trade(
    entry_ms: int,
    r_multiple: Decimal,
    net_usd: Decimal,
    *,
    reason: ExitReason = ExitReason.TAKE_PROFIT,
    source: StopPriceSource = StopPriceSource.MARK,
    funding_gap: bool = False,
    exit_ms: int | None = None,
) -> BacktestTrade:
    return BacktestTrade(
        side=PositionSide.LONG,
        entry_index=0,
        exit_index=1,
        entry_open_time_ms=entry_ms,
        exit_open_time_ms=entry_ms + HOUR if exit_ms is None else exit_ms,
        exit_reason=reason,
        stop_price_source=source,
        qty=Decimal("1"),
        net_usd=net_usd,
        loss_per_unit=Decimal("1"),
        r_multiple=r_multiple,
        risk_usd=Decimal("1"),
        funding_penalty_usd=Decimal("-0.1") if funding_gap else Decimal("0"),
        funding_gap=funding_gap,
    )


def _metrics(trades: tuple[BacktestTrade, ...], *, n_unusable: int = 0) -> BacktestMetrics:
    total_net = sum((trade.net_usd for trade in trades), Decimal(0))
    total_r = sum((trade.r_multiple for trade in trades), Decimal(0))
    profit = sum((trade.net_usd for trade in trades if trade.net_usd > 0), Decimal(0))
    loss = sum((-trade.net_usd for trade in trades if trade.net_usd < 0), Decimal(0))
    wins = sum(1 for trade in trades if trade.net_usd > 0)
    return BacktestMetrics(
        n_trades=len(trades),
        total_r=total_r,
        total_net_usd=total_net,
        profit_factor=None if loss == 0 else profit / loss,
        max_drawdown_pct=Decimal("1"),
        hit_rate=None if not trades else Decimal(wins) / Decimal(len(trades)),
        sharpe=None,
        max_consecutive_losses=0,
        n_incomplete=sum(1 for trade in trades if trade.exit_reason is ExitReason.DATA_GAP),
        n_unusable=n_unusable,
        n_funding_gaps=sum(1 for trade in trades if trade.funding_gap),
        ending_equity=Decimal("100"),
        peak_equity=Decimal("100"),
        halted=False,
    )


def _decisions(trades: tuple[BacktestTrade, ...], *, skipped: int) -> tuple[BacktestDecision, ...]:
    """Як у бектесті: кожна угода — один ENTER, кожен відкинутий сигнал — один SKIP."""
    entered = tuple(
        BacktestDecision(
            ts_ms=trade.entry_open_time_ms,
            entry_index=trade.entry_index,
            stage="RISK",
            decision="ENTER",
            reason_code=None,
            side=trade.side,
        )
        for trade in trades
    )
    skips = tuple(
        BacktestDecision(
            ts_ms=trades[0].entry_open_time_ms if trades else 0,
            entry_index=0,
            stage="RISK",
            decision="SKIP",
            reason_code="RISK_COOLDOWN",
            side=PositionSide.LONG,
        )
        for _ in range(skipped)
    )
    return entered + skips


def _report(
    symbol: str,
    trades: tuple[BacktestTrade, ...],
    *,
    seed: int = 7,
    cost_factor: Decimal = Decimal("1"),
    n_unusable: int = 0,
    skipped: int = 0,
    decisions: tuple[BacktestDecision, ...] | None = None,
) -> BacktestReport:
    return BacktestReport(
        symbol=symbol,
        seed=seed,
        cost_factor=cost_factor,
        trades=trades,
        decisions=_decisions(trades, skipped=skipped) if decisions is None else decisions,
        metrics=_metrics(trades, n_unusable=n_unusable),
    )


def _records(split: ResearchSplit, *, seed: int = 7, cost_factor: Decimal = Decimal("1")) -> list[FoldRecord]:
    records: list[FoldRecord] = []
    for index, fold in enumerate(split.folds):
        if index == 0:
            btc_trade = _trade(
                fold.test_start_ms,
                Decimal("-1.5"),
                Decimal("-3"),
                reason=ExitReason.DATA_GAP,
                source=StopPriceSource.LAST,
                funding_gap=True,
            )
        else:
            btc_trade = _trade(fold.test_start_ms, Decimal("1"), Decimal("3"))
        eth_trade = _trade(fold.test_start_ms, Decimal("-0.5"), Decimal("-2"))
        records.append(
            FoldRecord(
                reports=(
                    _report(BTC, (btc_trade,), seed=seed, cost_factor=cost_factor, skipped=1 if index == 0 else 0),
                    _report(ETH, (eth_trade,), seed=seed, cost_factor=cost_factor, n_unusable=1),
                ),
                train_labels=LabelStats(n_labels=4, n_last_stop=1, n_both_reached_tp=0),
                test_labels=LabelStats(n_labels=2, n_last_stop=0, n_both_reached_tp=1),
            )
        )
    return records


async def _experiment(session: AsyncSession, split: ResearchSplit, *, label_type: str = "meta", seed: int = 7) -> str:
    return await create_experiment(
        session,
        split=split,
        research_line=BTC,
        symbols=(BTC, ETH),
        label_type=label_type,
        primary_strategy={"name": "ema_cross", "fast": 20},
        params={"note": "baseline"},
        seed=seed,
    )


async def test_every_attempt_is_stored_and_the_line_counter_never_resets(conn: AsyncConnection) -> None:
    split = _split()
    async with AsyncSession(bind=conn) as session:
        with use_clock(FakeClock(T0)):
            first = await _experiment(session, split)
            second = await _experiment(session, split, label_type="direct", seed=8)
        assert int(second.removeprefix("exp-")) == int(first.removeprefix("exp-")) + 1
        stored = (await session.execute(select(Experiment).where(Experiment.id == first))).scalar_one()
        assert stored.created_at_ms == T0
        assert stored.seed == 7
        assert stored.n_trials_line is None
        assert stored.params["validation"] == validation_payload(split)
        assert stored.params["note"] == "baseline"

        complete = await record_trial(
            session,
            experiment_id=first,
            split=split,
            state="COMPLETE",
            params={"num_leaves": 8},
            folds=_records(split),
        )
        failed = await record_trial(session, experiment_id=first, split=split, state="FAILED", params={"broken": True})
        later = await record_trial(session, experiment_id=second, split=split, state="PRUNED", params={"stopped": True})
        assert (complete.line_trial_no, failed.line_trial_no, later.line_trial_no) == (1, 2, 3)
        assert await line_trial_count(session, BTC) == 3
        assert complete.trial_no == 1
        assert failed.trial_no == 2
        assert later.trial_no == 1

        stmt = select(ExperimentTrial).where(ExperimentTrial.id == complete.trial_id)
        row = (await session.execute(stmt)).scalar_one()
        assert row.oos_results[0] == "-1.5"
        assert row.oos_results[1:] == ["1"] * (len(split.folds) - 1)
        assert "-0.5" not in row.oos_results
        # Час входу кожної угоди — поруч із R, у тому самому порядку (для PBO за часом).
        assert row.oos_entry_ms == [fold.test_start_ms for fold in split.folds]
        fold0 = row.metrics["folds"][0]
        assert fold0["trading"]["n_signals"] == 2
        assert row.metrics["folds"][1]["trading"]["n_signals"] == 1
        assert fold0["trading"]["symbol"] == BTC
        assert fold0["trading"]["metrics"]["n_incomplete"] == 1
        assert fold0["trading"]["metrics"]["n_funding_gaps"] == 1
        assert fold0["trading"]["metrics"]["total_r"] == "-1.5"
        assert fold0["all_symbols"]["n_trades"] == 2
        assert fold0["all_symbols"]["n_unusable"] == 1
        assert fold0["all_symbols"]["total_net_usd"] == "-5"
        assert fold0["all_symbols"]["per_symbol"][0]["symbol"] == BTC
        assert fold0["labels"]["train"]["n_last_stop"] == 1
        assert fold0["labels"]["test"]["n_both_reached_tp"] == 1
        assert row.metrics["any_last_stop"] is True
        assert row.metrics["any_both_reached_tp"] is True
        assert row.metrics["holdout_opened"] is False
        assert row.metrics["line_trial_no"] == 1
        failed_row = select(ExperimentTrial).where(ExperimentTrial.id == failed.trial_id)
        empty = (await session.execute(failed_row)).scalar_one()
        assert empty.state == "FAILED"
        assert empty.oos_results == []
        assert empty.oos_entry_ms == []
        assert empty.metrics["folds"] == []


async def test_a_trial_cannot_hide_a_fold_or_touch_the_holdout(conn: AsyncConnection) -> None:
    split = _split()
    async with AsyncSession(bind=conn) as session:
        experiment_id = await _experiment(session, split)
        with pytest.raises(ValueError, match="кожен фолд"):
            await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="COMPLETE",
                params={},
                folds=_records(split)[:-1],
            )
        other = _split(horizon_bars=5)
        with pytest.raises(SplitLeakError, match="розбивка"):
            await record_trial(session, experiment_id=experiment_id, split=other, state="FAILED", params={})
        leaked = _records(split)
        entry = split.holdout_from_ms
        leaked[0] = FoldRecord(
            reports=(
                _report(BTC, (_trade(entry, Decimal("1"), Decimal("1")),)),
                _report(ETH, (_trade(entry, Decimal("1"), Decimal("1")),)),
            ),
            train_labels=LabelStats(n_labels=0, n_last_stop=0, n_both_reached_tp=0),
            test_labels=LabelStats(n_labels=0, n_last_stop=0, n_both_reached_tp=0),
        )
        with pytest.raises(SplitLeakError, match="hold-out"):
            await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="COMPLETE",
                params={},
                folds=leaked,
            )
        exit_in_holdout = _records(split)
        exit_in_holdout[0] = FoldRecord(
            reports=(
                _report(BTC, (_trade(split.folds[0].test_start_ms, Decimal("1"), Decimal("1"), exit_ms=entry),)),
                _report(ETH, (_trade(split.folds[0].test_start_ms, Decimal("1"), Decimal("1")),)),
            ),
            train_labels=LabelStats(n_labels=0, n_last_stop=0, n_both_reached_tp=0),
            test_labels=LabelStats(n_labels=0, n_last_stop=0, n_both_reached_tp=0),
        )
        with pytest.raises(SplitLeakError, match="hold-out"):
            await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="COMPLETE",
                params={},
                folds=exit_in_holdout,
            )
        too_long = _records(split)
        start = split.folds[0].test_start_ms
        too_long[0] = FoldRecord(
            reports=(
                _report(BTC, (_trade(start, Decimal("1"), Decimal("1"), exit_ms=start + 5 * HOUR),)),
                _report(ETH, (_trade(start, Decimal("1"), Decimal("1")),)),
            ),
            train_labels=LabelStats(n_labels=0, n_last_stop=0, n_both_reached_tp=0),
            test_labels=LabelStats(n_labels=0, n_last_stop=0, n_both_reached_tp=0),
        )
        with pytest.raises(SplitLeakError, match="горизонт"):
            await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="COMPLETE",
                params={},
                folds=too_long,
            )


async def test_every_trade_has_its_enter_decision(conn: AsyncConnection) -> None:
    # Без рішень журнал не знав би, скільки сигналів отримав фолд, і стрес G1 не мав би з чим звірятись.
    split = _split()
    async with AsyncSession(bind=conn) as session:
        experiment_id = await _experiment(session, split)
        silent = _records(split)
        start = split.folds[0].test_start_ms
        silent[0] = FoldRecord(
            reports=(
                _report(BTC, (_trade(start, Decimal("1"), Decimal("1")),), decisions=()),
                _report(ETH, (_trade(start, Decimal("1"), Decimal("1")),)),
            ),
            train_labels=LabelStats(n_labels=0, n_last_stop=0, n_both_reached_tp=0),
            test_labels=LabelStats(n_labels=0, n_last_stop=0, n_both_reached_tp=0),
        )
        with pytest.raises(ValueError, match="ENTER"):
            await record_trial(
                session,
                experiment_id=experiment_id,
                split=split,
                state="COMPLETE",
                params={},
                folds=silent,
            )


async def test_another_experiment_cannot_use_a_different_holdout(conn: AsyncConnection) -> None:
    wide = _split()
    narrow = build_split(
        data_from_ms=0,
        data_to_ms=500 * HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        holdout_fraction=Decimal("0.15"),
        min_train_ms=48 * HOUR,
        test_segment_ms=24 * HOUR,
        test_segment_extended_ms=48 * HOUR,
    )
    assert narrow.holdout_from_ms > wide.holdout_from_ms
    async with AsyncSession(bind=conn) as session:
        await _experiment(session, wide)
        with pytest.raises(SplitLeakError, match="зафіксований"):
            await _experiment(session, narrow, seed=8)


async def _open(session: AsyncSession, split: ResearchSplit, experiment_id: str, model: str) -> int:
    opened = await _open_holdout(
        session,
        research_line=BTC,
        holdout_from_ms=split.holdout_from_ms,
        holdout_to_ms=split.holdout_to_ms,
        experiment_id=experiment_id,
        model_version=model,
    )
    return opened.opening_no


async def _trial_flag(session: AsyncSession, experiment_id: str, split: ResearchSplit) -> bool:
    recorded = await record_trial(session, experiment_id=experiment_id, split=split, state="FAILED", params={})
    stmt = select(ExperimentTrial).where(ExperimentTrial.id == recorded.trial_id)
    flag: bool = (await session.execute(stmt)).scalar_one().metrics["holdout_opened"]
    return flag


async def test_a_shift_seals_the_new_window_for_the_whole_line(conn: AsyncConnection) -> None:
    model = await make_model(conn)
    old = _split()
    shifted = _split(data_to_ms=625 * HOUR)
    assert (shifted.holdout_from_ms, shifted.holdout_to_ms) == (500 * HOUR, 625 * HOUR)
    async with AsyncSession(bind=conn) as session:
        first = await _experiment(session, old)
        assert await _open(session, old, first, model) == 1
        # Зсув — новий експеримент з новішим вікном, а не відкриття.
        second = await _experiment(session, shifted, seed=9)
        assert await holdout_was_opened(session, second)
        assert await _trial_flag(session, first, old) is True
        with pytest.raises(SplitLeakError, match="зафіксований"):
            await _experiment(session, old, seed=10)
        with pytest.raises(HoldoutNotNewerError, match="відпущено"):
            await _open(session, old, first, model)
        assert await _open(session, shifted, second, model) == 1


async def test_an_exhausted_holdout_shifts_to_newer_data_without_an_opening(conn: AsyncConnection) -> None:
    model = await make_model(conn)
    old = _split()
    newer = _split(data_to_ms=625 * HOUR)
    async with AsyncSession(bind=conn) as session:
        first = await _experiment(session, old)
        assert [await _open(session, old, first, model) for _ in range(2)] == [1, 2]
        # ТЗ §5А.10: відкриття вичерпано → hold-out з новіших даних, старий — у walk-forward.
        second = await _experiment(session, newer, seed=8)
        windows = select(HoldoutOpening.holdout_from_ms).where(HoldoutOpening.research_line == BTC)
        assert (await session.execute(windows)).scalars().all() == [old.holdout_from_ms] * 2
        assert [await _open(session, newer, second, model) for _ in range(2)] == [1, 2]
        with pytest.raises(HoldoutLimitError):
            await _open(session, newer, second, model)


async def test_a_never_opened_holdout_moves_into_walk_forward_unseen(conn: AsyncConnection) -> None:
    model = await make_model(conn)
    old = _split()
    newer = _split(data_to_ms=700 * HOUR)
    async with AsyncSession(bind=conn) as session:
        first = await _experiment(session, old)
        second = await _experiment(session, newer, seed=8)
        assert not await holdout_was_opened(session, second)
        assert await _trial_flag(session, second, newer) is False
        with pytest.raises(HoldoutNotNewerError, match="відпущено"):
            await _open(session, old, first, model)


async def test_only_the_experiment_holdout_can_be_opened(conn: AsyncConnection) -> None:
    model = await make_model(conn)
    old = _split()
    async with AsyncSession(bind=conn) as session:
        first = await _experiment(session, old)
        foreign = (
            (500 * HOUR, 510 * HOUR),
            (old.holdout_to_ms, old.holdout_to_ms + 125 * HOUR),
            (old.holdout_from_ms - HOUR, old.holdout_to_ms),
            (old.holdout_from_ms, old.holdout_to_ms + HOUR),
        )
        for start, end in foreign:
            with pytest.raises(SplitLeakError, match="цього експерименту"):
                await _open_holdout(
                    session,
                    research_line=BTC,
                    holdout_from_ms=start,
                    holdout_to_ms=end,
                    experiment_id=first,
                    model_version=model,
                )
        openings = select(HoldoutOpening.id).where(HoldoutOpening.research_line == BTC)
        assert (await session.execute(openings)).scalars().all() == []
        # Чуже вікно лінію не запечатало: новіший експеримент створюється, свій hold-out відкривається.
        second = await _experiment(session, _split(data_to_ms=625 * HOUR), seed=9)
        assert await _open(session, _split(data_to_ms=625 * HOUR), second, model) == 1


async def test_holdout_opens_twice_and_a_new_window_must_be_newer(conn: AsyncConnection) -> None:
    model = await make_model(conn)
    split = _split()
    async with AsyncSession(bind=conn) as session:
        experiment_id = await _experiment(session, split)
        with use_clock(FakeClock(T0)):
            first = await _open_holdout(
                session,
                research_line=BTC,
                holdout_from_ms=split.holdout_from_ms,
                holdout_to_ms=split.holdout_to_ms,
                experiment_id=experiment_id,
                model_version=model,
                result={"net_pf": Decimal("1.2")},
            )
        assert first.opening_no == 1
        opening = select(HoldoutOpening).where(HoldoutOpening.id == first.opening_id)
        opened = (await session.execute(opening)).scalar_one()
        assert opened.opened_at_ms == T0
        assert opened.result == {"net_pf": "1.2"}
        assert await holdout_was_opened(session, experiment_id)
        second = await _open_holdout(
            session,
            research_line=BTC,
            holdout_from_ms=split.holdout_from_ms,
            holdout_to_ms=split.holdout_to_ms,
            experiment_id=experiment_id,
            model_version=model,
        )
        assert second.opening_no == 2
        with pytest.raises(HoldoutLimitError):
            await _open_holdout(
                session,
                research_line=BTC,
                holdout_from_ms=split.holdout_from_ms,
                holdout_to_ms=split.holdout_to_ms,
                experiment_id=experiment_id,
                model_version=model,
            )
        with pytest.raises(SplitLeakError, match="цього експерименту"):
            await _open_holdout(
                session,
                research_line=BTC,
                holdout_from_ms=split.holdout_from_ms - HOUR,
                holdout_to_ms=split.holdout_to_ms,
                experiment_id=experiment_id,
                model_version=model,
            )
        newer = _split(data_to_ms=625 * HOUR)
        shifted_id = await _experiment(session, newer, seed=8)
        assert await _open(session, newer, shifted_id, model) == 1
        with pytest.raises(HoldoutNotNewerError):
            await _open_holdout(
                session,
                research_line=BTC,
                holdout_from_ms=split.holdout_from_ms,
                holdout_to_ms=split.holdout_to_ms,
                experiment_id=experiment_id,
                model_version=model,
            )
        recorded = await record_trial(session, experiment_id=experiment_id, split=split, state="FAILED", params={})
        recorded_row = select(ExperimentTrial).where(ExperimentTrial.id == recorded.trial_id)
        row = (await session.execute(recorded_row)).scalar_one()
        assert row.metrics["holdout_opened"] is True


async def test_integrity_error_on_the_opening_limit_is_translated(
    conn: AsyncConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    model = await make_model(conn)
    split = _split()

    def collide(existing: list[tuple[int, int, int]], from_ms: int, to_ms: int) -> int:
        if len(existing) >= 2:
            return 2
        return _opening_no(existing, from_ms, to_ms)

    monkeypatch.setattr("app.ml.validation._opening_no", collide)
    async with AsyncSession(bind=conn) as session:
        experiment_id = await _experiment(session, split)
        window: dict[str, Any] = {
            "research_line": BTC,
            "holdout_from_ms": split.holdout_from_ms,
            "holdout_to_ms": split.holdout_to_ms,
            "experiment_id": experiment_id,
            "model_version": model,
        }
        await _open_holdout(session, **window)
        await _open_holdout(session, **window)
        with pytest.raises(HoldoutLimitError) as caught:
            await _open_holdout(session, **window)
        assert isinstance(caught.value.__cause__, IntegrityError)


async def test_a_missing_model_is_not_reported_as_the_opening_limit(conn: AsyncConnection) -> None:
    split = _split()
    async with AsyncSession(bind=conn) as session:
        experiment_id = await _experiment(session, split)
        with pytest.raises(IntegrityError):
            await _open_holdout(
                session,
                research_line=BTC,
                holdout_from_ms=split.holdout_from_ms,
                holdout_to_ms=split.holdout_to_ms,
                experiment_id=experiment_id,
                model_version="missing-model",
            )
