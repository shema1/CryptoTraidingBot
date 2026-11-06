"""`python -m app.ml.research` (T2.14): завантаження з БД, розбивка за всіма кандидатами, блокування, звіт."""

from __future__ import annotations

import argparse
from collections.abc import AsyncIterator, Callable, Sequence
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import delete, text
from sqlalchemy.engine import URL

from app.core.config import Settings
from app.core.db import create_engine
from app.core.instance_lock import LOCK_NAMESPACE, RESEARCH_LOCK_ID
from app.data.models import Candle
from app.ml import optuna_search
from app.ml.features import TimedValue
from app.ml.optuna_search import OptunaParamsError, _prepare_research, _SymbolRows, load_research_inputs
from app.ml.research import (
    EXIT_DATA,
    EXIT_LOCK_HELD,
    REASON_TEXT,
    main,
    render_iteration_one,
    research_limits,
    run,
)
from app.ml.research_cycle import ITERATION_TWO, IterationOneReport, RuleResult
from app.ml.statistics import G1_DSR, G1_EVALUATED_FOLDS, G1Preview, GateDecision
from app.ml.strategy import BREAKOUT, EMA_CROSS, PRIMARY_CANDIDATES, PrimarySelection, StrategySpec
from app.ml.trade_sim import SimCandle, TargetParams
from app.ml.validation import build_split
from app.risk.costs import CostModel
from tests.risk.conftest import instrument

BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
HOUR = 3_600_000


def _rows(symbol: str, hours: int) -> _SymbolRows:
    price = Decimal(100)
    bars = [
        SimCandle(open_time_ms=hour * HOUR, open=price, high=price + 2, low=price, close=price + 1)
        for hour in range(hours)
    ]
    return _SymbolRows(
        symbol=symbol,
        candles=tuple((bar, Decimal(5)) for bar in bars),
        instrument=instrument(symbol=symbol),
        marks=tuple(bars),
        payments=tuple((hour * HOUR, Decimal("0.0001")) for hour in range(0, hours, 8)),
        open_interest=tuple(TimedValue(hour * HOUR, 1.0) for hour in range(hours)),
        premium=(),
        long_short=(),
        liquidations=(),
        gaps={},
    )


def test_research_split_counts_trades_of_every_primary_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    """T2.12 (а): фолди подовжуються за найрідшим з усіх кандидатів, а не за правилом, що виграє порівняння."""
    seen: dict[str, Any] = {}

    def fake_count(history: Any, **kwargs: Any) -> Callable[[int, int], int]:
        seen["history"] = history.symbol
        seen["specs"] = kwargs["specs"]
        return lambda _start, _end: 99

    def fake_split(**kwargs: Any) -> Any:
        seen["split"] = kwargs
        return SimpleNamespace(holdout_from_ms=kwargs["data_to_ms"] - 20 * HOUR, folds=())

    monkeypatch.setattr(optuna_search, "executed_trade_count", fake_count)
    monkeypatch.setattr(optuna_search, "build_split", fake_split)
    inputs = _prepare_research(
        [_rows(BTC, 100), _rows(ETH, 100)],
        trading_symbol=BTC,
        limits=research_limits(min_equity_usd=Decimal(1000), max_notional_per_symbol_usd=Decimal(50000)),
        initial_equity=Decimal(10000),
        maintenance_margin_rate=Decimal("0.01"),
        seed=1,
        target=TargetParams(),
        costs=CostModel(),
    )
    assert seen["history"] == BTC
    assert tuple(seen["specs"]) == PRIMARY_CANDIDATES
    assert seen["split"]["horizon_bars"] == TargetParams().horizon_bars
    # Ряди ріжуться по початку hold-out: жодна свічка hold-out не потрапляє в історії циклу.
    assert all(bar.open_time_ms < 80 * HOUR for history in inputs.histories for bar in history.candles)


@pytest.mark.asyncio
async def test_research_refuses_a_trading_symbol_outside_the_symbols() -> None:
    with pytest.raises(OptunaParamsError):
        await load_research_inputs(
            None,  # type: ignore[arg-type]  # відмова до звернення до БД
            symbols=(ETH,),
            trading_symbol=BTC,
            limits=research_limits(min_equity_usd=Decimal(1000), max_notional_per_symbol_usd=Decimal(50000)),
            initial_equity=Decimal(10000),
            maintenance_margin_rate=Decimal("0.01"),
            seed=1,
            target=TargetParams(),
        )


def test_cli_needs_a_command_and_positive_numbers() -> None:
    with pytest.raises(SystemExit) as missing:
        main([])
    assert missing.value.code == 2
    with pytest.raises(SystemExit) as negative:
        main(["iteration-one", "--initial-equity", "-5"])
    assert negative.value.code == 2


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        command="iteration-one",
        seed=1,
        initial_equity=Decimal(10000),
        maintenance_margin_rate=Decimal("0.01"),
        min_equity_usd=Decimal(1000),
        max_notional_per_symbol_usd=Decimal(50000),
        report=tmp_path / "report.md",
    )


@pytest_asyncio.fixture
async def test_settings(migrated_db: URL) -> AsyncIterator[Settings]:
    """Settings, що дивляться в тестову БД (`run` бере `database_url()` без `test=True`). Свічок немає."""
    engine = create_engine(migrated_db)
    async with engine.begin() as conn:
        await conn.execute(delete(Candle))
    await engine.dispose()
    assert migrated_db.database is not None
    yield Settings().model_copy(update={"postgres_db": migrated_db.database})


@pytest.mark.integration
@pytest.mark.asyncio
async def test_second_run_on_the_same_db_exits_without_writing(test_settings: Settings, tmp_path: Path) -> None:
    """Кожен прогін пише спроби в журнал лінії: два одночасні подвоїли б N. Другий — код 10, звіту немає."""
    engine = create_engine(test_settings.database_url())
    try:
        async with engine.connect() as holder:
            got = await holder.scalar(
                text("SELECT pg_try_advisory_lock(:namespace, :lock_id)"),
                {"namespace": LOCK_NAMESPACE, "lock_id": RESEARCH_LOCK_ID},
            )
            assert got is True
            assert await run(test_settings, _args(tmp_path)) == EXIT_LOCK_HELD
    finally:
        await engine.dispose()
    assert not (tmp_path / "report.md").exists()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_run_without_candles_says_the_data_is_missing(test_settings: Settings, tmp_path: Path) -> None:
    assert await run(test_settings, _args(tmp_path)) == EXIT_DATA
    assert not (tmp_path / "report.md").exists()


def _rule(spec: StrategySpec, reasons: Sequence[str]) -> RuleResult:
    decision = GateDecision(
        passed=not reasons,
        reasons=tuple(reasons),
        dsr=Decimal("0.42"),
        pbo=Decimal("0.61"),
        n_trials=9,
        n_trades=73,
        median_pf=Decimal("1.08"),
        median_pf_unbounded=False,
        profitable_fold_share=Decimal("0.5"),
        warmup_folds=2,
    )
    preview = G1Preview(trial_id=7, decision=decision, dsr=decision.dsr, pbo=decision.pbo, n_trials=9)
    return RuleResult(spec_name=spec.name, experiment_id="exp-2", trial_id=7, decision_error=None, preview=preview)


def test_report_shows_the_gate_numbers_reasons_and_that_the_holdout_stayed_sealed() -> None:
    split = build_split(
        data_from_ms=0,
        data_to_ms=1500 * HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 1,
        min_train_ms=48 * HOUR,
        test_segment_ms=24 * HOUR,
        test_segment_extended_ms=48 * HOUR,
        min_trades_per_fold=1,
    )
    inputs = SimpleNamespace(split=split, histories=(SimpleNamespace(symbol=BTC), SimpleNamespace(symbol=ETH)))
    report = IterationOneReport(
        selection=PrimarySelection(chosen=(BREAKOUT, EMA_CROSS), beats_flat=(True, False)),
        comparison_trials=(),
        rules=(_rule(BREAKOUT, (G1_DSR, G1_EVALUATED_FOLDS)), _rule(EMA_CROSS, (G1_DSR,))),
        next_step=ITERATION_TWO,
    )
    markdown = render_iteration_one(
        report,
        inputs,  # type: ignore[arg-type]
        trading_symbol=BTC,
        n_line_trials=9,
        initial_equity=Decimal(10000),
        target=TargetParams(),
        costs=CostModel(),
        started_ms=0,
        finished_ms=60 * 60_000,
    )
    assert "Hold-out **не відкривався**" in markdown
    assert "| `breakout` (`exp-2`, спроба 7) | 73 | 1.08 | 50% | 0.42 | 0.61 | не пройдено |" in markdown
    assert REASON_TEXT[G1_EVALUATED_FOLDS] in markdown
    assert "`ema_cross` (не краще за «завжди FLAT»)" in markdown
    assert "Спроб у лінії BTC/USDT:USDT після прогону: **9**" in markdown
    assert "`iteration_two`" in markdown


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lock_check_sees_its_own_lock_and_only_its_own(test_settings: Settings) -> None:
    """Звіт попереджає, якщо lock прогону зник (обрив з'єднання мовчки його звільняє)."""
    from app.ml.research import _lock_still_held, _take_lock

    engine = create_engine(test_settings.database_url())
    try:
        async with engine.connect() as holder, engine.connect() as stranger:
            await _take_lock(holder)
            assert await _lock_still_held(holder) is True
            assert await _lock_still_held(stranger) is False
    finally:
        await engine.dispose()
