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
        variant="baseline",
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
    # У BTC бракує однієї години вікна — звіт її показує (дірки й неповні 4h-кошики не інтерполюються).
    inputs = SimpleNamespace(
        split=split,
        histories=(
            # Як у `load_research_inputs`: свічки лише до hold-out — запечатаний відрізок не пропуск.
            SimpleNamespace(symbol=BTC, candles=_flat_bars(split.holdout_from_ms // HOUR, missing=10)),
            SimpleNamespace(symbol=ETH, candles=_flat_bars(split.holdout_from_ms // HOUR)),
        ),
    )
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
    assert (
        "Пропущених свічок ТФ до hold-out (дірки даних або неповні 4h-кошики, без інтерполяції): BTC 1; ETH 0."
        in markdown
    )
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


def test_four_hour_research_aggregates_histories_cuts_at_the_sealed_end_and_keeps_hourly_features(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Рішення 0025: історії правил — 4h з 1h, фічі — 1h, кінець вікна — запечатаний hold-out лінії."""
    from app.ml.validation import H4_MS, holdout_bounds

    seen: dict[str, Any] = {}

    def fake_count(history: Any, **kwargs: Any) -> Callable[[int, int], int]:
        seen["count_bar_ms"] = history.bar_ms
        return lambda _start, _end: 99

    def fake_split(**kwargs: Any) -> Any:
        seen["split"] = kwargs
        cut, _end = holdout_bounds(kwargs["data_from_ms"], kwargs["data_to_ms"], bar_ms=kwargs["bar_ms"])
        return SimpleNamespace(holdout_from_ms=cut, folds=())

    monkeypatch.setattr(optuna_search, "executed_trade_count", fake_count)
    monkeypatch.setattr(optuna_search, "build_split", fake_split)
    sealed_end = 320 * HOUR
    inputs = _prepare_research(
        [_rows(BTC, 400), _rows(ETH, 400)],
        trading_symbol=BTC,
        limits=research_limits(min_equity_usd=Decimal(1000), max_notional_per_symbol_usd=Decimal(50000)),
        initial_equity=Decimal(10000),
        maintenance_margin_rate=Decimal("0.01"),
        seed=1,
        target=TargetParams(),
        costs=CostModel(),
        bar_ms=H4_MS,
        data_to_ms=sealed_end,
    )
    assert seen["split"]["bar_ms"] == H4_MS
    assert seen["split"]["data_to_ms"] == sealed_end
    assert seen["count_bar_ms"] == H4_MS
    cut, _end = holdout_bounds(0, sealed_end, bar_ms=H4_MS)
    for history in inputs.histories:
        assert history.bar_ms == H4_MS
        times = [bar.open_time_ms for bar in history.candles]
        assert all(time % H4_MS == 0 and time < cut for time in times)
        assert times == sorted(set(times))
    for data in inputs.feature_data.values():
        steps = {b.open_time_ms - a.open_time_ms for a, b in zip(data.candles, data.candles[1:], strict=False)}
        assert steps == {HOUR}
        assert data.candles[-1].open_time_ms < cut


def test_research_refuses_a_window_longer_than_the_data(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(optuna_search.OptunaDataError, match="запечатаного"):
        _prepare_research(
            [_rows(BTC, 100)],
            trading_symbol=BTC,
            limits=research_limits(min_equity_usd=Decimal(1000), max_notional_per_symbol_usd=Decimal(50000)),
            initial_equity=Decimal(10000),
            maintenance_margin_rate=Decimal("0.01"),
            seed=1,
            target=TargetParams(),
            costs=CostModel(),
            data_to_ms=200 * HOUR,
        )


def test_cli_runs_only_the_variants_fixed_in_decision_0025() -> None:
    from app.ml.research import VARIANTS, _parse_args
    from app.ml.validation import H4_MS

    assert _parse_args(["iteration-one"]).variant == "baseline"
    assert _parse_args(["iteration-one", "--variant", "B"]).variant == "B"
    # Довільний таргет з командного рядка — прихований підбір; лише зафіксовані варіанти.
    for argv in (["iteration-one", "--variant", "D"], ["iteration-one", "--tp-atr", "5"]):
        with pytest.raises(SystemExit):
            _parse_args(argv)
    assert VARIANTS["baseline"].target == TargetParams()
    assert (VARIANTS["A"].bar_ms, VARIANTS["A"].target.horizon_bars) == (H4_MS, 24)
    assert (VARIANTS["B"].target.tp_atr_mult, VARIANTS["B"].target.sl_atr_mult) == (Decimal(3), Decimal("1.5"))
    assert (VARIANTS["C"].bar_ms, VARIANTS["C"].target.horizon_bars) == (HOUR, 48)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sealed_holdout_of_a_line_is_the_window_of_its_experiments(conn: Any) -> None:
    """Раннер ріже дані по кінцю цього вікна: нові варіанти лінії мають той самий hold-out (рішення 0025)."""
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.ml.validation import create_experiment, sealed_holdout

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
    async with AsyncSession(bind=conn) as session:
        assert await sealed_holdout(session, BTC) is None
        await create_experiment(
            session,
            split=split,
            research_line=BTC,
            symbols=(BTC,),
            label_type="meta",
            primary_strategy={"name": "ema_cross"},
            params={},
            seed=1,
        )
        assert await sealed_holdout(session, BTC) == (split.holdout_from_ms, split.holdout_to_ms)
        assert await sealed_holdout(session, ETH) is None


def test_cli_iteration_two_takes_the_same_fixed_variants() -> None:
    from app.ml.research import COMMAND_ITERATION_TWO, _parse_args

    args = _parse_args(["iteration-two", "--variant", "B"])
    assert (args.command, args.variant) == (COMMAND_ITERATION_TWO, "B")
    assert _parse_args(["iteration-two"]).variant == "B"
    # Ітерація 2 — лише на таргеті, обраному після ітерації 1 (рішення 0025): решта по черзі — прихований вибір.
    for argv in (
        ["iteration-two", "--variant", "A"],
        ["iteration-two", "--variant", "baseline"],
        ["iteration-two", "--variant", "D"],
        ["iteration-two", "--horizon", "12"],
    ):
        with pytest.raises(SystemExit):
            _parse_args(argv)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iteration_two_without_iteration_one_in_the_journal_writes_nothing(conn: Any) -> None:
    from sqlalchemy import func, select

    from app.ml.models import Experiment
    from app.ml.research import IterationTwoRefused, iteration_two
    from tests.ml.test_meta import _EQUITY, _MARGIN, _SEED, _wide_limits, _zero_costs
    from tests.ml.test_research_cycle import _TARGET, _sessions, _two_fold_setup

    books, split = _two_fold_setup()
    sessions = _sessions(conn)
    inputs = SimpleNamespace(split=split, histories=books, feature_data=None)
    with pytest.raises(IterationTwoRefused, match="немає") as refused:
        await iteration_two(
            sessions,
            inputs,  # type: ignore[arg-type]
            trading_symbol=BTC,
            limits=_wide_limits(),
            initial_equity=_EQUITY,
            maintenance_margin_rate=_MARGIN,
            seed=_SEED,
            target=_TARGET,
            costs=_zero_costs(),
        )
    assert refused.value.kind == "missing"
    async with sessions() as session:
        assert (await session.execute(select(func.count()).select_from(Experiment))).scalar_one() == 0


@pytest.mark.asyncio
async def test_iteration_two_refuses_after_iteration_one_passed_or_holdout_opened(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ітерація 1 пройшла G1, hold-out лінії відкривали чи ітерацію 2 вже прогнано — ітерація 2 не стартує (код 16)."""
    import dataclasses

    from app.ml import research
    from app.ml.research_cycle import WAIT_FOR_HUMAN_HOLDOUT

    base = IterationOneReport(
        selection=PrimarySelection(chosen=(EMA_CROSS,), beats_flat=(False,)),
        comparison_trials=(),
        rules=(_rule(EMA_CROSS, (G1_DSR,)),),
        next_step=ITERATION_TWO,
    )

    class _Session:
        async def __aenter__(self) -> _Session:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

    async def _never(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("ітерація 2 не мала стартувати")

    monkeypatch.setattr(research, "run_iteration_two", _never)
    for restored, earlier, kind in (
        (dataclasses.replace(base, next_step=WAIT_FOR_HUMAN_HOLDOUT), (), "passed"),
        (dataclasses.replace(base, hold_out_opened=True), (), "holdout_opened"),
        (base, ("exp-0009",), "repeated"),
    ):

        async def _from_journal(*_args: object, _value: IterationOneReport = restored, **_kwargs: object) -> object:
            return _value

        async def _earlier(*_args: object, _value: tuple[str, ...] = earlier, **_kwargs: object) -> object:
            return _value

        monkeypatch.setattr(research, "iteration_one_from_journal", _from_journal)
        monkeypatch.setattr(research, "direct_runs_on_same_folds", _earlier)
        with pytest.raises(research.IterationTwoRefused) as refused:
            await research.iteration_two(
                _Session,  # type: ignore[arg-type]
                SimpleNamespace(split=None, histories=(), feature_data=None),  # type: ignore[arg-type]
                trading_symbol=BTC,
                limits=research_limits(min_equity_usd=Decimal(1000), max_notional_per_symbol_usd=Decimal(50000)),
                initial_equity=Decimal(10000),
                maintenance_margin_rate=Decimal("0.01"),
                seed=1,
                target=TargetParams(),
                costs=CostModel(),
            )
        assert refused.value.kind == kind
        assert research.REFUSAL_HINTS[kind]


def test_iteration_two_report_shows_folds_warmup_gate_and_the_restored_iteration_one() -> None:
    from app.ml.research import IterationTwoRun, render_iteration_two
    from app.ml.research_cycle import CHANGE_LINE_OR_TIMEFRAME, IterationTwoReport, RecordedTrial
    from app.ml.statistics import G1_MEDIAN_PF, FoldOutcome

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
    inputs = SimpleNamespace(split=split, histories=(SimpleNamespace(symbol=BTC, candles=_flat_bars(1500)),))
    after = IterationOneReport(
        selection=PrimarySelection(chosen=(EMA_CROSS,), beats_flat=(False,)),
        comparison_trials=(RecordedTrial(experiment_id="exp-0005", trial_id=13, trial_no=1, line_trial_no=13),),
        rules=(_rule(EMA_CROSS, (G1_DSR,)),),
        next_step=ITERATION_TWO,
    )
    direct = _renamed(_rule(EMA_CROSS, (G1_MEDIAN_PF, G1_DSR)), "direct_3class", "exp-0009")
    n_folds = len(split.folds)
    folds = tuple(
        FoldOutcome(
            n_trades=0 if index < 2 else 21,
            n_signals=0 if index < 2 else 25,
            total_net_usd=Decimal(0) if index < 2 else Decimal("-12.5"),
            profit_factor=None if index < 2 else Decimal("0.91"),
            below_min_trades=False,
            cost_factor=Decimal(1),
        )
        for index in range(n_folds)
    )
    outcome = IterationTwoRun(
        after=after,
        report=IterationTwoReport(experiment=direct, next_step=CHANGE_LINE_OR_TIMEFRAME),
        folds=folds,
        n_observations=tuple(100 + index for index in range(n_folds)),
        flat_rows=tuple(400 + index for index in range(n_folds)),
    )
    markdown = render_iteration_two(
        outcome,
        inputs,  # type: ignore[arg-type]
        trading_symbol=BTC,
        n_line_trials=25,
        initial_equity=Decimal(10000),
        target=TargetParams(),
        costs=CostModel(),
        started_ms=0,
        finished_ms=60 * 60_000,
    )
    assert "Hold-out **не відкривався**" in markdown
    assert "`exp-0005`" in markdown
    assert "| 0 (прогрів) |" in markdown
    assert "| 102 | 402 | 21 | -12.50 | 0.91 |" in markdown
    assert "| `direct_3class` (`exp-0009`, спроба 7) | 73 | 1.08 | 50% | 0.42 | 0.61 | не пройдено |" in markdown
    assert REASON_TEXT[G1_MEDIAN_PF] in markdown
    assert "`change_line_or_timeframe`" in markdown
    assert "Спроб у лінії BTC/USDT:USDT після прогону: **25**" in markdown


def _renamed(rule: RuleResult, name: str, experiment_id: str) -> RuleResult:
    import dataclasses

    return dataclasses.replace(rule, spec_name=name, experiment_id=experiment_id)


def _flat_bars(hours: int, *, missing: int | None = None) -> tuple[SimCandle, ...]:
    price = Decimal(100)
    return tuple(
        SimCandle(open_time_ms=hour * HOUR, open=price, high=price, low=price, close=price)
        for hour in range(hours)
        if hour != missing
    )
