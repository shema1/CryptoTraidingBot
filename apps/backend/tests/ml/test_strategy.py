"""Бейзлайни і первинна стратегія: сигнал на закритті t-1, вибір лише на walk-forward (ТЗ §5.1, §5А.3)."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.exchange.types import PositionSide
from app.ml.backtest import EntrySignal, run_backtest
from app.ml.features import EMA_FAST, EMA_SLOW, FEATURE_LOOKBACK_BARS, HOUR_MS
from app.ml.strategy import (
    ALWAYS_FLAT,
    ALWAYS_LONG,
    BREAKOUT,
    COMPARISON_SPECS,
    EMA_CROSS,
    PRIMARY_CANDIDATES,
    RANDOM_ENTRIES,
    REFERENCE_BASELINES,
    FoldScore,
    PrimarySelection,
    RuleReport,
    StrategyComparison,
    StrategySpec,
    SymbolHistory,
    compare_strategies,
    executed_trade_count,
    is_reference_baseline,
    primary_candidates,
    primary_side,
    primary_signals,
    record_strategy_comparison,
    select_primary,
)
from app.ml.trade_sim import SimCandle, SkipReason, TargetParams, label_meta
from app.ml.validation import ResearchSplit, SplitPurpose, build_split, create_experiment, rows_for
from app.risk.costs import CostModel, FundingEvent
from app.risk.reasons import RiskReason
from tests.risk.conftest import instrument, limits

BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
HOUR = HOUR_MS
_LONG = PositionSide.LONG
_SHORT = PositionSide.SHORT


def _bar(index: int, open_: str, high: str, low: str, close: str) -> SimCandle:
    return SimCandle(
        open_time_ms=index * HOUR,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
    )


def _flat(count: int, *, close: str = "100") -> list[SimCandle]:
    return [_bar(index, close, "101", "99", close) for index in range(count)]


def _zero_costs() -> CostModel:
    zero = Decimal(0)
    return CostModel(
        taker_fee_rate=Decimal("0.00055"),
        maker_fee_rate=Decimal("0.00055"),
        slippage_entry_atr=zero,
        slippage_entry_bps=zero,
        slippage_sl_atr=zero,
        slippage_sl_bps=zero,
        slippage_tp_atr=zero,
        slippage_tp_bps=zero,
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


def _score(fold: int, net: str, trades: int = 1) -> FoldScore:
    return FoldScore(
        fold_index=fold,
        test_start_ms=fold * HOUR,
        test_end_ms=(fold + 1) * HOUR,
        trading_net_usd=Decimal(net),
        trading_trades=trades,
        trading_signals=max(trades, 1),
        trading_profit_factor=None,
        all_symbols_net_usd=Decimal(net),
        all_symbols_trades=trades,
    )


def _report(spec: StrategySpec, nets: tuple[tuple[str, int], ...]) -> RuleReport:
    return RuleReport(
        spec=spec,
        scores=tuple(_score(index, net, trades) for index, (net, trades) in enumerate(nets)),
    )


def test_catalog_is_fixed_and_not_a_search() -> None:
    assert EMA_CROSS.fast == EMA_FAST == 20
    assert EMA_CROSS.slow == EMA_SLOW == 50
    assert EMA_CROSS.metadata() == {"name": "ema_cross", "params": {"fast": 20, "slow": 50}}
    assert BREAKOUT.metadata() == {"name": "breakout", "params": {"lookback": 24}}
    assert PRIMARY_CANDIDATES == (EMA_CROSS, BREAKOUT)
    assert [spec.name for spec in REFERENCE_BASELINES] == ["always_flat", "always_long", "random"]
    with pytest.raises(ValueError, match="перетин EMA"):
        StrategySpec(name="ema_cross", fast=50, slow=20)
    with pytest.raises(ValueError, match="бейзлайна"):
        StrategySpec(name="random", lookback=10)


def test_reference_baseline_is_the_rule_not_a_flag() -> None:
    assert is_reference_baseline(ALWAYS_FLAT.trial_params())
    assert is_reference_baseline(ALWAYS_LONG.metadata())
    assert is_reference_baseline(RANDOM_ENTRIES.trial_params())
    assert not is_reference_baseline(EMA_CROSS.trial_params())
    assert not is_reference_baseline(BREAKOUT.metadata())
    hidden = {"role": "baseline", "strategy": EMA_CROSS.metadata()}
    assert not is_reference_baseline(hidden)
    assert not is_reference_baseline({"role": "baseline"})
    # Спроба моделі з тим самим правилом або випадковий вхід із пошуком seed — не опорний бейзлайн.
    model_trial = {"role": "baseline", "strategy": ALWAYS_LONG.metadata(), "lgbm": {"num_leaves": 63}}
    assert not is_reference_baseline(model_trial)
    assert not is_reference_baseline({**RANDOM_ENTRIES.trial_params(), "seed_search": 17})
    assert not is_reference_baseline({**ALWAYS_FLAT.metadata(), "note": "x"})
    assert not is_reference_baseline({"role": "candidate", "strategy": ALWAYS_FLAT.metadata()})


def test_always_flat_emits_nothing_and_always_long_skips_a_gap() -> None:
    candles = _flat(8)
    assert primary_signals(ALWAYS_FLAT, candles, horizon_bars=4) == ()
    assert primary_candidates(ALWAYS_FLAT, candles, symbol=BTC) == ()
    gap = [_bar(0, "100", "101", "99", "100"), _bar(1, "100", "101", "99", "100"), _bar(3, "100", "101", "99", "100")]
    assert primary_side(ALWAYS_LONG, gap, 1) is _LONG
    assert primary_side(ALWAYS_LONG, gap, 2) is None
    assert primary_side(ALWAYS_LONG, candles, 0) is None


def test_signals_drop_the_tail_by_index_and_match_primary_side() -> None:
    candles = _flat(12)
    signals = primary_signals(ALWAYS_LONG, candles, horizon_bars=4)
    assert [signal.entry_index for signal in signals] == list(range(1, 8))
    assert all(signal.side is _LONG and signal.computed_at_ms is None for signal in signals)
    for signal in signals:
        assert primary_side(ALWAYS_LONG, candles, signal.entry_index) is signal.side
    candidates = primary_candidates(ALWAYS_LONG, candles, symbol=BTC)
    assert candidates[-1].entry_index == 11
    assert 11 not in {signal.entry_index for signal in signals}


def test_skipped_candidate_is_not_a_zero_label() -> None:
    candles = _flat(30)
    candidates = primary_candidates(ALWAYS_LONG, candles, symbol=BTC)
    labeled = label_meta(candles, candidates, params=TargetParams(horizon_bars=4))
    assert labeled.samples == ()
    assert labeled.skipped
    assert {item.reason for item in labeled.skipped} <= {SkipReason.NO_ATR, SkipReason.INCOMPLETE_PATH}
    assert len(labeled.skipped) == len(candidates)


def test_meta_label_of_a_long_candidate_comes_from_label_meta() -> None:
    candles = _flat(FEATURE_LOOKBACK_BARS + 30)
    entry = FEATURE_LOOKBACK_BARS
    candles[entry] = _bar(entry, "100", "120", "99", "110")
    candidate = next(item for item in primary_candidates(ALWAYS_LONG, candles, symbol=BTC) if item.entry_index == entry)
    labeled = label_meta(candles, (candidate,), params=TargetParams(horizon_bars=4), costs=_zero_costs())
    assert len(labeled.samples) == 1
    assert labeled.samples[0].label == 1
    assert labeled.skipped == ()


def test_ema_cross_uses_only_closes_before_the_entry() -> None:
    spec = StrategySpec(name="ema_cross", fast=2, slow=4)
    closes = ["10"] * 5 + ["30", "30"]
    candles = [_bar(index, close, close, close, close) for index, close in enumerate(closes)]
    candles.append(_bar(7, "1", "1", "1", "1"))
    assert primary_side(spec, candles, 5) is None
    assert primary_side(spec, candles, 6) is _LONG
    # Перетин уже стався: наступний бар, де швидка лишається вище, нового сигналу не дає.
    assert primary_side(spec, candles, 7) is None
    later = candles.copy()
    later[6] = _bar(6, "1", "1", "1", "1")
    later[7] = _bar(7, "1", "1", "1", "1")
    assert primary_side(spec, later, 6) is _LONG


def test_ema_ignores_closes_older_than_the_feature_window_and_the_entry_bar() -> None:
    spec = StrategySpec(name="ema_cross", fast=2, slow=4)
    count = FEATURE_LOOKBACK_BARS + 6
    candles = [_bar(index, "10", "10", "10", "10") for index in range(count)]
    entry = FEATURE_LOOKBACK_BARS + 2
    decision = entry - 1
    candles[decision] = _bar(decision, "30", "30", "30", "30")
    assert primary_side(spec, candles, entry) is _LONG
    candles[0] = _bar(0, "999", "999", "999", "999")
    candles[entry] = _bar(entry, "1", "1", "1", "1")
    candles[entry + 1] = _bar(entry + 1, "1", "1", "1", "1")
    assert primary_side(spec, candles, entry) is _LONG
    candles[decision] = _bar(decision, "10", "10", "10", "10")
    assert primary_side(spec, candles, entry) is None


def test_breakout_uses_the_prior_channel_not_the_entry_bar() -> None:
    spec = StrategySpec(name="breakout", lookback=3)
    candles = [
        _bar(0, "9", "10", "8", "9"),
        _bar(1, "10", "11", "9", "10"),
        _bar(2, "11", "12", "10", "11"),
        _bar(3, "12", "14", "11", "13"),
        _bar(4, "50", "80", "1", "70"),
    ]
    assert primary_side(spec, candles, 4) is _LONG
    candles[4] = _bar(4, "1", "1", "1", "1")
    assert primary_side(spec, candles, 4) is _LONG
    # Бар до каналу (вікно 3 для входу 5: канал — бари 1..3) на сигнал не впливає.
    short = [
        _bar(0, "1", "100", "1", "1"),
        _bar(1, "10", "11", "9", "10"),
        _bar(2, "10", "11", "9", "10"),
        _bar(3, "10", "11", "9", "10"),
        _bar(4, "12", "13", "8", "8"),
        _bar(5, "9", "9", "9", "9"),
    ]
    assert primary_side(spec, short, 5) is _SHORT
    short[0] = _bar(0, "1", "1", "1", "1")
    assert primary_side(spec, short, 5) is _SHORT


def test_random_entries_depend_on_seed_and_time_not_on_price() -> None:
    candles = _flat(40)
    first = primary_signals(RANDOM_ENTRIES, candles, horizon_bars=4, seed=3)
    assert first == primary_signals(RANDOM_ENTRIES, candles, horizon_bars=4, seed=3)
    bumped = [
        bar.model_copy(update={"open": bar.open + 5, "high": bar.high + 5, "low": bar.low + 5, "close": bar.close + 5})
        for bar in candles
    ]
    assert [(item.entry_index, item.side) for item in first] == [
        (item.entry_index, item.side) for item in primary_signals(RANDOM_ENTRIES, bumped, horizon_bars=4, seed=3)
    ]
    assert primary_signals(RANDOM_ENTRIES, candles, horizon_bars=4, seed=4) != first
    assert all(item.side in (_LONG, _SHORT) for item in first)
    assert len(first) < 36


def test_selection_keeps_at_most_two_candidates_that_beat_flat() -> None:
    split = _split()
    flat = _report(ALWAYS_FLAT, (("0", 0), ("0", 0)))
    ema = _report(EMA_CROSS, (("10", 3), ("-2", 3)))
    weak = _report(BREAKOUT, (("-5", 4), ("-5", 4)))
    both = StrategyComparison(
        split=split,
        seed=1,
        trading_symbol=BTC,
        variants=(flat, ema, weak),
        oos_rows=(),
    )
    chosen = select_primary(both)
    assert chosen.chosen == (EMA_CROSS,)
    assert chosen.beats_flat == (True,)
    assert chosen.metadata()["criterion"] == "median_fold_net_usd"
    assert "holdout" not in chosen.metadata()

    strong_breakout = _report(BREAKOUT, (("8", 2), ("8", 2)))
    pair = select_primary(
        StrategyComparison(
            split=split,
            seed=1,
            trading_symbol=BTC,
            variants=(flat, ema, strong_breakout),
            oos_rows=(),
        )
    )
    assert pair.chosen == (BREAKOUT, EMA_CROSS)
    assert pair.beats_flat == (True, True)

    losers = select_primary(
        StrategyComparison(
            split=split,
            seed=1,
            trading_symbol=BTC,
            variants=(flat, _report(EMA_CROSS, (("-1", 5), ("-1", 5))), weak),
            oos_rows=(),
        )
    )
    assert losers.chosen == (EMA_CROSS,)
    assert losers.beats_flat == (False,)
    with pytest.raises(ValueError, match="кандидат"):
        PrimarySelection(chosen=(ALWAYS_LONG,), beats_flat=(False,))


def _history(symbol: str, candles: list[SimCandle]) -> SymbolHistory:
    # Ставка 0 на епосі: ліміт funding вимкнений, а слот утримання не лишається без відомої ставки.
    funding = (FundingEvent(time_ms=0, rate=Decimal(0), mark_price=Decimal(100)),)
    return SymbolHistory(
        symbol=symbol,
        candles=tuple(candles),
        instrument=instrument(symbol=symbol),
        funding=funding,
    )


def _research_split() -> ResearchSplit:
    return build_split(
        data_from_ms=0,
        data_to_ms=2000 * HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        min_train_ms=1400 * HOUR,
        test_segment_ms=48 * HOUR,
        test_segment_extended_ms=96 * HOUR,
    )


def _books(candles: list[SimCandle]) -> tuple[SymbolHistory, SymbolHistory]:
    return (_history(BTC, candles), _history(ETH, candles))


def _compare(candles: list[SimCandle]) -> StrategyComparison:
    return _compare_on(_books(candles), COMPARISON_SPECS)


def _compare_on(books: tuple[SymbolHistory, ...], specs: tuple[StrategySpec, ...]) -> StrategyComparison:
    return compare_strategies(
        books,
        split=_research_split(),
        trading_symbol=BTC,
        limits=limits(),
        initial_equity=Decimal(10000),
        maintenance_margin_rate=Decimal("0.005"),
        seed=7,
        params=TargetParams(horizon_bars=4),
        costs=_zero_costs(),
        specs=specs,
    )


def _wave(count: int) -> list[SimCandle]:
    """Пилка з періодом 40 годин і дрібним шумом: і перетини EMA, і пробої в кожному фолді."""
    out: list[SimCandle] = []
    previous = Decimal(100)
    for index in range(count):
        phase = index % 40
        close = Decimal(90) + Decimal(min(phase, 40 - phase)) * Decimal("0.6") + Decimal(index * 7 % 5) * Decimal("0.4")
        high = max(previous, close) + Decimal("0.3")
        low = min(previous, close) - Decimal("0.3")
        out.append(SimCandle(open_time_ms=index * HOUR, open=previous, high=high, low=low, close=close))
        previous = close
    return out


def test_comparison_uses_the_backtester_and_ignores_the_holdout() -> None:
    candles = _flat(2000)
    comparison = _compare(candles)
    split = comparison.split
    flat = next(item for item in comparison.variants if item.spec == ALWAYS_FLAT)
    long = next(item for item in comparison.variants if item.spec == ALWAYS_LONG)
    assert flat.trading_trades == 0
    assert flat.median_net_usd == Decimal(0)
    assert long.trading_trades >= 1
    report = next(item for item in long.folds[0].reports if item.symbol == BTC)
    assert all(trade.side is _LONG for trade in report.trades)
    assert RiskReason.RISK_MAX_POSITIONS.value in {
        decision.reason_code for decision in report.decisions if decision.decision == "SKIP"
    }
    assert comparison.oos_rows == rows_for(comparison.oos_rows, split, SplitPurpose.PRIMARY_STRATEGY)
    assert all(row.time_ms < split.holdout_from_ms for row in comparison.oos_rows)
    for variant in comparison.variants:
        for fold in variant.folds:
            for trade in (trade for item in fold.reports for trade in item.trades):
                assert trade.entry_open_time_ms < split.holdout_from_ms
                assert trade.exit_open_time_ms < split.holdout_from_ms

    mutated = [
        bar.model_copy(update={"open": Decimal(150), "high": Decimal(160), "low": Decimal(140), "close": Decimal(155)})
        if bar.open_time_ms >= split.holdout_from_ms
        else bar
        for bar in candles
    ]
    again = _compare(mutated)
    assert [item.scores for item in again.variants] == [item.scores for item in comparison.variants]
    assert [(row.symbol, row.time_ms, row.index) for row in again.oos_rows] == [
        (row.symbol, row.time_ms, row.index) for row in comparison.oos_rows
    ]
    selected = select_primary(comparison)
    assert selected.chosen == (BREAKOUT,)
    assert selected.beats_flat == (False,)


def test_candidate_trades_come_from_the_full_series_rule_inside_each_fold() -> None:
    """EMA і пробій торгують: кожен вхід — сигнал `primary_side` повного ряду в тесті свого фолду."""
    candles = _wave(2000)
    comparison = _compare_on((_history(BTC, candles),), PRIMARY_CANDIDATES)
    split = comparison.split
    for variant in comparison.variants:
        assert all(score.trading_trades >= 1 for score in variant.scores)
        for fold, record in zip(split.folds, variant.folds, strict=True):
            hits = [
                bar.open_time_ms
                for index, bar in enumerate(candles)
                if fold.test_start_ms <= bar.open_time_ms < fold.test_end_ms
                and primary_side(variant.spec, candles, index) is not None
            ]
            report = record.reports[0]
            assert report.n_signals == len(hits)
            for trade in report.trades:
                assert trade.entry_open_time_ms in hits
                assert primary_side(variant.spec, candles, trade.entry_open_time_ms // HOUR) is trade.side
    mutated = [
        bar.model_copy(update={"open": Decimal(150), "high": Decimal(160), "low": Decimal(140), "close": Decimal(155)})
        if bar.open_time_ms >= split.holdout_from_ms
        else bar
        for bar in candles
    ]
    again = _compare_on((_history(BTC, mutated),), PRIMARY_CANDIDATES)
    assert [item.scores for item in again.variants] == [item.scores for item in comparison.variants]


def test_fold_path_ends_after_the_horizon_not_at_the_holdout() -> None:
    """Шлях фолду — до `test_end + horizon`: свічки до hold-out не розводять Sharpe барів фолду."""
    candles = _flat(2000)
    history = _history(BTC, candles)
    comparison = _compare_on((history,), (ALWAYS_LONG,))
    split = comparison.split
    fold = split.folds[0]
    path_from = fold.test_start_ms - FEATURE_LOOKBACK_BARS * HOUR
    path_to = fold.test_end_ms + split.horizon_bars * HOUR
    assert path_to < split.holdout_from_ms
    path = [bar for bar in candles if path_from <= bar.open_time_ms < path_to]
    signals = [
        EntrySignal(entry_index=position, side=_LONG)
        for position, bar in enumerate(path)
        if fold.test_start_ms <= bar.open_time_ms < fold.test_end_ms
    ]
    expected = run_backtest(
        path,
        signals,
        symbol=BTC,
        instrument=history.instrument,
        limits=limits(),
        initial_equity=Decimal(10000),
        maintenance_margin_rate=Decimal("0.005"),
        seed=7,
        costs=_zero_costs(),
        params=TargetParams(horizon_bars=4),
        funding=history.funding,
    )
    assert expected.metrics.sharpe is not None
    assert comparison.variants[0].folds[0].reports[0] == expected


def test_a_gap_after_the_last_signal_does_not_drop_it_from_the_fold() -> None:
    """Дірка між `test_end` і `test_end + horizon`: шлях подовжується за індексом, сигнал лишається."""
    fold = _research_split().folds[0]
    gap = fold.test_end_ms + HOUR
    candles = [bar for bar in _flat(2000) if bar.open_time_ms != gap]
    comparison = _compare_on((_history(BTC, candles),), (ALWAYS_LONG,))
    report = comparison.variants[0].folds[0].reports[0]
    assert report.n_signals == (fold.test_end_ms - fold.test_start_ms) // HOUR


def _counter(
    history: SymbolHistory,
    specs: tuple[StrategySpec, ...] = PRIMARY_CANDIDATES,
) -> Callable[[int, int], int]:
    return executed_trade_count(
        history,
        horizon_bars=4,
        limits=limits(),
        initial_equity=Decimal(10000),
        maintenance_margin_rate=Decimal("0.005"),
        seed=7,
        params=TargetParams(horizon_bars=4),
        costs=_zero_costs(),
        specs=specs,
    )


def test_trade_count_starts_a_fresh_account_and_stops_before_the_path_limit() -> None:
    candles = _flat(2000)
    history = _history(BTC, candles)
    start, end = 1400 * HOUR, 1448 * HOUR
    count = _counter(history, (ALWAYS_LONG,))
    assert count(start, end) >= 1
    assert _counter(history, (BREAKOUT,))(start, end) == 0
    path_limit = (end + 4 * HOUR) // HOUR
    candles[path_limit] = _bar(path_limit, "100", "180", "20", "100")
    assert _counter(_history(BTC, candles), (ALWAYS_LONG,))(start, end) == count(start, end)


def test_fold_length_follows_the_rarest_candidate_not_always_long() -> None:
    """ТЗ §5.2.4: фолд подовжується, коли угод бракує найрідшому кандидату; «завжди LONG» це ховав."""
    history = _history(BTC, _wave(2000))
    rarest = _counter(history)
    by_rule = {spec: _counter(history, (spec,)) for spec in PRIMARY_CANDIDATES}
    # Той самий прогін, що в порівнянні: угоди вікна збігаються з угодами фолду кожного кандидата.
    comparison = _compare_on((history,), PRIMARY_CANDIDATES)
    for variant in comparison.variants:
        for fold, score in zip(comparison.split.folds, variant.scores, strict=True):
            assert by_rule[variant.spec](fold.test_start_ms, fold.test_end_ms) == score.trading_trades
    for fold in comparison.split.folds:
        window = (fold.test_start_ms, fold.test_end_ms)
        assert rarest(*window) == min(count(*window) for count in by_rule.values())

    def split_with(count_trades: Callable[[int, int], int]) -> ResearchSplit:
        return build_split(
            data_from_ms=0,
            data_to_ms=2000 * HOUR,
            horizon_bars=4,
            count_trades=count_trades,
            min_train_ms=1400 * HOUR,
            test_segment_ms=48 * HOUR,
            test_segment_extended_ms=96 * HOUR,
            min_trades_per_fold=5,
        )

    long_only = split_with(_counter(history, (ALWAYS_LONG,)))
    assert not any(fold.extended for fold in long_only.folds)
    candidates = split_with(rarest)
    assert candidates.folds[0].extended
    assert candidates.folds[0].test_end_ms - candidates.folds[0].test_start_ms == 96 * HOUR
    assert len(candidates.folds) < len(long_only.folds)
    first = candidates.folds[0]
    assert first.n_trading_trades == rarest(first.test_start_ms, first.test_end_ms)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_every_variant_including_the_rejected_one_is_a_trial(conn: AsyncConnection) -> None:
    candles = _flat(2000)
    comparison = _compare(candles)
    selection = select_primary(comparison)
    async with AsyncSession(bind=conn) as session:
        experiment_id = await create_experiment(
            session,
            split=comparison.split,
            research_line=BTC,
            symbols=(BTC, ETH),
            label_type="meta",
            primary_strategy=selection.metadata(),
            params={"stage": "primary_strategy"},
            seed=comparison.seed,
        )
        recorded = await record_strategy_comparison(session, experiment_id=experiment_id, comparison=comparison)
    assert len(recorded) == 5
    assert [item.line_trial_no for item in recorded] == list(range(1, 6))
    assert is_reference_baseline(ALWAYS_FLAT.trial_params())
    assert not is_reference_baseline(selection.metadata()["selected"][0])
