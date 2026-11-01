"""Walk-forward, embargo і hold-out (ТЗ §5.2.4, §5А.6, T2.5)."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.data.gaps import GapRange
from app.exchange.types import PositionSide
from app.ml.features import (
    HOUR_MS,
    SOURCE_LIQUIDATIONS,
    CandleBar,
    LiquidationBar,
    SymbolData,
    compute_features,
)
from app.ml.trade_sim import (
    DirectClass,
    DirectSample,
    ExitReason,
    MetaSample,
    SimulatedTrade,
    StopPriceSource,
)
from app.ml.validation import (
    DAY_MS,
    DEFAULT_HOLDOUT_FRACTION,
    MIN_TRADES_PER_FOLD,
    MIN_TRAIN_MS,
    TEST_SEGMENT_EXTENDED_MS,
    TEST_SEGMENT_MS,
    HoldoutLimitError,
    HoldoutNotNewerError,
    LabelStats,
    ResearchSplit,
    SampleRow,
    SplitLeakError,
    SplitPurpose,
    TradeCount,
    _opening_no,
    _sealed_holdout,
    build_split,
    guard_sample_weights,
    holdout_bounds,
    is_holdout_row,
    label_diagnostics,
    label_overlaps_test,
    reject_holdout,
    require_train_coverage,
    rows_for,
    train_coverage_ms,
)

BTC = "BTC/USDT:USDT"
ETH = "ETH/USDT:USDT"
HOUR = HOUR_MS


def _count(n: int = MIN_TRADES_PER_FOLD) -> TradeCount:
    def count(_start: int, _end: int) -> int:
        return n

    return count


def _split(
    *,
    data_from_ms: int = 0,
    data_to_ms: int = 500 * HOUR,
    horizon_bars: int = 4,
    count_trades: TradeCount | None = None,
    embargo_bars: int | None = None,
    min_train_ms: int = 48 * HOUR,
    test_segment_ms: int = 24 * HOUR,
    test_segment_extended_ms: int = 48 * HOUR,
) -> ResearchSplit:
    return build_split(
        data_from_ms=data_from_ms,
        data_to_ms=data_to_ms,
        horizon_bars=horizon_bars,
        count_trades=_count() if count_trades is None else count_trades,
        embargo_bars=embargo_bars,
        min_train_ms=min_train_ms,
        test_segment_ms=test_segment_ms,
        test_segment_extended_ms=test_segment_extended_ms,
    )


def _row(symbol: str, hour: int, index: int | None = None) -> SampleRow:
    return SampleRow(symbol=symbol, time_ms=hour * HOUR, index=hour if index is None else index)


def _sim(
    *,
    side: PositionSide,
    source: StopPriceSource,
    reason: ExitReason = ExitReason.TAKE_PROFIT,
) -> SimulatedTrade:
    return SimulatedTrade(
        side=side,
        entry_index=0,
        exit_index=1,
        entry_open_time_ms=0,
        exit_open_time_ms=HOUR,
        exit_reason=reason,
        stop_price_source=source,
        reference_entry=Decimal("100"),
        reference_stop=Decimal("90"),
        reference_take_profit=Decimal("110"),
        reference_exit=Decimal("110"),
        entry_fill=Decimal("100"),
        exit_fill=Decimal("110"),
        funding=Decimal("0"),
        loss_per_unit=Decimal("10"),
        net_per_unit=Decimal("1"),
        atr=Decimal("10"),
    )


def test_other_symbol_that_crosses_the_test_is_not_in_train() -> None:
    split = _split()
    fold = split.folds[0]
    # Тест фолду — [48h, 72h), embargo — 4 години до нього.
    eth_inside = _row(ETH, 50)
    btc_inside = _row(BTC, 60, index=3)
    eth_overlap = _row(ETH, 45)
    eth_before = _row(ETH, 43)
    train = rows_for([eth_inside, btc_inside, eth_overlap, eth_before], split, SplitPurpose.TRAIN, fold_index=0)
    assert train == (eth_before,)
    assert label_overlaps_test(eth_inside, split, 0)
    assert label_overlaps_test(eth_overlap, split, 0)
    assert not label_overlaps_test(eth_before, split, 0)
    assert fold.test_start_ms == 48 * HOUR
    assert fold.embargo_start_ms == fold.test_start_ms - 4 * HOUR
    assert eth_inside in rows_for([eth_inside], split, SplitPurpose.TEST, fold_index=0)


def test_later_fold_trains_on_the_earlier_test_except_the_embargo() -> None:
    split = _split()
    early = _row(ETH, 50)
    tail = _row(BTC, 70)
    assert early in rows_for([early], split, SplitPurpose.TEST, fold_index=0)
    assert early in rows_for([early], split, SplitPurpose.TRAIN, fold_index=1)
    assert tail in rows_for([tail], split, SplitPurpose.TEST, fold_index=0)
    assert rows_for([tail], split, SplitPurpose.TRAIN, fold_index=1) == ()
    assert split.folds[1].embargo_start_ms == 68 * HOUR
    assert split.folds[1].train_start_ms == split.data_from_ms


def test_holdout_indexes_never_enter_train_optuna_or_calibration() -> None:
    split = _split()
    holdout = _row(ETH, 400, index=7)
    test_row = _row(BTC, 50, index=1)
    train_row = _row(ETH, 10, index=2)
    universe = [holdout, test_row, train_row]
    assert is_holdout_row(holdout, split)
    for purpose in (
        SplitPurpose.OPTUNA,
        SplitPurpose.CALIBRATION,
        SplitPurpose.THRESHOLDS,
        SplitPurpose.PRIMARY_STRATEGY,
    ):
        chosen = rows_for(universe, split, purpose)
        assert holdout not in chosen
        assert train_row not in chosen
        assert chosen == (test_row,)
        with pytest.raises(SplitLeakError, match="hold-out"):
            reject_holdout(universe, split, purpose=purpose)
    fitting = rows_for(universe, split, SplitPurpose.TRAIN, fold_index=0)
    assert holdout not in fitting
    assert test_row not in fitting
    assert fitting == (train_row,)
    with pytest.raises(SplitLeakError, match="hold-out"):
        reject_holdout([holdout], split, purpose=SplitPurpose.TRAIN)


def test_path_reserve_and_short_tail_are_not_scored() -> None:
    split = _split()
    # Останній тест закінчується раніше за hold-out на горизонт і на хвіст, коротший за відрізок.
    reserve = _row(BTC, split.decision_end_ms // HOUR)
    between = _row(ETH, split.unused_from_ms // HOUR)
    assert reserve.time_ms == split.decision_end_ms
    assert split.decision_end_ms == split.holdout_from_ms - split.horizon_bars * HOUR
    assert between not in rows_for([between], split, SplitPurpose.OPTUNA)
    assert reserve not in rows_for([reserve], split, SplitPurpose.CALIBRATION)
    assert rows_for([between, reserve], split, SplitPurpose.TRAIN, fold_index=0) == ()
    with pytest.raises(SplitLeakError, match="hold-out"):
        reject_holdout([reserve], split, purpose=SplitPurpose.OPTUNA)
    for fold in split.folds:
        last_entry = fold.test_end_ms - HOUR
        assert last_entry + split.horizon_bars * HOUR < split.holdout_from_ms


def test_wider_embargo_excludes_bars_before_the_label_would_reach_the_test() -> None:
    split = _split(embargo_bars=6)
    # Горизонт 4, embargo 6: бар за 5 годин до тесту в тест не дивиться, але в train не входить.
    almost = _row(ETH, 43)
    clear = _row(BTC, 41)
    assert split.folds[0].test_start_ms == 48 * HOUR
    assert not label_overlaps_test(almost, split, 0)
    assert rows_for([almost, clear], split, SplitPurpose.TRAIN, fold_index=0) == (clear,)


def test_fold_extends_until_the_trading_symbol_has_enough_trades() -> None:
    calls: list[tuple[int, int]] = []

    def count(start: int, end: int) -> int:
        calls.append((start, end))
        return 19 if end - start < 48 * HOUR else 20

    short = _split()
    extended = _split(count_trades=count)
    assert all(not fold.extended for fold in short.folds)
    assert len(extended.folds) < len(short.folds)
    assert extended.folds[0].extended
    assert extended.folds[0].test_end_ms - extended.folds[0].test_start_ms == 48 * HOUR
    assert not extended.folds[0].below_min_trades
    assert all(end <= extended.decision_end_ms < extended.holdout_from_ms for _start, end in calls)


def test_exact_minimum_does_not_extend_and_a_short_extension_stays_below() -> None:
    exact = _split(count_trades=_count(20))
    assert all(not fold.extended and not fold.below_min_trades for fold in exact.folds)
    calls: list[tuple[int, int]] = []

    def few(start: int, end: int) -> int:
        calls.append((start, end))
        return 5

    capped = _split(
        data_to_ms=200 * HOUR,
        count_trades=few,
        test_segment_extended_ms=1000 * HOUR,
    )
    assert len(capped.folds) == 1
    fold = capped.folds[0]
    assert fold.extended
    assert fold.below_min_trades
    assert fold.n_trading_trades == 5
    assert fold.test_end_ms == capped.decision_end_ms
    assert fold.test_end_ms < capped.holdout_from_ms
    assert all(end <= capped.decision_end_ms for _start, end in calls)


def test_defaults_are_one_year_then_ninety_days_and_twenty_percent() -> None:
    calls: list[int] = []

    def count(start: int, end: int) -> int:
        calls.append(end - start)
        return MIN_TRADES_PER_FOLD if end - start >= TEST_SEGMENT_EXTENDED_MS else MIN_TRADES_PER_FOLD - 1

    split = build_split(data_from_ms=0, data_to_ms=800 * DAY_MS, horizon_bars=24, count_trades=count)
    assert split.holdout_fraction == DEFAULT_HOLDOUT_FRACTION
    assert split.holdout_from_ms == 640 * DAY_MS
    assert split.min_train_ms == MIN_TRAIN_MS
    assert split.folds[0].test_start_ms == MIN_TRAIN_MS
    assert split.folds[0].extended
    assert split.folds[0].test_end_ms - split.folds[0].test_start_ms == TEST_SEGMENT_EXTENDED_MS
    assert split.embargo_bars == 24
    assert split.min_trades_per_fold == MIN_TRADES_PER_FOLD
    plain = build_split(
        data_from_ms=0,
        data_to_ms=800 * DAY_MS,
        horizon_bars=24,
        count_trades=_count(),
    )
    assert all(not fold.extended for fold in plain.folds)
    assert plain.folds[0].test_end_ms - plain.folds[0].test_start_ms == TEST_SEGMENT_MS
    assert plain == build_split(data_from_ms=0, data_to_ms=800 * DAY_MS, horizon_bars=24, count_trades=_count())


def test_holdout_fraction_is_at_least_the_request_and_rejects_the_rest() -> None:
    start, end = holdout_bounds(0, 10 * HOUR, fraction=Decimal("0.15"))
    assert start == 8 * HOUR
    assert end == 10 * HOUR
    assert end - start >= int(Decimal(10 * HOUR) * Decimal("0.15"))
    with pytest.raises(ValueError, match="15–20%"):
        holdout_bounds(0, 500 * HOUR, fraction=Decimal("0.14"))
    with pytest.raises(ValueError, match="15–20%"):
        holdout_bounds(0, 500 * HOUR, fraction=Decimal("0.21"))
    with pytest.raises(TypeError):
        holdout_bounds(0, 500 * HOUR, fraction=0.2)  # type: ignore[arg-type]


def test_embargo_shorter_than_the_horizon_is_rejected() -> None:
    with pytest.raises(ValueError, match="embargo"):
        _split(embargo_bars=3)
    with pytest.raises(ValueError, match="історія"):
        _split(data_to_ms=80 * HOUR)


def test_sample_weights_refuse_test_and_holdout_rows() -> None:
    split = _split()
    train = (_row(ETH, 10), _row(BTC, 20))
    assert guard_sample_weights(train, split, 0) == train
    with pytest.raises(SplitLeakError, match="train"):
        guard_sample_weights((*train, _row(ETH, 50)), split, 0)
    with pytest.raises(SplitLeakError, match="train"):
        guard_sample_weights((_row(BTC, 400),), split, 0)


def test_label_diagnostics_record_last_stop_and_both_targets() -> None:
    stopped = _sim(side=PositionSide.LONG, source=StopPriceSource.LAST, reason=ExitReason.STOP_LOSS)
    meta = MetaSample(symbol=BTC, label=0, trade=stopped)
    direct = DirectSample(
        symbol=ETH,
        entry_index=0,
        label=DirectClass.FLAT,
        both_reached_tp=True,
        long_trade=_sim(side=PositionSide.LONG, source=StopPriceSource.LAST),
        short_trade=_sim(side=PositionSide.SHORT, source=StopPriceSource.MARK),
    )
    marked = _sim(side=PositionSide.LONG, source=StopPriceSource.MARK, reason=ExitReason.TIME_STOP)
    clean = MetaSample(symbol=BTC, label=0, trade=marked)
    stats = label_diagnostics([meta, direct, clean])
    assert stats == LabelStats(n_labels=3, n_last_stop=2, n_both_reached_tp=1)
    assert stats.last_stop_share == Decimal(2) / Decimal(3)
    assert label_diagnostics([]).last_stop_share is None


def test_train_coverage_does_not_see_a_gap_in_the_test_or_the_holdout() -> None:
    split = _split()
    lo, hi = train_coverage_ms(split, 0)
    assert (lo, hi) == (0, split.folds[0].embargo_start_ms)
    require_train_coverage(split, 0, lo, hi)

    def columns(gap: GapRange, end: int) -> tuple[str, ...]:
        points = tuple(LiquidationBar(stamp, 0.0, 0.0) for stamp in range(0, end, HOUR))
        data = SymbolData(
            candles=(CandleBar(0, 100.0, 100.0, 100.0, 100.0, 1.0),),
            liquidations=points,
            gaps={SOURCE_LIQUIDATIONS: (gap,)},
        )
        rows = compute_features(
            {BTC: data},
            enabled_sources=frozenset({SOURCE_LIQUIDATIONS}),
            coverage_start_ms=lo,
            coverage_end_ms=end,
        )
        return rows.columns

    test_gap = GapRange(50 * HOUR, 51 * HOUR)
    assert "liq_ratio" in columns(test_gap, hi)
    assert "liq_ratio" not in columns(test_gap, split.folds[0].test_end_ms)
    holdout_gap = GapRange(split.holdout_from_ms, split.holdout_from_ms + HOUR)
    assert "liq_ratio" in columns(holdout_gap, hi)
    assert "liq_ratio" not in columns(holdout_gap, split.holdout_from_ms + HOUR)
    with pytest.raises(SplitLeakError, match="покриття"):
        require_train_coverage(split, 0, lo, split.folds[0].test_end_ms)
    with pytest.raises(SplitLeakError, match="покриття"):
        require_train_coverage(split, 0, lo, split.holdout_to_ms)


def test_the_line_keeps_one_sealed_holdout_until_a_shift() -> None:
    assert _sealed_holdout((), ()) is None
    assert _sealed_holdout(((400, 500),), ()) == (400, 500)
    with pytest.raises(SplitLeakError, match="кілька"):
        _sealed_holdout(((400, 500), (425, 500)), ())
    assert _sealed_holdout(((400, 500),), ((500, 600, 1),)) == (500, 600)
    # Зсув — новіший експеримент: старе вікно відпускається без жодного відкриття.
    assert _sealed_holdout(((400, 500), (500, 625)), ()) == (500, 625)
    assert _sealed_holdout(((400, 500), (500, 625)), ((400, 500, 1), (400, 500, 2))) == (500, 625)


def test_opening_counter_stops_at_two_and_a_shift_must_be_newer() -> None:
    assert _opening_no((), 0, 10) == 1
    assert _opening_no(((0, 10, 1),), 0, 10) == 2
    with pytest.raises(HoldoutLimitError):
        _opening_no(((0, 10, 1), (0, 10, 2)), 0, 10)
    with pytest.raises(HoldoutNotNewerError):
        _opening_no(((0, 10, 1),), 5, 12)
    assert _opening_no(((0, 10, 2),), 10, 20) == 1
    with pytest.raises(HoldoutNotNewerError):
        _opening_no(((0, 10, 1), (10, 20, 1)), 0, 10)


def test_unaligned_row_and_unknown_purpose_are_rejected() -> None:
    split = _split()
    with pytest.raises(ValueError, match="вирівнян"):
        rows_for([SampleRow(symbol=BTC, time_ms=1, index=0)], split, SplitPurpose.TRAIN, fold_index=0)
    with pytest.raises(ValueError, match="призначення"):
        rows_for([], split, "live")
    with pytest.raises(ValueError, match="fold_index"):
        rows_for([], split, SplitPurpose.OPTUNA, fold_index=0)


@given(hour=st.integers(min_value=0, max_value=499), symbol=st.sampled_from((BTC, ETH)))
@settings(max_examples=50)
def test_no_symbol_crosses_from_the_test_or_the_holdout_into_train(hour: int, symbol: str) -> None:
    split = _split()
    row = SampleRow(symbol=symbol, time_ms=hour * HOUR, index=hour)
    if is_holdout_row(row, split):
        assert rows_for([row], split, SplitPurpose.OPTUNA) == ()
        assert rows_for([row], split, SplitPurpose.CALIBRATION) == ()
        with pytest.raises(SplitLeakError):
            reject_holdout([row], split, purpose=SplitPurpose.THRESHOLDS)
    for fold in split.folds:
        fitting = rows_for([row], split, SplitPurpose.TRAIN, fold_index=fold.index)
        overlaps = fold.test_start_ms <= row.time_ms < fold.test_end_ms or label_overlaps_test(row, split, fold.index)
        if overlaps or is_holdout_row(row, split):
            assert fitting == ()
