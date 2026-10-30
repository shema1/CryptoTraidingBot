"""Фічі T2.1: look-ahead (1h, 4h/1d, деривативи), parity буфера й офлайну, прогалини."""

from __future__ import annotations

import math
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncConnection, async_sessionmaker

from app.core.config import H1_MS
from app.data.gaps import GapRange, window_has_gap
from app.data.models import GAP_SOURCES, DataGap
from app.ml.features import (
    ATR_PERIOD,
    D1_BARS,
    D1_MS,
    DAILY_EMA_SPAN,
    DAILY_SLOPE_LAG,
    DERIVATIVE_LAG_MS,
    DERIVATIVE_SOURCES,
    FEATURE_GAP_SOURCES,
    FEATURE_LOOKBACK_BARS,
    FUNDING_MAX_STALE_MS,
    H4_BARS,
    H4_MS,
    LIQ_BARS,
    OI_CHANGE_BARS,
    PREPROCESSING_VERSION,
    SOURCE_FUNDING,
    SOURCE_LIQUIDATIONS,
    SOURCE_LONG_SHORT,
    SOURCE_OPEN_INTEREST,
    SOURCE_PREMIUM,
    CandleBar,
    FeatureBuffer,
    FeatureRows,
    LiquidationBar,
    LongShortPoint,
    SymbolData,
    TimedValue,
    compute_features,
    derivative_window,
    feature_columns,
    feature_schema_hash,
    funding_gap_ranges,
    load_feature_gaps,
    sources_covering,
)

HOUR = H1_MS
SYMBOL = "BTC/USDT:USDT"
OTHER = "ETH/USDT:USDT"
_HTF = ("ret_4h", "ret_1d", "atr_pct_4h", "atr_pct_1d", "dist_ema50_1d", "slope_1d")
_HTF_4H = ("ret_4h", "atr_pct_4h")
_HTF_1D = ("ret_1d", "atr_pct_1d", "dist_ema50_1d", "slope_1d")
_HOURLY_DERIVATIVES = (SOURCE_OPEN_INTEREST, SOURCE_PREMIUM, SOURCE_LONG_SHORT, SOURCE_LIQUIDATIONS)


def _candles(n: int, *, start_ms: int = 0, seed: int = 1) -> tuple[CandleBar, ...]:
    rng = np.random.default_rng(seed)
    price = 100.0
    bars: list[CandleBar] = []
    for index in range(n):
        open_ = price
        close = float(price * math.exp(float(rng.normal(0.0, 0.01))))
        high = max(open_, close) * (1.0 + float(rng.uniform(0.0, 0.004)))
        low = min(open_, close) * (1.0 - float(rng.uniform(0.0, 0.004)))
        volume = float(rng.uniform(1.0, 40.0))
        bars.append(CandleBar(start_ms + index * HOUR, open_, high, low, close, volume))
        price = close
    return tuple(bars)


def _symbol(n: int, *, start_ms: int = 0, seed: int = 1) -> SymbolData:
    candles = _candles(n, start_ms=start_ms, seed=seed)
    return SymbolData(
        candles=candles,
        open_interest=tuple(TimedValue(bar.open_time_ms, 1_000.0 + index) for index, bar in enumerate(candles)),
        premium=tuple(TimedValue(bar.open_time_ms, 0.0001 * ((index % 5) - 2)) for index, bar in enumerate(candles)),
        long_short=tuple(
            LongShortPoint(bar.open_time_ms, 0.45 + (index % 6) * 0.02, 0.55 - (index % 6) * 0.02)
            for index, bar in enumerate(candles)
        ),
        liquidations=tuple(
            LiquidationBar(bar.open_time_ms, float(index % 5), float((index + 2) % 4))
            for index, bar in enumerate(candles)
        ),
        funding=tuple(
            TimedValue(bar.open_time_ms, 0.0001 * ((index // 8) % 5 - 2))
            for index, bar in enumerate(candles)
            if index % 8 == 0
        ),
    )


def _flat(n: int) -> SymbolData:
    candles = tuple(CandleBar(index * HOUR, 100.0, 100.0, 100.0, 100.0, 5.0) for index in range(n))
    return SymbolData(
        candles=candles,
        open_interest=tuple(TimedValue(bar.open_time_ms, 2_000.0) for bar in candles),
        premium=tuple(TimedValue(bar.open_time_ms, 0.0002) for bar in candles),
        long_short=tuple(LongShortPoint(bar.open_time_ms, 0.5, 0.5) for bar in candles),
        liquidations=tuple(LiquidationBar(bar.open_time_ms, 0.0, 0.0) for bar in candles),
        funding=tuple(TimedValue(bar.open_time_ms, 0.0001) for index, bar in enumerate(candles) if index % 8 == 0),
    )


def _shock(bar: CandleBar) -> CandleBar:
    close = bar.close * 1.5
    high = max(bar.high, bar.open, close) * 1.01
    low = min(bar.low, bar.open, close)
    return CandleBar(bar.open_time_ms, bar.open, high, low, close, bar.volume)


def _with_candle(data: SymbolData, index: int, bar: CandleBar) -> SymbolData:
    candles = list(data.candles)
    candles[index] = bar
    return replace(data, candles=tuple(candles))


def _with_ratio(points: tuple[LongShortPoint, ...], ts_ms: int, buy: float) -> tuple[LongShortPoint, ...]:
    found = False
    updated: list[LongShortPoint] = []
    for point in points:
        if point.ts_ms == ts_ms:
            updated.append(LongShortPoint(ts_ms, buy, point.sell_ratio))
            found = True
        else:
            updated.append(point)
    if not found:
        updated.append(LongShortPoint(ts_ms, buy, 1.0 - buy))
    return tuple(updated)


def _with_liquidation(points: tuple[LiquidationBar, ...], ts_ms: int, notional: float) -> tuple[LiquidationBar, ...]:
    found = False
    updated: list[LiquidationBar] = []
    for point in points:
        if point.ts_ms == ts_ms:
            updated.append(LiquidationBar(ts_ms, notional, 0.0))
            found = True
        else:
            updated.append(point)
    if not found:
        updated.append(LiquidationBar(ts_ms, notional, 0.0))
    return tuple(updated)


def _with_value(points: tuple[TimedValue, ...], ts_ms: int, value: float) -> tuple[TimedValue, ...]:
    found = False
    updated: list[TimedValue] = []
    for point in points:
        if point.ts_ms == ts_ms:
            updated.append(TimedValue(ts_ms, value))
            found = True
        else:
            updated.append(point)
    if not found:
        updated.append(TimedValue(ts_ms, value))
    return tuple(updated)


def _without(data: SymbolData, source: str, ts_ms: int) -> SymbolData:
    """Ряд джерела без точки години `ts_ms` — як пропущена біржею чи втрачена колектором година."""
    if source == SOURCE_OPEN_INTEREST:
        return replace(data, open_interest=tuple(p for p in data.open_interest if p.ts_ms != ts_ms))
    if source == SOURCE_PREMIUM:
        return replace(data, premium=tuple(p for p in data.premium if p.ts_ms != ts_ms))
    if source == SOURCE_LONG_SHORT:
        return replace(data, long_short=tuple(p for p in data.long_short if p.ts_ms != ts_ms))
    if source == SOURCE_LIQUIDATIONS:
        return replace(data, liquidations=tuple(p for p in data.liquidations if p.ts_ms != ts_ms))
    raise AssertionError(source)


def _push_hour(buffer: FeatureBuffer, data: SymbolData, ts_ms: int) -> None:
    """Остаточні точки деривативів години `ts_ms`, які колектор записав після її закриття."""
    for interest in data.open_interest:
        if interest.ts_ms == ts_ms:
            buffer.upsert_open_interest(interest)
    for premium in data.premium:
        if premium.ts_ms == ts_ms:
            buffer.upsert_premium(premium)
    for ratio in data.long_short:
        if ratio.ts_ms == ts_ms:
            buffer.upsert_long_short(ratio)
    for liquidation in data.liquidations:
        if liquidation.ts_ms == ts_ms:
            buffer.upsert_liquidation(liquidation)


def _load(buffer: FeatureBuffer, data: SymbolData) -> None:
    for candle in data.candles:
        buffer.upsert_candle(candle)
    for interest in data.open_interest:
        buffer.upsert_open_interest(interest)
    for premium in data.premium:
        buffer.upsert_premium(premium)
    for ratio in data.long_short:
        buffer.upsert_long_short(ratio)
    for liquidation in data.liquidations:
        buffer.upsert_liquidation(liquidation)
    for funding in data.funding:
        buffer.upsert_funding(funding)
    for source, gaps in data.gaps.items():
        buffer.set_gaps(source, gaps)


def _rows(
    data: SymbolData,
    *,
    enabled_sources: frozenset[str] | None = None,
    coverage_start_ms: int | None = None,
    coverage_end_ms: int | None = None,
) -> FeatureRows:
    return compute_features(
        {SYMBOL: data},
        enabled_sources=enabled_sources,
        coverage_start_ms=coverage_start_ms,
        coverage_end_ms=coverage_end_ms,
    )


def test_schema_is_bounded_and_stable() -> None:
    assert FEATURE_LOOKBACK_BARS >= 1300
    columns = feature_columns(DERIVATIVE_SOURCES)
    assert len(columns) == 30
    assert "symbol" not in columns
    assert feature_schema_hash(columns) == feature_schema_hash(columns)
    assert feature_schema_hash(columns).startswith("sha256:")
    shorter = feature_columns(frozenset(DERIVATIVE_SOURCES) - {SOURCE_FUNDING})
    assert feature_schema_hash(shorter) != feature_schema_hash(columns)
    rows = _rows(_symbol(FEATURE_LOOKBACK_BARS))
    assert rows.columns == columns
    assert rows.schema_hash == feature_schema_hash(columns)
    assert rows.preprocessing_version == PREPROCESSING_VERSION == "1.0.0"
    readme = (Path(__file__).resolve().parents[2] / "app" / "ml" / "README.md").read_text(encoding="utf-8")
    assert PREPROCESSING_VERSION in readme
    assert str(FEATURE_LOOKBACK_BARS) in readme
    assert set(FEATURE_GAP_SOURCES) <= set(GAP_SOURCES)
    assert SOURCE_FUNDING not in FEATURE_GAP_SOURCES


def test_flat_market_is_finite_and_neutral() -> None:
    rows = _rows(_flat(FEATURE_LOOKBACK_BARS))
    assert rows.valid.tolist() == [False] * (FEATURE_LOOKBACK_BARS - 1) + [True]
    last = FEATURE_LOOKBACK_BARS - 1
    assert rows.column("rsi_14")[last] == pytest.approx(50.0)
    assert rows.column("log_ret_1")[last] == pytest.approx(0.0)
    assert rows.column("atr_pct")[last] == pytest.approx(0.0)
    assert rows.column("ema_spread_atr")[last] == pytest.approx(0.0)
    assert rows.column("vol_ratio_24")[last] == pytest.approx(1.0)
    assert rows.column("liq_ratio")[last] == pytest.approx(1.0)
    assert rows.column("oi_chg_24")[last] == pytest.approx(0.0)
    assert rows.column("ret_4h")[last] == pytest.approx(0.0)
    assert rows.column("ret_1d")[last] == pytest.approx(0.0)
    assert int(rows.features_open_time_ms[last]) == int(rows.entry_open_time_ms[last]) - HOUR


def test_log_return_and_clock_match_the_window() -> None:
    data = _symbol(FEATURE_LOOKBACK_BARS, seed=3)
    rows = _rows(data)
    index = FEATURE_LOOKBACK_BARS - 1
    expected = math.log(data.candles[index].close / data.candles[index - 1].close)
    assert rows.column("log_ret_1")[index] == pytest.approx(expected)
    entry = 1300 * HOUR
    assert int(rows.entry_open_time_ms[index]) == entry
    assert rows.column("hour_sin")[index] == pytest.approx(math.sin(2.0 * math.pi * 4.0 / 24.0))
    assert rows.column("hour_cos")[index] == pytest.approx(math.cos(2.0 * math.pi * 4.0 / 24.0))
    # 1300h after the epoch is Tuesday (Monday = 0).
    assert rows.column("dow_sin")[index] == pytest.approx(math.sin(2.0 * math.pi * 1.0 / 7.0))


def test_every_hour_alignment_warms_daily_ema() -> None:
    for offset in range(24):
        rows = _rows(_symbol(FEATURE_LOOKBACK_BARS, start_ms=offset * HOUR, seed=offset + 1))
        assert bool(rows.valid[-1]), offset
        assert math.isfinite(float(rows.column("dist_ema50_1d")[-1]))


def test_lookahead_hourly_ignores_the_entry_candle_and_after() -> None:
    data = _symbol(FEATURE_LOOKBACK_BARS + 3, seed=4)
    rows = _rows(data)
    index = FEATURE_LOOKBACK_BARS  # entry candle exists and is in the future relative to this row
    mutated = data
    for cursor in range(index + 1, len(data.candles)):
        mutated = _with_candle(mutated, cursor, _shock(data.candles[cursor]))
    after = _rows(mutated)
    assert bool(rows.valid[index])
    assert np.array_equal(rows.values[index], after.values[index])
    touched = _rows(_with_candle(data, index, _shock(data.candles[index])))
    assert not np.array_equal(rows.values[index], touched.values[index])


def test_lookahead_higher_timeframes_ignore_the_forming_bar() -> None:
    # Index 1301 is 05:00 UTC. The 4h bar 04:00–08:00 and the current UTC day are still open at 06:00.
    data = _symbol(FEATURE_LOOKBACK_BARS + 2, seed=5)
    index = 1301
    assert data.candles[index].open_time_ms % (24 * HOUR) == 5 * HOUR
    base = _rows(data)
    forming = data
    for cursor in (1300, 1301):
        forming = _with_candle(forming, cursor, _shock(data.candles[cursor]))
    changed = _rows(forming)
    for name in _HTF:
        assert base.column(name)[index] == changed.column(name)[index]
    assert base.column("log_ret_1")[index] != changed.column("log_ret_1")[index]

    closed_4h = _rows(_with_candle(data, 1299, _shock(data.candles[1299])))
    assert base.column("ret_4h")[index] != closed_4h.column("ret_4h")[index]
    assert base.column("ret_1d")[index] == closed_4h.column("ret_1d")[index]

    closed_day = _rows(_with_candle(data, 1295, _shock(data.candles[1295])))
    assert base.column("ret_1d")[index] != closed_day.column("ret_1d")[index]


def test_lookahead_higher_bar_enters_exactly_when_it_closes() -> None:
    # Індекс i — свічка `t-1`, що відкрилась о (i mod 24):00; рішення — на годину пізніше.
    data = _symbol(FEATURE_LOOKBACK_BARS + 21, seed=21)
    base = _rows(data)

    def shocked(index: int) -> FeatureRows:
        return _rows(_with_candle(data, index, _shock(data.candles[index])))

    closes_4h, forming_4h, closes_day, forming_day = 1303, 1302, 1319, 1318
    hours = [(int(data.candles[i].open_time_ms) // HOUR) % 24 for i in (closes_4h, forming_4h, closes_day, forming_day)]
    assert hours == [7, 6, 23, 22]
    # Рішення о 08:00: 4h 04–08 щойно закрилась і входить разом зі своєю останньою годиною.
    assert base.column("ret_4h")[closes_4h] != shocked(closes_4h).column("ret_4h")[closes_4h]
    # Рішення о 07:00: 4h 04–08 ще формується; остання закрита — 00–04.
    moved = shocked(forming_4h)
    for name in _HTF:
        assert base.column(name)[forming_4h] == moved.column(name)[forming_4h], name
    # Рішення о 00:00: доба щойно закрилась.
    assert base.column("ret_1d")[closes_day] != shocked(closes_day).column("ret_1d")[closes_day]
    # Рішення о 23:00: доба ще формується.
    moved = shocked(forming_day)
    for name in _HTF_1D:
        assert base.column(name)[forming_day] == moved.column(name)[forming_day], name


def test_higher_timeframes_move_only_when_their_bar_closes() -> None:
    # Кількість старших свічок стала: година доби сама по собі нічого не зсуває.
    rows = _rows(_symbol(FEATURE_LOOKBACK_BARS + 60, seed=20), enabled_sources=frozenset())
    for index in range(FEATURE_LOOKBACK_BARS, rows.values.shape[0]):
        assert bool(rows.valid[index])
        assert bool(rows.valid[index - 1])
        hour = (int(rows.entry_open_time_ms[index]) // HOUR) % 24
        for name in _HTF_1D:
            moved = rows.column(name)[index] != rows.column(name)[index - 1]
            assert moved == (hour == 0), (name, hour)
        for name in _HTF_4H:
            moved = rows.column(name)[index] != rows.column(name)[index - 1]
            assert moved == (hour % 4 == 0), (name, hour)


def test_higher_bar_counts_always_fit_the_window() -> None:
    for bar_ms, count in ((H4_MS, H4_BARS), (D1_MS, D1_BARS)):
        per_bar = bar_ms // HOUR
        # Найгірша година: остання закрита старша свічка закінчилась на `крок - 1h` раніше за рішення.
        assert (count + 1) * per_bar - 1 <= FEATURE_LOOKBACK_BARS
        assert (count + 2) * per_bar - 1 > FEATURE_LOOKBACK_BARS
    assert max(DAILY_EMA_SPAN, ATR_PERIOD + 1, DAILY_SLOPE_LAG) <= D1_BARS
    assert H4_BARS >= ATR_PERIOD + 1


def test_lookahead_derivatives_skip_the_hour_closing_at_decision() -> None:
    # Година [T-1h, T) закривається рівно в T, але live отримує її пізніше за рішення: у фічу вона не входить.
    data = _symbol(FEATURE_LOOKBACK_BARS + 2, seed=6)
    index = 1301
    entry = int(data.candles[index].open_time_ms) + HOUR
    assert DERIVATIVE_LAG_MS == HOUR
    base = _rows(data)
    for stamp in (entry, entry - HOUR):
        future_oi = replace(data, open_interest=_with_value(data.open_interest, stamp, 9_999.0))
        future_premium = replace(data, premium=_with_value(data.premium, stamp, 0.05))
        future_ratio = replace(data, long_short=_with_ratio(data.long_short, stamp, 0.9))
        future_liq = replace(data, liquidations=_with_liquidation(data.liquidations, stamp, 1_000_000.0))
        for mutated in (future_oi, future_premium, future_ratio, future_liq):
            assert np.array_equal(base.values[index], _rows(mutated).values[index]), stamp
    future_funding = replace(data, funding=_with_value(data.funding, entry, 0.02))
    assert np.array_equal(base.values[index], _rows(future_funding).values[index])

    known = entry - 2 * HOUR
    known_oi = replace(data, open_interest=_with_value(data.open_interest, known, 50_000.0))
    known_oi_base = replace(
        data, open_interest=_with_value(data.open_interest, known - OI_CHANGE_BARS * HOUR, 50_000.0)
    )
    known_premium = replace(data, premium=_with_value(data.premium, known, -0.01))
    known_funding = replace(data, funding=_with_value(data.funding, entry - 1, 0.03))
    known_ratio = replace(data, long_short=_with_ratio(data.long_short, known, 0.1))
    known_liq = replace(data, liquidations=_with_liquidation(data.liquidations, known, 8_000.0))
    assert base.column("oi_chg_24")[index] != _rows(known_oi).column("oi_chg_24")[index]
    assert base.column("oi_chg_24")[index] != _rows(known_oi_base).column("oi_chg_24")[index]
    assert base.column("premium_close")[index] != _rows(known_premium).column("premium_close")[index]
    assert base.column("funding_rate")[index] != _rows(known_funding).column("funding_rate")[index]
    assert base.column("ls_buy_ratio")[index] != _rows(known_ratio).column("ls_buy_ratio")[index]
    assert base.column("liq_ratio")[index] != _rows(known_liq).column("liq_ratio")[index]
    assert base.column("log_ret_1")[index] == _rows(known_oi).column("log_ret_1")[index]


def test_parity_buffer_matches_offline_on_the_last_rows() -> None:
    data = _symbol(FEATURE_LOOKBACK_BARS + 5, seed=7)
    offline = _rows(data)
    for index in range(len(data.candles) - 5, len(data.candles)):
        buffer = FeatureBuffer(SYMBOL)
        _load(buffer, replace(data, candles=data.candles[: index + 1]))
        # Points of later hours stay in the series the buffer trims. They must not move the row.
        if index + 1 < len(data.candles):
            buffer.upsert_open_interest(TimedValue(data.candles[index].open_time_ms + HOUR, 123_456.0))
            buffer.upsert_funding(TimedValue(data.candles[index].open_time_ms + HOUR + 1, 0.09))
        live = buffer.latest()
        assert int(live.entry_open_time_ms[0]) == int(offline.entry_open_time_ms[index])
        assert bool(live.valid[0]) is bool(offline.valid[index])
        np.testing.assert_allclose(live.values[0], offline.values[index], atol=1e-9, rtol=0.0)


def test_parity_rolling_buffer_matches_offline_on_every_row() -> None:
    # Як у live: свічку кладуть після її закриття; дериватив тієї ж години спершу приходить незакритим, остаточний —
    # після рішення. Дірки (втрачена година OI і явна прогалина ліквідацій) мають давати ту саму валідність.
    data = _symbol(FEATURE_LOOKBACK_BARS + 45, seed=22)
    lost_oi = (FEATURE_LOOKBACK_BARS + 3) * HOUR
    lost_liq = (FEATURE_LOOKBACK_BARS + 12) * HOUR
    data = _without(_without(data, SOURCE_OPEN_INTEREST, lost_oi), SOURCE_LIQUIDATIONS, lost_liq)
    data = replace(data, gaps={SOURCE_LIQUIDATIONS: (GapRange(lost_liq, lost_liq + HOUR),)})
    offline = _rows(data)
    finals_oi = {point.ts_ms for point in data.open_interest}
    finals_liq = {point.ts_ms for point in data.liquidations}

    buffer = FeatureBuffer(SYMBOL)
    buffer.set_gaps(SOURCE_LIQUIDATIONS, data.gaps[SOURCE_LIQUIDATIONS])
    seen_valid = seen_invalid = False
    for index, candle in enumerate(data.candles):
        opened = candle.open_time_ms
        entry = opened + HOUR
        # Незакрите значення години, що щойно закрилась: колектор ще не записав остаточне.
        if opened in finals_oi:
            buffer.upsert_open_interest(TimedValue(opened, 77_777.0))
        if opened in finals_liq:
            buffer.upsert_liquidation(LiquidationBar(opened, 5_000_000.0, 0.0))
        buffer.upsert_candle(candle)
        # Funding пише прохід свічок; ставка рівно в момент рішення вже може бути в буфері.
        for point in data.funding:
            if opened < point.ts_ms <= entry or (index == 0 and point.ts_ms <= entry):
                buffer.upsert_funding(point)
        if index >= FEATURE_LOOKBACK_BARS - 1:
            live = buffer.latest()
            assert int(live.entry_open_time_ms[0]) == int(offline.entry_open_time_ms[index])
            assert bool(live.valid[0]) is bool(offline.valid[index]), index
            np.testing.assert_allclose(live.values[0], offline.values[index], atol=1e-9, rtol=0.0, equal_nan=True)
            seen_valid = seen_valid or bool(live.valid[0])
            seen_invalid = seen_invalid or not bool(live.valid[0])
        _push_hour(buffer, data, opened)
    assert seen_valid
    assert seen_invalid


def test_buffer_keeps_derivatives_that_arrive_before_their_candles() -> None:
    # Колектор і прохід свічок — різні процеси: деривативи кількох годин можуть прийти раніше за свічки.
    data = _symbol(FEATURE_LOOKBACK_BARS + 6, seed=23)
    offline = _rows(data)
    head = FEATURE_LOOKBACK_BARS
    buffer = FeatureBuffer(SYMBOL)
    _load(buffer, replace(data, candles=data.candles[:head]))
    for candle in data.candles[head:]:
        buffer.upsert_candle(candle)
    live = buffer.latest()
    assert bool(offline.valid[-1])
    assert bool(live.valid[0])
    np.testing.assert_allclose(live.values[0], offline.values[-1], atol=1e-9, rtol=0.0)
    # Старше за вікно буфер не тримає.
    kept = buffer.snapshot()
    start = int(offline.entry_open_time_ms[-1]) - FEATURE_LOOKBACK_BARS * HOUR
    assert len(kept.candles) == FEATURE_LOOKBACK_BARS
    assert min(point.ts_ms for point in kept.open_interest) == start
    assert min(point.ts_ms for point in kept.funding) >= start - FUNDING_MAX_STALE_MS


def test_buffer_replay_is_idempotent() -> None:
    data = _symbol(FEATURE_LOOKBACK_BARS, seed=8)
    buffer = FeatureBuffer(SYMBOL)
    _load(buffer, data)
    first = buffer.latest()
    buffer.upsert_candle(data.candles[-1])
    second = buffer.latest()
    assert np.array_equal(first.values, second.values)
    assert bool(first.valid[0])


def test_gap_touching_the_boundary_does_not_drop_the_row() -> None:
    data = _symbol(FEATURE_LOOKBACK_BARS + 2, seed=9)
    index = FEATURE_LOOKBACK_BARS
    entry = int(data.candles[index].open_time_ms) + HOUR
    start = entry - FEATURE_LOOKBACK_BARS * HOUR
    inside = GapRange(start, start + HOUR)
    edge = GapRange(0, start)
    after = GapRange(entry, entry + HOUR)
    assert window_has_gap((inside,), start_ms=start, end_ms=entry)
    assert window_has_gap((edge,), start_ms=start, end_ms=entry) is False
    assert window_has_gap((after,), start_ms=start, end_ms=entry) is False
    blocked = _rows(replace(data, gaps={"candles": (inside,)}))
    opened = _rows(replace(data, gaps={"candles": (edge, after)}))
    assert bool(blocked.valid[index]) is False
    assert np.isnan(blocked.values[index]).all()
    assert bool(opened.valid[index]) is True


def test_missing_hour_is_not_interpolated_and_a_later_window_recovers() -> None:
    data = _symbol(FEATURE_LOOKBACK_BARS + 80, seed=10)
    kept = [bar for index, bar in enumerate(data.candles) if index != 50]
    hole = _rows(replace(data, candles=tuple(kept)))
    assert bool(hole.valid[FEATURE_LOOKBACK_BARS - 1]) is False
    recovered = FEATURE_LOOKBACK_BARS + 50
    assert bool(hole.valid[recovered]) is True


def test_data_older_than_the_lookback_does_not_change_the_last_row() -> None:
    data = _symbol(FEATURE_LOOKBACK_BARS + 20, seed=11)
    base = _rows(data)
    shocked = _rows(_with_candle(data, 0, _shock(data.candles[0])))
    assert np.array_equal(base.values[-1], shocked.values[-1])


def test_derivative_gap_blocks_only_the_hours_its_feature_reads() -> None:
    data = _symbol(FEATURE_LOOKBACK_BARS + 2, seed=18)
    entry = int(data.candles[-1].open_time_ms) + HOUR
    end = entry - DERIVATIVE_LAG_MS
    reads = {
        SOURCE_OPEN_INTEREST: (end - (OI_CHANGE_BARS + 1) * HOUR, end),
        SOURCE_PREMIUM: (end - HOUR, end),
        SOURCE_LONG_SHORT: (end - HOUR, end),
        SOURCE_LIQUIDATIONS: (end - LIQ_BARS * HOUR, end),
        SOURCE_FUNDING: (entry - FUNDING_MAX_STALE_MS, entry),
    }
    assert bool(_rows(data).valid[-1])
    for source, (lo, hi) in reads.items():
        assert derivative_window(source, entry) == (lo, hi)
        for inside in (GapRange(lo, lo + 1), GapRange(hi - 1, hi)):
            assert bool(_rows(replace(data, gaps={source: (inside,)})).valid[-1]) is False, (source, inside)
        # Дотик межею — поза вікном. Година, що закривається в момент рішення, теж (лаг).
        for outside in (GapRange(lo - HOUR, lo), GapRange(hi, entry + HOUR)):
            assert bool(_rows(replace(data, gaps={source: (outside,)})).valid[-1]) is True, (source, outside)
    for source in _HOURLY_DERIVATIVES:
        lo, hi = reads[source]
        # Без явного рядка `data_gaps` дірку дає сама відсутня точка.
        assert bool(_rows(_without(data, source, lo)).valid[-1]) is False, source
        assert bool(_rows(_without(data, source, hi - HOUR)).valid[-1]) is False, source
        assert bool(_rows(_without(data, source, lo - HOUR)).valid[-1]) is True, source
        assert bool(_rows(_without(data, source, hi)).valid[-1]) is True, source
    # Остання ставка старша за межу свіжості — рядок відкидається, а не бере застаріле значення.
    stale = replace(data, funding=tuple(p for p in data.funding if p.ts_ms < entry - FUNDING_MAX_STALE_MS))
    assert bool(_rows(stale).valid[-1]) is False


def test_one_lost_liquidation_hour_blocks_only_the_rows_that_read_it() -> None:
    # Втрачену годину ліквідацій не докачати: дірка має вибивати LIQ_BARS рядків, а не все вікно свічок.
    data = _symbol(FEATURE_LOOKBACK_BARS + 80, seed=19)
    lost = (FEATURE_LOOKBACK_BARS + 20) * HOUR
    gapped = replace(
        _without(data, SOURCE_LIQUIDATIONS, lost), gaps={SOURCE_LIQUIDATIONS: (GapRange(lost, lost + HOUR),)}
    )
    rows = _rows(gapped)
    warm = FEATURE_LOOKBACK_BARS - 1
    blocked = [int(rows.entry_open_time_ms[i]) for i in range(warm, rows.valid.size) if not bool(rows.valid[i])]
    assert blocked == [lost + DERIVATIVE_LAG_MS + step * HOUR for step in range(1, LIQ_BARS + 1)]


def test_derivative_gap_blocks_only_an_enabled_source() -> None:
    data = _symbol(FEATURE_LOOKBACK_BARS, seed=12)
    entry = FEATURE_LOOKBACK_BARS * HOUR
    gap = (GapRange(entry - 2 * HOUR, entry - HOUR),)
    blocked = _rows(replace(data, gaps={SOURCE_LIQUIDATIONS: gap}))
    assert bool(blocked.valid[-1]) is False
    price_only = _rows(
        replace(data, gaps={SOURCE_LIQUIDATIONS: gap}),
        enabled_sources=frozenset(DERIVATIVE_SOURCES) - {SOURCE_LIQUIDATIONS},
    )
    assert bool(price_only.valid[-1]) is True
    assert "liq_ratio" not in price_only.columns


def test_coverage_turns_a_source_off_for_every_symbol() -> None:
    left = _symbol(FEATURE_LOOKBACK_BARS, seed=13)
    right = _symbol(FEATURE_LOOKBACK_BARS, seed=14)
    end = FEATURE_LOOKBACK_BARS * HOUR
    gapped = replace(right, gaps={SOURCE_LIQUIDATIONS: (GapRange(0, HOUR),)})
    covered = sources_covering({SYMBOL: left, OTHER: gapped}, start_ms=0, end_ms=end)
    assert SOURCE_LIQUIDATIONS not in covered
    assert SOURCE_FUNDING in covered
    # Дірка одразу за навчальним вікном (тестовий відрізок) вибір джерел не змінює.
    after_end = replace(right, gaps={SOURCE_LIQUIDATIONS: (GapRange(end, end + HOUR),)})
    assert sources_covering({SYMBOL: left, OTHER: after_end}, start_ms=0, end_ms=end) == frozenset(DERIVATIVE_SOURCES)
    rows = compute_features(
        {SYMBOL: left, OTHER: gapped},
        coverage_start_ms=0,
        coverage_end_ms=end,
    )
    assert "liq_ratio" not in rows.columns
    assert rows.schema_hash != feature_schema_hash(feature_columns(DERIVATIVE_SOURCES))
    btc = [index for index, symbol in enumerate(rows.symbol) if symbol == SYMBOL]
    eth = [index for index, symbol in enumerate(rows.symbol) if symbol == OTHER]
    assert btc
    assert eth
    assert max(btc) < min(eth)
    alone = _rows(left)
    for name in rows.columns:
        assert rows.column(name)[btc[-1]] == pytest.approx(float(alone.column(name)[-1]))


def test_symbols_do_not_leak_into_each_other() -> None:
    btc = _symbol(FEATURE_LOOKBACK_BARS, seed=15)
    eth = _symbol(FEATURE_LOOKBACK_BARS, seed=16)
    shocked = _with_candle(btc, -1, _shock(btc.candles[-1]))
    mixed = compute_features({SYMBOL: shocked, OTHER: eth})
    eth_only = compute_features({OTHER: eth})
    mixed_eth = mixed.select(-1)
    assert mixed_eth.symbol == (OTHER,)
    assert np.array_equal(mixed_eth.values, eth_only.select(-1).values)


def test_funding_holes_follow_the_stale_limit() -> None:
    step = 8 * HOUR
    times = list(range(0, 10 * step, step))
    assert funding_gap_ranges(times, start_ms=0, end_ms=10 * step) == ()
    missing = [stamp for stamp in times if stamp != 4 * step]
    holes = funding_gap_ranges(missing, start_ms=0, end_ms=10 * step)
    assert holes
    assert window_has_gap(holes, start_ms=0, end_ms=10 * step)


def test_invalid_candle_is_rejected() -> None:
    with pytest.raises(ValueError, match="OHLC"):
        _rows(replace(_flat(2), candles=(CandleBar(0, 10.0, 9.0, 8.0, 10.0, 1.0),)))
    with pytest.raises(ValueError, match="дубль"):
        _rows(replace(_flat(1), candles=(_flat(1).candles[0], _flat(1).candles[0])))


def test_valid_rows_are_finite_and_invalid_rows_are_all_nan() -> None:
    rows = _rows(_symbol(FEATURE_LOOKBACK_BARS + 3, seed=17))
    for index in range(rows.values.shape[0]):
        finite = bool(np.isfinite(rows.values[index]).all())
        assert bool(rows.valid[index]) is finite
        if not finite:
            assert np.isnan(rows.values[index]).all()


def _gap_row(
    source: str,
    from_ms: int,
    to_ms: int,
    *,
    symbol: str = SYMBOL,
    timeframe: str = "1h",
    resolved: bool = False,
) -> dict[str, object]:
    detected = 2_000 * HOUR
    return {
        "source": source,
        "exchange": "bybit",
        "symbol": symbol,
        "timeframe": timeframe,
        "from_ms": from_ms,
        "to_ms": to_ms,
        "detected_at_ms": detected,
        "resolved_at_ms": detected + 1 if resolved else None,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_load_feature_gaps_reads_only_open_rows_of_the_window(conn: AsyncConnection) -> None:
    start = 1_000 * HOUR
    end = start + 48 * HOUR
    await conn.execute(
        insert(DataGap),
        [
            _gap_row("candles", start + HOUR, start + 2 * HOUR),
            _gap_row("candles", start + 5 * HOUR, start + 6 * HOUR, resolved=True),
            _gap_row("liquidations", start - 10 * HOUR, start + 3 * HOUR),
            _gap_row("open_interest", end, end + HOUR),
            _gap_row("open_interest", start - HOUR, start),
            _gap_row("premium_index", start + 7 * HOUR, start + 8 * HOUR, symbol=OTHER),
            _gap_row("long_short", start + 9 * HOUR, start + 10 * HOUR, timeframe="5min"),
            _gap_row("mark_candles", start + HOUR, start + 2 * HOUR),
        ],
    )
    sessions = async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")
    async with sessions() as session, session.begin():
        found = await load_feature_gaps(session, exchange="bybit", symbol=SYMBOL, start_ms=start, end_ms=end)
    # Закриті рядки, дотик межею, чужий символ, інший інтервал і ряди не з фіч не читаються.
    assert found == {
        "candles": (GapRange(start + HOUR, start + 2 * HOUR),),
        "open_interest": (),
        "premium_index": (),
        "long_short": (),
        "liquidations": (GapRange(start - 10 * HOUR, start + 3 * HOUR),),
    }
