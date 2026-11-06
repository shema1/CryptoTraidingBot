"""Таймфрейм рішень 4h для каталогу і meta (рішення 0025, T2.19).

Свічки 4h для правил, ATR, симуляції й бектесту — повні кошики з 1h; фічі рішення на відкритті 4h-свічки T —
рядок 1h-ряду з `entry_open_time_ms = T` (дані строго до T). Допуск і live для 4h — явна відмова.
"""

from __future__ import annotations

import math
import random
from decimal import Decimal

import pytest

from app.ml.features import CandleBar, SymbolData, compute_features
from app.ml.meta_model import fit_frozen_meta, train_meta
from app.ml.optuna_search import aggregate_bars
from app.ml.strategy import BREAKOUT, EMA_CROSS, SymbolHistory, primary_side
from app.ml.trade_sim import SimCandle, TargetParams
from app.ml.validation import H4_MS, build_split
from app.risk.costs import CostModel, FundingEvent
from app.risk.limits import RiskLimits
from tests.risk.conftest import instrument

HOUR = 3_600_000
BTC = "BTC/USDT:USDT"


def _bar(hour: int, open_: str, high: str, low: str, close: str) -> SimCandle:
    return SimCandle(
        open_time_ms=hour * HOUR, open=Decimal(open_), high=Decimal(high), low=Decimal(low), close=Decimal(close)
    )


def test_four_hour_bars_are_full_buckets_from_midnight_without_interpolation() -> None:
    hourly = [_bar(hour, "100", str(101 + hour), "99", str(100 + hour)) for hour in range(1, 13)]
    del hourly[6]  # година 7 (кошик 4–8) пропущена
    bars = aggregate_bars(hourly, H4_MS)
    # Кошик 0–4 неповний (немає години 0), 4–8 — з діркою: лишається лише 8–12.
    assert [bar.open_time_ms for bar in bars] == [8 * HOUR]
    only = bars[0]
    assert (only.open, only.high, only.low, only.close) == (Decimal(100), Decimal(112), Decimal(99), Decimal(111))
    assert aggregate_bars(hourly, HOUR) == tuple(hourly)


def test_rules_on_four_hour_bars_count_periods_in_four_hour_candles() -> None:
    # Пробій 24 свічок: на 4h-ряду крок між свічками — 4h; той самий ряд з кроком 1h для 4h — дірка, сигналу немає.
    bars = [
        SimCandle(open_time_ms=index * H4_MS, open=Decimal(100), high=Decimal(101), low=Decimal(99), close=Decimal(100))
        for index in range(26)
    ]
    bars[24] = SimCandle(
        open_time_ms=24 * H4_MS, open=Decimal(100), high=Decimal(106), low=Decimal(99), close=Decimal(105)
    )
    assert primary_side(BREAKOUT, bars, 25, bar_ms=H4_MS) is not None
    assert primary_side(BREAKOUT, bars, 25) is None
    with pytest.raises(ValueError, match="1h або 4h"):
        primary_side(EMA_CROSS, bars, 25, bar_ms=2 * HOUR)


def test_history_rejects_candles_off_the_four_hour_grid() -> None:
    with pytest.raises(ValueError, match="сітці"):
        SymbolHistory(
            symbol=BTC,
            candles=(_bar(1, "100", "101", "99", "100"),),
            instrument=instrument(symbol=BTC),
            bar_ms=H4_MS,
        )


def _random_walk(hours: int, seed: int) -> list[SimCandle]:
    rng = random.Random(seed)  # noqa: S311 — детерміновані тестові дані
    price = 100.0
    bars: list[SimCandle] = []
    for hour in range(hours):
        # Мультиплікативний ряд: ціна не падає до нуля, фічі валідні на всій історії.
        open_ = price
        close = price * math.exp(rng.gauss(0, 0.006))
        high = max(open_, close) * (1 + abs(rng.gauss(0, 0.002)))
        low = min(open_, close) * (1 - abs(rng.gauss(0, 0.002)))
        bars.append(
            SimCandle(
                open_time_ms=hour * HOUR,
                open=Decimal(f"{open_:.2f}"),
                high=Decimal(f"{high:.2f}"),
                low=Decimal(f"{low:.2f}"),
                close=Decimal(f"{close:.2f}"),
            )
        )
        price = close
    return bars


def _feature_data(hourly: list[SimCandle]) -> SymbolData:
    return SymbolData(
        candles=tuple(
            CandleBar(
                open_time_ms=bar.open_time_ms,
                open=float(bar.open),
                high=float(bar.high),
                low=float(bar.low),
                close=float(bar.close),
                volume=1.0 + (bar.open_time_ms // HOUR) % 7,
            )
            for bar in hourly
        )
    )


def _four_hour_setup() -> tuple[tuple[SymbolHistory, ...], dict[str, SymbolData], object]:
    hours = 12000
    hourly = _random_walk(hours, seed=11)
    history = SymbolHistory(
        symbol=BTC,
        candles=aggregate_bars(hourly, H4_MS),
        instrument=instrument(symbol=BTC),
        funding=(FundingEvent(time_ms=0, rate=Decimal(0), mark_price=Decimal(100)),),
        bar_ms=H4_MS,
    )
    split = build_split(
        data_from_ms=0,
        data_to_ms=hours * HOUR,
        horizon_bars=6,
        count_trades=lambda _start, _end: 20,
        # ATR(t−1) на 4h — Wilder на 1300 свічках 4h до входу: розмічені кандидати є лише після ~217 діб історії.
        min_train_ms=1600 * H4_MS,
        test_segment_ms=120 * H4_MS,
        test_segment_extended_ms=240 * H4_MS,
        min_trades_per_fold=1,
        bar_ms=H4_MS,
    )
    return (history,), {BTC: _feature_data(hourly)}, split


def _limits() -> RiskLimits:
    return RiskLimits(min_equity_usd=Decimal(100), max_notional_per_symbol_usd=Decimal(10_000_000))


def test_meta_on_four_hours_trains_on_four_hour_candidates_with_one_hour_features() -> None:
    books, feature_data, split = _four_hour_setup()
    assert split.bar_ms == H4_MS  # type: ignore[attr-defined]
    run = train_meta(
        books,
        spec=BREAKOUT,
        split=split,  # type: ignore[arg-type]
        trading_symbol=BTC,
        limits=_limits(),
        initial_equity=Decimal(10_000),
        maintenance_margin_rate=Decimal("0.01"),
        seed=3,
        costs=CostModel(),
        params=TargetParams(horizon_bars=6),
        feature_data=feature_data,
    )
    assert run.folds
    features = compute_features(feature_data)
    by_time = {int(time): index for index, time in enumerate(features.entry_open_time_ms)}
    scored = [row for fold in run.folds for row in fold.scored]
    assert scored, "кандидатів у тестах фолдів немає — тест порожній"
    for row in scored:
        # Рішення — на відкритті 4h-свічки, а фічі — рядок 1h-ряду з тим самим часом (дані строго до T).
        assert row.time_ms % H4_MS == 0
        assert row.time_ms in by_time
    for fold in run.folds:
        for fitted in fold.fitted:
            assert fitted.time_ms % H4_MS == 0


def test_admission_model_is_one_hour_only_until_four_hours_passes_g1() -> None:
    books, feature_data, split = _four_hour_setup()
    with pytest.raises(ValueError, match="рішення 0025"):
        fit_frozen_meta(
            books,
            spec=BREAKOUT,
            split=split,  # type: ignore[arg-type]
            trading_symbol=BTC,
            seed=3,
            costs=CostModel(),
            params=TargetParams(horizon_bars=6),
            feature_data=feature_data,
        )


def test_hours_inside_the_four_hour_candle_t_never_change_the_decision_at_t() -> None:
    """Look-ahead 4h: 1h-свічки з [T, T+4h) — майбутнє для рішення на відкритті 4h-свічки T.

    Ні фічі (1h-ряд), ні ATR(t−1) (4h-свічки до T), ні сторона правила від них не залежать; а свічка T−1h — так.
    """
    from app.ml.trade_sim import atr_before_entry

    hourly = _random_walk(7000, seed=5)
    entry = 6000 * HOUR  # кратне 4h
    changed = list(hourly)
    for hour in range(6000, 6004):
        bar = hourly[hour]
        changed[hour] = SimCandle(
            open_time_ms=bar.open_time_ms,
            open=bar.open,
            high=bar.high * 3,
            low=bar.low / 3,
            close=bar.close * 2,
        )
    rows = {}
    for label, series in (("base", hourly), ("changed", changed)):
        features = compute_features({BTC: _feature_data(series)}, enabled_sources=())
        index = list(features.entry_open_time_ms).index(entry)
        assert features.valid[index], "рядок фіч на T невалідний — тест порожній"
        bars = aggregate_bars(series, H4_MS)
        position = [bar.open_time_ms for bar in bars].index(entry)
        rows[label] = (
            features.values[index].tolist(),
            atr_before_entry(bars, position, bar_ms=H4_MS),
            primary_side(BREAKOUT, bars, position, bar_ms=H4_MS),
            primary_side(EMA_CROSS, bars, position, bar_ms=H4_MS),
        )
    assert rows["base"] == rows["changed"]
    # Тест не порожній: свічка T−1h уже в даних рішення.
    past = list(hourly)
    bar = hourly[5999]
    past[5999] = SimCandle(
        open_time_ms=bar.open_time_ms, open=bar.open, high=bar.high * 3, low=bar.low / 3, close=bar.close * 2
    )
    features = compute_features({BTC: _feature_data(past)}, enabled_sources=())
    index = list(features.entry_open_time_ms).index(entry)
    assert features.values[index].tolist() != rows["base"][0]


def test_catalog_on_four_hours_labels_candidates_and_keeps_paths_before_the_holdout() -> None:
    from app.ml.strategy import compare_strategies

    books, _feature, split = _four_hour_setup()
    comparison = compare_strategies(
        books,
        split=split,  # type: ignore[arg-type]
        trading_symbol=BTC,
        limits=_limits(),
        initial_equity=Decimal(10_000),
        maintenance_margin_rate=Decimal("0.01"),
        seed=3,
        costs=CostModel(),
        params=TargetParams(horizon_bars=6),
    )
    breakout = next(item for item in comparison.variants if item.spec.name == "breakout")
    assert sum(record.test_labels.n_labels for record in breakout.folds) > 0
    for record in breakout.folds:
        for report in record.reports:
            for trade in report.trades:
                assert trade.entry_open_time_ms % H4_MS == 0
                assert trade.exit_open_time_ms < split.holdout_from_ms  # type: ignore[attr-defined]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_four_hour_experiment_is_journaled_as_4h_and_never_opens_the_holdout(conn: object) -> None:
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.ml.models import Experiment, HoldoutOpening
    from app.ml.statistics import open_holdout_after_gate
    from app.ml.validation import create_experiment

    _books, _feature, split = _four_hour_setup()
    async with AsyncSession(bind=conn) as session:  # type: ignore[call-overload, arg-type]
        experiment_id = await create_experiment(
            session,
            split=split,  # type: ignore[arg-type]
            research_line=BTC,
            symbols=(BTC,),
            label_type="rule_only",
            primary_strategy={"name": "breakout"},
            params={},
            seed=3,
        )
        stored = await session.get(Experiment, experiment_id)
        assert stored is not None
        assert stored.timeframe == "4h"
        with pytest.raises(ValueError, match="4h"):
            await open_holdout_after_gate(session, experiment_id=experiment_id, trial_id=1, model_version="m")
        openings = (await session.execute(select(func.count()).select_from(HoldoutOpening))).scalar_one()
        assert openings == 0
