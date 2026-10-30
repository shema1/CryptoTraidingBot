"""Фічі для свічки `t` лише з даних, відомих на закриття `t-1` (ТЗ §5.1, §5.5, §5А.4, T2.1).

Один `compute_features()` для навчання і для live. Live не перераховує історію: `FeatureBuffer` тримає останні
`FEATURE_LOOKBACK_BARS` закритих годинних свічок і викликає ту саму функцію.

Правило часу. Рядок з `entry_open_time_ms = T` — це рішення на відкритті свічки `t`. Свічки беруться з
`[T - lookback, T)`: годинна свічка входить, якщо її `open_time_ms < T` (вона вже закрита). Старші 4h/1d — рівно
`H4_BARS`/`D1_BARS` останніх повністю закритих (`bucket + крок <= T`): набір змінюється лише тоді, коли закривається
нова старша свічка, а не з годиною доби. Година дериватива `ts_ms` закривається в `ts_ms + 1h`, але в live потрапляє
в БД пізніше за рішення (ліквідації — після `SEAL_MARGIN_MS`, OI/premium/long-short — з проходом колектора раз на
`BACKFILL_INTERVAL_S`). Тому фічі читають лише години, що закрились щонайменше за `DERIVATIVE_LAG_MS` до `T`.
Funding пише прохід свічок: це подія строго до рішення (`funding_time_ms < T`, вік не більше `FUNDING_MAX_STALE_MS`);
ставка з міткою рівно `T` у live ще може не встигнути в БД, тож офлайн її теж не бачить. Попередня ставка віком рівно
8 год лишається свіжою.

Прогалини. Рядок не рахується (усі колонки NaN, `valid` хибний), якщо дірка перетинає години, які він читає: усе
вікно свічок (EMA і ATR ідуть по ньому цілком) і лише ті години дериватива, з яких рахується його фіча
(`derivative_window`). Дірка ряду — `find_gaps`, явна дірка — `window_has_gap`; обидва предикати з
`app/data/gaps.py`. Інтерполяції немає. Фіча дериватива вмикається на весь набір колонок, лише коли її історія
покриває переданий навчальний діапазон у кожному символі.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final, TypeVar

import numpy as np
from numpy.typing import NDArray
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import H1_MS
from app.core.db import SYMBOL_RE
from app.data.gaps import CANDLE_SOURCE, GapRange, find_gaps, open_gaps_in_window, window_has_gap

HOUR_MS: Final = H1_MS
H4_MS: Final = 4 * HOUR_MS
D1_MS: Final = 24 * HOUR_MS
# EMA(50) на 1d потребує 50 повних діб. 1300 годин вистачає за будь-якого зсуву відносно півночі UTC (ТЗ §5.5).
FEATURE_LOOKBACK_BARS: Final = 1300
FEATURE_LOOKBACK_MS: Final = FEATURE_LOOKBACK_BARS * HOUR_MS
# Будь-яка зміна формули без зміни чисел нижче — підняти версію вручну. Вона входить у schema hash.
PREPROCESSING_VERSION: Final = "1.0.0"

ATR_PERIOD: Final = 14
RSI_PERIOD: Final = 14
EMA_FAST: Final = 20
EMA_SLOW: Final = 50
DAILY_EMA_SPAN: Final = 50
RET_BARS: Final = (1, 6, 24)
STD_BARS: Final = 24
VOL_BARS: Final = 24
OI_CHANGE_BARS: Final = 24
LIQ_BARS: Final = 24
DAILY_SLOPE_LAG: Final = 10
# Скільки повністю закритих старших свічок є у вікні за будь-якої години рішення. Найгірше остання закрита
# закінчилась на `крок - 1h` раніше за `T`, тож вміщається `(FEATURE_LOOKBACK_BARS + 1) // годин_у_свічці - 1`.
# Кількість стала: інакше на 1d у вікні то на одну добу більше, то менше, і EMA/ATR зсуваються з годиною доби.
H4_BARS: Final = (FEATURE_LOOKBACK_BARS + 1) // (H4_MS // HOUR_MS) - 1
D1_BARS: Final = (FEATURE_LOOKBACK_BARS + 1) // (D1_MS // HOUR_MS) - 1
# Ставка старша за один стандартний інтервал USDT-perpetual не використовується. Коротший інтервал (4h) теж
# свіжий: остання подія тоді молодша за цю межу.
FUNDING_MAX_STALE_MS: Final = 8 * HOUR_MS
# Година дериватива, що закривається рівно в `T`, у live ще не в БД: ліквідації запечатуються через `SEAL_MARGIN_MS`,
# OI/premium/long-short колектор докачує раз на `BACKFILL_INTERVAL_S`, а сигнал старіє за `max_signal_age_ms`.
# Остання година дериватива — та, що закрилась у `T - DERIVATIVE_LAG_MS`; T5.2 перевіряє, що вона вже в БД.
DERIVATIVE_LAG_MS: Final = HOUR_MS

SOURCE_OPEN_INTEREST: Final = "open_interest"
SOURCE_PREMIUM: Final = "premium_index"
SOURCE_LONG_SHORT: Final = "long_short"
SOURCE_LIQUIDATIONS: Final = "liquidations"
SOURCE_FUNDING: Final = "funding"
# У `data_gaps` немає джерела funding (його пише прохід свічок). Дірки funding рахуються з самих подій.
DERIVATIVE_SOURCES: Final = (
    SOURCE_FUNDING,
    SOURCE_OPEN_INTEREST,
    SOURCE_PREMIUM,
    SOURCE_LONG_SHORT,
    SOURCE_LIQUIDATIONS,
)
FEATURE_GAP_SOURCES: Final = (
    CANDLE_SOURCE,
    SOURCE_OPEN_INTEREST,
    SOURCE_PREMIUM,
    SOURCE_LONG_SHORT,
    SOURCE_LIQUIDATIONS,
)
_GAP_KEYS: Final = frozenset((*FEATURE_GAP_SOURCES, SOURCE_FUNDING))
# Скільки останніх годин (до `T - DERIVATIVE_LAG_MS`) читає фіча годинного дериватива: зміна OI — дві точки на
# відстані `OI_CHANGE_BARS`, ліквідації — середнє за `LIQ_BARS`, premium і long/short — одна година.
_DERIVATIVE_SPAN_BARS: Final = {
    SOURCE_OPEN_INTEREST: OI_CHANGE_BARS + 1,
    SOURCE_PREMIUM: 1,
    SOURCE_LONG_SHORT: 1,
    SOURCE_LIQUIDATIONS: LIQ_BARS,
}

# Корельовані вікна зі списку прикладів ТЗ §5А.4 навмисно не входять: log-return 3 і 12, std за 12,
# зміна OI за 1 і 6. З усіма п'ятьма деривативами колонок рівно 30 — стеля ТЗ.
PRICE_COLUMNS: Final = (
    "log_ret_1",
    "log_ret_6",
    "log_ret_24",
    "atr_pct",
    "ret_std_24",
    "hl_range",
    "rsi_14",
    "dist_ema_20",
    "dist_ema_50",
    "ema20_slope",
    "vol_ratio_24",
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "ema_spread_atr",
    "ret_4h",
    "ret_1d",
    "atr_pct_4h",
    "atr_pct_1d",
    "dist_ema50_1d",
    "slope_1d",
)
DERIVATIVE_COLUMNS: Final = (
    "funding_rate",
    "oi_chg_24",
    "premium_close",
    "ls_buy_ratio",
    "liq_ratio",
)
TIME_COLUMNS: Final = ("hour_sin", "hour_cos", "dow_sin", "dow_cos")
_COLUMN_SOURCE: Final = {
    "funding_rate": SOURCE_FUNDING,
    "oi_chg_24": SOURCE_OPEN_INTEREST,
    "premium_close": SOURCE_PREMIUM,
    "ls_buy_ratio": SOURCE_LONG_SHORT,
    "liq_ratio": SOURCE_LIQUIDATIONS,
}

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
BoolArray = NDArray[np.bool_]
_HigherBars = tuple[FloatArray, FloatArray, FloatArray, FloatArray]

_SYMBOL: Final = re.compile(SYMBOL_RE)
_SCHEMA_TAGS: Final = {
    "ema_seed": "first_value",
    "ema_alpha": "2/(span+1)",
    "atr": "wilder",
    "rsi": "wilder",
    "std_ddof": "0",
    "higher_return": "log(close/open)",
    "higher_bars": "last_closed_fixed_count",
    "daily_slope": "log(close[-1]/close[-lag])/(lag-1)",
    "zero_mean_ratio": "1",
    "premium_field": "close",
    "derivative_asof": "hour_closed_by_T_minus_lag",
    "funding_asof": "strictly_before",
}


def feature_columns(enabled_sources: Collection[str]) -> tuple[str, ...]:
    """Колонки в сталому порядку. Вимкнене джерело дериватива не входить у вектор і в schema hash."""
    enabled = _require_sources(enabled_sources)
    derivative = tuple(name for name in DERIVATIVE_COLUMNS if _COLUMN_SOURCE[name] in enabled)
    columns = (*PRICE_COLUMNS, *derivative, *TIME_COLUMNS)
    if len(columns) > 30:
        raise ValueError("набір фіч довший за 30")
    return columns


def feature_schema_hash(columns: Sequence[str]) -> str:
    """`sha256:` канонічного опису колонок і параметрів вікон. Той самий набір — той самий хеш."""
    payload = {
        "preprocessing_version": PREPROCESSING_VERSION,
        "lookback_bars": FEATURE_LOOKBACK_BARS,
        "hour_ms": HOUR_MS,
        "parameters": {
            "atr_period": ATR_PERIOD,
            "rsi_period": RSI_PERIOD,
            "ema_fast": EMA_FAST,
            "ema_slow": EMA_SLOW,
            "daily_ema_span": DAILY_EMA_SPAN,
            "ret_bars": list(RET_BARS),
            "std_bars": STD_BARS,
            "vol_bars": VOL_BARS,
            "oi_change_bars": OI_CHANGE_BARS,
            "liq_bars": LIQ_BARS,
            "daily_slope_lag": DAILY_SLOPE_LAG,
            "funding_max_stale_ms": FUNDING_MAX_STALE_MS,
            "derivative_lag_ms": DERIVATIVE_LAG_MS,
            "higher_tf_ms": [H4_MS, D1_MS],
            "higher_tf_bars": [H4_BARS, D1_BARS],
            **_SCHEMA_TAGS,
        },
        "columns": list(columns),
    }
    raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def derivative_window(source: str, entry_open_time_ms: int) -> tuple[int, int]:
    """Інтервал `[start, end)`, який читає фіча джерела для рядка `entry_open_time_ms = T`.

    Дірка в ньому відкидає рядок; поза ним — ні. Годинні ряди закінчуються на `T - DERIVATIVE_LAG_MS`.
    Funding — `[T - FUNDING_MAX_STALE_MS, T)`: старша ставка вже несвіжа.
    """
    as_of = _require_ms("entry_open_time_ms", entry_open_time_ms)
    if as_of % HOUR_MS != 0:
        raise ValueError("entry_open_time_ms не вирівняний за 1h")
    if source == SOURCE_FUNDING:
        start, end = as_of - FUNDING_MAX_STALE_MS, as_of
    elif source in _DERIVATIVE_SPAN_BARS:
        end = as_of - DERIVATIVE_LAG_MS
        start = end - _DERIVATIVE_SPAN_BARS[source] * HOUR_MS
    else:
        raise ValueError(f"невідоме джерело: {source}")
    if start < 0:
        raise ValueError("вікно дериватива починається до 1970-01-01")
    return start, end


@dataclass(frozen=True, slots=True)
class CandleBar:
    """Закрита годинна свічка. Ціни вже у float: межа ML (у торговому шарі лишається Decimal)."""

    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True, slots=True)
class TimedValue:
    """Точка ряду. Для годинного дериватива `ts_ms` — початок години; для funding — час події."""

    ts_ms: int
    value: float


@dataclass(frozen=True, slots=True)
class LongShortPoint:
    ts_ms: int
    buy_ratio: float
    sell_ratio: float


@dataclass(frozen=True, slots=True)
class LiquidationBar:
    """Номінал ліквідацій за годину, USDT. Нулі тихої прослуханої години — дані, не прогалина."""

    ts_ms: int
    long_notional: float
    short_notional: float


@dataclass(frozen=True, slots=True)
class SymbolData:
    """Ряди одного символу і відкриті прогалини за ключем джерела (`GapRange`, інтервал `[from, to)`)."""

    candles: tuple[CandleBar, ...]
    open_interest: tuple[TimedValue, ...] = ()
    premium: tuple[TimedValue, ...] = ()
    long_short: tuple[LongShortPoint, ...] = ()
    liquidations: tuple[LiquidationBar, ...] = ()
    funding: tuple[TimedValue, ...] = ()
    gaps: Mapping[str, tuple[GapRange, ...]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FeatureRows:
    """Рядки фіч. `values[i]` скінченний тоді й лише тоді, коли `valid[i]`. Ідентифікатора символу в колонках немає."""

    columns: tuple[str, ...]
    schema_hash: str
    preprocessing_version: str
    symbol: tuple[str, ...]
    entry_open_time_ms: IntArray
    features_open_time_ms: IntArray
    values: FloatArray
    valid: BoolArray

    def column(self, name: str) -> FloatArray:
        return self.values[:, self.columns.index(name)]

    def select(self, index: int) -> FeatureRows:
        """Один рядок. Від'ємний індекс — від кінця."""
        size = self.entry_open_time_ms.size
        if index < 0:
            index += size
        if index < 0 or index >= size:
            raise IndexError("немає такого рядка фіч")
        return FeatureRows(
            columns=self.columns,
            schema_hash=self.schema_hash,
            preprocessing_version=self.preprocessing_version,
            symbol=(self.symbol[index],),
            entry_open_time_ms=self.entry_open_time_ms[index : index + 1].copy(),
            features_open_time_ms=self.features_open_time_ms[index : index + 1].copy(),
            values=self.values[index : index + 1].copy(),
            valid=self.valid[index : index + 1].copy(),
        )


def sources_covering(
    series: Mapping[str, SymbolData],
    *,
    start_ms: int,
    end_ms: int,
) -> frozenset[str]:
    """Джерела деривативів, чия історія покриває `[start_ms, end_ms)` в кожному символі.

    Межі вирівняні за годиною. Порожній набір символів не вмикає жодного джерела.
    """
    start, end = _aligned_bounds(start_ms, end_ms)
    if not series:
        return frozenset()
    kept: set[str] = set()
    for source in DERIVATIVE_SOURCES:
        if all(_source_covers(data, source, start, end) for data in series.values()):
            kept.add(source)
    return frozenset(kept)


def funding_gap_ranges(times: Iterable[int], *, start_ms: int, end_ms: int) -> tuple[GapRange, ...]:
    """Ділянки `[start_ms, end_ms)`, де немає ставки з віком `<= FUNDING_MAX_STALE_MS`.

    Подія `t` покриває миті `[t, t + FUNDING_MAX_STALE_MS]` включно.
    """
    start = _require_ms("start_ms", start_ms)
    end = _require_ms("end_ms", end_ms)
    if end < start:
        raise ValueError("end_ms менший за start_ms")
    if end == start:
        return ()
    intervals: list[tuple[int, int]] = []
    seen: set[int] = set()
    for raw in times:
        stamp = _require_ms("funding_time_ms", raw)
        if stamp in seen or stamp >= end:
            continue
        seen.add(stamp)
        hi = stamp + FUNDING_MAX_STALE_MS + 1
        if hi <= start:
            continue
        lo = max(stamp, start)
        clipped_hi = min(hi, end)
        if lo < clipped_hi:
            intervals.append((lo, clipped_hi))
    intervals.sort()
    cover: list[list[int]] = []
    for lo, hi in intervals:
        if not cover or lo > cover[-1][1]:
            cover.append([lo, hi])
        else:
            cover[-1][1] = max(cover[-1][1], hi)
    gaps: list[GapRange] = []
    cursor = start
    for lo, hi in cover:
        if lo > cursor:
            gaps.append(GapRange(cursor, lo))
        cursor = max(cursor, hi)
    if cursor < end:
        gaps.append(GapRange(cursor, end))
    return tuple(gaps)


def compute_features(
    series: Mapping[str, SymbolData],
    *,
    enabled_sources: Collection[str] | None = None,
    coverage_start_ms: int | None = None,
    coverage_end_ms: int | None = None,
) -> FeatureRows:
    """Фічі для кожного символу. Рядки згруповані за символом (алфавіт) і часом відкриття `t`.

    `coverage_start_ms`/`coverage_end_ms` — навчальне вікно: джерело, що його не покриває, зникає з колонок.
    Без цього вікна лишається `enabled_sources` (за замовчуванням усі деривативи), а дірка викидає окремий рядок.
    """
    enabled = _resolve_enabled(series, enabled_sources, coverage_start_ms, coverage_end_ms)
    columns = feature_columns(enabled)
    schema_hash = feature_schema_hash(columns)
    symbols: list[str] = []
    entries: list[IntArray] = []
    feature_times: list[IntArray] = []
    values: list[FloatArray] = []
    valid: list[BoolArray] = []
    for symbol in sorted(series):
        _require_symbol(symbol)
        block = _compute_symbol(series[symbol], columns, enabled)
        count = block[0].size
        symbols.extend([symbol] * count)
        entries.append(block[0])
        feature_times.append(block[1])
        values.append(block[2])
        valid.append(block[3])
    width = len(columns)
    if not entries:
        empty_i = np.empty(0, dtype=np.int64)
        return FeatureRows(
            columns=columns,
            schema_hash=schema_hash,
            preprocessing_version=PREPROCESSING_VERSION,
            symbol=(),
            entry_open_time_ms=empty_i,
            features_open_time_ms=empty_i.copy(),
            values=np.empty((0, width), dtype=np.float64),
            valid=np.empty(0, dtype=np.bool_),
        )
    return FeatureRows(
        columns=columns,
        schema_hash=schema_hash,
        preprocessing_version=PREPROCESSING_VERSION,
        symbol=tuple(symbols),
        entry_open_time_ms=np.concatenate(entries),
        features_open_time_ms=np.concatenate(feature_times),
        values=np.concatenate(values, axis=0),
        valid=np.concatenate(valid),
    )


class FeatureBuffer:
    """Кільцевий буфер одного символу. `latest()` — останній рядок того самого `compute_features()`.

    Довжина свічок — `FEATURE_LOOKBACK_BARS`. Старіші за вікно точки відкидаються, коли приходить новіша свічка.
    Точка дериватива, що прийшла раніше за свою свічку (колектор і прохід свічок — різні процеси), лишається в
    буфері й увійде у фічу, коли до неї дійде вікно. Точка години, що щойно закрилась або ще формується, у фічу не
    входить (`DERIVATIVE_LAG_MS`); остаточне значення замінює її за тим самим `ts_ms`. Funding з міткою рівно `T`
    у буфері лишається, але у фічу не входить.
    Буфер не дивиться на годинник: сюди кладуть лише вже закриті свічки.
    """

    def __init__(self, symbol: str, *, enabled_sources: Collection[str] | None = None) -> None:
        _require_symbol(symbol)
        self.symbol = symbol
        self.enabled_sources = (
            frozenset(DERIVATIVE_SOURCES) if enabled_sources is None else _require_sources(enabled_sources)
        )
        self._newest: int | None = None
        self._candles: dict[int, CandleBar] = {}
        self._oi: dict[int, float] = {}
        self._premium: dict[int, float] = {}
        self._long_short: dict[int, tuple[float, float]] = {}
        self._liquidations: dict[int, tuple[float, float]] = {}
        self._funding: dict[int, float] = {}
        self._gaps: dict[str, tuple[GapRange, ...]] = {}

    def upsert_candle(self, bar: CandleBar) -> None:
        if not self._keeps(bar.open_time_ms):
            return
        self._candles[bar.open_time_ms] = bar
        if self._newest is None or bar.open_time_ms > self._newest:
            self._newest = bar.open_time_ms
            self._trim()

    def upsert_open_interest(self, point: TimedValue) -> None:
        if self._keeps(point.ts_ms):
            self._oi[point.ts_ms] = point.value

    def upsert_premium(self, point: TimedValue) -> None:
        if self._keeps(point.ts_ms):
            self._premium[point.ts_ms] = point.value

    def upsert_long_short(self, point: LongShortPoint) -> None:
        if self._keeps(point.ts_ms):
            self._long_short[point.ts_ms] = (point.buy_ratio, point.sell_ratio)

    def upsert_liquidation(self, bar: LiquidationBar) -> None:
        if self._keeps(bar.ts_ms):
            self._liquidations[bar.ts_ms] = (bar.long_notional, bar.short_notional)

    def upsert_funding(self, point: TimedValue) -> None:
        # Ставка трохи до вікна ще може бути свіжою на його першій годині.
        if self._keeps(point.ts_ms + FUNDING_MAX_STALE_MS):
            self._funding[point.ts_ms] = point.value

    def set_gaps(self, source: str, gaps: Sequence[GapRange]) -> None:
        if source not in _GAP_KEYS:
            raise ValueError(f"невідоме джерело прогалин: {source}")
        self._gaps[source] = tuple(gaps)

    def latest(self) -> FeatureRows:
        """Фічі на відкритті години після останньої свічки буфера. Порожній буфер — порожній набір рядків."""
        rows = compute_features({self.symbol: self.snapshot()}, enabled_sources=self.enabled_sources)
        if rows.entry_open_time_ms.size == 0:
            return rows
        return rows.select(-1)

    def snapshot(self) -> SymbolData:
        candles = tuple(self._candles[ts] for ts in sorted(self._candles))
        return SymbolData(
            candles=candles,
            open_interest=tuple(TimedValue(ts, self._oi[ts]) for ts in sorted(self._oi)),
            premium=tuple(TimedValue(ts, self._premium[ts]) for ts in sorted(self._premium)),
            long_short=tuple(
                LongShortPoint(ts, self._long_short[ts][0], self._long_short[ts][1]) for ts in sorted(self._long_short)
            ),
            liquidations=tuple(
                LiquidationBar(ts, self._liquidations[ts][0], self._liquidations[ts][1])
                for ts in sorted(self._liquidations)
            ),
            funding=tuple(TimedValue(ts, self._funding[ts]) for ts in sorted(self._funding)),
            gaps=dict(self._gaps),
        )

    def _window_start(self) -> int | None:
        if self._newest is None:
            return None
        return self._newest + HOUR_MS - FEATURE_LOOKBACK_MS

    def _keeps(self, stamp: int) -> bool:
        start = self._window_start()
        return start is None or stamp >= start

    def _trim(self) -> None:
        # Лише на новій свічці: вікно зсувається раз на годину, а не на кожну точку.
        start = self._window_start()
        if start is None:
            return
        self._candles = _drop_before(self._candles, start)
        self._oi = _drop_before(self._oi, start)
        self._premium = _drop_before(self._premium, start)
        self._long_short = _drop_before(self._long_short, start)
        self._liquidations = _drop_before(self._liquidations, start)
        self._funding = _drop_before(self._funding, start - FUNDING_MAX_STALE_MS)


async def load_feature_gaps(
    session: AsyncSession,
    *,
    exchange: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> dict[str, tuple[GapRange, ...]]:
    """Відкриті прогалини вікна `[start_ms, end_ms)` для свічок і годинних деривативів.

    Лише відкриті рядки (`open_gaps_in_window`). Funding сюди не входить: дірки funding дає `funding_gap_ranges`.
    """
    found: dict[str, tuple[GapRange, ...]] = {}
    for source in FEATURE_GAP_SOURCES:
        found[source] = await open_gaps_in_window(
            session,
            source=source,
            exchange=exchange,
            symbol=symbol,
            timeframe="1h",
            start_ms=start_ms,
            end_ms=end_ms,
        )
    return found


@dataclass(frozen=True, slots=True)
class _Series:
    """Провалідовані ряди символу. Funding — відсортовані масиви для пошуку останньої події до `T`."""

    hourly: Mapping[str, Mapping[int, float]]
    funding_times: IntArray
    funding_rates: FloatArray
    gaps: Mapping[str, tuple[GapRange, ...]]


def _compute_symbol(
    data: SymbolData,
    columns: tuple[str, ...],
    enabled: frozenset[str],
) -> tuple[IntArray, IntArray, FloatArray, BoolArray]:
    candles = _validate_candles(data.candles)
    funding = _validate_timed(data.funding, "funding", minimum=None, aligned=False)
    funding_times = np.array(sorted(funding), dtype=np.int64)
    series = _Series(
        hourly={
            SOURCE_OPEN_INTEREST: _validate_timed(data.open_interest, "open_interest", minimum=0.0),
            SOURCE_PREMIUM: _validate_timed(data.premium, "premium", minimum=None),
            SOURCE_LONG_SHORT: _validate_long_short(data.long_short),
            SOURCE_LIQUIDATIONS: _validate_liquidations(data.liquidations),
        },
        funding_times=funding_times,
        funding_rates=np.array([funding[stamp] for stamp in funding_times.tolist()], dtype=np.float64),
        gaps=_validate_gaps(data.gaps),
    )
    count = len(candles)
    width = len(columns)
    entries = np.empty(count, dtype=np.int64)
    feature_times = np.empty(count, dtype=np.int64)
    values = np.full((count, width), np.nan, dtype=np.float64)
    valid = np.zeros(count, dtype=np.bool_)
    if count == 0:
        return entries, feature_times, values, valid
    opens = np.array([bar.open_time_ms for bar in candles], dtype=np.int64)
    opens_o = np.array([bar.open for bar in candles], dtype=np.float64)
    highs = np.array([bar.high for bar in candles], dtype=np.float64)
    lows = np.array([bar.low for bar in candles], dtype=np.float64)
    closes = np.array([bar.close for bar in candles], dtype=np.float64)
    volumes = np.array([bar.volume for bar in candles], dtype=np.float64)
    for index in range(count):
        as_of = int(opens[index]) + HOUR_MS
        entries[index] = as_of
        feature_times[index] = int(opens[index])
        start = as_of - FEATURE_LOOKBACK_MS
        if start < 0:
            continue
        # Мітки унікальні й вирівняні за годиною, тож рівно FEATURE_LOOKBACK_BARS свічок у [start, T) — це вікно
        # без жодної дірки сітки. Свічка `index` — остання з `open_time_ms < T`.
        first = int(np.searchsorted(opens, start, side="left"))
        if index + 1 - first != FEATURE_LOOKBACK_BARS:
            continue
        window = slice(first, index + 1)
        vector = _row_features(
            open_=opens_o[window],
            high=highs[window],
            low=lows[window],
            close=closes[window],
            volume=volumes[window],
            series=series,
            columns=columns,
            enabled=enabled,
            start=start,
            as_of=as_of,
        )
        if vector is None:
            continue
        values[index] = vector
        valid[index] = bool(np.isfinite(vector).all())
    return entries, feature_times, values, valid


def _row_features(
    *,
    open_: FloatArray,
    high: FloatArray,
    low: FloatArray,
    close: FloatArray,
    volume: FloatArray,
    series: _Series,
    columns: tuple[str, ...],
    enabled: frozenset[str],
    start: int,
    as_of: int,
) -> FloatArray | None:
    # Вікно свічок суцільне (перевірено вище); явна відкрита прогалина в ньому теж скасовує рядок.
    if window_has_gap(series.gaps.get(CANDLE_SOURCE, ()), start_ms=start, end_ms=as_of):
        return None
    for source in sorted(enabled):
        if _derivative_blocked(source, series, as_of):
            return None
    computed = _price_and_time(open_, high, low, close, volume, start=start, as_of=as_of)
    end = as_of - DERIVATIVE_LAG_MS
    if SOURCE_FUNDING in enabled:
        computed["funding_rate"] = _funding_rate(series.funding_times, series.funding_rates, as_of)
    if SOURCE_OPEN_INTEREST in enabled:
        computed["oi_chg_24"] = _log_change(series.hourly[SOURCE_OPEN_INTEREST], end, OI_CHANGE_BARS)
    if SOURCE_PREMIUM in enabled:
        computed["premium_close"] = _last_value(series.hourly[SOURCE_PREMIUM], end)
    if SOURCE_LONG_SHORT in enabled:
        computed["ls_buy_ratio"] = _last_value(series.hourly[SOURCE_LONG_SHORT], end)
    if SOURCE_LIQUIDATIONS in enabled:
        computed["liq_ratio"] = _trailing_ratio(series.hourly[SOURCE_LIQUIDATIONS], end, LIQ_BARS)
    vector = np.array([computed[name] for name in columns], dtype=np.float64)
    if not np.isfinite(vector).all():
        return None
    return vector


def _price_and_time(
    open_: FloatArray,
    high: FloatArray,
    low: FloatArray,
    close: FloatArray,
    volume: FloatArray,
    *,
    start: int,
    as_of: int,
) -> dict[str, float]:
    last = float(close[-1])
    ema_fast_prev, ema_fast = _ema_tail(close, EMA_FAST)
    _, ema_slow = _ema_tail(close, EMA_SLOW)
    atr = _atr_last(high, low, close, ATR_PERIOD)
    bars_4h = _closed_higher(open_, high, low, close, start=start, as_of=as_of, bar_ms=H4_MS, count=H4_BARS)
    bars_1d = _closed_higher(open_, high, low, close, start=start, as_of=as_of, bar_ms=D1_MS, count=D1_BARS)
    body, upper, lower = _candle_shape(open_, high, low, close)
    out: dict[str, float] = {
        "log_ret_1": _log_return(close, RET_BARS[0]),
        "log_ret_6": _log_return(close, RET_BARS[1]),
        "log_ret_24": _log_return(close, RET_BARS[2]),
        "atr_pct": _pct(atr, last),
        "ret_std_24": _return_std(close, STD_BARS),
        "hl_range": (float(high[-1]) - float(low[-1])) / last,
        "rsi_14": _rsi_last(close, RSI_PERIOD),
        "dist_ema_20": _distance(last, ema_fast),
        "dist_ema_50": _distance(last, ema_slow),
        "ema20_slope": _distance_delta(ema_fast, ema_fast_prev, last),
        "vol_ratio_24": _ratio_to_mean(volume, VOL_BARS),
        "body_ratio": body,
        "upper_wick_ratio": upper,
        "lower_wick_ratio": lower,
        "ema_spread_atr": _spread(ema_fast, ema_slow, atr),
        "ret_4h": _bar_return(bars_4h),
        "ret_1d": _bar_return(bars_1d),
        "atr_pct_4h": _higher_atr_pct(bars_4h),
        "atr_pct_1d": _higher_atr_pct(bars_1d),
        "dist_ema50_1d": _higher_distance(bars_1d, DAILY_EMA_SPAN),
        "slope_1d": _higher_slope(bars_1d, DAILY_SLOPE_LAG),
    }
    out.update(_clock_features(as_of))
    return out


def _closed_higher(
    open_: FloatArray,
    high: FloatArray,
    low: FloatArray,
    close: FloatArray,
    *,
    start: int,
    as_of: int,
    bar_ms: int,
    count: int,
) -> _HigherBars | None:
    """Останні `count` повністю закритих старших свічок суцільного вікна годин `[start, as_of)`.

    Остання закрита закінчується на `as_of - as_of % bar_ms` (`bucket + крок <= T`); свічка, що формується, не
    входить. Межі старших свічок — від 1970-01-01 00:00 UTC.
    """
    if open_.size != (as_of - start) // HOUR_MS:
        raise ValueError("вікно годин не суцільне")
    need = bar_ms // HOUR_MS
    end = as_of - as_of % bar_ms
    first = end - count * bar_ms
    if first < start:
        return None
    lo = (first - start) // HOUR_MS
    hi = lo + count * need
    shape = (count, need)
    return (
        open_[lo:hi].reshape(shape)[:, 0].copy(),
        high[lo:hi].reshape(shape).max(axis=1),
        low[lo:hi].reshape(shape).min(axis=1),
        close[lo:hi].reshape(shape)[:, -1].copy(),
    )


def _ema_tail(values: FloatArray, span: int) -> tuple[float, float]:
    """Останні два значення EMA. Насіння — перше значення вікна, alpha = 2/(span+1)."""
    if values.size < span:
        return math.nan, math.nan
    alpha = 2.0 / (span + 1.0)
    decay = 1.0 - alpha
    ema = float(values[0])
    previous = ema
    for value in values[1:]:
        previous = ema
        ema = alpha * float(value) + decay * ema
    return previous, ema


def _atr_last(high: FloatArray, low: FloatArray, close: FloatArray, period: int) -> float:
    if close.size < period + 1:
        return math.nan
    previous_close = close[:-1]
    true_range = np.maximum(
        high[1:] - low[1:],
        np.maximum(np.abs(high[1:] - previous_close), np.abs(low[1:] - previous_close)),
    )
    atr = float(np.mean(true_range[:period]))
    for value in true_range[period:]:
        atr = (atr * (period - 1) + float(value)) / period
    return atr


def _rsi_last(close: FloatArray, period: int) -> float:
    if close.size < period + 1:
        return math.nan
    delta = np.diff(close)
    gain = np.maximum(delta, 0.0)
    loss = np.maximum(-delta, 0.0)
    avg_gain = float(np.mean(gain[:period]))
    avg_loss = float(np.mean(loss[:period]))
    for index in range(period, gain.size):
        avg_gain = (avg_gain * (period - 1) + float(gain[index])) / period
        avg_loss = (avg_loss * (period - 1) + float(loss[index])) / period
    if avg_loss == 0.0:
        return 100.0 if avg_gain > 0.0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _log_return(close: FloatArray, bars: int) -> float:
    if close.size <= bars:
        return math.nan
    last = float(close[-1])
    previous = float(close[-1 - bars])
    if last <= 0.0 or previous <= 0.0:
        return math.nan
    return math.log(last / previous)


def _return_std(close: FloatArray, bars: int) -> float:
    if close.size <= bars:
        return math.nan
    window = close[-(bars + 1) :]
    if bool(np.any(window <= 0.0)):
        return math.nan
    returns = np.diff(np.log(window))
    return float(np.std(returns, ddof=0))


def _ratio_to_mean(values: FloatArray, bars: int) -> float:
    if values.size < bars:
        return math.nan
    window = values[-bars:]
    mean = float(np.mean(window))
    current = float(window[-1])
    if mean == 0.0:
        return 1.0 if current == 0.0 else math.nan
    return current / mean


def _candle_shape(
    open_: FloatArray, high: FloatArray, low: FloatArray, close: FloatArray
) -> tuple[float, float, float]:
    open_last = float(open_[-1])
    high_last = float(high[-1])
    low_last = float(low[-1])
    close_last = float(close[-1])
    span = high_last - low_last
    if span == 0.0:
        return 0.0, 0.0, 0.0
    body = (close_last - open_last) / span
    upper = (high_last - max(open_last, close_last)) / span
    lower = (min(open_last, close_last) - low_last) / span
    return body, upper, lower


def _pct(numerator: float, price: float) -> float:
    if not math.isfinite(numerator) or price <= 0.0:
        return math.nan
    return numerator / price


def _distance(price: float, ema: float) -> float:
    if not math.isfinite(ema) or price <= 0.0:
        return math.nan
    return (price - ema) / price


def _distance_delta(ema: float, previous: float, price: float) -> float:
    if not math.isfinite(ema) or not math.isfinite(previous) or price <= 0.0:
        return math.nan
    return (ema - previous) / price


def _spread(fast: float, slow: float, atr: float) -> float:
    if not math.isfinite(fast) or not math.isfinite(slow) or not math.isfinite(atr):
        return math.nan
    if atr == 0.0:
        return 0.0 if fast == slow else math.nan
    return (fast - slow) / atr


def _bar_return(bars: _HigherBars | None) -> float:
    if bars is None:
        return math.nan
    open_, _, _, close = bars
    if close.size == 0:
        return math.nan
    last_open = float(open_[-1])
    last_close = float(close[-1])
    if last_open <= 0.0 or last_close <= 0.0:
        return math.nan
    return math.log(last_close / last_open)


def _higher_atr_pct(bars: _HigherBars | None) -> float:
    if bars is None:
        return math.nan
    _, high, low, close = bars
    if close.size == 0:
        return math.nan
    return _pct(_atr_last(high, low, close, ATR_PERIOD), float(close[-1]))


def _higher_distance(bars: _HigherBars | None, span: int) -> float:
    if bars is None:
        return math.nan
    _, _, _, close = bars
    if close.size == 0:
        return math.nan
    _, ema = _ema_tail(close, span)
    return _distance(float(close[-1]), ema)


def _higher_slope(bars: _HigherBars | None, lag: int) -> float:
    if bars is None or lag < 2:
        return math.nan
    _, _, _, close = bars
    if close.size < lag:
        return math.nan
    last = float(close[-1])
    previous = float(close[-lag])
    if last <= 0.0 or previous <= 0.0:
        return math.nan
    return math.log(last / previous) / float(lag - 1)


def _clock_features(as_of: int) -> dict[str, float]:
    # Цілочисельний UTC. 1970-01-01 — четвер, тож зсув на 3 дає понеділок = 0.
    hour = (as_of // HOUR_MS) % 24
    dow = (as_of // D1_MS + 3) % 7
    hour_angle = 2.0 * math.pi * float(hour) / 24.0
    dow_angle = 2.0 * math.pi * float(dow) / 7.0
    return {
        "hour_sin": math.sin(hour_angle),
        "hour_cos": math.cos(hour_angle),
        "dow_sin": math.sin(dow_angle),
        "dow_cos": math.cos(dow_angle),
    }


def _funding_rate(times: IntArray, rates: FloatArray, as_of: int) -> float:
    # Остання подія строго до рішення: ставку з міткою рівно `T` live ще може не мати в БД.
    index = int(np.searchsorted(times, as_of, side="left")) - 1
    if index < 0 or as_of - int(times[index]) > FUNDING_MAX_STALE_MS:
        return math.nan
    return float(rates[index])


def _log_change(series: Mapping[int, float], end: int, bars: int) -> float:
    """log(значення останньої години перед `end` / значення за `bars` годин до неї)."""
    last = series.get(end - HOUR_MS)
    previous = series.get(end - HOUR_MS - bars * HOUR_MS)
    if last is None or previous is None or last <= 0.0 or previous <= 0.0:
        return math.nan
    return math.log(last / previous)


def _last_value(series: Mapping[int, float], end: int) -> float:
    value = series.get(end - HOUR_MS)
    if value is None:
        return math.nan
    return value


def _trailing_ratio(series: Mapping[int, float], end: int, bars: int) -> float:
    """Остання година перед `end` до середнього за `bars` годин, що закінчуються нею."""
    window: list[float] = []
    for stamp in range(end - bars * HOUR_MS, end, HOUR_MS):
        value = series.get(stamp)
        if value is None:
            return math.nan
        window.append(value)
    return _ratio_to_mean(np.array(window, dtype=np.float64), bars)


def _derivative_blocked(source: str, series: _Series, as_of: int) -> bool:
    """Чи перетинає дірка години, які читає фіча джерела. Решта вікна свічок на дериватив не впливає."""
    start, end = derivative_window(source, as_of)
    if window_has_gap(series.gaps.get(source, ()), start_ms=start, end_ms=end):
        return True
    if source == SOURCE_FUNDING:
        # Свіжість ставки перевіряє `_funding_rate`: без свіжої події фіча NaN і рядок не рахується.
        return False
    points = series.hourly[source]
    present = [stamp for stamp in range(start, end, HOUR_MS) if stamp in points]
    holes = find_gaps(present, start_ms=start, end_ms=end, step_ms=HOUR_MS)
    return window_has_gap(holes, start_ms=start, end_ms=end)


def _window_blocked(explicit: Sequence[GapRange], present: Iterable[int], start: int, end: int, step: int) -> bool:
    if window_has_gap(explicit, start_ms=start, end_ms=end):
        return True
    holes = find_gaps(present, start_ms=start, end_ms=end, step_ms=step)
    return window_has_gap(holes, start_ms=start, end_ms=end)


def _source_covers(data: SymbolData, source: str, start: int, end: int) -> bool:
    explicit = data.gaps.get(source, ())
    if source == SOURCE_FUNDING:
        holes = funding_gap_ranges((point.ts_ms for point in data.funding), start_ms=start, end_ms=end)
        return not window_has_gap(explicit, start_ms=start, end_ms=end) and not window_has_gap(
            holes, start_ms=start, end_ms=end
        )
    stamps = _source_timestamps(data, source)
    return not _window_blocked(explicit, stamps, start, end, HOUR_MS)


def _source_timestamps(data: SymbolData, source: str) -> list[int]:
    if source == SOURCE_OPEN_INTEREST:
        return [point.ts_ms for point in data.open_interest]
    if source == SOURCE_PREMIUM:
        return [point.ts_ms for point in data.premium]
    if source == SOURCE_LONG_SHORT:
        return [point.ts_ms for point in data.long_short]
    if source == SOURCE_LIQUIDATIONS:
        return [point.ts_ms for point in data.liquidations]
    raise ValueError(f"невідоме джерело: {source}")


def _resolve_enabled(
    series: Mapping[str, SymbolData],
    enabled_sources: Collection[str] | None,
    coverage_start_ms: int | None,
    coverage_end_ms: int | None,
) -> frozenset[str]:
    if (coverage_start_ms is None) != (coverage_end_ms is None):
        raise ValueError("межі навчального діапазону задаються разом")
    base = frozenset(DERIVATIVE_SOURCES) if enabled_sources is None else _require_sources(enabled_sources)
    if coverage_start_ms is None or coverage_end_ms is None:
        return base
    covered = sources_covering(series, start_ms=coverage_start_ms, end_ms=coverage_end_ms)
    return base & covered


def _require_sources(sources: Collection[str]) -> frozenset[str]:
    unknown = set(sources) - set(DERIVATIVE_SOURCES)
    if unknown:
        raise ValueError(f"невідоме джерело деривативів: {sorted(unknown)}")
    return frozenset(sources)


def _validate_gaps(gaps: Mapping[str, tuple[GapRange, ...]]) -> Mapping[str, tuple[GapRange, ...]]:
    for source in gaps:
        if source not in _GAP_KEYS:
            raise ValueError(f"невідоме джерело прогалин: {source}")
    return gaps


def _validate_candles(candles: Sequence[CandleBar]) -> tuple[CandleBar, ...]:
    ordered = tuple(sorted(candles, key=lambda bar: bar.open_time_ms))
    seen: set[int] = set()
    for bar in ordered:
        stamp = _require_ms("open_time_ms", bar.open_time_ms)
        if stamp % HOUR_MS != 0:
            raise ValueError("open_time_ms не вирівняний за 1h")
        if stamp in seen:
            raise ValueError("дубль open_time_ms")
        seen.add(stamp)
        open_ = _as_float("open", bar.open)
        high = _as_float("high", bar.high)
        low = _as_float("low", bar.low)
        close = _as_float("close", bar.close)
        volume = _as_float("volume", bar.volume)
        if low <= 0.0 or high < max(open_, close, low) or low > min(open_, close) or volume < 0.0:
            raise ValueError("свічка OHLC некоректна")
    return ordered


def _validate_timed(
    points: Sequence[TimedValue],
    name: str,
    *,
    minimum: float | None,
    aligned: bool = True,
) -> dict[int, float]:
    found: dict[int, float] = {}
    for point in points:
        stamp = _require_ms(f"{name}.ts_ms", point.ts_ms)
        if aligned and stamp % HOUR_MS != 0:
            raise ValueError(f"{name}: ts_ms не вирівняний за 1h")
        if stamp in found:
            raise ValueError(f"{name}: дубль ts_ms")
        value = _as_float(name, point.value)
        if minimum is not None and value < minimum:
            raise ValueError(f"{name}: значення менше за {minimum}")
        found[stamp] = value
    return found


def _validate_long_short(points: Sequence[LongShortPoint]) -> dict[int, float]:
    found: dict[int, float] = {}
    for point in points:
        stamp = _require_ms("long_short.ts_ms", point.ts_ms)
        if stamp % HOUR_MS != 0:
            raise ValueError("long_short: ts_ms не вирівняний за 1h")
        if stamp in found:
            raise ValueError("long_short: дубль ts_ms")
        buy = _as_float("buy_ratio", point.buy_ratio)
        sell = _as_float("sell_ratio", point.sell_ratio)
        if not 0.0 <= buy <= 1.0 or not 0.0 <= sell <= 1.0:
            raise ValueError("long_short: частка поза [0, 1]")
        found[stamp] = buy
    return found


def _validate_liquidations(bars: Sequence[LiquidationBar]) -> dict[int, float]:
    found: dict[int, float] = {}
    for bar in bars:
        stamp = _require_ms("liquidations.ts_ms", bar.ts_ms)
        if stamp % HOUR_MS != 0:
            raise ValueError("liquidations: ts_ms не вирівняний за 1h")
        if stamp in found:
            raise ValueError("liquidations: дубль ts_ms")
        long_notional = _as_float("long_notional", bar.long_notional)
        short_notional = _as_float("short_notional", bar.short_notional)
        if long_notional < 0.0 or short_notional < 0.0:
            raise ValueError("liquidations: номінал від'ємний")
        found[stamp] = long_notional + short_notional
    return found


def _aligned_bounds(start_ms: int, end_ms: int) -> tuple[int, int]:
    start = _require_ms("start_ms", start_ms)
    end = _require_ms("end_ms", end_ms)
    if end < start:
        raise ValueError("end_ms менший за start_ms")
    if start % HOUR_MS != 0 or end % HOUR_MS != 0:
        raise ValueError("межі навчального діапазону не вирівняні за 1h")
    return start, end


def _require_ms(name: str, value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} має бути невід'ємним int мс")
    return value


def _as_float(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} має бути числом")
    out = float(value)
    if not math.isfinite(out):
        raise ValueError(f"{name} має бути скінченним")
    return out


def _require_symbol(symbol: str) -> None:
    if _SYMBOL.fullmatch(symbol) is None:
        raise ValueError("символ не в канонічному форматі")


_T = TypeVar("_T")


def _drop_before(points: dict[int, _T], start: int) -> dict[int, _T]:
    return {stamp: value for stamp, value in points.items() if stamp >= start}
