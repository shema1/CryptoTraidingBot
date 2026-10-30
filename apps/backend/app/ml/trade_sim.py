"""Єдиний симулятор угоди: розмітка, бектест і очікувана вигода (ТЗ §5.2.1, §5.1, §5.2.4, §5А.5).

Бар'єри рахуються від `open[t]` і ATR, відомого на закриття `t-1` (те саме вікно, що `compute_features`).
Перевірка починається зі свічки `t`. SL дивиться на mark-свічки; якщо на вікно горизонту їх не вистачає —
на звичайні, і `stop_price_source = LAST` (журнал експерименту має це записати, T2.5). TP — завжди
за звичайними свічками. В одній свічці TP і SL разом — це SL. Геп через SL тригериться за mark, а виходить
за гіршим з `open` mark і last (market-вихід іде за last); геп через TP — за ціною TP, не за кращим `open`.
Time-stop — `open[t + horizon_bars]`, межа та сама, що `check_time_stop` (`opened_at_ms` = `open[t]`). Якщо
цей `open` уже за бар'єром, біржовий TP/SL спрацьовує раніше за market-закриття бота: діють ті самі правила
гепу (SL за mark цієї свічки, а без нього — за last і `stop_price_source = LAST`; обидва разом — SL).
Mark-свічки індексує `MarkSeries`, один раз на ряд.

Витрати — лише `CostModel`: вхід market (post-only у v1 вимкнено, ТЗ §6.6), вихід taker. Slippage SL і TP —
ноги `STOP` і `TARGET`. Time-stop — теж market, тож величина slippage як у входу (`ENTRY`), напрямок проти
закриття; окремої ноги time-stop у моделі витрат немає. Funding — `funding_over_hold` на інтервалі
`(open[t], open свічки виходу]`, кількість 1: `net_per_unit` лінійний за кількістю. Геп не в `costs.py`.
Обвал LONG нижче за slippage — вихід по 0, не пропуск: пропуск залежав би від результату угоди.

Майбутні свічки після `t-1` входять лише в шлях угоди (це і є мітка). Рівні бар'єрів і ATR їх не читають.
Мітка є лише коли шлях суцільний до `open[t + horizon]`: інакше біля дірки лишились би лише ранні TP.
Вихід не вигадується.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Hashable, Mapping, Sequence
from decimal import Decimal
from enum import StrEnum
from fractions import Fraction
from itertools import pairwise
from types import MappingProxyType
from typing import TypeVar

import numpy as np
from pydantic import Field, model_validator

from app.exchange.types import PositionSide
from app.ml.features import ATR_PERIOD, FEATURE_LOOKBACK_BARS, HOUR_MS, wilder_atr
from app.risk.costs import CostModel, FundingEvent, SlippageLeg, funding_over_hold
from app.risk.limits import check_time_stop
from app.risk.numbers import FrozenModel, fixed_context, require_decimal

_LabelT = TypeVar("_LabelT", bound=Hashable)


class ExitReason(StrEnum):
    """Чим закрилась угода. Meta-мітка 1 — лише `TAKE_PROFIT`."""

    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"
    TIME_STOP = "TIME_STOP"


class StopPriceSource(StrEnum):
    """Звідки брались ціни для SL. `LAST` — у журналі експерименту позначити fallback.

    `LAST` і тоді, коли вихід на гепі `open` свічки time-stop вирішено без mark цієї свічки.
    """

    MARK = "MARK"
    LAST = "LAST"


class DirectClass(StrEnum):
    """Ітерація 2. Одночасно LONG і SHORT не повертається: обидва TP стають `FLAT`."""

    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class SkipReason(StrEnum):
    """Чому кандидат не отримав мітку. Це не клас «не торгувати»."""

    INCOMPLETE_PATH = "INCOMPLETE_PATH"
    NO_ATR = "NO_ATR"
    UNUSABLE_EXIT = "UNUSABLE_EXIT"


class TradePathError(ValueError):
    """Мітки немає: шлях неповний, ATR невідомий або модель витрат не дає ціни виконання."""


class IncompletePath(TradePathError):
    """До time-stop не вистачає суцільних свічок, а бар'єр раніше не спрацював."""


class NoAtr(TradePathError):
    """ATR(t-1) немає: вікно фіч коротше за `FEATURE_LOOKBACK_BARS` або в ньому дірка."""


class UnusableExit(TradePathError):
    """Бар'єр або ціни виконання бар'єрів (`CostModel.execution`) не додатні, тож угоду не оцінюємо.

    Залежить лише від `open[t]`, ATR(t-1) і витрат, не від шляху: обвал після входу дає вихід по 0, а не пропуск.
    """


class BarrierInvariantError(RuntimeError):
    """LONG і SHORT разом досягли TP там, де геометрія бар'єрів це забороняє."""


class TargetParams(FrozenModel):
    """Множники бар'єрів і вертикальний бар'єр. Стартові значення — ТЗ §5А.5."""

    tp_atr_mult: Decimal = Field(default=Decimal("2"), gt=0)
    sl_atr_mult: Decimal = Field(default=Decimal("1"), gt=0)
    horizon_bars: int = 24

    @model_validator(mode="after")
    def _horizon(self) -> TargetParams:
        if type(self.horizon_bars) is not int or self.horizon_bars < 1:
            raise ValueError("horizon_bars має бути int ≥ 1")
        return self


class SimCandle(FrozenModel):
    """Одна закрита свічка симулятора. Ціни — Decimal, час — мс UTC."""

    open_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    @model_validator(mode="after")
    def _ohlc(self) -> SimCandle:
        if type(self.open_time_ms) is not int or self.open_time_ms < 0:
            raise ValueError("open_time_ms має бути невід'ємний int")
        require_decimal("open", self.open, positive=True)
        require_decimal("high", self.high, positive=True)
        require_decimal("low", self.low, positive=True)
        require_decimal("close", self.close, positive=True)
        if self.high < max(self.open, self.close, self.low):
            raise ValueError("high має бути ≥ open, close і low")
        if self.low > min(self.open, self.close):
            raise ValueError("low має бути ≤ open і close")
        return self


class MarkSeries:
    """Mark-свічки за `open_time_ms`, перевірені один раз; дубль часу — помилка.

    `label_meta` і `label_direct` будують його самі. Бектест будує один раз на ряд і передає в кожну
    `simulate_trade`: послідовність свічок там індексується заново на кожну угоду.
    """

    __slots__ = ("_by_time",)

    def __init__(self, candles: Sequence[SimCandle]) -> None:
        if not isinstance(candles, Sequence) or isinstance(candles, (str, bytes)):
            raise ValueError("mark-свічки мають бути послідовністю")
        found: dict[int, SimCandle] = {}
        for bar in candles:
            candle = _as_candle(bar)
            if candle.open_time_ms in found:
                raise ValueError("дубль mark-свічки")
            found[candle.open_time_ms] = candle
        self._by_time: Mapping[int, SimCandle] = MappingProxyType(found)

    def __len__(self) -> int:
        return len(self._by_time)

    def get(self, open_time_ms: int) -> SimCandle | None:
        return self._by_time.get(open_time_ms)


class SimulatedTrade(FrozenModel):
    """Одна угода на кількість 1. `net_per_unit` уже з комісіями і funding."""

    side: PositionSide
    entry_index: int
    exit_index: int
    entry_open_time_ms: int
    exit_open_time_ms: int
    exit_reason: ExitReason
    stop_price_source: StopPriceSource
    reference_entry: Decimal
    reference_stop: Decimal
    reference_take_profit: Decimal
    reference_exit: Decimal
    entry_fill: Decimal
    exit_fill: Decimal
    funding: Decimal
    loss_per_unit: Decimal
    net_per_unit: Decimal
    atr: Decimal

    @property
    def meta_label(self) -> int:
        """1 — закриття по TP; 0 — SL або time-stop (ТЗ §5.1). Знак PnL мітку не змінює."""
        if self.exit_reason is ExitReason.TAKE_PROFIT:
            return 1
        return 0


class EntryCandidate(FrozenModel):
    """Кандидат первинної стратегії: індекс свічки входу і напрямок. Стратегію обирає T2.7."""

    entry_index: int
    side: PositionSide
    symbol: str

    @model_validator(mode="after")
    def _fields(self) -> EntryCandidate:
        _require_index(self.entry_index)
        if type(self.side) is not PositionSide:
            raise ValueError("side має бути PositionSide")
        _require_symbol(self.symbol)
        return self


class SkippedEntry(FrozenModel):
    """Кандидат без мітки. У навчання його не підставляють як FLAT."""

    entry_index: int
    reason: SkipReason


class MetaSample(FrozenModel):
    """Meta-мітка одного кандидата. `label` збігається з `trade.meta_label`."""

    symbol: str
    label: int
    trade: SimulatedTrade

    @model_validator(mode="after")
    def _label(self) -> MetaSample:
        if self.label not in (0, 1) or self.label != self.trade.meta_label:
            raise ValueError("meta-мітка має бути 0 або 1 і збігатися з виходом угоди")
        return self


class DirectSample(FrozenModel):
    """Дві симуляції однієї свічки. `both_reached_tp` — обидва дійшли до TP, клас усе одно один."""

    symbol: str
    entry_index: int
    label: DirectClass
    both_reached_tp: bool
    long_trade: SimulatedTrade
    short_trade: SimulatedTrade

    @model_validator(mode="after")
    def _pair(self) -> DirectSample:
        if self.long_trade.side is not PositionSide.LONG or self.short_trade.side is not PositionSide.SHORT:
            raise ValueError("пара прямого прогнозу — LONG і SHORT")
        if self.long_trade.entry_index != self.entry_index or self.short_trade.entry_index != self.entry_index:
            raise ValueError("обидві угоди пари починаються на тій самій свічці")
        both = self.long_trade.meta_label == 1 and self.short_trade.meta_label == 1
        if self.both_reached_tp != both:
            raise ValueError("both_reached_tp не збігається з виходами пари")
        if self.label != _direct_class(self.long_trade.meta_label, self.short_trade.meta_label):
            raise ValueError("клас не збігається з виходами пари")
        return self


class MetaLabeling(FrozenModel):
    """Успішні meta-мітки і пропущені кандидати, кожен список у порядку входу."""

    samples: tuple[MetaSample, ...]
    skipped: tuple[SkippedEntry, ...]


class DirectLabeling(FrozenModel):
    """Прямі мітки і пропущені свічки, кожен список у порядку індексів."""

    samples: tuple[DirectSample, ...]
    skipped: tuple[SkippedEntry, ...]


class LabelSpan(FrozenModel):
    """Життя мітки: від відкриття свічки входу до відкриття свічки виходу, включно."""

    symbol: str
    entry_open_time_ms: int
    exit_open_time_ms: int
    bar_ms: int = HOUR_MS

    @model_validator(mode="after")
    def _span(self) -> LabelSpan:
        _require_symbol(self.symbol)
        _require_ms("entry_open_time_ms", self.entry_open_time_ms)
        _require_ms("exit_open_time_ms", self.exit_open_time_ms)
        _require_bar_ms(self.bar_ms)
        if self.exit_open_time_ms < self.entry_open_time_ms:
            raise ValueError("вихід мітки раніше за вхід")
        if (self.exit_open_time_ms - self.entry_open_time_ms) % self.bar_ms != 0:
            raise ValueError("час виходу мітки не на сітці барів")
        return self


@fixed_context
def atr_before_entry(candles: Sequence[SimCandle], entry_index: int, *, bar_ms: int = HOUR_MS) -> Decimal:
    """ATR(t-1): Wilder на `FEATURE_LOOKBACK_BARS` свічках строго до входу.

    Те саме вікно і та сама формула, що `atr_pct` у `compute_features` для рішення на `open` свічки
    `entry_index` (годинна сітка, `bar_ms` = година). Коротша історія або дірка — `NoAtr`: live на такому
    рядку фіч не має, тож і мітка не бере інший ATR.
    """
    _require_index(entry_index)
    _require_bar_ms(bar_ms)
    if entry_index >= len(candles):
        raise NoAtr("немає свічки входу")
    if entry_index < FEATURE_LOOKBACK_BARS:
        raise NoAtr("до входу менше свічок, ніж вікно ATR фіч")
    window = candles[entry_index - FEATURE_LOOKBACK_BARS : entry_index]
    for bar in window:
        _as_candle(bar)
    entry = _as_candle(candles[entry_index])
    if not _contiguous(window, bar_ms) or entry.open_time_ms - window[-1].open_time_ms != bar_ms:
        raise NoAtr("вікно ATR не суцільне або свічка входу не йде одразу після нього")
    high = np.array([float(bar.high) for bar in window], dtype=np.float64)
    low = np.array([float(bar.low) for bar in window], dtype=np.float64)
    close = np.array([float(bar.close) for bar in window], dtype=np.float64)
    value = wilder_atr(high, low, close, ATR_PERIOD)
    if not math.isfinite(value) or value <= 0.0:
        raise NoAtr("ATR не додатний")
    return Decimal(repr(value))


@fixed_context
def simulate_trade(
    candles: Sequence[SimCandle],
    *,
    entry_index: int,
    side: PositionSide,
    atr: Decimal,
    params: TargetParams | None = None,
    costs: CostModel | None = None,
    mark_candles: Sequence[SimCandle] | MarkSeries | None = None,
    funding: Sequence[FundingEvent] = (),
    bar_ms: int = HOUR_MS,
    entry_is_maker: bool = False,
) -> SimulatedTrade:
    """Закрити одну угоду. `atr` — вже ATR(t-1); ця функція його не перераховує і свічок після входу в нього не бере.

    Шлях має бути суцільним до свічки time-stop, навіть якщо бар'єр спрацював раніше: інакше біля кінця
    ряду і дірок у вибірці лишались би лише ранні TP. Немає цього шляху — `IncompletePath`.
    Багато угод на одному ряді — передавати `MarkSeries`, побудований один раз.
    """
    spec = params if params is not None else TargetParams()
    model = costs if costs is not None else CostModel()
    _require_index(entry_index)
    _require_bar_ms(bar_ms)
    if type(side) is not PositionSide:
        raise ValueError("side має бути PositionSide")
    if type(spec) is not TargetParams or type(model) is not CostModel:
        raise ValueError("params має бути TargetParams, costs — CostModel")
    if type(entry_is_maker) is not bool:
        raise ValueError("entry_is_maker має бути bool")
    if entry_is_maker:
        raise ValueError("post-only у версії 1 вимкнено: бектест моделює лише market-вхід")
    require_decimal("atr", atr, positive=True)
    if entry_index >= len(candles):
        raise IncompletePath("немає свічки входу")
    _require_path(candles, entry_index, spec.horizon_bars, bar_ms)
    entry = _as_candle(candles[entry_index])
    stop, take = _barriers(side, entry.open, atr, spec)
    priced = model.execution(
        side=side,
        entry=entry.open,
        stop=stop,
        take_profit=take,
        atr=atr,
        entry_is_maker=entry_is_maker,
    )
    if priced is None:
        raise UnusableExit("бар'єри не дають ціни виконання")
    priced_take = priced.take_profit_fill
    if priced_take is None:
        raise UnusableExit("бар'єри не дають ціни виконання")
    source, stop_marks = _stop_source(candles, entry_index, spec.horizon_bars, _mark_series(mark_candles))
    exit_index = entry_index + spec.horizon_bars
    previous = entry
    outcome: tuple[ExitReason, Decimal] | None = None
    for index in range(entry_index, min(len(candles), exit_index)):
        bar = _as_candle(candles[index])
        if index > entry_index and bar.open_time_ms - previous.open_time_ms != bar_ms:
            raise IncompletePath("дірка в траєкторії до бар'єра")
        previous = bar
        sl_bar = bar if stop_marks is None else _mark_bar(stop_marks, bar)
        outcome = _bar_outcome(side, last=bar, stop_bar=sl_bar, stop=stop, take=take)
        if outcome is not None:
            exit_index = index
            break
    if outcome is None:
        if exit_index >= len(candles):
            raise IncompletePath("немає свічки time-stop")
        exit_bar = _as_candle(candles[exit_index])
        if exit_bar.open_time_ms - previous.open_time_ms != bar_ms:
            raise IncompletePath("дірка перед time-stop")
        decision = check_time_stop(
            opened_at_ms=entry.open_time_ms,
            now_ms=exit_bar.open_time_ms,
            max_hold_bars=spec.horizon_bars,
            bar_ms=bar_ms,
        )
        if not decision.close:
            raise ValueError("time-stop не збігся з check_time_stop")
        # Біржовий TP/SL спрацьовує на гепі раніше, ніж бот закриє угоду по time-stop. Mark цієї свічки
        # не обов'язковий; без нього SL на її open перевіряється за last.
        time_stop_mark = None if stop_marks is None else stop_marks.get(exit_bar.open_time_ms)
        gap = _open_outcome(
            side,
            last=exit_bar,
            stop_bar=exit_bar if time_stop_mark is None else time_stop_mark,
            stop=stop,
            take=take,
        )
        if gap is None:
            outcome = (ExitReason.TIME_STOP, exit_bar.open)
        else:
            outcome = gap
            if stop_marks is not None and time_stop_mark is None:
                # Вихід вирішено без mark свічки time-stop: у журналі це fallback (ТЗ §5.2.1).
                source = StopPriceSource.LAST
    else:
        exit_bar = _as_candle(candles[exit_index])
    reason, reference = outcome
    return _close(
        model=model,
        side=side,
        entry_index=entry_index,
        exit_index=exit_index,
        entry=entry,
        exit_bar=exit_bar,
        reason=reason,
        source=source,
        stop=stop,
        take=take,
        reference_exit=reference,
        entry_fill=priced.entry_fill,
        barrier_exit=_barrier_fill(
            priced_stop=priced.stop_fill,
            priced_take=priced_take,
            reason=reason,
            reference=reference,
            stop=stop,
        ),
        atr=atr,
        funding=funding,
        entry_is_maker=entry_is_maker,
        loss_per_unit=priced.loss_per_unit,
    )


@fixed_context
def label_meta(
    candles: Sequence[SimCandle],
    candidates: Sequence[EntryCandidate],
    *,
    params: TargetParams | None = None,
    costs: CostModel | None = None,
    mark_candles: Sequence[SimCandle] | MarkSeries | None = None,
    funding: Sequence[FundingEvent] = (),
    atr_by_index: Mapping[int, Decimal] | None = None,
    bar_ms: int = HOUR_MS,
    entry_is_maker: bool = False,
) -> MetaLabeling:
    """Meta-мітки лише переданих кандидатів. Пропуск шляху не стає міткою 0."""
    marks = _mark_series(mark_candles)
    samples: list[MetaSample] = []
    skipped: list[SkippedEntry] = []
    for candidate in candidates:
        if type(candidate) is not EntryCandidate:
            raise ValueError("кандидат має бути EntryCandidate")
        if candidate.entry_index >= len(candles):
            skipped.append(SkippedEntry(entry_index=candidate.entry_index, reason=SkipReason.INCOMPLETE_PATH))
            continue
        try:
            atr = _atr_for(candles, candidate.entry_index, atr_by_index, bar_ms)
            trade = simulate_trade(
                candles,
                entry_index=candidate.entry_index,
                side=candidate.side,
                atr=atr,
                params=params,
                costs=costs,
                mark_candles=marks,
                funding=funding,
                bar_ms=bar_ms,
                entry_is_maker=entry_is_maker,
            )
        except TradePathError as exc:
            skipped.append(SkippedEntry(entry_index=candidate.entry_index, reason=_skip_reason(exc)))
            continue
        samples.append(MetaSample(symbol=candidate.symbol, label=trade.meta_label, trade=trade))
    return MetaLabeling(samples=tuple(samples), skipped=tuple(skipped))


@fixed_context
def label_direct(
    candles: Sequence[SimCandle],
    *,
    symbol: str,
    entry_indexes: Sequence[int] | None = None,
    params: TargetParams | None = None,
    costs: CostModel | None = None,
    mark_candles: Sequence[SimCandle] | MarkSeries | None = None,
    funding: Sequence[FundingEvent] = (),
    atr_by_index: Mapping[int, Decimal] | None = None,
    bar_ms: int = HOUR_MS,
    entry_is_maker: bool = False,
) -> DirectLabeling:
    """Дві симуляції на свічку: LONG, SHORT або FLAT.

    При `tp_atr_mult >= sl_atr_mult` і SL за тими самими свічками, що й TP, обидва TP неможливі
    (перший рух на відстань SL знімає протилежний бік раніше за його TP). Якщо це сталось —
    `BarrierInvariantError`, а не тиха мітка. Коли SL іде за іншими mark-свічками або TP ближче за SL,
    обидва TP можливі: клас `FLAT` і `both_reached_tp`, бо напрямок один.
    """
    _require_symbol(symbol)
    spec = params if params is not None else TargetParams()
    if type(spec) is not TargetParams:
        raise ValueError("params має бути TargetParams")
    indexes = range(len(candles)) if entry_indexes is None else entry_indexes
    marks = _mark_series(mark_candles)
    samples: list[DirectSample] = []
    skipped: list[SkippedEntry] = []
    for index in indexes:
        _require_index(index)
        if index >= len(candles):
            skipped.append(SkippedEntry(entry_index=index, reason=SkipReason.INCOMPLETE_PATH))
            continue
        try:
            atr = _atr_for(candles, index, atr_by_index, bar_ms)
            long_trade = simulate_trade(
                candles,
                entry_index=index,
                side=PositionSide.LONG,
                atr=atr,
                params=spec,
                costs=costs,
                mark_candles=marks,
                funding=funding,
                bar_ms=bar_ms,
                entry_is_maker=entry_is_maker,
            )
            short_trade = simulate_trade(
                candles,
                entry_index=index,
                side=PositionSide.SHORT,
                atr=atr,
                params=spec,
                costs=costs,
                mark_candles=marks,
                funding=funding,
                bar_ms=bar_ms,
                entry_is_maker=entry_is_maker,
            )
        except TradePathError as exc:
            skipped.append(SkippedEntry(entry_index=index, reason=_skip_reason(exc)))
            continue
        if long_trade.stop_price_source is not short_trade.stop_price_source:
            raise RuntimeError("LONG і SHORT однієї свічки мають спільне джерело SL")
        both = long_trade.meta_label == 1 and short_trade.meta_label == 1
        same_candles = long_trade.stop_price_source is StopPriceSource.LAST
        if both and same_candles and spec.tp_atr_mult >= spec.sl_atr_mult:
            raise BarrierInvariantError("LONG і SHORT одночасно досягли TP на одних свічках")
        samples.append(
            DirectSample(
                symbol=symbol,
                entry_index=index,
                label=_direct_class(long_trade.meta_label, short_trade.meta_label),
                both_reached_tp=both,
                long_trade=long_trade,
                short_trade=short_trade,
            )
        )
    return DirectLabeling(samples=tuple(samples), skipped=tuple(skipped))


def label_span(symbol: str, trade: SimulatedTrade, *, bar_ms: int = HOUR_MS) -> LabelSpan:
    """Інтервал, на якому мітка вже дивиться в майбутнє: від входу до свічки виходу включно."""
    if type(trade) is not SimulatedTrade:
        raise ValueError("trade має бути SimulatedTrade")
    return LabelSpan(
        symbol=symbol,
        entry_open_time_ms=trade.entry_open_time_ms,
        exit_open_time_ms=trade.exit_open_time_ms,
        bar_ms=bar_ms,
    )


def label_span_direct(symbol: str, sample: DirectSample, *, bar_ms: int = HOUR_MS) -> LabelSpan:
    """Життя прямої мітки — до пізнішого з двох виходів: раніше вона ще не відома.

    Коротший з двох виходів занизив би перекриття і зробив би embargo коротшим за горизонт мітки.
    """
    if type(sample) is not DirectSample:
        raise ValueError("sample має бути DirectSample")
    if sample.long_trade.entry_open_time_ms != sample.short_trade.entry_open_time_ms:
        raise ValueError("пара починається в різний час")
    return LabelSpan(
        symbol=symbol,
        entry_open_time_ms=sample.long_trade.entry_open_time_ms,
        exit_open_time_ms=max(sample.long_trade.exit_open_time_ms, sample.short_trade.exit_open_time_ms),
        bar_ms=bar_ms,
    )


@fixed_context
def uniqueness_weights(spans: Sequence[LabelSpan]) -> tuple[Decimal, ...]:
    """Середнє за життя мітки від `1 / кількість міток цього символу, активних на барі`.

    Мітки різних символів одна на одну не впливають, навіть якщо інтервали збігаються в часі.
    Одна мітка без сусідів має вагу 1. Порожній вхід — порожній результат.
    """
    return tuple(_to_decimal(weight) for weight in _uniqueness_fractions(spans))


@fixed_context
def sample_weights(spans: Sequence[LabelSpan], labels: Sequence[_LabelT]) -> tuple[Decimal, ...]:
    """`sample_weight` = вага унікальності × вага класу.

    Вага класу — `n / (k · count(class))` на всьому переданому наборі, не окремо по символу:
    модель вчиться на змішаних символах і бачить їхній спільний баланс. Унікальність лишається
    всередині символу. Добуток переводиться в Decimal один раз, щоб два округлення не розійшлися.
    Порожній набір — порожній результат.
    """
    if len(spans) != len(labels):
        raise ValueError("міток і інтервалів має бути порівну")
    if not spans:
        return ()
    balance = _class_balance(labels)
    return tuple(
        _to_decimal(weight * balance[label]) for weight, label in zip(_uniqueness_fractions(spans), labels, strict=True)
    )


def _uniqueness_fractions(spans: Sequence[LabelSpan]) -> tuple[Fraction, ...]:
    if not isinstance(spans, Sequence) or isinstance(spans, (str, bytes)):
        raise ValueError("spans має бути послідовністю")
    weights = [Fraction(0)] * len(spans)
    groups: dict[str, list[int]] = {}
    for index, span in enumerate(spans):
        if type(span) is not LabelSpan:
            raise ValueError("span має бути LabelSpan")
        groups.setdefault(span.symbol, []).append(index)
    for indexes in groups.values():
        bar_ms = spans[indexes[0]].bar_ms
        counts: Counter[int] = Counter()
        times = [_span_times(spans[index], bar_ms) for index in indexes]
        for stamps in times:
            for stamp in stamps:
                counts[stamp] += 1
        for index, stamps in zip(indexes, times, strict=True):
            total = sum((Fraction(1, counts[stamp]) for stamp in stamps), start=Fraction(0))
            weights[index] = total / len(stamps)
    return tuple(weights)


def _class_balance(labels: Sequence[_LabelT]) -> dict[_LabelT, Fraction]:
    counts: Counter[_LabelT] = Counter(labels)
    kinds = len(counts)
    total = len(labels)
    return {label: Fraction(total, kinds * count) for label, count in counts.items()}


def _to_decimal(value: Fraction) -> Decimal:
    return Decimal(value.numerator) / Decimal(value.denominator)


def _span_times(span: LabelSpan, bar_ms: int) -> range:
    if span.bar_ms != bar_ms:
        raise ValueError("у одного символу різний крок барів")
    return range(span.entry_open_time_ms, span.exit_open_time_ms + 1, span.bar_ms)


def _direct_class(long_label: int, short_label: int) -> DirectClass:
    if long_label == 1 and short_label == 0:
        return DirectClass.LONG
    if short_label == 1 and long_label == 0:
        return DirectClass.SHORT
    return DirectClass.FLAT


def _atr_for(
    candles: Sequence[SimCandle],
    entry_index: int,
    atr_by_index: Mapping[int, Decimal] | None,
    bar_ms: int,
) -> Decimal:
    if atr_by_index is None:
        return atr_before_entry(candles, entry_index, bar_ms=bar_ms)
    if entry_index not in atr_by_index:
        raise NoAtr("немає ATR для входу")
    atr = atr_by_index[entry_index]
    require_decimal("atr", atr, positive=True)
    return atr


def _skip_reason(exc: TradePathError) -> SkipReason:
    if type(exc) is IncompletePath:
        return SkipReason.INCOMPLETE_PATH
    if type(exc) is NoAtr:
        return SkipReason.NO_ATR
    if type(exc) is UnusableExit:
        return SkipReason.UNUSABLE_EXIT
    raise exc


def _barriers(side: PositionSide, entry: Decimal, atr: Decimal, params: TargetParams) -> tuple[Decimal, Decimal]:
    risk = params.sl_atr_mult * atr
    reward = params.tp_atr_mult * atr
    if side is PositionSide.LONG:
        stop, take = entry - risk, entry + reward
    else:
        stop, take = entry + risk, entry - reward
    if stop <= 0 or take <= 0:
        raise UnusableExit("бар'єр не додатний")
    return stop, take


def _bar_outcome(
    side: PositionSide,
    *,
    last: SimCandle,
    stop_bar: SimCandle,
    stop: Decimal,
    take: Decimal,
) -> tuple[ExitReason, Decimal] | None:
    """SL за `stop_bar` (mark або last), TP за `last`. Відкриття вже за SL — вихід за гіршим open."""
    long = side is PositionSide.LONG
    if _stop_gapped(side, stop_bar, stop):
        return ExitReason.STOP_LOSS, _gap_exit(side, last=last, stop_bar=stop_bar)
    stop_hit = (long and stop_bar.low <= stop) or (not long and stop_bar.high >= stop)
    take_hit = (long and last.high >= take) or (not long and last.low <= take)
    if stop_hit:
        return ExitReason.STOP_LOSS, stop
    if take_hit:
        return ExitReason.TAKE_PROFIT, take
    return None


def _open_outcome(
    side: PositionSide,
    *,
    last: SimCandle,
    stop_bar: SimCandle,
    stop: Decimal,
    take: Decimal,
) -> tuple[ExitReason, Decimal] | None:
    """Лише open свічки time-stop: SL-геп — за гіршим open, TP-геп — за ціною TP; обидва — SL."""
    if _stop_gapped(side, stop_bar, stop):
        return ExitReason.STOP_LOSS, _gap_exit(side, last=last, stop_bar=stop_bar)
    if (side is PositionSide.LONG and last.open >= take) or (side is PositionSide.SHORT and last.open <= take):
        return ExitReason.TAKE_PROFIT, take
    return None


def _stop_gapped(side: PositionSide, stop_bar: SimCandle, stop: Decimal) -> bool:
    if side is PositionSide.LONG:
        return stop_bar.open <= stop
    return stop_bar.open >= stop


def _gap_exit(side: PositionSide, *, last: SimCandle, stop_bar: SimCandle) -> Decimal:
    """SL-геп тригериться за `stop_bar`, а market-вихід виконується за last: гірший з двох open."""
    if side is PositionSide.LONG:
        return min(stop_bar.open, last.open)
    return max(stop_bar.open, last.open)


def _barrier_fill(
    *,
    priced_stop: Decimal,
    priced_take: Decimal,
    reason: ExitReason,
    reference: Decimal,
    stop: Decimal,
) -> Decimal | None:
    """Ціна виконання з `CostModel.execution`, коли вихід рівно на рівні бар'єра. Геп і time-stop — ні."""
    if reason is ExitReason.STOP_LOSS and reference == stop:
        return priced_stop
    if reason is ExitReason.TAKE_PROFIT:
        return priced_take
    return None


def _close(
    *,
    model: CostModel,
    side: PositionSide,
    entry_index: int,
    exit_index: int,
    entry: SimCandle,
    exit_bar: SimCandle,
    reason: ExitReason,
    source: StopPriceSource,
    stop: Decimal,
    take: Decimal,
    reference_exit: Decimal,
    entry_fill: Decimal,
    barrier_exit: Decimal | None,
    atr: Decimal,
    funding: Sequence[FundingEvent],
    entry_is_maker: bool,
    loss_per_unit: Decimal,
) -> SimulatedTrade:
    if barrier_exit is None:
        leg = SlippageLeg.STOP if reason is ExitReason.STOP_LOSS else SlippageLeg.ENTRY
        slip = model.slippage(price=reference_exit, atr=atr, leg=leg)
        # Обвал нижче за slippage — вихід по 0 (вся вартість позиції), а не пропуск: пропуск залежав би
        # від результату, і з вибірки випадали б саме найгірші збитки.
        exit_fill = max(reference_exit - slip, Decimal(0)) if side is PositionSide.LONG else reference_exit + slip
    else:
        exit_fill = barrier_exit
    if entry_fill <= 0:
        raise UnusableExit("ціна входу не додатна")
    paid = funding_over_hold(
        tuple(funding),
        side=side,
        qty=Decimal(1),
        opened_at_ms=entry.open_time_ms,
        closed_at_ms=exit_bar.open_time_ms,
    )
    fee_entry = model.entry_fee_rate(maker=entry_is_maker) * entry_fill
    fee_exit = model.taker_fee_rate * exit_fill
    gross = exit_fill - entry_fill if side is PositionSide.LONG else entry_fill - exit_fill
    return SimulatedTrade(
        side=side,
        entry_index=entry_index,
        exit_index=exit_index,
        entry_open_time_ms=entry.open_time_ms,
        exit_open_time_ms=exit_bar.open_time_ms,
        exit_reason=reason,
        stop_price_source=source,
        reference_entry=entry.open,
        reference_stop=stop,
        reference_take_profit=take,
        reference_exit=reference_exit,
        entry_fill=entry_fill,
        exit_fill=exit_fill,
        funding=paid,
        loss_per_unit=loss_per_unit,
        net_per_unit=gross - fee_entry - fee_exit + paid,
        atr=atr,
    )


def _stop_source(
    candles: Sequence[SimCandle],
    entry_index: int,
    horizon_bars: int,
    marks: MarkSeries | None,
) -> tuple[StopPriceSource, MarkSeries | None]:
    """MARK, лише якщо кожна свічка вікна `[t, t+horizon)` має mark. Інакше весь SL іде за last."""
    if marks is None or len(marks) == 0:
        return StopPriceSource.LAST, None
    end = min(len(candles), entry_index + horizon_bars)
    needed = [_as_candle(candles[index]).open_time_ms for index in range(entry_index, end)]
    if needed and all(marks.get(stamp) is not None for stamp in needed):
        return StopPriceSource.MARK, marks
    return StopPriceSource.LAST, None


def _mark_series(mark_candles: Sequence[SimCandle] | MarkSeries | None) -> MarkSeries | None:
    if mark_candles is None or isinstance(mark_candles, MarkSeries):
        return mark_candles
    return MarkSeries(mark_candles)


def _mark_bar(marks: MarkSeries, last: SimCandle) -> SimCandle:
    bar = marks.get(last.open_time_ms)
    if bar is None:
        raise RuntimeError("mark-свічка зникла після перевірки покриття")
    return bar


def _require_path(candles: Sequence[SimCandle], entry_index: int, horizon_bars: int, bar_ms: int) -> None:
    """Свічки `[t, t+horizon]` існують і йдуть рівно з кроком `bar_ms`."""
    exit_index = entry_index + horizon_bars
    if exit_index >= len(candles):
        raise IncompletePath("немає свічки time-stop")
    path = candles[entry_index : exit_index + 1]
    for bar in path:
        _as_candle(bar)
    if not _contiguous(path, bar_ms):
        raise IncompletePath("дірка в траєкторії")


def _contiguous(candles: Sequence[SimCandle], bar_ms: int) -> bool:
    return all(nxt.open_time_ms - prev.open_time_ms == bar_ms for prev, nxt in pairwise(candles))


def _as_candle(bar: object) -> SimCandle:
    if type(bar) is not SimCandle:
        raise ValueError("свічка має бути SimCandle")
    return bar


def _require_index(value: object) -> None:
    if type(value) is not int or isinstance(value, bool) or value < 0:
        raise ValueError("індекс свічки має бути невід'ємний int")


def _require_bar_ms(value: object) -> None:
    if type(value) is not int or isinstance(value, bool) or value < 1:
        raise ValueError("bar_ms має бути int ≥ 1")


def _require_ms(name: str, value: object) -> None:
    if type(value) is not int or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} має бути невід'ємний int")


def _require_symbol(value: object) -> None:
    if type(value) is not str or not value:
        raise ValueError("symbol має бути непорожній рядок")
