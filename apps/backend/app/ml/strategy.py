"""Бейзлайни і первинна стратегія (ТЗ §5.1, §5А.3, T2.7).

Рішення приймається на закритті свічки `t-1`, вхід — по `open[t]`. Той самий `primary_side`
викликає дослідження і live (T5.2): вікно EMA — останні `FEATURE_LOOKBACK_BARS` закриттів,
як у фічах (насіння — перше значення, alpha = 2/(span+1)). Ціни свічки входу правило не читає.

Параметри двох кандидатів зафіксовані тут і не перебираються в Optuna: перетин EMA(20)/EMA(50)
(ті самі періоди, що фічі) і пробій 24 годин (одна доба 1h). Який із них лишається — вирішує
`select_primary` лише за out-of-sample фолдами walk-forward. Hold-out у цей вибір не входить.

Опорні бейзлайни («завжди FLAT», «завжди LONG», випадковий вхід 1/24) теж проганяються
бектестером і записуються спробами лінії. У PBO вони не змагаються (рішення 0016): інакше
стратегія, яка завжди нижче обраної, занижує PBO. У N для DSR вони лишаються.
"""

from __future__ import annotations

import hashlib
from bisect import bisect_left
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pydantic import model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.exchange.types import Instrument, PositionSide
from app.ml.backtest import BacktestReport, EntrySignal, run_backtest
from app.ml.features import EMA_FAST, EMA_SLOW, FEATURE_LOOKBACK_BARS, HOUR_MS
from app.ml.trade_sim import EntryCandidate, MetaSample, SimCandle, TargetParams, label_meta
from app.ml.validation import (
    Fold,
    FoldRecord,
    LabelStats,
    RecordedTrial,
    ResearchSplit,
    SampleRow,
    SplitLeakError,
    SplitPurpose,
    is_test_row,
    is_train_row,
    label_diagnostics,
    record_trial,
    reject_holdout,
    rows_for,
)
from app.risk.costs import CostModel, FundingEvent
from app.risk.limits import RiskLimits
from app.risk.numbers import FrozenModel, require_decimal

# Один випадковий кандидат на добу 1h у середньому. Частка фіксована, не параметр пошуку.
RANDOM_ENTRY_NUMERATOR: int = 1
RANDOM_ENTRY_DENOMINATOR: int = 24
# Пробій попередніх 24 закритих годин. Доба 1h, не підібрана на історії.
BREAKOUT_LOOKBACK: int = 24
SELECTION_CRITERION: str = "median_fold_net_usd"

_BASELINE_NAMES = frozenset({"always_flat", "always_long", "random"})
_CANDIDATE_NAMES = frozenset({"ema_cross", "breakout"})
_NAMES = _BASELINE_NAMES | _CANDIDATE_NAMES


class StrategySpec(FrozenModel):
    """Одне правило. У бейзлайна параметрів немає; у кандидата — лише зафіксовані періоди."""

    name: str
    fast: int | None = None
    slow: int | None = None
    lookback: int | None = None

    @model_validator(mode="after")
    def _periods(self) -> StrategySpec:
        if self.name not in _NAMES:
            raise ValueError("невідоме правило")
        if self.name == "ema_cross":
            if (
                type(self.fast) is not int
                or type(self.slow) is not int
                or self.lookback is not None
                or not 2 <= self.fast < self.slow <= FEATURE_LOOKBACK_BARS
            ):
                raise ValueError("перетин EMA: 2 ≤ fast < slow ≤ вікно фіч, без lookback")
            return self
        if self.name == "breakout":
            if (
                self.fast is not None
                or self.slow is not None
                or type(self.lookback) is not int
                or not 2 <= self.lookback <= FEATURE_LOOKBACK_BARS
            ):
                raise ValueError("пробій: lookback від 2 до вікна фіч, без періодів EMA")
            return self
        if self.fast is not None or self.slow is not None or self.lookback is not None:
            raise ValueError("у бейзлайна немає параметрів")
        return self

    def metadata(self) -> dict[str, Any]:
        """Форма `primary_strategy` з ТЗ §5.4: ім'я і параметри."""
        if self.name == "ema_cross":
            return {"name": self.name, "params": {"fast": self.fast, "slow": self.slow}}
        if self.name == "breakout":
            return {"name": self.name, "params": {"lookback": self.lookback}}
        return {"name": self.name, "params": {}}

    def trial_params(self) -> dict[str, Any]:
        """Параметри спроби журналу. `role` — позначка; у PBO вирішує сам склад правила."""
        role = "baseline" if self.name in _BASELINE_NAMES else "candidate"
        return {"role": role, "strategy": self.metadata()}


# Модель створюється після класу: валідатор має вже існувати.
ALWAYS_FLAT = StrategySpec(name="always_flat")
ALWAYS_LONG = StrategySpec(name="always_long")
RANDOM_ENTRIES = StrategySpec(name="random")
EMA_CROSS = StrategySpec(name="ema_cross", fast=EMA_FAST, slow=EMA_SLOW)
BREAKOUT = StrategySpec(name="breakout", lookback=BREAKOUT_LOOKBACK)

REFERENCE_BASELINES: tuple[StrategySpec, ...] = (ALWAYS_FLAT, ALWAYS_LONG, RANDOM_ENTRIES)
PRIMARY_CANDIDATES: tuple[StrategySpec, ...] = (EMA_CROSS, BREAKOUT)
COMPARISON_SPECS: tuple[StrategySpec, ...] = REFERENCE_BASELINES + PRIMARY_CANDIDATES


def is_reference_baseline(params: Mapping[str, Any]) -> bool:
    """Чи це опорний бейзлайн із порожніми параметрами, а не пошук із тим самим ім'ям.

    Збігатися має весь `params`: `trial_params()` бейзлайна (як пише `record_strategy_comparison`)
    або його `metadata()` без обгортки. Зайвий ключ (гіперпараметри моделі, seed пошуку) —
    вже не бейзлайн. Прапорець `role` сам по собі спробу з PBO не виключає: інакше будь-яку
    вигідну спробу можна було б сховати від порівняння.
    """
    if not isinstance(params, Mapping):
        return False
    given = dict(params)
    return any(given in (spec.trial_params(), spec.metadata()) for spec in REFERENCE_BASELINES)


class SymbolHistory(FrozenModel):
    """Свічки одного символу для порівняння. Mark і funding — цього самого символу."""

    symbol: str
    candles: tuple[SimCandle, ...]
    instrument: Instrument
    mark_candles: tuple[SimCandle, ...] | None = None
    funding: tuple[FundingEvent, ...] = ()

    @model_validator(mode="after")
    def _own(self) -> SymbolHistory:
        if not self.candles:
            raise ValueError("історія символу порожня")
        if self.instrument.symbol != self.symbol:
            raise ValueError("інструмент іншого символу")
        previous = -1
        for bar in self.candles:
            if type(bar) is not SimCandle:
                raise ValueError("свічка має бути SimCandle")
            if bar.open_time_ms <= previous:
                raise ValueError("свічки мають йти за зростанням часу")
            previous = bar.open_time_ms
        if self.mark_candles is not None:
            previous = -1
            for bar in self.mark_candles:
                if type(bar) is not SimCandle or bar.open_time_ms <= previous:
                    raise ValueError("mark-свічки мають йти за зростанням часу")
                previous = bar.open_time_ms
        for event in self.funding:
            if type(event) is not FundingEvent:
                raise ValueError("подія funding має бути FundingEvent")
        return self


class FoldScore(FrozenModel):
    """Один фолд звіту. Рішення про правило — за `trading_net_usd`, не за пулом символів."""

    fold_index: int
    test_start_ms: int
    test_end_ms: int
    trading_net_usd: Decimal
    trading_trades: int
    trading_signals: int
    trading_profit_factor: Decimal | None
    all_symbols_net_usd: Decimal
    all_symbols_trades: int

    @model_validator(mode="after")
    def _numbers(self) -> FoldScore:
        require_decimal("trading_net_usd", self.trading_net_usd)
        require_decimal("all_symbols_net_usd", self.all_symbols_net_usd)
        if self.trading_profit_factor is not None:
            require_decimal("trading_profit_factor", self.trading_profit_factor)
        if self.trading_trades < 0 or self.trading_signals < 0 or self.all_symbols_trades < 0:
            raise ValueError("лічильник фолду від'ємний")
        if self.trading_signals < self.trading_trades:
            raise ValueError("сигналів менше, ніж угод")
        return self


class RuleReport(FrozenModel):
    """Прогони одного правила. `folds` порожній лише в тесті вибору; запис у журнал вимагає кожен фолд."""

    spec: StrategySpec
    scores: tuple[FoldScore, ...]
    folds: tuple[FoldRecord, ...] = ()

    @model_validator(mode="after")
    def _aligned(self) -> RuleReport:
        if not self.scores:
            raise ValueError("у правила немає фолдів")
        if self.folds and len(self.folds) != len(self.scores):
            raise ValueError("прогонів і оцінок різна кількість")
        return self

    @property
    def median_net_usd(self) -> Decimal:
        return _median(tuple(score.trading_net_usd for score in self.scores))

    @property
    def trading_trades(self) -> int:
        return sum(score.trading_trades for score in self.scores)


class StrategyComparison(FrozenModel):
    """Порівняння правил на одній розбивці. `oos_rows` — лише тест walk-forward, без hold-out."""

    split: ResearchSplit
    seed: int
    trading_symbol: str
    variants: tuple[RuleReport, ...]
    oos_rows: tuple[SampleRow, ...]

    @model_validator(mode="after")
    def _variants(self) -> StrategyComparison:
        names = [item.spec.metadata()["name"] for item in self.variants]
        if len(names) != len(set(names)) or not self.variants:
            raise ValueError("кожне правило в порівнянні рівно один раз")
        return self


class PrimarySelection(FrozenModel):
    """1–2 зафіксовані кандидати. `beats_flat` іде в тому самому порядку, що `chosen`."""

    chosen: tuple[StrategySpec, ...]
    beats_flat: tuple[bool, ...]

    @model_validator(mode="after")
    def _pair(self) -> PrimarySelection:
        if not self.chosen or len(self.chosen) > 2 or len(self.chosen) != len(self.beats_flat):
            raise ValueError("обрати можна одне або два правила")
        if any(spec not in PRIMARY_CANDIDATES for spec in self.chosen):
            raise ValueError("обирати можна лише зафіксованих кандидатів")
        return self

    def metadata(self) -> dict[str, Any]:
        """Те, що пишеться в `experiments.primary_strategy` експерименту вибору.

        У `model_metadata.primary_strategy` (ТЗ §5.4) іде не цей словник, а `StrategySpec.metadata()`
        одного правила, на кандидатах якого навчено модель: `{"name", "params"}`.
        """
        return {
            "selected": [spec.metadata() for spec in self.chosen],
            "criterion": SELECTION_CRITERION,
            "beats_flat": list(self.beats_flat),
        }


@dataclass(frozen=True, slots=True)
class _Hit:
    time_ms: int
    index: int
    side: PositionSide


@dataclass(frozen=True, slots=True)
class _Run:
    limits: RiskLimits
    initial_equity: Decimal
    maintenance_margin_rate: Decimal
    seed: int
    costs: CostModel
    params: TargetParams

    def report(
        self,
        history: SymbolHistory,
        path: tuple[SimCandle, ...],
        signals: tuple[EntrySignal, ...],
        *,
        path_from: int,
        path_to: int,
    ) -> BacktestReport:
        marks = None if history.mark_candles is None else _slice_time(history.mark_candles, path_from, path_to)
        # Ставка, відома до вікна, лишається: інакше слот funding усередині шляху не має історії.
        funding = tuple(event for event in history.funding if event.time_ms < path_to)
        return run_backtest(
            path,
            signals,
            symbol=history.symbol,
            instrument=history.instrument,
            limits=self.limits,
            initial_equity=self.initial_equity,
            maintenance_margin_rate=self.maintenance_margin_rate,
            seed=self.seed,
            costs=self.costs,
            params=self.params,
            mark_candles=marks,
            funding=funding,
        )


def primary_side(
    spec: StrategySpec,
    candles: Sequence[SimCandle],
    entry_index: int,
    *,
    seed: int = 0,
) -> PositionSide | None:
    """Напрямок входу на `open` свічки `entry_index`, або None. Дані — лише до цієї свічки.

    Live (T5.2) передає закриті свічки буфера і індекс свічки, що відкривається.
    `seed` впливає лише на випадковий бейзлайн і є функцією часу входу, не порядкового номера.
    """
    _spec(spec)
    _seed(seed)
    if type(entry_index) is not int or isinstance(entry_index, bool) or entry_index < 0:
        raise ValueError("entry_index має бути невід'ємний int")
    if entry_index >= len(candles):
        raise ValueError("немає свічки входу")
    if entry_index == 0:
        return None
    entry = _bar(candles, entry_index)
    previous = _bar(candles, entry_index - 1)
    if entry.open_time_ms - previous.open_time_ms != HOUR_MS:
        return None
    if spec.name == "always_flat":
        return None
    if spec.name == "always_long":
        return PositionSide.LONG
    if spec.name == "random":
        return _random_side(seed, entry.open_time_ms)
    if spec.name == "ema_cross":
        return _ema_cross(candles, entry_index, spec.fast, spec.slow)
    return _breakout(candles, entry_index, spec.lookback)


def primary_candidates(
    spec: StrategySpec,
    candles: Sequence[SimCandle],
    *,
    symbol: str,
    seed: int = 0,
) -> tuple[EntryCandidate, ...]:
    """Кандидати для `label_meta`, включно з хвостом ряду. Пропуск шляху міткою не стає."""
    _spec(spec)
    _seed(seed)
    if type(symbol) is not str or not symbol:
        raise ValueError("symbol має бути непорожній рядок")
    series = _series(candles)
    found: list[EntryCandidate] = []
    for index in range(1, len(series)):
        side = primary_side(spec, series, index, seed=seed)
        if side is not None:
            found.append(EntryCandidate(entry_index=index, side=side, symbol=symbol))
    return tuple(found)


def primary_signals(
    spec: StrategySpec,
    candles: Sequence[SimCandle],
    *,
    horizon_bars: int,
    seed: int = 0,
) -> tuple[EntrySignal, ...]:
    """Сигнали для `run_backtest`. Хвіст без `horizon_bars` свічок після входу відкинуто за індексом.

    `computed_at_ms` лишається порожнім: вік 0. Live ставить свій час розрахунку сам.
    """
    _bars(horizon_bars)
    series = _series(candles)
    last = len(series) - horizon_bars
    found: list[EntrySignal] = []
    for index in range(1, last):
        side = primary_side(spec, series, index, seed=seed)
        if side is not None:
            found.append(EntrySignal(entry_index=index, side=side))
    return tuple(found)


def executed_trade_count(
    history: SymbolHistory,
    *,
    horizon_bars: int,
    limits: RiskLimits,
    initial_equity: Decimal,
    maintenance_margin_rate: Decimal,
    seed: int,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    specs: Sequence[StrategySpec] = PRIMARY_CANDIDATES,
) -> Callable[[int, int], int]:
    """`count_trades` для `build_split`: найменше число угод серед `specs` на вікні з порожнім рахунком.

    За замовчуванням — мінімум зафіксованих кандидатів `PRIMARY_CANDIDATES` (рішення користувача,
    2026-11-02). Довжина фолду тоді не залежить від того, хто виграє порівняння, а фолд подовжується,
    коли угод бракує найрідшому кандидату (ТЗ §5.2.4). «Завжди LONG» для цього не годиться: угод у
    нього в рази більше, і подовження не спрацьовувало б. Кожне правило — окремий прогін, не зріз
    довшого. `horizon_bars` має збігатися з аргументом `build_split`: шлях вікна закінчується на
    `end + horizon` і не читає далі (hold-out починається ще на горизонт пізніше).
    """
    chosen = _specs(specs)
    run = _run(
        horizon_bars=horizon_bars,
        limits=limits,
        initial_equity=initial_equity,
        maintenance_margin_rate=maintenance_margin_rate,
        seed=seed,
        costs=costs,
        params=params,
    )
    series = history.candles

    def count(start: int, end: int) -> int:
        if type(start) is not int or type(end) is not int or start >= end:
            raise ValueError("вікно угод порожнє")
        path_to = end + horizon_bars * HOUR_MS
        return min(
            _window_report(
                history, series, spec, run, start_ms=start, end_ms=end, path_to=path_to, allowed=None
            ).metrics.n_trades
            for spec in chosen
        )

    return count


def compare_strategies(
    histories: Sequence[SymbolHistory],
    *,
    split: ResearchSplit,
    trading_symbol: str,
    limits: RiskLimits,
    initial_equity: Decimal,
    maintenance_margin_rate: Decimal,
    seed: int,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    specs: Sequence[StrategySpec] = COMPARISON_SPECS,
) -> StrategyComparison:
    """Прогнати кожне правило на тесті кожного фолду. Сигнали — лише `rows_for(PRIMARY_STRATEGY)`.

    Рахунок фолду порожній на його початку. Свічки до вікна — лише вікно правила й ATR; свічки
    після `test_end` — шлях угоди: до `test_end + horizon` (після дірки — `horizon` свічок за
    останнім сигналом) і не пізніше за hold-out. `atr_by_index` не передається.
    Стрес витрат сюди не входить: його рахує Gate G1 на вже обраних сигналах.
    """
    if split.bar_ms != HOUR_MS:
        raise ValueError("правила рахуються на свічках 1h")
    run = _run(
        horizon_bars=split.horizon_bars,
        limits=limits,
        initial_equity=initial_equity,
        maintenance_margin_rate=maintenance_margin_rate,
        seed=seed,
        costs=costs,
        params=params,
    )
    books = _histories(histories, trading_symbol)
    chosen = _specs(specs)
    oos: list[SampleRow] = []
    hits: dict[tuple[str, str], tuple[_Hit, ...]] = {}
    for spec in chosen:
        for history in books:
            found = _hits(spec, history.candles, seed=seed, end_ms=split.decision_end_ms)
            hits[(history.symbol, spec.name)] = found
            rows = tuple(SampleRow(symbol=history.symbol, time_ms=hit.time_ms, index=hit.index) for hit in found)
            reject_holdout(rows, split, purpose=SplitPurpose.PRIMARY_STRATEGY)
            oos.extend(rows_for(rows, split, SplitPurpose.PRIMARY_STRATEGY))
    reject_holdout(oos, split, purpose=SplitPurpose.PRIMARY_STRATEGY)
    allowed = {(row.symbol, row.time_ms) for row in oos}
    variants: list[RuleReport] = []
    for spec in chosen:
        scores: list[FoldScore] = []
        fold_records: list[FoldRecord] = []
        labeled = {history.symbol: _labeled(spec, history, split, run) for history in books}
        for fold in split.folds:
            reports: list[BacktestReport] = []
            for history in books:
                window_hits = tuple(
                    hit
                    for hit in hits[(history.symbol, spec.name)]
                    if fold.test_start_ms <= hit.time_ms < fold.test_end_ms and (history.symbol, hit.time_ms) in allowed
                )
                report = _window_report(
                    history,
                    history.candles,
                    spec,
                    run,
                    start_ms=fold.test_start_ms,
                    end_ms=fold.test_end_ms,
                    path_to=_fold_path_end(history.candles, window_hits, fold, split),
                    allowed=window_hits,
                )
                for trade in report.trades:
                    entered = trade.entry_open_time_ms
                    if entered >= split.holdout_from_ms or trade.exit_open_time_ms >= split.holdout_from_ms:
                        raise SplitLeakError("угода заходить у hold-out")
                    if (history.symbol, entered) not in allowed:
                        raise SplitLeakError("угода поза out-of-sample фолду")
                reports.append(report)
            record = FoldRecord(
                reports=tuple(reports),
                train_labels=_fold_labels(labeled, books, split, fold, train=True),
                test_labels=_fold_labels(labeled, books, split, fold, train=False),
            )
            fold_records.append(record)
            scores.append(_score(record, fold, trading_symbol))
        variants.append(RuleReport(spec=spec, scores=tuple(scores), folds=tuple(fold_records)))
    return StrategyComparison(
        split=split,
        seed=seed,
        trading_symbol=trading_symbol,
        variants=tuple(variants),
        oos_rows=tuple(oos),
    )


def select_primary(comparison: StrategyComparison) -> PrimarySelection:
    """Обрати 1–2 кандидати за медіаною чистого результату фолдів торгового символу.

    Поріг — медіана «завжди FLAT» цього ж прогону, не нуль, записаний у коді. Хто строго вищий
    за неї, той лишається (не більше двох, кращий першим). Якщо не вищий ніхто — лишається
    один найкращий, а `beats_flat` каже, що переваги над нулем угод немає. Бейзлайни і будь-яке
    правило поза `PRIMARY_CANDIDATES` у вибір не входять.
    """
    by_name = {item.spec.name: item for item in comparison.variants}
    flat = by_name.get(ALWAYS_FLAT.name)
    if flat is None:
        raise ValueError("порівняння без бейзлайну «завжди FLAT»")
    bar = flat.median_net_usd
    candidates = [by_name[spec.name] for spec in PRIMARY_CANDIDATES if spec.name in by_name]
    if not candidates:
        raise ValueError("у порівнянні немає зафіксованих кандидатів")
    ranked = sorted(candidates, key=lambda item: (-item.median_net_usd, -item.trading_trades, item.spec.name))
    beating = [item for item in ranked if item.median_net_usd > bar]
    picked = beating[:2] if beating else ranked[:1]
    return PrimarySelection(
        chosen=tuple(item.spec for item in picked),
        beats_flat=tuple(item.median_net_usd > bar for item in picked),
    )


async def record_strategy_comparison(
    session: AsyncSession,
    *,
    experiment_id: str,
    comparison: StrategyComparison,
) -> tuple[RecordedTrial, ...]:
    """Записати кожне правило порівняння, і те, що `select_primary` відкинув. Стан — `COMPLETE`.

    Викликати після `create_experiment` з тією самою розбивкою і тим самим seed. Опорний бейзлайн
    лишається в N; з PBO його прибирає `open_holdout_after_gate` за складом параметрів.
    """
    recorded: list[RecordedTrial] = []
    for variant in comparison.variants:
        if len(variant.folds) != len(comparison.split.folds):
            raise ValueError("у правила немає прогону кожного фолду")
        recorded.append(
            await record_trial(
                session,
                experiment_id=experiment_id,
                split=comparison.split,
                state="COMPLETE",
                params=variant.spec.trial_params(),
                folds=variant.folds,
            )
        )
    return tuple(recorded)


def _spec(spec: StrategySpec) -> None:
    if type(spec) is not StrategySpec:
        raise ValueError("правило має бути StrategySpec")


def _seed(seed: int) -> None:
    if type(seed) is not int or isinstance(seed, bool) or seed < 0:
        raise ValueError("seed має бути невід'ємний int")


def _bars(horizon_bars: int) -> None:
    if type(horizon_bars) is not int or isinstance(horizon_bars, bool) or horizon_bars < 1:
        raise ValueError("horizon_bars має бути int ≥ 1")


def _bar(candles: Sequence[SimCandle], index: int) -> SimCandle:
    bar = candles[index]
    if type(bar) is not SimCandle:
        raise ValueError("свічка має бути SimCandle")
    return bar


def _series(candles: Sequence[SimCandle]) -> tuple[SimCandle, ...]:
    if not isinstance(candles, Sequence) or isinstance(candles, (str, bytes)) or not candles:
        raise ValueError("свічки мають бути непорожньою послідовністю")
    series = tuple(candles)
    previous = -1
    for bar in series:
        if type(bar) is not SimCandle:
            raise ValueError("свічка має бути SimCandle")
        if bar.open_time_ms <= previous:
            raise ValueError("свічки мають йти за зростанням часу")
        previous = bar.open_time_ms
    return series


def _random_side(seed: int, open_time_ms: int) -> PositionSide | None:
    digest = hashlib.sha256(f"{seed}:{open_time_ms}".encode()).digest()
    if int.from_bytes(digest[:8], "big") % RANDOM_ENTRY_DENOMINATOR >= RANDOM_ENTRY_NUMERATOR:
        return None
    if digest[8] % 2 == 0:
        return PositionSide.LONG
    return PositionSide.SHORT


def _bounded(end: int, length: int) -> tuple[int, int]:
    return max(0, end - length), end


def _contiguous(candles: Sequence[SimCandle], start: int, end: int) -> bool:
    for index in range(start + 1, end):
        if candles[index].open_time_ms - candles[index - 1].open_time_ms != HOUR_MS:
            return False
    return True


def _ema_last(candles: Sequence[SimCandle], start: int, end: int, span: int) -> float | None:
    if end - start < span:
        return None
    alpha = 2.0 / (span + 1.0)
    decay = 1.0 - alpha
    ema = float(candles[start].close)
    for index in range(start + 1, end):
        ema = alpha * float(candles[index].close) + decay * ema
    return ema


def _ema_cross(
    candles: Sequence[SimCandle],
    entry_index: int,
    fast: int | None,
    slow: int | None,
) -> PositionSide | None:
    if type(fast) is not int or type(slow) is not int:
        raise ValueError("у перетину EMA немає періодів")
    now_start, now_end = _bounded(entry_index, FEATURE_LOOKBACK_BARS)
    prev_start, prev_end = _bounded(entry_index - 1, FEATURE_LOOKBACK_BARS)
    if not _contiguous(candles, prev_start, entry_index):
        return None
    now_fast = _ema_last(candles, now_start, now_end, fast)
    now_slow = _ema_last(candles, now_start, now_end, slow)
    prev_fast = _ema_last(candles, prev_start, prev_end, fast)
    prev_slow = _ema_last(candles, prev_start, prev_end, slow)
    if now_fast is None or now_slow is None or prev_fast is None or prev_slow is None:
        return None
    if prev_fast <= prev_slow and now_fast > now_slow:
        return PositionSide.LONG
    if prev_fast >= prev_slow and now_fast < now_slow:
        return PositionSide.SHORT
    return None


def _breakout(candles: Sequence[SimCandle], entry_index: int, lookback: int | None) -> PositionSide | None:
    if type(lookback) is not int:
        raise ValueError("у пробою немає вікна")
    decision = entry_index - 1
    channel_start = decision - lookback
    if channel_start < 0 or not _contiguous(candles, channel_start, entry_index):
        return None
    highest = candles[channel_start].high
    lowest = candles[channel_start].low
    for index in range(channel_start + 1, decision):
        if candles[index].high > highest:
            highest = candles[index].high
        if candles[index].low < lowest:
            lowest = candles[index].low
    close = candles[decision].close
    if close > highest:
        return PositionSide.LONG
    if close < lowest:
        return PositionSide.SHORT
    return None


def _hits(
    spec: StrategySpec,
    candles: Sequence[SimCandle],
    *,
    seed: int,
    end_ms: int,
    start_ms: int = 0,
) -> tuple[_Hit, ...]:
    """Сигнали на `[start_ms, end_ms)`. Кожен — з повного ряду; початок вікна лише економить прохід."""
    found: list[_Hit] = []
    first = max(1, bisect_left(candles, start_ms, key=_open_time))
    for index in range(first, len(candles)):
        time_ms = candles[index].open_time_ms
        if time_ms >= end_ms:
            break
        side = primary_side(spec, candles, index, seed=seed)
        if side is not None:
            found.append(_Hit(time_ms=time_ms, index=index, side=side))
    return tuple(found)


def _run(
    *,
    horizon_bars: int,
    limits: RiskLimits,
    initial_equity: Decimal,
    maintenance_margin_rate: Decimal,
    seed: int,
    costs: CostModel | None,
    params: TargetParams | None,
) -> _Run:
    _bars(horizon_bars)
    _seed(seed)
    if type(limits) is not RiskLimits:
        raise ValueError("limits має бути RiskLimits")
    require_decimal("initial_equity", initial_equity, positive=True)
    require_decimal("maintenance_margin_rate", maintenance_margin_rate, positive=True)
    model = costs if costs is not None else CostModel()
    if type(model) is not CostModel:
        raise ValueError("costs має бути CostModel")
    spec = params if params is not None else TargetParams()
    if type(spec) is not TargetParams:
        raise ValueError("params має бути TargetParams")
    if spec.horizon_bars != horizon_bars:
        raise ValueError("горизонт таргета і вікна різний")
    return _Run(
        limits=limits,
        initial_equity=initial_equity,
        maintenance_margin_rate=maintenance_margin_rate,
        seed=seed,
        costs=model,
        params=spec,
    )


def _histories(histories: Sequence[SymbolHistory], trading_symbol: str) -> tuple[SymbolHistory, ...]:
    if not isinstance(histories, Sequence) or isinstance(histories, (str, bytes)) or not histories:
        raise ValueError("потрібна історія хоча б одного символу")
    books = tuple(histories)
    symbols = [book.symbol for book in books]
    if len(symbols) != len(set(symbols)):
        raise ValueError("символ повторюється")
    if trading_symbol not in symbols:
        raise ValueError("немає історії торгового символу")
    for book in books:
        if type(book) is not SymbolHistory:
            raise ValueError("історія має бути SymbolHistory")
    return books


def _specs(specs: Sequence[StrategySpec]) -> tuple[StrategySpec, ...]:
    if not isinstance(specs, Sequence) or isinstance(specs, (str, bytes)) or not specs:
        raise ValueError("потрібне хоча б одне правило")
    chosen = tuple(specs)
    for spec in chosen:
        _spec(spec)
    if len({spec.name for spec in chosen}) != len(chosen):
        raise ValueError("правило повторюється")
    return chosen


def _open_time(bar: SimCandle) -> int:
    return bar.open_time_ms


def _slice_time(candles: Sequence[SimCandle], start: int, end: int) -> tuple[SimCandle, ...]:
    return tuple(bar for bar in candles if start <= bar.open_time_ms < end)


def _slice_funding(events: Sequence[FundingEvent], start: int, end: int) -> tuple[FundingEvent, ...]:
    return tuple(event for event in events if start <= event.time_ms < end)


def _fold_path_end(
    candles: Sequence[SimCandle],
    window_hits: tuple[_Hit, ...],
    fold: Fold,
    split: ResearchSplit,
) -> int:
    """Кінець шляху фолду: `test_end + horizon`; після дірки — `horizon` свічок за останнім сигналом.

    Угода закривається не пізніше `horizon_bars` свічок після входу, тож пізніші свічки угод не
    змінюють, але розводили б Sharpe барів фолду. Подовження за індексом не дає дірці після
    останнього сигналу викинути його з прогону. Далі за початок hold-out шлях не йде.
    """
    end = fold.test_end_ms + split.horizon_bars * HOUR_MS
    if window_hits:
        last = window_hits[-1].index + split.horizon_bars
        end = max(end, candles[last].open_time_ms + HOUR_MS) if last < len(candles) else split.holdout_from_ms
    return min(end, split.holdout_from_ms)


def _window_report(
    history: SymbolHistory,
    candles: Sequence[SimCandle],
    spec: StrategySpec,
    run: _Run,
    *,
    start_ms: int,
    end_ms: int,
    path_to: int,
    allowed: tuple[_Hit, ...] | None,
) -> BacktestReport:
    path_from = start_ms - FEATURE_LOOKBACK_BARS * HOUR_MS
    path = _slice_time(candles, path_from, path_to)
    if not path:
        raise ValueError("немає свічок вікна")
    index_of = {bar.open_time_ms: index for index, bar in enumerate(path)}
    if allowed is None:
        allowed = _hits(spec, candles, seed=run.seed, end_ms=end_ms, start_ms=start_ms)
    signals: list[EntrySignal] = []
    for hit in allowed:
        index = index_of.get(hit.time_ms)
        if index is None:
            raise ValueError("свічки рішення немає у вікні")
        if index + run.params.horizon_bars >= len(path):
            continue
        signals.append(EntrySignal(entry_index=index, side=hit.side))
    return run.report(history, path, tuple(signals), path_from=path_from, path_to=path_to)


def _labeled(
    spec: StrategySpec,
    history: SymbolHistory,
    split: ResearchSplit,
    run: _Run,
) -> dict[int, MetaSample]:
    """Meta-мітки кандидатів до hold-out. Той самий горизонт і витрати, що бектест. Пропуски не входять."""
    prefix = tuple(bar for bar in history.candles if bar.open_time_ms < split.holdout_from_ms)
    candidates = [
        candidate
        for candidate in primary_candidates(spec, prefix, symbol=history.symbol, seed=run.seed)
        if prefix[candidate.entry_index].open_time_ms < split.decision_end_ms
    ]
    marks = None if history.mark_candles is None else _slice_time(history.mark_candles, 0, split.holdout_from_ms)
    labeled = label_meta(
        prefix,
        candidates,
        params=run.params,
        costs=run.costs,
        mark_candles=marks,
        funding=_slice_funding(history.funding, 0, split.holdout_from_ms),
    )
    return {sample.trade.entry_index: sample for sample in labeled.samples}


def _fold_labels(
    labeled: Mapping[str, Mapping[int, Any]],
    books: Sequence[SymbolHistory],
    split: ResearchSplit,
    fold: Fold,
    *,
    train: bool,
) -> LabelStats:
    samples: list[Any] = []
    keep = is_train_row if train else is_test_row
    for history in books:
        candles = history.candles
        for index, sample in labeled[history.symbol].items():
            row = SampleRow(symbol=history.symbol, time_ms=candles[index].open_time_ms, index=index)
            if keep(row, split, fold.index):
                samples.append(sample)
    return label_diagnostics(samples)


def _score(record: FoldRecord, fold: Fold, trading_symbol: str) -> FoldScore:
    trading = next(report for report in record.reports if report.symbol == trading_symbol)
    trades = [trade for report in record.reports for trade in report.trades]
    net = sum((trade.net_usd for trade in trades), Decimal(0))
    return FoldScore(
        fold_index=fold.index,
        test_start_ms=fold.test_start_ms,
        test_end_ms=fold.test_end_ms,
        trading_net_usd=trading.metrics.total_net_usd,
        trading_trades=trading.metrics.n_trades,
        trading_signals=trading.n_signals,
        trading_profit_factor=trading.metrics.profit_factor,
        all_symbols_net_usd=net,
        all_symbols_trades=len(trades),
    )


def _median(values: Sequence[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("немає фолдів для медіани")
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2
