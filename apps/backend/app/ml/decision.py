"""Калібрування ймовірності і правило входу за очікуваною вигодою (ТЗ §5.5.1, §5А.8, T2.9).

Бустинг не повертає справжню ймовірність. Калібратор (isotonic або Platt) вчиться лише на
розмічених out-of-sample рядках walk-forward: `rows_for(CALIBRATION)` і `reject_holdout`.
Імовірність рядка має поставити модель того фолду, в чиєму тесті цей рядок лежить — тобто модель,
яка його не вчила. Щоб вибір методу і порогів не підганявся під ті самі мітки, калібратор кожного
фолду вчиться на інших фолдах (out-of-fold). Уже після вибору порогів фінальний калібратор
донавчається на всіх цих рядках: його викликає live.

```text
EV   = p · TP_net − (1 − p) · SL_net
край = EV / ATR
```

`TP_net` — `PriceExecution.tp_net_per_unit`, `SL_net` — `loss_per_unit`. Обидва з `CostModel`
і бар'єрів `barrier_prices` (ті самі витрати, що розмір позиції). Time-stop у цій формулі —
повний SL: meta-мітка time-stop уже 0, і член `(1−p)·SL_net` не замінюється на `net_per_unit`
фактичного виходу. Funding у формулу ТЗ не входить.

Вхід лише коли калібрована `p ≥ min_confidence` і край `≥ min_edge_atr` (обидві межі включно).
Поріг до калібрування `META_MIN_PROBABILITY` / `keep` лишається окремим. Пропущений шлях
(`true_label is None`) у навчання калібратора і у вибір порогів не йде і міткою 0 не стає.
"""

from __future__ import annotations

import math
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Annotated, Any

import numpy as np
from pydantic import Field, model_validator
from sklearn.exceptions import ConvergenceWarning
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression

from app.exchange.types import PositionSide
from app.ml.trade_sim import TargetParams, UnusableExit, barrier_prices
from app.ml.validation import (
    ResearchSplit,
    SampleRow,
    SplitLeakError,
    SplitPurpose,
    is_test_row,
    reject_holdout,
    rows_for,
)
from app.risk.costs import CostModel, PriceExecution
from app.risk.numbers import FrozenModel, fixed_context, require_decimal

DEFAULT_MIN_EDGE_ATR: Decimal = Decimal("0.10")
DEFAULT_MIN_CONFIDENCE: Decimal = Decimal("0.40")
# Сітка фіксована: це один рецепт, не окремі спроби Optuna. Дефолти ТЗ §5.4 усередині сітки.
EDGE_GRID: tuple[Decimal, ...] = (
    Decimal("0"),
    Decimal("0.05"),
    Decimal("0.10"),
    Decimal("0.15"),
    Decimal("0.20"),
    Decimal("0.30"),
)
CONFIDENCE_GRID: tuple[Decimal, ...] = (
    Decimal("0.30"),
    Decimal("0.35"),
    Decimal("0.40"),
    Decimal("0.45"),
    Decimal("0.50"),
    Decimal("0.55"),
    Decimal("0.60"),
    Decimal("0.70"),
)
THRESHOLD_CRITERION: str = "median_fold_edge_atr"
RELIABILITY_BIN_COUNT: int = 10
ENTER: str = "ENTER"
SKIP: str = "SKIP"
REASON_CONFIDENCE_BELOW_MIN: str = "CONFIDENCE_BELOW_MIN"
REASON_EDGE_BELOW_MIN: str = "EDGE_BELOW_MIN"
REASON_UNUSABLE_EXIT: str = "UNUSABLE_EXIT"

_METHODS: tuple[str, ...] = ("isotonic", "platt")
_SKIP_REASONS: tuple[str, ...] = (
    REASON_CONFIDENCE_BELOW_MIN,
    REASON_EDGE_BELOW_MIN,
    REASON_UNUSABLE_EXIT,
)
_PROBABILITY_QUANTUM: Decimal = Decimal("0.0000000001")
# Логіт Platt не визначений у 0 і 1. Зріз лише всередині калібратора, сира ймовірність не змінюється.
_PLATT_CLIP: float = 1e-6
_PLATT_MAX_ITER: int = 2000


class CalibrationError(ValueError):
    """Калібрування або поріг не зібрати чесно. Такий рецепт не дає входу."""


class DecisionRule(FrozenModel):
    """Поріг входу. Числа — дефолт ТЗ §5.5.1 і приклад `decision_rule` у §5.4."""

    min_edge_atr: Annotated[Decimal, Field(ge=0)] = DEFAULT_MIN_EDGE_ATR
    min_confidence: Annotated[Decimal, Field(ge=0, le=1)] = DEFAULT_MIN_CONFIDENCE

    @model_validator(mode="after")
    def _bounds(self) -> DecisionRule:
        require_decimal("min_edge_atr", self.min_edge_atr, non_negative=True)
        require_decimal("min_confidence", self.min_confidence)
        if self.min_confidence > 1:
            raise ValueError("min_confidence поза [0, 1]")
        return self

    def metadata(self) -> dict[str, str]:
        """`decision_rule` для метаданих моделі. Рядки Decimal, без float."""
        return {
            "min_edge_atr": format(self.min_edge_atr, "f"),
            "min_confidence": format(self.min_confidence, "f"),
        }


class EntryDecision(FrozenModel):
    """Рішення одного кандидата. `probability` вже калібрована. `expected_edge_atr` — у частках ATR.

    `ENTER` без причини. `SKIP` через поріг зберігає край (його пише журнал). Непридатний бар'єр
    краю не має.
    """

    decision: str
    probability: Decimal
    expected_edge_atr: Decimal | None
    reason: str | None

    @model_validator(mode="after")
    def _pair(self) -> EntryDecision:
        _probability(self.probability)
        if self.decision == ENTER:
            if self.reason is not None or self.expected_edge_atr is None:
                raise ValueError("ENTER має край і не має причини")
            require_decimal("expected_edge_atr", self.expected_edge_atr)
            return self
        if self.decision != SKIP:
            raise ValueError("рішення — ENTER або SKIP")
        if self.reason not in _SKIP_REASONS:
            raise ValueError("невідома причина SKIP")
        if self.reason == REASON_UNUSABLE_EXIT:
            if self.expected_edge_atr is not None:
                raise ValueError("непридатний вихід не має краю")
            return self
        if self.expected_edge_atr is None:
            raise ValueError("відмова за порогом має край")
        require_decimal("expected_edge_atr", self.expected_edge_atr)
        return self


class OosObservation(FrozenModel):
    """Кандидат, якого оцінила модель фолду, що цей рядок не бачила в train.

    `raw_probability` — ще не калібрована. `true_label` — `None`, якщо шлях після входу пропущено:
    такий рядок не вчиться і не обирає поріг. `entry` — `open[t]`, `atr` — ATR(t−1).
    """

    symbol: str
    time_ms: int
    index: int
    fold_index: int
    side: PositionSide
    raw_probability: Decimal
    true_label: int | None
    entry: Decimal
    atr: Decimal

    @model_validator(mode="after")
    def _row(self) -> OosObservation:
        SampleRow(symbol=self.symbol, time_ms=self.time_ms, index=self.index)
        if type(self.fold_index) is not int or self.fold_index < 0:
            raise ValueError("fold_index має бути невід'ємний int")
        if type(self.side) is not PositionSide:
            raise ValueError("side має бути PositionSide")
        _probability(self.raw_probability)
        if type(self.true_label) is not int and self.true_label is not None:
            raise ValueError("мітка — 0, 1 або відсутня")
        if self.true_label not in (None, 0, 1):
            raise ValueError("мітка — 0, 1 або відсутня")
        require_decimal("entry", self.entry, positive=True)
        require_decimal("atr", self.atr, positive=True)
        return self


class ReliabilityBin(FrozenModel):
    """Один бін діаграми надійності. Порожній бін лишається: видно, де модель мовчить."""

    lower: Decimal
    upper: Decimal
    n: int
    mean_predicted: Decimal | None
    fraction_positive: Decimal | None

    @model_validator(mode="after")
    def _bin(self) -> ReliabilityBin:
        require_decimal("lower", self.lower, non_negative=True)
        require_decimal("upper", self.upper, positive=True)
        if self.upper <= self.lower or type(self.n) is not int or self.n < 0:
            raise ValueError("бін діаграми зіпсований")
        empty = self.n == 0
        if empty != (self.mean_predicted is None) or empty != (self.fraction_positive is None):
            raise ValueError("порожній бін не має середніх, непорожній має")
        if self.mean_predicted is not None:
            _probability(self.mean_predicted)
        if self.fraction_positive is not None:
            _probability(self.fraction_positive)
        return self


class ReliabilityDiagram(FrozenModel):
    """Коли модель каже 0.45, частка TP у цьому біні має бути близько 0.45. Біни рівні, останній включно з 1."""

    bins: tuple[ReliabilityBin, ...]
    method: str | None = None

    @model_validator(mode="after")
    def _shape(self) -> ReliabilityDiagram:
        if len(self.bins) != RELIABILITY_BIN_COUNT:
            raise ValueError("діаграма має фіксовану кількість бінів")
        if self.method not in (None, *_METHODS):
            raise ValueError("метод калібрування — isotonic або platt")
        return self


class ThresholdSelection(FrozenModel):
    """Обраний поріг. `beats_abstain` — медіана краю фолдів строго більша за нуль (за відмову від угоди)."""

    rule: DecisionRule
    median_fold_edge_atr: Decimal
    n_taken: int
    n_folds: int
    beats_abstain: bool
    criterion: str = THRESHOLD_CRITERION

    @model_validator(mode="after")
    def _selection(self) -> ThresholdSelection:
        require_decimal("median_fold_edge_atr", self.median_fold_edge_atr)
        if self.criterion != THRESHOLD_CRITERION:
            raise ValueError("критерій порогу фіксований")
        if type(self.n_taken) is not int or type(self.n_folds) is not int or self.n_taken < 0 or self.n_folds < 1:
            raise ValueError("лічильники порогу зіпсовані")
        if self.beats_abstain != (self.median_fold_edge_atr > 0):
            raise ValueError("позначка переваги не збігається з медіаною")
        return self


@dataclass(frozen=True, slots=True)
class ProbabilityCalibrator:
    """Навчений калібратор. `estimator` — межа sklearn; ймовірності назовні — Decimal."""

    method: str
    estimator: Any

    def calibrate(self, probability: Decimal) -> Decimal:
        """Одна сира ймовірність → калібрована. Та сама функція в дослідженні і в live."""
        return self._apply((probability,))[0]

    def payload(self) -> dict[str, Any]:
        """Числа калібратора рядками hex. У файл артефакту, без pickle і без float у JSON."""
        if self.method == "isotonic":
            estimator = self.estimator
            return {
                "method": "isotonic",
                "x_thresholds": [_float_hex(value) for value in estimator.X_thresholds_],
                "y_thresholds": [_float_hex(value) for value in estimator.y_thresholds_],
            }
        if self.method == "platt":
            estimator = self.estimator
            return {
                "method": "platt",
                "coef": _float_hex(float(estimator.coef_[0, 0])),
                "intercept": _float_hex(float(estimator.intercept_[0])),
            }
        raise RuntimeError("невідомий метод калібрування")

    # Округлення до `_PROBABILITY_QUANTUM` не залежить від контексту потоку (CCXT лишає HALF_UP).
    @fixed_context
    def _apply(self, probabilities: Sequence[Decimal]) -> tuple[Decimal, ...]:
        if self.method not in _METHODS:
            raise RuntimeError("невідомий метод калібрування")
        if not probabilities:
            return ()
        raw = np.empty(len(probabilities), dtype=np.float64)
        for index, probability in enumerate(probabilities):
            _probability(probability)
            raw[index] = float(probability)
        if self.method == "isotonic":
            predicted = self.estimator.predict(raw)
        else:
            predicted = self.estimator.predict_proba(_logit(raw))[:, 1]
        return tuple(_as_probability(float(value)) for value in predicted)


@dataclass(frozen=True, slots=True)
class DecisionFit:
    """Калібратор для live, діаграма і поріг.

    `oof_probability` — те, на чому обирались метод і поріг, не відповідь live.
    """

    method: str
    calibrator: ProbabilityCalibrator
    diagram: ReliabilityDiagram
    selection: ThresholdSelection
    n_labeled: int
    n_unlabeled: int
    oof_brier: Decimal
    oof_probability: Mapping[tuple[str, int], Decimal]

    @property
    def rule(self) -> DecisionRule:
        return self.selection.rule


@dataclass(frozen=True, slots=True)
class _Prepared:
    fold_index: int
    probability: Decimal
    edge: Decimal | None
    outcome: Decimal | None


@fixed_context
def expected_value(*, probability: Decimal, tp_net: Decimal, sl_net: Decimal) -> Decimal:
    """`p·TP_net − (1−p)·SL_net` у ціні котирування. `sl_net` > 0; `tp_net` може бути від'ємним."""
    _probability(probability)
    require_decimal("tp_net", tp_net)
    require_decimal("sl_net", sl_net, positive=True)
    return probability * tp_net - (Decimal(1) - probability) * sl_net


@fixed_context
def expected_edge_atr(
    *,
    probability: Decimal,
    side: PositionSide,
    entry: Decimal,
    atr: Decimal,
    costs: CostModel,
    params: TargetParams,
) -> Decimal:
    """Край в ATR: EV / ATR. Витрати — `CostModel`, бар'єри — `barrier_prices`. Горизонт край не змінює.

    `UnusableExit` — стоп або ціна виконання не додатні. Funding сюди не входить.
    """
    _probability(probability)
    _priced_net = _priced(side=side, entry=entry, atr=atr, costs=costs, params=params)
    return expected_value(probability=probability, tp_net=_priced_net[1], sl_net=_priced_net[0].loss_per_unit) / atr


def judge_entry(
    *,
    probability: Decimal,
    expected_edge_atr: Decimal | None,
    rule: DecisionRule,
) -> EntryDecision:
    """Поріг уже каліброваної ймовірності. Спочатку непридатний вихід, потім упевненість, потім край.

    Обидва порівняння — `≥`. Якщо не проходять обидва пороги, причина — упевненість.
    """
    if type(rule) is not DecisionRule:
        raise ValueError("rule має бути DecisionRule")
    _probability(probability)
    if expected_edge_atr is not None:
        require_decimal("expected_edge_atr", expected_edge_atr)
    if expected_edge_atr is None:
        return EntryDecision(
            decision=SKIP,
            probability=probability,
            expected_edge_atr=None,
            reason=REASON_UNUSABLE_EXIT,
        )
    if probability < rule.min_confidence:
        reason = REASON_CONFIDENCE_BELOW_MIN
    elif expected_edge_atr < rule.min_edge_atr:
        reason = REASON_EDGE_BELOW_MIN
    else:
        return EntryDecision(
            decision=ENTER,
            probability=probability,
            expected_edge_atr=expected_edge_atr,
            reason=None,
        )
    return EntryDecision(
        decision=SKIP,
        probability=probability,
        expected_edge_atr=expected_edge_atr,
        reason=reason,
    )


@fixed_context
def decide_entry(
    *,
    raw_probability: Decimal,
    side: PositionSide,
    entry: Decimal,
    atr: Decimal,
    costs: CostModel,
    params: TargetParams,
    calibrator: ProbabilityCalibrator,
    rule: DecisionRule,
) -> EntryDecision:
    """Live і дослідження викликають це. Сира ймовірність калібрується, далі той самий `judge_entry`."""
    if type(calibrator) is not ProbabilityCalibrator:
        raise ValueError("calibrator має бути ProbabilityCalibrator")
    probability = calibrator.calibrate(raw_probability)
    edge: Decimal | None
    try:
        edge = expected_edge_atr(
            probability=probability,
            side=side,
            entry=entry,
            atr=atr,
            costs=costs,
            params=params,
        )
    except UnusableExit:
        edge = None
    return judge_entry(probability=probability, expected_edge_atr=edge, rule=rule)


@fixed_context
def reliability_diagram(
    probabilities: Sequence[Decimal],
    labels: Sequence[int],
    *,
    method: str | None = None,
) -> ReliabilityDiagram:
    """Рівні біни [0, 0.1), …, [0.9, 1]. Останній включає 1. Порожній бін має `n = 0`."""
    if len(probabilities) != len(labels):
        raise ValueError("ймовірності і мітки різної довжини")
    if method not in (None, *_METHODS):
        raise ValueError("метод калібрування — isotonic або platt")
    grouped: list[list[Decimal]] = [[] for _ in range(RELIABILITY_BIN_COUNT)]
    positives = [0 for _ in range(RELIABILITY_BIN_COUNT)]
    for probability, label in zip(probabilities, labels, strict=True):
        _probability(probability)
        if type(label) is not int or label not in (0, 1):
            raise ValueError("мітка діаграми — 0 або 1")
        slot = _bin_index(probability)
        grouped[slot].append(probability)
        positives[slot] += label
    bins: list[ReliabilityBin] = []
    width = Decimal(1) / Decimal(RELIABILITY_BIN_COUNT)
    for index, bucket in enumerate(grouped):
        lower = Decimal(index) / Decimal(RELIABILITY_BIN_COUNT)
        n = len(bucket)
        if n == 0:
            mean: Decimal | None = None
            fraction: Decimal | None = None
        else:
            mean = sum(bucket, Decimal(0)) / Decimal(n)
            fraction = Decimal(positives[index]) / Decimal(n)
        bins.append(
            ReliabilityBin(
                lower=lower,
                upper=lower + width,
                n=n,
                mean_predicted=mean,
                fraction_positive=fraction,
            )
        )
    return ReliabilityDiagram(bins=tuple(bins), method=method)


@fixed_context
def select_thresholds(
    rows: Sequence[OosObservation],
    split: ResearchSplit,
    calibrated: Mapping[tuple[str, int], Decimal],
    *,
    costs: CostModel,
    params: TargetParams,
) -> ThresholdSelection:
    """Поріг на вже каліброваних імовірностях. Рядки — лише тест своїх фолдів; hold-out — помилка.

    Критерій — медіана по фолдах суми реалізованого краю взятих угод, в ATR на одиницю кількості.
    Мітка 1 дає `tp_net / ATR`, мітка 0 — `−loss_per_unit / ATR` (повний SL, і для time-stop).
    Рядок без мітки сюди не приймається. Однаковий край: вищий `min_edge_atr`, потім вища
    `min_confidence`. Угода, яку `judge_entry` не бере, у суму не входить.
    """
    if not rows:
        raise CalibrationError("немає рядків для порогів")
    _unique(rows)
    _reject(rows, split, SplitPurpose.THRESHOLDS)
    _require_declared_fold(rows, split)
    keys = {(row.symbol, row.time_ms) for row in rows}
    if set(calibrated) != keys:
        raise ValueError("калібровані ймовірності не збігаються з рядками порогів")
    prepared: list[_Prepared] = []
    for row in rows:
        if row.true_label is None:
            raise ValueError("поріг не бачить рядок без мітки")
        probability = calibrated[(row.symbol, row.time_ms)]
        _probability(probability)
        prepared.append(_prepare(row, probability, costs, params))
    return _search(tuple(prepared))


def fit_decision(
    rows: Sequence[OosObservation],
    split: ResearchSplit,
    *,
    costs: CostModel,
    params: TargetParams,
    method: str | None = None,
) -> DecisionFit:
    """Калібрування і поріг. `method=None` обирає isotonic або Platt за меншим OOF Brier; нічия — Platt.

    Hold-out у переданому наборі — `SplitLeakError`, навіть якщо поруч є чесні рядки: він не відкидається
    мовчки. Train і рядки поза тестом у калібратор не входять. Рядок, чий `fold_index` не є фолдом,
    у тесті якого він лежить, — теж помилка: інакше ймовірність могла б бути з моделі, що його вчила.
    """
    if method is not None and method not in _METHODS:
        raise ValueError("метод калібрування — isotonic або platt")
    if type(split) is not ResearchSplit:
        raise ValueError("split має бути ResearchSplit")
    if type(costs) is not CostModel or type(params) is not TargetParams:
        raise ValueError("витрати і параметри таргета мають бути CostModel і TargetParams")
    _unique(rows)
    _reject(rows, split, SplitPurpose.CALIBRATION)
    _reject(rows, split, SplitPurpose.THRESHOLDS)
    calibration = _keep(rows, split, SplitPurpose.CALIBRATION)
    thresholds = _keep(rows, split, SplitPurpose.THRESHOLDS)
    _require_declared_fold(calibration, split)
    _require_declared_fold(thresholds, split)
    labeled_cal = tuple(row for row in calibration if row.true_label is not None)
    labeled_thr = tuple(row for row in thresholds if row.true_label is not None)
    unlabeled = {(row.symbol, row.time_ms) for row in (*calibration, *thresholds) if row.true_label is None}
    if not labeled_cal:
        raise CalibrationError("немає розмічених out-of-sample рядків для калібрування")
    if not labeled_thr:
        raise CalibrationError("немає розмічених out-of-sample рядків для порогів")
    chosen, oof_cal, brier = _chosen_method(labeled_cal, method)
    oof_thr = _oof(labeled_cal, labeled_thr, chosen)
    diagram = reliability_diagram(
        tuple(oof_cal[(row.symbol, row.time_ms)] for row in labeled_cal),
        tuple(_label(row) for row in labeled_cal),
        method=chosen,
    )
    selection = select_thresholds(labeled_thr, split, oof_thr, costs=costs, params=params)
    return DecisionFit(
        method=chosen,
        calibrator=_fit(chosen, labeled_cal),
        diagram=diagram,
        selection=selection,
        n_labeled=len(labeled_cal),
        n_unlabeled=len(unlabeled),
        oof_brier=brier,
        oof_probability=MappingProxyType(dict(oof_cal)),
    )


def _priced(
    *,
    side: PositionSide,
    entry: Decimal,
    atr: Decimal,
    costs: CostModel,
    params: TargetParams,
) -> tuple[PriceExecution, Decimal]:
    if type(side) is not PositionSide:
        raise ValueError("side має бути PositionSide")
    if type(costs) is not CostModel:
        raise ValueError("costs має бути CostModel")
    if type(params) is not TargetParams:
        raise ValueError("params має бути TargetParams")
    stop, take = barrier_prices(side, entry, atr, params)
    priced = costs.execution(
        side=side,
        entry=entry,
        stop=stop,
        take_profit=take,
        atr=atr,
        entry_is_maker=False,
    )
    tp_net = None if priced is None else priced.tp_net_per_unit
    if priced is None or tp_net is None:
        raise UnusableExit("немає цін виконання бар'єрів")
    return priced, tp_net


def _prepare(row: OosObservation, probability: Decimal, costs: CostModel, params: TargetParams) -> _Prepared:
    label = _label(row)
    try:
        priced, tp_net = _priced(side=row.side, entry=row.entry, atr=row.atr, costs=costs, params=params)
    except UnusableExit:
        return _Prepared(fold_index=row.fold_index, probability=probability, edge=None, outcome=None)
    # Мітка 0 — SL або time-stop. Обидва коштують повний бюджет стопа, не реалізований net time-stop.
    net = tp_net if label == 1 else -priced.loss_per_unit
    edge = expected_value(probability=probability, tp_net=tp_net, sl_net=priced.loss_per_unit) / row.atr
    return _Prepared(fold_index=row.fold_index, probability=probability, edge=edge, outcome=net / row.atr)


def _search(prepared: Sequence[_Prepared]) -> ThresholdSelection:
    folds = sorted({item.fold_index for item in prepared})
    best_rule: DecisionRule | None = None
    best_median: Decimal | None = None
    best_taken = 0
    for edge in EDGE_GRID:
        for confidence in CONFIDENCE_GRID:
            rule = DecisionRule(min_edge_atr=edge, min_confidence=confidence)
            sums = {fold: Decimal(0) for fold in folds}
            taken = 0
            for item in prepared:
                if item.edge is None or item.outcome is None:
                    continue
                decision = judge_entry(probability=item.probability, expected_edge_atr=item.edge, rule=rule)
                if decision.decision == ENTER:
                    sums[item.fold_index] += item.outcome
                    taken += 1
            median = _median(tuple(sums[fold] for fold in folds))
            if best_rule is None or best_median is None or _preferred(median, rule, best_median, best_rule):
                best_median = median
                best_rule = rule
                best_taken = taken
    if best_rule is None or best_median is None:
        raise RuntimeError("сітка порогів порожня")
    return ThresholdSelection(
        rule=best_rule,
        median_fold_edge_atr=best_median,
        n_taken=best_taken,
        n_folds=len(folds),
        beats_abstain=best_median > 0,
    )


def _preferred(median: Decimal, rule: DecisionRule, best_median: Decimal, best_rule: DecisionRule) -> bool:
    if median != best_median:
        return median > best_median
    if rule.min_edge_atr != best_rule.min_edge_atr:
        return rule.min_edge_atr > best_rule.min_edge_atr
    return rule.min_confidence > best_rule.min_confidence


def _median(values: Sequence[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise CalibrationError("немає фолдів для порогів")
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _chosen_method(
    rows: Sequence[OosObservation],
    method: str | None,
) -> tuple[str, dict[tuple[str, int], Decimal], Decimal]:
    if method is not None:
        mapped = _oof(rows, rows, method)
        return method, mapped, _brier(rows, mapped)
    best_name = ""
    best_map: dict[tuple[str, int], Decimal] = {}
    best_score: Decimal | None = None
    for candidate in _METHODS:
        mapped = _oof(rows, rows, candidate)
        score = _brier(rows, mapped)
        # Нічия лишає Platt: він стоїть другим і замінює рівний рахунок.
        # Isotonic при рівному Brier перенавчається легше.
        if best_score is None or score < best_score or (score == best_score and candidate == "platt"):
            best_name = candidate
            best_map = mapped
            best_score = score
    if best_score is None:
        raise RuntimeError("немає методу калібрування")
    return best_name, best_map, best_score


def _oof(
    train: Sequence[OosObservation],
    target: Sequence[OosObservation],
    method: str,
) -> dict[tuple[str, int], Decimal]:
    train_by_fold: dict[int, list[OosObservation]] = {}
    for row in train:
        train_by_fold.setdefault(row.fold_index, []).append(row)
    target_by_fold: dict[int, list[OosObservation]] = {}
    for row in target:
        target_by_fold.setdefault(row.fold_index, []).append(row)
    if len(target_by_fold) < 2:
        raise CalibrationError("калібрування потребує щонайменше два фолди walk-forward")
    mapped: dict[tuple[str, int], Decimal] = {}
    for fold_index in sorted(target_by_fold):
        pool = [row for other, group in train_by_fold.items() if other != fold_index for row in group]
        if not _both_classes(pool):
            raise CalibrationError(f"фолд {fold_index}: решта walk-forward не містить обидва класи")
        model = _fit(method, pool)
        group = target_by_fold[fold_index]
        probabilities = model._apply(tuple(row.raw_probability for row in group))
        for row, probability in zip(group, probabilities, strict=True):
            mapped[(row.symbol, row.time_ms)] = probability
    return mapped


def calibrator_from_payload(payload: Mapping[str, Any]) -> ProbabilityCalibrator:
    """Той самий калібратор, що `payload()`. Новий fit не відбувається: поріг уже обрано на walk-forward."""
    if not isinstance(payload, Mapping):
        raise ValueError("калібратор має бути об'єктом")
    method = payload.get("method")
    if method == "isotonic":
        x_raw = payload.get("x_thresholds")
        y_raw = payload.get("y_thresholds")
        if (
            not isinstance(x_raw, list)
            or not isinstance(y_raw, list)
            or not x_raw
            or len(x_raw) != len(y_raw)
            or set(payload) != {"method", "x_thresholds", "y_thresholds"}
        ):
            raise ValueError("isotonic-калібратор зіпсований")
        x_values = np.array([_float_from_hex(item) for item in x_raw], dtype=np.float64)
        y_values = np.array([_float_from_hex(item) for item in y_raw], dtype=np.float64)
        if np.any(np.diff(x_values) <= 0):
            raise ValueError("пороги isotonic не зростають")
        estimator = IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip")
        estimator.X_thresholds_ = x_values
        estimator.y_thresholds_ = y_values
        estimator.X_min_ = float(x_values[0])
        estimator.X_max_ = float(x_values[-1])
        estimator.increasing_ = True
        # sklearn clips to the end knots, then interpolates. np.interp does the same clip.
        estimator.f_ = _PiecewiseLinear(x_values, y_values)
        return ProbabilityCalibrator(method="isotonic", estimator=estimator)
    if method == "platt":
        if set(payload) != {"method", "coef", "intercept"}:
            raise ValueError("Platt-калібратор зіпсований")
        estimator = LogisticRegression(
            C=1.0,
            solver="lbfgs",
            max_iter=_PLATT_MAX_ITER,
            class_weight=None,
            random_state=0,
        )
        estimator.classes_ = np.array([0, 1], dtype=np.int64)
        estimator.coef_ = np.array([[_float_from_hex(payload["coef"])]], dtype=np.float64)
        estimator.intercept_ = np.array([_float_from_hex(payload["intercept"])], dtype=np.float64)
        estimator.n_features_in_ = 1
        return ProbabilityCalibrator(method="platt", estimator=estimator)
    raise ValueError("метод калібрування — isotonic або platt")


class _PiecewiseLinear:
    """Лінійна інтерполяція вузлів isotonic. Поза крайніми вузлами — значення краю."""

    def __init__(self, knots: np.ndarray, values: np.ndarray) -> None:
        self._knots = knots
        self._values = values

    def __call__(self, raw: np.ndarray) -> np.ndarray:
        return np.interp(np.asarray(raw, dtype=np.float64), self._knots, self._values)


def _float_hex(value: float) -> str:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("коефіцієнт калібратора не скінченний")
    return number.hex()


def _float_from_hex(value: object) -> float:
    if type(value) is not str:
        raise ValueError("коефіцієнт калібратора має бути hex-рядком")
    try:
        number = float.fromhex(value)
    except ValueError as exc:
        raise ValueError("коефіцієнт калібратора не hex") from exc
    if not math.isfinite(number):
        raise ValueError("коефіцієнт калібратора не скінченний")
    return number


def _fit(method: str, rows: Sequence[OosObservation]) -> ProbabilityCalibrator:
    if method not in _METHODS:
        raise ValueError("метод калібрування — isotonic або platt")
    if not _both_classes(rows):
        raise CalibrationError("калібрування потребує обидва класи")
    ordered = sorted(rows, key=lambda row: (row.time_ms, row.symbol, row.index))
    raw = np.array([float(row.raw_probability) for row in ordered], dtype=np.float64)
    labels = np.array([_label(row) for row in ordered], dtype=np.int64)
    if method == "isotonic":
        estimator: Any = IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip")
        estimator.fit(raw, labels)
        return ProbabilityCalibrator(method=method, estimator=estimator)
    estimator = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=_PLATT_MAX_ITER,
        class_weight=None,
        random_state=0,
    )
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", ConvergenceWarning)
            estimator.fit(_logit(raw), labels)
    except ConvergenceWarning as exc:
        raise CalibrationError("Platt не зійшовся") from exc
    if int(estimator.n_iter_[0]) >= _PLATT_MAX_ITER:
        raise CalibrationError("Platt не зійшовся")
    return ProbabilityCalibrator(method=method, estimator=estimator)


@fixed_context
def _brier(rows: Sequence[OosObservation], mapped: Mapping[tuple[str, int], Decimal]) -> Decimal:
    total = Decimal(0)
    for row in rows:
        error = mapped[(row.symbol, row.time_ms)] - Decimal(_label(row))
        total += error * error
    return total / Decimal(len(rows))


def _keep(rows: Sequence[OosObservation], split: ResearchSplit, purpose: str) -> tuple[OosObservation, ...]:
    allowed = rows_for(tuple(_sample(row) for row in rows), split, purpose)
    allowed_keys = {(row.symbol, row.time_ms) for row in allowed}
    return tuple(row for row in rows if (row.symbol, row.time_ms) in allowed_keys)


def _reject(rows: Sequence[OosObservation], split: ResearchSplit, purpose: str) -> None:
    reject_holdout(tuple(_sample(row) for row in rows), split, purpose=purpose)


def _require_declared_fold(rows: Sequence[OosObservation], split: ResearchSplit) -> None:
    for row in rows:
        if not is_test_row(_sample(row), split, row.fold_index):
            raise SplitLeakError(f"ймовірність рядка {row.symbol} {row.time_ms} не з тесту фолду {row.fold_index}")


def _unique(rows: Sequence[OosObservation]) -> None:
    seen: set[tuple[str, int]] = set()
    for row in rows:
        if type(row) is not OosObservation:
            raise ValueError("рядок має бути OosObservation")
        key = (row.symbol, row.time_ms)
        if key in seen:
            raise ValueError("повтор рядка калібрування")
        seen.add(key)


def _sample(row: OosObservation) -> SampleRow:
    return SampleRow(symbol=row.symbol, time_ms=row.time_ms, index=row.index)


def _both_classes(rows: Sequence[OosObservation]) -> bool:
    labels = {row.true_label for row in rows}
    return 0 in labels and 1 in labels


def _label(row: OosObservation) -> int:
    if type(row.true_label) is not int or row.true_label not in (0, 1):
        raise RuntimeError("калібрування без мітки")
    return row.true_label


def _probability(value: Decimal) -> None:
    require_decimal("probability", value)
    if value < 0 or value > 1:
        raise ValueError("ймовірність поза [0, 1]")


def _bin_index(probability: Decimal) -> int:
    if probability == 1:
        return RELIABILITY_BIN_COUNT - 1
    slot = int(probability * RELIABILITY_BIN_COUNT)
    if slot < 0 or slot >= RELIABILITY_BIN_COUNT:
        raise ValueError("ймовірність поза біном")
    return slot


def _logit(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, _PLATT_CLIP, 1.0 - _PLATT_CLIP)
    return np.log(clipped / (1.0 - clipped)).reshape(-1, 1)


def _as_probability(value: float) -> Decimal:
    if not math.isfinite(value) or value < -1e-8 or value > 1.0 + 1e-8:
        raise CalibrationError("калібрована ймовірність поза [0, 1]")
    clipped = min(1.0, max(0.0, value))
    quantized = Decimal(format(clipped, ".12f")).quantize(_PROBABILITY_QUANTUM)
    if quantized < 0 or quantized > 1:
        raise CalibrationError("калібрована ймовірність поза [0, 1]")
    return quantized
