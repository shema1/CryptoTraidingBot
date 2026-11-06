"""Пряме 3-класове прогнозування, ітерація 2 (ТЗ §5.1 п. 2, §5А.3 п. 13, §5А.11 п. 2; T2.13).

Одна LightGBM `multiclass` на всі свічки символів, а не на кандидатів первинного правила. Мітка свічки
`t` — `label_direct`: `LONG`, `SHORT` або `FLAT` (дві симуляції однієї свічки, одна з них дає клас).
Вага навчання — ваги унікальності × ваги класів (`sample_weights`), як у meta.

Рішення про сторону — argmax класу з **унікальним** максимумом. `FLAT`, нічия або рядок без валідних
фіч і ATR — без угоди. Сторона argmax дає один out-of-sample рядок: сира ймовірність — ймовірність
цього класу, мітка — чи дійшла угода цієї сторони до TP (`trade.meta_label`). Далі той самий шлях, що
в meta (рішення 0024): `fit_decision` на OOF-рядках лише минулих фолдів і `decide_entry` (§5.5.1) на фолді.
Правило входу однакове для обох ітерацій, і одна функція вирішує, чи брати угоду.

Спроба пишеться так само, як meta: `params` з рецептом, таргетом, витратами, правилом входу, схемою
фіч і оцінкою минулими фолдами; `metrics.folds[].direct.stress` — стрес тих самих сигналів (G1 бере його
з журналу). Hold-out сюди не входить ні мітками, ні фічами, ні угодами. Цикл hold-out не відкриває.

Рецепт — журнальний LightGBM без ранньої зупинки і без Optuna (рішення 0021): першу модель навчаємо
стандартними параметрами (§5А.3 п. 6). `device=cpu`.

Спільні з meta фічі, вікна, бектест і `_estimator` імпортуються з `meta_model`, а не копіюються, щоб обидві
ітерації бачили ті самі фічі та ті самі угоди.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.exchange.types import PositionSide
from app.ml.artifacts import decision_payload, entry_rule_record
from app.ml.backtest import BacktestReport, EntrySignal
from app.ml.decision import (
    ENTER,
    ENTRY_EVALUATION_PAST_FOLDS,
    ENTRY_RULE_WARMUP_FOLDS,
    CalibrationError,
    DecisionFit,
    OosObservation,
    decide_entry,
    fit_decision,
    past_fold_pool,
)
from app.ml.features import HOUR_MS, FeatureRows, SymbolData, compute_features
from app.ml.meta_model import (
    HoldoutGrant,
    _as_probability,
    _Backtest,
    _books,
    _estimator,
    _feature_series,
    _features_for_fold,
    _history,
    _seed,
    _shuffle_labels,
    _weight_array,
    _window,
    entry_rule_failure,
    lgbm_journal_params,
)
from app.ml.strategy import SymbolHistory
from app.ml.trade_sim import (
    DirectClass,
    DirectSample,
    NoAtr,
    TargetParams,
    atr_before_entry,
    label_direct,
    label_span_direct,
    sample_weights,
)
from app.ml.validation import (
    DIRECT_STAGE,
    DirectFoldRuns,
    FoldRecord,
    LabelStats,
    RecordedTrial,
    ResearchSplit,
    SampleRow,
    SplitLeakError,
    SplitPurpose,
    costs_payload,
    create_experiment,
    guard_sample_weights,
    is_holdout_row,
    label_diagnostics,
    record_trial,
    reject_holdout,
    rows_for,
    target_payload,
)
from app.risk.costs import CostModel
from app.risk.limits import RiskLimits

_LOG = get_logger("app.ml.direct_model")

# Порядок класів = індекси виходу моделі. `FLAT` — індекс 0: при нічиї він виграє, тобто угоди немає.
CLASS_LABELS: tuple[DirectClass, ...] = (DirectClass.FLAT, DirectClass.LONG, DirectClass.SHORT)
_CLASS_INDEX: dict[DirectClass, int] = {label: index for index, label in enumerate(CLASS_LABELS)}
_FLAT_INDEX: int = _CLASS_INDEX[DirectClass.FLAT]
_LONG_INDEX: int = _CLASS_INDEX[DirectClass.LONG]
_SHORT_INDEX: int = _CLASS_INDEX[DirectClass.SHORT]
# Стрес — той самий, що в meta (`_STRESS_COST`), і G1 вимагає саме його (`stress_cost_factor`).
_STRESS_COST: Decimal = Decimal(2)
# Прогрів правила минулих фолдів (рішення 0022, 0024) — одна константа з meta і G1 (`decision.py`).
PAST_FOLDS_MIN: int = ENTRY_RULE_WARMUP_FOLDS


class DirectTrainError(RuntimeError):
    """Модель прямого прогнозу не навчилась. Журнал, якщо експеримент створено, отримує `FAILED`."""

    def __init__(self, message: str, *, experiment_id: str | None = None) -> None:
        super().__init__(message)
        self.experiment_id = experiment_id


def direct_recipe(*, shuffled_labels: bool) -> dict[str, Any]:
    """Параметри спроби без `validation` і без таргета з витратами (їх дає `run_direct_experiment`).

    Журнальний рецепт LightGBM без ранньої зупинки (рішення 0021). `shuffled_labels` — діагностика ТЗ §5А.9:
    такий прогін у журнал не пишеться, але його параметри однакові, щоб відрізнялись лише мітки.
    """
    if type(shuffled_labels) is not bool:
        raise ValueError("shuffled_labels має бути bool")
    return {
        "stage": DIRECT_STAGE,
        "role": "direct",
        "algorithm": "lightgbm",
        "device": "cpu",
        "objective": "multiclass",
        "classes": [label.value for label in CLASS_LABELS],
        "shuffled_labels": shuffled_labels,
        "lgbm": lgbm_journal_params(),
    }


@dataclass(frozen=True, slots=True)
class DirectBar:
    """Свічка розбивки: рядок рішення, мітка прямого прогнозу (`None`, якщо шлях пропущено) і ATR(t−1)."""

    row: SampleRow
    sample: DirectSample | None
    open: Decimal
    atr: Decimal | None


@dataclass(frozen=True, slots=True)
class DirectFoldModel:
    """Модель одного фолду. Колонки — фічі покриття train, без сторони: модель бачить усі свічки."""

    fold_index: int
    feature_columns: tuple[str, ...]
    schema_hash: str
    preprocessing_version: str
    estimator: Any


@dataclass(frozen=True, slots=True)
class DirectFold:
    """Один фолд: модель, out-of-sample рядки (лише argmax LONG/SHORT) і мітки для журналу."""

    fold_index: int
    model: DirectFoldModel
    observations: tuple[OosObservation, ...]
    n_flat: int
    train_labels: LabelStats
    test_labels: LabelStats


@dataclass(frozen=True, slots=True)
class DirectRun:
    """Навчання прямого прогнозу на всіх фолдах. Hold-out сюди не входить."""

    split: ResearchSplit
    seed: int
    trading_symbol: str
    shuffled_labels: bool
    common: _Backtest
    folds: tuple[DirectFold, ...]


@dataclass(frozen=True, slots=True)
class DirectEvaluation:
    """Правило входу і прогони фолдів. `decision` — правило на всіх фолдах (йде в `params.decision`).

    `rules` — правило кожного фолду, підігнане лише на OOF-рядках фолдів `< k` (рішення 0022). Фолд 0 має
    `None`: минулих фолдів немає, угод у ньому немає. `cross_error` — чому не вийшло для фолду ≥ 1. Без обох
    правил угод немає: G1 такий рецепт відхиляє (`G1_ENTRY_RULE`), допуск теж.
    """

    records: tuple[FoldRecord, ...]
    rules: tuple[DecisionFit | None, ...]
    decision: DecisionFit | None
    decision_error: str | None
    cross_error: str | None

    @property
    def usable(self) -> bool:
        return self.decision is not None and self.cross_error is None


@dataclass(frozen=True, slots=True)
class DirectExperiment:
    experiment_id: str
    trial: RecordedTrial
    run: DirectRun
    evaluation: DirectEvaluation


def train_direct(
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
    feature_data: Mapping[str, SymbolData] | None = None,
    shuffled_labels: bool = False,
) -> DirectRun:
    """Навчити 3-класову модель на кожному фолді. Синхронна: з event loop — `asyncio.to_thread`.

    Фічі, мітки й угоди рахуються лише до hold-out (`holdout_from_ms`). Train фолду — рядки з міткою і
    валідними фічами; test — усі рядки фолду з валідними фічами (з міткою чи без). `shuffled_labels`
    перемішує лише навчальні мітки фолду, як у meta.
    """
    books = _books(histories, trading_symbol)
    _seed(seed)
    if split.bar_ms != HOUR_MS:
        raise ValueError("пряме прогнозування рахується на свічках 1h")
    if not split.folds:
        raise ValueError("у розбивці немає фолдів")
    if type(shuffled_labels) is not bool:
        raise ValueError("shuffled_labels має бути bool")
    model_costs = costs if costs is not None else CostModel()
    if type(model_costs) is not CostModel:
        raise ValueError("costs має бути CostModel")
    target = params if params is not None else TargetParams(horizon_bars=split.horizon_bars)
    if type(target) is not TargetParams:
        raise ValueError("params має бути TargetParams")
    if target.horizon_bars != split.horizon_bars:
        raise ValueError("горизонт таргета і розбивки різний")
    series = _feature_series(books, feature_data, split.holdout_from_ms)
    bars = tuple(bar for history in books for bar in _bars(history, split, target, model_costs))
    rows = tuple(bar.row for bar in bars)
    by_key = {(bar.row.symbol, bar.row.index): bar for bar in bars}
    common = _Backtest(
        limits=limits,
        initial_equity=initial_equity,
        maintenance_margin_rate=maintenance_margin_rate,
        seed=seed,
        costs=model_costs,
        params=target,
    )
    cache: dict[tuple[str, ...], FeatureRows] = {}
    folds = tuple(
        _train_fold(
            rows=rows,
            by_key=by_key,
            series=series,
            split=split,
            fold_index=index,
            cache=cache,
            seed=seed,
            shuffled_labels=shuffled_labels,
        )
        for index in range(len(split.folds))
    )
    return DirectRun(
        split=split,
        seed=seed,
        trading_symbol=trading_symbol,
        shuffled_labels=shuffled_labels,
        common=common,
        folds=folds,
    )


def direct_probability_gap(run: DirectRun) -> Decimal | None:
    """Середня ймовірність сторони в рядках, що дійшли до TP, мінус у рядках, що не дійшли (ТЗ §5А.9).

    На перемішаних мітках прогалина має бути близько нуля: мітка не була у фічах. `None` — одного з класів
    серед розмічених рядків немає. Рядки без мітки не рахуються.
    """
    wins = [row.raw_probability for fold in run.folds for row in fold.observations if row.true_label == 1]
    losses = [row.raw_probability for fold in run.folds for row in fold.observations if row.true_label == 0]
    if not wins or not losses:
        return None
    return sum(wins, Decimal(0)) / Decimal(len(wins)) - sum(losses, Decimal(0)) / Decimal(len(losses))


def evaluate_direct(run: DirectRun, histories: Sequence[SymbolHistory]) -> DirectEvaluation:
    """Правило входу на всіх OOF-рядках (для журналу, допуску і live), оцінка угод минулими фолдами і прогони для G1.

    Угоди фолду k — ті, що `decide_entry` бере з правилом, підігнаним лише на фолдах `< k` (рішення 0022).
    Фолд 0 угод не має. Стрес — ті самі сигнали з `cost_factor=2`. Без правила фолди йдуть без угод.
    """
    books = _books(histories, run.trading_symbol)
    rows = tuple(observation for fold in run.folds for observation in fold.observations)
    decision, decision_error = _fit_full(rows, run)
    rules, cross_error = _fit_past(run, rows)
    usable = decision is not None and cross_error is None
    records: list[FoldRecord] = []
    for index, fold in enumerate(run.folds):
        taken: tuple[OosObservation, ...] = ()
        rule = rules[index] if usable else None
        if rule is not None:
            taken = tuple(observation for observation in fold.observations if _enters(observation, rule, run.common))
        records.append(_fold_record(run, books, fold, taken))
    return DirectEvaluation(
        records=tuple(records),
        rules=rules if usable else (),
        decision=decision,
        decision_error=decision_error,
        cross_error=cross_error,
    )


def direct_trial_params(run: DirectRun, evaluation: DirectEvaluation) -> dict[str, Any]:
    """`params` COMPLETE-спроби прямого прогнозу: рецепт, схема фіч, правило входу і його оцінка минулими фолдами."""
    params = direct_recipe(shuffled_labels=run.shuffled_labels)
    params["feature_schema_hash"] = run.folds[-1].model.schema_hash
    params["feature_schema_hashes"] = [fold.model.schema_hash for fold in run.folds]
    params["n_observations"] = [len(fold.observations) for fold in run.folds]
    params["flat_rows"] = [fold.n_flat for fold in run.folds]
    if evaluation.decision is not None:
        params["decision"] = decision_payload(evaluation.decision.rule, evaluation.decision.calibrator)
    if evaluation.decision_error is not None:
        params["decision_error"] = evaluation.decision_error
    if evaluation.cross_error is not None:
        params["cross_error"] = evaluation.cross_error
    if evaluation.usable:
        params["entry_evaluation"] = ENTRY_EVALUATION_PAST_FOLDS
        params["entry_rules"] = [
            entry_rule_record(fold.fold_index, fit) for fold, fit in zip(run.folds, evaluation.rules, strict=True)
        ]
    return params


async def run_direct_experiment(
    session: AsyncSession,
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
    feature_data: Mapping[str, SymbolData] | None = None,
    shuffled_labels: bool = False,
) -> DirectExperiment:
    """Створити експеримент `label_type=direct`, навчити і записати одну спробу в журнал лінії.

    Пише в сесію викликача і не комітить: викликач комітить і після винятку, інакше `FAILED` зникне
    разом з rollback (як `run_meta_experiment`). Збій навчання — `FAILED` без фолдів, збій після навчання
    (правило входу, оцінка минулими фолдами) — `FAILED` з `failed_at = "entry_rule"`. Обидва збільшують N лінії.
    """
    target = params if params is not None else TargetParams(horizon_bars=split.horizon_bars)
    cost_model = costs if costs is not None else CostModel()
    recipe = {
        **direct_recipe(shuffled_labels=shuffled_labels),
        "target": target_payload(target),
        "costs": costs_payload(cost_model),
    }
    symbols = tuple(history.symbol for history in _books(histories, trading_symbol))
    experiment_id = await create_experiment(
        session,
        split=split,
        research_line=trading_symbol,
        symbols=symbols,
        label_type="direct",
        primary_strategy={},
        params=recipe,
        seed=seed,
    )
    try:
        run = await asyncio.to_thread(
            train_direct,
            histories,
            split=split,
            trading_symbol=trading_symbol,
            limits=limits,
            initial_equity=initial_equity,
            maintenance_margin_rate=maintenance_margin_rate,
            seed=seed,
            costs=cost_model,
            params=target,
            feature_data=feature_data,
            shuffled_labels=shuffled_labels,
        )
    except Exception as exc:
        # Будь-який збій навчання — спроба: N лінії не має зникати через те, що модель не навчилась.
        await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="FAILED",
            params={**recipe, "failed_at": "train", "error": f"{type(exc).__name__}: {exc}"},
            folds=(),
        )
        if isinstance(exc, DirectTrainError):
            exc.experiment_id = experiment_id
        exc.add_note(f"experiment_id={experiment_id}")
        raise
    try:
        evaluation = await asyncio.to_thread(evaluate_direct, run, histories)
    except Exception as exc:
        _LOG.error(
            "direct_entry_rule_failed",
            experiment_id=experiment_id,
            symbol=trading_symbol,
            error=type(exc).__name__,
        )
        await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="FAILED",
            params=entry_rule_failure(recipe, exc),
            folds=(),
        )
        exc.add_note(f"experiment_id={experiment_id}")
        raise
    trial = await record_trial(
        session,
        experiment_id=experiment_id,
        split=split,
        state="COMPLETE",
        params=direct_trial_params(run, evaluation),
        folds=evaluation.records,
    )
    return DirectExperiment(experiment_id=experiment_id, trial=trial, run=run, evaluation=evaluation)


@dataclass(frozen=True, slots=True)
class _Columns:
    """Вектор фіч рядка: `lookup` шукає слот у `FeatureRows` за (символ, час входу)."""

    features: FeatureRows
    lookup: dict[tuple[str, int], int]


def _bars(
    history: SymbolHistory,
    split: ResearchSplit,
    target: TargetParams,
    costs: CostModel,
    *,
    candle_cut_ms: int | None = None,
    decision_end_ms: int | None = None,
) -> tuple[DirectBar, ...]:
    """Усі свічки з рішенням до `decision_end`. Шлях рахується лише до hold-out (`holdout_from_ms`).

    `candle_cut_ms` / `decision_end_ms` — лише для перенавчання з грантом (рішення 0022): там свічки і рішення
    до `data_to_ms`. За замовчуванням — до hold-out, як у навчанні фолдів.
    """
    candle_cut = split.holdout_from_ms if candle_cut_ms is None else candle_cut_ms
    decision_end = split.decision_end_ms if decision_end_ms is None else decision_end_ms
    prefix = tuple(bar for bar in history.candles if bar.open_time_ms < candle_cut)
    marks = None
    if history.mark_candles is not None:
        marks = tuple(bar for bar in history.mark_candles if bar.open_time_ms < candle_cut)
    funding = tuple(event for event in history.funding if event.time_ms < candle_cut)
    indexes = tuple(index for index, bar in enumerate(prefix) if bar.open_time_ms < decision_end)
    labeled = label_direct(
        prefix,
        symbol=history.symbol,
        entry_indexes=indexes,
        params=target,
        costs=costs,
        mark_candles=marks,
        funding=funding,
    )
    if len(labeled.samples) + len(labeled.skipped) != len(indexes):
        raise RuntimeError("label_direct загубив свічку")
    by_index = {sample.entry_index: sample for sample in labeled.samples}
    bars: list[DirectBar] = []
    for index in indexes:
        bar = prefix[index]
        atr: Decimal | None
        try:
            atr = atr_before_entry(prefix, index)
        except NoAtr:
            atr = None
        bars.append(
            DirectBar(
                row=SampleRow(symbol=history.symbol, time_ms=bar.open_time_ms, index=index),
                sample=by_index.get(index),
                open=bar.open,
                atr=atr,
            )
        )
    return tuple(bars)


def _train_fold(
    *,
    rows: Sequence[SampleRow],
    by_key: Mapping[tuple[str, int], DirectBar],
    series: Mapping[str, SymbolData],
    split: ResearchSplit,
    fold_index: int,
    cache: dict[tuple[str, ...], FeatureRows],
    seed: int,
    shuffled_labels: bool,
) -> DirectFold:
    features = _features_for_fold(series, split, fold_index, cache)
    columns = _Columns(features=features, lookup=_lookup(features))
    train_rows = rows_for(rows, split, SplitPurpose.TRAIN, fold_index=fold_index)
    reject_holdout(train_rows, split, purpose=SplitPurpose.TRAIN)
    train_bars = [by_key[(row.symbol, row.index)] for row in train_rows]
    fitted_bars: list[DirectBar] = []
    vectors: list[np.ndarray] = []
    for bar in train_bars:
        if bar.sample is None:
            continue
        vector = _row_vector(columns, bar.row)
        if vector is not None:
            fitted_bars.append(bar)
            vectors.append(vector)
    if not fitted_bars:
        raise DirectTrainError(f"фолд {fold_index}: немає навчальних рядків із фічами")
    guard_sample_weights(tuple(bar.row for bar in fitted_bars), split, fold_index)
    samples = _samples(fitted_bars)
    true_labels = [_CLASS_INDEX[sample.label] for sample in samples]
    fit_labels = _shuffle_labels(true_labels, seed, fold_index) if shuffled_labels else list(true_labels)
    absent = [label.value for label in CLASS_LABELS if _CLASS_INDEX[label] not in fit_labels]
    if absent:
        raise DirectTrainError(f"фолд {fold_index}: у train немає класу {absent[0]}")
    spans = tuple(label_span_direct(sample.symbol, sample) for sample in samples)
    weights = sample_weights(spans, fit_labels)
    if not weights or any(weight <= 0 for weight in weights):
        raise DirectTrainError(f"фолд {fold_index}: без ваг унікальності навчати не можна")
    estimator = _estimator(seed, None, objective="multiclass")
    estimator.fit(
        np.vstack(vectors),
        np.asarray(fit_labels, dtype=np.int32),
        sample_weight=_weight_array(weights),
    )
    model = DirectFoldModel(
        fold_index=fold_index,
        feature_columns=features.columns,
        schema_hash=features.schema_hash,
        preprocessing_version=features.preprocessing_version,
        estimator=estimator,
    )
    test_rows = rows_for(rows, split, SplitPurpose.TEST, fold_index=fold_index)
    reject_holdout(test_rows, split, purpose=SplitPurpose.TEST)
    test_bars = [by_key[(row.symbol, row.index)] for row in test_rows]
    observations, n_flat = _test_observations(model, test_bars, columns, fold_index=fold_index)
    return DirectFold(
        fold_index=fold_index,
        model=model,
        observations=observations,
        n_flat=n_flat,
        train_labels=label_diagnostics(_samples([bar for bar in train_bars if bar.sample is not None])),
        test_labels=label_diagnostics(_samples([bar for bar in test_bars if bar.sample is not None])),
    )


def _test_observations(
    model: DirectFoldModel,
    bars: Sequence[DirectBar],
    columns: _Columns,
    *,
    fold_index: int,
) -> tuple[tuple[OosObservation, ...], int]:
    """Рядки для правила входу: argmax LONG або SHORT з валідними фічами і ATR. Повертає і число `FLAT`."""
    valid: list[tuple[DirectBar, np.ndarray]] = []
    for bar in bars:
        vector = _row_vector(columns, bar.row)
        if vector is not None:
            valid.append((bar, vector))
    if not valid:
        return (), 0
    probabilities = _class_probabilities(model.estimator, np.vstack([vector for _, vector in valid]))
    observations: list[OosObservation] = []
    n_flat = 0
    for (bar, _), values in zip(valid, probabilities, strict=True):
        winner = _winner(values)
        if winner == _FLAT_INDEX:
            n_flat += 1
            continue
        if winner is None or bar.atr is None:
            continue
        side = PositionSide.LONG if winner == _LONG_INDEX else PositionSide.SHORT
        label: int | None = None
        if bar.sample is not None:
            trade = bar.sample.long_trade if side is PositionSide.LONG else bar.sample.short_trade
            label = trade.meta_label
        observations.append(
            OosObservation(
                symbol=bar.row.symbol,
                time_ms=bar.row.time_ms,
                index=bar.row.index,
                fold_index=fold_index,
                side=side,
                raw_probability=values[winner],
                true_label=label,
                entry=bar.open,
                atr=bar.atr,
            )
        )
    return tuple(observations), n_flat


def _winner(values: Sequence[Decimal]) -> int | None:
    """Індекс класу з унікальним максимумом; нічия — `None`, тобто угоди немає."""
    top = max(values)
    winners = [index for index, value in enumerate(values) if value == top]
    return winners[0] if len(winners) == 1 else None


def _class_probabilities(estimator: Any, matrix: np.ndarray) -> list[tuple[Decimal, ...]]:
    classes = [int(label) for label in estimator.classes_]
    if classes != list(range(len(CLASS_LABELS))):
        raise DirectTrainError("модель не має всіх класів у порядку CLASS_LABELS")
    raw = estimator.predict_proba(matrix)
    return [tuple(_as_probability(float(value)) for value in row) for row in raw]


def _lookup(features: FeatureRows) -> dict[tuple[str, int], int]:
    return {(symbol, int(features.entry_open_time_ms[index])): index for index, symbol in enumerate(features.symbol)}


def _row_vector(columns: _Columns, row: SampleRow) -> np.ndarray | None:
    """Вектор фіч рядка або `None`: немає слоту, фіча невалідна чи не скінченна. Модель такий рядок не бачить."""
    slot = columns.lookup.get((row.symbol, row.time_ms))
    if slot is None or not bool(columns.features.valid[slot]):
        return None
    values = np.array(columns.features.values[slot], dtype=np.float64, copy=True)
    if not np.isfinite(values).all():
        return None
    return values


def _samples(bars: Sequence[DirectBar]) -> tuple[DirectSample, ...]:
    found: list[DirectSample] = []
    for bar in bars:
        if bar.sample is None:
            raise RuntimeError("рядок без мітки прямого прогнозу")
        found.append(bar.sample)
    return tuple(found)


def _fit_full(rows: Sequence[OosObservation], run: DirectRun) -> tuple[DecisionFit | None, str | None]:
    try:
        return fit_decision(rows, run.split, costs=run.common.costs, params=run.common.params), None
    except CalibrationError as exc:
        return None, str(exc)


def _fit_past(
    run: DirectRun,
    rows: Sequence[OosObservation],
) -> tuple[tuple[DecisionFit | None, ...], str | None]:
    """Правило фолду k — з OOF-рядків фолдів `< k` (рішення 0022).

    Фолди `< PAST_FOLDS_MIN` (`ENTRY_RULE_WARMUP_FOLDS`) — прогрів: правило `None`, угод немає; G1 рахує умови
    фолдів без них (рішення 0024).
    Пізніші фолди не входять ні в пул, ні в калібратор правила k. Помилка калібрування фолду k, для якого
    минулих достатньо, — `cross_error`: без угод, G1 відмовляє (`G1_ENTRY_RULE`).
    """
    rules: list[DecisionFit | None] = []
    try:
        for fold in run.folds:
            if fold.fold_index < PAST_FOLDS_MIN:
                rules.append(None)
                continue
            pool = past_fold_pool(rows, run.split, fold.fold_index)
            rules.append(fit_decision(pool, run.split, costs=run.common.costs, params=run.common.params))
    except CalibrationError as exc:
        return (), f"правило минулих фолдів: {exc}"
    if all(rule is None for rule in rules):
        return (), f"правило минулих фолдів: немає фолду з щонайменше {PAST_FOLDS_MIN} минулими"
    return tuple(rules), None


def _enters(observation: OosObservation, fit: DecisionFit, common: _Backtest) -> bool:
    verdict = decide_entry(
        raw_probability=observation.raw_probability,
        side=observation.side,
        entry=observation.entry,
        atr=observation.atr,
        costs=common.costs,
        params=common.params,
        calibrator=fit.calibrator,
        rule=fit.rule,
    )
    return verdict.decision == ENTER


def _fold_record(
    run: DirectRun,
    books: Sequence[SymbolHistory],
    fold: DirectFold,
    taken: Sequence[OosObservation],
) -> FoldRecord:
    window = run.split.folds[fold.fold_index]
    reports: tuple[BacktestReport, ...] = tuple(
        _window(history, _signals(history, taken), run.split, window, run.common) for history in books
    )
    trading = _history(books, run.trading_symbol)
    stress = _window(
        trading,
        _signals(trading, taken),
        run.split,
        window,
        run.common,
        cost_factor=_STRESS_COST,
    )
    return FoldRecord(
        reports=reports,
        train_labels=fold.train_labels,
        test_labels=fold.test_labels,
        direct=DirectFoldRuns(stress=stress),
    )


def _signals(history: SymbolHistory, taken: Sequence[OosObservation]) -> tuple[EntrySignal, ...]:
    signals: list[EntrySignal] = []
    for observation in taken:
        if observation.symbol != history.symbol:
            continue
        if history.candles[observation.index].open_time_ms != observation.time_ms:
            raise RuntimeError("рядок не збігається зі свічкою входу")
        signals.append(EntrySignal(entry_index=observation.index, side=observation.side))
    return tuple(sorted(signals, key=lambda signal: signal.entry_index))


@dataclass(frozen=True, slots=True)
class FrozenDirectFit:
    """Одна 3-класова модель за вже обраним рецептом (рішення 0022). Калібратор і поріг сюди не входять.

    `include_holdout` — навчання з грантом hold-out (лише перенавчання після допуску). `coverage_ms` —
    покриття фіч `[data_from, holdout_from)`, так само як у `fit_frozen_meta`.
    """

    model: DirectFoldModel
    n_rows: int
    row_times_ms: tuple[int, ...]
    lgbm: dict[str, Any]
    include_holdout: bool
    coverage_ms: tuple[int, int]


def fit_frozen_direct(
    histories: Sequence[SymbolHistory],
    *,
    split: ResearchSplit,
    trading_symbol: str,
    seed: int,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    feature_data: Mapping[str, SymbolData] | None = None,
    lgbm: Mapping[str, Any] | None = None,
    holdout_grant: HoldoutGrant | None = None,
    expected_schema_hash: str | None = None,
) -> FrozenDirectFit:
    """Навчити 3-класову модель один раз, без Optuna і без ранньої зупинки (рішення 0021, дзеркало `fit_frozen_meta`).

    Без гранту свічки, мітки і рішення — лише до hold-out (модель допуску). З грантом — до `data_to_ms`, і його
    вікно має збігатися з hold-out розбивки. Усі три класи мають бути в навчанні.
    """
    if holdout_grant is not None:
        if type(holdout_grant) is not HoldoutGrant:
            raise ValueError("допуск hold-out зіпсований")
        if (holdout_grant.holdout_from_ms, holdout_grant.holdout_to_ms) != (split.holdout_from_ms, split.holdout_to_ms):
            raise SplitLeakError("грант не цього hold-out")
    include_holdout = holdout_grant is not None
    books = _books(histories, trading_symbol)
    _seed(seed)
    if split.bar_ms != HOUR_MS:
        raise ValueError("пряме прогнозування рахується на свічках 1h")
    model_costs = costs if costs is not None else CostModel()
    if type(model_costs) is not CostModel:
        raise ValueError("costs має бути CostModel")
    target = params if params is not None else TargetParams(horizon_bars=split.horizon_bars)
    if type(target) is not TargetParams or target.horizon_bars != split.horizon_bars:
        raise ValueError("горизонт таргета і розбивки різний")
    candle_cut = split.data_to_ms if include_holdout else split.holdout_from_ms
    decision_cut = split.data_to_ms if include_holdout else split.decision_end_ms
    coverage = (split.data_from_ms, split.holdout_from_ms)
    if coverage[0] >= coverage[1]:
        raise ValueError("покриття фіч порожнє")
    series = _feature_series(books, feature_data, split.holdout_from_ms, cutoff_ms=candle_cut)
    bars = tuple(
        bar
        for history in books
        for bar in _bars(
            history,
            split,
            target,
            model_costs,
            candle_cut_ms=candle_cut,
            decision_end_ms=decision_cut,
        )
    )
    features = compute_features(series, coverage_start_ms=coverage[0], coverage_end_ms=coverage[1])
    if expected_schema_hash is not None and features.schema_hash != expected_schema_hash:
        raise DirectTrainError("схема фіч не збігається з моделлю, що пройшла hold-out")
    columns = _Columns(features=features, lookup=_lookup(features))
    fitted: list[DirectBar] = []
    vectors: list[np.ndarray] = []
    for bar in bars:
        if bar.sample is None:
            continue
        vector = _row_vector(columns, bar.row)
        if vector is not None:
            fitted.append(bar)
            vectors.append(vector)
    if not fitted:
        raise DirectTrainError("немає навчальних рядків із фічами")
    if not include_holdout:
        reject_holdout(tuple(bar.row for bar in fitted), split, purpose=SplitPurpose.TRAIN)
    times = tuple(bar.row.time_ms for bar in fitted)
    seen_holdout = any(time >= split.holdout_from_ms for time in times)
    if include_holdout and not seen_holdout:
        raise DirectTrainError("перенавчання не бачить жодного рядка hold-out")
    if not include_holdout and seen_holdout:
        raise DirectTrainError("модель допуску заходить у hold-out")
    samples = _samples(fitted)
    labels = [_CLASS_INDEX[sample.label] for sample in samples]
    absent = [label.value for label in CLASS_LABELS if _CLASS_INDEX[label] not in labels]
    if absent:
        raise DirectTrainError(f"у навчанні немає класу {absent[0]}")
    spans = tuple(label_span_direct(sample.symbol, sample) for sample in samples)
    weights = sample_weights(spans, labels)
    if not weights or any(weight <= 0 for weight in weights):
        raise DirectTrainError("без ваг унікальності навчати не можна")
    checked = lgbm_journal_params() if lgbm is None else dict(lgbm)
    estimator = _estimator(seed, checked, objective="multiclass")
    estimator.fit(np.vstack(vectors), np.asarray(labels, dtype=np.int32), sample_weight=_weight_array(weights))
    model = DirectFoldModel(
        fold_index=0,
        feature_columns=features.columns,
        schema_hash=features.schema_hash,
        preprocessing_version=features.preprocessing_version,
        estimator=estimator,
    )
    return FrozenDirectFit(
        model=model,
        n_rows=len(fitted),
        row_times_ms=times,
        lgbm=checked,
        include_holdout=include_holdout,
        coverage_ms=coverage,
    )


def _holdout_signals(
    fit: FrozenDirectFit,
    rule: DecisionFit,
    histories: Sequence[SymbolHistory],
    *,
    split: ResearchSplit,
    trading_symbol: str,
    costs: CostModel,
    params: TargetParams,
    feature_data: Mapping[str, SymbolData] | None = None,
) -> tuple[EntrySignal, ...]:
    """Сигнали входу на запечатаному hold-out тією самою моделлю і правилом журналу (рішення 0022).

    Не публічна: рядки hold-out віддає лише відкриття G1 (`rows_for_opened_holdout`, правило 10). Тут немає
    перевірки відкриття, тож звіт для допуску будує лише код, що вже має відкриття цього hold-out.

    Фічі — з тим самим покриттям і схемою, що в моделі допуску; рядки — лише вікна hold-out. Вхід — `decide_entry`
    з `rule` і його калібратором. Звіт бектесту будує викликач (як для meta). Лише для `fit` без гранту.
    """
    if fit.include_holdout:
        raise ValueError("сигнали hold-out рахує модель допуску, не перенавчена")
    books = _books(histories, trading_symbol)
    series = _feature_series(books, feature_data, split.holdout_from_ms, cutoff_ms=split.data_to_ms)
    features = compute_features(series, coverage_start_ms=fit.coverage_ms[0], coverage_end_ms=fit.coverage_ms[1])
    if features.schema_hash != fit.model.schema_hash:
        raise DirectTrainError("схема фіч hold-out не збігається з моделлю допуску")
    columns = _Columns(features=features, lookup=_lookup(features))
    trading = _history(books, trading_symbol)
    bars = _bars(trading, split, params, costs, candle_cut_ms=split.data_to_ms, decision_end_ms=split.data_to_ms)
    holdout_bars = [bar for bar in bars if is_holdout_row(bar.row, split)]
    observations, _ = _test_observations(fit.model, holdout_bars, columns, fold_index=0)
    taken = tuple(
        observation
        for observation in observations
        if decide_entry(
            raw_probability=observation.raw_probability,
            side=observation.side,
            entry=observation.entry,
            atr=observation.atr,
            costs=costs,
            params=params,
            calibrator=rule.calibrator,
            rule=rule.rule,
        ).decision
        == ENTER
    )
    return _signals(trading, taken)
