"""Бінарна meta-модель LightGBM (ТЗ §5.1, §5А.3, §5А.9, T2.8).

Первинне правило вже зафіксоване (`StrategySpec` з `PRIMARY_CANDIDATES`). Модель лише оцінює
ймовірність, що кандидат дійде до TP. За замовчуванням — стандартні параметри LightGBM, без Optuna,
`device=cpu`. Optuna (T2.10) передає вже перевірену точку з діапазонів ТЗ §5А.7; таргет і правило
вона не змінює. `sample_weight` — `sample_weights` (унікальність × баланс класів); `class_weight`
класифікатора не вмикається, інакше баланс врахувався б двічі.

Кожен фолд учиться лише на своєму train. Фічі цього фолду рахуються з покриттям train
(`train_coverage_ms`). Мітка у вектор не входить. Поріг до калібрування — `META_MIN_PROBABILITY`.
Тестовий кандидат оцінюється й без мітки: дірка в шляху після входу — це майбутнє, і фільтр її не знає.
Калібрування цих імовірностей і поріг live — `decision.py`, не `keep`.
Метрики — `run_backtest` на відфільтрованих `EntrySignal`, тим самим вікном, що правило без фільтра.
Стрес (`cost_factor=2`) лишається на об'єкті прогону і в журнал окремою спробою не пишеться.

`train_meta` синхронний: це навчання, не торговий цикл. `run_meta_experiment` виносить його
з event loop. Воркер Optuna викликає той самий `train_meta` з точкою пошуку.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

import lightgbm as lgb
import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.exchange.types import PositionSide
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
from app.ml.features import (
    HOUR_MS,
    CandleBar,
    FeatureRows,
    SymbolData,
    TimedValue,
    compute_features,
    feature_columns,
    sources_covering,
)
from app.ml.lgbm_space import EARLY_STOP_FRACTION, EARLY_STOPPING_ROUNDS, lgbm_search_record, search_point
from app.ml.statistics import FoldOutcome, fold_outcome_from_report, total_net
from app.ml.strategy import (
    PRIMARY_CANDIDATES,
    StrategySpec,
    SymbolHistory,
    backtest_fold_signals,
    primary_candidates,
    primary_signals,
)
from app.ml.trade_sim import MetaSample, NoAtr, TargetParams, atr_before_entry, label_meta, label_span, sample_weights
from app.ml.validation import (
    Fold,
    FoldRecord,
    LabelStats,
    MetaFoldRuns,
    RecordedTrial,
    ResearchSplit,
    SampleRow,
    SplitLeakError,
    SplitPurpose,
    costs_payload,
    create_experiment,
    guard_sample_weights,
    is_test_row,
    is_train_row,
    label_diagnostics,
    record_trial,
    reject_holdout,
    require_train_coverage,
    rows_for,
    target_payload,
    train_coverage_ms,
)
from app.risk.costs import CostModel
from app.risk.limits import RiskLimits
from app.risk.numbers import FrozenModel, require_decimal

_LOG = get_logger("app.ml.meta_model")

# До правила EV (T2.9). 0.5 — стандартний поріг бінарного класифікатора, не підібраний на історії.
META_MIN_PROBABILITY: Decimal = Decimal("0.5")
SIDE_COLUMN: str = "side_long"
_PROBABILITY_QUANTUM: Decimal = Decimal("0.0000000001")
_STRESS_COST: Decimal = Decimal(2)


class MetaTrainError(RuntimeError):
    """Фолд не можна навчити. Журнал, якщо експеримент уже створено, отримує `FAILED`.

    `completed_folds` — префікс фолдів, які встигли навчитися. Optuna записує його, не пропускаючи дірку.
    """

    def __init__(self, message: str, *, experiment_id: str | None = None) -> None:
        super().__init__(message)
        self.experiment_id = experiment_id
        self.completed_folds: tuple[MetaFold, ...] = ()


def lgbm_journal_params() -> dict[str, Any]:
    """Стандартний рецепт LightGBM 4.x для бінарної класифікації. Не діапазон Optuna (ТЗ §5А.7).

    `n_jobs=1` і `deterministic` фіксують рецепт між машинами. `class_weight` лишається порожнім:
    баланс класів уже всередині `sample_weights`. Float у журналі заборонений, тож частки — Decimal.
    """
    return {
        "boosting_type": "gbdt",
        "num_leaves": 31,
        "max_depth": -1,
        "learning_rate": Decimal("0.1"),
        "n_estimators": 100,
        "min_child_samples": 20,
        "min_child_weight": Decimal("0.001"),
        "subsample": Decimal(1),
        "subsample_freq": 0,
        "colsample_bytree": Decimal(1),
        "reg_alpha": Decimal(0),
        "reg_lambda": Decimal(0),
        "min_split_gain": Decimal(0),
        "deterministic": True,
        "force_col_wise": True,
        "n_jobs": 1,
        "class_weight": None,
    }


def meta_recipe(*, shuffled_labels: bool, min_probability: Decimal) -> dict[str, Any]:
    """Параметри експерименту без `validation`: її дописує `create_experiment`."""
    _probability_threshold(min_probability)
    if type(shuffled_labels) is not bool:
        raise ValueError("shuffled_labels має бути bool")
    return {
        "stage": "meta_model",
        "algorithm": "lightgbm",
        "device": "cpu",
        "objective": "binary",
        "min_probability": min_probability,
        "shuffled_labels": shuffled_labels,
        "extra_columns": [SIDE_COLUMN],
        "lgbm": lgbm_journal_params(),
    }


def keep(probability: Decimal, *, min_probability: Decimal = META_MIN_PROBABILITY) -> bool:
    """Чи залишити кандидата. Поріг імовірності TP до калібрування.

    Live-вхід — `decide_entry` у `decision.py`: калібрована ймовірність і край в ATR, не цей поріг.
    """
    require_decimal("probability", probability)
    _probability_threshold(min_probability)
    if probability < 0 or probability > 1:
        raise ValueError("ймовірність поза [0, 1]")
    return probability >= min_probability


class FittedRow(FrozenModel):
    """Рядок, на якому вчився фолд. `fit_label` після перемішування може відрізнятись від істинної мітки."""

    symbol: str
    time_ms: int
    exit_open_time_ms: int
    true_label: int
    fit_label: int
    weight: Decimal


class ScoredRow(FrozenModel):
    """Out-of-sample кандидат. Оцінений моделлю фолду, яка цей рядок не бачила в train.

    `true_label` — `None`, якщо `label_meta` пропустив шлях (дірка після входу). Рішення фільтра від
    мітки не залежить, тож кандидат оцінюється й торгується, але в `probability_gap` і калібрування не йде.
    """

    symbol: str
    time_ms: int
    entry_index: int
    side: PositionSide
    true_label: int | None
    probability: Decimal
    taken: bool


@dataclass(frozen=True, slots=True)
class MetaFoldModel:
    """Модель одного фолду. `columns` — фічі цього покриття плюс `side_long`, якого немає в schema hash."""

    fold_index: int
    columns: tuple[str, ...]
    feature_columns: tuple[str, ...]
    schema_hash: str
    preprocessing_version: str
    estimator: Any

    def probability(self, values: Sequence[float], *, side: PositionSide) -> Decimal:
        """Імовірність TP для одного кандидата. `values` — колонки фіч без сторони, у порядку `feature_columns`."""
        if type(side) is not PositionSide:
            raise ValueError("side має бути PositionSide")
        if len(values) != len(self.feature_columns):
            raise ValueError("довжина вектора не збігається з колонками фіч")
        vector = np.empty(len(self.columns), dtype=np.float64)
        for index, value in enumerate(values):
            number = float(value)
            if not math.isfinite(number):
                raise ValueError("фіча не скінченна")
            vector[index] = number
        vector[-1] = 1.0 if side is PositionSide.LONG else 0.0
        return _probabilities(self.estimator, vector.reshape(1, -1))[0]


@dataclass(frozen=True, slots=True)
class MetaFold:
    """Один фолд: модель, рядки навчання, оцінки тесту і прогін відфільтрованих сигналів.

    Правило без фільтра торгового символу рахується двічі: на всіх сигналах вікна тесту
    (`unfiltered_trading_net`) і лише на тих, які модель оцінила (`scored_unfiltered_trading_net`).
    `n_unscored_signals` — сигнали правила без валідних фіч: модель їх не бачила (рішення 0017).
    """

    fold_index: int
    model: MetaFoldModel
    fitted: tuple[FittedRow, ...]
    scored: tuple[ScoredRow, ...]
    record: FoldRecord
    unfiltered_trading_net: Decimal
    scored_unfiltered_trading_net: Decimal
    n_unscored_signals: int
    stress: FoldOutcome
    # Скільки дерев лишилось після ранньої зупинки. `None` — журнальний рецепт без неї.
    best_iteration: int | None = None


@dataclass(frozen=True, slots=True)
class MetaRun:
    """Прогін walk-forward. Hold-out сюди не входить ні мітками, ні фічами, ні угодами."""

    spec: StrategySpec
    split: ResearchSplit
    seed: int
    trading_symbol: str
    shuffled_labels: bool
    min_probability: Decimal
    folds: tuple[MetaFold, ...]
    # `None` — `lgbm_journal_params`. Інакше перевірена точка пошуку, не сирий словник викликача.
    lgbm: Mapping[str, Any] | None = None
    # Налаштування бектесту навчання: оцінка правила входу минулими фолдами перераховує фолди з тими самими.
    backtest: _Backtest | None = None
    # Правило входу кожного фолду, підігнане лише на минулих фолдах (`past_fold_entry`, рішення 0024);
    # `None` — прогрівний фолд без правила й без угод. Порожньо — угоди фолдів відібрані
    # `keep(min_probability)`, і G1 таку спробу meta не пропустить.
    entry_rules: tuple[DecisionFit | None, ...] = ()

    @property
    def filtered_total_net(self) -> Decimal:
        # Суми — у контексті G1 (`total_net`), не потоку: інакше журнал і G1 розійдуться на останньому знаку.
        return total_net(_trading_net(fold.record, self.trading_symbol) for fold in self.folds)

    @property
    def unfiltered_total_net(self) -> Decimal:
        return total_net(fold.unfiltered_trading_net for fold in self.folds)

    @property
    def scored_unfiltered_total_net(self) -> Decimal:
        return total_net(fold.scored_unfiltered_trading_net for fold in self.folds)

    @property
    def beats_unfiltered(self) -> bool:
        """Сума чистого результату торгового символу строго вища за обидва правила без фільтра (Gate G1).

        Лише все правило не годиться: сигнали без валідних фіч модель не оцінює, і фільтр «вигравав» би
        на них без жодного рішення (рішення 0017).
        """
        filtered = self.filtered_total_net
        return filtered > self.unfiltered_total_net and filtered > self.scored_unfiltered_total_net

    @property
    def records(self) -> tuple[FoldRecord, ...]:
        return tuple(fold.record for fold in self.folds)

    @property
    def stress(self) -> tuple[FoldOutcome, ...]:
        return tuple(fold.stress for fold in self.folds)

    def trial_params(self) -> dict[str, Any]:
        """Параметри спроби журналу. Зайві ключі не дають збігу з опорним бейзлайном, тож спроба йде в PBO.

        `comparison` — лише зведення для людини. G1 рахує обидва правила і стрес не з нього, а зі звітів
        `FoldRecord.meta`, які `record_trial` звіряє з угодами (рішення 0017). Стрес — не окрема спроба,
        тож N від нього не росте.
        """
        params = meta_recipe(shuffled_labels=self.shuffled_labels, min_probability=self.min_probability)
        if self.lgbm is not None:
            params["lgbm"] = dict(self.lgbm)
            params["best_iteration"] = [fold.best_iteration for fold in self.folds]
        params["role"] = "meta"
        params["strategy"] = self.spec.metadata()
        params["comparison"] = {
            "filtered_total_net": self.filtered_total_net,
            "unfiltered_total_net": self.unfiltered_total_net,
            "scored_unfiltered_total_net": self.scored_unfiltered_total_net,
            "n_unscored_signals": [fold.n_unscored_signals for fold in self.folds],
            "beats_unfiltered": self.beats_unfiltered,
        }
        if self.entry_rules:
            # Імпорт усередині: `artifacts` імпортує цей модуль. sha256 — той самий канонічний текст калібратора,
            # що в `params.decision`; `fit_on_folds` — фолди, на яких підігнано правило (рішення 0024).
            from app.ml.artifacts import entry_rule_record

            params["entry_evaluation"] = ENTRY_EVALUATION_PAST_FOLDS
            params["entry_rules"] = [
                entry_rule_record(fold.fold_index, fit) for fold, fit in zip(self.folds, self.entry_rules, strict=True)
            ]
        return params


@dataclass(frozen=True, slots=True)
class MetaExperiment:
    """Експеримент і єдина спроба моделі. Стрес у журналі окремим рядком не лежить."""

    experiment_id: str
    trial: RecordedTrial
    run: MetaRun
    # Правило входу і калібратор (T2.12). `None` — не вдалось підігнати: причина в `decision_error`,
    # і спроба G1 без `params.decision` допуском не стане.
    decision: DecisionFit | None = None
    decision_error: str | None = None


def probability_gap(scored: Sequence[ScoredRow]) -> Decimal | None:
    """Середня ймовірність справжніх TP мінус середня ймовірність решти.

    На перемішаних мітках прогалина близько нуля: мітка не була у фічах. `None` — одного з класів немає.
    Рядки без мітки (пропущений шлях) не рахуються.
    """
    wins = [row.probability for row in scored if row.true_label == 1]
    losses = [row.probability for row in scored if row.true_label == 0]
    if not wins or not losses:
        return None
    return _mean(wins) - _mean(losses)


def train_meta(
    histories: Sequence[SymbolHistory],
    *,
    spec: StrategySpec,
    split: ResearchSplit,
    trading_symbol: str,
    limits: RiskLimits,
    initial_equity: Decimal,
    maintenance_margin_rate: Decimal,
    seed: int,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    feature_data: Mapping[str, SymbolData] | None = None,
    min_probability: Decimal = META_MIN_PROBABILITY,
    shuffled_labels: bool = False,
    lgbm: Mapping[str, Any] | None = None,
    stop_after: Callable[[MetaFold], bool] | None = None,
) -> MetaRun:
    """Навчити по моделі на фолд і прогнати відфільтровані сигнали бектестом.

    `feature_data` дає обсяг і деривативи. Ціни фіч усе одно беруться зі свічок історії, щоб мітка
    і вектор дивились на один ряд. `None` — обсяг 1 і без деривативів (`vol_ratio` стає 1).
    `shuffled_labels` перемішує лише навчальні мітки фолду; фічі, спани унікальності й тест лишаються
    на своїх рядках. Генератор — PCG64 від seed, не глобальний стан numpy.
    `lgbm` — точка `lgbm_search_record` (Optuna). `None` лишає журнальний рецепт без ранньої зупинки.
    `stop_after` повертає True, щоб не вчити решту фолдів (префікс для `PRUNED`).
    """
    books = _books(histories, trading_symbol)
    _spec(spec)
    _seed(seed)
    _probability_threshold(min_probability)
    if type(shuffled_labels) is not bool:
        raise ValueError("shuffled_labels має бути bool")
    checked = None if lgbm is None else lgbm_search_record(search_point(lgbm))
    if stop_after is not None and not callable(stop_after):
        raise ValueError("stop_after має бути викликаним")
    if split.bar_ms != HOUR_MS:
        raise ValueError("meta-модель рахується на свічках 1h")
    if not split.folds:
        raise ValueError("у розбивці немає фолдів")
    model_costs = costs if costs is not None else CostModel()
    if type(model_costs) is not CostModel:
        raise ValueError("costs має бути CostModel")
    target = params if params is not None else TargetParams(horizon_bars=split.horizon_bars)
    if type(target) is not TargetParams:
        raise ValueError("params має бути TargetParams")
    if target.horizon_bars != split.horizon_bars:
        raise ValueError("горизонт таргета і розбивки різний")
    series = _feature_series(books, feature_data, split.holdout_from_ms)
    found = {history.symbol: _candidates(spec, history, split, target, model_costs, seed) for history in books}
    by_key = {(item.row.symbol, item.row.index): item for history in books for item in found[history.symbol]}
    samples = tuple(item.sample for history in books for item in found[history.symbol] if item.sample is not None)
    candidates = tuple(
        sorted((item.row for item in by_key.values()), key=lambda row: (row.time_ms, row.symbol, row.index))
    )
    reject_holdout(candidates, split, purpose=SplitPurpose.PRIMARY_STRATEGY)
    common = _Backtest(
        limits=limits,
        initial_equity=initial_equity,
        maintenance_margin_rate=maintenance_margin_rate,
        seed=seed,
        costs=model_costs,
        params=target,
    )
    # Ключ — набір колонок. Покриття змінює лише його, не значення вже порахованої колонки.
    cache: dict[tuple[str, ...], FeatureRows] = {}
    done: list[MetaFold] = []
    try:
        for fold in split.folds:
            trained = _train_fold(
                books,
                spec=spec,
                split=split,
                fold=fold,
                trading_symbol=trading_symbol,
                by_key=by_key,
                samples=samples,
                candidates=candidates,
                series=series,
                cache=cache,
                common=common,
                seed=seed,
                min_probability=min_probability,
                shuffled_labels=shuffled_labels,
                lgbm=checked,
            )
            done.append(trained)
            if stop_after is not None and stop_after(trained):
                break
    except MetaTrainError as exc:
        exc.completed_folds = tuple(done)
        raise
    return MetaRun(
        spec=spec,
        split=split,
        seed=seed,
        trading_symbol=trading_symbol,
        shuffled_labels=shuffled_labels,
        min_probability=min_probability,
        folds=tuple(done),
        lgbm=checked,
        backtest=common,
    )


_HOLDOUT_SEAL = object()


class HoldoutGrant:
    """Дозвіл навчатись на hold-out `[holdout_from_ms, holdout_to_ms)` після допуску відкриття `opening_id`.

    Конструктор приймає лише приватну печатку, тож грант видає `_grant_holdout`. Це домовленість модуля,
    а не криптографія: у `app/` `_grant_holdout` кличе лише `artifacts.retrain_registered_recipe` після
    рядка допуску в БД (тест `test_holdout_grant_is_issued_only_by_artifacts`).
    """

    __slots__ = ("holdout_from_ms", "holdout_to_ms", "opening_id")

    def __init__(self, opening_id: int, holdout_from_ms: int, holdout_to_ms: int, *, _seal: object) -> None:
        if _seal is not _HOLDOUT_SEAL:
            raise ValueError("допуск hold-out видає лише запис у БД")
        for value in (opening_id, holdout_from_ms, holdout_to_ms):
            if type(value) is not int or isinstance(value, bool) or value < 0:
                raise ValueError("грант hold-out зіпсований")
        if opening_id < 1 or holdout_to_ms <= holdout_from_ms:
            raise ValueError("грант hold-out зіпсований")
        self.opening_id = opening_id
        self.holdout_from_ms = holdout_from_ms
        self.holdout_to_ms = holdout_to_ms


def _grant_holdout(opening_id: int, holdout_from_ms: int, holdout_to_ms: int) -> HoldoutGrant:
    """Лише після рядка `holdout_openings` з успішним допуском. Кличе `retrain_registered_recipe`."""
    return HoldoutGrant(opening_id, holdout_from_ms, holdout_to_ms, _seal=_HOLDOUT_SEAL)


@dataclass(frozen=True, slots=True)
class FrozenFit:
    """Одна модель за вже обраним рецептом. Калібратор і поріг сюди не входять і не перебираються.

    `include_holdout` — рядки навчання заходять у hold-out. Це лише фінальне перенавчання після допуску.
    `row_times_ms` — час входу кожного навчального рядка. Ранньої зупинки немає.
    """

    model: MetaFoldModel
    n_rows: int
    row_times_ms: tuple[int, ...]
    lgbm: dict[str, Any]
    include_holdout: bool
    coverage_ms: tuple[int, int]


def fit_frozen_meta(
    histories: Sequence[SymbolHistory],
    *,
    spec: StrategySpec,
    split: ResearchSplit,
    trading_symbol: str,
    seed: int,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    feature_data: Mapping[str, SymbolData] | None = None,
    lgbm: Mapping[str, Any] | None = None,
    holdout_grant: HoldoutGrant | None = None,
    expected_schema_hash: str | None = None,
) -> FrozenFit:
    """Навчити один бустинг за фіксованими параметрами, без Optuna і без ранньої зупинки.

    Без `holdout_grant` свічки й мітки обрізаються до hold-out: цією моделлю його перевіряють.
    Грант видає лише `_grant_holdout` після рядка допуску в БД (`retrain_registered_recipe`), і його вікно
    має збігтися з hold-out розбивки. Прапорця «включити hold-out» немає. Поріг і калібратор не чіпає.
    """
    if holdout_grant is not None:
        if type(holdout_grant) is not HoldoutGrant:
            raise ValueError("допуск hold-out зіпсований")
        if (holdout_grant.holdout_from_ms, holdout_grant.holdout_to_ms) != (split.holdout_from_ms, split.holdout_to_ms):
            raise SplitLeakError("грант не цього hold-out")
    include_holdout = holdout_grant is not None
    books = _books(histories, trading_symbol)
    _spec(spec)
    _seed(seed)
    if split.bar_ms != HOUR_MS:
        raise ValueError("meta-модель рахується на свічках 1h")
    model_costs = costs if costs is not None else CostModel()
    if type(model_costs) is not CostModel:
        raise ValueError("costs має бути CostModel")
    target = params if params is not None else TargetParams(horizon_bars=split.horizon_bars)
    if type(target) is not TargetParams or target.horizon_bars != split.horizon_bars:
        raise ValueError("горизонт таргета і розбивки різний")
    candle_cut = split.data_to_ms if include_holdout else split.holdout_from_ms
    decision_cut = split.data_to_ms if include_holdout else split.decision_end_ms
    # Покриття колонок — завжди до hold-out: дірка в ньому не має міняти склад фіч рецепта.
    coverage = (split.data_from_ms, split.holdout_from_ms)
    if coverage[0] >= coverage[1]:
        raise ValueError("покриття фіч порожнє")
    series = _feature_series(books, feature_data, split.holdout_from_ms, cutoff_ms=candle_cut)
    found = {
        history.symbol: _candidates(
            spec,
            history,
            split,
            target,
            model_costs,
            seed,
            candle_before_ms=candle_cut,
            decision_before_ms=decision_cut,
        )
        for history in books
    }
    by_key = {(item.row.symbol, item.row.index): item for history in books for item in found[history.symbol]}
    candidates = tuple(
        sorted((item.row for item in by_key.values()), key=lambda row: (row.time_ms, row.symbol, row.index))
    )
    features = compute_features(series, coverage_start_ms=coverage[0], coverage_end_ms=coverage[1])
    if expected_schema_hash is not None and features.schema_hash != expected_schema_hash:
        raise MetaTrainError("схема фіч не збігається з моделлю, що пройшла hold-out")
    train = [item for item in _join(candidates, by_key, features) if item.candidate.sample is not None]
    if not include_holdout:
        reject_holdout(tuple(item.candidate.row for item in train), split, purpose=SplitPurpose.TRAIN)
    if not train:
        raise MetaTrainError("немає навчальних рядків із фічами")
    train_samples = tuple(_train_sample(item) for item in train)
    labels = [sample.label for sample in train_samples]
    if len(set(labels)) < 2:
        raise MetaTrainError("у навчанні один клас, бінарну модель не навчити")
    times = tuple(sample.trade.entry_open_time_ms for sample in train_samples)
    seen_holdout = any(time >= split.holdout_from_ms for time in times)
    if include_holdout and not seen_holdout:
        raise MetaTrainError("перенавчання не бачить жодного рядка hold-out")
    if not include_holdout and seen_holdout:
        raise MetaTrainError("модель допуску заходить у hold-out")
    spans = tuple(label_span(sample.symbol, sample.trade) for sample in train_samples)
    weights = sample_weights(spans, labels)
    if not weights or any(weight <= 0 for weight in weights):
        raise MetaTrainError("без ваг унікальності навчати не можна")
    checked = lgbm_journal_params() if lgbm is None else dict(lgbm)
    estimator = _estimator(seed, checked)
    estimator.fit(_matrix(train), np.asarray(labels, dtype=np.int32), sample_weight=_weight_array(weights))
    model = MetaFoldModel(
        fold_index=0,
        columns=(*features.columns, SIDE_COLUMN),
        feature_columns=features.columns,
        schema_hash=features.schema_hash,
        preprocessing_version=features.preprocessing_version,
        estimator=estimator,
    )
    return FrozenFit(
        model=model,
        n_rows=len(train),
        row_times_ms=times,
        lgbm=checked,
        include_holdout=include_holdout,
        coverage_ms=coverage,
    )


async def run_meta_experiment(
    session: AsyncSession,
    histories: Sequence[SymbolHistory],
    *,
    spec: StrategySpec,
    split: ResearchSplit,
    trading_symbol: str,
    limits: RiskLimits,
    initial_equity: Decimal,
    maintenance_margin_rate: Decimal,
    seed: int,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    feature_data: Mapping[str, SymbolData] | None = None,
    min_probability: Decimal = META_MIN_PROBABILITY,
    shuffled_labels: bool = False,
) -> MetaExperiment:
    """Створити експеримент `label_type=meta`, навчити і записати одну спробу.

    Пише в сесію викликача і не комітить. Викликач комітить і після винятку: інакше спроба `FAILED`
    (модель навчена) зникне з журналу разом з rollback. `run_iteration_one` так і робить.

    Невдале навчання теж записується (`FAILED`, порожній префікс фолдів) і збільшує N лінії. Так само
    `FAILED` (`failed_at = "entry_rule"`) — збій після навчання: модель навчена, отже це спроба.
    Спроба `COMPLETE` несе правило входу і схему фіч рецепта (`g1_recipe_fields`, рішення 0019) або
    `decision_error`, якщо правило не підганяється (тоді допуск її відхилить).
    `primary_strategy` — `StrategySpec.metadata()` цього правила, не словник вибору з T2.7.
    """
    recipe = meta_recipe(shuffled_labels=shuffled_labels, min_probability=min_probability)
    # Таргет і витрати — частина рецепта (ТЗ §5.3): допуск hold-out звіряє з ними модель допуску (T2.11).
    target = params if params is not None else TargetParams(horizon_bars=split.horizon_bars)
    recipe["target"] = target_payload(target)
    recipe["costs"] = costs_payload(costs if costs is not None else CostModel())
    symbols = tuple(history.symbol for history in _books(histories, trading_symbol))
    experiment_id = await create_experiment(
        session,
        split=split,
        research_line=trading_symbol,
        symbols=symbols,
        label_type="meta",
        primary_strategy=spec.metadata(),
        params=recipe,
        seed=seed,
    )
    try:
        run = await asyncio.to_thread(
            train_meta,
            histories,
            spec=spec,
            split=split,
            trading_symbol=trading_symbol,
            limits=limits,
            initial_equity=initial_equity,
            maintenance_margin_rate=maintenance_margin_rate,
            seed=seed,
            costs=costs,
            params=params,
            feature_data=feature_data,
            min_probability=min_probability,
            shuffled_labels=shuffled_labels,
        )
    except MetaTrainError as exc:
        failed = dict(recipe)
        failed["role"] = "meta"
        failed["strategy"] = spec.metadata()
        await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="FAILED",
            params=failed,
            folds=(),
        )
        exc.experiment_id = experiment_id
        raise
    # Правило входу і схема фіч — частина рецепта спроби G1 (T2.11, рішення 0019). Без них допуск відмовить.
    # Модель уже навчена: будь-який збій далі (витік у вибірці порогу, рядок без свічки входу) — теж
    # спроба. Вона пишеться як `FAILED` і збільшує N, інакше збій ховав би множинне тестування.
    entry_costs = costs if costs is not None else CostModel()
    try:
        recipe_run = await asyncio.to_thread(
            recipe_trial,
            run,
            histories,
            split=split,
            costs=entry_costs,
            params=target,
        )
    except Exception as exc:
        _LOG.error(
            "meta_entry_rule_failed",
            experiment_id=experiment_id,
            symbol=trading_symbol,
            strategy=spec.name,
            error=type(exc).__name__,
        )
        await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state="FAILED",
            params=entry_rule_failure({**recipe, "role": "meta", "strategy": spec.metadata()}, exc),
            folds=(),
        )
        exc.add_note(f"experiment_id={experiment_id}")
        raise
    trial = await record_trial(
        session,
        experiment_id=experiment_id,
        split=split,
        state="COMPLETE",
        params=recipe_run.params,
        folds=recipe_run.run.records,
    )
    return MetaExperiment(
        experiment_id=experiment_id,
        trial=trial,
        run=recipe_run.run,
        decision=recipe_run.decision,
        decision_error=recipe_run.decision_error,
    )


@dataclass(frozen=True, slots=True)
class RecipeTrial:
    """Що пише COMPLETE-спроба meta: прогін, `params` і правило входу рецепта (рішення 0019, 0020).

    `run` — фолди, оцінені правилом входу лише минулими фолдами (`past_fold_entry`), або прогін `keep`, якщо
    правило не підганяється: тоді `decision` — `None`, причина в `decision_error`, і G1 спробу не пропустить.
    """

    run: MetaRun
    params: dict[str, Any]
    decision: DecisionFit | None
    decision_error: str | None


def recipe_trial(
    run: MetaRun,
    histories: Sequence[SymbolHistory],
    *,
    split: ResearchSplit,
    costs: CostModel,
    params: TargetParams,
) -> RecipeTrial:
    """Спроба G1 з правилом входу рецепта: рецепт для live і чесна оцінка того самого правила.

    1. `fit_entry_rule` — калібратор і поріг на OOF-рядках усіх фолдів: це правило йде в `params.decision`,
       модель допуску і live.
    2. `past_fold_entry` — те саме правило, але для фолду k підігнане лише на фолдах `< k`, перераховує угоди
       кожного фолду (фолди 0 і 1 — прогрів без угод). Їх і читає G1: поріг, обраний на угодах цього або
       пізнішого фолду, завищив би результат (рішення 0024).
    Не підганяється (немає розмічених рядків, менше трьох фолдів, немає ATR) — `decision_error` і прогін
    `keep` без позначки `entry_evaluation`: G1 відмовить (`G1_ENTRY_RULE`), допуск теж. Будь-який інший
    виняток — збій після навчання: викликач пише спробу `FAILED` (`entry_rule_failure`) і прокидає його.
    Спільне для `run_meta_experiment` і Optuna (T2.12). Синхронна: з event loop — `asyncio.to_thread`.
    """
    common = run.backtest
    if common is not None:
        # Правило рецепта і правила фолдів G1 — з тими самими витратами й таргетом, що бектест навчання.
        if costs != common.costs:
            raise ValueError("витрати правила входу не ті, що в навчанні")
        if params != common.params:
            raise ValueError("таргет правила входу не той, що в навчанні")
    fit, error = fit_entry_rule(run, histories, split=split, costs=costs, params=params)
    if fit is None:
        return _without_rule(run, error)
    try:
        evaluated = past_fold_entry(run, histories)
    except (CalibrationError, NoAtr) as exc:
        return _without_rule(run, f"правило входу минулих фолдів: {exc}")
    trial_params = evaluated.trial_params()
    trial_params.update(g1_recipe_fields(evaluated, fit))
    return RecipeTrial(run=evaluated, params=trial_params, decision=fit, decision_error=None)


def _without_rule(run: MetaRun, error: str | None) -> RecipeTrial:
    if not error:
        raise RuntimeError("правило входу не підігнано, а причини немає")
    trial_params = run.trial_params()
    trial_params["decision_error"] = error
    return RecipeTrial(run=run, params=trial_params, decision=None, decision_error=error)


def past_fold_entry(run: MetaRun, histories: Sequence[SymbolHistory]) -> MetaRun:
    """Угоди кожного фолду за правилом входу рецепта, підігнаним лише на минулих фолдах (рішення 0024).

    Для фолду k калібратор і поріг — `fit_decision` на OOF-рядках фолдів `< k` (`past_fold_pool`, з purge
    біля межі тесту); кандидат фолду k береться, якщо `decide_entry` каже `ENTER` (та сама функція, що в
    live). Ні мітки фолду k, ні будь-що з пізніших фолдів у правило k не входять. Фолди
    `< ENTRY_RULE_WARMUP_FOLDS` — прогрів калібрування: правила немає (`None`), угод теж; G1 рахує умови
    фолдів без них. Далі — ті самі звіти, що в `train_meta`: відфільтровані угоди кожного символу і стрес на
    прийнятих сигналах; обидва правила без фільтра від рішення не залежать і лишаються як були.
    Фолдів — щонайменше `ENTRY_RULE_WARMUP_FOLDS + 1` (інакше `CalibrationError`). Бектест — з налаштувань
    навчання (`MetaRun.backtest`).
    """
    common = run.backtest
    if common is None:
        raise ValueError("прогін без налаштувань бектесту: оцінку правила входу рахує лише прогін train_meta")
    if run.entry_rules:
        raise ValueError("прогін уже оцінено правилом входу")
    books = _books(histories, run.trading_symbol)
    trading = _history(books, run.trading_symbol)
    by_index = {fold.index: fold for fold in run.split.folds}
    rows = decision_observations(run, histories)
    if len(run.folds) <= ENTRY_RULE_WARMUP_FOLDS:
        raise CalibrationError(
            f"оцінка минулих фолдів потребує щонайменше {ENTRY_RULE_WARMUP_FOLDS + 1} фолди walk-forward "
            f"({ENTRY_RULE_WARMUP_FOLDS} перші — прогрів калібрування)"
        )
    evaluated: list[MetaFold] = []
    rules: list[DecisionFit | None] = []
    for fold in run.folds:
        fit: DecisionFit | None = None
        if fold.fold_index >= ENTRY_RULE_WARMUP_FOLDS:
            pool = past_fold_pool(rows, run.split, fold.fold_index)
            fit = fit_decision(pool, run.split, costs=common.costs, params=common.params)
        own = {(row.symbol, row.time_ms): row for row in rows if row.fold_index == fold.fold_index}
        scored = tuple(
            row.model_copy(update={"taken": fit is not None and _enters(own[(row.symbol, row.time_ms)], fit, common)})
            for row in fold.scored
        )
        window = by_index[fold.fold_index]
        reports = tuple(
            _window(
                history,
                _scored_signals(
                    history,
                    _fold_signals(history, run.spec, run.split, window, common.seed),
                    scored,
                    taken_only=True,
                ),
                run.split,
                window,
                common,
            )
            for history in books
        )
        taken = _scored_signals(
            trading,
            _fold_signals(trading, run.spec, run.split, window, common.seed),
            scored,
            taken_only=True,
        )
        stress = _window(trading, taken, run.split, window, common, cost_factor=_STRESS_COST)
        before = fold.record.meta
        if before is None:
            raise RuntimeError("фолд meta без звітів правила")
        runs = MetaFoldRuns(unfiltered=before.unfiltered, scored_unfiltered=before.scored_unfiltered, stress=stress)
        record = FoldRecord(
            reports=reports,
            train_labels=fold.record.train_labels,
            test_labels=fold.record.test_labels,
        ).model_copy(update={"meta": runs})
        evaluated.append(
            replace(
                fold,
                scored=scored,
                record=record,
                stress=fold_outcome_from_report(stress, below_min_trades=window.below_min_trades),
            )
        )
        rules.append(fit)
    return replace(run, folds=tuple(evaluated), entry_rules=tuple(rules))


def _enters(row: OosObservation, fit: DecisionFit, common: _Backtest) -> bool:
    verdict = decide_entry(
        raw_probability=row.raw_probability,
        side=row.side,
        entry=row.entry,
        atr=row.atr,
        costs=common.costs,
        params=common.params,
        calibrator=fit.calibrator,
        rule=fit.rule,
    )
    return verdict.decision == ENTER


def entry_rule_failure(params: Mapping[str, Any], exc: BaseException) -> dict[str, Any]:
    """`params` спроби `FAILED`, коли модель навчена, а правило входу чи поля рецепта впали."""
    failed = dict(params)
    failed["failed_at"] = "entry_rule"
    failed["error"] = f"{type(exc).__name__}: {exc}"
    return failed


def decision_observations(run: MetaRun, histories: Sequence[SymbolHistory]) -> tuple[OosObservation, ...]:
    """Out-of-sample рядки всіх фолдів для калібрування і порогу входу (T2.9, T2.12).

    Ціна входу — `open` свічки `entry_index` ряду символу, ATR — ATR(t−1) того самого ряду
    (`atr_before_entry`). Hold-out сюди не потрапляє: `scored` містить лише тестові кандидати фолдів.
    Рядок без свічки входу або ATR — помилка, а не пропуск: інакше поріг бачив би іншу вибірку.
    """
    by_symbol = {history.symbol: history for history in histories}
    rows: list[OosObservation] = []
    for fold in run.folds:
        for scored in fold.scored:
            history = by_symbol.get(scored.symbol)
            if history is None:
                raise ValueError(f"немає історії символу {scored.symbol} для рядка фолду {fold.fold_index}")
            entry = history.candles[scored.entry_index]
            if entry.open_time_ms != scored.time_ms:
                raise RuntimeError("рядок не збігається зі свічкою входу")
            rows.append(
                OosObservation(
                    symbol=scored.symbol,
                    time_ms=scored.time_ms,
                    index=scored.entry_index,
                    fold_index=fold.fold_index,
                    side=scored.side,
                    raw_probability=scored.probability,
                    true_label=scored.true_label,
                    entry=entry.open,
                    atr=atr_before_entry(history.candles, scored.entry_index),
                )
            )
    return tuple(rows)


def fit_entry_rule(
    run: MetaRun,
    histories: Sequence[SymbolHistory],
    *,
    split: ResearchSplit,
    costs: CostModel,
    params: TargetParams,
) -> tuple[DecisionFit | None, str | None]:
    """Калібратор і поріг на out-of-fold рядках. Немає розмічених рядків або ATR — `(None, причина)`.

    Витоки (hold-out у вибірці, чужий фолд) не ковтаються: `SplitLeakError` летить далі.
    """
    try:
        rows = decision_observations(run, histories)
        return fit_decision(rows, split, costs=costs, params=params), None
    except (CalibrationError, NoAtr) as exc:
        return None, str(exc)


def g1_recipe_fields(run: MetaRun, fit: DecisionFit) -> dict[str, Any]:
    """Поля рецепта G1 у `params` спроби: правило входу і схема фіч (рішення 0019).

    Схема рецепта — схема останнього фолду: його train має найбільше покриття деривативів. Допуск
    порівнює з нею схему свого навчання; різниця — відмова, а не тихий пропуск. Імпорт усередині:
    `artifacts` імпортує цей модуль.
    """
    from app.ml.artifacts import decision_payload

    if not run.folds:
        raise ValueError("немає фолдів, з яких береться схема рецепта")
    return {
        "decision": decision_payload(fit.rule, fit.calibrator),
        "feature_schema_hash": run.folds[-1].model.schema_hash,
        "feature_schema_hashes": [fold.model.schema_hash for fold in run.folds],
    }


@dataclass(frozen=True, slots=True)
class _Backtest:
    limits: RiskLimits
    initial_equity: Decimal
    maintenance_margin_rate: Decimal
    seed: int
    costs: CostModel
    params: TargetParams


@dataclass(frozen=True, slots=True)
class _Candidate:
    """Кандидат правила до hold-out. `sample` — `None`, якщо `label_meta` пропустив шлях."""

    row: SampleRow
    side: PositionSide
    sample: MetaSample | None


@dataclass(frozen=True, slots=True)
class _Joined:
    candidate: _Candidate
    values: np.ndarray


def _train_fold(
    books: Sequence[SymbolHistory],
    *,
    spec: StrategySpec,
    split: ResearchSplit,
    fold: Fold,
    trading_symbol: str,
    by_key: Mapping[tuple[str, int], _Candidate],
    samples: tuple[MetaSample, ...],
    candidates: tuple[SampleRow, ...],
    series: Mapping[str, SymbolData],
    cache: dict[tuple[str, ...], FeatureRows],
    common: _Backtest,
    seed: int,
    min_probability: Decimal,
    shuffled_labels: bool,
    lgbm: Mapping[str, Any] | None,
) -> MetaFold:
    features = _features_for_fold(series, split, fold.index, cache)
    train_rows = rows_for(candidates, split, SplitPurpose.TRAIN, fold_index=fold.index)
    reject_holdout(train_rows, split, purpose=SplitPurpose.TRAIN)
    # Пропущений шлях мітки не має: у train не йде і нулем не стає.
    train = [item for item in _join(train_rows, by_key, features) if item.candidate.sample is not None]
    guard_sample_weights(tuple(item.candidate.row for item in train), split, fold.index)
    if not train:
        raise MetaTrainError(f"фолд {fold.index}: немає навчальних рядків із фічами")
    train_samples = tuple(_train_sample(item) for item in train)
    true_labels = [sample.label for sample in train_samples]
    fit_labels = _shuffle_labels(true_labels, seed, fold.index) if shuffled_labels else list(true_labels)
    if len(set(fit_labels)) < 2:
        raise MetaTrainError(f"фолд {fold.index}: у train один клас, бінарну модель не навчити")
    spans = tuple(label_span(sample.symbol, sample.trade) for sample in train_samples)
    weights = sample_weights(spans, fit_labels)
    if not weights or any(weight <= 0 for weight in weights):
        raise MetaTrainError("без ваг унікальності навчати не можна")
    matrix = _matrix(train)
    labels = np.asarray(fit_labels, dtype=np.int32)
    weight_array = _weight_array(weights)
    if lgbm is None:
        estimator = _estimator(seed, None)
        estimator.fit(matrix, labels, sample_weight=weight_array)
        best_iteration = None
    else:
        estimator, best_iteration = _fit_search(
            matrix,
            labels,
            weight_array,
            train,
            seed=seed,
            lgbm=lgbm,
            embargo_start_ms=fold.embargo_start_ms,
        )
    model = MetaFoldModel(
        fold_index=fold.index,
        columns=(*features.columns, SIDE_COLUMN),
        feature_columns=features.columns,
        schema_hash=features.schema_hash,
        preprocessing_version=features.preprocessing_version,
        estimator=estimator,
    )
    fitted = tuple(
        FittedRow(
            symbol=sample.symbol,
            time_ms=sample.trade.entry_open_time_ms,
            exit_open_time_ms=sample.trade.exit_open_time_ms,
            true_label=sample.label,
            fit_label=fit_label,
            weight=weight,
        )
        for sample, fit_label, weight in zip(train_samples, fit_labels, weights, strict=True)
    )
    test_rows = rows_for(candidates, split, SplitPurpose.TEST, fold_index=fold.index)
    reject_holdout(test_rows, split, purpose=SplitPurpose.TEST)
    # Тест — усі кандидати з валідними фічами, з міткою чи без: інакше фільтр знав би майбутні дірки.
    scored = _score(model, _join(test_rows, by_key, features), min_probability=min_probability)
    trading = _history(books, trading_symbol)
    rule = _fold_signals(trading, spec, split, fold, common.seed)
    seen = _scored_signals(trading, rule, scored, taken_only=False)
    taken = _scored_signals(trading, rule, scored, taken_only=True)
    # Звіти, а не числа: `record_trial` звірить їх з угодами і сигналами фільтра, G1 рахує з них (рішення 0017).
    runs = MetaFoldRuns(
        unfiltered=_window(trading, rule, split, fold, common),
        scored_unfiltered=_window(trading, seen, split, fold, common),
        stress=_window(trading, taken, split, fold, common, cost_factor=_STRESS_COST),
    )
    record = _filtered_record(books, spec, split, fold, scored, samples, common).model_copy(update={"meta": runs})
    return MetaFold(
        fold_index=fold.index,
        model=model,
        fitted=fitted,
        scored=scored,
        record=record,
        unfiltered_trading_net=runs.unfiltered.metrics.total_net_usd,
        scored_unfiltered_trading_net=runs.scored_unfiltered.metrics.total_net_usd,
        n_unscored_signals=len(rule) - len(seen),
        stress=fold_outcome_from_report(runs.stress, below_min_trades=fold.below_min_trades),
        best_iteration=best_iteration,
    )


def _features_for_fold(
    series: Mapping[str, SymbolData],
    split: ResearchSplit,
    fold_index: int,
    cache: dict[tuple[str, ...], FeatureRows],
) -> FeatureRows:
    start, end = train_coverage_ms(split, fold_index)
    require_train_coverage(split, fold_index, start, end)
    covered = sources_covering(series, start_ms=start, end_ms=end)
    columns = feature_columns(covered)
    found = cache.get(columns)
    if found is not None:
        return found
    rows = compute_features(series, coverage_start_ms=start, coverage_end_ms=end)
    if rows.columns != columns:
        raise RuntimeError("колонки фіч не збіглися з покриттям train")
    cache[columns] = rows
    return rows


def _filtered_record(
    books: Sequence[SymbolHistory],
    spec: StrategySpec,
    split: ResearchSplit,
    fold: Fold,
    scored: tuple[ScoredRow, ...],
    samples: Sequence[MetaSample],
    common: _Backtest,
) -> FoldRecord:
    reports = tuple(
        _window(
            history,
            _scored_signals(history, _fold_signals(history, spec, split, fold, common.seed), scored, taken_only=True),
            split,
            fold,
            common,
        )
        for history in books
    )
    return FoldRecord(
        reports=reports,
        train_labels=_label_stats(samples, split, fold, train=True),
        test_labels=_label_stats(samples, split, fold, train=False),
    )


def _window(
    history: SymbolHistory,
    signals: Sequence[EntrySignal],
    split: ResearchSplit,
    fold: Fold,
    common: _Backtest,
    cost_factor: Decimal = Decimal(1),
) -> BacktestReport:
    return backtest_fold_signals(
        history,
        signals,
        split=split,
        fold=fold,
        limits=common.limits,
        initial_equity=common.initial_equity,
        maintenance_margin_rate=common.maintenance_margin_rate,
        seed=common.seed,
        costs=common.costs,
        params=common.params,
        cost_factor=cost_factor,
    )


def _fold_signals(
    history: SymbolHistory,
    spec: StrategySpec,
    split: ResearchSplit,
    fold: Fold,
    seed: int,
) -> tuple[EntrySignal, ...]:
    signals = primary_signals(spec, history.candles, horizon_bars=split.horizon_bars, seed=seed)
    kept: list[EntrySignal] = []
    for signal in signals:
        stamp = history.candles[signal.entry_index].open_time_ms
        if fold.test_start_ms <= stamp < fold.test_end_ms:
            kept.append(signal)
    return tuple(kept)


def _scored_signals(
    history: SymbolHistory,
    signals: Sequence[EntrySignal],
    scored: Sequence[ScoredRow],
    *,
    taken_only: bool,
) -> tuple[EntrySignal, ...]:
    """Сигнали правила, які модель оцінила; `taken_only` — лише ті, що пройшли поріг."""
    by_time = {row.time_ms: row for row in scored if row.symbol == history.symbol and (row.taken or not taken_only)}
    kept: list[EntrySignal] = []
    for signal in signals:
        stamp = history.candles[signal.entry_index].open_time_ms
        row = by_time.get(stamp)
        if row is None:
            continue
        if row.side is not signal.side:
            raise RuntimeError("сторона сигналу не збігається з оціненим кандидатом")
        kept.append(signal)
    return tuple(kept)


def _label_stats(
    samples: Sequence[MetaSample],
    split: ResearchSplit,
    fold: Fold,
    *,
    train: bool,
) -> LabelStats:
    keep_row = is_train_row if train else is_test_row
    chosen = [
        sample
        for sample in samples
        if keep_row(
            SampleRow(
                symbol=sample.symbol,
                time_ms=sample.trade.entry_open_time_ms,
                index=sample.trade.entry_index,
            ),
            split,
            fold.index,
        )
    ]
    return label_diagnostics(chosen)


def _join(
    rows: Sequence[SampleRow],
    by_key: Mapping[tuple[str, int], _Candidate],
    features: FeatureRows,
) -> list[_Joined]:
    lookup: dict[tuple[str, int], int] = {}
    for index, symbol in enumerate(features.symbol):
        lookup[(symbol, int(features.entry_open_time_ms[index]))] = index
    joined: list[_Joined] = []
    for row in rows:
        candidate = by_key[(row.symbol, row.index)]
        if candidate.row != row:
            raise RuntimeError("рядок не збігається з кандидатом")
        slot = lookup.get((row.symbol, row.time_ms))
        if slot is None or not bool(features.valid[slot]):
            continue
        values = np.array(features.values[slot], dtype=np.float64, copy=True)
        if not np.isfinite(values).all():
            continue
        joined.append(_Joined(candidate=candidate, values=values))
    joined.sort(key=lambda item: (item.candidate.row.time_ms, item.candidate.row.symbol))
    return joined


def _train_sample(item: _Joined) -> MetaSample:
    sample = item.candidate.sample
    if sample is None:
        raise RuntimeError("навчальний рядок без мітки")
    return sample


def _matrix(joined: Sequence[_Joined]) -> np.ndarray:
    if not joined:
        raise MetaTrainError("порожня матриця навчання")
    width = joined[0].values.shape[0] + 1
    matrix = np.empty((len(joined), width), dtype=np.float64)
    for row_index, item in enumerate(joined):
        if item.values.shape != (width - 1,):
            raise RuntimeError("ширина фіч змінилася")
        matrix[row_index, :-1] = item.values
        matrix[row_index, -1] = 1.0 if item.candidate.side is PositionSide.LONG else 0.0
    return matrix


def _score(
    model: MetaFoldModel,
    joined: Sequence[_Joined],
    *,
    min_probability: Decimal,
) -> tuple[ScoredRow, ...]:
    if not joined:
        return ()
    probabilities = _probabilities(model.estimator, _matrix(joined))
    scored: list[ScoredRow] = []
    for item, probability in zip(joined, probabilities, strict=True):
        candidate = item.candidate
        scored.append(
            ScoredRow(
                symbol=candidate.row.symbol,
                time_ms=candidate.row.time_ms,
                entry_index=candidate.row.index,
                side=candidate.side,
                true_label=None if candidate.sample is None else candidate.sample.label,
                probability=probability,
                taken=keep(probability, min_probability=min_probability),
            )
        )
    return tuple(scored)


def _estimator(seed: int, raw: Mapping[str, Any] | None, *, objective: str = "binary") -> lgb.LGBMClassifier:
    """Класифікатор за журнальним рецептом. `objective`: `binary` для meta, `multiclass` для direct (T2.13)."""
    source = lgbm_journal_params() if raw is None else raw
    if source.get("n_jobs") != 1 or source.get("class_weight") is not None or source.get("deterministic") is not True:
        raise MetaTrainError("рецепт LightGBM змінився: очікувались n_jobs=1, deterministic і порожній class_weight")
    model = lgb.LGBMClassifier(
        objective=objective,
        boosting_type=str(source["boosting_type"]),
        num_leaves=int(source["num_leaves"]),
        max_depth=int(source["max_depth"]),
        learning_rate=float(source["learning_rate"]),
        n_estimators=int(source["n_estimators"]),
        min_child_samples=int(source["min_child_samples"]),
        min_child_weight=float(source["min_child_weight"]),
        subsample=float(source["subsample"]),
        subsample_freq=int(source["subsample_freq"]),
        colsample_bytree=float(source["colsample_bytree"]),
        reg_alpha=float(source["reg_alpha"]),
        reg_lambda=float(source["reg_lambda"]),
        min_split_gain=float(source["min_split_gain"]),
        deterministic=True,
        force_col_wise=True,
        n_jobs=1,
        class_weight=None,
        random_state=seed,
        device="cpu",
        verbosity=-1,
    )
    params = model.get_params()
    if params.get("device") != "cpu" or params.get("class_weight") is not None or params.get("n_jobs") != 1:
        raise MetaTrainError("рецепт LightGBM змінився: очікувались device=cpu, n_jobs=1 і порожній class_weight")
    return model


def _fit_search(
    matrix: np.ndarray,
    labels: np.ndarray,
    weights: np.ndarray,
    train: Sequence[_Joined],
    *,
    seed: int,
    lgbm: Mapping[str, Any],
    embargo_start_ms: int,
) -> tuple[Any, int]:
    """Навчити з ранньою зупинкою на хвості train і донавчити всі train-рядки з обраним числом дерев.

    Хвіст — частина train до embargo. Тест і hold-out у `eval_set` не потрапляють. Якщо хвоста
    не вистачає на `min_child_samples`, лишається верхня межа `n_estimators` без зупинки.
    """
    cap = int(lgbm["n_estimators"])
    parts = _early_stop_split(train, embargo_start_ms, min_child=int(lgbm["min_child_samples"]))
    if parts is None or len({int(item) for item in labels[parts[0]]}) < 2:
        estimator = _estimator(seed, lgbm)
        estimator.fit(matrix, labels, sample_weight=weights)
        return estimator, cap
    fit_idx, val_idx = parts
    stopper = _estimator(seed, lgbm)
    stopper.fit(
        matrix[fit_idx],
        labels[fit_idx],
        sample_weight=weights[fit_idx],
        eval_set=[(matrix[val_idx], labels[val_idx])],
        eval_sample_weight=[weights[val_idx]],
        callbacks=[
            lgb.early_stopping(EARLY_STOPPING_ROUNDS, verbose=False),
            lgb.log_evaluation(period=0),
        ],
    )
    best = int(getattr(stopper, "best_iteration_", 0) or 0)
    if best < 1 or best > cap:
        best = cap
    tuned = dict(lgbm)
    tuned["n_estimators"] = best
    refit = _estimator(seed, tuned)
    refit.fit(matrix, labels, sample_weight=weights)
    return refit, best


def _early_stop_split(
    train: Sequence[_Joined],
    embargo_start_ms: int,
    *,
    min_child: int,
) -> tuple[np.ndarray, np.ndarray] | None:
    count = len(train)
    val_count = int(Decimal(count) * EARLY_STOP_FRACTION)
    if val_count < 1 or count - val_count < min_child:
        return None
    val_start = count - val_count
    first_val = train[val_start].candidate.row.time_ms
    if first_val >= embargo_start_ms:
        raise MetaTrainError("рання зупинка залізла в embargo")
    fit: list[int] = []
    for index in range(val_start):
        row = train[index].candidate.row
        sample = train[index].candidate.sample
        if row.time_ms >= embargo_start_ms or sample is None:
            raise MetaTrainError("рання зупинка бачить не train")
        if sample.trade.exit_open_time_ms < first_val:
            fit.append(index)
    if len(fit) < min_child:
        return None
    for index in range(val_start, count):
        if train[index].candidate.row.time_ms >= embargo_start_ms:
            raise MetaTrainError("рання зупинка залізла в embargo")
    return np.asarray(fit, dtype=np.int64), np.asarray(range(val_start, count), dtype=np.int64)


def _probabilities(estimator: Any, matrix: np.ndarray) -> tuple[Decimal, ...]:
    classes = [int(label) for label in estimator.classes_]
    if 1 not in classes:
        raise MetaTrainError("модель не має класу TP")
    column = classes.index(1)
    raw = estimator.predict_proba(matrix)
    return tuple(_as_probability(float(row[column])) for row in raw)


def _shuffle_labels(labels: Sequence[int], seed: int, fold_index: int) -> list[int]:
    generator = np.random.Generator(np.random.PCG64(np.random.SeedSequence([seed, fold_index, 28])))
    order = generator.permutation(len(labels))
    shuffled = [labels[int(index)] for index in order]
    original = list(labels)
    if shuffled == original and len(set(original)) > 1:
        shuffled = original[1:] + original[:1]
    return shuffled


def _feature_series(
    books: Sequence[SymbolHistory],
    feature_data: Mapping[str, SymbolData] | None,
    holdout_from_ms: int,
    *,
    cutoff_ms: int | None = None,
) -> dict[str, SymbolData]:
    if feature_data is not None and set(feature_data) != {history.symbol for history in books}:
        raise ValueError("feature_data має бути на кожен символ історії")
    built: dict[str, SymbolData] = {}
    for history in books:
        candle_before = holdout_from_ms if cutoff_ms is None else cutoff_ms
        prefix = tuple(bar for bar in history.candles if bar.open_time_ms < candle_before)
        if len(prefix) < 2:
            raise ValueError("немає свічок до hold-out")
        extra = None if feature_data is None else feature_data[history.symbol]
        volumes = _volumes(extra, prefix)
        candles = tuple(
            CandleBar(
                open_time_ms=bar.open_time_ms,
                open=float(bar.open),
                high=float(bar.high),
                low=float(bar.low),
                close=float(bar.close),
                volume=volumes[bar.open_time_ms],
            )
            for bar in prefix
        )
        if extra is None:
            built[history.symbol] = SymbolData(candles=candles)
            continue
        built[history.symbol] = SymbolData(
            candles=candles,
            open_interest=_before(extra.open_interest, candle_before),
            premium=_before(extra.premium, candle_before),
            long_short=tuple(point for point in extra.long_short if point.ts_ms < candle_before),
            liquidations=tuple(bar for bar in extra.liquidations if bar.ts_ms < candle_before),
            funding=_before(extra.funding, candle_before),
            gaps=extra.gaps,
        )
    return built


def _volumes(extra: SymbolData | None, prefix: Sequence[Any]) -> dict[int, float]:
    if extra is None:
        return {bar.open_time_ms: 1.0 for bar in prefix}
    found = {bar.open_time_ms: bar.volume for bar in extra.candles}
    volumes: dict[int, float] = {}
    for bar in prefix:
        if bar.open_time_ms not in found:
            raise ValueError("у feature_data немає обсягу свічки")
        volume = float(found[bar.open_time_ms])
        if not math.isfinite(volume) or volume < 0.0:
            raise ValueError("обсяг свічки недійсний")
        volumes[bar.open_time_ms] = volume
    return volumes


def _before(points: Sequence[TimedValue], holdout_from_ms: int) -> tuple[TimedValue, ...]:
    return tuple(point for point in points if point.ts_ms < holdout_from_ms)


def _candidates(
    spec: StrategySpec,
    history: SymbolHistory,
    split: ResearchSplit,
    target: TargetParams,
    costs: CostModel,
    seed: int,
    *,
    candle_before_ms: int | None = None,
    decision_before_ms: int | None = None,
) -> tuple[_Candidate, ...]:
    candle_cut = split.holdout_from_ms if candle_before_ms is None else candle_before_ms
    decision_cut = split.decision_end_ms if decision_before_ms is None else decision_before_ms
    prefix = tuple(bar for bar in history.candles if bar.open_time_ms < candle_cut)
    found = tuple(
        candidate
        for candidate in primary_candidates(spec, prefix, symbol=history.symbol, seed=seed)
        if prefix[candidate.entry_index].open_time_ms < decision_cut
    )
    marks = None
    if history.mark_candles is not None:
        marks = tuple(bar for bar in history.mark_candles if bar.open_time_ms < candle_cut)
    funding = tuple(event for event in history.funding if event.time_ms < candle_cut)
    labeled = label_meta(prefix, found, params=target, costs=costs, mark_candles=marks, funding=funding)
    if len(labeled.samples) + len(labeled.skipped) != len(found):
        raise RuntimeError("label_meta загубив кандидата")
    by_index = {sample.trade.entry_index: sample for sample in labeled.samples}
    result: list[_Candidate] = []
    for candidate in found:
        time_ms = prefix[candidate.entry_index].open_time_ms
        sample = by_index.get(candidate.entry_index)
        if sample is not None and (
            sample.trade.entry_open_time_ms != time_ms or sample.trade.side is not candidate.side
        ):
            raise RuntimeError("мітка не збігається з кандидатом")
        row = SampleRow(symbol=history.symbol, time_ms=time_ms, index=candidate.entry_index)
        result.append(_Candidate(row=row, side=candidate.side, sample=sample))
    return tuple(result)


def _books(histories: Sequence[SymbolHistory], trading_symbol: str) -> tuple[SymbolHistory, ...]:
    if not isinstance(histories, Sequence) or isinstance(histories, (str, bytes)) or not histories:
        raise ValueError("потрібна хоча б одна історія")
    if type(trading_symbol) is not str or not trading_symbol:
        raise ValueError("торговий символ порожній")
    books = tuple(histories)
    symbols = [history.symbol for history in books]
    if len(set(symbols)) != len(symbols):
        raise ValueError("символ історії повторився")
    if trading_symbol not in symbols:
        raise ValueError("торгового символу немає в історіях")
    for history in books:
        if type(history) is not SymbolHistory:
            raise ValueError("історія має бути SymbolHistory")
    return books


def _history(books: Sequence[SymbolHistory], symbol: str) -> SymbolHistory:
    return next(history for history in books if history.symbol == symbol)


def _spec(spec: StrategySpec) -> None:
    if type(spec) is not StrategySpec or spec not in PRIMARY_CANDIDATES:
        raise ValueError("meta-модель учиться лише на зафіксованому кандидаті")


def _seed(seed: int) -> None:
    if type(seed) is not int or isinstance(seed, bool) or seed < 0:
        raise ValueError("seed має бути невід'ємний int")


def _probability_threshold(value: Decimal) -> None:
    require_decimal("min_probability", value)
    if value < 0 or value > 1:
        raise ValueError("поріг імовірності поза [0, 1]")


def _trading_net(record: FoldRecord, trading_symbol: str) -> Decimal:
    return next(report.metrics.total_net_usd for report in record.reports if report.symbol == trading_symbol)


def _weight_array(weights: Sequence[Decimal]) -> np.ndarray:
    return np.asarray([float(weight) for weight in weights], dtype=np.float64)


def _as_probability(value: float) -> Decimal:
    if not math.isfinite(value):
        raise MetaTrainError("ймовірність моделі не скінченна")
    quantized = Decimal(format(value, ".12f")).quantize(_PROBABILITY_QUANTUM)
    if quantized < 0 or quantized > 1:
        raise MetaTrainError("ймовірність моделі поза [0, 1]")
    return quantized


def _mean(values: Sequence[Decimal]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))
