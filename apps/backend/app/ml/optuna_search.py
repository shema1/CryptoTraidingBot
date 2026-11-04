"""Підбір гіперпараметрів LightGBM як задача воркера (ТЗ §5.6, §5А.7, T2.10).

Таргет і первинне правило не перебираються: правило — уже обраний `StrategySpec`, таргет — `TargetParams`.
Метрика спроби — медіана чистого результату торгового символу по фолдах після витрат мінус розкид
(ТЗ §5А.7). Не точність і не найкращий фолд; кількість угод перевіряє Gate G1, не метрика.

Кожна спроба, включно з `PRUNED` і `FAILED`, пишеться в `experiment_trials` до наступної. Спроба,
перервана скасуванням, SIGTERM або втратою lock, не пишеться: її результат нікуди не потрапив.
Hold-out не відкривається. N для DSR — `line_trial_count` лінії, не лічильник одного експерименту.

Сховище Optuna не використовується: відновлення програє журнал у новий study. TPE бачить уже записані
спроби, але генератор семплера не відновлюється біт-у-біт: seed зсувається на кількість записаних спроб,
інакше випадковий старт повторив би вже записані точки. Номерів-дублів немає: наступний номер —
довжина журналу, і розрив у номерах зупиняє задачу.
"""

from __future__ import annotations

import asyncio
import re
import threading
import uuid
from bisect import bisect_right
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import optuna
from optuna.distributions import BaseDistribution, FloatDistribution, IntDistribution
from optuna.pruners import MedianPruner
from optuna.samplers import TPESampler
from optuna.trial import TrialState, create_trial
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.db import SYMBOL_RE
from app.core.logging import get_logger
from app.data.gaps import GapRange
from app.data.models import (
    Candle,
    FundingRate,
    LiquidationsHourly,
    LongShortRatio,
    MarkIndexCandle,
    OpenInterest,
    PremiumIndex,
)
from app.exchange.models import Instrument as StoredInstrument
from app.exchange.types import Instrument
from app.jobs.models import Job
from app.ml.features import (
    HOUR_MS,
    CandleBar,
    LiquidationBar,
    LongShortPoint,
    SymbolData,
    TimedValue,
    load_feature_gaps,
)
from app.ml.lgbm_space import (
    EARLY_STOPPING_ROUNDS,
    SEARCH_BAGGING,
    SEARCH_LEARNING_RATE,
    SEARCH_MAX_DEPTH,
    SEARCH_MIN_CHILD_SAMPLES,
    SEARCH_N_ESTIMATORS,
    SEARCH_NUM_LEAVES,
    SEARCH_PARAM_NAMES,
    SEARCH_REG_LAMBDA,
    lgbm_search_record,
)
from app.ml.models import Experiment, ExperimentTrial
from app.ml.strategy import PRIMARY_CANDIDATES, StrategySpec, SymbolHistory, executed_trade_count
from app.ml.trade_sim import SimCandle, TargetParams
from app.ml.validation import (
    ResearchSplit,
    SplitLeakError,
    build_split,
    costs_payload,
    create_experiment,
    holdout_bounds,
    record_trial,
    split_from_payload,
    target_payload,
    validation_payload,
)
from app.risk.costs import CostModel, FundingEvent
from app.risk.limits import RiskLimits
from app.risk.numbers import fixed_context, require_decimal

_LOG = get_logger("app.ml.optuna_search")
_SYMBOL = re.compile(SYMBOL_RE)

JOB_KIND_OPTUNA: str = "optuna_meta"
# Більше число — пізніше за backfill (200) і деривативи (210): черга свічок не чекає пошук.
OPTUNA_JOB_PRIORITY: int = 300
OPTUNA_TRIALS_MIN: int = 100
OPTUNA_TRIALS_MAX: int = 200
OBJECTIVE_NAME: str = "median_fold_net_minus_spread"
SPREAD_PENALTY: Decimal = Decimal(1)
_EXCHANGE: str = "bybit"
_TIMEFRAME: str = "1h"
_INSTRUMENT_MODE: str = "demo"
_ACTIVE: tuple[str, ...] = ("QUEUED", "RUNNING")
_MONEY_KEYS: tuple[str, ...] = (
    "initial_equity",
    "maintenance_margin_rate",
    "min_equity_usd",
    "max_notional_per_symbol_usd",
)
_JOB_KEYS: frozenset[str] = frozenset(
    {"strategy", "n_trials", "seed", "symbols", "trading_symbol", "experiment_id", *_MONEY_KEYS}
)
_FORBIDDEN_KEYS: frozenset[str] = frozenset(
    {
        "tp_atr_mult",
        "sl_atr_mult",
        "horizon_bars",
        "fast",
        "slow",
        "lookback",
        "device",
        "class_weight",
        *SEARCH_PARAM_NAMES,
    }
)

SessionFactory = async_sessionmaker[AsyncSession]
CreatedHook = Callable[[AsyncSession, str], Awaitable[None]]
StopHook = Callable[[], Awaitable[bool]]
ProgressHook = Callable[[int, int], Awaitable[None]]


class OptunaParamsError(ValueError):
    """Параметри задачі не з контракту. Воркер записує це як `BAD_PARAMS`."""


class OptunaDataError(RuntimeError):
    """Історії або журналу не вистачає, щоб продовжити той самий експеримент. Новий вхід не починається."""


@dataclass(frozen=True, slots=True)
class OptunaJobRequest:
    """Розібрані параметри задачі. Правило — фіксований кандидат, не його періоди."""

    strategy: StrategySpec
    n_trials: int
    seed: int
    symbols: tuple[str, ...]
    trading_symbol: str
    initial_equity: Decimal
    maintenance_margin_rate: Decimal
    min_equity_usd: Decimal
    max_notional_per_symbol_usd: Decimal
    experiment_id: str | None = None


@dataclass(frozen=True, slots=True)
class OptunaInputs:
    """Історії і розбивка, з якими воркер кличе study. Розбивка відновлення — зі збереженого payload."""

    histories: tuple[SymbolHistory, ...]
    feature_data: dict[str, SymbolData]
    split: ResearchSplit


@dataclass(frozen=True, slots=True)
class OptunaResult:
    """Скільки спроб дописано цим проходом. `stopped` — задачу зняли між спробами."""

    experiment_id: str
    trials_recorded: int
    trials_total: int
    stopped: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "trials_recorded": self.trials_recorded,
            "trials_total": self.trials_total,
            "stopped": self.stopped,
        }


@dataclass(frozen=True, slots=True)
class _SymbolRows:
    """Ряди одного символу з БД до зрізу hold-out і вирівнювання mark. Свічка — разом з обсягом."""

    symbol: str
    candles: tuple[tuple[SimCandle, Decimal], ...]
    instrument: Instrument
    marks: tuple[SimCandle, ...]
    payments: tuple[tuple[int, Decimal], ...]
    open_interest: tuple[TimedValue, ...]
    premium: tuple[TimedValue, ...]
    long_short: tuple[LongShortPoint, ...]
    liquidations: tuple[LiquidationBar, ...]
    gaps: Mapping[str, tuple[GapRange, ...]]


@dataclass(slots=True)
class _TrialProbe:
    """`stop_after` для `train_meta`: метрика префікса, pruner і зупинка задачі. Викликається з потоку навчання.

    `halt` ставить event loop, коли задачу зняли; потік тоді не береться за наступний фолд.
    """

    trial: Any
    trading_symbol: str
    last_fold: int
    halt: threading.Event = field(default_factory=threading.Event)
    nets: list[Decimal] = field(default_factory=list)
    steps: list[Decimal] = field(default_factory=list)

    def __call__(self, fold: Any) -> bool:
        self.nets.append(_fold_net(fold, self.trading_symbol))
        score = objective_score(self.nets)
        self.steps.append(score)
        self.trial.report(float(score), step=fold.fold_index)
        if self.halt.is_set():
            return True
        if fold.fold_index == self.last_fold:
            return False
        return bool(self.trial.should_prune())


@dataclass(frozen=True, slots=True)
class _Replay:
    number: int
    state: str
    params: dict[str, float | int]
    value: float | None
    steps: dict[int, float]


@fixed_context
def objective_score(fold_nets: Sequence[Decimal]) -> Decimal:
    """Медіана чистого результату по фолдах мінус розкид (ТЗ §5А.7), нічого більше.

    Парна кількість фолдів — середнє двох центральних, як у виборі первинного правила.
    Один фолд розкиду не має. Штрафу за кількість угод немає: множник від медіани робив би
    кращою модель без угод, ніж прибуткову, а кількість угод перевіряє Gate G1.
    """
    if not isinstance(fold_nets, Sequence) or isinstance(fold_nets, (str, bytes)) or not fold_nets:
        raise ValueError("метрика Optuna потребує результат кожного фолду")
    for net in fold_nets:
        require_decimal("fold_net", net)
    ordered = sorted(fold_nets)
    mid = len(ordered) // 2
    median = ordered[mid] if len(ordered) % 2 == 1 else (ordered[mid - 1] + ordered[mid]) / 2
    if len(fold_nets) == 1:
        spread = Decimal(0)
    else:
        mean = sum(fold_nets, Decimal(0)) / Decimal(len(fold_nets))
        variance = sum((item - mean) ** 2 for item in fold_nets) / Decimal(len(fold_nets) - 1)
        spread = variance.sqrt()
    return median - SPREAD_PENALTY * spread


def suggest_lgbm(trial: optuna.trial.Trial) -> dict[str, Any]:
    """Одна точка діапазонів ТЗ §5А.7. Фіксовані поля рецепта додає `lgbm_search_record`."""
    low_rate, high_rate = float(SEARCH_LEARNING_RATE[0]), float(SEARCH_LEARNING_RATE[1])
    low_bag, high_bag = float(SEARCH_BAGGING[0]), float(SEARCH_BAGGING[1])
    return lgbm_search_record(
        {
            "num_leaves": trial.suggest_int("num_leaves", *SEARCH_NUM_LEAVES),
            "max_depth": trial.suggest_int("max_depth", *SEARCH_MAX_DEPTH),
            "learning_rate": trial.suggest_float("learning_rate", low_rate, high_rate, log=True),
            "n_estimators": trial.suggest_int("n_estimators", *SEARCH_N_ESTIMATORS),
            "min_child_samples": trial.suggest_int("min_child_samples", *SEARCH_MIN_CHILD_SAMPLES),
            "subsample": trial.suggest_float("subsample", low_bag, high_bag),
            "colsample_bytree": trial.suggest_float("colsample_bytree", low_bag, high_bag),
            "reg_lambda": trial.suggest_float(
                "reg_lambda", float(SEARCH_REG_LAMBDA[0]), float(SEARCH_REG_LAMBDA[1]), log=False
            ),
        }
    )


def lgbm_distributions() -> dict[str, BaseDistribution]:
    """Ті самі межі, що `suggest_lgbm`: інакше відтворення журналу не прийме точку."""
    low_rate, high_rate = float(SEARCH_LEARNING_RATE[0]), float(SEARCH_LEARNING_RATE[1])
    low_bag, high_bag = float(SEARCH_BAGGING[0]), float(SEARCH_BAGGING[1])
    space: dict[str, BaseDistribution] = {
        "num_leaves": IntDistribution(*SEARCH_NUM_LEAVES),
        "max_depth": IntDistribution(*SEARCH_MAX_DEPTH),
        "learning_rate": FloatDistribution(low_rate, high_rate, log=True),
        "n_estimators": IntDistribution(*SEARCH_N_ESTIMATORS),
        "min_child_samples": IntDistribution(*SEARCH_MIN_CHILD_SAMPLES),
        "subsample": FloatDistribution(low_bag, high_bag),
        "colsample_bytree": FloatDistribution(low_bag, high_bag),
        "reg_lambda": FloatDistribution(float(SEARCH_REG_LAMBDA[0]), float(SEARCH_REG_LAMBDA[1]), log=False),
    }
    return space


def search_space_payload() -> dict[str, Any]:
    """Діапазони, які експеримент зберігає. Жодного float: журнал його не приймає."""
    return {
        "num_leaves": list(SEARCH_NUM_LEAVES),
        "max_depth": list(SEARCH_MAX_DEPTH),
        "learning_rate": [format(SEARCH_LEARNING_RATE[0], "f"), format(SEARCH_LEARNING_RATE[1], "f")],
        "learning_rate_log": True,
        "n_estimators": list(SEARCH_N_ESTIMATORS),
        "min_child_samples": list(SEARCH_MIN_CHILD_SAMPLES),
        "subsample": [format(SEARCH_BAGGING[0], "f"), format(SEARCH_BAGGING[1], "f")],
        "colsample_bytree": [format(SEARCH_BAGGING[0], "f"), format(SEARCH_BAGGING[1], "f")],
        "reg_lambda": [format(SEARCH_REG_LAMBDA[0], "f"), format(SEARCH_REG_LAMBDA[1], "f")],
        "reg_lambda_log": False,
        "early_stopping_rounds": EARLY_STOPPING_ROUNDS,
        "metric": OBJECTIVE_NAME,
    }


def parse_optuna_job(params: Mapping[str, Any]) -> OptunaJobRequest:
    """Параметри задачі. Зайвий ключ, період правила або гіперпараметр моделі — відмова.

    Бюджет тут завжди 100–200. Тісніший бюджет є лише в `run_optuna_study` для тестів і в задачу не просочується.
    """
    if not isinstance(params, Mapping):
        raise OptunaParamsError("параметри задачі мають бути об'єктом")
    unknown = set(params) - _JOB_KEYS
    forbidden = set(params) & _FORBIDDEN_KEYS
    if unknown or forbidden:
        raise OptunaParamsError("параметри задачі містять ключ поза контрактом Optuna")
    missing = (_JOB_KEYS - {"experiment_id"}) - set(params)
    if missing:
        raise OptunaParamsError("у параметрах задачі бракує поля")
    n_trials = params["n_trials"]
    seed = params["seed"]
    if type(n_trials) is not int or isinstance(n_trials, bool):
        raise OptunaParamsError("n_trials має бути int")
    if n_trials < OPTUNA_TRIALS_MIN or n_trials > OPTUNA_TRIALS_MAX:
        raise OptunaParamsError("n_trials має бути від 100 до 200")
    if type(seed) is not int or isinstance(seed, bool) or seed < 0:
        raise OptunaParamsError("seed має бути невід'ємний int")
    symbols = _symbols(params["symbols"])
    trading = params["trading_symbol"]
    if type(trading) is not str or trading not in symbols:
        raise OptunaParamsError("trading_symbol має бути одним із symbols")
    experiment_id = params.get("experiment_id")
    if experiment_id is not None and (type(experiment_id) is not str or not experiment_id):
        raise OptunaParamsError("experiment_id має бути непорожній рядок")
    return OptunaJobRequest(
        strategy=_strategy(params["strategy"]),
        n_trials=n_trials,
        seed=seed,
        symbols=symbols,
        trading_symbol=trading,
        initial_equity=_money("initial_equity", params["initial_equity"]),
        maintenance_margin_rate=_rate(params["maintenance_margin_rate"]),
        min_equity_usd=_money("min_equity_usd", params["min_equity_usd"]),
        max_notional_per_symbol_usd=_money("max_notional_per_symbol_usd", params["max_notional_per_symbol_usd"]),
        experiment_id=experiment_id,
    )


def job_params(request: OptunaJobRequest) -> dict[str, Any]:
    """JSON задачі. Гроші — рядки Decimal, правило — лише ім'я."""
    stored: dict[str, Any] = {
        "strategy": request.strategy.name,
        "n_trials": request.n_trials,
        "seed": request.seed,
        "symbols": list(request.symbols),
        "trading_symbol": request.trading_symbol,
        "initial_equity": format(request.initial_equity, "f"),
        "maintenance_margin_rate": format(request.maintenance_margin_rate, "f"),
        "min_equity_usd": format(request.min_equity_usd, "f"),
        "max_notional_per_symbol_usd": format(request.max_notional_per_symbol_usd, "f"),
    }
    if request.experiment_id is not None:
        stored["experiment_id"] = request.experiment_id
    parse_optuna_job(stored)
    return stored


async def enqueue_optuna_job(session: AsyncSession, request: OptunaJobRequest) -> uuid.UUID:
    """Поставити пошук, якщо для цього символу ще немає активної задачі. Повертає id нової або вже активної."""
    if request.experiment_id is not None:
        raise OptunaParamsError("нова задача не посилається на чужий експеримент: це продовження")
    existing = await _active_job(session, request.trading_symbol)
    if existing is not None:
        return existing
    job = Job(
        kind=JOB_KIND_OPTUNA,
        priority=OPTUNA_JOB_PRIORITY,
        params=job_params(request),
        dedup_key=_dedup(request.trading_symbol),
        progress_message=None,
        result=None,
        error=None,
    )
    try:
        async with session.begin_nested():
            session.add(job)
            await session.flush()
    except IntegrityError:
        found = await _active_job(session, request.trading_symbol)
        if found is None:
            raise
        return found
    return _uuid(job.id)


async def resume_optuna_job(session: AsyncSession, job_id: uuid.UUID) -> uuid.UUID:
    """Нова `QUEUED` задача з тими самими параметрами, включно з `experiment_id`.

    Лише з `CANCELED` або `FAILED`. `DONE` не повторюється. Якщо пошук цього символу вже в черзі
    або виконується, повертається він, без другого рядка.
    """
    job = await session.get(Job, job_id)
    if job is None or job.kind != JOB_KIND_OPTUNA:
        raise OptunaParamsError("це не задача Optuna")
    request = parse_optuna_job(job.params)
    active = await _active_job(session, request.trading_symbol)
    if active is not None:
        return active
    if job.status not in ("CANCELED", "FAILED"):
        raise OptunaParamsError("продовжити можна лише скасовану або зірвану задачу")
    created = Job(
        kind=JOB_KIND_OPTUNA,
        priority=OPTUNA_JOB_PRIORITY,
        params=job_params(request),
        dedup_key=_dedup(request.trading_symbol),
        progress_message=None,
        result=None,
        error=None,
    )
    try:
        async with session.begin_nested():
            session.add(created)
            await session.flush()
    except IntegrityError:
        found = await _active_job(session, request.trading_symbol)
        if found is None:
            raise
        return found
    return _uuid(created.id)


async def load_optuna_inputs(sessions: SessionFactory, request: OptunaJobRequest) -> OptunaInputs:
    """Свічки, funding, деривативи і розбивка. Інструмент — лише `demo`. Немає ряду — відмова, не live-кеш.

    Нова задача будує розбивку один раз. Продовження читає `params.validation` і не кличе `build_split`:
    нові свічки інакше зсунули б вікно, і журнал відхилив би спробу як витік.
    БД читається однією короткою транзакцією. Далі все рахується в потоці поза транзакцією
    (`build_split` проганяє бектести): event loop воркера тим часом оновлює heartbeat.
    Ряди ріжуться по `holdout_from_ms` до вирівнювання mark і цін funding (`_assemble`).
    """
    target = TargetParams()
    stored: Mapping[str, Any] | None = None
    async with sessions() as session, session.begin():
        rows = await _load_rows(session, request.symbols)
        if request.experiment_id is not None:
            experiment = await _experiment(session, request.experiment_id)
            # Задача воркера вчить з витратами за замовчуванням, як і `run_optuna_search(costs=None)`.
            _match_experiment(experiment, request, target, CostModel())
            validation = experiment.params.get("validation")
            if not isinstance(validation, Mapping):
                raise OptunaDataError("у експерименту немає розбивки")
            stored = validation
    if stored is None:
        return await asyncio.to_thread(_prepare_new, rows, request, target)
    split = split_from_payload(stored)
    histories, features = await asyncio.to_thread(_assemble, rows, split.holdout_from_ms)
    return OptunaInputs(histories=histories, feature_data=features, split=split)


def _prepare_new(rows: Sequence[_SymbolRows], request: OptunaJobRequest, target: TargetParams) -> OptunaInputs:
    """Нова розбивка. Межу hold-out дає лише спільне вікно свічок; ряди ріжуться по ній до бектестів розбивки."""
    start = max(item.candles[0][0].open_time_ms for item in rows)
    end = min(item.candles[-1][0].open_time_ms for item in rows) + HOUR_MS
    if end <= start:
        raise OptunaDataError("історії символів не перетинаються")
    try:
        cut, _holdout_to = holdout_bounds(start, end)
    except ValueError as exc:
        raise OptunaDataError("з наявної історії не виходить розбивка") from exc
    histories, features = _assemble(rows, cut)
    split = _new_split(histories, request, target, data_from_ms=start, data_to_ms=end)
    if split.holdout_from_ms != cut:
        raise OptunaDataError("межа hold-out розбивки не збіглась зі зрізом рядів")
    return OptunaInputs(histories=histories, feature_data=features, split=split)


def _new_split(
    histories: Sequence[SymbolHistory],
    request: OptunaJobRequest,
    target: TargetParams,
    *,
    data_from_ms: int,
    data_to_ms: int,
) -> ResearchSplit:
    trading = next(item for item in histories if item.symbol == request.trading_symbol)
    try:
        return build_split(
            data_from_ms=data_from_ms,
            data_to_ms=data_to_ms,
            horizon_bars=target.horizon_bars,
            count_trades=executed_trade_count(
                trading,
                horizon_bars=target.horizon_bars,
                limits=_limits(request),
                initial_equity=request.initial_equity,
                maintenance_margin_rate=request.maintenance_margin_rate,
                seed=request.seed,
                params=target,
                specs=(request.strategy,),
            ),
        )
    except (ValueError, SplitLeakError) as exc:
        raise OptunaDataError("з наявної історії не виходить розбивка") from exc


async def remember_experiment(session: AsyncSession, job_id: uuid.UUID, experiment_id: str) -> None:
    """Записати `experiment_id` у ту саму транзакцію, що й створення експерименту.

    Рядок уже не `RUNNING` — відмова, і транзакція викликача відкочує експеримент разом із посиланням.
    """
    job = await session.get(Job, job_id)
    if job is None or job.status != "RUNNING" or job.kind != JOB_KIND_OPTUNA:
        raise OptunaDataError("задачу Optuna вже знято")
    params = dict(job.params)
    current = params.get("experiment_id")
    if current is not None and current != experiment_id:
        raise OptunaParamsError("у задачі вже інший експеримент")
    params["experiment_id"] = experiment_id
    parse_optuna_job(params)
    job.params = params


async def run_optuna_study(
    sessions: SessionFactory,
    histories: Sequence[SymbolHistory],
    *,
    spec: StrategySpec,
    split: ResearchSplit,
    trading_symbol: str,
    limits: RiskLimits,
    initial_equity: Decimal,
    maintenance_margin_rate: Decimal,
    seed: int,
    n_trials: int,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    feature_data: Mapping[str, SymbolData] | None = None,
    experiment_id: str | None = None,
    on_created: CreatedHook | None = None,
    should_stop: StopHook | None = None,
    on_progress: ProgressHook | None = None,
    trial_bounds: tuple[int, int] = (OPTUNA_TRIALS_MIN, OPTUNA_TRIALS_MAX),
    sampler: optuna.samplers.BaseSampler | None = None,
    pruner: optuna.pruners.BasePruner | None = None,
) -> OptunaResult:
    """Пройти бюджет ask/tell. Кожна спроба комітиться окремо, перш ніж питати наступну.

    Скасування посеред навчання не пише перервану спробу: потік довчає поточний фолд і зупиняється.
    `trial_bounds` звужує бюджет лише для тестів. Задача воркера його не передає, тож лишається 100–200.
    `sampler` і `pruner` теж для тестів: задача бере TPE із seed і медіанний pruner.
    """
    from app.ml.meta_model import META_MIN_PROBABILITY, MetaRun, MetaTrainError, train_meta

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _budget(n_trials, trial_bounds)
    if type(spec) is not StrategySpec or spec not in PRIMARY_CANDIDATES:
        raise OptunaParamsError("Optuna вчиться лише на зафіксованому кандидаті")
    target = params if params is not None else TargetParams()
    if type(target) is not TargetParams:
        raise OptunaParamsError("таргет має бути TargetParams")
    model_costs = costs if costs is not None else CostModel()
    if type(model_costs) is not CostModel:
        raise OptunaParamsError("витрати мають бути CostModel")
    books = tuple(_trim_history(item, split.holdout_from_ms) for item in histories)
    features = None
    if feature_data is not None:
        features = {key: _trim_features(item, split.holdout_from_ms) for key, item in feature_data.items()}
    created = await _ensure_experiment(
        sessions,
        books,
        spec=spec,
        split=split,
        trading_symbol=trading_symbol,
        seed=seed,
        target=target,
        costs=model_costs,
        experiment_id=experiment_id,
        on_created=on_created,
    )
    replayed = await _load_replay(sessions, created)
    if len(replayed) >= n_trials:
        return OptunaResult(experiment_id=created, trials_recorded=0, trials_total=len(replayed), stopped=False)
    study = _study(seed, replayed, sampler=sampler, pruner=pruner)
    recorded = 0
    total_budget = n_trials
    while len(replayed) + recorded < total_budget:
        if should_stop is not None and await should_stop():
            return OptunaResult(
                experiment_id=created,
                trials_recorded=recorded,
                trials_total=len(replayed) + recorded,
                stopped=True,
            )
        expected = len(replayed) + recorded
        trial = study.ask()
        if trial.number != expected:
            raise OptunaDataError("Optuna видала номер, який не продовжує журнал")
        sampled = suggest_lgbm(trial)
        probe = _TrialProbe(trial=trial, trading_symbol=trading_symbol, last_fold=split.folds[-1].index)
        steps = probe.steps
        try:
            run = await _train(
                train_meta,
                books,
                spec=spec,
                split=split,
                trading_symbol=trading_symbol,
                limits=limits,
                initial_equity=initial_equity,
                maintenance_margin_rate=maintenance_margin_rate,
                seed=seed,
                costs=model_costs,
                params=target,
                feature_data=features,
                lgbm=sampled,
                stop_after=probe,
            )
        except asyncio.CancelledError:
            # `cancel_job`, SIGTERM або втрата lock. Потік ззовні не зупинити: він довчить лише поточний фолд.
            # Спроба не пишеться: зупинка — не довше `BODY_STOP_TIMEOUT_MS`, без lock писати не можна,
            # а її результат ніхто не бачив, тож N не занижене. Продовження спитає цей номер знову.
            probe.halt.set()
            _LOG.warning("optuna_trial_interrupted", experiment_id=created, number=expected)
            raise
        except MetaTrainError as exc:
            partial = MetaRun(
                spec=spec,
                split=split,
                seed=seed,
                trading_symbol=trading_symbol,
                shuffled_labels=False,
                min_probability=META_MIN_PROBABILITY,
                folds=exc.completed_folds,
                lgbm=sampled,
            )
            await _write_trial(
                sessions,
                experiment_id=created,
                split=split,
                state="FAILED",
                number=expected,
                value=None,
                steps=steps,
                params=partial.trial_params(),
                folds=partial.records,
            )
            study.tell(trial, state=TrialState.FAIL)
        else:
            if len(run.folds) < len(split.folds):
                await _write_trial(
                    sessions,
                    experiment_id=created,
                    split=split,
                    state="PRUNED",
                    number=expected,
                    value=None,
                    steps=steps,
                    params=run.trial_params(),
                    folds=run.records,
                )
                study.tell(trial, state=TrialState.PRUNED)
            else:
                score = steps[-1]
                await _write_trial(
                    sessions,
                    experiment_id=created,
                    split=split,
                    state="COMPLETE",
                    number=expected,
                    value=score,
                    steps=steps,
                    params=run.trial_params(),
                    folds=run.records,
                )
                study.tell(trial, values=float(score))
        recorded += 1
        if on_progress is not None:
            await on_progress(len(replayed) + recorded, total_budget)
    return OptunaResult(
        experiment_id=created,
        trials_recorded=recorded,
        trials_total=len(replayed) + recorded,
        stopped=False,
    )


def _study(
    seed: int,
    replayed: Sequence[_Replay],
    *,
    sampler: optuna.samplers.BaseSampler | None,
    pruner: optuna.pruners.BasePruner | None,
) -> optuna.Study:
    """Study в пам'яті з уже записаними спробами журналу.

    Seed TPE зсувається на кількість записаних спроб. Новий `TPESampler(seed)` починав би випадковий старт
    спочатку і, поки в журналі менше `n_startup_trials` (10) завершених спроб, повторював би точки 0, 1, 2…,
    тож журнал отримував би копії, а бюджет пошуку меншав. Перервана, не записана спроба за того самого
    журналу отримує ту саму точку знову.
    """
    study = optuna.create_study(
        direction="maximize",
        sampler=TPESampler(seed=(seed + len(replayed)) % 2**32) if sampler is None else sampler,
        pruner=MedianPruner(n_startup_trials=5, n_warmup_steps=1) if pruner is None else pruner,
    )
    distributions = lgbm_distributions()
    for item in replayed:
        study.add_trial(
            create_trial(
                state=_optuna_state(item.state),
                value=item.value,
                params=item.params,
                distributions=distributions,
                intermediate_values=item.steps,
            )
        )
    return study


def _budget(n_trials: int, bounds: tuple[int, int]) -> None:
    if (
        type(n_trials) is not int
        or isinstance(n_trials, bool)
        or len(bounds) != 2
        or type(bounds[0]) is not int
        or type(bounds[1]) is not int
        or isinstance(bounds[0], bool)
        or isinstance(bounds[1], bool)
        or bounds[0] < 1
        or bounds[0] > bounds[1]
        or n_trials < bounds[0]
        or n_trials > bounds[1]
    ):
        raise OptunaParamsError("n_trials поза бюджетом цього проходу")


def _strategy(name: object) -> StrategySpec:
    if type(name) is not str:
        raise OptunaParamsError("стратегія задачі має бути рядком")
    found = [item for item in PRIMARY_CANDIDATES if item.name == name]
    if len(found) != 1:
        raise OptunaParamsError("стратегія Optuna — ema_cross або breakout")
    return found[0]


def _symbols(value: object) -> tuple[str, ...]:
    if type(value) is not list or not value:
        raise OptunaParamsError("symbols має бути непорожній список")
    symbols: list[str] = []
    for item in value:
        if type(item) is not str or _SYMBOL.fullmatch(item) is None:
            raise OptunaParamsError("символ не канонічний")
        symbols.append(item)
    if len(set(symbols)) != len(symbols):
        raise OptunaParamsError("символ у списку повторився")
    return tuple(symbols)


def _money(name: str, value: object) -> Decimal:
    if type(value) is not str:
        raise OptunaParamsError(f"{name} має бути рядком Decimal")
    try:
        number = Decimal(value)
    except ArithmeticError as exc:
        raise OptunaParamsError(f"{name} не читається") from exc
    require_decimal(name, number, positive=True)
    return number


def _rate(value: object) -> Decimal:
    number = _money("maintenance_margin_rate", value)
    if number >= 1:
        raise OptunaParamsError("maintenance_margin_rate має бути меншим за 1")
    return number


def _dedup(trading_symbol: str) -> str:
    return f"optuna:{trading_symbol}"


def _limits(request: OptunaJobRequest) -> RiskLimits:
    return RiskLimits(
        min_equity_usd=request.min_equity_usd,
        max_notional_per_symbol_usd=request.max_notional_per_symbol_usd,
    )


def _uuid(value: uuid.UUID) -> uuid.UUID:
    return uuid.UUID(int=value.int)


async def _active_job(session: AsyncSession, trading_symbol: str) -> uuid.UUID | None:
    found = await session.scalar(select(Job.id).where(Job.dedup_key == _dedup(trading_symbol), Job.status.in_(_ACTIVE)))
    if found is None:
        return None
    return _uuid(found)


async def _experiment(session: AsyncSession, experiment_id: str) -> Experiment:
    found = await session.get(Experiment, experiment_id)
    if found is None:
        raise OptunaDataError("експерименту Optuna немає")
    return found


def _match_experiment(
    experiment: Experiment, request: OptunaJobRequest, target: TargetParams, costs: CostModel
) -> None:
    if experiment.label_type != "meta" or experiment.research_line != request.trading_symbol:
        raise OptunaParamsError("експеримент не цієї meta-лінії")
    if list(experiment.symbols) != list(request.symbols):
        raise OptunaParamsError("символи експерименту не збігаються із задачею")
    if experiment.seed != request.seed:
        raise OptunaParamsError("seed експерименту не збігається із задачею")
    if experiment.primary_strategy != request.strategy.metadata():
        raise OptunaParamsError("правило експерименту не збігається із задачею")
    if experiment.params.get("target") != target_payload(target):
        raise OptunaParamsError("таргет експерименту не збігається із задачею")
    if experiment.params.get("costs") != costs_payload(costs):
        raise OptunaParamsError("витрати експерименту не збігаються із задачею")
    if experiment.params.get("optuna_search") != search_space_payload():
        raise OptunaParamsError("діапазони пошуку експерименту не збігаються з ТЗ §5А.7")


async def _load_rows(session: AsyncSession, symbols: Sequence[str]) -> tuple[_SymbolRows, ...]:
    loaded: list[_SymbolRows] = []
    for symbol in symbols:
        candles = await _candle_rows(session, symbol)
        if not candles:
            raise OptunaDataError("немає свічок для символу задачі")
        start = candles[0][0].open_time_ms
        end = candles[-1][0].open_time_ms + HOUR_MS
        loaded.append(
            _SymbolRows(
                symbol=symbol,
                candles=tuple(candles),
                instrument=await _instrument(session, symbol),
                marks=tuple(await _mark_candles(session, symbol)),
                payments=await _funding_payments(session, symbol),
                open_interest=await _timed(session, OpenInterest, OpenInterest.open_interest, symbol),
                premium=await _timed(session, PremiumIndex, PremiumIndex.close, symbol),
                long_short=await _long_short(session, symbol),
                liquidations=await _liquidations(session, symbol),
                gaps=await load_feature_gaps(session, exchange=_EXCHANGE, symbol=symbol, start_ms=start, end_ms=end),
            )
        )
    return tuple(loaded)


def _assemble(rows: Sequence[_SymbolRows], cut_ms: int) -> tuple[tuple[SymbolHistory, ...], dict[str, SymbolData]]:
    """Історії й фічі до `cut_ms` (початок hold-out). Спершу зріз, потім вирівнювання mark і ціни funding.

    Інакше відсутня mark-свічка в hold-out вимикала б mark для всього walk-forward (SL за last),
    і результат фолдів залежав би від даних, яких до відкриття hold-out не існує.
    """
    histories: list[SymbolHistory] = []
    features: dict[str, SymbolData] = {}
    for item in rows:
        kept = [(bar, volume) for bar, volume in item.candles if bar.open_time_ms < cut_ms]
        if not kept:
            raise OptunaDataError("до hold-out немає свічок символу задачі")
        candles = [bar for bar, _volume in kept]
        marks = _aligned_marks(candles, [bar for bar in item.marks if bar.open_time_ms < cut_ms])
        price_bars = marks if marks is not None else candles
        funding = _funding_events([payment for payment in item.payments if payment[0] < cut_ms], price_bars)
        histories.append(
            SymbolHistory(
                symbol=item.symbol,
                candles=tuple(candles),
                instrument=item.instrument,
                mark_candles=marks,
                funding=funding,
            )
        )
        features[item.symbol] = SymbolData(
            candles=tuple(_feature_bar(bar, volume) for bar, volume in kept),
            open_interest=tuple(point for point in item.open_interest if point.ts_ms < cut_ms),
            premium=tuple(point for point in item.premium if point.ts_ms < cut_ms),
            long_short=tuple(point for point in item.long_short if point.ts_ms < cut_ms),
            liquidations=tuple(point for point in item.liquidations if point.ts_ms < cut_ms),
            funding=tuple(TimedValue(event.time_ms, float(event.rate)) for event in funding),
            gaps=dict(item.gaps),
        )
    return tuple(histories), features


async def _candle_rows(session: AsyncSession, symbol: str) -> list[tuple[SimCandle, Decimal]]:
    """Свічки 1h одним запитом: ціни для симулятора і обсяг для фіч."""
    rows = (
        await session.scalars(
            select(Candle)
            .where(Candle.exchange == _EXCHANGE, Candle.symbol == symbol, Candle.timeframe == _TIMEFRAME)
            .order_by(Candle.open_time_ms)
        )
    ).all()
    return [
        (
            SimCandle(open_time_ms=row.open_time_ms, open=row.open, high=row.high, low=row.low, close=row.close),
            row.volume,
        )
        for row in rows
    ]


async def _instrument(session: AsyncSession, symbol: str) -> Instrument:
    row = await session.get(StoredInstrument, (_EXCHANGE, _INSTRUMENT_MODE, symbol))
    if row is None:
        raise OptunaDataError("немає інструмента demo для символу задачі")
    return Instrument(
        symbol=row.symbol,
        tick_size=row.tick_size,
        qty_step=row.qty_step,
        min_qty=row.min_qty,
        min_notional=row.min_notional,
        max_qty=row.max_qty,
        max_leverage=row.max_leverage,
        launch_time_ms=row.launch_time_ms,
        status=row.status,
    )


async def _mark_candles(session: AsyncSession, symbol: str) -> list[SimCandle]:
    rows = (
        await session.scalars(
            select(MarkIndexCandle)
            .where(
                MarkIndexCandle.exchange == _EXCHANGE,
                MarkIndexCandle.symbol == symbol,
                MarkIndexCandle.price_type == "mark",
                MarkIndexCandle.interval == _TIMEFRAME,
            )
            .order_by(MarkIndexCandle.ts_ms)
        )
    ).all()
    return [
        SimCandle(open_time_ms=row.ts_ms, open=row.open, high=row.high, low=row.low, close=row.close) for row in rows
    ]


def _aligned_marks(candles: Sequence[SimCandle], marks: Sequence[SimCandle]) -> tuple[SimCandle, ...] | None:
    if len(marks) != len(candles):
        return None
    if any(mark.open_time_ms != candle.open_time_ms for mark, candle in zip(marks, candles, strict=True)):
        return None
    return tuple(marks)


async def _funding_payments(session: AsyncSession, symbol: str) -> tuple[tuple[int, Decimal], ...]:
    rows = (
        await session.scalars(
            select(FundingRate)
            .where(FundingRate.exchange == _EXCHANGE, FundingRate.symbol == symbol)
            .order_by(FundingRate.funding_time_ms)
        )
    ).all()
    return tuple((row.funding_time_ms, row.rate) for row in rows)


def _funding_events(payments: Sequence[tuple[int, Decimal]], bars: Sequence[SimCandle]) -> tuple[FundingEvent, ...]:
    """Ціна виплати — close останньої свічки, що закрилась не пізніше виплати (`open + 1h ≤ час`).

    Свічка, що відкривається в момент виплати, закривається на годину пізніше: її close — ціна з майбутнього.
    Виплата раніше за першу закриту свічку пропускається. Пошук бінарний: лінійний прохід на кожну
    виплату на 5 роках історії займав секунди на символ без жодного `await`.
    """
    closes = [bar.open_time_ms + HOUR_MS for bar in bars]
    events: list[FundingEvent] = []
    for time_ms, rate in payments:
        index = bisect_right(closes, time_ms) - 1
        if index < 0:
            continue
        events.append(FundingEvent(time_ms=time_ms, rate=rate, mark_price=bars[index].close))
    return tuple(events)


def _feature_bar(bar: SimCandle, volume: Decimal) -> CandleBar:
    return CandleBar(
        open_time_ms=bar.open_time_ms,
        open=float(bar.open),
        high=float(bar.high),
        low=float(bar.low),
        close=float(bar.close),
        volume=float(volume),
    )


async def _timed(session: AsyncSession, model: Any, column: Any, symbol: str) -> tuple[TimedValue, ...]:
    rows = (
        await session.scalars(
            select(model)
            .where(model.exchange == _EXCHANGE, model.symbol == symbol, model.interval == _TIMEFRAME)
            .order_by(model.ts_ms)
        )
    ).all()
    return tuple(TimedValue(row.ts_ms, float(getattr(row, column.key))) for row in rows)


async def _long_short(session: AsyncSession, symbol: str) -> tuple[LongShortPoint, ...]:
    rows = (
        await session.scalars(
            select(LongShortRatio)
            .where(
                LongShortRatio.exchange == _EXCHANGE,
                LongShortRatio.symbol == symbol,
                LongShortRatio.interval == _TIMEFRAME,
            )
            .order_by(LongShortRatio.ts_ms)
        )
    ).all()
    return tuple(LongShortPoint(row.ts_ms, float(row.buy_ratio), float(row.sell_ratio)) for row in rows)


async def _liquidations(session: AsyncSession, symbol: str) -> tuple[LiquidationBar, ...]:
    rows = (
        await session.scalars(
            select(LiquidationsHourly)
            .where(
                LiquidationsHourly.exchange == _EXCHANGE,
                LiquidationsHourly.symbol == symbol,
                LiquidationsHourly.interval == _TIMEFRAME,
            )
            .order_by(LiquidationsHourly.ts_ms)
        )
    ).all()
    return tuple(LiquidationBar(row.ts_ms, float(row.long_notional), float(row.short_notional)) for row in rows)


def _trim_history(history: SymbolHistory, data_to_ms: int) -> SymbolHistory:
    candles = tuple(bar for bar in history.candles if bar.open_time_ms < data_to_ms)
    if not candles:
        raise OptunaDataError("після межі розбивки не лишилось свічок")
    marks = history.mark_candles
    trimmed_marks = None if marks is None else tuple(bar for bar in marks if bar.open_time_ms < data_to_ms)
    if trimmed_marks is not None and not trimmed_marks:
        trimmed_marks = None
    funding = tuple(event for event in history.funding if event.time_ms < data_to_ms)
    return SymbolHistory(
        symbol=history.symbol,
        candles=candles,
        instrument=history.instrument,
        mark_candles=trimmed_marks,
        funding=funding,
    )


def _trim_features(data: SymbolData, data_to_ms: int) -> SymbolData:
    return SymbolData(
        candles=tuple(bar for bar in data.candles if bar.open_time_ms < data_to_ms),
        open_interest=tuple(point for point in data.open_interest if point.ts_ms < data_to_ms),
        premium=tuple(point for point in data.premium if point.ts_ms < data_to_ms),
        long_short=tuple(point for point in data.long_short if point.ts_ms < data_to_ms),
        liquidations=tuple(point for point in data.liquidations if point.ts_ms < data_to_ms),
        funding=tuple(point for point in data.funding if point.ts_ms < data_to_ms),
        gaps=dict(data.gaps),
    )


async def _ensure_experiment(
    sessions: SessionFactory,
    histories: Sequence[SymbolHistory],
    *,
    spec: StrategySpec,
    split: ResearchSplit,
    trading_symbol: str,
    seed: int,
    target: TargetParams,
    costs: CostModel,
    experiment_id: str | None,
    on_created: CreatedHook | None,
) -> str:
    from app.ml.meta_model import META_MIN_PROBABILITY, meta_recipe

    if experiment_id is not None:
        async with sessions() as session, session.begin():
            experiment = await _experiment(session, experiment_id)
            if experiment.params.get("validation") != validation_payload(split):
                raise SplitLeakError("розбивка відновлення не збігається з експериментом")
            request = OptunaJobRequest(
                strategy=spec,
                n_trials=OPTUNA_TRIALS_MIN,
                seed=seed,
                symbols=tuple(history.symbol for history in histories),
                trading_symbol=trading_symbol,
                initial_equity=Decimal(1),
                maintenance_margin_rate=Decimal("0.005"),
                min_equity_usd=Decimal(1),
                max_notional_per_symbol_usd=Decimal(1),
                experiment_id=experiment_id,
            )
            _match_experiment(experiment, request, target, costs)
        return experiment_id
    recipe = meta_recipe(shuffled_labels=False, min_probability=META_MIN_PROBABILITY)
    recipe["optuna_search"] = search_space_payload()
    recipe["target"] = target_payload(target)
    recipe["costs"] = costs_payload(costs)
    async with sessions() as session, session.begin():
        created = await create_experiment(
            session,
            split=split,
            research_line=trading_symbol,
            symbols=tuple(history.symbol for history in histories),
            label_type="meta",
            primary_strategy=spec.metadata(),
            params=recipe,
            seed=seed,
        )
        if on_created is not None:
            await on_created(session, created)
        return created


async def _load_replay(sessions: SessionFactory, experiment_id: str) -> tuple[_Replay, ...]:
    async with sessions() as session, session.begin():
        rows = (
            await session.scalars(
                select(ExperimentTrial)
                .where(ExperimentTrial.experiment_id == experiment_id)
                .order_by(ExperimentTrial.trial_no)
            )
        ).all()
    replayed = tuple(_replay(row) for row in rows)
    numbers = [item.number for item in replayed]
    if numbers != list(range(len(numbers))):
        raise OptunaDataError("номери спроб Optuna мають іти підряд від нуля")
    return replayed


def _replay(row: ExperimentTrial) -> _Replay:
    params = row.params
    if not isinstance(params, Mapping):
        raise OptunaDataError("параметри спроби не об'єкт")
    block = params.get("optuna")
    lgbm = params.get("lgbm")
    if not isinstance(block, Mapping) or not isinstance(lgbm, Mapping):
        raise OptunaDataError("спроба експерименту не є точкою Optuna")
    number = block.get("number")
    if type(number) is not int or isinstance(number, bool) or number < 0:
        raise OptunaDataError("номер спроби Optuna зіпсований")
    state = row.state
    if state not in ("COMPLETE", "PRUNED", "FAILED"):
        raise OptunaDataError("невідомий стан спроби")
    value_text = block.get("value")
    if state == "COMPLETE":
        value = float(_stored_decimal(value_text))
    elif value_text is not None:
        raise OptunaDataError("незавершена спроба не має фінальної метрики")
    else:
        value = None
    raw_steps = block.get("steps")
    if type(raw_steps) is not list:
        raise OptunaDataError("кроки спроби не список")
    steps = {index: float(_stored_decimal(item)) for index, item in enumerate(raw_steps)}
    return _Replay(number=number, state=state, params=_distribution_params(lgbm), value=value, steps=steps)


def _distribution_params(lgbm: Mapping[str, Any]) -> dict[str, float | int]:
    return {
        "num_leaves": _stored_int(lgbm.get("num_leaves")),
        "max_depth": _stored_int(lgbm.get("max_depth")),
        "learning_rate": float(_stored_decimal(lgbm.get("learning_rate"))),
        "n_estimators": _stored_int(lgbm.get("n_estimators")),
        "min_child_samples": _stored_int(lgbm.get("min_child_samples")),
        "subsample": float(_stored_decimal(lgbm.get("subsample"))),
        "colsample_bytree": float(_stored_decimal(lgbm.get("colsample_bytree"))),
        "reg_lambda": float(_stored_decimal(lgbm.get("reg_lambda"))),
    }


def _stored_int(value: object) -> int:
    if type(value) is not int or isinstance(value, bool):
        raise OptunaDataError("ціле поле спроби зіпсоване")
    return value


def _stored_decimal(value: object) -> Decimal:
    if type(value) is not str:
        raise OptunaDataError("десяткове поле спроби має бути рядком")
    try:
        number = Decimal(value)
    except ArithmeticError as exc:
        raise OptunaDataError("десяткове поле спроби не читається") from exc
    require_decimal("replay", number)
    return number


def _optuna_state(state: str) -> TrialState:
    if state == "COMPLETE":
        return TrialState.COMPLETE
    if state == "PRUNED":
        return TrialState.PRUNED
    return TrialState.FAIL


def _fold_net(fold: Any, symbol: str) -> Decimal:
    report = next(item for item in fold.record.reports if item.symbol == symbol)
    net: Decimal = report.metrics.total_net_usd
    return net


async def _train(train_meta: Callable[..., Any], histories: Sequence[SymbolHistory], **kwargs: Any) -> Any:
    return await asyncio.to_thread(train_meta, histories, **kwargs)


async def _write_trial(
    sessions: SessionFactory,
    *,
    experiment_id: str,
    split: ResearchSplit,
    state: str,
    number: int,
    value: Decimal | None,
    steps: Sequence[Decimal],
    params: Mapping[str, Any],
    folds: Sequence[Any],
) -> None:
    stored = dict(params)
    stored["optuna"] = {
        "number": number,
        "state": state,
        "value": None if value is None else format(value, "f"),
        "steps": [format(step, "f") for step in steps],
    }
    async with sessions() as session, session.begin():
        await record_trial(
            session,
            experiment_id=experiment_id,
            split=split,
            state=state,
            params=stored,
            folds=folds,
        )
    _LOG.info("optuna_trial", experiment_id=experiment_id, number=number, state=state)
