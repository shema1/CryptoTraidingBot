"""Walk-forward, embargo, hold-out і журнал спроб (ТЗ §5.2.4, §5А.6, §5А.10).

Навчальне вікно розширюється. Між ним і тестом — embargo не коротший за `horizon_bars`,
один і той самий за часом для всіх символів: рядок, чия мітка перетинає тест, у train не входить.
Hold-out — останні 15–20% історії. Його рядки не потрапляють у train, Optuna, калібрування,
вибір порогів і параметрів первинної стратегії.

Тестовий відрізок — 90 діб, і 180, якщо угод торгового символу менше `min_trades_per_fold`.
Це не календарні місяці: межа фолду не залежить від того, на який день місяця припаде старт.
Перед hold-out лишається запас `horizon_bars` барів: шлях угоди дочитується там, а не в hold-out.

Журнал пише кожну спробу, включно з невдалими. Лічильник лінії — число рядків `experiment_trials`
цієї лінії; він не скидається. На лінії один запечатаний hold-out: інший експеримент не бере
його у walk-forward і не оголошує вже бачений відрізок чистим. Зсув — новий експеримент, чий
hold-out починається не раніше за кінець запечатаного; старе вікно тоді стає walk-forward.
Публічного відкриття hold-out тут немає: `_open_holdout` викликає лише
`statistics.open_holdout_after_gate` після Gate G1. Зсув новим експериментом G1 не потребує.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping, Sequence
from decimal import ROUND_CEILING, Decimal
from typing import Any

from pydantic import model_validator
from sqlalchemy import func, insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import now_ms
from app.core.db import SYMBOL_RE
from app.ml.backtest import BacktestMetrics, BacktestReport, BacktestTrade
from app.ml.features import HOUR_MS
from app.ml.models import LABEL_TYPES, TRIAL_STATES, Experiment, ExperimentTrial, HoldoutOpening
from app.ml.trade_sim import DirectSample, ExitReason, MetaSample, StopPriceSource
from app.risk.numbers import FrozenModel

# 90 і 180 діб — наближення «3 і 6 місяців» без прив'язки до календаря.
DAY_MS: int = 24 * HOUR_MS
TEST_SEGMENT_MS: int = 90 * DAY_MS
TEST_SEGMENT_EXTENDED_MS: int = 180 * DAY_MS
MIN_TRAIN_MS: int = 365 * DAY_MS
MIN_TRADES_PER_FOLD: int = 20
HOLDOUT_FRACTION_MIN: Decimal = Decimal("0.15")
HOLDOUT_FRACTION_MAX: Decimal = Decimal("0.20")
DEFAULT_HOLDOUT_FRACTION: Decimal = Decimal("0.20")

_TIMEFRAME = "1h"
_SYMBOL = re.compile(SYMBOL_RE)
_EXPERIMENT_ID = re.compile(r"^exp-(\d+)$")
_OPENING_LIMIT = ("opening_no_max_2", "uq_holdout_openings_window_opening_no")

TradeCount = Callable[[int, int], int]


class SplitLeakError(ValueError):
    """Рядок тесту, embargo або hold-out потрапив туди, де його не має бути."""


class HoldoutLimitError(ValueError):
    """Третє відкриття того самого відрізка. Такий самий сенс має IntegrityError БД."""


class HoldoutNotNewerError(ValueError):
    """Hold-out експерименту вже відпущений зсувом: на лінії запечатано новіше вікно."""


class SplitPurpose:
    """Призначення вибірки. Hold-out виключений з кожного."""

    TRAIN = "train"
    TEST = "test"
    OPTUNA = "optuna"
    CALIBRATION = "calibration"
    THRESHOLDS = "thresholds"
    PRIMARY_STRATEGY = "primary_strategy"


_FOLD_PURPOSES = frozenset({SplitPurpose.TRAIN, SplitPurpose.TEST})
_OOS_PURPOSES = frozenset(
    {
        SplitPurpose.OPTUNA,
        SplitPurpose.CALIBRATION,
        SplitPurpose.THRESHOLDS,
        SplitPurpose.PRIMARY_STRATEGY,
    }
)
_PURPOSES = _FOLD_PURPOSES | _OOS_PURPOSES


class SampleRow(FrozenModel):
    """Рядок рішення. `time_ms` — open свічки входу, `index` — позиція в ряді свого символу."""

    symbol: str
    time_ms: int
    index: int

    @model_validator(mode="after")
    def _row(self) -> SampleRow:
        if not _SYMBOL.fullmatch(self.symbol):
            raise ValueError("символ не канонічний")
        if self.time_ms < 0 or self.index < 0:
            raise ValueError("час і індекс рядка невід'ємні")
        return self


class Fold(FrozenModel):
    """Один крок walk-forward. Train — `[train_start, embargo_start)`, тест — `[test_start, test_end)`."""

    index: int
    train_start_ms: int
    embargo_start_ms: int
    test_start_ms: int
    test_end_ms: int
    extended: bool
    n_trading_trades: int
    below_min_trades: bool


class ResearchSplit(FrozenModel):
    """Розбивка однієї історії. Фолди не заходять у hold-out і не дочитують його цінами виходу."""

    data_from_ms: int
    data_to_ms: int
    holdout_from_ms: int
    holdout_to_ms: int
    holdout_fraction: Decimal
    horizon_bars: int
    embargo_bars: int
    bar_ms: int
    min_train_ms: int
    test_segment_ms: int
    test_segment_extended_ms: int
    min_trades_per_fold: int
    decision_end_ms: int
    unused_from_ms: int
    folds: tuple[Fold, ...]

    @model_validator(mode="after")
    def _geometry(self) -> ResearchSplit:
        problem = _split_problem(self)
        if problem is not None:
            raise ValueError(problem)
        return self


class LabelStats(FrozenModel):
    """Частка fallback SL і рядків, де обидва боки дійшли до TP. Нулі теж записуються."""

    n_labels: int
    n_last_stop: int
    n_both_reached_tp: int

    @model_validator(mode="after")
    def _counts(self) -> LabelStats:
        if self.n_labels < 0 or self.n_last_stop < 0 or self.n_both_reached_tp < 0:
            raise ValueError("лічильник міток від'ємний")
        if self.n_last_stop > self.n_labels or self.n_both_reached_tp > self.n_labels:
            raise ValueError("частка міток більша за кількість")
        return self

    @property
    def last_stop_share(self) -> Decimal | None:
        if self.n_labels == 0:
            return None
        return Decimal(self.n_last_stop) / Decimal(self.n_labels)

    @property
    def both_reached_tp_share(self) -> Decimal | None:
        if self.n_labels == 0:
            return None
        return Decimal(self.n_both_reached_tp) / Decimal(self.n_labels)


class FoldRecord(FrozenModel):
    """Прогони одного фолду. Кожен символ експерименту — рівно один `run_backtest`."""

    reports: tuple[BacktestReport, ...]
    train_labels: LabelStats
    test_labels: LabelStats


class RecordedTrial(FrozenModel):
    """Спроба, вже дописана в журнал. `line_trial_no` рахує всю лінію, не лише цей експеримент."""

    experiment_id: str
    trial_id: int
    trial_no: int
    line_trial_no: int


class RecordedOpening(FrozenModel):
    """Одне відкриття hold-out. `opening_no` — 1 або 2."""

    opening_id: int
    opening_no: int


def holdout_bounds(
    data_from_ms: int,
    data_to_ms: int,
    *,
    fraction: Decimal = DEFAULT_HOLDOUT_FRACTION,
) -> tuple[int, int]:
    """Останні `fraction` історії, вирівняні вниз за годиною, тож відрізок не коротший за частку.

    Частка — лише `Decimal` з діапазону 15–20%. Межі — `[from, to)`.
    """
    start = _millis("data_from_ms", data_from_ms)
    end = _millis("data_to_ms", data_to_ms)
    if end <= start:
        raise ValueError("історія порожня")
    if start % HOUR_MS != 0 or end % HOUR_MS != 0:
        raise ValueError("межі історії не вирівняні за 1h")
    _fraction(fraction)
    span = end - start
    length = int((Decimal(span) * fraction).to_integral_value(rounding=ROUND_CEILING))
    holdout_from = end - length
    holdout_from -= (holdout_from - start) % HOUR_MS
    if holdout_from <= start:
        raise ValueError("hold-out поглинув усю історію")
    return holdout_from, end


def build_split(
    *,
    data_from_ms: int,
    data_to_ms: int,
    horizon_bars: int,
    count_trades: TradeCount,
    embargo_bars: int | None = None,
    holdout_fraction: Decimal = DEFAULT_HOLDOUT_FRACTION,
    min_train_ms: int = MIN_TRAIN_MS,
    test_segment_ms: int = TEST_SEGMENT_MS,
    test_segment_extended_ms: int = TEST_SEGMENT_EXTENDED_MS,
    min_trades_per_fold: int = MIN_TRADES_PER_FOLD,
) -> ResearchSplit:
    """Розбивка з розширюваним вікном. `count_trades(start, end)` — угоди торгового символу на `[start, end)`.

    Рахунок має бути з бектесту, що стартує на `start` з порожнім рахунком: зріз довшого прогону
    бачить ліміти, нараховані до вікна. Сигнали — лише всередині вікна; свічки після `end` — шлях,
    і цей шлях не заходить у hold-out (запас `horizon_bars` вже відрізаний).
    """
    embargo = horizon_bars if embargo_bars is None else embargo_bars
    _bars("horizon_bars", horizon_bars)
    _bars("embargo_bars", embargo)
    if embargo < horizon_bars:
        raise ValueError("embargo коротший за horizon_bars")
    _duration("min_train_ms", min_train_ms)
    _duration("test_segment_ms", test_segment_ms)
    _duration("test_segment_extended_ms", test_segment_extended_ms)
    if test_segment_extended_ms <= test_segment_ms:
        raise ValueError("подовжений відрізок не довший за базовий")
    if type(min_trades_per_fold) is not int or min_trades_per_fold < 1:
        raise ValueError("min_trades_per_fold має бути ціле ≥ 1")
    if min_train_ms <= embargo * HOUR_MS:
        raise ValueError("мінімальне навчання має бути довше за embargo")
    holdout_from, holdout_to = holdout_bounds(data_from_ms, data_to_ms, fraction=holdout_fraction)
    decision_end = holdout_from - horizon_bars * HOUR_MS
    first = data_from_ms + min_train_ms
    if first + test_segment_ms > decision_end:
        raise ValueError("історія не вміщує навчання, тестовий відрізок, запас шляху і hold-out")
    folds = _folds(
        data_from=data_from_ms,
        first=first,
        decision_end=decision_end,
        embargo_bars=embargo,
        test_segment_ms=test_segment_ms,
        test_segment_extended_ms=test_segment_extended_ms,
        min_trades_per_fold=min_trades_per_fold,
        count_trades=count_trades,
    )
    return ResearchSplit(
        data_from_ms=data_from_ms,
        data_to_ms=holdout_to,
        holdout_from_ms=holdout_from,
        holdout_to_ms=holdout_to,
        holdout_fraction=holdout_fraction,
        horizon_bars=horizon_bars,
        embargo_bars=embargo,
        bar_ms=HOUR_MS,
        min_train_ms=min_train_ms,
        test_segment_ms=test_segment_ms,
        test_segment_extended_ms=test_segment_extended_ms,
        min_trades_per_fold=min_trades_per_fold,
        decision_end_ms=decision_end,
        unused_from_ms=folds[-1].test_end_ms,
        folds=tuple(folds),
    )


def train_coverage_ms(split: ResearchSplit, fold_index: int) -> tuple[int, int]:
    """Межі `compute_features(coverage_*)`: лише train, без embargo, тесту і hold-out."""
    fold = _fold(split, fold_index)
    return fold.train_start_ms, fold.embargo_start_ms


def require_train_coverage(split: ResearchSplit, fold_index: int, start_ms: int, end_ms: int) -> None:
    """Відмовити, якщо вікно покриття захоплює embargo, тест або hold-out.

    Інакше дірка в майбутньому прибирає колонку, і склад фіч залежить від даних після train.
    """
    lo, hi = train_coverage_ms(split, fold_index)
    if type(start_ms) is not int or type(end_ms) is not int or start_ms >= end_ms:
        raise ValueError("межі покриття мають бути int і start < end")
    if start_ms < lo or end_ms > hi:
        raise SplitLeakError("покриття фіч виходить за train фолду")


def label_overlaps_test(row: SampleRow, split: ResearchSplit, fold_index: int) -> bool:
    """Мітка дивиться на `[time, time + horizon]` включно з open виходу і перетинає тест."""
    _aligned(row, split)
    fold = _fold(split, fold_index)
    label_end = row.time_ms + split.horizon_bars * split.bar_ms
    return row.time_ms < fold.test_end_ms and label_end >= fold.test_start_ms


def is_holdout_row(row: SampleRow, split: ResearchSplit) -> bool:
    """Рядок лежить у `[holdout_from, holdout_to)`."""
    _aligned(row, split)
    return split.holdout_from_ms <= row.time_ms < split.holdout_to_ms


def is_train_row(row: SampleRow, split: ResearchSplit, fold_index: int) -> bool:
    """Train цього фолду: до embargo, мітка не перетинає тест, не hold-out."""
    _aligned(row, split)
    fold = _fold(split, fold_index)
    if is_holdout_row(row, split):
        return False
    if row.time_ms < fold.train_start_ms or row.time_ms >= fold.embargo_start_ms:
        return False
    if label_overlaps_test(row, split, fold_index):
        raise SplitLeakError("embargo не покриває горизонт мітки")
    return True


def is_test_row(row: SampleRow, split: ResearchSplit, fold_index: int) -> bool:
    """Рішення всередині тесту фолду. Hold-out сюди не входить."""
    _aligned(row, split)
    fold = _fold(split, fold_index)
    if is_holdout_row(row, split):
        return False
    return fold.test_start_ms <= row.time_ms < fold.test_end_ms


def rows_for(
    rows: Sequence[SampleRow],
    split: ResearchSplit,
    purpose: str,
    *,
    fold_index: int | None = None,
) -> tuple[SampleRow, ...]:
    """Рядки, які це призначення має право бачити. Hold-out у результаті — помилка інваріанта.

    `train` і `test` — один фолд. Optuna, калібрування, пороги і первинна стратегія — out-of-sample
    рядки всіх фолдів walk-forward, без train і без hold-out. Навчання всередині Optuna бере `train`.
    """
    if purpose not in _PURPOSES:
        raise ValueError("невідоме призначення вибірки")
    if purpose in _FOLD_PURPOSES:
        if type(fold_index) is not int:
            raise ValueError("фолд потрібен для train і test")
        keep = is_train_row if purpose == SplitPurpose.TRAIN else is_test_row
        chosen = [row for row in rows if keep(row, split, fold_index)]
    else:
        if fold_index is not None:
            raise ValueError("оцінка всіх фолдів не приймає fold_index")
        chosen = [row for row in rows if _in_any_test(row, split)]
    if any(_touches_holdout(row, split) for row in chosen):
        raise SplitLeakError("hold-out потрапив у вибірку")
    return tuple(chosen)


def reject_holdout(rows: Sequence[SampleRow], split: ResearchSplit, *, purpose: str) -> None:
    """Зупинитись, якщо набір містить hold-out або мітку, яка читає його ціни.

    Сюди входить запас шляху перед hold-out і будь-який рядок після його початку.
    Optuna, калібрування, вибір порогів і параметрів правила викликають це на кожному наборі,
    навіть якщо індекси прийшли не з `rows_for`.
    """
    if purpose not in _PURPOSES:
        raise ValueError("невідоме призначення вибірки")
    for row in rows:
        if _touches_holdout(row, split):
            raise SplitLeakError(f"{purpose}: індекс {row.index} символу {row.symbol} заходить у hold-out")


def guard_sample_weights(rows: Sequence[SampleRow], split: ResearchSplit, fold_index: int) -> tuple[SampleRow, ...]:
    """Ваги унікальності лише на train. Тест або hold-out у наборі — помилка, не тихе відсікання.

    Викликати після з'єднання з фічами: рядки, що відпали через дірку, інакше занижують вагу сусідів.
    """
    for row in rows:
        if not is_train_row(row, split, fold_index):
            raise SplitLeakError("sample_weights лише на train фолду")
    return tuple(rows)


def label_diagnostics(samples: Sequence[MetaSample | DirectSample]) -> LabelStats:
    """Скільки міток пішло зі SL за last і скільки мали обидва TP.

    Для прямої мітки `LAST` рахується, якщо хоч одна нога взяла last: журнал має бачити fallback.
    Meta-мітка `both_reached_tp` не має — це властивість пари.
    """
    last = 0
    both = 0
    for sample in samples:
        if _stop_source(sample) is StopPriceSource.LAST:
            last += 1
        if _both_reached(sample):
            both += 1
    return LabelStats(n_labels=len(samples), n_last_stop=last, n_both_reached_tp=both)


def validation_payload(split: ResearchSplit) -> dict[str, Any]:
    """Геометрія розбивки, яку експеримент зберігає і з якою звіряється кожна спроба."""
    return {
        "data_from_ms": split.data_from_ms,
        "data_to_ms": split.data_to_ms,
        "holdout_from_ms": split.holdout_from_ms,
        "holdout_to_ms": split.holdout_to_ms,
        "holdout_fraction": format(split.holdout_fraction, "f"),
        "horizon_bars": split.horizon_bars,
        "embargo_bars": split.embargo_bars,
        "bar_ms": split.bar_ms,
        "min_train_ms": split.min_train_ms,
        "test_segment_ms": split.test_segment_ms,
        "test_segment_extended_ms": split.test_segment_extended_ms,
        "min_trades_per_fold": split.min_trades_per_fold,
        "decision_end_ms": split.decision_end_ms,
        "unused_from_ms": split.unused_from_ms,
        "folds": [
            {
                "index": fold.index,
                "train_start_ms": fold.train_start_ms,
                "embargo_start_ms": fold.embargo_start_ms,
                "test_start_ms": fold.test_start_ms,
                "test_end_ms": fold.test_end_ms,
                "extended": fold.extended,
                "n_trading_trades": fold.n_trading_trades,
                "below_min_trades": fold.below_min_trades,
            }
            for fold in split.folds
        ],
    }


async def create_experiment(
    session: AsyncSession,
    *,
    split: ResearchSplit,
    research_line: str,
    symbols: Sequence[str],
    label_type: str,
    primary_strategy: Mapping[str, Any],
    params: Mapping[str, Any],
    seed: int,
) -> str:
    """Новий експеримент лінії. `seed` лише зберігається: розбивка від нього не залежить.

    Hold-out — запечатаний на лінії або новіший: початок не раніше за кінець запечатаного.
    Новіший відпускає старий у walk-forward (зсув, ТЗ §5А.10) і нічого не відкриває.
    `n_trials_line`, DSR і PBO лишаються порожніми, доки `statistics.open_holdout_after_gate` не відкриє hold-out.
    """
    line = _symbols(research_line, symbols)
    if label_type not in LABEL_TYPES:
        raise ValueError("невідомий label_type")
    if type(seed) is not int or seed < 0:
        raise ValueError("seed має бути невід'ємний int")
    if "validation" in params:
        raise ValueError("params.validation займає розбивка")
    await _lock_line(session, line)
    sealed = _sealed_holdout(await _line_windows(session, line), await _openings(session, line))
    window = (split.holdout_from_ms, split.holdout_to_ms)
    if sealed is not None and window != sealed and split.holdout_from_ms < sealed[1]:
        raise SplitLeakError("hold-out лінії вже зафіксований: новий має починатися не раніше за його кінець")
    stored = _jsonable(
        {**dict(params), "validation": validation_payload(split)},
        path="params",
    )
    strategy = _jsonable(dict(primary_strategy), path="primary_strategy")
    for _attempt in range(3):
        experiment_id = await _next_experiment_id(session)
        try:
            async with session.begin_nested():
                await session.execute(
                    _experiment_insert(
                        experiment_id=experiment_id,
                        research_line=line,
                        symbols=list(symbols),
                        label_type=label_type,
                        primary_strategy=strategy,
                        params=stored,
                        seed=seed,
                        split=split,
                    )
                )
        except IntegrityError as exc:
            if "pk_experiments" not in str(exc.orig or exc).lower():
                raise
            continue
        else:
            return experiment_id
    raise RuntimeError("не вдалося виділити id експерименту")


async def line_trial_count(session: AsyncSession, research_line: str) -> int:
    """Скільки спроб уже є в лінії. Жоден зсув hold-out і жоден підхід цього числа не зменшують."""
    if not _SYMBOL.fullmatch(research_line):
        raise ValueError("символ лінії не канонічний")
    stmt = (
        select(func.count())
        .select_from(ExperimentTrial)
        .join(Experiment, Experiment.id == ExperimentTrial.experiment_id)
        .where(Experiment.research_line == research_line)
    )
    return int((await session.execute(stmt)).scalar_one())


async def holdout_was_opened(session: AsyncSession, experiment_id: str) -> bool:
    """Чи був уже будь-який перегляд hold-out на лінії до цієї спроби.

    Зсув сам нічого не відкриває. Але кожна наступна спроба лінії йде після вже зробленого
    перегляду, тож прапорець не обмежений вікном, збереженим у самому експерименті.
    """
    experiment = await _experiment(session, experiment_id)
    stmt = (
        select(func.count()).select_from(HoldoutOpening).where(HoldoutOpening.research_line == experiment.research_line)
    )
    return int((await session.execute(stmt)).scalar_one()) > 0


async def record_trial(
    session: AsyncSession,
    *,
    experiment_id: str,
    split: ResearchSplit,
    state: str,
    params: Mapping[str, Any],
    folds: Sequence[FoldRecord] = (),
) -> RecordedTrial:
    """Дописати спробу. Невдала і обрізана теж збільшують лічильник лінії.

    `COMPLETE` містить кожен фолд. Інший стан — лише префікс фолдів, без пропусків:
    не можна залишити в журналі тільки вдалі відрізки. `oos_results` — R торгового символу
    в порядку фолдів і часу входу, рядком Decimal; `oos_entry_ms` — час входу кожного з них.
    Hold-out у ряді немає.
    """
    if state not in TRIAL_STATES:
        raise ValueError("невідомий стан спроби")
    experiment = await _experiment(session, experiment_id)
    await _lock_line(session, experiment.research_line)
    if experiment.params.get("validation") != validation_payload(split):
        raise SplitLeakError("розбивка спроби не збігається з експериментом")
    if state == "COMPLETE" and len(folds) != len(split.folds):
        raise ValueError("COMPLETE записує кожен фолд")
    if len(folds) > len(split.folds):
        raise ValueError("фолдів більше, ніж у розбивці")
    symbols = list(experiment.symbols)
    checked: list[dict[str, Any]] = []
    series: list[str] = []
    entries: list[int] = []
    any_last = False
    any_both = False
    for index, record in enumerate(folds):
        block, fold_r, fold_entries, last, both = _fold_block(
            record,
            split=split,
            fold_index=index,
            symbols=symbols,
            seed=experiment.seed,
            research_line=experiment.research_line,
        )
        checked.append(block)
        series.extend(fold_r)
        entries.extend(fold_entries)
        any_last = any_last or last
        any_both = any_both or both
    line_no = await line_trial_count(session, experiment.research_line) + 1
    trial_no = await _next_trial_no(session, experiment_id)
    opened = await holdout_was_opened(session, experiment_id)
    metrics = _jsonable(
        {
            "line_trial_no": line_no,
            "holdout_opened": opened,
            "any_last_stop": any_last,
            "any_both_reached_tp": any_both,
            "folds": checked,
        },
        path="metrics",
    )
    trial_params = _jsonable(dict(params), path="params")
    async with session.begin_nested():
        inserted = await session.execute(
            insert(ExperimentTrial)
            .values(
                experiment_id=experiment_id,
                trial_no=trial_no,
                state=state,
                params=trial_params,
                oos_results=series,
                oos_entry_ms=entries,
                metrics=metrics,
                created_at_ms=now_ms(),
            )
            .returning(ExperimentTrial.id)
        )
        trial_id = int(inserted.scalar_one())
    return RecordedTrial(
        experiment_id=experiment_id,
        trial_id=trial_id,
        trial_no=trial_no,
        line_trial_no=line_no,
    )


async def _open_holdout(
    session: AsyncSession,
    *,
    research_line: str,
    holdout_from_ms: int,
    holdout_to_ms: int,
    experiment_id: str,
    model_version: str,
    result: Mapping[str, Any] | None = None,
) -> RecordedOpening:
    """Записати відкриття hold-out цього експерименту. Друге ще дозволене, третє — `HoldoutLimitError`.

    Вікно має збігатися з hold-out експерименту: інше — `SplitLeakError`, і лінію воно не
    запечатує. Якщо новіший експеримент уже зсунув hold-out, старе вікно — walk-forward,
    і відкрити його не можна (`HoldoutNotNewerError`). Gate G1 тут не перевіряється, тому
    функція приватна: викликає її лише `statistics.open_holdout_after_gate`.
    """
    if not _SYMBOL.fullmatch(research_line):
        raise ValueError("символ лінії не канонічний")
    start = _millis("holdout_from_ms", holdout_from_ms)
    end = _millis("holdout_to_ms", holdout_to_ms)
    if start % HOUR_MS != 0 or end % HOUR_MS != 0 or end <= start:
        raise ValueError("вікно hold-out не вирівняне або порожнє")
    await _lock_line(session, research_line)
    experiment = await _experiment(session, experiment_id)
    if experiment.research_line != research_line:
        raise ValueError("лінія відкриття не збігається з експериментом")
    stored = experiment.params.get("validation")
    if not isinstance(stored, dict):
        raise SplitLeakError("в експерименту немає розбивки")
    if (start, end) != (stored["holdout_from_ms"], stored["holdout_to_ms"]):
        raise SplitLeakError("відкривається лише hold-out цього експерименту")
    existing = await _openings(session, research_line)
    if _sealed_holdout(await _line_windows(session, research_line), existing) != (start, end):
        raise HoldoutNotNewerError("hold-out експерименту вже відпущено зсувом")
    opening_no = _opening_no(existing, start, end)
    payload = None if result is None else _jsonable(dict(result), path="result")
    try:
        async with session.begin_nested():
            inserted = await session.execute(
                insert(HoldoutOpening)
                .values(
                    research_line=research_line,
                    holdout_from_ms=start,
                    holdout_to_ms=end,
                    opening_no=opening_no,
                    experiment_id=experiment_id,
                    model_version=model_version,
                    result=payload,
                    opened_at_ms=now_ms(),
                )
                .returning(HoldoutOpening.id)
            )
            opening_id = int(inserted.scalar_one())
    except IntegrityError as exc:
        if _is_opening_limit(exc):
            raise HoldoutLimitError("ліміт відкриттів цього hold-out вичерпано") from exc
        raise
    return RecordedOpening(opening_id=opening_id, opening_no=opening_no)


def _folds(
    *,
    data_from: int,
    first: int,
    decision_end: int,
    embargo_bars: int,
    test_segment_ms: int,
    test_segment_extended_ms: int,
    min_trades_per_fold: int,
    count_trades: TradeCount,
) -> list[Fold]:
    folds: list[Fold] = []
    cursor = first
    while cursor + test_segment_ms <= decision_end:
        end = cursor + test_segment_ms
        extended = False
        n_trades = _count(count_trades, cursor, end, decision_end)
        if n_trades < min_trades_per_fold:
            longer = min(cursor + test_segment_extended_ms, decision_end)
            if longer > end:
                end = longer
                extended = True
                n_trades = _count(count_trades, cursor, end, decision_end)
        folds.append(
            Fold(
                index=len(folds),
                train_start_ms=data_from,
                embargo_start_ms=cursor - embargo_bars * HOUR_MS,
                test_start_ms=cursor,
                test_end_ms=end,
                extended=extended,
                n_trading_trades=n_trades,
                below_min_trades=n_trades < min_trades_per_fold,
            )
        )
        cursor = end
    if not folds:
        raise ValueError("історія не вміщує жодного фолду")
    return folds


def _count(count_trades: TradeCount, start: int, end: int, decision_end: int) -> int:
    if start >= end or end > decision_end:
        raise ValueError("вікно угод порожнє або заходить у запас шляху чи hold-out")
    n_trades = count_trades(start, end)
    if type(n_trades) is not int or n_trades < 0:
        raise ValueError("кількість угод має бути невід'ємний int")
    return n_trades


def _split_problem(split: ResearchSplit) -> str | None:
    if split.bar_ms != HOUR_MS:
        return "валідація v1 лише для 1h"
    if split.horizon_bars < 1 or split.embargo_bars < split.horizon_bars:
        return "embargo коротший за horizon_bars"
    if split.min_trades_per_fold < 1:
        return "min_trades_per_fold має бути ціле ≥ 1"
    if split.min_train_ms <= split.embargo_bars * split.bar_ms:
        return "мінімальне навчання має бути довше за embargo"
    if split.test_segment_extended_ms <= split.test_segment_ms:
        return "подовжений відрізок не довший за базовий"
    if split.holdout_fraction < HOLDOUT_FRACTION_MIN or split.holdout_fraction > HOLDOUT_FRACTION_MAX:
        return "частка hold-out поза 15–20%"
    if split.data_from_ms < 0 or split.data_to_ms <= split.data_from_ms:
        return "межі історії"
    if split.holdout_to_ms != split.data_to_ms or split.holdout_from_ms <= split.data_from_ms:
        return "hold-out не в кінці історії"
    if split.decision_end_ms != split.holdout_from_ms - split.horizon_bars * split.bar_ms:
        return "запас шляху перед hold-out не дорівнює горизонту"
    span = split.data_to_ms - split.data_from_ms
    minimum = int((Decimal(span) * split.holdout_fraction).to_integral_value(rounding=ROUND_CEILING))
    if split.data_to_ms - split.holdout_from_ms < minimum:
        return "hold-out коротший за задану частку"
    if not split.folds:
        return "немає фолдів"
    if split.folds[0].test_start_ms != split.data_from_ms + split.min_train_ms:
        return "перший тест починається не після мінімального навчання"
    if split.unused_from_ms != split.folds[-1].test_end_ms:
        return "хвіст історії не збігається з кінцем останнього тесту"
    previous_end = split.folds[0].test_start_ms
    for fold in split.folds:
        if fold.test_start_ms != previous_end:
            return "між тестами є дірка або накладання"
        previous_end = fold.test_end_ms
        length = fold.test_end_ms - fold.test_start_ms
        if fold.extended:
            cap = min(fold.test_start_ms + split.test_segment_extended_ms, split.decision_end_ms)
            if fold.test_end_ms != cap or length <= split.test_segment_ms:
                return "подовжений фолд не впирається в 6 місяців або в запас шляху"
        elif length != split.test_segment_ms:
            return "базовий фолд не дорівнює 3 місяцям"
        if fold.test_end_ms > split.decision_end_ms:
            return "тест заходить у запас шляху або hold-out"
        if fold.train_start_ms != split.data_from_ms:
            return "навчання не розширюється від початку історії"
        if fold.embargo_start_ms != fold.test_start_ms - split.embargo_bars * split.bar_ms:
            return "embargo не дорівнює заданій кількості барів"
        if fold.below_min_trades != (fold.n_trading_trades < split.min_trades_per_fold):
            return "позначка нестачі угод не збігається з лічильником"
        if fold.index != split.folds.index(fold):
            return "індекси фолдів не послідовні"
    return None


def _fold(split: ResearchSplit, fold_index: int) -> Fold:
    if type(fold_index) is not int or fold_index < 0 or fold_index >= len(split.folds):
        raise ValueError("немає такого фолду")
    return split.folds[fold_index]


def _aligned(row: SampleRow, split: ResearchSplit) -> None:
    if row.time_ms % split.bar_ms != 0:
        raise ValueError("час рядка не вирівняний за 1h")


def _in_any_test(row: SampleRow, split: ResearchSplit) -> bool:
    return any(is_test_row(row, split, fold.index) for fold in split.folds)


def _touches_holdout(row: SampleRow, split: ResearchSplit) -> bool:
    """Рядок у hold-out, після нього, або його мітка дочитує open першого бару hold-out."""
    _aligned(row, split)
    if row.time_ms >= split.holdout_from_ms:
        return True
    label_end = row.time_ms + split.horizon_bars * split.bar_ms
    return label_end >= split.holdout_from_ms


def _stop_source(sample: MetaSample | DirectSample) -> StopPriceSource:
    if isinstance(sample, MetaSample):
        return sample.trade.stop_price_source
    if isinstance(sample, DirectSample):
        sources = (sample.long_trade.stop_price_source, sample.short_trade.stop_price_source)
        if StopPriceSource.LAST in sources:
            return StopPriceSource.LAST
        return StopPriceSource.MARK
    raise TypeError("очікувалась meta- або пряма мітка")


def _both_reached(sample: MetaSample | DirectSample) -> bool:
    if isinstance(sample, MetaSample):
        return False
    if isinstance(sample, DirectSample):
        return sample.both_reached_tp
    raise TypeError("очікувалась meta- або пряма мітка")


def _sealed_holdout(
    windows: Sequence[tuple[int, int]],
    openings: Sequence[tuple[int, int, int]],
) -> tuple[int, int] | None:
    """Єдиний hold-out лінії, який ще не відпущений зсувом.

    Вікно відпущене, коли інше вікно лінії — експерименту чи відкриття — починається не раніше
    за його кінець. Кілька різних незакритих вікон — лінія вже зламана, новий експеримент її не поглиблює.
    """
    known = {*windows, *((row[0], row[1]) for row in openings)}

    def released(end: int) -> bool:
        return any(other_from >= end for other_from, _other_to in known)

    sealed = {(start, end) for start, end in known if not released(end)}
    if not sealed:
        return None
    if len(sealed) != 1:
        raise SplitLeakError("у лінії кілька різних незакритих hold-out")
    return next(iter(sealed))


def _opening_no(existing: Sequence[tuple[int, int, int]], from_ms: int, to_ms: int) -> int:
    """`existing` — `(from, to, opening_no)` лінії. Старе вікно після зсуву знову не відкривається."""
    if not existing:
        return 1
    latest_to = max(row[1] for row in existing)
    latest = [row for row in existing if row[1] == latest_to]
    latest_from = latest[0][0]
    if any(row[0] != latest_from for row in latest):
        raise HoldoutNotNewerError("у лінії два різні вікна з одним кінцем")
    if (from_ms, to_ms) == (latest_from, latest_to):
        nxt = max(row[2] for row in latest) + 1
        if nxt > 2:
            raise HoldoutLimitError("ліміт відкриттів цього hold-out вичерпано")
        return nxt
    if from_ms < latest_to:
        raise HoldoutNotNewerError("новий hold-out має починатися не раніше за кінець попереднього")
    return 1


def _is_opening_limit(exc: IntegrityError) -> bool:
    text = str(exc.orig or exc).lower()
    return any(name in text for name in _OPENING_LIMIT)


def _fold_block(
    record: FoldRecord,
    *,
    split: ResearchSplit,
    fold_index: int,
    symbols: Sequence[str],
    seed: int,
    research_line: str,
) -> tuple[dict[str, Any], list[str], list[int], bool, bool]:
    got = [report.symbol for report in record.reports]
    if len(got) != len(set(got)) or set(got) != set(symbols):
        raise ValueError("прогін фолду має бути рівно по символах експерименту")
    if len({report.cost_factor for report in record.reports}) != 1:
        raise ValueError("cost_factor має збігатися між символами фолду")
    trading: BacktestReport | None = None
    for report in record.reports:
        if report.seed != seed:
            raise ValueError("seed прогону не збігається з експериментом")
        _report_matches_trades(report)
        for trade in report.trades:
            _trade_inside(trade, split, fold_index)
        if report.symbol == research_line:
            trading = report
    if trading is None:
        raise ValueError("немає прогону торгового символу")
    ordered = sorted(trading.trades, key=lambda trade: (trade.entry_open_time_ms, trade.entry_index))
    series = [_decimal(trade.r_multiple) for trade in ordered]
    entries = [trade.entry_open_time_ms for trade in ordered]
    last_trade = any(
        trade.stop_price_source is StopPriceSource.LAST for report in record.reports for trade in report.trades
    )
    any_last = last_trade or record.train_labels.n_last_stop > 0 or record.test_labels.n_last_stop > 0
    any_both = record.train_labels.n_both_reached_tp > 0 or record.test_labels.n_both_reached_tp > 0
    fold = split.folds[fold_index]
    block = {
        "fold_index": fold.index,
        "test_start_ms": fold.test_start_ms,
        "test_end_ms": fold.test_end_ms,
        "extended": fold.extended,
        "below_min_trades": fold.below_min_trades,
        "cost_factor": trading.cost_factor,
        "trading": _report_json(trading),
        "all_symbols": _pool(record.reports),
        "labels": {"train": _label_json(record.train_labels), "test": _label_json(record.test_labels)},
    }
    return block, series, entries, any_last, any_both


def _report_matches_trades(report: BacktestReport) -> None:
    trades = report.trades
    metrics = report.metrics
    if metrics.n_trades != len(trades):
        raise ValueError("n_trades не збігається з журналом угод")
    # Кожна угода — один ENTER: без рішень журнал не знав би, скільки сигналів отримав фолд.
    if sum(1 for decision in report.decisions if decision.decision == "ENTER") != len(trades):
        raise ValueError("рішень ENTER не стільки, скільки угод")
    incomplete = sum(1 for trade in trades if trade.exit_reason is ExitReason.DATA_GAP)
    gaps = sum(1 for trade in trades if trade.funding_gap)
    if metrics.n_incomplete != incomplete:
        raise ValueError("n_incomplete не збігається з угодами")
    if metrics.n_funding_gaps != gaps:
        raise ValueError("n_funding_gaps не збігається з угодами")
    total_r = sum((trade.r_multiple for trade in trades), Decimal(0))
    total_net = sum((trade.net_usd for trade in trades), Decimal(0))
    if metrics.total_r != total_r or metrics.total_net_usd != total_net:
        raise ValueError("суми R або PnL не збігаються з угодами")
    profit = sum((trade.net_usd for trade in trades if trade.net_usd > 0), Decimal(0))
    loss = sum((-trade.net_usd for trade in trades if trade.net_usd < 0), Decimal(0))
    wins = sum(1 for trade in trades if trade.net_usd > 0)
    profit_factor = None if loss == 0 else profit / loss
    hit_rate = None if not trades else Decimal(wins) / Decimal(len(trades))
    if metrics.profit_factor != profit_factor or metrics.hit_rate != hit_rate:
        raise ValueError("profit factor або hit rate не збігається з угодами")
    stopped = any(trade.exit_reason is ExitReason.EMERGENCY_STOP for trade in trades)
    if stopped and not metrics.halted:
        raise ValueError("EMERGENCY_STOP без halted")


def _trade_inside(trade: BacktestTrade, split: ResearchSplit, fold_index: int) -> None:
    fold = split.folds[fold_index]
    entry = trade.entry_open_time_ms
    if entry >= split.holdout_from_ms or trade.exit_open_time_ms >= split.holdout_from_ms:
        raise SplitLeakError("угода заходить у hold-out")
    span = trade.exit_open_time_ms - entry
    if span < 0 or span > split.horizon_bars * split.bar_ms:
        raise SplitLeakError("угода довша за горизонт розбивки")
    if entry < fold.test_start_ms or entry >= fold.test_end_ms:
        raise SplitLeakError("вхід угоди поза тестом фолду")


def _pool(reports: Sequence[BacktestReport]) -> dict[str, Any]:
    """Суми вже порахованих угод. Просадка і Sharpe лишаються в прогоні кожного символу."""
    trades = [trade for report in reports for trade in report.trades]
    total_net = sum((trade.net_usd for trade in trades), Decimal(0))
    total_r = sum((trade.r_multiple for trade in trades), Decimal(0))
    gross_profit = sum((trade.net_usd for trade in trades if trade.net_usd > 0), Decimal(0))
    gross_loss = sum((-trade.net_usd for trade in trades if trade.net_usd < 0), Decimal(0))
    wins = sum(1 for trade in trades if trade.net_usd > 0)
    n_trades = len(trades)
    return {
        "n_trades": n_trades,
        "total_r": total_r,
        "total_net_usd": total_net,
        "profit_factor": None if gross_loss == 0 else gross_profit / gross_loss,
        "hit_rate": None if n_trades == 0 else Decimal(wins) / Decimal(n_trades),
        "n_incomplete": sum(report.metrics.n_incomplete for report in reports),
        "n_unusable": sum(report.metrics.n_unusable for report in reports),
        "n_funding_gaps": sum(report.metrics.n_funding_gaps for report in reports),
        "per_symbol": [_report_json(report) for report in sorted(reports, key=lambda report: report.symbol)],
    }


def _report_json(report: BacktestReport) -> dict[str, Any]:
    return {"symbol": report.symbol, "n_signals": report.n_signals, "metrics": _metrics_json(report.metrics)}


def _metrics_json(metrics: BacktestMetrics) -> dict[str, Any]:
    return {
        "n_trades": metrics.n_trades,
        "total_r": metrics.total_r,
        "total_net_usd": metrics.total_net_usd,
        "profit_factor": metrics.profit_factor,
        "max_drawdown_pct": metrics.max_drawdown_pct,
        "hit_rate": metrics.hit_rate,
        "sharpe": metrics.sharpe,
        "max_consecutive_losses": metrics.max_consecutive_losses,
        "n_incomplete": metrics.n_incomplete,
        "n_unusable": metrics.n_unusable,
        "n_funding_gaps": metrics.n_funding_gaps,
        "ending_equity": metrics.ending_equity,
        "peak_equity": metrics.peak_equity,
        "halted": metrics.halted,
    }


def _label_json(stats: LabelStats) -> dict[str, Any]:
    return {
        "n_labels": stats.n_labels,
        "n_last_stop": stats.n_last_stop,
        "n_both_reached_tp": stats.n_both_reached_tp,
        "last_stop_share": stats.last_stop_share,
        "both_reached_tp_share": stats.both_reached_tp_share,
    }


def _experiment_insert(
    *,
    experiment_id: str,
    research_line: str,
    symbols: list[str],
    label_type: str,
    primary_strategy: Any,
    params: Any,
    seed: int,
    split: ResearchSplit,
) -> Any:
    """INSERT експерименту. Окремо, щоб `create_experiment` лишався коротким."""
    return insert(Experiment).values(
        id=experiment_id,
        research_line=research_line,
        symbols=symbols,
        timeframe=_TIMEFRAME,
        label_type=label_type,
        primary_strategy=primary_strategy,
        params=params,
        seed=seed,
        data_from_ms=split.data_from_ms,
        data_to_ms=split.data_to_ms,
        created_at_ms=now_ms(),
    )


async def _next_experiment_id(session: AsyncSession) -> str:
    ids = (await session.execute(select(Experiment.id))).scalars().all()
    numbers = [int(match.group(1)) for item in ids if (match := _EXPERIMENT_ID.fullmatch(item))]
    return f"exp-{max(numbers, default=0) + 1:04d}"


async def _next_trial_no(session: AsyncSession, experiment_id: str) -> int:
    stmt = select(func.max(ExperimentTrial.trial_no)).where(ExperimentTrial.experiment_id == experiment_id)
    current = (await session.execute(stmt)).scalar_one()
    return int(current or 0) + 1


async def _experiment(session: AsyncSession, experiment_id: str) -> Experiment:
    stmt = select(Experiment).where(Experiment.id == experiment_id)
    return (await session.execute(stmt)).scalar_one()


def _line_key(research_line: str) -> int:
    digest = hashlib.sha256(f"holdout:{research_line}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


async def _lock_line(session: AsyncSession, research_line: str) -> None:
    """Один запис розбивки або відкриття на лінію. Інакше два експерименти пройдуть перевірку разом."""
    await session.execute(select(func.pg_advisory_xact_lock(_line_key(research_line))))


async def _line_windows(session: AsyncSession, research_line: str) -> list[tuple[int, int]]:
    stmt = select(Experiment.params).where(Experiment.research_line == research_line)
    windows: list[tuple[int, int]] = []
    for params in (await session.execute(stmt)).scalars():
        validation = params.get("validation") if isinstance(params, dict) else None
        if not isinstance(validation, dict):
            raise SplitLeakError("в експерименту лінії немає розбивки")
        windows.append((int(validation["holdout_from_ms"]), int(validation["holdout_to_ms"])))
    return windows


async def _openings(session: AsyncSession, research_line: str) -> list[tuple[int, int, int]]:
    stmt = select(HoldoutOpening.holdout_from_ms, HoldoutOpening.holdout_to_ms, HoldoutOpening.opening_no).where(
        HoldoutOpening.research_line == research_line
    )
    return [(int(row[0]), int(row[1]), int(row[2])) for row in (await session.execute(stmt)).all()]


def _symbols(research_line: str, symbols: Sequence[str]) -> str:
    if not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("список символів порожній або має дубль")
    for symbol in symbols:
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError("символ не канонічний")
    if not _SYMBOL.fullmatch(research_line) or research_line not in symbols:
        raise ValueError("лінія має бути одним із символів експерименту")
    return research_line


def _jsonable(value: Any, *, path: str) -> Any:
    if value is None or type(value) is bool or type(value) is int or type(value) is str:
        return value
    if type(value) is Decimal:
        return _decimal(value)
    if type(value) is float:
        raise TypeError(f"{path}: float заборонений")
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item, path=f"{path}.{key}") for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    raise TypeError(f"{path}: несеріалізований тип {type(value).__name__}")


def _decimal(value: Decimal) -> str:
    return format(value, "f")


def _fraction(fraction: Decimal) -> None:
    if type(fraction) is not Decimal or not fraction.is_finite():
        raise TypeError("частка hold-out має бути Decimal")
    if fraction < HOLDOUT_FRACTION_MIN or fraction > HOLDOUT_FRACTION_MAX:
        raise ValueError("частка hold-out поза 15–20%")


def _millis(name: str, value: object) -> int:
    if type(value) is not int or value < 0:
        raise TypeError(f"{name} має бути невід'ємний int мс")
    return value


def _bars(name: str, value: object) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} має бути ціле ≥ 1")
    return value


def _duration(name: str, value: object) -> int:
    if type(value) is not int or value <= 0 or value % HOUR_MS != 0:
        raise ValueError(f"{name} має бути додатна ціла кількість годин")
    return value
