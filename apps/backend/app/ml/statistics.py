"""Блочний bootstrap, DSR, PBO, коридор і Gate G1 (ТЗ §5.2.5, §5.3, §5.7, §5А.10).

DSR (Bailey & López de Prado, 2014) рахується з ряду R торгового символу, не з річного Sharpe
барів бектесту. Кількість спроб — уся лінія, включно з `FAILED` і `PRUNED`; розкид Sharpe між
спробами — лише з рядів, де угод не менше за мінімум G1. PBO — CSCV на 16 блоках за часом
(Bailey, Borwein, López de Prado & Zhu, 2015): рядок матриці — доба відрізка walk-forward
кандидата, однакова для всіх спроб; частка розбиттів, де стратегія, найкраща на одній половині
блоків, на іншій не вища за медіану.

Блочний bootstrap групує угоди за тижнем входу (7 діб від епохи UTC) і з цих блоків набирає
шлях тієї самої довжини. Останній блок шляху може бути обрізаний. PF і коридор беруть уже
порахований `r_multiple`, включно з `DATA_GAP`, `EMERGENCY_STOP` і `funding_gap`.

Hold-out відкривається лише через `open_holdout_after_gate`: публічного відкриття без G1 немає.
Знімок DSR, PBO і `n_trials_line` на експерименті пишеться лише разом із відкриттям.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Context, Decimal, DivisionByZero, InvalidOperation, Overflow, localcontext
from itertools import combinations
from typing import Any

import numpy as np
from pydantic import model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.ml.backtest import BacktestReport, BacktestTrade
from app.ml.features import HOUR_MS
from app.ml.models import LABEL_TYPES, Experiment, ExperimentTrial
from app.ml.validation import MIN_TRADES_PER_FOLD, RecordedOpening, SplitLeakError, _lock_line, _open_holdout
from app.risk.limits import next_consecutive_losses
from app.risk.numbers import FrozenModel, require_decimal

DAY_MS: int = 24 * HOUR_MS
WEEK_MS: int = 7 * DAY_MS
CSCV_BLOCKS: int = 16
DSR_MIN: Decimal = Decimal("0.95")
PBO_MAX: Decimal = Decimal("0.5")
MEDIAN_PF_MIN: Decimal = Decimal("1.15")
PROFITABLE_FOLD_SHARE: Decimal = Decimal("0.70")
MIN_OOS_TRADES: int = 100
STRESS_MEDIAN_PF_MIN: Decimal = Decimal("1")
STRESS_COST_FACTOR: Decimal = Decimal("2")
BASE_COST_FACTOR: Decimal = Decimal("1")
CORRIDOR_QUANTILE: Decimal = Decimal("0.05")
DEFAULT_BOOTSTRAP_REPLICATES: int = 1000
MIN_BOOTSTRAP_REPLICATES: int = 20
DEFAULT_CONFIDENCE: Decimal = Decimal("0.90")
# Скільки чисел float64 займає одна порція розбиттів CSCV (≈ 8 МБ), хоч би скільки було спроб.
_CSCV_CELLS: int = 1 << 20
# Euler–Mascheroni, множник очікуваного максимуму Sharpe (Bailey & López de Prado, 2014).
_EULER: float = 0.5772156649015329
_CONTEXT = Context(
    prec=28,
    rounding=ROUND_HALF_EVEN,
    traps=[InvalidOperation, DivisionByZero, Overflow],
)

G1_MIN_TRADES_FOLD = "G1_MIN_TRADES_FOLD"
G1_MIN_TRADES_TOTAL = "G1_MIN_TRADES_TOTAL"
G1_MEDIAN_PF = "G1_MEDIAN_PF"
G1_FOLD_SHARE = "G1_FOLD_SHARE"
G1_COST_FACTOR = "G1_COST_FACTOR"
G1_STRESS = "G1_STRESS"
G1_FILTER = "G1_FILTER"
G1_DSR = "G1_DSR"
G1_PBO = "G1_PBO"

# Acklam (2003): раціональне наближення квантиля стандартної нормалі, похибка < 1.2e-9.
_PPF_A = (
    -3.969683028665376e01,
    2.209460984245205e02,
    -2.759285104469687e02,
    1.383577518672690e02,
    -3.066479806614716e01,
    2.506628277459239e00,
)
_PPF_B = (
    -5.447609879822406e01,
    1.615858368580409e02,
    -1.556989798598866e02,
    6.680131188771972e01,
    -1.328068155288572e01,
)
_PPF_C = (
    -7.784894002430293e-03,
    -3.223964580411385e-01,
    -2.400758277161838e00,
    -2.549732539343734e00,
    4.374664141464968e00,
    2.938163982698783e00,
)
_PPF_D = (
    7.784695709041462e-03,
    3.224671290700398e-01,
    2.445134137142996e00,
    3.754408661907416e00,
)
_PPF_LOW = 0.02425


class GateRejected(Exception):
    """Hold-out не відкривається: walk-forward не пройшов Gate G1."""

    def __init__(self, decision: GateDecision) -> None:
        self.decision = decision
        super().__init__("Gate G1 не пройдено: " + ", ".join(decision.reasons))


class ReturnMoments(FrozenModel):
    """Sharpe на угоду, асиметрія і сирий ексцес (у нормального розподілу 3, не 0)."""

    sharpe: Decimal
    skewness: Decimal
    kurtosis: Decimal
    n_obs: int


class BlockBootstrapPf(FrozenModel):
    """90% інтервал PF за блоками тижня. `*_no_losses` — у цій точці збиткових угод не було."""

    observed: Decimal | None
    observed_no_losses: bool
    lower: Decimal | None
    upper: Decimal | None
    lower_no_losses: bool
    upper_no_losses: bool
    n_replicates: int
    n_blocks: int
    n_trades: int
    confidence: Decimal

    def lower_above(self, threshold: Decimal) -> bool:
        """Нижня межа строго вища за поріг. Відсутність збитків у хвості це виконує."""
        require_decimal("threshold", threshold)
        if self.lower_no_losses:
            return True
        if self.lower is None:
            return False
        return self.lower > threshold

    @model_validator(mode="after")
    def _bounds(self) -> BlockBootstrapPf:
        require_decimal("confidence", self.confidence)
        if self.observed is not None:
            require_decimal("observed", self.observed, non_negative=True)
        if self.lower is not None:
            require_decimal("lower", self.lower, non_negative=True)
        if self.upper is not None:
            require_decimal("upper", self.upper, non_negative=True)
        if self.lower_no_losses != (self.lower is None) or self.upper_no_losses != (self.upper is None):
            raise ValueError("межа PF не збігається з позначкою відсутності збитків")
        if self.observed_no_losses and self.observed is not None:
            raise ValueError("PF без збитків не має скінченного значення")
        if self.n_replicates < MIN_BOOTSTRAP_REPLICATES or self.n_blocks < 1 or self.n_trades < 1:
            raise ValueError("розмір bootstrap")
        if self.confidence <= 0 or self.confidence >= 1:
            raise ValueError("confidence поза (0, 1)")
        return self


class LiveCorridor(FrozenModel):
    """5-й перцентиль сумарного R після кожних N угод і найдовша серія збитків зразка."""

    n_trades: tuple[int, ...]
    p5_cum_r: tuple[Decimal, ...]
    max_loss_streak: int

    @model_validator(mode="after")
    def _shape(self) -> LiveCorridor:
        if not self.n_trades or len(self.n_trades) != len(self.p5_cum_r):
            raise ValueError("коридор порожній або різної довжини")
        if self.n_trades != tuple(range(1, len(self.n_trades) + 1)):
            raise ValueError("N у коридорі має йти від 1")
        if self.max_loss_streak < 0:
            raise ValueError("серія збитків від'ємна")
        for value in self.p5_cum_r:
            require_decimal("p5_cum_r", value)
        return self


class FoldOutcome(FrozenModel):
    """Метрики торгового символу одного фолду. PF — з уже порахованого чистого результату.

    `n_signals` — скільки сигналів отримав прогін (ENTER і SKIP); угод не більше.
    """

    n_trades: int
    n_signals: int
    total_net_usd: Decimal
    profit_factor: Decimal | None
    below_min_trades: bool
    cost_factor: Decimal

    @model_validator(mode="after")
    def _fields(self) -> FoldOutcome:
        if self.n_trades < 0:
            raise ValueError("n_trades від'ємне")
        if self.n_signals < self.n_trades:
            raise ValueError("n_signals менше за n_trades: кожна угода — це сигнал")
        require_decimal("total_net_usd", self.total_net_usd)
        require_decimal("cost_factor", self.cost_factor, positive=True)
        if self.profit_factor is not None:
            require_decimal("profit_factor", self.profit_factor, non_negative=True)
        return self


class GateThresholds(FrozenModel):
    """Пороги ТЗ §5А.10. Мінімум угод фолду ще піднімається до значення розбивки, якщо воно вище.

    Пороги можна послабити (ТЗ: «пороги конфігуруються»), але вони потрапляють у запис відкриття
    разом зі списком `loosened_thresholds`. Стрес слабший за подвоєння витрат — не стрес: відмова.
    Базові фолди завжди з `cost_factor` 1, тож окремого порога для них немає.
    """

    median_pf_min: Decimal = MEDIAN_PF_MIN
    profitable_fold_share: Decimal = PROFITABLE_FOLD_SHARE
    min_trades_per_fold: int = MIN_TRADES_PER_FOLD
    min_oos_trades: int = MIN_OOS_TRADES
    stress_median_pf_min: Decimal = STRESS_MEDIAN_PF_MIN
    stress_cost_factor: Decimal = STRESS_COST_FACTOR
    dsr_min: Decimal = DSR_MIN
    pbo_max: Decimal = PBO_MAX

    @model_validator(mode="after")
    def _ranges(self) -> GateThresholds:
        require_decimal("median_pf_min", self.median_pf_min, positive=True)
        require_decimal("profitable_fold_share", self.profitable_fold_share, positive=True)
        if self.profitable_fold_share > 1:
            raise ValueError("частка прибуткових фолдів більша за 1")
        if self.min_trades_per_fold < 1 or self.min_oos_trades < 1:
            raise ValueError("мінімум угод має бути ціле ≥ 1")
        require_decimal("stress_median_pf_min", self.stress_median_pf_min, positive=True)
        require_decimal("stress_cost_factor", self.stress_cost_factor, positive=True)
        if self.stress_cost_factor < STRESS_COST_FACTOR:
            raise ValueError("стрес слабший за подвоєння витрат")
        require_decimal("dsr_min", self.dsr_min, positive=True)
        require_decimal("pbo_max", self.pbo_max, positive=True)
        if self.dsr_min > 1 or self.pbo_max > 1:
            raise ValueError("поріг DSR або PBO більший за 1")
        return self


class GateDecision(FrozenModel):
    """Усі причини одразу. `passed` лише коли список порожній."""

    passed: bool
    reasons: tuple[str, ...]
    dsr: Decimal | None
    pbo: Decimal | None
    n_trials: int
    n_trades: int
    median_pf: Decimal | None
    median_pf_unbounded: bool
    profitable_fold_share: Decimal | None

    @model_validator(mode="after")
    def _match(self) -> GateDecision:
        if self.passed != (not self.reasons):
            raise ValueError("passed не збігається з reasons")
        if self.n_trials < 1 or self.n_trades < 0:
            raise ValueError("лічильник Gate G1")
        for name, value in (("dsr", self.dsr), ("pbo", self.pbo)):
            if value is not None:
                require_decimal(name, value, non_negative=True)
                if value > 1:
                    raise ValueError(f"{name} більший за 1")
        if self.median_pf is not None:
            require_decimal("median_pf", self.median_pf, non_negative=True)
        if self.profitable_fold_share is not None:
            require_decimal("profitable_fold_share", self.profitable_fold_share, non_negative=True)
            if self.profitable_fold_share > 1:
                raise ValueError("частка фолдів більша за 1")
        return self


class TrialSeries(FrozenModel):
    """Ряд спроби для PBO: R, час входу кожної угоди і відрізок walk-forward, який спроба покрила.

    Відрізок — від початку першого тесту до кінця останнього записаного фолду. Спроба без фолдів
    (`FAILED`/`PRUNED` з порожнім префіксом) відрізка не має: у PBO не входить, у N для DSR — так.
    """

    returns: tuple[Decimal, ...]
    entry_ms: tuple[int, ...]
    covered_from_ms: int | None
    covered_to_ms: int | None

    @model_validator(mode="after")
    def _shape(self) -> TrialSeries:
        if len(self.returns) != len(self.entry_ms):
            raise ValueError("довжини R і часу входу різні")
        for value in self.returns:
            require_decimal("r", value)
        if (self.covered_from_ms is None) != (self.covered_to_ms is None):
            raise ValueError("відрізок спроби має обидві межі або жодної")
        if self.covered_from_ms is None or self.covered_to_ms is None:
            if self.returns:
                raise ValueError("угоди без відрізка walk-forward")
            return self
        if self.covered_from_ms < 0 or self.covered_to_ms <= self.covered_from_ms:
            raise ValueError("відрізок спроби порожній або від'ємний")
        previous = self.covered_from_ms
        for entry in self.entry_ms:
            if entry < self.covered_from_ms or entry >= self.covered_to_ms:
                raise ValueError("час входу поза відрізком спроби")
            if entry < previous:
                raise ValueError("час входу не в порядку фолдів")
            previous = entry
        return self


@dataclass(frozen=True, slots=True)
class _PathPf:
    value: Decimal | None
    infinite: bool


def parse_oos_results(raw: Sequence[object]) -> tuple[Decimal, ...]:
    """Ряд R із журналу. Лише рядки `Decimal`: float у JSON втратив би хвіст."""
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise TypeError("oos_results має бути послідовністю")
    parsed: list[Decimal] = []
    for item in raw:
        parsed.append(_decimal_text(item))
    return tuple(parsed)


def trial_series_from_journal(
    oos_results: Sequence[object],
    oos_entry_ms: Sequence[object],
    metrics: Mapping[str, Any],
) -> TrialSeries:
    """Ряд спроби з журналу. Рядок без часу кожної угоди (до міграції 0005) — відмова, а не пропуск."""
    returns = parse_oos_results(oos_results)
    if not isinstance(oos_entry_ms, Sequence) or isinstance(oos_entry_ms, (str, bytes)):
        raise TypeError("oos_entry_ms має бути послідовністю")
    entries: list[int] = []
    for item in oos_entry_ms:
        if type(item) is not int:
            raise ValueError("час входу має бути int мс")
        entries.append(item)
    if len(entries) != len(returns):
        raise ValueError("у спроби немає часу входу кожної угоди (рядок до міграції 0005)")
    folds = metrics.get("folds")
    if not isinstance(folds, list):
        raise ValueError("у спроби немає списку фолдів")
    if not folds:
        return TrialSeries(returns=returns, entry_ms=tuple(entries), covered_from_ms=None, covered_to_ms=None)
    first, last = folds[0], folds[-1]
    if not isinstance(first, Mapping) or not isinstance(last, Mapping):
        raise ValueError("фолд журналу не об'єкт")
    start = first.get("test_start_ms")
    end = last.get("test_end_ms")
    if type(start) is not int or type(end) is not int:
        raise ValueError("межі тесту фолду мають бути int мс")
    return TrialSeries(returns=returns, entry_ms=tuple(entries), covered_from_ms=start, covered_to_ms=end)


def r_profit_factor(returns: Sequence[Decimal]) -> Decimal | None:
    """PF за R. None, якщо від'ємних R немає: ділити на нуль не можна."""
    with localcontext(_CONTEXT):
        checked = _decimal_returns(returns)
        if not any(value < 0 for value in checked):
            return None
        return _classify(checked).value


def loss_streak(returns: Sequence[Decimal]) -> int:
    """Найдовша серія збитків. Нуль її не подовжує і не скидає — як `next_consecutive_losses`."""
    streak = 0
    longest = 0
    for value in _decimal_returns(returns):
        streak = next_consecutive_losses(streak, value)
        if streak > longest:
            longest = streak
    return longest


def block_bootstrap_pf(
    trades: Sequence[BacktestTrade],
    *,
    n_replicates: int = DEFAULT_BOOTSTRAP_REPLICATES,
    seed: int,
    confidence: Decimal = DEFAULT_CONFIDENCE,
) -> BlockBootstrapPf:
    """Довірчий інтервал PF. Той самий перебір тижнів, що й коридор."""
    require_decimal("confidence", confidence)
    if confidence <= 0 or confidence >= 1:
        raise ValueError("confidence поза (0, 1)")
    with localcontext(_CONTEXT):
        blocks = _week_blocks(trades)
        observed_returns = tuple(value for block in blocks for value in block)
        paths = _resample(blocks, n_replicates=n_replicates, seed=seed)
        samples = [_classify(path) for path in paths]
        low_q = (Decimal(1) - confidence) / 2
        high_q = Decimal(1) - low_q
        lower = _pf_percentile(samples, low_q)
        upper = _pf_percentile(samples, high_q)
        observed = _classify(observed_returns)
        has_loss = any(value < 0 for value in observed_returns)
        return BlockBootstrapPf(
            observed=None if observed.infinite or not has_loss else observed.value,
            observed_no_losses=observed.infinite,
            lower=None if lower.infinite else lower.value,
            upper=None if upper.infinite else upper.value,
            lower_no_losses=lower.infinite,
            upper_no_losses=upper.infinite,
            n_replicates=n_replicates,
            n_blocks=len(blocks),
            n_trades=len(observed_returns),
            confidence=confidence,
        )


def live_corridor(
    trades: Sequence[BacktestTrade],
    *,
    n_replicates: int = DEFAULT_BOOTSTRAP_REPLICATES,
    seed: int,
) -> LiveCorridor:
    """Для кожного N — 5-й перцентиль сумарного R після N угод. Серія збитків — зі зразка, не з перебору."""
    with localcontext(_CONTEXT):
        blocks = _week_blocks(trades)
        observed = tuple(value for block in blocks for value in block)
        paths = _resample(blocks, n_replicates=n_replicates, seed=seed)
        totals = [_cumulative(path) for path in paths]
        p5 = tuple(_percentile([row[index] for row in totals], CORRIDOR_QUANTILE) for index in range(len(observed)))
        return LiveCorridor(
            n_trades=tuple(range(1, len(observed) + 1)),
            p5_cum_r=p5,
            max_loss_streak=loss_streak(observed),
        )


def corridor_payload(corridor: LiveCorridor) -> dict[str, Any]:
    """`live_corridor` для метаданих: R рядком Decimal, без float."""
    if type(corridor) is not LiveCorridor:
        raise TypeError("коридор має бути LiveCorridor")
    return {
        "n_trades": list(corridor.n_trades),
        "p5_cum_r": [format(value, "f") for value in corridor.p5_cum_r],
        "max_loss_streak": corridor.max_loss_streak,
    }


def return_moments(returns: Sequence[Decimal]) -> ReturnMoments | None:
    """Моменти ряду R. None, якщо спостережень менше 4 або розкид нульовий."""
    moments = _moments(_float_returns(returns))
    if moments is None:
        return None
    sharpe, skew, kurt = moments
    return ReturnMoments(
        sharpe=_moment_decimal(sharpe),
        skewness=_moment_decimal(skew),
        kurtosis=_moment_decimal(kurt),
        n_obs=len(returns),
    )


def deflated_sharpe_ratio(
    returns: Sequence[Decimal],
    *,
    trial_returns: Sequence[Sequence[Decimal]],
    n_trials: int,
    variance_min_trades: int = MIN_OOS_TRADES,
) -> Decimal | None:
    """Імовірність, що Sharpe спроби вищий за очікуваний максимум з `n_trials` спроб.

    `trial_returns` — ряди всіх спроб лінії, включно з порожніми; їхня кількість дорівнює `n_trials`.
    Розкид Sharpe між спробами береться лише з рядів, де угод не менше за `variance_min_trades`:
    ряд із 2–3 угод дає Sharpe у десятки і назавжди обвалив би DSR лінії. Коротші ряди лишаються
    в N. Менше двох таких оцінок при `n_trials` > 1 — дисперсія однієї оцінки за нульової
    істинної Sharpe, `1/(T−1)`.
    """
    if type(n_trials) is not int or isinstance(n_trials, bool) or n_trials < 1:
        raise ValueError("n_trials має бути ціле ≥ 1")
    if type(variance_min_trades) is not int or isinstance(variance_min_trades, bool) or variance_min_trades < 2:
        raise ValueError("variance_min_trades має бути ціле ≥ 2")
    if not isinstance(trial_returns, Sequence) or isinstance(trial_returns, (str, bytes)):
        raise TypeError("ряди спроб мають бути послідовністю")
    if n_trials != len(trial_returns):
        raise ValueError("n_trials має дорівнювати кількості рядів")
    candidate = _float_returns(returns)
    moments = _moments(candidate)
    if moments is None:
        return None
    sharpe, skew, kurt = moments
    spread = [
        value
        for values in (_float_returns(series) for series in trial_returns)
        if values.size >= variance_min_trades and (value := _sharpe(values)) is not None
    ]
    if n_trials == 1:
        variance = 0.0
    elif len(spread) >= 2:
        mean_sr = sum(spread) / len(spread)
        variance = sum((item - mean_sr) ** 2 for item in spread) / (len(spread) - 1)
    else:
        variance = 1.0 / (len(candidate) - 1)
    benchmark = _selection_benchmark(n_trials, variance)
    probability = _psr(sharpe, benchmark, len(candidate), skew, kurt)
    if probability is None:
        return None
    return _probability(probability)


def probability_of_backtest_overfitting(
    trials: Sequence[TrialSeries],
    *,
    candidate_index: int,
    n_blocks: int = CSCV_BLOCKS,
    bin_ms: int = DAY_MS,
) -> Decimal | None:
    """PBO методом CSCV за часом. None, якщо відрізок кандидата коротший за блоки або немає суперника.

    Рядок матриці — `bin_ms` (доба) від початку відрізка walk-forward кандидата, сума R угод з
    входом у цю добу; доба без угод — 0. Матриця бере кратну `n_blocks` кількість діб від початку.
    Входить кожна спроба, що покрила весь відрізок кандидата, хоч би скільки в неї угод; її угоди
    поза відрізком не беруться. Решта — лише в N для DSR.
    """
    blocks_ok = type(n_blocks) is int and not isinstance(n_blocks, bool)
    if not blocks_ok or not 2 <= n_blocks <= CSCV_BLOCKS or n_blocks % 2 != 0:
        raise ValueError("число блоків CSCV парне від 2 до 16")
    if type(bin_ms) is not int or isinstance(bin_ms, bool) or bin_ms < 1:
        raise ValueError("bin_ms має бути int ≥ 1")
    if not isinstance(trials, Sequence) or isinstance(trials, (str, bytes)) or not trials:
        raise ValueError("немає рядів для PBO")
    _sequence("trials", trials, TrialSeries)
    if type(candidate_index) is not int or isinstance(candidate_index, bool):
        raise ValueError("candidate_index має бути int")
    if candidate_index < 0 or candidate_index >= len(trials):
        raise ValueError("candidate_index поза рядами")
    candidate = trials[candidate_index]
    if candidate.covered_from_ms is None or candidate.covered_to_ms is None:
        return None
    start = candidate.covered_from_ms
    end = candidate.covered_to_ms
    usable = (-(-(end - start) // bin_ms) // n_blocks) * n_blocks
    half = n_blocks // 2
    if usable < n_blocks or (usable // n_blocks) * half < 2:
        return None
    with localcontext(_CONTEXT):
        columns = [_binned(series, start, bin_ms, usable) for series in trials if _covers(series, start, end)]
    if len(columns) < 2:
        return None
    return _probability(_cscv_fraction(np.column_stack(columns), n_blocks))


def fold_outcome_from_report(report: BacktestReport, *, below_min_trades: bool) -> FoldOutcome:
    """Фолд стресу (чи будь-якого прогону) з `run_backtest`: сигнали — з журналу рішень прогону."""
    if type(report) is not BacktestReport:
        raise TypeError("очікувався BacktestReport")
    if type(below_min_trades) is not bool:
        raise TypeError("below_min_trades має бути bool")
    return FoldOutcome(
        n_trades=report.metrics.n_trades,
        n_signals=report.n_signals,
        total_net_usd=report.metrics.total_net_usd,
        profit_factor=report.metrics.profit_factor,
        below_min_trades=below_min_trades,
        cost_factor=report.cost_factor,
    )


def fold_outcomes_from_metrics(metrics: Mapping[str, Any]) -> tuple[FoldOutcome, ...]:
    """Фолди спроби з журналу: торговий символ, не пул усіх символів."""
    folds = metrics.get("folds")
    if not isinstance(folds, list):
        raise ValueError("у спроби немає списку фолдів")
    return tuple(_fold_outcome(fold) for fold in folds)


def loosened_thresholds(thresholds: GateThresholds) -> tuple[str, ...]:
    """Назви порогів, слабших за ТЗ §5А.10. Сильніші й рівні сюди не потрапляють."""
    default = GateThresholds()
    names = [
        name
        for name in (
            "median_pf_min",
            "profitable_fold_share",
            "min_trades_per_fold",
            "min_oos_trades",
            "stress_median_pf_min",
            "dsr_min",
        )
        if getattr(thresholds, name) < getattr(default, name)
    ]
    if thresholds.pbo_max > default.pbo_max:
        names.append("pbo_max")
    return tuple(names)


def evaluate_gate(
    *,
    label_type: str,
    folds: Sequence[FoldOutcome],
    stress: Sequence[FoldOutcome],
    dsr: Decimal | None,
    pbo: Decimal | None,
    n_trials: int,
    unfiltered_total_net: Decimal | None = None,
    thresholds: GateThresholds | None = None,
) -> GateDecision:
    """Gate G1 на walk-forward торгового символу. Hold-out у ці фолди не входить.

    «Не руйнується при подвоєнні витрат» — стрес із `cost_factor` 2 на тих самих сигналах (`n_signals`),
    медіана PF ≥ 1 і сумарний чистий результат > 0. Для `meta` фільтр має бути строго кращим за
    правило без фільтра за сумою `total_net_usd`; для `direct` і `rule_only` це порівняння не діє.
    """
    if label_type not in LABEL_TYPES:
        raise ValueError("невідомий label_type")
    if type(n_trials) is not int or isinstance(n_trials, bool) or n_trials < 1:
        raise ValueError("n_trials має бути ціле ≥ 1")
    limits = thresholds if thresholds is not None else GateThresholds()
    _sequence("folds", folds, FoldOutcome)
    _sequence("stress", stress, FoldOutcome)
    if dsr is not None:
        require_decimal("dsr", dsr)
    if pbo is not None:
        require_decimal("pbo", pbo)
    if unfiltered_total_net is not None:
        require_decimal("unfiltered_total_net", unfiltered_total_net)
    with localcontext(_CONTEXT):
        return _decide(
            label_type=label_type,
            folds=tuple(folds),
            stress=tuple(stress),
            dsr=dsr,
            pbo=pbo,
            n_trials=n_trials,
            unfiltered_total_net=unfiltered_total_net,
            thresholds=limits,
        )


async def open_holdout_after_gate(
    session: AsyncSession,
    *,
    experiment_id: str,
    trial_id: int,
    model_version: str,
    stress: Sequence[FoldOutcome],
    unfiltered_total_net: Decimal | None = None,
    thresholds: GateThresholds | None = None,
) -> RecordedOpening:
    """Відкрити hold-out експерименту, лише якщо ця COMPLETE-спроба пройшла Gate G1.

    DSR і PBO перераховуються з поточного журналу лінії. Розкид Sharpe для DSR — з рядів, де угод
    не менше за `min_oos_trades` G1. PBO — за часом; рядок журналу без часу угод — відмова.
    Відмова — `GateRejected` з рішенням
    (DSR, PBO, N усередині), без жодного запису в БД. Успіх пише відкриття з повним рішенням
    G1 (пороги, послаблені пороги, стрес, правило без фільтра) і лише тоді — знімок
    `n_trials_line`, `dsr`, `pbo` на експерименті: він завжди збігається з останнім відкриттям
    цього експерименту, а спроба — `result.trial_id` того відкриття.
    Зсув hold-out сюди не входить: його робить `create_experiment` з новішим вікном.
    """
    if type(trial_id) is not int or isinstance(trial_id, bool):
        raise ValueError("trial_id має бути int")
    limits = thresholds if thresholds is not None else GateThresholds()
    experiment, trial, rows = await _load_candidate(session, experiment_id, trial_id)
    stored = experiment.params.get("validation")
    if not isinstance(stored, dict):
        raise SplitLeakError("в експерименту немає розбивки")
    split_min = stored.get("min_trades_per_fold")
    if type(split_min) is not int or isinstance(split_min, bool) or split_min < 1:
        raise SplitLeakError("у розбивці немає min_trades_per_fold")
    effective = limits.model_copy(update={"min_trades_per_fold": max(limits.min_trades_per_fold, split_min)})
    series = [trial_series_from_journal(row.oos_results, row.oos_entry_ms, row.metrics) for row in rows]
    returns = [item.returns for item in series]
    candidate_index = next(index for index, row in enumerate(rows) if row.id == trial.id)
    dsr = deflated_sharpe_ratio(
        returns[candidate_index],
        trial_returns=returns,
        n_trials=len(series),
        variance_min_trades=max(2, effective.min_oos_trades),
    )
    pbo = probability_of_backtest_overfitting(series, candidate_index=candidate_index)
    decision = evaluate_gate(
        label_type=experiment.label_type,
        folds=fold_outcomes_from_metrics(trial.metrics),
        stress=stress,
        dsr=dsr,
        pbo=pbo,
        n_trials=len(series),
        unfiltered_total_net=unfiltered_total_net,
        thresholds=effective,
    )
    if not decision.passed or dsr is None or pbo is None:
        raise GateRejected(decision)
    opened = await _open_holdout(
        session,
        research_line=experiment.research_line,
        holdout_from_ms=int(stored["holdout_from_ms"]),
        holdout_to_ms=int(stored["holdout_to_ms"]),
        experiment_id=experiment.id,
        model_version=model_version,
        result={
            "gate": "G1",
            "passed": True,
            "trial_id": trial.id,
            "trial_no": trial.trial_no,
            "n_trials_line": len(series),
            "dsr": dsr,
            "pbo": pbo,
            "n_trades": decision.n_trades,
            "median_pf": decision.median_pf,
            "median_pf_unbounded": decision.median_pf_unbounded,
            "profitable_fold_share": decision.profitable_fold_share,
            "unfiltered_total_net": unfiltered_total_net,
            "stress": [item.model_dump() for item in stress],
            "thresholds": effective.model_dump(),
            "loosened": list(loosened_thresholds(effective)),
        },
    )
    # Після відкриття: відмова `_open_holdout` (ліміт, зсув) не лишає знімка без рядка відкриття.
    experiment.n_trials_line = len(series)
    experiment.dsr = dsr
    experiment.pbo = pbo
    await session.flush()
    return opened


def _decide(
    *,
    label_type: str,
    folds: tuple[FoldOutcome, ...],
    stress: tuple[FoldOutcome, ...],
    dsr: Decimal | None,
    pbo: Decimal | None,
    n_trials: int,
    unfiltered_total_net: Decimal | None,
    thresholds: GateThresholds,
) -> GateDecision:
    reasons: list[str] = []
    n_trades = sum(fold.n_trades for fold in folds)
    median, unbounded = _median_pf(folds)
    share = None if not folds else Decimal(sum(1 for fold in folds if fold.total_net_usd > 0)) / Decimal(len(folds))
    if not folds or any(fold.below_min_trades or fold.n_trades < thresholds.min_trades_per_fold for fold in folds):
        reasons.append(G1_MIN_TRADES_FOLD)
    if n_trades < thresholds.min_oos_trades:
        reasons.append(G1_MIN_TRADES_TOTAL)
    if unbounded:
        median_ok = True
    elif median is None:
        median_ok = False
    else:
        median_ok = median >= thresholds.median_pf_min
    if not median_ok:
        reasons.append(G1_MEDIAN_PF)
    if share is None or share < thresholds.profitable_fold_share:
        reasons.append(G1_FOLD_SHARE)
    if not folds or any(fold.cost_factor != BASE_COST_FACTOR for fold in folds):
        reasons.append(G1_COST_FACTOR)
    if not _stress_holds(folds, stress, thresholds):
        reasons.append(G1_STRESS)
    if label_type == "meta" and not _filter_better(folds, unfiltered_total_net):
        reasons.append(G1_FILTER)
    if dsr is None or dsr < thresholds.dsr_min:
        reasons.append(G1_DSR)
    if pbo is None or pbo >= thresholds.pbo_max:
        reasons.append(G1_PBO)
    return GateDecision(
        passed=not reasons,
        reasons=tuple(reasons),
        dsr=dsr,
        pbo=pbo,
        n_trials=n_trials,
        n_trades=n_trades,
        median_pf=median,
        median_pf_unbounded=unbounded,
        profitable_fold_share=share,
    )


def _stress_holds(
    folds: tuple[FoldOutcome, ...],
    stress: tuple[FoldOutcome, ...],
    thresholds: GateThresholds,
) -> bool:
    if len(stress) != len(folds) or not folds:
        return False
    if any(item.cost_factor != thresholds.stress_cost_factor for item in stress):
        return False
    # Ті самі входи, дорожче виконання: сигнали ті самі, а угод ліміти (cooldown) можуть пропустити.
    if any(item.n_signals != base.n_signals for item, base in zip(stress, folds, strict=True)):
        return False
    median, unbounded = _median_pf(stress)
    if not unbounded and (median is None or median < thresholds.stress_median_pf_min):
        return False
    return sum((item.total_net_usd for item in stress), Decimal(0)) > 0


def _filter_better(folds: tuple[FoldOutcome, ...], unfiltered_total_net: Decimal | None) -> bool:
    if unfiltered_total_net is None:
        return False
    return sum((fold.total_net_usd for fold in folds), Decimal(0)) > unfiltered_total_net


def _median_pf(folds: Sequence[FoldOutcome]) -> tuple[Decimal | None, bool]:
    """Медіана PF. Фольд без збитків і з додатним результатом стоїть вище за будь-який скінченний PF.

    Для парної кількості, якщо верхній із двох центральних беззбитковий, порівнюється нижній:
    усереднювати зі знаком «нескінченність» немає сенсу, а нижній не дає одному ідеальному фолду
    сховати сусіда нижче за поріг.
    """
    if not folds:
        return None, False
    ranked = [_pf_rank(fold) for fold in folds]
    ranked.sort(key=lambda item: (1, Decimal(0)) if item is None else (0, item))
    count = len(ranked)
    if count % 2 == 1:
        middle = ranked[count // 2]
        if middle is None:
            return None, True
        return middle, False
    low = ranked[count // 2 - 1]
    high = ranked[count // 2]
    if low is None:
        return None, True
    if high is None:
        return low, False
    return (low + high) / 2, False


def _pf_rank(fold: FoldOutcome) -> Decimal | None:
    if fold.profit_factor is None:
        if fold.total_net_usd > 0:
            return None
        return Decimal(0)
    return fold.profit_factor


def _fold_outcome(fold: object) -> FoldOutcome:
    if not isinstance(fold, Mapping):
        raise ValueError("фолд журналу не об'єкт")
    trading = fold.get("trading")
    if not isinstance(trading, Mapping):
        raise ValueError("у фолду немає торгового прогону")
    metrics = trading.get("metrics")
    if not isinstance(metrics, Mapping):
        raise ValueError("у фолду немає метрик")
    profit_factor = metrics.get("profit_factor")
    n_trades = metrics.get("n_trades")
    n_signals = trading.get("n_signals")
    below = fold.get("below_min_trades")
    if type(n_trades) is not int or isinstance(n_trades, bool) or n_trades < 0:
        raise ValueError("n_trades фолду має бути невід'ємний int")
    if type(n_signals) is not int or isinstance(n_signals, bool) or n_signals < 0:
        raise ValueError("n_signals фолду має бути невід'ємний int")
    if type(below) is not bool:
        raise ValueError("below_min_trades має бути bool")
    return FoldOutcome(
        n_trades=n_trades,
        n_signals=n_signals,
        total_net_usd=_decimal_text(metrics.get("total_net_usd")),
        profit_factor=None if profit_factor is None else _decimal_text(profit_factor),
        below_min_trades=below,
        cost_factor=_decimal_text(fold.get("cost_factor")),
    )


async def _load_candidate(
    session: AsyncSession,
    experiment_id: str,
    trial_id: int,
) -> tuple[Experiment, ExperimentTrial, list[ExperimentTrial]]:
    experiment = (await session.execute(select(Experiment).where(Experiment.id == experiment_id))).scalar_one()
    await _lock_line(session, experiment.research_line)
    await session.refresh(experiment)
    stmt = (
        select(ExperimentTrial)
        .join(Experiment, Experiment.id == ExperimentTrial.experiment_id)
        .where(Experiment.research_line == experiment.research_line)
        .order_by(ExperimentTrial.created_at_ms, ExperimentTrial.id)
    )
    rows = list((await session.execute(stmt)).scalars())
    trial = next((row for row in rows if row.id == trial_id), None)
    if trial is None or trial.experiment_id != experiment_id:
        raise ValueError("спроба не належить експерименту")
    if trial.state != "COMPLETE":
        raise ValueError("Gate G1 оцінює лише COMPLETE")
    return experiment, trial, rows


def _covers(series: TrialSeries, start: int, end: int) -> bool:
    if series.covered_from_ms is None or series.covered_to_ms is None:
        return False
    return series.covered_from_ms <= start and series.covered_to_ms >= end


def _binned(series: TrialSeries, start: int, bin_ms: int, n_bins: int) -> np.ndarray:
    """Сума R за добу входу; сума — у Decimal, у float переходить лише результат."""
    sums = [Decimal(0)] * n_bins
    for value, entry in zip(series.returns, series.entry_ms, strict=True):
        index = (entry - start) // bin_ms
        if 0 <= index < n_bins:
            sums[index] += value
    return np.asarray([float(value) for value in sums], dtype=np.float64)


def _week_blocks(trades: Sequence[BacktestTrade]) -> tuple[tuple[Decimal, ...], ...]:
    if not isinstance(trades, Sequence) or isinstance(trades, (str, bytes)) or not trades:
        raise ValueError("немає угод для bootstrap")
    ordered = sorted(trades, key=lambda trade: (_entry_time(trade), trade.entry_index))
    blocks: list[list[Decimal]] = []
    current: int | None = None
    for trade in ordered:
        week = trade.entry_open_time_ms // WEEK_MS
        if week != current:
            blocks.append([])
            current = week
        blocks[-1].append(trade.r_multiple)
    return tuple(tuple(block) for block in blocks)


def _entry_time(trade: BacktestTrade) -> int:
    if type(trade) is not BacktestTrade:
        raise TypeError("угода має бути BacktestTrade")
    if type(trade.entry_open_time_ms) is not int or isinstance(trade.entry_open_time_ms, bool):
        raise ValueError("час входу має бути int")
    if trade.entry_open_time_ms < 0:
        raise ValueError("час входу від'ємний")
    return trade.entry_open_time_ms


def _resample(
    blocks: Sequence[Sequence[Decimal]],
    *,
    n_replicates: int,
    seed: int,
) -> list[tuple[Decimal, ...]]:
    _bootstrap_args(n_replicates, seed)
    n_trades = sum(len(block) for block in blocks)
    generator = np.random.Generator(np.random.PCG64(seed))
    n_blocks = len(blocks)
    paths: list[tuple[Decimal, ...]] = []
    for _ in range(n_replicates):
        drawn: list[Decimal] = []
        while len(drawn) < n_trades:
            block = blocks[int(generator.integers(0, n_blocks))]
            drawn.extend(block[: n_trades - len(drawn)])
        paths.append(tuple(drawn))
    return paths


def _bootstrap_args(n_replicates: int, seed: int) -> None:
    if type(n_replicates) is not int or isinstance(n_replicates, bool) or n_replicates < MIN_BOOTSTRAP_REPLICATES:
        raise ValueError(f"n_replicates має бути ціле ≥ {MIN_BOOTSTRAP_REPLICATES}")
    if type(seed) is not int or isinstance(seed, bool) or seed < 0:
        raise ValueError("seed bootstrap має бути невід'ємний int")


def _cumulative(path: Sequence[Decimal]) -> list[Decimal]:
    total = Decimal(0)
    out: list[Decimal] = []
    for value in path:
        total += value
        out.append(total)
    return out


def _classify(returns: Sequence[Decimal]) -> _PathPf:
    profit = Decimal(0)
    loss = Decimal(0)
    for value in returns:
        if value > 0:
            profit += value
        elif value < 0:
            loss -= value
    if loss == 0:
        if profit > 0:
            return _PathPf(None, True)
        return _PathPf(Decimal(0), False)
    return _PathPf(profit / loss, False)


def _pf_percentile(samples: Sequence[_PathPf], quantile: Decimal) -> _PathPf:
    ordered = sorted(samples, key=lambda item: (1, Decimal(0)) if item.infinite else (0, item.value or Decimal(0)))
    position = quantile * Decimal(len(ordered) - 1)
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    weight = position - Decimal(low)
    left = ordered[low]
    right = ordered[high]
    if weight == 0 or left.infinite or right.infinite:
        if weight == 0:
            return left
        if left.infinite:
            return left
        return _PathPf(None, True)
    if left.value is None or right.value is None:
        raise ValueError("скінченний PF без значення")
    blended = left.value * (Decimal(1) - weight) + right.value * weight
    return _PathPf(blended, False)


def _percentile(values: Sequence[Decimal], quantile: Decimal) -> Decimal:
    ordered = sorted(values)
    position = quantile * Decimal(len(ordered) - 1)
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    weight = position - Decimal(low)
    if weight == 0:
        return ordered[low]
    return ordered[low] * (Decimal(1) - weight) + ordered[high] * weight


def _decimal_returns(returns: Sequence[Decimal]) -> tuple[Decimal, ...]:
    if not isinstance(returns, Sequence) or isinstance(returns, (str, bytes)):
        raise TypeError("ряд R має бути послідовністю")
    return tuple(require_decimal("r", value) for value in returns)


def _decimal_text(value: object) -> Decimal:
    if type(value) is not str:
        raise ValueError("очікувався рядок Decimal")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("рядок не є Decimal") from exc
    if not parsed.is_finite():
        raise ValueError("Decimal не скінченний")
    return parsed


def _float_returns(returns: Sequence[Decimal]) -> np.ndarray:
    checked = _decimal_returns(returns)
    return np.asarray([float(value) for value in checked], dtype=np.float64)


def _moments(values: np.ndarray) -> tuple[float, float, float] | None:
    """Вибірковий Sharpe (дільник T−1), скоригована асиметрія і сирий ексцес."""
    count = int(values.size)
    if count < 4:
        return None
    mean = float(values.mean())
    centered = values - mean
    second = centered * centered
    m2 = float(np.sum(second) / count)
    if m2 <= 0.0:
        return None
    sample_var = float(np.sum(second) / (count - 1))
    if sample_var <= 0.0:
        return None
    sharpe = mean / math.sqrt(sample_var)
    m3 = float(np.sum(second * centered) / count)
    m4 = float(np.sum(second * second) / count)
    g1 = m3 / (m2**1.5)
    g2 = m4 / (m2 * m2) - 3.0
    skew = math.sqrt(count * (count - 1)) / (count - 2) * g1
    excess = ((count - 1) / ((count - 2) * (count - 3))) * ((count + 1) * g2 + 6.0)
    return sharpe, skew, excess + 3.0


def _sharpe(values: np.ndarray) -> float | None:
    count = int(values.size)
    if count < 2:
        return None
    mean = float(values.mean())
    centered = values - mean
    variance = float(np.sum(centered * centered) / (count - 1))
    if variance <= 0.0:
        return None
    return mean / math.sqrt(variance)


def _selection_benchmark(n_trials: int, variance: float) -> float:
    if n_trials <= 1 or variance <= 0.0:
        return 0.0
    independence = 1.0 - 1.0 / n_trials
    tail = 1.0 - 1.0 / (n_trials * math.e)
    return math.sqrt(variance) * ((1.0 - _EULER) * _norm_ppf(independence) + _EULER * _norm_ppf(tail))


def _psr(sharpe: float, benchmark: float, n_obs: int, skew: float, kurtosis: float) -> float | None:
    if n_obs < 2:
        return None
    denominator = 1.0 - skew * sharpe + (kurtosis - 1.0) / 4.0 * sharpe * sharpe
    if denominator <= 0.0:
        return None
    z_score = (sharpe - benchmark) * math.sqrt(n_obs - 1) / math.sqrt(denominator)
    if not math.isfinite(z_score):
        return None
    return _norm_cdf(z_score)


def _cscv_fraction(matrix: np.ndarray, n_blocks: int) -> float:
    """Частка розбиттів, де найкращий in-sample на out-of-sample не вищий за медіану.

    Розбиття йдуть порціями по `_CSCV_CELLS` чисел: масив «розбиття × половина блоків × спроби»
    цілком займав би гігабайти на сотнях спроб лінії.
    """
    row_count, n_strategies = matrix.shape
    block_size = row_count // n_blocks
    blocks = matrix[: block_size * n_blocks].reshape(n_blocks, block_size, n_strategies)
    block_sum = blocks.sum(axis=1)
    block_sq = np.square(blocks).sum(axis=1)
    total_sum = block_sum.sum(axis=0)
    total_sq = block_sq.sum(axis=0)
    half = n_blocks // 2
    count = half * block_size
    chosen = np.asarray(list(combinations(range(n_blocks), half)), dtype=np.int64)
    step = max(1, _CSCV_CELLS // (half * n_strategies))
    bad = 0
    for start in range(0, int(chosen.shape[0]), step):
        part = chosen[start : start + step]
        in_sample_sum = block_sum[part].sum(axis=1)
        in_sample_sq = block_sq[part].sum(axis=1)
        in_sample = _sharpe_rows(in_sample_sum, in_sample_sq, count)
        out_of_sample = _sharpe_rows(total_sum - in_sample_sum, total_sq - in_sample_sq, count)
        best = np.argmax(in_sample, axis=1)
        picked = out_of_sample[np.arange(best.size), best][:, None]
        # Середній ранг обраної (1 — найгірша; нічия ділить місця порівну).
        below = np.count_nonzero(out_of_sample < picked, axis=1)
        tied = np.count_nonzero(out_of_sample == picked, axis=1)
        ranks = below + (tied + 1) / 2
        bad += int(np.count_nonzero(ranks / (n_strategies + 1) <= 0.5))
    return bad / int(chosen.shape[0])


def _sharpe_rows(sum_: np.ndarray, sum_sq: np.ndarray, count: int) -> np.ndarray:
    mean = sum_ / count
    variance = (sum_sq - (sum_ * sum_) / count) / (count - 1)
    variance = np.maximum(variance, 0.0)
    std = np.sqrt(variance)
    sharpe = np.zeros(mean.shape, dtype=np.float64)
    positive = std > 0.0
    np.divide(mean, std, out=sharpe, where=positive)
    sharpe = np.where(~positive & (mean > 0.0), np.inf, sharpe)
    return np.where(~positive & (mean < 0.0), -np.inf, sharpe)


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_ppf(probability: float) -> float:
    if probability <= 0.0 or probability >= 1.0:
        raise ValueError("квантиль нормального розподілу визначений на (0, 1)")
    if probability < _PPF_LOW:
        return _ppf_tail(math.sqrt(-2.0 * math.log(probability)))
    if probability > 1.0 - _PPF_LOW:
        return -_ppf_tail(math.sqrt(-2.0 * math.log(1.0 - probability)))
    center = probability - 0.5
    radius = center * center
    numerator = _PPF_A[0]
    for coefficient in _PPF_A[1:]:
        numerator = numerator * radius + coefficient
    numerator *= center
    denominator = _PPF_B[0]
    for coefficient in _PPF_B[1:]:
        denominator = denominator * radius + coefficient
    denominator = denominator * radius + 1.0
    return numerator / denominator


def _ppf_tail(q: float) -> float:
    numerator = _PPF_C[0]
    for coefficient in _PPF_C[1:]:
        numerator = numerator * q + coefficient
    denominator = _PPF_D[0]
    for coefficient in _PPF_D[1:]:
        denominator = denominator * q + coefficient
    return numerator / (denominator * q + 1.0)


def _probability(value: float) -> Decimal:
    if not math.isfinite(value) or value < -1e-9 or value > 1.0 + 1e-9:
        raise ValueError("ймовірність поза [0, 1]")
    clipped = min(1.0, max(0.0, value))
    return Decimal(f"{clipped:.12f}")


def _moment_decimal(value: float) -> Decimal:
    if not math.isfinite(value):
        raise ValueError("момент не скінченний")
    return Decimal(f"{value:.8f}")


def _sequence(name: str, values: Sequence[object], kind: type[object]) -> None:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise TypeError(f"{name} має бути послідовністю")
    if any(type(item) is not kind for item in values):
        raise TypeError(f"{name} містить не {kind.__name__}")
