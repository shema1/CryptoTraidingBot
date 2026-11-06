"""Дослідницький цикл моделі, ітерація 1 (ТЗ §5А.3 п. 2–10, §5А.11 п. 1; T2.12).

Порядок: бейзлайни й кандидати на walk-forward → вибір 1–2 правил (`select_primary`) → по одному
meta-експерименту на правило → перегляд Gate G1 без запису. Кожна спроба — у журналі лінії, тож
N для DSR росте з кожним кроком.

Цикл hold-out не відкриває ніколи. Він повертає перегляди, а рішення відкрити hold-out приймає
людина окремим викликом `open_holdout_after_gate` (§5А.10, крок 2).

Ітерація 2 (T2.13) — пряме 3-класове прогнозування (`direct_model.py`) — запускається лише після
ітерації 1, що не пройшла G1 (`run_iteration_two` відмовляє інакше, ТЗ §5А.3 п. 13). Її спроба — у
тому ж журналі лінії, перегляд G1 — без відкриття hold-out.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logging import get_logger
from app.ml.direct_model import run_direct_experiment
from app.ml.features import SymbolData
from app.ml.meta_model import MetaExperiment, run_meta_experiment
from app.ml.models import Experiment
from app.ml.statistics import G1Preview, GateThresholds, preview_gate_g1
from app.ml.strategy import (
    PrimarySelection,
    StrategyComparison,
    StrategySpec,
    SymbolHistory,
    compare_strategies,
    record_strategy_comparison,
    select_primary,
)
from app.ml.trade_sim import TargetParams
from app.ml.validation import (
    RecordedTrial,
    ResearchSplit,
    costs_payload,
    create_experiment,
    target_payload,
    validation_payload,
)
from app.risk.costs import CostModel
from app.risk.limits import RiskLimits

SessionFactory = async_sessionmaker[AsyncSession]

_LOG = get_logger("app.ml.research_cycle")

# Наступний крок, якщо жодне правило не пройшло перегляд G1 (§5А.11 п. 2).
ITERATION_TWO = "iteration_two"
# Хоча б одне правило пройшло перегляд. Відкриває hold-out лише людина (`open_holdout_after_gate`).
WAIT_FOR_HUMAN_HOLDOUT = "wait_for_human_holdout"
# Ітерація 2 не пройшла G1: наступний крок — зміна символу або таймфрейму (ТЗ §5А.11 п. 3). Вирішує людина.
CHANGE_LINE_OR_TIMEFRAME = "change_line_or_timeframe"
# Ім'я перегляду прямого прогнозу в `RuleResult.spec_name` (правила meta мають назву стратегії).
DIRECT_SPEC_NAME = "direct_3class"


@dataclass(frozen=True, slots=True)
class RuleResult:
    """Перегляд G1 одного обраного правила. `passed` — лише про перегляд, не про відкриття.

    `passed` вимагає порогів ТЗ (без `preview.loosened`) і підігнаного правила входу: інакше відкриття
    hold-out не може дати `CANDIDATE`, і його не варто витрачати.
    """

    spec_name: str
    experiment_id: str
    trial_id: int
    decision_error: str | None
    preview: G1Preview

    @property
    def passed(self) -> bool:
        return (
            self.decision_error is None
            and self.preview.decision.passed
            and not self.preview.loosened
            and self.preview.dsr is not None
            and self.preview.pbo is not None
        )


@dataclass(frozen=True, slots=True)
class IterationOneReport:
    selection: PrimarySelection
    comparison_trials: tuple[RecordedTrial, ...]
    rules: tuple[RuleResult, ...]
    next_step: str
    # Цикл hold-out не відкриває. Поле фіксує це в звіті, а не лише в документації.
    hold_out_opened: bool = False
    # Порівняння каталогу (бейзлайни й кандидати по фолдах) — для звіту людині. Числа ті самі, що в журналі.
    comparison: StrategyComparison | None = None


def iteration_one_verdict(passed: Sequence[bool]) -> str:
    """Що далі після перегляду G1 обраних правил. Жодного відкриття тут не відбувається."""
    return WAIT_FOR_HUMAN_HOLDOUT if any(passed) else ITERATION_TWO


async def run_iteration_one(
    sessions: SessionFactory,
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
    thresholds: GateThresholds | None = None,
) -> IterationOneReport:
    """Ітерація 1 на вже завантаженій розбивці. Завантаження історій — окремий крок (`load_optuna_inputs`).

    Порівняння каталогу йде на тих самих фолдах, що й meta-експерименти: один `split`, один `seed`.
    Вибір `select_primary` — лише за OOS walk-forward, до будь-якого навчання meta-моделі.
    `sessions` — фабрика сесій: каталог і кожен meta-експеримент комітяться окремо, спроба `FAILED` —
    ще до прокидання винятку. Так журнал має кожну спробу, навіть якщо цикл упав посередині.
    Перегляди G1 — лише після запису спроб усіх обраних правил: кожен бачить повне N лінії і всіх
    суперників у PBO, як потім побачить `open_holdout_after_gate`.
    Таргет і витрати визначаються один раз (без явних — горизонт розбивки і `CostModel()`, як у
    `run_meta_experiment`) і йдуть і в порівняння каталогу, і в meta-експерименти, і в журнал каталогу.
    """
    target = params if params is not None else TargetParams(horizon_bars=split.horizon_bars)
    cost_model = costs if costs is not None else CostModel()
    comparison = await asyncio.to_thread(
        compare_strategies,
        histories,
        split=split,
        trading_symbol=trading_symbol,
        limits=limits,
        initial_equity=initial_equity,
        maintenance_margin_rate=maintenance_margin_rate,
        seed=seed,
        costs=cost_model,
        params=target,
    )
    selection = select_primary(comparison)
    # Кожен крок — окрема транзакція, як спроби Optuna: те, що людина побачить у перегляді, уже в журналі,
    # і rollback викликача не стирає спроб (N для DSR не занижується).
    async with sessions() as session, session.begin():
        comparison_experiment = await create_experiment(
            session,
            split=split,
            research_line=trading_symbol,
            symbols=tuple(history.symbol for history in histories),
            label_type="meta",
            primary_strategy=selection.metadata(),
            params={"stage": "primary_strategy", "target": target_payload(target), "costs": costs_payload(cost_model)},
            seed=seed,
        )
        comparison_trials = await record_strategy_comparison(
            session,
            experiment_id=comparison_experiment,
            comparison=comparison,
        )
    # Спершу всі спроби, потім перегляди: інакше перше правило бачило б менше N і не мало б суперника в PBO,
    # а людина — оптимістичніший перегляд, ніж порахує саме відкриття.
    recorded: list[tuple[StrategySpec, MetaExperiment]] = []
    for spec in selection.chosen:
        async with sessions() as session:
            try:
                meta = await run_meta_experiment(
                    session,
                    histories,
                    spec=spec,
                    split=split,
                    trading_symbol=trading_symbol,
                    limits=limits,
                    initial_equity=initial_equity,
                    maintenance_margin_rate=maintenance_margin_rate,
                    seed=seed,
                    costs=cost_model,
                    params=target,
                    feature_data=feature_data,
                )
            except Exception:
                # `run_meta_experiment` уже поклав у сесію спробу `FAILED` (модель навчена): фіксуємо її до
                # прокидання, інакше закриття сесії відкотило б її. Скасування (`CancelledError`) не пишеться.
                await _keep_failed_trial(session, spec.name)
                raise
            await session.commit()
        recorded.append((spec, meta))
    rules: list[RuleResult] = []
    async with sessions() as session:
        for spec, meta in recorded:
            preview = await preview_gate_g1(
                session,
                experiment_id=meta.experiment_id,
                trial_id=meta.trial.trial_id,
                stress=meta.run.stress,
                thresholds=thresholds,
            )
            rules.append(
                RuleResult(
                    spec_name=spec.name,
                    experiment_id=meta.experiment_id,
                    trial_id=meta.trial.trial_id,
                    decision_error=meta.decision_error,
                    preview=preview,
                )
            )
    return IterationOneReport(
        selection=selection,
        comparison_trials=comparison_trials,
        rules=tuple(rules),
        next_step=iteration_one_verdict(tuple(item.passed for item in rules)),
        comparison=comparison,
    )


@dataclass(frozen=True, slots=True)
class IterationTwoReport:
    """Пряме 3-класове прогнозування: один перегляд G1. `hold_out_opened` завжди `False`, як і в ітерації 1."""

    experiment: RuleResult
    next_step: str
    hold_out_opened: bool = False


def iteration_two_verdict(passed: bool) -> str:
    """Після ітерації 2 — відкриття hold-out людиною, або зміна символу чи таймфрейму (§5А.11 п. 3)."""
    return WAIT_FOR_HUMAN_HOLDOUT if passed else CHANGE_LINE_OR_TIMEFRAME


async def run_iteration_two(
    sessions: SessionFactory,
    histories: Sequence[SymbolHistory],
    *,
    after: IterationOneReport,
    split: ResearchSplit,
    trading_symbol: str,
    limits: RiskLimits,
    initial_equity: Decimal,
    maintenance_margin_rate: Decimal,
    seed: int,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    feature_data: Mapping[str, SymbolData] | None = None,
    thresholds: GateThresholds | None = None,
) -> IterationTwoReport:
    """Ітерація 2 лише після ітерації 1, що не пройшла G1 (ТЗ §5А.3 п. 13). Інакше `ValueError` до будь-якого запису.

    Спроба `direct` пишеться в журнал лінії й кожна її частина комітиться окремо, як в ітерації 1.
    Перегляд G1 — після запису спроби; hold-out не відкривається.
    """
    if after.hold_out_opened or after.next_step != ITERATION_TWO:
        raise ValueError("ітерація 2 лише після ітерації 1, що не пройшла G1 (ТЗ §5А.3 п. 13)")
    async with sessions() as session:
        # Ті самі фолди, що в ітерації 1 (T2.13, рев'ю): розбивка і лінія мають збігатися з експериментами перегляду.
        await _require_same_folds(session, after, split=split, trading_symbol=trading_symbol)
        try:
            recorded = await run_direct_experiment(
                session,
                histories,
                split=split,
                trading_symbol=trading_symbol,
                limits=limits,
                initial_equity=initial_equity,
                maintenance_margin_rate=maintenance_margin_rate,
                seed=seed,
                costs=costs,
                params=params,
                feature_data=feature_data,
            )
        except Exception:
            # `run_direct_experiment` уже поклав у сесію спробу `FAILED`: фіксуємо її до прокидання винятку.
            await _keep_failed_trial(session, DIRECT_SPEC_NAME)
            raise
        await session.commit()
    async with sessions() as session:
        preview = await preview_gate_g1(
            session,
            experiment_id=recorded.experiment_id,
            trial_id=recorded.trial.trial_id,
            thresholds=thresholds,
        )
    evaluation = recorded.evaluation
    result = RuleResult(
        spec_name=DIRECT_SPEC_NAME,
        experiment_id=recorded.experiment_id,
        trial_id=recorded.trial.trial_id,
        decision_error=evaluation.decision_error or evaluation.cross_error,
        preview=preview,
    )
    return IterationTwoReport(experiment=result, next_step=iteration_two_verdict(result.passed))


async def _require_same_folds(
    session: AsyncSession,
    after: IterationOneReport,
    *,
    split: ResearchSplit,
    trading_symbol: str,
) -> None:
    """Експерименти ітерації 1 мають ту саму розбивку і лінію, що й ітерація 2. Інакше — `ValueError` без запису."""
    expected = validation_payload(split)
    ids = {rule.experiment_id for rule in after.rules} | {trial.experiment_id for trial in after.comparison_trials}
    for experiment_id in sorted(ids):
        experiment = await session.get(Experiment, experiment_id)
        if experiment is None:
            raise ValueError(f"експеримента {experiment_id} ітерації 1 немає в журналі")
        if experiment.research_line != trading_symbol:
            raise ValueError("лінія ітерації 2 не та, що в ітерації 1")
        if experiment.params.get("validation") != expected:
            raise ValueError("розбивка ітерації 2 не та, що в ітерації 1 (ті самі фолди обов'язкові)")


async def _keep_failed_trial(session: AsyncSession, spec_name: str) -> None:
    """Закомітити спробу `FAILED` перед прокиданням винятку. Невдалий commit — у лог; далі летить
    початковий виняток, бо він пояснює збій, а не commit."""
    try:
        await session.commit()
    except SQLAlchemyError as exc:
        _LOG.error("research_cycle_failed_trial_not_committed", strategy=spec_name, error=type(exc).__name__)
