"""Дослідницький прогін на ринкових даних з БД (T2.14; ТЗ §5А.3 п. 2–10, §5А.11 п. 1; T2.12 (а), (б)).

`python -m app.ml.research iteration-one`:

1. Читає з БД свічки 1h, mark, funding, деривативи і кеш інструментів (`demo`) для `TRADING_SYMBOL` і
   `RESEARCH_SYMBOLS` (`load_research_inputs`). Дані заздалегідь качає воркер: `python -m app.jobs.enqueue market-data`.
2. Нова розбивка: спільне вікно свічок усіх символів, hold-out — останні 20% (запечатаний), фолди walk-forward
   подовжуються за найрідшим кандидатом `PRIMARY_CANDIDATES`.
3. `run_iteration_one`: каталог бейзлайнів і кандидатів → вибір 1–2 правил → meta-експеримент на кожне → перегляд
   Gate G1 без запису. Кожна спроба — у журналі лінії, тож N для DSR росте з кожним запуском: запускати свідомо.
4. Звіт для людини — Markdown у `docs/research/` (шлях — у лозі `research_report_written`, `logs/research/`).

Hold-out не відкривається ніколи: рішення відкрити його приймає людина окремим викликом `open_holdout_after_gate`
(ТЗ §5А.10). На одній БД одночасно працює лише один прогін (advisory lock `RESEARCH_LOCK_ID`).

Рахунок бектесту — параметри дослідження, а не `risk_config` live: ризик на угоду, плече й ліміти — `RiskLimits`
за замовчуванням (ТЗ §8.2); `min_equity_usd` і `max_notional_per_symbol_usd`, які в ТЗ «задаються», — аргументи
команди з консервативними значеннями за замовчуванням.

Коди виходу: 0 — прогін завершено (незалежно від вердикту G1); 1 — необроблений виняток; 2 — аргументи;
10 — інший прогін уже працює; 11 — БД недоступна; 12 — lock прогону втрачено до кінця (звіт з попередженням);
13 — даних бракує для розбивки; 14 — розбивку відхилено (зазвичай hold-out лінії вже зафіксований попереднім
прогоном, а нове вікно даних дає інший: повторний прогін лінії — рішення людини, T2.17); 15 — конфіг невалідний.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection

from app.core.clock import now_ms
from app.core.config import REPO_ROOT, Settings
from app.core.db import create_engine, session_factory
from app.core.instance_lock import LOCK_NAMESPACE, RESEARCH_LOCK_ID
from app.core.logging import configure_logging, get_logger, shutdown_logging
from app.data.backfill import backfill_symbols
from app.ml.decision import ENTRY_RULE_WARMUP_FOLDS
from app.ml.optuna_search import OptunaDataError, OptunaInputs, load_research_inputs
from app.ml.research_cycle import (
    CHANGE_LINE_OR_TIMEFRAME,
    ITERATION_TWO,
    WAIT_FOR_HUMAN_HOLDOUT,
    IterationOneReport,
    RuleResult,
    run_iteration_one,
)
from app.ml.statistics import (
    DSR_MIN,
    G1_COST_FACTOR,
    G1_DSR,
    G1_ENTRY_RULE,
    G1_EVALUATED_FOLDS,
    G1_FILTER,
    G1_FOLD_SHARE,
    G1_MEDIAN_PF,
    G1_MIN_TRADES_FOLD,
    G1_MIN_TRADES_TOTAL,
    G1_PBO,
    G1_STRESS,
    MEDIAN_PF_MIN,
    MIN_EVALUATED_FOLDS,
    MIN_OOS_TRADES,
    PBO_MAX,
    PROFITABLE_FOLD_SHARE,
    GateThresholds,
)
from app.ml.strategy import RuleReport, StrategyComparison
from app.ml.trade_sim import TargetParams
from app.ml.validation import ResearchSplit, SplitLeakError, line_trial_count
from app.risk.costs import CostModel
from app.risk.limits import RiskLimits

EXIT_OK: Final = 0
EXIT_UNCAUGHT: Final = 1
EXIT_LOCK_HELD: Final = 10
EXIT_DB_UNAVAILABLE: Final = 11
EXIT_LOCK_LOST: Final = 12
EXIT_DATA: Final = 13
EXIT_SPLIT_REJECTED: Final = 14
EXIT_CONFIG_INVALID: Final = 15

COMMAND_ITERATION_ONE: Final = "iteration-one"
REPORT_DIR: Final = REPO_ROOT / "docs" / "research"
# Параметри рахунку бектесту за замовчуванням (не live). Розмір угоди задає ризик 0,5% (ТЗ §8.2), тож межа номіналу
# символу з запасом не обмежує угоди на 10 000 USDT; підтримуюча маржа 1% — консервативніша за 0,5% першого тиру BTC.
DEFAULT_SEED: Final = 1
DEFAULT_INITIAL_EQUITY: Final = Decimal("10000")
DEFAULT_MAINTENANCE_MARGIN_RATE: Final = Decimal("0.01")
DEFAULT_MIN_EQUITY_USD: Final = Decimal("1000")
DEFAULT_MAX_NOTIONAL_PER_SYMBOL_USD: Final = Decimal("50000")

REASON_TEXT: Final[Mapping[str, str]] = {
    G1_MIN_TRADES_FOLD: "у якомусь оціненому фолді менше мінімуму угод торгового символу",
    G1_MIN_TRADES_TOTAL: f"менше {MIN_OOS_TRADES} угод торгового символу на оцінених фолдах",
    G1_EVALUATED_FOLDS: (f"оцінених фолдів (без {ENTRY_RULE_WARMUP_FOLDS} прогрівних) менше {MIN_EVALUATED_FOLDS}"),
    G1_MEDIAN_PF: f"медіана profit factor фолдів нижча за {MEDIAN_PF_MIN}",
    G1_FOLD_SHARE: f"прибуткових фолдів менше {(PROFITABLE_FOLD_SHARE * 100).normalize():f}%",
    G1_COST_FACTOR: "фолди пораховано не з базовими витратами",
    G1_STRESS: "результат не витримує подвоєння комісій і slippage",
    G1_FILTER: "ML-фільтр не кращий за правило без фільтра",
    G1_ENTRY_RULE: "правило входу не оцінене лише минулими фолдами (або не підігнане)",
    G1_DSR: f"DSR < {DSR_MIN}: результат пояснюється кількістю спроб, а не перевагою",
    G1_PBO: f"PBO ≥ {PBO_MAX}: вибір найкращої конфігурації нестабільний",
}
NEXT_STEP_TEXT: Final[Mapping[str, str]] = {
    ITERATION_TWO: (
        "Жодне правило не пройшло перегляд G1. За ТЗ §5А.11 далі — ітерація 2 (пряме 3-класове прогнозування) "
        "на тих самих фолдах, або зміна таймфрейму/таргета. Hold-out не відкривати."
    ),
    WAIT_FOR_HUMAN_HOLDOUT: (
        "Хоча б одне правило пройшло перегляд G1 на порогах ТЗ. Рішення відкрити hold-out (≤ 2 відкриттів на лінію) "
        "приймає людина окремим викликом `open_holdout_after_gate`; спершу — модель допуску (`fit_frozen_meta`)."
    ),
    CHANGE_LINE_OR_TIMEFRAME: "Ітерація 2 теж не пройшла: змінити символ або таймфрейм (ТЗ §5А.11 п. 3).",
}

_LOG = get_logger("app.ml.research")


class ResearchLockHeld(Exception):
    """Інший дослідницький прогін на цій БД ще працює."""


def research_limits(*, min_equity_usd: Decimal, max_notional_per_symbol_usd: Decimal) -> RiskLimits:
    """Ліміти бектесту: значення ТЗ §8.2 за замовчуванням і дві обов'язкові межі з аргументів."""
    return RiskLimits(min_equity_usd=min_equity_usd, max_notional_per_symbol_usd=max_notional_per_symbol_usd)


async def run(settings: Settings, args: argparse.Namespace) -> int:
    try:
        url = settings.database_url()
    except ValueError:
        _LOG.error("research_config_invalid", reason="POSTGRES_PASSWORD")
        return EXIT_CONFIG_INVALID
    engine = create_engine(url)
    try:
        async with engine.connect() as lock_connection:
            await _take_lock(lock_connection)
            sessions = session_factory(engine)
            symbols = backfill_symbols(
                trading_symbol=settings.trading_symbol, research_symbols=settings.research_symbols
            )
            limits = research_limits(
                min_equity_usd=args.min_equity_usd, max_notional_per_symbol_usd=args.max_notional_per_symbol_usd
            )
            target = TargetParams()
            costs = CostModel()
            started = now_ms()
            _LOG.info("research_loading", symbols=list(symbols), trading_symbol=settings.trading_symbol)
            inputs = await load_research_inputs(
                sessions,
                symbols=symbols,
                trading_symbol=settings.trading_symbol,
                limits=limits,
                initial_equity=args.initial_equity,
                maintenance_margin_rate=args.maintenance_margin_rate,
                seed=args.seed,
                target=target,
                costs=costs,
            )
            _LOG.info(
                "research_split_ready",
                folds=len(inputs.split.folds),
                data_from_ms=inputs.split.data_from_ms,
                holdout_from_ms=inputs.split.holdout_from_ms,
            )
            report = await run_iteration_one(
                sessions,
                inputs.histories,
                split=inputs.split,
                trading_symbol=settings.trading_symbol,
                limits=limits,
                initial_equity=args.initial_equity,
                maintenance_margin_rate=args.maintenance_margin_rate,
                seed=args.seed,
                costs=costs,
                params=target,
                feature_data=inputs.feature_data,
            )
            async with sessions() as session:
                n_line = await line_trial_count(session, settings.trading_symbol)
            lock_held = await _lock_still_held(lock_connection)
    except ResearchLockHeld:
        _LOG.error("research_lock_held", hint="інший прогін python -m app.ml.research ще працює")
        return EXIT_LOCK_HELD
    except OptunaDataError as exc:
        _LOG.error("research_data_insufficient", error=str(exc), hint="python -m app.jobs.enqueue market-data")
        return EXIT_DATA
    except SplitLeakError as exc:
        # Найчастіше: hold-out лінії вже зафіксований попереднім прогоном, а нове вікно даних дає інший
        # (validation.create_experiment). Повторний прогін на тій самій лінії — рішення людини (T2.17).
        _LOG.error("research_split_rejected", error=str(exc), hint="docs/runbooks/getting-started.md, крок 5А.3")
        return EXIT_SPLIT_REJECTED
    except (OSError, SQLAlchemyError) as exc:
        _LOG.error("research_db_unavailable", error_type=type(exc).__name__)
        return EXIT_DB_UNAVAILABLE
    finally:
        await engine.dispose()
    finished = now_ms()
    path = args.report or REPORT_DIR / f"{_date(finished)}-iteration-one-{_slug(settings.trading_symbol)}.md"
    markdown = render_iteration_one(
        report,
        inputs,
        trading_symbol=settings.trading_symbol,
        n_line_trials=n_line,
        initial_equity=args.initial_equity,
        target=target,
        costs=costs,
        started_ms=started,
        finished_ms=finished,
    )
    if not lock_held:
        # Спроби вже в журналі (N їх рахує), але звіт міг не побачити спроб паралельного прогону.
        markdown = (
            "> **Увага:** блокування прогону втрачено до кінця роботи — число спроб лінії у звіті може бути "
            "неповним; актуальне N рахує журнал.\n\n" + markdown
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown, encoding="utf-8")
    _LOG.info(
        "research_report_written",
        path=str(path),
        next_step=report.next_step,
        chosen=[spec.name for spec in report.selection.chosen],
        passed=[rule.spec_name for rule in report.rules if rule.passed],
        n_line_trials=n_line,
        duration_ms=finished - started,
        hold_out_opened=report.hold_out_opened,
    )
    if not lock_held:
        _LOG.error("research_lock_lost", path=str(path))
        return EXIT_LOCK_LOST
    return EXIT_OK


async def _lock_still_held(connection: AsyncConnection) -> bool:
    """Чи цей сеанс досі тримає lock прогону (обрив з'єднання звільняє його мовчки)."""
    found = await connection.scalar(
        text(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND classid = :namespace "
            "AND objid = :lock_id AND objsubid = 2 AND granted AND pid = pg_backend_pid()"
        ),
        {"namespace": LOCK_NAMESPACE, "lock_id": RESEARCH_LOCK_ID},
    )
    await connection.commit()
    return found == 1


async def _take_lock(connection: AsyncConnection) -> None:
    got = await connection.scalar(
        text("SELECT pg_try_advisory_lock(:namespace, :lock_id)"),
        {"namespace": LOCK_NAMESPACE, "lock_id": RESEARCH_LOCK_ID},
    )
    await connection.commit()
    if got is not True:
        raise ResearchLockHeld


def render_iteration_one(
    report: IterationOneReport,
    inputs: OptunaInputs,
    *,
    trading_symbol: str,
    n_line_trials: int,
    initial_equity: Decimal,
    target: TargetParams,
    costs: CostModel,
    started_ms: int,
    finished_ms: int,
) -> str:
    """Звіт людині українською. Лише числа з журналу/звіту циклу, без оцінок «добре/погано» поза порогами ТЗ."""
    split = inputs.split
    lines: list[str] = [
        f"# Ітерація 1 на ринкових даних — {trading_symbol}, {_date(finished_ms)}",
        "",
        "> Згенеровано `python -m app.ml.research iteration-one` (T2.14). "
        + ("Hold-out **відкривався**." if report.hold_out_opened else "Hold-out **не відкривався**.")
        + " Кожна спроба нижче вже записана в журнал лінії й рахується в N для DSR.",
        "",
        "## Дані й розбивка",
        "",
        f"- Символи: {', '.join(item.symbol for item in inputs.histories)}; рішення — за {trading_symbol}.",
        f"- Спільне вікно свічок: {_date(split.data_from_ms)} … {_date(split.data_to_ms)} (UTC).",
        f"- Hold-out (запечатаний): {_date(split.holdout_from_ms)} … {_date(split.holdout_to_ms)} "
        f"({_pct(split.holdout_fraction)} історії).",
        f"- Walk-forward: {len(split.folds)} фолдів, перший тест з {_date(split.folds[0].test_start_ms)}, "
        f"подовжених до 6 міс.: {sum(1 for fold in split.folds if fold.extended)}; embargo {split.embargo_bars} год; "
        f"мінімум угод на фолд {split.min_trades_per_fold}. Два перші фолди — прогрів правила входу (рішення 0024).",
        f"- Таргет: TP {target.tp_atr_mult}·ATR, SL {target.sl_atr_mult}·ATR, горизонт {target.horizon_bars} год.",
        f"- Витрати: taker {_pct(costs.taker_fee_rate)} за сторону, slippage входу {costs.slippage_entry_atr}·ATR, "
        f"SL {costs.stop_slippage_atr}·ATR, TP {costs.slippage_tp_atr}·ATR.",
        f"- Рахунок бектесту: {initial_equity} USDT, ліміти Risk Engine за замовчуванням (ризик 0,5% на угоду).",
        f"- Спроб у лінії {trading_symbol} після прогону: **{n_line_trials}**. "
        f"Тривалість: {round((finished_ms - started_ms) / 60_000)} хв.",
        "",
    ]
    if report.comparison is not None:
        lines.extend(_comparison_section(report.comparison))
    chosen = ", ".join(
        f"`{spec.name}`" + ("" if beats else " (не краще за «завжди FLAT»)")
        for spec, beats in zip(report.selection.chosen, report.selection.beats_flat, strict=True)
    )
    lines.extend([f"Обрано для meta-моделі: {chosen}.", "", "## Meta-модель і перегляд Gate G1", ""])
    lines.extend(_rules_section(report.rules, split))
    lines.extend(
        [
            "## Висновок",
            "",
            f"Наступний крок (`{report.next_step}`): {NEXT_STEP_TEXT.get(report.next_step, report.next_step)}",
            "",
        ]
    )
    return "\n".join(lines)


def _comparison_section(comparison: StrategyComparison) -> list[str]:
    lines = [
        "## Каталог правил (торговий символ, out-of-sample walk-forward, після витрат)",
        "",
        "| Правило | Медіана фолду, USDT | Сума, USDT | Угод | Фолдів у плюсі | Медіана PF |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for variant in comparison.variants:
        lines.append(_variant_row(variant))
    lines.extend(
        [
            "",
            "«Завжди FLAT», «завжди LONG» і випадкові входи — опорні бейзлайни (у N, не в PBO). "
            "Правило лишається кандидатом, лише якщо медіана його фолдів строго вища за «завжди FLAT».",
            "",
        ]
    )
    return lines


def _variant_row(variant: RuleReport) -> str:
    scores = variant.scores
    total = sum((score.trading_net_usd for score in scores), Decimal(0))
    positive = sum(1 for score in scores if score.trading_net_usd > 0)
    pfs = sorted(score.trading_profit_factor for score in scores if score.trading_profit_factor is not None)
    median_pf = _fmt(pfs[len(pfs) // 2]) if pfs else "—"
    return (
        f"| `{variant.spec.name}` | {_fmt(variant.median_net_usd)} | {_fmt(total)} | {variant.trading_trades} "
        f"| {positive}/{len(scores)} | {median_pf} |"
    )


def _rules_section(rules: Sequence[RuleResult], split: ResearchSplit) -> list[str]:
    lines = [
        "| Правило | Угод | Медіана PF | Фолдів у плюсі | DSR | PBO | G1 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    explained: list[str] = []
    for rule in rules:
        decision = rule.preview.decision
        verdict = "пройдено" if rule.passed else "не пройдено"
        lines.append(
            f"| `{rule.spec_name}` (`{rule.experiment_id}`, спроба {rule.trial_id}) | {decision.n_trades} "
            f"| {_fmt(decision.median_pf) if decision.median_pf is not None and decision.n_trades else '—'} "
            f"| {_pct(decision.profitable_fold_share) if decision.profitable_fold_share is not None else '—'} "
            f"| {_fmt(rule.preview.dsr)} | {_fmt(rule.preview.pbo)} | {verdict} |"
        )
        for reason in decision.reasons:
            explained.append(f"- `{rule.spec_name}` — `{reason}`: {REASON_TEXT.get(reason, reason)}.")
        if rule.decision_error:
            explained.append(f"- `{rule.spec_name}` — правило входу не підігнано: {rule.decision_error}.")
    lines.append("")
    gate = GateThresholds()
    per_fold = max(gate.min_trades_per_fold, split.min_trades_per_fold)
    warmups = sorted({rule.preview.decision.warmup_folds for rule in rules})
    lines.append(
        f"Оцінені фолди — без прогрівних ({', '.join(str(item) for item in warmups) or '—'}); пороги ТЗ §5А.10: "
        f"медіана PF ≥ {gate.median_pf_min}, ≥ {(gate.profitable_fold_share * 100).normalize():f}% фолдів у плюсі, "
        f"≥ {per_fold} угод на фолд, ≥ {gate.min_oos_trades} угод, ≥ {gate.min_evaluated_folds} оцінених фолдів, "
        f"стрес ×{gate.stress_cost_factor}, DSR ≥ {gate.dsr_min}, PBO < {gate.pbo_max}."
    )
    lines.append("")
    if explained:
        lines.extend(["Причини відмови:", "", *explained, ""])
    return lines


def _date(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.UTC).strftime("%Y-%m-%d")


def _slug(symbol: str) -> str:
    return symbol.split("/")[0].lower()


def _fmt(value: Decimal | None) -> str:
    if value is None:
        return "—"
    return f"{value.quantize(Decimal('0.01')):f}" if value.is_finite() else str(value)


def _pct(value: Decimal) -> str:
    return f"{(value * 100).normalize():f}%"


def _decimal(text_value: str) -> Decimal:
    try:
        value = Decimal(text_value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("очікувалось десяткове число") from exc
    if not value.is_finite() or value <= 0:
        raise argparse.ArgumentTypeError("число має бути додатним")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входу `python -m app.ml.research`."""
    args = _parse_args(argv)
    try:
        settings = Settings()
    except ValidationError:
        return EXIT_CONFIG_INVALID
    configure_logging(settings, process="research")
    try:
        return asyncio.run(run(settings, args))
    except (SystemExit, KeyboardInterrupt):
        raise
    except BaseException:  # noqa: BLE001 — маскований лог і код 1; текст винятку в stderr без маски не потрапляє
        _LOG.critical("uncaught_exception", exc_info=True)
        return EXIT_UNCAUGHT
    finally:
        shutdown_logging()


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m app.ml.research",
        description="Дослідницький прогін на даних з БД. Пише спроби в журнал лінії; hold-out не відкриває.",
        allow_abbrev=False,
    )
    commands = parser.add_subparsers(dest="command", required=True)
    one = commands.add_parser(COMMAND_ITERATION_ONE, help="бейзлайни, вибір правила, meta-модель, перегляд G1")
    one.add_argument("--seed", type=int, default=DEFAULT_SEED)
    one.add_argument("--initial-equity", type=_decimal, default=DEFAULT_INITIAL_EQUITY)
    one.add_argument("--maintenance-margin-rate", type=_decimal, default=DEFAULT_MAINTENANCE_MARGIN_RATE)
    one.add_argument("--min-equity-usd", type=_decimal, default=DEFAULT_MIN_EQUITY_USD)
    one.add_argument("--max-notional-per-symbol-usd", type=_decimal, default=DEFAULT_MAX_NOTIONAL_PER_SYMBOL_USD)
    one.add_argument("--report", type=Path, default=None, help="куди записати звіт Markdown")
    return parser.parse_args(list(argv) if argv is not None else None)


if __name__ == "__main__":
    raise SystemExit(main())
