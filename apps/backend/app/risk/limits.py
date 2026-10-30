"""Чисті перевірки лімітів (ТЗ §5.2.3, §8.2, §8.3, §8.5, §15).

Без БД, мережі й годинника: час і капітал передаються аргументами. Ті самі функції
викликають бектест і `RiskGate`. Перша відмова в `check_entry_limits` — єдина, щоб
обидва шляхи бачили одну й ту саму причину.

Порядок входу: просадка, денний збиток, мінімальний капітал, позиція бота (не більше
однієї), позиції акаунта, кількість угод за добу, cooldown, funding (лише якщо ліміт
заданий). Найважча причина — першою: просадка раніше за день (з неї випливає `HALTED`,
а не пауза до півночі), обидві — раніше за поріг капіталу, щоб він їх не маскував.
Статус рахунку (`HALTED`, `PAUSED`) з причини відмови входу не виводиться: його визначають
`check_drawdown` і `check_daily_loss` на кожному оновленні капіталу (T4.2).

Денний збиток і просадка рахуються з total equity. `sizing_equity` їх не стосується.
Капітал вище за переданий пік — нова вершина, якої знімок ще не записав: пік і є цей капітал.
Збережений пік викликач оновлює `next_peak_equity` на кожному капіталі, з яким приймає рішення:
інакше падіння після зростання рахувалось би від старого, нижчого піку.
"""

from __future__ import annotations

from decimal import ROUND_CEILING, Decimal
from typing import Annotated

from pydantic import Field, model_validator

from app.exchange.types import PositionSide
from app.risk.numbers import FrozenModel, fixed_context, mul_div, require_decimal
from app.risk.reasons import RiskReason

_BPS = Decimal(10000)


class RiskLimits(FrozenModel):
    """Ліміти ТЗ §8.2. Частки — часткою від 1 (`0.005` = 0,5%), плече — множником (`3` = 3×).

    `min_equity_usd` і `max_notional_per_symbol_usd` у ТЗ «задаються»: без дефолта,
    щоб відсутній конфіг не став безмежним розміром. `risk_per_trade_pct` не вищий
    за 2% — межа з підказки ТЗ §16А («вище небезпечне»).
    """

    risk_per_trade_pct: Annotated[Decimal, Field(gt=0, le=Decimal("0.02"))] = Decimal("0.005")
    max_leverage: Annotated[Decimal, Field(ge=1, le=100)] = Decimal(3)
    max_open_positions: Annotated[int, Field(ge=1, le=100)] = 3
    max_total_exposure_pct: Annotated[Decimal, Field(gt=0, le=10)] = Decimal("1.5")
    max_notional_per_symbol_usd: Annotated[Decimal, Field(gt=0)]
    daily_loss_limit_pct: Annotated[Decimal, Field(gt=0, le=1)] = Decimal("0.02")
    max_drawdown_pct: Annotated[Decimal, Field(gt=0, le=1)] = Decimal("0.10")
    max_consecutive_losses: Annotated[int, Field(ge=1, le=100)] = 4
    cooldown_bars: Annotated[int, Field(ge=1, le=10_000)] = 8
    max_trades_per_day: Annotated[int, Field(ge=1, le=10_000)] = 10
    # Строго більше 1: оцінка ліквідації не покриває комісію закриття, множник 1 її запасу не дає.
    min_liq_distance_sl_multiple: Annotated[Decimal, Field(gt=1, le=20)] = Decimal(2)
    max_slippage_bps: Annotated[Decimal, Field(gt=0, le=10_000)] = Decimal(15)
    min_equity_usd: Annotated[Decimal, Field(gt=0)]
    # None — перевірку funding не вмикати (ТЗ: опція).
    max_funding_rate_abs: Annotated[Decimal, Field(gt=0, lt=1)] | None = None


class LimitDecision(FrozenModel):
    """`ok` — вхід за цією перевіркою дозволено. Інакше `reason` пояснює відмову."""

    ok: bool
    reason: RiskReason | None

    @model_validator(mode="after")
    def _paired(self) -> LimitDecision:
        if self.ok == (self.reason is not None):
            raise ValueError("ok і reason не узгоджені")
        return self


class TimeStopDecision(FrozenModel):
    """`close` — вік позиції дійшов до time-stop і її треба закривати, не блокувати захист."""

    close: bool
    reason: RiskReason | None

    @model_validator(mode="after")
    def _paired(self) -> TimeStopDecision:
        if self.close != (self.reason is RiskReason.TIME_STOP):
            raise ValueError("time-stop: close лише разом із TIME_STOP")
        return self


class EquityAnchors(FrozenModel):
    """`peak_equity` і `day_start_equity` після зовнішнього грошового потоку. `equity` — капітал після потоку."""

    peak_equity: Decimal
    day_start_equity: Decimal
    equity: Decimal

    @model_validator(mode="after")
    def _ordered(self) -> EquityAnchors:
        require_decimal("peak_equity", self.peak_equity, non_negative=True)
        require_decimal("day_start_equity", self.day_start_equity, non_negative=True)
        require_decimal("equity", self.equity, non_negative=True)
        if self.peak_equity < self.equity:
            raise ValueError("peak_equity має бути ≥ equity")
        return self


@fixed_context
def check_entry_limits(
    *,
    total_equity: Decimal,
    peak_equity: Decimal,
    day_start_equity: Decimal,
    bot_open_positions: int,
    open_positions: int,
    trades_today: int,
    consecutive_losses: int,
    bars_since_last_loss: int,
    limits: RiskLimits,
    funding_rate: Decimal | None = None,
    side: PositionSide | None = None,
) -> LimitDecision:
    """Перша відмова входу. `total_equity` — капітал з біржі, включно з нереалізованим PnL.

    `bot_open_positions` — позиції цього бота (ТЗ §5.2.3: одна одночасно), `open_positions` — усього
    акаунта (стеля `max_open_positions`). Позицій бота більше, ніж акаунта, — `ValueError`.
    """
    if type(limits) is not RiskLimits:
        raise ValueError("limits має бути RiskLimits")
    _require_count("bot_open_positions", bot_open_positions)
    _require_count("open_positions", open_positions)
    if bot_open_positions > open_positions:
        raise ValueError("bot_open_positions більше за open_positions акаунта")
    steps = (
        check_drawdown(equity=total_equity, peak_equity=peak_equity, limit_pct=limits.max_drawdown_pct),
        check_daily_loss(equity=total_equity, day_start_equity=day_start_equity, limit_pct=limits.daily_loss_limit_pct),
        check_min_equity(total_equity, limits.min_equity_usd),
        check_max_positions(bot_open_positions, 1),
        check_max_positions(open_positions, limits.max_open_positions),
        check_max_trades(trades_today, limits.max_trades_per_day),
        check_cooldown(
            consecutive_losses=consecutive_losses,
            bars_since_last_loss=bars_since_last_loss,
            max_consecutive_losses=limits.max_consecutive_losses,
            cooldown_bars=limits.cooldown_bars,
        ),
    )
    for decision in steps:
        if not decision.ok:
            return decision
    return _funding_gate(limits, funding_rate, side)


def check_min_equity(equity: Decimal, min_equity_usd: Decimal) -> LimitDecision:
    """Нижче за `min_equity_usd` вхід заборонено. Рівно межа — ще дозволено (ТЗ §8.2: «нижче»)."""
    require_decimal("equity", equity, non_negative=True)
    require_decimal("min_equity_usd", min_equity_usd, positive=True)
    if equity < min_equity_usd:
        return _block(RiskReason.RISK_MIN_EQUITY)
    return _allow()


def next_peak_equity(peak_equity: Decimal, equity: Decimal) -> Decimal:
    """Пік після ще одного спостереження капіталу: `max(peak, equity)`. `equity` — total equity.

    Бектест і `RiskGate` викликають це на кожному капіталі, з яким приймають рішення (бектест —
    на кожному барі, з нереалізованим PnL), і зберігають результат. Без цього падіння після
    зростання рахувалось би від старого піку і просадку було б занижено.
    """
    require_decimal("peak_equity", peak_equity, non_negative=True)
    require_decimal("equity", equity, non_negative=True)
    return max(peak_equity, equity)


@fixed_context
def check_drawdown(*, equity: Decimal, peak_equity: Decimal, limit_pct: Decimal) -> LimitDecision:
    """Просадка `(peak - equity) / peak`. Блок, коли вона ≥ ліміту. `equity` — total equity.

    `equity` вище за `peak_equity` — нова вершина, якої знімок ще не записав: просадка нульова.
    Total equity з біржі включає нереалізований PnL і росте між знімками, тож це не зіпсовані дані.
    Збережений пік від цього сам не оновлюється — `next_peak_equity`.
    """
    require_decimal("equity", equity, non_negative=True)
    require_decimal("peak_equity", peak_equity, positive=True)
    require_decimal("limit_pct", limit_pct, positive=True)
    if equity <= max(peak_equity, equity) * (1 - limit_pct):
        return _block(RiskReason.RISK_MAX_DRAWDOWN)
    return _allow()


@fixed_context
def check_daily_loss(*, equity: Decimal, day_start_equity: Decimal, limit_pct: Decimal) -> LimitDecision:
    """Денний результат `(equity - day_start) / day_start`. Блок, коли збиток ≥ ліміту."""
    require_decimal("equity", equity, non_negative=True)
    require_decimal("day_start_equity", day_start_equity, positive=True)
    require_decimal("limit_pct", limit_pct, positive=True)
    if equity <= day_start_equity * (1 - limit_pct):
        return _block(RiskReason.RISK_DAILY_LOSS)
    return _allow()


def check_max_positions(open_positions: int, max_open_positions: int) -> LimitDecision:
    """Новий вхід, якщо відкритих уже `max`. Для одного бота передають `1` (ТЗ §5.2.3)."""
    _require_count("open_positions", open_positions)
    _require_count("max_open_positions", max_open_positions, minimum=1)
    if open_positions >= max_open_positions:
        return _block(RiskReason.RISK_MAX_POSITIONS)
    return _allow()


def check_max_trades(trades_today: int, max_trades_per_day: int) -> LimitDecision:
    """Добова стеля угод. Рівно `max` — наступний вхід уже не береться."""
    _require_count("trades_today", trades_today)
    _require_count("max_trades_per_day", max_trades_per_day, minimum=1)
    if trades_today >= max_trades_per_day:
        return _block(RiskReason.RISK_MAX_TRADES_PER_DAY)
    return _allow()


def check_cooldown(
    *,
    consecutive_losses: int,
    bars_since_last_loss: int,
    max_consecutive_losses: int,
    cooldown_bars: int,
) -> LimitDecision:
    """Пауза, поки серія збитків не коротша за ліміт і не минуло `cooldown_bars` барів.

    Бар останнього збитку має `bars_since_last_loss = 0`. Рівно `cooldown_bars` — пауза скінчилась.
    """
    _require_count("consecutive_losses", consecutive_losses)
    _require_count("bars_since_last_loss", bars_since_last_loss)
    _require_count("max_consecutive_losses", max_consecutive_losses, minimum=1)
    _require_count("cooldown_bars", cooldown_bars, minimum=1)
    if consecutive_losses >= max_consecutive_losses and bars_since_last_loss < cooldown_bars:
        return _block(RiskReason.RISK_COOLDOWN)
    return _allow()


def next_consecutive_losses(previous: int, realized_pnl: Decimal) -> int:
    """Наступна довжина серії. Збиток збільшує, прибуток скидає, нуль не чіпає (це не збиток і не перемога)."""
    _require_count("previous", previous)
    require_decimal("realized_pnl", realized_pnl)
    if realized_pnl < 0:
        return previous + 1
    if realized_pnl > 0:
        return 0
    return previous


def check_time_stop(
    *,
    opened_at_ms: int,
    now_ms: int,
    max_hold_bars: int,
    bar_ms: int,
) -> TimeStopDecision:
    """Закривати, коли `now_ms >= opened_at_ms + max_hold_bars * bar_ms`.

    Це вихід за `open[t + horizon]` (ТЗ §5.2.1): рівно на межі вже time-stop, на мілісекунду раніше — ні.
    `opened_at_ms` — час відкриття свічки входу `open[t]`, не час fill: інакше межа зсунулась би
    на затримку входу, і цикл на `open[t + horizon]` пропустив би time-stop до наступного бару.
    `now_ms` раніше за `opened_at_ms` — `ValueError`; на шляху виходу викликач не трактує його
    як «не закривати назавжди», а повторює перевірку на наступному тіку (інваріант 6).
    """
    _require_ms("opened_at_ms", opened_at_ms)
    _require_ms("now_ms", now_ms)
    _require_count("max_hold_bars", max_hold_bars, minimum=1)
    _require_count("bar_ms", bar_ms, minimum=1)
    if now_ms < opened_at_ms:
        raise ValueError("now_ms раніше за відкриття позиції")
    if now_ms >= opened_at_ms + max_hold_bars * bar_ms:
        return TimeStopDecision(close=True, reason=RiskReason.TIME_STOP)
    return TimeStopDecision(close=False, reason=None)


@fixed_context
def check_entry_slippage(*, reference: Decimal, actual: Decimal, max_slippage_bps: Decimal) -> LimitDecision:
    """Відхилення фактичної ціни від опорної. Блок, коли bps строго більші за ліміт (ТЗ §6.6: «при перевищенні»).

    У `check_entry_limits` цієї перевірки немає: у live порівнюють котирування з ціною сигналу,
    у бектесті виконання вже задає модель витрат. Обидва викликають цю функцію самі (T4.1).
    """
    require_decimal("reference", reference, positive=True)
    require_decimal("actual", actual, positive=True)
    require_decimal("max_slippage_bps", max_slippage_bps, positive=True)
    if abs(actual - reference) * _BPS > max_slippage_bps * reference:
        return _block(RiskReason.RISK_MAX_SLIPPAGE)
    return _allow()


def check_funding_rate(*, rate: Decimal, side: PositionSide, max_abs: Decimal | None) -> LimitDecision:
    """Не входити проти надмірного funding. `max_abs is None` — опція вимкнена, вхід дозволено.

    Лонг платить при додатній ставці, шорт — при від'ємній. Рівно `max_abs` ще дозволено.
    """
    require_decimal("rate", rate)
    if type(side) is not PositionSide:
        raise ValueError("side має бути PositionSide")
    if max_abs is None:
        return _allow()
    require_decimal("max_abs", max_abs, positive=True)
    paid = rate if side is PositionSide.LONG else -rate
    if paid > max_abs:
        return _block(RiskReason.RISK_FUNDING)
    return _allow()


@fixed_context
def adjust_equity_anchors(
    *,
    equity_after: Decimal,
    cashflow: Decimal,
    peak_equity: Decimal,
    day_start_equity: Decimal,
) -> EquityAnchors:
    """Зсуває пік і початок дня на депозит або виведення (ТЗ §15).

    `cashflow` — як у `ledger`: депозит > 0, виведення < 0. `equity_after` уже містить цей потік
    і не містить торгового PnL після нього. Обидва якорі множаться на
    `equity_after / (equity_after - cashflow)`, тож відсоток просадки і денний результат
    не змінюються. Інакше депозит зменшив би просадку, а виведення збільшило б її.

    Пік нижче капіталу до потоку — нова вершина між знімками: масштабується сам капітал до потоку.
    Обидва якорі округлюються вгору: вищий пік і вищий початок дня не занижують ні просадку,
    ні денний збиток. Пік не нижчий за новий equity (обмеження БД `peak >= equity`).
    """
    require_decimal("equity_after", equity_after, non_negative=True)
    require_decimal("cashflow", cashflow)
    require_decimal("peak_equity", peak_equity, non_negative=True)
    require_decimal("day_start_equity", day_start_equity, non_negative=True)
    equity_before = equity_after - cashflow
    if equity_before <= 0:
        raise ValueError("капітал до потоку має бути > 0")
    peak = max(peak_equity, equity_before)
    if cashflow == 0:
        return EquityAnchors(peak_equity=peak, day_start_equity=day_start_equity, equity=equity_after)
    peak_after = max(mul_div(peak, equity_after, equity_before, rounding=ROUND_CEILING), equity_after)
    if day_start_equity == 0:
        day_after = Decimal(0)
    else:
        day_after = mul_div(day_start_equity, equity_after, equity_before, rounding=ROUND_CEILING)
    return EquityAnchors(peak_equity=peak_after, day_start_equity=day_after, equity=equity_after)


@fixed_context
def apply_cashflows(
    *,
    peak_equity: Decimal,
    day_start_equity: Decimal,
    equity: Decimal,
    cashflows: tuple[Decimal, ...] | list[Decimal],
) -> EquityAnchors:
    """Кілька потоків поспіль без торгового PnL між ними.

    Порожній список повертає ті самі якорі, лише пік не нижчий за `equity` (нова вершина
    між знімками піднімає пік до `equity`).
    """
    require_decimal("equity", equity, non_negative=True)
    require_decimal("peak_equity", peak_equity, non_negative=True)
    require_decimal("day_start_equity", day_start_equity, non_negative=True)
    state = EquityAnchors(peak_equity=max(peak_equity, equity), day_start_equity=day_start_equity, equity=equity)
    for cashflow in cashflows:
        require_decimal("cashflow", cashflow)
        state = adjust_equity_anchors(
            equity_after=state.equity + cashflow,
            cashflow=cashflow,
            peak_equity=state.peak_equity,
            day_start_equity=state.day_start_equity,
        )
    return state


def _funding_gate(limits: RiskLimits, funding_rate: Decimal | None, side: PositionSide | None) -> LimitDecision:
    if limits.max_funding_rate_abs is None:
        return _allow()
    if funding_rate is None or side is None:
        return _block(RiskReason.RISK_FUNDING)
    return check_funding_rate(rate=funding_rate, side=side, max_abs=limits.max_funding_rate_abs)


def _allow() -> LimitDecision:
    return LimitDecision(ok=True, reason=None)


def _block(reason: RiskReason) -> LimitDecision:
    return LimitDecision(ok=False, reason=reason)


def _require_count(name: str, value: object, *, minimum: int = 0) -> int:
    if type(value) is not int or isinstance(value, bool) or value < minimum:
        raise ValueError(f"{name} має бути int ≥ {minimum}")
    return value


def _require_ms(name: str, value: object) -> int:
    if type(value) is not int or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} має бути невід'ємний int")
    return value
