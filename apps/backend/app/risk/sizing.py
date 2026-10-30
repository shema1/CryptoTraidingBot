"""Розмір позиції (ТЗ §8.3). Єдина функція для `RiskGate` і бектесту.

```
sizing_equity = min(wallet_balance, total_equity)
risk_usd      = sizing_equity * risk_per_trade_pct
sl_dist       = збиток на 1 qty за цінами виконання моделі витрат
qty_risk      = risk_usd / sl_dist          (ділення вниз)
qty_lev       = sizing_equity * leverage / notional_price
qty_expo      = (sizing_equity * max_total_exposure_pct - current_notional) / notional_price
qty           = floor_to_lot(min(qty_risk, qty_lev, qty_expo, qty_symbol_cap))
```

`notional_price` — більше з округленого входу і ціни виконання. Так номінал не виходить
за стелю ні за опорною ціною, ні за fill: у шорта fill нижчий, і ділення на нього
дало б більше контрактів, ніж дозволяє ліміт. Мінімальний номінал біржі — навпаки, за
меншою з двох цін (`lot_price`): у лонга fill вищий, і за ним лот пройшов би, а біржа
за опорною ціною відхилила б ордер.

`min_equity_usd` тут — за `sizing_equity`: плаваючий прибуток не тримає вхід над порогом,
коли гаманець уже нижче (`check_entry_limits` окремо перевіряє total equity).

Нереалізований прибуток не збільшує розмір, плаваючий збиток зменшує. Плече —
менше з конфігу та `instrument.max_leverage`, ще нижче, якщо інакше ліквідація
ближче ніж `min_liq_distance_sl_multiple` відстаней до стопа. На біржу виставляється
саме повернене плече: менша кількість при високому плечі ліквідацію не відсуває
(при нульовому MM deduction вона від кількості не залежить).

Опорні ціни округлюються до тіка до розрахунку ризику. Вхід market — проти угоди
(лонгу вгору, шорту вниз), стоп і ціль — ролями `PriceRole`, що не збільшують ризик.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Context, Decimal, localcontext

from pydantic import model_validator

from app.exchange.rounding import PriceRole, QtyRejectReason, ceil_to_step, floor_to_step, round_entry_qty, round_price
from app.exchange.types import Instrument, PositionSide
from app.risk.costs import CostModel, PriceExecution
from app.risk.limits import RiskLimits, check_entry_limits, check_min_equity
from app.risk.numbers import FrozenModel, div_down, div_up, fixed_context, require_decimal
from app.risk.reasons import RiskReason

# TODO(verify-bybit) T3.7: крок set_leverage для linear USDT. 0.01 — крок у бік меншого плеча.
LEVERAGE_STEP = Decimal("0.01")
_ONE = Decimal(1)

_QTY_REASON = {
    QtyRejectReason.ZERO_AFTER_ROUND: RiskReason.RISK_QTY_ZERO,
    QtyRejectReason.BELOW_MIN_QTY: RiskReason.RISK_BELOW_MIN_QTY,
    QtyRejectReason.BELOW_MIN_NOTIONAL: RiskReason.RISK_BELOW_MIN_NOTIONAL,
}


class PositionSize(FrozenModel):
    """Результат розрахунку. `qty = 0` — входу немає, `reason` каже чому.

    `entry_price` / `stop_price` / `take_profit_price` — уже округлені тригери.
    `entry_fill` / `stop_fill` — виконання з урахуванням slippage. `leverage` — те,
    що треба виставити на біржі. `risk_usd` — бюджет, не фактичний збиток.
    """

    qty: Decimal
    reason: RiskReason | None
    side: PositionSide
    entry_price: Decimal
    stop_price: Decimal
    take_profit_price: Decimal | None
    entry_fill: Decimal
    stop_fill: Decimal
    leverage: Decimal
    sizing_equity: Decimal
    risk_usd: Decimal
    loss_per_unit: Decimal
    liq_price: Decimal | None

    @model_validator(mode="after")
    def _paired(self) -> PositionSize:
        if (self.qty > 0) == (self.reason is not None):
            raise ValueError("qty і reason не узгоджені")
        if self.qty > 0 and (self.liq_price is None or self.loss_per_unit <= 0):
            raise ValueError("прийнятий розмір без ліквідації або збитку")
        return self


class EntryDecision(FrozenModel):
    """Рішення про новий вхід. `size is None` — відмовив ліміт ще до розрахунку кількості."""

    reason: RiskReason | None
    size: PositionSize | None

    @model_validator(mode="after")
    def _paired(self) -> EntryDecision:
        if self.size is None:
            if self.reason is None:
                raise ValueError("відмова без причини")
            return self
        if self.reason != self.size.reason:
            raise ValueError("причина рішення і розміру різні")
        return self

    @property
    def ok(self) -> bool:
        return self.reason is None


def sizing_equity(*, wallet_balance: Decimal, total_equity: Decimal) -> Decimal:
    """`min(wallet, total equity)`. Прибуток, що плаває, розмір не збільшує (ТЗ §8.3)."""
    wallet = require_decimal("wallet_balance", wallet_balance, non_negative=True)
    equity = require_decimal("total_equity", total_equity, non_negative=True)
    return min(wallet, equity)


def isolated_liq_price(
    *,
    side: PositionSide,
    entry: Decimal,
    qty: Decimal,
    leverage: Decimal,
    maintenance_margin_rate: Decimal,
    mm_deduction: Decimal = Decimal(0),
    taker_fee_rate: Decimal,
    extra_margin: Decimal = Decimal(0),
) -> Decimal:
    """Оцінка ціни ліквідації ізольованого USDT-перпетуала (one-way), extra margin за замовчуванням 0.

    Лонг: `(entry·qty − entry·qty/L − extra/(1−taker) − deduction) / (qty − qty·mmr)`.
    Шорт: `(entry·qty + entry·qty/L + extra/(1+taker) + deduction) / (qty + qty·mmr)`.

    Формула Bybit UTA після 2025-09-02 (Help Center, Liquidation Process, USDT perpetual,
    isolated). Приклад з документа: 1 BTC, вхід 40 000, 50×, extra 3 000, MMR 0,5%,
    taker 0,055% → 36 380,25. При нульових extra і deduction кількість скорочується.
    Комісію закриття формула покриває не повністю (примітка Bybit); запас дає
    `min_liq_distance_sl_multiple`.
    """
    if type(side) is not PositionSide:
        raise ValueError("side має бути PositionSide")
    require_decimal("entry", entry, positive=True)
    require_decimal("qty", qty, positive=True)
    require_decimal("leverage", leverage, positive=True)
    require_decimal("maintenance_margin_rate", maintenance_margin_rate, positive=True)
    require_decimal("mm_deduction", mm_deduction, non_negative=True)
    require_decimal("taker_fee_rate", taker_fee_rate, non_negative=True)
    require_decimal("extra_margin", extra_margin, non_negative=True)
    if maintenance_margin_rate >= 1 or taker_fee_rate >= 1:
        raise ValueError("ставка маржі або комісії має бути < 1")
    long = side is PositionSide.LONG
    with localcontext(Context(prec=50)):
        if extra_margin == 0:
            extra_term = Decimal(0)
        elif long:
            extra_term = extra_margin / (_ONE - taker_fee_rate)
        else:
            extra_term = extra_margin / (_ONE + taker_fee_rate)
        position = entry * qty
        margin = position / leverage
        if long:
            numerator = position - margin - extra_term - mm_deduction
            denominator = qty - qty * maintenance_margin_rate
        else:
            numerator = position + margin + extra_term + mm_deduction
            denominator = qty + qty * maintenance_margin_rate
        if denominator <= 0:
            raise ValueError("знаменник ціни ліквідації не додатний")
        return numerator / denominator


@fixed_context
def calc_position_size(
    *,
    side: PositionSide,
    entry_price: Decimal,
    stop_price: Decimal,
    take_profit_price: Decimal | None = None,
    atr: Decimal,
    wallet_balance: Decimal,
    total_equity: Decimal,
    current_notional: Decimal,
    instrument: Instrument,
    limits: RiskLimits,
    costs: CostModel,
    maintenance_margin_rate: Decimal,
    mm_deduction: Decimal = Decimal(0),
    entry_is_maker: bool = False,
) -> PositionSize:
    """Розмір нового входу або відмова з кодом причини. Кількість вгору не округлюється."""
    _require_context(side, instrument, limits, costs, entry_is_maker)
    equity = sizing_equity(wallet_balance=wallet_balance, total_equity=total_equity)
    require_decimal("current_notional", current_notional, non_negative=True)
    require_decimal("maintenance_margin_rate", maintenance_margin_rate, positive=True)
    require_decimal("mm_deduction", mm_deduction, non_negative=True)
    require_decimal("entry_price", entry_price, positive=True)
    require_decimal("stop_price", stop_price, positive=True)
    require_decimal("atr", atr, non_negative=True)
    if take_profit_price is not None:
        require_decimal("take_profit_price", take_profit_price, positive=True)
    cap = _leverage_cap(limits.max_leverage, instrument.max_leverage)
    risk_usd = equity * limits.risk_per_trade_pct

    levels = _round_levels(side, entry_price, stop_price, take_profit_price, instrument.tick_size)
    if isinstance(levels, RiskReason):
        return _rejected(
            reason=levels,
            side=side,
            entry=entry_price,
            stop=stop_price,
            target=take_profit_price,
            leverage=cap,
            equity=equity,
            risk_usd=risk_usd,
        )
    entry_r, stop_r, target_r = levels

    geometry = _geometry_reason(side, entry_r, stop_r, target_r)
    if geometry is not None:
        return _rejected(
            reason=geometry,
            side=side,
            entry=entry_r,
            stop=stop_r,
            target=target_r,
            leverage=cap,
            equity=equity,
            risk_usd=risk_usd,
        )

    executed = costs.execution(
        side=side,
        entry=entry_r,
        stop=stop_r,
        take_profit=target_r,
        atr=atr,
        entry_is_maker=entry_is_maker,
    )
    base = _Base(
        side=side,
        entry=entry_r,
        stop=stop_r,
        target=target_r,
        leverage=cap,
        equity=equity,
        risk_usd=risk_usd,
        executed=executed,
    )
    if executed is None:
        return base.reject(_unexecutable_reason(costs, side, entry_r, stop_r, atr, entry_is_maker))
    floor = check_min_equity(equity, limits.min_equity_usd)
    if not floor.ok:
        return base.reject(RiskReason.RISK_MIN_EQUITY)
    if equity <= 0:
        return base.reject(RiskReason.RISK_QTY_ZERO)

    fill = executed.entry_fill
    loss = executed.loss_per_unit
    notional_price = max(entry_r, fill)
    lot_price = min(entry_r, fill)
    room = equity * limits.max_total_exposure_pct - current_notional
    if room <= 0:
        return base.reject(RiskReason.RISK_EXPOSURE)
    qty_risk = div_down(risk_usd, loss)
    qty_expo = div_down(room, notional_price)
    qty_symbol = div_down(limits.max_notional_per_symbol_usd, notional_price)
    required = limits.min_liq_distance_sl_multiple * abs(fill - stop_r)

    def max_qty(leverage: Decimal) -> Decimal:
        return min(qty_risk, div_down(equity * leverage, notional_price), qty_expo, qty_symbol)

    def liq_ok(leverage: Decimal, qty: Decimal) -> bool:
        if qty <= 0:
            return False
        liq = isolated_liq_price(
            side=side,
            entry=fill,
            qty=qty,
            leverage=leverage,
            maintenance_margin_rate=maintenance_margin_rate,
            mm_deduction=mm_deduction,
            taker_fee_rate=costs.taker_fee_rate,
        )
        return _liq_distance(side, fill, liq) >= required

    legal = _minimum_legal_qty(instrument, lot_price)
    at_cap = round_entry_qty(max_qty(cap), instrument, price=lot_price)
    if at_cap.qty == 0:
        if at_cap.reason is None:
            raise ValueError("нульова кількість без причини")
        return base.reject(_QTY_REASON[at_cap.reason])

    chosen = _highest_safe_leverage(cap, legal, max_qty, liq_ok)
    if chosen is None:
        return base.reject(RiskReason.RISK_LIQ_DISTANCE)

    raw = max_qty(chosen)
    rounded = round_entry_qty(raw, instrument, price=lot_price)
    if rounded.qty == 0:
        return base.reject(RiskReason.RISK_LIQ_DISTANCE)
    qty = rounded.qty

    def fits(candidate: Decimal) -> bool:
        return liq_ok(chosen, candidate)

    if not fits(qty):
        shrunk = _shrink_qty(instrument, lot_price, legal, qty, fits)
        if shrunk is None:
            return base.reject(RiskReason.RISK_LIQ_DISTANCE)
        qty = shrunk
    liq = isolated_liq_price(
        side=side,
        entry=fill,
        qty=qty,
        leverage=chosen,
        maintenance_margin_rate=maintenance_margin_rate,
        mm_deduction=mm_deduction,
        taker_fee_rate=costs.taker_fee_rate,
    )
    return base.accept(qty=qty, leverage=chosen, liq_price=liq)


@fixed_context
def assess_entry(
    *,
    side: PositionSide,
    entry_price: Decimal,
    stop_price: Decimal,
    take_profit_price: Decimal | None = None,
    atr: Decimal,
    wallet_balance: Decimal,
    total_equity: Decimal,
    peak_equity: Decimal,
    day_start_equity: Decimal,
    current_notional: Decimal,
    bot_open_positions: int,
    open_positions: int,
    trades_today: int,
    consecutive_losses: int,
    bars_since_last_loss: int,
    instrument: Instrument,
    limits: RiskLimits,
    costs: CostModel,
    maintenance_margin_rate: Decimal,
    mm_deduction: Decimal = Decimal(0),
    entry_is_maker: bool = False,
    funding_rate: Decimal | None = None,
) -> EntryDecision:
    """Ліміти, потім розмір. Бектест і `RiskGate` викликають це, а не копію формул.

    `check_entry_slippage` сюди не входить: ціна, з якою його порівнюють, у live і в
    бектесті різна (котирування проти ціни сигналу).
    """
    blocked = check_entry_limits(
        total_equity=total_equity,
        peak_equity=peak_equity,
        day_start_equity=day_start_equity,
        bot_open_positions=bot_open_positions,
        open_positions=open_positions,
        trades_today=trades_today,
        consecutive_losses=consecutive_losses,
        bars_since_last_loss=bars_since_last_loss,
        limits=limits,
        funding_rate=funding_rate,
        side=side,
    )
    if not blocked.ok:
        return EntryDecision(reason=blocked.reason, size=None)
    size = calc_position_size(
        side=side,
        entry_price=entry_price,
        stop_price=stop_price,
        take_profit_price=take_profit_price,
        atr=atr,
        wallet_balance=wallet_balance,
        total_equity=total_equity,
        current_notional=current_notional,
        instrument=instrument,
        limits=limits,
        costs=costs,
        maintenance_margin_rate=maintenance_margin_rate,
        mm_deduction=mm_deduction,
        entry_is_maker=entry_is_maker,
    )
    return EntryDecision(reason=size.reason, size=size)


class _Base:
    """Спільні поля відмови і прийняття, щоб не повторювати конструктор."""

    def __init__(
        self,
        *,
        side: PositionSide,
        entry: Decimal,
        stop: Decimal,
        target: Decimal | None,
        leverage: Decimal,
        equity: Decimal,
        risk_usd: Decimal,
        executed: PriceExecution | None,
    ) -> None:
        self.side = side
        self.entry = entry
        self.stop = stop
        self.target = target
        self.leverage = leverage
        self.equity = equity
        self.risk_usd = risk_usd
        self.executed = executed

    def reject(self, reason: RiskReason) -> PositionSize:
        executed = self.executed
        return PositionSize(
            qty=Decimal(0),
            reason=reason,
            side=self.side,
            entry_price=self.entry,
            stop_price=self.stop,
            take_profit_price=self.target,
            entry_fill=executed.entry_fill if executed is not None else self.entry,
            stop_fill=executed.stop_fill if executed is not None else self.stop,
            leverage=self.leverage,
            sizing_equity=self.equity,
            risk_usd=self.risk_usd,
            loss_per_unit=executed.loss_per_unit if executed is not None else Decimal(0),
            liq_price=None,
        )

    def accept(self, *, qty: Decimal, leverage: Decimal, liq_price: Decimal) -> PositionSize:
        executed = self.executed
        if executed is None:
            raise ValueError("немає цін виконання")
        return PositionSize(
            qty=qty,
            reason=None,
            side=self.side,
            entry_price=self.entry,
            stop_price=self.stop,
            take_profit_price=self.target,
            entry_fill=executed.entry_fill,
            stop_fill=executed.stop_fill,
            leverage=leverage,
            sizing_equity=self.equity,
            risk_usd=self.risk_usd,
            loss_per_unit=executed.loss_per_unit,
            liq_price=liq_price,
        )


def _rejected(
    *,
    reason: RiskReason,
    side: PositionSide,
    entry: Decimal,
    stop: Decimal,
    target: Decimal | None,
    leverage: Decimal,
    equity: Decimal,
    risk_usd: Decimal,
) -> PositionSize:
    return _Base(
        side=side,
        entry=entry,
        stop=stop,
        target=target,
        leverage=leverage,
        equity=equity,
        risk_usd=risk_usd,
        executed=None,
    ).reject(reason)


def _require_context(
    side: PositionSide,
    instrument: Instrument,
    limits: RiskLimits,
    costs: CostModel,
    entry_is_maker: bool,
) -> None:
    if type(side) is not PositionSide:
        raise ValueError("side має бути PositionSide")
    if type(instrument) is not Instrument:
        raise TypeError("instrument має бути Instrument")
    if type(limits) is not RiskLimits:
        raise ValueError("limits має бути RiskLimits")
    if type(costs) is not CostModel:
        raise ValueError("costs має бути CostModel")
    if type(entry_is_maker) is not bool:
        raise ValueError("entry_is_maker має бути bool")


def _unexecutable_reason(
    costs: CostModel,
    side: PositionSide,
    entry: Decimal,
    stop: Decimal,
    atr: Decimal,
    entry_is_maker: bool,
) -> RiskReason:
    """Яка нога дала не додатну ціну виконання. Геометрію стопа і цілі вже перевірено.

    Без цілі виконання є — зламалась ціль. Інакше: лонгу fill входу зсувається вгору, тож
    не додатним став стоп; шорту fill стопа зсувається вгору, тож не додатним став вхід.
    """
    if (
        costs.execution(side=side, entry=entry, stop=stop, take_profit=None, atr=atr, entry_is_maker=entry_is_maker)
        is not None
    ):
        return RiskReason.RISK_TARGET_INVALID
    return RiskReason.RISK_STOP_INVALID if side is PositionSide.LONG else RiskReason.RISK_PRICE_INVALID


def _round_levels(
    side: PositionSide,
    entry: Decimal,
    stop: Decimal,
    target: Decimal | None,
    tick: Decimal,
) -> tuple[Decimal, Decimal, Decimal | None] | RiskReason:
    """Рівні, кратні тіку, або код тієї ноги, що не округлюється (менша за тік або неточна)."""
    long = side is PositionSide.LONG
    try:
        entry_r = ceil_to_step(entry, tick) if long else floor_to_step(entry, tick)
    except ValueError:
        return RiskReason.RISK_PRICE_INVALID
    if entry_r <= 0:
        return RiskReason.RISK_PRICE_INVALID
    try:
        stop_r = round_price(stop, tick, PriceRole.STOP_LONG if long else PriceRole.STOP_SHORT)
    except ValueError:
        return RiskReason.RISK_STOP_INVALID
    target_r = None
    if target is not None:
        try:
            target_r = round_price(target, tick, PriceRole.TAKE_PROFIT_LONG if long else PriceRole.TAKE_PROFIT_SHORT)
        except ValueError:
            return RiskReason.RISK_TARGET_INVALID
    return entry_r, stop_r, target_r


def _geometry_reason(
    side: PositionSide,
    entry: Decimal,
    stop: Decimal,
    target: Decimal | None,
) -> RiskReason | None:
    long = side is PositionSide.LONG
    if long and stop >= entry:
        return RiskReason.RISK_STOP_INVALID
    if not long and stop <= entry:
        return RiskReason.RISK_STOP_INVALID
    if target is None:
        return None
    if long and target <= entry:
        return RiskReason.RISK_TARGET_INVALID
    if not long and target >= entry:
        return RiskReason.RISK_TARGET_INVALID
    return None


def _leverage_cap(configured: Decimal, exchange_max: Decimal) -> Decimal:
    capped = min(configured, exchange_max)
    floored = floor_to_step(capped, LEVERAGE_STEP)
    if floored < 1:
        raise ValueError("плече після кроку менше за 1")
    return floored


def _liq_distance(side: PositionSide, entry: Decimal, liq: Decimal) -> Decimal:
    if side is PositionSide.LONG:
        if liq <= 0:
            return entry
        return entry - liq if liq < entry else Decimal(0)
    if liq <= entry:
        return Decimal(0)
    return liq - entry


def _minimum_legal_qty(instrument: Instrument, price: Decimal) -> Decimal | None:
    step = instrument.qty_step
    qty = ceil_to_step(instrument.min_qty, step)
    if instrument.min_notional > 0:
        qty = max(qty, ceil_to_step(div_up(instrument.min_notional, price), step))
    if instrument.max_qty is not None and qty > instrument.max_qty:
        return None
    checked = round_entry_qty(qty, instrument, price=price)
    if checked.qty == 0:
        return None
    return checked.qty


def _highest_safe_leverage(
    cap: Decimal,
    legal: Decimal | None,
    max_qty: Callable[[Decimal], Decimal],
    liq_ok: Callable[[Decimal, Decimal], bool],
) -> Decimal | None:
    """Найвищий крок плеча, на якому мінімальний лот вміщається і ліквідація ще далеко.

    Кімната за плечем зростає з плечем, відстань до ліквідації — спадає. Безпечні
    значення — відрізок, не промінь від 1×: на 1× лот може не вміститись, а між 1×
    і стелею ще є плече, де вміщається.
    """
    if legal is None:
        return None
    lot = legal

    def room_ok(step: int) -> bool:
        return lot <= max_qty(LEVERAGE_STEP * step)

    def liq_at(step: int) -> bool:
        return liq_ok(LEVERAGE_STEP * step, lot)

    high = _steps(cap)
    if not room_ok(high):
        return None
    if liq_at(high):
        return cap
    low_room = 100
    if not room_ok(100):
        lo = 100
        hi = high
        while lo + 1 < hi:
            mid = (lo + hi) // 2
            if room_ok(mid):
                hi = mid
            else:
                lo = mid
        low_room = hi
    if not liq_at(low_room):
        return None
    lo = low_room
    hi = high
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if liq_at(mid):
            lo = mid
        else:
            hi = mid
    return LEVERAGE_STEP * lo


def _shrink_qty(
    instrument: Instrument,
    price: Decimal,
    legal: Decimal | None,
    qty: Decimal,
    ok: Callable[[Decimal], bool],
) -> Decimal | None:
    """Найбільша кратна кроку кількість не вища за `qty`, що проходить `ok`. Менша qty безпечніша при deduction > 0."""
    if legal is None or legal > qty:
        return None
    step = instrument.qty_step
    high = _units(qty, step)
    low = _units(legal, step)
    if not ok(legal):
        return None
    if ok(qty):
        return qty
    while low + 1 < high:
        mid = (low + high) // 2
        if ok(step * mid):
            low = mid
        else:
            high = mid
    candidate = step * low
    checked = round_entry_qty(candidate, instrument, price=price)
    if checked.qty == 0 or not ok(checked.qty):
        return None
    return checked.qty


def _steps(leverage: Decimal) -> int:
    return _units(leverage, LEVERAGE_STEP)


def _units(value: Decimal, step: Decimal) -> int:
    units, remainder = divmod(value, step)
    if remainder != 0:
        raise ValueError("значення не кратне кроку")
    return int(units)
