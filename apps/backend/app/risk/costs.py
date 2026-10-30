"""Модель витрат, спільна для симулятора, бектесту і розміру позиції (ТЗ §5.2.2, §8.3).

Ціни входу тут — опорні (до slippage): `open[t]` у бектесті, котирування на момент
відправки в live (ТЗ §6.6: SL/TP рахуються від нього ж). Slippage зсуває фактичне
виконання проти угоди; SL/TP лишаються на опорних рівнях (тригери). Якщо live передасть
ціну сигналу, а відправить за котируванням, різниця між ними (до `max_slippage_bps`) у
бюджет ризику не потрапить. Геп через рівень — справа симулятора (ТЗ §5.2.1), не цього модуля.

Запас збитку на стопі — повне виконання: гірший вхід (`slippage_entry`), гірший SL
(`slippage_sl`, за замовчуванням 2× входу) і комісія обох сторін. Інакше збиток при
спрацюванні SL вийшов би більшим за `risk_usd`.

Значення за замовчуванням збігаються з `cost_model` у ТЗ §5.4: taker 0,055%,
частки ATR 0,05 / 0,10 / 0,05. Фіксовані bps є в моделі (ТЗ §5.2.2) і за замовчуванням
0 — приклад у ТЗ їх не задає. Maker за замовчуванням дорівнює taker: знижку біржі
не вигадуємо; post-only у версії 1 вимкнено (ТЗ §6.6).
"""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from typing import Annotated

from pydantic import Field, ValidationError, model_validator

from app.exchange.types import PositionSide
from app.risk.numbers import FrozenModel, fixed_context, require_decimal

# ТЗ §5.5: сигнал старший за це не виконується. Рівно межа — ще свіжий.
DEFAULT_MAX_SIGNAL_AGE_MS = 5_000
_BPS = Decimal(10000)


class SlippageLeg(StrEnum):
    """Яка нога slippage. SL за замовчуванням удвічі більший за вхід."""

    ENTRY = "ENTRY"
    STOP = "STOP"
    TARGET = "TARGET"


class CostModel(FrozenModel):
    """Ставки комісії і slippage. `scaled` лише збільшує витрати (стрес ТЗ §5.2.3)."""

    taker_fee_rate: Annotated[Decimal, Field(gt=0, lt=1)] = Decimal("0.00055")
    maker_fee_rate: Annotated[Decimal, Field(ge=0, lt=1)] = Decimal("0.00055")
    slippage_entry_bps: Annotated[Decimal, Field(ge=0)] = Decimal(0)
    slippage_entry_atr: Annotated[Decimal, Field(ge=0)] = Decimal("0.05")
    # None — взяти 2× відповідної компоненти входу (ТЗ §5.2.2).
    slippage_sl_bps: Annotated[Decimal, Field(ge=0)] | None = None
    slippage_sl_atr: Annotated[Decimal, Field(ge=0)] | None = None
    slippage_tp_bps: Annotated[Decimal, Field(ge=0)] = Decimal(0)
    slippage_tp_atr: Annotated[Decimal, Field(ge=0)] = Decimal("0.05")

    @property
    def stop_slippage_bps(self) -> Decimal:
        if self.slippage_sl_bps is None:
            return self.slippage_entry_bps * 2
        return self.slippage_sl_bps

    @property
    def stop_slippage_atr(self) -> Decimal:
        if self.slippage_sl_atr is None:
            return self.slippage_entry_atr * 2
        return self.slippage_sl_atr

    def entry_fee_rate(self, *, maker: bool) -> Decimal:
        if type(maker) is not bool:
            raise ValueError("maker має бути bool")
        return self.maker_fee_rate if maker else self.taker_fee_rate

    @fixed_context
    def slippage(self, *, price: Decimal, atr: Decimal, leg: SlippageLeg) -> Decimal:
        """Зсув ціни в одиницях котирування: `price * bps/10000 + atr * частка`."""
        require_decimal("price", price, positive=True)
        require_decimal("atr", atr, non_negative=True)
        bps, fraction = _leg_params(self, leg)
        return price * bps / _BPS + atr * fraction

    @fixed_context
    def scaled(self, factor: Decimal) -> CostModel:
        """Ті самі ставки, помножені на `factor`. `factor` < 1 занизив би ризик — відхиляється."""
        require_decimal("factor", factor, positive=True)
        if factor < 1:
            raise ValueError("множник витрат менший за 1")
        data = self.model_dump()
        for key in (
            "taker_fee_rate",
            "maker_fee_rate",
            "slippage_entry_bps",
            "slippage_entry_atr",
            "slippage_tp_bps",
            "slippage_tp_atr",
        ):
            data[key] = data[key] * factor
        for key in ("slippage_sl_bps", "slippage_sl_atr"):
            if data[key] is not None:
                data[key] = data[key] * factor
        try:
            return CostModel.model_validate(data)
        except ValidationError as exc:
            raise ValueError("множник витрат виводить ставку за допустиму межу") from exc

    @fixed_context
    def execution(
        self,
        *,
        side: PositionSide,
        entry: Decimal,
        stop: Decimal,
        take_profit: Decimal | None,
        atr: Decimal,
        entry_is_maker: bool = False,
    ) -> PriceExecution | None:
        """Ціни виконання і збиток на 1 одиницю кількості.

        `None` — стоп не дає збитку (не з того боку від входу) або ціна виконання
        не додатна. Протилежний slippage збільшує відстань до стопа, а не перетинає його.
        """
        if type(side) is not PositionSide:
            raise ValueError("side має бути PositionSide")
        require_decimal("entry", entry, positive=True)
        require_decimal("stop", stop, positive=True)
        require_decimal("atr", atr, non_negative=True)
        if take_profit is not None:
            require_decimal("take_profit", take_profit, positive=True)
        slip_entry = self.slippage(price=entry, atr=atr, leg=SlippageLeg.ENTRY)
        slip_stop = self.slippage(price=stop, atr=atr, leg=SlippageLeg.STOP)
        long = side is PositionSide.LONG
        entry_fill = entry + slip_entry if long else entry - slip_entry
        stop_fill = stop - slip_stop if long else stop + slip_stop
        if entry_fill <= 0 or stop_fill <= 0:
            return None
        if long and stop_fill >= entry_fill:
            return None
        if not long and stop_fill <= entry_fill:
            return None
        fee_entry = self.entry_fee_rate(maker=entry_is_maker) * entry_fill
        fee_stop = self.taker_fee_rate * stop_fill
        loss = (entry_fill - stop_fill if long else stop_fill - entry_fill) + fee_entry + fee_stop
        tp_fill: Decimal | None = None
        tp_net: Decimal | None = None
        if take_profit is not None:
            slip_tp = self.slippage(price=take_profit, atr=atr, leg=SlippageLeg.TARGET)
            tp_fill = take_profit - slip_tp if long else take_profit + slip_tp
            if tp_fill <= 0:
                return None
            gross = tp_fill - entry_fill if long else entry_fill - tp_fill
            tp_net = gross - fee_entry - self.taker_fee_rate * tp_fill
        return PriceExecution(
            entry_fill=entry_fill,
            stop_fill=stop_fill,
            take_profit_fill=tp_fill,
            loss_per_unit=loss,
            tp_net_per_unit=tp_net,
        )


class PriceExecution(FrozenModel):
    """Виконання однієї одиниці кількості. `tp_net_per_unit` може бути від'ємним: витрати з'їли ціль."""

    entry_fill: Decimal
    stop_fill: Decimal
    take_profit_fill: Decimal | None
    loss_per_unit: Decimal
    tp_net_per_unit: Decimal | None

    @model_validator(mode="after")
    def _paired(self) -> PriceExecution:
        if self.loss_per_unit <= 0:
            raise ValueError("збиток на одиницю має бути > 0")
        if (self.take_profit_fill is None) != (self.tp_net_per_unit is None):
            raise ValueError("ціль і її чистий результат задаються разом")
        return self


class FundingEvent(FrozenModel):
    """Одна виплата funding. `rate` > 0 — лонги платять шортам (лінійний USDT)."""

    time_ms: int
    rate: Decimal
    mark_price: Decimal

    @model_validator(mode="after")
    def _sane(self) -> FundingEvent:
        if type(self.time_ms) is not int or isinstance(self.time_ms, bool) or self.time_ms < 0:
            raise ValueError("time_ms має бути невід'ємний int")
        require_decimal("rate", self.rate)
        require_decimal("mark_price", self.mark_price, positive=True)
        return self


@fixed_context
def funding_payment(
    *,
    rate: Decimal,
    qty: Decimal,
    mark_price: Decimal,
    side: PositionSide,
) -> Decimal:
    """Знакова виплата в USDT: «+» — отримали, «−» — заплатили. Номінал = qty × mark."""
    require_decimal("rate", rate)
    require_decimal("qty", qty, non_negative=True)
    require_decimal("mark_price", mark_price, positive=True)
    if type(side) is not PositionSide:
        raise ValueError("side має бути PositionSide")
    signed = rate * qty * mark_price
    return -signed if side is PositionSide.LONG else signed


@fixed_context
def funding_over_hold(
    events: tuple[FundingEvent, ...] | list[FundingEvent],
    *,
    side: PositionSide,
    qty: Decimal,
    opened_at_ms: int,
    closed_at_ms: int | None,
) -> Decimal:
    """Сума виплат, поки позиція відкрита: `opened_at_ms < time <= closed_at_ms`.

    Час — відкриття свічки, на якій угода відкрилась і закрилась (симулятор), або фактичний
    час fill. Мітки funding Bybit стоять рівно на відкритті години, а ордер виконується вже
    після відкриття свічки: подія в мілісекунду входу не входить (позиції на мітці ще немає),
    у мілісекунду виходу — входить (time-stop за `open[t+horizon]` і SL/TP у свічці, що почалась
    з мітки, закривають позицію вже після неї). З часом fill збіг із міткою практично неможливий.
    `closed_at_ms is None` — позиція ще відкрита, беруться всі події після `opened_at_ms`.
    """
    if type(opened_at_ms) is not int or isinstance(opened_at_ms, bool) or opened_at_ms < 0:
        raise ValueError("opened_at_ms має бути невід'ємний int")
    if closed_at_ms is not None and (
        type(closed_at_ms) is not int or isinstance(closed_at_ms, bool) or closed_at_ms < opened_at_ms
    ):
        raise ValueError("closed_at_ms має бути int не менший за opened_at_ms")
    total = Decimal(0)
    for event in events:
        if type(event) is not FundingEvent:
            raise ValueError("подія funding має бути FundingEvent")
        if event.time_ms <= opened_at_ms:
            continue
        if closed_at_ms is not None and event.time_ms > closed_at_ms:
            continue
        total += funding_payment(rate=event.rate, qty=qty, mark_price=event.mark_price, side=side)
    return total


def entry_is_stale(
    *,
    computed_at_ms: int,
    entry_candle_open_time_ms: int,
    max_signal_age_ms: int = DEFAULT_MAX_SIGNAL_AGE_MS,
) -> bool:
    """Сигнал протух, якщо затримка від'ємна або строго більша за `max_signal_age_ms` (ТЗ §5.5)."""
    for name, value in (
        ("computed_at_ms", computed_at_ms),
        ("entry_candle_open_time_ms", entry_candle_open_time_ms),
    ):
        if type(value) is not int or isinstance(value, bool) or value < 0:
            raise ValueError(f"{name} має бути невід'ємний int")
    if type(max_signal_age_ms) is not int or isinstance(max_signal_age_ms, bool) or max_signal_age_ms < 0:
        raise ValueError("max_signal_age_ms має бути невід'ємний int")
    delay = computed_at_ms - entry_candle_open_time_ms
    return delay < 0 or delay > max_signal_age_ms


def _leg_params(costs: CostModel, leg: SlippageLeg) -> tuple[Decimal, Decimal]:
    if type(leg) is not SlippageLeg:
        raise ValueError("leg має бути SlippageLeg")
    if leg is SlippageLeg.ENTRY:
        return costs.slippage_entry_bps, costs.slippage_entry_atr
    if leg is SlippageLeg.STOP:
        return costs.stop_slippage_bps, costs.stop_slippage_atr
    return costs.slippage_tp_bps, costs.slippage_tp_atr
