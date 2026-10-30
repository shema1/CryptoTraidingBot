"""Округлення ціни й кількості за правилами інструмента (ТЗ §8.3, T1.7).

Кількість завжди вниз до `qtyStep`. Якщо після цього вона нульова, менша за `minQty` або її номінал менший за
`minNotional` — результат `0` і причина: вхід пропускається, розмір не піднімається до мінімального лота.
`maxQty` обрізає зверху тим самим кроком униз.

Ціна кратна `tickSize`. Напрям — той, що не збільшує ризик угоди:

| Роль | Напрям | Навіщо |
| --- | --- | --- |
| `LIMIT_BUY` | вниз | не платити більше за розраховану ціну |
| `LIMIT_SELL` | вгору | не продавати дешевше |
| `STOP_LONG` | вгору | стоп ближче до входу, збиток не більший |
| `STOP_SHORT` | вниз | те саме для короткої позиції |
| `TAKE_PROFIT_LONG` | вниз | ціль не далі, ніж розраховано |
| `TAKE_PROFIT_SHORT` | вгору | те саме для короткої позиції |

Ризик і очікувана вигода рахуються вже за цими цінами (ТЗ §8.3). Ціна, що після округлення вниз стала б 0
(менша за один тік), — `ValueError`: такий ордер не відправляється.

Округлення до кроку точне для будь-якої кількості значущих цифр входу (результат ділення в T2.2 має 28):
`divmod`, а не ділення в контексті Decimal. Неможливість точного результату — `ValueError`.
"""

from __future__ import annotations

from decimal import Context, Decimal, Inexact, InvalidOperation, localcontext
from enum import StrEnum
from typing import Final

from app.exchange.types import Instrument

# Контекст округлення до кроку: запас точності і пастка на `Inexact` — результат або точний, або `ValueError`.
_EXACT: Final = Context(prec=100, traps=[Inexact, InvalidOperation])


class PriceRole(StrEnum):
    """Куди округлити ціну, щоб ризик угоди не зріс."""

    LIMIT_BUY = "LIMIT_BUY"
    LIMIT_SELL = "LIMIT_SELL"
    STOP_LONG = "STOP_LONG"
    STOP_SHORT = "STOP_SHORT"
    TAKE_PROFIT_LONG = "TAKE_PROFIT_LONG"
    TAKE_PROFIT_SHORT = "TAKE_PROFIT_SHORT"


class QtyRejectReason(StrEnum):
    """Чому вхід після округлення не береться. Кількість у відповіді тоді `0`."""

    ZERO_AFTER_ROUND = "ZERO_AFTER_ROUND"
    BELOW_MIN_QTY = "BELOW_MIN_QTY"
    BELOW_MIN_NOTIONAL = "BELOW_MIN_NOTIONAL"


class EntryQty:
    """Кількість входу після округлення. `qty == 0` — не входити (`reason` пояснює чому)."""

    __slots__ = ("qty", "reason")

    def __init__(self, qty: Decimal, reason: QtyRejectReason | None) -> None:
        if type(qty) is not Decimal or not qty.is_finite() or qty < 0:
            raise ValueError("qty має бути невід'ємний скінченний Decimal")
        if (reason is None) != (qty > 0):
            raise ValueError("qty = 0 має причину, додатна кількість — ні")
        self.qty = qty
        self.reason = reason

    def __eq__(self, other: object) -> bool:
        if type(other) is not EntryQty:
            return NotImplemented
        return self.qty == other.qty and self.reason == other.reason

    def __repr__(self) -> str:
        return f"EntryQty(qty={self.qty}, reason={self.reason})"


# Униз: не платити більше, стоп короткої ближче до входу, ціль довгої не далі.
_FLOOR_ROLES: frozenset[PriceRole] = frozenset({PriceRole.LIMIT_BUY, PriceRole.STOP_SHORT, PriceRole.TAKE_PROFIT_LONG})


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Найбільший кратний `step` додаток, що не більший за `value`. `value` ≥ 0, `step` > 0. Точно, без контексту."""
    return _to_step(value, step, up=False)


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Найменший кратний `step` додаток, що не менший за `value`. `value` ≥ 0, `step` > 0. Точно, без контексту."""
    return _to_step(value, step, up=True)


def round_price(price: Decimal, tick_size: Decimal, role: PriceRole) -> Decimal:
    """Ціна, кратна `tickSize`, у бік меншого ризику для `role`.

    Округлення вниз, що дало б 0, — `ValueError` (ціна менша за один тік).
    """
    _require_decimal("price", price, positive=True)
    _require_decimal("tick_size", tick_size, positive=True)
    if type(role) is not PriceRole:
        raise ValueError("role має бути PriceRole")
    rounded = floor_to_step(price, tick_size) if role in _FLOOR_ROLES else ceil_to_step(price, tick_size)
    if rounded <= 0:
        raise ValueError("ціна після округлення менша за один тік")
    return rounded


def round_entry_qty(qty: Decimal, instrument: Instrument, *, price: Decimal) -> EntryQty:
    """Кількість входу: вниз до `qtyStep`, не вище `maxQty`, інакше `0` і причина.

    Додатний результат кратний кроку, не більший за `qty`, не менший за `minQty`, а `qty * price` —
    не менший за `minNotional`. До мінімального лота кількість не піднімається.
    """
    if type(instrument) is not Instrument:
        raise TypeError("instrument має бути Instrument")
    _require_decimal("qty", qty, positive=False)
    _require_decimal("price", price, positive=True)
    floored = floor_to_step(qty, instrument.qty_step)
    if instrument.max_qty is not None and floored > instrument.max_qty:
        floored = floor_to_step(instrument.max_qty, instrument.qty_step)
    if floored == 0:
        return EntryQty(Decimal(0), QtyRejectReason.ZERO_AFTER_ROUND)
    if floored < instrument.min_qty:
        return EntryQty(Decimal(0), QtyRejectReason.BELOW_MIN_QTY)
    if floored * price < instrument.min_notional:
        return EntryQty(Decimal(0), QtyRejectReason.BELOW_MIN_NOTIONAL)
    return EntryQty(floored, None)


def _to_step(value: Decimal, step: Decimal, *, up: bool) -> Decimal:
    _require_decimal("value", value, positive=False)
    _require_decimal("step", step, positive=True)
    # Не `value / step`: частка з понад 28 цифрами округлилась би в контексті Decimal, і 9.99…9 з кроком 0.5
    # стало б 10 (більше за вхід). `divmod` точний, а будь-яке неточне округлення тут — виняток, не тихий зсув.
    with localcontext(_EXACT):
        try:
            units, remainder = divmod(value, step)
            if up and remainder:
                units += 1
            return units * step
        except (Inexact, InvalidOperation) as exc:
            raise ValueError("value і step не округлюються точно") from exc


def _require_decimal(name: str, value: object, *, positive: bool) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise ValueError(f"{name} має бути скінченний Decimal")
    if positive:
        if value <= 0:
            raise ValueError(f"{name} має бути > 0")
    elif value < 0:
        raise ValueError(f"{name} має бути ≥ 0")
