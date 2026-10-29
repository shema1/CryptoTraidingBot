"""Доменні типи біржового шару (ТЗ §3, §7, §9.1; T1.1).

Усе, що приймає або повертає `ExchangeAdapter`, — ці моделі, а не сирі dict-и CCXT/Bybit. Правила:
- гроші, ціни, кількості — `Decimal`; час — `int` мілісекунди UTC (ТЗ §4.3, AGENTS.md інваріанти 2–3);
- `strict=True`: `float`, `int` і `str` у `Decimal`-полях, `bool`/`float` у полях часу і «сирі» рядки замість enum
  відхиляються. Адаптер конвертує значення біржі явно (`Decimal(raw)`), тож `float` не просочується в торговий шар;
- NaN/Infinity відхиляються; моделі незмінні (`frozen`) і без зайвих полів (`extra="forbid"`);
- **факти біржі** (ордери, позиції, fills) валідуються лише на внутрішню узгодженість: `client_order_id` ордера —
  будь-який рядок (ручні ордери, чужий `HOST_ID` — їх класифікує звірка, ТЗ §11.4). **Запити** суворіші:
  `place_order` приймає лише наш формат `clientOrderId`, а SL/TP мусять бути з правильного боку для сторони угоди;
- помилка нормалізації відповіді біржі не «губить» ордер чи позицію мовчки: адаптер загортає її в
  `MalformedResponse` (`app/exchange/errors.py`).

Назви `Candle`, `Order`, `Fill`, `Position`, `Instrument` збігаються з ORM-моделями (`app/*/models.py`), але це різні
речі: тут — факти біржі на межі адаптера, там — рядки БД. Імпортуйте модулем (`from app.exchange import types`).
"""

from __future__ import annotations

import re
import uuid
from decimal import Decimal
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.core.db import SYMBOL_RE

# --- Ідентифікатори ордерів бота (ТЗ §6.4, §7.2) ---
# `tb` + HOST_ID (2 × [a-z0-9]) + uuid4().hex (32 × [0-9a-f]) = 36 символів — максимум Bybit `orderLinkId`.
# Єдине джерело формату: CHECK `orders.client_order_id_format` (`app/trading/models.py`) бере його звідси.
BOT_ORDER_PREFIX = "tb"
_HOST_ID_BODY = "[a-z0-9]{2}"
HOST_ID_RE = f"^{_HOST_ID_BODY}$"
CLIENT_ORDER_ID_RE = f"^{BOT_ORDER_PREFIX}{_HOST_ID_BODY}[0-9a-f]{{32}}$"
CLIENT_ORDER_ID_LEN = 36
# Для Python `re` — лише `fullmatch`: `$` у `re.match` пропускає завершальний `\n`.
_HOST_ID = re.compile(_HOST_ID_BODY)
_BOT_CLIENT_ORDER_ID = re.compile(f"{BOT_ORDER_PREFIX}({_HOST_ID_BODY})[0-9a-f]{{32}}")


# --- Базові анотовані типи ---
TimestampMs = Annotated[int, Field(ge=0)]
Price = Annotated[Decimal, Field(gt=0)]
Qty = Annotated[Decimal, Field(gt=0)]
NonNegativeDecimal = Annotated[Decimal, Field(ge=0)]
Symbol = Annotated[str, Field(pattern=SYMBOL_RE)]
NonEmptyStr = Annotated[str, Field(min_length=1)]
ClientOrderId = Annotated[str, Field(pattern=CLIENT_ORDER_ID_RE)]


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)


# --- Enum-и ---
class Side(StrEnum):
    """Сторона ордера/fill (значення — як `ORDER_SIDES` у БД)."""

    BUY = "BUY"
    SELL = "SELL"


class PositionSide(StrEnum):
    """Сторона позиції (значення — як `DIRECTIONS` у БД)."""

    LONG = "LONG"
    SHORT = "SHORT"


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class TimeInForce(StrEnum):
    """Post-only свідомо відсутній: у версії 1 вимкнено (ТЗ §6.6)."""

    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"


class TriggerBy(StrEnum):
    """Ціна-тригер умовного ордера / TP / SL (ТЗ §9.1: SL — mark, TP — last)."""

    MARK_PRICE = "MARK_PRICE"
    LAST_PRICE = "LAST_PRICE"
    INDEX_PRICE = "INDEX_PRICE"


class ExchangeOrderStatus(StrEnum):
    """Статус ордера **на біржі** (факт). Це не стан машини станів `ORDER_STATES` (ТЗ §7.1) — мапінг робить T3.1.

    - `CANCELED` у деривативах може мати `filled_qty > 0` (частковий fill, далі скасування залишку, ТЗ §7.1).
    - `TRIGGERED` — миттєвий перехід умовного ордера з `UNTRIGGERED` у звичайний. Bybit відносить його до закритих
      статусів, але за документацією це перехід до `New` — ордер ще може виконатися, тож тут він **не** термінальний
      (fail-safe). TODO(verify-bybit): чи той самий `orderId` переходить у `New` і як це видно в WS/REST (T3.7).
    - `UNKNOWN` — статус, якого адаптер не знає (синоніми на кшталт Bybit `Created` адаптер мапить явно, T3.2).
      Не термінальний: ордер ще може виконатися. Один такий ордер не робить увесь список нерозбірним
      (`MalformedResponse` засліпив би звірку); машина станів не робить за ним переходів, звірка — CRITICAL.
    """

    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    UNTRIGGERED = "UNTRIGGERED"
    TRIGGERED = "TRIGGERED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    DEACTIVATED = "DEACTIVATED"
    UNKNOWN = "UNKNOWN"

    @property
    def is_terminal(self) -> bool:
        """Ордер більше не може виконатися (виконаний, скасований, відхилений, деактивований)."""
        return self in _TERMINAL_ORDER_STATUSES


class StopOrderType(StrEnum):
    """Тип умовного ордера (ТЗ §9.4: після закриття позиції залишкові умовні ордери скасовуються)."""

    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"
    TRAILING_STOP = "TRAILING_STOP"
    CONDITIONAL = "CONDITIONAL"
    OTHER = "OTHER"


class ExecType(StrEnum):
    """Тип виконання. Funding — не виконання ордера: він іде в `ledger` (T3.4), а не в `fills`."""

    TRADE = "TRADE"
    ADL_TRADE = "ADL_TRADE"
    BUST_TRADE = "BUST_TRADE"
    DELIVERY = "DELIVERY"


class MarginMode(StrEnum):
    """Підтримується лише `ISOLATED` (ТЗ §0, §9.1); `CROSS` потрібен, щоб звірка побачила розбіжність (ТЗ §11.4)."""

    ISOLATED = "ISOLATED"
    CROSS = "CROSS"


class Timeframe(StrEnum):
    """Таймфрейм свічки. Тиждень і місяць свідомо відсутні: інше вирівнювання (понеділок) і змінна довжина."""

    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    H6 = "6h"
    H12 = "12h"
    D1 = "1d"

    @property
    def duration_ms(self) -> int:
        return _TIMEFRAME_MS[self]


_MINUTE_MS = 60_000
_TIMEFRAME_MS: dict[Timeframe, int] = {
    Timeframe.M1: _MINUTE_MS,
    Timeframe.M5: 5 * _MINUTE_MS,
    Timeframe.M15: 15 * _MINUTE_MS,
    Timeframe.M30: 30 * _MINUTE_MS,
    Timeframe.H1: 60 * _MINUTE_MS,
    Timeframe.H2: 2 * 60 * _MINUTE_MS,
    Timeframe.H4: 4 * 60 * _MINUTE_MS,
    Timeframe.H6: 6 * 60 * _MINUTE_MS,
    Timeframe.H12: 12 * 60 * _MINUTE_MS,
    Timeframe.D1: 24 * 60 * _MINUTE_MS,
}
_TERMINAL_ORDER_STATUSES = frozenset(
    {
        ExchangeOrderStatus.FILLED,
        ExchangeOrderStatus.CANCELED,
        ExchangeOrderStatus.REJECTED,
        ExchangeOrderStatus.DEACTIVATED,
    }
)


def _check_protection_orientation(
    *, long: bool, stop_loss: Decimal | None, take_profit: Decimal | None, reference: Decimal | None
) -> None:
    """SL/TP з правильного боку: LONG — SL < reference < TP, SHORT — навпаки. Помилка знаку = збиток (ТЗ §9.1)."""
    if long:
        if stop_loss is not None and take_profit is not None and not stop_loss < take_profit:
            raise ValueError("LONG: stop_loss має бути нижче за take_profit")
        if reference is not None and stop_loss is not None and not stop_loss < reference:
            raise ValueError("LONG: stop_loss має бути нижче за ціну входу")
        if reference is not None and take_profit is not None and not take_profit > reference:
            raise ValueError("LONG: take_profit має бути вище за ціну входу")
    else:
        if stop_loss is not None and take_profit is not None and not stop_loss > take_profit:
            raise ValueError("SHORT: stop_loss має бути вище за take_profit")
        if reference is not None and stop_loss is not None and not stop_loss > reference:
            raise ValueError("SHORT: stop_loss має бути вище за ціну входу")
        if reference is not None and take_profit is not None and not take_profit < reference:
            raise ValueError("SHORT: take_profit має бути нижче за ціну входу")


# --- Факти біржі ---
class Candle(_Model):
    """Свічка `[open_time_ms, open_time_ms + timeframe)`. Адаптер повертає лише закриті (контракт `fetch_candles`)."""

    symbol: Symbol
    timeframe: Timeframe
    open_time_ms: TimestampMs
    open: Price
    high: Price
    low: Price
    close: Price
    volume: NonNegativeDecimal

    @model_validator(mode="after")
    def _consistent(self) -> Candle:
        if self.high < max(self.open, self.close, self.low):
            raise ValueError("high має бути ≥ open, close і low")
        if self.low > min(self.open, self.close):
            raise ValueError("low має бути ≤ open і close")
        if self.open_time_ms % self.timeframe.duration_ms != 0:
            raise ValueError("open_time_ms не вирівняний за таймфреймом")
        return self

    @property
    def close_time_ms(self) -> int:
        """Кінець свічки (не включно): свічка закрита, коли `close_time_ms ≤ now_ms()`."""
        return self.open_time_ms + self.timeframe.duration_ms


class Instrument(_Model):
    """Правила інструмента (ТЗ §8.3, §15). `status` — як повертає біржа; «торгується чи ні» визначає T1.7."""

    symbol: Symbol
    tick_size: Price
    qty_step: Qty
    min_qty: Qty
    min_notional: NonNegativeDecimal
    max_qty: Qty | None
    max_leverage: Annotated[Decimal, Field(ge=1)]
    status: NonEmptyStr

    @model_validator(mode="after")
    def _consistent(self) -> Instrument:
        if self.max_qty is not None and self.max_qty < self.min_qty:
            raise ValueError("max_qty має бути ≥ min_qty")
        return self


class Order(_Model):
    """Ордер на біржі (факт).

    `client_order_id` — будь-який рядок або `None` (ордер створила біржа: спрацювання TP/SL, ліквідація; або ручний
    без id); чий це ордер — `client_order_id_origin()`. `qty = 0` допустимий лише для умовних ордерів
    (`stop_order_type`): TP/SL позиції в режимі `Full` закривають усю позицію — TODO(verify-bybit): яку `qty`
    повертає Bybit для таких ордерів (T3.7). `time_in_force=None` — TIF, якого немає в домені (напр. PostOnly
    ручного ордера). Інваріанти заповнення для статусів `NEW`/`UNTRIGGERED`/`PARTIALLY_FILLED`/`FILLED` — нижче;
    для `UNKNOWN` перевіряється лише `filled_qty ≤ qty` і узгодженість `avg_fill_price`.
    """

    symbol: Symbol
    exchange_order_id: NonEmptyStr
    client_order_id: NonEmptyStr | None
    side: Side
    order_type: OrderType
    status: ExchangeOrderStatus
    qty: NonNegativeDecimal
    filled_qty: NonNegativeDecimal
    avg_fill_price: Price | None
    price: Price | None
    trigger_price: Price | None = None
    stop_order_type: StopOrderType | None = None
    time_in_force: TimeInForce | None
    reduce_only: bool
    reject_reason: str | None = None
    created_at_ms: TimestampMs
    updated_at_ms: TimestampMs

    @model_validator(mode="after")
    def _consistent(self) -> Order:
        if self.qty == 0 and self.stop_order_type is None:
            raise ValueError("qty = 0 допустимий лише для умовного ордера")
        if self.qty > 0:
            if self.filled_qty > self.qty:
                raise ValueError("filled_qty більший за qty")
            if self.status is ExchangeOrderStatus.FILLED and self.filled_qty != self.qty:
                raise ValueError("FILLED: filled_qty має дорівнювати qty")
            if self.status is ExchangeOrderStatus.PARTIALLY_FILLED and not 0 < self.filled_qty < self.qty:
                raise ValueError("PARTIALLY_FILLED: 0 < filled_qty < qty")
        if self.status in (ExchangeOrderStatus.NEW, ExchangeOrderStatus.UNTRIGGERED) and self.filled_qty != 0:
            raise ValueError(f"{self.status}: filled_qty має бути 0")
        if (self.avg_fill_price is None) != (self.filled_qty == 0):
            raise ValueError("avg_fill_price задається тоді й лише тоді, коли filled_qty > 0")
        return self


class Fill(_Model):
    """Виконання (ТЗ §7.2). Дедуплікація — за `exec_id`; комісія може бути від'ємною (rebate мейкера)."""

    exec_id: NonEmptyStr
    symbol: Symbol
    exchange_order_id: NonEmptyStr
    client_order_id: NonEmptyStr | None
    side: Side
    price: Price
    qty: Qty
    fee: Decimal
    fee_currency: NonEmptyStr
    is_maker: bool
    exec_type: ExecType
    ts_ms: TimestampMs


class Position(_Model):
    """Ненульова позиція one-way (ТЗ §0). Порожня позиція не існує як `Position` — її просто немає в списку.

    `stop_loss`/`take_profit` = `None`, коли на біржі не задано (Bybit повертає "0"). `liq_price` = `None`, коли біржа
    її не повертає (напр. плече 1×).
    """

    symbol: Symbol
    side: PositionSide
    qty: Qty
    entry_price: Price
    mark_price: Price
    liq_price: Price | None
    leverage: Annotated[Decimal, Field(gt=0)]
    margin_mode: MarginMode
    stop_loss: Price | None
    take_profit: Price | None
    unrealized_pnl: Decimal
    updated_at_ms: TimestampMs


class Balance(_Model):
    """Баланс у валюті розрахунку (USDT). `sizing_equity = min(wallet_balance, equity)` рахує Risk Engine (T2.2)."""

    coin: NonEmptyStr
    wallet_balance: Decimal
    equity: Decimal
    available_balance: Decimal
    unrealized_pnl: Decimal


class FundingRate(_Model):
    symbol: Symbol
    funding_time_ms: TimestampMs
    rate: Decimal


# --- Запити ---
class PlaceOrderRequest(_Model):
    """Запит на ордер. `reduce_only` обов'язковий явно: вихід, відправлений як вхід, збільшив би експозицію.

    - `client_order_id` — лише наш формат (`CLIENT_ORDER_ID_RE`);
    - LIMIT ⇔ `price`; MARKET — без `price` і без `time_in_force` (біржа виконує як IOC);
      `time_in_force=None` для LIMIT — GTC;
    - нативні SL/TP (ТЗ §9.1) — лише для входу (`reduce_only=False`) і з правильного боку: BUY — SL < TP
      (для LIMIT ще SL < price < TP), SELL — навпаки.
    """

    symbol: Symbol
    client_order_id: ClientOrderId
    side: Side
    order_type: OrderType
    qty: Qty
    reduce_only: bool
    price: Price | None = None
    time_in_force: TimeInForce | None = None
    stop_loss: Price | None = None
    take_profit: Price | None = None
    sl_trigger_by: TriggerBy = TriggerBy.MARK_PRICE
    tp_trigger_by: TriggerBy = TriggerBy.LAST_PRICE

    @model_validator(mode="after")
    def _consistent(self) -> PlaceOrderRequest:
        if self.order_type is OrderType.LIMIT and self.price is None:
            raise ValueError("LIMIT потребує price")
        if self.order_type is OrderType.MARKET:
            if self.price is not None:
                raise ValueError("MARKET не має price")
            if self.time_in_force is not None:
                raise ValueError("MARKET без time_in_force (біржа виконує як IOC)")
        if self.reduce_only and (self.stop_loss is not None or self.take_profit is not None):
            raise ValueError("reduce_only-ордер не несе SL/TP: захист — на позиції")
        _check_protection_orientation(
            long=self.side is Side.BUY, stop_loss=self.stop_loss, take_profit=self.take_profit, reference=self.price
        )
        return self


class PositionProtection(_Model):
    """Захист позиції (`set_position_protection`, ТЗ §9.1, режим `Full`).

    `stop_loss` обов'язковий і > 0: зняти SL через доменний запит неможливо за побудовою (SL знімається лише разом
    із закриттям позиції). `take_profit=None` — TP не змінюється. `position_side` — для перевірки орієнтації
    (LONG: SL < TP, SHORT: SL > TP) і звірки зі стороною фактичної позиції.
    """

    symbol: Symbol
    position_side: PositionSide
    stop_loss: Price
    take_profit: Price | None = None
    sl_trigger_by: TriggerBy = TriggerBy.MARK_PRICE
    tp_trigger_by: TriggerBy = TriggerBy.LAST_PRICE

    @model_validator(mode="after")
    def _consistent(self) -> PositionProtection:
        _check_protection_orientation(
            long=self.position_side is PositionSide.LONG,
            stop_loss=self.stop_loss,
            take_profit=self.take_profit,
            reference=None,
        )
        return self


class OrderAck(_Model):
    """Біржа прийняла ордер (ТЗ §7.1: `SUBMITTING → OPEN`). Статус і fills приходять окремо (WS/REST)."""

    client_order_id: ClientOrderId
    exchange_order_id: NonEmptyStr


# --- Події приватного потоку (ТЗ §11.2) ---
class StreamConnected(_Model):
    """Перша подія кожної підписки: з'єднання встановлено, підписки order/execution/position/wallet підтверджено."""

    connected_at_ms: TimestampMs


class OrderUpdate(_Model):
    order: Order


class FillUpdate(_Model):
    fill: Fill


class PositionUpdate(_Model):
    """`position=None` — позиції за символом більше немає (закрита)."""

    symbol: Symbol
    position: Position | None

    @model_validator(mode="after")
    def _consistent(self) -> PositionUpdate:
        if self.position is not None and self.position.symbol != self.symbol:
            raise ValueError("position.symbol має дорівнювати symbol")
        return self


class BalanceUpdate(_Model):
    balance: Balance


PrivateStreamEvent = StreamConnected | OrderUpdate | FillUpdate | PositionUpdate | BalanceUpdate


class ClientOrderIdOrigin(StrEnum):
    """Чий ордер за його `clientOrderId` (ТЗ §6.4, §11.4).

    - `BOT` — точно наш формат `tb` + `HOST_ID` + uuid4 hex; `HOST_ID` — з `client_order_host_id()`;
    - `BOT_PREFIX_MALFORMED` — префікс `tb` (без урахування регістру), але формат не наш. Звірка трактує такий ордер
      як ордер **іншого комп'ютера** (пауза `FOREIGN_HOST_ORDERS`), а не як ручний: неоднозначність вирішується
      в безпечний бік;
    - `NOT_BOT` — без префікса `tb` або без id: ручний ордер чи ордер біржі («не чіпати, сповіщення»).
    """

    BOT = "BOT"
    BOT_PREFIX_MALFORMED = "BOT_PREFIX_MALFORMED"
    NOT_BOT = "NOT_BOT"


def new_client_order_id(host_id: str) -> str:
    """Новий `clientOrderId` = `tb` + `HOST_ID` + uuid4 hex (ТЗ §7.2). Кожна спроба — новий id (ніколи не повтор)."""
    if type(host_id) is not str or _HOST_ID.fullmatch(host_id) is None:
        raise ValueError("HOST_ID має бути 2 символи [a-z0-9] (ТЗ §6.4)")
    return f"{BOT_ORDER_PREFIX}{host_id}{uuid.uuid4().hex}"


def client_order_id_origin(client_order_id: str | None) -> ClientOrderIdOrigin:
    """Класифікація ордера за id для звірки (ТЗ §11.4); див. `ClientOrderIdOrigin`."""
    if client_order_id is None:
        return ClientOrderIdOrigin.NOT_BOT
    if _BOT_CLIENT_ORDER_ID.fullmatch(client_order_id) is not None:
        return ClientOrderIdOrigin.BOT
    if client_order_id[:2].lower() == BOT_ORDER_PREFIX:
        return ClientOrderIdOrigin.BOT_PREFIX_MALFORMED
    return ClientOrderIdOrigin.NOT_BOT


def client_order_host_id(client_order_id: str | None) -> str | None:
    """`HOST_ID` з id ордера бота або `None`, якщо id не точно нашого формату.

    `None` **не** означає «ручний ордер»: id з префіксом `tb` неточного формату звірка трактує як чужий
    (`client_order_id_origin()` → `BOT_PREFIX_MALFORMED`).
    """
    if client_order_id is None:
        return None
    match = _BOT_CLIENT_ORDER_ID.fullmatch(client_order_id)
    return match.group(1) if match is not None else None
