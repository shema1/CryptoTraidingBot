"""`FakeExchangeAdapter` — детермінований in-memory симулятор біржі з ін'єкцією збоїв. **Лише для тестів** (ТЗ §3).

Виконує контракт `ExchangeAdapter` (`app/exchange/adapter.py`) без мережі й без `sleep`; час — `app.core.clock`
(у тестах — `FakeClock`) плюс `server_time_offset_ms` для часу біржі. Жоден модуль `app/**` не імпортує цей файл
(перевіряє `tests/exchange/test_no_fake_in_production.py`).

Модель біржі (спрощена, але з тими самими гарантіями, на які спирається торгове ядро):
- one-way позиції зі знаковим неттінгом: середня ціна входу, реалізований PnL, комісії з wallet balance;
- MARKET виконується одразу за last-ціною (тейкер); LIMIT, маркетабельний за last-ціною, — теж одразу (тейкер);
  немаркетабельний LIMIT GTC стоїть, доки тест не викличе `fill_order()` (мейкер) або ордер не скасують,
  немаркетабельний IOC/FOK — `CANCELED`;
- фільтри інструмента → `OrderRejected`: крок і межі кількості, крок ціни, мін. номінал (лише для входів),
  статус не `Trading` (дозволені лише reduceOnly), SL/TP з неправильного боку від ціни;
- спрощена маржа: `qty × ціна / плече` для частини, що збільшує позицію, проти доступного балансу →
  `InsufficientMargin`;
- повторний `clientOrderId` (прийнятого раніше ордера) → `DuplicateClientOrderId`;
- reduceOnly без позиції потрібного боку → `OrderRejected`; reduceOnly більший за позицію виконується лише на
  розмір позиції, залишок скасовується (TODO(verify-bybit), T3.7);
- нативні SL/TP вхідного ордера ставляться на позицію (режим `Full`); `set_price()` перевіряє тригери (SL — за ціною
  `sl_trigger_by`, TP — `tp_trigger_by`; INDEX трактується як mark) і закриває позицію біржовим ордером
  (`client_order_id=None`, `StopOrderType.STOP_LOSS`/`TAKE_PROFIT`) за last-ціною; якщо спрацювали обидва — SL;
- TP/SL позиції зберігаються як поля позиції й **не** показуються окремими ордерами в `fetch_open_orders`
  (TODO(verify-bybit): чи показує їх Bybit умовними ордерами, T3.7);
- `liq_price` позицій — `None` (біржа не розраховується);
- свічки — лише закриті за часом біржі (`now_ms() + server_time_offset_ms`);
- напрям руху SL не перевіряється (як і в реальному адаптері — це робота Position Protection Manager, T4.3);
  повтор тих самих SL/TP — успіх.

Ін'єкція збоїв (ТЗ §18.2, `.claude/rules/testing.md`):
- `fail_next(method, Fault, times=n)` — наступні `n` викликів методу падають. `Fault.applied=True` означає
  «біржа виконала запит, але відповідь втрачено» (таймаут / 5xx / нерозбірна відповідь після застосування);
  `Fault.config_error()` — `AdapterConfigError` (напр. позиція в hedge mode);
- `partial_fill_next(*chunks)` — наступний MARKET/IOC ордер виконується частинами `chunks`, залишок скасовується;
- WS: `ws_drop_next(n)`, `ws_duplicate_next(n)`, `ws_disconnect()` — події без підписника губляться, як у житті;
- зовнішні дії: `place_external_order()`, `open_external_position()`, `close_position_externally()`,
  `clear_position_protection()`; зсув часу біржі — `server_time_offset_ms`.
Журнал `calls` фіксує кожен виклик методу адаптера (включно з невдалими) — для перевірок «ордерів не відправлено».
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncGenerator, Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Literal, TypeVar, get_args

from app.core.clock import now_ms
from app.exchange.errors import (
    AdapterConfigError,
    DuplicateClientOrderId,
    ExchangeAuthError,
    ExchangeError,
    ExchangeTimeout,
    ExchangeUnavailable,
    InsufficientMargin,
    MalformedResponse,
    OrderNotActive,
    OrderRejected,
    PrivateStreamLost,
    RateLimited,
    RequestRejected,
)
from app.exchange.types import (
    Balance,
    BalanceUpdate,
    Candle,
    ExchangeOrderStatus,
    ExecType,
    Fill,
    FillUpdate,
    FundingRate,
    Instrument,
    MarginMode,
    Order,
    OrderAck,
    OrderType,
    OrderUpdate,
    PlaceOrderRequest,
    Position,
    PositionProtection,
    PositionSide,
    PositionUpdate,
    PrivateStreamEvent,
    Side,
    StopOrderType,
    StreamConnected,
    Timeframe,
    TimeInForce,
    TriggerBy,
)

FakeMethod = Literal[
    "fetch_time",
    "fetch_instruments",
    "fetch_candles",
    "fetch_funding_history",
    "place_order",
    "cancel_order",
    "fetch_order_by_client_id",
    "fetch_open_orders",
    "fetch_positions",
    "set_position_protection",
    "set_leverage",
    "set_margin_mode",
    "fetch_balance",
    "fetch_executions",
    "subscribe_private_stream",
]
_FAKE_METHODS: frozenset[str] = frozenset(get_args(FakeMethod))

# Статус інструмента, у якому дозволені входи (як повертає Bybit `instruments-info`).
TRADING_STATUS = "Trading"

_T = TypeVar("_T")
_ZERO = Decimal(0)


class FaultKind(StrEnum):
    TIMEOUT = "TIMEOUT"  # → ExchangeTimeout
    UNAVAILABLE = "UNAVAILABLE"  # 5xx / обрив → ExchangeUnavailable
    MALFORMED = "MALFORMED"  # нерозбірна відповідь → MalformedResponse
    RATE_LIMITED = "RATE_LIMITED"  # 429 → RateLimited
    REJECTED = "REJECTED"  # → OrderRejected (place_order) / RequestRejected (інші методи)
    INSUFFICIENT_MARGIN = "INSUFFICIENT_MARGIN"  # → InsufficientMargin
    AUTH = "AUTH"  # → ExchangeAuthError
    CONFIG = "CONFIG"  # → AdapterConfigError (напр. позиція в hedge mode)


# Лише для цих збоїв можливе «виконано, але відповідь втрачено».
_APPLICABLE_KINDS = frozenset({FaultKind.TIMEOUT, FaultKind.UNAVAILABLE, FaultKind.MALFORMED})


@dataclass(frozen=True)
class Fault:
    """Збій одного виклику. `applied=True` (лише TIMEOUT/UNAVAILABLE/MALFORMED): дія виконана, відповідь втрачена."""

    kind: FaultKind
    applied: bool = False
    code: str = "FAKE_REJECTED"
    reason: str = "injected rejection"
    retry_after_ms: int | None = None

    def __post_init__(self) -> None:
        if self.applied and self.kind not in _APPLICABLE_KINDS:
            raise ValueError(f"{self.kind}: біржа відповіла, тож запит не міг бути виконаний (applied=False)")
        if self.retry_after_ms is not None and (
            self.kind is not FaultKind.RATE_LIMITED or type(self.retry_after_ms) is not int or self.retry_after_ms < 0
        ):
            raise ValueError("retry_after_ms — лише для RATE_LIMITED, int мс ≥ 0")
        if not self.code or not self.reason:
            raise ValueError("code і reason не можуть бути порожніми")

    @classmethod
    def timeout(cls, *, applied: bool = False) -> Fault:
        return cls(FaultKind.TIMEOUT, applied=applied)

    @classmethod
    def unavailable(cls, *, applied: bool = False) -> Fault:
        return cls(FaultKind.UNAVAILABLE, applied=applied)

    @classmethod
    def malformed(cls, *, applied: bool = False) -> Fault:
        return cls(FaultKind.MALFORMED, applied=applied)

    @classmethod
    def rate_limited(cls, *, retry_after_ms: int | None = None) -> Fault:
        return cls(FaultKind.RATE_LIMITED, retry_after_ms=retry_after_ms)

    @classmethod
    def rejected(cls, *, code: str = "FAKE_REJECTED", reason: str = "injected rejection") -> Fault:
        return cls(FaultKind.REJECTED, code=code, reason=reason)

    @classmethod
    def insufficient_margin(cls) -> Fault:
        return cls(FaultKind.INSUFFICIENT_MARGIN, code="FAKE_INSUFFICIENT_MARGIN", reason="injected")

    @classmethod
    def auth_error(cls) -> Fault:
        return cls(FaultKind.AUTH)

    @classmethod
    def config_error(cls) -> Fault:
        return cls(FaultKind.CONFIG)


@dataclass(frozen=True)
class FakeCall:
    """Один виклик методу адаптера: назва, аргументи, локальний час виклику (мс)."""

    method: FakeMethod
    args: Mapping[str, object] = field(default_factory=dict)
    ts_ms: int = 0


@dataclass
class _OrderState:
    symbol: str
    exchange_order_id: str
    client_order_id: str | None
    side: Side
    order_type: OrderType
    status: ExchangeOrderStatus
    qty: Decimal
    price: Decimal | None
    time_in_force: TimeInForce
    reduce_only: bool
    created_at_ms: int
    updated_at_ms: int
    trigger_price: Decimal | None = None
    stop_order_type: StopOrderType | None = None
    # Нативні SL/TP (режим `Full`): переносяться на позицію при виконанні.
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    sl_trigger_by: TriggerBy = TriggerBy.MARK_PRICE
    tp_trigger_by: TriggerBy = TriggerBy.LAST_PRICE
    filled_qty: Decimal = _ZERO
    avg_fill_price: Decimal | None = None

    @property
    def remaining(self) -> Decimal:
        return self.qty - self.filled_qty

    @property
    def is_active(self) -> bool:
        return not self.status.is_terminal

    def snapshot(self) -> Order:
        return Order(
            symbol=self.symbol,
            exchange_order_id=self.exchange_order_id,
            client_order_id=self.client_order_id,
            side=self.side,
            order_type=self.order_type,
            status=self.status,
            qty=self.qty,
            filled_qty=self.filled_qty,
            avg_fill_price=self.avg_fill_price,
            price=self.price,
            trigger_price=self.trigger_price,
            stop_order_type=self.stop_order_type,
            time_in_force=self.time_in_force,
            reduce_only=self.reduce_only,
            created_at_ms=self.created_at_ms,
            updated_at_ms=self.updated_at_ms,
        )


@dataclass
class _PositionState:
    side: PositionSide
    qty: Decimal
    entry_price: Decimal
    leverage: Decimal
    margin_mode: MarginMode
    updated_at_ms: int
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    sl_trigger_by: TriggerBy = TriggerBy.MARK_PRICE
    tp_trigger_by: TriggerBy = TriggerBy.LAST_PRICE


class _Disconnect:
    """Маркер обриву в черзі підписника."""


_DISCONNECT = _Disconnect()


def _side_of(side: Side) -> PositionSide:
    return PositionSide.LONG if side is Side.BUY else PositionSide.SHORT


def _positive_decimal(name: str, value: object) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise ValueError(f"{name} має бути скінченний Decimal > 0")
    return value


def _require_ms(name: str, value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} має бути int мс ≥ 0")
    return value


def _check_range(start_ms: int, end_ms: int | None) -> None:
    _require_ms("start_ms", start_ms)
    if end_ms is not None and _require_ms("end_ms", end_ms) <= start_ms:
        raise ValueError("end_ms має бути більшим за start_ms")


def _is_multiple(value: Decimal, step: Decimal) -> bool:
    return value % step == 0


class FakeExchangeAdapter:
    """In-memory біржа для тестів. Див. docstring модуля."""

    def __init__(
        self,
        *,
        coin: str = "USDT",
        wallet_balance: Decimal = Decimal("10000"),
        taker_fee_rate: Decimal = Decimal("0.00055"),
        maker_fee_rate: Decimal = Decimal("0.0002"),
        default_leverage: Decimal = Decimal("1"),
        default_margin_mode: MarginMode = MarginMode.CROSS,
        server_time_offset_ms: int = 0,
    ) -> None:
        if not isinstance(wallet_balance, Decimal) or not wallet_balance.is_finite():
            raise ValueError("wallet_balance має бути скінченний Decimal")
        for name, rate in (("taker_fee_rate", taker_fee_rate), ("maker_fee_rate", maker_fee_rate)):
            if not isinstance(rate, Decimal) or not rate.is_finite():
                raise ValueError(f"{name} має бути скінченний Decimal")
        if _positive_decimal("default_leverage", default_leverage) < 1:
            raise ValueError("default_leverage має бути ≥ 1")
        if type(server_time_offset_ms) is not int:
            raise ValueError("server_time_offset_ms має бути int мс")
        if not coin:
            raise ValueError("coin не може бути порожнім")
        self._coin = coin
        self._wallet = wallet_balance
        self._taker_fee_rate = taker_fee_rate
        self._maker_fee_rate = maker_fee_rate
        self._default_leverage = default_leverage
        self._default_margin_mode = default_margin_mode
        # Зсув годинника біржі відносно локального (мс): `fetch_time` і час подій біржі.
        self.server_time_offset_ms = server_time_offset_ms

        self._instruments: dict[str, Instrument] = {}
        self._last: dict[str, Decimal] = {}
        self._mark: dict[str, Decimal] = {}
        self._leverage: dict[str, Decimal] = {}
        self._margin_mode: dict[str, MarginMode] = {}
        self._candles: dict[tuple[str, Timeframe], dict[int, Candle]] = {}
        self._funding: dict[str, dict[int, FundingRate]] = {}

        self._orders: dict[str, _OrderState] = {}
        self._by_client_id: dict[str, str] = {}
        self._positions: dict[str, _PositionState] = {}
        self._fills: list[Fill] = []
        self._order_seq = 0
        self._exec_seq = 0

        self._faults: dict[str, deque[Fault]] = {}
        self._fill_plan: tuple[Decimal, ...] | None = None
        self._calls: list[FakeCall] = []

        self._subscribers: list[asyncio.Queue[PrivateStreamEvent | _Disconnect]] = []
        self._ws_drop = 0
        self._ws_duplicate = 0

    # ------------------------------------------------------------------ налаштування ринку (лише тести)
    def add_instrument(self, instrument: Instrument, *, last_price: Decimal, mark_price: Decimal | None = None) -> None:
        """Додати інструмент з початковими цінами (`mark_price=None` — дорівнює last). Повторне додавання — помилка."""
        if instrument.symbol in self._instruments:
            raise ValueError(f"інструмент {instrument.symbol} уже є — використайте update_instrument")
        last = _positive_decimal("last_price", last_price)
        mark = last if mark_price is None else _positive_decimal("mark_price", mark_price)
        self._instruments[instrument.symbol] = instrument
        self._last[instrument.symbol] = last
        self._mark[instrument.symbol] = mark
        self._leverage[instrument.symbol] = min(self._default_leverage, instrument.max_leverage)
        self._margin_mode[instrument.symbol] = self._default_margin_mode

    def update_instrument(self, instrument: Instrument) -> None:
        """Замінити правила наявного інструмента (напр. статус «не торгується»)."""
        self._instrument(instrument.symbol)
        self._instruments[instrument.symbol] = instrument

    def add_candles(self, candles: Iterable[Candle]) -> None:
        """Додати свічки; та сама `(symbol, timeframe, open_time_ms)` замінюється."""
        for candle in candles:
            self._instrument(candle.symbol)
            self._candles.setdefault((candle.symbol, candle.timeframe), {})[candle.open_time_ms] = candle

    def add_funding_rates(self, rates: Iterable[FundingRate]) -> None:
        for rate in rates:
            self._instrument(rate.symbol)
            self._funding.setdefault(rate.symbol, {})[rate.funding_time_ms] = rate

    def set_price(self, symbol: str, *, last: Decimal | None = None, mark: Decimal | None = None) -> None:
        """Змінити last і/або mark ціну; після зміни перевіряються тригери SL/TP позиції."""
        self._instrument(symbol)
        if last is None and mark is None:
            raise ValueError("потрібна last і/або mark")
        if last is not None:
            self._last[symbol] = _positive_decimal("last", last)
        if mark is not None:
            self._mark[symbol] = _positive_decimal("mark", mark)
        self._check_triggers(symbol)

    # ------------------------------------------------------------------ ін'єкція збоїв
    def fail_next(self, method: FakeMethod, fault: Fault, *, times: int = 1) -> None:
        """Наступні `times` викликів `method` падають з `fault` (черга; кілька `fail_next` — по порядку)."""
        if method not in _FAKE_METHODS:
            raise ValueError(f"невідомий метод адаптера: {method}")
        if type(times) is not int or times < 1:
            raise ValueError("times має бути int ≥ 1")
        self._faults.setdefault(method, deque()).extend([fault] * times)

    def clear_faults(self) -> None:
        self._faults.clear()
        self._fill_plan = None

    def partial_fill_next(self, *chunks: Decimal) -> None:
        """Наступний MARKET/IOC ордер виконується окремими fills `chunks`; залишок скасовується (`CANCELED`)."""
        if not chunks:
            raise ValueError("потрібна щонайменше одна частина")
        self._fill_plan = tuple(_positive_decimal("chunk", chunk) for chunk in chunks)

    def fill_order(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
        qty: Decimal | None = None,
        price: Decimal | None = None,
    ) -> Fill:
        """Виконати активний LIMIT-ордер: `qty=None` — увесь залишок, `price=None` — ціна ордера (мейкер)."""
        order = self._find_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)
        if order is None:
            raise ValueError("ордер не знайдено")
        if order.order_type is not OrderType.LIMIT or order.status not in (
            ExchangeOrderStatus.NEW,
            ExchangeOrderStatus.PARTIALLY_FILLED,
        ):
            raise ValueError(f"виконати можна лише активний LIMIT-ордер, а не {order.order_type}/{order.status}")
        fill_qty = order.remaining if qty is None else _positive_decimal("qty", qty)
        if fill_qty > order.remaining:
            raise ValueError("qty більша за залишок ордера")
        fill_qty = self._cap_reduce_only(order, fill_qty)
        if fill_qty == 0:
            raise ValueError("reduceOnly-ордер: немає позиції, яку він зменшує")
        limit_price = order.price
        if limit_price is None:
            raise ValueError("LIMIT-ордер без ціни")
        fill_price = limit_price if price is None else _positive_decimal("price", price)
        return self._apply_fill(order, fill_qty, fill_price, is_maker=True)

    def ws_drop_next(self, n: int = 1) -> None:
        """Наступні `n` подій приватного потоку губляться (для всіх підписників)."""
        if type(n) is not int or n < 1:
            raise ValueError("n має бути int ≥ 1")
        self._ws_drop += n

    def ws_duplicate_next(self, n: int = 1) -> None:
        """Наступні `n` подій приватного потоку доставляються двічі."""
        if type(n) is not int or n < 1:
            raise ValueError("n має бути int ≥ 1")
        self._ws_duplicate += n

    def ws_disconnect(self) -> None:
        """Обрив: кожен поточний підписник після вже отриманих подій отримує `PrivateStreamLost`; події до нової
        підписки губляться."""
        for queue in self._subscribers:
            queue.put_nowait(_DISCONNECT)
        self._subscribers.clear()

    @property
    def stream_subscribers(self) -> int:
        return len(self._subscribers)

    def pending_stream_events(self) -> list[int]:
        """Кількість недоставлених подій у черзі кожного підписника."""
        return [queue.qsize() for queue in self._subscribers]

    # ------------------------------------------------------------------ дії поза ботом (вручну, інший комп'ютер)
    def place_external_order(
        self,
        *,
        symbol: str,
        side: Side,
        qty: Decimal,
        price: Decimal,
        client_order_id: str | None = None,
        order_type: OrderType = OrderType.LIMIT,
        reduce_only: bool = False,
        stop_order_type: StopOrderType | None = None,
        trigger_price: Decimal | None = None,
        status: ExchangeOrderStatus | None = None,
    ) -> Order:
        """Ордер, поставлений не ботом (без префікса `tb` або з чужим `HOST_ID`). Стоїть, не виконується сам.

        Статус за замовчуванням — `NEW`, з `stop_order_type` — `UNTRIGGERED`; явно — лише ці два або `UNKNOWN`
        (статус, якого адаптер не знає: для тестів «немає переходу» / «звірка — CRITICAL»).
        """
        self._instrument(symbol)
        if status is None:
            status = ExchangeOrderStatus.NEW if stop_order_type is None else ExchangeOrderStatus.UNTRIGGERED
        elif status not in (ExchangeOrderStatus.NEW, ExchangeOrderStatus.UNTRIGGERED, ExchangeOrderStatus.UNKNOWN):
            raise ValueError("зовнішній ордер створюється лише в статусі NEW, UNTRIGGERED або UNKNOWN")
        if order_type is not OrderType.LIMIT:
            raise ValueError("зовнішній MARKET-ордер — open_external_position / close_position_externally")
        if client_order_id is not None and (not client_order_id or client_order_id in self._by_client_id):
            raise ValueError("client_order_id порожній або вже використаний")
        order = self._new_order(
            symbol=symbol,
            client_order_id=client_order_id,
            side=side,
            order_type=OrderType.LIMIT,
            qty=_positive_decimal("qty", qty),
            price=_positive_decimal("price", price),
            time_in_force=TimeInForce.GTC,
            reduce_only=reduce_only,
            status=status,
            stop_order_type=stop_order_type,
            trigger_price=None if trigger_price is None else _positive_decimal("trigger_price", trigger_price),
        )
        self._emit(OrderUpdate(order=order.snapshot()))
        return order.snapshot()

    def open_external_position(self, *, symbol: str, side: PositionSide, qty: Decimal) -> None:
        """Позиція, відкрита вручну (market за last-ціною, ордер без `client_order_id`)."""
        self._instrument(symbol)
        order = self._new_order(
            symbol=symbol,
            client_order_id=None,
            side=Side.BUY if side is PositionSide.LONG else Side.SELL,
            order_type=OrderType.MARKET,
            qty=_positive_decimal("qty", qty),
            price=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=False,
            status=ExchangeOrderStatus.NEW,
        )
        self._apply_fill(order, order.qty, self._last[symbol], is_maker=False)

    def close_position_externally(self, symbol: str) -> None:
        """Закрити позицію вручну (market за last-ціною, ордер без `client_order_id`)."""
        position = self._positions.get(symbol)
        if position is None:
            raise ValueError(f"позиції {symbol} немає")
        order = self._new_order(
            symbol=symbol,
            client_order_id=None,
            side=Side.SELL if position.side is PositionSide.LONG else Side.BUY,
            order_type=OrderType.MARKET,
            qty=position.qty,
            price=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=True,
            status=ExchangeOrderStatus.NEW,
        )
        self._apply_fill(order, order.qty, self._last[symbol], is_maker=False)

    def clear_position_protection(self, symbol: str) -> None:
        """SL/TP позиції зникли (напр. зняті вручну на біржі)."""
        position = self._positions.get(symbol)
        if position is None:
            raise ValueError(f"позиції {symbol} немає")
        position.stop_loss = None
        position.take_profit = None
        position.updated_at_ms = self._exchange_now()
        self._emit_position(symbol)

    # ------------------------------------------------------------------ огляд стану (лише тести)
    @property
    def calls(self) -> list[FakeCall]:
        return list(self._calls)

    def calls_to(self, method: FakeMethod) -> list[FakeCall]:
        return [call for call in self._calls if call.method == method]

    def all_orders(self) -> list[Order]:
        """Усі ордери біржі в порядку створення (будь-який статус)."""
        return [order.snapshot() for order in self._orders.values()]

    # ------------------------------------------------------------------ ExchangeAdapter
    async def fetch_time(self) -> int:
        return self._run("fetch_time", {}, self._exchange_now)

    async def fetch_instruments(self, *, symbols: Collection[str] | None = None) -> list[Instrument]:
        def action() -> list[Instrument]:
            wanted = None if symbols is None else set(symbols)
            return [
                self._instruments[symbol] for symbol in sorted(self._instruments) if wanted is None or symbol in wanted
            ]

        return self._run("fetch_instruments", {"symbols": symbols}, action)

    async def fetch_candles(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[Candle]:
        _check_range(start_ms, end_ms)
        if limit is not None and (type(limit) is not int or limit < 1):
            raise ValueError("limit має бути int ≥ 1")

        def action() -> list[Candle]:
            self._known_symbol(symbol)
            # Закритість — за годинником біржі, а не локальним (ТЗ §4.3 допускає розбіжність до 1 с).
            exchange_now = self._exchange_now()
            series = self._candles.get((symbol, timeframe), {})
            result = [
                series[open_ms]
                for open_ms in sorted(series)
                if open_ms >= start_ms
                and (end_ms is None or open_ms < end_ms)
                and series[open_ms].close_time_ms <= exchange_now
            ]
            return result if limit is None else result[:limit]

        args = {"symbol": symbol, "timeframe": timeframe, "start_ms": start_ms, "end_ms": end_ms, "limit": limit}
        return self._run("fetch_candles", args, action)

    async def fetch_funding_history(
        self, *, symbol: str, start_ms: int, end_ms: int | None = None
    ) -> list[FundingRate]:
        _check_range(start_ms, end_ms)

        def action() -> list[FundingRate]:
            self._known_symbol(symbol)
            series = self._funding.get(symbol, {})
            return [series[ts] for ts in sorted(series) if ts >= start_ms and (end_ms is None or ts < end_ms)]

        return self._run("fetch_funding_history", {"symbol": symbol, "start_ms": start_ms, "end_ms": end_ms}, action)

    async def place_order(self, request: PlaceOrderRequest) -> OrderAck:
        return self._run("place_order", {"request": request}, lambda: self._accept_order(request))

    async def cancel_order(
        self, *, symbol: str, client_order_id: str | None = None, exchange_order_id: str | None = None
    ) -> None:
        if (client_order_id is None) == (exchange_order_id is None):
            raise ValueError("потрібен рівно один з client_order_id / exchange_order_id")

        def action() -> None:
            order = self._find_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)
            if order is None or order.symbol != symbol or not order.is_active:
                raise OrderNotActive(f"ордера немає серед активних: {client_order_id or exchange_order_id}")
            self._finish(order, ExchangeOrderStatus.CANCELED)

        args = {"symbol": symbol, "client_order_id": client_order_id, "exchange_order_id": exchange_order_id}
        self._run("cancel_order", args, action)

    async def fetch_order_by_client_id(self, *, symbol: str, client_order_id: str) -> Order | None:
        def action() -> Order | None:
            order = self._find_order(client_order_id=client_order_id, exchange_order_id=None)
            return order.snapshot() if order is not None and order.symbol == symbol else None

        return self._run("fetch_order_by_client_id", {"symbol": symbol, "client_order_id": client_order_id}, action)

    async def fetch_open_orders(self, *, symbol: str | None = None) -> list[Order]:
        def action() -> list[Order]:
            return [
                order.snapshot()
                for order in self._orders.values()
                if order.is_active and (symbol is None or order.symbol == symbol)
            ]

        return self._run("fetch_open_orders", {"symbol": symbol}, action)

    async def fetch_positions(self, *, symbols: Collection[str] | None = None) -> list[Position]:
        def action() -> list[Position]:
            wanted = None if symbols is None else set(symbols)
            return [
                self._position_snapshot(symbol)
                for symbol in sorted(self._positions)
                if wanted is None or symbol in wanted
            ]

        return self._run("fetch_positions", {"symbols": symbols}, action)

    async def set_position_protection(self, request: PositionProtection) -> None:
        self._run("set_position_protection", {"request": request}, lambda: self._protect(request))

    async def set_leverage(self, *, symbol: str, leverage: Decimal) -> None:
        def action() -> None:
            instrument = self._known_symbol(symbol)
            if not isinstance(leverage, Decimal) or not leverage.is_finite():
                raise ValueError("leverage має бути скінченний Decimal")
            value = leverage
            if not 1 <= value <= instrument.max_leverage:
                raise RequestRejected("LEVERAGE_OUT_OF_RANGE", f"плече {value} поза [1, {instrument.max_leverage}]")
            if self._leverage[symbol] == value:
                return
            self._leverage[symbol] = value
            position = self._positions.get(symbol)
            if position is not None:
                position.leverage = value
                position.updated_at_ms = self._exchange_now()
                self._emit_position(symbol)

        self._run("set_leverage", {"symbol": symbol, "leverage": leverage}, action)

    async def set_margin_mode(self, *, symbol: str, mode: MarginMode) -> None:
        def action() -> None:
            self._known_symbol(symbol)
            if self._margin_mode[symbol] is mode:
                return
            if symbol in self._positions:
                raise RequestRejected("POSITION_EXISTS", "режим маржі не змінюється з відкритою позицією")
            self._margin_mode[symbol] = mode

        self._run("set_margin_mode", {"symbol": symbol, "mode": mode}, action)

    async def fetch_balance(self) -> Balance:
        return self._run("fetch_balance", {}, self._balance_snapshot)

    async def fetch_executions(
        self, *, start_ms: int, end_ms: int | None = None, symbol: str | None = None
    ) -> list[Fill]:
        _check_range(start_ms, end_ms)

        def action() -> list[Fill]:
            fills = [
                fill
                for fill in self._fills
                if fill.ts_ms >= start_ms
                and (end_ms is None or fill.ts_ms < end_ms)
                and (symbol is None or fill.symbol == symbol)
            ]
            return sorted(fills, key=lambda fill: (fill.ts_ms, fill.exec_id))

        return self._run("fetch_executions", {"start_ms": start_ms, "end_ms": end_ms, "symbol": symbol}, action)

    async def subscribe_private_stream(self) -> AsyncGenerator[PrivateStreamEvent, None]:
        connected_at_ms = self._run("subscribe_private_stream", {}, self._exchange_now)
        queue: asyncio.Queue[PrivateStreamEvent | _Disconnect] = asyncio.Queue()
        self._subscribers.append(queue)
        try:
            yield StreamConnected(connected_at_ms=connected_at_ms)
            while True:
                item = await queue.get()
                if isinstance(item, _Disconnect):
                    raise PrivateStreamLost("injected disconnect")
                yield item
        finally:
            if queue in self._subscribers:
                self._subscribers.remove(queue)

    # ------------------------------------------------------------------ внутрішнє: виклики і збої
    def _run(self, method: FakeMethod, args: Mapping[str, object], action: Callable[[], _T]) -> _T:
        """Виклик методу: журнал → збій до дії → дія → збій після дії (`applied`: відповідь втрачено)."""
        self._calls.append(FakeCall(method=method, args=dict(args), ts_ms=now_ms()))
        queue = self._faults.get(method)
        fault = queue.popleft() if queue else None
        if fault is not None and not fault.applied:
            raise self._fault_error(fault, method)
        try:
            result = action()
        except ExchangeError:
            # Відповідь втрачено: клієнт не бачить навіть відмови біржі.
            if fault is not None:
                raise self._fault_error(fault, method) from None
            raise
        if fault is not None:
            raise self._fault_error(fault, method)
        return result

    @staticmethod
    def _fault_error(fault: Fault, method: FakeMethod) -> ExchangeError:
        if fault.kind is FaultKind.TIMEOUT:
            return ExchangeTimeout(f"injected timeout: {method}")
        if fault.kind is FaultKind.UNAVAILABLE:
            return ExchangeUnavailable(f"injected 5xx: {method}")
        if fault.kind is FaultKind.MALFORMED:
            return MalformedResponse(f"injected malformed response: {method}")
        if fault.kind is FaultKind.RATE_LIMITED:
            return RateLimited(f"injected rate limit: {method}", retry_after_ms=fault.retry_after_ms)
        if fault.kind is FaultKind.INSUFFICIENT_MARGIN:
            return InsufficientMargin(fault.code, fault.reason)
        if fault.kind is FaultKind.AUTH:
            return ExchangeAuthError(f"injected auth error: {method}")
        if fault.kind is FaultKind.CONFIG:
            return AdapterConfigError(f"injected unsupported configuration: {method}")
        if method == "place_order":
            return OrderRejected(fault.code, fault.reason)
        return RequestRejected(fault.code, fault.reason)

    # ------------------------------------------------------------------ внутрішнє: ордери
    def _exchange_now(self) -> int:
        if type(self.server_time_offset_ms) is not int:
            raise ValueError("server_time_offset_ms має бути int мс")
        return now_ms() + self.server_time_offset_ms

    def _instrument(self, symbol: str) -> Instrument:
        instrument = self._instruments.get(symbol)
        if instrument is None:
            raise ValueError(f"невідомий інструмент {symbol} — спершу add_instrument")
        return instrument

    def _known_symbol(self, symbol: str) -> Instrument:
        instrument = self._instruments.get(symbol)
        if instrument is None:
            raise RequestRejected("SYMBOL_NOT_FOUND", f"невідомий символ {symbol}")
        return instrument

    def _find_order(self, *, client_order_id: str | None, exchange_order_id: str | None) -> _OrderState | None:
        if exchange_order_id is not None:
            return self._orders.get(exchange_order_id)
        if client_order_id is not None:
            order_id = self._by_client_id.get(client_order_id)
            return None if order_id is None else self._orders[order_id]
        return None

    def _new_order(
        self,
        *,
        symbol: str,
        client_order_id: str | None,
        side: Side,
        order_type: OrderType,
        qty: Decimal,
        price: Decimal | None,
        time_in_force: TimeInForce,
        reduce_only: bool,
        status: ExchangeOrderStatus,
        stop_order_type: StopOrderType | None = None,
        trigger_price: Decimal | None = None,
    ) -> _OrderState:
        self._order_seq += 1
        exchange_order_id = f"fake-ord-{self._order_seq:08d}"
        ts = self._exchange_now()
        order = _OrderState(
            symbol=symbol,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
            side=side,
            order_type=order_type,
            status=status,
            qty=qty,
            price=price,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            created_at_ms=ts,
            updated_at_ms=ts,
            stop_order_type=stop_order_type,
            trigger_price=trigger_price,
        )
        self._orders[exchange_order_id] = order
        if client_order_id is not None:
            self._by_client_id[client_order_id] = exchange_order_id
        return order

    def _accept_order(self, request: PlaceOrderRequest) -> OrderAck:
        """Перевірки біржі → створення ордера → негайне виконання MARKET/IOC/FOK."""
        instrument = self._instruments.get(request.symbol)
        if instrument is None:
            raise OrderRejected("SYMBOL_NOT_FOUND", f"невідомий символ {request.symbol}")
        if request.client_order_id in self._by_client_id:
            raise DuplicateClientOrderId(request.client_order_id)
        if instrument.status != TRADING_STATUS and not request.reduce_only:
            raise OrderRejected("SYMBOL_NOT_TRADING", f"статус {instrument.status}: дозволено лише reduceOnly")
        self._check_qty(instrument, request.qty)
        for name, value in (
            ("price", request.price),
            ("stop_loss", request.stop_loss),
            ("take_profit", request.take_profit),
        ):
            if value is not None and not _is_multiple(value, instrument.tick_size):
                raise OrderRejected("INVALID_PRICE", f"{name} не кратна tick_size {instrument.tick_size}")
        reference = request.price if request.price is not None else self._last[request.symbol]
        if request.reduce_only:
            position = self._positions.get(request.symbol)
            if position is None or _side_of(request.side) is position.side:
                raise OrderRejected("REDUCE_ONLY_NO_POSITION", "reduceOnly: немає позиції, яку ордер зменшує")
        else:
            if request.qty * reference < instrument.min_notional:
                raise OrderRejected("MIN_NOTIONAL", f"номінал нижче {instrument.min_notional}")
            self._check_entry_protection(request, reference)
            self._check_margin(request, reference)

        time_in_force = TimeInForce.IOC if request.order_type is OrderType.MARKET else request.time_in_force
        order = self._new_order(
            symbol=request.symbol,
            client_order_id=request.client_order_id,
            side=request.side,
            order_type=request.order_type,
            qty=request.qty,
            price=request.price,
            time_in_force=time_in_force or TimeInForce.GTC,
            reduce_only=request.reduce_only,
            status=ExchangeOrderStatus.NEW,
        )
        order.stop_loss = request.stop_loss
        order.take_profit = request.take_profit
        order.sl_trigger_by = request.sl_trigger_by
        order.tp_trigger_by = request.tp_trigger_by
        if order.time_in_force is TimeInForce.GTC and not self._is_marketable(order):
            self._emit(OrderUpdate(order=order.snapshot()))
        else:
            self._execute_immediately(order)
        return OrderAck(client_order_id=request.client_order_id, exchange_order_id=order.exchange_order_id)

    @staticmethod
    def _check_qty(instrument: Instrument, qty: Decimal) -> None:
        if not _is_multiple(qty, instrument.qty_step):
            raise OrderRejected("INVALID_QTY", f"qty не кратна qty_step {instrument.qty_step}")
        if qty < instrument.min_qty:
            raise OrderRejected("INVALID_QTY", f"qty нижче min_qty {instrument.min_qty}")
        if instrument.max_qty is not None and qty > instrument.max_qty:
            raise OrderRejected("INVALID_QTY", f"qty вище max_qty {instrument.max_qty}")

    @staticmethod
    def _check_entry_protection(request: PlaceOrderRequest, reference: Decimal) -> None:
        long = request.side is Side.BUY
        if request.stop_loss is not None and (
            request.stop_loss >= reference if long else request.stop_loss <= reference
        ):
            raise OrderRejected("INVALID_STOP_LOSS", "stop_loss з неправильного боку від ціни")
        if request.take_profit is not None and (
            request.take_profit <= reference if long else request.take_profit >= reference
        ):
            raise OrderRejected("INVALID_TAKE_PROFIT", "take_profit з неправильного боку від ціни")

    def _check_margin(self, request: PlaceOrderRequest, reference: Decimal) -> None:
        position = self._positions.get(request.symbol)
        increase = request.qty
        if position is not None and _side_of(request.side) is not position.side:
            increase = max(request.qty - position.qty, _ZERO)
        required = increase * reference / self._leverage[request.symbol]
        if required > self._available_balance():
            raise InsufficientMargin("INSUFFICIENT_MARGIN", "недостатньо доступної маржі")

    def _is_marketable(self, order: _OrderState) -> bool:
        """MARKET — завжди; LIMIT — якщо last не гірша за ціну ордера (BUY: last ≤ price, SELL: last ≥ price)."""
        if order.order_type is OrderType.MARKET or order.price is None:
            return True
        last = self._last[order.symbol]
        return last <= order.price if order.side is Side.BUY else last >= order.price

    def _execute_immediately(self, order: _OrderState) -> None:
        """Виконати зараз за last-ціною (тейкер): MARKET, IOC/FOK, маркетабельний GTC; залишок — `CANCELED`."""
        if not self._is_marketable(order):
            self._finish(order, ExchangeOrderStatus.CANCELED)
            return
        last = self._last[order.symbol]
        chunks: tuple[Decimal, ...] = (order.qty,)
        if self._fill_plan is not None and order.time_in_force is TimeInForce.IOC:
            chunks, self._fill_plan = self._fill_plan, None
        for chunk in chunks:
            fill_qty = self._cap_reduce_only(order, min(chunk, order.remaining))
            if fill_qty <= 0:
                break
            self._apply_fill(order, fill_qty, last, is_maker=False)
        if order.filled_qty < order.qty:
            self._finish(order, ExchangeOrderStatus.CANCELED)

    def _cap_reduce_only(self, order: _OrderState, qty: Decimal) -> Decimal:
        """reduceOnly не збільшує й не розвертає позицію: не більше її розміру, 0 — якщо зменшувати нічого."""
        if not order.reduce_only:
            return qty
        position = self._positions.get(order.symbol)
        if position is None or _side_of(order.side) is position.side:
            return _ZERO
        return min(qty, position.qty)

    def _apply_fill(self, order: _OrderState, qty: Decimal, price: Decimal, *, is_maker: bool) -> Fill:
        ts = self._exchange_now()
        self._exec_seq += 1
        fee = qty * price * (self._maker_fee_rate if is_maker else self._taker_fee_rate)
        fill = Fill(
            exec_id=f"fake-exec-{self._exec_seq:08d}",
            symbol=order.symbol,
            exchange_order_id=order.exchange_order_id,
            client_order_id=order.client_order_id,
            side=order.side,
            price=price,
            qty=qty,
            fee=fee,
            fee_currency=self._coin,
            is_maker=is_maker,
            exec_type=ExecType.TRADE,
            ts_ms=ts,
        )
        self._fills.append(fill)
        filled = order.filled_qty + qty
        order.avg_fill_price = (
            price if order.avg_fill_price is None else (order.avg_fill_price * order.filled_qty + price * qty) / filled
        )
        order.filled_qty = filled
        order.status = ExchangeOrderStatus.FILLED if filled == order.qty else ExchangeOrderStatus.PARTIALLY_FILLED
        order.updated_at_ms = ts
        realized = self._apply_to_position(order, qty, price, ts)
        self._wallet += realized - fee
        self._emit(FillUpdate(fill=fill))
        self._emit(OrderUpdate(order=order.snapshot()))
        self._emit_position(order.symbol)
        self._emit(BalanceUpdate(balance=self._balance_snapshot()))
        return fill

    def _apply_to_position(self, order: _OrderState, qty: Decimal, price: Decimal, ts: int) -> Decimal:
        """Неттінг one-way: доливання — нова середня; зменшення — реалізований PnL; надлишок — розворот."""
        symbol = order.symbol
        side = _side_of(order.side)
        position = self._positions.get(symbol)
        if position is not None and position.side is side:
            position.entry_price = (position.entry_price * position.qty + price * qty) / (position.qty + qty)
            position.qty += qty
            position.updated_at_ms = ts
            self._apply_native_protection(position, order)
            return _ZERO
        realized = _ZERO
        rest = qty
        if position is not None:
            closed = min(position.qty, qty)
            direction = 1 if position.side is PositionSide.LONG else -1
            realized = (price - position.entry_price) * closed * direction
            position.qty -= closed
            position.updated_at_ms = ts
            rest = qty - closed
            if position.qty > 0:
                return realized
            del self._positions[symbol]
        if rest > 0:
            opened = _PositionState(
                side=side,
                qty=rest,
                entry_price=price,
                leverage=self._leverage[symbol],
                margin_mode=self._margin_mode[symbol],
                updated_at_ms=ts,
            )
            self._positions[symbol] = opened
            self._apply_native_protection(opened, order)
        return realized

    @staticmethod
    def _apply_native_protection(position: _PositionState, order: _OrderState) -> None:
        if order.stop_loss is not None:
            position.stop_loss = order.stop_loss
            position.sl_trigger_by = order.sl_trigger_by
        if order.take_profit is not None:
            position.take_profit = order.take_profit
            position.tp_trigger_by = order.tp_trigger_by

    def _finish(self, order: _OrderState, status: ExchangeOrderStatus) -> None:
        order.status = status
        order.updated_at_ms = self._exchange_now()
        self._emit(OrderUpdate(order=order.snapshot()))

    # ------------------------------------------------------------------ внутрішнє: захист позиції
    def _trigger_price(self, symbol: str, trigger_by: TriggerBy) -> Decimal:
        return self._last[symbol] if trigger_by is TriggerBy.LAST_PRICE else self._mark[symbol]

    def _protect(self, request: PositionProtection) -> None:
        instrument = self._known_symbol(request.symbol)
        position = self._positions.get(request.symbol)
        if position is None:
            raise RequestRejected("ZERO_POSITION", "немає позиції для SL/TP")
        if position.side is not request.position_side:
            raise RequestRejected("POSITION_SIDE_MISMATCH", f"позиція {position.side}, запит {request.position_side}")
        for name, value in (("stop_loss", request.stop_loss), ("take_profit", request.take_profit)):
            if value is not None and not _is_multiple(value, instrument.tick_size):
                raise RequestRejected("INVALID_PRICE", f"{name} не кратна tick_size {instrument.tick_size}")
        long = position.side is PositionSide.LONG
        sl_reference = self._trigger_price(request.symbol, request.sl_trigger_by)
        if request.stop_loss >= sl_reference if long else request.stop_loss <= sl_reference:
            raise RequestRejected("INVALID_STOP_LOSS", "stop_loss з неправильного боку від поточної ціни")
        if request.take_profit is not None:
            tp_reference = self._trigger_price(request.symbol, request.tp_trigger_by)
            if request.take_profit <= tp_reference if long else request.take_profit >= tp_reference:
                raise RequestRejected("INVALID_TAKE_PROFIT", "take_profit з неправильного боку від поточної ціни")
        position.stop_loss = request.stop_loss
        position.sl_trigger_by = request.sl_trigger_by
        if request.take_profit is not None:
            position.take_profit = request.take_profit
            position.tp_trigger_by = request.tp_trigger_by
        position.updated_at_ms = self._exchange_now()
        self._emit_position(request.symbol)

    def _check_triggers(self, symbol: str) -> None:
        """Спрацювання SL/TP позиції за ціною їхнього тригера; обидва одразу — песимістично SL."""
        position = self._positions.get(symbol)
        if position is None:
            return
        long = position.side is PositionSide.LONG
        trigger: tuple[StopOrderType, Decimal] | None = None
        if position.take_profit is not None:
            price = self._trigger_price(symbol, position.tp_trigger_by)
            if price >= position.take_profit if long else price <= position.take_profit:
                trigger = (StopOrderType.TAKE_PROFIT, position.take_profit)
        if position.stop_loss is not None:
            price = self._trigger_price(symbol, position.sl_trigger_by)
            if price <= position.stop_loss if long else price >= position.stop_loss:
                trigger = (StopOrderType.STOP_LOSS, position.stop_loss)
        if trigger is None:
            return
        stop_order_type, trigger_price = trigger
        order = self._new_order(
            symbol=symbol,
            client_order_id=None,
            side=Side.SELL if long else Side.BUY,
            order_type=OrderType.MARKET,
            qty=position.qty,
            price=None,
            time_in_force=TimeInForce.IOC,
            reduce_only=True,
            status=ExchangeOrderStatus.TRIGGERED,
            stop_order_type=stop_order_type,
            trigger_price=trigger_price,
        )
        self._emit(OrderUpdate(order=order.snapshot()))
        self._apply_fill(order, order.qty, self._last[symbol], is_maker=False)

    # ------------------------------------------------------------------ внутрішнє: знімки стану і WS
    def _position_snapshot(self, symbol: str) -> Position:
        position = self._positions[symbol]
        mark = self._mark[symbol]
        direction = 1 if position.side is PositionSide.LONG else -1
        return Position(
            symbol=symbol,
            side=position.side,
            qty=position.qty,
            entry_price=position.entry_price,
            mark_price=mark,
            liq_price=None,
            leverage=position.leverage,
            margin_mode=position.margin_mode,
            stop_loss=position.stop_loss,
            take_profit=position.take_profit,
            unrealized_pnl=(mark - position.entry_price) * position.qty * direction,
            updated_at_ms=position.updated_at_ms,
        )

    def _unrealized_pnl(self) -> Decimal:
        return sum((self._position_snapshot(symbol).unrealized_pnl for symbol in self._positions), _ZERO)

    def _available_balance(self) -> Decimal:
        """Спрощено: wallet + нереалізований PnL − маржа позицій − маржа активних вхідних LIMIT-ордерів."""
        used = sum((p.entry_price * p.qty / p.leverage for p in self._positions.values()), _ZERO)
        reserved = sum(
            (
                order.remaining * order.price / self._leverage[order.symbol]
                for order in self._orders.values()
                if order.is_active and not order.reduce_only and order.price is not None and order.qty > 0
            ),
            _ZERO,
        )
        return self._wallet + self._unrealized_pnl() - used - reserved

    def _balance_snapshot(self) -> Balance:
        unrealized = self._unrealized_pnl()
        return Balance(
            coin=self._coin,
            wallet_balance=self._wallet,
            equity=self._wallet + unrealized,
            available_balance=self._available_balance(),
            unrealized_pnl=unrealized,
        )

    def _emit_position(self, symbol: str) -> None:
        position = self._position_snapshot(symbol) if symbol in self._positions else None
        self._emit(PositionUpdate(symbol=symbol, position=position))

    def _emit(self, event: PrivateStreamEvent) -> None:
        """Подія всім поточним підписникам; `ws_drop_next`/`ws_duplicate_next` діють на наступні події біржі."""
        if self._ws_drop > 0:
            self._ws_drop -= 1
            return
        copies = 1
        if self._ws_duplicate > 0:
            self._ws_duplicate -= 1
            copies = 2
        for queue in self._subscribers:
            for _ in range(copies):
                queue.put_nowait(event)
