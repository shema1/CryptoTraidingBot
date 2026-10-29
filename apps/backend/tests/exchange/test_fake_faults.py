"""`FakeExchangeAdapter`: ін'єкція збоїв, модель виконання, захист позиції, приватний потік (T1.1; ТЗ §7, §9, §11.2).

Фейк — основа всіх тестів торгового ядра (T3.x, T4.x), тому тут перевіряється, що він моделює саме ті ситуації, від
яких захищається ядро: «таймаут, але ордер виконано», частковий fill, гонка cancel vs fill, відмова SL, спрацювання
SL біржею, дубль/пропуск/обрив WS і дозавантаження через REST.

Числа: BTC last = mark = 60000, taker 0.00055, maker 0.0002, wallet 10000, плече за замовчуванням 1.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.core.clock import FakeClock
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
from app.exchange.fake import FakeExchangeAdapter, FakeMethod, Fault, FaultKind
from app.exchange.types import (
    BalanceUpdate,
    ExchangeOrderStatus,
    ExecType,
    FillUpdate,
    MarginMode,
    Order,
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
    TimeInForce,
    TriggerBy,
    client_order_host_id,
    new_client_order_id,
)
from tests.exchange.factories import (
    FAKE_NOW_MS,
    H1_MS,
    INSTRUMENT,
    OTHER_SYMBOL,
    SYMBOL,
    drain,
    hourly_candle,
    limit,
    make_fake,
    market,
)

D = Decimal


@pytest.fixture
def fake() -> FakeExchangeAdapter:
    return make_fake()


async def _order(fake: FakeExchangeAdapter, client_order_id: str) -> Order:
    found = await fake.fetch_order_by_client_id(symbol=SYMBOL, client_order_id=client_order_id)
    assert found is not None, f"ордер {client_order_id} не знайдено"
    return found


async def _position(fake: FakeExchangeAdapter, symbol: str = SYMBOL) -> Position:
    positions = await fake.fetch_positions(symbols=[symbol])
    assert len(positions) == 1, f"очікувалась одна позиція, є {positions}"
    return positions[0]


async def _open(
    fake: FakeExchangeAdapter,
    side: Side,
    qty: str = "0.01",
    *,
    stop_loss: str | None = None,
    take_profit: str | None = None,
) -> PlaceOrderRequest:
    request = market(side, qty, stop_loss=stop_loss, take_profit=take_profit)
    await fake.place_order(request)
    return request


async def _subscribe(fake: FakeExchangeAdapter) -> AsyncIterator[PrivateStreamEvent]:
    stream = fake.subscribe_private_stream()
    first = await anext(stream)
    assert type(first) is StreamConnected
    return stream


# ================================================================ журнал викликів


@pytest.mark.asyncio
async def test_calls_record_successful_and_failed_calls(fake: FakeExchangeAdapter) -> None:
    request = market(Side.BUY, "0.01")
    await fake.fetch_time()
    fake.fail_next("place_order", Fault.timeout())
    with pytest.raises(ExchangeTimeout):
        await fake.place_order(request)
    assert [c.method for c in fake.calls] == ["fetch_time", "place_order"]
    place_calls = fake.calls_to("place_order")
    assert len(place_calls) == 1
    assert place_calls[0].ts_ms == FAKE_NOW_MS
    assert request in place_calls[0].args.values()
    assert fake.calls_to("cancel_order") == []


@pytest.mark.asyncio
async def test_calls_record_rejected_order(fake: FakeExchangeAdapter) -> None:
    with pytest.raises(OrderRejected):
        await fake.place_order(market(Side.SELL, "0.01", reduce_only=True))
    assert [c.method for c in fake.calls_to("place_order")] == ["place_order"]


# ================================================================ збої place_order

PLACE_FAULTS: list[tuple[Fault, type[ExchangeError]]] = [
    (Fault.timeout(), ExchangeTimeout),
    (Fault.unavailable(), ExchangeUnavailable),
    (Fault.malformed(), MalformedResponse),
    (Fault.rate_limited(retry_after_ms=1_500), RateLimited),
    (Fault.rejected(code="110007", reason="ab not enough"), OrderRejected),
    (Fault.insufficient_margin(), InsufficientMargin),
    (Fault.auth_error(), ExchangeAuthError),
]


@pytest.mark.parametrize(("fault", "expected"), PLACE_FAULTS, ids=lambda v: getattr(v, "kind", v))
@pytest.mark.asyncio
async def test_place_order_fault_not_applied_leaves_no_order(
    fake: FakeExchangeAdapter, fault: Fault, expected: type[ExchangeError]
) -> None:
    request = market(Side.BUY, "0.01")
    fake.fail_next("place_order", fault)
    with pytest.raises(expected) as exc:
        await fake.place_order(request)
    assert type(exc.value) is expected
    assert await fake.fetch_order_by_client_id(symbol=SYMBOL, client_order_id=request.client_order_id) is None
    assert fake.all_orders() == []
    assert await fake.fetch_positions() == []
    assert await fake.fetch_executions(start_ms=0) == []


@pytest.mark.parametrize(
    "fault", [Fault.timeout(applied=True), Fault.unavailable(applied=True), Fault.malformed(applied=True)]
)
@pytest.mark.asyncio
async def test_place_order_fault_applied_executes_order_but_raises(fake: FakeExchangeAdapter, fault: Fault) -> None:
    """Найнебезпечніший збій: біржа виконала ордер, відповідь втрачено (ТЗ §7.2, SUBMISSION_UNKNOWN)."""
    expected = {
        FaultKind.TIMEOUT: ExchangeTimeout,
        FaultKind.UNAVAILABLE: ExchangeUnavailable,
        FaultKind.MALFORMED: MalformedResponse,
    }[fault.kind]
    request = market(Side.BUY, "0.01")
    fake.fail_next("place_order", fault)
    with pytest.raises(expected):
        await fake.place_order(request)

    order = await _order(fake, request.client_order_id)
    assert order.status is ExchangeOrderStatus.FILLED
    assert order.filled_qty == D("0.01")
    assert [o.client_order_id for o in fake.all_orders()] == [request.client_order_id]
    fills = await fake.fetch_executions(start_ms=0)
    assert [(f.client_order_id, f.qty, f.price) for f in fills] == [(request.client_order_id, D("0.01"), D("60000"))]
    position = await _position(fake)
    assert (position.side, position.qty) == (PositionSide.LONG, D("0.01"))
    # Повтор тим самим id — дубль, а не другий ордер.
    with pytest.raises(DuplicateClientOrderId):
        await fake.place_order(request)
    assert len(fake.all_orders()) == 1


@pytest.mark.asyncio
async def test_rate_limited_carries_retry_after(fake: FakeExchangeAdapter) -> None:
    fake.fail_next("place_order", Fault.rate_limited(retry_after_ms=1_500))
    with pytest.raises(RateLimited) as exc:
        await fake.place_order(market(Side.BUY, "0.01"))
    assert exc.value.retry_after_ms == 1_500


@pytest.mark.asyncio
async def test_rejected_fault_carries_code_and_reason(fake: FakeExchangeAdapter) -> None:
    fake.fail_next("place_order", Fault.rejected(code="110007", reason="ab not enough"))
    with pytest.raises(OrderRejected) as exc:
        await fake.place_order(market(Side.BUY, "0.01"))
    assert (exc.value.code, exc.value.reason) == ("110007", "ab not enough")


@pytest.mark.parametrize(
    "kind", [FaultKind.RATE_LIMITED, FaultKind.REJECTED, FaultKind.INSUFFICIENT_MARGIN, FaultKind.AUTH]
)
def test_fault_applied_only_for_outcome_unknown_kinds(kind: FaultKind) -> None:
    # Відмова/429/auth означають, що біржа запит НЕ виконала — «applied» тут суперечність.
    with pytest.raises(ValueError, match=r".+"):
        Fault(kind, applied=True)


@pytest.mark.asyncio
async def test_fail_next_times_n(fake: FakeExchangeAdapter) -> None:
    fake.fail_next("place_order", Fault.timeout(), times=2)
    first, second, third = (market(Side.BUY, "0.001") for _ in range(3))
    with pytest.raises(ExchangeTimeout):
        await fake.place_order(first)
    with pytest.raises(ExchangeTimeout):
        await fake.place_order(second)
    ack = await fake.place_order(third)
    assert ack.client_order_id == third.client_order_id
    assert [o.client_order_id for o in fake.all_orders()] == [third.client_order_id]


@pytest.mark.asyncio
async def test_fail_next_queue_is_consumed_in_order(fake: FakeExchangeAdapter) -> None:
    fake.fail_next("place_order", Fault.timeout())
    fake.fail_next("place_order", Fault.rejected(code="10001", reason="params"))
    with pytest.raises(ExchangeTimeout):
        await fake.place_order(market(Side.BUY, "0.001"))
    with pytest.raises(OrderRejected) as exc:
        await fake.place_order(market(Side.BUY, "0.001"))
    assert exc.value.code == "10001"
    ok = market(Side.BUY, "0.001")
    assert (await fake.place_order(ok)).client_order_id == ok.client_order_id


@pytest.mark.asyncio
async def test_clear_faults(fake: FakeExchangeAdapter) -> None:
    fake.fail_next("place_order", Fault.timeout(), times=5)
    fake.clear_faults()
    request = market(Side.BUY, "0.001")
    assert (await fake.place_order(request)).client_order_id == request.client_order_id


@pytest.mark.asyncio
async def test_fault_affects_only_its_method(fake: FakeExchangeAdapter) -> None:
    fake.fail_next("place_order", Fault.timeout())
    assert await fake.fetch_positions() == []
    assert await fake.fetch_open_orders() == []
    with pytest.raises(ExchangeTimeout):
        await fake.place_order(market(Side.BUY, "0.001"))


# ================================================================ збої читань і інших записів

ReadCall = Callable[[FakeExchangeAdapter], Awaitable[object]]

READS: dict[FakeMethod, ReadCall] = {
    "fetch_time": lambda f: f.fetch_time(),
    "fetch_instruments": lambda f: f.fetch_instruments(),
    "fetch_positions": lambda f: f.fetch_positions(),
    "fetch_open_orders": lambda f: f.fetch_open_orders(),
    "fetch_balance": lambda f: f.fetch_balance(),
    "fetch_executions": lambda f: f.fetch_executions(start_ms=0),
}

READ_FAULTS: list[tuple[Fault, type[ExchangeError]]] = [
    (Fault.timeout(), ExchangeTimeout),
    (Fault.unavailable(), ExchangeUnavailable),
    (Fault.malformed(), MalformedResponse),
    (Fault.rate_limited(), RateLimited),
    (Fault.rejected(), RequestRejected),
    (Fault.auth_error(), ExchangeAuthError),
]


@pytest.mark.parametrize(("fault", "expected"), READ_FAULTS, ids=lambda v: getattr(v, "kind", v))
@pytest.mark.parametrize("method", list(READS))
@pytest.mark.asyncio
async def test_read_faults_raise_and_next_call_succeeds(
    fake: FakeExchangeAdapter, method: FakeMethod, fault: Fault, expected: type[ExchangeError]
) -> None:
    fake.fail_next(method, fault)
    with pytest.raises(expected) as exc:
        await READS[method](fake)
    assert type(exc.value) is expected
    # Збій одноразовий: наступний виклик того самого методу проходить.
    await READS[method](fake)
    assert len(fake.calls_to(method)) == 2


@pytest.mark.parametrize("existing", [True, False])
@pytest.mark.parametrize("fault", [Fault.timeout(), Fault.unavailable(), Fault.malformed()])
@pytest.mark.asyncio
async def test_fetch_order_by_client_id_timeout_is_never_none(
    fake: FakeExchangeAdapter, fault: Fault, existing: bool
) -> None:
    """На «None = ордера немає» тримається розв'язання SUBMISSION_UNKNOWN: збій не може його імітувати."""
    request = market(Side.BUY, "0.01")
    if existing:
        await fake.place_order(request)
    fake.fail_next("fetch_order_by_client_id", fault)
    with pytest.raises(ExchangeError):
        await fake.fetch_order_by_client_id(symbol=SYMBOL, client_order_id=request.client_order_id)
    after = await fake.fetch_order_by_client_id(symbol=SYMBOL, client_order_id=request.client_order_id)
    assert (after is not None) is existing


@pytest.mark.asyncio
async def test_set_position_protection_fault_not_applied(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY)
    fake.fail_next("set_position_protection", Fault.timeout())
    with pytest.raises(ExchangeTimeout):
        await fake.set_position_protection(
            PositionProtection(symbol=SYMBOL, position_side=PositionSide.LONG, stop_loss=D("58000"))
        )
    assert (await _position(fake)).stop_loss is None


@pytest.mark.asyncio
async def test_set_position_protection_fault_applied(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY)
    fake.fail_next("set_position_protection", Fault.unavailable(applied=True))
    with pytest.raises(ExchangeUnavailable):
        await fake.set_position_protection(
            PositionProtection(symbol=SYMBOL, position_side=PositionSide.LONG, stop_loss=D("58000"))
        )
    assert (await _position(fake)).stop_loss == D("58000")


@pytest.mark.asyncio
async def test_set_position_protection_retry_after_lost_response_is_success(fake: FakeExchangeAdapter) -> None:
    """SL встановлено, відповідь втрачено → повтор того самого SL — успіх, а не відмова (інакше після 3 «невдач»
    захищену позицію примусово закрили б, ТЗ §9.2). Наявність SL перевіряється лише через `fetch_positions`."""
    await _open(fake, Side.BUY)
    protection = PositionProtection(
        symbol=SYMBOL, position_side=PositionSide.LONG, stop_loss=D("58000"), take_profit=D("62000")
    )
    fake.fail_next("set_position_protection", Fault.timeout(applied=True))
    with pytest.raises(ExchangeTimeout):
        await fake.set_position_protection(protection)
    await fake.set_position_protection(protection)
    await fake.set_position_protection(protection)
    position = await _position(fake)
    assert (position.stop_loss, position.take_profit) == (D("58000"), D("62000"))


@pytest.mark.asyncio
async def test_config_fault_raises_adapter_config_error(fake: FakeExchangeAdapter) -> None:
    """Напр. позиція в hedge mode: не пропуск, а помилка (невидима позиція не може бути «безпечною»)."""
    fake.fail_next("fetch_positions", Fault.config_error())
    with pytest.raises(AdapterConfigError):
        await fake.fetch_positions()
    assert await fake.fetch_positions() == []


def test_config_fault_cannot_be_applied() -> None:
    with pytest.raises(ValueError, match=r".+"):
        Fault(FaultKind.CONFIG, applied=True)


@pytest.mark.parametrize(("offset_ms", "returned"), [(-500, False), (-1, False), (0, True), (500, True)])
@pytest.mark.asyncio
async def test_candle_closedness_is_decided_by_exchange_time(offset_ms: int, returned: bool) -> None:
    """Свічка, що закривається рівно зараз за локальним годинником: якщо годинник біржі позаду — вона ще формується
    (ТЗ §4.3 допускає розбіжність до 1 с), і backfill не може забрати її неповною."""
    fake = make_fake(server_time_offset_ms=offset_ms)
    candle = hourly_candle(FAKE_NOW_MS - H1_MS, "60000")
    fake.add_candles([candle])
    result = await fake.fetch_candles(symbol=SYMBOL, timeframe=candle.timeframe, start_ms=0)
    assert result == ([candle] if returned else [])


@pytest.mark.asyncio
async def test_closing_stream_unsubscribes(fake: FakeExchangeAdapter) -> None:
    stream = fake.subscribe_private_stream()
    assert type(await anext(stream)) is StreamConnected
    assert fake.stream_subscribers == 1
    await stream.aclose()
    assert fake.stream_subscribers == 0
    await fake.place_order(market(Side.BUY, "0.01"))
    assert fake.pending_stream_events() == []


@pytest.mark.asyncio
async def test_set_position_protection_rejected_fault(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY)
    fake.fail_next("set_position_protection", Fault.rejected(code="10001", reason="sl"))
    with pytest.raises(RequestRejected) as exc:
        await fake.set_position_protection(
            PositionProtection(symbol=SYMBOL, position_side=PositionSide.LONG, stop_loss=D("58000"))
        )
    assert exc.value.code == "10001"
    assert (await _position(fake)).stop_loss is None


@pytest.mark.asyncio
async def test_cancel_order_fault_applied_cancels_but_raises(fake: FakeExchangeAdapter) -> None:
    request = limit(Side.BUY, "0.01", "50000")
    await fake.place_order(request)
    fake.fail_next("cancel_order", Fault.timeout(applied=True))
    with pytest.raises(ExchangeTimeout):
        await fake.cancel_order(symbol=SYMBOL, client_order_id=request.client_order_id)
    assert (await _order(fake, request.client_order_id)).status is ExchangeOrderStatus.CANCELED


@pytest.mark.asyncio
async def test_cancel_order_fault_not_applied_keeps_order(fake: FakeExchangeAdapter) -> None:
    request = limit(Side.BUY, "0.01", "50000")
    await fake.place_order(request)
    fake.fail_next("cancel_order", Fault.unavailable())
    with pytest.raises(ExchangeUnavailable):
        await fake.cancel_order(symbol=SYMBOL, client_order_id=request.client_order_id)
    assert (await _order(fake, request.client_order_id)).status is ExchangeOrderStatus.NEW


@pytest.mark.asyncio
async def test_subscribe_fault_raises_before_stream_connected(fake: FakeExchangeAdapter) -> None:
    fake.fail_next("subscribe_private_stream", Fault.unavailable())
    stream = fake.subscribe_private_stream()
    with pytest.raises(ExchangeUnavailable):
        await anext(stream)
    assert fake.stream_subscribers == 0
    # Наступна підписка працює.
    retry = fake.subscribe_private_stream()
    assert type(await anext(retry)) is StreamConnected
    assert fake.stream_subscribers == 1


# ================================================================ виконання


@pytest.mark.asyncio
async def test_market_order_fills_at_last_with_taker_fee(fake: FakeExchangeAdapter) -> None:
    request = market(Side.BUY, "0.01")
    ack = await fake.place_order(request)
    assert ack.client_order_id == request.client_order_id

    order = await _order(fake, request.client_order_id)
    assert order.exchange_order_id == ack.exchange_order_id
    assert order.status is ExchangeOrderStatus.FILLED
    assert (order.qty, order.filled_qty, order.avg_fill_price) == (D("0.01"), D("0.01"), D("60000"))
    assert order.order_type is OrderType.MARKET

    fills = await fake.fetch_executions(start_ms=0)
    assert len(fills) == 1
    fill = fills[0]
    assert fill.exchange_order_id == ack.exchange_order_id
    assert fill.client_order_id == request.client_order_id
    assert (fill.side, fill.qty, fill.price) == (Side.BUY, D("0.01"), D("60000"))
    assert fill.fee == D("0.33")  # 0.01 × 60000 × 0.00055
    assert fill.fee_currency == "USDT"
    assert fill.is_maker is False
    assert fill.exec_type is ExecType.TRADE
    assert fill.ts_ms == FAKE_NOW_MS

    position = await _position(fake)
    assert (position.side, position.qty, position.entry_price) == (PositionSide.LONG, D("0.01"), D("60000"))
    assert position.unrealized_pnl == D("0")
    assert position.stop_loss is None
    assert (await fake.fetch_balance()).wallet_balance == D("9999.67")


@pytest.mark.asyncio
async def test_partial_fill_next_market_order_canceled_with_filled(fake: FakeExchangeAdapter) -> None:
    fake.partial_fill_next(D("0.003"), D("0.002"))
    request = market(Side.BUY, "0.01")
    await fake.place_order(request)
    order = await _order(fake, request.client_order_id)
    assert order.status is ExchangeOrderStatus.CANCELED
    assert order.filled_qty == D("0.005")
    assert order.avg_fill_price == D("60000")
    fills = await fake.fetch_executions(start_ms=0)
    assert [f.qty for f in fills] == [D("0.003"), D("0.002")]
    assert len({f.exec_id for f in fills}) == 2
    assert (await _position(fake)).qty == D("0.005")


@pytest.mark.asyncio
async def test_partial_fill_applies_only_to_next_order(fake: FakeExchangeAdapter) -> None:
    fake.partial_fill_next(D("0.003"))
    first = market(Side.BUY, "0.01")
    second = market(Side.BUY, "0.01")
    await fake.place_order(first)
    await fake.place_order(second)
    assert (await _order(fake, second.client_order_id)).status is ExchangeOrderStatus.FILLED
    assert (await _position(fake)).qty == D("0.013")


@pytest.mark.asyncio
async def test_limit_gtc_partial_fill_then_cancel(fake: FakeExchangeAdapter) -> None:
    request = limit(Side.BUY, "0.01", "59000")
    ack = await fake.place_order(request)
    order = await _order(fake, request.client_order_id)
    assert order.status is ExchangeOrderStatus.NEW
    assert order.filled_qty == D("0")

    fill = fake.fill_order(client_order_id=request.client_order_id, qty=D("0.004"))
    assert fill.exchange_order_id == ack.exchange_order_id
    assert (fill.qty, fill.price, fill.is_maker) == (D("0.004"), D("59000"), True)
    assert fill.fee == D("0.0472")  # 0.004 × 59000 × 0.0002

    partially = await _order(fake, request.client_order_id)
    assert partially.status is ExchangeOrderStatus.PARTIALLY_FILLED
    assert (partially.filled_qty, partially.avg_fill_price) == (D("0.004"), D("59000"))
    assert [o.client_order_id for o in await fake.fetch_open_orders(symbol=SYMBOL)] == [request.client_order_id]

    await fake.cancel_order(symbol=SYMBOL, client_order_id=request.client_order_id)
    canceled = await _order(fake, request.client_order_id)
    assert canceled.status is ExchangeOrderStatus.CANCELED
    assert canceled.filled_qty == D("0.004")
    assert await fake.fetch_open_orders(symbol=SYMBOL) == []
    position = await _position(fake)
    assert (position.qty, position.entry_price) == (D("0.004"), D("59000"))


@pytest.mark.asyncio
async def test_cancel_after_full_fill_is_order_not_active(fake: FakeExchangeAdapter) -> None:
    """Гонка cancel vs fill: ордер встиг виконатися (ТЗ §7.1: PENDING_CANCEL → FILLED легальний)."""
    request = limit(Side.BUY, "0.01", "59000")
    await fake.place_order(request)
    fill = fake.fill_order(client_order_id=request.client_order_id)
    assert fill.qty == D("0.01")
    assert (await _order(fake, request.client_order_id)).status is ExchangeOrderStatus.FILLED
    with pytest.raises(OrderNotActive):
        await fake.cancel_order(symbol=SYMBOL, client_order_id=request.client_order_id)
    after = await _order(fake, request.client_order_id)
    assert after.status is ExchangeOrderStatus.FILLED
    assert after.filled_qty == D("0.01")


@pytest.mark.asyncio
async def test_fill_order_by_exchange_id_with_explicit_price(fake: FakeExchangeAdapter) -> None:
    request = limit(Side.SELL, "0.01", "61000")
    ack = await fake.place_order(request)
    fill = fake.fill_order(exchange_order_id=ack.exchange_order_id, price=D("61000.5"))
    assert (fill.side, fill.price, fill.qty) == (Side.SELL, D("61000.5"), D("0.01"))
    position = await _position(fake)
    assert (position.side, position.qty) == (PositionSide.SHORT, D("0.01"))


@pytest.mark.asyncio
async def test_cancel_unknown_order_is_order_not_active(fake: FakeExchangeAdapter) -> None:
    with pytest.raises(OrderNotActive):
        await fake.cancel_order(symbol=SYMBOL, client_order_id=new_client_order_id("aa"))


@pytest.mark.parametrize("tif", [TimeInForce.IOC, TimeInForce.FOK])
@pytest.mark.asyncio
async def test_marketable_ioc_fok_limit_fills(fake: FakeExchangeAdapter, tif: TimeInForce) -> None:
    request = limit(Side.BUY, "0.01", "61000", time_in_force=tif)
    await fake.place_order(request)
    order = await _order(fake, request.client_order_id)
    assert order.status is ExchangeOrderStatus.FILLED
    assert order.filled_qty == D("0.01")
    fills = await fake.fetch_executions(start_ms=0)
    # Виконання за last-ціною (маркетабельний за last), тейкер.
    assert [(f.price, f.is_maker) for f in fills] == [(D("60000"), False)]


@pytest.mark.parametrize(("side", "price"), [(Side.BUY, "60000"), (Side.BUY, "61000"), (Side.SELL, "59000")])
@pytest.mark.asyncio
async def test_marketable_gtc_limit_fills_immediately_as_taker(
    fake: FakeExchangeAdapter, side: Side, price: str
) -> None:
    """Маркетабельний GTC не «стоїть» (так поводиться біржа): виконується одразу за last, тейкер."""
    request = limit(side, "0.01", price)
    await fake.place_order(request)
    order = await _order(fake, request.client_order_id)
    assert (order.status, order.filled_qty, order.avg_fill_price) == (
        ExchangeOrderStatus.FILLED,
        D("0.01"),
        D("60000"),
    )
    fills = await fake.fetch_executions(start_ms=0)
    assert [(f.price, f.is_maker, f.fee) for f in fills] == [(D("60000"), False, D("0.33"))]
    assert await fake.fetch_open_orders() == []


@pytest.mark.asyncio
async def test_marketable_gtc_ignores_partial_fill_plan(fake: FakeExchangeAdapter) -> None:
    """`partial_fill_next` діє лише на MARKET/IOC: GTC виконується повністю, план лишається для наступного."""
    fake.partial_fill_next(D("0.004"))
    gtc = limit(Side.BUY, "0.01", "61000")
    await fake.place_order(gtc)
    assert (await _order(fake, gtc.client_order_id)).filled_qty == D("0.01")
    following = market(Side.BUY, "0.01")
    await fake.place_order(following)
    after = await _order(fake, following.client_order_id)
    assert (after.status, after.filled_qty) == (ExchangeOrderStatus.CANCELED, D("0.004"))


@pytest.mark.parametrize("tif", [TimeInForce.IOC, TimeInForce.FOK])
@pytest.mark.asyncio
async def test_non_marketable_ioc_fok_limit_is_canceled(fake: FakeExchangeAdapter, tif: TimeInForce) -> None:
    request = limit(Side.BUY, "0.01", "59000", time_in_force=tif)
    await fake.place_order(request)
    order = await _order(fake, request.client_order_id)
    assert order.status is ExchangeOrderStatus.CANCELED
    assert order.filled_qty == D("0")
    assert await fake.fetch_executions(start_ms=0) == []
    assert await fake.fetch_positions() == []
    assert await fake.fetch_open_orders() == []


@pytest.mark.asyncio
async def test_open_orders_filtered_by_symbol(fake: FakeExchangeAdapter) -> None:
    btc = limit(Side.BUY, "0.01", "50000")
    eth = limit(Side.BUY, "0.1", "2000.00", symbol=OTHER_SYMBOL)
    await fake.place_order(btc)
    await fake.place_order(eth)
    assert [o.client_order_id for o in await fake.fetch_open_orders(symbol=SYMBOL)] == [btc.client_order_id]
    assert [o.client_order_id for o in await fake.fetch_open_orders(symbol=OTHER_SYMBOL)] == [eth.client_order_id]
    assert {o.client_order_id for o in await fake.fetch_open_orders()} == {btc.client_order_id, eth.client_order_id}


@pytest.mark.asyncio
async def test_fetch_executions_range_and_symbol(fake: FakeExchangeAdapter, fake_clock: FakeClock) -> None:
    first = await _open(fake, Side.BUY)
    fake_clock.advance(5_000)
    second = market(Side.BUY, "0.1", symbol=OTHER_SYMBOL)
    await fake.place_order(second)
    t2 = FAKE_NOW_MS + 5_000
    assert [f.client_order_id for f in await fake.fetch_executions(start_ms=t2)] == [second.client_order_id]
    assert [f.client_order_id for f in await fake.fetch_executions(start_ms=0, end_ms=t2)] == [first.client_order_id]
    assert [f.client_order_id for f in await fake.fetch_executions(start_ms=0, symbol=SYMBOL)] == [
        first.client_order_id
    ]
    both = await fake.fetch_executions(start_ms=0)
    assert [f.ts_ms for f in both] == [FAKE_NOW_MS, t2]


# ================================================================ фільтри інструмента


async def _assert_rejected_without_order(fake: FakeExchangeAdapter, request: PlaceOrderRequest) -> None:
    with pytest.raises(OrderRejected) as exc:
        await fake.place_order(request)
    assert type(exc.value) is OrderRejected
    assert await fake.fetch_order_by_client_id(symbol=request.symbol, client_order_id=request.client_order_id) is None


@pytest.mark.asyncio
async def test_qty_not_multiple_of_step_is_rejected(fake: FakeExchangeAdapter) -> None:
    await _assert_rejected_without_order(fake, market(Side.BUY, "0.0015"))


@pytest.mark.asyncio
async def test_qty_below_min_is_rejected(fake: FakeExchangeAdapter) -> None:
    fake.update_instrument(INSTRUMENT.model_copy(update={"min_qty": D("0.01")}))
    await _assert_rejected_without_order(fake, market(Side.BUY, "0.009"))
    ok = market(Side.BUY, "0.01")
    await fake.place_order(ok)
    assert (await _order(fake, ok.client_order_id)).status is ExchangeOrderStatus.FILLED


@pytest.mark.asyncio
async def test_qty_above_max_is_rejected(fake: FakeExchangeAdapter) -> None:
    fake.update_instrument(INSTRUMENT.model_copy(update={"max_qty": D("0.05")}))
    await _assert_rejected_without_order(fake, market(Side.BUY, "0.051"))
    ok = market(Side.BUY, "0.05")
    await fake.place_order(ok)
    assert (await _order(fake, ok.client_order_id)).status is ExchangeOrderStatus.FILLED


@pytest.mark.asyncio
async def test_price_not_multiple_of_tick_is_rejected(fake: FakeExchangeAdapter) -> None:
    await _assert_rejected_without_order(fake, limit(Side.BUY, "0.01", "59000.05"))


@pytest.mark.asyncio
async def test_min_notional_applies_to_entries_only(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, "0.01")
    fake.update_instrument(INSTRUMENT.model_copy(update={"min_notional": D("600")}))
    await _assert_rejected_without_order(fake, market(Side.BUY, "0.009"))  # 540 < 600
    boundary = market(Side.BUY, "0.01")  # 600 = 600
    await fake.place_order(boundary)
    assert (await _order(fake, boundary.client_order_id)).status is ExchangeOrderStatus.FILLED
    exit_ = market(Side.SELL, "0.001", reduce_only=True)  # 60 < 600, але це вихід
    await fake.place_order(exit_)
    assert (await _order(fake, exit_.client_order_id)).status is ExchangeOrderStatus.FILLED
    assert (await _position(fake)).qty == D("0.019")


@pytest.mark.asyncio
async def test_not_trading_status_blocks_entries_but_not_exits(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, "0.01")
    fake.update_instrument(INSTRUMENT.model_copy(update={"status": "Settling"}))
    await _assert_rejected_without_order(fake, market(Side.BUY, "0.01"))
    exit_ = market(Side.SELL, "0.01", reduce_only=True)
    await fake.place_order(exit_)
    assert (await _order(fake, exit_.client_order_id)).status is ExchangeOrderStatus.FILLED
    assert await fake.fetch_positions() == []


@pytest.mark.parametrize(
    ("side", "sl", "tp"),
    [
        (Side.BUY, "60500", None),  # SL LONG вище last
        (Side.BUY, None, "59500"),  # TP LONG нижче last
        (Side.SELL, "59500", None),  # SL SHORT нижче last
        (Side.SELL, None, "60500"),  # TP SHORT вище last
    ],
)
@pytest.mark.asyncio
async def test_entry_sl_tp_on_wrong_side_of_price_is_rejected(
    fake: FakeExchangeAdapter, side: Side, sl: str | None, tp: str | None
) -> None:
    await _assert_rejected_without_order(fake, market(side, "0.01", stop_loss=sl, take_profit=tp))
    assert await fake.fetch_positions() == []


# ================================================================ маржа


@pytest.mark.asyncio
async def test_insufficient_margin_and_leverage_effect(fake: FakeExchangeAdapter) -> None:
    big = market(Side.BUY, "0.2")  # 12000 USDT при плечі 1 > 10000
    with pytest.raises(InsufficientMargin):
        await fake.place_order(big)
    assert await fake.fetch_order_by_client_id(symbol=SYMBOL, client_order_id=big.client_order_id) is None
    await fake.set_leverage(symbol=SYMBOL, leverage=D("2"))  # 6000 USDT маржі
    retry = market(Side.BUY, "0.2")
    await fake.place_order(retry)
    assert (await _order(fake, retry.client_order_id)).status is ExchangeOrderStatus.FILLED
    assert (await _position(fake)).qty == D("0.2")


# ================================================================ reduceOnly


@pytest.mark.asyncio
async def test_reduce_only_without_position_is_rejected(fake: FakeExchangeAdapter) -> None:
    await _assert_rejected_without_order(fake, market(Side.SELL, "0.01", reduce_only=True))


@pytest.mark.asyncio
async def test_reduce_only_same_side_as_position_is_rejected(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY)
    await _assert_rejected_without_order(fake, market(Side.BUY, "0.01", reduce_only=True))
    assert (await _position(fake)).qty == D("0.01")


@pytest.mark.asyncio
async def test_reduce_only_larger_than_position_is_capped(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, "0.01")
    exit_ = market(Side.SELL, "0.05", reduce_only=True)
    await fake.place_order(exit_)
    order = await _order(fake, exit_.client_order_id)
    assert order.status is ExchangeOrderStatus.CANCELED
    assert order.filled_qty == D("0.01")
    assert order.qty == D("0.05")
    fills = [f for f in await fake.fetch_executions(start_ms=0) if f.client_order_id == exit_.client_order_id]
    assert [f.qty for f in fills] == [D("0.01")]
    assert await fake.fetch_positions() == []


# ================================================================ неттінг one-way


@pytest.mark.asyncio
async def test_netting_average_price_realized_pnl_and_wallet(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, "0.01")  # 60000, fee 0.33
    fake.set_price(SYMBOL, last=D("62000"), mark=D("62000"))
    await _open(fake, Side.BUY, "0.01")  # 62000, fee 0.341
    position = await _position(fake)
    assert (position.qty, position.entry_price) == (D("0.02"), D("61000"))
    assert position.unrealized_pnl == D("20")  # (62000 − 61000) × 0.02

    fake.set_price(SYMBOL, last=D("63000"), mark=D("63000"))
    await fake.place_order(market(Side.SELL, "0.01", reduce_only=True))  # fee 0.3465, realized +20
    position = await _position(fake)
    assert (position.side, position.qty, position.entry_price) == (PositionSide.LONG, D("0.01"), D("61000"))
    assert (await fake.fetch_balance()).wallet_balance == D("10018.9825")  # 10000 − 1.0175 + 20


@pytest.mark.asyncio
async def test_non_reduce_only_order_reverses_position(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, "0.01")  # fee 0.33
    fake.set_price(SYMBOL, last=D("61000"), mark=D("61000"))
    await _open(fake, Side.SELL, "0.03")  # fee 0.03 × 61000 × 0.00055 = 1.0065; realized +10
    position = await _position(fake)
    assert (position.side, position.qty, position.entry_price) == (PositionSide.SHORT, D("0.02"), D("61000"))
    assert (await fake.fetch_balance()).wallet_balance == D("10008.6635")


# ================================================================ захист позиції


@pytest.mark.asyncio
async def test_entry_native_sl_tp_is_set_on_position(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, stop_loss="58000", take_profit="63000")
    position = await _position(fake)
    assert (position.stop_loss, position.take_profit) == (D("58000"), D("63000"))


@pytest.mark.asyncio
async def test_entry_without_sl_gives_unprotected_position(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY)
    position = await _position(fake)
    assert (position.stop_loss, position.take_profit) == (None, None)


@pytest.mark.asyncio
async def test_clear_position_protection(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, stop_loss="58000", take_profit="63000")
    fake.clear_position_protection(SYMBOL)
    position = await _position(fake)
    assert (position.stop_loss, position.take_profit) == (None, None)
    assert position.qty == D("0.01")


@pytest.mark.asyncio
async def test_set_protection_without_position_is_rejected(fake: FakeExchangeAdapter) -> None:
    with pytest.raises(RequestRejected):
        await fake.set_position_protection(
            PositionProtection(symbol=SYMBOL, position_side=PositionSide.LONG, stop_loss=D("58000"))
        )


@pytest.mark.asyncio
async def test_set_protection_for_wrong_position_side_is_rejected(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY)
    with pytest.raises(RequestRejected):
        await fake.set_position_protection(
            PositionProtection(symbol=SYMBOL, position_side=PositionSide.SHORT, stop_loss=D("62000"))
        )
    assert (await _position(fake)).stop_loss is None


@pytest.mark.parametrize(("side", "sl"), [(Side.BUY, "60500"), (Side.SELL, "59500")])
@pytest.mark.asyncio
async def test_set_protection_sl_on_wrong_side_of_mark_is_rejected(
    fake: FakeExchangeAdapter, side: Side, sl: str
) -> None:
    await _open(fake, side)
    position_side = PositionSide.LONG if side is Side.BUY else PositionSide.SHORT
    with pytest.raises(RequestRejected):
        await fake.set_position_protection(
            PositionProtection(symbol=SYMBOL, position_side=position_side, stop_loss=D(sl))
        )
    assert (await _position(fake)).stop_loss is None


@pytest.mark.asyncio
async def test_set_protection_take_profit_none_keeps_existing_tp(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY)
    await fake.set_position_protection(
        PositionProtection(symbol=SYMBOL, position_side=PositionSide.LONG, stop_loss=D("58000"), take_profit=D("63000"))
    )
    position = await _position(fake)
    assert (position.stop_loss, position.take_profit) == (D("58000"), D("63000"))
    await fake.set_position_protection(
        PositionProtection(symbol=SYMBOL, position_side=PositionSide.LONG, stop_loss=D("59000"))
    )
    position = await _position(fake)
    assert (position.stop_loss, position.take_profit) == (D("59000"), D("63000"))


@pytest.mark.asyncio
async def test_set_protection_on_short(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.SELL)
    await fake.set_position_protection(
        PositionProtection(
            symbol=SYMBOL, position_side=PositionSide.SHORT, stop_loss=D("62000"), take_profit=D("57000")
        )
    )
    position = await _position(fake)
    assert (position.side, position.stop_loss, position.take_profit) == (PositionSide.SHORT, D("62000"), D("57000"))


def _exchange_closes(orders: list[Order]) -> list[Order]:
    return [o for o in orders if o.client_order_id is None]


@pytest.mark.asyncio
async def test_long_stop_loss_triggers_on_mark(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, stop_loss="58000")
    fake.set_price(SYMBOL, last=D("58000.1"), mark=D("58000.1"))
    assert (await _position(fake)).qty == D("0.01")  # ще не спрацював

    fake.set_price(SYMBOL, last=D("57990"), mark=D("58000"))  # mark = SL → спрацьовує
    assert await fake.fetch_positions() == []
    closes = _exchange_closes(fake.all_orders())
    assert len(closes) == 1
    close = closes[0]
    assert close.stop_order_type is StopOrderType.STOP_LOSS
    assert (close.side, close.status, close.reduce_only) == (Side.SELL, ExchangeOrderStatus.FILLED, True)
    assert (close.filled_qty, close.avg_fill_price) == (D("0.01"), D("57990"))
    fills = [f for f in await fake.fetch_executions(start_ms=0) if f.exchange_order_id == close.exchange_order_id]
    assert [(f.side, f.qty, f.price, f.client_order_id) for f in fills] == [(Side.SELL, D("0.01"), D("57990"), None)]
    assert await fake.fetch_open_orders() == []


@pytest.mark.asyncio
async def test_short_stop_loss_triggers_on_mark(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.SELL, stop_loss="62000")
    fake.set_price(SYMBOL, mark=D("61999.9"))
    assert (await _position(fake)).side is PositionSide.SHORT
    fake.set_price(SYMBOL, last=D("62010"), mark=D("62000"))
    assert await fake.fetch_positions() == []
    closes = _exchange_closes(fake.all_orders())
    assert [(o.side, o.stop_order_type, o.avg_fill_price) for o in closes] == [
        (Side.BUY, StopOrderType.STOP_LOSS, D("62010"))
    ]


@pytest.mark.asyncio
async def test_take_profit_triggers_on_last_not_mark(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, stop_loss="58000", take_profit="63000")
    fake.set_price(SYMBOL, mark=D("63500"))  # TP за last, mark не важить
    assert (await _position(fake)).qty == D("0.01")
    fake.set_price(SYMBOL, last=D("63000"))
    assert await fake.fetch_positions() == []
    closes = _exchange_closes(fake.all_orders())
    assert [(o.stop_order_type, o.avg_fill_price) for o in closes] == [(StopOrderType.TAKE_PROFIT, D("63000"))]


@pytest.mark.asyncio
async def test_stop_loss_with_last_price_trigger(fake: FakeExchangeAdapter) -> None:
    request = market(Side.BUY, "0.01", stop_loss="58000").model_copy(update={"sl_trigger_by": TriggerBy.LAST_PRICE})
    await fake.place_order(request)
    fake.set_price(SYMBOL, mark=D("57000"))
    assert (await _position(fake)).qty == D("0.01")
    fake.set_price(SYMBOL, last=D("58000"))
    assert await fake.fetch_positions() == []


@pytest.mark.asyncio
async def test_both_triggers_at_once_stop_loss_wins(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, stop_loss="58000", take_profit="63000")
    fake.set_price(SYMBOL, last=D("63000"), mark=D("58000"))
    assert await fake.fetch_positions() == []
    closes = _exchange_closes(fake.all_orders())
    assert [o.stop_order_type for o in closes] == [StopOrderType.STOP_LOSS]


@pytest.mark.asyncio
async def test_cleared_protection_does_not_trigger(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, stop_loss="58000")
    fake.clear_position_protection(SYMBOL)
    fake.set_price(SYMBOL, last=D("50000"), mark=D("50000"))
    assert (await _position(fake)).qty == D("0.01")
    assert _exchange_closes(fake.all_orders()) == []


# ================================================================ плече і режим маржі


@pytest.mark.parametrize("bad", ["0", "0.5", "100.1", "101"])
@pytest.mark.asyncio
async def test_set_leverage_out_of_range_is_rejected(fake: FakeExchangeAdapter, bad: str) -> None:
    with pytest.raises(RequestRejected):
        await fake.set_leverage(symbol=SYMBOL, leverage=D(bad))


@pytest.mark.parametrize("value", ["1", "100"])
@pytest.mark.asyncio
async def test_set_leverage_bounds_inclusive_and_idempotent(fake: FakeExchangeAdapter, value: str) -> None:
    await fake.set_leverage(symbol=SYMBOL, leverage=D(value))
    await fake.set_leverage(symbol=SYMBOL, leverage=D(value))  # «not modified» — успіх
    assert len(fake.calls_to("set_leverage")) == 2
    await _open(fake, Side.BUY)
    assert (await _position(fake)).leverage == D(value)


@pytest.mark.asyncio
async def test_position_gets_symbol_leverage_and_margin_mode(fake: FakeExchangeAdapter) -> None:
    await fake.set_margin_mode(symbol=SYMBOL, mode=MarginMode.ISOLATED)
    await fake.set_leverage(symbol=SYMBOL, leverage=D("5"))
    await _open(fake, Side.BUY)
    position = await _position(fake)
    assert (position.leverage, position.margin_mode) == (D("5"), MarginMode.ISOLATED)


@pytest.mark.asyncio
async def test_default_margin_mode_is_visible_on_position(fake: FakeExchangeAdapter) -> None:
    # Фейк за замовчуванням CROSS: звірка (ТЗ §11.4) має бачити розбіжність із конфігом.
    await _open(fake, Side.BUY)
    position = await _position(fake)
    assert (position.leverage, position.margin_mode) == (D("1"), MarginMode.CROSS)


@pytest.mark.asyncio
async def test_set_margin_mode_with_open_position(fake: FakeExchangeAdapter) -> None:
    await fake.set_margin_mode(symbol=SYMBOL, mode=MarginMode.ISOLATED)
    await _open(fake, Side.BUY)
    with pytest.raises(RequestRejected):
        await fake.set_margin_mode(symbol=SYMBOL, mode=MarginMode.CROSS)
    await fake.set_margin_mode(symbol=SYMBOL, mode=MarginMode.ISOLATED)  # той самий — успіх
    assert (await _position(fake)).margin_mode is MarginMode.ISOLATED


# ================================================================ дії поза ботом


@pytest.mark.asyncio
async def test_external_manual_order_without_id_is_visible(fake: FakeExchangeAdapter) -> None:
    order = fake.place_external_order(symbol=SYMBOL, side=Side.BUY, qty=D("0.01"), price=D("50000"))
    assert order.client_order_id is None
    assert order.status is ExchangeOrderStatus.NEW
    fake.set_price(SYMBOL, last=D("49000"), mark=D("49000"))  # сам не виконується
    open_orders = await fake.fetch_open_orders(symbol=SYMBOL)
    assert [(o.exchange_order_id, o.client_order_id, o.status) for o in open_orders] == [
        (order.exchange_order_id, None, ExchangeOrderStatus.NEW)
    ]


@pytest.mark.asyncio
async def test_external_order_with_foreign_host_id_is_visible(fake: FakeExchangeAdapter) -> None:
    foreign = new_client_order_id("zz")
    fake.place_external_order(symbol=SYMBOL, side=Side.SELL, qty=D("0.01"), price=D("70000"), client_order_id=foreign)
    open_orders = await fake.fetch_open_orders()
    assert [o.client_order_id for o in open_orders] == [foreign]
    assert client_order_host_id(open_orders[0].client_order_id) == "zz"
    found = await fake.fetch_order_by_client_id(symbol=SYMBOL, client_order_id=foreign)
    assert found is not None
    assert found.status is ExchangeOrderStatus.NEW


@pytest.mark.asyncio
async def test_external_order_with_unknown_status_is_active(fake: FakeExchangeAdapter) -> None:
    """Статус, якого адаптер не знає: ордер видно серед активних (він ще може виконатися) — для тестів T3.1/T4.4."""
    client_order_id = new_client_order_id("aa")
    order = fake.place_external_order(
        symbol=SYMBOL,
        side=Side.BUY,
        qty=D("0.01"),
        price=D("50000"),
        client_order_id=client_order_id,
        status=ExchangeOrderStatus.UNKNOWN,
    )
    assert (order.status, order.filled_qty) == (ExchangeOrderStatus.UNKNOWN, D("0"))
    assert [o.exchange_order_id for o in await fake.fetch_open_orders(symbol=SYMBOL)] == [order.exchange_order_id]
    assert await fake.fetch_order_by_client_id(symbol=SYMBOL, client_order_id=client_order_id) == order


@pytest.mark.parametrize(
    "status",
    [
        ExchangeOrderStatus.PARTIALLY_FILLED,
        ExchangeOrderStatus.FILLED,
        ExchangeOrderStatus.CANCELED,
        ExchangeOrderStatus.REJECTED,
        ExchangeOrderStatus.DEACTIVATED,
        ExchangeOrderStatus.TRIGGERED,
    ],
)
def test_external_order_status_override_is_limited_to_resting_statuses(
    fake: FakeExchangeAdapter, status: ExchangeOrderStatus
) -> None:
    with pytest.raises(ValueError, match=r".+"):
        fake.place_external_order(symbol=SYMBOL, side=Side.BUY, qty=D("0.01"), price=D("50000"), status=status)


@pytest.mark.asyncio
async def test_external_conditional_order_is_untriggered(fake: FakeExchangeAdapter) -> None:
    order = fake.place_external_order(
        symbol=SYMBOL,
        side=Side.SELL,
        qty=D("0.01"),
        price=D("55000"),
        client_order_id="manual-sl-1",
        stop_order_type=StopOrderType.STOP_LOSS,
        trigger_price=D("55000"),
    )
    assert order.status is ExchangeOrderStatus.UNTRIGGERED
    open_orders = await fake.fetch_open_orders(symbol=SYMBOL)
    assert [(o.client_order_id, o.status, o.stop_order_type, o.trigger_price) for o in open_orders] == [
        ("manual-sl-1", ExchangeOrderStatus.UNTRIGGERED, StopOrderType.STOP_LOSS, D("55000"))
    ]
    assert client_order_host_id(open_orders[0].client_order_id) is None


@pytest.mark.asyncio
async def test_external_position_open_and_close(fake: FakeExchangeAdapter) -> None:
    fake.open_external_position(symbol=SYMBOL, side=PositionSide.LONG, qty=D("0.01"))
    position = await _position(fake)
    assert (position.side, position.qty, position.entry_price, position.stop_loss) == (
        PositionSide.LONG,
        D("0.01"),
        D("60000"),
        None,
    )
    fake.close_position_externally(SYMBOL)
    assert await fake.fetch_positions() == []
    fills = await fake.fetch_executions(start_ms=0)
    assert [(f.side, f.qty, f.client_order_id) for f in fills] == [
        (Side.BUY, D("0.01"), None),
        (Side.SELL, D("0.01"), None),
    ]


# ================================================================ зсув часу біржі


@pytest.mark.asyncio
async def test_server_time_offset_shifts_exchange_time_only(fake_clock: FakeClock) -> None:
    fake = make_fake(server_time_offset_ms=-2_500)
    assert await fake.fetch_time() == FAKE_NOW_MS - 2_500
    fake_clock.advance(1_000)
    assert await fake.fetch_time() == FAKE_NOW_MS - 1_500

    request = market(Side.BUY, "0.01")
    await fake.place_order(request)
    order = await _order(fake, request.client_order_id)
    assert order.created_at_ms == FAKE_NOW_MS - 1_500
    fills = await fake.fetch_executions(start_ms=0)
    assert [f.ts_ms for f in fills] == [FAKE_NOW_MS - 1_500]
    # Журнал викликів — у локальному часі.
    assert fake.calls_to("place_order")[0].ts_ms == FAKE_NOW_MS + 1_000


# ================================================================ приватний потік (WS)


def _fills(events: list[PrivateStreamEvent]) -> list[FillUpdate]:
    return [e for e in events if isinstance(e, FillUpdate)]


@pytest.mark.asyncio
async def test_stream_starts_with_stream_connected(fake: FakeExchangeAdapter) -> None:
    stream = fake.subscribe_private_stream()
    first = await anext(stream)
    assert first == StreamConnected(connected_at_ms=FAKE_NOW_MS)
    assert fake.stream_subscribers == 1
    assert fake.pending_stream_events() == [0]


@pytest.mark.asyncio
async def test_stream_events_after_market_entry(fake: FakeExchangeAdapter) -> None:
    stream = await _subscribe(fake)
    request = market(Side.BUY, "0.01")
    await fake.place_order(request)
    events = await drain(stream, fake.pending_stream_events()[0])
    assert fake.pending_stream_events() == [0]
    assert {type(e) for e in events} == {FillUpdate, OrderUpdate, PositionUpdate, BalanceUpdate}

    fills = _fills(events)
    assert len(fills) == 1
    rest_fills = await fake.fetch_executions(start_ms=0)
    assert fills[0].fill == rest_fills[0]
    assert (fills[0].fill.client_order_id, fills[0].fill.qty, fills[0].fill.price) == (
        request.client_order_id,
        D("0.01"),
        D("60000"),
    )

    orders = [e.order for e in events if isinstance(e, OrderUpdate)]
    assert orders[-1].client_order_id == request.client_order_id
    assert (orders[-1].status, orders[-1].filled_qty) == (ExchangeOrderStatus.FILLED, D("0.01"))

    positions = [e for e in events if isinstance(e, PositionUpdate)]
    assert positions[-1].symbol == SYMBOL
    assert positions[-1].position is not None
    assert (positions[-1].position.side, positions[-1].position.qty) == (PositionSide.LONG, D("0.01"))

    balances = [e.balance for e in events if isinstance(e, BalanceUpdate)]
    assert balances[-1].wallet_balance == D("9999.67")


@pytest.mark.asyncio
async def test_stream_reports_position_closed_by_stop_loss(fake: FakeExchangeAdapter) -> None:
    await _open(fake, Side.BUY, stop_loss="58000")
    stream = await _subscribe(fake)
    fake.set_price(SYMBOL, last=D("58000"), mark=D("58000"))
    events = await drain(stream, fake.pending_stream_events()[0])
    closed = [e for e in events if isinstance(e, PositionUpdate)]
    assert closed[-1] == PositionUpdate(symbol=SYMBOL, position=None)
    fills = _fills(events)
    assert [(f.fill.side, f.fill.client_order_id) for f in fills] == [(Side.SELL, None)]


@pytest.mark.asyncio
async def test_stream_receives_events_of_applied_but_failed_place_order(fake: FakeExchangeAdapter) -> None:
    """Fill з WS може прийти, хоча `place_order` упав таймаутом (ТЗ §7.2: перевірка приватного потоку)."""
    stream = await _subscribe(fake)
    request = market(Side.BUY, "0.01")
    fake.fail_next("place_order", Fault.timeout(applied=True))
    with pytest.raises(ExchangeTimeout):
        await fake.place_order(request)
    events = await drain(stream, fake.pending_stream_events()[0])
    assert [f.fill.client_order_id for f in _fills(events)] == [request.client_order_id]


async def _baseline_event_count(fake: FakeExchangeAdapter, stream: AsyncIterator[PrivateStreamEvent]) -> int:
    """Скільки подій дає MARKET BUY 0.01 (без збоїв); потік після цього порожній."""
    await fake.place_order(market(Side.BUY, "0.01"))
    count = fake.pending_stream_events()[0]
    assert count >= 4
    await drain(stream, count)
    return count


@pytest.mark.asyncio
async def test_ws_duplicate_next_delivers_same_exec_id_twice(fake: FakeExchangeAdapter) -> None:
    stream = await _subscribe(fake)
    k = await _baseline_event_count(fake, stream)
    fake.ws_duplicate_next(k)
    request = market(Side.BUY, "0.01")
    await fake.place_order(request)
    assert fake.pending_stream_events() == [2 * k]
    events = await drain(stream, 2 * k)
    exec_ids = [f.fill.exec_id for f in _fills(events)]
    assert len(exec_ids) == 2
    assert exec_ids[0] == exec_ids[1]
    rest = [f for f in await fake.fetch_executions(start_ms=0) if f.client_order_id == request.client_order_id]
    assert [f.exec_id for f in rest] == [exec_ids[0]]  # на біржі — одне виконання


@pytest.mark.asyncio
async def test_ws_drop_next_loses_event_but_rest_has_it(fake: FakeExchangeAdapter) -> None:
    stream = await _subscribe(fake)
    k = await _baseline_event_count(fake, stream)
    fake.ws_drop_next(1)
    await fake.place_order(market(Side.BUY, "0.01"))
    assert fake.pending_stream_events() == [k - 1]
    await drain(stream, k - 1)

    fake.ws_drop_next(k)
    lost = market(Side.BUY, "0.01")
    await fake.place_order(lost)
    assert fake.pending_stream_events() == [0]
    rest = await fake.fetch_executions(start_ms=0)
    assert [f.client_order_id for f in rest][-1] == lost.client_order_id
    assert (await _position(fake)).qty == D("0.03")


@pytest.mark.asyncio
async def test_ws_disconnect_delivers_queued_then_raises_and_rest_recovers(fake: FakeExchangeAdapter) -> None:
    stream = await _subscribe(fake)
    first = market(Side.BUY, "0.01")
    await fake.place_order(first)
    queued = fake.pending_stream_events()[0]
    assert queued >= 4

    fake.ws_disconnect()
    delivered = await drain(stream, queued)
    assert [f.fill.client_order_id for f in _fills(delivered)] == [first.client_order_id]
    with pytest.raises(PrivateStreamLost):
        await anext(stream)
    assert fake.stream_subscribers == 0

    # Подія без підписника губиться.
    second = market(Side.BUY, "0.01")
    await fake.place_order(second)

    reconnected = await _subscribe(fake)
    assert fake.pending_stream_events() == [0]
    # Пропущене — лише через REST (ТЗ §11.2).
    rest = await fake.fetch_executions(start_ms=0)
    assert [f.client_order_id for f in rest] == [first.client_order_id, second.client_order_id]
    assert (await _position(fake)).qty == D("0.02")

    # Нова підписка отримує нові події.
    third = market(Side.BUY, "0.01")
    await fake.place_order(third)
    events = await drain(reconnected, fake.pending_stream_events()[0])
    assert [f.fill.client_order_id for f in _fills(events)] == [third.client_order_id]


@pytest.mark.asyncio
async def test_events_without_subscriber_are_lost(fake: FakeExchangeAdapter) -> None:
    request = market(Side.BUY, "0.01")
    await fake.place_order(request)
    stream = fake.subscribe_private_stream()
    assert type(await anext(stream)) is StreamConnected
    assert fake.pending_stream_events() == [0]
    assert [f.client_order_id for f in await fake.fetch_executions(start_ms=0)] == [request.client_order_id]
    assert (await _position(fake)).qty == D("0.01")


@pytest.mark.asyncio
async def test_two_subscribers_receive_same_events(fake: FakeExchangeAdapter) -> None:
    first = await _subscribe(fake)
    second = await _subscribe(fake)
    assert fake.stream_subscribers == 2
    await fake.place_order(market(Side.BUY, "0.01"))
    pending = fake.pending_stream_events()
    assert len(pending) == 2
    assert pending[0] == pending[1]
    assert pending[0] >= 4
    events_first = await drain(first, pending[0])
    events_second = await drain(second, pending[1])
    assert events_first == events_second
    assert fake.pending_stream_events() == [0, 0]


# ================================================================ property: позиція = знакова сума fills

_trade = st.tuples(
    st.sampled_from([Side.BUY, Side.SELL]),
    st.integers(min_value=1, max_value=50),  # кроки qty_step (0.001)
    st.integers(min_value=500_000, max_value=700_000),  # ціна в тиках (0.1) → 50000.0 … 70000.0
)


async def _run_trades(trades: list[tuple[Side, int, int]]) -> None:
    fake = make_fake(wallet_balance="10000000")
    requests: list[PlaceOrderRequest] = []
    for side, steps, ticks in trades:
        price = D(str(ticks)) * D("0.1")
        fake.set_price(SYMBOL, last=price, mark=price)
        request = market(side, str(D(str(steps)) * D("0.001")))
        await fake.place_order(request)
        requests.append(request)

    fills = await fake.fetch_executions(start_ms=0, symbol=SYMBOL)
    signed = sum((f.qty if f.side is Side.BUY else -f.qty for f in fills), D("0"))
    positions = await fake.fetch_positions(symbols=[SYMBOL])
    if signed == 0:
        assert positions == []
    else:
        assert len(positions) == 1
        expected_side = PositionSide.LONG if signed > 0 else PositionSide.SHORT
        assert (positions[0].side, positions[0].qty) == (expected_side, abs(signed))

    for request in requests:
        order = await fake.fetch_order_by_client_id(symbol=SYMBOL, client_order_id=request.client_order_id)
        assert order is not None
        own = [f for f in fills if f.exchange_order_id == order.exchange_order_id]
        assert sum((f.qty for f in own), D("0")) == order.filled_qty == request.qty
        assert order.status is ExchangeOrderStatus.FILLED


@settings(max_examples=50, deadline=None)
@given(trades=st.lists(_trade, min_size=1, max_size=15))
def test_position_equals_signed_sum_of_fills(trades: list[tuple[Side, int, int]]) -> None:
    asyncio.run(_run_trades(trades))
