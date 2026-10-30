"""Спільні фабрики для тестів біржового шару (T1.1): інструмент, свічки, запити, фейкова біржа, читання WS-черги.

Усі `Decimal` — з рядків; час — від `FAKE_NOW_MS` (autouse-фікстура `fake_clock`, tests/conftest.py).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal

from app.exchange.fake import FakeExchangeAdapter
from app.exchange.types import (
    Candle,
    Instrument,
    OrderType,
    PlaceOrderRequest,
    PrivateStreamEvent,
    Side,
    Timeframe,
    TimeInForce,
    new_client_order_id,
)

# 2026-01-01T00:00:00Z — як у tests/conftest.py; кратне всім таймфреймам (включно з 1d).
FAKE_NOW_MS = 1_767_225_600_000
H1_MS = 3_600_000

SYMBOL = "BTC/USDT:USDT"
OTHER_SYMBOL = "ETH/USDT:USDT"
HOST_ID = "aa"
LAST_PRICE = Decimal("60000")
# Довільний час лістингу, кратний 1h (2020-03-25T08:00:00Z). Не звірене з біржею значення — лише для тестів.
LAUNCH_MS = 1_585_123_200_000

# Валідний id нашого формату (36 символів) для тестів типів, де генератор не потрібен.
VALID_CLIENT_ORDER_ID = "tbaa" + "0123456789abcdef" * 2

INSTRUMENT = Instrument(
    symbol=SYMBOL,
    tick_size=Decimal("0.1"),
    qty_step=Decimal("0.001"),
    min_qty=Decimal("0.001"),
    min_notional=Decimal("5"),
    max_qty=Decimal("100"),
    max_leverage=Decimal("100"),
    launch_time_ms=LAUNCH_MS,
    status="Trading",
)

OTHER_INSTRUMENT = Instrument(
    symbol=OTHER_SYMBOL,
    tick_size=Decimal("0.01"),
    qty_step=Decimal("0.01"),
    min_qty=Decimal("0.01"),
    min_notional=Decimal("5"),
    max_qty=Decimal("1000"),
    max_leverage=Decimal("50"),
    launch_time_ms=LAUNCH_MS,
    status="Trading",
)


def cid() -> str:
    """Новий `clientOrderId` нашого `HOST_ID`."""
    return new_client_order_id(HOST_ID)


def make_fake(*, wallet_balance: str = "10000", server_time_offset_ms: int = 0) -> FakeExchangeAdapter:
    """Фейкова біржа з BTC (last = mark = 60000) і ETH (last = mark = 3000)."""
    fake = FakeExchangeAdapter(wallet_balance=Decimal(wallet_balance), server_time_offset_ms=server_time_offset_ms)
    fake.add_instrument(INSTRUMENT, last_price=LAST_PRICE)
    fake.add_instrument(OTHER_INSTRUMENT, last_price=Decimal("3000"))
    return fake


def market(
    side: Side,
    qty: str,
    *,
    reduce_only: bool = False,
    stop_loss: str | None = None,
    take_profit: str | None = None,
    client_order_id: str | None = None,
    symbol: str = SYMBOL,
) -> PlaceOrderRequest:
    return PlaceOrderRequest(
        symbol=symbol,
        client_order_id=client_order_id if client_order_id is not None else cid(),
        side=side,
        order_type=OrderType.MARKET,
        qty=Decimal(qty),
        reduce_only=reduce_only,
        stop_loss=Decimal(stop_loss) if stop_loss is not None else None,
        take_profit=Decimal(take_profit) if take_profit is not None else None,
    )


def limit(
    side: Side,
    qty: str,
    price: str,
    *,
    time_in_force: TimeInForce | None = None,
    reduce_only: bool = False,
    stop_loss: str | None = None,
    take_profit: str | None = None,
    client_order_id: str | None = None,
    symbol: str = SYMBOL,
) -> PlaceOrderRequest:
    return PlaceOrderRequest(
        symbol=symbol,
        client_order_id=client_order_id if client_order_id is not None else cid(),
        side=side,
        order_type=OrderType.LIMIT,
        qty=Decimal(qty),
        price=Decimal(price),
        time_in_force=time_in_force,
        reduce_only=reduce_only,
        stop_loss=Decimal(stop_loss) if stop_loss is not None else None,
        take_profit=Decimal(take_profit) if take_profit is not None else None,
    )


def hourly_candle(open_time_ms: int, close: str, *, symbol: str = SYMBOL) -> Candle:
    """Годинна свічка з валідним OHLC навколо `close` (open = close − 10, high = close + 20, low = close − 30)."""
    c = Decimal(close)
    return Candle(
        symbol=symbol,
        timeframe=Timeframe.H1,
        open_time_ms=open_time_ms,
        open=c - Decimal("10"),
        high=c + Decimal("20"),
        low=c - Decimal("30"),
        close=c,
        volume=Decimal("12.5"),
    )


async def drain(stream: AsyncIterator[PrivateStreamEvent], count: int) -> list[PrivateStreamEvent]:
    """Забрати рівно `count` подій, що вже лежать у черзі (без очікування часу: дія → `anext`)."""
    return [await anext(stream) for _ in range(count)]
