"""Публічні виклики Bybit Testnet (T1.3). Ключі не потрібні: лише market data.

Запуск: `uv run pytest -q -m testnet tests/exchange/test_bybit_testnet.py`
Ніколи не mainnet. Хост перевіряється до першого запиту.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from decimal import Decimal

import pytest
import pytest_asyncio

from app.core.clock import SystemClock, now_ms, use_clock
from app.exchange.bybit import KLINE_FINALITY_MARGIN_MS, BybitAdapter, ExchangeMode
from app.exchange.rate_limit import ProcessRole, RateLimitConfig, RateLimiter
from app.exchange.types import Timeframe

_SYMBOL = "BTC/USDT:USDT"
_SKEW_MS = 5_000
# Похибка оцінки часу біржі однією `fetch_time` (половина RTT) — щоб запит точно прийшов після запасу.
_EDGE_BUFFER_MS = 300
_RECHECK_S = 20


@pytest.fixture(autouse=True)
def fake_clock() -> Iterator[None]:
    """Порівняння `fetch_time` з реальним годинником (контракт T1.1). Фейковий час тут заважав би."""
    with use_clock(SystemClock()):
        yield


@pytest_asyncio.fixture
async def testnet_adapter() -> AsyncIterator[BybitAdapter]:
    limiter = RateLimiter(RateLimitConfig(role=ProcessRole.COLLECTOR, share_pct=20))
    adapter = BybitAdapter(mode=ExchangeMode.TESTNET, limiter=limiter)
    origin = adapter.public_origin
    if "testnet" not in origin or "demo" in origin:
        await adapter.aclose()
        await limiter.aclose()
        pytest.fail(f"публічний хост не testnet: {origin}")
    try:
        yield adapter
    finally:
        await adapter.aclose()
        await limiter.aclose()


@pytest.mark.testnet
@pytest.mark.asyncio
async def test_public_market_data_on_testnet(testnet_adapter: BybitAdapter) -> None:
    adapter = testnet_adapter
    server = await adapter.fetch_time()
    assert type(server) is int
    assert abs(server - now_ms()) <= _SKEW_MS

    instruments = await adapter.fetch_instruments(symbols=[_SYMBOL, "NOSUCHCOIN/USDT:USDT"])
    assert [item.symbol for item in instruments] == [_SYMBOL]
    instrument = instruments[0]
    assert instrument.tick_size > 0
    assert instrument.qty_step > 0
    assert instrument.min_qty > 0
    assert instrument.min_qty % instrument.qty_step == 0
    assert instrument.min_notional >= 0
    assert instrument.max_qty is not None
    assert instrument.max_qty >= instrument.min_qty
    assert instrument.max_leverage >= 1
    assert instrument.launch_time_ms >= 0
    assert instrument.status != ""

    all_usdt = await adapter.fetch_instruments()
    symbols = {item.symbol for item in all_usdt}
    assert _SYMBOL in symbols
    assert "BTC/USDC:USDC" not in symbols
    assert all(item.symbol.endswith("/USDT:USDT") for item in all_usdt)
    assert len(all_usdt) > 100

    step = Timeframe.H1.duration_ms
    start = now_ms() - 24 * step
    candles = await adapter.fetch_candles(symbol=_SYMBOL, timeframe=Timeframe.H1, start_ms=start)
    assert len(candles) >= 3
    opens = [candle.open_time_ms for candle in candles]
    assert opens == sorted(set(opens))
    fresh = await adapter.fetch_time()
    assert all(candle.close_time_ms <= fresh for candle in candles)

    end = candles[2].open_time_ms
    bounded = await adapter.fetch_candles(symbol=_SYMBOL, timeframe=Timeframe.H1, start_ms=start, end_ms=end)
    assert [candle.open_time_ms for candle in bounded] == opens[:2]
    limited = await adapter.fetch_candles(symbol=_SYMBOL, timeframe=Timeframe.H1, start_ms=start, limit=2)
    assert [candle.open_time_ms for candle in limited] == opens[:2]

    funding_start = now_ms() - 3 * 86_400_000
    funding_end = now_ms()
    rates = await adapter.fetch_funding_history(symbol=_SYMBOL, start_ms=funding_start, end_ms=funding_end)
    assert rates != []
    times = [row.funding_time_ms for row in rates]
    assert times == sorted(set(times))
    assert all(funding_start <= ts < funding_end for ts in times)
    assert all(type(row.rate) is Decimal for row in rates)


@pytest.mark.testnet
@pytest.mark.slow
@pytest.mark.asyncio
async def test_kline_returned_right_after_the_finality_margin_does_not_change_later(
    testnet_adapter: BybitAdapter,
) -> None:
    """TODO(verify-bybit) остаточності kline: свічка, віддана одразу після запасу, пізніше не змінюється.

    Запас — `KLINE_FINALITY_MARGIN_MS`. Реальне очікування тут неминуче: перевіряється поведінка біржі на межі
    хвилини (до ~85 с). Testnet малоліквідний, тож це перевірка механіки; для mainnet питання лишається відкритим.
    """
    adapter = testnet_adapter
    step = Timeframe.M1.duration_ms
    offset = await adapter.fetch_time() - now_ms()
    boundary = ((now_ms() + offset) // step + 1) * step
    wake = boundary + KLINE_FINALITY_MARGIN_MS + _EDGE_BUFFER_MS
    await asyncio.sleep(max(0, wake - (now_ms() + offset)) / 1000)

    first = await adapter.fetch_candles(
        symbol=_SYMBOL, timeframe=Timeframe.M1, start_ms=boundary - step, end_ms=boundary
    )
    await asyncio.sleep(_RECHECK_S)
    again = await adapter.fetch_candles(
        symbol=_SYMBOL, timeframe=Timeframe.M1, start_ms=boundary - step, end_ms=boundary
    )
    assert [candle.open_time_ms for candle in first] == [boundary - step]
    assert first == again
