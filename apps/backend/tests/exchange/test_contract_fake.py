"""Contract-набір `ExchangeAdapter` проти `FakeExchangeAdapter` (T1.1) + точні перевірки ринкових даних фейку.

Сам набір — `tests/exchange/contract.py` (лише методи протоколу); T1.3/T3.2 підключать його до Bybit Testnet/Demo.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.core.clock import FakeClock
from app.exchange.adapter import ExchangeAdapter
from app.exchange.fake import FakeExchangeAdapter
from app.exchange.rate_limit import FIXED_METHOD_PRIORITY, RequestPriority
from app.exchange.types import Candle, FundingRate, PositionSide, Side, Timeframe
from tests.exchange.contract import ContractMarket, ExchangeAdapterContract
from tests.exchange.factories import FAKE_NOW_MS, H1_MS, INSTRUMENT, SYMBOL, hourly_candle, market

HISTORY_HOURS = 6
EIGHT_HOURS_MS = 8 * H1_MS


def _as_adapter(fake: FakeExchangeAdapter) -> ExchangeAdapter:
    """Статична перевірка: mypy (проганяється й по tests/) підтверджує, що фейк виконує протокол."""
    return fake


def _closed_candles() -> list[Candle]:
    # Від найстарішої до найновішої закритої: open = now − k·1h, k = 6..1; остання закривається рівно в now.
    return [hourly_candle(FAKE_NOW_MS - k * H1_MS, str(60_000 + k * 10)) for k in range(HISTORY_HOURS, 0, -1)]


def _forming_candle() -> Candle:
    return hourly_candle(FAKE_NOW_MS, "60005")


def _funding() -> list[FundingRate]:
    return [
        FundingRate(symbol=SYMBOL, funding_time_ms=FAKE_NOW_MS - k * EIGHT_HOURS_MS, rate=Decimal(f"0.000{k}"))
        for k in (3, 2, 1)
    ]


@pytest.fixture
def fake() -> FakeExchangeAdapter:
    fake = FakeExchangeAdapter()
    fake.add_instrument(INSTRUMENT, last_price=Decimal("60000"))
    # Свічки свідомо додаються не по порядку: фейк сам сортує.
    fake.add_candles([_forming_candle(), *reversed(_closed_candles())])
    fake.add_funding_rates(_funding())
    return fake


@pytest.fixture
def adapter(fake: FakeExchangeAdapter) -> ExchangeAdapter:
    return _as_adapter(fake)


@pytest.fixture
def contract_market() -> ContractMarket:
    return ContractMarket(symbol=SYMBOL, host_id="ct", mode="fake", timeframe=Timeframe.H1, history_bars=HISTORY_HOURS)


class TestFakeExchangeContract(ExchangeAdapterContract):
    """Contract-набір на фейку."""


# ---------------------------------------------------------------- безпека самого contract-набору


@pytest.mark.asyncio
async def test_round_trip_never_touches_existing_position(
    fake: FakeExchangeAdapter, contract_market: ContractMarket
) -> None:
    """На Demo/Testnet за символом може бути позиція бота або ручна: набір падає на передумові й не закриває її
    (ТЗ §11.3 — невідома позиція «не чіпати»)."""
    fake.open_external_position(symbol=SYMBOL, side=PositionSide.SHORT, qty=Decimal("0.05"))
    orders_before = len(fake.all_orders())
    with pytest.raises(AssertionError, match="передумова"):
        await ExchangeAdapterContract().test_position_round_trip_with_protection(_as_adapter(fake), contract_market)
    [position] = await fake.fetch_positions(priority=RequestPriority.RECONCILE)
    assert (position.side, position.qty) == (PositionSide.SHORT, Decimal("0.05"))
    assert len(fake.all_orders()) == orders_before
    assert fake.calls_to("place_order") == []


@pytest.mark.parametrize("mode", ["live", "mainnet", "LIVE", ""])
def test_contract_market_refuses_live_and_unknown_modes(mode: str) -> None:
    """Набір відкриває реальну MARKET-позицію: проти live-акаунта він не запускається ніколи (AGENTS.md)."""
    with pytest.raises(ValueError, match="live"):
        ContractMarket(symbol=SYMBOL, host_id="ct", mode=mode)  # type: ignore[arg-type]


# ---------------------------------------------------------------- точні значення, відомі лише для фейку


@pytest.mark.asyncio
async def test_fake_fetch_time_equals_local_clock(fake: FakeExchangeAdapter, fake_clock: FakeClock) -> None:
    assert await fake.fetch_time() == FAKE_NOW_MS
    fake_clock.advance(1_234)
    assert await fake.fetch_time() == FAKE_NOW_MS + 1_234


@pytest.mark.asyncio
async def test_fake_fetch_instruments_returns_added_instrument(fake: FakeExchangeAdapter) -> None:
    assert await fake.fetch_instruments() == [INSTRUMENT]
    assert await fake.fetch_instruments(symbols=[SYMBOL]) == [INSTRUMENT]


def test_fake_add_instrument_twice_is_an_error(fake: FakeExchangeAdapter) -> None:
    with pytest.raises(ValueError, match=r".+"):
        fake.add_instrument(INSTRUMENT, last_price=Decimal("1"))


@pytest.mark.asyncio
async def test_fake_candles_exclude_forming_candle(fake: FakeExchangeAdapter) -> None:
    candles = await fake.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0)
    assert candles == _closed_candles()


@pytest.mark.asyncio
async def test_fake_candle_becomes_visible_exactly_when_closed(
    fake: FakeExchangeAdapter, fake_clock: FakeClock
) -> None:
    fake_clock.advance(H1_MS - 1)
    before = await fake.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=FAKE_NOW_MS)
    assert before == []
    fake_clock.advance(1)
    after = await fake.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=FAKE_NOW_MS)
    assert after == [_forming_candle()]


@pytest.mark.asyncio
async def test_fake_candles_range_and_limit(fake: FakeExchangeAdapter) -> None:
    closed = _closed_candles()
    start = closed[1].open_time_ms
    end = closed[4].open_time_ms
    ranged = await fake.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=start, end_ms=end)
    assert ranged == closed[1:4]
    limited = await fake.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=start, limit=2)
    assert limited == closed[1:3]
    other_tf = await fake.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.M1, start_ms=0)
    assert other_tf == []


@pytest.mark.asyncio
async def test_fake_add_candles_replaces_same_key(fake: FakeExchangeAdapter) -> None:
    oldest = _closed_candles()[0]
    replacement = hourly_candle(oldest.open_time_ms, "70000")
    fake.add_candles([replacement])
    candles = await fake.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0)
    assert len(candles) == HISTORY_HOURS
    assert candles[0] == replacement


@pytest.mark.asyncio
async def test_fake_funding_history_range(fake: FakeExchangeAdapter) -> None:
    funding = _funding()
    assert await fake.fetch_funding_history(symbol=SYMBOL, start_ms=0) == funding
    middle = await fake.fetch_funding_history(
        symbol=SYMBOL, start_ms=funding[1].funding_time_ms, end_ms=funding[2].funding_time_ms
    )
    assert middle == [funding[1]]


@pytest.mark.asyncio
async def test_fake_records_the_priority_of_each_call(fake: FakeExchangeAdapter) -> None:
    await fake.fetch_time()
    await fake.fetch_instruments()
    await fake.fetch_candles(symbol=SYMBOL, timeframe=Timeframe.H1, start_ms=0)
    await fake.fetch_funding_history(symbol=SYMBOL, start_ms=0)
    for method in ("fetch_time", "fetch_instruments", "fetch_candles", "fetch_funding_history"):
        assert fake.calls_to(method)[-1].priority is FIXED_METHOD_PRIORITY[method]

    request = market(Side.BUY, "0.001")
    await fake.place_order(request, priority=RequestPriority.PROTECTION)
    recorded = fake.calls_to("place_order")[-1]
    assert recorded.priority is RequestPriority.PROTECTION
    assert request in recorded.args.values()
    assert "priority" not in recorded.args

    await fake.fetch_balance(priority=RequestPriority.DATA)
    assert fake.calls_to("fetch_balance")[-1].priority is RequestPriority.DATA

    stream = fake.subscribe_private_stream()
    await anext(stream)
    await stream.aclose()
    assert fake.calls_to("subscribe_private_stream")[-1].priority is None

    with pytest.raises(TypeError):
        await fake.place_order(request)  # type: ignore[call-arg]
