"""Contract-тести `ExchangeAdapter` (ТЗ §3, T1.1): один набір для фейку, Bybit Testnet і Demo.

Використовуються **лише методи протоколу** (`app/exchange/adapter.py`) — жодних методів, специфічних для фейку,
жодних знань про конкретні ціни. Підключення: модуль тестів визначає фікстури `adapter: ExchangeAdapter` і
`contract_market: ContractMarket` та клас-нащадок з префіксом `Test`:

    class TestFakeExchangeContract(ExchangeAdapterContract): ...      # tests/exchange/test_contract_fake.py
    @pytest.mark.testnet
    class TestBybitTestnetContract(ExchangeAdapterContract): ...      # T1.3 / T3.2

Ціна «далекого» LIMIT-ордера рахується від останньої закритої свічки (×0.5, вниз до tick), розмір — від правил
інструмента, тож на Testnet/Demo ордер не виконується й не впирається в мін. номінал. Кожен тест, що ставить ордер,
прибирає його за собою (`_cancel_quietly`), навіть якщо впав посередині.

Цикл позиції (`test_position_round_trip_with_protection`) відкриває мінімальну позицію MARKET-ордером з нативними
SL/TP і закриває її reduceOnly. Передумова — за символом немає позиції; якщо вона є (бот Demo, ручна), тест падає до
будь-якого ордера й чужу позицію не чіпає (ТЗ §11.3). При падінні після входу закривається лише власний вхід
(`_close_own_long_quietly`). Набір ніколи не запускається проти live: `ContractMarket.mode` — лише
`fake`/`testnet`/`demo`, а модуль підключення до мережі (T1.3/T3.2) додатково перевіряє адреси самого адаптера.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Literal

import pytest

from app.core.clock import now_ms
from app.exchange.adapter import ExchangeAdapter
from app.exchange.errors import DuplicateClientOrderId, ExchangeError, OrderNotActive
from app.exchange.rate_limit import RequestPriority
from app.exchange.types import (
    Balance,
    ExchangeOrderStatus,
    Instrument,
    MarginMode,
    OrderType,
    PlaceOrderRequest,
    Position,
    PositionProtection,
    PositionSide,
    Side,
    StreamConnected,
    Timeframe,
    new_client_order_id,
)

ContractMode = Literal["fake", "testnet", "demo"]
_CONTRACT_MODES: frozenset[str] = frozenset({"fake", "testnet", "demo"})


@dataclass(frozen=True)
class ContractMarket:
    """Параметри ринку для contract-набору. `mode` — де запускається набір: лише `fake`/`testnet`/`demo` (він
    відкриває позиції). `max_time_skew_ms` — допустима різниця `fetch_time` з локальним часом (фейк — 0 + запас;
    мережа — з урахуванням RTT)."""

    symbol: str
    host_id: str
    mode: ContractMode
    timeframe: Timeframe = Timeframe.H1
    history_bars: int = 5
    max_time_skew_ms: int = 1_000

    def __post_init__(self) -> None:
        if self.mode not in _CONTRACT_MODES:
            raise ValueError("contract-набір відкриває позиції: лише fake/testnet/demo, ніколи live")


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


async def _instrument(adapter: ExchangeAdapter, symbol: str) -> Instrument:
    found = [i for i in await adapter.fetch_instruments(symbols=[symbol]) if i.symbol == symbol]
    assert len(found) == 1, f"інструмент {symbol} не знайдено"
    return found[0]


async def _far_limit_buy(adapter: ExchangeAdapter, market: ContractMarket) -> PlaceOrderRequest:
    """LIMIT BUY за ½ ціни останньої закритої свічки: гарантовано не маркетабельний."""
    instrument = await _instrument(adapter, market.symbol)
    price = _floor_to_step(await _last_close(adapter, market) * Decimal("0.5"), instrument.tick_size)
    # 10% запасу над мін. номіналом; не менше min_qty.
    qty = _ceil_to_step(max(instrument.min_qty, instrument.min_notional * Decimal("1.1") / price), instrument.qty_step)
    return PlaceOrderRequest(
        symbol=market.symbol,
        client_order_id=new_client_order_id(market.host_id),
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        qty=qty,
        price=price,
        reduce_only=False,
    )


async def _cancel_quietly(adapter: ExchangeAdapter, symbol: str, client_order_id: str) -> None:
    with contextlib.suppress(OrderNotActive):
        await adapter.cancel_order(symbol=symbol, client_order_id=client_order_id, priority=RequestPriority.RECONCILE)


async def _last_close(adapter: ExchangeAdapter, market: ContractMarket) -> Decimal:
    tf_ms = market.timeframe.duration_ms
    candles = await adapter.fetch_candles(
        symbol=market.symbol, timeframe=market.timeframe, start_ms=now_ms() - market.history_bars * tf_ms
    )
    assert candles != [], "немає закритих свічок для розрахунку ціни"
    return candles[-1].close


async def _close_own_long_quietly(adapter: ExchangeAdapter, market: ContractMarket, qty: Decimal) -> None:
    """Прибирання після входу тесту: закрити LONG не більше за власний вхід `qty` (до входу позиції не було —
    передумова), reduceOnly. Чужу позицію чи інший бік не чіпає; помилки біржі не маскують падіння тесту."""
    with contextlib.suppress(ExchangeError):
        for position in await adapter.fetch_positions(symbols=[market.symbol], priority=RequestPriority.RECONCILE):
            if position.side is not PositionSide.LONG:
                continue
            await adapter.place_order(
                PlaceOrderRequest(
                    symbol=market.symbol,
                    client_order_id=new_client_order_id(market.host_id),
                    side=Side.SELL,
                    order_type=OrderType.MARKET,
                    qty=min(position.qty, qty),
                    reduce_only=True,
                ),
                priority=RequestPriority.RECONCILE,
            )


class ExchangeAdapterContract:
    """Набір contract-тестів. Без префікса `Test`, щоб pytest не збирав базовий клас сам по собі."""

    # ------------------------------------------------------------ час і ринкові дані

    @pytest.mark.asyncio
    async def test_fetch_time_is_close_to_local_clock(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        local = now_ms()
        server = await adapter.fetch_time()
        assert type(server) is int
        assert abs(server - local) <= contract_market.max_time_skew_ms

    @pytest.mark.asyncio
    async def test_fetch_instruments_contains_symbol_with_positive_rules(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        instrument = await _instrument(adapter, contract_market.symbol)
        assert instrument.tick_size > 0
        assert instrument.qty_step > 0
        assert instrument.min_qty > 0
        assert instrument.min_notional >= 0
        assert instrument.max_leverage >= 1
        assert instrument.launch_time_ms >= 0
        # min_qty кратний кроку — інакше округлення вниз до кроку дало б кількість, меншу за мінімум.
        assert instrument.min_qty % instrument.qty_step == 0

    @pytest.mark.asyncio
    async def test_fetch_instruments_ignores_unknown_symbol(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        result = await adapter.fetch_instruments(symbols=["NOSUCHCOIN/USDT:USDT"])
        assert result == []

    @pytest.mark.asyncio
    async def test_fetch_candles_closed_ascending_unique_in_range(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        tf_ms = contract_market.timeframe.duration_ms
        now = now_ms()
        start = now - contract_market.history_bars * tf_ms
        candles = await adapter.fetch_candles(
            symbol=contract_market.symbol, timeframe=contract_market.timeframe, start_ms=start
        )
        assert candles != []
        opens = [c.open_time_ms for c in candles]
        assert opens == sorted(set(opens))  # за зростанням, без дублів
        assert all(c.symbol == contract_market.symbol for c in candles)
        assert all(c.timeframe is contract_market.timeframe for c in candles)
        assert all(c.open_time_ms >= start for c in candles)
        # Лише закриті за часом біржі: свічка, що формується, не повертається ніколи.
        server_now = await adapter.fetch_time()
        assert all(c.close_time_ms <= server_now for c in candles)
        assert all(c.open_time_ms % tf_ms == 0 for c in candles)

    @pytest.mark.asyncio
    async def test_fetch_candles_end_is_exclusive_and_limit_takes_earliest(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        tf_ms = contract_market.timeframe.duration_ms
        start = now_ms() - contract_market.history_bars * tf_ms
        full = await adapter.fetch_candles(
            symbol=contract_market.symbol, timeframe=contract_market.timeframe, start_ms=start
        )
        assert len(full) >= 3
        end = full[2].open_time_ms
        bounded = await adapter.fetch_candles(
            symbol=contract_market.symbol, timeframe=contract_market.timeframe, start_ms=start, end_ms=end
        )
        assert bounded == full[:2]
        limited = await adapter.fetch_candles(
            symbol=contract_market.symbol, timeframe=contract_market.timeframe, start_ms=start, limit=2
        )
        assert limited == full[:2]

    @pytest.mark.asyncio
    async def test_fetch_funding_history_ascending_in_range(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        now = now_ms()
        start = now - 3 * 86_400_000
        rates = await adapter.fetch_funding_history(symbol=contract_market.symbol, start_ms=start, end_ms=now)
        assert rates != [], "за 3 доби funding нараховується щонайменше раз"
        times = [r.funding_time_ms for r in rates]
        assert times == sorted(set(times))
        assert all(start <= t < now for t in times)
        assert all(r.symbol == contract_market.symbol for r in rates)

    # ------------------------------------------------------------ акаунт

    @pytest.mark.asyncio
    async def test_fetch_balance_returns_decimal_fields(self, adapter: ExchangeAdapter) -> None:
        balance = await adapter.fetch_balance(priority=RequestPriority.RECONCILE)
        assert type(balance) is Balance
        assert balance.coin == "USDT"
        for value in (balance.wallet_balance, balance.equity, balance.available_balance, balance.unrealized_pnl):
            assert type(value) is Decimal
            assert value.is_finite() is True

    @pytest.mark.asyncio
    async def test_set_margin_mode_isolated_is_idempotent(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        # Підтримується лише isolated (ТЗ §9.1); повтор того самого режиму — успіх, а не відмова.
        await adapter.set_margin_mode(
            symbol=contract_market.symbol, mode=MarginMode.ISOLATED, priority=RequestPriority.RECONCILE
        )
        await adapter.set_margin_mode(
            symbol=contract_market.symbol, mode=MarginMode.ISOLATED, priority=RequestPriority.RECONCILE
        )

    @pytest.mark.asyncio
    async def test_private_stream_starts_with_stream_connected(self, adapter: ExchangeAdapter) -> None:
        stream = adapter.subscribe_private_stream()
        try:
            first = await anext(stream)
            assert type(first) is StreamConnected
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_position_round_trip_with_protection(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        """Вхід з нативними SL/TP → SL видно на позиції → підтягування SL (і повтор — ідемпотентно) → плече тим самим
        значенням → reduceOnly-вихід → позиції немає, обидва виконання є в `fetch_executions` (ТЗ §9.1, §9.2, §11.2)."""
        symbol = contract_market.symbol
        instrument = await _instrument(adapter, symbol)
        reference = await _last_close(adapter, contract_market)
        qty = _ceil_to_step(
            max(instrument.min_qty, instrument.min_notional * Decimal("1.1") / reference), instrument.qty_step
        )
        stop_loss = _floor_to_step(reference * Decimal("0.8"), instrument.tick_size)
        take_profit = _ceil_to_step(reference * Decimal("1.2"), instrument.tick_size)
        entry = PlaceOrderRequest(
            symbol=symbol,
            client_order_id=new_client_order_id(contract_market.host_id),
            side=Side.BUY,
            order_type=OrderType.MARKET,
            qty=qty,
            reduce_only=False,
            stop_loss=stop_loss,
            take_profit=take_profit,
        )
        exit_ = PlaceOrderRequest(
            symbol=symbol,
            client_order_id=new_client_order_id(contract_market.host_id),
            side=Side.SELL,
            order_type=OrderType.MARKET,
            qty=qty,
            reduce_only=True,
        )
        started_ms = await adapter.fetch_time() - 60_000
        # Передумова — до try: якщо позиція вже є, тест падає, не відправивши жодного ордера.
        assert await adapter.fetch_positions(symbols=[symbol], priority=RequestPriority.RECONCILE) == [], (
            "передумова: позиції за символом немає"
        )
        entered = False
        try:
            entered = True  # до виклику: вхід міг виконатися, навіть якщо відповідь втрачено
            await adapter.place_order(entry, priority=RequestPriority.RECONCILE)
            # TODO(verify-bybit): на Testnet MARKET може бути ще не FILLED одразу після відповіді (T3.2).
            filled = await adapter.fetch_order_by_client_id(
                symbol=symbol, client_order_id=entry.client_order_id, priority=RequestPriority.RECONCILE
            )
            assert filled is not None
            assert (filled.status, filled.filled_qty) == (ExchangeOrderStatus.FILLED, qty)

            [position] = await adapter.fetch_positions(symbols=[symbol], priority=RequestPriority.RECONCILE)
            assert type(position) is Position
            assert (position.symbol, position.side, position.qty) == (symbol, PositionSide.LONG, qty)
            assert (position.stop_loss, position.take_profit) == (stop_loss, take_profit)

            tighter = PositionProtection(
                symbol=symbol,
                position_side=PositionSide.LONG,
                stop_loss=_floor_to_step(reference * Decimal("0.9"), instrument.tick_size),
            )
            await adapter.set_position_protection(tighter)
            await adapter.set_position_protection(tighter)  # той самий SL — успіх (повтор після втраченої відповіді)
            [protected] = await adapter.fetch_positions(symbols=[symbol], priority=RequestPriority.RECONCILE)
            assert (protected.stop_loss, protected.take_profit) == (tighter.stop_loss, take_profit)

            await adapter.set_leverage(
                symbol=symbol, leverage=protected.leverage, priority=RequestPriority.RECONCILE
            )  # те саме значення — успіх

            await adapter.place_order(exit_, priority=RequestPriority.RECONCILE)
            assert await adapter.fetch_positions(symbols=[symbol], priority=RequestPriority.RECONCILE) == []

            executions = await adapter.fetch_executions(
                start_ms=started_ms, symbol=symbol, priority=RequestPriority.RECONCILE
            )
            ours = [f for f in executions if f.client_order_id in (entry.client_order_id, exit_.client_order_id)]
            assert len({f.exec_id for f in ours}) == len(ours)
            by_order = {
                cid: sum((f.qty for f in ours if f.client_order_id == cid), Decimal(0))
                for cid in (entry.client_order_id, exit_.client_order_id)
            }
            assert by_order == {entry.client_order_id: qty, exit_.client_order_id: qty}
            assert [f.ts_ms for f in executions] == sorted(f.ts_ms for f in executions)
        finally:
            if entered:
                await _close_own_long_quietly(adapter, contract_market, qty)

    # ------------------------------------------------------------ життєвий цикл ордера

    @pytest.mark.asyncio
    async def test_far_limit_order_lifecycle(self, adapter: ExchangeAdapter, contract_market: ContractMarket) -> None:
        request = await _far_limit_buy(adapter, contract_market)
        symbol = contract_market.symbol
        try:
            ack = await adapter.place_order(request, priority=RequestPriority.RECONCILE)
            assert ack.client_order_id == request.client_order_id
            assert ack.exchange_order_id != ""

            found = await adapter.fetch_order_by_client_id(
                symbol=symbol, client_order_id=request.client_order_id, priority=RequestPriority.RECONCILE
            )
            assert found is not None
            assert found.status is ExchangeOrderStatus.NEW
            assert found.client_order_id == request.client_order_id
            assert found.exchange_order_id == ack.exchange_order_id
            assert found.symbol == symbol
            assert found.side is Side.BUY
            assert found.order_type is OrderType.LIMIT
            assert found.qty == request.qty
            assert found.price == request.price
            assert found.filled_qty == Decimal("0")
            assert found.avg_fill_price is None
            assert found.reduce_only is False

            open_by_symbol = await adapter.fetch_open_orders(symbol=symbol, priority=RequestPriority.RECONCILE)
            assert [o.exchange_order_id for o in open_by_symbol if o.client_order_id == request.client_order_id] == [
                ack.exchange_order_id
            ]
            open_all = await adapter.fetch_open_orders(priority=RequestPriority.RECONCILE)
            assert [o.exchange_order_id for o in open_all if o.client_order_id == request.client_order_id] == [
                ack.exchange_order_id
            ]

            await adapter.cancel_order(
                symbol=symbol, client_order_id=request.client_order_id, priority=RequestPriority.RECONCILE
            )

            canceled = await adapter.fetch_order_by_client_id(
                symbol=symbol, client_order_id=request.client_order_id, priority=RequestPriority.RECONCILE
            )
            assert canceled is not None
            assert canceled.status is ExchangeOrderStatus.CANCELED
            assert canceled.filled_qty == Decimal("0")
            open_after = await adapter.fetch_open_orders(symbol=symbol, priority=RequestPriority.RECONCILE)
            assert [o for o in open_after if o.client_order_id == request.client_order_id] == []

            with pytest.raises(OrderNotActive):
                await adapter.cancel_order(
                    symbol=symbol, client_order_id=request.client_order_id, priority=RequestPriority.RECONCILE
                )
        finally:
            await _cancel_quietly(adapter, symbol, request.client_order_id)

    @pytest.mark.asyncio
    async def test_cancel_by_exchange_order_id(self, adapter: ExchangeAdapter, contract_market: ContractMarket) -> None:
        request = await _far_limit_buy(adapter, contract_market)
        symbol = contract_market.symbol
        try:
            ack = await adapter.place_order(request, priority=RequestPriority.RECONCILE)
            await adapter.cancel_order(
                symbol=symbol, exchange_order_id=ack.exchange_order_id, priority=RequestPriority.RECONCILE
            )
            canceled = await adapter.fetch_order_by_client_id(
                symbol=symbol, client_order_id=request.client_order_id, priority=RequestPriority.RECONCILE
            )
            assert canceled is not None
            assert canceled.status is ExchangeOrderStatus.CANCELED
        finally:
            await _cancel_quietly(adapter, symbol, request.client_order_id)

    @pytest.mark.asyncio
    async def test_unknown_client_order_id_returns_none(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        unknown = new_client_order_id(contract_market.host_id)
        result = await adapter.fetch_order_by_client_id(
            symbol=contract_market.symbol, client_order_id=unknown, priority=RequestPriority.RECONCILE
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_duplicate_client_order_id_is_not_a_rejection(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        request = await _far_limit_buy(adapter, contract_market)
        symbol = contract_market.symbol
        try:
            first = await adapter.place_order(request, priority=RequestPriority.RECONCILE)
            with pytest.raises(DuplicateClientOrderId) as exc:
                await adapter.place_order(request, priority=RequestPriority.RECONCILE)
            assert exc.value.client_order_id == request.client_order_id
            # Другий ордер не створено: за id — той самий, перший.
            open_orders = await adapter.fetch_open_orders(symbol=symbol, priority=RequestPriority.RECONCILE)
            assert [o.exchange_order_id for o in open_orders if o.client_order_id == request.client_order_id] == [
                first.exchange_order_id
            ]
        finally:
            await _cancel_quietly(adapter, symbol, request.client_order_id)

    @pytest.mark.asyncio
    async def test_cancel_requires_exactly_one_identifier(
        self, adapter: ExchangeAdapter, contract_market: ContractMarket
    ) -> None:
        request = await _far_limit_buy(adapter, contract_market)
        symbol = contract_market.symbol
        try:
            ack = await adapter.place_order(request, priority=RequestPriority.RECONCILE)
            with pytest.raises(ValueError, match=r".+"):
                await adapter.cancel_order(symbol=symbol, priority=RequestPriority.RECONCILE)
            with pytest.raises(ValueError, match=r".+"):
                await adapter.cancel_order(
                    symbol=symbol,
                    client_order_id=request.client_order_id,
                    exchange_order_id=ack.exchange_order_id,
                    priority=RequestPriority.RECONCILE,
                )
            # Жодне з невалідних скасувань не зачепило ордер.
            still = await adapter.fetch_order_by_client_id(
                symbol=symbol, client_order_id=request.client_order_id, priority=RequestPriority.RECONCILE
            )
            assert still is not None
            assert still.status is ExchangeOrderStatus.NEW
        finally:
            await _cancel_quietly(adapter, symbol, request.client_order_id)
