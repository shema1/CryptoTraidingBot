"""Агрегація ліквідацій і реконект колектора (T1.10). Події — записаний кадр Bybit, не жива мережа."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from decimal import Decimal
from typing import Any, cast

import pytest
from sqlalchemy import insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from app.core.clock import FakeClock, now_ms
from app.core.config import H1_MS
from app.core.instance_lock import BODY_STOP_TIMEOUT_MS
from app.data.backfill import SessionFactory
from app.data.collector import (
    DB_RETRY_BASE_MS,
    DB_RETRY_CAP_MS,
    FINAL_FLUSH_TIMEOUT_MS,
    collect_with_clock,
    db_retry_delay_ms,
    reconnect_delay_ms,
    run_collector,
    watch_clock,
)
from app.data.liquidations import (
    CLOCK_MARGIN_MS,
    SEAL_MARGIN_MS,
    LiquidationBook,
    SealedHour,
    hour_start_ms,
    save_liquidations,
    sync_liquidation_gaps,
)
from app.data.models import DataGap, LiquidationsHourly
from app.data.time_sync import ClockBlockReason, ClockGuard, ClockState
from app.exchange.errors import ExchangeUnavailable
from app.exchange.liquidations import (
    MAINNET_LINEAR_WS,
    LiquidationEvent,
    parse_liquidation_message,
    read_liquidation_session,
)
from app.exchange.models import Instrument as StoredInstrument
from tests.data.test_time_sync import ScriptedTime
from tests.exchange.factories import FAKE_NOW_MS, SYMBOL
from tests.exchange.test_liquidation_messages import (
    AVAX,
    MAINNET_EVENTS,
    MAINNET_REJECTED,
    MAINNET_SYMBOLS,
    OFFICIAL_SELL,
    PEPE,
    ROSE,
)

_SYMBOLS = {"ROSEUSDT": ROSE}
_PING_MS = H1_MS
ETH = "ETH/USDT:USDT"
# Підписка за запас до межі години (локальний годинник може відставати від біржі до 1 с).
_EARLY_MS = CLOCK_MARGIN_MS
# Запобіжник від зависання тесту (регресія: після `stop` колектор чекав кадру без кінця). Справний код — миттєво.
_STOP_GUARD_S = 5.0


class _Clock:
    """Стан годинника для книги: `trusted` — свіжий вимір і зсув < 1 с; `epoch` — номер стрибка."""

    def __init__(self, *, trusted: bool = True, epoch: int = 0) -> None:
        self.trusted = trusted
        self.epoch = epoch

    def state(self, scope: str | None = None) -> ClockState:
        return ClockState(
            block_reasons=() if self.trusted else (ClockBlockReason.UNVERIFIED,),
            offset_ms=0 if self.trusted else None,
            uncertainty_ms=0 if self.trusted else None,
            drift_bound_ms=0 if self.trusted else None,
            sample_age_ms=0 if self.trusted else None,
            jump_epoch=self.epoch,
            scope=scope,
            reconciled_epoch=0,
        )


def _book(*symbols: str, clock: _Clock | None = None) -> LiquidationBook:
    return LiquidationBook(symbols, clock=clock if clock is not None else _Clock(), seal_margin_ms=SEAL_MARGIN_MS)


def _subscribe(fake_clock: FakeClock, book: LiquidationBook, symbol: str, at_ms: int) -> None:
    fake_clock.set_now(at_ms)
    book.subscribed(symbol, at_ms)


def _sessions(conn: AsyncConnection) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")


def _event(symbol: str, exchange_id: str, side: str, ts: int, qty: str, price: str) -> str:
    return json.dumps(
        {
            "topic": f"allLiquidation.{exchange_id}",
            "type": "snapshot",
            "ts": ts,
            "data": [{"T": ts, "s": exchange_id, "S": side, "v": qty, "p": price}],
        }
    )


def _ack(exchange_id: str) -> str:
    topic = f"allLiquidation.{exchange_id}"
    return json.dumps({"success": True, "ret_msg": "", "conn_id": "c1", "req_id": topic, "op": "subscribe"})


def _rejection(exchange_id: str) -> str:
    return MAINNET_REJECTED.replace("NOSUCHCOINUSDT", exchange_id)


def _subscribe_request(exchange_id: str) -> str:
    topic = f"allLiquidation.{exchange_id}"
    return json.dumps({"req_id": topic, "op": "subscribe", "args": [topic]}, separators=(",", ":"))


def _official_events() -> tuple[LiquidationEvent, LiquidationEvent]:
    sell = parse_liquidation_message(OFFICIAL_SELL, symbols=_SYMBOLS)
    hour = hour_start_ms(sell.events[0].time_ms)
    buy = parse_liquidation_message(_event(ROSE, "ROSEUSDT", "Buy", hour + 1_000, "1.5", "0.04"), symbols=_SYMBOLS)
    return sell.events[0], buy.events[0]


def _events(logs: Sequence[Any], name: str) -> list[Any]:
    return [entry for entry in logs if entry["event"] == name]


def test_recorded_events_seal_long_and_short_for_one_hour(fake_clock: FakeClock) -> None:
    short, long = _official_events()
    hour = hour_start_ms(short.time_ms)
    book = _book(ROSE)
    _subscribe(fake_clock, book, ROSE, hour - _EARLY_MS)
    book.accept((short, long))
    assert book.capture(hour + H1_MS) == ()

    fake_clock.set_now(hour + H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    rows = book.capture(now_ms())

    assert len(rows) == 1
    row = rows[0]
    assert row.symbol == ROSE
    assert row.ts_ms == hour
    assert row.short_count == 1
    assert row.short_qty == Decimal("20000")
    assert row.short_notional == Decimal("899.8")
    assert row.long_count == 1
    assert row.long_qty == Decimal("1.5")
    assert row.long_notional == Decimal("0.06")
    assert book.capture(now_ms()) == rows


def test_recorded_mainnet_events_aggregate_per_symbol(fake_clock: FakeClock) -> None:
    messages = [parse_liquidation_message(raw, symbols=MAINNET_SYMBOLS) for raw in MAINNET_EVENTS]
    hour = hour_start_ms(messages[0].events[0].time_ms)
    book = _book(AVAX, PEPE)
    _subscribe(fake_clock, book, AVAX, hour - _EARLY_MS)
    book.subscribed(PEPE, hour - _EARLY_MS)
    for message in messages:
        book.accept(message.events)
    fake_clock.set_now(hour + H1_MS + SEAL_MARGIN_MS)
    book.note_alive()

    rows = {row.symbol: row for row in book.capture(now_ms())}

    assert sorted(rows) == [PEPE, AVAX]
    avax = rows[AVAX]
    assert (avax.ts_ms, avax.long_count, avax.long_qty, avax.long_notional) == (
        hour,
        2,
        Decimal("1789.4"),
        Decimal("14556.6326"),  # 1783.2 × 8.135 + 6.2 × 8.113
    )
    assert (avax.short_count, avax.short_qty, avax.short_notional) == (0, Decimal(0), Decimal(0))
    pepe = rows[PEPE]
    assert (pepe.long_count, pepe.long_qty, pepe.long_notional) == (1, Decimal("2000"), Decimal("10.24"))


def test_two_identical_events_both_count(fake_clock: FakeClock) -> None:
    short, _long = _official_events()
    hour = hour_start_ms(short.time_ms)
    book = _book(ROSE)
    _subscribe(fake_clock, book, ROSE, hour - _EARLY_MS)
    book.accept((short, short))
    fake_clock.set_now(hour + H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    row = book.capture(now_ms())[0]
    assert row.short_count == 2
    assert row.short_qty == Decimal("40000")
    assert row.short_notional == Decimal("1799.6")


def test_quiet_hour_seals_as_zeros_and_a_late_event_does_not_change_it(fake_clock: FakeClock) -> None:
    book = _book(SYMBOL)
    _subscribe(fake_clock, book, SYMBOL, FAKE_NOW_MS - _EARLY_MS)
    fake_clock.set_now(FAKE_NOW_MS + H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    rows = book.capture(now_ms())
    assert len(rows) == 1
    assert rows[0].ts_ms == FAKE_NOW_MS
    assert rows[0].long_count == 0
    assert rows[0].short_qty == Decimal(0)
    book.ack(rows)

    late = LiquidationEvent(SYMBOL, "short", FAKE_NOW_MS + 1_000, Decimal("3"), Decimal("2"))
    book.accept((late,))
    assert book.open_hours() == ()
    assert book.capture(now_ms()) == ()


def test_partial_hour_and_unusable_frame_are_not_sealed(fake_clock: FakeClock) -> None:
    partial = _book(SYMBOL)
    _subscribe(fake_clock, partial, SYMBOL, FAKE_NOW_MS + 1)
    partial.accept((LiquidationEvent(SYMBOL, "long", FAKE_NOW_MS + 2, Decimal("1"), Decimal("1")),))
    fake_clock.set_now(FAKE_NOW_MS + H1_MS + SEAL_MARGIN_MS)
    partial.note_alive()
    assert partial.capture(now_ms()) == ()

    tainted = _book(SYMBOL)
    _subscribe(fake_clock, tainted, SYMBOL, FAKE_NOW_MS - _EARLY_MS)
    tainted.mark_unusable(FAKE_NOW_MS + 1_000)
    fake_clock.set_now(FAKE_NOW_MS + H1_MS + SEAL_MARGIN_MS)
    tainted.note_alive()
    assert tainted.capture(now_ms()) == ()


def test_subscription_inside_the_clock_margin_does_not_cover_the_hour(fake_clock: FakeClock) -> None:
    # Локальний годинник міг відставати до 1 с: підписка за 999 мс до межі за часом біржі могла бути вже після неї.
    book = _book(SYMBOL)
    _subscribe(fake_clock, book, SYMBOL, FAKE_NOW_MS - _EARLY_MS + 1)
    fake_clock.set_now(FAKE_NOW_MS + H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    assert book.capture(now_ms()) == ()


def test_proof_frame_must_come_a_clock_margin_after_the_hour(fake_clock: FakeClock) -> None:
    book = _book(SYMBOL)
    _subscribe(fake_clock, book, SYMBOL, FAKE_NOW_MS - _EARLY_MS)
    # Останній кадр за 999 мс після межі за локальним годинником — за часом біржі це могло бути ще до неї.
    fake_clock.set_now(FAKE_NOW_MS + H1_MS + _EARLY_MS - 1)
    book.note_alive()
    fake_clock.set_now(FAKE_NOW_MS + H1_MS + SEAL_MARGIN_MS)
    assert book.capture(now_ms()) == ()

    book.note_alive()
    assert [row.ts_ms for row in book.capture(now_ms())] == [FAKE_NOW_MS]


def test_disconnect_keeps_a_proven_hour_and_drops_the_open_one(fake_clock: FakeClock) -> None:
    book = _book(SYMBOL)
    _subscribe(fake_clock, book, SYMBOL, FAKE_NOW_MS - _EARLY_MS)
    book.accept((LiquidationEvent(SYMBOL, "long", FAKE_NOW_MS + 1_000, Decimal("1"), Decimal("10")),))
    fake_clock.set_now(FAKE_NOW_MS + H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    book.accept((LiquidationEvent(SYMBOL, "short", FAKE_NOW_MS + H1_MS + 1_000, Decimal("4"), Decimal("1")),))
    assert len(book.open_hours()) == 2

    book.disconnect()

    assert book.open_hours() == ()
    kept = book.capture(now_ms())
    assert len(kept) == 1
    assert kept[0].ts_ms == FAKE_NOW_MS
    assert kept[0].long_qty == Decimal("1")
    assert kept[0].short_count == 0


def test_only_subscribed_symbols_get_hours(fake_clock: FakeClock) -> None:
    book = _book(SYMBOL, ETH)
    _subscribe(fake_clock, book, SYMBOL, FAKE_NOW_MS - _EARLY_MS)
    book.accept((LiquidationEvent(ETH, "long", FAKE_NOW_MS + 1_000, Decimal("1"), Decimal("1")),))
    fake_clock.set_now(FAKE_NOW_MS + H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    assert [(row.symbol, row.long_count) for row in book.capture(now_ms())] == [(SYMBOL, 0)]


def test_untrusted_clock_seals_nothing_until_trust_returns(fake_clock: FakeClock) -> None:
    clock = _Clock(trusted=False)
    book = _book(SYMBOL, clock=clock)
    _subscribe(fake_clock, book, SYMBOL, FAKE_NOW_MS - _EARLY_MS)
    book.accept((LiquidationEvent(SYMBOL, "long", FAKE_NOW_MS + 1_000, Decimal("1"), Decimal("1")),))
    fake_clock.set_now(FAKE_NOW_MS + H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    assert book.open_hours() == ()
    assert book.capture(now_ms()) == ()

    # Довіра повернулась посеред другої години: рахуємо з третьої.
    clock.trusted = True
    with capture_logs() as logs:
        book.observe_clock()
    assert [entry["event"] for entry in logs] == ["liquidation_clock_trusted"]
    fake_clock.set_now(FAKE_NOW_MS + 3 * H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    assert [row.ts_ms for row in book.capture(now_ms())] == [FAKE_NOW_MS + 2 * H1_MS]


def test_losing_clock_trust_drops_the_open_hour_and_keeps_the_ready_one(fake_clock: FakeClock) -> None:
    clock = _Clock()
    book = _book(SYMBOL, clock=clock)
    _subscribe(fake_clock, book, SYMBOL, FAKE_NOW_MS - _EARLY_MS)
    fake_clock.set_now(FAKE_NOW_MS + H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    ready = book.capture(now_ms())
    book.accept((LiquidationEvent(SYMBOL, "short", FAKE_NOW_MS + H1_MS + 2_000, Decimal("1"), Decimal("1")),))
    assert len(book.open_hours()) == 1

    clock.trusted = False
    with capture_logs() as logs:
        book.note_alive()
    untrusted = _events(logs, "liquidation_clock_untrusted")
    assert len(untrusted) == 1
    assert untrusted[0]["reasons"] == ["CLOCK_UNVERIFIED"]
    assert book.open_hours() == ()
    assert book.capture(now_ms()) == ready

    clock.trusted = True
    book.observe_clock()  # довіра повернулась ще в другій годині
    fake_clock.set_now(FAKE_NOW_MS + 3 * H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    # Друга година пережила недовіру — її немає; третя почалась після повернення довіри.
    assert [row.ts_ms for row in book.capture(now_ms())] == [FAKE_NOW_MS, FAKE_NOW_MS + 2 * H1_MS]


def test_clock_jump_restarts_the_count_even_if_trust_returns_at_once(fake_clock: FakeClock) -> None:
    clock = _Clock()
    book = _book(SYMBOL, clock=clock)
    _subscribe(fake_clock, book, SYMBOL, FAKE_NOW_MS - _EARLY_MS)
    book.accept((LiquidationEvent(SYMBOL, "long", FAKE_NOW_MS + 1_000, Decimal("1"), Decimal("1")),))

    fake_clock.set_now(FAKE_NOW_MS + 10_000)
    clock.epoch = 1  # стрибок і свіжий вимір між двома кадрами: стан знову «довіряємо», але епоха інша
    book.note_alive()
    assert book.open_hours() == ()
    fake_clock.set_now(FAKE_NOW_MS + 2 * H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    assert [row.ts_ms for row in book.capture(now_ms())] == [FAKE_NOW_MS + H1_MS]


@pytest.mark.asyncio
async def test_watch_clock_suspends_the_book_on_the_tick_that_sees_a_jump(fake_clock: FakeClock) -> None:
    guard = ClockGuard(ScriptedTime(fake_clock), samples=1)
    book = LiquidationBook((SYMBOL,), clock=guard)
    ticks: list[list[str]] = []

    class _Done(Exception):
        pass

    with capture_logs() as logs:

        async def sleep_ms(ms: int) -> None:
            ticks.append([entry["event"] for entry in _events(logs, "liquidation_clock_untrusted")])
            for _ in range(5):
                await asyncio.sleep(0)  # дати перевірці `fetch_time` завершитись
            if len(ticks) == 2:
                fake_clock.jump_wall(5_000)  # системний годинник стрибнув, монотонний — ні
            if len(ticks) == 3:
                raise _Done
            fake_clock.advance(ms)

        with pytest.raises(ExceptionGroup) as raised:
            await watch_clock(guard, book, sleep_ms=sleep_ms)

    assert raised.group_contains(_Done)
    assert [entry["event"] for entry in logs if entry["event"].startswith("liquidation_clock")] == [
        "liquidation_clock_trusted",
        "liquidation_clock_untrusted",
    ]
    # Тік 3 побачив стрибок — і книга дізналась про нього до сну цього ж тіку.
    assert ticks == [[], [], ["liquidation_clock_untrusted"]]


class _Watch:
    def __init__(self, error: BaseException | None = None) -> None:
        self._error = error
        self.cancelled = False

    async def run(self, *, sleep: Any = None, on_jump: Any = None) -> None:
        try:
            if self._error is not None:
                raise self._error
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


@pytest.mark.asyncio
async def test_collector_end_stops_the_clock_watch() -> None:
    watch = _Watch()
    finished: list[bool] = []

    async def body() -> None:
        await asyncio.sleep(0)
        finished.append(True)

    await collect_with_clock(cast(ClockGuard, watch), _book(SYMBOL), body)

    assert finished == [True]
    assert watch.cancelled


@pytest.mark.asyncio
async def test_clock_watch_failure_stops_the_collector() -> None:
    body_cancelled: list[bool] = []

    async def body() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            body_cancelled.append(True)
            raise

    with pytest.raises(ExceptionGroup) as raised:
        await asyncio.wait_for(
            collect_with_clock(cast(ClockGuard, _Watch(RuntimeError("clock"))), _book(SYMBOL), body), 5
        )

    assert raised.group_contains(RuntimeError)
    assert body_cancelled == [True]


def test_reconnect_delay_doubles_until_the_cap() -> None:
    def lowest(low: int, high: int) -> int:
        return low

    def highest(low: int, high: int) -> int:
        return high

    assert reconnect_delay_ms(0, jitter=lowest) == 500
    assert reconnect_delay_ms(0, jitter=highest) == 1_000
    assert reconnect_delay_ms(1, jitter=lowest) == 1_000
    assert reconnect_delay_ms(2, jitter=lowest) == 2_000
    assert reconnect_delay_ms(16, jitter=lowest) == 30_000
    assert reconnect_delay_ms(100, jitter=lowest) == 30_000


def test_db_retry_delay_doubles_until_the_cap() -> None:
    assert db_retry_delay_ms(1) == DB_RETRY_BASE_MS
    assert db_retry_delay_ms(2) == 2 * DB_RETRY_BASE_MS
    assert db_retry_delay_ms(7) == DB_RETRY_CAP_MS
    assert db_retry_delay_ms(1_000) == DB_RETRY_CAP_MS
    with pytest.raises(ValueError, match="failures"):
        db_retry_delay_ms(0)


def test_final_flush_fits_the_lock_stop_budget() -> None:
    # Після втрати lock `run_exclusive` чекає зупинки роботи `BODY_STOP_TIMEOUT_MS`, потім — аварійний вихід.
    assert FINAL_FLUSH_TIMEOUT_MS < BODY_STOP_TIMEOUT_MS


class _Boom:
    def __call__(self) -> object:
        raise SQLAlchemyError("no-db")


class _HangingSession:
    async def __aenter__(self) -> object:
        await asyncio.Event().wait()
        raise AssertionError("БД не мала відповісти")

    async def __aexit__(self, *exc: object) -> None:
        return None


class _HangingDb:
    """БД, що зависла: з'єднання не повертається і не падає."""

    def __call__(self) -> _HangingSession:
        return _HangingSession()


class _ScriptSocket:
    def __init__(self, items: list[str | ExchangeUnavailable], *, then_quiet: bool = False) -> None:
        self._items = items
        self._then_quiet = then_quiet
        self.sent: list[str] = []
        self.closed = False

    async def send_text(self, text: str) -> None:
        self.sent.append(text)

    async def receive_text(self) -> str:
        if not self._items:
            if self._then_quiet:
                await asyncio.Event().wait()
            raise ExchangeUnavailable("closed")
        item = self._items.pop(0)
        if isinstance(item, ExchangeUnavailable):
            raise item
        return item

    async def close(self) -> None:
        self.closed = True


class _ScriptConnector:
    def __init__(self, scripts: list[list[str | ExchangeUnavailable]], *, then_quiet: bool = False) -> None:
        self._scripts = scripts
        self._then_quiet = then_quiet
        self.sockets: list[_ScriptSocket] = []
        self.urls: list[str] = []

    async def connect(self, url: str) -> _ScriptSocket:
        self.urls.append(url)
        socket = _ScriptSocket(self._scripts.pop(0), then_quiet=self._then_quiet)
        self.sockets.append(socket)
        return socket

    async def aclose(self) -> None:
        return None


class _Down:
    def __init__(self, stop: asyncio.Event) -> None:
        self._stop = stop
        self.calls = 0

    async def connect(self, url: str) -> _ScriptSocket:
        self.calls += 1
        if self.calls >= 3:
            self._stop.set()
        raise ExchangeUnavailable("down")

    async def aclose(self) -> None:
        return None


class _RecordingBook(LiquidationBook):
    def __init__(self, stop: asyncio.Event, *symbols: str, stop_after: int = 2) -> None:
        super().__init__(symbols or (SYMBOL,), clock=_Clock(), seal_margin_ms=SEAL_MARGIN_MS)
        self._stop = stop
        self._stop_after = stop_after
        self.seen: list[LiquidationEvent] = []
        self.snapshots: list[tuple[SealedHour, ...]] = []

    def accept(self, events: Sequence[LiquidationEvent]) -> None:
        self.seen.extend(events)
        super().accept(events)
        if len(self.seen) >= self._stop_after:
            self._stop.set()

    def disconnect(self) -> None:
        self.snapshots.append(self.open_hours())
        super().disconnect()


def _lowest(low: int, high: int) -> int:
    return low


async def _parked_sleep(stop: asyncio.Event, sleeps: list[float], seconds: float) -> None:
    # Довгий сон — інтервал ping. Його скасовує stop або закриття сесії, і в паузу реконекту він не входить.
    if seconds >= 10:
        await stop.wait()
        return
    sleeps.append(seconds)


@pytest.mark.asyncio
async def test_collector_reconnects_with_backoff_and_resubscribes(fake_clock: FakeClock) -> None:
    fake_clock.set_now(FAKE_NOW_MS - _EARLY_MS)
    stop = asyncio.Event()
    sleeps: list[float] = []
    first = _event(SYMBOL, "BTCUSDT", "Sell", FAKE_NOW_MS + 1_000, "1", "1")
    second = _event(SYMBOL, "BTCUSDT", "Sell", FAKE_NOW_MS + 2_000, "2", "1")
    connector = _ScriptConnector([[_ack("BTCUSDT"), first, ExchangeUnavailable("closed")], [_ack("BTCUSDT"), second]])
    book = _RecordingBook(stop)

    async def sleep(seconds: float) -> None:
        await _parked_sleep(stop, sleeps, seconds)

    await run_collector(
        cast(SessionFactory, _Boom()),
        book=book,
        floor_ms=FAKE_NOW_MS - 2 * H1_MS,
        connector=connector,
        stop=stop,
        sleep=sleep,
        url=MAINNET_LINEAR_WS,
        jitter=_lowest,
        ping_interval_ms=_PING_MS,
        stale_after_ms=_PING_MS,
        subscribe_timeout_ms=_PING_MS,
    )

    subscribe = _subscribe_request("BTCUSDT")
    assert connector.urls == [MAINNET_LINEAR_WS, MAINNET_LINEAR_WS]
    assert [socket.sent for socket in connector.sockets] == [[subscribe], [subscribe]]
    assert sleeps == [0.5]
    assert [event.qty for event in book.seen] == [Decimal("1"), Decimal("2")]
    assert [[(row.short_qty, row.ts_ms) for row in snap] for snap in book.snapshots] == [
        [(Decimal("1"), FAKE_NOW_MS)],
        [(Decimal("2"), FAKE_NOW_MS)],
    ]
    assert book.capture(now_ms()) == ()
    assert book.open_hours() == ()


@pytest.mark.asyncio
async def test_connect_failures_double_the_pause_until_stop() -> None:
    stop = asyncio.Event()
    sleeps: list[float] = []
    down = _Down(stop)

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    await run_collector(
        cast(SessionFactory, _Boom()),
        book=_book(SYMBOL),
        floor_ms=FAKE_NOW_MS - 2 * H1_MS,
        connector=down,
        stop=stop,
        sleep=sleep,
        url=MAINNET_LINEAR_WS,
        jitter=_lowest,
        ping_interval_ms=_PING_MS,
        stale_after_ms=_PING_MS,
        subscribe_timeout_ms=_PING_MS,
    )

    assert down.calls == 3
    assert sleeps == [0.5, 1.0]


async def _run_script(
    connector: _ScriptConnector,
    book: LiquidationBook,
    stop: asyncio.Event,
    *,
    sessions: object = None,
    sleep: Any = None,
    final_flush_timeout_ms: int = FINAL_FLUSH_TIMEOUT_MS,
) -> list[Any]:
    sleeps: list[float] = []

    async def parked(seconds: float) -> None:
        await _parked_sleep(stop, sleeps, seconds)

    with capture_logs() as logs:
        await asyncio.wait_for(
            run_collector(
                cast(SessionFactory, sessions if sessions is not None else _Boom()),
                book=book,
                floor_ms=FAKE_NOW_MS - 2 * H1_MS,
                connector=connector,
                stop=stop,
                sleep=sleep if sleep is not None else parked,
                url=MAINNET_LINEAR_WS,
                jitter=_lowest,
                ping_interval_ms=_PING_MS,
                stale_after_ms=_PING_MS,
                subscribe_timeout_ms=_PING_MS,
                final_flush_timeout_ms=final_flush_timeout_ms,
            ),
            _STOP_GUARD_S,
        )
    return list(logs)


@pytest.mark.asyncio
async def test_rejected_symbol_does_not_stop_the_others(fake_clock: FakeClock) -> None:
    fake_clock.set_now(FAKE_NOW_MS - _EARLY_MS)
    stop = asyncio.Event()
    btc = _event(SYMBOL, "BTCUSDT", "Sell", FAKE_NOW_MS + 1_000, "1", "1")
    eth = _event(ETH, "ETHUSDT", "Buy", FAKE_NOW_MS + 1_000, "7", "1")
    connector = _ScriptConnector([[_ack("BTCUSDT"), _rejection("ETHUSDT"), eth, btc]], then_quiet=True)
    book = _RecordingBook(stop, SYMBOL, ETH, stop_after=1)

    logs = await _run_script(connector, book, stop)

    assert connector.sockets[0].sent == [_subscribe_request("BTCUSDT"), _subscribe_request("ETHUSDT")]
    assert len(connector.urls) == 1
    assert [event.symbol for event in book.seen] == [SYMBOL]
    rejected = _events(logs, "liquidation_topic_rejected")
    assert [(entry["symbol"], entry["detail"]) for entry in rejected] == [
        (ETH, "error:handler not found,topic:allLiquidation.ETHUSDT")
    ]
    assert _events(logs, "liquidation_stream_lost") == []


@pytest.mark.asyncio
async def test_all_symbols_rejected_reconnects_with_backoff(fake_clock: FakeClock) -> None:
    fake_clock.set_now(FAKE_NOW_MS - _EARLY_MS)
    stop = asyncio.Event()
    btc = _event(SYMBOL, "BTCUSDT", "Sell", FAKE_NOW_MS + 1_000, "1", "1")
    connector = _ScriptConnector([[_rejection("BTCUSDT")], [_ack("BTCUSDT"), btc]], then_quiet=True)
    book = _RecordingBook(stop, stop_after=1)
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        await _parked_sleep(stop, sleeps, seconds)

    logs = await _run_script(connector, book, stop, sleep=sleep)

    assert len(connector.urls) == 2
    assert sleeps == [0.5]
    assert [entry["error_type"] for entry in _events(logs, "liquidation_stream_lost")] == ["ExchangeUnavailable"]
    assert [event.qty for event in book.seen] == [Decimal("1")]


class _Sink:
    def __init__(self) -> None:
        self.subscriptions: list[str] = []
        self.listening = asyncio.Event()

    def subscribed(self, symbol: str, at_ms: int) -> None:
        self.subscriptions.append(symbol)
        self.listening.set()

    def accept(self, events: Sequence[LiquidationEvent]) -> None:
        return None

    def mark_unusable(self, hour_ms: int | None) -> None:
        return None

    def note_alive(self) -> None:
        return None


def _time_after_subscription(sink: _Sink, fake_clock: FakeClock, sleeps: list[float]) -> Any:
    """Сон нагляду сесії: чекає першої підтвердженої підписки, потім рухає монотонний годинник на `seconds`."""

    async def sleep(seconds: float) -> None:
        await sink.listening.wait()
        sleeps.append(seconds)
        fake_clock.advance(int(seconds * 1000))

    return sleep


@pytest.mark.asyncio
async def test_unanswered_subscription_times_out(fake_clock: FakeClock) -> None:
    sink = _Sink()
    sleeps: list[float] = []
    socket = _ScriptSocket([_ack("BTCUSDT")], then_quiet=True)

    with capture_logs() as logs, pytest.raises(ExchangeUnavailable, match="subscribe timeout"):
        await asyncio.wait_for(
            read_liquidation_session(
                socket,
                symbols=(SYMBOL, ETH),
                sink=sink,
                stop=asyncio.Event(),
                sleep=_time_after_subscription(sink, fake_clock, sleeps),
                subscribe_timeout_ms=10_000,
            ),
            _STOP_GUARD_S,
        )

    assert sink.subscriptions == [SYMBOL]
    assert [entry["awaiting"] for entry in _events(logs, "liquidation_subscribe_timeout")] == [[ETH]]
    assert sleeps == [10.0]


@pytest.mark.asyncio
async def test_silent_stream_is_dropped_as_stale_after_two_pings(fake_clock: FakeClock) -> None:
    sink = _Sink()
    sleeps: list[float] = []
    socket = _ScriptSocket([_ack("BTCUSDT")], then_quiet=True)

    with pytest.raises(ExchangeUnavailable, match="stale"):
        await asyncio.wait_for(
            read_liquidation_session(
                socket,
                symbols=(SYMBOL,),
                sink=sink,
                stop=asyncio.Event(),
                sleep=_time_after_subscription(sink, fake_clock, sleeps),
                ping_interval_ms=20_000,
                stale_after_ms=60_000,
                subscribe_timeout_ms=10_000,
            ),
            _STOP_GUARD_S,
        )

    # Ping на 20 і 40 с без жодної відповіді; на 60 с тиші з'єднання мертве.
    assert socket.sent[1:] == ['{"op":"ping"}', '{"op":"ping"}']
    assert sleeps == [10.0, 10.0, 20.0, 20.0]


@pytest.mark.asyncio
async def test_db_outage_is_logged_once_per_backoff_and_reading_goes_on(fake_clock: FakeClock) -> None:
    fake_clock.set_now(FAKE_NOW_MS - _EARLY_MS)
    stop = asyncio.Event()
    frames: list[str | ExchangeUnavailable] = [_ack("BTCUSDT")]
    frames += [_event(SYMBOL, "BTCUSDT", "Sell", FAKE_NOW_MS + 1_000 * i, "1", "1") for i in range(1, 6)]
    connector = _ScriptConnector([frames], then_quiet=True)
    book = _RecordingBook(stop, stop_after=5)

    logs = await _run_script(connector, book, stop)

    assert len(book.seen) == 5
    # Старт і останній прохід при зупинці. Кадри між ними йдуть у паузу після помилки й БД не чіпають.
    failed = _events(logs, "liquidation_gap_sync_failed")
    assert [(entry["failures"], entry["retry_in_ms"]) for entry in failed] == [
        (1, DB_RETRY_BASE_MS),
        (2, 2 * DB_RETRY_BASE_MS),
    ]


@pytest.mark.asyncio
async def test_hanging_db_does_not_stall_reading_and_stop_is_bounded(fake_clock: FakeClock) -> None:
    fake_clock.set_now(FAKE_NOW_MS - _EARLY_MS)
    stop = asyncio.Event()
    frames: list[str | ExchangeUnavailable] = [_ack("BTCUSDT")]
    frames += [_event(SYMBOL, "BTCUSDT", "Sell", FAKE_NOW_MS + 1_000 * i, "1", "1") for i in range(1, 6)]
    connector = _ScriptConnector([frames], then_quiet=True)
    book = _RecordingBook(stop, stop_after=5)

    logs = await _run_script(connector, book, stop, sessions=_HangingDb(), final_flush_timeout_ms=50)

    assert len(book.seen) == 5
    assert _events(logs, "liquidation_stream_lost") == []
    assert [entry["timeout_ms"] for entry in _events(logs, "liquidation_final_flush_timeout")] == [50]


class _QuietSocket:
    """Тихий ринок: після (необов'язкового) ack кадрів немає, а ping ще не час — `receive_text` чекає вічно."""

    def __init__(self, *, ack: bool) -> None:
        self._ack = ack
        self.sent: list[str] = []
        self.closed = False
        self.waiting = asyncio.Event()

    async def send_text(self, text: str) -> None:
        self.sent.append(text)

    async def receive_text(self) -> str:
        if self._ack:
            self._ack = False
            return _ack("BTCUSDT")
        self.waiting.set()
        await asyncio.Event().wait()
        raise AssertionError("тихий сокет не віддає кадрів")

    async def close(self) -> None:
        self.closed = True


class _OneSocket:
    def __init__(self, socket: _QuietSocket, *, stop_on_connect: asyncio.Event | None = None) -> None:
        self._socket = socket
        self._stop_on_connect = stop_on_connect
        self.calls = 0

    async def connect(self, url: str) -> _QuietSocket:
        self.calls += 1
        if self._stop_on_connect is not None:
            self._stop_on_connect.set()
        return self._socket

    async def aclose(self) -> None:
        return None


class _HangingConnect:
    """З'єднання не встановлюється й не падає (TCP є, відповіді на upgrade немає)."""

    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def connect(self, url: str) -> _QuietSocket:
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("з'єднання не мало встановитись")

    async def aclose(self) -> None:
        return None


async def _collect_until_stopped(
    connector: _OneSocket | _HangingConnect, stop: asyncio.Event, *, started: asyncio.Event | None
) -> list[str]:
    async def sleep(seconds: float) -> None:
        await _parked_sleep(stop, [], seconds)

    with capture_logs() as logs:
        run = asyncio.create_task(
            run_collector(
                cast(SessionFactory, _Boom()),
                book=_book(SYMBOL),
                floor_ms=FAKE_NOW_MS - 2 * H1_MS,
                connector=connector,
                stop=stop,
                sleep=sleep,
                url=MAINNET_LINEAR_WS,
                jitter=_lowest,
                ping_interval_ms=_PING_MS,
                stale_after_ms=_PING_MS,
                subscribe_timeout_ms=_PING_MS,
            )
        )
        if started is not None:
            await asyncio.wait_for(started.wait(), _STOP_GUARD_S)
            stop.set()
        await asyncio.wait_for(run, _STOP_GUARD_S)
    return [str(entry["event"]) for entry in logs]


@pytest.mark.asyncio
@pytest.mark.parametrize("ack", [True, False], ids=["subscribed", "awaiting-ack"])
async def test_stop_ends_a_quiet_session_without_waiting_for_a_frame(ack: bool) -> None:
    stop = asyncio.Event()
    socket = _QuietSocket(ack=ack)
    connector = _OneSocket(socket)

    events = await _collect_until_stopped(connector, stop, started=socket.waiting)

    assert connector.calls == 1
    assert socket.sent == [_subscribe_request("BTCUSDT")]
    assert socket.closed
    assert "liquidation_stream_lost" not in events


@pytest.mark.asyncio
async def test_stop_interrupts_a_hanging_connect() -> None:
    stop = asyncio.Event()
    connector = _HangingConnect()

    events = await _collect_until_stopped(connector, stop, started=connector.started)

    assert "liquidation_connect_failed" not in events


@pytest.mark.asyncio
async def test_socket_opened_after_stop_is_closed_without_subscribing() -> None:
    stop = asyncio.Event()
    socket = _QuietSocket(ack=True)
    connector = _OneSocket(socket, stop_on_connect=stop)

    await _collect_until_stopped(connector, stop, started=None)

    assert connector.calls == 1
    assert socket.sent == []
    assert socket.closed


async def _open_gaps(conn: AsyncConnection) -> list[tuple[str, int, int]]:
    rows = await conn.execute(
        select(DataGap.symbol, DataGap.from_ms, DataGap.to_ms)
        .where(DataGap.source == "liquidations", DataGap.resolved_at_ms.is_(None))
        .order_by(DataGap.from_ms)
    )
    return [(symbol, start, end) for symbol, start, end in rows.all()]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sealed_hour_is_stored_once_and_a_missed_hour_is_a_gap(
    conn: AsyncConnection, fake_clock: FakeClock
) -> None:
    short, long = _official_events()
    hour = hour_start_ms(short.time_ms)
    book = _book(ROSE)
    _subscribe(fake_clock, book, ROSE, hour - _EARLY_MS)
    book.accept((short, long))
    fake_clock.set_now(hour + H1_MS + SEAL_MARGIN_MS)
    book.note_alive()
    rows = book.capture(now_ms())
    sessions = _sessions(conn)

    assert await save_liquidations(sessions, rows) == 1
    changed = SealedHour(
        ROSE,
        hour,
        long_count=9,
        long_qty=Decimal("9"),
        long_notional=Decimal("9"),
        short_count=9,
        short_qty=Decimal("9"),
        short_notional=Decimal("9"),
    )
    assert await save_liquidations(sessions, (changed,)) == 0
    stored = (
        await conn.execute(
            select(
                LiquidationsHourly.long_count,
                LiquidationsHourly.long_qty,
                LiquidationsHourly.short_count,
                LiquidationsHourly.short_qty,
                LiquidationsHourly.short_notional,
            ).where(LiquidationsHourly.symbol == ROSE)
        )
    ).one()
    assert stored == (1, Decimal("1.5"), 1, Decimal("20000"), Decimal("899.8"))

    await sync_liquidation_gaps(
        sessions,
        symbols=(ROSE,),
        floor_ms=hour - H1_MS,
        now=hour + H1_MS + SEAL_MARGIN_MS,
    )
    assert await _open_gaps(conn) == [(ROSE, hour - H1_MS, hour)]

    quiet = SealedHour(ROSE, hour - H1_MS, 0, Decimal(0), Decimal(0), 0, Decimal(0), Decimal(0))
    assert await save_liquidations(sessions, (quiet,)) == 1
    await sync_liquidation_gaps(
        sessions,
        symbols=(ROSE,),
        floor_ms=hour - H1_MS,
        now=hour + H1_MS + SEAL_MARGIN_MS,
    )
    assert await _open_gaps(conn) == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_hours_before_listing_are_not_a_liquidation_gap(conn: AsyncConnection) -> None:
    hour = FAKE_NOW_MS
    floor = hour - 4 * H1_MS
    launch = hour - 2 * H1_MS
    sessions = _sessions(conn)
    await sync_liquidation_gaps(sessions, symbols=(SYMBOL,), floor_ms=floor, now=hour + SEAL_MARGIN_MS)
    assert await _open_gaps(conn) == [(SYMBOL, floor, hour)]

    await conn.execute(
        insert(StoredInstrument).values(
            exchange="bybit",
            mode="demo",
            symbol=SYMBOL,
            tick_size=Decimal("0.1"),
            qty_step=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
            max_leverage=Decimal("100"),
            launch_time_ms=launch,
            status="Trading",
            updated_at_ms=hour,
        )
    )
    await sync_liquidation_gaps(sessions, symbols=(SYMBOL,), floor_ms=floor, now=hour + SEAL_MARGIN_MS)
    assert await _open_gaps(conn) == [(SYMBOL, launch, hour)]
