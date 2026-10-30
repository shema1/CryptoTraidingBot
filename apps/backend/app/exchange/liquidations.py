"""Публічний WebSocket ліквідацій Bybit v5 (ТЗ §4.4, T1.10).

Тема `allLiquidation.{symbol}` на `wss://…/v5/public/linear`. Історії немає: потік лише вперед, підписка не
відтворює пропущені події. Формат звірено з документацією v5 (context7, 2026-10-30):

- `T` — час ліквідації, мс; `s` — символ; `v` — виконаний розмір (рядок); `p` — bankruptcy price (рядок);
- `S` — сторона позиції: `Buy` означає, що ліквідовано long; `Sell` — short (та сама документація: «When you
  receive a Buy update, this means that a long position has been liquidated»);
- push кожні 500 мс, `type = snapshot` (це пачка нових подій, не знімок години);
- ping — JSON `{"op":"ping"}` кожні 20 с; відповідь публічного linear: `op = ping`, `ret_msg = pong`.

Перевірено на публічному mainnet 2026-10-30 (без ключів): `req_id` запиту повертається у відповіді; запит, де хоч одна
тема невідома, відхиляється цілком (`success: false`, `ret_msg: "error:handler not found,topic:…"`, без
`failTopics`). Тому кожен символ — окремий запит із `req_id` = темою: невідомий символ не зупиняє решту. Без ping
з'єднання закривається приблизно за 60 с (код 1006).

`category=linear` сидить у шляху URL. Ключі не використовуються. Сирі винятки aiohttp за модуль не виходять.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Final, Literal, Protocol, cast

import aiohttp

from app.core.clock import monotonic_ms, now_ms
from app.core.logging import get_logger
from app.exchange.bybit import ExchangeMode, exchange_symbol
from app.exchange.errors import AdapterConfigError, ExchangeUnavailable

_LOG = get_logger(__name__)

MAINNET_LINEAR_WS: Final = "wss://stream.bybit.com/v5/public/linear"
TESTNET_LINEAR_WS: Final = "wss://stream-testnet.bybit.com/v5/public/linear"
TOPIC_PREFIX: Final = "allLiquidation."
# Bybit просить ping кожні 20 с. Мовчання довше за три інтервали — з'єднання мертве.
PING_INTERVAL_MS: Final = 20_000
STALE_AFTER_MS: Final = 60_000
SUBSCRIBE_TIMEOUT_MS: Final = 10_000
CONNECT_TIMEOUT_S: Final = 10.0
# Усе відкриття з'єднання (DNS, TCP, TLS, upgrade). Відповідь на upgrade aiohttp без цього чекає вічно.
HANDSHAKE_TIMEOUT_S: Final = 15.0
# Пачка більша за це — не розбираємо частинами: інакше тихо втратили б хвіст.
_MAX_EVENTS: Final = 5_000
_MAX_MESSAGE_CHARS: Final = 1_000_000
_DECIMAL_CHARS: Final = 100
_DETAIL_CHARS: Final = 200
_MS_DIGITS: Final = 16
_PING: Final = '{"op":"ping"}'

PositionSide = Literal["long", "short"]
_SIDE: Final[Mapping[str, PositionSide]] = {"Buy": "long", "Sell": "short"}
MessageKind = Literal["events", "subscribed", "rejected", "pong", "ignore", "unusable"]

Sleep = Callable[[float], Awaitable[None]]
# Сигнал «прийшов кадр» без очікування: запис у БД іде окремою задачею, читання на неї не чекає.
Progress = Callable[[], None]


def liquidation_stream_url(mode: ExchangeMode) -> str:
    """Публічний linear WS. Demo і live — один mainnet (як REST у `BybitAdapter`); testnet — окремий хост."""
    if mode is ExchangeMode.TESTNET:
        return TESTNET_LINEAR_WS
    if mode is ExchangeMode.DEMO or mode is ExchangeMode.LIVE:
        return MAINNET_LINEAR_WS
    raise AdapterConfigError("mode має бути ExchangeMode: demo, testnet або live")


def liquidation_topic(symbol: str) -> str:
    """Канонічний символ → тема `allLiquidation.BTCUSDT`."""
    return f"{TOPIC_PREFIX}{exchange_symbol(symbol)}"


@dataclass(frozen=True, slots=True)
class LiquidationEvent:
    """Одна ліквідація. `position_side` — сторона позиції, яку закрила біржа, не сторона ордера."""

    symbol: str
    position_side: PositionSide
    time_ms: int
    qty: Decimal
    price: Decimal

    @property
    def notional(self) -> Decimal:
        """Обсяг у USDT: розмір × bankruptcy price, без округлення."""
        return self.qty * self.price


@dataclass(frozen=True, slots=True)
class LiquidationMessage:
    """Розбір одного кадру. `unusable` — кадр нашої теми, якому не можна вірити (годину не закриваємо).

    `symbol` — канонічний символ відповіді на підписку (`subscribed` / `rejected`), за `req_id`; `detail` — `ret_msg`
    відмови, обрізаний.
    """

    kind: MessageKind
    events: tuple[LiquidationEvent, ...] = ()
    exchange_ts_ms: int | None = None
    symbol: str | None = None
    detail: str = ""


class LiquidationSocket(Protocol):
    """Одне WS-з'єднання. `receive_text` чекає кадр; обрив — `ExchangeUnavailable`, не порожній рядок."""

    async def send_text(self, text: str) -> None: ...

    async def receive_text(self) -> str: ...

    async def close(self) -> None: ...


class LiquidationConnector(Protocol):
    """Відкрити публічний потік. Тести підставляють скрипт; прод — `AiohttpLiquidationConnector`."""

    async def connect(self, url: str) -> LiquidationSocket: ...

    async def aclose(self) -> None: ...


class LiquidationSink(Protocol):
    """Куди сесія кладе події. Реалізація — `app.data.liquidations.LiquidationBook`."""

    def subscribed(self, symbol: str, at_ms: int) -> None: ...

    def accept(self, events: Sequence[LiquidationEvent]) -> None: ...

    def mark_unusable(self, hour_ms: int | None) -> None: ...

    def note_alive(self) -> None: ...


def parse_liquidation_message(raw: str, *, symbols: Mapping[str, str]) -> LiquidationMessage:
    """Розібрати кадр. `symbols` — біржовий id (`BTCUSDT`) → канонічний (`BTC/USDT:USDT`).

    Порожня пачка `data` — валідні `events` без подій. Чужий символ — `ignore`. Зламаний кадр нашої теми —
    `unusable`: годину з таким кадром не можна записувати як нуль. Відповідь на підписку прив'язується до символу
    за `req_id` (= тема); відповідь без нашого `req_id` — `ignore`.
    """
    if type(raw) is not str or len(raw) > _MAX_MESSAGE_CHARS:
        return LiquidationMessage("unusable")
    try:
        parsed: object = json.loads(raw)
    except json.JSONDecodeError:
        return LiquidationMessage("unusable")
    if type(parsed) is not dict:
        return LiquidationMessage("unusable")
    body = cast("dict[str, object]", parsed)
    op = body.get("op")
    if type(op) is str:
        return _control(body, op, symbols)
    topic = body.get("topic")
    if type(topic) is not str or not topic.startswith(TOPIC_PREFIX):
        return LiquidationMessage("ignore")
    exchange_id = topic[len(TOPIC_PREFIX) :]
    canonical = symbols.get(exchange_id)
    if canonical is None:
        return LiquidationMessage("ignore")
    exchange_ts = _optional_ms(body.get("ts"))
    if body.get("type") != "snapshot":
        return LiquidationMessage("unusable", exchange_ts_ms=exchange_ts)
    data = body.get("data")
    if type(data) is not list or len(data) > _MAX_EVENTS:
        return LiquidationMessage("unusable", exchange_ts_ms=exchange_ts)
    events: list[LiquidationEvent] = []
    for item in data:
        event = _event(item, exchange_id=exchange_id, canonical=canonical)
        if event is None:
            return LiquidationMessage("unusable", exchange_ts_ms=exchange_ts)
        events.append(event)
    return LiquidationMessage("events", events=tuple(events), exchange_ts_ms=exchange_ts)


async def read_liquidation_session(
    socket: LiquidationSocket,
    *,
    symbols: Sequence[str],
    sink: LiquidationSink,
    stop: asyncio.Event,
    sleep: Sleep,
    on_progress: Progress | None = None,
    ping_interval_ms: int = PING_INTERVAL_MS,
    stale_after_ms: int = STALE_AFTER_MS,
    subscribe_timeout_ms: int = SUBSCRIBE_TIMEOUT_MS,
) -> None:
    """Читати одне з'єднання, доки його не обірвано або не виставлено `stop`.

    Кожен символ — окремий запит підписки з `req_id` = темою. Відхилений символ — `liquidation_topic_rejected`
    (error), решта працює далі; його години лишаються прогалинами. Відхилено всі символи, обрив, тиша довша за
    `stale_after_ms` і відповіді не на всі підписки за `subscribe_timeout_ms` — `ExchangeUnavailable`.
    `stop` завершує сесію без винятку одразу, не чекаючи наступного кадру. Ping і перевірка тиші — окрема задача:
    таймаут `receive` в aiohttp позначає сокет закритим. `on_progress` викликається після кожного кадру і не
    повинен чекати (БД пише окрема задача).
    """
    tracked = tuple(symbols)
    if not tracked or len(set(tracked)) != len(tracked):
        raise ValueError("symbols має бути непорожнім списком без дублікатів")
    _require_ms("ping_interval_ms", ping_interval_ms, minimum=1)
    _require_ms("stale_after_ms", stale_after_ms, minimum=ping_interval_ms)
    _require_ms("subscribe_timeout_ms", subscribe_timeout_ms, minimum=1)
    symbol_map = {exchange_symbol(symbol): symbol for symbol in tracked}
    for symbol in tracked:
        topic = liquidation_topic(symbol)
        request = {"req_id": topic, "op": "subscribe", "args": [topic]}
        await socket.send_text(json.dumps(request, separators=(",", ":")))
    state = _SessionState(started_mono=monotonic_ms(), awaiting=set(tracked))
    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(
                _receive(
                    socket,
                    symbol_map=symbol_map,
                    sink=sink,
                    stop=stop,
                    state=state,
                    on_progress=on_progress,
                ),
                name="liquidation-recv",
            )
            group.create_task(
                _watch(
                    socket,
                    stop=stop,
                    sleep=sleep,
                    state=state,
                    ping_interval_ms=ping_interval_ms,
                    stale_after_ms=stale_after_ms,
                    subscribe_timeout_ms=subscribe_timeout_ms,
                ),
                name="liquidation-watch",
            )
    except* ExchangeUnavailable as errors:
        raise errors.exceptions[0] from None


class AiohttpLiquidationConnector:
    """Прод-транспорт публічного WS. Сесія живе між реконектами; `aclose` закриває її.

    Відкриття з'єднання обмежене `handshake_timeout_s`: `ClientTimeout` нижче не покриває відповідь на upgrade
    (`total` і `sock_read` вимкнені, бо інакше обмежили б і читання потоку).
    """

    def __init__(self, *, handshake_timeout_s: float = HANDSHAKE_TIMEOUT_S) -> None:
        if type(handshake_timeout_s) is not float or not 0 < handshake_timeout_s <= 300:
            raise ValueError("handshake_timeout_s має бути float у (0, 300] с")
        self._session: aiohttp.ClientSession | None = None
        self._handshake_timeout_s = handshake_timeout_s

    async def connect(self, url: str) -> LiquidationSocket:
        if type(url) is not str or not url.startswith("wss://"):
            raise ValueError("url публічного потоку має починатися з wss://")
        session = self._session
        if session is None or session.closed:
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None, connect=CONNECT_TIMEOUT_S, sock_connect=CONNECT_TIMEOUT_S)
            )
            self._session = session
        try:
            async with asyncio.timeout(self._handshake_timeout_s):
                # autoping відповідає на протокольний PING. JSON-ping шле сесія сама: Bybit тримає канал по ньому.
                ws = await session.ws_connect(url, autoping=True, heartbeat=None, receive_timeout=None)
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise ExchangeUnavailable("liquidation stream connect failed") from exc
        return _AiohttpSocket(ws)

    async def aclose(self) -> None:
        session = self._session
        self._session = None
        if session is not None and not session.closed:
            await session.close()


class _AiohttpSocket:
    def __init__(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self._ws = ws

    async def send_text(self, text: str) -> None:
        try:
            await self._ws.send_str(text)
        except (aiohttp.ClientError, ConnectionError, RuntimeError) as exc:
            raise ExchangeUnavailable("liquidation stream send failed") from exc

    async def receive_text(self) -> str:
        # Без timeout: TimeoutError у receive() ставить abnormal closure і ламає сокет (aiohttp 3.14).
        try:
            msg = await self._ws.receive()
        except (aiohttp.ClientError, ConnectionError, RuntimeError) as exc:
            raise ExchangeUnavailable("liquidation stream closed") from exc
        if msg.type is aiohttp.WSMsgType.TEXT:
            data = msg.data
            if type(data) is not str:
                raise ExchangeUnavailable("liquidation stream closed")
            return data
        raise ExchangeUnavailable("liquidation stream closed")

    async def close(self) -> None:
        if not self._ws.closed:
            await self._ws.close()


@dataclass
class _SessionState:
    """Стан одного з'єднання. `awaiting` — символи без відповіді на підписку; порожній — сесія усталилась."""

    started_mono: int
    awaiting: set[str]
    subscribed: set[str] = field(default_factory=set)
    last_rx_mono: int = 0

    def __post_init__(self) -> None:
        self.last_rx_mono = self.started_mono

    @property
    def settled(self) -> bool:
        return not self.awaiting


def _control(body: dict[str, object], op: str, symbols: Mapping[str, str]) -> LiquidationMessage:
    if op == "subscribe":
        return _subscribe_reply(body, symbols)
    if op in ("ping", "pong") and (op == "pong" or body.get("ret_msg") == "pong" or body.get("success") is True):
        return LiquidationMessage("pong")
    return LiquidationMessage("ignore")


def _subscribe_reply(body: dict[str, object], symbols: Mapping[str, str]) -> LiquidationMessage:
    topic = body.get("req_id")
    if type(topic) is not str or not topic.startswith(TOPIC_PREFIX):
        return LiquidationMessage("ignore")
    canonical = symbols.get(topic[len(TOPIC_PREFIX) :])
    if canonical is None:
        return LiquidationMessage("ignore")
    if body.get("success") is True and not _failed_topic(body, topic):
        return LiquidationMessage("subscribed", symbol=canonical)
    # `success: false`, тема у `failTopics` або дивна відповідь — символу не віримо, доки нова сесія не підтвердить.
    ret_msg = body.get("ret_msg")
    detail = ret_msg[:_DETAIL_CHARS] if type(ret_msg) is str else ""
    return LiquidationMessage("rejected", symbol=canonical, detail=detail)


def _failed_topic(body: dict[str, object], topic: str) -> bool:
    data = body.get("data")
    if type(data) is not dict:
        return False
    failed = data.get("failTopics")
    return type(failed) is list and topic in failed


def _event(item: object, *, exchange_id: str, canonical: str) -> LiquidationEvent | None:
    if type(item) is not dict:
        return None
    if item.get("s") != exchange_id:
        return None
    side_raw = item.get("S")
    if type(side_raw) is not str:
        return None
    side = _SIDE.get(side_raw)
    if side is None:
        return None
    time_ms = _optional_ms(item.get("T"))
    qty = _positive_decimal(item.get("v"))
    price = _positive_decimal(item.get("p"))
    if time_ms is None or qty is None or price is None:
        return None
    return LiquidationEvent(symbol=canonical, position_side=side, time_ms=time_ms, qty=qty, price=price)


def _optional_ms(raw: object) -> int | None:
    if type(raw) is bool:
        return None
    if type(raw) is int and raw >= 0 and raw.bit_length() <= 53:
        return raw
    if type(raw) is str and raw.isdigit() and len(raw) <= _MS_DIGITS:
        return int(raw)
    return None


def _positive_decimal(raw: object) -> Decimal | None:
    if type(raw) is not str or raw == "" or len(raw) > _DECIMAL_CHARS or raw != raw.strip():
        return None
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None
    if not value.is_finite() or value <= 0:
        return None
    return value


async def _receive(
    socket: LiquidationSocket,
    *,
    symbol_map: Mapping[str, str],
    sink: LiquidationSink,
    stop: asyncio.Event,
    state: _SessionState,
    on_progress: Progress | None,
) -> None:
    # Події символу, чия підписка ще без відповіді: книга їх однаково відкине, якщо година вже йшла, але порядок
    # «підписка → події» зберігається.
    early: dict[str, list[LiquidationEvent]] = {}
    stopped = asyncio.ensure_future(stop.wait())
    try:
        while not stop.is_set():
            raw = await _next_frame(socket, stopped)
            if raw is None:
                return
            state.last_rx_mono = monotonic_ms()
            message = parse_liquidation_message(raw, symbols=symbol_map)
            if message.kind == "subscribed":
                _confirm(message.symbol, sink=sink, state=state, early=early)
            elif message.kind == "rejected":
                _reject(message, state=state, early=early)
                sink.note_alive()
            elif message.kind == "events":
                live: list[LiquidationEvent] = []
                for event in message.events:
                    if event.symbol in state.subscribed:
                        live.append(event)
                    elif event.symbol in state.awaiting:
                        early.setdefault(event.symbol, []).append(event)
                sink.accept(live)
            elif message.kind == "unusable":
                sink.mark_unusable(message.exchange_ts_ms)
            else:
                sink.note_alive()
            if on_progress is not None:
                on_progress()
            _LOG.debug("liquidation_frame", kind=message.kind, events=len(message.events))
    finally:
        stopped.cancel()
        await asyncio.wait((stopped,))


def _confirm(
    symbol: str | None, *, sink: LiquidationSink, state: _SessionState, early: dict[str, list[LiquidationEvent]]
) -> None:
    if symbol is None or symbol not in state.awaiting:
        sink.note_alive()  # повторна відповідь на ту саму підписку
        return
    state.awaiting.discard(symbol)
    state.subscribed.add(symbol)
    sink.subscribed(symbol, now_ms())
    pending = early.pop(symbol, None)
    if pending:
        sink.accept(pending)


def _reject(message: LiquidationMessage, *, state: _SessionState, early: dict[str, list[LiquidationEvent]]) -> None:
    symbol = message.symbol
    _LOG.error(
        "liquidation_topic_rejected",
        symbol=symbol,
        detail=message.detail,
        hint="символ не збирається до наступного з'єднання; перевірте RESEARCH_SYMBOLS",
    )
    if symbol is None or symbol in state.subscribed:
        # Відмова для вже підтвердженої теми — стан сесії невідомий.
        raise ExchangeUnavailable("liquidation subscribe rejected")
    if symbol not in state.awaiting:
        return
    state.awaiting.discard(symbol)
    early.pop(symbol, None)
    if not state.awaiting and not state.subscribed:
        raise ExchangeUnavailable("liquidation subscribe rejected")


async def _next_frame(socket: LiquidationSocket, stopped: asyncio.Future[Any]) -> str | None:
    """Наступний кадр або `None`, щойно виставили `stop`.

    У тихому потоці кадру можна чекати хвилинами, а після `stop` ping уже не йде — без цього сесія не завершилась
    би до першої ліквідації. Кадр, що прийшов разом зі `stop`, ще обробляється. Перерване очікування — завжди
    `None`, хоч би чим воно скінчилось: зупинка не виглядає як обрив.
    """
    receiving = asyncio.ensure_future(socket.receive_text())
    interrupted = False
    try:
        await asyncio.wait((receiving, stopped), return_when=asyncio.FIRST_COMPLETED)
    finally:
        if not receiving.done():
            interrupted = True
            receiving.cancel()
            await asyncio.wait((receiving,))
        if not receiving.cancelled():
            receiving.exception()  # прочитано: при скасуванні ззовні asyncio не лаятиметься в лог
    if interrupted:
        return None
    return receiving.result()


async def _watch(
    socket: LiquidationSocket,
    *,
    stop: asyncio.Event,
    sleep: Sleep,
    state: _SessionState,
    ping_interval_ms: int,
    stale_after_ms: int,
    subscribe_timeout_ms: int,
) -> None:
    next_ping = state.started_mono + ping_interval_ms
    while not stop.is_set():
        now = monotonic_ms()
        if not state.settled and now - state.started_mono >= subscribe_timeout_ms:
            _LOG.warning("liquidation_subscribe_timeout", awaiting=sorted(state.awaiting))
            raise ExchangeUnavailable("liquidation subscribe timeout")
        if state.settled and now - state.last_rx_mono >= stale_after_ms:
            raise ExchangeUnavailable("liquidation stream stale")
        wake = next_ping
        if not state.settled:
            wake = min(wake, state.started_mono + subscribe_timeout_ms)
        else:
            wake = min(wake, state.last_rx_mono + stale_after_ms)
        if wake > now:
            await _wait_stop(stop, (wake - now) / 1000, sleep)
            continue
        await socket.send_text(_PING)
        next_ping = monotonic_ms() + ping_interval_ms


async def _wait_stop(stop: asyncio.Event, seconds: float, sleep: Sleep) -> None:
    if stop.is_set() or seconds <= 0:
        return
    sleeper = asyncio.ensure_future(sleep(seconds))
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (sleeper, stopper):
            if not task.done():
                task.cancel()
        for task in (sleeper, stopper):
            with contextlib.suppress(asyncio.CancelledError):
                await task


def _require_ms(name: str, value: int, *, minimum: int) -> int:
    if type(value) is not int or isinstance(value, bool) or value < minimum:
        raise ValueError(f"{name} має бути int мс ≥ {minimum}")
    return value
