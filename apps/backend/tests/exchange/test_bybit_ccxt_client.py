"""`CcxtBybitPublicClient` поверх справжнього CCXT без мережі (T1.3).

Підмінена лише сесія aiohttp: URL будує `sign()` CCXT, далі `fetch()` і `handle_errors()` — як у проді.
Тому тут видно те, чого не видно через `ScriptedClient`: кодування query і обробку транспортних збоїв.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import aiohttp
import pytest
from structlog.testing import capture_logs

from app.core.clock import FakeClock
from app.exchange.bybit import (
    PATH_INSTRUMENTS,
    PATH_KLINE,
    PATH_TIME,
    BybitAdapter,
    CcxtBybitPublicClient,
    ExchangeMode,
)
from app.exchange.errors import ExchangeTimeout, ExchangeUnavailable
from tests.exchange.test_bybit_public import _JITTER, SpyLimiter, _instruments, _perp, _sleep_for, _time_body

_TESTNET_ORIGIN = "https://api-testnet.bybit.com"


@dataclass
class Reply:
    """Відповідь фейкової сесії. `body`: dict — JSON, str — як є. `gate` тримає відповідь до `set()`."""

    status: int = 200
    body: object = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    gate: asyncio.Event | None = None
    error: BaseException | None = None


class _PendingRequest:
    """Те, що повертає `session.get(...)` у aiohttp: async context manager з відповіддю."""

    def __init__(self, reply: Reply) -> None:
        self._reply = reply
        self.status = reply.status
        self.reason = "OK" if reply.status == 200 else "ERR"
        # Як у Bybit: JSON — `application/json`, сторінка помилки CDN — текст.
        content_type = "text/plain" if isinstance(reply.body, str) else "application/json"
        self.headers = {"Content-Type": content_type, **reply.headers}

    async def __aenter__(self) -> _PendingRequest:
        if self._reply.gate is not None:
            await self._reply.gate.wait()
        if self._reply.error is not None:
            raise self._reply.error
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def text(self, errors: str = "strict") -> str:
        body = self._reply.body
        return body if isinstance(body, str) else json.dumps(body)


class FakeSession:
    """Мінімальна aiohttp-сесія: записує URL, відповідь вибирає `route`."""

    def __init__(self, route: Callable[[str], Reply]) -> None:
        self._route = route
        self.urls: list[str] = []
        # CCXT додає заголовки сесії до кожного запиту (`prepare_request_headers`).
        self.headers: dict[str, str] = {}

    def get(self, url: object, **_kwargs: object) -> _PendingRequest:
        text = str(url)
        self.urls.append(text)
        return _PendingRequest(self._route(text))

    async def close(self) -> None:
        return None


@asynccontextmanager
async def _client(route: Callable[[str], Reply]) -> AsyncIterator[tuple[CcxtBybitPublicClient, FakeSession]]:
    session = FakeSession(route)
    client = CcxtBybitPublicClient(ExchangeMode.TESTNET, session=session)
    try:
        yield client, session
    finally:
        await client.aclose()


async def _yield_until(predicate: Callable[[], bool], *, rounds: int = 200) -> bool:
    """Віддати цикл подій кілька разів (без реального часу), доки умова не стане істинною."""
    for _ in range(rounds):
        if predicate():
            return True
        await asyncio.sleep(0)
    return predicate()


@pytest.mark.asyncio
async def test_response_status_headers_and_json_body_reach_the_adapter() -> None:
    def route(_url: str) -> Reply:
        return Reply(body=_time_body(), headers={"X-Bapi-Limit": "600"})

    async with _client(route) as (client, session):
        response = await client.get(PATH_TIME, {})
    assert session.urls == [f"{_TESTNET_ORIGIN}/{PATH_TIME}"]
    assert response.status == 200
    assert response.body == _time_body()
    assert response.headers["X-Bapi-Limit"] == "600"


@pytest.mark.asyncio
async def test_http_error_body_is_returned_not_raised() -> None:
    """Класифікує відповідь адаптер (ліміт, 5xx, невідомий код), а не `handle_errors` CCXT."""

    def route(_url: str) -> Reply:
        return Reply(status=403, body="Access too frequent. Please try again in 5 minutes.")

    async with _client(route) as (client, _session):
        response = await client.get(PATH_TIME, {})
    assert response.status == 403
    assert response.body == "Access too frequent. Please try again in 5 minutes."


@pytest.mark.asyncio
async def test_cursor_reaches_the_url_percent_encoded_exactly_once(fake_clock: FakeClock) -> None:
    """Bybit public `sign()` CCXT шле значення query як є (`rawencode`). Курсор має піти закодованим один раз.

    Testnet (2026-10-29): `cursor=first=…&last=…` (без кодування) — retCode 10001; закодований — наступна сторінка.
    """
    pages = [
        _instruments([_perp()], cursor="first%3DBTCUSDT%26last%3DBTCUSDT"),
        _instruments([_perp(symbol="ETHUSDT", base="ETH")], cursor=""),
    ]

    def route(_url: str) -> Reply:
        return Reply(body=pages.pop(0))

    session = FakeSession(route)
    limiter = SpyLimiter(fake_clock)
    adapter = BybitAdapter(
        mode=ExchangeMode.TESTNET,
        limiter=limiter,
        client=CcxtBybitPublicClient(ExchangeMode.TESTNET, session=session),
        sleeper=_sleep_for(fake_clock),
        jitter=_JITTER,
    )
    try:
        instruments = await adapter.fetch_instruments()
    finally:
        await adapter.aclose()
        await limiter.aclose()
    assert [item.symbol for item in instruments] == ["BTC/USDT:USDT", "ETH/USDT:USDT"]
    assert session.urls[0] == f"{_TESTNET_ORIGIN}/{PATH_INSTRUMENTS}?category=linear&limit=1000"
    assert session.urls[1] == (
        f"{_TESTNET_ORIGIN}/{PATH_INSTRUMENTS}?category=linear&limit=1000&cursor=first%3DBTCUSDT%26last%3DBTCUSDT"
    )


@pytest.mark.asyncio
async def test_plain_query_values_are_sent_unchanged() -> None:
    def route(_url: str) -> Reply:
        return Reply(body={"retCode": 0, "result": {}, "time": 1})

    query = {"category": "linear", "symbol": "1000PEPEUSDT", "interval": "D", "start": "0", "end": "86399999"}
    async with _client(route) as (client, session):
        await client.get(PATH_KLINE, query)
    assert session.urls == [
        f"{_TESTNET_ORIGIN}/{PATH_KLINE}?category=linear&symbol=1000PEPEUSDT&interval=D&start=0&end=86399999"
    ]


@pytest.mark.asyncio
async def test_concurrent_requests_do_not_queue_behind_each_other_and_keep_their_own_response() -> None:
    """Пріоритет вирішує лімітер. Клієнт не ставить запит `RECONCILE` у FIFO за повільним запитом даних."""
    gate = asyncio.Event()

    def route(url: str) -> Reply:
        if PATH_KLINE in url:
            return Reply(body={"which": "slow"}, headers={"X-Probe": "slow"}, gate=gate)
        return Reply(status=503, body="fast", headers={"X-Probe": "fast"})

    async with _client(route) as (client, session):
        slow = asyncio.create_task(client.get(PATH_KLINE, {"category": "linear"}))
        fast: asyncio.Task[object] | None = None
        try:
            assert await _yield_until(lambda: len(session.urls) == 1)
            fast = asyncio.create_task(client.get(PATH_TIME, {}))
            finished_first = await _yield_until(fast.done)
        finally:
            gate.set()
        slow_response = await slow
        assert fast is not None
        fast_response = await fast
    assert finished_first, "швидкий запит чекав на повільний"
    assert fast_response.status == 503  # type: ignore[attr-defined]
    assert fast_response.body == "fast"  # type: ignore[attr-defined]
    assert fast_response.headers["X-Probe"] == "fast"  # type: ignore[attr-defined]
    assert slow_response.status == 200
    assert slow_response.body == {"which": "slow"}
    assert slow_response.headers["X-Probe"] == "slow"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected", "ccxt_type"),
    [
        (TimeoutError(), ExchangeTimeout, "RequestTimeout"),
        (aiohttp.ClientConnectionError("reset"), ExchangeUnavailable, "ExchangeNotAvailable"),
        (aiohttp.ClientPayloadError("truncated"), ExchangeUnavailable, "ExchangeError"),
    ],
)
async def test_transport_errors_become_domain_errors_and_are_logged(
    error: BaseException, expected: type[Exception], ccxt_type: str
) -> None:
    def route(_url: str) -> Reply:
        return Reply(error=error)

    async with _client(route) as (client, _session):
        with capture_logs() as logs, pytest.raises(expected) as caught:
            await client.get(PATH_TIME, {})
    assert type(caught.value) is expected
    assert "bybit.com" not in str(caught.value)
    transport = [entry for entry in logs if entry["event"] == "bybit_transport_error"]
    assert transport == [
        {"event": "bybit_transport_error", "log_level": "warning", "path": PATH_TIME, "error_type": ccxt_type}
    ]


@pytest.mark.asyncio
async def test_unexpected_error_is_not_disguised_as_a_transport_failure() -> None:
    """Помилка в коді — не «біржа недоступна»: інакше адаптер мовчки повторював би її як збій мережі."""

    def route(_url: str) -> Reply:
        return Reply(error=RuntimeError("bug"))

    async with _client(route) as (client, _session):
        with pytest.raises(RuntimeError, match="bug"):
            await client.get(PATH_TIME, {})


@pytest.mark.asyncio
async def test_closed_client_does_not_send() -> None:
    def route(_url: str) -> Reply:
        return Reply(body=_time_body())

    async with _client(route) as (client, session):
        await client.aclose()
        await client.aclose()
        with pytest.raises(ExchangeUnavailable):
            await client.get(PATH_TIME, {})
    assert session.urls == []


@pytest.mark.asyncio
async def test_close_lets_the_request_in_flight_finish() -> None:
    gate = asyncio.Event()

    def route(_url: str) -> Reply:
        return Reply(body=_time_body(), gate=gate)

    async with _client(route) as (client, session):
        pending = asyncio.create_task(client.get(PATH_TIME, {}))
        assert await _yield_until(lambda: len(session.urls) == 1)
        closing = asyncio.create_task(client.aclose())
        await _yield_until(closing.done, rounds=20)
        with pytest.raises(ExchangeUnavailable):
            await client.get(PATH_TIME, {})
        gate.set()
        response = await pending
        # Запобіжник від зависання (лічильник запитів у польоті), не очікування часу.
        await asyncio.wait_for(closing, timeout=5)
    assert response.status == 200
    assert response.body == _time_body()
    assert len(session.urls) == 1
