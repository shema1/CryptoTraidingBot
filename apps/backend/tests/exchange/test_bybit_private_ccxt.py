"""`CcxtBybitPrivateClient` поверх справжнього CCXT без мережі (T3.2).

Підмінена лише сесія aiohttp: URL, заголовки й підпис будує `sign()` CCXT, далі `fetch()` — як у проді. Тест
перераховує HMAC-SHA256 за документацією Bybit v5 (`timestamp + apiKey + recvWindow + (query | JSON-тіло)`) і
порівнює з `X-BAPI-SIGN`: так видно, що підписано рівно те, що пішло в мережу (зокрема курсор, закодований раз).
Ключі тут — вигадані рядки, не справжні.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import aiohttp
import pytest
from pydantic import SecretStr, ValidationError

from app.core.config import RECV_WINDOW_MAX_MS, RECV_WINDOW_MIN_MS
from app.exchange.bybit import ExchangeMode
from app.exchange.bybit_codes import PATH_ACCOUNT_INFO, PATH_ORDER_CREATE, PATH_ORDER_REALTIME, PRIVATE_ROUTES
from app.exchange.bybit_private import PRIVATE_HOSTS, BybitCredentials, CcxtBybitPrivateClient
from app.exchange.errors import AdapterConfigError, ExchangeUnavailable
from tests.exchange.test_bybit_ccxt_client import Reply, _PendingRequest

_KEY = "TESTKEYtestkey0123"
_SECRET = "TESTSECRETtestsecret0123456789"  # noqa: S105 — вигаданий рядок для перевірки підпису
_RECV_WINDOW_MS = 5_000
_TESTNET = f"https://{PRIVATE_HOSTS[ExchangeMode.TESTNET]}"


@dataclass
class Sent:
    """Один запит, як його отримала сесія aiohttp від CCXT."""

    method: str
    url: str
    headers: dict[str, str]
    body: bytes | None


@dataclass
class FakeSession:
    """aiohttp-сесія з `get` і `post`: записує метод, URL, заголовки й тіло."""

    route: Callable[[Sent], Reply]
    sent: list[Sent] = field(default_factory=list)
    # CCXT додає заголовки сесії до кожного запиту (`prepare_request_headers`).
    headers: dict[str, str] = field(default_factory=dict)

    def _request(self, method: str, url: object, kwargs: dict[str, object]) -> _PendingRequest:
        headers = kwargs.get("headers")
        data = kwargs.get("data")
        assert isinstance(headers, dict)
        assert data is None or isinstance(data, bytes)
        request = Sent(method=method, url=str(url), headers={str(k): str(v) for k, v in headers.items()}, body=data)
        self.sent.append(request)
        return _PendingRequest(self.route(request))

    def get(self, url: object, **kwargs: object) -> _PendingRequest:
        return self._request("GET", url, kwargs)

    def post(self, url: object, **kwargs: object) -> _PendingRequest:
        return self._request("POST", url, kwargs)

    async def close(self) -> None:
        return None


def _ok(_request: Sent) -> Reply:
    return Reply(body={"retCode": 0, "retMsg": "OK", "result": {}, "time": 1})


def _credentials() -> BybitCredentials:
    return BybitCredentials(api_key=SecretStr(_KEY), api_secret=SecretStr(_SECRET))


@asynccontextmanager
async def _client(
    route: Callable[[Sent], Reply] = _ok, *, mode: ExchangeMode = ExchangeMode.TESTNET
) -> AsyncIterator[tuple[CcxtBybitPrivateClient, FakeSession]]:
    session = FakeSession(route)
    client = CcxtBybitPrivateClient(mode, _credentials(), recv_window_ms=_RECV_WINDOW_MS, session=session)
    try:
        yield client, session
    finally:
        await client.aclose()


def _expected_sign(timestamp: str, payload: str) -> str:
    message = f"{timestamp}{_KEY}{_RECV_WINDOW_MS}{payload}"
    return hmac.new(_SECRET.encode(), message.encode(), hashlib.sha256).hexdigest()


def _assert_signed(request: Sent, payload: str) -> None:
    headers = request.headers
    assert headers["X-BAPI-API-KEY"] == _KEY
    assert headers["X-BAPI-RECV-WINDOW"] == str(_RECV_WINDOW_MS)
    timestamp = headers["X-BAPI-TIMESTAMP"]
    assert timestamp.isdigit()
    assert len(timestamp) == 13  # мілісекунди
    assert headers["X-BAPI-SIGN"] == _expected_sign(timestamp, payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", list(ExchangeMode))
async def test_each_mode_signs_requests_for_its_own_private_host_only(mode: ExchangeMode) -> None:
    async with _client(mode=mode) as (client, session):
        await client.request(PATH_ACCOUNT_INFO, {})
    origin = f"https://{PRIVATE_HOSTS[mode]}"
    assert client.private_origin == origin
    assert [request.method for request in session.sent] == ["GET"]
    assert session.sent[0].url.split("?")[0] == f"{origin}/{PATH_ACCOUNT_INFO}"


@pytest.mark.asyncio
async def test_get_is_signed_over_exactly_the_query_that_is_sent() -> None:
    async with _client() as (client, session):
        response = await client.request(
            PATH_ORDER_REALTIME, {"category": "linear", "symbol": "BTCUSDT", "orderLinkId": "tbHOST1abc"}
        )
    assert response.status == 200
    [request] = session.sent
    query = "category=linear&symbol=BTCUSDT&orderLinkId=tbHOST1abc"
    assert request.url == f"{_TESTNET}/{PATH_ORDER_REALTIME}?{query}"
    assert request.body is None
    _assert_signed(request, query)


@pytest.mark.asyncio
async def test_cursor_is_percent_encoded_once_and_the_signature_covers_the_encoded_form() -> None:
    """Логічний курсор (як його тримає адаптер після `unquote`) іде в URL закодованим рівно раз.

    Підпис Bybit — над рядком query, як він прийшов на сервер, тож підписано теж закодовану форму.
    """
    async with _client() as (client, session):
        await client.request(PATH_ORDER_REALTIME, {"category": "linear", "cursor": "page_args=abc&symbol=BTCUSDT"})
    [request] = session.sent
    query = "category=linear&cursor=page_args%3Dabc%26symbol%3DBTCUSDT"
    assert request.url == f"{_TESTNET}/{PATH_ORDER_REALTIME}?{query}"
    _assert_signed(request, query)


@pytest.mark.asyncio
async def test_post_sends_compact_json_body_and_signs_it() -> None:
    params: dict[str, str | bool] = {
        "category": "linear",
        "symbol": "BTCUSDT",
        "side": "Buy",
        "orderType": "Limit",
        "qty": "0.001",
        "price": "65000.5",
        "orderLinkId": "tbHOST1abc",
        "reduceOnly": False,
    }
    async with _client() as (client, session):
        await client.request(PATH_ORDER_CREATE, params)
    [request] = session.sent
    assert request.method == "POST"
    assert request.url == f"{_TESTNET}/{PATH_ORDER_CREATE}"
    assert request.headers["Content-Type"] == "application/json"
    assert request.body is not None
    body = request.body.decode()
    assert json.loads(body) == params
    assert '"reduceOnly":false' in body  # bool — JSON-літерал, не рядок "False"
    _assert_signed(request, body)


@pytest.mark.asyncio
async def test_every_private_route_uses_its_declared_http_method() -> None:
    async with _client() as (client, session):
        for path, (http_method, _ccxt_method) in PRIVATE_ROUTES.items():
            await client.request(path, {"category": "linear"})
            assert session.sent[-1].method == http_method, path
            assert session.sent[-1].url.split("?")[0] == f"{_TESTNET}/{path}"


@pytest.mark.asyncio
async def test_unknown_path_and_wrong_value_types_are_refused_before_the_network() -> None:
    async with _client() as (client, session):
        with pytest.raises(AdapterConfigError):
            await client.request("v5/asset/withdraw/create", {})
        with pytest.raises(TypeError):
            await client.request(PATH_ORDER_REALTIME, {"openOnly": True})
        with pytest.raises(TypeError):
            await client.request(PATH_ORDER_CREATE, {"qty": 1})  # type: ignore[dict-item]
    assert session.sent == []


@pytest.mark.asyncio
async def test_key_and_secret_never_reach_log_records_or_error_messages(caplog: pytest.LogCaptureFixture) -> None:
    """Сирі записи `logging` (до маскування `app.core.logging`) — теж без ключа й підпису."""

    def route(request: Sent) -> Reply:
        if request.method == "POST":
            return Reply(error=aiohttp.ClientConnectionError("reset"))
        return _ok(request)

    caplog.set_level(logging.DEBUG)
    errors: list[str] = []
    async with _client(route) as (client, session):
        await client.request(PATH_ORDER_REALTIME, {"category": "linear"})
        with pytest.raises(ExchangeUnavailable) as caught:
            await client.request(PATH_ORDER_CREATE, {"category": "linear"})
        errors.append(str(caught.value))
        errors.append(repr(caught.value))
    signatures = {request.headers["X-BAPI-SIGN"] for request in session.sent}
    assert len(signatures) == 2
    texts = [record.getMessage() for record in caplog.records] + errors
    texts += [repr(_credentials()), str(_credentials())]
    for text in texts:
        assert _KEY not in text
        assert _SECRET not in text
        for signature in signatures:
            assert signature not in text


def test_credentials_refuse_blank_or_padded_values() -> None:
    for key, secret in [("", _SECRET), (_KEY, ""), (f" {_KEY}", _SECRET), (_KEY, f"{_SECRET}\n")]:
        with pytest.raises(ValidationError) as caught:
            BybitCredentials(api_key=SecretStr(key), api_secret=SecretStr(secret))
        assert _SECRET not in str(caught.value)
        assert _KEY not in str(caught.value)


@pytest.mark.parametrize("recv_window_ms", [RECV_WINDOW_MIN_MS - 1, RECV_WINDOW_MAX_MS + 1, True, 5_000.0])
def test_recv_window_outside_the_config_range_is_refused(recv_window_ms: object) -> None:
    with pytest.raises(AdapterConfigError):
        CcxtBybitPrivateClient(
            ExchangeMode.TESTNET,
            _credentials(),
            recv_window_ms=recv_window_ms,  # type: ignore[arg-type]
            session=FakeSession(_ok),
        )


def test_wrong_mode_or_credentials_type_is_refused() -> None:
    with pytest.raises(AdapterConfigError):
        CcxtBybitPrivateClient("testnet", _credentials(), recv_window_ms=_RECV_WINDOW_MS)  # type: ignore[arg-type]
    with pytest.raises(AdapterConfigError):
        CcxtBybitPrivateClient(
            ExchangeMode.TESTNET,
            {"api_key": _KEY, "api_secret": _SECRET},  # type: ignore[arg-type]
            recv_window_ms=_RECV_WINDOW_MS,
        )


@pytest.mark.asyncio
async def test_closed_client_does_not_send() -> None:
    async with _client() as (client, session):
        await client.aclose()
        with pytest.raises(ExchangeUnavailable):
            await client.request(PATH_ACCOUNT_INFO, {})
    assert session.sent == []
