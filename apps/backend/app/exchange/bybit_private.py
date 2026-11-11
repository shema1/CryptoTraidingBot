"""Підписаний (приватний) REST Bybit v5 через CCXT (ТЗ §3, §14; T3.2).

Ключі — `BybitCredentials` (`SecretStr`): значення розкриваються лише один раз, при створенні клієнта CCXT, і
ніколи не потрапляють у логи, повідомлення винятків чи `repr`. Звідки їх брати (`.env`, `Settings`) — T3.6.

Хост приватного API — за режимом, точним збігом (`PRIVATE_HOSTS`): testnet — `api-testnet.bybit.com`
(`set_sandbox_mode`), demo — `api-demo.bybit.com` (`enable_demo_trading`), live — `api.bybit.com`. Інший хост —
`AdapterConfigError` до будь-якого запиту.

Підпис робить `sign()` CCXT (v5: заголовки `X-BAPI-API-KEY`, `X-BAPI-TIMESTAMP`, `X-BAPI-RECV-WINDOW`,
`X-BAPI-SIGN` = HMAC-SHA256 від `timestamp + key + recvWindow + (query | JSON-тіло)`). Час підпису — локальний
годинник (`adjustForTimeDifference=False`): розбіжність з біржею стереже `ClockGuard` (T1.6), а відповідь `10002`
адаптер передає йому. GET-значення CCXT шле як є (`rawencode`), тож їх кодує цей клієнт (`_wire_query`, як для
публічних запитів); POST — JSON. `verbose=False`: CCXT у verbose-режимі логував би заголовки разом із ключем.
З тієї ж причини DEBUG-записи CCXT (`Request: <заголовки> <тіло>`, `Response: …`) приватний клієнт не пише взагалі
(`_CcxtLoggerWithoutDebug`): маскування в `app.core.logging` їх теж закрило б, але ключ не має доходити навіть до
стороннього хендлера, підключеного до запуску `configure_logging()`.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Final, Protocol

from pydantic import BaseModel, ConfigDict, SecretStr, field_validator

from app.core.config import RECV_WINDOW_MAX_MS, RECV_WINDOW_MIN_MS
from app.exchange.bybit import (
    HTTP_TIMEOUT_MS,
    BybitRawResponse,
    ExchangeMode,
    _CcxtTransport,
    _load_ccxt,
    _wire_query,
)
from app.exchange.bybit_codes import PRIVATE_ROUTES
from app.exchange.errors import AdapterConfigError, ExchangeUnavailable

# Точний хост приватного REST для кожного режиму. Live — лише mainnet; demo — окремий хост Demo Trading.
PRIVATE_HOSTS: Final[Mapping[ExchangeMode, str]] = {
    ExchangeMode.TESTNET: "api-testnet.bybit.com",
    ExchangeMode.DEMO: "api-demo.bybit.com",
    ExchangeMode.LIVE: "api.bybit.com",
}


class BybitCredentials(BaseModel):
    """Ключ API Bybit (лише торгівля, без права виводу — перевірка дозволів — T3.6). `repr` значень не показує."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    api_key: SecretStr
    api_secret: SecretStr

    @field_validator("api_key", "api_secret")
    @classmethod
    def _not_blank(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if raw == "" or raw != raw.strip():
            raise ValueError("ключ Bybit порожній або з пробілами по краях")
        return value


class BybitPrivateClient(Protocol):
    """Транспорт підписаних запитів. Тести підставляють запис відповідей; прод — `CcxtBybitPrivateClient`."""

    @property
    def private_origin(self) -> str:
        """Схема й хост приватного REST без шляху, напр. `https://api-testnet.bybit.com`."""
        ...

    async def request(self, path: str, params: Mapping[str, str | bool]) -> BybitRawResponse:
        """Один підписаний запит на шлях з `bybit_codes.PRIVATE_ROUTES` (метод — з маршруту).

        Мережева помилка — `ExchangeTimeout` / `ExchangeUnavailable`, не порожнє тіло. GET приймає лише рядки.
        """
        ...

    async def aclose(self) -> None:
        """Закрити сесію. Повторний виклик — нічого."""
        ...


class CcxtBybitPrivateClient(_CcxtTransport):
    """Приватний REST через CCXT async. Власний rate limit CCXT вимкнено — черга в `RateLimiter`."""

    def __init__(
        self,
        mode: ExchangeMode,
        credentials: BybitCredentials,
        *,
        recv_window_ms: int,
        session: object | None = None,
    ) -> None:
        """`session` — лише для тестів: aiohttp-сумісна сесія замість мережевої (CCXT її не закриває)."""
        if type(mode) is not ExchangeMode:
            raise AdapterConfigError("mode має бути ExchangeMode")
        if type(credentials) is not BybitCredentials:
            raise AdapterConfigError("credentials мають бути BybitCredentials")
        if (
            type(recv_window_ms) is not int
            or isinstance(recv_window_ms, bool)
            or not RECV_WINDOW_MIN_MS <= recv_window_ms <= RECV_WINDOW_MAX_MS
        ):
            raise AdapterConfigError(f"recv_window_ms має бути в [{RECV_WINDOW_MIN_MS}, {RECV_WINDOW_MAX_MS}] мс")
        ccxt = _load_ccxt()
        config: dict[str, object] = {
            "apiKey": credentials.api_key.get_secret_value(),
            "secret": credentials.api_secret.get_secret_value(),
            "enableRateLimit": False,
            "timeout": HTTP_TIMEOUT_MS,
            "verbose": False,
            "options": {
                "defaultType": "swap",
                "defaultSubType": "linear",
                "adjustForTimeDifference": False,
                "recvWindow": recv_window_ms,
            },
        }
        if session is not None:
            config["session"] = session
        exchange: Any = ccxt.bybit(config)
        exchange.enableRateLimit = False
        exchange.verbose = False
        exchange.logger = _CcxtLoggerWithoutDebug(exchange.logger)
        if mode is ExchangeMode.TESTNET:
            exchange.set_sandbox_mode(True)
        elif mode is ExchangeMode.DEMO:
            exchange.enable_demo_trading(True)
        origin = _private_origin(exchange)
        _assert_private_origin(mode, origin)
        self._origin = origin
        super().__init__(exchange, ccxt)

    @property
    def private_origin(self) -> str:
        return self._origin

    async def request(self, path: str, params: Mapping[str, str | bool]) -> BybitRawResponse:
        if self._closed:
            raise ExchangeUnavailable("клієнт Bybit закрито")
        route = PRIVATE_ROUTES.get(path)
        if route is None:
            raise AdapterConfigError(f"немає приватного методу для {path}")
        http_method, ccxt_method = route
        wire: Mapping[str, object]
        if http_method == "GET":
            if any(type(value) is not str for value in params.values()):
                raise TypeError("query приватного GET Bybit — лише рядки")
            wire = _wire_query({key: str(value) for key, value in params.items()})
        else:
            for key, value in params.items():
                if type(key) is not str or type(value) not in (str, bool):
                    raise TypeError("тіло приватного POST Bybit — рядки й bool")
            wire = dict(params)
        return await self._send(ccxt_method, wire, path=path)


class _CcxtLoggerWithoutDebug(logging.LoggerAdapter[logging.Logger]):
    """Логер екземпляра CCXT приватного клієнта: DEBUG (заголовки з ключем і підписом, тіла відповідей) — ніколи."""

    def isEnabledFor(self, level: int) -> bool:
        # `LoggerAdapter.log` перевіряє саме цей метод перед кожним записом.
        return level > logging.DEBUG and self.logger.isEnabledFor(level)


def _private_origin(exchange: Any) -> str:
    urls: object = exchange.urls
    hostname: object = exchange.hostname
    if type(urls) is not dict or type(hostname) is not str or hostname == "":
        raise AdapterConfigError("CCXT не дав приватний URL Bybit")
    api = urls.get("api")
    if type(api) is not dict:
        raise AdapterConfigError("CCXT не дав приватний URL Bybit")
    template = api.get("private")
    if type(template) is not str or "{hostname}" not in template:
        raise AdapterConfigError("CCXT не дав приватний URL Bybit")
    return template.replace("{hostname}", hostname)


def _assert_private_origin(mode: ExchangeMode, origin: str) -> None:
    """Приватний хост — рівно той, що для режиму: ключ demo чи testnet не піде на mainnet і навпаки."""
    expected = f"https://{PRIVATE_HOSTS[mode]}"
    if origin != expected:
        raise AdapterConfigError(f"приватний URL Bybit для {mode.value} має бути {expected}")
