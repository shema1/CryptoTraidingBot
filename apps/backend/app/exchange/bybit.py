"""Адаптер Bybit v5: публічні ринкові дані та `fetch_time` (ТЗ §3, §4; T1.3).

Один клас на demo / testnet / live. Режим змінює лише публічний хост: testnet — `set_sandbox_mode`,
demo і live — публічний mainnet (ТЗ §13: у demo ринкові дані ті самі, що на mainnet; хост `api-demo`
лише для приватних методів, T3.2). `category=linear` підставляється в кожен запит даних; інша категорія —
`AdapterConfigError` до будь-якого вводу-виводу.

Торгові методи протоколу (`place_order`, позиції, приватний WS) — T3.2/T3.5. Перевірка one-way і isolated
теж потребує приватного API і лишається на старті торгового клієнта (T3.2). Цей клас навмисно їх не
оголошує: до T3.2 він не є повним `ExchangeAdapter`.

Ціни й кількості беруться з рядків тіла Bybit (`Decimal(str)`). Unified-методи CCXT віддають `float` —
їх не використовуємо. Усі REST-виклики йдуть через `RateLimiter` (група `MARKET`).

Поля звірені з документацією v5 (context7, 2026-10-29) і відповідями Testnet того ж дня:
- `GET /v5/market/time`: `time` (мс) і `result.timeNano`;
- `GET /v5/market/kline`: список від новіших до старіших, `start`/`end` включні по `startTime`,
  незакрита свічка входить у список (`close` — остання ціна);
- `GET /v5/market/instruments-info`: `contractType=LinearPerpetual` (приклад у доках ще каже `Perpetual`),
  `priceFilter` / `lotSizeFilter` / `leverageFilter`, `launchTime` (мс, рядок), `status`
  (`Trading` / `PreLaunch` / `PendingOpen` / `Delivering` / `Closed` / `Settling`; без фільтра `status`
  біржа віддає й неторговані), курсор `nextPageCursor`;
- `GET /v5/market/funding/history`: `startTime`/`endTime` включні, список від новіших до старіших, сторінка ≤ 200.
Свічка віддається з запасом остаточності `KLINE_FINALITY_MARGIN_MS` після закриття за часом біржі.
TODO(verify-bybit): чи вистачає цього запасу на mainnet (Testnet: остаточна вже через 0,22–0,69 с).

Дані деривативів (T1.9, документація v5 і Testnet 2026-10-30, глибина — `docs/data/derivatives-history.md`):
- `GET /v5/market/open-interest`: `intervalTime` (`1h`, не `60`), сторінка ≤ 200, курсор, список від новіших;
  зберігаємо `openInterest` (сума сторін), не `singleOpenInterest`;
- `GET /v5/market/mark-price-kline`, `index-price-kline`, `premium-index-price-kline`: як kline, але 5 полів
  без обсягу, `interval` як у trade-kline (`60` для 1h), сторінка ≤ 1000, без курсора;
- `GET /v5/market/account-ratio`: `period` (`1h`), сторінка ≤ 500, курсор, `buyRatio`/`sellRatio`.
Незакритий інтервал входить у список (Testnet: поточна година вже в відповіді). `open` premium — close попередньої
години, тож може лежати поза `[low, high]` і віддається як є (рішення 0009). Premium зі зламаним діапазоном години
(`high < low`, `close` поза `[low, high]`) і mark/index із неузгодженим OHLC не віддаються — це прогалина, не
виправлення цін.
"""

from __future__ import annotations

import asyncio
import random
import re
from collections.abc import Awaitable, Callable, Collection, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Final, Protocol, TypeVar, cast
from urllib.parse import quote, unquote

from pydantic import ValidationError

from app.core.clock import now_ms
from app.core.db import SYMBOL_RE
from app.core.logging import get_logger
from app.exchange.errors import (
    AdapterConfigError,
    ExchangeOutcomeUnknown,
    ExchangeTimeout,
    ExchangeUnavailable,
    MalformedResponse,
)
from app.exchange.rate_limit import (
    FIXED_METHOD_PRIORITY as _FIXED_PRIORITY,
)
from app.exchange.rate_limit import (
    EndpointGroup,
    Jitter,
    RateLimiter,
    Sleep,
    limit_breach_from_response,
    parse_limit_headers,
    retry_after_ms_from,
)
from app.exchange.types import (
    Candle,
    DerivativePriceType,
    FundingRate,
    Instrument,
    LongShortRatio,
    MarkIndexCandle,
    OpenInterest,
    PremiumIndex,
    Timeframe,
)

_log = get_logger(__name__)

_T = TypeVar("_T")
_Point = OpenInterest | PremiumIndex | MarkIndexCandle | LongShortRatio
_P = TypeVar("_P", bound=_Point)
_BucketParse = Callable[..., tuple[list[_Point], int, int | None, list[int]]]
_KlineParse = Callable[..., tuple[list[_Point], int, int, list[int]]]

LINEAR_CATEGORY: Final = "linear"
# Сторінки Bybit v5 (документація): kline ≤ 1000, funding ≤ 200, instruments ≤ 1000,
# open interest ≤ 200, long/short ratio ≤ 500.
KLINE_PAGE: Final = 1000
FUNDING_PAGE: Final = 200
INSTRUMENT_PAGE: Final = 1000
OI_PAGE: Final = 200
RATIO_PAGE: Final = 500
# Сторінок курсора всередині одного вікна. Вікно backfill не довше за сторінку, тож 8 — запас на дублі.
_CURSOR_PAGE_CAP: Final = 8
# Запас остаточності kline: свічка віддається, лише коли `close_time_ms + запас ≤ time` відповіді біржі.
# Рішення власника (T1.3, 2026-10-29): 500 мс укладаються в бюджет ТЗ §5.5 (ціль < 1 с, `max_signal_age_ms` 5 с).
# Testnet того ж дня: свічка вже остаточна через 0,22–0,69 с після межі. Mainnet — TODO(verify-bybit).
KLINE_FINALITY_MARGIN_MS: Final = 500
# Захист від курсора, що ніколи не закінчується. 50 × 1000 інструментів — з запасом проти ~1 тис. linear.
_INSTRUMENT_PAGE_CAP: Final = 50
HTTP_TIMEOUT_MS: Final = 10_000
# Скільки транспортних спроб (таймаут, 5xx, обрив) на одне ідемпотентне читання, включно з першою.
READ_ATTEMPTS: Final = 3
READ_BACKOFF_BASE_MS: Final = 200
READ_BACKOFF_CAP_MS: Final = 2_000
# `timeNano` — наносекунди рядком; довший рядок не є міткою часу.
_NANO_DIGITS: Final = 21
_DECIMAL_CHARS: Final = 100
_MSG_CHARS: Final = 120

PATH_TIME: Final = "v5/market/time"
PATH_KLINE: Final = "v5/market/kline"
PATH_INSTRUMENTS: Final = "v5/market/instruments-info"
PATH_FUNDING: Final = "v5/market/funding/history"
PATH_OPEN_INTEREST: Final = "v5/market/open-interest"
PATH_MARK_KLINE: Final = "v5/market/mark-price-kline"
PATH_INDEX_KLINE: Final = "v5/market/index-price-kline"
PATH_PREMIUM_KLINE: Final = "v5/market/premium-index-price-kline"
PATH_ACCOUNT_RATIO: Final = "v5/market/account-ratio"

# Приклад у доках — `Perpetual`; Testnet 2026-10-29 віддає `LinearPerpetual`. Обидва — безстроковий linear.
_PERPETUAL_TYPES: Final = frozenset({"LinearPerpetual", "Perpetual"})

_INTERVAL: Final[Mapping[Timeframe, str]] = {
    Timeframe.M1: "1",
    Timeframe.M5: "5",
    Timeframe.M15: "15",
    Timeframe.M30: "30",
    Timeframe.H1: "60",
    Timeframe.H2: "120",
    Timeframe.H4: "240",
    Timeframe.H6: "360",
    Timeframe.H12: "720",
    Timeframe.D1: "D",
}
# Open interest і long/short: інший алфавіт інтервалу, ніж у kline (`1h`, не `60`). 1m/2h/6h/12h у v5 немає.
_BUCKET_INTERVAL: Final[Mapping[Timeframe, str]] = {
    Timeframe.M5: "5min",
    Timeframe.M15: "15min",
    Timeframe.M30: "30min",
    Timeframe.H1: "1h",
    Timeframe.H4: "4h",
    Timeframe.D1: "1d",
}

_IMPLICIT: Final[Mapping[str, str]] = {
    PATH_TIME: "publicGetV5MarketTime",
    PATH_KLINE: "publicGetV5MarketKline",
    PATH_INSTRUMENTS: "publicGetV5MarketInstrumentsInfo",
    PATH_FUNDING: "publicGetV5MarketFundingHistory",
    PATH_OPEN_INTEREST: "publicGetV5MarketOpenInterest",
    PATH_MARK_KLINE: "publicGetV5MarketMarkPriceKline",
    PATH_INDEX_KLINE: "publicGetV5MarketIndexPriceKline",
    PATH_PREMIUM_KLINE: "publicGetV5MarketPremiumIndexPriceKline",
    PATH_ACCOUNT_RATIO: "publicGetV5MarketAccountRatio",
}

_CANONICAL = re.compile(SYMBOL_RE)


class ExchangeMode(StrEnum):
    """Режим адаптера. Збігається з `MODES` у БД (`demo` / `testnet` / `live`)."""

    DEMO = "demo"
    TESTNET = "testnet"
    LIVE = "live"


class BybitRawResponse:
    """Сира відповідь публічного REST: HTTP-статус, розібране JSON-тіло (або рядок), заголовки."""

    __slots__ = ("body", "headers", "status")

    def __init__(self, *, status: int, body: object, headers: Mapping[str, str]) -> None:
        if type(status) is not int or isinstance(status, bool) or not 100 <= status <= 599:
            raise ValueError("status має бути HTTP-кодом")
        self.status = status
        self.body = body
        self.headers = dict(headers)


class BybitPublicClient(Protocol):
    """Транспорт публічних GET. Тести підставляють запис відповідей; прод — `CcxtBybitPublicClient`."""

    @property
    def public_origin(self) -> str:
        """Схема й хост публічного REST без шляху, напр. `https://api-testnet.bybit.com`."""
        ...

    async def get(self, path: str, query: Mapping[str, str]) -> BybitRawResponse:
        """Один GET. Мережева помилка — `ExchangeTimeout` або `ExchangeUnavailable`, не порожнє тіло."""
        ...

    async def aclose(self) -> None:
        """Закрити сесію. Повторний виклик — нічого."""
        ...


class BybitAdapter:
    """Публічні методи Bybit. Див. docstring модуля."""

    def __init__(
        self,
        *,
        mode: ExchangeMode,
        limiter: RateLimiter,
        category: str = LINEAR_CATEGORY,
        client: BybitPublicClient | None = None,
        sleeper: Sleep | None = None,
        jitter: Jitter | None = None,
        read_attempts: int = READ_ATTEMPTS,
    ) -> None:
        # Категорія — до створення клієнта: інша категорія не відкриває сесію і не ходить у мережу.
        if type(category) is not str or category != LINEAR_CATEGORY:
            raise AdapterConfigError(f"category має бути {LINEAR_CATEGORY!r}, отримано {category!r}")
        if type(mode) is not ExchangeMode:
            raise AdapterConfigError("mode має бути ExchangeMode: demo, testnet або live")
        if not isinstance(limiter, RateLimiter):
            raise TypeError("limiter має бути RateLimiter")
        if type(read_attempts) is not int or isinstance(read_attempts, bool) or read_attempts < 1:
            raise ValueError("read_attempts має бути int ≥ 1")
        self._mode = mode
        self._limiter = limiter
        self._client = client if client is not None else CcxtBybitPublicClient(mode)
        self._sleep = sleeper if sleeper is not None else _default_sleep
        self._jitter = jitter if jitter is not None else _system_jitter()
        self._read_attempts = read_attempts
        self._closed = False

    @property
    def mode(self) -> ExchangeMode:
        return self._mode

    @property
    def public_origin(self) -> str:
        """Хост, на який підуть публічні запити. Testnet-тест перевіряє його до першого виклику."""
        return self._client.public_origin

    async def aclose(self) -> None:
        """Закрити HTTP-сесію. Повторний виклик безпечний."""
        if self._closed:
            return
        self._closed = True
        await self._client.aclose()

    async def fetch_time(self) -> int:
        """Час біржі, мс UTC. Пріоритет `RECONCILE`. Береться поле `time`, звірене з `timeNano`."""
        self._ensure_open()
        response = await self._read(method="fetch_time", path=PATH_TIME, query={})
        body = _success(response)
        result = _result(body)
        nano = result.get("timeNano")
        if type(nano) is not str or not nano.isdigit() or len(nano) > _NANO_DIGITS:
            raise MalformedResponse("bybit: timeNano не є рядком наносекунд")
        server = _envelope_time(body)
        nano_ms = int(nano) // 1_000_000
        if abs(server - nano_ms) > 1:
            raise MalformedResponse("bybit: time і timeNano розходяться більш ніж на 1 мс")
        return server

    async def fetch_instruments(self, *, symbols: Collection[str] | None = None) -> list[Instrument]:
        """USDT linear perpetual, які можна назвати канонічним символом (`BTC/USDT:USDT`).

        USDC-контракти `*PERP`, dated futures і символи поза `SYMBOL_RE` відсутні в результаті, а не помилка.
        `symbols=None` — усі такі інструменти категорії; невідомий символ відсутній. Статус — як у біржі
        (без фільтра `status` Bybit віддає і `Trading`, і ще не відкриті). `launch_time_ms` — `launchTime`.
        `max_qty` — менший із `maxOrderQty`
        і `maxMktOrderQty`, щоб ринковий ордер не перевищив біржовий максимум.
        Пріоритет `DATA`.
        """
        requested = _requested_symbols(symbols)
        if requested is not None and not requested:
            return []
        self._ensure_open()
        found: dict[str, Instrument] = {}
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _page in range(_INSTRUMENT_PAGE_CAP):
            query: dict[str, str] = {"category": LINEAR_CATEGORY, "limit": str(INSTRUMENT_PAGE)}
            if cursor is not None:
                query["cursor"] = cursor
            response = await self._read(method="fetch_instruments", path=PATH_INSTRUMENTS, query=query)
            body = _success(response)
            result = _result(body)
            _require_linear(result)
            _absorb_instruments(result, found)
            nxt = _next_cursor(result)
            if nxt is None:
                break
            if nxt in seen_cursors:
                raise MalformedResponse("bybit: nextPageCursor повторився")
            seen_cursors.add(nxt)
            cursor = nxt
        else:
            raise ExchangeUnavailable("bybit: забагато сторінок instruments")
        if requested is None:
            return [found[key] for key in sorted(found)]
        return [found[key] for key in sorted(requested) if key in found]

    async def fetch_candles(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[Candle]:
        """Закриті свічки з `open_time_ms` у `[start_ms, end_ms)`, за зростанням.

        Закритість — за полем `time` тієї самої відповіді kline, не за локальним годинником, і з запасом
        остаточності: `close_time_ms + KLINE_FINALITY_MARGIN_MS ≤ time`. Свічка в межах запасу ще не віддається —
        це не пропуск. `end_ms=None` — до `fetch_time()` (виключно). Пропуски біржі не заповнюються.
        `limit` — не більше стількох найраніших свічок. Пріоритет `DATA`.
        TODO(verify-bybit): чи вистачає запасу на mainnet.
        """
        exchange_id = _canonical_symbol(symbol)
        if type(timeframe) is not Timeframe:
            raise ValueError("timeframe має бути Timeframe")
        _require_ms("start_ms", start_ms)
        if limit is not None and (type(limit) is not int or isinstance(limit, bool) or limit < 1):
            raise ValueError("limit має бути int ≥ 1")
        if end_ms is not None:
            _require_ms("end_ms", end_ms)
            if end_ms < start_ms:
                raise ValueError("end_ms має бути ≥ start_ms")
            if end_ms == start_ms:
                return []
            resolved_end = end_ms
        else:
            self._ensure_open()
            resolved_end = await self.fetch_time()
            if resolved_end <= start_ms:
                return []
        self._ensure_open()
        tf = timeframe.duration_ms
        collected: list[Candle] = []
        first = _ceil_align(start_ms, tf)
        while first < resolved_end and (limit is None or len(collected) < limit):
            window_end = min(resolved_end, first + KLINE_PAGE * tf)
            collected.extend(
                await self._kline_window(
                    symbol=symbol,
                    exchange_id=exchange_id,
                    timeframe=timeframe,
                    first=first,
                    window_end=window_end,
                )
            )
            first = window_end
        merged = _merge_candles(collected)
        if limit is not None:
            return merged[:limit]
        return merged

    async def fetch_funding_history(
        self, *, symbol: str, start_ms: int, end_ms: int | None = None
    ) -> list[FundingRate]:
        """Funding з `funding_time_ms` у `[start_ms, end_ms)`, за зростанням. Пріоритет `DATA`.

        `end_ms=None` — до `fetch_time()` (виключно). Повна сторінка (200) не вважається повною історією:
        вікно ділиться навпіл, бо біржа віддає найновіші записи сторінки.
        """
        exchange_id = _canonical_symbol(symbol)
        _require_ms("start_ms", start_ms)
        if end_ms is not None:
            _require_ms("end_ms", end_ms)
            if end_ms < start_ms:
                raise ValueError("end_ms має бути ≥ start_ms")
            if end_ms == start_ms:
                return []
            resolved_end = end_ms
        else:
            self._ensure_open()
            resolved_end = await self.fetch_time()
            if resolved_end <= start_ms:
                return []
        self._ensure_open()
        return await self._funding_between(symbol=symbol, exchange_id=exchange_id, start=start_ms, end=resolved_end)

    async def fetch_open_interest(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[OpenInterest]:
        """Open interest із `ts_ms` у `[start_ms, end_ms)`, за зростанням. Лише закриті інтервали. Пріоритет `DATA`.

        `open_interest` — поле `openInterest` (сума двох сторін). `singleOpenInterest` не зберігається.
        Пропуск на біржі пропуском і лишається. `end_ms=None` — до `fetch_time()` (виключно).
        """
        return cast(
            list[OpenInterest],
            await self._fetch_buckets(
                method="fetch_open_interest",
                path=PATH_OPEN_INTEREST,
                page=OI_PAGE,
                interval_key="intervalTime",
                symbol=symbol,
                timeframe=timeframe,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=limit,
                parse=_parse_open_interest,
            ),
        )

    async def fetch_premium_index(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[PremiumIndex]:
        """Закриті свічки premium index у `[start_ms, end_ms)`, за зростанням. Пріоритет `DATA`.

        Неузгоджений OHLC (буває на Testnet) не повертається: ціну не виправляємо. `end_ms=None` — до `fetch_time()`.
        """
        return cast(
            list[PremiumIndex],
            await self._fetch_derivative_klines(
                method="fetch_premium_index",
                path=PATH_PREMIUM_KLINE,
                symbol=symbol,
                timeframe=timeframe,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=limit,
                parse=_parse_premium_page,
                price_type=None,
            ),
        )

    async def fetch_mark_price_candles(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[MarkIndexCandle]:
        """Закриті mark-свічки у `[start_ms, end_ms)`, за зростанням. Пріоритет `DATA`.

        `end_ms=None` — до `fetch_time()` (виключно).
        """
        return cast(
            list[MarkIndexCandle],
            await self._fetch_derivative_klines(
                method="fetch_mark_price_candles",
                path=PATH_MARK_KLINE,
                symbol=symbol,
                timeframe=timeframe,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=limit,
                parse=_parse_mark_index_page,
                price_type=DerivativePriceType.MARK,
            ),
        )

    async def fetch_index_price_candles(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[MarkIndexCandle]:
        """Закриті index-свічки у `[start_ms, end_ms)`, за зростанням. Пріоритет `DATA`.

        `end_ms=None` — до `fetch_time()` (виключно).
        """
        return cast(
            list[MarkIndexCandle],
            await self._fetch_derivative_klines(
                method="fetch_index_price_candles",
                path=PATH_INDEX_KLINE,
                symbol=symbol,
                timeframe=timeframe,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=limit,
                parse=_parse_mark_index_page,
                price_type=DerivativePriceType.INDEX,
            ),
        )

    async def fetch_long_short_ratio(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[LongShortRatio]:
        """Long/short ratio із `ts_ms` у `[start_ms, end_ms)`, за зростанням. Лише закриті інтервали. Пріоритет `DATA`.

        `end_ms=None` — до `fetch_time()` (виключно). Пропуски не заповнюються.
        """
        return cast(
            list[LongShortRatio],
            await self._fetch_buckets(
                method="fetch_long_short_ratio",
                path=PATH_ACCOUNT_RATIO,
                page=RATIO_PAGE,
                interval_key="period",
                symbol=symbol,
                timeframe=timeframe,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=limit,
                parse=_parse_long_short,
            ),
        )

    async def _fetch_buckets(
        self,
        *,
        method: str,
        path: str,
        page: int,
        interval_key: str,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None,
        limit: int | None,
        parse: _BucketParse,
    ) -> list[_Point]:
        exchange_id = _canonical_symbol(symbol)
        code = _bucket_code(timeframe)
        _require_limit(limit)
        resolved_end = await self._resolved_end(start_ms, end_ms)
        if resolved_end is None:
            return []
        step = timeframe.duration_ms
        collected: list[_Point] = []
        first = _ceil_align(start_ms, step)
        while first < resolved_end and (limit is None or len(collected) < limit):
            window_end = min(resolved_end, first + page * step)
            collected.extend(
                await self._bucket_window(
                    method=method,
                    path=path,
                    page=page,
                    interval_key=interval_key,
                    interval_code=code,
                    symbol=symbol,
                    exchange_id=exchange_id,
                    timeframe=timeframe,
                    first=first,
                    window_end=window_end,
                    parse=parse,
                )
            )
            first = window_end
        merged = _merge_points(collected)
        if limit is not None:
            return merged[:limit]
        return merged

    async def _bucket_window(
        self,
        *,
        method: str,
        path: str,
        page: int,
        interval_key: str,
        interval_code: str,
        symbol: str,
        exchange_id: str,
        timeframe: Timeframe,
        first: int,
        window_end: int,
        parse: _BucketParse,
    ) -> list[_Point]:
        """Одне вікно через курсор. Повна сторінка, чий найстаріший час ще пізніший за `first`, не є кінцем вікна."""
        cursor: str | None = None
        seen: set[str] = set()
        collected: list[_Point] = []
        for _page in range(_CURSOR_PAGE_CAP):
            query: dict[str, str] = {
                "category": LINEAR_CATEGORY,
                "symbol": exchange_id,
                interval_key: interval_code,
                "startTime": str(first),
                "endTime": str(window_end - 1),
                "limit": str(page),
            }
            if cursor is not None:
                query["cursor"] = cursor
            response = await self._read(method=method, path=path, query=query)
            body = _success(response)
            points, raw_count, oldest, rejected = parse(
                body,
                symbol=symbol,
                exchange_id=exchange_id,
                timeframe=timeframe,
                first=first,
                window_end=window_end,
            )
            _log_rejected(path, symbol, rejected)
            collected.extend(points)
            nxt = _next_cursor(_result(body))
            if nxt is None or raw_count < page or (oldest is not None and oldest <= first):
                return _merge_points(collected)
            if nxt in seen:
                raise MalformedResponse("bybit: nextPageCursor повторився")
            seen.add(nxt)
            cursor = nxt
        raise ExchangeUnavailable("bybit: забагато сторінок курсора")

    async def _fetch_derivative_klines(
        self,
        *,
        method: str,
        path: str,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None,
        limit: int | None,
        parse: _KlineParse,
        price_type: DerivativePriceType | None,
    ) -> list[_Point]:
        exchange_id = _canonical_symbol(symbol)
        if type(timeframe) is not Timeframe:
            raise ValueError("timeframe має бути Timeframe")
        _require_limit(limit)
        resolved_end = await self._resolved_end(start_ms, end_ms)
        if resolved_end is None:
            return []
        step = timeframe.duration_ms
        collected: list[_Point] = []
        first = _ceil_align(start_ms, step)
        while first < resolved_end and (limit is None or len(collected) < limit):
            window_end = min(resolved_end, first + KLINE_PAGE * step)
            collected.extend(
                await self._derivative_kline_window(
                    method=method,
                    path=path,
                    symbol=symbol,
                    exchange_id=exchange_id,
                    timeframe=timeframe,
                    first=first,
                    window_end=window_end,
                    parse=parse,
                    price_type=price_type,
                )
            )
            first = window_end
        merged = _merge_points(collected)
        if limit is not None:
            return merged[:limit]
        return merged

    async def _derivative_kline_window(
        self,
        *,
        method: str,
        path: str,
        symbol: str,
        exchange_id: str,
        timeframe: Timeframe,
        first: int,
        window_end: int,
        parse: _KlineParse,
        price_type: DerivativePriceType | None,
    ) -> list[_Point]:
        slots = -(-(window_end - first) // timeframe.duration_ms)
        if slots <= 0:
            return []
        response = await self._read(
            method=method,
            path=path,
            query={
                "category": LINEAR_CATEGORY,
                "symbol": exchange_id,
                "interval": _INTERVAL[timeframe],
                "start": str(first),
                "end": str(window_end - 1),
                "limit": str(KLINE_PAGE),
            },
        )
        closed, raw_count, in_window, rejected = parse(
            _success(response),
            symbol=symbol,
            exchange_id=exchange_id,
            timeframe=timeframe,
            price_type=price_type,
            first=first,
            window_end=window_end,
        )
        _log_rejected(path, symbol, rejected)
        if raw_count < KLINE_PAGE or in_window >= slots:
            return closed
        if slots == 1:
            raise MalformedResponse("bybit: сторінка kline обрізана на одній свічці")
        mid = first + (slots // 2) * timeframe.duration_ms
        left = await self._derivative_kline_window(
            method=method,
            path=path,
            symbol=symbol,
            exchange_id=exchange_id,
            timeframe=timeframe,
            first=first,
            window_end=mid,
            parse=parse,
            price_type=price_type,
        )
        right = await self._derivative_kline_window(
            method=method,
            path=path,
            symbol=symbol,
            exchange_id=exchange_id,
            timeframe=timeframe,
            first=mid,
            window_end=window_end,
            parse=parse,
            price_type=price_type,
        )
        return _merge_points([*left, *right])

    async def _resolved_end(self, start_ms: int, end_ms: int | None) -> int | None:
        """`end` діапазону або `None`, якщо діапазон порожній. `end_ms=None` питає `fetch_time`."""
        _require_ms("start_ms", start_ms)
        if end_ms is not None:
            _require_ms("end_ms", end_ms)
            if end_ms < start_ms:
                raise ValueError("end_ms має бути ≥ start_ms")
            if end_ms == start_ms:
                return None
            self._ensure_open()
            return end_ms
        self._ensure_open()
        resolved = await self.fetch_time()
        if resolved <= start_ms:
            return None
        return resolved

    async def _kline_window(
        self,
        *,
        symbol: str,
        exchange_id: str,
        timeframe: Timeframe,
        first: int,
        window_end: int,
    ) -> list[Candle]:
        # Неповний останній слот — теж слот: свічка з `open_time_ms < window_end` входить у діапазон.
        slots = -(-(window_end - first) // timeframe.duration_ms)
        if slots <= 0:
            return []
        response = await self._read(
            method="fetch_candles",
            path=PATH_KLINE,
            query={
                "category": LINEAR_CATEGORY,
                "symbol": exchange_id,
                "interval": _INTERVAL[timeframe],
                "start": str(first),
                "end": str(window_end - 1),
                "limit": str(KLINE_PAGE),
            },
        )
        closed, raw_count, in_window = _parse_klines(
            _success(response),
            symbol=symbol,
            exchange_id=exchange_id,
            timeframe=timeframe,
            first=first,
            window_end=window_end,
        )
        if raw_count < KLINE_PAGE or in_window >= slots:
            return closed
        if slots == 1:
            raise MalformedResponse("bybit: сторінка kline обрізана на одній свічці")
        mid = first + (slots // 2) * timeframe.duration_ms
        left = await self._kline_window(
            symbol=symbol, exchange_id=exchange_id, timeframe=timeframe, first=first, window_end=mid
        )
        right = await self._kline_window(
            symbol=symbol, exchange_id=exchange_id, timeframe=timeframe, first=mid, window_end=window_end
        )
        return _merge_candles([*left, *right])

    async def _funding_between(self, *, symbol: str, exchange_id: str, start: int, end: int) -> list[FundingRate]:
        if end <= start:
            return []
        response = await self._read(
            method="fetch_funding_history",
            path=PATH_FUNDING,
            query={
                "category": LINEAR_CATEGORY,
                "symbol": exchange_id,
                "startTime": str(start),
                "endTime": str(end - 1),
                "limit": str(FUNDING_PAGE),
            },
        )
        rows, raw_count = _parse_funding(
            _success(response), symbol=symbol, exchange_id=exchange_id, start=start, end=end
        )
        if raw_count < FUNDING_PAGE:
            return rows
        if end - start <= 1:
            raise MalformedResponse("bybit: сторінка funding обрізана")
        mid = start + (end - start) // 2
        left = await self._funding_between(symbol=symbol, exchange_id=exchange_id, start=start, end=mid)
        right = await self._funding_between(symbol=symbol, exchange_id=exchange_id, start=mid, end=end)
        return _merge_funding([*left, *right])

    def _ensure_open(self) -> None:
        if self._closed:
            raise ExchangeUnavailable("адаптер Bybit закрито")

    async def _read(self, *, method: str, path: str, query: Mapping[str, str]) -> BybitRawResponse:
        priority = _FIXED_PRIORITY[method]
        failures = 0
        while True:
            try:
                return await self._limiter.call(
                    priority=priority,
                    group=EndpointGroup.MARKET,
                    operation=self._operation(path, query),
                    retry=True,
                )
            except (ExchangeTimeout, ExchangeUnavailable) as exc:
                failures += 1
                if failures >= self._read_attempts:
                    raise
                _log.warning(
                    "bybit_public_retry",
                    path=path,
                    attempt=failures,
                    error_type=type(exc).__name__,
                )
                await self._sleep(self._backoff_ms(failures))

    def _operation(self, path: str, query: Mapping[str, str]) -> Callable[[], Awaitable[BybitRawResponse]]:
        async def operation() -> BybitRawResponse:
            response = await self._client.get(path, query)
            self._raise_if_limited(response)
            if response.status >= 500:
                raise ExchangeUnavailable(f"bybit HTTP {response.status}")
            return response

        return operation

    def _raise_if_limited(self, response: BybitRawResponse) -> None:
        snapshot = parse_limit_headers(response.headers)
        if snapshot is not None:
            self._limiter.observe(EndpointGroup.MARKET, snapshot)
        breach = limit_breach_from_response(
            http_status=response.status,
            ret_code=_ret_code(response.body),
            ret_msg=_message(response.body),
            retry_after_ms=retry_after_ms_from(
                retry_after=_header(response.headers, "retry-after"),
                reset_ts_ms=None if snapshot is None else snapshot.reset_ts_ms,
                now=now_ms(),
            ),
        )
        if breach is not None:
            raise breach

    def _backoff_ms(self, failure: int) -> int:
        shift = min(failure - 1, 4)
        high = min(READ_BACKOFF_CAP_MS, READ_BACKOFF_BASE_MS * (1 << shift))
        low = high // 2
        chosen = self._jitter(low, high)
        if type(chosen) is not int or isinstance(chosen, bool) or chosen < low or chosen > high:
            return low
        return chosen


@dataclass(slots=True)
class _Captured:
    """Відповідь одного виклику з аргументів `handle_errors` CCXT: статус, заголовки, тіло (JSON або текст)."""

    status: object = None
    headers: dict[str, str] = field(default_factory=dict)
    body: object = None


# Слот відповіді поточного виклику. Це не спільний стан: `get` ставить свій слот і знімає його, а кожна
# asyncio-задача бачить лише свій, тож паралельні запити через один екземпляр CCXT не перемішують відповіді.
_CAPTURED: ContextVar[_Captured | None] = ContextVar("bybit_ccxt_captured", default=None)


class CcxtBybitPublicClient:
    """Публічний REST через CCXT async. Власний rate limit CCXT вимкнено — черга в `RateLimiter`.

    `handle_errors` біржі не кидає: код відповіді класифікує адаптер (allowlist відмов — T3.2). Статус, заголовки
    й тіло беруться з аргументів `handle_errors` у слот поточного виклику, а не з `last_*` екземпляра CCXT. Тому
    запити не стоять у черзі на lock клієнта: порядок вирішує лише пріоритет `RateLimiter`.

    Значення query кодує цей клієнт: Bybit public `sign()` CCXT шле їх у URL як є (`rawencode`).
    Транспортні збої — `ExchangeTimeout` / `ExchangeUnavailable` з логом `bybit_transport_error`; інший виняток
    (помилка в коді) летить як є, щоб адаптер не повторював його як збій мережі. Ключі сюди не передаються.
    """

    def __init__(self, mode: ExchangeMode, *, session: object | None = None) -> None:
        """`session` — лише для тестів: aiohttp-сумісна сесія замість мережевої (CCXT її не закриває)."""
        if type(mode) is not ExchangeMode:
            raise AdapterConfigError("mode має бути ExchangeMode")
        ccxt = _load_ccxt()
        config: dict[str, object] = {
            "enableRateLimit": False,
            "timeout": HTTP_TIMEOUT_MS,
            "options": {"defaultType": "swap", "defaultSubType": "linear", "adjustForTimeDifference": False},
        }
        if session is not None:
            config["session"] = session
        exchange: Any = ccxt.bybit(config)
        exchange.enableRateLimit = False
        if mode is ExchangeMode.TESTNET:
            exchange.set_sandbox_mode(True)
        origin = _public_origin(exchange)
        _assert_origin(mode, origin)
        self._exchange: Any = exchange
        self._origin = origin
        # Класи винятків CCXT: модуль вантажиться ліниво, тож вони беруться з нього, а не з імпорту модуля.
        self._ccxt_timeout: type[Exception] = ccxt.RequestTimeout
        self._ccxt_error: type[Exception] = ccxt.BaseError
        self._closed = False
        self._in_flight = 0
        self._idle = asyncio.Event()
        self._idle.set()
        _install_capture(exchange)

    @property
    def public_origin(self) -> str:
        return self._origin

    async def get(self, path: str, query: Mapping[str, str]) -> BybitRawResponse:
        if self._closed:
            raise ExchangeUnavailable("клієнт Bybit закрито")
        method = _IMPLICIT.get(path)
        if method is None:
            raise AdapterConfigError(f"немає публічного методу для {path}")
        params = _wire_query(query)
        captured = _Captured()
        token = _CAPTURED.set(captured)
        self._in_flight += 1
        self._idle.clear()
        try:
            async with asyncio.timeout((HTTP_TIMEOUT_MS + 1_000) / 1000):
                await _invoke(self._exchange, method, params)
        except TimeoutError:
            _log.warning("bybit_transport_error", path=path, error_type="TimeoutError")
            raise ExchangeTimeout(path) from None
        except self._ccxt_error as exc:
            # Повідомлення CCXT (у ньому URL) у наш виняток не копіюється; тип лишається в лозі.
            _log.warning("bybit_transport_error", path=path, error_type=type(exc).__name__)
            if isinstance(exc, self._ccxt_timeout):
                raise ExchangeTimeout(path) from None
            raise ExchangeUnavailable(path) from None
        finally:
            _CAPTURED.reset(token)
            self._in_flight -= 1
            if self._in_flight == 0:
                self._idle.set()
        status = captured.status
        if type(status) is not int or isinstance(status, bool):
            raise ExchangeUnavailable(path)
        return BybitRawResponse(status=status, body=captured.body, headers=captured.headers)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Нових запитів уже немає; ті, що в польоті (кожен обмежений таймаутом), завершуються до закриття сесії.
        await self._idle.wait()
        close = self._exchange.close
        await close()


def _install_capture(exchange: Any) -> None:
    """Замінити обробку помилок CCXT: відповідь лягає в слот поточного виклику, нічого не кидається."""

    def handle_errors(
        http_status: object,
        _reason: object,
        _url: object,
        _method: object,
        headers: object,
        body: object,
        parsed: object,
        *_rest: object,
        **_kwargs: object,
    ) -> None:
        captured = _CAPTURED.get()
        if captured is None:
            return
        captured.status = http_status
        captured.headers = _copy_headers(headers)
        captured.body = parsed if parsed is not None else body

    def ignore_status(*_rest: object, **_kwargs: object) -> None:
        return None

    exchange.handle_errors = handle_errors
    exchange.handle_http_status_code = ignore_status


def _wire_query(query: Mapping[str, str]) -> dict[str, str]:
    """Значення query у вигляді для URL (percent-encoding один раз).

    Bybit public `sign()` CCXT робить `rawencode` = `unquote(urlencode(params))`, тобто шле значення як є.
    Без кодування курсор `first=…&last=…` розпався б на два параметри (Testnet 2026-10-29: retCode 10001).
    Звичайні значення (`linear`, `BTCUSDT`, мс) від цього не змінюються.
    """
    wire: dict[str, str] = {}
    for key, value in query.items():
        if type(key) is not str or type(value) is not str:
            raise TypeError("query Bybit — лише рядки")
        wire[key] = quote(value, safe="")
    return wire


def _load_ccxt() -> Any:
    import ccxt.async_support as ccxt

    return ccxt


def _public_origin(exchange: Any) -> str:
    urls: object = cast(object, exchange.urls)
    hostname: object = cast(object, exchange.hostname)
    if type(urls) is not dict or type(hostname) is not str or hostname == "":
        raise AdapterConfigError("CCXT не дав публічний URL Bybit")
    api = urls.get("api")
    if type(api) is not dict:
        raise AdapterConfigError("CCXT не дав публічний URL Bybit")
    template = api.get("public")
    if type(template) is not str or "{hostname}" not in template:
        raise AdapterConfigError("CCXT не дав публічний URL Bybit")
    return template.replace("{hostname}", hostname)


def _assert_origin(mode: ExchangeMode, origin: str) -> None:
    if not origin.startswith("https://"):
        raise AdapterConfigError("публічний URL Bybit має бути https")
    host = origin.removeprefix("https://")
    if host == "" or "/" in host:
        raise AdapterConfigError("публічний URL Bybit має бути хостом без шляху")
    if mode is ExchangeMode.TESTNET:
        if "testnet" not in host:
            raise AdapterConfigError("testnet-клієнт зібрано не на testnet-хості")
        return
    if "testnet" in host or "demo" in host:
        raise AdapterConfigError("публічні дані demo і live беруться з mainnet, не з testnet і не з api-demo")


async def _invoke(exchange: Any, method: str, params: dict[str, str]) -> None:
    """Виклик implicit-методу CCXT. Відповідь — у слоті `_CAPTURED` (`handle_errors`), не в результаті."""
    fn: Any = getattr(exchange, method)
    await fn(params)


def _copy_headers(raw: object) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        return {}
    copied: dict[str, str] = {}
    for key, value in raw.items():
        if type(key) is str and type(value) is str:
            copied[key] = value
    return copied


def _requested_symbols(symbols: Collection[str] | None) -> frozenset[str] | None:
    if symbols is None:
        return None
    requested = tuple(symbols)
    for symbol in requested:
        _canonical_symbol(symbol)
    return frozenset(requested)


def exchange_symbol(symbol: str) -> str:
    """Канонічний `BTC/USDT:USDT` → біржовий `BTCUSDT`. Інший запис — `ValueError` без мережі."""
    return _canonical_symbol(symbol)


def _canonical_symbol(symbol: object) -> str:
    """Канонічний `BTC/USDT:USDT` → біржовий `BTCUSDT`. Інший запис — `ValueError` без мережі."""
    if type(symbol) is not str or _CANONICAL.fullmatch(symbol) is None:
        raise ValueError("символ має бути канонічним CCXT, напр. BTC/USDT:USDT")
    base, quote, settle = symbol.split("/")[0], symbol.split("/")[1].split(":")[0], symbol.split(":")[1]
    if quote != settle:
        raise ValueError("linear perpetual: quote і settle мають збігатися")
    return f"{base}{quote}"


def _require_ms(name: str, value: object) -> int:
    if type(value) is not int or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} має бути int мс ≥ 0")
    return value


def _ceil_align(ms: int, step: int) -> int:
    remainder = ms % step
    if remainder == 0:
        return ms
    return ms + (step - remainder)


def _success(response: BybitRawResponse) -> dict[str, object]:
    if response.status != 200:
        raise ExchangeOutcomeUnknown(f"bybit HTTP {response.status}")
    if type(response.body) is not dict:
        raise MalformedResponse("bybit: тіло відповіді не є об'єктом")
    code = _ret_code(response.body)
    if code is None:
        raise MalformedResponse("bybit: немає retCode")
    if code != "0":
        text = _message(response.body) or ""
        short = " ".join(text.split())[:_MSG_CHARS]
        suffix = f": {short}" if short else ""
        raise ExchangeOutcomeUnknown(f"bybit retCode {code}{suffix}")
    _envelope_time(response.body)
    return response.body


def _result(body: dict[str, object]) -> dict[str, object]:
    result = body.get("result")
    if type(result) is not dict:
        raise MalformedResponse("bybit: result не є об'єктом")
    return result


def _require_linear(result: dict[str, object]) -> None:
    if result.get("category") != LINEAR_CATEGORY:
        raise MalformedResponse("bybit: category відповіді не linear")


def _envelope_time(body: dict[str, object]) -> int:
    raw = body.get("time")
    if type(raw) is not int or isinstance(raw, bool) or raw < 0:
        raise MalformedResponse("bybit: time відповіді не є мс")
    return raw


def _ret_code(body: object) -> str | None:
    if type(body) is not dict:
        return None
    raw = body.get("retCode")
    if type(raw) is int and not isinstance(raw, bool):
        return str(raw)
    if type(raw) is str and raw.isdigit():
        return raw
    return None


def _message(body: object) -> str | None:
    if type(body) is dict:
        raw = body.get("retMsg")
        return raw if type(raw) is str else None
    if type(body) is str:
        return body[:500]
    return None


def _header(headers: Mapping[str, str], name: str) -> str | None:
    folded = name.lower()
    for key, value in headers.items():
        if key.lower() == folded:
            return value
    return None


def _next_cursor(result: dict[str, object]) -> str | None:
    raw = result.get("nextPageCursor", "")
    if raw is None or raw == "":
        return None
    if type(raw) is not str:
        raise MalformedResponse("bybit: nextPageCursor не є рядком")
    # Біржа віддає курсор percent-encoded. Адаптер тримає логічне значення; для URL його кодує клієнт
    # (`_wire_query`), тож у запит курсор іде закодованим рівно один раз.
    decoded = unquote(raw)
    if decoded == "":
        return None
    return decoded


def _absorb_instruments(result: dict[str, object], found: dict[str, Instrument]) -> None:
    rows = result.get("list")
    if type(rows) is not list:
        raise MalformedResponse("bybit: instruments list не є масивом")
    for row in rows:
        instrument = _instrument_or_skip(row)
        if instrument is None:
            continue
        previous = found.get(instrument.symbol)
        if previous is not None and previous != instrument:
            raise MalformedResponse(f"bybit: суперечливі правила {instrument.symbol}")
        found[instrument.symbol] = instrument


def _instrument_or_skip(row: object) -> Instrument | None:
    if type(row) is not dict:
        raise MalformedResponse("bybit: елемент instruments не є об'єктом")
    contract_type = row.get("contractType")
    if type(contract_type) is not str:
        raise MalformedResponse("bybit: contractType не є рядком")
    if contract_type not in _PERPETUAL_TYPES:
        return None
    base = _text(row, "baseCoin")
    quote = _text(row, "quoteCoin")
    settle = _text(row, "settleCoin")
    exchange_symbol = _text(row, "symbol")
    # USDC perp має id `BTCPERP`, не `BTCUSDC`. Такі контракти цим адаптером не адресуються.
    if quote != settle or exchange_symbol != f"{base}{quote}":
        return None
    canonical = f"{base}/{quote}:{settle}"
    if _CANONICAL.fullmatch(canonical) is None:
        return None
    price = _obj(row, "priceFilter")
    lot = _obj(row, "lotSizeFilter")
    leverage = _obj(row, "leverageFilter")
    max_order = _decimal(lot.get("maxOrderQty"), what="maxOrderQty")
    max_market = _decimal(lot.get("maxMktOrderQty"), what="maxMktOrderQty")
    return _build(
        lambda: Instrument(
            symbol=canonical,
            tick_size=_decimal(price.get("tickSize"), what="tickSize"),
            qty_step=_decimal(lot.get("qtyStep"), what="qtyStep"),
            min_qty=_decimal(lot.get("minOrderQty"), what="minOrderQty"),
            min_notional=_decimal(lot.get("minNotionalValue"), what="minNotionalValue"),
            max_qty=min(max_order, max_market),
            max_leverage=_decimal(leverage.get("maxLeverage"), what="maxLeverage"),
            launch_time_ms=_ms(row.get("launchTime"), what="launchTime"),
            status=_text(row, "status"),
        )
    )


def _parse_klines(
    body: dict[str, object],
    *,
    symbol: str,
    exchange_id: str,
    timeframe: Timeframe,
    first: int,
    window_end: int,
) -> tuple[list[Candle], int, int]:
    result = _result(body)
    _require_linear(result)
    if result.get("symbol") != exchange_id:
        raise MalformedResponse("bybit: symbol відповіді kline не збігається із запитом")
    rows = result.get("list")
    if type(rows) is not list:
        raise MalformedResponse("bybit: kline list не є масивом")
    server = _envelope_time(body)
    parsed = [_candle_row(row, symbol=symbol, timeframe=timeframe) for row in rows]
    merged = _merge_candles(parsed)
    in_window = [candle for candle in merged if first <= candle.open_time_ms < window_end]
    closed = [candle for candle in in_window if candle.close_time_ms + KLINE_FINALITY_MARGIN_MS <= server]
    return closed, len(rows), len(in_window)


def _candle_row(row: object, *, symbol: str, timeframe: Timeframe) -> Candle:
    if type(row) is not list or len(row) < 6:
        raise MalformedResponse("bybit: свічка не є масивом із 6 полів")
    open_ms = _ms(row[0], what="startTime")
    return _build(
        lambda: Candle(
            symbol=symbol,
            timeframe=timeframe,
            open_time_ms=open_ms,
            open=_decimal(row[1], what="open"),
            high=_decimal(row[2], what="high"),
            low=_decimal(row[3], what="low"),
            close=_decimal(row[4], what="close"),
            volume=_decimal(row[5], what="volume"),
        )
    )


def _funding_rate(symbol: str, funding_time_ms: int, rate: Decimal) -> FundingRate:
    return _build(lambda: FundingRate(symbol=symbol, funding_time_ms=funding_time_ms, rate=rate))


def _parse_funding(
    body: dict[str, object], *, symbol: str, exchange_id: str, start: int, end: int
) -> tuple[list[FundingRate], int]:
    result = _result(body)
    _require_linear(result)
    rows = result.get("list")
    if type(rows) is not list:
        raise MalformedResponse("bybit: funding list не є масивом")
    parsed: list[FundingRate] = []
    for row in rows:
        if type(row) is not dict:
            raise MalformedResponse("bybit: елемент funding не є об'єктом")
        if row.get("symbol") != exchange_id:
            raise MalformedResponse("bybit: symbol funding не збігається із запитом")
        funding_time = _ms(row.get("fundingRateTimestamp"), what="fundingRateTimestamp")
        rate = _decimal(row.get("fundingRate"), what="fundingRate")
        if start <= funding_time < end:
            parsed.append(_funding_rate(symbol, funding_time, rate))
    return _merge_funding(parsed), len(rows)


def _merge_candles(candles: list[Candle]) -> list[Candle]:
    by_open: dict[int, Candle] = {}
    for candle in candles:
        previous = by_open.get(candle.open_time_ms)
        if previous is not None and previous != candle:
            raise MalformedResponse("bybit: суперечливі свічки одного open_time")
        by_open[candle.open_time_ms] = candle
    return [by_open[key] for key in sorted(by_open)]


def _merge_funding(rows: list[FundingRate]) -> list[FundingRate]:
    by_time: dict[int, FundingRate] = {}
    for row in rows:
        previous = by_time.get(row.funding_time_ms)
        if previous is not None and previous != row:
            raise MalformedResponse("bybit: суперечливі funding одного часу")
        by_time[row.funding_time_ms] = row
    return [by_time[key] for key in sorted(by_time)]


def _bucket_code(timeframe: Timeframe) -> str:
    if type(timeframe) is not Timeframe:
        raise ValueError("timeframe має бути Timeframe")
    code = _BUCKET_INTERVAL.get(timeframe)
    if code is None:
        raise ValueError(f"інтервал {timeframe.value} для open interest і long/short Bybit не задано")
    return code


def _require_limit(limit: int | None) -> None:
    if limit is not None and (type(limit) is not int or isinstance(limit, bool) or limit < 1):
        raise ValueError("limit має бути int ≥ 1")


def _interval_closed(ts_ms: int, timeframe: Timeframe, server: int) -> bool:
    return ts_ms + timeframe.duration_ms + KLINE_FINALITY_MARGIN_MS <= server


def _log_rejected(path: str, symbol: str, rejected: list[int]) -> None:
    if not rejected:
        return
    _log.warning(
        "derivative_point_rejected",
        path=path,
        symbol=symbol,
        count=len(rejected),
        sample_ms=rejected[:3],
    )


def _merge_points(points: list[_P]) -> list[_P]:
    by_time: dict[int, _P] = {}
    for point in points:
        previous = by_time.get(point.ts_ms)
        if previous is not None and previous != point:
            raise MalformedResponse("bybit: суперечливі точки одного часу")
        by_time[point.ts_ms] = point
    return [by_time[key] for key in sorted(by_time)]


def _accept_open_interest(symbol: str, timeframe: Timeframe, ts: int, value: Decimal) -> OpenInterest | None:
    try:
        return OpenInterest(symbol=symbol, timeframe=timeframe, ts_ms=ts, open_interest=value)
    except (ValidationError, ValueError):
        return None


def _accept_long_short(
    symbol: str, timeframe: Timeframe, ts: int, buy: Decimal, sell: Decimal
) -> LongShortRatio | None:
    try:
        return LongShortRatio(symbol=symbol, timeframe=timeframe, ts_ms=ts, buy_ratio=buy, sell_ratio=sell)
    except (ValidationError, ValueError):
        return None


def _try_point(factory: Callable[[], _P]) -> _P | None:
    """`None` — рядок розібрано, але він не проходить інваріант доменної моделі (його не виправляємо)."""
    try:
        return factory()
    except (ValidationError, ValueError):
        return None


def _parse_open_interest(
    body: dict[str, object],
    *,
    symbol: str,
    exchange_id: str,
    timeframe: Timeframe,
    first: int,
    window_end: int,
) -> tuple[list[_Point], int, int | None, list[int]]:
    result = _result(body)
    _require_linear(result)
    if result.get("symbol") != exchange_id:
        raise MalformedResponse("bybit: symbol open interest не збігається із запитом")
    rows = _object_rows(result, what="open interest")
    server = _envelope_time(body)
    points: list[_Point] = []
    rejected: list[int] = []
    timestamps: list[int] = []
    for row in rows:
        ts = _ms(row.get("timestamp"), what="timestamp")
        timestamps.append(ts)
        value = _decimal(row.get("openInterest"), what="openInterest")
        if not first <= ts < window_end or not _interval_closed(ts, timeframe, server):
            continue
        built: _Point | None = _accept_open_interest(symbol, timeframe, ts, value)
        if built is None:
            rejected.append(ts)
        else:
            points.append(built)
    oldest = min(timestamps) if timestamps else None
    return _merge_points(points), len(rows), oldest, rejected


def _parse_long_short(
    body: dict[str, object],
    *,
    symbol: str,
    exchange_id: str,
    timeframe: Timeframe,
    first: int,
    window_end: int,
) -> tuple[list[_Point], int, int | None, list[int]]:
    result = _result(body)
    if "category" in result:
        _require_linear(result)
    rows = _object_rows(result, what="long/short")
    server = _envelope_time(body)
    points: list[_Point] = []
    rejected: list[int] = []
    timestamps: list[int] = []
    for row in rows:
        if row.get("symbol") != exchange_id:
            raise MalformedResponse("bybit: symbol long/short не збігається із запитом")
        ts = _ms(row.get("timestamp"), what="timestamp")
        timestamps.append(ts)
        buy = _decimal(row.get("buyRatio"), what="buyRatio")
        sell = _decimal(row.get("sellRatio"), what="sellRatio")
        if not first <= ts < window_end or not _interval_closed(ts, timeframe, server):
            continue
        built = _accept_long_short(symbol, timeframe, ts, buy, sell)
        if built is None:
            rejected.append(ts)
        else:
            points.append(built)
    oldest = min(timestamps) if timestamps else None
    return _merge_points(points), len(rows), oldest, rejected


def _parse_premium_page(
    body: dict[str, object],
    *,
    symbol: str,
    exchange_id: str,
    timeframe: Timeframe,
    price_type: DerivativePriceType | None,
    first: int,
    window_end: int,
) -> tuple[list[_Point], int, int, list[int]]:
    del price_type
    return _parse_price_page(
        body,
        symbol=symbol,
        exchange_id=exchange_id,
        timeframe=timeframe,
        first=first,
        window_end=window_end,
        build=lambda row, ts: _try_point(
            lambda: PremiumIndex(
                symbol=symbol,
                timeframe=timeframe,
                ts_ms=ts,
                open=_decimal(row[1], what="open"),
                high=_decimal(row[2], what="high"),
                low=_decimal(row[3], what="low"),
                close=_decimal(row[4], what="close"),
            )
        ),
    )


def _parse_mark_index_page(
    body: dict[str, object],
    *,
    symbol: str,
    exchange_id: str,
    timeframe: Timeframe,
    price_type: DerivativePriceType | None,
    first: int,
    window_end: int,
) -> tuple[list[_Point], int, int, list[int]]:
    if price_type is None:
        raise MalformedResponse("bybit: price_type mark/index не задано")
    kind = price_type

    def build(row: list[object], ts: int) -> _Point | None:
        return _try_point(
            lambda: MarkIndexCandle(
                symbol=symbol,
                timeframe=timeframe,
                price_type=kind,
                ts_ms=ts,
                open=_decimal(row[1], what="open"),
                high=_decimal(row[2], what="high"),
                low=_decimal(row[3], what="low"),
                close=_decimal(row[4], what="close"),
            )
        )

    return _parse_price_page(
        body,
        symbol=symbol,
        exchange_id=exchange_id,
        timeframe=timeframe,
        first=first,
        window_end=window_end,
        build=build,
    )


def _parse_price_page(
    body: dict[str, object],
    *,
    symbol: str,
    exchange_id: str,
    timeframe: Timeframe,
    first: int,
    window_end: int,
    build: Callable[[list[object], int], _Point | None],
) -> tuple[list[_Point], int, int, list[int]]:
    del symbol
    result = _result(body)
    _require_linear(result)
    if result.get("symbol") != exchange_id:
        raise MalformedResponse("bybit: symbol kline не збігається із запитом")
    rows = result.get("list")
    if type(rows) is not list:
        raise MalformedResponse("bybit: kline list не є масивом")
    server = _envelope_time(body)
    points: list[_Point] = []
    rejected: list[int] = []
    in_window = 0
    for row in rows:
        if type(row) is not list or len(row) < 5:
            raise MalformedResponse("bybit: свічка дериватива не є масивом із 5 полів")
        typed = cast(list[object], row)
        ts = _ms(typed[0], what="startTime")
        if not first <= ts < window_end:
            continue
        in_window += 1
        if not _interval_closed(ts, timeframe, server):
            continue
        # Ціни читаємо до моделі: нечисло — нерозбірна відповідь, а не пропущена свічка.
        for index, name in ((1, "open"), (2, "high"), (3, "low"), (4, "close")):
            _decimal(typed[index], what=name)
        built = build(typed, ts)
        if built is None:
            rejected.append(ts)
        else:
            points.append(built)
    return _merge_points(points), len(rows), in_window, rejected


def _object_rows(result: dict[str, object], *, what: str) -> list[dict[str, object]]:
    rows = result.get("list")
    if type(rows) is not list:
        raise MalformedResponse(f"bybit: {what} list не є масивом")
    parsed: list[dict[str, object]] = []
    for row in rows:
        if type(row) is not dict:
            raise MalformedResponse(f"bybit: елемент {what} не є об'єктом")
        parsed.append(row)
    return parsed


def _text(row: dict[str, object], key: str) -> str:
    value = row.get(key)
    if type(value) is not str or value == "":
        raise MalformedResponse(f"bybit: {key} не є непорожнім рядком")
    return value


def _obj(row: dict[str, object], key: str) -> dict[str, object]:
    value = row.get(key)
    if type(value) is not dict:
        raise MalformedResponse(f"bybit: {key} не є об'єктом")
    return value


def _decimal(raw: object, *, what: str) -> Decimal:
    if type(raw) is not str or raw == "" or len(raw) > _DECIMAL_CHARS or raw != raw.strip():
        raise MalformedResponse(f"bybit: {what} не є рядком числа")
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise MalformedResponse(f"bybit: {what} не є числом") from exc
    if not value.is_finite():
        raise MalformedResponse(f"bybit: {what} не є скінченним")
    return value


def _ms(raw: object, *, what: str) -> int:
    if type(raw) is not str or not raw.isdigit() or len(raw) > 16:
        raise MalformedResponse(f"bybit: {what} не є рядком мс")
    return int(raw)


def _build(factory: Callable[[], _T]) -> _T:
    try:
        return factory()
    except (ValidationError, ValueError) as exc:
        raise MalformedResponse("bybit: відповідь не відповідає доменній моделі") from exc


def _system_jitter() -> Jitter:
    rng = random.Random()  # noqa: S311 — jitter паузи читання, не секрет і не ключ

    def jitter(low: int, high: int) -> int:
        if high <= low:
            return low
        return low + rng.randrange(high - low + 1)

    return jitter


async def _default_sleep(ms: int) -> None:
    if ms > 0:
        await asyncio.sleep(ms / 1000)
    else:
        await asyncio.sleep(0)
