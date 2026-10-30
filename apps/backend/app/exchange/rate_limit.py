"""Централізований rate limiter процесу (ТЗ §6.5, T1.2).

Усі REST-виклики біржі (адаптер Bybit, T1.3/T3.2) проходять через один `RateLimiter` процесу.
`FakeExchangeAdapter` лімітер не викликає: тести мають лишатися детермінованими й без очікування;
пріоритет у фейку лише записується в журнал викликів.

Модель:

- **Віконний token bucket** на групу. Витрачений токен зайнятий до кінця вікна, тож у будь-якому вікні
  вага не перевищує місткість. Класичний bucket із плавним поповненням дозволив би майже подвійний сплеск
  на межі вікна й вичерпав би ліміт Bybit (для IP це бан щонайменше на 10 хв).
- **IP-ліміт спільний для процесів.** Місткість цього процесу — `IP_REQUESTS * share_pct // 100`
  (за замовчуванням торговий 50%, воркер 30%, колектор 20%). Сума часток — 100, тож разом процеси
  не перевищують ліміт IP. Приватні групи UID належать лише торговому процесу (ключі є лише в нього)
  і на частку не діляться.
- **Пріоритети** `PROTECTION` > `RECONCILE` > `DATA`. Захисний запит забирає наступний вільний слот
  попереду вже поставлених у чергу даних і звірки. Дані не обганяють захист ніколи.
- **Без голодування** між `RECONCILE` і `DATA`: якщо нижчий пріоритет чекає довше за `STARVATION_MS`,
  він отримує слот попереду вищого, але не попереду `PROTECTION`.
- **Роль процесу** задає точка входу (`from_settings(settings, role=...)`), а не `.env`: файл спільний для трьох
  процесів, і роль звідти дала б воркеру частку торгового процесу.
- **Заголовки Bybit** (`X-Bapi-Limit`, `X-Bapi-Limit-Status`, `X-Bapi-Limit-Reset-Timestamp`) лише
  зменшують місткість групи або доступний залишок. Підняти стелю вище задокументованого ліміту вони не можуть.
  Очікування reset при нульовому залишку не довше за вікно групи (reset — час біржі; зсув годинника не заморожує SL).
- **429, retCode 10006 і IP-бан** ставлять на паузу всю чергу (включно із захистом — нові виклики під час
  бану лише подовжують його) і вмикають експоненційний backoff із jitter. Сходинка росте раз на епізод паузи,
  а `retry_after_ms` не довший за `BACKOFF_CAP_MS`. Повтор — лише якщо викликач
  передав `retry=True`. `place_order` і `cancel_order` не повторюються (`NON_RETRYABLE_METHODS`):
  `RateLimited` для них — не відмова, а `SUBMISSION_UNKNOWN` (`errors.py`).

Числа лімітів звірені з документацією Bybit v5 (2026-10-29):
https://bybit-exchange.github.io/docs/v5/rate-limit
https://bybit-exchange.github.io/docs/v5/rate-limit/rules-for-pros/introduction
Публічні market-ендпоінти в таблиці UID-лімітів відсутні — для них діє лише IP-ліміт, доки відповідь
не принесе заголовки (тоді з'являється окрема група й лише стискає ліміт).
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Final, TypeVar

from structlog.stdlib import BoundLogger

from app.core.clock import monotonic_ms, now_ms
from app.exchange.errors import RateLimited

_T = TypeVar("_T")

# --- ліміти Bybit, звірені 2026-10-29 (єдине джерело чисел; тест фіксує їх) ---

# HTTP IP limit: 600 запитів за 5 с на IP. Перевищення — HTTP 403 «access too frequent», бан ≥ 10 хв.
IP_REQUESTS: Final = 600
IP_WINDOW_MS: Final = 5_000
IP_BAN_MS: Final = 10 * 60 * 1_000

# UID-ліміти — ковзне вікно 1 с (документація: «per second per UID»). Лінійні USDT-контракти, UTA 2.0 /
# default futures 10/s на create (сторінка PRO: Default = 10/s). Заголовок може лише знизити стелю.
UID_WINDOW_MS: Final = 1_000

# Backoff після 429 / 10006. Стеля не нижча за мінімальну паузу IP-бану.
BACKOFF_BASE_MS: Final = 200
BACKOFF_CAP_MS: Final = IP_BAN_MS
# Скільки разів `call(..., retry=True)` виконує операцію, перш ніж прокинути `RateLimited` далі.
MAX_ATTEMPTS: Final = 4
# Скільки нижчий пріоритет (не PROTECTION) може чекати, перш ніж обігнати вищий не-захисний.
STARVATION_MS: Final = 2_000
# Якщо IP-вікно процесу не менше за поріг, не-захисні запити лишають ceiling // дільник слотів під SL.
IP_RESERVE_MIN_CAPACITY: Final = 10
IP_RESERVE_DIVISOR: Final = 20

# Вага одного batch-запиту Bybit — кількість ордерів у ньому, не більше 10.
MAX_WEIGHT: Final = 10

# Частки спільного IP-ліміту (ТЗ §6.5). Їх же читає `Settings`.
DEFAULT_SHARE_TRADING_PCT: Final = 50
DEFAULT_SHARE_WORKER_PCT: Final = 30
DEFAULT_SHARE_COLLECTOR_PCT: Final = 20

# retCode перевищення API rate limit (не IP-бан).
BYBIT_RATE_LIMIT_RET_CODE: Final = "10006"
_IP_BAN_MARK: Final = "access too frequent"

_HEADER_LIMIT: Final = "x-bapi-limit"
_HEADER_REMAINING: Final = "x-bapi-limit-status"
_HEADER_RESET: Final = "x-bapi-limit-reset-timestamp"

Sleep = Callable[[int], Awaitable[None]]
Jitter = Callable[[int, int], int]


class RequestPriority(StrEnum):
    """Клас запиту в черзі. Числовий ранг — `_PRIORITY_RANK`, не порядок оголошення."""

    PROTECTION = "PROTECTION"
    RECONCILE = "RECONCILE"
    DATA = "DATA"


_PRIORITY_RANK: Final[Mapping[RequestPriority, int]] = MappingProxyType(
    {
        RequestPriority.PROTECTION: 0,
        RequestPriority.RECONCILE: 1,
        RequestPriority.DATA: 2,
    }
)


class ProcessRole(StrEnum):
    """Процес, якому належить лімітер. Частку IP обирає конфіг за цією роллю."""

    TRADING = "trading"
    WORKER = "worker"
    COLLECTOR = "collector"


class EndpointGroup(StrEnum):
    """Окремий bucket UID Bybit (лінійні контракти). `MARKET` — публічні дані, спершу лише IP."""

    MARKET = "MARKET"
    ORDER_CREATE = "ORDER_CREATE"  # POST /v5/order/create — 10/s
    ORDER_CANCEL = "ORDER_CANCEL"  # POST /v5/order/cancel — 10/s
    ORDER_REALTIME = "ORDER_REALTIME"  # GET /v5/order/realtime — 50/s
    ORDER_HISTORY = "ORDER_HISTORY"  # GET /v5/order/history — 50/s
    EXECUTION_LIST = "EXECUTION_LIST"  # GET /v5/execution/list — 50/s
    POSITION_LIST = "POSITION_LIST"  # GET /v5/position/list — 50/s
    TRADING_STOP = "TRADING_STOP"  # POST /v5/position/trading-stop — 10/s
    SET_LEVERAGE = "SET_LEVERAGE"  # POST /v5/position/set-leverage — 10/s
    WALLET_BALANCE = "WALLET_BALANCE"  # GET /v5/account/wallet-balance — 50/s
    SET_MARGIN_MODE = "SET_MARGIN_MODE"  # POST /v5/account/set-margin-mode — 5/s


class RateLimiterError(RuntimeError):
    """Лімітер не може виконати запит (закритий, черга перервана, вага не вміщується після корекції).

    Запит **не відправлявся**. Для `place_order` це не `RequestRejected`: результат невідомий лише якщо
    виклик уже пішов у мережу (`errors.is_definite_rejection`).
    """


# Методи протоколу, чий пріоритет не залежить від викликача. Решта REST-методів вимагають `priority`.
FIXED_METHOD_PRIORITY: Final[Mapping[str, RequestPriority]] = MappingProxyType(
    {
        "fetch_time": RequestPriority.RECONCILE,
        "fetch_instruments": RequestPriority.DATA,
        "fetch_candles": RequestPriority.DATA,
        "fetch_funding_history": RequestPriority.DATA,
        "fetch_open_interest": RequestPriority.DATA,
        "fetch_premium_index": RequestPriority.DATA,
        "fetch_mark_price_candles": RequestPriority.DATA,
        "fetch_index_price_candles": RequestPriority.DATA,
        "fetch_long_short_ratio": RequestPriority.DATA,
        "set_position_protection": RequestPriority.PROTECTION,
    }
)

# Повтор після ліміту заборонений: ордер міг уже існувати, друге скасування — інша дія (ТЗ §7.2).
NON_RETRYABLE_METHODS: Final[frozenset[str]] = frozenset({"place_order", "cancel_order"})


@dataclass(frozen=True, slots=True)
class GroupLimit:
    """Місткість групи: не більше `requests` одиниць ваги за `window_ms`."""

    requests: int
    window_ms: int


def default_group_limits() -> Mapping[EndpointGroup, GroupLimit]:
    """Задокументовані UID-ліміти лінійних контрактів. `MARKET` тут немає — лише IP, доки немає заголовків."""
    per_second = UID_WINDOW_MS
    return MappingProxyType(
        {
            EndpointGroup.ORDER_CREATE: GroupLimit(10, per_second),
            EndpointGroup.ORDER_CANCEL: GroupLimit(10, per_second),
            EndpointGroup.ORDER_REALTIME: GroupLimit(50, per_second),
            EndpointGroup.ORDER_HISTORY: GroupLimit(50, per_second),
            EndpointGroup.EXECUTION_LIST: GroupLimit(50, per_second),
            EndpointGroup.POSITION_LIST: GroupLimit(50, per_second),
            EndpointGroup.TRADING_STOP: GroupLimit(10, per_second),
            EndpointGroup.SET_LEVERAGE: GroupLimit(10, per_second),
            EndpointGroup.WALLET_BALANCE: GroupLimit(50, per_second),
            EndpointGroup.SET_MARGIN_MODE: GroupLimit(5, per_second),
        }
    )


def share_capacity(limit: int, share_pct: int) -> int:
    """Ціла місткість частки. Сума `floor` по частках, що дають 100%, не перевищує `limit`."""
    return limit * share_pct // 100


@dataclass(frozen=True, slots=True)
class RateLimitConfig:
    """Параметри одного процесу. Частки трьох процесів разом перевіряє `Settings`."""

    role: ProcessRole
    share_pct: int
    ip_requests: int = IP_REQUESTS
    ip_window_ms: int = IP_WINDOW_MS
    starvation_ms: int = STARVATION_MS
    backoff_base_ms: int = BACKOFF_BASE_MS
    backoff_cap_ms: int = BACKOFF_CAP_MS
    ip_ban_ms: int = IP_BAN_MS
    max_attempts: int = MAX_ATTEMPTS
    groups: Mapping[EndpointGroup, GroupLimit] = field(default_factory=default_group_limits)

    def __post_init__(self) -> None:
        if not isinstance(self.role, ProcessRole):
            raise ValueError("role має бути ProcessRole")
        _require_int("share_pct", self.share_pct, minimum=1, maximum=99)
        _require_int("ip_requests", self.ip_requests, minimum=1)
        _require_int("ip_window_ms", self.ip_window_ms, minimum=1)
        _require_int("starvation_ms", self.starvation_ms, minimum=0)
        _require_int("backoff_base_ms", self.backoff_base_ms, minimum=1)
        _require_int("backoff_cap_ms", self.backoff_cap_ms, minimum=self.backoff_base_ms)
        _require_int("ip_ban_ms", self.ip_ban_ms, minimum=1, maximum=self.backoff_cap_ms)
        _require_int("max_attempts", self.max_attempts, minimum=1)
        if share_capacity(self.ip_requests, self.share_pct) < 1:
            raise ValueError("частка IP-ліміту округлюється до 0: збільште ліміт або частку")
        for group, limit in self.groups.items():
            if not isinstance(group, EndpointGroup):
                raise ValueError(f"невідома група ендпоінтів: {group!r}")
            _require_int(f"{group}.requests", limit.requests, minimum=1)
            _require_int(f"{group}.window_ms", limit.window_ms, minimum=1)


@dataclass(frozen=True, slots=True)
class LimitSnapshot:
    """Заголовки ліміту однієї відповіді Bybit. `remaining` — `X-Bapi-Limit-Status`."""

    limit: int
    remaining: int
    reset_ts_ms: int

    def __post_init__(self) -> None:
        _require_int("limit", self.limit, minimum=1)
        _require_int("remaining", self.remaining, minimum=0, maximum=self.limit)
        _require_int("reset_ts_ms", self.reset_ts_ms, minimum=0)


@dataclass(slots=True)
class _Window:
    """Вікно: токен, витрачений у момент `ts`, звільняється в `ts + window_ms`."""

    capacity: int
    window_ms: int
    ceiling: int
    _events: deque[tuple[int, int]] = field(default_factory=deque)
    _held_until: int | None = None

    def available(self, now: int) -> int:
        self._expire(now)
        if self._held_until is not None and now < self._held_until:
            return 0
        used = sum(weight for _, weight in self._events)
        return max(0, self.capacity - used)

    def spend(self, now: int, weight: int) -> None:
        self._events.append((now, weight))

    def ms_until(self, now: int, weight: int) -> int:
        if self.available(now) >= weight:
            return 0
        waits: list[int] = []
        if self._held_until is not None and self._held_until > now:
            waits.append(self._held_until - now)
        if self._events:
            expiry = self._events[0][0] + self.window_ms - now
            waits.append(expiry if expiry > 0 else 1)
        if not waits:
            raise RateLimiterError(f"вага {weight} не вміщується в місткість {self.capacity}")
        return min(waits)

    def clamp(self, now: int, *, limit: int, remaining: int, hold_until: int | None) -> bool:
        """Зменшити стелю й залишок. Повертає `True`, якщо стеля знизилась. Вгору не росте."""
        lowered = False
        capped = min(self.ceiling, limit)
        if capped < self.capacity:
            self.capacity = capped
            lowered = True
        avail = self.available(now)
        if remaining < avail:
            self._events.append((now, avail - remaining))
        if (
            remaining == 0
            and hold_until is not None
            and hold_until > now
            and (self._held_until is None or hold_until > self._held_until)
        ):
            self._held_until = hold_until
        return lowered

    def _expire(self, now: int) -> None:
        while self._events and self._events[0][0] + self.window_ms <= now:
            self._events.popleft()
        if self._held_until is not None and self._held_until <= now:
            self._held_until = None


@dataclass(slots=True)
class _Waiter:
    priority: RequestPriority
    group: EndpointGroup
    weight: int
    enqueued_mono_ms: int
    seq: int
    future: asyncio.Future[None]


def parse_limit_headers(headers: Mapping[str, str]) -> LimitSnapshot | None:
    """Розібрати заголовки ліміту Bybit. Неповні або нечислові — `None` (лімітер лишається на своїй стелі).

    Імена заголовків порівнюються без урахування регістру.
    """
    lowered = {name.lower(): value.strip() for name, value in headers.items()}
    limit_s = lowered.get(_HEADER_LIMIT)
    remaining_s = lowered.get(_HEADER_REMAINING)
    reset_s = lowered.get(_HEADER_RESET)
    if limit_s is None or remaining_s is None or reset_s is None or limit_s == "" or remaining_s == "" or reset_s == "":
        return None
    try:
        return LimitSnapshot(limit=int(limit_s), remaining=int(remaining_s), reset_ts_ms=int(reset_s))
    except ValueError:
        return None


def retry_after_ms_from(*, retry_after: str | None, reset_ts_ms: int | None, now: int) -> int | None:
    """Скільки мс чекати: більша з пауз `Retry-After` (цілі секунди HTTP) і `reset_ts_ms` біржі.

    Нечисловий `Retry-After` (HTTP-date) ігнорується — лишається backoff лімітера.
    """
    waits: list[int] = []
    if retry_after is not None and retry_after.strip().isdigit():
        waits.append(int(retry_after.strip()) * 1_000)
    if reset_ts_ms is not None and type(reset_ts_ms) is int and reset_ts_ms > now:
        waits.append(reset_ts_ms - now)
    return max(waits) if waits else None


def limit_breach_from_response(
    *,
    http_status: int,
    ret_code: str | int | None = None,
    ret_msg: str | None = None,
    retry_after_ms: int | None = None,
) -> RateLimited | None:
    """`RateLimited`, якщо відповідь — перевищення ліміту; інакше `None` (це не auth і не відмова ордера).

    - HTTP 429 — пауза черги (ТЗ §6.5);
    - `retCode` 10006 — API rate limit Bybit;
    - HTTP 403 із текстом «access too frequent» — бан IP, пауза не коротша за `IP_BAN_MS`.
      Інший 403 сюди не потрапляє: це може бути авторизація (мапінг — T3.2).
    """
    code = _normalize_ret_code(ret_code)
    ip_ban = http_status == 403 and ret_msg is not None and _IP_BAN_MARK in ret_msg.lower()
    if http_status != 429 and code != BYBIT_RATE_LIMIT_RET_CODE and not ip_ban:
        return None
    if retry_after_ms is not None and (type(retry_after_ms) is not int or retry_after_ms < 0):
        retry_after_ms = None
    return RateLimited(
        "ip banned" if ip_ban else "rate limited",
        retry_after_ms=retry_after_ms,
        ip_ban=ip_ban,
    )


class RateLimiter:
    """Черга одного процесу. Створюється в робочому циклі подій; `aclose()` зупиняє фонову задачу."""

    def __init__(
        self,
        config: RateLimitConfig,
        *,
        sleeper: Sleep | None = None,
        jitter: Jitter | None = None,
    ) -> None:
        self._config = config
        self._sleep = sleeper if sleeper is not None else _default_sleep
        self._jitter = jitter if jitter is not None else _system_jitter()
        self._ip = _Window(
            capacity=share_capacity(config.ip_requests, config.share_pct),
            window_ms=config.ip_window_ms,
            ceiling=share_capacity(config.ip_requests, config.share_pct),
        )
        self._buckets: dict[EndpointGroup, _Window] = {
            group: _Window(capacity=limit.requests, window_ms=limit.window_ms, ceiling=limit.requests)
            for group, limit in config.groups.items()
        }
        self._waiters: list[_Waiter] = []
        self._seq = 0
        self._strikes = 0
        self._paused_until = 0
        self._wake = asyncio.Event()
        self._drain_task: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def role(self) -> ProcessRole:
        return self._config.role

    @property
    def ip_capacity(self) -> int:
        """Скільки запитів цей процес може тримати у своєму IP-вікні."""
        return self._ip.ceiling

    @classmethod
    def from_settings(
        cls,
        settings: object,
        *,
        role: ProcessRole,
        sleeper: Sleep | None = None,
        jitter: Jitter | None = None,
    ) -> RateLimiter:
        """Зібрати лімітер: частка — із `Settings`, роль — від точки входу процесу.

        Роль не читається з `.env`: файл спільний для трьох процесів, і роль звідти дала б воркеру чи колектору
        частку торгового процесу (сума часток понад 100% — бан IP). Імпорт `Settings` локальний, щоб `config`
        міг імпортувати константи звідси.
        """
        from app.core.config import Settings

        if not isinstance(settings, Settings):
            raise TypeError("from_settings очікує Settings")
        if not isinstance(role, ProcessRole):
            raise ValueError("role має бути ProcessRole")
        shares = {
            ProcessRole.TRADING: settings.rate_limit_share_trading_pct,
            ProcessRole.WORKER: settings.rate_limit_share_worker_pct,
            ProcessRole.COLLECTOR: settings.rate_limit_share_collector_pct,
        }
        return cls(RateLimitConfig(role=role, share_pct=shares[role]), sleeper=sleeper, jitter=jitter)

    async def acquire(
        self,
        *,
        priority: RequestPriority,
        group: EndpointGroup,
        weight: int = 1,
    ) -> None:
        """Дочекатися права відправити один REST-виклик. Сам виклик і повтор — на боці `call` або адаптера.

        IP коштує 1 (один HTTP-запит). `weight` списується з UID-групи (batch Bybit — кількість ордерів).
        """
        if self._closed:
            raise RateLimiterError("лімітер закрито")
        if not isinstance(priority, RequestPriority):
            raise ValueError("priority має бути RequestPriority")
        if not isinstance(group, EndpointGroup):
            raise ValueError("group має бути EndpointGroup")
        self._check_role(group)
        self._check_weight(group, weight)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        waiter = _Waiter(
            priority=priority,
            group=group,
            weight=weight,
            enqueued_mono_ms=monotonic_ms(),
            seq=self._seq,
            future=future,
        )
        self._seq += 1
        self._waiters.append(waiter)
        self._wake.set()
        self._ensure_drain()
        try:
            await future
        except asyncio.CancelledError:
            self._discard(waiter)
            self._wake.set()
            raise

    def observe(self, group: EndpointGroup, snapshot: LimitSnapshot) -> None:
        """Підкрутити групу за заголовками відповіді. Стеля не росте; залишок може лише зменшитись."""
        self._check_role(group)
        if not isinstance(snapshot, LimitSnapshot):
            raise TypeError("snapshot має бути LimitSnapshot")
        bucket = self._buckets.get(group)
        if bucket is None:
            if group is not EndpointGroup.MARKET:
                raise ValueError(f"невідома група {group}")
            cap = min(snapshot.limit, self._ip.ceiling)
            bucket = _Window(capacity=cap, window_ms=UID_WINDOW_MS, ceiling=cap)
            self._buckets[group] = bucket
        now = monotonic_ms()
        hold: int | None = None
        if snapshot.remaining == 0:
            # Reset — час біржі, а не локальний: зсув годинника чи сміття в заголовку не тримає групу (а з нею
            # і SL у `TRADING_STOP`) довше за одне вікно. Якщо біржа справді ще не готова, прийде 10006 → backoff.
            delay = snapshot.reset_ts_ms - now_ms()
            if delay > bucket.window_ms:
                _logger().warning(
                    "rate_limit_reset_beyond_window",
                    role=self._config.role.value,
                    group=group.value,
                    reset_in_ms=delay,
                    window_ms=bucket.window_ms,
                )
            if delay > 0:
                hold = now + min(delay, bucket.window_ms)
        lowered = bucket.clamp(now, limit=snapshot.limit, remaining=snapshot.remaining, hold_until=hold)
        if lowered:
            _logger().warning(
                "rate_limit_capacity_lowered",
                role=self._config.role.value,
                group=group.value,
                limit=bucket.capacity,
            )
        self._fail_impossible()
        self._wake.set()

    def penalize(self, *, retry_after_ms: int | None = None, ip_ban: bool = False) -> None:
        """Пауза всієї черги: 429 / 10006 / бан IP. Наступні `acquire` чекають кінця паузи.

        На закритому лімітері — нічого не робить: запит уже був у мережі, і `RateLimited` викликача не можна
        підміняти на `RateLimiterError` («запит не відправлявся»).
        """
        if retry_after_ms is not None and (type(retry_after_ms) is not int or retry_after_ms < 0):
            raise ValueError("retry_after_ms має бути int мс ≥ 0")
        if type(ip_ban) is not bool:
            raise ValueError("ip_ban має бути bool")
        if self._closed:
            return
        # Одна сходинка на епізод паузи. Пачка відповідей 429, що прийшли, поки черга вже стоїть,
        # не розганяє backoff і не заморожує SL на хвилини.
        if monotonic_ms() >= self._paused_until:
            self._strikes += 1
        pause = self._pause_ms(retry_after_ms=retry_after_ms, ip_ban=ip_ban)
        self._paused_until = max(self._paused_until, monotonic_ms() + pause)
        _logger().warning(
            "rate_limit_paused",
            role=self._config.role.value,
            pause_ms=pause,
            strikes=self._strikes,
            ip_ban=ip_ban,
            retry_after_ms=retry_after_ms,
        )
        self._wake.set()

    def note_success(self) -> None:
        """Успішна відповідь скидає сходинку backoff. Поточну паузу не скорочує."""
        self._strikes = 0

    async def call(
        self,
        *,
        priority: RequestPriority,
        group: EndpointGroup,
        operation: Callable[[], Awaitable[_T]],
        retry: bool,
        weight: int = 1,
    ) -> _T:
        """`acquire`, потім `operation`. `RateLimited` ставить паузу; повтор — лише за `retry=True`.

        `place_order` / `cancel_order` передають `retry=False` (`NON_RETRYABLE_METHODS`).
        Інші винятки не ховаються і backoff не чіпають.
        """
        if type(retry) is not bool:
            raise ValueError("retry має бути bool")
        attempts = 0
        while True:
            await self.acquire(priority=priority, group=group, weight=weight)
            try:
                result = await operation()
            except RateLimited as exc:
                attempts += 1
                self.penalize(retry_after_ms=exc.retry_after_ms, ip_ban=exc.ip_ban)
                # Закритий лімітер — не повторювати: викликач має отримати саме `RateLimited`.
                if not retry or self._closed or attempts >= self._config.max_attempts:
                    raise
                continue
            self.note_success()
            return result

    async def aclose(self) -> None:
        """Зупинити чергу. Ті, хто ще чекає слот, отримують `CancelledError`."""
        self._closed = True
        self._wake.set()
        task = self._drain_task
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._drain_task = None
        for waiter in list(self._waiters):
            self._discard(waiter)
            if not waiter.future.done():
                waiter.future.cancel()

    async def __aenter__(self) -> RateLimiter:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def _check_role(self, group: EndpointGroup) -> None:
        if group is EndpointGroup.MARKET or self._config.role is ProcessRole.TRADING:
            return
        raise ValueError(f"{self._config.role.value}: приватна група {group.value} лише для торгового процесу")

    def _check_weight(self, group: EndpointGroup, weight: int) -> None:
        _require_int("weight", weight, minimum=1, maximum=MAX_WEIGHT)
        bucket = self._buckets.get(group)
        if bucket is None:
            if group is not EndpointGroup.MARKET:
                raise ValueError(f"невідома група {group.value}")
            if weight != 1:
                raise ValueError("публічний запит без UID-ліміту має вагу 1")
            return
        if weight > bucket.capacity:
            raise ValueError(f"вага {weight} більша за місткість {group.value} ({bucket.capacity})")

    def _ensure_drain(self) -> None:
        task = self._drain_task
        if task is not None and task.done():
            with contextlib.suppress(asyncio.CancelledError, RateLimiterError):
                task.result()
            self._drain_task = None
        if self._drain_task is None:
            self._drain_task = asyncio.create_task(self._drain_loop(), name="rate-limiter")

    async def _drain_loop(self) -> None:
        try:
            while not self._closed:
                self._fail_impossible()
                now = monotonic_ms()
                waiter = self._select(now)
                if waiter is not None:
                    self._spend(waiter, now)
                    self._discard(waiter)
                    if not waiter.future.done():
                        waiter.future.set_result(None)
                    await self._sleep(0)
                    continue
                self._wake.clear()
                if self._closed or self._select(monotonic_ms()) is not None:
                    continue
                delay = self._delay_ms(monotonic_ms())
                if delay is None:
                    await self._wake.wait()
                    continue
                if delay <= 0:
                    # Немає кого слати, але й чекати нічого: поступитися циклу, не валити чергу.
                    await asyncio.sleep(0)
                    continue
                await self._interruptible_sleep(delay)
        finally:
            if not self._closed:
                error = RateLimiterError("чергу лімітера перервано")
                for waiter in list(self._waiters):
                    self._discard(waiter)
                    if not waiter.future.done():
                        waiter.future.set_exception(error)

    def _select(self, now: int) -> _Waiter | None:
        if now < self._paused_until:
            return None
        affordable = [waiter for waiter in self._waiters if not waiter.future.done() and self._fits(waiter, now)]
        if not affordable:
            return None
        protection = [waiter for waiter in affordable if waiter.priority is RequestPriority.PROTECTION]
        if protection:
            return min(protection, key=lambda waiter: waiter.seq)
        starved = [waiter for waiter in affordable if now - waiter.enqueued_mono_ms >= self._config.starvation_ms]
        if starved:
            return min(starved, key=lambda waiter: (waiter.enqueued_mono_ms, waiter.seq))
        return min(affordable, key=lambda waiter: (_PRIORITY_RANK[waiter.priority], waiter.seq))

    def _ip_needed(self, waiter: _Waiter) -> int:
        if waiter.priority is RequestPriority.PROTECTION:
            return 1
        return 1 + self._protection_ip_reserve()

    def _fits(self, waiter: _Waiter, now: int) -> bool:
        if self._ip.available(now) < self._ip_needed(waiter):
            return False
        bucket = self._buckets.get(waiter.group)
        return bucket is None or bucket.available(now) >= waiter.weight

    def _protection_ip_reserve(self) -> int:
        """Скільки IP-слотів лишається під SL/закриття, коли дані заповнюють вікно.

        На малому вікні (тести, частка < 10) резерву немає: інакше дані не пройшли б зовсім.
        """
        ceiling = self._ip.ceiling
        if ceiling < IP_RESERVE_MIN_CAPACITY:
            return 0
        return max(1, ceiling // IP_RESERVE_DIVISOR)

    def _spend(self, waiter: _Waiter, now: int) -> None:
        self._ip.spend(now, 1)
        bucket = self._buckets.get(waiter.group)
        if bucket is not None:
            bucket.spend(now, waiter.weight)

    def _delay_ms(self, now: int) -> int | None:
        if not self._waiters:
            return None
        pause = self._paused_until - now if self._paused_until > now else 0
        soonest = min(self._waiter_delay(waiter, now) for waiter in self._waiters)
        return max(pause, soonest)

    def _waiter_delay(self, waiter: _Waiter, now: int) -> int:
        waits = [self._ip.ms_until(now, self._ip_needed(waiter))]
        bucket = self._buckets.get(waiter.group)
        if bucket is not None:
            waits.append(bucket.ms_until(now, waiter.weight))
        return max(waits)

    def _pause_ms(self, *, retry_after_ms: int | None, ip_ban: bool) -> int:
        # `int ** int` у typeshed — Any (від'ємний степінь дає float). Зсув лишає результат int.
        shift = min(max(self._strikes - 1, 0), 16)
        exp = min(self._config.backoff_cap_ms, self._config.backoff_base_ms * (1 << shift))
        # `Retry-After` і reset не довші за стелю backoff: хибні добу не зупиняють захист.
        reported = 0 if retry_after_ms is None else retry_after_ms
        floor = min(self._config.backoff_cap_ms, reported)
        if ip_ban:
            floor = min(self._config.backoff_cap_ms, max(floor, self._config.ip_ban_ms))
        low = max(floor, exp // 2)
        high = max(low, min(self._config.backoff_cap_ms, max(exp, floor)))
        if high <= low:
            return low
        chosen = self._jitter(low, high)
        if type(chosen) is not int or chosen < low or chosen > high:
            return low
        return chosen

    def _fail_impossible(self) -> None:
        for waiter in list(self._waiters):
            bucket = self._buckets.get(waiter.group)
            if bucket is not None and waiter.weight > bucket.capacity:
                self._discard(waiter)
                if not waiter.future.done():
                    waiter.future.set_exception(
                        RateLimiterError(f"вага {waiter.weight} більша за місткість {waiter.group.value}")
                    )

    def _discard(self, waiter: _Waiter) -> None:
        with contextlib.suppress(ValueError):
            self._waiters.remove(waiter)

    async def _interruptible_sleep(self, delay_ms: int) -> None:
        # `Sleep` — Awaitable, а `create_task` приймає корутину. Задачі мають різні результати
        # (`None` і `bool`), тож їх не складають в один ітерований набір — Task інваріантний.
        sleep_task = asyncio.create_task(self._await_sleep(delay_ms), name="rate-limiter-sleep")
        wake_task = asyncio.create_task(self._wake.wait(), name="rate-limiter-wake")
        try:
            await asyncio.wait({sleep_task, wake_task}, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            sleep_task.cancel()
            wake_task.cancel()
            await _consume(sleep_task)
            await _consume(wake_task)
            raise
        if not sleep_task.done():
            sleep_task.cancel()
        if not wake_task.done():
            wake_task.cancel()
        await _consume(sleep_task)
        await _consume(wake_task)
        if sleep_task.done() and not sleep_task.cancelled():
            sleep_task.result()

    async def _await_sleep(self, delay_ms: int) -> None:
        await self._sleep(delay_ms)


async def _consume(task: asyncio.Task[_T]) -> None:
    with contextlib.suppress(asyncio.CancelledError):
        await task


def _logger() -> BoundLogger:
    # Локальний імпорт: `core.logging` тягне `Settings`, а `Settings` — константи цього модуля.
    from app.core.logging import get_logger

    return get_logger(__name__)


def _require_int(name: str, value: object, *, minimum: int, maximum: int | None = None) -> int:
    # bool — підклас int: частка «True» не є відсотком.
    if type(value) is not int:
        raise ValueError(f"{name} має бути int")
    if value < minimum or (maximum is not None and value > maximum):
        upper = "" if maximum is None else f" … {maximum}"
        raise ValueError(f"{name} має бути в межах {minimum}{upper}")
    return value


def _normalize_ret_code(value: str | int | None) -> str | None:
    if value is None:
        return None
    if type(value) is bool or type(value) not in (str, int):
        return None
    return str(value)


def _system_jitter() -> Jitter:
    rng = random.Random()  # noqa: S311 — jitter паузи rate limiter, не секрет і не ключ

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
