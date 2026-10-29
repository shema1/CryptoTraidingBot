"""Доменні винятки біржового шару (ТЗ §3, §7.1, §7.2; T1.1).

Адаптер нормалізує кожну помилку біржі/мережі в один із цих класів; сирі винятки CCXT/aiohttp/pydantic за межі
`app/exchange/` не виходять. Головне розрізнення — **чи міг запит виконатися**:

```
ExchangeError
├── ExchangeOutcomeUnknown        результат невідомий: запит міг виконатися
│   ├── ExchangeTimeout
│   ├── ExchangeUnavailable       5xx, обрив з'єднання
│   │   └── PrivateStreamLost     приватний WS втрачено (ping/pong, `ws_stale_s`, обрив)
│   └── MalformedResponse         відповідь не вдалося розібрати
├── RateLimited                   ліміт запитів (429 / 10006)
├── DuplicateClientOrderId        ордер з цим clientOrderId УЖЕ є на біржі — це НЕ відмова
├── RequestRejected               біржа точно не виконала запит (код і причина)
│   └── OrderRejected
│       └── InsufficientMargin
├── OrderNotActive                cancel: ордера немає серед активних
├── ExchangeAuthError             ключ, підпис, дозволи, IP
└── AdapterConfigError            конфігурація не підтримується (category ≠ linear, hedge mode, не isolated)
```

**Правило для `place_order`** (ТЗ §7.1, AGENTS.md інваріант 7): ордер точно не створено лише тоді, коли
`is_definite_rejection(exc)` повертає `True`. Будь-що інше — таймаут, 5xx, `RateLimited`, `DuplicateClientOrderId`,
`ExchangeAuthError`, навіть виняток, що не є `ExchangeError`, — це `SUBMISSION_UNKNOWN` і розв'язання за ТЗ §7.2.
Таймаут ніколи не перетворюється на «ордер відхилено».

**Правило мапінгу кодів біржі** (T3.2): у `RequestRejected` і підкласи потрапляють лише коди з **явного переліку**
відмов, для яких документація біржі гарантує, що запит не виконано. Код, якого немає в переліку (новий, «внутрішня
помилка сервісу», незадокументований), — `ExchangeOutcomeUnknown`: «біржа відповіла» ще не означає «точно не
виконала». Помилка в бік «невідомо» коштує кількох перевірок, а в бік «відхилено» — позиції, про яку бот не знає.

Повідомлення винятків не містять секретів: адаптер не передає в них ключі, підписи чи заголовки запиту.
"""

from __future__ import annotations


class ExchangeError(Exception):
    """База всіх доменних помилок біржі."""


class ExchangeOutcomeUnknown(ExchangeError):
    """Результат запиту невідомий: біржа могла його виконати. Для `place_order` → `SUBMISSION_UNKNOWN`."""


class ExchangeTimeout(ExchangeOutcomeUnknown):
    """Відповідь не отримано вчасно."""


class ExchangeUnavailable(ExchangeOutcomeUnknown):
    """5xx, обрив або скидання з'єднання — у т.ч. після того, як запит міг дійти до біржі."""


class PrivateStreamLost(ExchangeUnavailable):
    """Приватний WebSocket втрачено. Споживач переводить ботів акаунта в `RECONCILING` (ТЗ §11.2) і підписується
    знову; події, що прийшли без підписки, дозавантажуються через REST."""


class MalformedResponse(ExchangeOutcomeUnknown):
    """Відповідь біржі не вдалося нормалізувати в доменні типи. Запит міг виконатися — результат невідомий."""


class RateLimited(ExchangeError):
    """Перевищено ліміт запитів. `retry_after_ms` — через скільки мс можна повторити (якщо біржа повідомила)."""

    def __init__(self, message: str = "rate limited", *, retry_after_ms: int | None = None) -> None:
        if retry_after_ms is not None and (type(retry_after_ms) is not int or retry_after_ms < 0):
            raise ValueError("retry_after_ms має бути int мс ≥ 0")
        super().__init__(message)
        self.retry_after_ms = retry_after_ms


class DuplicateClientOrderId(ExchangeError):
    """Біржа повідомила, що ордер з цим `clientOrderId` уже існує.

    Це **не** відмова: ордер, найімовірніше, створено попередньою відправкою, відповідь на яку втрачено.
    Розв'язується як `SUBMISSION_UNKNOWN` — пошуком `fetch_order_by_client_id` (ТЗ §7.2).
    """

    def __init__(self, client_order_id: str) -> None:
        super().__init__(f"clientOrderId already exists: {client_order_id}")
        self.client_order_id = client_order_id


class RequestRejected(ExchangeError):
    """Біржа відповіла відмовою: запит **точно** не виконано. `code` — код біржі, `reason` — її повідомлення.

    Лише для кодів з явного переліку мапінгу адаптера; невідомий код — `ExchangeOutcomeUnknown` (docstring модуля).
    """

    def __init__(self, code: str, reason: str) -> None:
        super().__init__(f"{code}: {reason}")
        self.code = code
        self.reason = reason


class OrderRejected(RequestRejected):
    """Ордер відхилено біржею (фільтри, ціна SL/TP, reduceOnly без позиції тощо): `SUBMITTING → REJECTED`."""


class InsufficientMargin(OrderRejected):
    """Недостатньо маржі для ордера."""


class OrderNotActive(ExchangeError):
    """`cancel_order`: ордера немає серед активних — він уже виконаний, скасований або не існує.

    Скасування не відбулося; фактичний стан — `fetch_order_by_client_id`
    (ТЗ §7.1: `PENDING_CANCEL → FILLED` — легальний перехід).
    """


class ExchangeAuthError(ExchangeError):
    """Ключ недійсний, підпис невірний, бракує дозволів або IP не в whitelist."""


class AdapterConfigError(ExchangeError):
    """Непідтримувана конфігурація: category ≠ linear, hedge mode, режим маржі не isolated (ТЗ §3, §9.1).

    Адаптер з такою конфігурацією не стартує; позиція не в one-way режимі — помилка, а не пропуск.
    """


def is_definite_rejection(exc: BaseException) -> bool:
    """`True` лише якщо біржа точно не виконала запит (`RequestRejected` і підкласи).

    Для `place_order`: `True` → `REJECTED`; `False` → `SUBMISSION_UNKNOWN` (fail-closed).
    """
    return isinstance(exc, RequestRejected)
