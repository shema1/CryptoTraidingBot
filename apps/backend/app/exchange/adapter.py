"""Інтерфейс `ExchangeAdapter` (ТЗ §3; T1.1) — єдина точка доступу торгової логіки до біржі.

Один адаптер Bybit для live / demo / testnet (режим визначає лише адреси й ключі) і `FakeExchangeAdapter` для тестів
виконують цей самий контракт. Прямі виклики CCXT поза `app/exchange/` заборонені (AGENTS.md інваріант 4).

Загальні правила контракту:
- аргументи — keyword-only або доменна модель запиту; результати — доменні моделі `app/exchange/types.py`;
- символ — канонічний формат CCXT (`BTC/USDT:USDT`); Bybit `category=linear` інкапсульований в адаптері;
- помилки — лише винятки `app/exchange/errors.py`. Мережева помилка **ніколи** не повертається як «порожньо» /
  `None` / «не знайдено»: порожній результат означає, що біржа відповіла й підтвердила відсутність;
- код помилки біржі, якого немає в явному переліку мапінгу адаптера (T3.2), — `ExchangeOutcomeUnknown`, а не
  `RequestRejected`: «біржа відповіла» ще не означає «точно не виконала» (`app/exchange/errors.py`);
- елемент відповіді, який не вдалося нормалізувати, робить нерозбірною всю відповідь (`MalformedResponse`), а не
  пропускається. Тому захист і Emergency Stop запитують точково (`fetch_positions(symbols=[s])`,
  `fetch_open_orders(symbol=s)`), щоб «зламаний» ордер на чужому символі не засліпив захист їхнього;
- адаптер не повторює запити, що змінюють стан (`place_order`, `cancel_order`, `set_*`): повтор — рішення машини
  станів (ТЗ §7.2). Ідемпотентні читання адаптер може повторювати сам (з backoff і лімітом спроб);
- одиниці: час — `int` мс UTC, ціни/кількості/гроші — `Decimal`, кількість — у базовій валюті контракту.

Конфігурацію акаунта (category = linear, one-way, isolated) реальний адаптер перевіряє при старті й не стартує з
іншою (`AdapterConfigError`, ТЗ §3, §9.1, §11.1 п. 3) — окремого методу протоколу для цього немає (T1.3/T3.2).
Адаптер не вирішує, чи можна збільшувати експозицію: вхід — лише після `RiskGate` (AGENTS.md інваріант 5, T4.1).

Пріоритет rate limiter (ТЗ §6.5) задає викликач там, де той самий метод буває і захистом, і звіркою, і даними
(`place_order`, `cancel_order`, `fetch_order_by_client_id`, `fetch_open_orders`, `fetch_positions`, `fetch_balance`,
`fetch_executions`, `set_leverage`, `set_margin_mode`) — аргумент `priority` обов'язковий. Решта REST-методів має
фіксований клас у `FIXED_METHOD_PRIORITY` (`set_position_protection` — завжди `PROTECTION`, свічки й funding —
`DATA`, `fetch_time` — `RECONCILE`). Приватний потік — не REST, пріоритету немає. Рекомендація для торгового циклу:
закриття й SL — `PROTECTION`; звірка й розв'язання `SUBMISSION_UNKNOWN` — `RECONCILE`; нові входи, свічки й історія —
`DATA` (вхід може зачекати, захист — ні; великий backfill іде іншим процесом зі своєю часткою ліміту).
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Collection
from decimal import Decimal
from typing import Protocol

from app.exchange.rate_limit import RequestPriority
from app.exchange.types import (
    Balance,
    Candle,
    Fill,
    FundingRate,
    Instrument,
    MarginMode,
    Order,
    OrderAck,
    PlaceOrderRequest,
    Position,
    PositionProtection,
    PrivateStreamEvent,
    Timeframe,
)


class ExchangeAdapter(Protocol):
    """Контракт біржі. Методи перелічено в ТЗ §3; docstring кожного методу — частина контракту."""

    async def fetch_time(self) -> int:
        """Час біржі, мс UTC (ТЗ §4.3: розбіжність > 1 с з локальним годинником блокує нові входи).

        Пріоритет фіксований: `RECONCILE` (`FIXED_METHOD_PRIORITY`).
        """
        ...

    async def fetch_instruments(self, *, symbols: Collection[str] | None = None) -> list[Instrument]:
        """Правила інструментів (tick, крок і межі кількості, мін. номінал, макс. плече, статус, час лістингу).

        `symbols=None` — усі інструменти категорії; невідомі символи в результаті просто відсутні.
        Пріоритет фіксований: `DATA`.
        """
        ...

    async def fetch_candles(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[Candle]:
        """Свічки з `open_time_ms` у `[start_ms, end_ms)`, за зростанням, без дублів.

        Лише **закриті за часом біржі**: `close_time_ms ≤` час сервера (з тієї самої відповіді або `fetch_time`),
        а не за локальним годинником — він може випереджати біржу до 1 с (ТЗ §4.3), і неповна свічка потрапила б у БД
        назавжди (`ON CONFLICT DO NOTHING`). Свічка, що ще формується, не повертається ніколи. Інтерполяції немає:
        пропуск на біржі — пропуск у результаті (ТЗ §4.2). `limit` — не більше стількох найраніших свічок діапазону;
        `None` — усі (адаптер пагінує сам). Реалізація може тримати запас остаточності після закриття (Bybit —
        `KLINE_FINALITY_MARGIN_MS`, 500 мс; фейк — без запасу): свічка в межах запасу ще не повертається, і це не
        пропуск. TODO(verify-bybit): чи вистачає запасу на mainnet (T1.3).
        Пріоритет фіксований: `DATA`.
        """
        ...

    async def fetch_funding_history(
        self, *, symbol: str, start_ms: int, end_ms: int | None = None
    ) -> list[FundingRate]:
        """Історія funding rate з `funding_time_ms` у `[start_ms, end_ms)`, за зростанням (ТЗ §4.1).

        Пріоритет фіксований: `DATA`.
        """
        ...

    async def place_order(self, request: PlaceOrderRequest, *, priority: RequestPriority) -> OrderAck:
        """Відправити ордер. Викликається лише після коміту `SUBMITTING` (write-ahead, ТЗ §7.2).

        Успіх (`OrderAck`) — біржа **прийняла** ордер; статус і fills приходять окремо (WS або REST) і можуть прийти
        раніше за цю відповідь. Ордер точно не створено лише при `is_definite_rejection(exc)`; будь-який інший
        виняток — `SUBMISSION_UNKNOWN` (`app/exchange/errors.py`). У `OrderRejected` адаптер мапить лише явно
        перелічені коди відмов; невідомий код — `ExchangeOutcomeUnknown`. Адаптер **не** повторює `place_order`.
        Нативні SL/TP запиту ставляться на позицію в режимі `Full` (ТЗ §9.1); позиція — one-way (`positionIdx=0`).

        TODO(verify-bybit): чи відхиляє Bybit повторний `orderLinkId` назавжди чи лише серед активних (ТЗ §19, T3.7);
        як біржа обробляє reduceOnly-ордер, більший за позицію; чи діє `minNotional` на reduceOnly-ордери.

        `priority`: закриття й аварійний вихід — `PROTECTION`; новий вхід — `DATA` (може зачекати за захистом і
        звіркою). Адаптер цей виклик не повторює після ліміту (`NON_RETRYABLE_METHODS`).
        """
        ...

    async def cancel_order(
        self,
        *,
        priority: RequestPriority,
        symbol: str,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> None:
        """Скасувати ордер за **одним** з ідентифікаторів (обидва або жоден — `ValueError` без мережевого виклику).

        Успіх — біржа прийняла скасування; підтвердження стану (`CANCELED` або встиг `FILLED`) — з WS/REST.
        `OrderNotActive` — ордера немає серед активних (виконаний, скасований або не існує): скасування не відбулося.
        `priority`: аварійне скасування — `PROTECTION`, інакше `RECONCILE`. Після ліміту не повторюється.
        """
        ...

    async def fetch_order_by_client_id(
        self, *, priority: RequestPriority, symbol: str, client_order_id: str
    ) -> Order | None:
        """Ордер за `clientOrderId` у будь-якому статусі, включно з виконаними й скасованими.

        `None` — лише коли біржа підтвердила, що такого ордера немає (з урахуванням історії ордерів, а не лише
        активних). Помилка мережі/біржі — виняток, ніколи не `None` (на цьому тримається розв'язання
        `SUBMISSION_UNKNOWN`, ТЗ §7.2). TODO(verify-bybit): Demo зберігає ордери 7 днів (ТЗ §13).
        `priority`: розв'язання невідомого й звірка — `RECONCILE`; читання заради рішення про закриття — `PROTECTION`.
        """
        ...

    async def fetch_open_orders(self, *, priority: RequestPriority, symbol: str | None = None) -> list[Order]:
        """Активні ордери (`NEW`, `PARTIALLY_FILLED`, `UNTRIGGERED`; також `TRIGGERED` і `UNKNOWN`, якщо біржа їх
        повертає), включно з умовними (TP/SL) і ручними. `symbol=None` — по всіх символах категорії (адаптер пагінує
        сам). Захист і Emergency Stop запитують точково за символом (див. docstring модуля). `priority` — як у
        `fetch_order_by_client_id`."""
        ...

    async def fetch_positions(
        self, *, priority: RequestPriority, symbols: Collection[str] | None = None
    ) -> list[Position]:
        """Ненульові позиції (one-way). `symbols=None` — по всіх символах категорії.

        Позиція в hedge mode → `AdapterConfigError`, а не пропуск: невидима позиція не може бути «безпечною».
        `priority`: перевірка «чи є SL / чи закривати» — `PROTECTION`; звірка — `RECONCILE`.
        """
        ...

    async def set_position_protection(self, request: PositionProtection) -> None:
        """Встановити SL (і, за наявності, TP) на позицію в режимі `Full` (ТЗ §9.1; Bybit `trading-stop`).

        Ідемпотентно: ті самі SL/TP, що вже стоять, — успіх (Bybit «not modified» — не відмова). Інакше повтор після
        втраченої відповіді давав би «відмову» на вже захищеній позиції і вів би до її примусового закриття (ТЗ §9.2).
        Успіх — біржа прийняла запит. Висновок «SL є» робиться **лише** з позиції (`fetch_positions`/WS: ненульовий
        `stop_loss`, ТЗ §9.2), а не з результату чи типу винятку цього виклику: `RequestRejected` — цей запит не
        застосовано (попередній SL міг лишитися), `ExchangeOutcomeUnknown` — невідомо. Рух SL лише в бік зменшення
        ризику перевіряє Position Protection Manager, а не адаптер.
        TODO(verify-bybit): код «not modified» для `trading-stop` (T3.2).
        Пріоритет фіксований: `PROTECTION` — зняти його аргументом неможливо.
        """
        ...

    async def set_leverage(self, *, priority: RequestPriority, symbol: str, leverage: Decimal) -> None:
        """Встановити плече символу. Ідемпотентно: те саме значення — успіх (Bybit «not modified» — не помилка)."""
        ...

    async def set_margin_mode(self, *, priority: RequestPriority, symbol: str, mode: MarginMode) -> None:
        """Встановити режим маржі. Ідемпотентно: той самий режим — успіх.

        TODO(verify-bybit): в Unified Trading Account режим маржі задається на рівні акаунта, а не символу (T3.2/T3.7).
        """
        ...

    async def fetch_balance(self, *, priority: RequestPriority) -> Balance:
        """Баланс у валюті розрахунку (USDT): wallet balance, equity, доступна маржа, нереалізований PnL."""
        ...

    async def fetch_executions(
        self, *, priority: RequestPriority, start_ms: int, end_ms: int | None = None, symbol: str | None = None
    ) -> list[Fill]:
        """Торгові виконання з `ts_ms` у `[start_ms, end_ms)`, за зростанням часу (дозавантаження пропущених fills,
        ТЗ §11.2). Без funding (він — у `ledger`, T3.4). Адаптер сам ділить діапазон і пагінує."""
        ...

    def subscribe_private_stream(self) -> AsyncGenerator[PrivateStreamEvent, None]:
        """Приватний потік (order, execution, position, wallet; ТЗ §11.2). Споживач закриває його `aclose()`.

        Перша подія — `StreamConnected` (підписки підтверджено). Події можуть **дублюватися, губитися й приходити
        не по порядку**: споживач дедуплікує fills за `exec_id` і звіряє стан через REST. Втрата з'єднання
        (ping/pong, `ws_stale_s`, обрив) — виняток `PrivateStreamLost` з ітератора; реконект — новий виклик,
        а події за час розриву дозавантажуються через REST (`fetch_executions`, `fetch_open_orders`, `fetch_positions`).
        """
        ...
