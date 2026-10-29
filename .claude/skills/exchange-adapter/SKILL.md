---
name: exchange-adapter
description: Використовуй при роботі з ExchangeAdapter, Bybit v5 адаптером (режими live / demo / testnet), rate limiter, приватним WebSocket або CCXT (apps/backend/app/exchange/). Містить контракт інтерфейсу і правила перевірки поведінки Bybit.
paths:
  - "apps/backend/app/exchange/**"
---

# ExchangeAdapter / Bybit v5

Вимоги: ТЗ §3 (інтерфейс), §6.5 (rate limiter), §9.1 (нативні TP/SL), §11.2 (WS), §13 (режими Demo / Testnet / Live), §19 (припущення про Bybit).

## Контракт
Методи: `fetch_candles`, `place_order`, `cancel_order`, `fetch_order_by_client_id`, `fetch_positions`, `fetch_open_orders`, `set_position_protection`, `fetch_time`, `subscribe_private_stream`, `set_leverage`, `set_margin_mode`, `fetch_balance`, `fetch_instruments`, `fetch_funding_history`, `fetch_executions`.
- Вхід/вихід — доменні pydantic-моделі з `Decimal` і `int` мс. Сирі CCXT-dict-и за межі адаптера не виходять.
- Типи — `app/exchange/types.py` (pydantic strict: `float` у `Decimal`-полях відхиляється, адаптер конвертує рядки біржі явно); контракт кожного методу — docstring `app/exchange/adapter.py`.
- Помилки нормалізуються в доменні винятки `app/exchange/errors.py`: `ExchangeOutcomeUnknown` (`ExchangeTimeout`, `ExchangeUnavailable` — 5xx/обрив, `PrivateStreamLost`, `MalformedResponse` — у т.ч. помилка нормалізації відповіді), `RateLimited(retry_after_ms)`, `DuplicateClientOrderId`, `RequestRejected(code, reason)` → `OrderRejected` → `InsufficientMargin`, `OrderNotActive`, `ExchangeAuthError`, `AdapterConfigError`. Таймаут ніколи не перетворюється на «ордер відхилено».
- Для `place_order` ордер точно не створено лише при `is_definite_rejection(exc)` (`RequestRejected` і підкласи). `DuplicateClientOrderId` — **не** відмова (ордер уже існує), як і `RateLimited`: усе, крім `RequestRejected`, — `SUBMISSION_UNKNOWN`.
- Мапінг кодів Bybit — **allowlist**: у `RequestRejected`/`OrderRejected` лише коди, для яких документація гарантує «запит не виконано»; невідомий або «внутрішній» код (напр. service error) — `ExchangeOutcomeUnknown`. Коди «not modified» (`set_leverage`, `set_margin_mode`, `set_position_protection`) — успіх, не відмова.
- Невідомий статус ордера — `ExchangeOrderStatus.UNKNOWN` (не термінальний), а не `MalformedResponse` для всього списку; класифікація чужих ордерів — `client_order_id_origin()` (префікс `tb` неточного формату = чужий комп'ютер).
- Мережева помилка ніколи не повертається як порожній список / `None`: `fetch_order_by_client_id` → `None` лише при підтвердженій відсутності (з історією ордерів).
- `category=linear` береться з конфігу адаптера й підставляється в усі REST/WS запити; інша категорія → адаптер не стартує.
- Підтримуються лише one-way + isolated; інше — відмова на старті.

## Правила
1. **Не вигадуй API Bybit.** Перед використанням поля, параметра чи коду помилки — перевір документацію Bybit v5 / CCXT (MCP `context7`). Якщо перевірити неможливо — `TODO(verify-bybit): …` + тест із маркером `testnet`, що це перевіряє.
2. Усі REST-виклики — через `rate_limiter` з пріоритетом: `PROTECTION` > `RECONCILE` > `DATA`.
3. Таймаути на кожному виклику; retry лише для ідемпотентних читань. `place_order` не ретраїться в адаптері — це рішення машини станів.
4. Режими demo / testnet / live — **один** адаптер Bybit; режим визначає лише адреси (`api-demo.bybit.com` / `stream-demo.bybit.com` для demo, testnet-адреси, mainnet для live) і набір ключів з `.env`. Власного симулятора виконання (Paper) немає і не додавай його (`docs/decisions/0003-demo-instead-of-paper.md`). Публічні ринкові дані в demo — з mainnet. Обмеження Demo (ордери зберігаються 7 днів, не всі ендпоінти) — враховуй і перевіряй.
5. Приватний WS: ping/pong, `ws_stale_s`, реконект з backoff+jitter, після реконекту — REST-дозавантаження пропущених подій.
6. `FakeExchangeAdapter` (`app/exchange/fake.py`, лише для тестів — жоден модуль `app/**` його не імпортує) оновлюється разом з інтерфейсом. Збої: `fail_next(method, Fault.timeout(applied=True))` (`applied` — «виконано, але відповідь втрачено»), `partial_fill_next(*chunks)`, `ws_drop_next` / `ws_duplicate_next` / `ws_disconnect`, зовнішні ордери й позиції; журнал викликів `calls_to(method)`.
7. Публічні дані деривативів (open interest, premium/mark/index-свічки, long/short ratio) і WS-потік ліквідацій (ТЗ §4.4) отримуються також через адаптер/публічний клієнт `data/`; глибину історії кожного ендпоінта перевір і задокументуй, а не припускай.

## Тести
- Contract-тест: один набір тестів (`tests/exchange/contract.py`, лише методи протоколу) проганяється проти `FakeExchangeAdapter` і (з маркерами `testnet` / `demo`) Bybit Testnet та Bybit Demo.
- Мапінг кожного коду помилки Bybit → доменний виняток.
- Округлення до `tickSize`/`qtyStep` з `fetch_instruments`.
