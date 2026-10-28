---
name: exchange-adapter
description: Використовуй при роботі з ExchangeAdapter, Bybit v5 адаптером, PaperExchangeAdapter, rate limiter, приватним WebSocket або CCXT (apps/backend/app/exchange/). Містить контракт інтерфейсу і правила перевірки поведінки Bybit.
paths:
  - "apps/backend/app/exchange/**"
---

# ExchangeAdapter / Bybit v5

Вимоги: ТЗ §3 (інтерфейс), §6.5 (rate limiter), §9.1 (нативні TP/SL), §11.2 (WS), §13 (Paper), §19 (припущення про Bybit).

## Контракт
Методи: `fetch_candles`, `place_order`, `cancel_order`, `fetch_order_by_client_id`, `fetch_positions`, `fetch_open_orders`, `set_position_protection`, `fetch_time`, `subscribe_private_stream`, `set_leverage`, `set_margin_mode`, `fetch_balance`, `fetch_instruments`, `fetch_funding_history`, `fetch_executions`.
- Вхід/вихід — доменні pydantic-моделі з `Decimal` і `int` мс. Сирі CCXT-dict-и за межі адаптера не виходять.
- Помилки нормалізуються в доменні винятки: `ExchangeTimeout`, `ExchangeUnavailable` (5xx/обрив), `RateLimited`, `OrderRejected(code, reason)`, `DuplicateClientOrderId`, `InsufficientMargin`. Таймаут ніколи не перетворюється на «ордер відхилено».
- `category=linear` береться з конфігу адаптера й підставляється в усі REST/WS запити; інша категорія → адаптер не стартує.
- Підтримуються лише one-way + isolated; інше — відмова на старті.

## Правила
1. **Не вигадуй API Bybit.** Перед використанням поля, параметра чи коду помилки — перевір документацію Bybit v5 / CCXT (MCP `context7`). Якщо перевірити неможливо — `TODO(verify-bybit): …` + тест із маркером `testnet`, що це перевіряє.
2. Усі REST-виклики — через `rate_limiter` з пріоритетом: `PROTECTION` > `RECONCILE` > `DATA`.
3. Таймаути на кожному виклику; retry лише для ідемпотентних читань. `place_order` не ретраїться в адаптері — це рішення машини станів.
4. `PaperExchangeAdapter` реалізує той самий інтерфейс (ТЗ §13): ціни з live WS, комісія, slippage від спреду, затримка 100–300 мс, SL/TP за mark price.
5. Приватний WS: ping/pong, `ws_stale_s`, реконект з backoff+jitter, після реконекту — REST-дозавантаження пропущених подій.
6. `FakeExchangeAdapter` для тестів з ін'єкцією збоїв оновлюється разом з інтерфейсом.
7. Публічні дані деривативів (open interest, premium/mark/index-свічки, long/short ratio) і WS-потік ліквідацій (ТЗ §4.4) отримуються також через адаптер/публічний клієнт `data/`; глибину історії кожного ендпоінта перевір і задокументуй, а не припускай.

## Тести
- Contract-тест: один набір тестів проганяється проти `FakeExchangeAdapter`, `PaperExchangeAdapter` і (з маркером `testnet`) Bybit Testnet.
- Мапінг кожного коду помилки Bybit → доменний виняток.
- Округлення до `tickSize`/`qtyStep` з `fetch_instruments`.
