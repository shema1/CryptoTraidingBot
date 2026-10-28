# `app/exchange` — ExchangeAdapter, адаптер Bybit, FakeExchangeAdapter, rate limiter

## Призначення

Єдина точка доступу до біржі: інтерфейс `ExchangeAdapter`, адаптер Bybit (live / demo / testnet — один код), `FakeExchangeAdapter` з ін'єкцією збоїв для тестів, централізований rate limiter. Прямі виклики CCXT поза цим пакетом заборонені.

> Заплановано: T1.1, T1.2, T1.3, T1.7, T3.2, T3.5. Зараз пакет порожній (каркас T0.1).

## Розділи ТЗ

- ТЗ §3 — інтерфейс `ExchangeAdapter`, інкапсуляція `category=linear`
- ТЗ §6.5 — централізований rate limiter і пріоритети
- ТЗ §9.1 — нативні TP/SL
- ТЗ §19 — припущення про Bybit, що перевіряються на Testnet

## Публічний інтерфейс

Поки немає.

## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T1.1 | `[ ]` | Доменні типи, `ExchangeAdapter`, `FakeExchangeAdapter` |
| T1.2 | `[ ]` | Rate limiter з пріоритетами |
| T1.3 | `[ ]` | Bybit: публічні дані та `fetch_time` |
| T1.7 | `[ ]` | Кеш `instruments` і округлення |
| T3.2 | `[ ]` | Bybit: торгові методи та мапінг помилок |
| T3.5 | `[ ]` | Приватний WebSocket і реконект |
