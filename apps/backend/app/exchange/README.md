# `app/exchange` — ExchangeAdapter, адаптер Bybit, FakeExchangeAdapter, rate limiter

## Призначення

Єдина точка доступу до біржі: інтерфейс `ExchangeAdapter`, адаптер Bybit (live / demo / testnet — один код), `FakeExchangeAdapter` з ін'єкцією збоїв для тестів, централізований rate limiter. Прямі виклики CCXT поза цим пакетом заборонені.

> Заплановано: T1.1, T1.2, T1.3, T1.7, T3.2, T3.5. Зараз є лише моделі БД (T0.4).

## Розділи ТЗ

- ТЗ §3 — інтерфейс `ExchangeAdapter`, інкапсуляція `category=linear`
- ТЗ §6.5 — централізований rate limiter і пріоритети
- ТЗ §9.1 — нативні TP/SL
- ТЗ §19 — припущення про Bybit, що перевіряються на Testnet

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `ExchangeAccount`, `Instrument` | `models.py` | таблиці `exchange_accounts`, `instruments` (ТЗ §6.4, §15) |
| `EXCHANGES`, `KEY_CHECK_RESULTS` | `models.py` | допустимі біржі (`bybit`); результати перевірки ключа |

## Таблиці БД

- `exchange_accounts` — акаунт біржі: `name` (UNIQUE), `exchange`, `mode` (`demo`/`testnet`/`live`), `env_key_set`
  (назва набору змінних `.env`, напр. `BYBIT_DEMO` → `BYBIT_DEMO_API_KEY`/`_SECRET`; самих ключів у БД немає;
  **UNIQUE** — два записи з тими самими ключами означали б один реальний акаунт із розділеними лімітами),
  `allowed_host_id` (`^[a-z0-9]{2}$`, ТЗ §6.4), результат перевірки ключа `key_check_result`
  (`OK` / `WITHDRAW_ENABLED` / `KEY_MISSING` / `UNVERIFIABLE`) разом із `key_check_at_ms` і `key_check_details`
  (T3.6 може уточнити набір). `UNIQUE (id, mode)` — ціль складених FK: режим бота, ордера, позиції, fill дорівнює
  режиму акаунта.
- `instruments` — кеш правил, PK `(exchange, mode, symbol)` (символ — канонічний CCXT) (на Testnet правила можуть відрізнятися): `tick_size`,
  `qty_step`, `min_qty` (> 0), `min_notional` (≥ 0), `max_qty`, `max_leverage` (≥ 1), `status` (як повертає біржа;
  «торгується чи ні» визначає T1.7), `updated_at_ms`.

Схема створюється міграцією `migrations/versions/0001_initial_schema.py`; обмеження й тригери перевіряють `tests/db/test_constraints.py`, `tests/db/test_guards.py` і `tests/db/test_migrations.py`.


## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.4 | `[x]` | таблиці `exchange_accounts`, `instruments` |
| T1.1 | `[ ]` | Доменні типи, `ExchangeAdapter`, `FakeExchangeAdapter` |
| T1.2 | `[ ]` | Rate limiter з пріоритетами |
| T1.3 | `[ ]` | Bybit: публічні дані та `fetch_time` |
| T1.7 | `[ ]` | Кеш `instruments` і округлення |
| T3.2 | `[ ]` | Bybit: торгові методи та мапінг помилок |
| T3.5 | `[ ]` | Приватний WebSocket і реконект |
