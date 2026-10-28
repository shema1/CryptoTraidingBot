# `apps/backend` — бекенд

Python 3.11+ (керується `uv`). Три процеси з одного коду: торговий (FastAPI), воркер `python -m app.jobs.worker`
і колектор `python -m app.data.collector` (ТЗ §6.4). Структура пакетів — ТЗ §3; кожен пакет `app/<модуль>/` має
власний `README.md` (шаблон — `docs/templates/backend-module-README.md`).

| Пакет | Відповідальність |
| --- | --- |
| [`api`](app/api/README.md) | REST + WebSocket локального API |
| [`exchange`](app/exchange/README.md) | `ExchangeAdapter`, Bybit, `FakeExchangeAdapter`, rate limiter |
| [`data`](app/data/README.md) | свічки, дані деривативів, колектор, прогалини, час |
| [`ml`](app/ml/README.md) | фічі, симулятор угоди, валідація, бектест, інференс |
| [`trading`](app/trading/README.md) | цикл бота, order manager, машина станів |
| [`risk`](app/risk/README.md) | модель витрат, розмір позиції, ліміти, `RiskGate` |
| [`protection`](app/protection/README.md) | Position Protection Manager |
| [`reconcile`](app/reconcile/README.md) | стартова й періодична звірка |
| [`jobs`](app/jobs/README.md) | воркер задач |
| [`core`](app/core/README.md) | конфіг, логування, годинник, основа БД, Telegram |

## Команди (з `apps/backend`)

```bash
uv sync                                           # залежності (створює/оновлює uv.lock — комітиться)
uv run pytest -q                                  # тести без мережевих (testnet/demo вимкнені за замовчуванням)
uv run pytest -q -m "not slow"                    # швидкі тести
uv run pytest -q -m testnet                       # лише Testnet (потрібні тестові ключі в .env)
uv run pytest -q -m integration                   # схема БД і міграції на ctb_test (потрібна docker compose БД)
uv run alembic upgrade head                       # міграції основної dev-БД (`-x db=test` — тестової)
uv run alembic revision --autogenerate --rev-id 0002 -m "коротко"   # нова міграція (TimescaleDB/тригери — дописати вручну)
uv run alembic check                              # моделі й міграції не розходяться
uv run ruff check . && uv run ruff format --check .
uv run mypy                                       # app/, tests/, migrations/env.py; strict для exchange/ trading/ risk/ protection/ reconcile/ core/
```

## Інструменти якості (`pyproject.toml`)

- **ruff:** E, W, F, I, UP, B, **S** (безпека), **DTZ** (naive datetime), BLE, **T20** (заборона `print`), C4, SIM, PT, ASYNC, RUF,
  **TID251** (заборона `time.time()`, `time.monotonic()`, `datetime.now()` тощо поза `app/core/clock.py`).
  RUF001–003 вимкнено, бо коментарі та рядки — українською. У `tests/` дозволено `assert` (S101); вигадані «секрети»
  (S105/S106) — лише в `tests/test_logging.py` і `tests/test_config.py`; у `migrations/versions/` дозволено довгі рядки (E501: SQL-тексти CHECK з моделей).
- **mypy:** `disallow_untyped_defs` для всього коду; повний набір прапорців `--strict` для торгового ядра і `core`.
- **pytest:** маркери `unit` (ставиться автоматично), `integration`, `testnet`, `demo`, `slow`, `chaos`; `--strict-markers`.
  `testnet` і `demo` вимкнені через `addopts` і вмикаються лише явним `-m`.
- **hypothesis** — для property-based тестів (округлення, sizing, машина станів).

## Конфігурація

`app/core/config.py` читає `.env` з **кореня репозиторію**. Усі змінні описані в `/.env.example`
(тест `tests/test_env_example.py` перевіряє відповідність в обидва боки).

## База даних

- Моделі — `app/<модуль>/models.py` (перелік — `app.core.db.MODEL_MODULES`), спільна основа — `app/core/db.py`;
  таблиці описані в README модулів і ТЗ §15. Інваріанти (унікальність ордерів, fills, сигналів, активного бота на
  символ тощо) тримає сама БД: `UNIQUE`, `CHECK`, складені FK, часткові індекси, тригери append-only.
- Міграції — `migrations/` (Alembic, async asyncpg). Закомічену міграцію не редагувати — лише нова (hook блокує).
  Offline-режим (`--sql`) не підтримується.
- Тригери міграції 0001: append-only журнали, незмінність ордерів і ботів (`ctb_orders_guard`, `ctb_bots_guard`),
  підтвердження небезпечних дій записом `audit_log` (`ctb_require_confirmed_audit`) — деталі в README модулів.
- Integration-тести (`tests/db/`, маркер `integration`) працюють лише з `<POSTGRES_DB>_test`: на старті сесії
  `downgrade base` → `upgrade head`, кожен тест — у транзакції з rollback; фабрики рядків — `tests/db/factories.py`. Без БД — skip з причиною,
  `CTB_REQUIRE_DB=1` перетворює skip на помилку. Async-тести — `pytest-asyncio` (strict, `@pytest.mark.asyncio`).

## Логи і час

- Кожен процес на старті викликає `configure_logging(settings, process=...)` (`app/core/logging.py`): JSON-рядки
  у `<LOG_DIR>/<process>/` — `app.log` (усе), `trading.log` (торгові рішення), `orders.log` (ордери),
  `errors.log` (ERROR+ з будь-якого логера), з ротацією за розміром (`LOG_MAX_BYTES`, `LOG_BACKUP_COUNT`).
  Секрети маскуються в усіх записах, включно з винятками та логами сторонніх бібліотек — деталі в
  [`app/core/README.md`](app/core/README.md).
- Час — лише через `app.core.clock` (`now_ms()`, `monotonic_ms()`), UTC у мілісекундах. У тестах autouse-фікстура
  `fake_clock` (`tests/conftest.py`) підміняє годинник на `FakeClock`; реальний час — лише явним `SystemClock()`.
