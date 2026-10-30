# `apps/backend` — бекенд

Python 3.11+ (керується `uv`). Три процеси з одного коду: торговий `python -m app` (`app/__main__.py`), воркер
`python -m app.jobs.worker` і колектор `python -m app.data.collector` (ТЗ §6.4). Структура пакетів — ТЗ §3; кожен пакет `app/<модуль>/` має
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
uv run pytest -q -m testnet                       # лише Testnet; публічні дані T1.3 ключів не потребують (приватні виклики — T3.x, ключі в .env); тест остаточності kline — ще й slow (до ~85 с), без нього: -m "testnet and not slow"
uv run pytest -q -m integration                   # схема БД, міграції, lock на ctb_test (потрібна docker compose БД)
uv run python -m app                              # торговий процес: lock + локальний API на 127.0.0.1:API_PORT
uv run python -m app.jobs.worker                  # воркер задач (backfill, далі Optuna); без торгових ключів і без API_TOKEN
uv run alembic upgrade head                       # міграції основної dev-БД (`-x db=test` — тестової)
uv run alembic revision --autogenerate --rev-id 0002 -m "коротко"   # нова міграція (TimescaleDB/тригери — дописати вручну)
uv run alembic check                              # моделі й міграції не розходяться
uv run ruff check . && uv run ruff format --check .
uv run mypy                                       # app/, tests/, migrations/env.py; strict для api/ exchange/ trading/ risk/ protection/ reconcile/ core/
```

## Інструменти якості (`pyproject.toml`)

- **ruff:** E, W, F, I, UP, B, **S** (безпека), **DTZ** (naive datetime), BLE, **T20** (заборона `print`), C4, SIM, PT, ASYNC, RUF,
  **TID251** (заборона `time.time()`, `time.monotonic()`, `datetime.now()` тощо поза `app/core/clock.py`).
  RUF001–003 вимкнено, бо коментарі та рядки — українською. У `tests/` дозволено `assert` (S101); вигадані «секрети»
  (S105/S106) — лише в `tests/test_logging.py` і `tests/test_config.py`; у `migrations/versions/` дозволено довгі рядки (E501: SQL-тексти CHECK з моделей).
- **mypy:** `disallow_untyped_defs` для всього коду; повний набір прапорців `--strict` для торгового ядра, `core` і `api` (межа безпеки).
- **pytest:** маркери `unit` (ставиться автоматично), `integration`, `testnet`, `demo`, `slow`, `chaos`; `--strict-markers`.
  `testnet` і `demo` вимкнені через `addopts` і вмикаються лише явним `-m`.
- **hypothesis** — для property-based тестів (округлення, sizing, машина станів).

## Торговий процес (`python -m app`)

```bash
uv run python -m app                              # торговий процес: lock → робота до SIGTERM/SIGINT (з apps/backend)
```

- Старт: `Settings` → `configure_logging(process="backend")` → перевірка `API_TOKEN` (немає — код 15, до БД) →
  single-instance lock (`app/core/instance_lock.py`, деталі — [`app/core/README.md`](app/core/README.md)) → робота під
  watchdog-ом lock → звільнення lock. Робота — локальний API (`app/api/server.py`, деталі —
  [`app/api/README.md`](app/api/README.md)): uvicorn лише на `127.0.0.1:API_PORT`, bearer-токен, перевірка
  `Host`/`Origin`, CORS, `Idempotency-Key`, `GET /system/health` (> Заплановано: цикли ботів — етап 5).
- Перевірка, що API живий (токен через stdin, а не в аргументах — інакше його видно в `ps`):
  `printf 'Authorization: Bearer %s\n' "$API_TOKEN" | curl -H @- http://127.0.0.1:8000/system/health`.
- Другий екземпляр на тій самій БД не стартує: `LOCK_WAIT_S` (60 с) повторює спроби, потім пише в лог власника lock
  (`instance_lock_held_by_other`: pid сесії, pid процесу ОС, час старту) і завершується.
- Прапорець перехоплення lock (`--force-takeover`) — **лише людиною** з інтерактивного терміналу (stdin і stderr — TTY),
  після перевірки, що старий процес не працює: показує власника, чекає введення `TAKEOVER` до 120 с (мовчання,
  інша відповідь чи помилка читання — відмова), перевіряє, що власник не змінився, пише `audit_log`, завершує
  сесію-власника. Без TTY — відмова ще до підключення до БД. Ніколи не додавайте його в конфіг служби
  (launchd/systemd, T5.10): кожен перезапуск вбивав би робочий екземпляр. Агентам запуск із цим прапорцем заборонено (hook).
- Коди виходу (`app/__main__.py`):

| Код | Константа | Значення |
| --- | --- | --- |
| 0 | `EXIT_OK` | завершення за SIGTERM/SIGINT, lock звільнено |
| 1 | `EXIT_UNCAUGHT` | необроблений виняток під час роботи (зокрема сервер API зупинився без сигналу) — записується маскованим `uncaught_exception` (CRITICAL, `errors.log`) до зняття хендлерів. Помилка конфігу (`Settings()` до налаштування логера) — стандартний traceback pydantic без введених значень (`hide_input_in_errors`) |
| 2 | — | неправильні аргументи (argparse; скорочення прапорців вимкнено) |
| 10 | `EXIT_LOCK_HELD` | lock тримає інший процес весь `LOCK_WAIT_S` |
| 11 | `EXIT_DB_UNAVAILABLE` | БД недоступна весь `LOCK_WAIT_S` |
| 12 | `EXIT_LOCK_LOST` | lock втрачено під час роботи (рестарт БД, обрив, перехоплення) — fail-closed; новий старт іде через звірку. Зокрема аварійний вихід (`os._exit`), якщо після втрати робота не зупинилась за 5 с |
| 13 | `EXIT_TAKEOVER_REFUSED` | перехоплення не підтверджено (вкл. мовчання 120 с), власник змінився, немає TTY або не записано `audit_log` |
| 14 | `EXIT_STOP_TIMEOUT` | після SIGTERM/SIGINT робота не зупинилась за 5 с — аварійний вихід (`os._exit`) |
| 15 | `EXIT_CONFIG_INVALID` | не задано `API_TOKEN` — процес не стартує (до підключення до БД і перевірки TTY) |
| 16 | `EXIT_API_BIND_FAILED` | `127.0.0.1:API_PORT` зайнятий або недоступний (`api_bind_failed`) — lock звільнено |

## Воркер задач (`python -m app.jobs.worker`)

```bash
uv run python -m app.jobs.worker                  # черга jobs до SIGTERM/SIGINT (з apps/backend)
```

- Старт: `Settings` → `configure_logging(process="worker")` → знижений пріоритет ОС → single-instance lock воркера
  (`WORKER_LOCK_ID`, до `LOCK_WAIT_S`) → публічний адаптер Bybit (mainnet, роль rate limiter `WORKER`) → цикл черги
  під watchdog-ом lock. `API_TOKEN` не потрібен. На одній БД працює лише один воркер: частка rate limit розрахована
  на один процес (`docs/decisions/0008-single-worker.md`). Деталі черги, lock і кодів `jobs.error` —
  [`app/jobs/README.md`](app/jobs/README.md).
- Логи — `logs/worker/`.
- Коди виходу (`app/jobs/worker.py`, ті самі числа, що в торгового процесу):

| Код | Константа | Значення |
| --- | --- | --- |
| 0 | `EXIT_OK` | зупинка за SIGTERM/SIGINT (зокрема під час очікування lock); поточна задача повертається в `QUEUED` (БД недоступна — процес однаково виходить, задача лишається `RUNNING` до прострочення heartbeat) |
| 1 | `EXIT_UNCAUGHT` | необроблений виняток — маскований `uncaught_exception` |
| 2 | — | зайві аргументи (argparse) |
| 10 | `EXIT_LOCK_HELD` | уже працює інший воркер на тій самій БД (весь `LOCK_WAIT_S`) — лог `instance_lock_held_by_other` показує його pid |
| 11 | `EXIT_DB_UNAVAILABLE` | БД недоступна весь `LOCK_WAIT_S` або на першому проході черги |
| 12 | `EXIT_LOCK_LOST` | lock втрачено під час роботи (рестарт БД, обрив); задача лишається `RUNNING` до прострочення heartbeat |
| 14 | `EXIT_STOP_TIMEOUT` | цикл не зупинився за 5 с після зовнішнього скасування — аварійний вихід |
| 15 | `EXIT_CONFIG_INVALID` | не задано `POSTGRES_PASSWORD` |

- Тести процесу: `tests/jobs/test_worker_process.py` (маркери `integration` + `slow`: другий воркер не стартує —
  код 10; SIGTERM під час очікування lock — код 0; втрата lock — код 12; `kill -9` лишає `RUNNING` до наступного
  старту; SIGTERM — код 0 і `QUEUED`).

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
- Single-instance lock: `tests/db/test_instance_lock_db.py` (реальні сесії, перехоплення з `audit_log`) і
  `tests/db/test_instance_lock_process.py` (маркери `integration` + `slow`: реальні процеси `python -m app` — другий
  не стартує, `kill -9` звільняє lock, SIGTERM — код 0, зокрема під час очікування зайнятого lock).
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
