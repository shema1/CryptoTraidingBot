# План робіт: Crypto Trading Bot — задачі для AI-агентів

Джерело вимог — `AppInfo.md` (далі «ТЗ §N»). Порядок етапів — ТЗ §17.
Статуси: `[ ]` не почато · `[~]` в роботі · `[x]` готово · `[!]` заблоковано / потребує рішення людини.
Взяти наступну задачу в Claude Code: `/next-task` (або `/next-task T3.1`).

---

## 1. Як обирати модель

Код торгує реальними грошима, тому модель підбирається не за «складністю» задачі, а за **ціною помилки**.

| Клас | Що означає помилка | Модель Claude Code | Effort | Режим і контроль |
|---|---|---|---|---|
| **R3 — критичний** | пряма втрата грошей: подвійна позиція, позиція без SL, обхід лімітів, неправильний розмір | **Fable 5.1** — `/model fable` | `xhigh` | Plan Mode → тести спочатку (`test-engineer`) → код → `/safety-check` (субагент `trading-safety-reviewer`) → ви читаєте дифф |
| **R2 — високий** | хибні дані / хибна модель / витік даних — збиток опосередковано, через «погану» модель або пропущені свічки | **Opus 5.5** — `/model opus` | `high` або `xhigh` (вказано) | Plan Mode → тести → код → `/safety-check` (для ML — `ml-leakage-auditor`) |
| **R1 — середній** | функціональна помилка, яку видно одразу (UI, сповіщення, інфраструктура), гроші не під загрозою | **Sonnet 5.5** — `/model sonnet` | `high` | звичайний режим, тести, лінтери |
| **R0 — низький** | рутина, генерація, конфіги | **Haiku 5.5** — `/model haiku` (або Sonnet `medium`) | `medium` | звичайний режим |

Пояснення вибору:
- **Fable 5.1** — найсильніша доступна модель (Mythos-клас). Найкраще тримає в голові складні інваріанти (машина станів + звірка + конкурентність одночасно) і рідше пропускає крайові випадки. Дорожча й має жорсткіші ліміти, тому лише для R3. Якщо Fable недоступна на вашому тарифі — використовуйте `/model best` (автоматично Opus) з `xhigh` і **обов'язково** друге рев'ю.
- **Opus 5.5** — сильне міркування для ML і даних, де помилки тонкі (look-ahead, embargo), але не призводять до миттєвої втрати.
- **Sonnet 5.5** — швидкий і достатньо точний для UI, API-обв'язки, інфраструктури.
- **Haiku 5.5** — лише для механічних задач, де результат перевіряється компілятором/тестом.

**Cursor:** оберіть у списку моделей ту саму модель Claude, якщо вона там доступна, або найсильнішу доступну. R3-задачі краще виконувати в Claude Code, де працюють субагенти-рев'юери й hooks; якщо робите в Cursor — рев'ю `/safety-check` запускайте в Claude Code.

### Наскрізні правила для всіх задач
1. Одна задача — одна сесія. Між задачами `/clear`, щоб контекст попередньої не «протікав».
2. R2/R3: спершу план, ви його затверджуєте, тільки потім код.
3. Тести пишуться **до** реалізації і мають падати з очікуваної причини.
4. Автор коду не є єдиним рев'юером: рев'ю робить субагент у свіжому контексті, а фінальне рішення — ви.
5. Жодна задача до етапу 8 не торгує на mainnet і не торкається live-ключів; публічні ринкові дані mainnet без ключів читати можна.
6. **Тести:** бекенд і ML — обов'язково (unit, property-based, збої з `FakeExchangeAdapter`, інтеграційні з БД, Testnet, chaos; для ML — look-ahead, parity, перемішані мітки). Веб — лише `npm run typecheck` і тест покриття підказок (ТЗ §16А.4); e2e і тести компонентів не пишемо, бо UI не містить бізнес-логіки, а небезпечні дії перевіряє бекенд.
7. **Документація = частина задачі.** Задача не `[x]`, поки документація не відповідає коду: ТЗ, README модуля (кожен модуль `apps/backend/app/<модуль>/` має свій), `docs/**`, `.env.example`, довідка UI. Розбіжність коду з ТЗ вирішує користувач; зміни ТЗ фіксуються в `docs/decisions/NNNN-*.md`. Правило: `.claude/rules/documentation.md`.
8. Після задачі: статус тут, коротка примітка (зокрема, які документи оновлено), коміт. Вимога до іншої задачі — пункт у рядку «Передано з попередніх задач» розділу цільової задачі (з ID джерела), а не лише в примітці; питання без цільової задачі — у розділ 5.

---

## 2. Зведена таблиця

| ID | Задача | Ризик | Модель / effort | Залежить від |
|---|---|---|---|---|
| T0.1 | Каркас монорепозиторію та інструменти якості | R1 | sonnet / high | — |
| T0.2 | Docker Compose: TimescaleDB | R1 | sonnet / medium | T0.1 |
| T0.3 | Конфіг, логування з маскуванням, `clock` | R2 | opus / high | T0.1 |
| T0.4 | Схема БД і базові міграції (ТЗ §15) | R2 | opus / xhigh | T0.2, T0.3 |
| T0.6 | Single-instance advisory lock | R3 | fable / xhigh | T0.4 |
| T0.7 | Каркас FastAPI і безпека локального API | R2 | opus / high | T0.3 |
| T1.1 | Доменні типи, інтерфейс `ExchangeAdapter`, `FakeExchangeAdapter` | R3 | fable / xhigh | T0.3 |
| T1.2 | Централізований rate limiter з пріоритетами | R2 | opus / xhigh | T1.1 |
| T1.3 | Bybit: публічні дані та `fetch_time` | R2 | opus / high | T1.1, T1.2 |
| T1.4 | Backfill свічок і funding | R2 | opus / high | T0.4, T1.3 |
| T1.5 | Виявлення прогалин (`data_gaps`) | R2 | opus / high | T1.4 |
| T1.6 | Синхронізація та стрибки часу | R2 | opus / high | T1.3 |
| T1.7 | Кеш `instruments` і округлення | R3 | fable / xhigh | T1.3 |
| T1.8 | Воркер задач (`jobs`) | R1 | sonnet / high | T0.4 |
| T1.9 | Backfill даних деривативів | R2 | opus / high | T1.4 |
| T1.10 | Колектор ліквідацій (запустити якомога раніше) | R1 | sonnet / high | T0.4, T1.3 |
| T2.1 | `compute_features()` + буфер (1h, 4h/1d, деривативи, кілька символів) | R2 | opus / xhigh | T1.5, T1.9 |
| T2.2 | Модель витрат, `calc_position_size()`, ліміти `risk/limits.py` | R3 | fable / xhigh | T1.7 |
| T2.3 | Симулятор угоди, розмітка і ваги унікальності | R2 | opus / xhigh | T2.1, T2.2, T1.9 |
| T2.4 | Бектестер (виконання і ліміти = live) | R2 | opus / xhigh | T2.3 |
| T2.5 | Валідація: walk-forward, embargo, hold-out, журнал спроб | R2 | opus / xhigh | T2.4 |
| T2.6 | Статистика: блочний bootstrap, DSR, PBO, коридор, Gate G1 | R2 | opus / xhigh | T2.5 |
| T2.7 | Бейзлайни і первинна стратегія | R2 | opus / high | T2.4, T2.5 |
| T2.8 | Meta-модель LightGBM і метрики | R2 | opus / high | T2.6, T2.7 |
| T2.9 | Калібрування і правило очікуваної вигоди | R2 | opus / high | T2.8 |
| T2.10 | Optuna як задача воркера | R1 | sonnet / high | T1.8, T2.8 |
| T2.11 | Артефакти, відтворюваність, статуси, перенавчання рецепта | R3 | fable / xhigh | T2.9 |
| T2.12 | Дослідницький цикл моделі (з людиною) | R2 | opus / high | T2.6–T2.11 |
| T3.1 | Машина станів ордера | R3 | fable / xhigh | T0.4, T1.1 |
| T3.2 | Bybit: торгові методи та мапінг помилок | R3 | fable / xhigh | T1.2, T3.1 |
| T3.3 | Розв'язання `SUBMISSION_UNKNOWN` | R3 | fable / xhigh | T3.2 |
| T3.4 | Fills, `ledger`, `equity_snapshots` | R3 | fable / xhigh | T3.2 |
| T3.5 | Приватний WebSocket і реконект | R3 | fable / xhigh | T3.2 |
| T3.6 | Ключі з `.env` і перевірка дозволів при старті | R2 | opus / high | T0.3, T3.2 |
| T3.7 | Перевірка припущень про Bybit на Testnet і Demo | R2 | opus / high | T3.2–T3.5 |
| T4.1 | `RiskGate` і pre-trade перевірки | R3 | fable / xhigh | T2.2, T3.4 |
| T4.2 | Ліміти рахунку та статуси Risk Engine | R3 | fable / xhigh | T4.1 |
| T4.3 | Position Protection Manager | R3 | fable / xhigh | T3.3, T3.5 |
| T4.4 | Звірка при старті та правила розбіжностей | R3 | fable / xhigh | T4.3 |
| T4.5 | Періодична звірка і розрив WS | R3 | fable / xhigh | T4.4 |
| T4.6 | Протокол Emergency Stop | R3 | fable / xhigh | T4.2, T4.4 |
| T4.7 | Chaos-тести на `FakeExchangeAdapter` | R2 | opus / xhigh | T4.1–T4.6 |
| T5.1 | Статуси бота, OCC, блокування | R3 | fable / xhigh | T0.6, T4.1 |
| T5.2 | Цикл бота на закритті свічки | R3 | fable / xhigh | T5.1, T2.11, T4.3 |
| T5.3 | Входи, виходи, time-stop | R3 | fable / xhigh | T5.2 |
| T5.4 | Перемикання моделі (`MODEL_SWAP`) | R3 | fable / xhigh | T5.2 |
| T5.6 | Telegram-сповіщення | R1 | sonnet / high | T0.3 |
| T5.7 | Dead-man's switch | R1 | sonnet / medium | T5.6 |
| T5.8 | Drift-моніторинг і коридор результатів | R2 | opus / high | T5.2, T2.6 |
| T5.9 | Резервні копії та перевірка відновлення | R1 | sonnet / medium | T0.2 |
| T5.10 | Середовище запуску й автозапуск (Windows + WSL2 і macOS) | R1 | sonnet / medium | T5.2 |
| T5.11 | Demo-прогін на реальних цінах | R2 | opus / high | T5.1–T5.7 |
| T6.1 | REST API (ТЗ §16) | R2 | opus / high | T5.1 |
| T6.2 | Небезпечні дії в API | R3 | fable / xhigh | T6.1, T4.6 |
| T6.3 | WebSocket-канали для UI | R1 | sonnet / high | T6.1 |
| T6.4 | Генерація TS-типів з OpenAPI | R0 | haiku / medium | T6.1 |
| T6.5 | `<FieldHelp>`, файл довідки, CI-перевірка | R1 | sonnet / high | T6.4 |
| T6.6 | Тексти підказок українською | R1 | sonnet / high | T6.5 |
| T6.7 | Дашборд, боти, позиції, журнал рішень | R1 | sonnet / high | T6.3, T6.5 |
| T6.8 | Графік (Lightweight Charts) | R0 | sonnet / medium | T6.7 |
| T6.9 | ML Studio | R1 | sonnet / high | T6.7, T2.10 |
| T6.10 | UI ризиків і Emergency Stop | R2 | opus / high | T6.2, T6.7 |
| T7.1 | Chaos-сценарії на Testnet | R3 | fable / xhigh | T4.7, T5.11 |
| T7.2 | 7-денний Testnet-прогін і аналіз | R2 | opus / high | T7.1 |
| T7.3 | Незалежний аудит торгового шляху перед Live | R3 | fable / max | T7.2 |
| T8.1 | Pre-live чекліст (ТЗ §18.3) | R3 | fable / high | T7.3 |
| T8.2 | Live з мінімальним розміром | — | лише людина | T8.1 |

---

## 3. Задачі

### Етап 0. Фундамент

#### T0.1 · Каркас монорепозиторію та інструменти якості `[x]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `high`) · **ТЗ:** §2, §3, §14
- **Зробити:** структура тек з ТЗ §3; `apps/backend/pyproject.toml` (uv, Python 3.11+), ruff (вкл. правила `S`, `B`, `DTZ`), mypy (strict для `exchange/ trading/ risk/ protection/ reconcile/ core/`), pytest з маркерами `unit/integration/testnet/slow/chaos`, hypothesis; `apps/web` (Vite + React + TS, скрипти `typecheck`, `test`, `gen:api`); pre-commit (ruff, mypy, `python3 scripts/sync_ai_rules.py --check`, заборона комітів `.env`); `.env.example` без значень; README з командами; `git init` (потрібен для hook перевірки документації); шаблон `docs/decisions/0000-template.md`; шаблон `README.md` для модулів бекенду; тест, що всі поля конфігу присутні в `.env.example`.
- **Готово, коли:** `uv run pytest`, `ruff`, `mypy`, `npm run typecheck` проходять на порожньому каркасі; pre-commit встановлено; кожен пакет у `apps/backend/app/` має `README.md`.
- **Примітки (2026-10-28):** створено каркас: `apps/backend` (pyproject з ruff/mypy/pytest/hypothesis, пакети ТЗ §3 з README, мінімальний `core/config.py`, тести структури, `.env.example` і `scripts/check_no_env_files.py`), `apps/web` (Vite + React + TS; `gen:api` — заглушка до T6.4), `.pre-commit-config.yaml`, `.env.example`, README кореня й модулів, шаблон `docs/templates/backend-module-README.md`. Git-репозиторій уже існував. ruff і pytest (22 тести) пройдено в хмарному середовищі; **На Mac (2026-10-28) виконано:** `uv sync`, `npm install` (створено `uv.lock`, `package-lock.json` — закомітити), `uv run mypy`, `npm run typecheck`, pytest (22), `pre-commit run --all-files`; додано `pythonpath` у pytest та відсутній `.pre-commit-config.yaml`. `.claude/` встановлено (`ai-setup/install.sh`), усі хуки `pre-commit run --all-files` проходять. `pre-commit install` виконано (2026-10-28). `npm audit` — 3 вразливості у dev-залежності vitest: рішення користувача (2026-10-28) — залишити до наступної задачі (апгрейд до 5.x — breaking change)  Документація: README.md, apps/backend/README.md, apps/web/README.md, README модулів, .env.example, AGENTS.md (mypy strict включає `core/`).

#### T0.2 · Docker Compose: TimescaleDB `[x]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `medium`) · **ТЗ:** §2, §12.1, §12.2
- **Зробити:** сервіс `timescaledb` з `restart: unless-stopped`, `healthcheck` (`pg_isready`), іменований volume, порт лише `127.0.0.1:5432`; init-скрипт: основна роль і **read-only роль** для MCP (`MCP_DATABASE_URI`); окрема тестова БД для `integration`-тестів.
- **Готово, коли:** `docker compose up -d` → healthy; порт недоступний ззовні; read-only роль не може `INSERT`.
- **Примітки (2026-10-28):** `docker-compose.yml` (образ `timescale/timescaledb:2.17.2-pg16`, порт `127.0.0.1:5432`, `restart: unless-stopped`, healthcheck `pg_isready`, volume `timescaledb_data`, паролі обов'язкові — без них compose відмовляється стартувати); `infra/db/init/01-roles-and-test-db.sh` (роль `ctb_readonly` з `default_transaction_read_only`, `SELECT` на поточні й майбутні таблиці, БД `<POSTGRES_DB>_test`, розширення timescaledb в обох БД; скрипт `/bin/sh`, бо образ Alpine); поля `postgres_*` у `Settings` і `.env.example`; `tests/test_compose.py` (статична перевірка compose); `scripts/check_db.sh` (жива перевірка умов «Готово, коли»); dev-залежність `types-pyyaml` (стаби для mypy). Перевірено на Mac у окремому проєкті compose з одноразовими паролями: healthy, порт `127.0.0.1:5432`, `ctb_readonly` читає, але `INSERT`/`CREATE` → помилка, до `ctb_test` не підключається. Init-скрипт виконується лише на порожньому volume. Документація: README.md, docs/ai/AI_SETUP.md, app/core/README.md, .env.example.

#### T0.3 · Конфіг, логування з маскуванням, `clock` `[x]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §4.3, §12, §14
- **Зробити:** `core/config.py` (pydantic-settings, `SecretStr`, консервативні defaults, валідація); `core/logging.py` (structlog JSON, ротація, канали `trading`/`orders`/`errors`, процесор маскування секретів); `core/clock.py` (`now_ms()`, монотонний годинник, підмінний у тестах).
- **Готово, коли:** тест: секрет у полі/винятку/`repr` не потрапляє в лог; тести використовують фейковий clock.
- **Примітки (2026-10-28):** `core/clock.py` (`now_ms()`/`monotonic_ms()` у `int` мс, `SystemClock`, `FakeClock` з `advance`/`jump_wall`/`set_now`, `use_clock`; холдер — модульна змінна, бо `run_in_executor` не копіює `ContextVar`); autouse-фікстура `fake_clock` у `tests/conftest.py`; ruff `TID251` забороняє `time.time`/`time.monotonic`/`datetime.now` тощо поза `clock.py`. `core/config.py`: `frozen`, `validate_default`, `hide_input_in_errors`, поля `LOG_DIR`/`LOG_MAX_BYTES` (1 MiB…1 GiB)/`LOG_BACKUP_COUNT` (1…100)/`LOG_CONSOLE`, `secret_values()` (вкл. вкладені моделі, колекції, `SecretBytes`). `core/logging.py`: structlog → stdlib, JSON з `ts_ms` із clock, файли `logs/<process>/{app,trading,orders,errors}.log` з ротацією; `SecretMasker` (SecretStr, чутливі назви полів, відомі значення секретів ≥ 6 символів **до** шаблонів і після, шаблони в тексті й DSN, числа, repr об'єктів до рендера, обрізання рядків > 64 KiB); маскування і до передачі в stdlib, і у спільному форматері (рендер один раз на запис); fail-closed заглушки `log_masking_failed`/`log_format_failed`/`log_handler_failed`; `configure_logging` знімає чужі root-хендлери і ставить `sys.excepthook`/`threading.excepthook`/`sys.unraisablehook`. Рев'ю `trading-safety-reviewer` (Opus — Fable недоступна: немає кредитів): блокер (хвіст зареєстрованого секрету після `;`/`,`/`)`/`@`/пробілу) і всі зауваження «важливо» виправлено з регресійними тестами; виправлено й дрібниці (акроніми/множина в назвах, числовий секрет, права наявної теки, від'ємний час `FakeClock`, S105/S106 лише у двох тест-файлах, тест відповідності defaults/меж у документації коду). **Для наступних задач:** T0.7 — uvicorn з `log_config=None` (інакше його логери з `propagate=False` обходять маскування); T3.6/T5.6/T5.7 — ключі біржі, токен Telegram, URL heartbeat лише як `SecretStr`-поля `Settings` (маскуються за значенням; у шляху URL шаблони їх не ловлять). Нова залежність `structlog` 25.5. Тести: 157 passed; ruff, mypy — чисто. Документація: `app/core/README.md`, `apps/backend/README.md`, `.env.example`.

#### T0.4 · Схема БД і базові міграції `[x]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `xhigh`) · skill `db-migration` · **ТЗ:** §4.1, §7.2, §15
- **Зробити:** SQLAlchemy-моделі та Alembic-міграції для всіх таблиць §15 (включно з `equity_snapshots`, `ledger`, `instruments`, `experiments`); **`account_id`** у `equity_snapshots`, `ledger`, `risk_config`, `risk_events`, `system_state`, `emergency_runs`; `exchange_accounts.allowed_host_id`; `bots.pause_reasons` і `pending_model_version`; статус у `signals`; `data_gaps.source`; обов'язкові `UNIQUE`/`CHECK`/часткові індекси; hypertable `candles` + compression 7 днів; `NUMERIC` для грошей, `BIGINT` мс для часу; append-only для `order_events`, `audit_log`, `ledger`.
- **Готово, коли:** `upgrade → downgrade -1 → upgrade` проходить; тести: дубль `client_order_id`, `exec_id`, сигналу на свічку, другий активний бот на символ → `IntegrityError`. Рев'ю: `trading-safety-reviewer`.
- **Примітки (2026-10-28):** моделі в `app/<модуль>/models.py` (30 таблиць, перелік модулів — `app.core.db.MODEL_MODULES`), основа — `app/core/db.py` (`Base`, naming convention, `TimeMs`/`CreatedMs`/`Money`, `enum_check`, `symbol_check`, async engine/сесії); міграція `0001_initial_schema` (autogenerate + вручну: `ctb_unix_now_ms()`, hypertables `candles` і 5 таблиць деривативів, compression `candles` після 7 днів, без retention; тригери: append-only `ctb_forbid_mutation()` для `order_events`, `audit_log`, `ledger`, `experiment_trials`, `risk_config`; `ctb_orders_guard()` — незмінна ідентичність ордера, без переходів з термінальних станів, без DELETE; `ctb_bots_guard()` — незмінні акаунт/режим/символ, `STOPPED` лише без відкритої позиції й нетермінальних ордерів; `ctb_bot_children_guard()` — `FOR SHARE` на бота при вставці ордера/позиції (без гонки з паралельною зупинкою; вхід для `STOPPED` бота відхиляється, виходи й позиції — ні); `ctb_positions_guard()` — відкриту позицію не видалити; `ctb_require_confirmed_audit()` — послаблення ризику, вихід з Emergency Stop і `APPROVED` лише з підтвердженим записом `audit_log` потрібної дії для того самого акаунта/моделі, один запис — одна дія). Конфіг: `POSTGRES_HOST` (лише loopback), `POSTGRES_PORT`, `Settings.database_url(test=)`; DSN не в `alembic.ini`. Понад обов'язкові обмеження: `orders.origin` (`BOT`/`MANUAL_TEST`/`ORPHAN`/`EXCHANGE`; вхід бота — з сигналом у кожному режимі; ордери біржі без `clientOrderId`, але з fills), складені FK (акаунт, режим, символ і сторона узгоджені між ботом, ордером, fill, позицією, `risk_events`, `decision_log`), канонічний символ CCXT у всіх таблицях, одна відкрита позиція на `(account_id, symbol)`, один незавершений Emergency Stop на акаунт, `UNIQUE(env_key_set)`. Рішення з власником (`docs/decisions/0005-schema-t0-4.md`, ТЗ §15 оновлено): `price_type` у PK `mark_index_candles`, таблиця `holdout_openings` (≤ 2 відкриття), окремі джерела прогалин `mark_candles`/`index_candles` (частковий UNIQUE серед відкритих), заборона `STOPPED` з відкритою позицією. Виправлено `.gitignore`: правило `data/` ховало весь пакет `apps/backend/app/data/` (з T0.1) — тепер `/data/`, тест `test_source_files_are_not_git_ignored`. Integration-тести `tests/db/` (лише `<db>_test`; без БД — skip, `CTB_REQUIRE_DB=1` — помилка; фабрики — `tests/db/factories.py`). Нові залежності: `sqlalchemy[asyncio]` 2.0, `alembic` 1.20, `asyncpg` 0.32, dev `pytest-asyncio` 1.4. Рев'ю `trading-safety-reviewer` (Opus — Fable недоступна: немає кредитів), три раунди, фінальний вердикт — ПРОЙДЕНО: 3 блокери (`.gitignore`, fills ордерів біржі, прогалини mark/index) і всі зауваження «важливо» (гонка зупинки бота — тест на двох з'єднаннях, перевірено, що без `FOR SHARE` він падає; `qty` ордерів `EXCHANGE`; прив'язка підтвердження до акаунта/моделі й одного використання) виправлено з тестами. **Для наступних задач:** T1.5 — upsert прогалини, що росте, окремо для mark/index; T1.9 — mark/index окремими рядками з `price_type`; T2.5 — `IntegrityError` на `holdout_openings` = ліміт вичерпано, перевіряти, що новий відрізок справді новіший; T3.1 — `ORDER_STATES` з `trading/models.py`, граф переходів у БД не дублюється; T3.2 — завжди явна `qty` у виходах (`qty > 0`); T3.4 — ордери `EXCHANGE` з fill-подій, явний мапінг видів `ledger` (TRANSFER/BONUS/комісії ліквідації), невідомий вид — помилка; T3.3/T4.4 — ордер, знайдений після `REJECTED`, — лише через fills, `IntegrityError` переходу не обриває звірку; T3.4 — ордерам `EXCHANGE` ставити `bot_id`, `qty` з виконань, `equity_snapshots` — `ON CONFLICT DO NOTHING`; T1.3/T3.4 — символи Bybit → формат CCXT до вставки; T0.7/T6.2 — `audit_log` з `confirmed`, `account_id`/`target`, новий запис на кожну дію; T3.6 — набір `key_check_result` можна уточнити новою міграцією; T3.7 — `createType`/`orderLinkId` ордерів TP/SL позиції (`TODO(verify-bybit)`); T4.2 — немає рядка `system_state` або `peak_equity`/`day_start_equity` = 0 → fail-closed, а не `NORMAL`; T4.6 — новий тригер при `COMPLETED`-запуску продовжує наявний запуск; T5.1 — попередній статус після `RECONCILING` не зберігається; цикл бота під `asyncio.Lock` (дві транзакції «вставка ордера + оновлення бота» одного бота паралельно → deadlock); T3.4/T4.3/T4.6 — обробники fills, Protection Manager і Emergency Stop не оновлюють рядок `bots` в одній транзакції зі вставкою ордера/позиції та повторюють захисні операції при SQLSTATE `40P01`; T4.4 — звірка виявляє OPEN-позицію `STOPPED`-бота (пізній fill дозволений БД) → CRITICAL і пауза `UNKNOWN_POSITION`/`ERROR`; T6.1/T6.2 — `[Test Long]`/`[Test Short]` лише для не-`STOPPED` бота (БД відхиляє вхід зупиненого); окремо — роль застосунку не-власник БД (зараз застосунок працює від superuser, тож append-only тригери не захищають від `DISABLE TRIGGER`). Тести: 330 passed (з БД); ruff, mypy — чисто; `alembic check` — без дрейфу.

#### T0.6 · Single-instance advisory lock `[x]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §6.4
- **Зробити:** lock на окремому з'єднанні з TCP keepalive; повтори протягом `lock_wait_s`; при невдачі — вивести власника (`pg_locks` + `pg_stat_activity`) і завершитися; `--force-takeover` лише вручну (агент його не запускає).
- **Готово, коли:** integration-тест: другий процес не стартує; після `kill -9` першого lock звільняється в межах keepalive-таймауту.
- **Примітки (2026-10-29):** `core/instance_lock.py`: advisory lock `(0x63746200, 1)` на окремому raw-з'єднанні asyncpg поза пулом SQLAlchemy; серверні `tcp_keepalives_idle/interval/count` = 10/5/3 (≤ 25 с), `idle_session_timeout = 0`, `application_name = "ctb-backend pid=<pid ОС>"` (за Docker `client_addr` — шлюз); повтори щосекунди протягом `LOCK_WAIT_S` (новий параметр: 60 с, 0…600), зокрема поки БД недоступна; власник — у лог `instance_lock_held_by_other`; watchdog (listener закриття з'єднання + heartbeat 5 с із таймаутом 5 с); втрата lock → `held = False`, робота скасовується й дочікується (≤ 5 с, інакше `os._exit`), лише потім з'єднання закривається; ручне перехоплення — лише з TTY (stdin і stderr), слово `TAKEOVER`, таймаут 120 с, повторна перевірка власника, `audit_log` (`instance.force_takeover`) до умовного `pg_terminate_backend` (pid + `backend_start`). `app/__main__.py` — торговий процес `python -m app` (поки lock + очікування сигналу), коди виходу 0/1/2/10–14, необроблений виняток — маскований `uncaught_exception`. Рішення з власником (`docs/decisions/0006-instance-lock.md`, ТЗ §6.4 оновлено): серверні keepalive + heartbeat замість libpq-параметрів; втрата lock → вихід процесу; перехоплення лише з TTY + аудит. mypy: `ignore_missing_imports` для asyncpg (нових залежностей немає). Перевірено на Mac: після `kill -9` lock звільняється за ~6 мс. Рев'ю `trading-safety-reviewer` (Opus — Fable недоступна: немає кредитів), два раунди, фінальний вердикт — ПРОЙДЕНО: 3 зауваження «важливо» (немаскований необроблений виняток після `shutdown_logging()`; lock віддавався до зупинки роботи; хибне твердження, що `allowed_host_id` захищає від другої БД на тому самому хості) і дрібниці (одноразовий `acquire`, `held` з `is_closed()`, `ExceptionGroup`, таймаут підтвердження, застарілий власник при перехопленні, сигнал під час зупинки, збій watchdog-а) виправлено з регресійними тестами. **Залишковий ризик (рішення за власником):** lock діє в межах однієї БД — другий процес на тому самому комп'ютері з іншою `POSTGRES_DB` (напр. відновлена копія з ботами того самого акаунта) не блокується. **Для наступних задач:** T0.7 — API запускається як `body` у `run_exclusive` (замість `_serve`; обробники сигналів uvicorn вимкнені або узгоджені з `_run`, `log_config=None`); контракт `body`: скасування ≤ 5 с, без необмеженого `asyncio.shield`, власні задачі — у `TaskGroup`, відірвані обробники запитів викликають `lock.ensure_held()` перед дією, що збільшує експозицію; T5.2 — `ensure_held()` перед кожним входом; T4.3 — втрата lock (зокрема збій БД > 5 с) зупиняє і захист до рестарту й звірки: SL на боці біржі якомога раніше, час рестарту закладати в `protection_timeout_s`; T5.10 — ніколи не додавати прапорець перехоплення в plist/systemd, ненульовий код виходу → перезапуск; T3.6 — процесні тести (`tests/db/test_instance_lock_process.py`) мають обнуляти `BYBIT_*` в оточенні дочірнього процесу. Тести: 426 passed (з БД); ruff, mypy — чисто. Документація: `app/core/README.md`, `apps/backend/README.md`, `README.md`, `app/api/README.md`, `.env.example`, `AppInfo.md` §6.4, `docs/decisions/0006-instance-lock.md`.

#### T0.7 · Каркас FastAPI і безпека локального API `[x]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §14, §16
- **Зробити:** бінд `127.0.0.1`; bearer-токен; перевірка `Origin`/`Host` (DNS rebinding, CSRF); суворий CORS; middleware `Idempotency-Key` для змінюючих запитів; `audit_log`; `/system/health`.
- **Готово, коли:** тести: запит з чужим `Origin`/`Host` → 403; без токена → 401; повтор з тим самим `Idempotency-Key` не виконує дію двічі.
- **Примітки (2026-10-29):** `app/api/`: `security.py` — чисті ASGI-middleware `LocalOriginGuard` (`Host` рівно `127.0.0.1|localhost:<API_PORT>`, `Origin` точно з allowlist, `Sec-Fetch-Site: cross-site|same-site` без `Origin` → 403; WebSocket — закриття 1008 до accept; `Cache-Control: no-store`, `nosniff`) і `BearerAuth` (SHA-256 + `hmac.compare_digest`, 401 `AUTH_REQUIRED`/`AUTH_INVALID` з `WWW-Authenticate`, без токена лише CORS-preflight); порядок: ServerError → Guard → CORS (Starlette, без credentials і PNA) → Auth → Idempotency → роутер. `idempotency.py` — обов'язковий `Idempotency-Key` для POST/PUT/PATCH/DELETE (16–128 `[A-Za-z0-9_.:-]`), тіло ≤ 1 MiB, відбиток SHA-256 (метод, шлях, query, тіло), запис `IN_PROGRESS` комітиться до обробника (нова таблиця `api_idempotency_keys`, міграція `0002`), повтор → збережена відповідь + `Idempotency-Replayed`, інший запит → 422, `FAILED`/`IN_PROGRESS` іншого `owner` → 409 `IDEMPOTENCY_RESULT_UNKNOWN` (ніколи не виконується вдруге), БД недоступна → 503 без виконання. `audit.py` — `AuditWriter` у транзакції викликача з маскуванням (`SecretMasker.mask`, новий публічний метод). `system.py` — `GET /system/health` (lock + `SELECT 1` ≤ 2 с, 503 при деградації). `errors.py` — єдиний формат `{"error": {"code", "message"}}`. `server.py` — власний сокет `127.0.0.1:API_PORT` після lock (`ApiBindError`), uvicorn без `capture_signals`, `log_config=None`, без access-логу, `proxy_headers=False`, `lifespan="off"`, `ws="none"`, керована зупинка: сервер і обробники запитів дочікуються (бюджет 4,5 с), застряглі — дедлайн `run_exclusive` (5 с) → аварійний вихід; після втрати lock — без graceful-очікування. Swagger UI/ReDoc вимкнені, `/openapi.json` — з токеном; OpenAPI описує `bearerAuth` і обов'язковий заголовок. Конфіг: `API_PORT` (8000, 1024…65535), `API_TOKEN` (`SecretStr`, ≥ 32 token68), `API_ALLOWED_ORIGINS` (лише loopback, default `http://127.0.0.1:5173`); `API_BIND_HOST` не налаштовується. `__main__`: без `API_TOKEN` — код 15 до БД; порт зайнятий — код 16. strict-mypy поширено на `app.api.*`. Рішення з власником (`docs/decisions/0007-local-api.md`, ТЗ §14/§15/§16 оновлено): ключі в БД, токен для всіх ендпоінтів, процес без токена не стартує, невідомий результат не повторюється. Нові залежності: `fastapi` 0.162 (тягне `starlette` 1.10 і `opentelemetry-api` — лише API, без SDK), `uvicorn` 0.54 (без extras), dev `httpx` 0.28 (Starlette 1.10 радить `httpx2` для TestClient — лише попередження). Рев'ю `trading-safety-reviewer` (Opus — Fable недоступна), три раунди, фінальний вердикт — ПРОЙДЕНО: блокерів немає; зауваження «важливо» виправлено з регресійними тестами — 500 усередині CORS (`CatchUnhandled`), `serve_api` дочікується обробників запитів (lock не віддається, поки вони працюють), обмежений лог відмов (`RejectionLog`), `Connection: close` на 401/403; другий раунд: `limit_concurrency` прибрано (дозволяв чужій сторінці відрізати UI відповіддю 503), повтор, поки переможець ще в `begin()`, — `IN_PROGRESS`, після втрати lock обробники скасовуються на початку зупинки (самим `serve_api`, незалежно від версії Python) без graceful-очікування, підсумок придушених відмов — на першому запиті після вікна; дрібниці — `IN_PROGRESS` без виконання → `RESULT_UNKNOWN`, невідомі небезпечні методи → 405, тригер `ctb_api_idempotency_guard()` у 0002, маскування `reason`/`target`, `UUID` рядком у маскері, слабкий токен (< 10 різних символів) відхиляється, межі ключа з одного джерела, `curl` без токена в argv. **Відкриті рішення власника** (`docs/decisions/0007-local-api.md`): збій API зупиняє весь процес (T4.3/T5.1), недоступне сховище ключів блокує й захисні команди (T6.2), стандартний порт Vite як origin (T6.7). Перевірено вручну: `python -m app` на `ctb_test` — 200/401/403/400/повтор/404 `/docs`, слухає лише 127.0.0.1, SIGTERM → код 0 за 0,3 с, токена в логах немає. Тести: 676 passed (з БД); ruff, mypy, `alembic check` — чисто. Документація: `app/api/README.md`, `app/core/README.md`, `apps/backend/README.md`, `README.md`, `.env.example`, `AGENTS.md`, `.claude/rules/backend-python.md`, `AppInfo.md` §14/§15/§16, `docs/decisions/0007-local-api.md`. **Для наступних задач:** T6.1 — обробники беруть ключ із `request.state.idempotency_key`, пишуть `audit_log` через `AuditWriter` у тій самій транзакції; ендпоінти, що збільшують експозицію, — `Depends(require_instance_lock)`; помилки — `ApiError(status, ErrorCode, message)`; T6.2 — `confirmed=True` лише після підтвердження людини; T6.3 — браузерний WebSocket не задає `Authorization` (вирішити, напр., підпротоколом), `ws="none"` → увімкнути й додати `websockets`; T6.4 — схема офлайн з `create_app(ctx).openapi()` (без сервера і токена); T6.7 — як UI отримує токен (не в URL); T5.10 — `API_TOKEN` для служби лише з `.env`; після оновлення — `uv run alembic upgrade head` на dev-БД (таблиця `api_idempotency_keys`).

### Етап 1. Дані

#### T1.1 · Доменні типи, інтерфейс `ExchangeAdapter`, `FakeExchangeAdapter` `[x]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · skill `exchange-adapter` · **ТЗ:** §3, §7, §9.1
- **Зробити:** доменні pydantic-моделі (`Candle`, `Order`, `Fill`, `Position`, `Balance`, `Instrument`, …) з `Decimal` і `int` мс; `Protocol` `ExchangeAdapter` з усіма методами ТЗ §3; ієрархія доменних винятків; `FakeExchangeAdapter` з ін'єкцією збоїв (таймаут, 5xx, 429, дубль/пропуск WS, частковий fill, відмова SL, «ордер прийнято, але відповідь втрачено»).
- **Готово, коли:** contract-тести проходять на Fake; ін'єкція кожного типу збою покрита тестом. Рев'ю: `trading-safety-reviewer`.
- **Примітки (2026-10-29):** `app/exchange/`: `types.py` — доменні моделі pydantic `strict`/`frozen`/`extra=forbid` (`float`/`int`/`str` у `Decimal`, `bool`/`float` у мс, NaN/Infinity, сирі рядки замість enum — відхиляються): `Candle` (OHLC як CHECK БД, вирівнювання за таймфреймом, `close_time_ms`), `Instrument`, `Order` (факт біржі: довільний `client_order_id`, інваріанти заповнення за статусом, `qty = 0` лише в умовного), `Fill`, `Position` (лише ненульова), `Balance`, `FundingRate`; запити `PlaceOrderRequest` (лише наш формат id, явний `reduce_only`, LIMIT ⇔ price, MARKET без TIF, reduceOnly без SL/TP, орієнтація SL/TP за стороною) і `PositionProtection` (SL обов'язковий > 0 — зняти SL доменним запитом неможливо; `take_profit=None` — не змінювати); події WS; `ExchangeOrderStatus` (`TRIGGERED` і `UNKNOWN` не термінальні); `CLIENT_ORDER_ID_RE` перенесено сюди як єдине джерело (`trading/models.py` імпортує, схема без змін), `new_client_order_id`, `client_order_id_origin` (`BOT` / `BOT_PREFIX_MALFORMED` — префікс `tb`, формат не наш, трактується як чужий комп'ютер / `NOT_BOT`). `errors.py` — ієрархія (`ExchangeOutcomeUnknown` ⊃ таймаут, 5xx, `PrivateStreamLost`, `MalformedResponse`; `RateLimited`; `DuplicateClientOrderId` — не відмова; `RequestRejected` ⊃ `OrderRejected` ⊃ `InsufficientMargin`; `OrderNotActive`; `ExchangeAuthError`; `AdapterConfigError`) і `is_definite_rejection()` — єдине правило «ордер точно не створено». `adapter.py` — `Protocol` з 15 методами ТЗ §3, docstring = контракт (мережева помилка ніколи не `None`/порожньо; невідомий код біржі — `ExchangeOutcomeUnknown`; свічки закриті за часом біржі; `set_*` ідемпотентні, SL підтверджується лише з позиції; потік — async-генератор з `StreamConnected`). `fake.py` — `FakeExchangeAdapter`: one-way неттінг, фільтри інструмента, спрощена маржа, нативні SL/TP і їх спрацювання, `fail_next(method, Fault, times)` з `applied` («виконано, але відповідь втрачено») для всіх видів збою, `partial_fill_next`, WS-дубль/пропуск/обрив, зовнішні ордери (зокрема `UNKNOWN`) і позиції, зсув часу біржі, журнал `calls`; AST-тест гарантує, що `app/**` фейк не імпортує. Тести (`tests/exchange/`, спершу — субагент `test-engineer`, червоні до реалізації): типи (+ hypothesis), винятки, contract-набір лише через протокол (включно з циклом позиції; відмова для live, наявну позицію не чіпає), кожен збій фейку, property-тест неттінгу. Рішення в межах плану: пріоритет rate limiter не в сигнатурах (T1.2), маркетабельний LIMIT GTC виконується одразу (тейкер). Рев'ю `trading-safety-reviewer` (Opus — Fable недоступна: немає кредитів), два раунди: блокерів немає; «потребує змін» виправлено з регресійними тестами — allowlist кодів відмов, ідемпотентність `set_position_protection`, класифікація id з префіксом `tb`, закритість свічки за часом біржі, `UNKNOWN`-статус замість нерозбірного списку, `AdapterConfigError` у фейку, contract-набір: передумова до будь-якого ордера, закривається лише власний вхід, режим лише `fake`/`testnet`/`demo`. Нових залежностей немає. Тести: 1217 passed (з БД); ruff, mypy strict, `alembic check` — чисто. Документація: `app/exchange/README.md`, `app/trading/README.md`, `.claude/skills/exchange-adapter/SKILL.md`, примітка в T1.2; ТЗ, `.env.example`, runbook — перевірено, змін не потрібно (контракт уточнює ТЗ §3 без розбіжностей, нових змінних і кроків запуску немає). **Для наступних задач:** T1.2 — `priority` у протоколі (див. примітку T1.2); T1.3/T3.2 — підключення contract-набору до Testnet/Demo: фікстура перевіряє адреси адаптера (не mainnet), окремий тестовий символ або субакаунт без запущених ботів, перевизначити autouse `fake_clock` на `SystemClock`; закритість свічки — за часом сервера, перевірити остаточність kline після межі; T3.2 — мапінг кодів лише allowlist (невідомий/service error → `ExchangeOutcomeUnknown`), «not modified» (`set_leverage`, `set_margin_mode`, `trading-stop`) → успіх, помилка нормалізації → `MalformedResponse`, синоніми статусів (`Created` → `NEW`) явно, решта → `UNKNOWN`; T3.1/T3.3 — `place_order` лише через `is_definite_rejection`; ордер зі статусом `UNKNOWN` не є «знайдено» для розв'язання `SUBMISSION_UNKNOWN` і переходу не дає; T4.1 — розглянути обов'язковий `RiskApproval` у сигнатурі `place_order` для `reduce_only=False` (зараз інваріант 5 тримається дисципліною викликів); T4.3/T4.6 — точкові запити за символом, «SL є» лише з позиції, напрям руху SL перевіряє PPM (адаптер і фейк не перевіряють); T4.4 — `client_order_id_origin`: `BOT_PREFIX_MALFORMED` → `FOREIGN_HOST_ORDERS`, `UNKNOWN`-статус → CRITICAL; у runbook попередити, що ручний ордер з `orderLinkId` на `tb` ставить ботів акаунта на паузу; T3.7 — перевірити `TODO(verify-bybit)` з `app/exchange/README.md` (TP/SL `Full` як умовні ордери і їхня `qty`, `TRIGGERED`, вікно унікальності `orderLinkId`, reduceOnly > позиції).

#### T1.2 · Централізований rate limiter з пріоритетами `[x]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `xhigh`) · **ТЗ:** §6.5
- **Зробити:** token bucket на групу ендпоінтів з вагами; корекція за заголовками Bybit; backoff з jitter і пауза черги на 429; пріоритети `PROTECTION > RECONCILE > DATA`; **частка спільного ліміту IP для кожного процесу** (торговий 50%, воркер 30%, колектор 20% — конфігуровано, ТЗ §6.5).
- **Готово, коли:** тест: при заповненій черзі `DATA` запит `PROTECTION` виконується першим; на 429 — пауза і повтор; без голодування; процес не перевищує своєї частки.
- **Примітки (2026-10-29):** `app/exchange/rate_limit.py` — один `RateLimiter` на процес. Token bucket ТЗ §6.5 реалізовано вікном: витрачена вага зайнята до `ts + window_ms`, тож у будь-якому вікні місткість не перевищується (плавне поповнення дало б сплеск на межі вікна Bybit). IP-місткість процесу — `IP_REQUESTS * share_pct // 100`; сума часток у `Settings` рівно 100. Приватні UID-групи лише в ролі `trading`. Пріоритети `PROTECTION` > `RECONCILE` > `DATA`; антиголодування після `STARVATION_MS` не обганяє захист. Заголовки `X-Bapi-Limit*` лише стискають групу; `MARKET` спершу лише IP, UID-bucket з'являється після заголовків. 429, retCode `10006` і HTTP 403 «access too frequent» (`ip_ban`) ставлять на паузу всю чергу цього процесу; `call(..., retry=True)` повторює до `MAX_ATTEMPTS`. `place_order` і `cancel_order` — `retry=False` (`NON_RETRYABLE_METHODS`): `RateLimited` лишається `SUBMISSION_UNKNOWN`. Числа лімітів — константи модуля (тест фіксує їх; звірено з документацією Bybit v5 2026-10-29). Пріоритет у протоколі: keyword `priority` у методів, чий клас залежить від контексту; фіксований клас — `FIXED_METHOD_PRIORITY` (`fetch_time` = `RECONCILE`, ринкові дані = `DATA`, `set_position_protection` = `PROTECTION`); `subscribe_private_stream` без пріоритету. `FakeCall.priority` пише передане значення, фейк лімітер не викликає. Конфіг: `RATE_LIMIT_SHARE_TRADING_PCT` / `_WORKER_` / `_COLLECTOR_` (50/30/20, кожна 1…98, сума 100); роль процесу — не змінна `.env` (файл спільний для трьох процесів), а аргумент `RateLimiter.from_settings(settings, role=...)` від точки входу. Нових залежностей немає. Тести: 1276 passed, 3 deselected (`not testnet and not slow`); ruff, mypy — чисто. Документація: `app/exchange/README.md`, `app/exchange/errors.py`, `app/core/README.md`, `.env.example`, `docs/runbooks/getting-started.md`, `.claude/skills/exchange-adapter/SKILL.md`. ТЗ не змінювалось: дерево в `AppInfo.md` згадує `rate_limiter.py`, модуль називається `rate_limit.py`. Рев'ю `trading-safety-reviewer`: два блокери виправлено (сходинка backoff на пачку 429; `retry_after_ms` обмежено `BACKOFF_CAP_MS`) і додано резерв IP під захист. Повторна перевірка (2026-10-29), виправлено з регресійними тестами: роль процесу читалась зі спільного `.env` з default `trading` (воркер і колектор без явної ролі брали б по 50%, разом 150% IP-ліміту → бан ≥ 10 хв) — тепер її передає точка входу, `RATE_LIMIT_ROLE` прибрано; очікування reset із заголовка при нульовому залишку обмежено вікном групи замість `BACKOFF_CAP_MS` (reset — час біржі: зсув годинника на 45 с тримав `TRADING_STOP` 45 с при `protection_timeout_s` = 5 с), аномалія — лог `rate_limit_reset_beyond_window`; `penalize` на закритому лімітері нічого не робить, а `call` не повторює, тож `RateLimited` після `aclose` не підміняється на `RateLimiterError` («не відправлявся»). **Відкриті питання до ТЗ §6.5 (рішення користувача, ТЗ не змінено):** частки в сумі дають рівно 600 запитів / 5 с без запасу (Bybit не радить працювати на межі), а два комп'ютери за одним NAT мають спільний IP-ліміт; retCode `10006` (ліміт одного ендпоінта UID) ставить на паузу всю чергу, включно з SL в інших групах. **Для наступних задач:** T1.3 — `python -m app` створює лімітер з `role=ProcessRole.TRADING`, T1.8 (воркер) — `WORKER`, T1.10 (колектор) — `COLLECTOR`; у CCXT вимкнути власний обмежувач (`enableRateLimit=False`), інакше його FIFO-черга стоїть під нашою і SL чекає за даними; неявні запити CCXT (`load_markets`, звірка часу) теж мають іти через лімітер або бути вимкнені. T3.2 — вирішити мапінг retCode `429` («high server load», таблиця помилок UTA Bybit): `limit_breach_from_response` його зараз не розпізнає. T1.3/T3.2 — кожен REST через `RateLimiter.call`/`acquire` (групи в README модуля), `retry=False` для `NON_RETRYABLE_METHODS` (зараз це дисципліна викликача, не перевірка всередині `call`), перевищення — `limit_breach_from_response`, після відповіді — `observe`; `fetch_order_by_client_id` може потребувати двох груп (realtime і history). До T3.2 розвести чергу Demo і Live: зараз одна черга на процес, тож 429 одного UID зупинив би SL іншого (ТЗ §8.1). Частки 50/30/20 у сумі дають рівно IP-ліміт, без запасу на розкид мережевої затримки.

#### T1.3 · Bybit: публічні дані та `fetch_time` `[x]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · skill `exchange-adapter` · **ТЗ:** §3, §4
- **Зробити:** `fetch_candles`, `fetch_instruments`, `fetch_funding_history`, `fetch_time` через CCXT async; інкапсуляція `category=linear`; відмова старту при іншій категорії; нормалізація в доменні типи.
- **Готово, коли:** unit-тести на записаних відповідях; `testnet`-тест реальних викликів; поля перевірені за документацією (context7), неперевірене — `TODO(verify-bybit)`.
- **Примітки (2026-10-29):** `app/exchange/bybit.py` — `BybitAdapter` лише для публічних методів (повний `ExchangeAdapter` і one-way/isolated — T3.2). CCXT implicit GET (`publicGetV5Market*`), не unified float. `category=linear` до I/O, інакше `AdapterConfigError`. Усі виклики — `RateLimiter.call`, група `MARKET`, `retry=True`; `fetch_time` — `RECONCILE`, решта — `DATA`. Таймаут/5xx — до `READ_ATTEMPTS` (3); `MalformedResponse` і невідомий `retCode` (`ExchangeOutcomeUnknown`) не повторюються. `fetch_time`: конверт `time` звірений із `timeNano` (≤ 1 мс). Свічки: лише закриті за `time` відповіді, `[start_ms, end_ms)` через `end-1`, `end is None` бере `fetch_time`; вікна від початку діапазону, повна сторінка без усіх слотів ділиться навпіл. Інструменти: увесь список linear (щоб `10001` не став порожнім списком); `LinearPerpetual` і `Perpetual`; `symbol == base+quote` і `quote == settle`; ф'ючерси й `BTCPERP` пропускаються; `max_qty = min(maxOrderQty, maxMktOrderQty)`. Курсор: адаптер декодує один раз, `CcxtBybitPublicClient` кодує значення query сам (`_wire_query`), бо Bybit public `sign()` CCXT шле їх як є (`rawencode`); розкодований курсор на Testnet давав retCode `10001` (виправлено після рев'ю 2026-10-29; на Testnet 911 linear при сторінці 1000). Funding: повна сторінка (200) не вважається повною історією — вікно ділиться. Demo/live — публічний хост CCXT за замовчуванням (не `api-demo`); testnet — `set_sandbox_mode`. Поля звірені з v5 (context7) і Testnet 2026-10-29. Запас остаточності kline `KLINE_FINALITY_MARGIN_MS` = 500 мс — рішення власника (бюджет ТЗ §5.5: ціль < 1 с, `max_signal_age_ms` 5 с); Testnet 2026-10-29: свічка остаточна вже через 0,22–0,69 с після межі. **TODO(verify-bybit):** чи вистачає запасу на mainnet. Невирівняний `end_ms` більше не губить свічку (неповний слот вікна запитується). Транспорт без lock: відповідь з аргументів `handle_errors` у слот виклику (`ContextVar`), запити паралельні, порядок — лише пріоритет лімітера; транспортні збої CCXT — `ExchangeTimeout`/`ExchangeUnavailable` з логом `bybit_transport_error`, помилка в коді летить як є. Тести: `tests/exchange/test_bybit_public.py`; `tests/exchange/test_bybit_ccxt_client.py` (справжній CCXT, підмінена сесія aiohttp: URL курсора, паралельність, мапінг збоїв); `tests/exchange/test_bybit_testnet.py` (`-m testnet`, без ключів, `SystemClock`; остаточність kline — ще й `slow`, до ~85 с). Залежність `ccxt>=4.5.91,<5`. `AppInfo.md` і `.env` без змін: поведінка збігається з ТЗ §3/§13. **Для наступних задач:** T5.2 — цикл бота запитує щойно закриту свічку не раніше `close + KLINE_FINALITY_MARGIN_MS` за часом біржі, а `max_signal_age_ms` у конфігу має бути більшим за запас (тест-інваріант разом із полем); T1.5 — свічка в межах запасу ще не прогалина.

#### T1.4 · Backfill свічок і funding `[x]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §4.1, §4.4
- **Зробити:** інкрементальне докачування при старті і за розкладом для торгового символу і всіх `research_symbols` (фіксований список 5–8 символів, ТЗ §4.4, §5А.6); лише закриті свічки; `ON CONFLICT DO NOTHING`; funding history; як задача воркера для великих обсягів.
- **Готово, коли:** повторний backfill не змінює кількість рядків; незакрита свічка не потрапляє в БД (тест з фейковим clock).
- **Примітки (2026-10-29):** `app/data/backfill.py`. Символи — `TRADING_SYMBOL` плюс `RESEARCH_SYMBOLS` (5–8, за замовчуванням 8 пар із ТЗ §5А.6), таймфрейм лише `1h`, не раніше `BACKFILL_START_MS` (2020-01-01 UTC). Курсор ряду — `max(час)` у БД; вставка `ON CONFLICT DO NOTHING`. Свічка пишеться лише якщо закрита за локальним годинником (`close_time_ms ≤ now`), funding — лише з `funding_time_ms ≤ now`. Простій до 168 годин докачує `sync_market_data` / `MarketDataSync.run_if_due` (перший виклик — старт, далі кожні `BACKFILL_INTERVAL_S`, за замовчуванням 600 с). Довша дірка свічок або funding ставить одну задачу `jobs.kind = backfill_candles` (`dedup_key`, пріоритет 200); виконує її воркер T1.8 через `run_backfill_job` (статус рядка цей модуль не міняє). Прогалини не інтерполюються. Запис у `data_gaps` додає T1.5 (виклик детектора після проходу свічок). Виклик із торгового процесу — разом із циклом бота (T5): окремий entrypoint свідомо не чіпає API-старт.
- **Виправлено після рев'ю (2026-10-30):** funding качається вікнами `FUNDING_WINDOW_MS` (60 діб), кожне комітиться окремо, вставка — порціями `INSERT_CHUNK_ROWS` (1000): раніше вся історія йшла одним INSERT і від 8192 записів падала на ліміті asyncpg (32767 параметрів). Збій біржі на одному символі не зупиняє інші: `SyncResult.failed_symbols`; задача воркера докачує решту і кидає `BackfillIncomplete`; задача `backfill_candles` ставиться до мережі, в одній транзакції з курсорами. `MarketDataSync` рахує розклад за `monotonic_ms()` (стрибок системного годинника не зсуває його), невдалий прохід повторюється через backoff `SYNC_RETRY_BASE_MS` (30 с, ×2 до інтервалу, jitter), а не на кожному виклику. Тести — `tests/data/test_backfill.py`. Лишилось: лог `backfill_enqueued` при вже активній задачі показує нові символи з id старої; `TRADING_SYMBOL` у `.env` — тимчасово, до символів ботів із `bots` (T5.1).

#### T1.5 · Виявлення прогалин `[x]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §4.2
- **Зробити:** детектор прогалин → `data_gaps` + лог; API «чи є прогалина у вікні [a, b]» для навчання і live; без інтерполяції.
- **Готово, коли:** тести на синтетичних даних з пропусками на межах вікна.
- **Примітки (2026-10-30):** `app/data/gaps.py`. Інтервал і вікно — напіввідкриті `[from_ms, to_ms)` / `[start_ms, end_ms)`: дотик межею не є перетином, пропуск першого чи останнього слота — є. `find_gaps` лише порівнює мітки з сіткою (мітка не на кроці слот не закриває, нічого не інтерполюється). `detect_candle_gaps` пише дірки свічок; `sync_open_gaps` зводить відкриті рядки будь-якого `source` (T1.9 і T1.10 викличуть його). Дірка бере відкритий рядок, який перекриває (id і `detected_at_ms` зберігаються, межі оновлюються з будь-якого боку); заповнена дірка отримує `resolved_at_ms` і не заважає новій з тією самою `from_ms`; повтор без змін не пише і не логує. Нова дірка і дірка, де пропущених слотів побільшало, — `data_gap` / `data_gap_extended` (warning); скорочена, злита, закрита — info. `has_open_gap` читає лише відкриті рядки: `False` — це «рядка немає», не «ряд уже перевіряли». Backfill після проходу свічок (і для символу, який прохід не качав, включно зі збоєм біржі) викликає детектор. Курсор `max(час)` за внутрішньою діркою не повертається — рядок лишається відкритим, доки свічка не з'явиться в БД. Тести — `tests/data/test_gaps.py`. Документація: `app/data/README.md`. `AppInfo.md` без змін (поведінка §4.2).
- **Виправлено після рев'ю (2026-10-30):** (1) остання свічка вважалась діркою одразу після межі години, хоча адаптер віддає її через `KLINE_FINALITY_MARGIN_MS` за часом біржі, а годинник може відставати до 1 с — тепер сітка до `settled_end_ms(now)`, запас `CANDLE_SETTLE_MS` = `KLINE_FINALITY_MARGIN_MS` + `CLOCK_DRIFT_LIMIT_MS` (1500 мс); (2) відкат системного годинника давав порушення CHECK `resolved_after_detected` і зупиняв весь прохід синхронізації — `resolved_at_ms = max(now, detected_at_ms)`; (3) дірка, яку заповнювали з початку (так качає воркер), щоразу логувалась як нова (`data_gap`, warning) і губила час виявлення — тепер рядок зводиться за перекриттям; (4) зіткнення двох процесів на `uq_data_gaps_open` зупиняло прохід решти символів — один повтор (`GAP_SYNC_ATTEMPTS` = 2), далі ряд пропускається з логом `data_gap_sync_conflict`, розпізнає `is_open_gap_conflict` (лише цей індекс). Тести на кожен пункт, два старі тести backfill переведено на годинник після запасу (твердження ті самі). Передано: T1.6 (поріг 1 с), T2.1 (виключення вікон), T5.2 (перевірка вікна фіч live і докачування інтервалу дірки). Період до лістингу як відкрита прогалина закрито в T1.7. Повний перескан історії з 2020 року — ~0,2 с на символ (52,6 тис. свічок, тестова БД), лишено як є.

#### T1.6 · Синхронізація та стрибки часу `[x]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §4.3
- **Зробити:** порівняння з `fetch_time` при старті і кожні 10 хв; розбіжність > 1 с → блок нових входів + попередження (поріг — `CLOCK_DRIFT_LIMIT_MS`, одне джерело; на ньому ж стоїть запас детектора прогалин `CANDLE_SETTLE_MS`); щосекундне порівняння монотонного і системного годинника → `RECONCILING` при стрибку; `recvWindow`.
- **Готово, коли:** тести з фейковим clock на обидва сценарії.
- **Примітки (2026-10-30):** `app/data/time_sync.py` (`ClockGuard`; ТЗ §3 відносить час до `app/data`), у строгому mypy. Вимір — `fetch_time` між двома читаннями годинника: локальний час — середина запиту, похибка ⌈RTT/2⌉ плюс 100 ppm від віку виміру; з 3 вимірів — найменший RTT, RTT > 1000 мс відкидається, уся перевірка ≤ 30 с. Блок (`CLOCK_DRIFT`), коли `|зсув| + похибка ≥ 1000 мс`: fail-closed (не вдалося підтвердити < 1 с); рівно 1000 блокує, бо ТЗ §4.3 дозволяє повернення лише при розбіжності «менша за 1 с» (з «більше 1 с — блок» це не суперечить). Попередження — лог `clock_drift_exceeded` (Telegram — T5.6). Між вимірами зсув оцінюється безперервно (зсув системного годинника відносно монотонного), тож кроки NTP < 1 с, що в сумі дають ≥ 1 с, блокують без нового запиту. Збій перевірки лишає попередній вимір до 20 хв, далі й до першого виміру — `CLOCK_UNVERIFIED`; поки годинник не підтверджено — повтор кожні 30 с. Стрибок: |Δ системного − Δ монотонного| між тіками > 1000 мс → `ClockJump` і нова епоха; вимір старої епохи не чинний, а `CLOCK_JUMP_UNRECONCILED` знімається окремо для кожної області звірки (акаунта, ТЗ §8.1) квитком `begin_reconcile(scope)` → `confirm_reconciled(ticket)`, узятим до старту успішної звірки. Так правило ТЗ «у `RUNNING` лише коли розбіжність < 1 с і звірка пройшла» тримає сам годинник, навіть якщо обробник стрибка впав. `request_recheck(reason=)` скасовує чинний вимір до свіжого. Годинник не коригується (Bybit радить локальний час + NTP). `CLOCK_DRIFT_LIMIT_MS` перенесено з `app/data/gaps.py` в `app/core/clock.py`: на нього посилаються `gaps.py`, `CLOCK_TOLERANCE_MS` у `trading/models.py` (міграція 0001 — літерал 1000, зміна ліміту потребує міграції) і нижня межа `RECV_WINDOW_MS`, а `config.py` не може імпортувати `app/data` (цикл через логування). `RECV_WINDOW_MS` — 5000 мс (default Bybit), межі 2000 (= 2 × ліміт: допустиме відставання + передача) … 10000; правило біржі `server_time − recv_window ≤ timestamp < server_time + 1000` (документація v5, context7); до T3.2 ні на що не впливає. Нових залежностей немає. Тести: `tests/data/test_time_sync.py` (70: обидва сценарії, межі 999/1000/1001, похибка RTT і старіння, збої, дедлайн і скасування перевірки, стрибок і `request_recheck` посеред виміру, паралельні перевірки, області звірки й квитки, пауза VM, розклад, цикл `run`, hypothesis на вердикт), `tests/test_config.py`. Швидкий набір — 1488 passed; ruff, mypy — чисто. Документація: `app/data/README.md` (розділ «Час»), `app/core/README.md`, `app/trading/README.md`, `app/exchange/README.md`, `.env.example`, `docs/runbooks/getting-started.md` (NTP, WSL2). ТЗ не змінювалось.
- **Виправлено після рев'ю (2026-10-30, `trading-safety-reviewer`):** (1) одне підтвердження звірки знімало блок стрибка для всіх акаунтів — тепер області звірки й квитки; (2) `request_recheck` не скасовував чинний вимір (після `10002` і невдалої перевірки входи лишались дозволеними до 10–20 хв) — тепер вимір не чинний до свіжого, повтор кожні 30 с; (3) рівно 1000 мс тепер блокує (див. вище); (4) похибка старіє на 100 ppm від віку виміру; (5) дедлайн перевірки 30 с (черга лімітера могла тримати її без кінця); (6) частковий збій видно в `clock_drift_ok.partial_error_type`; (7) пауза VM (WSL2 у сні Windows: обидва локальні годинники стоять) `tick()` не ловить — задокументовано, тест фіксує обмеження, ловлять планова перевірка й `request_recheck`. Тести на кожен пункт.
- **Відкриті питання до ТЗ (рішення користувача, ТЗ не змінено):** (а) ТЗ §4.3 каже, що «найгірший наслідок — пропущена свічка», але годинник, що випереджає біржу приблизно на 1 с і більше, дає retCode `10002` на **всі** приватні запити, включно з SL, примусовим закриттям і Emergency Stop (правило `timestamp < server_time + 1000` від `recvWindow` не залежить). Варіант для T3.2: на `10002` для `reduceOnly`/SL/закриття повторювати з часом, виправленим на виміряний зсув; (б) ТЗ §7.2 (до 3 перевірок `SUBMISSION_UNKNOWN` з інтервалом 2–3 с) коротше за вікно, в якому Bybit ще може прийняти запит (`recv_window` + ~1 с після відправки): «не знайдено → REJECTED» може прийти раніше за пізній вхід. Варіант для T3.3: остання перевірка — не раніше `submit + recv_window_ms + CLOCK_DRIFT_LIMIT_MS`. TODO(verify-bybit): де Bybit перевіряє `recvWindow`.
- **Передано:** T5.1 — `ClockGuard.run(on_jump=...)` у торговому процесі (один на процес; `fetch_time` публічний, режим адаптера не важливий), `on_jump` → усі боти в `RECONCILING`, вихід — після звірки акаунта й `confirm_reconciled`, завершення `run` → блок нових входів; T4.4/T4.5 — `check_drift()` і квиток у стартовій (ТЗ §11.1 п. 6) і періодичній звірці; T4.1/T5.2 — `state(scope).entries_allowed` перед входом, причина в `decision_log`; T3.2 — CCXT `options.recvWindow` = `Settings.recv_window_ms`, підпис локальним часом, retCode `10002` → `request_recheck`, питання (а); T3.3 — питання (б); T3.5 — реконект WS → `request_recheck`; T5.6 — Telegram для `clock_drift_exceeded`, `clock_jump_detected`, `clock_recheck_requested`; T5.10 — поведінка годинника WSL2 після сну Windows.

#### T1.7 · Кеш `instruments` і округлення `[x]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §8.3, §15
- **Зробити:** таблиця/кеш `instruments`, оновлення при старті, раз на добу і після відхилення через фільтри; функції округлення: кількість — вниз до `qtyStep`, ціни — до `tickSize` (напрям для SL/TP — у бік меншого ризику); статус «не торгується» → `PAUSED`; `launchTime` інструмента → початок скану прогалин свічок `max(BACKFILL_START_MS, launchTime)` (зараз період до лістингу — відкрита прогалина, T1.5).
- **Готово, коли:** property-тести (hypothesis): результат кратний кроку, не більший за вхід, ніколи не < `minQty` без повернення «0 / не входити».
- **Примітки (2026-10-30):** `app/exchange/rounding.py`, `app/exchange/instruments.py`, `app/trading/instrument_status.py`, міграція `0003_instrument_launch_time.py` (`launch_time_ms` ≥ 0; наявні рядки кешу стерто — час лістингу не вигадується). Кількість завжди вниз до `qtyStep`, потім до `maxQty`; `0` з причиною `ZERO_AFTER_ROUND` / `BELOW_MIN_QTY` / `BELOW_MIN_NOTIONAL` — не входити, лот не піднімається. Ціна: `LIMIT_BUY`, `STOP_SHORT`, `TAKE_PROFIT_LONG` — вниз; `LIMIT_SELL`, `STOP_LONG`, `TAKE_PROFIT_SHORT` — вгору (стоп ближче до входу, ціль не далі); ціна нижче одного тіка — `ValueError`. Кеш: знімок поза транзакцією; порожній повний знімок — `EmptyInstrumentSnapshot`, рядки не стираються; символ, що зник, лишається зі статусом `Absent`. Торгується лише `Trading` (v5, context7 2026-10-30: `PreLaunch`/`PendingOpen` ордер не приймають). `RUNNING` + неторговий статус → `PAUSED` з `INSTRUMENT_NOT_TRADING`; інші статуси не змінюються; повернення в `Trading` знімає лише цю причину і `PAUSED` без причин повертає в `RUNNING`. OCC за `bots.version`. Оновлення після кодів `110003`, `110017`, `110094`, `30208`, `30209`, `30228`; `10001` — ні. `entry_block`: немає рядка — `INSTRUMENT_UNKNOWN`; не `Trading` — `INSTRUMENT_NOT_TRADING`; вік ≥ 24 год або мітка з майбутнього — `INSTRUMENT_RULES_STALE` (вхід блокує, паузу не ставить). Сітка свічок — `max(BACKFILL_START_MS, перша година ≥ launchTime)`; серед режимів береться найраніший лістинг; немає рядка — лишається `BACKFILL_START_MS`. Funding не зсувається. Нової змінної оточення немає. Тести: `tests/exchange/test_rounding.py` (hypothesis), `tests/exchange/test_instrument_rules.py`, `tests/trading/test_instrument_status.py`, `tests/data/test_instrument_cache.py`, `tests/data/test_gaps.py`, `tests/db/test_guards.py`, `tests/db/test_migrations.py`. Швидкий набір — 1534 passed (після перевірки); ruff, mypy — чисто. `AppInfo.md` без змін: поведінка §8.3/§6.1/§15 збігається; колонка `launch_time_ms` у зведенні §15 не перелічена (як і вже наявний `max_qty`) — підтвердження на правку ТЗ не бралось.
- **Передано:** T5 — `InstrumentCache.refresh` при старті і `refresh_if_due` у циклі (до backfill, інакше долістинговий період лишається діркою, доки перший прохід після появи рядка кешу її не закриє); T3.2 — `refresh_after_rejection` після відхилення ордера; T4.1/T5.2 — `entry_block` перед входом; T5.1 — не виходити в `RUNNING`, поки стоїть `INSTRUMENT_NOT_TRADING`, і викликати стартовий `refresh` лише після переведення ботів у `STARTING`: `apply_instrument_pauses` сам повертає `PAUSED`, у якого лишилась тільки ця причина, у `RUNNING`, і до стартової звірки це обійшло б ТЗ §11.1; T5.6 — Telegram для `instrument_not_trading` (зараз лише лог); T2.2 — `round_entry_qty` / `round_price` у розмірі позиції, плече в `qty_lev` = менше з конфігу та `instruments.max_leverage` (ТЗ §15; кеш його зберігає, функції ще немає); T3.2/T4.4 — те саме плече в `set_leverage` і в звірці плеча.
- **Виправлено після рев'ю (2026-10-30):** (1) паралельні знімки одного режиму серіалізовані сесійним `pg_advisory_lock` на час читання біржі і запису — старий `Trading` не знімає паузу після новішого `Closed`; лог паузи — після коміту; (2) уже відкрита долістингова прогалина закривається наступним сканом: авторитетне вікно починається з `BACKFILL_START_MS`, пропущені свічки шукаються від лістингу.
- **Виправлено після перевірки (2026-10-30):** (1) округлення до кроку ділило `value / step` у контексті Decimal (28 знаків): `9.999…9` (28 цифр) з тіком `0.5` давало `10` — більше за вхід, для `floor_to_step`, `round_price(LIMIT_BUY)` і `round_entry_qty`; тепер `divmod` у контексті з пасткою `Inexact` — результат точний або `ValueError`. Тести: приклад на межі точності й hypothesis біля кратного кроку (старі генерували ≤ 4 знаків і ваду не ловили); (2) тести OCC паузи: конфлікт `bots.version` повторюється на свіжому рядку (чужа причина лишається), другий — `InstrumentPauseConflict` і відкат усього знімка; (3) передачі T5.1 (порядок стартового `refresh`) і T2.2/T3.2/T4.4 (плече з `instruments.max_leverage`).

#### T1.8 · Воркер задач (`jobs`) `[x]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `high`) · **ТЗ:** §5.6, §6.4
- **Зробити:** `python -m app.jobs.worker`; `SELECT … FOR UPDATE SKIP LOCKED`; обробник `backfill_candles` → `app.data.backfill.run_backfill_job` (адаптер з роллю воркера в rate limiter; `BackfillIncomplete` → `FAILED`); прогрес і `heartbeat_at`; зависла `RUNNING` → `FAILED` при старті; знижений пріоритет ОС; GPU-задачі по одній; воркер не має торгових ключів.
- **Готово, коли:** integration-тести: два воркери не беруть одну задачу; відновлення після `kill -9`.
- **Примітки (2026-10-30):** `app/jobs/worker.py`. Черга — `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED)`. Прострочення (`STALE_AFTER_MS` 30 с, heartbeat кожні 5 с) знімається на кожній ітерації, тож воркер звільняє `dedup_key` без рестарту. `SIGTERM` повертає поточну задачу в `QUEUED`; `kill -9` лишає `RUNNING`, доки heartbeat не зістаріє (`FAILED` / `STALE_HEARTBEAT`). Один воркер на БД (див. «Рішення» нижче), задачі по одній — GPU-задачі не паралеляться. Адаптер backfill — публічний mainnet (`ExchangeMode.DEMO`), роль лімітера `WORKER`, ключі Bybit не читаються. `FAILED` воркер сам не повторює: backfill ставить новий рядок наступним `enqueue_backfill_job`, коли ключ вільний. Нових змінних `.env` і міграції немає. `AppInfo.md` §6.4 уточнено (див. «Рішення» нижче). Тести — `tests/jobs/`. Лишилось: види на кшталт Optuna (T2.10); ручний перезапуск не-backfill `FAILED` — окремий API.
- **Виправлено після перевірки (2026-10-30):** SIGTERM/SIGINT, коли БД недоступна вже після першого проходу, не зупиняв процес: `_wait` при встановленому `stop` повертався одразу, і цикл без паузи повторював звернення до БД (≈12 тис. спроб і рядків `worker_db_unavailable` за секунду, вихід лише через `kill -9`). Тепер збій БД при запитаній зупинці — вихід з кодом 0 без повтору; невернута в `QUEUED` задача стає `FAILED` за простроченим heartbeat. Регресійні тести в `tests/jobs/test_worker.py` (збій на першому проході, під час роботи і під час зупинки).
- **Рішення (2026-10-30, `docs/decisions/0008-single-worker.md`):** кілька воркерів разом брали б по 30% IP-ліміту кожен (ТЗ §6.5: 300 + 180 + 180 + 120 > 600 запитів / 5 с → бан IP, а з ним і SL торгового процесу). Тепер воркер бере single-instance lock `(LOCK_NAMESPACE, WORKER_LOCK_ID)` = `(0x63746200, 2)` механізмом `app/core/instance_lock.py` (`application_name` `ctb-worker pid=N`, новий параметр `application_name_prefix`) і працює під `run_exclusive`: другий воркер — код 10, втрата lock — код 12 (задача лишається `RUNNING` до прострочення), SIGTERM під час очікування lock — код 0. Очікування — `LOCK_WAIT_S`. GPU-слот `0x63746202` прибрано разом з тестами на нього. Критерій «два воркери не беруть одну задачу» покривають тест `SKIP LOCKED` на двох сесіях і процесний тест «другий воркер не стартує». Документація: `app/jobs/README.md`, `app/core/README.md`, `apps/backend/README.md`, `.env.example`, `docs/runbooks/getting-started.md`. **ТЗ:** §6.4 уточнено з підтвердженням власника (2026-10-30): торговий процес — «єдиний працює з ордерами», воркер — один на БД із власним single-instance lock (було/стало — у рішенні 0008).

#### T1.9 · Backfill даних деривативів `[x]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · skill `exchange-adapter`, `db-migration` · **ТЗ:** §4.4, §15
- **Зробити:** спершу перевірити й задокументувати (`docs/data/derivatives-history.md`) глибину історії кожного ендпоінта Bybit v5; таблиці `open_interest`, `premium_index`, `mark_index_candles`, `long_short_ratio`; інкрементальний ідемпотентний backfill для `research_symbols` через rate limiter (пріоритет `DATA`); прогалини → `data_gaps`.
- **Готово, коли:** повторний backfill не змінює кількість рядків; дані у UTC мс; поля перевірені за документацією (context7), неперевірене — `TODO(verify-bybit)`.
- **Примітки (2026-10-30):** таблиці й hypertables з T0.4; CHECK-и `premium_index` замінює міграція 0004 (див. нижче). Читання лише в `BybitAdapter` (не в протоколі `ExchangeAdapter`): `fetch_open_interest`, `fetch_premium_index`, `fetch_mark_price_candles`, `fetch_index_price_candles`, `fetch_long_short_ratio`, пріоритет `DATA`. Інтервал backfill — `1h` (`intervalTime`/`period` = `1h`, kline = `60`). `ts_ms` — початок інтервалу, UTC мс; незакриту годину відкидає той самий запас, що й trade-kline. Зберігається `openInterest`, не `singleOpenInterest`. Неузгоджений OHLC не виправляється: точка пропускається (`derivative_point_rejected`), дірка лишається (для premium — лише зламаний діапазон години, див. нижче). `app/data/derivatives.py`: курсор `max(ts_ms)`, `ON CONFLICT DO NOTHING`, порожнє вікно не зупиняє прохід, сітка від `max(BACKFILL_START_MS, лістинг)`. Хвіст до 168 год — `sync_derivatives` / `DerivativesSync` (лише `research_symbols`; розклад крутить колектор — T1.10, 2026-10-30); довше — одна задача `backfill_derivatives` (пріоритет 210, свій `dedup_key`). Воркер пише `BACKFILL_INCOMPLETE`, якщо біржа впала на частині символів. Підлоги «раніше за дату X не питати» в коді немає. Найраніші точки на mainnet перевірено 2026-10-30 після зняття заборони на read-only публічні дані mainnet: усі п'ять рядів для восьми `research_symbols` (таблиця в `docs/data/derivatives-history.md`, розділ «Що видно на mainnet»). `launchTime` — не початок даних (BTC +10 діб, ADA +2 міс., LTC `launchTime` 2018-01-01, дані з 2020-10-19): ці години лишаються відкритими `data_gaps`; OI і long/short BTCUSDT суцільні лише з 2020-08-04 08:00. Тести: `tests/exchange/test_bybit_derivatives.py`, `tests/data/test_derivatives_backfill.py`, `tests/jobs/test_worker_db.py`. Документація: `docs/data/derivatives-history.md`, `app/data/README.md`, `app/exchange/README.md`, `app/jobs/README.md`. **ТЗ (2026-10-30, з підтвердженням власника, рішення `docs/decisions/0010-derivatives-history-verified.md`):** у §19 глибину історії OI, premium, mark/index і long/short перенесено в блок «Уже перевірено» з посиланням на `docs/data/derivatives-history.md`; формат WS-потоку ліквідацій лишився неперевіреним (T1.10).
- **Виправлено після перевірки (2026-10-30, рішення `docs/decisions/0009-premium-open.md`):** premium на Testnet втрачав 67–77% годин (червень 2024) і 12–19% (останні 48 год). Причина: Bybit віддає як `open` close попередньої години, а `high`/`low`/`close` — лише за поточну, тож `open` часто поза `[low, high]`. Це не збій даних. Міграція `0004_premium_open_unbounded.py` замінює CHECK-и `ck_premium_index_high_ge_open_close` / `_low_le_open_close` на `ck_premium_index_high_ge_close` / `_low_le_close` (`high_ge_low` лишається). `PremiumIndex` перевіряє те саме (`_finite_hour_range`). Відкат видаляє рядки з `open` поза діапазоном. Після зміни Testnet дає повний ряд: 500/500 і 48/48 годин. Mainnet (публічний GET, BTCUSDT 2024-06-01) підтверджує форму: `open` = попередній `close` у 23/23, поза діапазоном у 20/24. Ширше на mainnet (8 символів × 2 вікна по 1000 год): `open` = попередній `close` 999/999, поза діапазоном 61–90% годин, зламаного діапазону — 0. Mark/index і свічки не змінювались. Тести: `tests/exchange/test_types.py`, `tests/exchange/test_bybit_derivatives.py`, `tests/db/test_constraints.py`, `tests/db/test_migrations.py`, `tests/data/test_derivatives_backfill.py`. Документація: рішення 0009, `docs/data/derivatives-history.md` (виправлено й першу годину OI/long-short на Testnet: 2020-07-09 08:00, далі дірка до 2020-07-10 09:00), `app/data/README.md`, `app/exchange/README.md`.

#### T1.10 · Колектор ліквідацій `[~]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `high`) · **ТЗ:** §4.4, §12
- **Зробити:** окремий процес `python -m app.data.collector` без торгових ключів (власна частка rate limit, ТЗ §6.5); підписка на публічний WS-потік ліквідацій для `research_symbols`; агрегація по годинах (кількість і обсяг окремо long/short) у `liquidations_hourly`; реконект з backoff; періоди простою → `data_gaps`. **Запустити якомога раніше** — історію ліквідацій не завантажити заднім числом.
- **Готово, коли:** колектор працює добу без втрат (перевірка за `data_gaps`); тест агрегації на записаних подіях; тест реконекту.
- **Примітки (2026-10-30):** `app/exchange/liquidations.py` (кадр, сесія, aiohttp) і `app/data/liquidations.py` + `app/data/collector.py` (агрегат, прогалини, процес). Тема `allLiquidation.{symbol}`, публічний mainnet, без ключів. `Buy` — long, `Sell` — short. Година пишеться лише цілком прослухана (`SEAL_MARGIN_MS` 30 с); тиха — нулями; зіпсований кадр годину не закриває. `ON CONFLICT DO NOTHING`. Реконект від 1 с, ×2, стеля 60 с, jitter, скидання після ack підписки. Один колектор на БД: `COLLECTOR_LOCK_ID` = 3, `ctb-collector pid=N`. Лімітер ролі `COLLECTOR` тримає частку 20%; REST колектора — лише `fetch_time` для `ClockGuard` (див. 2-й прохід нижче). Деривативи (хвіст за розкладом `DerivativesSync`) — теж у колекторі, див. нижче. Тести: `tests/exchange/test_liquidation_messages.py` (кадр-приклад ROSEUSDT з документації v5, не запис живого потоку), `tests/data/test_liquidations.py` (агрегація, реконект, дірка і долістинговий період). Документація: `docs/data/liquidations.md`, README модулів `data` / `exchange` / `core` / `jobs`, `apps/backend/README.md`, `docs/runbooks/getting-started.md`. **Ще не `[x]`:** добу без втрат за `data_gaps` не перевірено — процес треба тримати запущеним; SQL у `docs/data/liquidations.md`. Історичний префікс до першої повної години — очікувана відкрита прогалина. **ТЗ §19 (2026-10-30, з підтвердження власника, рішення `docs/decisions/0012-liquidation-stream-verified.md`):** формат WS ліквідацій перенесено в «Уже перевірено». Живі події з mainnet (AVAXUSDT ×2, 1000PEPEUSDT, 12:29 UTC) збігаються з документацією і записані у тести: `test_recorded_mainnet_events_parse_exactly`, `test_recorded_mainnet_events_aggregate_per_symbol` — критерій «тест агрегації на записаних подіях» тепер на справжніх кадрах. Стара тема `liquidation.*` на mainnet уже не існує. За ~20 хв проб вісім `research_symbols` не дали жодної ліквідації (угоди BTCUSDT при цьому йшли) — тихі години з нулями очікувані; залишковий ризик «біржа мовчки не шле події після ack» описано в рішенні 0012.
- **Виправлено після перевірки (2026-10-30):** SIGTERM/SIGINT не зупиняли колектор у тихому потоці: сигнал лише ставить `stop`, читання чекало `receive_text()` без кінця, а `_watch` після `stop` більше не шле ping — тож процес чекав першої ліквідації (під launchd — SIGKILL, вручну — лише `kill`). Тепер `read_liquidation_session` чекає кадру або `stop` (`_next_frame`; кадр, що прийшов разом зі `stop`, ще обробляється), а `run_collector` так само перериває з'єднання, що висить (`_connect_unless_stopped`); сокет, відкритий разом зі `stop`, закривається без підписки. Регресійні тести в `tests/data/test_liquidations.py`: тиха сесія з ack і без ack, завислий connect, connect разом зі `stop`. Код виходу 14 у `apps/backend/README.md` описано як у воркера (після зовнішнього скасування; завислий цикл після втрати lock — код 12). Документація: `apps/backend/README.md`, `app/data/README.md`, `app/exchange/README.md`.
- **Виправлено після перевірки, 2-й прохід (2026-10-30):** (а) Підписка: mainnet (перевірено наживо, публічний WS без ключів) відхиляє спільний запит цілком, якщо хоч одна тема невідома (`success: false`, `ret_msg: "error:handler not found,topic:…"`, без `failTopics`) — один поганий символ у `RESEARCH_SYMBOLS` зупиняв збір усіх. Тепер кожен символ — окремий запит із `req_id` = темою (`req_id` повертається у відповіді — документація v5 і mainnet); відхилений символ — `liquidation_topic_rejected` (error) і прогалини лише для нього, відхилено всі — реконект; таймаут підписки — `liquidation_subscribe_timeout` з переліком символів без відповіді. `LiquidationBook.subscribed(symbol, at_ms)` — по символу. (б) Запис у БД винесено з циклу читання в окрему задачу `_Writer`: після помилки БД пауза 1 с ×2 до 60 с (`db_retry_delay_ms`, лог із `failures` і `retry_in_ms` раз на паузу, а не на кожен кадр; `liquidation_db_recovered`); зависла БД більше не робить здорове з'єднання stale; останній запис при зупинці ≤ `FINAL_FLUSH_TIMEOUT_MS` 3 с (< `BODY_STOP_TIMEOUT_MS`). `run_collector` тепер приймає `book` (символи — з нього). (в) Годинник (ТЗ §4.3): колектор тримає `ClockGuard` (`fetch_time` через `BybitAdapter` з лімітером ролі `COLLECTOR` — частка §6.5 тепер справді використовується), `watch_clock` звіряє книгу після кожного тіку, `collect_with_clock` зупиняє колектор, якщо контроль часу впав. Години закриваються лише коли годиннику вірять (немає `CLOCK_UNVERIFIED`/`CLOCK_DRIFT`; стрибок — за `jump_epoch`), з запасом `CLOCK_MARGIN_MS` 1 с з обох боків години; втрата довіри відкидає відкриті години. Поведінка змінилась: підписка рівно на межі години більше не робить її повною — потрібна ≥ 1 с до межі. (г) Handshake WS: `HANDSHAKE_TIMEOUT_S` 15 с на все відкриття з'єднання (aiohttp без `sock_read` чекав відповіді на upgrade вічно; перевірено в коді aiohttp 3.14). Також виміряно: без ping mainnet закриває з'єднання за ~60 с (код 1006), а не за ~10 хв, як було в документації. Тести: `tests/data/test_liquidations.py` (51 разом з повідомленнями: запас 1 с, довіра й стрибок, `watch_clock` зі справжнім `ClockGuard`, відхилений символ, усі відхилені, таймаут підписки, stale після двох ping, БД недоступна і зависла, `collect_with_clock`), `tests/exchange/test_liquidation_messages.py` (записані з mainnet відмова і pong, `req_id`, таймаут handshake). Живий прогін без БД на mainnet: `clock_drift_ok` (зсув 5 мс), відхилено лише вигаданий символ, BTC/ETH/SOL працювали, зупинка за 3 мс. Документація: `docs/data/liquidations.md`, `app/data/README.md`, `app/exchange/README.md`, `apps/backend/README.md`, `docs/runbooks/getting-started.md`, docstring `ClockGuard`. **Запущений колектор працює зі старим кодом — його треба перезапустити, і добова перевірка починається заново.**
- **Деривативи в колекторі (2026-10-30, рішення власника — як у ТЗ §4.4, §6.4):** `run_derivatives` крутить `DerivativesSync.run_if_due` поруч із потоком ліквідацій, з того самого `BybitAdapter` (лімітер `COLLECTOR`); опитування розкладу — `DERIVATIVES_POLL_MS` 5 с, інтервал — `BACKFILL_INTERVAL_S`, пауза після збою — сам `DerivativesSync`. Хвіст ≤ 168 год качає колектор, довшу історію — воркер (задачу `backfill_derivatives` ставить той самий прохід). Прохід лише коли годиннику вірять (`derivatives_waiting_for_clock`): незакриту біржову годину `ON CONFLICT DO NOTHING` уже не виправив би. Збій проходу — `derivatives_sync_failed` (несподіваний виняток — з трасуванням), колектор ліквідацій працює далі; `stop` перериває прохід (`derivatives_sync_interrupted`). Раніше `DerivativesSync` ніхто не викликав (планувався торговий процес, T5). Funding лишається в `MarketDataSync` разом зі свічками (торговий процес, T5) — рішення власника, `docs/decisions/0011-funding-with-candles.md`, ТЗ §4.1/§4.4/§6.4 уточнено з його підтвердження. Тести: `tests/data/test_collector_derivatives.py` (годинник, збій БД і несподіваний, зупинка посеред проходу, лог підсумку, запис хвоста в тестову БД справжнім `DerivativesSync`). Живий прогін усього процесу не робився: запущений колектор тримає lock, а прохід писав би в робочу БД. Документація: `app/data/README.md`, `apps/backend/README.md`, `docs/runbooks/getting-started.md`. Код тепер відповідає ТЗ §4.4/§6.4 щодо OI, premium, mark/index і long/short.
- **Відкрито (2026-10-30):** (1) `serve()` і `main()` колектора (коди виходу, lock, сигнал, склад задач) тестами не покриті.

### Етап 2. ML та бектест

Методологія — ТЗ §5, §5А (v3.1) і `docs/decisions/0001-ml-methodology.md`. Порядок: спершу спільна інфраструктура (фічі, витрати, симулятор, бектест, валідація, статистика), потім первинна стратегія і модель.

#### T2.1 · `compute_features()` + кільцевий буфер `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `xhigh`) · skill `ml-pipeline` · **ТЗ:** §5.1, §5.5, §5А.4
- **Зробити:** стартовий набір ~25 відносних фіч (≤ 30): 1h; **4h і 1d, побудовані з 1h, лише з повністю закритих старших свічок**; дані деривативів за правилом «відомо станом на `t-1`» (фіча вмикається, лише коли історія покриває навчальний діапазон); робота для кількох символів; `feature_schema_hash`; інкрементальний розрахунок останнього рядка з буфера (довжина ≥ 1300 свічок 1h для EMA(50) на 1d); виключення вікон з прогалинами (`window_has_gap` / `open_gaps_in_window` з `app/data/gaps.py`, межі `[start, end)`, лише відкриті рядки).
- **Готово, коли:** зелені тести look-ahead (окремо для 1h, старших ТF і деривативів) і parity (буфер == офлайн). Рев'ю: `ml-leakage-auditor`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T1.9) історія деривативів починається не з `launchTime`: години до першої точки — відкриті `data_gaps` (глибина рядів — `docs/data/derivatives-history.md`); `open` свічки premium index — close попередньої години (`docs/decisions/0009-premium-open.md`).
  - (T1.10) ліквідації є лише від першої повністю прослуханої години колектора; префікс до неї — постійна відкрита прогалина `source = liquidations` (`docs/data/liquidations.md`).

#### T2.2 · Модель витрат, `calc_position_size()`, ліміти `risk/limits.py` `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · skill `risk-engine-change` · **ТЗ:** §5.2.2, §5.2.3, §8.3
- **Зробити:** `risk/costs.py` — комісії (taker на виходах), `slippage_entry`, `slippage_sl` (за замовчуванням 2× входу), `slippage_tp` (фікс + частка ATR), funding; `risk/sizing.py` — єдина функція розміру (запас `sl_dist` з `slippage_sl`); `risk/limits.py` — **чисті функції** лімітів без БД і мережі (одна позиція, cooldown, денний збиток, `max_trades_per_day`, просадка, time-stop), які потім використають і бектест, і `RiskGate` (T4.1, T4.2).
- **Готово, коли:** property-тести (ризик за SL ≤ `risk_usd`, LONG/SHORT, нуль/мінімальний лот); тести кожного ліміту на межі; parity-тест. Рев'ю: `trading-safety-reviewer`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T1.7) розмір позиції — через `round_entry_qty` / `round_price` (`app/exchange/rounding.py`); плече в `qty_lev` = менше з конфігу та `instruments.max_leverage` (ТЗ §15; кеш його зберігає, функції ще немає).

#### T2.3 · Симулятор угоди, розмітка і ваги унікальності `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `xhigh`) · skill `ml-pipeline` · **ТЗ:** §5.1, §5.2.1, §5.2.4, §5А.5
- **Зробити:** `ml/trade_sim.py` — єдиний симулятор: опорна ціна `open[t]`, ATR(`t-1`), бар'єри з самої свічки `t`, SL першим при неоднозначності, **SL за mark-свічками** (`mark_index_candles`, fallback — звичайні свічки з позначкою в журналі), правило гепу через SL, time-stop за `open[t+horizon_bars]`, витрати з T2.2; meta-мітки для кандидатів первинної стратегії; двостороння розмітка LONG/SHORT/FLAT (дві симуляції) для ітерації 2; ваги унікальності (всередині символу) × ваги класів.
- **Готово, коли:** тести на синтетичних свічках: кожне правило симулятора, взаємовиключність LONG/SHORT, ваги на відомих перекриттях. Рев'ю: `ml-leakage-auditor`.

#### T2.4 · Бектестер (виконання і ліміти = live) `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `xhigh`) · **ТЗ:** §5.2.3, §6.6
- **Зробити:** бектест через симулятор T2.3, sizing і ліміти T2.2; коди причин відмов як у `decision_log`; звіт угод (у R) і метрик; стрес-режим «подвоєні витрати».
- **Готово, коли:** повторюваний результат з тим самим seed; 3–5 угод на синтетиці, перераховані вручну, збігаються; тест, що бектест не відкриває другу позицію і поважає cooldown/денний ліміт.

#### T2.5 · Валідація: walk-forward, embargo, hold-out, журнал спроб `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `xhigh`) · **ТЗ:** §5.2.4, §5А.6, §5А.10
- **Зробити:** розширюване вікно; purging і embargo ≥ `horizon_bars` **за часом для всіх символів**; адаптивні фолди (подовження 3 → 6 міс., якщо угод торгового символу < `min_trades_per_fold` = 20); ізольований hold-out з лічильником відкриттів (≤ 2); таблиці `experiments` / `experiment_trials` з OOS-результатами **кожної** спроби і загальною кількістю спроб лінії; метрики окремо по всіх символах і по торговому.
- **Готово, коли:** тести: рядок будь-якого символу, що перетинає тестовий час, не потрапляє в train; індекси hold-out ніколи не потрапляють у train/Optuna/калібрування; кожна спроба записана.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) `IntegrityError` на `holdout_openings` = ліміт відкриттів вичерпано; перевіряти, що новий відрізок справді новіший.

#### T2.6 · Статистика: блочний bootstrap, DSR, PBO, коридор, Gate G1 `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `xhigh`) · skill `ml-pipeline` · **ТЗ:** §5.2.5, §5.3, §5.7, §5А.10
- **Зробити:** блочний bootstrap PF (блоки по тижню); Deflated Sharpe Ratio з урахуванням кількості спроб; PBO методом CSCV (16 блоків); розрахунок коридору результатів (5-й перцентиль сумарного R для N угод, максимальна серія збитків); функція Gate G1 з усіма умовами (вкл. `DSR ≥ 0.95`, `PBO < 0.5`, ML-фільтр кращий за правило без фільтра); спроба відкрити hold-out без G1 → помилка.
- **Готово, коли:** тести на синтетичних даних з відомим результатом (випадкові спроби → DSR низький, PBO ≈ 0.5; справжня перевага → DSR високий). Рев'ю: `ml-leakage-auditor`.

#### T2.7 · Бейзлайни і первинна стратегія `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §5.1, §5А.3
- **Зробити:** «завжди FLAT», «завжди LONG», випадкові входи, перетин EMA, пробій — через бектестер з витратами і лімітами; вибір 1–2 первинних стратегій з мінімумом параметрів; параметри фіксуються лише на walk-forward; той самий код правила використовується в live (T5.2). Кожен варіант правила — спроба в журналі.
- **Готово, коли:** звіт порівняння по фолдах; обрані правила зафіксовані в конфігу/метаданих.

#### T2.8 · Meta-модель LightGBM і метрики `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · skill `ml-pipeline` · **ТЗ:** §5.1, §5А.3, §5А.9
- **Зробити:** бінарна LightGBM на кандидатах первинної стратегії, стандартні параметри, `sample_weight` = ваги унікальності × ваги класів; метрики після витрат з лімітами live; порівняння з правилом без фільтра; тест із перемішаними мітками; `device=cpu`.
- **Готово, коли:** звіт експерименту в `experiments`; shuffled-labels ≈ 0. Рев'ю: `ml-leakage-auditor`.

#### T2.9 · Калібрування і правило очікуваної вигоди `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §5.5.1, §5А.8
- **Зробити:** isotonic/Platt на out-of-sample; діаграма надійності; функція `EV = p·TP_net − (1−p)·SL_net` з витратами з T2.2 (time-stop = повний SL); вибір `min_edge` і `min_confidence` лише на walk-forward; та сама функція використовується в live (T5.2).
- **Готово, коли:** тести EV на граничних значеннях; тест, що калібрування і вибір порогів не бачать hold-out.

#### T2.10 · Optuna як задача воркера `[ ]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `high`) · **ТЗ:** §5.6, §5А.7
- **Зробити:** діапазони §5А.7; метрика — медіана по фолдах мінус штраф за розкид; 100–200 спроб; **кожна спроба → `experiment_trials`**; прогрес через `jobs`; параметри таргета й первинної стратегії не перебираються.
- **Готово, коли:** задача запускається, скасовується, відновлюється; hold-out не використовується; кількість спроб коректно потрапляє в DSR.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T1.8) обробник нового виду задачі — у `app/jobs/worker.py` (зараз лише `backfill_candles` і `backfill_derivatives`); воркер один на БД і бере задачі по одній (`docs/decisions/0008-single-worker.md`), тож довга Optuna затримує задачі backfill.

#### T2.11 · Артефакти, відтворюваність, статуси, перенавчання рецепта `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §5.3, §5.4, §5.4.1
- **Зробити:** `models/<version>/` + `model_metadata.json` за схемою ТЗ §5.4 (`label_type`: `meta` / `direct` / `rule_only` — артефакт `rule_only` без файлу моделі, ТЗ §5.1; первинна стратегія, `decision_rule`, `validated_by`, `live_corridor`, `git_commit`, `git_dirty`, версії бібліотек, `data_hash`, seed, `experiment_id`, `n_trials_total`, DSR, PBO); переходи `TRAINED → CANDIDATE → APPROVED → RETIRED` (APPROVED лише вручну); **коридор результатів рахується при переході в CANDIDATE** (ТЗ §5.7); `git_dirty` → заборона CANDIDATE; процедура перенавчання рецепта на всіх даних без підбору; блок інференсу при розбіжності `feature_schema_hash`, `preprocessing_version` або версії бібліотеки; Live приймає лише APPROVED.
- **Готово, коли:** тести: Live-бот не стартує з CANDIDATE; модель з `git_dirty` не стає CANDIDATE; інша версія LightGBM → інференс заблоковано. Рев'ю: `trading-safety-reviewer`.

#### T2.12 · Дослідницький цикл моделі (з людиною) `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §5А.3, §5А.10, §5А.11
- **Зробити:** ітерація 1 — meta-labeling з 1–2 первинними стратегіями на walk-forward; ітерація 2 — пряме 3-класове прогнозування (лише після ітерації 1); усі спроби в одному журналі. Рішення про відкриття hold-out приймає **людина** після Gate G1. Після успіху — перенавчання рецепта (T2.11), коридор, CANDIDATE. Критерій зупинки §5А.11.
- **Готово, коли:** є модель у статусі CANDIDATE або задокументоване рішення торгувати первинною стратегією без ML.

### Етап 3. Біржовий шар

#### T3.1 · Машина станів ордера `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · skill `order-state-machine` · **ТЗ:** §7.1, §7.2
- **Зробити:** таблиця переходів в одному місці; з термінальних станів переходів немає; OCC; `order_events` в тій самій транзакції; write-ahead `SUBMITTING`; `clientOrderId = "tb" + HOST_ID + uuid4().hex` (36 символів, ТЗ §7.2).
- **Готово, коли:** тест кожного дозволеного і недозволеного переходу; property-тест послідовностей подій; конкурентний апдейт. Рев'ю: `trading-safety-reviewer`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) стани — `ORDER_STATES` з `app/trading/models.py`; граф переходів у БД не дублюється.
  - (T1.1) результат `place_order` класифікується лише через `is_definite_rejection` (`app/exchange/errors.py`); ордер біржі зі статусом `UNKNOWN` не є «знайдено» і переходу не дає.

#### T3.2 · Bybit: торгові методи та мапінг помилок `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · skill `exchange-adapter` · **ТЗ:** §3, §6.6, §9.1
- **Зробити:** `place_order` (з нативними TP/SL, `tpslMode=Full`, `reduceOnly`), `cancel_order`, `fetch_order_by_client_id`, `fetch_positions`, `fetch_open_orders`, `set_leverage`, `set_margin_mode`, `set_position_protection`, `fetch_balance`, `fetch_executions`; мапінг кодів помилок у доменні винятки; таймаут ≠ відхилення; one-way + isolated перевіряються на старті.
- **Готово, коли:** contract-тести на Fake і на Testnet (мінімальний розмір). Рев'ю: `trading-safety-reviewer`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) у виходах завжди явна `qty` (`qty > 0`).
  - (T1.1) contract-набір на Testnet/Demo: фікстура перевіряє адреси адаптера (не mainnet), окремий тестовий символ або субакаунт без запущених ботів, autouse `fake_clock` перевизначено на `SystemClock`. Мапінг кодів — лише allowlist (невідомий код і service error → `ExchangeOutcomeUnknown`); «not modified» (`set_leverage`, `set_margin_mode`, `trading-stop`) → успіх; помилка нормалізації → `MalformedResponse`; синоніми статусів (`Created` → `NEW`) — явно, решта → `UNKNOWN`.
  - (T1.2) кожен REST — через `RateLimiter.call` / `acquire` (групи — `app/exchange/README.md`); `retry=False` для `NON_RETRYABLE_METHODS` (зараз це дисципліна викликача, `call` не перевіряє); перевищення — `limit_breach_from_response`, після відповіді — `observe`; `fetch_order_by_client_id` може потребувати двох груп (realtime і history). CCXT: `enableRateLimit=False`, неявні запити (`load_markets`, звірка часу) — через лімітер або вимкнені. Вирішити мапінг retCode `429` («high server load»): `limit_breach_from_response` його не розпізнає. Розвести чергу лімітера Demo і Live: зараз одна черга на процес, тож 429 одного UID зупинить SL іншого (ТЗ §8.1). Торговий процес `python -m app` має створити лімітер з `role=ProcessRole.TRADING` (зараз не створює).
  - (T1.6) CCXT `options.recvWindow` = `Settings.recv_window_ms`, підпис — локальним часом; retCode `10002` → `ClockGuard.request_recheck`. **Рішення користувача** (ТЗ §4.3 не змінено): годинник, що випереджає біржу на ≥ 1 с, дає `10002` на **всі** приватні запити, включно з SL, примусовим закриттям і Emergency Stop. Варіант: на `10002` для `reduceOnly` / SL / закриття повторювати з часом, виправленим на виміряний зсув.
  - (T1.7) після відхилення через фільтри — `InstrumentCache.refresh_after_rejection` (коди — `app/exchange/README.md`); плече в `set_leverage` = менше з конфігу та `instruments.max_leverage`.

#### T3.3 · Розв'язання `SUBMISSION_UNKNOWN` `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · skill `order-state-machine` · **ТЗ:** §7.2
- **Зробити:** алгоритм з 6 кроків (ТЗ §7.2); блок нових ордерів бота; після «не знайдено → REJECTED»: **вхідний ордер не повторюється** (сигнал → `EXPIRED_UNEXECUTED`), **вихід `reduceOnly` повторюється новим ордером з новим `clientOrderId`**, якщо позиція ще відкрита; ескалація `unknown_escalation_s` → `ERROR` + CRITICAL.
- **Готово, коли:** тести всіх гілок на Fake («прийнято, відповідь втрачено», «не прийнято» — окремо для входу і для виходу, «біржа недоступна 2 хв»); тест, що вхід ніколи не повторюється. Рев'ю: `trading-safety-reviewer`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) ордер, знайдений після `REJECTED`, обробляється лише через fills; `IntegrityError` переходу не обриває звірку.
  - (T1.1) ордер зі статусом `UNKNOWN` не є «знайдено» для розв'язання `SUBMISSION_UNKNOWN`.
  - (T1.6) **Рішення користувача** (ТЗ §7.2 не змінено): три перевірки з інтервалом 2–3 с коротші за вікно, в якому Bybit ще може прийняти запит (`recv_window` + ~1 с після відправки), тож «не знайдено → REJECTED» може прийти раніше за пізній вхід. Варіант: остання перевірка — не раніше `submit + recv_window_ms + CLOCK_DRIFT_LIMIT_MS`. `TODO(verify-bybit)`: де Bybit перевіряє `recvWindow`.

#### T3.4 · Fills, `ledger`, `equity_snapshots` `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §7.2, §15
- **Зробити:** дедуп `exec_id` (WS + REST); середня ціна, комісії, PnL лише з fills; `ledger` з дедупом за id події; `equity_snapshots` раз на хвилину і після fill (equity з біржі); корекція `peak_equity`/`day_start_equity` на депозити/виведення.
- **Готово, коли:** тести: дубль fill з WS і REST → один запис; PnL збігається з ручним розрахунком; депозит не зменшує просадку хибно.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) ордери `EXCHANGE` — з fill-подій, з `bot_id` і `qty` з виконань; явний мапінг видів `ledger` (TRANSFER, BONUS, комісії ліквідації), невідомий вид — помилка; `equity_snapshots` — `ON CONFLICT DO NOTHING`; символи Bybit → формат CCXT до вставки.
  - (T0.4) обробник fills не оновлює рядок `bots` в одній транзакції зі вставкою ордера/позиції і повторює захисні операції при SQLSTATE `40P01`.

#### T3.5 · Приватний WebSocket і реконект `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §5.5, §11.2
- **Зробити:** підписки order/execution/position/wallet (+ закриті свічки публічного потоку); ping/pong, `ws_stale_s`; реконект з backoff+jitter; після реконекту — REST-дозавантаження (`fetch_executions`); подія розриву → `RECONCILING`.
- **Готово, коли:** тести з Fake: розрив, дублікати, пропуски; `testnet`-тест реконекту.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T1.6) реконект WS → `ClockGuard.request_recheck(reason=...)`.

#### T3.6 · Ключі з `.env` і перевірка дозволів при старті `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §14, §19 · рішення `docs/decisions/0002-keys-in-env.md`
- **Зробити:** читання `BYBIT_<MODE>_API_KEY/SECRET` з `.env` як `SecretStr` (через конфіг T0.3); `HOST_ID` комп'ютера в `.env` і `allowed_host_id` акаунта (ТЗ §6.4); `.env.example` з усіма змінними без значень; при старті — запит дозволів кожного ключа через API Bybit; ключ із правом виводу → боти цього режиму не стартують + CRITICAL; результат і час перевірки — в `exchange_accounts`; статус (без ключів) — в `/system`; якщо API не дозволяє перевірити — `[!]` і ручне підтвердження.
- **Готово, коли:** тести з фейковими відповідями (є право виводу → відмова; немає ключа → зрозуміла помилка; ключ не потрапляє в логи/відповіді); перевірено на Testnet- і Demo-ключах.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.3) ключі біржі — лише `SecretStr`-поля `Settings` (логер маскує їх за значенням).
  - (T0.4) набір `key_check_result` можна уточнити новою міграцією.
  - (T0.6) процесні тести (`tests/db/test_instance_lock_process.py`) обнуляють `BYBIT_*` в оточенні дочірнього процесу.

#### T3.7 · Перевірка припущень про Bybit на Testnet і Demo `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · skill `chaos-scenario` · **ТЗ:** §19
- **Зробити:** експериментально перевірити: відхилення повторного `clientOrderId`; поведінку прив'язаних TP/SL при частковому вході; формат/довжину `orderLinkId`; актуальні комісії; поведінку `reduceOnly` без позиції; **доступність у Demo Trading** усіх потрібних ендпоінтів і WS-потоків (ордери, trading-stop, позиції, execution list, приватний WS, баланс, перевірка дозволів ключа) — прогнати contract-тести адаптера на Demo.
- **Готово, коли:** `docs/bybit-assumptions.md` з результатами; всі `TODO(verify-bybit)` закриті або перетворені на задачі.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) `createType` і `orderLinkId` ордерів TP/SL позиції.
  - (T1.1) `TODO(verify-bybit)` з `app/exchange/README.md`: TP/SL `Full` як умовні ордери і їхня `qty`, статус `TRIGGERED`, вікно унікальності `orderLinkId`, `reduceOnly` більший за позицію.
  - (T1.3) чи вистачає `KLINE_FINALITY_MARGIN_MS` (500 мс) на mainnet.
  - (T1.6) де саме Bybit перевіряє `recvWindow` (див. T3.3).

### Етап 4. Ризик і захист

#### T4.1 · `RiskGate` і pre-trade перевірки `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · skill `risk-engine-change` · **ТЗ:** §8.1, §8.2, §8.4
- **Зробити:** версіонований `risk_config` (послаблення лише з підтвердженням); 6 перевірок §8.4 з кодами причин (вкл. статус моделі, версії бібліотек, `allowed_host_id`), усе **окремо для кожного акаунта**, перевірки лімітів — через чисті функції `risk/limits.py` з T2.2 (ті самі, що в бектесті); запис у `risk_events`/`decision_log`; `reduceOnly` і аварійне закриття ніколи не блокуються.
- **Готово, коли:** тест на кожну перевірку (на межі, вище, нижче); тест, що немає шляху до вхідного ордера поза `RiskGate`. Рев'ю: `trading-safety-reviewer`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T1.1) розглянути обов'язковий `RiskApproval` у сигнатурі `place_order` для `reduce_only=False` (зараз інваріант 5 тримається дисципліною викликів).
  - (T1.6) перед входом — `ClockGuard.state(scope).entries_allowed` для акаунта бота; причина відмови — у `decision_log`.
  - (T1.7) перед входом — `entry_block` (`INSTRUMENT_UNKNOWN`, `INSTRUMENT_NOT_TRADING`, `INSTRUMENT_RULES_STALE`).

#### T4.2 · Ліміти рахунку та статуси Risk Engine `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §8.2, §8.5, §15
- **Зробити:** підключення чистих функцій лімітів з T2.2 до реальних даних рахунку — **окремо для кожного акаунта** (equity, ліміти, статус Risk Engine): денний збиток (від `day_start_equity`, з нереалізованим PnL), просадка від `peak_equity`, серія збитків + `cooldown_bars`, `max_trades_per_day`; статуси `NORMAL/RESTRICTED/HALTED`; дії §8.5; `HALTED` → Emergency Stop.
- **Готово, коли:** тести переходів статусів і дій; перехід через 00:00 UTC; parity-тест лімітів бектест ↔ `RiskGate`; тест, що `HALTED` у Demo не зачіпає Live.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) немає рядка `system_state` або `peak_equity` / `day_start_equity` = 0 → fail-closed, а не `NORMAL`.

#### T4.3 · Position Protection Manager `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · skill `risk-engine-change` · **ТЗ:** §9
- **Зробити:** нативний TP/SL (Full, MarkPrice для SL); fallback `set_position_protection`; стан `ENTRY_FILLED_UNPROTECTED` з `protection_timeout_s` → 3 повтори → примусове закриття → `ERROR`; перевірка при кожному fill; SL лише в бік зменшення ризику; прибирання залишкових умовних ордерів.
- **Готово, коли:** тести всіх гілок; інваріант «без SL ≤ timeout» перевіряється property-тестом на Fake. Рев'ю: `trading-safety-reviewer`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) Protection Manager не оновлює рядок `bots` в одній транзакції зі вставкою ордера/позиції; захисні операції повторюються при SQLSTATE `40P01`.
  - (T0.6) втрата lock (зокрема збій БД > 5 с) зупиняє й захист до рестарту і звірки: SL на боці біржі — якомога раніше, час рестарту закладати в `protection_timeout_s`.
  - (T0.7) **Рішення користувача** (`docs/decisions/0007-local-api.md`): збій API зупиняє весь процес, разом із захистом.
  - (T1.1) точкові запити за символом; «SL є» — лише з позиції; напрям руху SL перевіряє PPM (адаптер і фейк не перевіряють).

#### T4.4 · Звірка при старті та правила розбіжностей `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §6.4, §11.1, §11.3, §11.4
- **Зробити:** 6 кроків стартової звірки; таблиця розбіжностей §11.4 (orphan-ордери з `tb` + наш `HOST_ID`, **ордери з чужим `HOST_ID` → `PAUSED` (`FOREIGN_HOST_ORDERS`)**, ручні ордери, невідома позиція → `PAUSED` без усиновлення, зниклі позиції → `CLOSED_EXTERNALLY` з PnL з fills, розмір, відсутній SL, плече/маржа); обробка незавершених сигналів (§6.3).
- **Готово, коли:** тест на кожен рядок таблиці §11.4. Рев'ю: `trading-safety-reviewer`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) ордер, знайдений після `REJECTED`, — лише через fills; `IntegrityError` переходу не обриває звірку. OPEN-позиція `STOPPED`-бота (пізній fill БД дозволяє) → CRITICAL і пауза `UNKNOWN_POSITION` / `ERROR`.
  - (T1.1) `client_order_id_origin`: `BOT_PREFIX_MALFORMED` → `FOREIGN_HOST_ORDERS`; ордер зі статусом `UNKNOWN` → CRITICAL; у runbook попередити, що ручний ордер з `orderLinkId` на `tb` ставить ботів акаунта на паузу.
  - (T1.6) у стартовій звірці (ТЗ §11.1 п. 6) — `check_drift()` і квиток `begin_reconcile(scope)` → `confirm_reconciled(ticket)`.
  - (T1.7) звірка плеча: менше з конфігу та `instruments.max_leverage`.

#### T4.5 · Періодична звірка і розрив WS `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §6.1, §6.6, §11.2
- **Зробити:** REST-звірка кожні 15–30 с і після `SUBMISSION_UNKNOWN`; сценарій розриву WS (5 кроків); `reconcile_escalation_s`; повторне закриття за time-stop для позицій старших за `horizon_bars`; `max_reconcile_age_s` для `RiskGate`.
- **Готово, коли:** тести з Fake на кожен крок сценарію.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T1.6) у періодичній звірці — `check_drift()` і квиток `begin_reconcile(scope)` → `confirm_reconciled(ticket)`.

#### T4.6 · Протокол Emergency Stop `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §10
- **Зробити:** 6 кроків для одного акаунта і режим «Зупинити все» (по запуску на кожен акаунт); `emergency_runs`/`emergency_steps` з `account_id`; продовження після рестарту з потрібного кроку; SL лишається до підтвердження закриття; limit IOC як запасний варіант; нагадування в Telegram; вихід лише вручну.
- **Готово, коли:** тест з «рестартом» після кожного кроку → цільовий стан досягнуто без подвійних дій. Рев'ю: `trading-safety-reviewer`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) новий тригер при `COMPLETED`-запуску продовжує наявний запуск; Emergency Stop не оновлює рядок `bots` в одній транзакції зі вставкою ордера/позиції і повторює кроки при SQLSTATE `40P01`.
  - (T1.1) точкові запити за символом; «SL є» — лише з позиції.

#### T4.7 · Chaos-тести на `FakeExchangeAdapter` `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `xhigh`) · субагент `test-engineer`, skill `chaos-scenario` · **ТЗ:** §18.2
- **Зробити:** автоматизовані сценарії 1–8 з перевіркою всіх інваріантів після відновлення.
- **Готово, коли:** `pytest -m chaos` зелений; знайдені баги оформлені задачами.

### Етап 5. Бот

#### T5.1 · Статуси бота, OCC, блокування `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §6.1, §6.2, §6.4 (вкл. «один акаунт — один комп'ютер»)
- **Зробити:** статуси й переходи (вкл. `RECONCILING`), таблиця дозволів «входи / виходи»; **причини паузи `pause_reasons`** і правила автоматичного/ручного виходу (таблиця ТЗ §6.1; вихід лише коли знято всі причини); бот не стартує на комп'ютері, відмінному від `allowed_host_id` акаунта; `asyncio.Lock` на бота; OCC `version`; один активний бот на символ; режим фіксується при створенні.
- **Готово, коли:** тести переходів і конкурентних команд; тести кожної причини паузи (авто- і ручний вихід, кілька причин одночасно); тест відмови старту на чужому `HOST_ID`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) попередній статус після `RECONCILING` не зберігається; цикл бота — під `asyncio.Lock` (дві паралельні транзакції «вставка ордера + оновлення бота» одного бота → deadlock).
  - (T0.7) **Рішення користувача** (`docs/decisions/0007-local-api.md`): збій API зупиняє весь торговий процес.
  - (T1.4) `TRADING_SYMBOL` у `.env` — тимчасово, до символів ботів із `bots`.
  - (T1.6) `ClockGuard.run(on_jump=...)` у торговому процесі (один на процес): `on_jump` → усі боти в `RECONCILING`, вихід — після звірки акаунта й `confirm_reconciled`; завершення `run` → блок нових входів.
  - (T1.7) не виходити в `RUNNING`, поки стоїть `INSTRUMENT_NOT_TRADING`; стартовий `InstrumentCache.refresh` — лише після переведення ботів у `STARTING`: `apply_instrument_pauses` сам повертає в `RUNNING` бота `PAUSED` з єдиною цією причиною, і до стартової звірки це обійшло б ТЗ §11.1.

#### T5.2 · Цикл бота на закритті свічки `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · skill `order-state-machine` · **ТЗ:** §5.5, §5.5.1, §6.3
- **Зробити:** порядок обробки (WS → буфер → первинна стратегія → фічі/інференс meta-моделі → правило очікуваної вигоди з T2.9 → асинхронний запис у БД; сигнал правила без ML-фільтра не торгується); `signals` зі статусами `NEW/EXECUTING/DONE/EXPIRED_UNEXECUTED`; stale signal (`max_signal_age_ms`); fail-closed; прогрів моделі при старті; вимір затримки; відновлення після падіння між сигналом і ордером; цілісність вікна фіч (ТЗ §4.2): `has_open_gap` **і** наявність свічки `t-1` у БД (`False` означає лише «відкритого рядка немає»), прогалина → не торгувати, `DATA_GAP`, попередження і докачування саме інтервалу дірки (курсор backfill `max(час)` внутрішню дірку не закриває); детектор після закриття свічки — не раніше `settled_end_ms` (`CANDLE_SETTLE_MS` = 1500 мс).
- **Готово, коли:** тест повторної обробки тієї самої свічки (немає дублів); тест «падіння після сигналу до ордера» для всіх трьох гілок. Рев'ю: `trading-safety-reviewer`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.6) `InstanceLock.ensure_held()` перед кожним входом.
  - (T1.3) щойно закриту свічку запитувати не раніше `close + KLINE_FINALITY_MARGIN_MS` за часом біржі; `max_signal_age_ms` у конфігу більший за цей запас (тест-інваріант разом із полем).
  - (T1.4, рішення `docs/decisions/0011-funding-with-candles.md`) `MarketDataSync.run_if_due` (свічки 1h і funding для `TRADING_SYMBOL` і `RESEARCH_SYMBOLS`) — при старті й кожні `BACKFILL_INTERVAL_S`. Зараз його не запускає жоден процес (розділ 5).
  - (T1.6) перед входом — `state(scope).entries_allowed`, причина відмови — у `decision_log`.
  - (T1.7) `InstrumentCache.refresh_if_due` у циклі — до backfill (інакше долістинговий період лишається діркою); `entry_block` перед входом.

#### T5.3 · Входи, виходи, time-stop `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §6.6
- **Зробити:** market-вхід з `max_slippage_bps` (post-only у v1 вимкнено, ТЗ §6.6); SL/TP від `target_params`; time-stop `max_hold_bars ≤ horizon_bars` через машину станів; реальний slippage у `decision_log`; без розвороту/доливання.
- **Готово, коли:** тести LONG/SHORT; перевищення slippage → вхід скасовано; time-stop при `REJECTED` закритті повторюється з новим `clientOrderId`.

#### T5.4 · Перемикання моделі (`MODEL_SWAP`) `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §5.4
- **Зробити:** процедура з 5 кроків; лише без позиції, на межі свічки, лише на модель потрібного статусу; відкат однією дією.
- **Готово, коли:** тест: при відкритій позиції перемикання відкладається; несумісна схема → стара модель лишається.

#### T5.6 · Telegram-сповіщення `[ ]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `high`) · **ТЗ:** §12
- **Зробити:** рівні INFO/WARNING/CRITICAL; повтор CRITICAL кожні `critical_repeat_s`, доки стан триває і доки не натиснуто «Підтвердити» (inline-кнопка Telegram або UI); черга з retry (недоступний Telegram не блокує торговий цикл); без секретів у текстах.
- **Готово, коли:** тест: збій Telegram не впливає на цикл бота.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.3) токен Telegram — лише `SecretStr`-поле `Settings`.
  - (T1.6) сповіщення для `clock_drift_exceeded`, `clock_jump_detected`, `clock_recheck_requested`.
  - (T1.7) сповіщення для `instrument_not_trading` (зараз лише лог).

#### T5.7 · Dead-man's switch `[ ]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `medium`) · **ТЗ:** §12
- **Зробити:** heartbeat раз на 60 с на зовнішній сервіс (Healthchecks.io); heartbeat лише коли торговий цикл реально живий (а не просто процес).
- **Готово, коли:** перевірено навмисним зупиненням бекенду.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.3) URL heartbeat — лише `SecretStr`-поле `Settings`: маскується за значенням, шаблони маскера в шляху URL його не ловлять.

#### T5.8 · Drift-моніторинг і коридор результатів `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §5.7
- **Зробити:** щоденний PSI ключових фіч (> 0.25 → попередження; > 0.25 у 3 перевірках поспіль → `PAUSED`); **коридор результатів** з `model_metadata.live_corridor` — після кожної закритої угоди в **Demo і Live** (на Testnet — лише запис у лог): сумарний R нижче коридору або серія збитків > 1.5× максимуму з бектесту → `PAUSED` з причиною `MODEL_UNDERPERFORMS`, Telegram, відновлення лише вручну; калібрування slippage входу/SL/TP за фактичними fills.
- **Готово, коли:** тести на синтетичному зсуві розподілу і на синтетичній серії угод нижче коридору.

#### T5.9 · Резервні копії та перевірка відновлення `[ ]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `medium`) · **ТЗ:** §12.2
- **Зробити:** щоденний `pg_dump` на інший диск; ротація 7 щоденних + 4 тижневі; скрипт перевірки відновлення в тестову БД зі звіркою кількостей.
- **Готово, коли:** відновлення перевірене вручну один раз.

#### T5.10 · Середовище запуску й автозапуск (Windows + WSL2 і macOS) `[ ]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `medium`) · **ТЗ:** §6.4, §12, §12.1, §12.1.1
- **Зробити:** інструкції і скрипти для обох ОС (ТЗ §12.1, §12.1.1). Windows: WSL2 + Docker, Task Scheduler/systemd з автоперезапуском (бекенд і колектор), вимкнення сну, Windows Update, NTP, дані БД у volume WSL. macOS: Docker Desktop/OrbStack, launchd-агенти (`KeepAlive`), `pmset`/`caffeinate`, синхронізація часу. `HOST_ID` у `.env` на кожному комп'ютері; `allowed_host_id` для кожного акаунта.
- **Готово, коли:** після перезавантаження кожної ОС бекенд і колектор стартують самі й проходять звірку; на недозволеному комп'ютері бот акаунта не стартує.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.6) ніколи не додавати `--force-takeover` у plist / systemd; ненульовий код виходу → перезапуск.
  - (T0.7) `API_TOKEN` для служби — лише з `.env`.
  - (T1.6) годинник WSL2 після сну Windows: обидва локальні годинники стоять, `tick()` паузу не бачить — її ловлять планова перевірка і `request_recheck`.

#### T5.11 · Demo-прогін на реальних цінах `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §13, §17 (етап 5)
- **Зробити:** прогін ≥ 3–5 днів у Bybit Demo Trading (`BYBIT_DEMO_*` у `.env`, рішення `docs/decisions/0003-demo-instead-of-paper.md`); аналіз логів, `decision_log`, затримок сигналів, розбіжностей з бектестом. Прогін перевіряє **механіку**, а не прибутковість (ТЗ §5.3).
- **Готово, коли:** звіт `docs/runs/demo-<дата>.md` без аномалій.

### Етап 6. API та UI

#### T6.1 · REST API `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §16
- **Зробити:** групи `/data /models /bots /positions /orders /risk /system`; pydantic-схеми з default і діапазонами (для FieldHelp); `Idempotency-Key`; зрозумілі коди помилок.
- **Готово, коли:** OpenAPI-схема повна; тести ендпоінтів.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.7) обробники беруть ключ із `request.state.idempotency_key` і пишуть `audit_log` через `AuditWriter` у тій самій транзакції; ендпоінти, що збільшують експозицію, — `Depends(require_instance_lock)`; помилки — `ApiError(status, ErrorCode, message)`.
  - (T0.4) `[Test Long]` / `[Test Short]` — лише для не-`STOPPED` бота (БД відхиляє вхід зупиненого).
  - (T1.8) ручний перезапуск `FAILED`-задачі (крім backfill: його ставить наступний прохід) — окремий ендпоінт.

#### T6.2 · Небезпечні дії в API `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · **ТЗ:** §8.1, §13, §14, §16
- **Зробити:** запуск Live, послаблення ризиків, вихід з `EMERGENCY_STOP`/`HALTED` (для акаунта або для всіх), approve моделі — лише з явним підтвердженням (окремий токен/фраза) + `audit_log`; `[Test Long]/[Test Short]` — лише Demo/Testnet; перехід на Live лише через зупинку бота.
- **Готово, коли:** тест на кожну дію без підтвердження → відмова; тест, що Test Long недоступний у Live. Рев'ю: `trading-safety-reviewer`.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.4) `audit_log` з `confirmed`, `account_id` / `target`; новий запис на кожну дію (тригер БД приймає одне підтвердження на одну дію).
  - (T0.7) `confirmed=True` — лише після підтвердження людини.
  - (T0.7) **Рішення користувача** (`docs/decisions/0007-local-api.md`): недоступне сховище ключів блокує й захисні команди.

#### T6.3 · WebSocket-канали для UI `[ ]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `high`) · **ТЗ:** §16
- **Зробити:** `bots positions orders jobs alerts prices`; автентифікація токеном; backpressure.
- **Готово, коли:** тести підписки та авторизації.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.7) браузерний WebSocket не задає `Authorization` — вирішити, як передати токен (напр., підпротоколом); `ws="none"` в `app/api/server.py` → увімкнути й додати залежність `websockets`.

#### T6.4 · Генерація TS-типів з OpenAPI `[ ]`
- **Ризик:** R0 · **Модель:** Haiku 5.5 (`haiku`, `medium`) · **ТЗ:** §2, §16
- **Зробити:** скрипт `npm run gen:api` (напр. `openapi-typescript`); перевірка в CI, що типи актуальні.
- **Готово, коли:** `npm run typecheck` проходить на згенерованих типах.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.7) схема — офлайн з `create_app(ctx).openapi()` (без сервера і токена); `apps/web/scripts/gen-api.mjs` — заглушка, її коментар про відсутній FastAPI застарів.

#### T6.5 · `<FieldHelp>`, файл довідки, CI-перевірка `[ ]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `high`) · skill `field-help` · **ТЗ:** §16А.1, §16А.4
- **Зробити:** компонент Tooltip/Popover (клавіатура, `aria-describedby`, Esc, дотик); файл довідки; default/range з OpenAPI; тест покриття полів.
- **Готово, коли:** тест покриття падає на полі без довідки.

#### T6.6 · Тексти підказок українською `[ ]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `high`) · skill `field-help` · **ТЗ:** §16А.2, §16А.3
- **Зробити:** тексти для всіх місць §16А.3 за структурою з 5 частин; живі розрахунки для числових полів ризику. Описи ризикових параметрів звіряються з кодом Risk Engine.
- **Готово, коли:** CI-тест покриття зелений; ви вибірково перевірили тексти ризиків.

#### T6.7 · Дашборд, боти, позиції, журнал рішень `[ ]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `high`) · **ТЗ:** §12, §16
- **Зробити:** стан ботів, heartbeat і затримка даних, статус Risk Engine, позиції зі станом захисту, `decision_log`; без бізнес-логіки у фронті.
- **Готово, коли:** усі дані — з API/WS; `npm run typecheck` і тест покриття підказок зелені; ручна перевірка в браузері.
- **Передано з попередніх задач** (у дужках — задача, у примітці якої подробиці):
  - (T0.7) як UI отримує токен (не в URL). **Рішення користувача** (`docs/decisions/0007-local-api.md`): стандартний порт Vite як origin.

#### T6.8 · Графік `[ ]`
- **Ризик:** R0 · **Модель:** Sonnet 5.5 (`sonnet`, `medium`) · **ТЗ:** §2, §16А.3
- **Зробити:** Lightweight Charts зі свічками, позначками угод, SL/TP, легендою з підказками.
- **Готово, коли:** відображення перевірено на реальних даних.

#### T6.9 · ML Studio `[ ]`
- **Ризик:** R1 · **Модель:** Sonnet 5.5 (`sonnet`, `high`) · **ТЗ:** §5, §16А.3
- **Зробити:** запуск задач навчання/Optuna, прогрес, метрики по фолдах (по всіх символах і по торговому), журнал експериментів і кількість спроб, DSR, PBO, порівняння з первинною стратегією без фільтра, коридор результатів, лічильник відкриттів hold-out, approve (через T6.2).
- **Готово, коли:** повний цикл «навчити → переглянути → CANDIDATE» з UI.

#### T6.10 · UI ризиків і Emergency Stop `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §8, §10, §16А
- **Зробити:** налаштування ризиків з живим розрахунком; модальні підтвердження з наслідками; кнопки Emergency Stop для акаунта і «Зупинити все», процедура виходу; підтвердження CRITICAL-сповіщень; відображення прогресу `emergency_steps`.
- **Готово, коли:** ручна перевірка: послаблення ліміту й вихід з `EMERGENCY_STOP` без підтвердження неможливі (сама заборона гарантується бекенд-тестами T6.2).


### Етап 7. Testnet

#### T7.1 · Chaos-сценарії на Testnet `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `xhigh`) · skill `chaos-scenario` · **ТЗ:** §18.2
- **Зробити:** провести сценарії 1–9 (ТЗ §18.2, вкл. запуск акаунта з другого комп'ютера) на реальному Testnet API (мінімальний розмір), проаналізувати `order_events`, звіти в `docs/chaos/`.
- **Готово, коли:** усі 9 пройдено; інваріанти не порушено.

#### T7.2 · 7-денний Testnet-прогін і аналіз `[ ]`
- **Ризик:** R2 · **Модель:** Opus 5.5 (`opus`, `high`) · **ТЗ:** §5.3 (п. 5), §17 (етап 7)
- **Зробити:** ≥ 7 днів безперервно; щоденний аналіз логів; калібрування slippage (вхід, SL, TP); перевірка роботи коридору результатів; перевірка Telegram і dead-man's switch навмисним вимкненням.
- **Готово, коли:** звіт `docs/runs/testnet-<дата>.md`: 0 втрачених ордерів, 0 подвійних позицій, 0 позицій без SL. Висновків про прибутковість з 7 днів не робимо (5–15 угод).

#### T7.3 · Незалежний аудит торгового шляху перед Live `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `max`) — свіжа сесія; друга думка — Opus 5.5 (`opus`, `xhigh`) окремою сесією
- **Зробити:** повний прохід шляху «свічка → сигнал → ризик → ордер → fill → захист → вихід → звірка → Emergency Stop» на відповідність ТЗ та `AGENTS.md`; субагенти `trading-safety-reviewer` і `ml-leakage-auditor`.
- **Готово, коли:** немає відкритих блокерів; звіт `docs/audit/pre-live-<дата>.md`.

### Етап 8. Live

#### T8.1 · Pre-live чекліст `[ ]`
- **Ризик:** R3 · **Модель:** Fable 5.1 (`fable`, `high`) — допомагає перевірити; рішення приймає людина · **ТЗ:** §18.3
- **Зробити:** пройти всі пункти §18.3; ключ без права виводу; модель `APPROVED`; стартовий розмір ≤ 10–20% запланованого; ліміти активні.
- **Готово, коли:** чекліст підписаний вами в `docs/audit/pre-live-<дата>.md`.

#### T8.2 · Live з мінімальним розміром `[ ]`
- **Ризик:** — · **Виконує лише людина.** Агент не запускає Live, не змінює live-ключі і не підвищує розміри (це заблоковано hooks).
- **Зробити:** запуск через UI з підтвердженням; поступове ручне підвищення розміру.

---

## 4. Відкладені задачі

Не входять у критичний шлях. Повертаються, коли виконано умову (ТЗ §5.6, `docs/decisions/0001-ml-methodology.md`).

| ID | Задача | Ризик | Модель / effort | Умова повернення |
|---|---|---|---|---|
| D1 | Перевірка GPU-стеку (RTX 5070, CUDA 12.8+, WSL2) | R0 | sonnet / medium | один прогін Optuna > 1 години або з'являється нейромережа |
| D2 | XGBoost / GPU-перемикач і заміри CPU vs GPU | R0 | sonnet / medium | після D1 |

#### D1 · Перевірка GPU-стеку (RTX 5070) `[ ]`
- **Ризик:** R0 · **Модель:** Sonnet 5.5 (`sonnet`, `medium`) · **ТЗ:** §5.6, §12.1
- **Зробити:** скрипт `scripts/check_gpu.py`: версія драйвера/CUDA (12.8+), `torch.cuda.is_available()`, тестове навчання XGBoost `device=cuda`, fallback на CPU.
- **Готово, коли:** звіт у `docs/env/gpu-check.md` з результатами на цільовому ПК (WSL2).

#### D2 · XGBoost / GPU-перемикач і заміри `[ ]`
- **Ризик:** R0 · **Модель:** Sonnet 5.5 (`sonnet`, `medium`) · **ТЗ:** §5.6
- **Зробити:** `device: cpu | cuda` з fallback; заміри CPU vs GPU на реальних даних; перевірка відтворюваності результатів GPU.
- **Готово, коли:** звіт `docs/env/gpu-benchmark.md`; рішення, чи вмикати GPU.

---

## 5. Відкриті питання власника

Питання з приміток виконаних задач, для яких немає цільової задачі (питання з цільовою задачею — у її рядку «Передано з попередніх задач», позначка **Рішення користувача**). Вирішує користувач; після рішення — нова задача або `docs/decisions/NNNN-*.md`, а пункт звідси прибирається.

| Звідки | Питання |
|---|---|
| T1.4, T1.7 | Свічки 1h, funding і кеш `instruments` не оновлює жоден процес: `MarketDataSync` і `InstrumentCache` чекають T5.1/T5.2 (funding — разом зі свічками, `docs/decisions/0011-funding-with-candles.md`), а етапу 2 потрібна історія свічок. Без кешу `instruments` сітка прогалин деривативів і ліквідацій іде від `BACKFILL_START_MS`, а не від лістингу. |
| T1.2 | ТЗ §6.5: частки 50/30/20 дають рівно 600 запитів / 5 с, без запасу; два комп'ютери за одним NAT ділять ліміт IP; retCode `10006` ставить на паузу всю чергу процесу, включно з SL в інших групах. |
| T0.4 | Роль застосунку — не власник БД: зараз застосунок працює від superuser, тож append-only тригери не захищають від `DISABLE TRIGGER`. |
| T0.6 | Lock діє в межах однієї БД: другий процес на тому самому комп'ютері з іншою `POSTGRES_DB` (напр. відновлена копія з ботами того самого акаунта) не блокується. |
| T1.2, T1.7 | Розбіжності ТЗ з кодом без рішення: §3 називає `rate_limiter.py` (модуль — `rate_limit.py`); у §15 в `instruments` немає `launch_time_ms` і `max_qty`. |
| T0.1 | `npm audit`: 3 вразливості (2 critical) у dev-залежності `vitest` (через `tinypool`); виправлення — `vitest` 5.x (breaking). Рішення 2026-10-28 — «до наступної задачі»; станом на 2026-10-30 не переглядалось. |

