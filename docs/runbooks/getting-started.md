# Перший запуск: що заповнити й як запустити

Покрокова інструкція для нового комп'ютера (macOS або Windows + WSL2, ТЗ §12.1). Усі команди — з кореня репозиторію,
якщо не сказано інше. Документ оновлюється в тій самій задачі, що змінює кроки запуску або змінні `.env`
(`.claude/rules/documentation.md`); тест `apps/backend/tests/test_env_example.py` перевіряє, що тут описано кожну змінну з
`.env.example`.

> Стан проєкту: працюють БД, міграції, торговий процес із локальним API (`GET /system/health`), воркер публічного
> backfill, колектор ліквідацій та деривативів (публічні WebSocket і REST Bybit, без ордерів і без ключів) і
> дослідницький ML-конвеєр на історії з БД (крок 5А). Боти, торгівля й екрани UI — ще не реалізовані (`docs/ai/TASKS.md`).

## 1. Встановити

| Що | Навіщо | Перевірка |
| --- | --- | --- |
| [`uv`](https://docs.astral.sh/uv/) | Python-залежності бекенду; сам встановить Python 3.11 (`apps/backend/.python-version`) | `uv --version` |
| Node.js ≥ 20.19 і npm | веб-інтерфейс `apps/web` | `node --version` |
| Docker Desktop або OrbStack | TimescaleDB (`docker-compose.yml`) | `docker compose version` |

На Windows усе встановлюється й запускається **всередині WSL2**, а не в нативній консолі (ТЗ §12.1).

Увімкніть автоматичну синхронізацію часу ОС (macOS: «Дата й час → Встановлювати автоматично»; Windows — так само,
WSL2 бере час від Windows). Розбіжність з часом біржі від 1 с блокуватиме нові входи, а стрибок годинника (сон,
ручна зміна) переведе ботів у звірку (ТЗ §4.3). Зсув на macOS показує `sntp time.apple.com`. У WSL2 після сну
Windows годинник Linux може відставати від Windows: порівняйте `date -u` у WSL2 з годинником Windows; типовий обхід —
`sudo hwclock -s` або `wsl --shutdown` (перевірка на реальній WSL2 — T5.10).

## 2. Файл `.env` — заповнити перед першим запуском

```bash
cp .env.example .env && chmod 600 .env
```

`.env` лежить у корені, у git не потрапляє (pre-commit це блокує) і не пересилається нікому. Порожнє значення
(`NAME=`) означає «взяти значення за замовчуванням».

### Обов'язкові (без них нічого не запуститься)

| Змінна | Що це | Як отримати |
| --- | --- | --- |
| `POSTGRES_PASSWORD` | пароль власника БД `ctb` | довгий випадковий рядок (команда нижче) |
| `POSTGRES_READONLY_PASSWORD` | пароль read-only ролі `ctb_readonly` (MCP `postgres-readonly`) | інший довгий випадковий рядок |
| `API_TOKEN` | bearer-токен локального API: «пароль», без якого жодна сторінка в браузері не може керувати ботом (ТЗ §14). Без нього торговий процес не стартує (код виходу 15) | `≥ 32` символи `[A-Za-z0-9._~+/-]`, з них ≥ 10 різних — команда нижче |

Згенерувати кожне значення окремо (щоразу нове):

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

- Паролі `POSTGRES_*` застосовуються **лише при першому створенні volume** БД. Змінити їх пізніше — через
  `ALTER ROLE … PASSWORD …` у `psql`, а не правкою `.env`.
- Зберігайте копію `.env` у менеджері паролів (не поряд із дампами БД, ТЗ §14).
- Токен або пароль засвітився — згенеруйте новий, замініть у `.env` і перезапустіть бекенд.

### Необов'язкові (є безпечні значення за замовчуванням)

| Змінна | За замовчуванням | Коли змінювати |
| --- | --- | --- |
| `LOG_LEVEL` | `INFO` | `DEBUG` для діагностики |
| `LOG_DIR` | `<корінь>/logs` | інше місце для логів |
| `LOG_MAX_BYTES`, `LOG_BACKUP_COUNT` | 50 MiB, 10 копій | обсяг логів на диску |
| `LOG_CONSOLE` | `true` | `false` для служби launchd/systemd (stderr-файл не ротується) |
| `POSTGRES_HOST`, `POSTGRES_PORT` | `127.0.0.1`, `5432` | БД на іншому локальному порту (хост — лише loopback) |
| `POSTGRES_DB`, `POSTGRES_USER` | `ctb`, `ctb` | майже ніколи; тестова БД — `<POSTGRES_DB>_test` |
| `LOCK_WAIT_S` | `60` | скільки секунд при старті чекати, поки звільниться lock іншого екземпляра (у торгового процесу, воркера і колектора — у кожного свій) |
| `API_PORT` | `8000` | порт 8000 зайнятий іншою програмою (код виходу 16) |
| `API_ALLOWED_ORIGINS` | `http://127.0.0.1:5173` | UI відкривається з іншої адреси (лише `127.0.0.1` / `localhost` / `[::1]` з портом) |
| `RATE_LIMIT_SHARE_TRADING_PCT` | `50` | частка IP-ліміту Bybit торгового процесу, %; сума трьох часток — рівно 100 (роль процесу в `.env` не задається — її визначає команда запуску) |
| `RATE_LIMIT_SHARE_WORKER_PCT` | `30` | частка воркера, % |
| `RATE_LIMIT_SHARE_COLLECTOR_PCT` | `20` | частка колектора, % |
| `TRADING_SYMBOL` | `BTC/USDT:USDT` | символ, яким торгує бот; свічки качаються і для нього |
| `RESEARCH_SYMBOLS` | 8 пар `…/USDT:USDT` (BTC, ETH, SOL, XRP, BNB, DOGE, ADA, LTC) | список для навчання, 5–8 символів через кому; міняти після старту історії не варто (survivorship bias, ТЗ §5А.6) |
| `BACKFILL_START_MS` | `1577836800000` | з якої мілісекунди UTC качати історію свічок і funding (кратно 1 годині) |
| `BACKFILL_INTERVAL_S` | `600` | як часто повторювати докачування, секунди |
| `RECV_WINDOW_MS` | `5000` | вікно дійсності приватного запиту Bybit, мс (2000 … 10000); розбіжність годинника не лікує — для неї NTP |

Межі й точні формулювання — коментарі в `.env.example`, опис полів — `apps/backend/app/core/README.md`.

> Заплановано: ключі біржі `BYBIT_<MODE>_API_KEY` / `BYBIT_<MODE>_API_SECRET` (лише право торгівлі, без виводу) — T3.6;
> `HOST_ID` комп'ютера й `allowed_host_id` акаунтів — T5.1; токен Telegram — T5.6; URL heartbeat — T5.7. Додавати їх
> у `.env` зараз не потрібно — конфіг їх ще не читає.

## 3. Залежності

```bash
(cd apps/backend && uv sync)
(cd apps/web && npm ci)
uv run --project apps/backend pre-commit install    # git-хуки (лише для розробки)
bash ai-setup/install.sh                            # лише якщо в корені є тека ai-setup/ (AI-агенти: .claude/, .mcp.json)
```

На macOS офіційне колесо LightGBM шукає `libomp`. Якщо `import lightgbm` падає з `Library not loaded: libomp.dylib`, поставте `brew install libomp` і повторіть `uv sync`. Рецепт meta-моделі фіксує `n_jobs=1`, тож OpenMP на результат навчання не впливає, але без бібліотеки колесо просто не імпортується.

Якщо `brew install libomp` зупиняється з `Refusing to load formula … from untrusted tap …`, справа не в libomp (він з `homebrew/core`): Homebrew 7 вимагає явної довіри до сторонніх tap-ів, з яких уже щось встановлено. Довіртеся лише тим формулам, які brew перелічить у попередженні (`brew trust --formula <user>/<tap>/<formula> …`), або приберіть непотрібний tap (`brew untap <user>/<tap>`), і повторіть `brew install libomp`.

Працюєте з AI-агентами (Claude Code, Cursor) — після кроку 4 задайте `MCP_DATABASE_URI` для MCP `postgres-readonly`
(агент читає dev-БД через роль `ctb_readonly`): `docs/ai/AI_SETUP.md`, «Встановлення», крок 2. Змінна живе в профілі
shell, а не в `.env`; без неї бекенд працює, лише агент не бачить БД.

## 4. База даних

```bash
docker compose up -d --wait timescaledb
```

Команда чекає стану `healthy`. Помилка `задайте POSTGRES_PASSWORD у .env` — не заповнено крок 2.
Повна перевірка (порт лише на 127.0.0.1, read-only роль не пише, тестова БД існує) — з паролем `ctb_readonly`:

```bash
PGRO_PASSWORD=<POSTGRES_READONLY_PASSWORD> bash scripts/check_db.sh
```

Міграції схеми (і після кожного `git pull`, що додав нові):

```bash
cd apps/backend && uv run alembic upgrade head
```

## 5. Торговий процес і API

```bash
cd apps/backend && uv run python -m app
```

Процес бере single-instance lock у БД і відкриває API на `127.0.0.1:8000` (лише цей комп'ютер). Зупинка — Ctrl+C
або SIGTERM (код 0). Логи — `logs/backend/` (`app.log`, `errors.log`, …).

Перевірка в іншому терміналі (з кореня; токен через stdin, щоб його не було видно в `ps` та історії):

```bash
export API_TOKEN=$(grep '^API_TOKEN=' .env | cut -d= -f2-)
```

```bash
printf 'Authorization: Bearer %s\n' "$API_TOKEN" | curl -H @- http://127.0.0.1:8000/system/health
```

Очікувано: `{"status":"ok", …, "checks":{"instance_lock":true,"database":true}}`. Без токена — 401, це правильно.

Воркер задач (backfill довгої історії, підбір гіперпараметрів Optuna — задача `optuna_meta`) — окремий процес. `API_TOKEN` йому не потрібен: лише
БД з кроку 4. Ключі біржі він не читає; свічки бере з публічного mainnet. Пріоритет процесу нижчий за звичайний.
На одній БД працює лише один воркер: другий чекає `LOCK_WAIT_S` і завершується з кодом 10.

```bash
cd apps/backend && uv run python -m app.jobs.worker
```

Зупинка — Ctrl+C (код 0, поточна задача знову `QUEUED`). Якщо БД у цей момент недоступна, воркер однаково
завершується, а задача стає `FAILED` за простроченим heartbeat. Логи — `logs/worker/`.

Колектор ліквідацій — теж окремий процес. `API_TOKEN` і ключі біржі йому не потрібні: лише БД з кроку 4.
Він відкриває публічний WebSocket Bybit (mainnet) для `RESEARCH_SYMBOLS` і пише години в `liquidations_hourly`.
Ще він раз на 10 хв питає час біржі (публічний REST `fetch_time`, mainnet, без ключів) і раз на
`BACKFILL_INTERVAL_S` (за замовчуванням 600 с) докачує останні години деривативів (open interest, premium, mark/index,
long/short) для `RESEARCH_SYMBOLS`. Якщо якогось ряду бракує більше ніж 168 годин, колектор ставить задачу
`backfill_derivatives` — її виконає воркер (крок вище). Поки розбіжність годинника не перевірена або сягає 1 с, години
ліквідацій не закриваються і деривативи не качаються. Тож потрібен інтернет і синхронізація часу ОС (NTP).
Історію ліквідацій біржа не віддає, тож процес варто тримати запущеним. На одній БД працює лише один колектор:
другий чекає `LOCK_WAIT_S` і завершується з кодом 10. Логи — `logs/collector/`. Перевірка простою — відкриті
`data_gaps` з `source = liquidations` (`docs/data/liquidations.md`). Після оновлення коду колектор треба
перезапустити: запущений процес працює зі старим кодом.

```bash
cd apps/backend && uv run python -m app.data.collector
```

Зупинка — Ctrl+C (код 0).

## 5А. Історія ринку і дослідницький прогін (етап 2)

**1. Поставити задачі історії** (лише БД, без мережі й ключів; повтор — безпечний і докачує лише нове):

```bash
cd apps/backend && uv run python -m app.jobs.enqueue market-data
```

Ставить три задачі воркера: `refresh_instruments` (кеш правил інструментів без зміни пауз ботів; пропускається, поки в БД є
боти режиму `demo` — тоді кеш оновлює торговий процес), `backfill_candles` (свічки 1h і funding від `BACKFILL_START_MS`) і
`backfill_derivatives`. Виконує їх воркер (крок 5). Перший прохід — кілька хвилин; стан — `SELECT kind, status, result FROM jobs`.

**2. Тримати колектор і воркер запущеними (macOS).** Ноутбук, що засинає, не збирає ліквідації: агенти launchd
стартують процеси при вході, перезапускають після збою і тримають заборону сну (`caffeinate -i -s`, лише від мережі
живлення; закрита кришка без зовнішнього монітора однаково присипляє Mac). Без sudo, у `~/Library/LaunchAgents`:

```bash
scripts/macos/launch_agents.sh install
```

```bash
scripts/macos/launch_agents.sh status
```

Прибрати — `scripts/macos/launch_agents.sh uninstall`. Логи процесів — як і раніше в `logs/collector/` і
`logs/worker/`; вивід launchd — `logs/launchd/`. Якщо колектор чи воркер уже запущені вручну, агент чекає lock і
перезапускається кожні 30 с, доки ручний процес не зупиниться. Після `git pull` — `install` ще раз (перезапуск).

**3. Дослідницький прогін ітерації 1** (бейзлайни → вибір правила → meta-модель → перегляд Gate G1):

```bash
cd apps/backend && uv run python -m app.ml.research iteration-one
```

Варіанти таргета й таймфрейму — лише зафіксовані в `docs/decisions/0025-target-and-timeframe-before-iteration-two.md`
(`--variant A` — 4h 2/1 ATR, `B` — 4h 3/1,5 ATR, `C` — 1h 3/1,5 ATR; без прапорця — `baseline`, 1h 2/1 ATR):

```bash
cd apps/backend && uv run python -m app.ml.research iteration-one --variant A
```

Важливо: **кожен прогін назавжди пише спроби в журнал лінії** (N для DSR росте), тож запускати свідомо, не «для
перевірки». Перший прогін фіксує hold-out лінії (останні 20% вікна): повторний прогін зі свіжішими даними
відмовить з кодом 14. Hold-out прогін не відкриває. Тривалість — десятки хвилин (CPU). Звіт — `docs/research/<дата>-iteration-one-btc.md`,
шлях — у лозі `research_report_written` (`logs/research/`).

## 6. Веб-інтерфейс (поки каркас)

```bash
cd apps/web && npm run dev    # http://127.0.0.1:5173
```

> Заплановано: екрани й введення `API_TOKEN` в UI — T6.7; зараз UI до API не звертається.

## 7. Якщо щось не так

| Симптом | Причина | Що зробити |
| --- | --- | --- |
| `python -m app` → код 15, `api_token_missing` | немає `API_TOKEN` у `.env` | крок 2 |
| помилка валідації `API_TOKEN: щонайменше 32 символи…` | токен закороткий або слабкий | згенерувати командою з кроку 2 |
| код 16, `api_bind_failed` | порт `API_PORT` зайнятий | закрити програму на порту або змінити `API_PORT` |
| код 10, `instance_lock_held_by_other` | уже працює інший екземпляр на тій самій БД | зупинити його; лог показує pid процесу |
| код 11, `instance_lock_db_unavailable` | БД не запущена | крок 4 |
| `python -m app.jobs.worker` → код 15, `worker_db_not_configured` | немає `POSTGRES_PASSWORD` | крок 2 |
| воркер → код 10, `instance_lock_held_by_other` | уже працює інший воркер на тій самій БД | зупинити його; лог показує pid процесу |
| воркер → код 11, `instance_lock_db_unavailable` або `worker_db_unavailable` | БД не запущена | крок 4 |
| воркер → код 12, `instance_lock_lost` | БД перезапустилась або обірвалось з'єднання | запустити воркер знову; задачу, що виконувалась, буде позначено `FAILED` |
| `python -m app.data.collector` → код 15, `collector_db_not_configured` | немає `POSTGRES_PASSWORD` | крок 2 |
| колектор → код 10, `instance_lock_held_by_other` | уже працює інший колектор на тій самій БД | зупинити його; лог показує pid процесу |
| колектор → код 11, `instance_lock_db_unavailable` | БД не запущена | крок 4 |
| колектор → код 12, `instance_lock_lost` | БД перезапустилась або обірвалось з'єднання | запустити колектор знову; простій ляже в `data_gaps` |
| колектор → `liquidation_topic_rejected` (error) | символ із `RESEARCH_SYMBOLS` біржа не знає (опечатка, знятий з торгів) | виправити `RESEARCH_SYMBOLS` і перезапустити; решта символів збирається, години цього — прогалини |
| колектор → `liquidation_clock_untrusted`, `clock_drift_exceeded` або `clock_drift_check_failed` | годинник ОС розходиться з біржею на ≥ 1 с, стрибок часу або біржа недоступна по REST | увімкнути синхронізацію часу ОС; поки годиннику не вірять, години стають прогалинами |
| колектор → `liquidation_save_failed` / `liquidation_gap_sync_failed` (error, `retry_in_ms`) | БД недоступна | крок 4; колектор сам повторює запис з паузою до 60 с, готові години чекають у пам'яті |
| колектор → `derivatives_sync_failed` (error) або `derivatives_sync_retry` | БД чи біржа недоступні під час проходу деривативів | нічого: повтор після паузи (від 30 с до `BACKFILL_INTERVAL_S`), ліквідації збираються далі; якщо повторюється — дивитися `error_type` |
| колектор → `derivatives_enqueued`, а дані деривативів не з'являються | історії бракує більше ніж 168 год — її качає воркер | запустити воркер (крок вище) |
| колектор → `clock_jump_detected` кожні ~15 хв, жодного `liquidations_sealed` | Mac засинає (стрибок ~900 с) | крок 5А.2 (агенти з `caffeinate`), живлення від мережі |
| `python -m app.jobs.enqueue` → код 11 | БД не запущена | крок 4 |
| воркер → `instruments_refresh_skipped` (`bots_exist`) | у БД є боти режиму `demo`: кеш оновлює торговий процес | нічого; для досліджень кеш уже є |
| `python -m app.ml.research` → код 13, `research_data_insufficient` | немає свічок або інструментів для символів | крок 5А.1 і воркер |
| `python -m app.ml.research` → код 10, `research_lock_held` | уже йде інший дослідницький прогін на цій БД | дочекатися його завершення |
| `python -m app.ml.research` → код 14, `research_split_rejected` | hold-out лінії вже зафіксований першим прогоном, а свіжі дані дають інше вікно | не запускати повторно «для перевірки»: повторний прогін лінії — рішення T2.17 (кожен прогін додає спроби в N) |
| health → 503, `"database": false` | БД недоступна | `docker compose ps`, крок 4 |
| змінюючий запит → 503 `IDEMPOTENCY_STORE_UNAVAILABLE` | не застосовано міграції або БД недоступна | `uv run alembic upgrade head` |
| 403 `HOST_NOT_ALLOWED` / `ORIGIN_NOT_ALLOWED` | запит не на `127.0.0.1:<API_PORT>` або UI з адреси поза `API_ALLOWED_ORIGINS` | відкривати UI з `http://127.0.0.1:5173` або додати адресу в `API_ALLOWED_ORIGINS` |

Повний перелік кодів виходу — `apps/backend/README.md`.

## 8. Після `git pull`

```bash
(cd apps/backend && uv sync && uv run alembic upgrade head)
(cd apps/web && npm ci)
```

Перевірте, чи не з'явились нові обов'язкові змінні: `git diff HEAD@{1} -- .env.example`.
