# Crypto Trading Bot & ML Analytics

Особистий локальний бот для Bybit USDT-perpetual (API v5) з ML-сигналами, жорстким Risk Engine і безпечною
поведінкою при збоях. Некомерційний проєкт, запуск лише локально (Windows + WSL2 і macOS).

- ТЗ: [`AppInfo.md`](AppInfo.md)
- Інструкції для AI-агентів та інваріанти: [`AGENTS.md`](AGENTS.md)
- План робіт: [`docs/ai/TASKS.md`](docs/ai/TASKS.md), налаштування AI-інструментів: [`docs/ai/AI_SETUP.md`](docs/ai/AI_SETUP.md)
- Рішення та відхилення від ТЗ: [`docs/decisions/`](docs/decisions/)
- Перший запуск: [`docs/runbooks/getting-started.md`](docs/runbooks/getting-started.md)

## Структура

```text
apps/backend/   Python-бекенд (FastAPI, воркер, колектор) — див. apps/backend/README.md
apps/web/       React + Vite + TypeScript — див. apps/web/README.md
models/         артефакти моделей (у git не потрапляють)
docs/           план, рішення, інструкції запуску (runbooks), шаблони
scripts/        службові скрипти (синхронізація правил AI, перевірка секретів, перевірка БД)
infra/db/init/  init-скрипти TimescaleDB (read-only роль, тестова БД)
docker-compose.yml  TimescaleDB (лише 127.0.0.1:5432)
```

## Перший запуск

Покроково — **[`docs/runbooks/getting-started.md`](docs/runbooks/getting-started.md)**: що встановити, які змінні
`.env` заповнити (обов'язкові: `POSTGRES_PASSWORD`, `POSTGRES_READONLY_PASSWORD`, `API_TOKEN`), запуск БД, міграцій,
торгового процесу й перевірка API, типові помилки.

## Щоденні команди

```bash
# бекенд (з apps/backend)
uv run pytest -q
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run python -m app                              # торговий процес + API на 127.0.0.1:8000 (другий екземпляр на тій самій БД не стартує; коди виходу — apps/backend/README.md)
uv run python -m app.jobs.worker                  # воркер задач (backfill); коди виходу — apps/backend/README.md
uv run python -m app.data.collector               # колектор ліквідацій і хвоста деривативів; коди виходу — apps/backend/README.md

# веб (з apps/web)
npm run typecheck
npm test
npm run dev                                       # http://127.0.0.1:5173

# усі pre-commit перевірки (з кореня)
uv run --project apps/backend pre-commit run --all-files
```

## База даних (TimescaleDB)

```bash
docker compose up -d --wait timescaledb          # запуск; чекає стану healthy (потрібні паролі POSTGRES_* у файлі .env)
PGRO_PASSWORD=<POSTGRES_READONLY_PASSWORD> bash scripts/check_db.sh   # healthy, порт лише 127.0.0.1, ctb_readonly не пише, тестова БД
docker compose stop timescaledb                  # зупинка (дані лишаються в іменованому volume timescaledb_data)
```

- Порт публікується лише на `127.0.0.1:5432`; `restart: unless-stopped`, `healthcheck` через `pg_isready`.
- Основна БД `ctb` (власник `ctb`), окрема БД `ctb_test` для `integration`-тестів, read-only роль `ctb_readonly`
  для MCP `postgres-readonly` (сесії за замовчуванням read-only, доступ лише до основної БД, `SELECT` на всі поточні й майбутні таблиці).
  MCP підключається через змінну `MCP_DATABASE_URI` у профілі shell — [`docs/ai/AI_SETUP.md`](docs/ai/AI_SETUP.md), «Встановлення», крок 2.
- Init-скрипт (`infra/db/init/`) виконується **лише при першій ініціалізації порожнього volume**. Зміна пароля пізніше — `ALTER ROLE` у `psql`.
  Видалення volume знищує всі дані БД — робіть це лише свідомо й на dev-БД.

Схема — міграції Alembic (`apps/backend/migrations/`, T0.4); DSN береться з `.env` через `Settings`, не з `alembic.ini`:

```bash
cd apps/backend
uv run alembic upgrade head                      # основна dev-БД
uv run alembic -x db=test upgrade head           # тестова БД ctb_test (integration-тести роблять це самі)
uv run pytest -q -m integration                  # тести схеми й міграцій (без запущеної БД — skip; CTB_REQUIRE_DB=1 — помилка)
```

> Заплановано: `npm run gen:api` — генерація типів з OpenAPI (T6.4, зараз заглушка).

## Pre-commit

Хуки (`.pre-commit-config.yaml`): заборона комітів `.env*` / `*.pem` / `*.key` (крім `.env.example`), `ruff check`,
`ruff format --check`, `mypy`, `scripts/sync_ai_rules.py --check` (правила Cursor відповідають `.claude/rules/`),
`tsc --noEmit` для вебу.

## Безпека

Ключі біржі — лише в `.env` (без права виводу), сервіси слухають лише `127.0.0.1`, до етапу 8 на
mainnet нічого не торгує і live-ключі не використовуються (публічні ринкові дані mainnet читаються без ключів). Деталі — ТЗ §14 і `docs/decisions/0002-keys-in-env.md`.
