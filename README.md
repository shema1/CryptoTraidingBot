# Crypto Trading Bot & ML Analytics

Особистий локальний бот для Bybit USDT-perpetual (API v5) з ML-сигналами, жорстким Risk Engine і безпечною
поведінкою при збоях. Некомерційний проєкт, запуск лише локально (Windows + WSL2 і macOS).

- ТЗ: [`AppInfo.md`](AppInfo.md)
- Інструкції для AI-агентів та інваріанти: [`AGENTS.md`](AGENTS.md)
- План робіт: [`docs/ai/TASKS.md`](docs/ai/TASKS.md), налаштування AI-інструментів: [`docs/ai/AI_SETUP.md`](docs/ai/AI_SETUP.md)
- Рішення та відхилення від ТЗ: [`docs/decisions/`](docs/decisions/)

## Структура

```text
apps/backend/   Python-бекенд (FastAPI, воркер, колектор) — див. apps/backend/README.md
apps/web/       React + Vite + TypeScript — див. apps/web/README.md
models/         артефакти моделей (у git не потрапляють)
docs/           план, рішення, шаблони
scripts/        службові скрипти (синхронізація правил AI, перевірка секретів, перевірка БД)
infra/db/init/  init-скрипти TimescaleDB (read-only роль, тестова БД)
docker-compose.yml  TimescaleDB (лише 127.0.0.1:5432)
```

## Потрібно встановити

- [`uv`](https://docs.astral.sh/uv/) (сам встановить Python 3.11 за `apps/backend/.python-version`)
- Node.js ≥ 20.19 і npm
- Docker (Docker Desktop або OrbStack) — для TimescaleDB

## Перше налаштування

```bash
bash ai-setup/install.sh                          # один раз: розгортає .claude/ і .mcp.json (якщо ще не зроблено)
cp .env.example .env && chmod 600 .env            # секрети лише тут, файл не комітиться; задайте POSTGRES_PASSWORD і POSTGRES_READONLY_PASSWORD
(cd apps/backend && uv sync)                      # Python-залежності + uv.lock
(cd apps/web && npm install)                      # JS-залежності + package-lock.json
uv run --project apps/backend pre-commit install  # git-хуки
```

## Щоденні команди

```bash
# бекенд (з apps/backend)
uv run pytest -q
uv run ruff check . && uv run ruff format --check . && uv run mypy

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

Ключі біржі — лише в `.env` (без права виводу), сервіси слухають лише `127.0.0.1`, нічого не запускається проти
mainnet до етапу 8. Деталі — ТЗ §14 і `docs/decisions/0002-keys-in-env.md`.
