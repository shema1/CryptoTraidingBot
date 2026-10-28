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
scripts/        службові скрипти (синхронізація правил AI, перевірка секретів)
```

## Потрібно встановити

- [`uv`](https://docs.astral.sh/uv/) (сам встановить Python 3.11 за `apps/backend/.python-version`)
- Node.js ≥ 20.19 і npm
- Docker (TimescaleDB, з'явиться в задачі T0.2)

## Перше налаштування

```bash
bash ai-setup/install.sh                          # один раз: розгортає .claude/ і .mcp.json (якщо ще не зроблено)
cp .env.example .env && chmod 600 .env            # секрети лише тут, файл не комітиться
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

> Заплановано: `docker compose up -d timescaledb` (T0.2), `uv run alembic upgrade head` (T0.4),
> `npm run gen:api` — генерація типів з OpenAPI (T6.4, зараз заглушка).

## Pre-commit

Хуки (`.pre-commit-config.yaml`): заборона комітів `.env*` / `*.pem` / `*.key` (крім `.env.example`), `ruff check`,
`ruff format --check`, `mypy`, `scripts/sync_ai_rules.py --check` (правила Cursor відповідають `.claude/rules/`),
`tsc --noEmit` для вебу.

## Безпека

Ключі біржі — лише в `.env` (без права виводу), сервіси слухають лише `127.0.0.1`, нічого не запускається проти
mainnet до етапу 8. Деталі — ТЗ §14 і `docs/decisions/0002-keys-in-env.md`.
