# Перший запуск: що заповнити й як запустити

Покрокова інструкція для нового комп'ютера (macOS або Windows + WSL2, ТЗ §12.1). Усі команди — з кореня репозиторію,
якщо не сказано інше. Документ оновлюється в тій самій задачі, що змінює кроки запуску або змінні `.env`
(`.claude/rules/documentation.md`); тест `apps/backend/tests/test_env_example.py` перевіряє, що тут описано кожну змінну з
`.env.example`.

> Стан проєкту: працюють БД, міграції й торговий процес із локальним API (`GET /system/health`). Боти, біржа, ML і
> екрани UI — ще не реалізовані (`docs/ai/TASKS.md`). Нічого не торгує й не звертається до біржі.

## 1. Встановити

| Що | Навіщо | Перевірка |
| --- | --- | --- |
| [`uv`](https://docs.astral.sh/uv/) | Python-залежності бекенду; сам встановить Python 3.11 (`apps/backend/.python-version`) | `uv --version` |
| Node.js ≥ 20.19 і npm | веб-інтерфейс `apps/web` | `node --version` |
| Docker Desktop або OrbStack | TimescaleDB (`docker-compose.yml`) | `docker compose version` |

На Windows усе встановлюється й запускається **всередині WSL2**, а не в нативній консолі (ТЗ §12.1).

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
| `LOCK_WAIT_S` | `60` | скільки секунд при старті чекати, поки звільниться lock іншого екземпляра |
| `API_PORT` | `8000` | порт 8000 зайнятий іншою програмою (код виходу 16) |
| `API_ALLOWED_ORIGINS` | `http://127.0.0.1:5173` | UI відкривається з іншої адреси (лише `127.0.0.1` / `localhost` / `[::1]` з портом) |
| `RATE_LIMIT_SHARE_TRADING_PCT` | `50` | частка IP-ліміту Bybit торгового процесу, %; сума трьох часток — рівно 100 (роль процесу в `.env` не задається — її визначає команда запуску) |
| `RATE_LIMIT_SHARE_WORKER_PCT` | `30` | частка воркера, % |
| `RATE_LIMIT_SHARE_COLLECTOR_PCT` | `20` | частка колектора, % |

Межі й точні формулювання — коментарі в `.env.example`, опис полів — `apps/backend/app/core/README.md`.

> Заплановано: ключі біржі `BYBIT_<MODE>_API_KEY` / `BYBIT_<MODE>_API_SECRET` (лише право торгівлі, без виводу) — T3.6;
> `HOST_ID` комп'ютера й `allowed_host_id` акаунтів — T5.1; токен Telegram — T5.6; URL heartbeat — T5.7. Додавати їх
> у `.env` зараз не потрібно — конфіг їх ще не читає.

## 3. Залежності

```bash
(cd apps/backend && uv sync)
(cd apps/web && npm ci)
uv run --project apps/backend pre-commit install    # git-хуки (лише для розробки)
bash ai-setup/install.sh                            # лише якщо працюєте з AI-агентами (.claude/, .mcp.json)
```

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
