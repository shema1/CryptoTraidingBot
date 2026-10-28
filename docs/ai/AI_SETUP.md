# Налаштування AI-інструментів (Claude Code + Cursor)

## Що де лежить

| Що | Claude Code | Cursor | Джерело істини |
|---|---|---|---|
| Головні інструкції та інваріанти | `CLAUDE.md` (імпортує `AGENTS.md`) | `AGENTS.md` + `.cursor/rules/00-project.mdc` | `AGENTS.md` |
| Правила за областями коду | `.claude/rules/*.md` (поле `paths`) | `.cursor/rules/*.mdc` (поле `globs`) | `.claude/rules/` → генерується скриптом |
| Skills (покрокові процедури) | `.claude/skills/*/SKILL.md` | ті самі файли (Cursor читає `.claude/skills/`) | `.claude/skills/` |
| Субагенти-рев'юери | `.claude/agents/*.md` | — (використовуйте `/safety-check` у Claude Code) | `.claude/agents/` |
| Запобіжники (hooks) | `.claude/settings.json` → `.claude/hooks/guard.py` | `.cursor/hooks.json` → той самий `guard.py` | `.claude/hooks/` |
| MCP-сервери | `.mcp.json` | `.cursor/mcp.json` | обидва файли (синтаксис змінних різний) |
| Що не передавати в AI | `permissions.deny` + hooks | `.cursorignore` + hooks | — |
| План робіт і вибір моделі | `docs/ai/TASKS.md` | `docs/ai/TASKS.md` | `docs/ai/TASKS.md` |

## Skills
| Skill | Коли |
|---|---|
| `order-state-machine` | машина станів ордера, `SUBMISSION_UNKNOWN`, fills |
| `risk-engine-change` | Risk Engine, sizing, захист позицій |
| `exchange-adapter` | `ExchangeAdapter`, Bybit (live / demo / testnet), rate limiter, WS |
| `ml-pipeline` | фічі, таргет, бектест, валідація |
| `db-migration` | схема БД, Alembic, TimescaleDB |
| `field-help` | підказки в UI |
| `chaos-scenario` | chaos-тести |
| `/safety-check` | вручну перед комітом: тести + незалежне рев'ю |
| `/next-task` | вручну: взяти задачу з `TASKS.md` з перевіркою моделі |

## Субагенти
| Агент | Модель | Роль |
|---|---|---|
| `trading-safety-reviewer` | Fable 5.1, xhigh | рев'ю торгового ядра, ризику, захисту, безпеки (лише читання) |
| `ml-leakage-auditor` | Opus 5.5, xhigh | пошук look-ahead і витоку даних (лише читання) |
| `test-engineer` | Opus 5.5, high | тести до реалізації (не змінює продакшн-код) |

Якщо Fable недоступна на вашому тарифі — змініть `model: fable` на `model: opus` у `.claude/agents/trading-safety-reviewer.md`.

## Hooks: що блокується
- **Заборонено:** читати/редагувати `.env*` (крім `.env.example`), використовувати `BYBIT_LIVE_API_KEY/SECRET` у командах, ключі `*.pem/*.key`; звертатися до mainnet Bybit (`api.bybit.com`, `stream.bybit.com`); запуск з `TRADING_MODE=live` / `BYBIT_TESTNET=false`; `--force-takeover`; `docker compose down -v`, видалення volume; `rm -rf` кореня, `models/`, `migrations/`; force-push; редагування закомічених міграцій, артефактів у `models/`, згенерованих `.cursor/rules/*.mdc`.
- **З підтвердженням:** `DROP/TRUNCATE`, `alembic downgrade`, `git reset --hard`, інший `rm -r`, `pip install` поза lock-файлом, зміна `AppInfo.md`.
- **Після редагування `.py`:** `ruff format` + `ruff check`; проблеми повертаються агенту для виправлення.
- **Після редагування `.claude/rules/*.md`:** автоматично перегенеровуються правила Cursor.
- **Перед завершенням відповіді (Claude Code, `docs_sync_check.py`):** якщо в git змінено код, але не змінено жодного документа, агент отримує нагадування і мусить оновити документацію або пояснити, чому вона не зачеплена (правило `documentation.md`). Працює після `git init` (задача T0.1). У Cursor це правило діє через `.cursor/rules/documentation.mdc` (завжди активне).

## Встановлення (один раз)

0. Якщо в корені є тека `ai-setup/` — з кореня проєкту виконайте `bash ai-setup/install.sh`. Скрипт розгорне `.claude/` і `.mcp.json`, перевірить запобіжники й видалить `ai-setup/`.
1. Потрібні: `python3`, [uv](https://docs.astral.sh/uv/) (для `uvx postgres-mcp`), Node.js (для `npx`), Docker.
2. Змінна для MCP бази (read-only роль створюється в задачі T0.2), наприклад у `~/.zshrc`:
   ```bash
   export MCP_DATABASE_URI="postgresql://ctb_readonly:<пароль>@127.0.0.1:5432/ctb"
   ```
   До виконання T0.2 сервер `postgres-readonly` просто не підключиться — це нормально.
3. **Claude Code:** відкрийте теку проєкту, запустіть `claude`, підтвердіть довіру проєкту та MCP-сервери з `.mcp.json`. Перевірка: `/mcp` (сервери), `/agents` (3 субагенти), `/hooks` (hooks), `/memory` (CLAUDE.md і rules).
4. **Cursor:** Settings → MCP — увімкніть сервери з `.cursor/mcp.json`; Settings → Rules — мають бути видні правила з `.cursor/rules` і `AGENTS.md`; Settings → Hooks — перевірте, що `.cursor/hooks.json` підхоплено (за потреби перезапустіть Cursor). Skills Cursor знайде в `.claude/skills/`.
5. **Windows / WSL2:** якщо працюєте в Windows без WSL і команда `python3` недоступна — замініть `python3` на `python` у `.claude/settings.json` та `.cursor/hooks.json`.

## Перевірка hooks
```bash
echo '{"tool_input":{"command":"cat .env"}}' | python3 .claude/hooks/guard.py bash   # має бути ЗАБЛОКОВАНО, код 2
echo '{"command":"ls"}' | python3 .claude/hooks/guard.py cursor-shell              # {"permission": "allow"}
```

## Як змінювати налаштування
- Правила — лише в `.claude/rules/*.md`, потім `python3 scripts/sync_ai_rules.py` (pre-commit перевіряє актуальність).
- Нове правило з обмеженням за шляхами — додайте `description` і `paths` у frontmatter.
- Персональні налаштування, які не мають потрапити в git — `.claude/settings.local.json` і `CLAUDE.local.md`.

## MCP-сервери
| Сервер | Для чого | Чому безпечно |
|---|---|---|
| `context7` | актуальна документація CCXT, FastAPI, SQLAlchemy, LightGBM тощо — щоб агент не вигадував API | лише читання публічних доків |
| `postgres-readonly` | агент бачить схему й дані dev-БД, перевіряє запити та індекси | `--access-mode=restricted` + окрема read-only роль |

**Свідомо не підключено:** MCP із торговими ключами біржі. Агент не повинен мати можливості відправити ордер поза кодом проєкту.
