@AGENTS.md

# Специфічне для Claude Code

- Детальні правила по областях коду лежать у `.claude/rules/` і підвантажуються автоматично за шляхами файлів.
- Skills (`.claude/skills/`): `order-state-machine`, `risk-engine-change`, `exchange-adapter`, `ml-pipeline`, `db-migration`, `field-help`, `chaos-scenario`, `safety-check`, `next-task`.
- Субагенти (`.claude/agents/`): `trading-safety-reviewer`, `ml-leakage-auditor`, `test-engineer`. Рев'юери працюють лише на читання й не бачать ходу реалізації — це навмисно.
- Модель і effort для задачі беруться з `docs/ai/TASKS.md` (`/model <alias>`, `/effort <level>`). Для R3-задач починай у Plan Mode.
- Hooks у `.claude/settings.json` блокують читання/редагування `.env`, зміну застосованих міграцій, артефактів моделей та небезпечні shell-команди. Якщо hook заблокував дію — не обходь його іншою командою; поясни користувачу, що потрібно.
