# `app/api` — REST- і WebSocket-ендпоінти локального API

## Призначення

Приймає команди UI та віддає стан. Слухає лише `127.0.0.1`; бізнес-логіки не містить — делегує модулям `trading`, `risk`, `reconcile`, `ml`.

> Заплановано: T0.7, T6.1, T6.2, T6.3. Зараз є лише модель `audit_log` (T0.4).

## Розділи ТЗ

- ТЗ §14 — безпека локального API (bearer-токен, `Origin`/`Host`, CORS)
- ТЗ §16 — REST-групи, WebSocket-канали, `Idempotency-Key`

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `AuditLog` | `models.py` | таблиця `audit_log` (ТЗ §14, §15) |
| `AUDIT_ACTION_RISK_LOOSEN`, `AUDIT_ACTION_EMERGENCY_RELEASE`, `AUDIT_ACTION_MODEL_APPROVE` | `models.py` | дії, що підтверджують небезпечні зміни (`risk.config.loosen`, `emergency.release`, `model.approve`) |

## Таблиці БД

- `audit_log` — дії користувача: `actor`, `action` (`^[a-z][a-z0-9_.]*$`), `account_id`/`bot_id` (як дані, без FK —
  журнал не залежить від інших таблиць), `target` (інша ціль дії, напр. `model_version`), `reason`, `confirmed`, `idempotency_key`, `request`/`result` (JSONB, без
  секретів), `ts_ms`. **Append-only**: UPDATE/DELETE/TRUNCATE відхиляє тригер `ctb_forbid_mutation()`.
- На записи `audit_log` посилаються небезпечні дії через `confirmed_audit_id`: `risk_config` (послаблення
  лімітів, дія `risk.config.loosen`), `emergency_runs` (вихід з Emergency Stop, `emergency.release`), `models`
  (`APPROVED`, `model.approve`). Тригер `ctb_require_confirmed_audit()` пропускає лише запис із
  `confirmed = true`, саме цією дією і тією самою ціллю: `account_id` акаунта (`risk_config`, `emergency_runs`)
  або `target = model_version` (`models`). Один запис підтверджує лише одну дію (частковий
  `UNIQUE (confirmed_audit_id)` у кожній із трьох таблиць). `confirmed = true` пише лише API після підтвердження в UI (T0.7, T6.2).

Схема створюється міграцією `migrations/versions/0001_initial_schema.py`; обмеження й тригери перевіряють `tests/db/test_constraints.py`, `tests/db/test_guards.py` і `tests/db/test_migrations.py`.


## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.4 | `[x]` | модель і таблиця `audit_log` (append-only) |
| T0.7 | `[ ]` | Каркас FastAPI і безпека локального API |
| T6.1 | `[ ]` | REST API |
| T6.2 | `[ ]` | Небезпечні дії в API |
| T6.3 | `[ ]` | WebSocket-канали для UI |
