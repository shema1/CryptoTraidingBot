# `app/risk` — Risk Engine: модель витрат, розмір позиції, ліміти, RiskGate

## Призначення

`costs.py`, `sizing.py` (`calc_position_size()`), `limits.py` (чисті функції лімітів) — спільні з бектестом; `RiskGate` з veto-правом на будь-який ордер, що збільшує експозицію; статуси Risk Engine окремо для кожного акаунта.

> Заплановано: T2.2, T4.1, T4.2. Зараз є лише моделі БД (T0.4).

## Розділи ТЗ

- ТЗ §5.2.2 — модель витрат
- ТЗ §8 — Risk Engine

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `RiskConfig`, `RiskEvent`, `SystemState`, `EmergencyRun`, `EmergencyStep`, `EquitySnapshot`, `LedgerEntry` | `models.py` | таблиці ризику, Emergency Stop і обліку капіталу |
| `RISK_STATUSES`, `EMERGENCY_TRIGGERS`, `EMERGENCY_RUN_STATUSES`, `LEDGER_KINDS` | `models.py` | набори значень |

## Таблиці БД

Усе — окремо для кожного акаунта (`account_id` обов'язковий, ТЗ §8.1).

- `risk_config` — версії лімітів, **append-only** (тригер): `UNIQUE (account_id, version)`, чинна — найбільша
  версія; `params` (JSONB, схему й defaults визначає T4.1); `loosening` → обов'язковий `confirmed_audit_id`
  (підтверджений запис `audit_log` цього акаунта з дією `risk.config.loosen`, тригер
  `ctb_require_confirmed_audit()`; один запис — одна версія).
- `risk_events` — `kind` `REJECT`/`LIMIT_TRIGGERED`/`STATUS_CHANGE`, `reason_code` (`^[A-Z][A-Z0-9_]*$`,
  напр. `RISK_DAILY_LOSS`), `bot_id` (бот того самого акаунта), `signal_id` (сигнал того самого бота), `details`,
  `ts_ms`.
- `system_state` — рядок на акаунт: `risk_status` `NORMAL`/`RESTRICTED`/`HALTED`, `emergency_run_id` (Emergency Stop
  активний ⇔ не NULL; складений FK — запуск того самого акаунта), `last_reconcile_at_ms`, `version`.
- `emergency_runs` — `trigger` `UI`/`RISK_HALTED`/`CRITICAL_ERROR`/`TELEGRAM`, `group_id` («Зупинити все»),
  `status` `RUNNING` → `COMPLETED` (цільовий стан досягнуто) → `RELEASED` (вихід вручну: потрібні `released_at_ms` і
  `confirmed_audit_id` — підтверджений запис цього акаунта з дією `emergency.release`, один запис — один вихід), `current_step` 1…6, `report`. Частковий
  `UNIQUE (account_id) WHERE status <> 'RELEASED'`: новий тригер при `COMPLETED`-запуску продовжує той самий запуск.
- `emergency_steps` — `UNIQUE (run_id, step_no, attempt)`, `status` `STARTED`/`SUCCEEDED`/`FAILED`,
  `finished_at_ms` заданий ⇔ не `STARTED`.
- `equity_snapshots` — `UNIQUE (account_id, ts_ms)`; `equity`, `wallet_balance`, `unrealized_pnl`, `peak_equity`
  (≥ `equity`), `day_start_equity` (USDT, з біржі).
- `ledger` — **append-only**; `UNIQUE (account_id, event_id, kind)`, `kind` `FEE`/`FUNDING`/`REALIZED_PNL`/
  `DEPOSIT`/`WITHDRAWAL`, `amount` зі знаком (`DEPOSIT > 0`, `WITHDRAWAL < 0`), `currency`, `symbol` (канонічний
  або NULL), `ts_ms`.

Схема створюється міграцією `migrations/versions/0001_initial_schema.py`; обмеження й тригери перевіряють `tests/db/test_constraints.py`, `tests/db/test_guards.py` і `tests/db/test_migrations.py`.


## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.4 | `[x]` | таблиці ризику, Emergency Stop, `equity_snapshots`, `ledger` |
| T2.2 | `[ ]` | Модель витрат, `calc_position_size()`, `limits.py` |
| T4.1 | `[ ]` | `RiskGate` і pre-trade перевірки |
| T4.2 | `[ ]` | Ліміти рахунку та статуси Risk Engine |
