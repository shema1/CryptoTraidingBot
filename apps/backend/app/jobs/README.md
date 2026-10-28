# `app/jobs` — Воркер задач

## Призначення

Окремий процес `python -m app.jobs.worker`: бере задачі з таблиці `jobs` (`FOR UPDATE SKIP LOCKED`) — Optuna, навчання, backfill. Не торгує і не має торгових ключів.

> Заплановано: T1.8, T2.10. Зараз є лише модель БД (T0.4).

## Розділи ТЗ

- ТЗ §5.6 — Optuna та задачі
- ТЗ §6.4 — процеси

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `Job`, `JOB_STATUSES` | `models.py` | таблиця `jobs`; `QUEUED`/`RUNNING`/`DONE`/`FAILED`/`CANCELED` |

## Таблиці БД

- `jobs` — `kind` (`^[a-z][a-z0-9_]*$`, набір видів визначає T1.8), `status`, `priority` (менше — раніше),
  `params`, `progress` ∈ [0, 1], `progress_message`, `result`, `error`, `attempt`, `created_at_ms`, `started_at_ms`,
  `heartbeat_at_ms`, `finished_at_ms`. `RUNNING` має `started_at_ms` і `heartbeat_at_ms`; `finished_at_ms` заданий
  ⇔ термінальний статус. `dedup_key` — частковий `UNIQUE` серед `QUEUED`/`RUNNING` (друга така сама задача не
  створюється). Індекс черги `(priority, created_at_ms) WHERE status = 'QUEUED'`.

Схема створюється міграцією `migrations/versions/0001_initial_schema.py`; обмеження й тригери перевіряють `tests/db/test_constraints.py`, `tests/db/test_guards.py` і `tests/db/test_migrations.py`.


## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.4 | `[x]` | таблиця `jobs` |
| T1.8 | `[ ]` | Воркер задач (`jobs`) |
| T2.10 | `[ ]` | Optuna як задача воркера |
