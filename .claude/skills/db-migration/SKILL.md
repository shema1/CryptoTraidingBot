---
name: db-migration
description: Використовуй, коли треба створити або змінити схему БД, SQLAlchemy-моделі, Alembic-міграцію або налаштування TimescaleDB (hypertable, compression).
paths:
  - "apps/backend/migrations/**"
  - "apps/backend/app/**/models.py"
---

# Міграція БД

1. Перевір таблицю ТЗ §15 і правила `.claude/rules/database.md`.
2. Зміни SQLAlchemy-моделі. Інваріанти — як обмеження БД (`UNIQUE`, `CHECK`, FK, часткові індекси), не лише в коді.
3. Створи **нову** міграцію: `uv run alembic revision --autogenerate --rev-id <NNNN> -m "<коротко>"` (номер — наступний за останнім у `migrations/versions/`; модуль нових моделей додай у `app.core.db.MODEL_MODULES`). Існуючі застосовані міграції не редагуй (hook це блокує).
4. Перевір згенерований файл вручну: autogenerate створює `CHECK` і часткові індекси нових таблиць з моделей, але **не помічає змін** `CHECK` в існуючих таблицях і не бачить TimescaleDB-функцій (`create_hypertable`, `set_integer_now_func`, `add_compression_policy`) та тригерів (append-only — `ctb_forbid_mutation()`) — допиши через `op.execute`.
5. Типи: час — `BIGINT` мс (`TimeMs`/`CreatedMs` з `app/core/db.py`); гроші — `NUMERIC` (`Money`); статуси — `TEXT + CHECK` (`enum_check()`). Назви обмежень ≤ 63 символів (інакше PostgreSQL обріже; довгим — явне `name=`).
6. Для великих таблиць (`candles`, `fills`, `order_events`) — індекси під реальні запити; без блокуючих операцій на заповнених таблицях без пояснення.
7. Перевірка на локальній dev-БД:
   ```bash
   uv run alembic upgrade head && uv run alembic downgrade -1 && uv run alembic upgrade head
   uv run alembic check
   uv run pytest -q -m integration tests/db
   ```
8. Додай тест, що обмеження реально спрацьовує (вставка дубля → `IntegrityError`).
