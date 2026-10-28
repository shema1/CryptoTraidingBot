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
3. Створи **нову** міграцію: `uv run alembic revision --autogenerate -m "<коротко>"`. Існуючі застосовані міграції не редагуй (hook це блокує).
4. Перевір згенерований файл вручну: autogenerate не бачить часткових індексів, `CHECK`, TimescaleDB-функцій (`create_hypertable`, `add_compression_policy`) — допиши через `op.execute`.
5. Типи: час — `BIGINT` мс; гроші — `NUMERIC`; статуси — `TEXT + CHECK` або enum.
6. Для великих таблиць (`candles`, `fills`, `order_events`) — індекси під реальні запити; без блокуючих операцій на заповнених таблицях без пояснення.
7. Перевірка на локальній dev-БД:
   ```bash
   uv run alembic upgrade head && uv run alembic downgrade -1 && uv run alembic upgrade head
   uv run pytest -q -m integration tests/db
   ```
8. Додай тест, що обмеження реально спрацьовує (вставка дубля → `IntegrityError`).
