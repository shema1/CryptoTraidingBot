---
description: Правила для моделей БД, міграцій Alembic і TimescaleDB
paths:
  - "apps/backend/migrations/**"
  - "apps/backend/app/**/models.py"
  - "apps/backend/app/**/repository.py"
  - "apps/backend/app/core/db*.py"
  - "docker-compose.yml"
---

# База даних (ТЗ §4.1, §15)

- Міграції — лише Alembic. **Застосовану міграцію не редагувати** — тільки нова. Кожна міграція має `downgrade()` (або явне пояснення, чому неможливо).
- Інваріанти — на рівні БД, не лише в коді: `UNIQUE`, `CHECK`, `FOREIGN KEY`, часткові унікальні індекси. Обов'язкові:
  - `orders.client_order_id UNIQUE`; вхідний ордер `UNIQUE(signal_id)`;
  - `signals UNIQUE(bot_id, entry_candle_open_time_ms)`;
  - `fills.exec_id UNIQUE`;
  - `bots`: частковий `UNIQUE(account_id, symbol) WHERE status <> 'STOPPED'`;
  - `candles` PK `(exchange, symbol, timeframe, open_time_ms)`.
- Час — `BIGINT` мілісекунди UTC (єдиний формат у проєкті). Не змішувати з `timestamptz`.
- Гроші/ціни/кількості — `NUMERIC`, не `double precision`.
- `order_events`, `audit_log`, `ledger` — append-only (без UPDATE/DELETE у коді; бажано тригер або REVOKE).
- Статуси — `TEXT` + `CHECK` або PostgreSQL enum; набір значень — як у ТЗ.
- Вставка свічок — `ON CONFLICT DO NOTHING`; зберігаються лише закриті свічки. Compression після 7 днів, retention не застосовується.
- Поля `mode` (demo/testnet/live) у таблицях ордерів/позицій/fills обов'язкові.
- Дані деривативів (`open_interest`, `premium_index`, `mark_index_candles`, `long_short_ratio`, `liquidations_hourly`) — hypertables з PK `(exchange, symbol, interval, ts_ms)`, вставка `ON CONFLICT DO NOTHING`.
- `account_id` обов'язковий у `equity_snapshots`, `ledger`, `risk_config`, `risk_events`, `system_state`, `emergency_runs`: ризик ведеться окремо для кожного акаунта.
- `experiment_trials` — append-only: спроби не видаляються і не перезаписуються (від цього залежить коректність DSR/PBO).
- Транзакції — короткі; мережеві виклики до біржі всередині транзакції заборонені.
- Тести міграцій: `upgrade head` на порожній БД + `downgrade -1` + `upgrade head`.
