# `app/data` — Свічки, дані деривативів, колектор, прогалини, час

## Призначення

Завантаження та зберігання закритих свічок для `research_symbols`, даних деривативів і ліквідацій (окремий процес `python -m app.data.collector`), виявлення прогалин без інтерполяції, контроль синхронізації часу.

> Заплановано: T1.4, T1.5, T1.6, T1.9, T1.10. Зараз є лише моделі БД (T0.4).

## Розділи ТЗ

- ТЗ §4.1 — зберігання свічок
- ТЗ §4.2 — політика пропусків
- ТЗ §4.3 — час (UTC, мс)
- ТЗ §4.4 — кілька символів і дані деривативів

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `Candle`, `DataGap`, `FundingRate` | `models.py` | `candles`, `data_gaps`, `funding_rates` |
| `OpenInterest`, `PremiumIndex`, `MarkIndexCandle`, `LongShortRatio`, `LiquidationsHourly` | `models.py` | таблиці деривативів (ТЗ §4.4) |
| `GAP_SOURCES`, `PRICE_TYPES` | `models.py` | джерела прогалин; `mark` / `index` |

## Таблиці БД

- `candles` — hypertable, PK `(exchange, symbol, timeframe, open_time_ms)`; `open/high/low/close/volume` — `NUMERIC`;
  лише закриті свічки (`CHECK (is_closed)`); `high ≥ max(open, close)`, `low ≤ min(open, close)`, `low > 0`,
  `volume ≥ 0`. Chunk — 7 днів, **compression після 7 днів** (`segmentby` = exchange, symbol, timeframe), retention
  немає (ТЗ §4.1). Вставка — `ON CONFLICT DO NOTHING`.
- `data_gaps` — `[from_ms, to_ms)` з `source` (`candles`, `open_interest`, `premium_index`, `mark_candles`,
  `index_candles`, `long_short`, `liquidations`; mark і index — окремі ряди з окремими прогалинами),
  `detected_at_ms`, `resolved_at_ms` (дані докачано й перевірено). Частковий `UNIQUE (source, exchange, symbol,
  timeframe, from_ms) WHERE resolved_at_ms IS NULL` (`uq_data_gaps_open`): відкрита прогалина не дублюється
  (росте upsert-ом `to_ms`, T1.5), а закрита не заважає зареєструвати нову.
- `funding_rates` — PK `(exchange, symbol, funding_time_ms)`, `rate` (може бути від'ємним).
- Деривативи — hypertables (chunk 30 днів, без compression), PK `(exchange, symbol, interval, ts_ms)`:
  `open_interest` (`open_interest ≥ 0`), `premium_index` (OHLC, можуть бути від'ємними), `long_short_ratio`
  (`buy_ratio`, `sell_ratio` ∈ [0, 1]), `liquidations_hourly` (`long_/short_` `count`, `qty` у базовій валюті,
  `notional` у USDT, усе ≥ 0; `ts_ms` — початок години). `mark_index_candles` — PK
  `(exchange, symbol, price_type, interval, ts_ms)`, `price_type` = `mark` / `index` (`docs/decisions/0005-schema-t0-4.md`).
- Таймфрейм/інтервал — `^[0-9]+(m|min|h|d|w)$`; символ — канонічний CCXT (`app.core.db.SYMBOL_RE`).

Схема створюється міграцією `migrations/versions/0001_initial_schema.py`; обмеження й тригери перевіряють `tests/db/test_constraints.py`, `tests/db/test_guards.py` і `tests/db/test_migrations.py`.


## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.4 | `[x]` | таблиці свічок, прогалин, funding і деривативів; hypertables, compression `candles` |
| T1.4 | `[ ]` | Backfill свічок і funding |
| T1.5 | `[ ]` | Виявлення прогалин |
| T1.6 | `[ ]` | Синхронізація та стрибки часу |
| T1.9 | `[ ]` | Backfill даних деривативів |
| T1.10 | `[ ]` | Колектор ліквідацій |
