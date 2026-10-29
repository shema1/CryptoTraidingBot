# `app/data` — Свічки, дані деривативів, колектор, прогалини, час

## Призначення

Завантаження та зберігання закритих свічок для торгового символу і `research_symbols`, даних деривативів і
ліквідацій (окремий процес `python -m app.data.collector`), виявлення прогалин без інтерполяції, контроль
синхронізації часу.

> Заплановано: T1.5, T1.6, T1.9, T1.10. Backfill свічок 1h і funding — T1.4 (`backfill.py`).

## Розділи ТЗ

- ТЗ §4.1 — зберігання свічок
- ТЗ §4.2 — політика пропусків
- ТЗ §4.3 — час (UTC, мс)
- ТЗ §4.4 — кілька символів і дані деривативів
- ТЗ §6.5 — backfill довгої історії не сидить на частці ліміту торгового процесу

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `Candle`, `DataGap`, `FundingRate` | `models.py` | `candles`, `data_gaps`, `funding_rates` |
| `OpenInterest`, `PremiumIndex`, `MarkIndexCandle`, `LongShortRatio`, `LiquidationsHourly` | `models.py` | таблиці деривативів (ТЗ §4.4) |
| `GAP_SOURCES`, `PRICE_TYPES` | `models.py` | джерела прогалин; `mark` / `index` |
| `backfill_symbol`, `sync_market_data`, `run_backfill_job` | `backfill.py` | докачати один символ; прохід старту/розкладу; виконати задачу воркера (збій біржі на символі — `BackfillIncomplete` після решти символів) |
| `enqueue_backfill_job`, `JOB_KIND_BACKFILL`, `BACKFILL_DEDUP_KEY` | `backfill.py` | поставити `jobs.kind = backfill_candles` (активна задача з цим ключем лише одна) |
| `MarketDataSync`, `MarketDataSync.from_settings` | `backfill.py` | перший `run_if_due` — старт, далі кожні `BACKFILL_INTERVAL_S` секунд за монотонним годинником; невдалий прохід — повтор через backoff `SYNC_RETRY_BASE_MS` (30 с), ×2 до інтервалу, jitter `[пауза / 2, пауза]` |
| `backfill_symbols`, `forming_open_ms`, `candles_closed_at`, `funding_known_at`, `backfill_is_due` | `backfill.py` | склад символів; межа закритої свічки; фільтр локального годинника; чи настав інтервал |
| `INLINE_MAX_BARS`, `CANDLE_FETCH_LIMIT`, `BACKFILL_TIMEFRAME`, `EXCHANGE` | `backfill.py` | тиждень годинних свічок у процесі, сторінка 1000, таймфрейм `1h`, біржа `bybit` |
| `FUNDING_WINDOW_MS`, `INSERT_CHUNK_ROWS`, `SYNC_RETRY_BASE_MS` | `backfill.py` | вікно funding за виклик (60 діб); рядків в одному INSERT (1000, ліміт asyncpg — 32767 параметрів); перша пауза повтору (30 с) |
| `BackfillStats`, `SyncResult`, `BackfillIncomplete` | `backfill.py` | скільки рядків додано; id задачі, символи, віддані воркеру, і `failed_symbols`; виняток задачі з `failed_symbols` і `stats` |

Виклик при старті і за розкладом — `MarketDataSync.run_if_due` (торговий процес підключить його разом із циклом
бота, T5). Довша за `INLINE_MAX_BARS` (168 годин) дірка свічок або funding не качається в цьому процесі: створюється
задача `backfill_candles`, її виконує воркер (T1.8) викликом `run_backfill_job`. Хвіст ряду не пишеться раніше за
початок — курсор це `max(час)` у БД. Прогалини цей модуль не заповнює і не записує в `data_gaps` (T1.5).

Свічки йдуть сторінками по 1000, funding — вікнами `FUNDING_WINDOW_MS`; кожна сторінка чи вікно комітиться окремо,
тож обірване завантаження продовжується з курсора. Задача воркера ставиться в одній транзакції з читанням курсорів,
до мережі. Збій біржі (`ExchangeError`) на одному символі не зупиняє інші: прохід розкладу повертає його в
`SyncResult.failed_symbols`, задача воркера докачує решту і кидає `BackfillIncomplete`. Інші винятки (БД, помилка в
коді) летять одразу.

Параметри задачі: `symbols`, `timeframe` (`1h`), `start_ms`. Кінець діапазону — годинник у момент виконання.
Поки задача `QUEUED` або `RUNNING`, друга з тим самим `dedup_key` не створюється; її параметри не переписуються.

Конфіг: `TRADING_SYMBOL`, `RESEARCH_SYMBOLS`, `BACKFILL_START_MS`, `BACKFILL_INTERVAL_S` (`app/core/config.py`).

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
| T1.4 | `[x]` | інкрементальний backfill закритих свічок 1h і funding; довга історія — задача `backfill_candles` |
| T1.5 | `[ ]` | Виявлення прогалин |
| T1.6 | `[ ]` | Синхронізація та стрибки часу |
| T1.9 | `[ ]` | Backfill даних деривативів |
| T1.10 | `[ ]` | Колектор ліквідацій |
