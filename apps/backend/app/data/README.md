# `app/data` — Свічки, дані деривативів, колектор, прогалини, час

## Призначення

Завантаження та зберігання закритих свічок для торгового символу і `research_symbols`, даних деривативів і
ліквідацій (окремий процес `python -m app.data.collector`), виявлення прогалин без інтерполяції, контроль
синхронізації часу.

> Заплановано: T1.6, T1.9, T1.10. Backfill свічок 1h і funding — T1.4 (`backfill.py`). Прогалини — T1.5 (`gaps.py`).

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
| `GAP_SOURCES`, `PRICE_TYPES`, `OPEN_GAP_INDEX` | `models.py` | джерела прогалин; `mark` / `index`; ім'я часткового UNIQUE відкритих прогалин |
| `backfill_symbol`, `sync_market_data`, `run_backfill_job` | `backfill.py` | докачати один символ; прохід старту/розкладу; виконати задачу воркера (збій біржі на символі — `BackfillIncomplete` після решти символів) |
| `enqueue_backfill_job`, `JOB_KIND_BACKFILL`, `BACKFILL_DEDUP_KEY` | `backfill.py` | поставити `jobs.kind = backfill_candles` (активна задача з цим ключем лише одна) |
| `MarketDataSync`, `MarketDataSync.from_settings` | `backfill.py` | перший `run_if_due` — старт, далі кожні `BACKFILL_INTERVAL_S` секунд за монотонним годинником; невдалий прохід — повтор через backoff `SYNC_RETRY_BASE_MS` (30 с), ×2 до інтервалу, jitter `[пауза / 2, пауза]` |
| `backfill_symbols`, `forming_open_ms`, `candles_closed_at`, `funding_known_at`, `backfill_is_due` | `backfill.py` | склад символів; межа закритої свічки; фільтр локального годинника; чи настав інтервал |
| `INLINE_MAX_BARS`, `CANDLE_FETCH_LIMIT`, `BACKFILL_TIMEFRAME`, `EXCHANGE` | `backfill.py` | тиждень годинних свічок у процесі, сторінка 1000, таймфрейм `1h`, біржа `bybit` |
| `FUNDING_WINDOW_MS`, `INSERT_CHUNK_ROWS`, `SYNC_RETRY_BASE_MS` | `backfill.py` | вікно funding за виклик (60 діб); рядків в одному INSERT (1000, ліміт asyncpg — 32767 параметрів); перша пауза повтору (30 с) |
| `BackfillStats`, `SyncResult`, `BackfillIncomplete` | `backfill.py` | скільки рядків додано; id задачі, символи, віддані воркеру, і `failed_symbols`; виняток задачі з `failed_symbols` і `stats` |
| `GapRange`, `find_gaps`, `window_has_gap`, `plan_open_gaps` | `gaps.py` | напіввідкритий інтервал; дірки сітки; чи вікно їх перетинає; зведення після скану |
| `detect_candle_gaps`, `sync_open_gaps`, `GapSync` | `gaps.py` | записати дірки свічок; звести відкриті рядки будь-якого `source` (його викличуть T1.9 і T1.10); що змінилось за прохід |
| `has_open_gap`, `open_gaps_in_window` | `gaps.py` | чи є відкрита прогалина у вікні `[start_ms, end_ms)` і які саме |
| `settled_end_ms`, `CANDLE_SETTLE_MS`, `CLOCK_DRIFT_LIMIT_MS` | `gaps.py` | кінець сітки, яку вже можна перевіряти; запас після закриття свічки (`KLINE_FINALITY_MARGIN_MS` 500 мс + допуск годинника 1 с = 1500 мс) |
| `is_open_gap_conflict` | `gaps.py` | чи `IntegrityError` — зіткнення двох процесів на `uq_data_gaps_open` (а не інша помилка цілісності) |
| `CANDLE_SOURCE`, `GAP_SYNC_ATTEMPTS` | `gaps.py`, `backfill.py` | `data_gaps.source` для свічок (`candles`); спроб звести ряд при зіткненні (2) |

Виклик при старті і за розкладом — `MarketDataSync.run_if_due` (торговий процес підключить його разом із циклом
бота, T5). Довша за `INLINE_MAX_BARS` (168 годин) дірка свічок або funding не качається в цьому процесі: створюється
задача `backfill_candles`, її виконує воркер (T1.8) викликом `run_backfill_job`. Хвіст ряду не пишеться раніше за
початок — курсор це `max(час)` у БД. Після проходу свічок (і для символу, який цей прохід не качав) викликається
`detect_candle_gaps` по сітці до `settled_end_ms(now)`: дірки пишуться в `data_gaps`, значення між свічками не
добудовуються. Курсор за діркою позаду не повертається, тож внутрішню дірку цей прохід не закриє — рядок лишається
відкритим, доки дані не з'являться в БД і детектор не пройде знов. Зіткнення з іншим процесом на тому самому ряді
прогалин (`uq_data_gaps_open`) повторюється один раз; якщо й повтор зіткнувся — лог `data_gap_sync_conflict`, ряд
пропускається до наступного проходу, решта символів і funding докачуються.

Свічки йдуть сторінками по 1000, funding — вікнами `FUNDING_WINDOW_MS`; кожна сторінка чи вікно комітиться окремо,
тож обірване завантаження продовжується з курсора. Задача воркера ставиться в одній транзакції з читанням курсорів,
до мережі. Збій біржі (`ExchangeError`) на одному символі не зупиняє інші: прохід розкладу повертає його в
`SyncResult.failed_symbols`, задача воркера докачує решту і кидає `BackfillIncomplete`. Інші винятки (БД, помилка в
коді) летять одразу.

Параметри задачі: `symbols`, `timeframe` (`1h`), `start_ms`. Кінець діапазону — годинник у момент виконання.
Поки задача `QUEUED` або `RUNNING`, друга з тим самим `dedup_key` не створюється; її параметри не переписуються.

Конфіг: `TRADING_SYMBOL`, `RESEARCH_SYMBOLS`, `BACKFILL_START_MS`, `BACKFILL_INTERVAL_S` (`app/core/config.py`).

## Прогалини (T1.5, ТЗ §4.2)

Інтервал і вікно — напіввідкриті `[from_ms, to_ms)` / `[start_ms, end_ms)`. Дотик межею не є перетином: дірка, що
закінчується рівно на `start`, і дірка, що починається рівно на `end`, у вікно не входять. Пропуск першого або
останнього слота сітки — входить.

`find_gaps` порівнює наявні мітки з сіткою. Мітка не на кроці, до `start` або на `end` слот не закриває. Нічого не
інтерполюється. `detect_candle_gaps` читає `open_time_ms` свічок і пише результат через `sync_open_gaps`. Кожна дірка
бере відкритий рядок, який перекриває (спершу — з тією самою `from_ms`): id і `detected_at_ms` зберігаються, межі
оновлюються з будь-якого боку (дірка росте в хвості, воркер заповнює її з початку). Середину докачано — друга частина
стає новим рядком із часом виявлення батьківської. Заповнена дірка отримує `resolved_at_ms`; рядок, чия дірка злилась
з іншою (між ними зник слот), закривається як злитий. Повтор без змін нічого не пише і не логує.

Логи: `data_gap` (нова дірка) і `data_gap_extended` (пропущених слотів побільшало) — warning; `data_gap_shrunk`
(дірка лише скоротилась або розпалась), `data_gap_merged`, `data_gap_resolved` — info. Дірка, якої на біржі немає,
лишається відкритою. Закритий рядок не заважає новій дірці з тією самою `from_ms`. Якщо системний годинник
відкотився, `resolved_at_ms` не буває раніше за `detected_at_ms` (CHECK схеми не зупиняє прохід).

Остання свічка входить у сітку лише через `CANDLE_SETTLE_MS` (1500 мс) після закриття за локальним годинником
(`settled_end_ms`): Bybit-адаптер віддає її через `KLINE_FINALITY_MARGIN_MS` (500 мс) за часом біржі, а годинник може
відставати до `CLOCK_DRIFT_LIMIT_MS` (1 с, ТЗ §4.3). Раніше її відсутність — не прогалина, а ще не віддана свічка.

Період до лістингу символу зараз теж рахується прогалиною: скан починається з `BACKFILL_START_MS` (2020-01-01), і
рядок `[BACKFILL_START_MS, перша свічка)` лишається відкритим. Навчанню й live це не шкодить (даних там немає).
> Заплановано: T1.7 — початок скану `max(BACKFILL_START_MS, launchTime інструмента)`.

`has_open_gap` / `open_gaps_in_window` читають лише відкриті рядки. `False` означає «відкритого рядка в цьому вікні
немає», а не «ряд уже перевіряли»: перед тим, як трактувати його як дозвіл торгувати, вікно має пройти детектор
(backfill викликає його після проходу свічок). Інші джерела (`open_interest`, `mark_candles`, …) пишуть тим самим
`sync_open_gaps`; до T1.9/T1.10 їхніх рядків немає, і це не прогалина свічок. Два процеси, що зводять один ряд
одночасно, можуть зіткнутися на `uq_data_gaps_open` (`IntegrityError`): транзакція відкочується, а виклик розпізнає
це через `is_open_gap_conflict` і повторює (як `_record_candle_gaps` у backfill), не зупиняючи інші ряди.

## Таблиці БД

- `candles` — hypertable, PK `(exchange, symbol, timeframe, open_time_ms)`; `open/high/low/close/volume` — `NUMERIC`;
  лише закриті свічки (`CHECK (is_closed)`); `high ≥ max(open, close)`, `low ≤ min(open, close)`, `low > 0`,
  `volume ≥ 0`. Chunk — 7 днів, **compression після 7 днів** (`segmentby` = exchange, symbol, timeframe), retention
  немає (ТЗ §4.1). Вставка — `ON CONFLICT DO NOTHING`.
- `data_gaps` — `[from_ms, to_ms)` з `source` (`candles`, `open_interest`, `premium_index`, `mark_candles`,
  `index_candles`, `long_short`, `liquidations`; mark і index — окремі ряди з окремими прогалинами),
  `detected_at_ms`, `resolved_at_ms` (дані докачано й перевірено). Частковий `UNIQUE (source, exchange, symbol,
  timeframe, from_ms) WHERE resolved_at_ms IS NULL` (`uq_data_gaps_open`): відкрита прогалина не дублюється
  (рядок зберігає id і `detected_at_ms`, межі оновлюються, T1.5), а закрита не заважає зареєструвати нову.
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
| T1.4 | `[x]` | інкрементальний backfill закритих свічок 1h і funding; довга історія — задача `backfill_candles`; після проходу — детектор прогалин |
| T1.5 | `[x]` | детектор прогалин свічок → `data_gaps`; запит вікна `[start, end)`; без інтерполяції; запас остаточності останньої свічки; повтор при зіткненні процесів |
| T1.6 | `[ ]` | Синхронізація та стрибки часу |
| T1.9 | `[ ]` | Backfill даних деривативів |
| T1.10 | `[ ]` | Колектор ліквідацій |
