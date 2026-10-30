# `app/trading` — Цикл бота, order manager, машина станів ордера

## Призначення

Статуси бота, цикл на закритті свічки, машина станів ордера (граф ТЗ §7.1, write-ahead `SUBMITTING`), розв'язання `SUBMISSION_UNKNOWN`, fills і облік капіталу, входи/виходи, перемикання моделі. Пауза, коли інструмент не торгується, — `instrument_status.py` (T1.7).

> Заплановано: T3.1, T3.3, T3.4, T5.1, T5.2, T5.3, T5.4. Зараз є моделі БД (T0.4) і пауза `INSTRUMENT_NOT_TRADING` (T1.7).

## Розділи ТЗ

- ТЗ §6 — торговий рушій
- ТЗ §7 — машина станів ордера та ідемпотентність

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `Bot`, `Signal`, `Order`, `OrderEvent`, `Fill`, `Position`, `DecisionLog` | `models.py` | таблиці торгового ядра |
| `BOT_STATUSES`, `PAUSE_REASONS`, `SIGNAL_STATUSES`, `ORDER_STATES`, `ORDER_TERMINAL_STATES`, `ORDER_ORIGINS`, `POSITION_STATUSES`, `PROTECTION_STATUSES`, `CLOCK_TOLERANCE_MS` | `models.py` | набори значень як у ТЗ §6.1, §6.3, §7.1, §9, §11.4; допуск годинника `CLOCK_TOLERANCE_MS` = `CLOCK_DRIFT_LIMIT_MS` (1000 мс, `app/core/clock.py`, §4.3; міграція 0001 має його літералом — зміна ліміту потребує нової міграції). Формат `clientOrderId` для CHECK — `CLIENT_ORDER_ID_RE` з `app/exchange/types.py` (T1.1) |
| `instrument_pause_transition`, `apply_instrument_pauses`, `INSTRUMENT_NOT_TRADING` | `instrument_status.py` | додати/зняти причину `INSTRUMENT_NOT_TRADING`; `RUNNING` ↔ `PAUSED` лише через неї; OCC за `bots.version` |

## Таблиці БД

- `bots` — `account_id` + `mode` (складений FK на `exchange_accounts (id, mode)`), `symbol`, `timeframe`, `status`
  (`STOPPED`, `STARTING`, `RUNNING`, `PAUSED`, `RECONCILING`, `ERROR`, `EMERGENCY_STOP`), `pause_reasons TEXT[]`
  (лише коди ТЗ §6.1), `model_version`, `pending_model_version` (FK на `models`, різні), `config`, `version` (OCC).
  Частковий **`UNIQUE (account_id, symbol) WHERE status <> 'STOPPED'`**; `PAUSED` має ≥ 1 причину, `RUNNING` — жодної
  (інші статуси можуть зберігати причини). Тригер `ctb_bots_guard()`: `account_id`, `mode`, `symbol` незмінні;
  перехід у `STOPPED` відхиляється, поки в бота є `OPEN`-позиція або нетермінальні ордери (ТЗ §6.2).
  Тригер `ctb_bot_children_guard()` на вставці ордера (і `UPDATE OF bot_id`) та позиції бере `FOR SHARE` на рядок
  бота: паралельна зупинка чекає на коміт вставки й бачить її (тест `tests/db/test_concurrency.py`). Вхідний ордер
  для `STOPPED` бота відхиляється; виходи (`reduce_only`) і позиції — ні.
  Попередній статус після `RECONCILING` не зберігається окремо (визначає T5.1).
  Пауза інструмента (T1.7, `instrument_status.py`): неторговий статус додає `INSTRUMENT_NOT_TRADING` і з `RUNNING`
  переводить у `PAUSED`; інші статуси (`EMERGENCY_STOP`, `ERROR`, `STOPPED`, `STARTING`, `RECONCILING`, уже `PAUSED`)
  лишаються, причина лише дописується. Повернення в `Trading` знімає лише цю причину; `PAUSED` без причин, що
  лишились, стає `RUNNING`. SL не знімається. T5.1 не має виходити в `RUNNING`, поки причина стоїть
  (CHECK `running_has_no_reason`), а стартовий `refresh` має йти лише після переведення ботів у `STARTING`:
  інакше модуль поверне `PAUSED` лише з цією причиною в `RUNNING` до стартової звірки (ТЗ §11.1). Лог `instrument_not_trading` / `instrument_trading_resumed`; Telegram — T5.6.
- `signals` — **`UNIQUE (bot_id, entry_candle_open_time_ms)`**; `features_candle_open_time_ms <
  entry_candle_open_time_ms` (look-ahead), `computed_at_ms ≥ entry_candle_open_time_ms − 1000` (допуск годинника); `primary_signal`
  `LONG`/`SHORT`, `prediction` `LONG`/`SHORT`/`NONE`, `probability` ∈ [0, 1], `expected_edge_atr`, `decision`
  `ENTER`/`SKIP`, `status` `NEW`/`EXECUTING`/`DONE`/`EXPIRED_UNEXECUTED`.
- `orders` — **`client_order_id UNIQUE`** у форматі `^tb[a-z0-9]{2}[0-9a-f]{32}$` (порожній лише для
  `origin = 'EXCHANGE'`); `state` — 10 станів ТЗ §7.1;
  `reduce_only`, `qty > 0`, `0 ≤ filled_qty ≤ qty` (кеш суми `fills`), `price` для `LIMIT`; `UNIQUE (account_id,
  exchange_order_id)`. Частковий **`UNIQUE (signal_id) WHERE NOT reduce_only`** — один вхід на сигнал (виходів з тим
  самим сигналом може бути кілька). `origin`:
  - `BOT` — цикл бота; вхід (не `reduce_only`) — лише з сигналом, у **кожному** режимі;
  - `MANUAL_TEST` — `[Test Long]`/`[Test Short]` (ТЗ §16), лише не в Live, без сигналу;
  - `ORPHAN` — ордер з нашим `HOST_ID`, знайдений при звірці (ТЗ §11.4); `bot_id` може бути порожнім;
  - `EXCHANGE` — створений біржею (спрацювання TP/SL позиції, ліквідація, ADL): без `clientOrderId`, але його
    fills записуються (PnL — лише з `fills`).
  `ORPHAN`/`EXCHANGE` — без сигналу, з `exchange_order_id` і ніколи не в `CREATED`/`SUBMITTING`.
  Тригер `ctb_orders_guard()`: ідентичність ордера незмінна (акаунт, режим, `origin`, символ, сторона, тип,
  `reduce_only`, `qty` — у `EXCHANGE` лише зростає; `client_order_id`, `signal_id`; `bot_id` і
  `exchange_order_id` — після заповнення), з
  термінального стану переходів немає, DELETE/TRUNCATE заборонені; `filled_qty`, `position_id`, `price` змінні.
  Обмежень на поєднання стану й `filled_qty` немає: fill з WS може прийти раніше за відповідь REST.
- `order_events` — **append-only** (тригер): `from_state` (NULL лише для першого запису), `to_state`, `source`
  `WS`/`REST`/`LOCAL`, `reason`, `payload`, `ts_ms`. Граф переходів у БД не дублюється — T3.1 (`order_state.py`).
- `fills` — **`exec_id UNIQUE`** (дедуплікація WS/REST, `ON CONFLICT DO NOTHING`), `price > 0`, `qty > 0`, `fee`
  (може бути від'ємною), `fee_currency`, `is_maker`, `ts_ms`, `mode`; FK `(order_id, account_id, mode, symbol, side)`
  — символ і сторона fill дорівнюють ордеру. Fill на ордер у `SUBMITTING` чи термінальному стані дозволений.
- `positions` — `side` `LONG`/`SHORT`, `status` `OPEN`/`CLOSED`/`CLOSED_EXTERNALLY`, `protection_status`
  `ENTRY_FILLED_UNPROTECTED`/`PROTECTED` (обов'язковий для `OPEN`; для `ENTRY_FILLED_UNPROTECTED` потрібен
  `unprotected_since_ms`), SL/TP і тригери, `entry_atr`, `max_hold_bars`, `realized_pnl`, `version`. Частковий
  **`UNIQUE (account_id, symbol) WHERE status = 'OPEN'`** (one-way mode); `closed_at_ms` заданий ⇔ не `OPEN`
  (порядок `opened_at_ms`/`closed_at_ms` не перевіряється: час відкриття може бути локальним, закриття — біржовим).
  Відкриту позицію видалити не можна (`ctb_positions_guard()`).
- `decision_log` — `stage` `SIGNAL`/`RISK`/`ORDER`/`RESULT`, `decision`, `reason_code`, `probability`,
  `expected_edge_atr`, `slippage_atr`, `details`, `ts_ms`; сигнал і ордер — того самого бота (складені FK).
- Узгодженість: складені FK `(bot_id, account_id, mode) → bots`, `(signal_id, bot_id) → signals`,
  `(order_id, account_id, mode, symbol, side) → orders`, `(position_id, account_id, mode) → positions` — акаунт і
  режим не можуть розійтися; `signal_id` без `bot_id` заборонений (інакше FK з NULL не перевірявся б). Символ у
  всіх таблицях — канонічний CCXT (`app.core.db.SYMBOL_RE`, напр. `BTC/USDT:USDT`).

Схема створюється міграцією `migrations/versions/0001_initial_schema.py`; обмеження й тригери перевіряють `tests/db/test_constraints.py`, `tests/db/test_guards.py` і `tests/db/test_migrations.py`.


## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.4 | `[x]` | таблиці `bots`, `signals`, `orders`, `order_events`, `fills`, `positions`, `decision_log` |
| T1.7 | `[x]` | пауза `INSTRUMENT_NOT_TRADING`: `RUNNING` ↔ `PAUSED`, інші статуси не знімаються |
| T3.1 | `[ ]` | Машина станів ордера |
| T3.3 | `[ ]` | Розв'язання `SUBMISSION_UNKNOWN` |
| T3.4 | `[ ]` | Fills, `ledger`, `equity_snapshots` |
| T5.1 | `[ ]` | Статуси бота, OCC, блокування |
| T5.2 | `[ ]` | Цикл бота на закритті свічки |
| T5.3 | `[ ]` | Входи, виходи, time-stop |
| T5.4 | `[ ]` | Перемикання моделі |
