# `app/risk` — Risk Engine: модель витрат, розмір позиції, ліміти, RiskGate

## Призначення

`costs.py`, `sizing.py` (`calc_position_size()`), `limits.py` (чисті функції лімітів) — спільні з бектестом; `RiskGate` з veto-правом на будь-який ордер, що збільшує експозицію; статуси Risk Engine окремо для кожного акаунта.

Чисті функції `costs.py`, `sizing.py` і `limits.py` уже є (T2.2). `RiskGate`, запис у `risk_events` і статуси рахунку — T4.1, T4.2.

> Заплановано: T4.1, T4.2.

## Розділи ТЗ

- ТЗ §5.2.2 — модель витрат
- ТЗ §8 — Risk Engine

## Публічний інтерфейс

Бектест і `RiskGate` викликають `assess_entry`, а не копію формул. Імена з таблиці нижче (крім `models.py`) реекспортує `app.risk`; окремі перевірки — з `app.risk.limits`.

| Що | Де | Опис |
| --- | --- | --- |
| `CostModel`, `PriceExecution`, `FundingEvent`, `funding_payment`, `funding_over_hold`, `entry_is_stale` | `costs.py` | комісії, slippage, funding, вік сигналу |
| `calc_position_size`, `assess_entry`, `PositionSize`, `EntryDecision`, `sizing_equity`, `isolated_liq_price` | `sizing.py` | розмір входу і оцінка ліквідації |
| `RiskLimits`, `check_entry_limits`, `check_time_stop`, `next_peak_equity`, `adjust_equity_anchors`, `apply_cashflows` | `limits.py` | ліміти, пік і корекція якорів капіталу |
| `RiskReason` | `reasons.py` | коди відмови; запис у журнал — T4.1 |
| `RiskConfig`, `RiskEvent`, `SystemState`, `EmergencyRun`, `EmergencyStep`, `EquitySnapshot`, `LedgerEntry` | `models.py` | таблиці ризику, Emergency Stop і обліку капіталу |
| `RISK_STATUSES`, `EMERGENCY_TRIGGERS`, `EMERGENCY_RUN_STATUSES`, `LEDGER_KINDS` | `models.py` | набори значень |

Окремі перевірки (`check_min_equity`, `check_drawdown`, `check_daily_loss`, `check_max_positions`, `check_max_trades`, `check_cooldown`, `check_entry_slippage`, `check_funding_rate`, `next_consecutive_losses`) лишаються публічними: бектест і `RiskGate` викликають їх і поза складеним входом (time-stop, серія збитків, котирування проти ціни сигналу).

## Формули

`sizing_equity = min(wallet_balance, total_equity)`. Ризик угоди, плече і кімната експозиції рахуються з нього: плаваючий прибуток розмір не збільшує, плаваючий збиток зменшує. Денний збиток і просадка — з `total_equity`, не з `sizing_equity`.

Опорна ціна входу (до slippage) — `open[t]` у бектесті і котирування на момент відправки в live; SL/TP рахуються від неї ж (ТЗ §6.6). Якщо live передасть ціну сигналу, а відправить за котируванням, різниця між ними (до `max_slippage_bps`) у бюджет ризику не потрапить: при вузькому стопі це десятки відсотків понад `risk_usd`. Опорні ціни округлюються до тіка до розрахунку ризику. Market-вхід — проти угоди (лонг угору, шорт униз). Стоп і ціль — ролями `PriceRole`, які не збільшують ризик. Рівень, що не округлюється (менший за тік), — код своєї ноги: вхід `RISK_PRICE_INVALID`, стоп `RISK_STOP_INVALID`, ціль `RISK_TARGET_INVALID`.

Публічні функції рахують у власному контексті `Decimal` (`fixed_context` у `numbers.py`: prec 28, `ROUND_HALF_EVEN`), а не в контексті потоку. CCXT у торговому процесі міняє контекст потоку (`ROUND_HALF_UP`, пастка `Underflow`), воркер бектесту — ні; без цього той самий вхід міг би дати різний розмір.

```
loss_per_unit = |entry_fill − stop_fill| + комісія входу + taker-комісія виходу
risk_usd      = sizing_equity * risk_per_trade_pct
qty_risk      = risk_usd / loss_per_unit                         (ділення вниз)
notional_price = max(entry_price, entry_fill)
lot_price     = min(entry_price, entry_fill)
qty_lev       = sizing_equity * leverage / notional_price
qty_expo      = (sizing_equity * max_total_exposure_pct − current_notional) / notional_price
qty_symbol    = max_notional_per_symbol_usd / notional_price
qty           = round_entry_qty(min(...), price=lot_price)       (униз до qtyStep)
```

`loss_per_unit` включає slippage входу і slippage стопа. Інакше збиток при спрацюванні SL вийшов би більшим за `risk_usd`. Геп через стоп рахує симулятор (T2.3), не модель витрат.

Дільник `qty_lev`, `qty_expo` і стелі номіналу — більше з округленої опорної ціни і ціни виконання. У шорта fill нижчий за опору: ділення на нього дало б більше контрактів, ніж дозволяє ліміт у ціні сигналу. Мінімальний номінал біржі — навпаки, за меншою з двох цін: у лонга fill вищий, і лот, що проходить за fill, біржа за опорною ціною відхилила б. Ліквідація й далі рахується від fill.

`min_equity_usd` у розмірі перевіряється за `sizing_equity`: якщо гаманець уже нижче порогу, плаваючий прибуток вхід не рятує. `check_entry_limits` окремо перевіряє той самий поріг за `total_equity`.

Не додатна ціна виконання після slippage — відмова з кодом тієї ноги, що зламалась: ціль — `RISK_TARGET_INVALID`, стоп лонга — `RISK_STOP_INVALID`, вхід шорта — `RISK_PRICE_INVALID`.

Плече — менше з конфігу та `instrument.max_leverage`, вниз до `LEVERAGE_STEP` `0.01` (`TODO(verify-bybit)`, T3.7), і ще нижче, якщо інакше ліквідація ближче ніж `min_liq_distance_sl_multiple` відстаней від ціни виконання входу до тригера стопа. Пошук іде по відрізку: на 1× мінімальний лот може не вміститись, а між 1× і стелею безпечне плече ще є. Відмова `RISK_LIQ_DISTANCE` — лише коли жодне таке плече лот не тримає. На біржу виставляється повернене `PositionSize.leverage`: менша кількість при вищому плечі ліквідацію не відсуває, коли MM deduction = 0. Якщо лот не вміщається навіть на стелі плеча — причина округлення (`RISK_QTY_ZERO`, `RISK_BELOW_MIN_QTY`, `RISK_BELOW_MIN_NOTIONAL`), лот угору не піднімається. Якщо лот є, а безпечного плеча немає — `RISK_LIQ_DISTANCE`.

Оцінка ліквідації — формула Bybit UTA для ізольованого USDT-перпетуала після 2025-09-02 (Help Center, Liquidation Process). Extra margin за замовчуванням 0. MMR і `mm_deduction` передає викликач (тир 1 — deduction 0); у коді їх немає. Комісію закриття формула покриває не повністю; запас — множник відстані до стопа. При deduction > 0 менша кількість безпечніша або така сама: спершу шукається найвище плече, на якому мінімальний лот ще проходить, потім кількість зменшується. Це безпечно, але не максимум розміру: кількість може вийти в рази меншою, ніж дозволило б нижче плече. Розрахунок розміру розрахований на тир 1 (deduction 0); номінал вище за межу тиру 1 — справа `RiskGate` (T4.1).

Порядок `check_entry_limits`: просадка, денний збиток, мінімальний капітал (блок лише строго нижче), позиція бота (`bot_open_positions`, не більше однієї, ТЗ §5.2.3), позиції акаунта (`open_positions`, стеля `max_open_positions`), угоди за добу, cooldown, funding. Найважча причина — першою: просадка раніше за день (з неї випливає `HALTED`), обидві раніше за поріг капіталу, щоб він їх не маскував. Статус рахунку (`HALTED`, `PAUSED` з `DAILY_LOSS`) з причини відмови входу не виводиться: його визначають `check_drawdown` і `check_daily_loss` на кожному оновленні капіталу (T4.2). Обидва ліміти позицій — `RISK_MAX_POSITIONS`; позицій бота більше, ніж акаунта, — `ValueError`. Рівність на ліміті просадки, дня, позицій, угод і серії збитків — уже блок. Cooldown: бар останнього збитку має відлік 0; рівно `cooldown_bars` пауза скінчилась. Пауза серію не скидає (скидає лише прибуткова угода), тож кожен наступний збиток після паузи знову дає `cooldown_bars` барів паузи.

Пік: total equity вище за переданий пік — нова вершина, якої знімок ще не записав, просадка нульова, а не помилка. Збережений пік від цього сам не росте: бектест і `RiskGate` оновлюють його `next_peak_equity(peak, total_equity)` на кожному капіталі, з яким приймають рішення (бектест — на кожному барі з нереалізованим PnL), і зберігають. Інакше падіння після зростання рахувалось би від старого піку: пік 1000, зростання до 1500, падіння до 1100 — без оновлення просадки «немає», а справжня — 26,7%.

Time-stop закриває, коли `now >= opened + max_hold_bars * bar_ms` (вихід за `open[t+horizon]`). `opened_at_ms` — час відкриття свічки входу, не fill: інакше межа зсунулась би на затримку входу, і цикл на `open[t+horizon]` пропустив би time-stop до наступного бару. `ValueError` (`now` раніше за відкриття) на шляху виходу не означає «не закривати»: перевірка повторюється на наступному тіку.

Funding за утримання (`funding_over_hold`) — події з `opened_at_ms < time <= closed_at_ms`. Мітки funding Bybit стоять рівно на відкритті години, а ордер виконується вже після відкриття свічки: подія на свічці входу не платиться, на свічці виходу (time-stop за `open[t+horizon]`, SL/TP у свічці, що почалась з мітки) — платиться. Симулятор передає час відкриття свічок входу і виходу; фактичний час fill дає той самий результат.

`check_entry_slippage` не входить у `assess_entry`: у live порівнюють котирування з ціною сигналу, у бектесті виконання задає модель. Блок лише коли відхилення в bps строго більше за ліміт. Funding у складеній перевірці лише якщо `max_funding_rate_abs` заданий; ставка або бік відсутні — `RISK_FUNDING` (fail-closed). Лонг платить при додатній ставці, шорт — при від'ємній; рівно ліміт ще дозволено.

Депозит і виведення (`adjust_equity_anchors`) множать пік і початок дня на `equity_after / (equity_after − cashflow)`, тож відсотки просадки і денного результату не змінюються. Обидва якорі округлюються вгору (вищий пік і вищий початок дня не занижують ні просадку, ні денний збиток); пік не опускається нижче нового equity. Пік нижче капіталу до потоку — нова вершина між знімками: масштабується сам капітал до потоку. Приклад: капітал 90, пік 100, початок дня 100, депозит 10 → equity 100, просадка лишається 10%, а не нуль.

Зіпсовані входи (від'ємний капітал, капітал до потоку ≤ 0, початок дня ≤ 0, `now` раніше за відкриття, позицій бота більше, ніж акаунта) — `ValueError`. T4.1 і T4.2 трактують виняток як fail-closed: нового входу немає.

## Значення за замовчуванням

Частки — часткою від 1 (`0.005` = 0,5%), плече — множником. З ТЗ §8.2 і `cost_model` §5.4, у бік більшого ризику не зрушені:

| Поле | Значення |
| --- | --- |
| `taker_fee_rate`, `maker_fee_rate` | `0.00055` (maker = taker: знижку біржі не вигадуємо; post-only у v1 вимкнено) |
| `slippage_entry_atr`, `slippage_sl_atr`, `slippage_tp_atr` | `0.05`, `None` → 2× входу, `0.05` |
| фіксовані bps slippage | `0` (у прикладі ТЗ їх немає; поля є) |
| `risk_per_trade_pct` | `0.005`, стеля `0.02` |
| `max_leverage` | `3` (задати можна до 100, дефолт лишається 3) |
| `max_open_positions`, `max_total_exposure_pct` | `3`, `1.5` |
| `daily_loss_limit_pct`, `max_drawdown_pct` | `0.02`, `0.10` |
| `max_consecutive_losses`, `cooldown_bars`, `max_trades_per_day` | `4`, `8`, `10` |
| `min_liq_distance_sl_multiple`, `max_slippage_bps` | `2` (строго більше 1: оцінка ліквідації не покриває комісію закриття), `15` |
| `min_equity_usd`, `max_notional_per_symbol_usd` | без дефолта (ТЗ: «задається») |
| `max_funding_rate_abs` | `None` — перевірка вимкнена |
| `DEFAULT_MAX_SIGNAL_AGE_MS` | `5000`; рівно 5000 мс сигнал ще свіжий |

`CostModel.scaled(factor)` лише збільшує витрати (`factor` ≥ 1). Множаться комісії і slippage, не сира відстань `|entry − stop|`. Стрес бектесту — `scaled(2)`.

## Коди причин

`RISK_MIN_EQUITY`, `RISK_MAX_DRAWDOWN`, `RISK_DAILY_LOSS`, `RISK_MAX_POSITIONS`, `RISK_MAX_TRADES_PER_DAY`, `RISK_COOLDOWN`, `RISK_FUNDING`, `RISK_MAX_SLIPPAGE`, `RISK_EXPOSURE`, `RISK_LIQ_DISTANCE`, `RISK_QTY_ZERO`, `RISK_BELOW_MIN_QTY`, `RISK_BELOW_MIN_NOTIONAL`, `RISK_PRICE_INVALID`, `RISK_STOP_INVALID`, `RISK_TARGET_INVALID`, `TIME_STOP`. Усі проходять CHECK `risk_events.reason_code`. `TIME_STOP` — не відмова входу, а сигнал закрити позицію.

## Таблиці БД

Усе — окремо для кожного акаунта (`account_id` обов'язковий, ТЗ §8.1).

- `risk_config` — версії лімітів, **append-only** (тригер): `UNIQUE (account_id, version)`, чинна — найбільша
  версія; `params` (JSONB, схема — `RiskLimits`, T2.2; запис і підтвердження послаблення — T4.1). `RiskLimits` строга
  (strict): писати `model_dump_json()` (Decimal — рядками, без float), читати `model_validate_json()`; `model_validate`
  на dict з JSONB відхиляє рядки і float. `loosening` → обов'язковий `confirmed_audit_id`
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
| T2.2 | `[x]` | модель витрат, `calc_position_size()`, чисті ліміти |
| T4.1 | `[ ]` | `RiskGate` і pre-trade перевірки |
| T4.2 | `[ ]` | Ліміти рахунку та статуси Risk Engine |
