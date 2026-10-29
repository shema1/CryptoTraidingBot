# `app/exchange` — ExchangeAdapter, адаптер Bybit, FakeExchangeAdapter, rate limiter

## Призначення

Єдина точка доступу до біржі: інтерфейс `ExchangeAdapter`, доменні типи й винятки на його межі, адаптер Bybit
(live / demo / testnet — один код), `FakeExchangeAdapter` з ін'єкцією збоїв для тестів, централізований rate limiter.
Прямі виклики CCXT поза цим пакетом заборонені (AGENTS.md інваріант 4).

> Заплановано: T1.7 (кеш `instruments`, округлення), T3.2 (Bybit: торгові методи, мапінг кодів помилок),
> T3.5 (приватний WebSocket). Зараз є контракт, доменні типи, винятки, `FakeExchangeAdapter` (T1.1),
> rate limiter (T1.2), публічні REST-методи Bybit і `fetch_time` (`bybit.py`, T1.3) і моделі БД (T0.4).
> `BybitAdapter` навмисно не є повним `ExchangeAdapter`: ордери, позиції й приватний WS — T3.2/T3.5.

## Розділи ТЗ

- ТЗ §3 — інтерфейс `ExchangeAdapter`, інкапсуляція `category=linear`, `FakeExchangeAdapter` лише для тестів
- ТЗ §6.5 — централізований rate limiter і пріоритети
- ТЗ §7.1, §7.2 — стани ордера, `SUBMISSION_UNKNOWN`, формат `clientOrderId`, дедуплікація fills
- ТЗ §9.1 — нативні TP/SL на рівні позиції (режим `Full`), тригери SL — mark, TP — last
- ТЗ §11.2, §11.4 — приватний потік, розрив WS, класифікація чужих ордерів
- ТЗ §19 — припущення про Bybit, що перевіряються на Testnet

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `ExchangeAdapter` | `adapter.py` | `Protocol` з 15 методами ТЗ §3; docstring кожного методу — контракт (див. нижче) |
| `Candle`, `Instrument`, `Order`, `Fill`, `Position`, `Balance`, `FundingRate` | `types.py` | факти біржі: `Decimal`, час — `int` мс UTC, символ — канонічний CCXT (`BTC/USDT:USDT`) |
| `PlaceOrderRequest`, `PositionProtection`, `OrderAck` | `types.py` | запити й підтвердження |
| `StreamConnected`, `OrderUpdate`, `FillUpdate`, `PositionUpdate`, `BalanceUpdate` (`PrivateStreamEvent`) | `types.py` | події приватного потоку |
| `Side`, `PositionSide`, `OrderType`, `TimeInForce`, `TriggerBy`, `ExchangeOrderStatus`, `StopOrderType`, `ExecType`, `MarginMode`, `Timeframe` | `types.py` | enum-и; `Timeframe.duration_ms`, `ExchangeOrderStatus.is_terminal` (`TRIGGERED` і `UNKNOWN` — не термінальні) |
| `CLIENT_ORDER_ID_RE`, `HOST_ID_RE`, `new_client_order_id(host_id)`, `client_order_host_id(id)` | `types.py` | формат `clientOrderId` = `tb` + `HOST_ID` + uuid4 hex (36 символів, ТЗ §7.2) — єдине джерело, з нього ж CHECK у `orders` |
| `ClientOrderIdOrigin`, `client_order_id_origin(id)` | `types.py` | класифікація для звірки (ТЗ §11.4): `BOT` / `BOT_PREFIX_MALFORMED` (префікс `tb`, формат не наш — трактується як чужий комп'ютер) / `NOT_BOT` (ручний, біржовий) |
| `ExchangeError` і підкласи, `is_definite_rejection(exc)` | `errors.py` | ієрархія винятків (нижче) |
| `FakeExchangeAdapter`, `Fault`, `FaultKind`, `FakeCall` | `fake.py` | in-memory біржа для тестів (нижче); **імпортується лише з `tests/`**; `FakeCall.priority` |
| `RateLimiter`, `RequestPriority`, `ProcessRole`, `EndpointGroup` | `rate_limit.py` | черга REST одного процесу (нижче); числа лімітів — константи модуля |
| `BybitAdapter`, `ExchangeMode`, `BybitPublicClient` | `bybit.py` | публічні REST і `fetch_time` (нижче); імпорт `from app.exchange.bybit import …` (пакет `app.exchange` CCXT не тягне) |
| `ExchangeAccount`, `Instrument` (ORM) | `models.py` | таблиці `exchange_accounts`, `instruments` (ТЗ §6.4, §15) |
| `EXCHANGES`, `KEY_CHECK_RESULTS` | `models.py` | допустимі біржі (`bybit`); результати перевірки ключа |

Назви `Candle`/`Order`/`Fill`/`Position`/`Instrument` у `types.py` збігаються з ORM-моделями інших модулів: це факти
біржі на межі адаптера, а не рядки БД. Імпортуйте модулем: `from app.exchange import types`.

## Доменні типи

- Усі моделі — pydantic v2 `strict`, `frozen`, `extra="forbid"`: `float`/`int`/`str` у `Decimal`-полях, `bool`/`float`
  у мс, «сирі» рядки замість enum, NaN/Infinity — відхиляються. Адаптер конвертує значення біржі явно.
- **Факти біржі** перевіряються лише на внутрішню узгодженість (`Order`: `0 ≤ filled_qty ≤ qty`, `FILLED` ⇒
  `filled_qty = qty`, `PARTIALLY_FILLED` ⇒ `0 < filled_qty < qty`, `NEW`/`UNTRIGGERED` ⇒ `filled_qty = 0`,
  `avg_fill_price` ⇔ `filled_qty > 0`, `qty = 0` лише в умовного ордера; `Candle`: OHLC як CHECK у БД і вирівнювання
  `open_time_ms` за таймфреймом). `Order.client_order_id` — будь-який рядок або `None` (ручні ордери, ордери біржі).
  `CANCELED` з `filled_qty > 0` — частковий fill і скасування залишку (ТЗ §7.1). Невідомий статус → `UNKNOWN`
  (не термінальний; один такий ордер не робить увесь список нерозбірним). `time_in_force=None` — TIF поза доменом
  (напр. PostOnly ручного ордера).
- **Запити** суворіші: `PlaceOrderRequest.client_order_id` — лише наш формат; `reduce_only` задається явно; LIMIT ⇔
  `price`; MARKET — без `price` і `time_in_force`; reduceOnly-ордер без SL/TP; SL/TP з правильного боку (BUY:
  SL < TP, для LIMIT ще SL < price < TP; SELL — навпаки). `PositionProtection.stop_loss` обов'язковий і > 0 — зняти SL
  доменним запитом неможливо; `take_profit=None` — TP не змінюється; `position_side` перевіряє орієнтацію.
- `Position` — лише ненульова one-way позиція; `stop_loss`/`take_profit` = `None`, коли не задано.

## Винятки (`errors.py`)

```
ExchangeError
├── ExchangeOutcomeUnknown        результат невідомий: запит міг виконатися
│   ├── ExchangeTimeout
│   ├── ExchangeUnavailable       5xx, обрив з'єднання
│   │   └── PrivateStreamLost     приватний WS втрачено
│   └── MalformedResponse         відповідь не вдалося нормалізувати
├── RateLimited(retry_after_ms, ip_ban)
├── DuplicateClientOrderId        ордер з цим clientOrderId УЖЕ є на біржі — НЕ відмова
├── RequestRejected(code, reason) біржа точно не виконала запит
│   └── OrderRejected
│       └── InsufficientMargin
├── OrderNotActive                cancel: ордера немає серед активних
├── ExchangeAuthError
└── AdapterConfigError            category ≠ linear, hedge mode, не isolated — адаптер не стартує
```

**Правило `place_order`:** ордер точно не створено лише коли `is_definite_rejection(exc)` → `True`
(`RequestRejected` і підкласи). Усе інше — таймаут, 5xx, `RateLimited`, `DuplicateClientOrderId`, помилка авторизації,
будь-який не-`ExchangeError` — це `SUBMISSION_UNKNOWN` і розв'язання за ТЗ §7.2. Машина станів (T3.1) використовує саме
цю функцію, а не власні `except`.

**Правило мапінгу кодів біржі (T3.2):** у `RequestRejected` потрапляють лише коди з явного переліку відмов; невідомий
код — `ExchangeOutcomeUnknown` («біржа відповіла» ≠ «точно не виконала»).

## Контракт `ExchangeAdapter` (коротко)

- Аргументи keyword-only або модель запиту; мережева помилка ніколи не повертається як «порожньо»/`None`.
- `place_order` / `cancel_order` / `set_*` адаптер не повторює; ідемпотентні читання — може (backoff, ліміт спроб).
- `fetch_candles` — лише закриті **за часом біржі** (`close_time_ms ≤` час сервера, не локальний годинник), за
  зростанням, `[start_ms, end_ms)`.
- `fetch_order_by_client_id` — будь-який статус, з історією; `None` лише при підтвердженій відсутності.
- `cancel_order` — рівно один з ідентифікаторів; `OrderNotActive`, якщо ордер уже не активний.
- `fetch_open_orders` — включно з умовними (TP/SL) і ручними; `fetch_positions` — лише ненульові, hedge mode →
  `AdapterConfigError`. Нерозбірний елемент → `MalformedResponse` для всієї відповіді, тому захист і Emergency Stop
  запитують точково за символом.
- `set_position_protection` — ідемпотентний (ті самі SL/TP — успіх); успіх означає «прийнято», а висновок «SL є» —
  лише з `fetch_positions`/WS (ТЗ §9.2), не з типу винятку.
- `set_leverage`, `set_margin_mode` — ідемпотентні.
- `fetch_executions` — лише торгові виконання, адаптер пагінує сам.
- `subscribe_private_stream` — async-генератор (закривається `aclose()`); перша подія `StreamConnected`; події можуть
  дублюватися, губитися й іти не по порядку; втрата → `PrivateStreamLost`, реконект — новий виклик, пропущене — через
  REST.
- Конфігурацію (linear, one-way, isolated) реальний адаптер перевіряє на старті (`AdapterConfigError`), окремого
  методу немає. Адаптер не вирішує, чи можна збільшувати експозицію (це `RiskGate`, T4.1), і не перевіряє напрям руху
  SL (Position Protection Manager, T4.3).
- Пріоритет черги: методи з фіксованим класом параметра не мають (`FIXED_METHOD_PRIORITY` у `rate_limit.py`:
  `fetch_time` — `RECONCILE`, `fetch_instruments` / `fetch_candles` / `fetch_funding_history` — `DATA`,
  `set_position_protection` — `PROTECTION` і знизити його не можна). Решта REST-методів вимагають keyword
  `priority: RequestPriority`. `subscribe_private_stream` — не REST. Новий вхід рекомендовано як `DATA`
  (може почекати); закриття і SL — `PROTECTION`; звірка і розв'язання `SUBMISSION_UNKNOWN` — `RECONCILE`.

**TODO(verify-bybit)** (T3.2/T3.7, і kline — лишається з T1.3): вікно унікальності `orderLinkId`; reduceOnly-ордер більший
за позицію; чи діє `minNotional` на reduceOnly; яку `qty` мають TP/SL позиції в режимі `Full` і чи видно їх у списку
відкритих ордерів; режим маржі в UTA — на рівні акаунта; Demo зберігає ордери 7 днів; код «not modified» для
`trading-stop`; чи вистачає запасу остаточності kline на mainnet; чи MARKET уже `Filled` одразу після відповіді на Testnet.

## Публічний Bybit (`bybit.py`, T1.3)

`BybitAdapter` — один клас на `ExchangeMode` (`demo` / `testnet` / `live`). `category` лише `linear`; інше значення —
`AdapterConfigError` до будь-якого вводу-виводу. Режим змінює публічний хост: testnet — CCXT `set_sandbox_mode`
(`api-testnet`); demo і live — публічний хост CCXT за замовчуванням (ті самі ринкові дані, що на mainnet, ТЗ §13).
`enable_demo_trading` не викликається: він перевів би й публічні запити на `api-demo`. Ключі для цих чотирьох методів
не потрібні. One-way і isolated перевіряються на старті торгового клієнта (T3.2), бо це приватний API.

Усі REST ідуть через `RateLimiter.call` (група `MARKET`, `retry=True`). Пріоритет — `FIXED_METHOD_PRIORITY`:
`fetch_time` — `RECONCILE`, решта — `DATA`. Транспортні збої (таймаут, 5xx, обрив) адаптер повторює сам до
`READ_ATTEMPTS` (3) з backoff. `MalformedResponse` і невідомий `retCode` (`ExchangeOutcomeUnknown`, не
`RequestRejected` — перелік відмов це T3.2) не повторюються. Порожній список — лише підтверджена порожня вибірка.
Заголовки `X-Bapi-Limit*` передаються в `observe`. 429, retCode `10006` і HTTP 403 «access too frequent» лишаються
паузі лімітера.

Тіло читається сирими implicit-GET CCXT (`publicGetV5Market*`), не unified-методами: ціни й кількості — рядки Bybit
у `Decimal`, не `float` CCXT. Час — `int` мс UTC з поля конверта `time`.

`CcxtBybitPublicClient` (транспорт):
- статус, заголовки й тіло беруться з аргументів `handle_errors` CCXT у слот поточного виклику (`ContextVar`), а не з
  `last_*` екземпляра. Тому lock на клієнті немає: запити йдуть паралельно, порядок вирішує лише пріоритет лімітера
  (інакше `RECONCILE` стояв би у FIFO за повільним запитом даних). `aclose` чекає запитів у польоті;
- значення query кодує клієнт (`_wire_query`, percent-encoding один раз): Bybit public `sign()` CCXT шле їх у URL як є
  (`rawencode` = `unquote(urlencode(...))`). Тест `test_bybit_ccxt_client.py` закріплює це через справжній CCXT;
- транспортні збої: таймаут (CCXT `RequestTimeout` або власний `asyncio.timeout`) → `ExchangeTimeout`, інші винятки
  CCXT (`NetworkError`, `ExchangeError` з aiohttp, закритий екземпляр) → `ExchangeUnavailable`; обидва — з логом
  `bybit_transport_error` (`path`, `error_type`, без повідомлення CCXT з URL). Інший виняток (помилка в коді) летить
  як є й не повторюється як збій мережі.

- `fetch_time` — `time` конверта, звірений із `result.timeNano` (різниця ≤ 1 мс; Testnet 2026-10-29 — 0 мс на 30
  запитах). `timeSecond` не використовується.
- `fetch_candles` — лише закриті за часом відповіді з запасом остаточності: `close_time_ms + KLINE_FINALITY_MARGIN_MS
  ≤ time` (500 мс), не за локальним годинником. Свічка в межах запасу ще не віддається, і це не пропуск. Діапазон
  `[start_ms, end_ms)`: Bybit включний, тож у запит іде `end_ms - 1`, а відкриття на межі `end_ms` відсікається.
  Невирівняний `end_ms` не губить свічку, відкриту до нього: неповний слот вікна теж запитується.
  `end_ms is None` — спершу `fetch_time`, і цей час є виключною межею. Порядок за зростанням, без дублів і без
  інтерполяції. `limit` — найраніші N. Сторінка kline ≤ 1000 і від новіших до старіших, тож вікно йде від початку
  діапазону кроками не довшими за сторінку; повна сторінка, що не покриває всі слоти вікна, ділиться навпіл (інакше
  обрізання лишило б найновіші й загубило ранні). Інтервали: `1`, `5`, `15`, `30`, `60`, `120`, `240`, `360`, `720`, `D`.
- `fetch_instruments` — увесь список `linear` (невідомий символ інакше дає retCode `10001`, а не порожній список),
  далі фільтр за `symbols`. Лишаються безстрокові з `contractType` `LinearPerpetual` або `Perpetual` (приклад у доках
  ще каже `Perpetual`; Testnet — `LinearPerpetual`), у яких `symbol == base+quote` і `quote == settle` — канонічний
  `BASE/QUOTE:SETTLE`. Датовані ф'ючерси й USDC-perp (`BTCPERP`) пропускаються. `max_qty` — менше з `maxOrderQty` і
  `maxMktOrderQty`, щоб ринковий ордер вміщався в ліміт. Бракований фільтр у інструмента, який мав би увійти у
  відповідь, валить усю відповідь (`MalformedResponse`). Курсор Bybit приходить percent-encoded: адаптер декодує
  його один раз (`unquote`) у логічне значення, а клієнт кодує для URL. Розкодований курсор у URL розпадається на
  два параметри — Testnet відповідає retCode `10001` (перевірено 2026-10-29; на Testnet уже 911 linear-інструментів
  при сторінці 1000).
- `fetch_funding_history` — `[start_ms, end_ms)`, від старіших до новіших; від'ємна ставка допустима. Bybit віддає
  новіші спочатку і на повній сторінці (200) лишає саме їх, тож повна сторінка не вважається повною історією: вікно
  ділиться навпіл, доки сторінка не стане коротшою. Дві різні ставки на одну мітку — `MalformedResponse`.

**Запас остаточності kline** — `KLINE_FINALITY_MARGIN_MS` = 500 мс, рішення власника (T1.3, 2026-10-29). Він повністю
додається до затримки сигналу, тож має вкладатися в бюджет ТЗ §5.5 (ціль < 1 с від відкриття свічки входу, межа
`max_signal_age_ms` = 5 с). Замір на Testnet того ж дня (5 меж M1): свічка вже остаточна через 0,22–0,69 с після межі
за часом біржі. **TODO(verify-bybit):** чи вистачає запасу на mainnet (Testnet малоліквідний). Перевірка на Testnet —
`test_kline_returned_right_after_the_finality_margin_does_not_change_later` (`-m testnet`, `slow`, до ~85 с).

## Rate limiter (`rate_limit.py`, ТЗ §6.5)

Один `RateLimiter` на процес. Публічні методи `BybitAdapter` ідуть через `call` (група `MARKET`, `retry=True`).
Торгові методи (T3.2) — теж через `call` або `acquire`.
`RateLimiter.from_settings(settings, role=...)` бере частку з `Settings`, а роль — від точки входу процесу
(`python -m app` — `TRADING`, воркер — `WORKER`, колектор — `COLLECTOR`). З `.env` роль не читається: файл спільний
для трьох процесів, і роль звідти дала б воркеру частку торгового (сума часток понад 100% — бан IP).

- **Віконний token bucket.** Витрачена вага зайнята до `ts + window_ms`, тож у будь-якому вікні місткість не
  перевищується. Так реалізовано token bucket ТЗ §6.5 під ковзне вікно Bybit: плавне поповнення дало б майже
  подвійний сплеск на межі вікна.
- **IP спільний.** Місткість процесу — `IP_REQUESTS * share_pct // 100` (за замовчуванням 50/30/20). Сума
  часток у `Settings` — рівно 100, тож сума `floor` не перевищує ліміт IP. Приватні UID-групи на частку не
  діляться і доступні лише ролі `trading` (ключі є лише в торговому процесі). `MARKET` для воркера і колектора
  дозволений.
- **Пріоритети** `PROTECTION` > `RECONCILE` > `DATA` (ранг — `_PRIORITY_RANK`). Захист забирає наступний вільний
  слот попереду вже поставлених даних і звірки. Якщо не-захисний запит чекає довше за `STARVATION_MS`, найстаріший
  такий отримує слот попереду вищого не-захисного. `DATA` захист не обганяє. Однаковий пріоритет — FIFO.
- **Заголовки** `X-Bapi-Limit`, `X-Bapi-Limit-Status`, `X-Bapi-Limit-Reset-Timestamp` лише стискають групу
  (`observe`). Публічні market-ендпоінти в таблиці UID відсутні: `MARKET` спершу лише IP, окремий bucket
  з'являється ліниво після перших заголовків. При нульовому залишку група чекає reset, але не довше за своє вікно
  (UID — 1 с): reset — час біржі, і зсув годинника чи сміття в заголовку інакше заморозили б SL у `TRADING_STOP`.
  Reset далі за вікно пишеться в лог як `rate_limit_reset_beyond_window`.
- **429, retCode `10006`, HTTP 403 «access too frequent»** (`limit_breach_from_response`) ставлять на паузу всю
  чергу цього процесу, включно із захистом. Backoff експоненційний із jitter; підлога — `retry_after_ms`, для
  IP-бану ще `IP_BAN_MS`. І підлога, і сходинка не довші за `BACKOFF_CAP_MS`. Сходинка росте раз на епізод паузи:
  пачка 429, що прийшла, поки черга вже стоїть, її не розганяє. `call(..., retry=True)` повторює до `MAX_ATTEMPTS`.
  `place_order` і `cancel_order`
  (`NON_RETRYABLE_METHODS`) передають `retry=False`: одна спроба, пауза черги, `RateLimited` не є відмовою.
  Інші винятки backoff не чіпають. Якщо лімітер закрили, поки запит був у мережі, викликач отримує саме
  `RateLimited` без повтору (`penalize` на закритому лімітері нічого не робить). `RateLimiterError` означає лише
  «запит не відправлявся».
- Вага HTTP для IP завжди 1. `weight` (1…`MAX_WEIGHT`) списується лише з UID-групи (batch Bybit — кількість
  ордерів). Якщо IP-вікно процесу не менше за `IP_RESERVE_MIN_CAPACITY`, не-захисні запити лишають
  `ceiling // IP_RESERVE_DIVISOR` слотів (щонайменше один) під `PROTECTION`. Заголовок, що знизив стелю групи,
  не піднімає її назад до рестарту процесу. Час вікон і пауз — `monotonic_ms()` (`app.core.clock`).
- Одна черга на процес, не на акаунт. Поки торговий процес не веде Demo і Live одночасно, це збігається з ТЗ §6.5.
  Якщо обидва режими опиняться в одному процесі, пауза одного UID зупинить SL іншого — це треба розвести до T3.2
  (ТЗ §8.1).

Числа лімітів, вікон, backoff і часток — константи `rate_limit.py` (єдине джерело; тест
`test_documented_bybit_limits_are_the_single_source` фіксує їх). Звірені з документацією Bybit v5 2026-10-29.

Групи публічних методів уже в `bybit.py`: `fetch_time`, `fetch_instruments`, `fetch_candles`, `fetch_funding_history` →
`MARKET`. Для T3.2: `place_order` → `ORDER_CREATE` (`retry=False`); `cancel_order` → `ORDER_CANCEL`
(`retry=False`); `fetch_open_orders` → `ORDER_REALTIME`; `fetch_order_by_client_id` може потребувати і realtime,
і history (два `acquire`, T3.2); `fetch_executions` → `EXECUTION_LIST`; `fetch_positions` → `POSITION_LIST`;
`set_position_protection` → `TRADING_STOP` (`retry=True`, ідемпотентний); `set_leverage` → `SET_LEVERAGE`;
`fetch_balance` → `WALLET_BALANCE`; `set_margin_mode` → `SET_MARGIN_MODE`.

## `FakeExchangeAdapter` (лише тести)

Детермінований in-memory симулятор, без мережі й `sleep`; час — `app.core.clock` (у тестах `FakeClock`) плюс
`server_time_offset_ms`. Тест `tests/exchange/test_no_fake_in_production.py` гарантує, що жоден модуль `app/**` його
не імпортує.

- Ринок: `add_instrument(instrument, last_price=..., mark_price=...)`, `update_instrument`, `add_candles`,
  `add_funding_rates`, `set_price(symbol, last=..., mark=...)` (перевіряє тригери SL/TP).
- Виконання: MARKET і маркетабельний LIMIT — одразу за last (тейкер); немаркетабельний LIMIT GTC стоїть до
  `fill_order(...)` (мейкер) або скасування, немаркетабельний IOC/FOK — `CANCELED`. Свічки — лише закриті за часом
  біржі (`now_ms() + server_time_offset_ms`). Фільтри інструмента, SL/TP відносно ціни, спрощена маржа → `OrderRejected`/`InsufficientMargin`;
  дубль id → `DuplicateClientOrderId`; reduceOnly без позиції → відмова, більший за позицію — обрізається.
  One-way неттінг: середня ціна, реалізований PnL і комісії в wallet balance.
- Нативні SL/TP вхідного ордера переходять на позицію; спрацювання SL/TP закриває позицію біржовим ордером
  (`client_order_id=None`, `StopOrderType`), обидва одразу — SL.
- Збої: `fail_next(method, Fault.timeout(applied=True), times=n)` тощо — `applied=True` означає «виконано, але
  відповідь втрачено»; `Fault.config_error()` — `AdapterConfigError`; зовнішній ордер можна створити й зі статусом
  `UNKNOWN` (`place_external_order(..., status=...)`); `partial_fill_next(*chunks)`; WS — `ws_drop_next`, `ws_duplicate_next`, `ws_disconnect`;
  зовнішні дії — `place_external_order`, `open_external_position`, `close_position_externally`,
  `clear_position_protection`.
- Огляд: `calls` / `calls_to(method)` (включно з невдалими викликами) зберігають `priority` (`None` лише в
  `subscribe_private_stream`; в `args` пріоритет не дублюється), `all_orders()`, `stream_subscribers`,
  `pending_stream_events()`. Фейк лімітер не викликає.

## Таблиці БД

- `exchange_accounts` — акаунт біржі: `name` (UNIQUE), `exchange`, `mode` (`demo`/`testnet`/`live`), `env_key_set`
  (назва набору змінних `.env`, напр. `BYBIT_DEMO` → `BYBIT_DEMO_API_KEY`/`_SECRET`; самих ключів у БД немає;
  **UNIQUE** — два записи з тими самими ключами означали б один реальний акаунт із розділеними лімітами),
  `allowed_host_id` (`^[a-z0-9]{2}$`, ТЗ §6.4), результат перевірки ключа `key_check_result`
  (`OK` / `WITHDRAW_ENABLED` / `KEY_MISSING` / `UNVERIFIABLE`) разом із `key_check_at_ms` і `key_check_details`
  (T3.6 може уточнити набір). `UNIQUE (id, mode)` — ціль складених FK: режим бота, ордера, позиції, fill дорівнює
  режиму акаунта.
- `instruments` — кеш правил, PK `(exchange, mode, symbol)` (символ — канонічний CCXT) (на Testnet правила можуть відрізнятися): `tick_size`,
  `qty_step`, `min_qty` (> 0), `min_notional` (≥ 0), `max_qty`, `max_leverage` (≥ 1), `status` (як повертає біржа;
  «торгується чи ні» визначає T1.7), `updated_at_ms`.

Схема створюється міграцією `migrations/versions/0001_initial_schema.py`; обмеження й тригери перевіряють `tests/db/test_constraints.py`, `tests/db/test_guards.py` і `tests/db/test_migrations.py`.

## Тести

`tests/exchange/`: `test_types.py` (strict, інваріанти, орієнтація SL/TP і класифікація `clientOrderId` — у т.ч.
hypothesis), `test_errors.py` (ієрархія, `is_definite_rejection`), `contract.py` + `test_contract_fake.py`
(contract-набір лише через методи протоколу, включно з циклом позиції з нативним SL, підтягуванням SL і reduceOnly-
виходом; T1.3/T3.2 підключать його до Testnet/Demo маркерами — модулю для мережі треба перевизначити autouse-фікстуру
`fake_clock` на `SystemClock`; `ContractMarket.mode` — лише `fake`/`testnet`/`demo`, а наявну позицію за символом набір
не чіпає), `test_fake_faults.py` (кожен тип збою, часткові fills, гонка cancel/fill, SL/TP, WS),
`test_no_fake_in_production.py`, `test_rate_limit.py` (пріоритети, частка IP, 429, заголовки, backoff),
`test_bybit_public.py` (записані відповіді v5: закриті свічки, запас остаточності, невирівняний `end_ms`, пагінація,
курсор, `max_qty`, невідомий `retCode`), `test_bybit_ccxt_client.py` (справжній CCXT з підміненою сесією aiohttp: URL
і кодування курсора, паралельні запити без черги, мапінг транспортних збоїв і лог, закриття),
`test_bybit_testnet.py` (`-m testnet`, без ключів; модуль перевизначає autouse `fake_clock` на `SystemClock` і
перевіряє хост testnet до першого запиту). Повний contract-набір на Testnet/Demo — після T3.2: він ставить ордери.

## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.4 | `[x]` | таблиці `exchange_accounts`, `instruments` |
| T1.1 | `[x]` | доменні типи, `ExchangeAdapter`, ієрархія винятків, `FakeExchangeAdapter` з ін'єкцією збоїв |
| T1.2 | `[x]` | `rate_limit.py`: черга з пріоритетами, частка IP, backoff, заголовки Bybit |
| T1.3 | `[x]` | `bybit.py`: `fetch_time`, свічки, інструменти, funding; `category=linear`; група `MARKET` |
| T1.7 | `[ ]` | Кеш `instruments` і округлення |
| T3.2 | `[ ]` | Bybit: торгові методи та мапінг помилок |
| T3.5 | `[ ]` | Приватний WebSocket і реконект |
