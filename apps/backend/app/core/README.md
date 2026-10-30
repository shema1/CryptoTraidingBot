# `app/core` — конфіг, логування, годинник, сповіщення

## Призначення

Спільна інфраструктура всіх трьох процесів (торговий, воркер, колектор): конфігурація з `.env`,
структуроване логування з маскуванням секретів, єдиний годинник `clock`, основа БД (`db.py`), single-instance lock
торгового процесу (`instance_lock.py`), Telegram-сповіщення.

## Розділи ТЗ

- ТЗ §4.3 — час (UTC, мілісекунди)
- ТЗ §6.4 — конкурентність: advisory lock торгового процесу
- ТЗ §12 — моніторинг, сповіщення, логи
- ТЗ §14 — безпека, ключі в `.env`
- ТЗ §15 — модель даних (спільна основа моделей і міграцій)

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `Settings` | `config.py` | налаштування процесу з `.env` у корені репозиторію; ім'я змінної = назва поля у верхньому регістрі; порожнє значення = default; незмінний (`frozen`) |
| `Settings.secret_values()` | `config.py` | відкриті значення всіх заданих `SecretStr`/`SecretBytes`, включно з вкладеними моделями й колекціями — лише для реєстрації в маскері логів |
| `REPO_ROOT`, `ENV_FILE` | `config.py` | корінь репозиторію і шлях до `.env` |
| `Settings.database_url(*, test=False)` | `config.py` | `sqlalchemy.engine.URL` (`postgresql+asyncpg`) основної або тестової (`<POSTGRES_DB>_test`) БД; у `repr`/`str` пароль `***`; без `POSTGRES_PASSWORD` — `ValueError` |
| `Settings.postgres_test_db`, `TEST_DB_SUFFIX`, `LOOPBACK_HOSTS` | `config.py` | ім'я тестової БД; суфікс `_test`; дозволені значення `POSTGRES_HOST` |
| `API_BIND_HOST`, `API_TOKEN_MIN_LEN`, `API_TOKEN_MIN_DISTINCT` | `config.py` | адреса локального API — лише `127.0.0.1` (не налаштовується); мінімальна довжина `API_TOKEN` (32) і кількість різних символів у ньому (10) |
| `H1_MS`, `DEFAULT_TRADING_SYMBOL`, `DEFAULT_RESEARCH_SYMBOLS` | `config.py` | година в мс; торговий символ і список 8 символів навчання (ТЗ §5А.6) |
| `DEFAULT_BACKFILL_START_MS`, `DEFAULT_BACKFILL_INTERVAL_S`, `BACKFILL_INTERVAL_MIN_S`, `BACKFILL_INTERVAL_MAX_S`, `RESEARCH_SYMBOLS_MIN`, `RESEARCH_SYMBOLS_MAX` | `config.py` | початок історії, інтервал докачування (60…86400 с) і межі довжини `research_symbols` (5…8) |
| `DEFAULT_RECV_WINDOW_MS`, `RECV_WINDOW_MIN_MS`, `RECV_WINDOW_MAX_MS` | `config.py` | recvWindow приватних запитів Bybit: 5000 мс (default біржі), межі 2000 (= 2 × `CLOCK_DRIFT_LIMIT_MS`) … 10000 |
| `Base`, `NAMING_CONVENTION` | `db.py` | декларативна основа всіх моделей; детерміновані назви обмежень (`pk_`, `uq_`, `ck_`, `fk_`, `ix_`) |
| `TimeMs`, `CreatedMs`, `Money` | `db.py` | анотації колонок: `BIGINT` мс; `BIGINT` мс з default `now_ms()` при вставці; `NUMERIC` (`Decimal`) |
| `enum_check(column, values)`, `sql_list(values)`, `MODES` | `db.py` | `CHECK (column IN (...))` для статусів; режими `demo`/`testnet`/`live` |
| `SYMBOL_RE`, `symbol_check(column)` | `db.py` | канонічний символ CCXT (`BTC/USDT:USDT`) і його `CHECK` — в усіх таблицях із символом |
| `MODEL_MODULES`, `load_models()` | `db.py` | перелік модулів `app/*/models.py`; імпорт усіх і спільний `MetaData` (Alembic, тести) |
| `create_engine(url, **kw)`, `session_factory(engine)` | `db.py` | async engine (asyncpg, `pool_pre_ping`) і фабрика `AsyncSession` (`expire_on_commit=False`) |
| `now_ms()`, `monotonic_ms()` | `clock.py` | системний час (UTC epoch, `int` мс) і монотонний годинник (`int` мс, лише для інтервалів) |
| `CLOCK_DRIFT_LIMIT_MS` | `clock.py` | 1000 мс — допуск розбіжності з часом біржі (ТЗ §4.3); одне джерело для контролю часу (`app/data/time_sync.py`), запасу детектора прогалин, допуску `signals` і нижньої межі `RECV_WINDOW_MS` |
| `Clock`, `SystemClock`, `FakeClock` | `clock.py` | протокол годинника, реальний і фейковий; `FakeClock.advance(ms)` рухає обидва, `jump_wall(delta_ms)` / `set_now(ms)` — лише системний (імітація стрибка) |
| `get_clock()`, `use_clock(clock)` | `clock.py` | поточний годинник і context manager підміни (відновлює попередній і при винятку) |
| `configure_logging(settings, *, process, extra_secrets=())` | `logging.py` | налаштувати логування процесу (`backend` / `worker` / `collector`); ідемпотентно |
| `shutdown_logging()` | `logging.py` | зняти й закрити хендлери (завершення процесу, тести) |
| `get_logger(name=APP, **initial)` | `logging.py` | structlog-логер; канали `APP`, `TRADING`, `ORDERS` або `app.<модуль>` |
| `bind_context(**values)`, `clear_context()` | `logging.py` | контекст запису (`bot_id`, `symbol`, `client_order_id`, `signal_id`) через contextvars |
| `SecretMasker`, `MASK`, `SECRET_MASK`, `MIN_SECRET_LEN`, `MAX_STR_LEN` | `logging.py` | процесор маскування і його константи; `SecretMasker.mask(value)` — маскована JSON-придатна копія значення поза логами (`request`/`result` в `audit_log`, T0.7) |
| `InstanceLock(url, *, lock_id=BACKEND_LOCK_ID, application_name_prefix=APPLICATION_NAME_PREFIX)` | `instance_lock.py` | advisory lock процесу (торговий; воркер — `WORKER_LOCK_ID` і `WORKER_APPLICATION_NAME_PREFIX`; колектор — `COLLECTOR_LOCK_ID` і `COLLECTOR_APPLICATION_NAME_PREFIX`) на окремій сесії (одноразовий): `acquire(wait_ms=, takeover=)`, `watch()`, `release()`, `held`, `session_pid`, `lost_reason`, `ensure_held()` |
| `run_exclusive(lock, body, *, on_stuck)` | `instance_lock.py` | виконати `body` під watchdog-ом lock; втрата lock чи зовнішнє скасування → `body` скасовано й дочекано (≤ `BODY_STOP_TIMEOUT_MS`, інакше `on_stuck`), потім `InstanceLockLost` / `CancelledError` |
| `InstanceLockHeld`, `InstanceLockUnavailable`, `InstanceLockLost`, `TakeoverRefused` | `instance_lock.py` | lock тримає інша сесія (з `holders`); БД недоступна (`last_error`); lock втрачено (`reason`); перехоплення не виконано (`not_confirmed` / `holder_changed` / `audit_failed`) |
| `LockHolder` | `instance_lock.py` | власник lock з `pg_locks` + `pg_stat_activity`: pid сесії, pid процесу ОС з `application_name`, адреса, користувач, стан, час у мс |
| `connect_lock_session(url, *, server_settings)`, `lock_server_settings(pid=None, *, prefix=)`, `application_name(pid, *, prefix=)` | `instance_lock.py` | окреме raw-з'єднання asyncpg (поза пулом) і параметри сесії lock (`application_name` `ctb-backend pid=N`, `ctb-worker pid=N` або `ctb-collector pid=N`; інший префікс — `ValueError`; серверні keepalive) |
| `LOCK_NAMESPACE`, `BACKEND_LOCK_ID`, `WORKER_LOCK_ID`, `COLLECTOR_LOCK_ID`, `KEEPALIVE_*`, `RETRY_INTERVAL_MS`, `HEARTBEAT_*`, `TERMINATE_WAIT_MS`, `BODY_STOP_TIMEOUT_MS` | `instance_lock.py` | ключ lock і таймінги (розділ «Single-instance lock» нижче) |

Поля `Settings` (межі й defaults визначені в `config.py`):

| Поле | Змінна | За замовчуванням | Опис |
| --- | --- | --- | --- |
| `log_level` | `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `log_dir` | `LOG_DIR` | `<корінь репозиторію>/logs` | тека логів; відносний шлях — від кореня репозиторію; кожен процес пише в підтеку `<process>/` |
| `log_max_bytes` | `LOG_MAX_BYTES` | `52428800` (50 MiB) | розмір файла до ротації, байти; 1 MiB … 1 GiB |
| `log_backup_count` | `LOG_BACKUP_COUNT` | `10` | ротованих копій на файл; 1 … 100 |
| `log_console` | `LOG_CONSOLE` | `true` | дублювати JSON-логи в stderr (під launchd/systemd можна вимкнути: stderr-файл не ротується) |
| `postgres_host` | `POSTGRES_HOST` | `127.0.0.1` | хост БД; лише loopback (`127.0.0.1`, `localhost`, `::1`), інше → помилка валідації |
| `postgres_port` | `POSTGRES_PORT` | `5432` | порт БД; 1 … 65535 |
| `postgres_db` | `POSTGRES_DB` | `ctb` | ім'я основної БД (docker-compose.yml); тестова БД — `<ім'я>_test` |
| `postgres_user` | `POSTGRES_USER` | `ctb` | власник БД (superuser контейнера, лише локально) |
| `postgres_password` | `POSTGRES_PASSWORD` | — (обов'язково для compose) | `SecretStr` |
| `postgres_readonly_password` | `POSTGRES_READONLY_PASSWORD` | — (обов'язково для compose) | `SecretStr`; пароль ролі `ctb_readonly`, застосовується лише при першій ініціалізації volume |
| `lock_wait_s` | `LOCK_WAIT_S` | `60` | скільки секунд при старті торгового процесу і воркера повторювати спроби взяти свій lock; 0 … 600 (0 — одна спроба) |
| `api_port` | `API_PORT` | `8000` | порт локального API; 1024 … 65535. Адреса не налаштовується: `API_BIND_HOST` = `127.0.0.1` |
| `api_token` | `API_TOKEN` | — (обов'язково для торгового процесу) | `SecretStr`; bearer-токен UI, ≥ `API_TOKEN_MIN_LEN` = 32 символи token68 (`[A-Za-z0-9._~+/-]`, `=` лише в кінці), з них ≥ `API_TOKEN_MIN_DISTINCT` = 10 різних (`aaaa…` відхиляється); без нього `python -m app` завершується з кодом 15 |
| `api_allowed_origins` | `API_ALLOWED_ORIGINS` | `http://127.0.0.1:5173` | origins UI для перевірки `Origin` і CORS; у `.env` через кому; лише `http(s)://127.0.0.1\|localhost\|[::1]:<порт>` без шляху (інше, `*`, `null` → помилка валідації); нормалізуються в нижній регістр, дублікати прибираються; порожній список — браузер не має доступу |
| `rate_limit_share_trading_pct` | `RATE_LIMIT_SHARE_TRADING_PCT` | `50` | частка торгового процесу в спільному IP-ліміті Bybit, %; 1 … 98; сума трьох часток — рівно 100. Роль процесу (`ProcessRole`) не налаштовується: `.env` спільний, роль передає точка входу в `RateLimiter.from_settings(settings, role=...)` |
| `rate_limit_share_worker_pct` | `RATE_LIMIT_SHARE_WORKER_PCT` | `30` | частка воркера, %; 1 … 98 |
| `rate_limit_share_collector_pct` | `RATE_LIMIT_SHARE_COLLECTOR_PCT` | `20` | частка колектора, %; 1 … 98 |
| `trading_symbol` | `TRADING_SYMBOL` | `BTC/USDT:USDT` | торговий символ (канонічний CCXT). Backfill свічок іде і для нього, навіть якщо його немає в `research_symbols` |
| `research_symbols` | `RESEARCH_SYMBOLS` | BTC, ETH, SOL, XRP, BNB, DOGE, ADA, LTC (`…/USDT:USDT`) | 5–8 USDT-perpetual для навчання (ТЗ §5А.6); у `.env` через кому; дубль або інший формат — помилка валідації |
| `backfill_start_ms` | `BACKFILL_START_MS` | `1577836800000` | початок історії свічок і funding, мс UTC (2020-01-01T00:00:00Z); має бути кратним 1h |
| `backfill_interval_s` | `BACKFILL_INTERVAL_S` | `600` | інтервал інкрементального докачування, секунди; 60 … 86400 |
| `recv_window_ms` | `RECV_WINDOW_MS` | `5000` | recvWindow приватних запитів Bybit, мс; 2000 … 10000. Bybit приймає запит, коли `server_time − recv_window ≤ timestamp < server_time + 1000`: нижня межа — допустиме відставання годинника (1 с) плюс стільки ж на передачу. Годинник, що випереджає біржу на ~1 с і більше, ламає всі приватні запити незалежно від вікна. Поки ні на що не впливає: читатиме торговий клієнт (T3.2, CCXT `options.recvWindow`) |

## Логування

- Файли: `<log_dir>/<process>/app.log` (усе), `trading.log` (логер `trading` — торгові рішення), `orders.log`
  (логер `orders` — ордери), `errors.log` (ERROR+ з будь-якого логера, зокрема сторонніх бібліотек).
  `RotatingFileHandler` за розміром, utf-8; файл створюється при першому записі; тека процесу — `0700`
  (права виставляються і для вже наявної теки). Окрема підтека на процес, бо ротація одного файла кількома
  процесами не підтримується.
- Формат: один JSON-об'єкт на рядок: `event`, `level`, `logger`, `ts_ms` (`int` мс UTC з `clock.now_ms()`),
  контекст і поля виклику; `exception` — текст трасування **без локальних змінних**. `Decimal` пишеться рядком
  (без втрати точності), `Enum` — значенням.
- `configure_logging()` — перша дія кожного процесу; до неї маскування немає. Він:
  - знімає **всі** хендлери root-логера (напр. після `logging.basicConfig()`) і ставить свої;
  - ставить `sys.excepthook`, `threading.excepthook`, `sys.unraisablehook` — неперехоплені винятки пишуться
    маскованими (`uncaught_exception`, `uncaught_thread_exception`, `unraisable_exception`), а не сирим traceback-ом
    у stderr; вмикає `logging.captureWarnings`; `logging.raiseExceptions = False` (вбудований звіт stdlib про помилку
    чужого хендлера друкує сирі `msg`/`args`). `shutdown_logging()` повертає все це як було.
- Сторонні stdlib-логери (ccxt, sqlalchemy, asyncpg), що поширюють записи до root, проходять той самий ланцюжок.
  Логер із власними хендлерами і `propagate=False` його обходить — так робить стандартний `log_config` uvicorn,
  тому uvicorn запускається з `log_config=None` (`app/api/server.py`, T0.7); його access-лог вимкнено.
- Спільний форматер рендерить запис один раз і повторно використовує результат для всіх наших хендлерів
  (`RotatingFileHandler` викликає `format()` двічі). Кожен рядок довший за `MAX_STR_LEN` (64 KiB) обрізається
  після заміни відомих секретів: маскування синхронне й не повинне блокувати цикл подій.

### Маскування секретів (ТЗ §14)

1. Значення `SecretStr` / `SecretBytes` → `**********`.
2. Поля з чутливою назвою → `***`, рекурсивно у вкладених dict/list/tuple/set. Назва чутлива, якщо злита назва в
   нижньому регістрі (`X-BAPI-API-KEY` → `xbapiapikey`, `DBPassword` → `dbpassword`) містить `secret`, `password`,
   `passwd`, `pwd`, `token`, `signature`, `authorization`, `cookie`, `passphrase`, `credential`, `apikey`,
   `privatekey` або `accesskey`, або має слово `sign` / закінчується на `sign` (`X-BAPI-SIGN`). `signal_id`,
   `designation`, `assignee` не маскуються.
3. У кожному рядку (повідомлення, значення, ключі, трасування), у такому порядку:
   1. відомі значення секретів — усі `SecretStr`/`SecretBytes` з `Settings`, включно з вкладеними моделями й
      колекціями (`Settings.secret_values()`), та `extra_secrets` (колекція, не рядок — рядок → `TypeError`);
      довжиною ≥ `MIN_SECRET_LEN` = 6 (коротші маскуються лише за п. 1–2); у сирому, `repr`- та `ascii`-вигляді.
      **Першими**, бо шаблони нижче маскують значення лише до роздільника, і хвіст секрету інакше лишився б;
   2. обрізання до `MAX_STR_LEN`;
   3. пароль у DSN (`scheme://user:***@`) і шаблони `name=value`, `name: value`, `"name": "value"`,
      `Authorization: Bearer value` для чутливих назв (з множиною й цифрами: `tokens=`, `password2=`, `apiKeys:`);
   4. повторно відомі значення.
4. Числа (`int`/`float`), що містять зареєстрований секрет, → `***` до рендера (JSON лишається валідним).
5. `UUID` пишеться рядком; довільні об'єкти перетворюються на `repr()` і маскуються **до** рендера JSON; `repr`, що падає, →
   `<unrepresentable Тип>`; вкладеність понад 20 рівнів → `<too deep Тип>`.
6. structlog-записи маскуються ще до передачі в stdlib (чужі хендлери бачать лише маски), а у форматері наших
   хендлерів — повторно (ідемпотентно), потім готовий JSON-рядок ще раз очищується від відомих значень
   (і JSON-екранованих форм).
7. Fail-closed, без вихідного тексту запису:
   - збій маскування → `{"event": "log_masking_failed", "level", "ts_ms", "error_type"}`;
   - збій форматування (напр. `%d` з рядком у сторонній бібліотеці) → `log_format_failed` з `logger`, `level`;
   - збій хендлера (диск заповнений, права) → рядок `log_handler_failed` у stderr.

Обмеження: шаблони п. 3.3 маскують значення до першого пробілу/лапки/`&`; незареєстрований секрет у шляху URL
(токен Telegram `…/bot<token>/…`, UUID healthchecks) або з пробілами під нейтральною назвою може лишитися. Тому
кожен секрет має бути `SecretStr`-полем `Settings` (тоді він маскується за значенням) — обов'язково для T3.6
(ключі біржі: помилка підпису Bybit містить ключ, склеєний без роздільників), T5.6 (Telegram), T5.7 (heartbeat).
`Settings` має `hide_input_in_errors=True`: `ValidationError` не повторює введене значення.

## Годинник

- Уся логіка бере час через `clock.now_ms()` / `clock.monotonic_ms()`. Прямі `time.time()`, `time.monotonic()`,
  `time.perf_counter()`, `datetime.now()` тощо заборонені ruff-правилом `TID251` (banned-api у `pyproject.toml`)
  всюди, крім `clock.py` і його тесту. `FakeClock` приймає лише `int` мс (`float` і `bool` → `TypeError`);
  `advance` не йде назад, системний час не може стати від'ємним.
- Поточний годинник — модульна змінна (свідомий виняток із «без глобального стану»: це точка підміни часу, а не
  торговий стан). `ContextVar` не підходить: `loop.run_in_executor` не копіює контекст, і потік мовчки взяв би
  реальний час.
- Тести: autouse-фікстура `fake_clock` (`tests/conftest.py`) ставить `FakeClock` (2026-01-01T00:00:00Z) для кожного тесту.
- `CLOCK_DRIFT_LIMIT_MS` (1000 мс, ТЗ §4.3) лежить тут, а не в `app/data`: його імпортує `config.py` (межа
  `RECV_WINDOW_MS`), а `app/data` залежить від `core`. Порівняння з `fetch_time()` біржі й детектор стрибків
  системного годинника відносно монотонного — `app/data/time_sync.py` (T1.6, README `app/data`).

## База даних (`db.py`)

- Моделі — у `app/<модуль>/models.py` (перелік — `MODEL_MODULES`; тест `tests/db/test_models_registry.py`
  перевіряє, що жоден `models.py` не пропущено). Опис таблиць — README відповідних модулів і ТЗ §15.
- Домовленості: час — лише `BIGINT` мс (`*_ms`), default часу ставить код через `now_ms()` (у тестах — `FakeClock`),
  а не `server_default`; гроші/ціни/кількості — `NUMERIC` без обмеження точності; статуси — `TEXT` + `CHECK`;
  інваріанти — обмеженнями БД (`UNIQUE`, `CHECK`, FK, часткові індекси), а не лише в коді.
- Міграції — `apps/backend/migrations/` (Alembic, async). DSN береться з `Settings.database_url()`, не з `alembic.ini`.

## Single-instance lock (`instance_lock.py`)

Не дає запустити другий екземпляр торгового процесу (`python -m app`) на тій самій БД (ТЗ §6.4, рішення
`docs/decisions/0006-instance-lock.md`). Той самий механізм з ключем `(LOCK_NAMESPACE, WORKER_LOCK_ID)` =
`(0x63746200, 2)` і `application_name = "ctb-worker pid=<pid ОС>"` тримає воркер задач (`app/jobs/worker.py`,
рішення `docs/decisions/0008-single-worker.md`): один воркер на БД. Колектор ліквідацій бере
`(LOCK_NAMESPACE, COLLECTOR_LOCK_ID)` = `(0x63746200, 3)` і `application_name = "ctb-collector pid=<pid ОС>"`
(`app/data/collector.py`): частка IP колектора теж розрахована на один процес. Перехоплення (`takeover`) воркер і
колектор не використовують; решта розділу — однаково для всіх трьох процесів, коди виходу воркера —
`app/jobs/README.md`, колектора — `apps/backend/README.md`.

- **Ключ:** `pg_try_advisory_lock(LOCK_NAMESPACE, BACKEND_LOCK_ID)` = `(0x63746200 "ctb\0", 1)`; у `pg_locks` —
  `locktype = 'advisory'`, `classid`/`objid`, `objsubid = 2`. Lock діє в межах **однієї БД** (`POSTGRES_DB`): другий
  процес з іншою БД на тому самому комп'ютері (напр. відновлена копія) його не бачить, і `allowed_host_id` від цього
  **не** захищає (`HOST_ID` той самий) — залишковий ризик, рішення 0006; між комп'ютерами захищає `allowed_host_id` (T5.1).
- **Окреме з'єднання:** raw asyncpg поза пулом SQLAlchemy (пул/автореконект мовчки підмінили б сесію на нову, без
  lock). Параметри сесії (`server_settings`): `application_name = "ctb-backend pid=<pid ОС>"`,
  `tcp_keepalives_idle = 10`, `tcp_keepalives_interval = 5`, `tcp_keepalives_count = 3` — PostgreSQL виявляє мертвого
  TCP-співрозмовника не пізніше `KEEPALIVE_TIMEOUT_S` = 25 с і звільняє lock; `idle_session_timeout = 0`.
  Після `kill -9` ОС закриває сокет одразу, і lock звільняється за мілісекунди. За Docker Desktop / WSL2 співрозмовник
  сервера — проксі портів (`client_addr` у `pg_stat_activity` — адреса шлюзу): звільнення тримається на тому, що
  проксі передає закриття з'єднання, а keepalive перевіряє проксі, не процес. Тому процес-власника видно лише з
  `application_name`.
- **Старт (`acquire`):** спроби кожні `RETRY_INTERVAL_MS` = 1000 мс до `LOCK_WAIT_S`; помилки з'єднання до кінця
  очікування не фатальні (БД може ще стартувати після перезавантаження). Наприкінці: lock зайнятий →
  `instance_lock_held_by_other` (ERROR, з `holders`) і `InstanceLockHeld`; БД недоступна →
  `instance_lock_db_unavailable` і `InstanceLockUnavailable`. Невдача чи скасування спроби закриває з'єднання — lock,
  який сервер міг видати в останню мить, не лишається. `acquire()` викликається один раз на екземпляр (повторно —
  `RuntimeError`): після невдачі, звільнення чи втрати lock береться лише новим процесом.
- **Робота (`run_exclusive` / `watch`):** listener закриття з'єднання asyncpg (миттєво) і heartbeat кожні
  `HEARTBEAT_INTERVAL_MS` = 5 с — запит «чи моя сесія тримає lock» з таймаутом `HEARTBEAT_TIMEOUT_MS` = 5 с.
  Закриття, помилка, таймаут або `false` → `instance_lock_lost` (CRITICAL), `held = False` (нові входи заборонені), але
  з'єднання **ще відкрите**: робота скасовується й дочікується до `BODY_STOP_TIMEOUT_MS` = 5 с, лише потім
  `InstanceLockLost` і `release()` — при таймауті heartbeat lock на сервері може бути ще нашим, і віддавати його до
  зупинки роботи не можна. Робота не зупинилась вчасно → `instance_lock_body_stuck` і `on_stuck` (у процесі —
  `os._exit`, код 12; після сигналу — код 14). Сигнал посеред зупинки дедлайн не скорочує, а втрата lock важливіша за
  сигнал (код 12). Збій самого watchdog-а (виняток не `InstanceLockLost`) теж позначає lock втраченим
  (`watchdog_failed:<тип>`) ще до зупинки роботи. Контракт `body` (T0.7, етап 5): скасування обробляється за ≤ 5 с, без
  необмежених `asyncio.shield` і без ковтання `CancelledError`; `body` володіє всіма своїми задачами (`TaskGroup`, без
  відірваних `create_task`) — задач поза `body` `run_exclusive` не скасовує; відірвані обробники (запити API —
  залежність `require_instance_lock`, `app/api/context.py`) викликають `ensure_held()` перед будь-якою дією, що збільшує експозицію. Повторно lock у тому самому процесі не береться: новий
  старт іде через звірку (ТЗ §11.1). `held` / `ensure_held()` — перевірка перед кожною дією, що збільшує експозицію
  (T5.2); `held` враховує і закритий сокет ще до спрацювання listener-а.
- **Перехоплення (`acquire(takeover=confirm)`):** лише якщо lock зайнятий; один раз: `confirm(holders)` → `False` =
  `TakeoverRefused("not_confirmed")`; `True` → власники читаються знову (людина могла думати хвилини): власника вже
  немає → без перехоплення, звичайна спроба; власник інший (`session_pid` + `backend_start_ms`) →
  `TakeoverRefused("holder_changed")`; той самий → запис `audit_log` (`instance.force_takeover`, `actor = cli`,
  `target = backend`, `confirmed = true`, `request` — власники і pid ОС) **до** `pg_terminate_backend(pid, 5000)`, який
  виконується одним запитом лише для сесії, що досі тримає lock і має той самий `backend_start` (pid міг бути
  перевикористаний). Аудит не записано → `TakeoverRefused("audit_failed")`, нікого не завершено. Після підтвердження
  `LOCK_WAIT_S` відраховується заново. Процес, у якого перехопили lock, отримує `InstanceLockLost` і завершується.
- **Події логу** (`app.core.instance_lock`, контекст `lock = [namespace, id]`): `instance_lock_acquired`,
  `instance_lock_busy`, `instance_lock_db_error`, `instance_lock_held_by_other`, `instance_lock_db_unavailable`,
  `instance_lock_takeover_refused`, `instance_lock_holder_gone`, `instance_lock_holder_changed`,
  `instance_lock_takeover_audit_failed`, `instance_lock_force_takeover`, `instance_lock_holder_terminated`,
  `instance_lock_lost`, `instance_lock_watchdog_failed`, `instance_lock_cancel_during_stop`, `instance_lock_body_stuck`,
  `instance_lock_body_failed`, `instance_lock_close_failed`, `instance_lock_released`.
- Обмеження: завислий процес (SIGSTOP, deadlock) тримає lock далі — це правильно, бо він «живий»; його ловить
  dead-man's switch (T5.7), а людина може перехопити lock вручну.

## Інваріанти

- Кожне поле `Settings` є в `.env.example` без значення, і навпаки; значення за замовчуванням у `.env.example` і в
  таблиці полів вище збігаються з кодом (`tests/test_env_example.py`).
- Невідомі змінні в `.env` ігноруються (там лежать і змінні інших задач, напр. ключі біржі).
- Секрет у полі, винятку чи `repr` не потрапляє в жоден лог і в stderr (`tests/test_logging.py`, вкл. property-based тест).

## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.1 | `[x]` | мінімальний `Settings` (`log_level`), тест відповідності `.env.example` |
| T0.2 | `[x]` | поля `postgres_*` для docker-compose; повний DSN — T0.4 |
| T0.3 | `[x]` | повний `Settings` (`frozen`, межі, `LOG_*`, `hide_input_in_errors`), `logging.py` з каналами, ротацією, маскуванням і хуками винятків, `clock.py` з `FakeClock`, заборона прямого доступу до часу (`TID251`) |
| T0.4 | `[x]` | `POSTGRES_HOST` (лише loopback), `POSTGRES_PORT`, `database_url()`; `db.py` — `Base`, naming convention, типи колонок, engine/сесії |
| T0.6 | `[x]` | `instance_lock.py` — advisory lock торгового процесу (окрема сесія з серверними keepalive, повтори `LOCK_WAIT_S`, власник у лог, watchdog втрати, ручне перехоплення з аудитом); `LOCK_WAIT_S` |
| T1.6 | `[x]` | `CLOCK_DRIFT_LIMIT_MS` перенесено сюди з `app/data/gaps.py` (одне джерело); `RECV_WINDOW_MS` |
| T5.6 | `[ ]` | Telegram-сповіщення |
