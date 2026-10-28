# `app/core` — конфіг, логування, годинник, сповіщення

## Призначення

Спільна інфраструктура всіх трьох процесів (торговий, воркер, колектор): конфігурація з `.env`,
структуроване логування з маскуванням секретів, єдиний годинник `clock`, Telegram-сповіщення.

## Розділи ТЗ

- ТЗ §4.3 — час (UTC, мілісекунди)
- ТЗ §12 — моніторинг, сповіщення, логи
- ТЗ §14 — безпека, ключі в `.env`

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `Settings` | `config.py` | налаштування процесу з `.env` у корені репозиторію; ім'я змінної = назва поля у верхньому регістрі; порожнє значення = default; незмінний (`frozen`) |
| `Settings.secret_values()` | `config.py` | відкриті значення всіх заданих `SecretStr`/`SecretBytes`, включно з вкладеними моделями й колекціями — лише для реєстрації в маскері логів |
| `REPO_ROOT`, `ENV_FILE` | `config.py` | корінь репозиторію і шлях до `.env` |
| `now_ms()`, `monotonic_ms()` | `clock.py` | системний час (UTC epoch, `int` мс) і монотонний годинник (`int` мс, лише для інтервалів) |
| `Clock`, `SystemClock`, `FakeClock` | `clock.py` | протокол годинника, реальний і фейковий; `FakeClock.advance(ms)` рухає обидва, `jump_wall(delta_ms)` / `set_now(ms)` — лише системний (імітація стрибка) |
| `get_clock()`, `use_clock(clock)` | `clock.py` | поточний годинник і context manager підміни (відновлює попередній і при винятку) |
| `configure_logging(settings, *, process, extra_secrets=())` | `logging.py` | налаштувати логування процесу (`backend` / `worker` / `collector`); ідемпотентно |
| `shutdown_logging()` | `logging.py` | зняти й закрити хендлери (завершення процесу, тести) |
| `get_logger(name=APP, **initial)` | `logging.py` | structlog-логер; канали `APP`, `TRADING`, `ORDERS` або `app.<модуль>` |
| `bind_context(**values)`, `clear_context()` | `logging.py` | контекст запису (`bot_id`, `symbol`, `client_order_id`, `signal_id`) через contextvars |
| `SecretMasker`, `MASK`, `SECRET_MASK`, `MIN_SECRET_LEN`, `MAX_STR_LEN` | `logging.py` | процесор маскування і його константи |

Поля `Settings` (межі й defaults визначені в `config.py`):

| Поле | Змінна | За замовчуванням | Опис |
| --- | --- | --- | --- |
| `log_level` | `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `log_dir` | `LOG_DIR` | `<корінь репозиторію>/logs` | тека логів; відносний шлях — від кореня репозиторію; кожен процес пише в підтеку `<process>/` |
| `log_max_bytes` | `LOG_MAX_BYTES` | `52428800` (50 MiB) | розмір файла до ротації, байти; 1 MiB … 1 GiB |
| `log_backup_count` | `LOG_BACKUP_COUNT` | `10` | ротованих копій на файл; 1 … 100 |
| `log_console` | `LOG_CONSOLE` | `true` | дублювати JSON-логи в stderr (під launchd/systemd можна вимкнути: stderr-файл не ротується) |
| `postgres_db` | `POSTGRES_DB` | `ctb` | ім'я основної БД (docker-compose.yml); тестова БД — `<ім'я>_test` |
| `postgres_user` | `POSTGRES_USER` | `ctb` | власник БД (superuser контейнера, лише локально) |
| `postgres_password` | `POSTGRES_PASSWORD` | — (обов'язково для compose) | `SecretStr` |
| `postgres_readonly_password` | `POSTGRES_READONLY_PASSWORD` | — (обов'язково для compose) | `SecretStr`; пароль ролі `ctb_readonly`, застосовується лише при першій ініціалізації volume |

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
  тому uvicorn запускається з `log_config=None` (T0.7).
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
5. Довільні об'єкти перетворюються на `repr()` і маскуються **до** рендера JSON; `repr`, що падає, →
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

> Заплановано: порівняння з `fetch_time()` біржі, детектор стрибка системного годинника відносно монотонного,
> `recvWindow` — T1.6; T5.6 — Telegram.

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
| T5.6 | `[ ]` | Telegram-сповіщення |
