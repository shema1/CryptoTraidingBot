# `app/api` — REST- і WebSocket-ендпоінти локального API

## Призначення

Приймає команди UI та віддає стан. Працює в торговому процесі (`python -m app`) як робота під single-instance lock
(T0.6), слухає лише `127.0.0.1:API_PORT`; бізнес-логіки не містить — делегує модулям `trading`, `risk`, `reconcile`, `ml`.
Каркас і безпека — T0.7 (рішення `docs/decisions/0007-local-api.md`).

> Заплановано: T6.1 (групи `/data /models /bots /positions /orders /risk /system`), T6.2 (небезпечні дії з
> підтвердженням), T6.3 (WebSocket-канали). Зараз є лише `GET /system/health`.

## Розділи ТЗ

- ТЗ §6.4 — API працює під single-instance lock торгового процесу
- ТЗ §14 — безпека локального API (bearer-токен, `Origin`/`Host`, CORS), аудит небезпечних дій
- ТЗ §15 — таблиці `audit_log`, `api_idempotency_keys`
- ТЗ §16 — REST-групи, WebSocket-канали, `Idempotency-Key`, коди помилок

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `serve_api(settings, lock, *, sock=None)` | `server.py` | робота під lock (`run_exclusive`): сокет `127.0.0.1:API_PORT`, uvicorn до скасування, керована зупинка, закриття пулу БД |
| `bind_api_socket(port)`, `ApiBindError`, `ApiStoppedUnexpectedly` | `server.py` | слухаючий сокет лише на `API_BIND_HOST`; порт зайнятий → `ApiBindError` (`port`, `reason`); сервер зупинився без сигналу → помилка процесу |
| `uvicorn_config(app, settings)`, `GRACEFUL_SHUTDOWN_S`, `SERVER_STOP_TIMEOUT_MS`, `SERVER_FORCE_STOP_MS`, `DISPOSE_TIMEOUT_MS`, `DB_*_TIMEOUT_S` | `server.py` | конфіг uvicorn і таймінги зупинки/БД (розділ «Сервер») |
| `create_app(ctx)`, `LocalApi` | `app.py` | застосунок FastAPI з middleware безпеки і OpenAPI, що описує токен і `Idempotency-Key`; без `API_TOKEN` → `ValueError` |
| `ApiContext`, `LockState`, `DbPing`, `context(request)` | `context.py` | що API отримує від процесу: `settings`, lock, ping БД, сховище ключів, `AuditWriter`, `instance_id`, час старту |
| `require_instance_lock` | `context.py` | залежність для ендпоінтів, що збільшують експозицію: lock втрачено → 503 `INSTANCE_LOCK_LOST` |
| `LocalOriginGuard`, `BearerAuth`, `allowed_hosts(port)`, `is_cors_preflight`, `header_values` | `security.py` | ASGI-middleware `Host`/`Origin` (403) і токена (401) |
| `RejectionLog`, `REJECTION_LOG_BURST`, `REJECTION_LOG_WINDOW_MS` | `security.py` | обмежений лог відмов (спільний для обох middleware) |
| `CatchUnhandled` | `errors.py` | 500 `INTERNAL_ERROR` усередині CORS для необробленого винятку обробника |
| `IdempotencyMiddleware`, `SAFE_METHODS`, `IdempotencyStore`, `DbIdempotencyStore`, `IdempotencyRecord`, `StoredResponse`, `IdempotencyStatus`, `request_fingerprint` | `idempotency.py` | обов'язковий `Idempotency-Key` і повтор відповіді |
| `MAX_REQUEST_BODY_BYTES`, `MAX_STORED_RESPONSE_BYTES`, `KEY_MIN_LEN`, `KEY_MAX_LEN`, `IDEMPOTENCY_HEADER`, `REPLAYED_HEADER` | `idempotency.py` | межі: тіло запиту й збережена відповідь — по 1 MiB; ключ 16–128 символів |
| `AuditWriter`, `AuditWriter.from_settings(settings)`, `API_ACTOR` | `audit.py` | запис `audit_log` у транзакції викликача з маскуванням секретів |
| `ErrorCode`, `ErrorResponse`, `ApiError`, `install_error_handlers` | `errors.py` | єдиний формат помилок |
| `router`, `HealthResponse`, `HEALTH_DB_TIMEOUT_MS` | `system.py` | `GET /system/health` |
| `AuditLog`, `ApiIdempotencyKey` | `models.py` | таблиці `audit_log`, `api_idempotency_keys` |
| `AUDIT_ACTION_RISK_LOOSEN`, `AUDIT_ACTION_EMERGENCY_RELEASE`, `AUDIT_ACTION_MODEL_APPROVE` | `models.py` | дії, що підтверджують небезпечні зміни (`risk.config.loosen`, `emergency.release`, `model.approve`) |
| `AUDIT_ACTION_FORCE_TAKEOVER` | `models.py` | `instance.force_takeover` — ручне перехоплення single-instance lock торгового процесу (пише `app/core/instance_lock.py` до завершення сесії-власника, `actor = cli`, `confirmed = true` після введення `TAKEOVER` у терміналі; ні на що не посилається) |

## Безпека запиту (порядок middleware, зовні → всередину)

1. `ServerErrorMiddleware` (Starlette) — запасний 500 лише для збою самих зовнішніх middleware.
2. **`LocalOriginGuard`** (HTTP і WebSocket):
   - `Host` рівно `127.0.0.1:<API_PORT>` або `localhost:<API_PORT>` (без урахування регістру); інший домен (DNS
     rebinding), інший порт, без порту, відсутній чи дубльований `Host` → 403 `HOST_NOT_ALLOWED`;
   - `Origin`, якщо є, — точно один і точно з `API_ALLOWED_ORIGINS` (інакше, зокрема `null`, → 403
     `ORIGIN_NOT_ALLOWED`, у тому числі для CORS-preflight);
   - без `Origin`, але `Sec-Fetch-Site` = `cross-site`/`same-site` (браузерний no-cors запит з іншої сторінки) → 403;
     без обох заголовків (curl, скрипти) — далі за токеном;
   - кожна HTTP-відповідь отримує `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`;
   - WebSocket відхиляється закриттям `1008` до accept (сервер відповідає 403). До T6.3 uvicorn працює з `ws="none"`,
     тож upgrade не виконується і відмова — звичайна HTTP-відповідь 403/401.
3. **`CORSMiddleware`** (Starlette): лише `API_ALLOWED_ORIGINS`; методи `GET, POST, PUT, PATCH, DELETE`; заголовки
   `Authorization`, `Content-Type`, `Idempotency-Key`; expose `Idempotency-Replayed`; **без** credentials (кук немає) і без
   Private Network Access; `max-age` 600 с. Відповідає на preflight без токена.
4. **`CatchUnhandled`** (`errors.py`) — необроблений виняток обробника → 500 `INTERNAL_ERROR` без подробиць,
   **з** CORS і `no-store` (інакше UI бачив би «мережеву помилку» і не відрізнив би її від «результат невідомий»);
   трасування — один маскований запис `api_unhandled_exception`, виняток далі не перекидається.
5. **`BearerAuth`**: `Authorization: Bearer <API_TOKEN>` (схема без урахування регістру, рівно один пробіл);
   немає → 401 `AUTH_REQUIRED`, інша схема/значення/два заголовки → 401 `AUTH_INVALID`, обидва з
   `WWW-Authenticate: Bearer`. Порівняння SHA-256 дайджестів через `hmac.compare_digest`. Токен у query не приймається.
   Стоїть усередині CORS, тож UI бачить 401. WebSocket без токена → закриття `1008` (як передати токен із браузера — T6.3).
6. **`IdempotencyMiddleware`** — нижче. Стоїть після автентифікації: неавтентифікований запит не займає ключ.
7. Роутер. Swagger UI / ReDoc вимкнені (вантажать JS з CDN в origin API); `/openapi.json` — лише з токеном.

Відмови пишуться в лог `api_request_rejected` (WARNING: `reason`, метод, шлях, клієнт; для `Host`/`Origin` — їхні
значення до 200 символів), значення `Authorization` — ніколи. Будь-яка сторінка в браузері може слати відхилені
запити в циклі, тому лог обмежено (`RejectionLog`): не більше `REJECTION_LOG_BURST` = 20 записів за
`REJECTION_LOG_WINDOW_MS` = 60 с, решта рахується за кодом причини й підсумовується одним
`api_request_rejected_suppressed` (`counts`) на першому запиті (будь-якому) після закінчення вікна. Відмови 401/403
мають `Connection: close` — відхилений клієнт не тримає keep-alive сокет. Ліміту з'єднань (`limit_concurrency`)
**немає** свідомо: uvicorn рахує й простійні з'єднання, тож чужа сторінка могла б вичерпати ліміт і відрізати UI
(зокрема Emergency Stop) відповіддю 503. Успішні запити — `api_request` (DEBUG: статус, `duration_ms`).

## Ідемпотентність (`Idempotency-Key`)

Без ключа проходять лише `GET`/`HEAD`/`OPTIONS`; інший метод, крім `POST`/`PUT`/`PATCH`/`DELETE` (`TRACE`, `post`, …),
→ 405 `METHOD_NOT_ALLOWED` ще до обробника. Кожен `POST`/`PUT`/`PATCH`/`DELETE` мусить мати рівно один заголовок `Idempotency-Key` (16–128 символів
`[A-Za-z0-9_.:-]`; UI — UUID на кожну дію людини): немає → 400 `IDEMPOTENCY_KEY_REQUIRED`, некоректний → 400
`IDEMPOTENCY_KEY_INVALID`. Тіло читається повністю (> 1 MiB → 413 `REQUEST_TOO_LARGE`, без виконання), відбиток
запиту — SHA-256 методу, шляху, query і тіла. Запис `IN_PROGRESS` комітиться в `api_idempotency_keys` **до** виконання
обробника (сховище недоступне → 503 `IDEMPOTENCY_STORE_UNAVAILABLE`, дію не виконано). Ключ доступний обробнику як
`request.state.idempotency_key` (для `audit_log`).

| Наявний запис з цим ключем | Відповідь |
| --- | --- |
| немає | обробник виконується; відповідь (статус, заголовки без `content-length`, тіло ≤ 1 MiB) → `COMPLETED` |
| інший відбиток (метод, шлях, query чи тіло) | 422 `IDEMPOTENCY_KEY_REUSED` |
| `COMPLETED` | збережена відповідь + `Idempotency-Replayed: true` (зокрема збережені 4xx) |
| `IN_PROGRESS`, запит якого зараз у цьому процесі (у `begin()` або виконується; лічильники в пам'яті) | 409 `IDEMPOTENCY_REQUEST_IN_PROGRESS` |
| `FAILED` або будь-який інший `IN_PROGRESS` (упалий процес, скасований обробник, не записаний підсумок) | 409 `IDEMPOTENCY_RESULT_UNKNOWN` |

- Необроблений виняток обробника → `FAILED` (500 клієнту); відповідь, яку не можна зберегти (> 1 MiB, неповна), → `FAILED`.
  Скасування обробника (зупинка процесу) лишає `IN_PROGRESS` — повтор отримує «невідомо» (і до, і після рестарту).
- Невідомий результат **ніколи** не виконується повторно з тим самим ключем: людина перевіряє стан і повторює дію з
  новим ключем (як `SUBMISSION_UNKNOWN` для ордерів).
- Збій запису підсумку → ERROR `idempotency_result_not_stored`, клієнт отримує відповідь, запис лишається
  `IN_PROGRESS` → повтор отримає 409 `IDEMPOTENCY_RESULT_UNKNOWN` (fail-closed).
- Ключі не протухають і не видаляються (обсяг команд UI мізерний).
- Події логу: `idempotency_key_missing`, `idempotency_key_invalid`, `idempotency_request_too_large`,
  `idempotency_client_disconnected`, `idempotency_store_unavailable`, `idempotency_key_reused`, `idempotency_replayed`,
  `idempotency_request_in_progress`, `idempotency_result_unknown`, `idempotency_response_not_storable`,
  `idempotency_result_not_stored` (з `idempotency_key`, методом і шляхом).

## Помилки (`errors.py`)

Тіло: `{"error": {"code": "<ErrorCode>", "message": "<текст українською>", "details": ...}}` (`details` — лише якщо є,
напр. для `VALIDATION_ERROR`: `loc`, `type`, `msg` без введених значень). OpenAPI описує 401/403 для кожної
операції і 400/409/413/422/503 для змінюючих; 422 — схемою `ErrorResponse` (схеми FastAPI `HTTPValidationError`
прибрано).

| Код | HTTP | Коли |
| --- | --- | --- |
| `HOST_NOT_ALLOWED` | 403 | `Host` не loopback з портом API |
| `ORIGIN_NOT_ALLOWED` | 403 | `Origin` не з allowlist або крос-сайтовий запит без `Origin` |
| `AUTH_REQUIRED` / `AUTH_INVALID` | 401 | немає / неправильний токен |
| `IDEMPOTENCY_KEY_REQUIRED` / `IDEMPOTENCY_KEY_INVALID` | 400 | немає / некоректний ключ |
| `IDEMPOTENCY_KEY_REUSED` | 422 | ключ уже використано для іншого запиту |
| `IDEMPOTENCY_REQUEST_IN_PROGRESS` / `IDEMPOTENCY_RESULT_UNKNOWN` | 409 | див. таблицю ідемпотентності |
| `IDEMPOTENCY_STORE_UNAVAILABLE` | 503 | БД недоступна до виконання — дію не виконано |
| `REQUEST_TOO_LARGE` | 413 | тіло > 1 MiB |
| `VALIDATION_ERROR` | 422 | параметри не пройшли валідацію |
| `NOT_FOUND` / `METHOD_NOT_ALLOWED` / `HTTP_ERROR` | 404 / 405 / інший | помилки роутера; 405 — і для невідомого небезпечного методу |
| `INSTANCE_LOCK_LOST` | 503 | `require_instance_lock`: lock втрачено, процес завершується |
| `INTERNAL_ERROR` | 500 | необроблений виняток |

## `GET /system/health`

200 `{"status": "ok", "time_ms", "uptime_ms", "checks": {"instance_lock": true, "database": true}}`; lock втрачено або
`SELECT 1` не пройшов за `HEALTH_DB_TIMEOUT_MS` = 2000 мс → 503 з `"status": "degraded"` і тими самими полями (текст
помилки БД назовні не віддається; у лог — `api_health_db_failed` з типом помилки). Потрібен токен.

## Аудит (`audit.py`)

`AuditWriter.write(session, *, action, actor="api", account_id, bot_id, target, reason, confirmed=False,
idempotency_key, request, result) -> id` додає запис у **сесію викликача** (flush, без коміту): небезпечна дія (T6.2) і
її підтверджений запис — в одній транзакції, тригер `ctb_require_confirmed_audit()` бачить запис до коміту, а відкат
дії відкочує й аудит. `action` перевіряється до БД (`^[a-z][a-z0-9_.]*$`), `actor` не порожній. `request`/`result`
маскуються `SecretMasker.mask` з усіма секретами `Settings` (чутливі назви полів → `***`, зареєстровані значення,
`Decimal` і `UUID` → рядок); `reason` і `target` — від зареєстрованих значень і шаблонів (`scrub_text`). `confirmed = true` — лише після явного підтвердження людини в UI (T6.2).

## Сервер (`server.py`)

- Сокет `AF_INET` на `127.0.0.1:API_PORT` (`SO_REUSEADDR`, backlog 128) береться **після** lock; зайнятий порт →
  `ApiBindError` → код виходу 16. Без `API_TOKEN` процес не доходить до lock (код 15, `app/__main__.py`).
- uvicorn: `log_config=None` (записи йдуть до root-хендлерів із маскуванням), `access_log=False`, `proxy_headers=False`
  (`X-Forwarded-*` не довіряються), `server_header=False`, `lifespan="off"` (ресурси створює `serve_api`), `ws="none"`
  (до T6.3), `timeout_graceful_shutdown` = `GRACEFUL_SHUTDOWN_S` = 2 с. `capture_signals()` вимкнено: SIGTERM/SIGINT
  обробляє `_run` (`app/__main__.py`).
- Пул БД API: `pool_timeout` 5 с, підключення asyncpg 5 с, `command_timeout` 10 с — запит не висить на недоступній БД.
- `limit_concurrency` не задано (див. «Безпека запиту»).
- Зупинка (сигнал або втрата lock → скасування роботи): `should_exit` (якщо lock уже втрачено — ще й `force_exit`:
  без graceful-очікування; обробники скасовує сам `serve_api` на початку зупинки, вони лише дописують прибирання) → очікування сервера `SERVER_STOP_TIMEOUT_MS` =
  3000 мс (повторне скасування дедлайн не скорочує; не встиг — `force_exit` і скасування задачі сервера) → очікування
  **задач запитів** (`server_state.tasks`; ще не скасовані — скасовуються, уже скасовані uvicorn-ом не скасовуються
  вдруге, щоб не перервати їхнє прибирання) до `SERVER_FORCE_STOP_MS` = 500 мс → закриття пулу (`DISPOSE_TIMEOUT_MS` =
  1000 мс). Разом < `BODY_STOP_TIMEOUT_MS` (5 с, T0.6). `serve_api` **не повертається, поки живі сервер або обробники**
  (інакше обробник дописував би роботу вже після звільнення lock): не встигли — CRITICAL `api_server_stuck` і очікування
  без дедлайну, а дедлайн тримає `run_exclusive` (`on_stuck` → аварійний вихід). Ключ скасованого запиту лишається
  `IN_PROGRESS`.
- Сервер зупинився без сигналу → `ApiStoppedUnexpectedly` → код 1 (служба перезапустить).
- Події логу: `api_starting`, `api_stopped`, `api_stop_timeout`, `api_server_stuck`, `api_server_failed_on_stop`,
  `api_cancel_during_stop`, `api_engine_dispose_failed`.

## Таблиці БД

- `audit_log` — дії користувача: `actor`, `action` (`^[a-z][a-z0-9_.]*$`), `account_id`/`bot_id` (як дані, без FK —
  журнал не залежить від інших таблиць), `target` (інша ціль дії, напр. `model_version`), `reason`, `confirmed`, `idempotency_key`, `request`/`result` (JSONB, без
  секретів), `ts_ms`. **Append-only**: UPDATE/DELETE/TRUNCATE відхиляє тригер `ctb_forbid_mutation()`.
- На записи `audit_log` посилаються небезпечні дії через `confirmed_audit_id`: `risk_config` (послаблення
  лімітів, дія `risk.config.loosen`), `emergency_runs` (вихід з Emergency Stop, `emergency.release`), `models`
  (`APPROVED`, `model.approve`). Тригер `ctb_require_confirmed_audit()` пропускає лише запис із
  `confirmed = true`, саме цією дією і тією самою ціллю: `account_id` акаунта (`risk_config`, `emergency_runs`)
  або `target = model_version` (`models`). Один запис підтверджує лише одну дію (частковий
  `UNIQUE (confirmed_audit_id)` у кожній із трьох таблиць). `confirmed = true` пише лише API після підтвердження в UI (T6.2)
  і торговий процес при ручному перехопленні lock після введення `TAKEOVER` у терміналі (`instance.force_takeover`, T0.6).
- `api_idempotency_keys` (міграція `0002`, T0.7) — змінюючі запити API: `idempotency_key` (PK, CHECK формату),
  `method` (`POST`/`PUT`/`PATCH`/`DELETE`), `path`, `request_hash` (SHA-256 hex), `owner` (`instance_id` процесу),
  `status` (`IN_PROGRESS`/`COMPLETED`/`FAILED`), `response_status` (100…599), `response_headers` (JSONB, пари),
  `response_body` (BYTEA), `created_ms`, `completed_ms`. CHECK-и: `completed_ms` є тоді й лише тоді, коли статус
  не `IN_PROGRESS`; відповідь є тоді й лише тоді, коли `COMPLETED`. Переходи лише з `IN_PROGRESS` того самого
  `owner` (умовний `UPDATE` у `DbIdempotencyStore`) і тригер `ctb_api_idempotency_guard()`: ідентичність запиту
  незмінна, із `COMPLETED`/`FAILED` переходів немає (`restrict_violation`). DELETE тригер дозволяє, але майбутнє
  очищення може видаляти лише старі `COMPLETED` (дні), а не `FAILED`/`IN_PROGRESS`: видалений рядок дозволив би
  автоматичному повтору UI з тим самим ключем виконати дію вдруге.

Схема створюється міграціями `migrations/versions/0001_initial_schema.py` і `0002_api_idempotency_keys.py`;
обмеження й тригери перевіряють `tests/db/test_constraints.py`, `tests/db/test_guards.py`,
`tests/db/test_api_idempotency_db.py` і `tests/db/test_migrations.py`.

## Тести

- `tests/api/` (unit, без БД; фейки — `tests/api/fakes.py`): `test_security.py` (`Host`/`Origin` → 403, токен → 401,
  CORS, preflight, WebSocket, заголовки, логи без токена), `test_idempotency.py` (повтор не виконує дію двічі, 400/409/
  413/422/503, збої сховища), `test_health.py`, `test_app.py` (OpenAPI, формат помилок, `require_instance_lock`),
  `test_server.py` (реальний uvicorn на ефемерному порту: бінд лише 127.0.0.1, зайнятий порт, зупинка в межах 5 с,
  сигнали не перехоплено, логи uvicorn через root).
- `tests/db/test_api_idempotency_db.py`, `tests/db/test_audit_writer.py` (integration).

## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.4 | `[x]` | модель і таблиця `audit_log` (append-only) |
| T0.7 | `[x]` | каркас FastAPI: `127.0.0.1`, токен, `Host`/`Origin`, CORS, `Idempotency-Key` (таблиця `api_idempotency_keys`), `AuditWriter`, `/system/health` |
| T6.1 | `[ ]` | REST API |
| T6.2 | `[ ]` | Небезпечні дії в API |
| T6.3 | `[ ]` | WebSocket-канали для UI |
