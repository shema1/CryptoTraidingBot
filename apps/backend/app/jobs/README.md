# `app/jobs` — Воркер задач

## Призначення

Окремий процес `python -m app.jobs.worker`: бере задачі з таблиці `jobs` (`SELECT … FOR UPDATE SKIP LOCKED`),
виконує їх по одній, пише прогрес і `heartbeat_at_ms`. Не торгує і не читає торгові ключі. З торговим процесом
спілкується лише через таблицю `jobs`.

На одній БД працює **лише один воркер** (`docs/decisions/0008-single-worker.md`): частка IP-ліміту Bybit
`RATE_LIMIT_SHARE_WORKER_PCT` (ТЗ §6.5) розрахована на один процес, а два воркери разом з торговим процесом і
колектором перевищили б ліміт (бан IP зачепив би і SL торгового процесу). Тому воркер тримає single-instance lock
`WORKER_LOCK_ID` = `2` у просторі `LOCK_NAMESPACE` = `0x63746200` (`app/core/instance_lock.py`, той самий механізм,
що в торгового процесу з `BACKEND_LOCK_ID` = `1`): окрема сесія з серверними keepalive, `application_name` =
`ctb-worker pid=<pid ОС>`. Колектор з тієї самої причини тримає `COLLECTOR_LOCK_ID` = `3`
(`app/data/collector.py`). Задачі йдуть по одній, тож і GPU-задачі (`device: cuda`, ТЗ §5.6) не паралеляться.
`SKIP LOCKED` лишається захистом черги від подвійного взяття.

> Заплановано: T2.10 — Optuna як `kind` цього ж воркера.

## Розділи ТЗ

- ТЗ §5.6 — Optuna, навчання, `device: cpu | cuda`
- ТЗ §6.4 — процес воркера, `SKIP LOCKED`, пріоритет ОС, прострочена `RUNNING`
- ТЗ §6.5 — частка rate limit воркера

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `main`, `serve` | `worker.py` | `python -m app.jobs.worker`. `serve` бере lock воркера (до `LOCK_WAIT_S`), крутить чергу під watchdog-ом lock (`run_exclusive`) до SIGTERM/SIGINT і повертає код виходу |
| `run_worker` | `worker.py` | цикл: зняти прострочені `RUNNING`, взяти одну `QUEUED`, виконати. Порожня черга або збій БД — пауза `POLL_INTERVAL_MS`; збій БД, коли зупинку вже запитано, — вихід без повтору |
| `claim_next` | `worker.py` | `FOR UPDATE SKIP LOCKED`. Повертає `(задача, False)`, `(None, False)` якщо черга порожня, `(None, True)` якщо рядок уже записаний `FAILED` (`BAD_PARAMS`) і треба брати наступний без паузи |
| `fail_stale_jobs` | `worker.py` | `RUNNING`, чий `heartbeat_at_ms` старіший за `STALE_AFTER_MS`, → `FAILED` / `STALE_HEARTBEAT`. Вік рівно поріг ще живий |
| `handle_backfill`, `handle_derivatives`, `default_handlers` | `worker.py` | `backfill_candles` → `run_backfill_job`; `backfill_derivatives` → `run_derivatives_job`. Словник `handlers=` у `serve` / `run_worker` замінює цей набір, а не доповнює |
| `open_worker_market` | `worker.py` | публічний `BybitAdapter` режиму `demo` (mainnet без ключів) і `RateLimiter` з роллю `WORKER` |
| `JobContext.report` | `worker.py` | прогрес `Decimal` у `[0, 1]` і `progress_message` (обрізається до 500 символів); разом оновлює heartbeat |
| `lower_os_priority` | `worker.py` | POSIX `nice(+NICE_INCREMENT)`; Windows — `BELOW_NORMAL`. Немає прав — попередження в лог, процес працює далі |
| `Job`, `JOB_STATUSES` | `models.py` | таблиця `jobs` |

Константи часу й пріоритету — у `worker.py` (тест фіксує значення): heartbeat `HEARTBEAT_INTERVAL_MS` = 5000,
прострочення `STALE_AFTER_MS` = 30000 (не менше трьох heartbeat), пауза порожньої черги `POLL_INTERVAL_MS` = 1000,
`NICE_INCREMENT` = 10. Ключ lock — `WORKER_LOCK_ID` у `app/core/instance_lock.py`.

## Стани та інваріанти

`QUEUED` → `RUNNING` (у тій самій транзакції, що й `SKIP LOCKED`, до виклику обробника) → `DONE` або `FAILED`.
`CANCELED` воркер сам не ставить. `RUNNING` має `started_at_ms` і `heartbeat_at_ms`. `finished_at_ms` заданий
лише для `DONE` / `FAILED` / `CANCELED`.

Пріоритет: менше число — раніше; за рівних — `created_at_ms`, потім `id`.

Прострочення знімається на початку кожної ітерації, включно з першою після старту. Свіжий heartbeat не чіпається.
Після `FAILED` частковий `UNIQUE` на `dedup_key` цей рядок уже не тримає: наступний `enqueue_backfill_job` ставить
нову задачу. Сам воркер `FAILED` назад у чергу не кладе.

Lock воркера: старт повторює спроби кожну секунду до `LOCK_WAIT_S` (зокрема поки БД ще не піднялась); зайнятий весь
цей час — `instance_lock_held_by_other` з власником (`ctb-worker pid=…`) і код 10, задачі не беруться. SIGTERM /
SIGINT під час очікування — код 0. Під час роботи watchdog lock (закриття з'єднання, heartbeat 5 с) при втраті
(рестарт БД, обрив) скасовує цикл, поточна задача лишається `RUNNING`, процес завершується з кодом 12 — так
другий воркер не може стартувати, поки перший ще ходить до біржі. Задачу потім знімає прострочення heartbeat.

`SIGTERM` / `SIGINT` повертають поточну задачу в `QUEUED` (очищають `started_at_ms` і `heartbeat_at_ms`).
Backfill ідемпотентний, тож повтор безпечний. `kill -9` прибирання не робить: рядок лишається `RUNNING`, lock
звільняється разом із сесією, і наступний старт ставить `FAILED`, щойно heartbeat стане старішим за 30 с.
Якщо під час зупинки БД недоступна, процес однаково завершується з кодом 0 і БД більше не опитує; задача, яку не
вдалося повернути в `QUEUED`, лишається `RUNNING` і так само стає `FAILED` за простроченим heartbeat.

Коди `jobs.error` (текст винятку і DSN у рядок не пишуться):

| Код | Коли |
| --- | --- |
| `STALE_HEARTBEAT` | `RUNNING` без свіжого heartbeat |
| `UNKNOWN_KIND` | для `kind` немає обробника |
| `BAD_PARAMS` | `params` — не JSON-об'єкт |
| `BACKFILL_INCOMPLETE` | біржа відхилила частину символів свічок або деривативів; решту вже докачано (`result.failed_symbols`). Свічки: `candles_inserted`, `funding_inserted`. Деривативи: лічильники п'яти рядів |
| `HANDLER_ERROR` | інший виняток обробника (`result.error_type` — лише ім'я класу) |

Успіх: `progress = 1`, `result` — JSON-об'єкт з обробника або `null`, `error = null`, статус `DONE`.

Коди виходу (`worker.py`; ті самі числа, що в торгового процесу):

| Код | Константа | Значення |
| --- | --- | --- |
| 0 | `EXIT_OK` | зупинка за SIGTERM/SIGINT (зокрема під час очікування lock або коли БД у цей момент недоступна) |
| 1 | `EXIT_UNCAUGHT` | необроблений виняток, маскований `uncaught_exception` |
| 2 | — | зайві аргументи (argparse) |
| 10 | `EXIT_LOCK_HELD` | lock воркера тримає інший процес весь `LOCK_WAIT_S` — другий воркер не стартує |
| 11 | `EXIT_DB_UNAVAILABLE` | БД недоступна весь `LOCK_WAIT_S` або на першому проході черги |
| 12 | `EXIT_LOCK_LOST` | lock втрачено під час роботи; поточна задача лишається `RUNNING` до прострочення heartbeat |
| 14 | `EXIT_STOP_TIMEOUT` | цикл не зупинився за `BODY_STOP_TIMEOUT_MS` (5 с) після зовнішнього скасування — аварійний вихід |
| 15 | `EXIT_CONFIG_INVALID` | немає `POSTGRES_PASSWORD` |

Подія логів: `worker_starting`, `worker_ready` (`stale_failed`), `job_started`, `job_done`, `job_failed`,
`job_requeued`, `job_stale`, `shutdown_signal`, `worker_stop_timeout`, `worker_exit`; події lock —
`instance_lock_*` (`app/core/README.md`). Процес у `configure_logging` — `worker` (тека `logs/worker/`).

## Конфігурація

Нових змінних немає. Пароль і адреса БД — ті самі, що в торгового процесу (`POSTGRES_*`). Очікування lock при
старті — `LOCK_WAIT_S` (спільна з торговим процесом, за замовчуванням 60). Частка IP —
`RATE_LIMIT_SHARE_WORKER_PCT` (за замовчуванням 30). Роль лімітера задає код (`ProcessRole.WORKER`), не `.env`.
`API_TOKEN` і ключі Bybit воркер не читає. Інтервали heartbeat, прострочення, опитування і nice — константи
модуля, не поля `Settings`.

## Тести

- `tests/jobs/test_worker.py` — пріоритет ОС, ключ lock воркера і коди виходу, відсутність ключів у коді, публічний mainnet і роль `WORKER`; збій БД на першому проході, під час роботи (пауза й повтор) і під час зупинки (вихід без повтору).
- `tests/jobs/test_worker_db.py` (`integration`) — два сеанси не беруть один рядок; прострочена `RUNNING`; backfill `DONE` / `BACKFILL_INCOMPLETE`; heartbeat і прогрес.
- `tests/jobs/test_worker_process.py` (`integration` + `slow`) — другий воркер не стартує (код 10, задачу не бере); SIGTERM під час очікування lock — код 0; втрата lock — код 12, задача `RUNNING`; `kill -9` лишає `RUNNING`, наступний старт бере lock і ставить `FAILED`; SIGTERM повертає задачу в `QUEUED`.

## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.4 | `[x]` | таблиця `jobs` |
| T1.8 | `[x]` | процес воркера (один на БД, lock `WORKER_LOCK_ID`), `backfill_candles`, прострочений heartbeat |
| T1.9 | `[x]` | той самий процес виконує `backfill_derivatives` (пріоритет 210, після свічок) |
| T2.10 | `[ ]` | Optuna як задача воркера |
