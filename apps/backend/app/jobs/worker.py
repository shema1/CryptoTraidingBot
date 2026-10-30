"""Воркер задач: `python -m app.jobs.worker` (ТЗ §5.6, §6.4, T1.8).

Окремий процес без торгових ключів. На одній БД працює лише один воркер: частка rate limit воркера (ТЗ §6.5)
розрахована на один процес (`docs/decisions/0008-single-worker.md`). Тому при старті береться single-instance lock
`WORKER_LOCK_ID` (`app/core/instance_lock.py`), і цикл працює під його watchdog-ом (`run_exclusive`): другий
екземпляр чекає `LOCK_WAIT_S` і завершується, втрата lock зупиняє процес.

Бере задачі з `jobs` запитом `SELECT … FOR UPDATE SKIP LOCKED`, виконує їх по одній (тож GPU-задачі не
паралеляться) і оновлює `progress` та `heartbeat_at_ms`.

Зависла `RUNNING` (heartbeat старіший за `STALE_AFTER_MS`) на старті і перед кожною спробою взяти задачу
стає `FAILED` (`STALE_HEARTBEAT`). Так рядок звільняє `dedup_key`, і наступний прохід backfill ставить
нову задачу. `SIGTERM` повертає поточну задачу в `QUEUED` (backfill ідемпотентний). `kill -9` і втрата lock
цього не роблять: рядок лишається `RUNNING`, доки heartbeat не прострочиться.

Публічні свічки воркер читає з mainnet (режим `demo`: той самий публічний хост, що й live, без ключів).
Testnet для історії навчання не використовується.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import ctypes
import os
import signal
import sys
import uuid
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final, cast

import asyncpg
from sqlalchemy import text, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import now_ms
from app.core.config import Settings
from app.core.db import create_engine, session_factory
from app.core.instance_lock import (
    WORKER_APPLICATION_NAME_PREFIX,
    WORKER_LOCK_ID,
    InstanceLock,
    InstanceLockHeld,
    InstanceLockLost,
    InstanceLockUnavailable,
    run_exclusive,
)
from app.core.logging import configure_logging, get_logger, shutdown_logging
from app.data.backfill import (
    JOB_KIND_BACKFILL,
    BackfillIncomplete,
    MarketDataSource,
    SessionFactory,
    run_backfill_job,
)
from app.data.derivatives import (
    JOB_KIND_DERIVATIVES,
    DerivativesBackfillIncomplete,
    DerivativesSource,
    run_derivatives_job,
)
from app.exchange.bybit import BybitAdapter, ExchangeMode
from app.exchange.rate_limit import ProcessRole, RateLimiter
from app.jobs.models import Job

_LOG = get_logger("app.jobs.worker")

# Інтервал запису heartbeat. Прострочення — кілька пропущених ударів, один запізнілий запис задачу не валить.
HEARTBEAT_INTERVAL_MS: Final = 5_000
STALE_AFTER_MS: Final = 30_000
# Пауза, коли черга порожня. Є робота — береться одразу, без цієї паузи.
POLL_INTERVAL_MS: Final = 1_000
# На скільки підняти niceness (POSIX). Більше — нижчий пріоритет відносно торгового процесу.
NICE_INCREMENT: Final = 10
# Windows: нижче звичайного, але не IDLE (IDLE голодує під навантаженням ОС).
BELOW_NORMAL_PRIORITY_CLASS: Final = 0x00004000

ERROR_STALE_HEARTBEAT: Final = "STALE_HEARTBEAT"
ERROR_UNKNOWN_KIND: Final = "UNKNOWN_KIND"
ERROR_BACKFILL_INCOMPLETE: Final = "BACKFILL_INCOMPLETE"
ERROR_HANDLER: Final = "HANDLER_ERROR"
ERROR_BAD_PARAMS: Final = "BAD_PARAMS"
PROGRESS_MESSAGE_MAX_LEN: Final = 500

# Коди виходу — ті самі числа, що в торгового процесу (`app/__main__.py`).
EXIT_OK: Final = 0
EXIT_UNCAUGHT: Final = 1
EXIT_LOCK_HELD: Final = 10
EXIT_DB_UNAVAILABLE: Final = 11
EXIT_LOCK_LOST: Final = 12
EXIT_STOP_TIMEOUT: Final = 14
EXIT_CONFIG_INVALID: Final = 15

_CLAIM_SQL: Final = """
UPDATE jobs
SET status = 'RUNNING',
    started_at_ms = :now,
    heartbeat_at_ms = :now
WHERE id = (
    SELECT id
    FROM jobs
    WHERE status = 'QUEUED'
    ORDER BY priority, created_at_ms, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, kind, params
"""

_DB_ERRORS: Final[tuple[type[BaseException], ...]] = (
    OSError,
    TimeoutError,
    asyncpg.PostgresError,
    asyncpg.InterfaceError,
    SQLAlchemyError,
)

Sleep = Callable[[float], Coroutine[Any, Any, None]]


class WorkerDbUnavailable(Exception):
    """БД недоступна на першому проході черги. Текст — лише тип помилки, без DSN."""


class JobLost(RuntimeError):
    """Рядок уже не `RUNNING`: писати прогрес або heartbeat нікуди."""


def lower_os_priority() -> None:
    """Знизити пріоритет процесу. Не вийшло (немає прав) — воркер усе одно працює, у лог пишеться попередження."""
    if sys.platform == "win32":
        _lower_windows_priority()
        return
    try:
        new_nice = os.nice(NICE_INCREMENT)
    except OSError as exc:
        _LOG.warning("worker_priority_unchanged", error_type=type(exc).__name__)
        return
    _LOG.info("worker_priority_lowered", nice=new_nice, increment=NICE_INCREMENT)


def _lower_windows_priority() -> None:
    # WinDLL є лише в типах Windows; на інших ОС функцію не викликають (і тест її підміняє).
    windll: Any = getattr(ctypes, "WinDLL", None)
    last_error: Any = getattr(ctypes, "get_last_error", None)
    if windll is None or last_error is None:
        _LOG.warning("worker_priority_unchanged", error_type="OSError")
        return
    kernel: Any = windll("kernel32", use_last_error=True)
    if not kernel.SetPriorityClass(kernel.GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS):
        _LOG.warning("worker_priority_unchanged", error_type="OSError", winerror=last_error())
        return
    _LOG.info("worker_priority_lowered", priority_class="BELOW_NORMAL")


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    """Задача, вже записана як `RUNNING` у поточній транзакції викликача."""

    id: uuid.UUID
    kind: str
    params: dict[str, Any]


@dataclass
class JobContext:
    """Те, що бачить обробник. `report` комітить прогрес одразу, поза транзакцією біржових запитів."""

    job_id: uuid.UUID
    kind: str
    params: Mapping[str, Any]
    sessions: SessionFactory
    source: MarketDataSource

    async def report(self, progress: Decimal, message: str | None = None) -> None:
        await report_progress(self.sessions, self.job_id, progress, message)


# `Awaitable[Any]`: результат корутини в mypy інваріантний, тож `None` і `dict` інакше не збігаються.
JobHandler = Callable[[JobContext], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class WorkerMarket:
    """Публічний адаптер воркера. Ключів немає: частка IP — роль `WORKER`, хост — mainnet (`demo`)."""

    limiter: RateLimiter
    adapter: BybitAdapter

    async def aclose(self) -> None:
        await self.adapter.aclose()
        await self.limiter.aclose()


def open_worker_market(settings: Settings) -> WorkerMarket:
    """Адаптер backfill. `ExchangeMode.DEMO` — публічний mainnet; секретів у конструктор не передається."""
    limiter = RateLimiter.from_settings(settings, role=ProcessRole.WORKER)
    adapter = BybitAdapter(mode=ExchangeMode.DEMO, limiter=limiter)
    return WorkerMarket(limiter=limiter, adapter=adapter)


async def handle_backfill(ctx: JobContext) -> dict[str, int]:
    """`backfill_candles`. `BackfillIncomplete` навмисно летить далі: цикл запише `FAILED`."""
    stats = await run_backfill_job(ctx.sessions, ctx.source, ctx.params)
    return {"candles_inserted": stats.candles_inserted, "funding_inserted": stats.funding_inserted}


async def handle_derivatives(ctx: JobContext) -> dict[str, int]:
    """`backfill_derivatives`. `DerivativesBackfillIncomplete` летить далі: цикл запише `FAILED`."""
    stats = await run_derivatives_job(ctx.sessions, _as_derivatives(ctx.source), ctx.params)
    return stats.as_dict()


def default_handlers() -> dict[str, JobHandler]:
    return {JOB_KIND_BACKFILL: handle_backfill, JOB_KIND_DERIVATIVES: handle_derivatives}


_DERIVATIVE_METHODS: Final = (
    "fetch_open_interest",
    "fetch_premium_index",
    "fetch_mark_price_candles",
    "fetch_index_price_candles",
    "fetch_long_short_ratio",
)


def _as_derivatives(source: object) -> DerivativesSource:
    """`BybitAdapter` вміє і свічки, і деривативи. Джерело лише зі свічками — помилка обробника, не тихий пропуск."""
    if any(not callable(getattr(source, name, None)) for name in _DERIVATIVE_METHODS):
        raise TypeError("джерело воркера не вміє читати деривативи")
    return cast(DerivativesSource, source)


def _require_ms(name: str, value: int, *, minimum: int) -> None:
    if type(value) is not int or isinstance(value, bool) or value < minimum:
        raise ValueError(f"{name} має бути int ≥ {minimum}")


def _check_intervals(heartbeat_ms: int, stale_after_ms: int, poll_ms: int) -> None:
    _require_ms("heartbeat_ms", heartbeat_ms, minimum=1)
    _require_ms("stale_after_ms", stale_after_ms, minimum=heartbeat_ms * 3)
    _require_ms("poll_ms", poll_ms, minimum=1)


def _as_uuid(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    if isinstance(value, str):
        return uuid.UUID(value)
    raise TypeError(f"jobs.id: очікувався UUID, отримано {type(value).__name__}")


def _as_params(value: object) -> dict[str, Any] | None:
    """Об'єкт JSON. Список, число або рядок — не параметри задачі (`None`, цикл поставить `FAILED`)."""
    if isinstance(value, dict):
        return dict(value)
    return None


async def fail_stale_jobs(session: AsyncSession, *, stale_after_ms: int = STALE_AFTER_MS) -> list[uuid.UUID]:
    """`RUNNING`, чий heartbeat старіший за вікно, → `FAILED`. Свіжий heartbeat не чіпає.

    Викликати всередині транзакції викликача. Поріг — суворий: вік рівно `stale_after_ms` ще живий.
    """
    _require_ms("stale_after_ms", stale_after_ms, minimum=1)
    now = now_ms()
    rows = (
        await session.execute(
            update(Job)
            .where(Job.status == "RUNNING", Job.heartbeat_at_ms < now - stale_after_ms)
            .values(status="FAILED", error=ERROR_STALE_HEARTBEAT, finished_at_ms=now)
            .returning(Job.id, Job.heartbeat_at_ms)
        )
    ).all()
    failed: list[uuid.UUID] = []
    for job_id, heartbeat_at_ms in rows:
        _LOG.warning("job_stale", job_id=str(job_id), heartbeat_at_ms=heartbeat_at_ms)
        failed.append(job_id)
    return failed


async def claim_next(session: AsyncSession) -> tuple[ClaimedJob | None, bool]:
    """Взяти одну `QUEUED` задачу (`FOR UPDATE SKIP LOCKED`) і позначити `RUNNING`.

    Повертає `(задача, False)`, `(None, False)` коли черга порожня, або `(None, True)` коли рядок
    був непридатний і вже записаний як `FAILED` — викликач бере наступний без паузи.
    Викликати всередині транзакції.
    """
    row = (await session.execute(text(_CLAIM_SQL), {"now": now_ms()})).mappings().one_or_none()
    if row is None:
        return None, False
    job_id = _as_uuid(row["id"])
    kind = row["kind"]
    if type(kind) is not str:
        raise TypeError(f"jobs.kind: очікувався str, отримано {type(kind).__name__}")
    params = _as_params(row["params"])
    if params is None:
        await _fail_open(
            session,
            job_id,
            error=ERROR_BAD_PARAMS,
            result={"error_type": type(row["params"]).__name__},
        )
        _LOG.error("job_failed", job_id=str(job_id), kind=kind, error=ERROR_BAD_PARAMS)
        return None, True
    return ClaimedJob(id=job_id, kind=kind, params=params), False


async def report_progress(
    sessions: SessionFactory,
    job_id: uuid.UUID,
    progress: Decimal,
    message: str | None = None,
) -> None:
    """Записати прогрес і освіжити heartbeat. Рядок уже не `RUNNING` — `JobLost`."""
    if type(progress) is not Decimal or isinstance(progress, bool) or not progress.is_finite():
        raise ValueError("progress має бути скінченний Decimal")
    if progress < 0 or progress > 1:
        raise ValueError("progress має бути в межах [0, 1]")
    if message is not None and type(message) is not str:
        raise ValueError("progress_message має бути str")
    stored = None if message is None else message[:PROGRESS_MESSAGE_MAX_LEN]
    if not await _update_running(
        sessions,
        job_id,
        progress=progress,
        progress_message=stored,
        heartbeat_at_ms=now_ms(),
    ):
        raise JobLost(str(job_id))


async def touch_heartbeat(sessions: SessionFactory, job_id: uuid.UUID) -> bool:
    """Освіжити heartbeat. `False` — рядка в `RUNNING` уже немає."""
    return await _update_running(sessions, job_id, heartbeat_at_ms=now_ms())


async def run_worker(
    sessions: SessionFactory,
    source: MarketDataSource,
    *,
    handlers: Mapping[str, JobHandler] | None = None,
    stop: asyncio.Event | None = None,
    sleep: Sleep = asyncio.sleep,
    heartbeat_ms: int = HEARTBEAT_INTERVAL_MS,
    stale_after_ms: int = STALE_AFTER_MS,
    poll_ms: int = POLL_INTERVAL_MS,
) -> None:
    """Цикл черги. Перший прохід знімає прострочені `RUNNING` навіть якщо зупинка вже запитана.

    Порожня черга — пауза `poll_ms`. Помилка БД на першому проході — `WorkerDbUnavailable`; далі — пауза і повтор,
    а коли зупинку вже запитано — вихід без повтору (інакше `_wait` не чекає і цикл крутився б без паузи).
    """
    _check_intervals(heartbeat_ms, stale_after_ms, poll_ms)
    registry = dict(default_handlers() if handlers is None else handlers)
    stopping = asyncio.Event() if stop is None else stop
    first = True
    while True:
        try:
            stale_ids = await _sweep(sessions, stale_after_ms)
        except _DB_ERRORS as exc:
            _db_down(exc, first=first)
            if stopping.is_set():
                return
            await _wait(stopping, poll_ms, sleep)
            continue
        if first:
            _LOG.info("worker_ready", stale_failed=len(stale_ids))
            first = False
        if stopping.is_set():
            return
        try:
            claimed, discarded = await _claim_one(sessions)
        except _DB_ERRORS as exc:
            _db_down(exc, first=False)
            if stopping.is_set():
                return
            await _wait(stopping, poll_ms, sleep)
            continue
        if claimed is None:
            if not discarded:
                await _wait(stopping, poll_ms, sleep)
            continue
        try:
            await _dispatch(
                sessions,
                source,
                claimed,
                registry,
                stop=stopping,
                sleep=sleep,
                heartbeat_ms=heartbeat_ms,
            )
        except _DB_ERRORS as exc:
            _LOG.error("job_execution_db_error", job_id=str(claimed.id), error_type=type(exc).__name__)


async def serve(
    settings: Settings,
    *,
    handlers: Mapping[str, JobHandler] | None = None,
    source: MarketDataSource | None = None,
) -> int:
    """Взяти lock воркера, підняти адаптер і крутити чергу під watchdog-ом lock до SIGTERM/SIGINT.

    Повертає код виходу. Втрата lock скасовує цикл (задача лишається `RUNNING`) і дає `EXIT_LOCK_LOST`.
    """
    lower_os_priority()
    try:
        url = settings.database_url()
    except ValueError:
        _LOG.error("worker_db_not_configured")
        return EXIT_CONFIG_INVALID
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal, sig, stop)
        except NotImplementedError:
            _LOG.warning("shutdown_signal_unsupported", signal=sig.name)
        else:
            installed.append(sig)
    lock = InstanceLock(url, lock_id=WORKER_LOCK_ID, application_name_prefix=WORKER_APPLICATION_NAME_PREFIX)
    engine = create_engine(url)
    sessions = session_factory(engine)
    market: WorkerMarket | None = None
    try:
        _LOG.info(
            "worker_starting",
            lock_wait_s=settings.lock_wait_s,
            heartbeat_ms=HEARTBEAT_INTERVAL_MS,
            stale_after_ms=STALE_AFTER_MS,
        )
        if not await _acquire_unless_stopped(lock, settings.lock_wait_s * 1000, stop):
            return EXIT_OK
        if source is None:
            market = open_worker_market(settings)
            source = market.adapter
        jobs_source = source
        await run_exclusive(
            lock,
            lambda: run_worker(sessions, jobs_source, handlers=handlers, stop=stop),
            on_stuck=_exit_stuck,
        )
        return EXIT_OK
    except InstanceLockHeld:
        return EXIT_LOCK_HELD
    except (InstanceLockUnavailable, WorkerDbUnavailable):
        return EXIT_DB_UNAVAILABLE
    except InstanceLockLost:
        return EXIT_LOCK_LOST
    finally:
        await lock.release()
        if market is not None:
            await market.aclose()
        await engine.dispose()
        for sig in installed:
            loop.remove_signal_handler(sig)


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входу `python -m app.jobs.worker`."""
    _parse_args(argv)
    settings = Settings()
    configure_logging(settings, process="worker")
    try:
        code = asyncio.run(serve(settings))
        _LOG.info("worker_exit", exit_code=code)
        return code
    except (SystemExit, KeyboardInterrupt):
        raise
    except BaseException:  # noqa: BLE001 — маскований лог і код 1; текст винятку в stderr без маски не потрапляє
        _LOG.critical("uncaught_exception", exc_info=True)
        return EXIT_UNCAUGHT
    finally:
        shutdown_logging()


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m app.jobs.worker",
        description="Воркер задач Crypto Trading Bot (Optuna, навчання, backfill). Не торгує.",
        allow_abbrev=False,
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _on_signal(sig: signal.Signals, stop: asyncio.Event) -> None:
    if not stop.is_set():
        _LOG.info("shutdown_signal", signal=sig.name)
        stop.set()


async def _acquire_unless_stopped(lock: InstanceLock, wait_ms: int, stop: asyncio.Event) -> bool:
    """Взяти lock воркера. `False` — SIGTERM/SIGINT прийшов раніше: спробу скасовано, з'єднання lock закрито."""
    acquiring = asyncio.create_task(lock.acquire(wait_ms=wait_ms), name="worker-lock-acquire")
    stopper = asyncio.create_task(stop.wait(), name="worker-lock-stop")
    try:
        await asyncio.wait({acquiring, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        await _cancel_tasks(stopper, acquiring)
    if acquiring.cancelled():
        return False
    acquiring.result()
    return True


def _exit_stuck(reason: str) -> None:
    """Робота не зупинилась після скасування: аварійний вихід (ОС закриє сокет, сервер звільнить lock)."""
    code = EXIT_LOCK_LOST if reason == "lock_lost" else EXIT_STOP_TIMEOUT
    _LOG.critical("worker_stop_timeout", reason=reason, exit_code=code)
    shutdown_logging()
    os._exit(code)


def _db_down(exc: BaseException, *, first: bool) -> None:
    _LOG.error("worker_db_unavailable", error_type=type(exc).__name__)
    if first:
        raise WorkerDbUnavailable(type(exc).__name__) from None


async def _sweep(sessions: SessionFactory, stale_after_ms: int) -> list[uuid.UUID]:
    async with sessions() as session, session.begin():
        return await fail_stale_jobs(session, stale_after_ms=stale_after_ms)


async def _claim_one(sessions: SessionFactory) -> tuple[ClaimedJob | None, bool]:
    async with sessions() as session, session.begin():
        return await claim_next(session)


async def _dispatch(
    sessions: SessionFactory,
    source: MarketDataSource,
    claimed: ClaimedJob,
    handlers: Mapping[str, JobHandler],
    *,
    stop: asyncio.Event,
    sleep: Sleep,
    heartbeat_ms: int,
) -> None:
    handler = handlers.get(claimed.kind)
    if handler is None:
        _LOG.error("job_failed", job_id=str(claimed.id), kind=claimed.kind, error=ERROR_UNKNOWN_KIND)
        await _finish_failed(sessions, claimed.id, error=ERROR_UNKNOWN_KIND, result={"kind": claimed.kind})
        return
    _LOG.info("job_started", job_id=str(claimed.id), kind=claimed.kind)
    await _execute(sessions, source, claimed, handler, stop=stop, sleep=sleep, heartbeat_ms=heartbeat_ms)


async def _execute(
    sessions: SessionFactory,
    source: MarketDataSource,
    claimed: ClaimedJob,
    handler: JobHandler,
    *,
    stop: asyncio.Event,
    sleep: Sleep,
    heartbeat_ms: int,
) -> None:
    ctx = JobContext(
        job_id=claimed.id,
        kind=claimed.kind,
        params=claimed.params,
        sessions=sessions,
        source=source,
    )
    lost = asyncio.Event()

    async def beat() -> None:
        try:
            while not stop.is_set() and not lost.is_set():
                await sleep(heartbeat_ms / 1000)
                if stop.is_set() or lost.is_set():
                    return
                if not await touch_heartbeat(sessions, claimed.id):
                    _LOG.warning("job_heartbeat_lost", job_id=str(claimed.id), kind=claimed.kind)
                    lost.set()
                    return
        except _DB_ERRORS as exc:
            _LOG.error("job_heartbeat_failed", job_id=str(claimed.id), error_type=type(exc).__name__)
            lost.set()
        except Exception as exc:  # noqa: BLE001 — без heartbeat задачу має підхопити прострочення, а не живий цикл
            _LOG.error(
                "job_heartbeat_failed",
                job_id=str(claimed.id),
                error_type=type(exc).__name__,
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            lost.set()

    beat_task = asyncio.create_task(beat(), name=f"job-heartbeat-{claimed.id}")
    work_task = asyncio.create_task(_call_handler(handler, ctx), name=f"job-{claimed.kind}-{claimed.id}")
    stop_task = asyncio.create_task(stop.wait(), name=f"job-stop-{claimed.id}")
    lost_task = asyncio.create_task(lost.wait(), name=f"job-lost-{claimed.id}")
    try:
        await asyncio.wait({work_task, stop_task, lost_task}, return_when=asyncio.FIRST_COMPLETED)
        if work_task.done() and not work_task.cancelled():
            exc = work_task.exception()
            if exc is None:
                try:
                    stored = _as_result(work_task.result())
                except TypeError as bad:
                    await _finish_exc(sessions, claimed, bad)
                    return
                await _finish_ok(sessions, claimed, stored)
            else:
                await _finish_exc(sessions, claimed, exc)
            return
        work_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await work_task
        if stop.is_set():
            await _requeue(sessions, claimed)
    finally:
        # Уже завершену роботу не await-имо вдруге: інакше її виняток вилетів би ще раз після FAILED/DONE.
        await _cancel_tasks(beat_task, stop_task, lost_task, work_task)


async def _call_handler(handler: JobHandler, ctx: JobContext) -> Any:
    return await handler(ctx)


def _as_result(raw: object) -> Mapping[str, Any] | None:
    """JSON-об'єкт або `None`. Список і чужі ключі — `TypeError`, задача стане `FAILED`."""
    if raw is None:
        return None
    if not isinstance(raw, Mapping) or any(type(key) is not str for key in raw):
        raise TypeError("результат обробника має бути JSON-об'єктом або None")
    return cast(Mapping[str, Any], raw)


async def _finish_ok(sessions: SessionFactory, claimed: ClaimedJob, result: Mapping[str, Any] | None) -> None:
    stored = None if result is None else dict(result)
    wrote = await _update_running(
        sessions,
        claimed.id,
        status="DONE",
        progress=Decimal(1),
        result=stored,
        error=None,
        finished_at_ms=now_ms(),
    )
    if wrote:
        _LOG.info("job_done", job_id=str(claimed.id), kind=claimed.kind)
        return
    _LOG.warning("job_finish_skipped", job_id=str(claimed.id), kind=claimed.kind, status="DONE")


async def _finish_exc(sessions: SessionFactory, claimed: ClaimedJob, exc: BaseException) -> None:
    if isinstance(exc, DerivativesBackfillIncomplete):
        _LOG.error(
            "job_failed",
            job_id=str(claimed.id),
            kind=claimed.kind,
            error=ERROR_BACKFILL_INCOMPLETE,
            failed_symbols=list(exc.failed_symbols),
        )
        await _finish_failed(
            sessions,
            claimed.id,
            error=ERROR_BACKFILL_INCOMPLETE,
            result={**exc.stats.as_dict(), "failed_symbols": list(exc.failed_symbols)},
        )
        return
    if isinstance(exc, BackfillIncomplete):
        _LOG.error(
            "job_failed",
            job_id=str(claimed.id),
            kind=claimed.kind,
            error=ERROR_BACKFILL_INCOMPLETE,
            failed_symbols=list(exc.failed_symbols),
        )
        await _finish_failed(
            sessions,
            claimed.id,
            error=ERROR_BACKFILL_INCOMPLETE,
            result={
                "candles_inserted": exc.stats.candles_inserted,
                "funding_inserted": exc.stats.funding_inserted,
                "failed_symbols": list(exc.failed_symbols),
            },
        )
        return
    if isinstance(exc, JobLost):
        _LOG.warning("job_finish_skipped", job_id=str(claimed.id), kind=claimed.kind, status="RUNNING")
        return
    if not isinstance(exc, Exception):
        raise exc
    _LOG.error(
        "job_failed",
        job_id=str(claimed.id),
        kind=claimed.kind,
        error=ERROR_HANDLER,
        error_type=type(exc).__name__,
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    await _finish_failed(sessions, claimed.id, error=ERROR_HANDLER, result={"error_type": type(exc).__name__})


async def _finish_failed(
    sessions: SessionFactory,
    job_id: uuid.UUID,
    *,
    error: str,
    result: dict[str, Any] | None,
) -> None:
    wrote = await _update_running(
        sessions,
        job_id,
        status="FAILED",
        error=error,
        result=result,
        finished_at_ms=now_ms(),
    )
    if not wrote:
        _LOG.warning("job_finish_skipped", job_id=str(job_id), status="FAILED")


async def _requeue(sessions: SessionFactory, claimed: ClaimedJob) -> None:
    wrote = await _update_running(
        sessions,
        claimed.id,
        status="QUEUED",
        started_at_ms=None,
        heartbeat_at_ms=None,
    )
    if wrote:
        _LOG.info("job_requeued", job_id=str(claimed.id), kind=claimed.kind)
        return
    _LOG.warning("job_finish_skipped", job_id=str(claimed.id), kind=claimed.kind, status="QUEUED")


async def _fail_open(session: AsyncSession, job_id: uuid.UUID, *, error: str, result: dict[str, Any]) -> None:
    await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == "RUNNING")
        .values(status="FAILED", error=error, result=result, finished_at_ms=now_ms())
    )


async def _update_running(sessions: SessionFactory, job_id: uuid.UUID, **values: Any) -> bool:
    async with sessions() as session, session.begin():
        found = await session.execute(
            update(Job).where(Job.id == job_id, Job.status == "RUNNING").values(**values).returning(Job.id)
        )
        return found.scalar_one_or_none() is not None


async def _cancel_tasks(*tasks: asyncio.Task[Any]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    for task in tasks:
        if task.done() and not task.cancelled():
            task.exception()
            continue
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _wait(stop: asyncio.Event, poll_ms: int, sleep: Sleep) -> None:
    sleeper = asyncio.create_task(sleep(poll_ms / 1000))
    stopper = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (sleeper, stopper):
            task.cancel()
        for task in (sleeper, stopper):
            with contextlib.suppress(asyncio.CancelledError):
                await task


if __name__ == "__main__":
    raise SystemExit(main())
