"""Колектор даних деривативів і ліквідацій: `python -m app.data.collector` (ТЗ §4.3, §4.4, §6.4, §6.5, T1.10).

Окремий процес без торгових ключів. Слухає публічний WS Bybit (`allLiquidation`) для `research_symbols`,
агрегує події по годинах у `liquidations_hourly` і пише простій у `data_gaps`. Історію ліквідацій заднім числом не
завантажити, тож процес має працювати безперервно. Поруч за розкладом `DerivativesSync` докачує хвіст деривативів
(OI, premium, mark/index, long/short); довшу історію той віддає воркеру задачею `backfill_derivatives`.

Читання потоку не чекає БД: години й прогалини пише окрема задача (`_Writer`), помилки БД — з backoff. Межі години
рахуються за локальним годинником, тому колектор тримає власний `ClockGuard` (ТЗ §4.3). Поки годиннику не вірять,
години не закриваються, а деривативи не качаються. Увесь REST колектора (`fetch_time`, деривативи) іде через лімітер
ролі `COLLECTOR` (частка ТЗ §6.5).

На одній БД — один колектор (`COLLECTOR_LOCK_ID`): частка rate limit колектора розрахована на один процес.
Хост — публічний mainnet (`ExchangeMode.DEMO`), як у воркера.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import random
import signal
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Final

import asyncpg
from sqlalchemy.exc import SQLAlchemyError

from app.core.clock import monotonic_ms, now_ms
from app.core.config import Settings
from app.core.db import create_engine, session_factory
from app.core.instance_lock import (
    COLLECTOR_APPLICATION_NAME_PREFIX,
    COLLECTOR_LOCK_ID,
    InstanceLock,
    InstanceLockHeld,
    InstanceLockLost,
    InstanceLockUnavailable,
    run_exclusive,
)
from app.core.logging import configure_logging, get_logger, shutdown_logging
from app.data.backfill import SessionFactory
from app.data.derivatives import DerivativesSync, DerivativesSyncResult
from app.data.liquidations import (
    ClockView,
    LiquidationBook,
    clock_trusted,
    save_liquidations,
    sync_liquidation_gaps,
)
from app.data.time_sync import ClockGuard, SleepMs
from app.exchange.bybit import BybitAdapter, ExchangeMode
from app.exchange.errors import ExchangeError, ExchangeUnavailable
from app.exchange.liquidations import (
    PING_INTERVAL_MS,
    STALE_AFTER_MS,
    SUBSCRIBE_TIMEOUT_MS,
    AiohttpLiquidationConnector,
    LiquidationConnector,
    LiquidationEvent,
    LiquidationSocket,
    Sleep,
    liquidation_stream_url,
    read_liquidation_session,
)
from app.exchange.rate_limit import ProcessRole, RateLimiter

_LOG = get_logger(__name__)

RECONNECT_BASE_MS: Final = 1_000
RECONNECT_CAP_MS: Final = 60_000
GAP_SYNC_INTERVAL_MS: Final = 60_000
# Пауза після помилки БД: подвоюється до стелі. Поки вона триває, запис не пробується (і лог не повторюється).
DB_RETRY_BASE_MS: Final = 1_000
DB_RETRY_CAP_MS: Final = 60_000
# Останній запис при зупинці. Менше за `BODY_STOP_TIMEOUT_MS`: після втрати lock робота встигає зупинитись.
FINAL_FLUSH_TIMEOUT_MS: Final = 3_000
# Як часто питати розклад деривативів (`run_if_due` нічого не робить, якщо ще рано). Сам інтервал —
# `BACKFILL_INTERVAL_S`, пауза після збою — `DerivativesSync`.
DERIVATIVES_POLL_MS: Final = 5_000
_BACKOFF_SHIFT_CAP: Final = 16

EXIT_OK: Final = 0
EXIT_UNCAUGHT: Final = 1
EXIT_LOCK_HELD: Final = 10
EXIT_DB_UNAVAILABLE: Final = 11
EXIT_LOCK_LOST: Final = 12
EXIT_STOP_TIMEOUT: Final = 14
EXIT_CONFIG_INVALID: Final = 15

_DB_ERRORS: Final[tuple[type[BaseException], ...]] = (
    OSError,
    TimeoutError,
    asyncpg.PostgresError,
    asyncpg.InterfaceError,
    SQLAlchemyError,
)

Jitter = Callable[[int, int], int]
DerivativesPass = Callable[[], Awaitable[DerivativesSyncResult | None]]


async def run_collector(
    sessions: SessionFactory,
    *,
    book: LiquidationBook,
    floor_ms: int,
    connector: LiquidationConnector,
    stop: asyncio.Event,
    sleep: Sleep,
    url: str,
    jitter: Jitter | None = None,
    ping_interval_ms: int = PING_INTERVAL_MS,
    stale_after_ms: int = STALE_AFTER_MS,
    subscribe_timeout_ms: int = SUBSCRIBE_TIMEOUT_MS,
    gap_sync_interval_ms: int = GAP_SYNC_INTERVAL_MS,
    final_flush_timeout_ms: int = FINAL_FLUSH_TIMEOUT_MS,
) -> None:
    """Тримати підписку на символи `book`, поки не виставлено `stop`. Обрив — пауза з backoff і нове з'єднання.

    Година, обірвана посеред, у таблицю не пишеться: її закриє `sync_liquidation_gaps` як прогалину. Запис у БД —
    окрема задача; після `stop` вона ще раз пише готові години й прогалини, але не довше `final_flush_timeout_ms`.
    """
    if (
        type(final_flush_timeout_ms) is not int
        or isinstance(final_flush_timeout_ms, bool)
        or final_flush_timeout_ms < 1
    ):
        raise ValueError("final_flush_timeout_ms має бути int мс ≥ 1")
    writer = _Writer(sessions, book, floor_ms=floor_ms, gap_sync_interval_ms=gap_sync_interval_ms)
    async with asyncio.TaskGroup() as tasks:
        writing = tasks.create_task(writer.run(), name="liquidation-writer")
        try:
            await _stream(
                book,
                writer,
                connector=connector,
                stop=stop,
                sleep=sleep,
                url=url,
                pause=jitter if jitter is not None else _system_jitter(),
                ping_interval_ms=ping_interval_ms,
                stale_after_ms=stale_after_ms,
                subscribe_timeout_ms=subscribe_timeout_ms,
            )
        finally:
            writer.close()
            done, _ = await asyncio.wait((writing,), timeout=final_flush_timeout_ms / 1000)
            if not done:
                _LOG.error("liquidation_final_flush_timeout", timeout_ms=final_flush_timeout_ms)
                writing.cancel()


async def watch_clock(clock: ClockGuard, book: LiquidationBook, *, sleep_ms: SleepMs | None = None) -> None:
    """`ClockGuard.run` (тік щосекунди, `fetch_time` за розкладом) і після кожного тіку — `book.observe_clock()`.

    Так стрибок годинника книга бачить у тому ж тіку, що й `ClockGuard`, а не з наступним кадром потоку.
    Завершується лише скасуванням або винятком, як і `ClockGuard.run`.
    """
    pause = sleep_ms if sleep_ms is not None else _sleep_ms

    async def observe_then_sleep(ms: int) -> None:
        book.observe_clock()
        await pause(ms)

    await clock.run(sleep=observe_then_sleep)


async def collect_with_clock(
    clock: ClockGuard, book: LiquidationBook, body: Callable[[], Awaitable[None]], *, sleep_ms: SleepMs | None = None
) -> None:
    """Виконати `body` (колектор) поруч із `watch_clock`. Збій контролю часу зупиняє колектор, а не навпаки."""
    async with asyncio.TaskGroup() as tasks:
        watcher = tasks.create_task(watch_clock(clock, book, sleep_ms=sleep_ms), name="collector-clock")
        try:
            await body()
        finally:
            watcher.cancel()


async def run_derivatives(
    run_if_due: DerivativesPass,
    *,
    clock: ClockView,
    stop: asyncio.Event,
    sleep: Sleep,
    poll_ms: int = DERIVATIVES_POLL_MS,
) -> None:
    """Хвіст деривативів за розкладом (`DerivativesSync.run_if_due`), поки не виставлено `stop` (ТЗ §4.4, §6.4).

    Прохід лише коли годиннику вірять: «закрита» година рахується за локальним годинником, а незакриту біржову
    годину вставка `ON CONFLICT DO NOTHING` вже не виправила б. Збій проходу — у лог (`derivatives_sync_failed`), повтор
    після паузи самого `DerivativesSync`. Колектор ліквідацій він не зупиняє: деривативи можна докачати пізніше,
    ліквідації — ні. `stop` перериває прохід посеред: вікна комітяться окремо, повтор ідемпотентний.
    """
    if type(poll_ms) is not int or isinstance(poll_ms, bool) or poll_ms < 1:
        raise ValueError("poll_ms має бути int мс ≥ 1")
    waiting = False
    while not stop.is_set():
        state = clock.state()
        if not clock_trusted(state):
            if not waiting:
                _LOG.warning(
                    "derivatives_waiting_for_clock",
                    reasons=[reason.value for reason in state.block_reasons],
                    hint="деривативи не качаються, доки розбіжність з біржею не перевірена (< 1 с)",
                )
            waiting = True
        else:
            waiting = False
            await _derivatives_pass(run_if_due, stop)
            if stop.is_set():
                return
        await _wait_stop(stop, poll_ms / 1000, sleep)


async def _derivatives_pass(run_if_due: DerivativesPass, stop: asyncio.Event) -> None:
    passing = asyncio.ensure_future(run_if_due())
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({passing, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        await _cancel(stopper, passing)
    if passing.cancelled():
        _LOG.info("derivatives_sync_interrupted")
        return
    error = passing.exception()
    if error is not None:
        failure: dict[str, Any] = {"error_type": type(error).__name__}
        if not isinstance(error, (*_DB_ERRORS, ExchangeError)):
            # Не збій БД чи біржі, а несподіваний виняток — потрібне трасування.
            failure["exc_info"] = error
        _LOG.error("derivatives_sync_failed", **failure, hint="повтор після паузи; ліквідації збираються далі")
        return
    result = passing.result()
    if result is None:
        return
    _LOG.info(
        "derivatives_synced",
        **result.inline.as_dict(),
        job_id=str(result.job_id) if result.job_id is not None else None,
        job_symbols=list(result.job_symbols),
        failed_symbols=list(result.failed_symbols),
    )


async def _stream(
    book: LiquidationBook,
    writer: _Writer,
    *,
    connector: LiquidationConnector,
    stop: asyncio.Event,
    sleep: Sleep,
    url: str,
    pause: Jitter,
    ping_interval_ms: int,
    stale_after_ms: int,
    subscribe_timeout_ms: int,
) -> None:
    attempt = 0

    def reset_attempt() -> None:
        nonlocal attempt
        attempt = 0

    sink = _LiveSink(book, reset_attempt)
    while not stop.is_set():
        try:
            socket = await _connect_unless_stopped(connector, url, stop)
        except ExchangeUnavailable as exc:
            _LOG.warning("liquidation_connect_failed", error_type=type(exc).__name__)
            writer.poke(gaps=True)
            if stop.is_set():
                return
            await _wait_stop(stop, reconnect_delay_ms(attempt, jitter=pause) / 1000, sleep)
            attempt += 1
            continue
        if socket is None:
            break
        try:
            await read_liquidation_session(
                socket,
                symbols=book.symbols,
                sink=sink,
                stop=stop,
                sleep=sleep,
                on_progress=writer.poke,
                ping_interval_ms=ping_interval_ms,
                stale_after_ms=stale_after_ms,
                subscribe_timeout_ms=subscribe_timeout_ms,
            )
        except ExchangeUnavailable as exc:
            _LOG.warning("liquidation_stream_lost", error_type=type(exc).__name__, attempt=attempt)
            _end_session(book, writer)
            if stop.is_set():
                return
            await _wait_stop(stop, reconnect_delay_ms(attempt, jitter=pause) / 1000, sleep)
            attempt += 1
            continue
        finally:
            await _close_quietly(socket)
        _end_session(book, writer)
        return
    _end_session(book, writer)


async def _connect_unless_stopped(
    connector: LiquidationConnector, url: str, stop: asyncio.Event
) -> LiquidationSocket | None:
    """Нове з'єднання або `None`, якщо раніше виставили `stop`: handshake може висіти, а SIGTERM має спрацювати.

    Сокет, що встиг відкритись разом зі `stop`, закривається без підписки.
    """
    connecting = asyncio.ensure_future(connector.connect(url))
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({connecting, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        await _cancel(stopper, connecting)
    if connecting.cancelled():
        return None
    socket = connecting.result()
    if stop.is_set():
        await _close_quietly(socket)
        return None
    return socket


async def _close_quietly(socket: LiquidationSocket) -> None:
    with contextlib.suppress(ExchangeUnavailable, OSError, ConnectionError):
        await socket.close()


def reconnect_delay_ms(attempt: int, *, jitter: Jitter) -> int:
    """Пауза перед наступним з'єднанням: від `RECONNECT_BASE_MS`, подвоюється, стеля `RECONNECT_CAP_MS`, jitter."""
    if type(attempt) is not int or isinstance(attempt, bool) or attempt < 0:
        raise ValueError("attempt має бути int ≥ 0")
    shift = min(attempt, _BACKOFF_SHIFT_CAP)
    delay = min(RECONNECT_CAP_MS, RECONNECT_BASE_MS * (2**shift))
    low = delay // 2
    picked = jitter(low, delay)
    if type(picked) is not int or isinstance(picked, bool) or picked < 0:
        raise ValueError("jitter має повернути int мс ≥ 0")
    return picked


def db_retry_delay_ms(failures: int) -> int:
    """Пауза після `failures`-ї поспіль помилки БД: від `DB_RETRY_BASE_MS`, подвоюється, стеля `DB_RETRY_CAP_MS`."""
    if type(failures) is not int or isinstance(failures, bool) or failures < 1:
        raise ValueError("failures має бути int ≥ 1")
    return min(DB_RETRY_CAP_MS, DB_RETRY_BASE_MS * (2 ** min(failures - 1, _BACKOFF_SHIFT_CAP)))


class _Writer:
    """Пише закриті години й прогалини окремою задачею: цикл читання лише смикає `poke` і БД не чекає.

    Записи йдуть по одному, на кожен `poke` (кадр, кінець сесії, невдале з'єднання). Після помилки БД наступна
    спроба — не раніше `db_retry_delay_ms`; `poke` у цей час нічого не робить. Готові години лишаються в книзі до
    успішного запису.
    """

    def __init__(
        self, sessions: SessionFactory, book: LiquidationBook, *, floor_ms: int, gap_sync_interval_ms: int
    ) -> None:
        self._sessions = sessions
        self._book = book
        self._floor_ms = floor_ms
        self._gap_sync_interval_ms = gap_sync_interval_ms
        self._wake = asyncio.Event()
        self._closing = False
        self._gaps_due = True
        self._last_gap_mono: int | None = None
        self._retry_at_mono = 0
        self._failures = 0
        self._wake.set()  # прогалини зводяться одразу на старті

    def poke(self, *, gaps: bool = False) -> None:
        """Є що записати (`gaps` — зведення прогалин потрібне зараз, а не за розкладом)."""
        if gaps:
            self._gaps_due = True
        self._wake.set()

    def close(self) -> None:
        """Останній прохід без урахування паузи після помилки, потім `run` завершується."""
        self._closing = True
        self._gaps_due = True
        self._wake.set()

    async def run(self) -> None:
        while True:
            await self._wake.wait()
            self._wake.clear()
            final = self._closing
            await self._step(final=final)
            if final:
                return

    async def _step(self, *, final: bool) -> None:
        mono = monotonic_ms()
        if not final and mono < self._retry_at_mono:
            return
        rows = self._book.capture(now_ms())
        if rows:
            try:
                inserted = await save_liquidations(self._sessions, rows)
            except _DB_ERRORS as exc:
                self._failed("liquidation_save_failed", exc, hours=len(rows))
                return
            self._book.ack(rows)
            self._recovered()
            self._gaps_due = True
            _LOG.info("liquidations_sealed", hours=len(rows), inserted=inserted)
        scheduled = self._last_gap_mono is None or mono - self._last_gap_mono >= self._gap_sync_interval_ms
        if not (self._gaps_due or scheduled):
            return
        try:
            await sync_liquidation_gaps(
                self._sessions,
                symbols=self._book.symbols,
                floor_ms=self._floor_ms,
                now=now_ms(),
                seal_margin_ms=self._book.seal_margin_ms,
            )
        except _DB_ERRORS as exc:
            self._failed("liquidation_gap_sync_failed", exc)
            return
        self._recovered()
        self._gaps_due = False
        self._last_gap_mono = mono

    def _failed(self, event: str, exc: BaseException, **context: int) -> None:
        self._failures += 1
        delay = db_retry_delay_ms(self._failures)
        self._retry_at_mono = monotonic_ms() + delay
        _LOG.error(event, error_type=type(exc).__name__, failures=self._failures, retry_in_ms=delay, **context)

    def _recovered(self) -> None:
        if self._failures:
            _LOG.info("liquidation_db_recovered", failures=self._failures)
        self._failures = 0
        self._retry_at_mono = 0


class _LiveSink:
    """Скидає лічильник backoff, щойно підписку підтверджено: наступний обрив знову чекає базову паузу."""

    def __init__(self, book: LiquidationBook, reset_attempt: Callable[[], None]) -> None:
        self._book = book
        self._reset_attempt = reset_attempt

    def subscribed(self, symbol: str, at_ms: int) -> None:
        self._book.subscribed(symbol, at_ms)
        self._reset_attempt()

    def accept(self, events: Sequence[LiquidationEvent]) -> None:
        self._book.accept(events)

    def mark_unusable(self, hour_ms: int | None) -> None:
        self._book.mark_unusable(hour_ms)

    def note_alive(self) -> None:
        self._book.note_alive()


def _end_session(book: LiquidationBook, writer: _Writer) -> None:
    book.disconnect()
    writer.poke(gaps=True)


async def serve(
    settings: Settings,
    *,
    connector: LiquidationConnector | None = None,
    sleep: Sleep | None = None,
    jitter: Jitter | None = None,
) -> int:
    """Взяти lock колектора і слухати потік до SIGTERM/SIGINT. Повертає код виходу."""
    try:
        url = settings.database_url()
    except ValueError:
        _LOG.error("collector_db_not_configured")
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
    lock = InstanceLock(url, lock_id=COLLECTOR_LOCK_ID, application_name_prefix=COLLECTOR_APPLICATION_NAME_PREFIX)
    engine = create_engine(url)
    sessions = session_factory(engine)
    limiter = RateLimiter.from_settings(settings, role=ProcessRole.COLLECTOR)
    # `fetch_time` (ТЗ §4.3) і деривативи (ТЗ §4.4); публічний mainnet, ключів у конструктор не передається.
    market = BybitAdapter(mode=ExchangeMode.DEMO, limiter=limiter)
    clock = ClockGuard(market)
    book = LiquidationBook(settings.research_symbols, clock=clock)
    derivatives = DerivativesSync.from_settings(settings, sessions, market)
    owned = connector is None
    stream: LiquidationConnector = connector if connector is not None else AiohttpLiquidationConnector()
    stream_url = liquidation_stream_url(ExchangeMode.DEMO)
    pause = sleep if sleep is not None else asyncio.sleep

    async def collect() -> None:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(
                run_derivatives(derivatives.run_if_due, clock=clock, stop=stop, sleep=pause),
                name="collector-derivatives",
            )
            await run_collector(
                sessions,
                book=book,
                floor_ms=settings.backfill_start_ms,
                connector=stream,
                stop=stop,
                sleep=pause,
                jitter=jitter,
                url=stream_url,
            )

    try:
        async with limiter:
            _LOG.info(
                "collector_starting",
                role=limiter.role.value,
                ip_capacity=limiter.ip_capacity,
                symbols=len(settings.research_symbols),
                stream=stream_url,
                derivatives_interval_s=settings.backfill_interval_s,
                lock_wait_s=settings.lock_wait_s,
            )
            if not await _acquire_unless_stopped(lock, settings.lock_wait_s * 1000, stop):
                return EXIT_OK
            await run_exclusive(lock, lambda: collect_with_clock(clock, book, collect), on_stuck=_exit_stuck)
            return EXIT_OK
    except InstanceLockHeld:
        return EXIT_LOCK_HELD
    except InstanceLockUnavailable:
        return EXIT_DB_UNAVAILABLE
    except InstanceLockLost:
        return EXIT_LOCK_LOST
    finally:
        await lock.release()
        if owned:
            await stream.aclose()
        await market.aclose()
        await engine.dispose()
        for sig in installed:
            loop.remove_signal_handler(sig)


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входу `python -m app.data.collector`."""
    _parse_args(argv)
    settings = Settings()
    configure_logging(settings, process="collector")
    try:
        code = asyncio.run(serve(settings))
        _LOG.info("collector_exit", exit_code=code)
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
        prog="python -m app.data.collector",
        description="Колектор ліквідацій Bybit (публічний WebSocket, без торгових ключів).",
        allow_abbrev=False,
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _on_signal(sig: signal.Signals, stop: asyncio.Event) -> None:
    if not stop.is_set():
        _LOG.info("shutdown_signal", signal=sig.name)
        stop.set()


def _exit_stuck(reason: str) -> None:
    code = EXIT_LOCK_LOST if reason == "lock_lost" else EXIT_STOP_TIMEOUT
    _LOG.critical("collector_stop_timeout", reason=reason, exit_code=code)
    shutdown_logging()
    os._exit(code)


async def _acquire_unless_stopped(lock: InstanceLock, wait_ms: int, stop: asyncio.Event) -> bool:
    acquiring = asyncio.create_task(lock.acquire(wait_ms=wait_ms), name="collector-lock-acquire")
    stopper = asyncio.create_task(stop.wait(), name="collector-lock-stop")
    try:
        await asyncio.wait({acquiring, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        await _cancel(stopper, acquiring)
    if acquiring.cancelled() or not acquiring.done():
        return False
    acquiring.result()
    return True


async def _cancel(*tasks: asyncio.Future[Any]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    for task in tasks:
        if task.done() and not task.cancelled():
            task.exception()
            continue
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _wait_stop(stop: asyncio.Event, seconds: float, sleep: Sleep) -> None:
    if stop.is_set() or seconds <= 0:
        return
    sleeper = asyncio.ensure_future(sleep(seconds))
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        await _cancel(sleeper, stopper)


async def _sleep_ms(ms: int) -> None:
    await asyncio.sleep(ms / 1000)


def _system_jitter() -> Jitter:
    rng = random.Random()  # noqa: S311 — jitter паузи реконекту, не секрет і не ключ

    def jitter(low: int, high: int) -> int:
        if high <= low:
            return low
        return rng.randint(low, high)

    return jitter


if __name__ == "__main__":
    raise SystemExit(main())
