"""Торговий процес (бекенд): `python -m app` (ТЗ §6.4, T0.6).

Порядок старту: `Settings` → `configure_logging(process="backend")` → single-instance lock
(`app/core/instance_lock.py`) → робота під lock (`run_exclusive`) до SIGTERM/SIGINT → звільнення lock.
Зараз «робота» — лише очікування сигналу: локальний API додає T0.7, цикли ботів — етап 5.

`--force-takeover` — лише людиною з інтерактивного терміналу після перевірки, що старий процес не працює:
без TTY процес відмовляє ще до підключення до БД (прапорець у конфігу служби launchd/systemd вбивав би робочий
екземпляр при кожному перезапуску). Підтвердження — введення слова `TAKEOVER_WORD`.

Коди виходу — `EXIT_*` нижче (1 — необроблений виняток, 2 — помилка аргументів argparse).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import signal
import sys
import threading
from collections.abc import Callable, Coroutine, Sequence
from datetime import UTC, datetime
from typing import Any, Final, TextIO

from app.core.config import Settings
from app.core.instance_lock import (
    InstanceLock,
    InstanceLockHeld,
    InstanceLockLost,
    InstanceLockUnavailable,
    LockHolder,
    TakeoverRefused,
    run_exclusive,
)
from app.core.logging import configure_logging, get_logger, shutdown_logging

EXIT_OK: Final = 0  # завершення за сигналом
EXIT_LOCK_HELD: Final = 10  # lock тримає інший процес весь `LOCK_WAIT_S` — другий екземпляр не стартує
EXIT_DB_UNAVAILABLE: Final = 11  # БД недоступна весь `LOCK_WAIT_S`
EXIT_LOCK_LOST: Final = 12  # lock втрачено під час роботи (fail-closed: новий старт = звірка, ТЗ §11.1)
EXIT_TAKEOVER_REFUSED: Final = 13  # перехоплення не підтверджено/змінився власник, немає TTY або не записано аудит
EXIT_STOP_TIMEOUT: Final = 14  # після сигналу робота не зупинилась за BODY_STOP_TIMEOUT_MS — аварійне завершення
EXIT_UNCAUGHT: Final = 1  # необроблений виняток (записується маскованим `uncaught_exception`)

TAKEOVER_WORD: Final = "TAKEOVER"
# Скільки чекати відповіді людини на запит перехоплення; мовчання = відмова (процес не висить вічно під PTY).
CONFIRM_TIMEOUT_S: Final = 120.0

_log = get_logger("app.main")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    settings = Settings()
    configure_logging(settings, process="backend")
    try:
        if args.force_takeover and not _interactive_terminal():
            _log.error("force_takeover_requires_tty")
            return EXIT_TAKEOVER_REFUSED
        code = asyncio.run(_run(settings, force_takeover=args.force_takeover))
        _log.info("backend_exit", exit_code=code)
        return code
    except (SystemExit, KeyboardInterrupt):
        raise
    except BaseException:  # noqa: BLE001 — записується маскованим логом (exc_info) і процес завершується з кодом 1
        # Записати маскованим ще до shutdown_logging(): після нього sys.excepthook знову стандартний і надрукував би
        # трасування разом із текстом винятку (URL, тіла запитів) без маскування.
        _log.critical("uncaught_exception", exc_info=True)
        return EXIT_UNCAUGHT
    finally:
        shutdown_logging()


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    # allow_abbrev=False: скорочення на кшталт `--force` не повинні вмикати перехоплення.
    parser = argparse.ArgumentParser(
        prog="python -m app",
        description="Торговий процес Crypto Trading Bot (ТЗ §6.4).",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--force-takeover",
        action="store_true",
        help=(
            "перехопити lock у сесії, що його тримає (лише з терміналу, після перевірки, що старий процес "
            "не працює; потребує введення TAKEOVER і пишеться в audit_log)"
        ),
    )
    return parser.parse_args(argv)


def _interactive_terminal() -> bool:
    """І stdin, і stderr — термінал: людина бачить запит (stderr) і може відповісти (stdin)."""
    try:
        return all(stream is not None and stream.isatty() for stream in (sys.stdin, sys.stderr))
    except (AttributeError, OSError, ValueError):
        return False


def _exit_stuck(reason: str) -> None:
    """Робота не зупинилась після скасування: аварійний вихід (ОС закриє сокет, сервер звільнить lock)."""
    code = EXIT_LOCK_LOST if reason == "lock_lost" else EXIT_STOP_TIMEOUT
    _log.critical("backend_stop_timeout", reason=reason, exit_code=code)
    shutdown_logging()
    os._exit(code)


async def _run(settings: Settings, *, force_takeover: bool) -> int:
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()
    if main_task is None:
        raise RuntimeError("_run має виконуватися як задача asyncio")
    stopping: list[str] = []

    def on_signal(sig: signal.Signals) -> None:
        if not stopping:
            stopping.append(sig.name)
            _log.info("shutdown_signal", signal=sig.name)
            main_task.cancel()

    signals = (signal.SIGTERM, signal.SIGINT)
    for sig in signals:
        loop.add_signal_handler(sig, on_signal, sig)
    lock = InstanceLock(settings.database_url())
    _log.info("backend_starting", lock_wait_s=settings.lock_wait_s, force_takeover=force_takeover)
    try:
        await lock.acquire(
            wait_ms=settings.lock_wait_s * 1000,
            takeover=confirm_takeover_on_tty if force_takeover else None,
        )
        await run_exclusive(lock, _serve, on_stuck=_exit_stuck)
        return EXIT_OK
    except asyncio.CancelledError:
        if not stopping:
            raise
        return EXIT_OK
    except InstanceLockHeld:
        return EXIT_LOCK_HELD
    except InstanceLockUnavailable:
        return EXIT_DB_UNAVAILABLE
    except InstanceLockLost:
        return EXIT_LOCK_LOST
    except TakeoverRefused:
        return EXIT_TAKEOVER_REFUSED
    except BaseExceptionGroup as errors:
        # Робота з вкладеною TaskGroup (T0.7, етап 5) може віддати втрату lock усередині групи.
        if errors.subgroup(InstanceLockLost) is not None:
            _log.critical("instance_lock_lost_in_group", error_types=[type(e).__name__ for e in errors.exceptions])
            return EXIT_LOCK_LOST
        raise
    finally:
        await lock.release()
        for sig in signals:
            loop.remove_signal_handler(sig)


async def _serve() -> None:
    """Робота торгового процесу під lock. Поки — лише очікування сигналу завершення (T0.7: API, етап 5: боти)."""
    await asyncio.Event().wait()


async def confirm_takeover_on_tty(
    holders: Sequence[LockHolder],
    *,
    read_line: Callable[[], Coroutine[Any, Any, str]] | None = None,
    out: TextIO | None = None,
    timeout_s: float = CONFIRM_TIMEOUT_S,
) -> bool:
    """Показати власників lock і перехопити лише після введення `TAKEOVER_WORD`.

    Інша відповідь, EOF, помилка читання чи мовчання довше `timeout_s` — відмова.
    """
    stream = sys.stderr if out is None else out
    stream.write(_describe_holders(holders, timeout_s))
    stream.flush()
    try:
        async with asyncio.timeout(timeout_s):
            answer = await (read_line or _read_stdin_line)()
    except (EOFError, OSError, ValueError, TimeoutError) as exc:
        _log.warning("force_takeover_no_answer", error_type=type(exc).__name__)
        answer = ""
    confirmed = answer.strip() == TAKEOVER_WORD
    _log.warning("force_takeover_answer", confirmed=confirmed)
    return confirmed


def _iso(ms: int | None) -> str:
    return "?" if ms is None else datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(timespec="seconds")


def _describe_holders(holders: Sequence[LockHolder], timeout_s: float) -> str:
    lines = ["", "УВАГА: lock торгового процесу тримає інша сесія PostgreSQL:"]
    for h in holders:
        os_pid = "невідомо" if h.client_os_pid is None else str(h.client_os_pid)
        lines.append(
            f"  сесія pid={h.session_pid}, процес ОС pid={os_pid}, application_name={h.application_name!r}, "
            f"користувач={h.usename}, стан={h.state}, старт сесії={_iso(h.backend_start_ms)}"
        )
        if h.client_os_pid is not None:
            lines.append(f"  перевірка процесу: ps -p {h.client_os_pid}")
    lines += [
        "Перехоплення завершить цю сесію. Якщо старий процес ще працює, він втратить lock і завершиться,",
        "але до того може встигнути надіслати ордер. Продовжуйте, лише якщо впевнені, що він не працює.",
        f"Введіть {TAKEOVER_WORD}, щоб перехопити lock (будь-що інше або мовчання {timeout_s:.0f} с — відмова): ",
    ]
    return "\n".join(lines)


async def _read_stdin_line() -> str:
    """Рядок зі stdin у фоновому потоці-демоні: Ctrl-C під час очікування не блокує завершення процесу."""
    loop = asyncio.get_running_loop()
    future: asyncio.Future[str] = loop.create_future()

    def deliver(line: str | None, error: BaseException | None) -> None:
        if future.done():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(line or "")

    def reader() -> None:
        line: str | None = None
        error: BaseException | None = None
        try:
            line = sys.stdin.readline()
            if line == "":
                error = EOFError()
        except (OSError, ValueError) as exc:
            error = exc
        with contextlib.suppress(RuntimeError):  # цикл подій уже закрито (процес завершується)
            loop.call_soon_threadsafe(deliver, line, error)

    threading.Thread(target=reader, name="force-takeover-confirm", daemon=True).start()
    return await future


if __name__ == "__main__":
    sys.exit(main())
