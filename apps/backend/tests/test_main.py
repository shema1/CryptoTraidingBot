"""Торговий процес `python -m app` (T0.6, T0.7): аргументи, коди виходу, підтвердження `--force-takeover`, API.

Без БД: `InstanceLock` і `run_exclusive` підмінені. Реальні процеси — `tests/db/test_instance_lock_process.py`.
"""

from __future__ import annotations

import asyncio
import io
import os
import secrets
import sys
from collections.abc import Callable, Coroutine, Sequence
from pathlib import Path
from typing import Any, ClassVar

import pytest

from app import __main__ as backend
from app.api.server import ApiBindError
from app.core.config import Settings
from app.core.instance_lock import (
    InstanceLockHeld,
    InstanceLockLost,
    InstanceLockUnavailable,
    LockHolder,
    TakeoverRefused,
)

HOLDER = LockHolder(
    session_pid=777,
    application_name="ctb-backend pid=4321",
    client_os_pid=4321,
    client_addr="172.18.0.1",
    client_port=55210,
    backend_start_ms=1_767_225_000_000,
    state="idle",
    state_change_ms=1_767_225_000_500,
    usename="ctb",
)


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("LOG_CONSOLE", "false")
    monkeypatch.setenv("POSTGRES_PASSWORD", "unit-test-only")
    monkeypatch.setenv("LOCK_WAIT_S", "7")
    monkeypatch.setenv("API_TOKEN", secrets.token_urlsafe(32))


class FakeLock:
    # Журнал створених екземплярів і помилка наступного acquire — скидаються фікстурою `fake_lock`.
    instances: ClassVar[list[FakeLock]] = []
    next_error: ClassVar[BaseException | None] = None

    def __init__(self, url: object, **kwargs: object) -> None:
        self.error: BaseException | None = None
        self.acquire_calls: list[dict[str, object]] = []
        self.released = False
        FakeLock.instances.append(self)

    async def acquire(self, *, wait_ms: int, takeover: object = None) -> None:
        self.acquire_calls.append({"wait_ms": wait_ms, "takeover": takeover})
        if FakeLock.next_error is not None:
            raise FakeLock.next_error

    async def release(self) -> None:
        self.released = True


@pytest.fixture
def fake_lock(monkeypatch: pytest.MonkeyPatch) -> type[FakeLock]:
    FakeLock.instances = []
    FakeLock.next_error = None
    monkeypatch.setattr(backend, "InstanceLock", FakeLock)
    return FakeLock


def _exclusive_raising(error: BaseException) -> Callable[..., Coroutine[Any, Any, None]]:
    async def run_exclusive(lock: object, body: object, **kwargs: object) -> None:
        raise error

    return run_exclusive


def test_force_takeover_without_tty_is_refused_before_touching_db(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    monkeypatch.setattr(backend, "_interactive_terminal", lambda: False)

    def no_db(*args: object, **kwargs: object) -> None:
        raise AssertionError("без TTY підключення до БД не повинно відбуватися")

    monkeypatch.setattr(backend, "InstanceLock", no_db)
    assert backend.main(["--force-takeover"]) == backend.EXIT_TAKEOVER_REFUSED


def test_unknown_argument_is_usage_error() -> None:
    with pytest.raises(SystemExit) as info:
        backend.main(["--force"])
    assert info.value.code == 2


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (InstanceLockHeld((HOLDER,)), backend.EXIT_LOCK_HELD),
        (InstanceLockUnavailable("ConnectionRefusedError"), backend.EXIT_DB_UNAVAILABLE),
        (TakeoverRefused("not_confirmed"), backend.EXIT_TAKEOVER_REFUSED),
    ],
)
def test_acquire_failures_map_to_exit_codes(fake_lock: type[FakeLock], error: BaseException, code: int) -> None:
    fake_lock.next_error = error
    assert backend.main([]) == code
    (lock,) = fake_lock.instances
    assert lock.acquire_calls == [{"wait_ms": 7000, "takeover": None}]
    assert lock.released is True


def test_lost_lock_maps_to_exit_code_and_releases(fake_lock: type[FakeLock], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(backend, "run_exclusive", _exclusive_raising(InstanceLockLost("connection_closed")))
    assert backend.main([]) == backend.EXIT_LOCK_LOST
    assert fake_lock.instances[0].released is True


def test_exit_codes_are_distinct() -> None:
    codes = [
        backend.EXIT_OK,
        backend.EXIT_LOCK_HELD,
        backend.EXIT_DB_UNAVAILABLE,
        backend.EXIT_LOCK_LOST,
        backend.EXIT_TAKEOVER_REFUSED,
        backend.EXIT_STOP_TIMEOUT,
        backend.EXIT_CONFIG_INVALID,
        backend.EXIT_API_BIND_FAILED,
    ]
    assert len(set(codes)) == len(codes)
    assert backend.EXIT_OK == 0
    assert backend.EXIT_UNCAUGHT == 1
    assert all(2 < code < 126 for code in codes[1:])  # не 1 (необроблений виняток), не 2 (argparse), не сигнали


# --- локальний API (T0.7) ---


def test_missing_api_token_refuses_start_before_lock(
    fake_lock: type[FakeLock], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("API_TOKEN")
    # Без файла змінних розробника: там токен може бути.
    monkeypatch.setattr(backend, "Settings", lambda: Settings(_env_file=None))
    assert backend.main([]) == backend.EXIT_CONFIG_INVALID
    assert fake_lock.instances == []


def test_missing_api_token_is_checked_before_takeover(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_TOKEN")
    monkeypatch.setattr(backend, "Settings", lambda: Settings(_env_file=None))
    monkeypatch.setattr(backend, "_interactive_terminal", lambda: True)

    def no_db(*args: object, **kwargs: object) -> None:
        raise AssertionError("без API_TOKEN підключення до БД не повинно відбуватися")

    monkeypatch.setattr(backend, "InstanceLock", no_db)
    assert backend.main(["--force-takeover"]) == backend.EXIT_CONFIG_INVALID


def test_api_bind_failure_maps_to_exit_code_and_releases(
    fake_lock: type[FakeLock], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(backend, "run_exclusive", _exclusive_raising(ApiBindError(8000, "EADDRINUSE")))
    assert backend.main([]) == backend.EXIT_API_BIND_FAILED
    assert fake_lock.instances[0].released is True


def test_work_under_lock_is_the_api_server(fake_lock: type[FakeLock], monkeypatch: pytest.MonkeyPatch) -> None:
    served: list[tuple[Settings, object]] = []

    async def fake_serve_api(settings: Settings, lock: object) -> None:
        served.append((settings, lock))

    async def run_exclusive(lock: object, body: Callable[[], Coroutine[Any, Any, None]], **kwargs: object) -> None:
        await body()

    monkeypatch.setattr(backend, "serve_api", fake_serve_api)
    monkeypatch.setattr(backend, "run_exclusive", run_exclusive)
    assert backend.main([]) == backend.EXIT_OK
    ((settings, lock),) = served
    assert lock is fake_lock.instances[0]
    assert settings.api_token is not None


# --- підтвердження --force-takeover ---


def _reader(answer: str | BaseException) -> Callable[[], Coroutine[Any, Any, str]]:
    async def read_line() -> str:
        if isinstance(answer, BaseException):
            raise answer
        return answer

    return read_line


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        ("TAKEOVER", True),
        ("TAKEOVER\n", True),
        ("  TAKEOVER  ", True),
        ("takeover", False),
        ("yes", False),
        ("y", False),
        ("", False),
        (EOFError(), False),
    ],
)
async def test_takeover_needs_exact_word(answer: str | BaseException, expected: bool) -> None:
    out = io.StringIO()
    confirmed = await backend.confirm_takeover_on_tty([HOLDER], read_line=_reader(answer), out=out)
    assert confirmed is expected


@pytest.mark.asyncio
async def test_takeover_prompt_describes_holder() -> None:
    out = io.StringIO()
    holders: Sequence[LockHolder] = [HOLDER]
    await backend.confirm_takeover_on_tty(holders, read_line=_reader("no"), out=out)
    text = out.getvalue()
    assert "777" in text  # pid сесії PostgreSQL
    assert "4321" in text  # pid процесу ОС з application_name
    assert "TAKEOVER" in text


# --- рев'ю T0.6 ---


def test_uncaught_exception_is_logged_masked_not_printed_raw(
    fake_lock: type[FakeLock], monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("LOG_CONSOLE", "true")
    db_password = os.environ["POSTGRES_PASSWORD"]
    fake_lock.next_error = RuntimeError(f"boom postgresql://ctb:{db_password}@127.0.0.1/ctb and plain {db_password}")

    assert backend.main([]) == backend.EXIT_UNCAUGHT

    err = capfd.readouterr().err
    assert db_password not in err
    assert "uncaught_exception" in err
    errors_log = (tmp_path / "logs" / "backend" / "errors.log").read_text(encoding="utf-8")
    assert "uncaught_exception" in errors_log
    assert "RuntimeError" in errors_log
    assert db_password not in errors_log
    assert fake_lock.instances[0].released is True


def test_lost_lock_inside_exception_group_maps_to_lock_lost(
    fake_lock: type[FakeLock], monkeypatch: pytest.MonkeyPatch
) -> None:
    group = ExceptionGroup("робота", [InstanceLockLost("lock_not_held"), ValueError("паралельна помилка")])
    monkeypatch.setattr(backend, "run_exclusive", _exclusive_raising(group))
    assert backend.main([]) == backend.EXIT_LOCK_LOST


def test_takeover_flag_on_terminal_passes_interactive_confirmation(
    fake_lock: type[FakeLock], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(backend, "_interactive_terminal", lambda: True)
    fake_lock.next_error = TakeoverRefused("not_confirmed")

    assert backend.main(["--force-takeover"]) == backend.EXIT_TAKEOVER_REFUSED
    assert fake_lock.instances[0].acquire_calls == [{"wait_ms": 7000, "takeover": backend.confirm_takeover_on_tty}]


def test_interactive_terminal_needs_both_stdin_and_stderr(monkeypatch: pytest.MonkeyPatch) -> None:
    class Tty(io.StringIO):
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr(sys, "stdin", Tty())
    monkeypatch.setattr(sys, "stderr", io.StringIO())  # запит пішов би у файл — людина його не побачить
    assert backend._interactive_terminal() is False
    monkeypatch.setattr(sys, "stderr", Tty())
    assert backend._interactive_terminal() is True


@pytest.mark.asyncio
async def test_takeover_confirmation_times_out_as_refusal() -> None:
    async def silent() -> str:
        await asyncio.Event().wait()
        return backend.TAKEOVER_WORD

    confirmed = await backend.confirm_takeover_on_tty([HOLDER], read_line=silent, out=io.StringIO(), timeout_s=0.01)
    assert confirmed is False


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [OSError("stdin"), ValueError("closed file")])
async def test_takeover_confirmation_read_error_is_refusal(error: BaseException) -> None:
    confirmed = await backend.confirm_takeover_on_tty([HOLDER], read_line=_reader(error), out=io.StringIO())
    assert confirmed is False


class _Exited(Exception):
    def __init__(self, code: int) -> None:
        self.code = code


@pytest.mark.parametrize(
    ("reason", "code"), [("lock_lost", backend.EXIT_LOCK_LOST), ("shutdown", backend.EXIT_STOP_TIMEOUT)]
)
def test_stuck_body_exits_hard_with_code(monkeypatch: pytest.MonkeyPatch, reason: str, code: int) -> None:
    def fake_exit(exit_code: int) -> None:
        raise _Exited(exit_code)

    monkeypatch.setattr(backend.os, "_exit", fake_exit)
    with pytest.raises(_Exited) as info:
        backend._exit_stuck(reason)
    assert info.value.code == code
