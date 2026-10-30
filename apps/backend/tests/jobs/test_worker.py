"""Чисті перевірки воркера без БД (T1.8): пріоритет ОС, слот GPU, відсутність торгових ключів."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from app.core.config import Settings
from app.core.instance_lock import (
    BACKEND_LOCK_ID,
    LOCK_NAMESPACE,
    WORKER_APPLICATION_NAME_PREFIX,
    WORKER_LOCK_ID,
)
from app.exchange.bybit import ExchangeMode
from app.exchange.fake import FakeExchangeAdapter
from app.exchange.instruments import _LOCK_NAMESPACE as INSTRUMENT_LOCK_NAMESPACE
from app.exchange.rate_limit import IP_REQUESTS, EndpointGroup, ProcessRole, RequestPriority, share_capacity
from app.jobs import worker


def test_stale_window_covers_three_heartbeats() -> None:
    assert worker.HEARTBEAT_INTERVAL_MS == 5_000
    assert worker.STALE_AFTER_MS == 30_000
    assert worker.STALE_AFTER_MS >= 3 * worker.HEARTBEAT_INTERVAL_MS
    assert worker.POLL_INTERVAL_MS == 1_000
    assert worker.NICE_INCREMENT == 10


def test_worker_lock_is_its_own_key_in_the_instance_namespace() -> None:
    assert (LOCK_NAMESPACE, WORKER_LOCK_ID) == (0x63746200, 2)
    # Множини, а не `!=`: mypy `strict_equality` відкидає порівняння різних `Final`-літералів.
    assert len({WORKER_LOCK_ID, BACKEND_LOCK_ID}) == 2
    assert len({LOCK_NAMESPACE, INSTRUMENT_LOCK_NAMESPACE}) == 2
    assert WORKER_APPLICATION_NAME_PREFIX == "ctb-worker"


def test_exit_codes_match_the_trading_process() -> None:
    assert (worker.EXIT_LOCK_HELD, worker.EXIT_DB_UNAVAILABLE, worker.EXIT_LOCK_LOST) == (10, 11, 12)
    assert (worker.EXIT_STOP_TIMEOUT, worker.EXIT_CONFIG_INVALID) == (14, 15)


def test_claim_sql_skips_locked_rows() -> None:
    assert "FOR UPDATE SKIP LOCKED" in worker._CLAIM_SQL
    assert "ORDER BY priority, created_at_ms, id" in worker._CLAIM_SQL


def test_posix_priority_adds_nice(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def nice(increment: int) -> int:
        calls.append(increment)
        return 12

    monkeypatch.setattr(worker.sys, "platform", "darwin")
    monkeypatch.setattr(worker.os, "nice", nice)
    worker.lower_os_priority()
    assert calls == [worker.NICE_INCREMENT]


def test_nice_failure_does_not_stop_the_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    def nice(_increment: int) -> int:
        raise PermissionError

    monkeypatch.setattr(worker.sys, "platform", "linux")
    monkeypatch.setattr(worker.os, "nice", nice)
    worker.lower_os_priority()


def test_windows_uses_below_normal(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(worker.sys, "platform", "win32")
    monkeypatch.setattr(worker, "_lower_windows_priority", lambda: calls.append("win"))
    worker.lower_os_priority()
    assert calls == ["win"]
    assert worker.BELOW_NORMAL_PRIORITY_CLASS == 0x00004000


def test_worker_source_does_not_mention_trading_keys() -> None:
    source = Path(worker.__file__).read_text(encoding="utf-8")
    assert "API_KEY" not in source
    assert "API_SECRET" not in source


def test_unknown_argument_is_usage_error() -> None:
    with pytest.raises(SystemExit) as info:
        worker.main(["--trade"])
    assert info.value.code == 2


def test_main_serves_until_the_worker_returns(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("LOG_CONSOLE", "false")
    seen: list[Settings] = []

    async def fake_serve(settings: Settings, **_kwargs: object) -> int:
        seen.append(settings)
        return worker.EXIT_OK

    monkeypatch.setattr(worker, "serve", fake_serve)
    assert worker.main([]) == worker.EXIT_OK
    assert len(seen) == 1


@pytest.mark.asyncio
async def test_worker_market_is_public_mainnet_with_the_worker_share(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BYBIT_LIVE_API_KEY", "live-key-value")
    monkeypatch.setenv("BYBIT_LIVE_API_SECRET", "live-secret-value")
    monkeypatch.setenv("BYBIT_DEMO_API_KEY", "demo-key-value")
    monkeypatch.setenv("BYBIT_DEMO_API_SECRET", "demo-secret-value")
    settings = Settings(_env_file=None)  # type: ignore[arg-type]
    market = worker.open_worker_market(settings)
    try:
        assert market.limiter.role is ProcessRole.WORKER
        assert market.limiter.ip_capacity == share_capacity(IP_REQUESTS, settings.rate_limit_share_worker_pct)
        assert market.adapter.mode is ExchangeMode.DEMO
        origin = market.adapter.public_origin
        assert origin.startswith("https://")
        assert "testnet" not in origin
        assert "demo" not in origin
        exchange = getattr(market.adapter._client, "_exchange", None)
        assert exchange is not None
        assert getattr(exchange, "apiKey", None) in (None, "")
        assert getattr(exchange, "secret", None) in (None, "")
        with pytest.raises(ValueError, match="приватна група"):
            await market.limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.ORDER_CREATE)
    finally:
        await market.aclose()


def test_bool_is_not_a_millisecond_count() -> None:
    with pytest.raises(ValueError, match="stale_after_ms"):
        worker._require_ms("stale_after_ms", True, minimum=1)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_report_progress_rejects_a_value_outside_the_range() -> None:
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        await worker.report_progress(None, uuid.uuid4(), Decimal(2))  # type: ignore[arg-type]


class _EmptyRows:
    """Відповідь і для зняття прострочених (`all`), і для взяття задачі (`mappings().one_or_none()`)."""

    def all(self) -> list[Any]:
        return []

    def mappings(self) -> _EmptyRows:
        return self

    def one_or_none(self) -> None:
        return None


class _EmptySession:
    def begin(self) -> contextlib.nullcontext[None]:
        return contextlib.nullcontext()

    async def execute(self, *_args: object, **_kwargs: object) -> _EmptyRows:
        return _EmptyRows()


class _DbDropsAfter:
    """Перші `healthy` сесій бачать порожню чергу, далі БД недоступна (`OSError`).

    `stop_on_drop` — SIGTERM приходить разом зі збоєм. Цикл без виходу впирається в `limit` (`AssertionError`),
    а не висить.
    """

    def __init__(self, healthy: int, stop: asyncio.Event, *, stop_on_drop: bool, limit: int = 20) -> None:
        self.healthy = healthy
        self.stop = stop
        self.stop_on_drop = stop_on_drop
        self.limit = limit
        self.calls = 0

    def __call__(self) -> contextlib.nullcontext[_EmptySession]:
        self.calls += 1
        if self.calls > self.limit:
            raise AssertionError("цикл повторює БД без паузи і не виходить")
        if self.calls > self.healthy:
            if self.stop_on_drop:
                self.stop.set()
            raise OSError("connection refused")
        return contextlib.nullcontext(_EmptySession())


async def _instant(_seconds: float) -> None:
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_db_down_on_the_first_pass_stops_the_worker_even_when_stopping() -> None:
    stop = asyncio.Event()
    stop.set()
    sessions = _DbDropsAfter(0, stop, stop_on_drop=False)
    with pytest.raises(worker.WorkerDbUnavailable):
        await worker.run_worker(
            sessions,  # type: ignore[arg-type]
            FakeExchangeAdapter(),
            handlers={},
            stop=stop,
            sleep=_instant,
        )
    assert sessions.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("healthy", [1, 2], ids=["claim_fails", "sweep_fails"])
async def test_stop_while_the_db_is_down_returns_without_retrying(healthy: int) -> None:
    stop = asyncio.Event()
    sessions = _DbDropsAfter(healthy, stop, stop_on_drop=True)
    await worker.run_worker(
        sessions,  # type: ignore[arg-type]
        FakeExchangeAdapter(),
        handlers={},
        stop=stop,
        sleep=_instant,
    )
    assert sessions.calls == healthy + 1


@pytest.mark.asyncio
async def test_db_down_without_stop_waits_and_retries_then_stop_exits() -> None:
    stop = asyncio.Event()
    sessions = _DbDropsAfter(1, stop, stop_on_drop=False)
    pauses: list[float] = []

    async def sleep(seconds: float) -> None:
        pauses.append(seconds)
        if len(pauses) == 3:
            stop.set()
        await asyncio.sleep(0)

    await worker.run_worker(
        sessions,  # type: ignore[arg-type]
        FakeExchangeAdapter(),
        handlers={},
        stop=stop,
        sleep=sleep,
    )
    # Кожен збій без зупинки — пауза `POLL_INTERVAL_MS`. Збій після SIGTERM у третій паузі — вихід без паузи.
    assert pauses == [worker.POLL_INTERVAL_MS / 1000] * 3
    assert sessions.calls == 1 + 3 + 1
