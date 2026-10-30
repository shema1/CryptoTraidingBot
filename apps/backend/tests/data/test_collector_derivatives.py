"""Хвіст деривативів у колекторі (ТЗ §4.4, §6.4, T1.10): розклад `DerivativesSync`, годинник, збої, зупинка."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection
from structlog.testing import capture_logs

from app.data.collector import run_derivatives
from app.data.derivatives import DerivativesStats, DerivativesSync, DerivativesSyncResult
from tests.data.test_derivatives_backfill import HOUR, _counts, _fill, _sessions
from tests.data.test_derivatives_backfill import Book as DerivativesBook
from tests.data.test_liquidations import _STOP_GUARD_S, _Clock, _events
from tests.exchange.factories import FAKE_NOW_MS, SYMBOL


def _stop_after(stop: asyncio.Event, polls: int, *, each: Any = None) -> Any:
    """Сон між опитуваннями: на `polls`-му виклику виставляє `stop`; `each(n)` — дія на n-му виклику."""
    calls = 0

    async def sleep(seconds: float) -> None:
        nonlocal calls
        calls += 1
        if each is not None:
            each(calls)
        if calls >= polls:
            stop.set()

    return sleep


@pytest.mark.asyncio
async def test_derivatives_wait_for_a_trusted_clock() -> None:
    clock = _Clock(trusted=False)
    stop = asyncio.Event()
    passes: list[int] = []

    async def run_if_due() -> DerivativesSyncResult | None:
        passes.append(1)
        return None

    def trust_on_first_poll(n: int) -> None:
        if n == 1:
            clock.trusted = True

    with capture_logs() as logs:
        await asyncio.wait_for(
            run_derivatives(run_if_due, clock=clock, stop=stop, sleep=_stop_after(stop, 2, each=trust_on_first_poll)),
            _STOP_GUARD_S,
        )

    # Незакриту за часом біржі годину локальний годинник із зсувом ≥ 1 с прийняв би за закриту — і записав назавжди.
    assert passes == [1]
    assert [entry["event"] for entry in _events(logs, "derivatives_waiting_for_clock")] == [
        "derivatives_waiting_for_clock"
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "traced"),
    [(SQLAlchemyError("no-db"), False), (RuntimeError("bug"), True)],
    ids=["db", "unexpected"],
)
async def test_failed_derivatives_pass_is_logged_and_the_loop_goes_on(error: Exception, traced: bool) -> None:
    stop = asyncio.Event()
    passes: list[int] = []

    async def run_if_due() -> DerivativesSyncResult | None:
        passes.append(1)
        if len(passes) == 1:
            raise error
        return None

    with capture_logs() as logs:
        await asyncio.wait_for(
            run_derivatives(run_if_due, clock=_Clock(), stop=stop, sleep=_stop_after(stop, 2)), _STOP_GUARD_S
        )

    assert len(passes) == 2
    failed = _events(logs, "derivatives_sync_failed")
    assert [(entry["error_type"], "exc_info" in entry) for entry in failed] == [(type(error).__name__, traced)]


@pytest.mark.asyncio
async def test_stop_interrupts_a_running_derivatives_pass() -> None:
    stop = asyncio.Event()
    started = asyncio.Event()
    cancelled: list[bool] = []

    async def run_if_due() -> DerivativesSyncResult | None:
        started.set()
        try:
            await asyncio.Event().wait()  # біржа повільна або лімітер тримає чергу
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        raise AssertionError("прохід не мав завершитись")

    async def never(seconds: float) -> None:
        raise AssertionError("до сну між опитуваннями не мало дійти")

    with capture_logs() as logs:
        loop = asyncio.create_task(run_derivatives(run_if_due, clock=_Clock(), stop=stop, sleep=never))
        await asyncio.wait_for(started.wait(), _STOP_GUARD_S)
        stop.set()
        await asyncio.wait_for(loop, _STOP_GUARD_S)

    assert cancelled == [True]
    assert [entry["event"] for entry in _events(logs, "derivatives_sync_interrupted")] == [
        "derivatives_sync_interrupted"
    ]
    assert _events(logs, "derivatives_sync_failed") == []


@pytest.mark.asyncio
async def test_queued_history_and_failed_symbols_are_in_the_pass_log() -> None:
    stop = asyncio.Event()
    result = DerivativesSyncResult(
        inline=DerivativesStats(open_interest=2, long_short=1),
        job_id=None,
        job_symbols=("ETH/USDT:USDT",),
        failed_symbols=(SYMBOL,),
    )

    async def run_if_due() -> DerivativesSyncResult | None:
        return result

    with capture_logs() as logs:
        await asyncio.wait_for(
            run_derivatives(run_if_due, clock=_Clock(), stop=stop, sleep=_stop_after(stop, 1)), _STOP_GUARD_S
        )

    synced = _events(logs, "derivatives_synced")
    assert len(synced) == 1
    assert synced[0]["open_interest"] == 2
    assert synced[0]["long_short"] == 1
    assert synced[0]["job_symbols"] == ["ETH/USDT:USDT"]
    assert synced[0]["failed_symbols"] == [SYMBOL]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_collector_loop_writes_the_derivatives_tail(conn: AsyncConnection) -> None:
    book = DerivativesBook()
    _fill(book, [FAKE_NOW_MS - 2 * HOUR], SYMBOL)
    sync = DerivativesSync(
        _sessions(conn), book, symbols=(SYMBOL,), start_ms=FAKE_NOW_MS - 2 * HOUR, interval_ms=600_000
    )
    stop = asyncio.Event()

    with capture_logs() as logs:
        await asyncio.wait_for(
            run_derivatives(sync.run_if_due, clock=_Clock(), stop=stop, sleep=_stop_after(stop, 1)), _STOP_GUARD_S
        )

    counts = await _counts(conn)
    assert counts["open_interest"] == 1
    assert counts["long_short_ratio"] == 1
    assert [entry["open_interest"] for entry in _events(logs, "derivatives_synced")] == [1]
