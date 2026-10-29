"""Rate limiter процесу: пріоритети, частка IP, 429, заголовки Bybit (ТЗ §6.5, T1.2).

Час — `FakeClock` (autouse). Очікування лімітера паркується в `ManualSleeper` і зрушує годинник лише
коли тест відпускає паузу; скасована пауза час не рухає.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

import pytest
from structlog.testing import capture_logs

from app.core.clock import FakeClock, monotonic_ms, now_ms
from app.exchange.errors import RateLimited
from app.exchange.rate_limit import (
    BACKOFF_BASE_MS,
    BACKOFF_CAP_MS,
    DEFAULT_SHARE_COLLECTOR_PCT,
    DEFAULT_SHARE_TRADING_PCT,
    DEFAULT_SHARE_WORKER_PCT,
    IP_BAN_MS,
    IP_REQUESTS,
    IP_WINDOW_MS,
    STARVATION_MS,
    UID_WINDOW_MS,
    EndpointGroup,
    GroupLimit,
    Jitter,
    LimitSnapshot,
    ProcessRole,
    RateLimitConfig,
    RateLimiter,
    RateLimiterError,
    RequestPriority,
    default_group_limits,
    limit_breach_from_response,
    parse_limit_headers,
    retry_after_ms_from,
    share_capacity,
)


def _min_jitter(low: int, _high: int) -> int:
    return low


class ManualSleeper:
    """Паркує `sleep(ms>0)`, доки тест не викличе `release`. `ms <= 0` — лише поступитися циклу."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.generation = 0
        self.pending_ms: int | None = None
        self._resume: asyncio.Future[None] | None = None

    def is_parked(self) -> bool:
        resume = self._resume
        return resume is not None and not resume.done()

    async def sleep(self, ms: int) -> None:
        if ms <= 0:
            await asyncio.sleep(0)
            return
        resume: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._resume = resume
        self.pending_ms = ms
        self.generation += 1
        try:
            await resume
        except asyncio.CancelledError:
            if not resume.done():
                resume.cancel()
            self.pending_ms = None
            raise
        self.clock.advance(ms)
        self.pending_ms = None

    async def release(self) -> None:
        resume = self._resume
        if resume is None or resume.done():
            raise RuntimeError("лімітер не чекає")
        resume.set_result(None)
        for _ in range(40):
            await asyncio.sleep(0)


async def until_parked(sleeper: ManualSleeper) -> None:
    """Дочекатися паузи, яка не переривається кілька тіків (черга вже перерахована)."""
    seen = 0
    generation = -1
    for _ in range(300):
        if sleeper.is_parked() and sleeper.generation == generation:
            seen += 1
            if seen >= 5:
                return
        else:
            generation = sleeper.generation if sleeper.is_parked() else -1
            seen = 0
        await asyncio.sleep(0)
    raise AssertionError(f"лімітер не зупинився (generation={sleeper.generation})")


def make_config(
    *,
    role: ProcessRole = ProcessRole.TRADING,
    share_pct: int = 50,
    ip_requests: int = 100,
    ip_window_ms: int = 1_000,
    starvation_ms: int = 10_000,
    backoff_base_ms: int = 200,
    backoff_cap_ms: int = 60_000,
    ip_ban_ms: int = 5_000,
    max_attempts: int = 4,
    groups: Mapping[EndpointGroup, GroupLimit] | None = None,
) -> RateLimitConfig:
    return RateLimitConfig(
        role=role,
        share_pct=share_pct,
        ip_requests=ip_requests,
        ip_window_ms=ip_window_ms,
        starvation_ms=starvation_ms,
        backoff_base_ms=backoff_base_ms,
        backoff_cap_ms=backoff_cap_ms,
        ip_ban_ms=ip_ban_ms,
        max_attempts=max_attempts,
        groups={} if groups is None else groups,
    )


@asynccontextmanager
async def opened(
    sleeper: ManualSleeper, config: RateLimitConfig | None = None, *, jitter: Jitter = _min_jitter
) -> AsyncIterator[RateLimiter]:
    limiter = RateLimiter(make_config() if config is None else config, sleeper=sleeper.sleep, jitter=jitter)
    try:
        yield limiter
    finally:
        await limiter.aclose()


async def _grab(
    limiter: RateLimiter, priority: RequestPriority, order: list[RequestPriority], group: EndpointGroup
) -> None:
    await limiter.acquire(priority=priority, group=group)
    order.append(priority)


async def _cancel(task: asyncio.Task[object]) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------- числа Bybit


def test_documented_bybit_limits_are_the_single_source() -> None:
    assert (IP_REQUESTS, IP_WINDOW_MS, IP_BAN_MS) == (600, 5_000, 600_000)
    assert (UID_WINDOW_MS, BACKOFF_BASE_MS, BACKOFF_CAP_MS, STARVATION_MS) == (1_000, 200, IP_BAN_MS, 2_000)
    assert (DEFAULT_SHARE_TRADING_PCT, DEFAULT_SHARE_WORKER_PCT, DEFAULT_SHARE_COLLECTOR_PCT) == (50, 30, 20)
    groups = default_group_limits()
    assert groups[EndpointGroup.ORDER_CREATE] == GroupLimit(10, 1_000)
    assert groups[EndpointGroup.ORDER_CANCEL] == GroupLimit(10, 1_000)
    assert groups[EndpointGroup.ORDER_REALTIME] == GroupLimit(50, 1_000)
    assert groups[EndpointGroup.ORDER_HISTORY] == GroupLimit(50, 1_000)
    assert groups[EndpointGroup.EXECUTION_LIST] == GroupLimit(50, 1_000)
    assert groups[EndpointGroup.POSITION_LIST] == GroupLimit(50, 1_000)
    assert groups[EndpointGroup.TRADING_STOP] == GroupLimit(10, 1_000)
    assert groups[EndpointGroup.SET_LEVERAGE] == GroupLimit(10, 1_000)
    assert groups[EndpointGroup.WALLET_BALANCE] == GroupLimit(50, 1_000)
    assert groups[EndpointGroup.SET_MARGIN_MODE] == GroupLimit(5, 1_000)
    assert EndpointGroup.MARKET not in groups


def test_default_shares_do_not_exceed_the_ip_limit() -> None:
    total = sum(
        share_capacity(IP_REQUESTS, pct)
        for pct in (DEFAULT_SHARE_TRADING_PCT, DEFAULT_SHARE_WORKER_PCT, DEFAULT_SHARE_COLLECTOR_PCT)
    )
    assert total <= IP_REQUESTS
    assert total == 300 + 180 + 120


def test_zero_share_capacity_is_rejected() -> None:
    with pytest.raises(ValueError, match="частка"):
        make_config(ip_requests=10, share_pct=1)


# ---------------------------------------------------------------- пріоритети


@pytest.mark.asyncio
async def test_protection_runs_before_queued_data(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    # Місткість IP = 1: другий запит уже стоїть у черзі, коли приходить захист.
    config = make_config(ip_requests=2, share_pct=50, ip_window_ms=1_000)
    order: list[RequestPriority] = []
    async with opened(sleeper, config) as limiter:
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)
        data = [
            asyncio.create_task(_grab(limiter, RequestPriority.DATA, order, EndpointGroup.MARKET)) for _ in range(2)
        ]
        protection = asyncio.create_task(_grab(limiter, RequestPriority.PROTECTION, order, EndpointGroup.MARKET))
        await until_parked(sleeper)
        assert protection.done() is False
        await sleeper.release()
        await protection
        assert order == [RequestPriority.PROTECTION]
        assert all(task.done() is False for task in data)
        for task in data:
            await _cancel(task)


@pytest.mark.asyncio
async def test_protection_does_not_wait_for_a_full_data_group(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    groups: dict[EndpointGroup, GroupLimit] = {
        EndpointGroup.ORDER_CREATE: GroupLimit(1, 60_000),
        EndpointGroup.TRADING_STOP: GroupLimit(5, 60_000),
    }
    config = make_config(ip_requests=200, share_pct=50, groups=groups)
    async with opened(sleeper, config) as limiter:
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.ORDER_CREATE)
        waiting = asyncio.create_task(limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.ORDER_CREATE))
        await until_parked(sleeper)
        await limiter.acquire(priority=RequestPriority.PROTECTION, group=EndpointGroup.TRADING_STOP)
        assert waiting.done() is False
        await _cancel(waiting)


@pytest.mark.asyncio
async def test_reconcile_beats_data_until_starvation(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    # Вікно 1000, поріг голодування 1001: на момент звільнення слота дані ще не «голодні».
    config = make_config(ip_requests=2, share_pct=50, ip_window_ms=1_000, starvation_ms=1_001)
    order: list[RequestPriority] = []
    async with opened(sleeper, config) as limiter:
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)
        data = asyncio.create_task(_grab(limiter, RequestPriority.DATA, order, EndpointGroup.MARKET))
        reconcile = asyncio.create_task(_grab(limiter, RequestPriority.RECONCILE, order, EndpointGroup.MARKET))
        await until_parked(sleeper)
        await sleeper.release()
        await reconcile
        assert order == [RequestPriority.RECONCILE]
        assert data.done() is False
        await _cancel(data)


@pytest.mark.asyncio
async def test_data_is_not_starved_by_reconcile(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    # Обидва чекають рівно вікно, і це вже поріг голодування: старший (DATA) іде першим.
    config = make_config(ip_requests=2, share_pct=50, ip_window_ms=1_000, starvation_ms=1_000)
    order: list[RequestPriority] = []
    async with opened(sleeper, config) as limiter:
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)
        data = asyncio.create_task(_grab(limiter, RequestPriority.DATA, order, EndpointGroup.MARKET))
        reconcile = asyncio.create_task(_grab(limiter, RequestPriority.RECONCILE, order, EndpointGroup.MARKET))
        await until_parked(sleeper)
        await sleeper.release()
        await data
        assert order == [RequestPriority.DATA]
        assert reconcile.done() is False
        await _cancel(reconcile)


@pytest.mark.asyncio
async def test_protection_beats_starved_data(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    config = make_config(ip_requests=2, share_pct=50, ip_window_ms=1_000, starvation_ms=0)
    order: list[RequestPriority] = []
    async with opened(sleeper, config) as limiter:
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)
        data = asyncio.create_task(_grab(limiter, RequestPriority.DATA, order, EndpointGroup.MARKET))
        protection = asyncio.create_task(_grab(limiter, RequestPriority.PROTECTION, order, EndpointGroup.MARKET))
        await until_parked(sleeper)
        await sleeper.release()
        await protection
        assert order == [RequestPriority.PROTECTION]
        assert data.done() is False
        await _cancel(data)


@pytest.mark.asyncio
async def test_same_priority_is_fifo(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    config = make_config(ip_requests=2, share_pct=50, ip_window_ms=1_000)
    order: list[int] = []

    async def grab(index: int, limiter: RateLimiter) -> None:
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)
        order.append(index)

    async with opened(sleeper, config) as limiter:
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)
        tasks = [asyncio.create_task(grab(index, limiter)) for index in range(3)]
        for index in range(3):
            await until_parked(sleeper)
            await sleeper.release()
            await tasks[index]
        assert order == [0, 1, 2]


# ---------------------------------------------------------------- частка процесу


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("role", "share_pct", "capacity"),
    [
        (ProcessRole.TRADING, 50, 5),
        (ProcessRole.WORKER, 30, 3),
        (ProcessRole.COLLECTOR, 20, 2),
    ],
)
async def test_process_never_exceeds_its_ip_share(
    fake_clock: FakeClock, role: ProcessRole, share_pct: int, capacity: int
) -> None:
    sleeper = ManualSleeper(fake_clock)
    window = 1_000
    config = make_config(role=role, share_pct=share_pct, ip_requests=10, ip_window_ms=window)
    grants: list[int] = []
    async with opened(sleeper, config) as limiter:
        assert limiter.ip_capacity == capacity
        for _ in range(capacity * 2):
            task = asyncio.create_task(limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET))
            completed = False
            for _ in range(30):
                await asyncio.sleep(0)
                if task.done():
                    completed = True
                    break
                if sleeper.is_parked():
                    break
            if not completed:
                assert sleeper.pending_ms == window
                await sleeper.release()
            await task
            grants.append(monotonic_ms())
    for moment in grants:
        in_window = [grant for grant in grants if moment - window < grant <= moment]
        assert len(in_window) <= capacity


@pytest.mark.asyncio
async def test_data_leaves_the_last_ip_slots_for_protection(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    # Вікно 10, резерв max(1, 10 // 20) = 1: дев'ять DATA, десятий слот — лише PROTECTION.
    config = make_config(ip_requests=20, share_pct=50, ip_window_ms=5_000)
    async with opened(sleeper, config) as limiter:
        assert limiter.ip_capacity == 10
        for _ in range(9):
            await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)
        waiting = asyncio.create_task(limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET))
        await until_parked(sleeper)
        assert waiting.done() is False
        await limiter.acquire(priority=RequestPriority.PROTECTION, group=EndpointGroup.MARKET)
        assert waiting.done() is False
        await _cancel(waiting)


@pytest.mark.asyncio
async def test_worker_cannot_spend_a_private_bucket(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    config = make_config(role=ProcessRole.WORKER, share_pct=30)
    async with opened(sleeper, config) as limiter:
        with pytest.raises(ValueError, match="приватна група"):
            await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.ORDER_CREATE)
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)


@pytest.mark.asyncio
async def test_cancelled_wait_does_not_spend_a_token(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    config = make_config(ip_requests=2, share_pct=50, ip_window_ms=5_000)
    async with opened(sleeper, config) as limiter:
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)
        waiting = asyncio.create_task(limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET))
        await until_parked(sleeper)
        await _cancel(waiting)
        again = asyncio.create_task(limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET))
        await until_parked(sleeper)
        assert again.done() is False
        await _cancel(again)


# ---------------------------------------------------------------- 429 і backoff


@pytest.mark.asyncio
async def test_rate_limit_pauses_and_retries_idempotent_call(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    attempts = 0

    async def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RateLimited("429", retry_after_ms=1_000)
        return "ok"

    async with opened(sleeper) as limiter:
        task = asyncio.create_task(
            limiter.call(
                priority=RequestPriority.DATA,
                group=EndpointGroup.MARKET,
                operation=operation,
                retry=True,
            )
        )
        await until_parked(sleeper)
        assert attempts == 1
        assert sleeper.pending_ms is not None
        assert sleeper.pending_ms >= 1_000
        before = monotonic_ms()
        await sleeper.release()
        assert await task == "ok"
        assert attempts == 2
        assert monotonic_ms() - before >= 1_000


@pytest.mark.asyncio
async def test_non_retryable_call_is_not_repeated_but_pauses_the_queue(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    attempts = 0

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise RateLimited("429", retry_after_ms=2_000)

    order: list[RequestPriority] = []
    async with opened(sleeper) as limiter:
        with pytest.raises(RateLimited):
            await limiter.call(
                priority=RequestPriority.DATA,
                group=EndpointGroup.MARKET,
                operation=operation,
                retry=False,
            )
        assert attempts == 1
        data = asyncio.create_task(_grab(limiter, RequestPriority.DATA, order, EndpointGroup.MARKET))
        protection = asyncio.create_task(_grab(limiter, RequestPriority.PROTECTION, order, EndpointGroup.MARKET))
        await until_parked(sleeper)
        assert sleeper.pending_ms is not None
        assert sleeper.pending_ms >= 2_000
        assert protection.done() is False
        await sleeper.release()
        await protection
        assert order[0] is RequestPriority.PROTECTION
        if not data.done():
            await _cancel(data)


@pytest.mark.asyncio
async def test_retries_stop_at_max_attempts(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    attempts = 0

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise RateLimited("429", retry_after_ms=50)

    config = make_config(max_attempts=3, backoff_base_ms=200, backoff_cap_ms=60_000)
    async with opened(sleeper, config) as limiter:
        task = asyncio.create_task(
            limiter.call(
                priority=RequestPriority.RECONCILE,
                group=EndpointGroup.MARKET,
                operation=operation,
                retry=True,
            )
        )
        await until_parked(sleeper)
        assert attempts == 1
        await sleeper.release()
        await until_parked(sleeper)
        assert attempts == 2
        await sleeper.release()
        with pytest.raises(RateLimited):
            await task
        assert attempts == 3


@pytest.mark.asyncio
async def test_ip_ban_pauses_at_least_the_configured_minimum(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    config = make_config(ip_ban_ms=5_000, backoff_cap_ms=60_000)

    async def operation() -> None:
        raise RateLimited("403 access too frequent", retry_after_ms=0, ip_ban=True)

    async with opened(sleeper, config) as limiter:
        with pytest.raises(RateLimited) as exc:
            await limiter.call(
                priority=RequestPriority.PROTECTION,
                group=EndpointGroup.MARKET,
                operation=operation,
                retry=False,
            )
        assert exc.value.ip_ban is True
        waiting = asyncio.create_task(limiter.acquire(priority=RequestPriority.PROTECTION, group=EndpointGroup.MARKET))
        await until_parked(sleeper)
        assert sleeper.pending_ms is not None
        assert sleeper.pending_ms >= 5_000
        await _cancel(waiting)


@pytest.mark.asyncio
async def test_backoff_grows_and_stays_inside_jitter_bounds(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    chosen = iter((150, 300, 400))

    def jitter(low: int, high: int) -> int:
        value = next(chosen)
        assert low <= value <= high
        return value

    pauses: list[int] = []

    async def operation() -> None:
        raise RateLimited("429")

    config = make_config(max_attempts=3, backoff_base_ms=200, backoff_cap_ms=60_000)
    async with opened(sleeper, config, jitter=jitter) as limiter:
        task = asyncio.create_task(
            limiter.call(
                priority=RequestPriority.DATA,
                group=EndpointGroup.MARKET,
                operation=operation,
                retry=True,
            )
        )
        await until_parked(sleeper)
        assert sleeper.pending_ms is not None
        pauses.append(sleeper.pending_ms)
        await sleeper.release()
        await until_parked(sleeper)
        assert sleeper.pending_ms is not None
        pauses.append(sleeper.pending_ms)
        await sleeper.release()
        with pytest.raises(RateLimited):
            await task
    # Сходинка 1: [base/2, base] = [100, 200]. Сходинка 2: [200, 400].
    assert 100 <= pauses[0] <= 200
    assert 200 <= pauses[1] <= 400
    assert pauses[1] >= pauses[0]


# ---------------------------------------------------------------- заголовки


def test_parse_limit_headers_is_case_insensitive() -> None:
    snapshot = parse_limit_headers(
        {
            "X-Bapi-Limit": "10",
            "X-Bapi-Limit-Status": "9",
            "X-Bapi-Limit-Reset-Timestamp": "1672738134824",
        }
    )
    assert snapshot == LimitSnapshot(limit=10, remaining=9, reset_ts_ms=1_672_738_134_824)


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-Bapi-Limit": "10", "X-Bapi-Limit-Status": "9"},
        {"X-Bapi-Limit": "0", "X-Bapi-Limit-Status": "0", "X-Bapi-Limit-Reset-Timestamp": "1"},
        {"X-Bapi-Limit": "10", "X-Bapi-Limit-Status": "11", "X-Bapi-Limit-Reset-Timestamp": "1"},
        {"X-Bapi-Limit": "nope", "X-Bapi-Limit-Status": "1", "X-Bapi-Limit-Reset-Timestamp": "1"},
    ],
)
def test_malformed_limit_headers_are_ignored(headers: dict[str, str]) -> None:
    assert parse_limit_headers(headers) is None


def test_retry_after_uses_the_longer_of_header_and_reset() -> None:
    assert retry_after_ms_from(retry_after="2", reset_ts_ms=5_000, now=1_000) == 4_000
    assert retry_after_ms_from(retry_after="Wed, 21 Oct 2015 07:28:00 GMT", reset_ts_ms=None, now=0) is None
    assert retry_after_ms_from(retry_after=None, reset_ts_ms=500, now=1_000) is None


@pytest.mark.asyncio
async def test_headers_can_only_tighten_the_bucket(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    groups = {EndpointGroup.ORDER_CREATE: GroupLimit(2, 60_000)}
    config = make_config(groups=groups)
    async with opened(sleeper, config) as limiter:
        limiter.observe(
            EndpointGroup.ORDER_CREATE,
            LimitSnapshot(limit=100, remaining=100, reset_ts_ms=now_ms()),
        )
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.ORDER_CREATE)
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.ORDER_CREATE)
        extra = asyncio.create_task(limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.ORDER_CREATE))
        await until_parked(sleeper)
        assert extra.done() is False
        await _cancel(extra)


@pytest.mark.asyncio
async def test_headers_remaining_blocks_until_reset(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    groups = {EndpointGroup.ORDER_CREATE: GroupLimit(5, 60_000)}
    config = make_config(groups=groups)
    async with opened(sleeper, config) as limiter:
        limiter.observe(
            EndpointGroup.ORDER_CREATE,
            LimitSnapshot(limit=5, remaining=0, reset_ts_ms=now_ms() + 4_000),
        )
        waiting = asyncio.create_task(
            limiter.acquire(priority=RequestPriority.PROTECTION, group=EndpointGroup.ORDER_CREATE)
        )
        await until_parked(sleeper)
        assert sleeper.pending_ms is not None
        assert sleeper.pending_ms >= 4_000
        # Інша група й IP не заморожені цим заголовком.
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)
        assert waiting.done() is False
        await _cancel(waiting)


def test_limit_breach_classification() -> None:
    banned = limit_breach_from_response(http_status=403, ret_msg="403, access too frequent")
    assert banned is not None
    assert banned.ip_ban is True
    limited = limit_breach_from_response(http_status=200, ret_code=10006, retry_after_ms=250)
    assert limited is not None
    assert (limited.ip_ban, limited.retry_after_ms) == (False, 250)
    assert limit_breach_from_response(http_status=429) is not None
    # Інший 403 — не ліміт (може бути авторизація).
    assert limit_breach_from_response(http_status=403, ret_msg="invalid api key") is None
    assert limit_breach_from_response(http_status=400, ret_code="10001") is None


@pytest.mark.asyncio
async def test_closed_limiter_rejects_new_acquire(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    limiter = RateLimiter(make_config(), sleeper=sleeper.sleep, jitter=_min_jitter)
    await limiter.aclose()
    with pytest.raises(RateLimiterError):
        await limiter.acquire(priority=RequestPriority.DATA, group=EndpointGroup.MARKET)


@pytest.mark.asyncio
async def test_from_settings_takes_the_role_share() -> None:
    from app.core.config import Settings

    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    async with RateLimiter.from_settings(settings, role=ProcessRole.COLLECTOR) as limiter:
        assert limiter.role is ProcessRole.COLLECTOR
        assert limiter.ip_capacity == IP_REQUESTS * DEFAULT_SHARE_COLLECTOR_PCT // 100


@pytest.mark.asyncio
async def test_role_comes_from_the_entry_point_not_the_shared_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.core.config import Settings

    # `.env` спільний для трьох процесів: роль із нього дала б воркеру частку торгового процесу.
    monkeypatch.setenv("RATE_LIMIT_ROLE", "trading")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    async with RateLimiter.from_settings(settings, role=ProcessRole.WORKER) as limiter:
        assert limiter.role is ProcessRole.WORKER
        assert limiter.ip_capacity == IP_REQUESTS * DEFAULT_SHARE_WORKER_PCT // 100


def test_role_must_be_a_process_role() -> None:
    with pytest.raises(ValueError, match="role"):
        RateLimitConfig(role="trading", share_pct=50)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_a_burst_of_429_does_not_escalate_the_pause(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    async with opened(sleeper, make_config()) as limiter:
        for _ in range(13):
            limiter.penalize()
        waiting = asyncio.create_task(limiter.acquire(priority=RequestPriority.PROTECTION, group=EndpointGroup.MARKET))
        await until_parked(sleeper)
        first = sleeper.pending_ms
        assert first == 100
        await sleeper.release()
        await waiting
        limiter.penalize()
        again = asyncio.create_task(limiter.acquire(priority=RequestPriority.PROTECTION, group=EndpointGroup.MARKET))
        await until_parked(sleeper)
        assert sleeper.pending_ms == 200
        await _cancel(again)


@pytest.mark.asyncio
async def test_header_reset_beyond_the_window_holds_the_group_at_most_one_window(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    # UID-вікно 1 с. Reset на 45 с уперед — зсув годинника біржі або сміття в заголовку: SL не чекає 45 с.
    groups = {EndpointGroup.TRADING_STOP: GroupLimit(10, UID_WINDOW_MS)}
    async with opened(sleeper, make_config(groups=groups)) as limiter:
        with capture_logs() as logs:
            limiter.observe(
                EndpointGroup.TRADING_STOP,
                LimitSnapshot(limit=10, remaining=0, reset_ts_ms=now_ms() + 45_000),
            )
        assert [entry["reset_in_ms"] for entry in logs if entry["event"] == "rate_limit_reset_beyond_window"] == [
            45_000
        ]
        start = monotonic_ms()
        waiting = asyncio.create_task(
            limiter.acquire(priority=RequestPriority.PROTECTION, group=EndpointGroup.TRADING_STOP)
        )
        await until_parked(sleeper)
        assert sleeper.pending_ms == UID_WINDOW_MS
        await sleeper.release()
        assert waiting.done() is True
        await waiting
        assert monotonic_ms() - start == UID_WINDOW_MS


@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [False, True])
async def test_rate_limited_survives_close_during_the_request(fake_clock: FakeClock, retry: bool) -> None:
    # Запит уже пішов у мережу: викликач має побачити `RateLimited` (не відмова), а не `RateLimiterError`
    # («запит не відправлявся»).
    sleeper = ManualSleeper(fake_clock)
    attempts = 0
    async with opened(sleeper) as limiter:

        async def operation() -> None:
            nonlocal attempts
            attempts += 1
            await limiter.aclose()
            raise RateLimited("429", retry_after_ms=1_000)

        with pytest.raises(RateLimited):
            await limiter.call(
                priority=RequestPriority.PROTECTION,
                group=EndpointGroup.MARKET,
                operation=operation,
                retry=retry,
            )
        assert attempts == 1


@pytest.mark.asyncio
async def test_penalize_after_close_is_a_no_op(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    limiter = RateLimiter(make_config(), sleeper=sleeper.sleep, jitter=_min_jitter)
    await limiter.aclose()
    limiter.penalize(retry_after_ms=1_000)
    with pytest.raises(ValueError, match="retry_after_ms"):
        limiter.penalize(retry_after_ms=-1)


@pytest.mark.asyncio
async def test_retry_after_is_capped_by_backoff(fake_clock: FakeClock) -> None:
    sleeper = ManualSleeper(fake_clock)
    config = make_config(backoff_cap_ms=60_000, ip_ban_ms=5_000)
    async with opened(sleeper, config) as limiter:
        limiter.penalize(retry_after_ms=86_400_000)
        waiting = asyncio.create_task(limiter.acquire(priority=RequestPriority.PROTECTION, group=EndpointGroup.MARKET))
        await until_parked(sleeper)
        assert sleeper.pending_ms == 60_000
        await _cancel(waiting)
