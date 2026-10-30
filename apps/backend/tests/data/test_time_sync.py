"""Контроль часу (T1.6, ТЗ §4.3): розбіжність з `fetch_time` біржі і стрибки системного годинника.

Два сценарії «Готово, коли»: (1) розбіжність > 1 с → блок нових входів і попередження; (2) стрибок системного
годинника відносно монотонного → `ClockJump` (бот іде в `RECONCILING`), нові входи заблоковані, доки годинник не
перевірено знову й звірку після стрибка не підтверджено. Час — лише `FakeClock`; біржа — межа адаптера
(`fetch_time`): `FakeExchangeAdapter` або скриптове джерело з керованим RTT.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, MutableMapping, Sequence
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st
from structlog.testing import capture_logs

from app.core.clock import CLOCK_DRIFT_LIMIT_MS, FakeClock, now_ms, use_clock
from app.data.gaps import CANDLE_SETTLE_MS
from app.data.time_sync import (
    CLOCK_JUMP_LIMIT_MS,
    CLOCK_RATE_TOLERANCE_PPM,
    DRIFT_CHECK_INTERVAL_MS,
    DRIFT_CHECK_TIMEOUT_MS,
    DRIFT_RECHECK_MS,
    DRIFT_RESULT_MAX_AGE_MS,
    DRIFT_SAMPLES,
    JUMP_CHECK_INTERVAL_MS,
    MAX_SAMPLE_RTT_MS,
    ClockBlockReason,
    ClockGuard,
    ClockJump,
    ReconcileTicket,
)
from app.exchange.bybit import KLINE_FINALITY_MARGIN_MS
from app.exchange.errors import ExchangeTimeout, ExchangeUnavailable
from app.exchange.fake import Fault
from tests.exchange.factories import make_fake

UNVERIFIED = ClockBlockReason.UNVERIFIED
DRIFT = ClockBlockReason.DRIFT
JUMP = ClockBlockReason.JUMP_UNRECONCILED
# Області звірки — акаунти (ТЗ §8.1: стан окремо для кожного акаунта).
DEMO = "account:demo"
LIVE = "account:live"

SleepMs = Callable[[int], Awaitable[None]]

# Час фейкової біржі = локальний годинник + `server_time_offset_ms`. Тож `jump_wall(d)` без зміни зсуву — це
# «годинник був правильний, змінився лише монотонний» (пробудження після сну), а «зламався лише локальний
# годинник» — `jump_wall(d)` разом зі зсувом `-d`.


class ScriptedTime:
    """`fetch_time` з керованим зсувом біржі й RTT: годинник іде вперед на RTT, час біржі — на середині запиту."""

    def __init__(
        self,
        clock: FakeClock,
        *,
        offset_ms: int = 0,
        rtts: Sequence[int] = (0,),
        errors: Sequence[BaseException | None] = (),
        jump_during_call: int | None = None,
    ) -> None:
        self._clock = clock
        self.offset_ms = offset_ms
        self.rtts = list(rtts)
        self.errors = list(errors)
        self.jump_during_call = jump_during_call
        self.calls = 0
        self.result: object = None

    async def fetch_time(self) -> int:
        index = self.calls
        self.calls += 1
        if index < len(self.errors) and self.errors[index] is not None:
            raise self.errors[index]  # type: ignore[misc]
        rtt = self.rtts[min(index, len(self.rtts) - 1)]
        half = rtt // 2
        self._clock.advance(half)
        server = now_ms() + self.offset_ms
        if self.jump_during_call is not None:
            self._clock.jump_wall(self.jump_during_call)
        self._clock.advance(rtt - half)
        if self.result is not None:
            return self.result  # type: ignore[return-value]
        return server


def _guard(source: object, **kwargs: int) -> ClockGuard:
    return ClockGuard(source, **kwargs)  # type: ignore[arg-type]


def _events(logs: Sequence[MutableMapping[str, Any]], name: str) -> list[MutableMapping[str, Any]]:
    return [entry for entry in logs if entry["event"] == name]


# --------------------------------------------------------------------- константи: одне джерело (ТЗ §4.3)


def test_thresholds_follow_the_spec_and_share_one_source() -> None:
    assert CLOCK_DRIFT_LIMIT_MS == 1_000
    assert CLOCK_JUMP_LIMIT_MS == CLOCK_DRIFT_LIMIT_MS
    assert JUMP_CHECK_INTERVAL_MS == 1_000
    assert DRIFT_CHECK_INTERVAL_MS == 10 * 60 * 1_000
    assert DRIFT_RECHECK_MS < DRIFT_CHECK_INTERVAL_MS
    assert DRIFT_RESULT_MAX_AGE_MS > DRIFT_CHECK_INTERVAL_MS
    assert DRIFT_SAMPLES >= 1
    assert DRIFT_CHECK_TIMEOUT_MS <= DRIFT_RECHECK_MS
    # 20 хв старіння виміру не з'їдають більшу частину допуску.
    assert DRIFT_RESULT_MAX_AGE_MS * CLOCK_RATE_TOLERANCE_PPM // 1_000_000 < CLOCK_DRIFT_LIMIT_MS // 4
    # Половина RTT — похибка виміру; вона має лишати місце для розбіжності в межах ліміту.
    assert MAX_SAMPLE_RTT_MS // 2 < CLOCK_DRIFT_LIMIT_MS
    # Запас детектора прогалин стоїть на тому самому допуску годинника.
    assert CANDLE_SETTLE_MS == KLINE_FINALITY_MARGIN_MS + CLOCK_DRIFT_LIMIT_MS


def test_trading_clock_tolerance_is_the_same_limit() -> None:
    from app.trading.models import CLOCK_TOLERANCE_MS

    assert CLOCK_TOLERANCE_MS == CLOCK_DRIFT_LIMIT_MS


# --------------------------------------------------------------------- сценарій 1: розбіжність з біржею


def test_entries_are_blocked_until_the_clock_is_checked() -> None:
    guard = _guard(make_fake())
    state = guard.state()
    assert state.block_reasons == (UNVERIFIED,)
    assert state.entries_allowed is False
    assert state.offset_ms is None


@pytest.mark.asyncio
async def test_clock_in_sync_with_the_exchange_allows_entries() -> None:
    fake = make_fake()
    guard = _guard(fake)
    with capture_logs() as logs:
        state = await guard.check_drift()
    assert state.block_reasons == ()
    assert state.entries_allowed is True
    assert state.offset_ms == 0
    assert state.uncertainty_ms == 0
    assert len(fake.calls_to("fetch_time")) == DRIFT_SAMPLES
    assert _events(logs, "clock_drift_ok")[0]["offset_ms"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [1_000, -1_000, 1_001, -1_001, 1_500, -1_500, 60_000])
async def test_drift_of_one_second_or_more_blocks_entries_and_warns(offset: int) -> None:
    # ТЗ §4.3: «більше 1 с» — блок, а повернення — лише коли розбіжність «менша за 1 с»: рівно 1000 мс блокує.
    guard = _guard(make_fake(server_time_offset_ms=offset))
    with capture_logs() as logs:
        state = await guard.check_drift()
    assert state.block_reasons == (DRIFT,)
    assert state.entries_allowed is False
    assert state.offset_ms == offset
    [warning] = _events(logs, "clock_drift_exceeded")
    assert warning["log_level"] == "warning"
    assert warning["offset_ms"] == offset
    assert warning["limit_ms"] == CLOCK_DRIFT_LIMIT_MS
    assert warning["local_clock"] == ("behind" if offset > 0 else "ahead")


@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [999, -999, 0])
async def test_drift_under_one_second_is_allowed(offset: int) -> None:
    state = await _guard(make_fake(server_time_offset_ms=offset)).check_drift()
    assert state.entries_allowed is True


@pytest.mark.asyncio
async def test_half_of_the_round_trip_counts_against_the_limit(fake_clock: FakeClock) -> None:
    # 900 мс розбіжності + 150 мс невизначеності (RTT 300) — можливо понад 1 с: fail-closed.
    source = ScriptedTime(fake_clock, offset_ms=900, rtts=(300,))
    state = await _guard(source).check_drift()
    assert state.offset_ms == 900
    assert state.uncertainty_ms == 150
    assert state.drift_bound_ms == 1_050
    assert state.block_reasons == (DRIFT,)


@pytest.mark.asyncio
async def test_odd_round_trip_rounds_uncertainty_up(fake_clock: FakeClock) -> None:
    source = ScriptedTime(fake_clock, offset_ms=-900, rtts=(201,))
    state = await _guard(source).check_drift()
    assert state.uncertainty_ms == 101
    assert state.drift_bound_ms == 1_001
    assert state.block_reasons == (DRIFT,)


@pytest.mark.asyncio
async def test_sample_with_the_smallest_round_trip_wins(fake_clock: FakeClock) -> None:
    source = ScriptedTime(fake_clock, offset_ms=800, rtts=(700, 60, 400))
    state = await _guard(source).check_drift()
    assert source.calls == 3
    assert state.uncertainty_ms == 30
    assert state.offset_ms == 800
    assert state.entries_allowed is True


@pytest.mark.asyncio
async def test_slow_samples_are_inconclusive_and_do_not_count_as_drift(fake_clock: FakeClock) -> None:
    source = ScriptedTime(fake_clock, offset_ms=0, rtts=(MAX_SAMPLE_RTT_MS + 1,))
    guard = _guard(source)
    with capture_logs() as logs:
        state = await guard.check_drift()
    assert state.block_reasons == (UNVERIFIED,)
    [warning] = _events(logs, "clock_drift_check_inconclusive")
    assert warning["log_level"] == "warning"
    assert warning["rtts_ms"] == [MAX_SAMPLE_RTT_MS + 1] * DRIFT_SAMPLES


@pytest.mark.asyncio
async def test_sample_at_the_round_trip_cap_is_used(fake_clock: FakeClock) -> None:
    source = ScriptedTime(fake_clock, offset_ms=0, rtts=(MAX_SAMPLE_RTT_MS,))
    state = await _guard(source).check_drift()
    assert state.uncertainty_ms == MAX_SAMPLE_RTT_MS // 2
    assert state.entries_allowed is True


@pytest.mark.asyncio
async def test_failed_check_without_a_previous_result_keeps_entries_blocked() -> None:
    fake = make_fake()
    fake.fail_next("fetch_time", Fault.timeout())
    guard = _guard(fake)
    with capture_logs() as logs:
        state = await guard.check_drift()
    assert state.block_reasons == (UNVERIFIED,)
    # Після збою решта вимірів цієї перевірки не робиться: біржа, найімовірніше, недоступна.
    assert len(fake.calls_to("fetch_time")) == 1
    [warning] = _events(logs, "clock_drift_check_failed")
    assert warning["log_level"] == "warning"
    assert warning["error_type"] == "ExchangeTimeout"


@pytest.mark.asyncio
async def test_unexpected_error_is_logged_with_traceback_and_blocks(fake_clock: FakeClock) -> None:
    source = ScriptedTime(fake_clock, errors=[RuntimeError("bug")])
    with capture_logs() as logs:
        state = await _guard(source).check_drift()
    assert state.block_reasons == (UNVERIFIED,)
    [warning] = _events(logs, "clock_drift_check_failed")
    assert warning["error_type"] == "RuntimeError"
    assert isinstance(warning["exc_info"], RuntimeError)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [1.5e12, True, "1767225600000", 0, -1, None])
async def test_non_millisecond_exchange_time_is_a_failed_check(fake_clock: FakeClock, bad: object) -> None:
    source = ScriptedTime(fake_clock)
    source.result = bad if bad is not None else object()
    with capture_logs() as logs:
        state = await _guard(source).check_drift()
    assert state.block_reasons == (UNVERIFIED,)
    assert _events(logs, "clock_drift_check_failed")[0]["error_type"] == "MalformedResponse"


@pytest.mark.asyncio
async def test_partial_failure_uses_the_samples_already_taken(fake_clock: FakeClock) -> None:
    source = ScriptedTime(fake_clock, offset_ms=100, rtts=(40,), errors=[None, ExchangeUnavailable("5xx")])
    state = await _guard(source).check_drift()
    assert source.calls == 2
    assert state.offset_ms == 100
    assert state.entries_allowed is True


@pytest.mark.asyncio
async def test_failed_check_keeps_the_previous_result_until_it_expires(fake_clock: FakeClock) -> None:
    fake = make_fake()
    guard = _guard(fake)
    assert (await guard.check_drift()).entries_allowed is True
    fake_clock.advance(DRIFT_CHECK_INTERVAL_MS)
    fake.fail_next("fetch_time", Fault.unavailable(), times=100)
    assert (await guard.check_drift()).entries_allowed is True
    fake_clock.advance(DRIFT_RESULT_MAX_AGE_MS - DRIFT_CHECK_INTERVAL_MS)
    assert guard.state().entries_allowed is True
    fake_clock.advance(1)
    state = guard.state()
    assert state.block_reasons == (UNVERIFIED,)
    assert state.sample_age_ms == DRIFT_RESULT_MAX_AGE_MS + 1


@pytest.mark.asyncio
async def test_drift_clears_after_the_clock_is_fixed() -> None:
    fake = make_fake(server_time_offset_ms=2_000)
    guard = _guard(fake)
    assert (await guard.check_drift()).block_reasons == (DRIFT,)
    fake.server_time_offset_ms = 30
    assert (await guard.check_drift()).entries_allowed is True


@pytest.mark.asyncio
async def test_small_wall_steps_between_checks_accumulate_into_drift(fake_clock: FakeClock) -> None:
    # Кроки по 0,6 с — не стрибки (≤ 1 с), але разом розбіжність 1,2 с: нові входи блокуються без нового запиту.
    fake = make_fake()
    guard = _guard(fake)
    await guard.check_drift()
    fake_clock.jump_wall(600)
    assert guard.tick() is None
    assert guard.state().entries_allowed is True
    fake_clock.advance(1_000)
    fake_clock.jump_wall(600)
    assert guard.tick() is None
    state = guard.state()
    assert state.offset_ms == -1_200
    assert state.block_reasons == (DRIFT,)
    assert len(fake.calls_to("fetch_time")) == DRIFT_SAMPLES


@pytest.mark.asyncio
async def test_uncertainty_grows_with_the_age_of_the_sample(fake_clock: FakeClock) -> None:
    # Швидкості монотонного і системного годинників розходяться: 100 ppm від віку виміру.
    guard = _guard(make_fake(server_time_offset_ms=900))
    assert (await guard.check_drift()).entries_allowed is True
    fake_clock.advance(999_999)
    assert guard.state().uncertainty_ms == 99
    assert guard.state().entries_allowed is True
    fake_clock.advance(1)
    state = guard.state()
    assert state.uncertainty_ms == 100
    assert state.drift_bound_ms == 1_000
    assert state.block_reasons == (DRIFT,)


@given(
    offset=st.integers(min_value=-5_000, max_value=5_000),
    rtt=st.integers(min_value=0, max_value=MAX_SAMPLE_RTT_MS),
)
def test_verdict_is_fail_closed_for_any_offset_and_round_trip(offset: int, rtt: int) -> None:
    clock = FakeClock(now_ms=1_767_225_600_000, monotonic_ms=1_000_000)
    with use_clock(clock):
        state = asyncio.run(_guard(ScriptedTime(clock, offset_ms=offset, rtts=(rtt,))).check_drift())
    bound = abs(offset) + (rtt + 1) // 2
    assert state.drift_bound_ms == bound
    assert state.entries_allowed is (bound < CLOCK_DRIFT_LIMIT_MS)


# --------------------------------------------------------------------- сценарій 2: стрибок системного годинника


@pytest.mark.parametrize("delta", [CLOCK_JUMP_LIMIT_MS + 1, -(CLOCK_JUMP_LIMIT_MS + 1), 3_600_000, -30_000])
def test_wall_jump_is_detected(fake_clock: FakeClock, delta: int) -> None:
    guard = _guard(make_fake())
    fake_clock.advance(JUMP_CHECK_INTERVAL_MS)
    fake_clock.jump_wall(delta)
    with capture_logs() as logs:
        jump = guard.tick()
    assert jump == ClockJump(
        skew_ms=delta,
        wall_delta_ms=JUMP_CHECK_INTERVAL_MS + delta,
        mono_delta_ms=JUMP_CHECK_INTERVAL_MS,
        epoch=1,
    )
    [warning] = _events(logs, "clock_jump_detected")
    assert warning["log_level"] == "warning"
    assert warning["skew_ms"] == delta
    assert warning["epoch"] == 1
    assert guard.state().jump_epoch == 1


@pytest.mark.parametrize("delta", [CLOCK_JUMP_LIMIT_MS, -CLOCK_JUMP_LIMIT_MS, 0, 15])
def test_step_of_at_most_one_second_is_not_a_jump(fake_clock: FakeClock, delta: int) -> None:
    guard = _guard(make_fake())
    fake_clock.advance(JUMP_CHECK_INTERVAL_MS)
    fake_clock.jump_wall(delta)
    assert guard.tick() is None
    assert guard.state().jump_epoch == 0


def test_stalled_loop_is_not_a_jump(fake_clock: FakeClock) -> None:
    # Цикл подій простояв 30 с: обидва годинники пройшли однаково.
    guard = _guard(make_fake())
    fake_clock.advance(30_000)
    assert guard.tick() is None


def test_wake_from_sleep_is_a_jump(fake_clock: FakeClock) -> None:
    # Монотонний годинник ОС (macOS `mach_absolute_time`, Linux `CLOCK_MONOTONIC`) у сні стоїть, системний — іде.
    guard = _guard(make_fake())
    fake_clock.jump_wall(3_600_000)
    jump = guard.tick()
    assert jump is not None
    assert jump.skew_ms == 3_600_000
    assert jump.mono_delta_ms == 0


def test_ticks_compare_with_the_previous_tick(fake_clock: FakeClock) -> None:
    guard = _guard(make_fake())
    fake_clock.jump_wall(5_000)
    assert guard.tick() is not None
    fake_clock.advance(JUMP_CHECK_INTERVAL_MS)
    assert guard.tick() is None
    assert guard.state().jump_epoch == 1


@pytest.mark.asyncio
async def test_jump_blocks_entries_until_fresh_check_and_confirmed_reconcile(fake_clock: FakeClock) -> None:
    fake = make_fake()
    guard = _guard(fake)
    assert (await guard.check_drift()).entries_allowed is True
    # Локальний годинник відскочив на 5 с назад, біржа — ні.
    fake_clock.jump_wall(-5_000)
    fake.server_time_offset_ms = 5_000
    jump = guard.tick()
    assert jump is not None
    early = guard.begin_reconcile(DEMO)
    # Вимір до стрибка вже не чинний; звірки після стрибка ще не було.
    assert guard.state(DEMO).block_reasons == (UNVERIFIED, JUMP)
    # Годинник ОС ще не синхронізовано: свіжий вимір показує розбіжність.
    state = await guard.check_drift()
    assert state.block_reasons == (DRIFT, JUMP)
    assert state.offset_ms == 5_000
    # ОС виправила годинник (це теж стрибок — назад до правильного часу).
    fake_clock.jump_wall(5_000)
    fake.server_time_offset_ms = 0
    second = guard.tick()
    assert second is not None
    assert second.epoch == 2
    assert (await guard.check_drift()).block_reasons == (JUMP,)
    # Звірка, почата до другого стрибка, стрибок не знімає.
    guard.confirm_reconciled(early)
    assert guard.state(DEMO).block_reasons == (JUMP,)
    ticket = guard.begin_reconcile(DEMO)
    with capture_logs() as logs:
        guard.confirm_reconciled(ticket)
    assert guard.state(DEMO).block_reasons == ()
    assert guard.state(DEMO).entries_allowed is True
    assert guard.state(DEMO).reconciled_epoch == 2
    [info] = _events(logs, "clock_jump_reconciled")
    assert (info["scope"], info["epoch"]) == (DEMO, 2)
    # Без області — як для області, що ще не звірялась: стрибок блокує.
    assert guard.state().block_reasons == (JUMP,)


@pytest.mark.asyncio
async def test_reconcile_confirmation_does_not_replace_a_clock_check(fake_clock: FakeClock) -> None:
    guard = _guard(make_fake())
    await guard.check_drift()
    fake_clock.jump_wall(2_000)
    assert guard.tick() is not None
    guard.confirm_reconciled(guard.begin_reconcile(DEMO))
    assert guard.state(DEMO).block_reasons == (UNVERIFIED,)


@pytest.mark.asyncio
async def test_reconcile_of_one_account_does_not_clear_another(fake_clock: FakeClock) -> None:
    # Стрибок перевів Demo і Live у RECONCILING; звірка Demo пройшла, Live — ні (обробник упав чи звірка висить).
    guard = _guard(make_fake())
    await guard.check_drift()
    fake_clock.jump_wall(3_600_000)
    assert guard.tick() is not None
    demo, live = guard.begin_reconcile(DEMO), guard.begin_reconcile(LIVE)
    await guard.check_drift()
    guard.confirm_reconciled(demo)
    assert guard.state(DEMO).entries_allowed is True
    assert guard.state(LIVE).block_reasons == (JUMP,)
    guard.confirm_reconciled(live)
    assert guard.state(LIVE).entries_allowed is True


@pytest.mark.asyncio
async def test_account_started_after_a_jump_is_blocked_until_its_own_reconcile(fake_clock: FakeClock) -> None:
    guard = _guard(make_fake())
    fake_clock.jump_wall(5_000)
    assert guard.tick() is not None
    await guard.check_drift()
    assert guard.state("account:new").block_reasons == (JUMP,)
    # Стартова звірка нового бота (ТЗ §11.1) теж бере квиток.
    guard.confirm_reconciled(guard.begin_reconcile("account:new"))
    assert guard.state("account:new").entries_allowed is True


def test_forged_or_foreign_tickets_are_rejected(fake_clock: FakeClock) -> None:
    guard = _guard(make_fake())
    for bad in (ReconcileTicket(scope=DEMO, epoch=1), ReconcileTicket(scope=DEMO, epoch=-1), 0, (DEMO, 0)):
        with pytest.raises(ValueError, match="ticket"):
            guard.confirm_reconciled(bad)  # type: ignore[arg-type]
    for scope in ("", None, 1):
        with pytest.raises(ValueError, match="scope"):
            guard.begin_reconcile(scope)  # type: ignore[arg-type]
    guard.confirm_reconciled(guard.begin_reconcile(DEMO))
    assert guard.state(DEMO).reconciled_epoch == 0


def test_older_ticket_never_moves_the_scope_back(fake_clock: FakeClock) -> None:
    guard = _guard(make_fake())
    fake_clock.jump_wall(2_000)
    guard.tick()
    first = guard.begin_reconcile(DEMO)
    fake_clock.jump_wall(2_000)
    guard.tick()
    second = guard.begin_reconcile(DEMO)
    guard.confirm_reconciled(second)
    guard.confirm_reconciled(first)
    assert guard.state(DEMO).reconciled_epoch == 2


@pytest.mark.asyncio
async def test_jump_inside_a_request_shows_as_drift_at_once_and_voids_the_sample(fake_clock: FakeClock) -> None:
    # Локальний годинник стрибнув на +3 с посеред запиту (біржа — ні), детектор ще не тікав.
    source = ScriptedTime(fake_clock, rtts=(100,), jump_during_call=3_000)
    guard = _guard(source, samples=1)
    state = await guard.check_drift()
    # Оцінка враховує зсув системного годинника відносно монотонного з моменту виміру — ще до `tick()`.
    assert state.offset_ms == -3_000
    assert state.block_reasons == (DRIFT,)
    assert guard.tick() is not None
    assert guard.state().block_reasons == (UNVERIFIED, JUMP)


@pytest.mark.asyncio
async def test_samples_from_before_a_jump_detected_mid_check_are_dropped(fake_clock: FakeClock) -> None:
    class TickingSource(ScriptedTime):
        async def fetch_time(self) -> int:
            if self.calls == 1:
                # Пробудження після сну посеред другого запиту: детектор бачить стрибок.
                self._clock.jump_wall(4_000)
                assert guard.tick() is not None
            return await super().fetch_time()

    source = TickingSource(fake_clock, rtts=(100, 500, 900))
    guard = _guard(source)
    state = await guard.check_drift()
    # Перший вимір (найменший RTT) — зі старої епохи, другий почався до стрибка: обидва відкинуто.
    assert source.calls == 3
    assert state.uncertainty_ms == 450
    assert state.offset_ms == 0
    assert state.block_reasons == (JUMP,)


@pytest.mark.asyncio
async def test_check_with_only_pre_jump_samples_is_inconclusive(fake_clock: FakeClock) -> None:
    class TickingSource(ScriptedTime):
        async def fetch_time(self) -> int:
            result = await super().fetch_time()
            if self.calls == 1:
                self._clock.jump_wall(4_000)
                assert guard.tick() is not None
            return result

    guard = _guard(TickingSource(fake_clock, rtts=(100,)), samples=1)
    with capture_logs() as logs:
        state = await guard.check_drift()
    assert state.block_reasons == (UNVERIFIED, JUMP)
    [warning] = _events(logs, "clock_drift_check_inconclusive")
    assert warning["reason"] == "invalidated_during_check"
    # Повторна перевірка потрібна одразу, а не через паузу.
    assert guard.drift_check_due() is True


# --------------------------------------------------------------------- розклад перевірок


@pytest.mark.asyncio
async def test_drift_check_schedule(fake_clock: FakeClock) -> None:
    fake = make_fake()
    guard = _guard(fake)
    assert guard.drift_check_due() is True
    await guard.check_drift()
    assert guard.drift_check_due() is False
    fake_clock.advance(DRIFT_CHECK_INTERVAL_MS - 1)
    assert guard.drift_check_due() is False
    fake_clock.advance(1)
    assert guard.drift_check_due() is True


@pytest.mark.asyncio
async def test_blocked_clock_is_rechecked_sooner(fake_clock: FakeClock) -> None:
    guard = _guard(make_fake(server_time_offset_ms=5_000))
    await guard.check_drift()
    fake_clock.advance(DRIFT_RECHECK_MS - 1)
    assert guard.drift_check_due() is False
    fake_clock.advance(1)
    assert guard.drift_check_due() is True


@pytest.mark.asyncio
async def test_failed_check_is_retried_after_the_recheck_pause(fake_clock: FakeClock) -> None:
    fake = make_fake()
    fake.fail_next("fetch_time", Fault.timeout())
    guard = _guard(fake)
    await guard.check_drift()
    assert guard.drift_check_due() is False
    fake_clock.advance(DRIFT_RECHECK_MS)
    assert guard.drift_check_due() is True


@pytest.mark.asyncio
async def test_jump_and_explicit_request_make_a_check_due_immediately(fake_clock: FakeClock) -> None:
    guard = _guard(make_fake())
    await guard.check_drift()
    fake_clock.jump_wall(10_000)
    guard.tick()
    assert guard.drift_check_due() is True
    await guard.check_drift()
    assert guard.drift_check_due() is False
    guard.request_recheck(reason="test")
    assert guard.drift_check_due() is True


@pytest.mark.asyncio
async def test_no_second_check_while_one_is_in_flight(fake_clock: FakeClock) -> None:
    release = asyncio.Event()

    class SlowSource:
        async def fetch_time(self) -> int:
            await release.wait()
            return now_ms()

    guard = _guard(SlowSource(), samples=1)
    task = asyncio.create_task(guard.check_drift())
    await asyncio.sleep(0)
    assert guard.drift_check_due() is False
    release.set()
    await task
    assert guard.state().entries_allowed is True


@pytest.mark.asyncio
async def test_recheck_request_voids_the_current_sample_until_a_fresh_one(fake_clock: FakeClock) -> None:
    # Біржа відхилила запит за recvWindow (10002) — прямий доказ, що годинник поза вікном.
    fake = make_fake()
    guard = _guard(fake)
    assert (await guard.check_drift()).entries_allowed is True
    with capture_logs() as logs:
        guard.request_recheck(reason="recv_window_rejected")
    assert guard.state().block_reasons == (UNVERIFIED,)
    [warning] = _events(logs, "clock_recheck_requested")
    assert (warning["log_level"], warning["reason"]) == ("warning", "recv_window_rejected")
    assert guard.drift_check_due() is True
    # Перевірка впала — вимір лишається скасованим, повтор через 30 с, а не через 10 хв.
    fake.fail_next("fetch_time", Fault.unavailable())
    assert (await guard.check_drift()).block_reasons == (UNVERIFIED,)
    assert guard.drift_check_due() is False
    fake_clock.advance(DRIFT_RECHECK_MS)
    assert guard.drift_check_due() is True
    assert (await guard.check_drift()).entries_allowed is True
    # Звірки recheck не вимагає: це не стрибок.
    assert guard.state(DEMO).entries_allowed is True


@pytest.mark.asyncio
async def test_recheck_request_during_a_check_voids_its_samples(fake_clock: FakeClock) -> None:
    class RequestingSource(ScriptedTime):
        async def fetch_time(self) -> int:
            result = await super().fetch_time()
            guard.request_recheck(reason="ws_reconnected")
            return result

    guard = _guard(RequestingSource(fake_clock), samples=1)
    with capture_logs() as logs:
        state = await guard.check_drift()
    assert state.block_reasons == (UNVERIFIED,)
    assert _events(logs, "clock_drift_check_inconclusive")[0]["reason"] == "invalidated_during_check"
    assert guard.drift_check_due() is True


@pytest.mark.asyncio
async def test_older_parallel_check_does_not_overwrite_a_newer_sample(fake_clock: FakeClock) -> None:
    release = asyncio.Event()

    class TwoCalls:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_time(self) -> int:
            self.calls += 1
            if self.calls == 1:
                await release.wait()
                return now_ms() + 700
            return now_ms() + 100

    guard = _guard(TwoCalls(), samples=1)
    older = asyncio.create_task(guard.check_drift())
    await asyncio.sleep(0)
    fake_clock.advance(800)
    assert (await guard.check_drift()).offset_ms == 100
    release.set()
    await older
    # Старіший вимір (середина запиту раніше) не замінив новіший.
    assert guard.state().offset_ms == 100


@pytest.mark.asyncio
async def test_paused_vm_is_invisible_to_ticks_and_caught_by_the_exchange(fake_clock: FakeClock) -> None:
    # Обмеження: пауза VM (WSL2 у сні Windows) зупиняє обидва локальні годинники, а біржа йде далі (+30 хв).
    fake = make_fake()
    guard = _guard(fake)
    await guard.check_drift()
    fake.server_time_offset_ms = 1_800_000
    assert guard.tick() is None
    assert guard.state().entries_allowed is True
    # Ловить лише час біржі: планова перевірка або сигнал торгового клієнта (10002, реконект WS).
    guard.request_recheck(reason="ws_reconnected")
    assert guard.state().entries_allowed is False
    state = await guard.check_drift()
    assert state.block_reasons == (DRIFT,)
    assert state.offset_ms == 1_800_000


@pytest.mark.asyncio
async def test_cancelled_check_releases_the_in_flight_slot(fake_clock: FakeClock) -> None:
    never = asyncio.Event()

    class HangingSource:
        async def fetch_time(self) -> int:
            await never.wait()
            return now_ms()

    guard = _guard(HangingSource())
    task = asyncio.create_task(guard.check_drift())
    await asyncio.sleep(0)
    assert guard.drift_check_due() is False
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # Годинник не підтверджено — наступна спроба за звичайною паузою повтору.
    assert guard.state().block_reasons == (UNVERIFIED,)
    assert guard.drift_check_due() is False
    fake_clock.advance(DRIFT_RECHECK_MS)
    assert guard.drift_check_due() is True


@pytest.mark.asyncio
async def test_check_has_a_deadline(fake_clock: FakeClock) -> None:
    # Черга лімітера (IP-бан) чи завислий запит не тримають перевірку вічно. Дедлайн — реальний час циклу подій.
    never = asyncio.Event()

    class HangingSource:
        async def fetch_time(self) -> int:
            await never.wait()
            return now_ms()

    guard = _guard(HangingSource(), check_timeout_ms=1)
    with capture_logs() as logs:
        state = await guard.check_drift()
    assert state.block_reasons == (UNVERIFIED,)
    [warning] = _events(logs, "clock_drift_check_failed")
    assert warning["error_type"] == "TimeoutError"
    assert "exc_info" not in warning
    assert guard.drift_check_due() is False


@pytest.mark.asyncio
async def test_partial_failure_is_visible_in_the_result_log(fake_clock: FakeClock) -> None:
    source = ScriptedTime(fake_clock, offset_ms=100, rtts=(40,), errors=[None, ExchangeTimeout("lost")])
    with capture_logs() as logs:
        await _guard(source).check_drift()
    [info] = _events(logs, "clock_drift_ok")
    assert info["partial_error_type"] == "ExchangeTimeout"


def test_samples_must_be_a_positive_int() -> None:
    for bad in (0, -1, True, 1.0):
        with pytest.raises(ValueError, match="samples"):
            _guard(make_fake(), samples=bad)  # type: ignore[arg-type]
    for bad in (0, -1, True, 1.0):
        with pytest.raises(ValueError, match="check_timeout_ms"):
            _guard(make_fake(), check_timeout_ms=bad)  # type: ignore[arg-type]


# --------------------------------------------------------------------- цикл `run`


class _Stop(Exception):
    pass


def _sleeper(clock: FakeClock, *, ticks: int, wall_jumps: dict[int, int] | None = None) -> SleepMs:
    """Підмінний `sleep(ms)`: рухає фейковий годинник, на заданих кроках — стрибок, після `ticks` — зупинка.

    Як і справжній `sleep`, спершу віддає керування (задачі перевірки й обробника, створені на цьому тіку, стартують
    у той самий момент), потім «минає» час. `asyncio.sleep(0)` — лише перемикання задач, реального очікування немає.
    """
    count = 0
    jumps = wall_jumps or {}

    async def sleep(ms: int) -> None:
        nonlocal count
        assert ms == JUMP_CHECK_INTERVAL_MS
        count += 1
        if count > ticks:
            raise _Stop
        await asyncio.sleep(0)
        clock.advance(ms)
        if count in jumps:
            clock.jump_wall(jumps[count])

    return sleep


@pytest.mark.asyncio
async def test_run_checks_at_start_and_every_ten_minutes(fake_clock: FakeClock) -> None:
    fake = make_fake()
    guard = _guard(fake)
    # Ще один крок після 10 хв — щоб планова перевірка встигла виконатись до зупинки.
    ticks = DRIFT_CHECK_INTERVAL_MS // JUMP_CHECK_INTERVAL_MS + 1
    with pytest.raises(ExceptionGroup) as raised:
        await guard.run(sleep=_sleeper(fake_clock, ticks=ticks))
    assert raised.group_contains(_Stop)
    # Старт + одна планова перевірка через 10 хв.
    assert len(fake.calls_to("fetch_time")) == 2 * DRIFT_SAMPLES
    assert guard.state().entries_allowed is True


@pytest.mark.asyncio
async def test_run_reports_a_jump_and_rechecks_the_clock(fake_clock: FakeClock) -> None:
    fake = make_fake()
    guard = _guard(fake)
    jumps: list[ClockJump] = []

    async def on_jump(jump: ClockJump) -> None:
        jumps.append(jump)

    # Пробудження після 2 год сну: системний годинник правильний, монотонний стояв.
    with pytest.raises(ExceptionGroup):
        await guard.run(on_jump=on_jump, sleep=_sleeper(fake_clock, ticks=5, wall_jumps={3: 7_200_000}))
    assert [jump.skew_ms for jump in jumps] == [7_200_000]
    # Перевірка на старті і одразу після стрибка (не через 10 хв).
    assert len(fake.calls_to("fetch_time")) == 2 * DRIFT_SAMPLES
    assert guard.state().block_reasons == (JUMP,)


@pytest.mark.asyncio
async def test_failing_jump_handler_is_logged_and_does_not_stop_the_guard(fake_clock: FakeClock) -> None:
    guard = _guard(make_fake())
    calls: list[int] = []

    async def on_jump(jump: ClockJump) -> None:
        calls.append(jump.epoch)
        raise RuntimeError("db down")

    with capture_logs() as logs, pytest.raises(ExceptionGroup) as raised:
        await guard.run(on_jump=on_jump, sleep=_sleeper(fake_clock, ticks=6, wall_jumps={2: 5_000, 4: -5_000}))
    assert not raised.group_contains(RuntimeError)
    assert calls == [1, 2]
    failures = _events(logs, "clock_jump_handler_failed")
    assert [entry["epoch"] for entry in failures] == [1, 2]
    assert all(entry["log_level"] == "error" for entry in failures)
    # Обробник не перевів ботів у RECONCILING — годинник однаково блокує нові входи (fail-closed).
    assert JUMP in guard.state().block_reasons


@pytest.mark.asyncio
async def test_slow_exchange_does_not_stop_jump_detection(fake_clock: FakeClock) -> None:
    never = asyncio.Event()

    class HangingSource:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_time(self) -> int:
            self.calls += 1
            await never.wait()
            raise ExchangeTimeout("never answered")

    source = HangingSource()
    guard = _guard(source)
    jumps: list[ClockJump] = []

    async def on_jump(jump: ClockJump) -> None:
        jumps.append(jump)

    with pytest.raises(ExceptionGroup) as raised:
        await guard.run(on_jump=on_jump, sleep=_sleeper(fake_clock, ticks=40, wall_jumps={20: 9_000}))
    # Зависла перевірка скасована разом із циклом, а не «проковтнута».
    assert raised.group_contains(_Stop)
    assert [jump.epoch for jump in jumps] == [1]
    # Перевірка висіла весь час — другу паралельно не запущено.
    assert source.calls == 1
    assert guard.state().block_reasons == (UNVERIFIED, JUMP)
