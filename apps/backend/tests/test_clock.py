"""Єдиний годинник `app.core.clock` (ТЗ §4.3: UTC, int мілісекунди; підмінний у тестах)."""

from __future__ import annotations

import time

import pytest

from app.core import clock
from app.core.clock import FakeClock, SystemClock, use_clock

# Значення autouse-фікстури `fake_clock` з conftest.py (2026-01-01T00:00:00Z).
FAKE_NOW_MS = 1_767_225_600_000
FAKE_MONOTONIC_MS = 1_000_000


def test_system_clock_returns_int_utc_milliseconds() -> None:
    real = SystemClock()
    before = time.time_ns() // 1_000_000
    value = real.now_ms()
    after = time.time_ns() // 1_000_000
    assert type(value) is int
    assert before <= value <= after


def test_system_monotonic_clock_is_int_and_non_decreasing() -> None:
    real = SystemClock()
    first = real.monotonic_ms()
    second = real.monotonic_ms()
    assert type(first) is int
    assert second >= first


def test_tests_use_fake_clock_by_default(fake_clock: FakeClock) -> None:
    assert clock.get_clock() is fake_clock
    assert clock.now_ms() == FAKE_NOW_MS
    assert clock.monotonic_ms() == FAKE_MONOTONIC_MS


def test_fake_clock_advance_moves_both_clocks(fake_clock: FakeClock) -> None:
    fake_clock.advance(1_500)
    assert clock.now_ms() == FAKE_NOW_MS + 1_500
    assert clock.monotonic_ms() == FAKE_MONOTONIC_MS + 1_500


def test_fake_clock_advance_zero_is_allowed(fake_clock: FakeClock) -> None:
    fake_clock.advance(0)
    assert clock.now_ms() == FAKE_NOW_MS


def test_fake_clock_rejects_negative_advance(fake_clock: FakeClock) -> None:
    with pytest.raises(ValueError, match="advance"):
        fake_clock.advance(-1)
    assert clock.now_ms() == FAKE_NOW_MS
    assert clock.monotonic_ms() == FAKE_MONOTONIC_MS


@pytest.mark.parametrize("delta", [5_000, -5_000])
def test_fake_clock_jump_wall_moves_only_system_clock(fake_clock: FakeClock, delta: int) -> None:
    fake_clock.jump_wall(delta)
    assert clock.now_ms() == FAKE_NOW_MS + delta
    assert clock.monotonic_ms() == FAKE_MONOTONIC_MS


def test_fake_clock_set_now_changes_only_system_clock(fake_clock: FakeClock) -> None:
    fake_clock.set_now(42)
    assert clock.now_ms() == 42
    assert clock.monotonic_ms() == FAKE_MONOTONIC_MS


@pytest.mark.parametrize("bad", [1.5, True, "1000"])
def test_fake_clock_accepts_only_int_milliseconds(bad: object) -> None:
    with pytest.raises(TypeError):
        FakeClock(now_ms=bad)  # type: ignore[arg-type]
    fake = FakeClock(now_ms=0)
    with pytest.raises(TypeError):
        fake.advance(bad)  # type: ignore[arg-type]


def test_fake_clock_rejects_negative_start() -> None:
    with pytest.raises(ValueError, match="now_ms"):
        FakeClock(now_ms=-1)
    with pytest.raises(ValueError, match="monotonic_ms"):
        FakeClock(now_ms=0, monotonic_ms=-1)


def test_use_clock_substitutes_and_restores(fake_clock: FakeClock) -> None:
    other = FakeClock(now_ms=7, monotonic_ms=8)
    with use_clock(other) as active:
        assert active is other
        assert clock.now_ms() == 7
        assert clock.monotonic_ms() == 8
    assert clock.get_clock() is fake_clock


def test_use_clock_restores_on_exception(fake_clock: FakeClock) -> None:
    other = FakeClock(now_ms=7)
    with pytest.raises(RuntimeError), use_clock(other):
        raise RuntimeError("boom")
    assert clock.get_clock() is fake_clock


def test_use_clock_is_seen_from_other_threads(fake_clock: FakeClock) -> None:
    # Холдер — модульна змінна, а не ContextVar: потік executor-а бачить той самий годинник.
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        assert pool.submit(clock.now_ms).result() == FAKE_NOW_MS


def test_fake_clock_rejects_negative_wall_time(fake_clock: FakeClock) -> None:
    with pytest.raises(ValueError, match="now_ms"):
        fake_clock.set_now(-5)
    with pytest.raises(ValueError, match="now_ms"):
        fake_clock.jump_wall(-FAKE_NOW_MS - 1)
    assert clock.now_ms() == FAKE_NOW_MS
