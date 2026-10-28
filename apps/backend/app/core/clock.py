"""Єдиний годинник процесу (ТЗ §4.3): UTC, `int` мілісекунди.

Уся торгова логіка бере час лише звідси (`now_ms()`, `monotonic_ms()`), а не з `time.time()` /
`datetime.now()`, щоб у тестах годинник можна було підмінити на `FakeClock` (`use_clock`).

Поточний годинник зберігається в модульній змінній — свідомий виняток із правила «без глобального стану»:
це не торговий стан, а точка підміни часу. `ContextVar` не підходить: `loop.run_in_executor` не копіює
контекст, і код у потоці мовчки взяв би реальний час замість фейкового.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int:
        """Системний (wall) час: UTC epoch у мілісекундах."""
        ...

    def monotonic_ms(self) -> int:
        """Монотонний годинник у мілісекундах: не стрибає при зміні системного часу; лише для інтервалів."""
        ...


class SystemClock:
    """Реальний годинник ОС."""

    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def monotonic_ms(self) -> int:
        return time.monotonic_ns() // 1_000_000


def _require_ms(name: str, value: object) -> int:
    # bool — підклас int, float — ймовірно секунди: обидва відхиляються (ТЗ §4.3).
    if type(value) is not int:
        raise TypeError(f"{name} має бути int мілісекунд, отримано {type(value).__name__}")
    return value


class FakeClock:
    """Підмінний годинник для тестів: час рухається лише явними викликами."""

    def __init__(self, now_ms: int, monotonic_ms: int = 0) -> None:
        if _require_ms("now_ms", now_ms) < 0:
            raise ValueError("now_ms не може бути від'ємним")
        if _require_ms("monotonic_ms", monotonic_ms) < 0:
            raise ValueError("monotonic_ms не може бути від'ємним")
        self._now_ms = now_ms
        self._monotonic_ms = monotonic_ms

    def now_ms(self) -> int:
        return self._now_ms

    def monotonic_ms(self) -> int:
        return self._monotonic_ms

    def advance(self, ms: int) -> None:
        """Плин часу: обидва годинники рухаються вперед на `ms`."""
        if _require_ms("ms", ms) < 0:
            raise ValueError("advance: час не може йти назад")
        self._now_ms += ms
        self._monotonic_ms += ms

    def jump_wall(self, delta_ms: int) -> None:
        """Стрибок лише системного годинника (сон, ручна зміна часу, NTP); монотонний не змінюється."""
        self.set_now(self._now_ms + _require_ms("delta_ms", delta_ms))

    def set_now(self, now_ms: int) -> None:
        """Встановити системний час; монотонний не змінюється."""
        if _require_ms("now_ms", now_ms) < 0:
            raise ValueError("now_ms не може бути від'ємним")
        self._now_ms = now_ms


_current: Clock = SystemClock()


def get_clock() -> Clock:
    return _current


@contextmanager
def use_clock(clock: Clock) -> Iterator[Clock]:
    """Тимчасово підмінити годинник процесу; попередній відновлюється й при винятку."""
    global _current
    previous = _current
    _current = clock
    try:
        yield clock
    finally:
        _current = previous


def now_ms() -> int:
    """Поточний час UTC у мілісекундах."""
    return _current.now_ms()


def monotonic_ms() -> int:
    """Монотонний час у мілісекундах (для інтервалів і таймаутів)."""
    return _current.monotonic_ms()
