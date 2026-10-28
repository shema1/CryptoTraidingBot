"""Спільні налаштування pytest (маркери — у pyproject.toml, правила — .claude/rules/testing.md)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from app.core.clock import FakeClock, use_clock

# Маркери, що описують «тип» тесту. Тест без жодного з них вважається unit-тестом.
_CATEGORY_MARKERS = frozenset({"unit", "integration", "testnet", "demo", "chaos"})

# 2026-01-01T00:00:00Z у мс — стартовий час фейкового годинника.
FAKE_NOW_MS = 1_767_225_600_000
FAKE_MONOTONIC_MS = 1_000_000


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        if not _CATEGORY_MARKERS.intersection(m.name for m in item.iter_markers()):
            item.add_marker(pytest.mark.unit)


@pytest.fixture(autouse=True)
def fake_clock() -> Iterator[FakeClock]:
    """Кожен тест працює з фейковим годинником (правило: час у тестах — лише через підмінний `clock`).

    Тест, якому потрібен реальний час, створює `SystemClock()` явно.
    """
    clock = FakeClock(now_ms=FAKE_NOW_MS, monotonic_ms=FAKE_MONOTONIC_MS)
    with use_clock(clock):
        yield clock
