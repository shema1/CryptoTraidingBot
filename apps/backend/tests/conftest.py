"""Спільні налаштування pytest (маркери — у pyproject.toml, правила — .claude/rules/testing.md)."""

from __future__ import annotations

import pytest

# Маркери, що описують «тип» тесту. Тест без жодного з них вважається unit-тестом.
_CATEGORY_MARKERS = frozenset({"unit", "integration", "testnet", "demo", "chaos"})


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        if not _CATEGORY_MARKERS.intersection(m.name for m in item.iter_markers()):
            item.add_marker(pytest.mark.unit)
