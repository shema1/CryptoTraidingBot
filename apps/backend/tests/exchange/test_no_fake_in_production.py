"""`FakeExchangeAdapter` — лише для тестів (ТЗ §3): жоден модуль `app/**`, крім самого `app/exchange/fake.py`,
не імпортує `app.exchange.fake`. Інакше фейкова біржа могла б опинитися в торговому процесі замість Bybit.

Сканер розбирає AST (а не шукає підрядок), тож ловить `import`, `from ... import`, відносні імпорти
(`from .fake import …`, `from . import fake`) і динамічні `importlib.import_module("app.exchange.fake")`.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
APP_DIR = BACKEND_DIR / "app"
FAKE_MODULE = "app.exchange.fake"
FAKE_FILE = APP_DIR / "exchange" / "fake.py"


def _module_name(path: Path) -> str:
    parts = list(path.relative_to(BACKEND_DIR).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _is_fake(module: str) -> bool:
    return module == FAKE_MODULE or module.startswith(FAKE_MODULE + ".")


def _resolve_relative(current_module: str, is_package: bool, level: int, module: str | None) -> str:
    package_parts = current_module.split(".") if is_package else current_module.split(".")[:-1]
    base = package_parts[: len(package_parts) - (level - 1)] if level > 1 else package_parts
    return ".".join([*base, module] if module else base)


def find_fake_imports(source: str, module_name: str, *, is_package: bool = False) -> list[str]:
    """Повертає список імпортованих модулів, що є `app.exchange.fake` (порожній — чисто)."""
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            found.extend(alias.name for alias in node.names if _is_fake(alias.name))
        elif isinstance(node, ast.ImportFrom):
            base = (
                _resolve_relative(module_name, is_package, node.level, node.module)
                if node.level
                else (node.module or "")
            )
            if _is_fake(base):
                found.append(base)
            else:
                found.extend(f"{base}.{a.name}" for a in node.names if _is_fake(f"{base}.{a.name}"))
        elif isinstance(node, ast.Call):
            # importlib.import_module("app.exchange.fake") / __import__("app.exchange.fake")
            found.extend(
                arg.value
                for arg in node.args
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and _is_fake(arg.value)
            )
    return found


# ---------------------------------------------------------------- самоперевірка сканера


@pytest.mark.parametrize(
    ("source", "module_name", "is_package"),
    [
        ("import app.exchange.fake", "app.trading.runner", False),
        ("import app.exchange.fake as f", "app.trading.runner", False),
        ("from app.exchange.fake import FakeExchangeAdapter", "app.trading.runner", False),
        ("from app.exchange import fake", "app.trading.runner", False),
        ("from .fake import FakeExchangeAdapter", "app.exchange.bybit", False),
        ("from . import fake", "app.exchange.bybit", False),
        ("from . import fake", "app.exchange", True),
        ("from ..exchange.fake import Fault", "app.trading.runner", False),
        ("import importlib\nimportlib.import_module('app.exchange.fake')", "app.main", False),
        ("def f() -> None:\n    from app.exchange.fake import Fault\n", "app.risk.x", False),
    ],
)
def test_scanner_detects_fake_imports(source: str, module_name: str, is_package: bool) -> None:
    assert find_fake_imports(source, module_name, is_package=is_package) == [FAKE_MODULE]


@pytest.mark.parametrize(
    "source",
    [
        "from app.exchange import types",
        "from app.exchange.adapter import ExchangeAdapter",
        "import app.exchange.fakes_helper",
        "from .types import Order",
        "x = 'app.exchange.fake is test-only'",
    ],
)
def test_scanner_ignores_other_imports(source: str) -> None:
    assert find_fake_imports(source, "app.exchange.bybit") == []


# ---------------------------------------------------------------- сам скан app/**


def test_fake_module_exists() -> None:
    assert FAKE_FILE.is_file() is True


def test_no_production_module_imports_fake() -> None:
    offenders: dict[str, list[str]] = {}
    scanned = 0
    for path in sorted(APP_DIR.rglob("*.py")):
        if path == FAKE_FILE:
            continue
        scanned += 1
        hits = find_fake_imports(
            path.read_text(encoding="utf-8"), _module_name(path), is_package=path.name == "__init__.py"
        )
        if hits:
            offenders[str(path.relative_to(BACKEND_DIR))] = hits
    assert offenders == {}
    assert scanned > 10  # сканер справді обійшов дерево app/
