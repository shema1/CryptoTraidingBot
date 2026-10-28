"""Структура бекенду відповідає ТЗ §3, і кожен пакет має README.md (правило документації)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from app.core.config import REPO_ROOT

APP_DIR = Path(__file__).resolve().parents[1] / "app"

# Пакети з ТЗ §3.
EXPECTED_PACKAGES = {"api", "exchange", "data", "ml", "trading", "risk", "protection", "reconcile", "jobs", "core"}


def _packages() -> list[Path]:
    return sorted(p.parent for p in APP_DIR.rglob("__init__.py") if p.parent != APP_DIR)


def test_packages_match_spec() -> None:
    top_level = {p.name for p in APP_DIR.iterdir() if (p / "__init__.py").is_file()}
    assert top_level == EXPECTED_PACKAGES


@pytest.mark.parametrize("package", _packages(), ids=lambda p: str(p.relative_to(APP_DIR)))
def test_every_package_has_readme(package: Path) -> None:
    readme = package / "README.md"
    assert readme.is_file(), f"немає {readme.relative_to(APP_DIR.parent)}"
    text = readme.read_text(encoding="utf-8")
    assert "ТЗ §" in text, f"{readme.name} пакета {package.name} має посилатися на розділи ТЗ"


CHECK_NO_ENV = REPO_ROOT / "scripts" / "check_no_env_files.py"


@pytest.mark.parametrize(
    ("files", "expected_code"),
    [
        ([".env.example", "apps/backend/app/core/config.py"], 0),
        ([".env"], 1),
        (["apps/backend/.env"], 1),
        ([".env.live"], 1),
        (["secrets/bybit.pem"], 1),
        (["keys/api.key"], 1),
    ],
)
def test_check_no_env_files(files: list[str], expected_code: int) -> None:
    result = subprocess.run(  # noqa: S603 — фіксований скрипт репозиторію, без shell
        [sys.executable, str(CHECK_NO_ENV), *files], check=False, capture_output=True, text=True
    )
    assert result.returncode == expected_code, result.stdout + result.stderr
