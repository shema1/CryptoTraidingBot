"""launchd-агенти колектора і воркера (T1.12, ТЗ §12.1.1): статична перевірка згенерованих plist."""

from __future__ import annotations

import plistlib
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from app.core.config import REPO_ROOT

SCRIPT = REPO_ROOT / "scripts" / "macos" / "launch_agents.sh"
TEMPLATE = REPO_ROOT / "infra" / "macos" / "launch-agent.plist.template"
SERVICES = {"collector": "app.data.collector", "worker": "app.jobs.worker"}

pytestmark = pytest.mark.skipif(
    sys.platform != "darwin" or shutil.which("sh") is None, reason="T1.12: launchd-агенти — лише macOS"
)


def _render(name: str) -> dict[str, Any]:
    result = subprocess.run(  # noqa: S603 — фіксований скрипт репозиторію, аргументи без вводу користувача
        ["/bin/sh", str(SCRIPT), "render", name],
        check=True,
        capture_output=True,
    )
    loaded = plistlib.loads(result.stdout)
    assert isinstance(loaded, dict)
    return loaded


@pytest.mark.parametrize(("name", "module"), sorted(SERVICES.items()))
def test_agent_runs_the_module_under_a_sleep_assertion(name: str, module: str) -> None:
    plist = _render(name)
    assert plist["Label"] == f"com.cryptotradingbot.{name}"
    shell, flag, command = plist["ProgramArguments"]
    assert (shell, flag) == ("/bin/sh", "-c")
    # caffeinate стежить за PID оболонки, а exec віддає цей PID Python: SIGTERM launchctl отримує сам процес.
    assert command.startswith("/usr/bin/caffeinate -i -s -w $$ & exec ")
    assert command.endswith(f" -m {module}")
    assert str(REPO_ROOT / "apps" / "backend" / ".venv" / "bin" / "python") in command
    assert plist["WorkingDirectory"] == str(REPO_ROOT / "apps" / "backend")


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_agent_restarts_only_after_a_failure(name: str) -> None:
    plist = _render(name)
    assert plist["RunAtLoad"] is True
    # Код 0 — штатна зупинка (SIGTERM); ненульовий (втрата lock, БД недоступна) — перезапуск (T0.6).
    assert plist["KeepAlive"] == {"SuccessfulExit": False}
    assert plist["ThrottleInterval"] >= 10


@pytest.mark.parametrize("name", sorted(SERVICES))
def test_agent_logs_through_rotating_files_not_launchd_stderr(name: str) -> None:
    plist = _render(name)
    assert plist["EnvironmentVariables"]["LOG_CONSOLE"] == "false"
    assert plist["StandardErrorPath"] == str(REPO_ROOT / "logs" / "launchd" / f"{name}.launchd.log")


def test_template_never_takes_over_a_lock() -> None:
    """Перехоплення lock — лише вручну з TTY (T0.6): у службі прапорця немає."""
    assert "takeover" not in TEMPLATE.read_text(encoding="utf-8").lower()
    assert "takeover" not in SCRIPT.read_text(encoding="utf-8").lower()


def test_trading_process_is_not_an_agent_yet() -> None:
    """Автозапуск торгового процесу — T5.10 (HOST_ID, звірка), не цей скрипт."""
    with pytest.raises(subprocess.CalledProcessError) as exc:
        _render("backend")
    assert exc.value.returncode == 2


def test_script_and_template_exist() -> None:
    assert Path(SCRIPT).is_file()
    assert Path(TEMPLATE).is_file()
