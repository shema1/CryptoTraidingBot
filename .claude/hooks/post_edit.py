#!/usr/bin/env python3
"""Після редагування файлу агентом:
- *.py  -> ruff format + ruff check (проблеми повертаються агенту, щоб він їх виправив);
- .claude/rules/*.md -> перегенерувати правила Cursor (scripts/sync_ai_rules.py).

Claude Code PostToolUse: exit 2 + stderr — показати проблеми агенту.
Cursor afterFileEdit: викликається з аргументом `cursor`, завжди exit 0.
Якщо ruff/uv ще не встановлені (етап 0) — тихо пропускає.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.environ.get("CLAUDE_PROJECT_DIR") or os.environ.get("CURSOR_PROJECT_DIR") or Path(__file__).resolve().parents[2])


def ruff_cmd(path: Path) -> list[str] | None:
    backend = ROOT / "apps" / "backend"
    if shutil.which("uv") and (backend / "pyproject.toml").exists() and backend in path.parents:
        return ["uv", "run", "--directory", str(backend), "ruff"]
    if shutil.which("ruff"):
        return ["ruff"]
    return None


def main() -> int:
    cursor = len(sys.argv) > 1 and sys.argv[1] == "cursor"
    try:
        data = json.load(sys.stdin)
    except Exception:
        return 0
    raw = (data.get("tool_input") or {}).get("file_path") or data.get("file_path") or ""
    if not raw:
        return 0
    path = Path(raw).resolve()
    problems = ""

    if path.suffix == ".py" and path.exists():
        ruff = ruff_cmd(path)
        if ruff:
            subprocess.run([*ruff, "format", str(path)], check=False, capture_output=True, timeout=60)
            r = subprocess.run([*ruff, "check", "--output-format", "concise", str(path)],
                               check=False, capture_output=True, text=True, timeout=60)
            if r.returncode != 0:
                problems = f"ruff знайшов проблеми у {path.name}:\n{r.stdout.strip()}"

    try:
        rules_dir = (ROOT / ".claude" / "rules").resolve()
        if path.suffix == ".md" and rules_dir in path.parents:
            subprocess.run([sys.executable, str(ROOT / "scripts" / "sync_ai_rules.py")],
                           check=False, capture_output=True, timeout=60)
    except Exception:
        pass

    if problems and not cursor:
        print(problems, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
