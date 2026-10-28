#!/usr/bin/env python3
"""Stop-hook Claude Code: перед завершенням відповіді перевіряє, чи разом із кодом змінено документацію.

Якщо в робочому дереві git змінено код (apps/**, scripts/**, docker-compose.yml, migrations), але не змінено
жодного документа — повертає агенту нагадування (exit 2), і агент мусить або оновити документацію,
або явно пояснити, чому вона не зачеплена. Повторно в тому ж циклі не спрацьовує (stop_hook_active),
тому зациклення немає. Без git — нічого не робить.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.environ.get("CLAUDE_PROJECT_DIR") or Path(__file__).resolve().parents[2])

CODE_PREFIXES = ("apps/", "scripts/", "docker-compose.yml")
DOC_SUFFIXES = (".md", ".mdc")
DOC_EXTRA = (".env.example", "apps/web/src/help/")


def changed_files() -> list[str]:
    try:
        r = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all"], cwd=ROOT,
                           check=False, capture_output=True, text=True, timeout=10)
    except Exception:  # noqa: BLE001 — без git перевірка просто не виконується
        return []
    if r.returncode != 0:
        return []
    files = []
    for line in r.stdout.splitlines():
        path = line[3:].strip()
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        files.append(path.strip('"'))
    return files


def is_doc(path: str) -> bool:
    return path.endswith(DOC_SUFFIXES) or path.startswith(DOC_EXTRA)


def is_code(path: str) -> bool:
    return path.startswith(CODE_PREFIXES) and not is_doc(path) and "/tests/" not in path


def main() -> int:
    try:
        data = json.load(sys.stdin)
    except Exception:  # noqa: BLE001
        data = {}
    if data.get("stop_hook_active"):
        return 0
    files = changed_files()
    code = [f for f in files if is_code(f)]
    docs = [f for f in files if is_doc(f)]
    if code and not docs:
        shown = "\n".join(f"  - {f}" for f in code[:15])
        more = f"\n  … і ще {len(code) - 15}" if len(code) > 15 else ""
        print(
            "Правило синхронізації документації (.claude/rules/documentation.md): змінено код, "
            "але не змінено жодного документа.\n"
            f"Змінений код:\n{shown}{more}\n"
            "Перевір AppInfo.md (ТЗ), README модулів, docs/**, .env.example, довідку UI, docs/ai/TASKS.md. "
            "Онови те, що зачеплено (зміна AppInfo.md — лише з підтвердженням користувача), "
            "або в підсумку явно напиши в розділі «Документація», чому оновлення не потрібне.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
