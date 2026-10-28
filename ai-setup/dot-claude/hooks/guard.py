#!/usr/bin/env python3
"""Запобіжники для AI-агентів (Claude Code і Cursor).

Використання (stdin — JSON від інструмента):
  guard.py bash          Claude Code PreToolUse, matcher Bash
  guard.py edit          Claude Code PreToolUse, matcher Edit|Write|MultiEdit|NotebookEdit
  guard.py read          Claude Code PreToolUse, matcher Read
  guard.py cursor-shell  Cursor beforeShellExecution
  guard.py cursor-read   Cursor beforeReadFile

Claude Code: deny -> exit 2 + повідомлення в stderr; ask -> JSON permissionDecision=ask.
Cursor: JSON {"permission": "allow" | "deny" | "ask", ...} у stdout.
Без зовнішніх залежностей; лише стандартна бібліотека.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(os.environ.get("CLAUDE_PROJECT_DIR") or os.environ.get("CURSOR_PROJECT_DIR") or Path(__file__).resolve().parents[2])

# ---------- правила для shell-команд ----------
# (regex, рішення, пояснення)
SHELL_RULES: list[tuple[str, str, str]] = [
    (r"(?:^|[\s/'\"=:<>])\.env(?!\.example\b)(?:\.[\w-]+)?\b", "deny",
     "Доступ до .env заборонено: там секрети. Використовуйте .env.example або попросіть користувача."),
    (r"--force-takeover", "deny",
     "--force-takeover виконується лише людиною вручну після перевірки, що старий процес не працює (ТЗ §6.4)."),
    (r"\b(?:api|api2|stream)\.bybit\.com\b|\bapi\.bytick\.com\b", "deny",
     "Звернення до MAINNET Bybit заборонене для агента. Використовуйте testnet (api-testnet / stream-testnet.bybit.com) або demo (api-demo / stream-demo.bybit.com)."),
    (r"\b(?:TRADING_MODE|BOT_MODE|MODE)=['\"]?live\b|\bBYBIT_TESTNET=['\"]?(?:false|0)\b|--mode[= ]live\b", "deny",
     "Запуск у режимі LIVE агентом заборонений. Live запускає лише людина через UI з підтвердженням."),
    (r"docker\s+(?:compose\s+down\b.*\s-v\b|compose\s+down\b.*--volumes|volume\s+(?:rm|prune))|docker-compose\s+down\b.*\s-v\b", "deny",
     "Видалення Docker volume знищить дані БД (свічки, ордери, журнал)."),
    (r"\brm\s+-[a-zA-Z]*r[a-zA-Z]*f?\s+(?:/|~|\$HOME|\.|\*|models\b|apps/backend/migrations\b)(?:\s|/?$)", "deny",
     "Рекурсивне видалення кореня, домашньої теки, models/ або migrations/ заборонене."),
    (r"git\s+push\b.*(?:--force\b|\s-f\b|--force-with-lease\b)", "deny",
     "Force-push заборонений."),
    (r"\b(?:DROP\s+(?:TABLE|DATABASE|SCHEMA)|TRUNCATE)\b", "ask",
     "Руйнівна SQL-команда. Підтвердіть, що це локальна dev/test БД."),
    (r"alembic\s+downgrade\b", "ask",
     "alembic downgrade змінює схему БД. Підтвердіть, що це dev/test БД."),
    (r"git\s+(?:reset\s+--hard|clean\s+-[a-zA-Z]*f|checkout\s+--\s|restore\s+\.)", "ask",
     "Команда безповоротно відкидає локальні зміни."),
    (r"\brm\s+-[a-zA-Z]*r", "ask", "Рекурсивне видалення файлів."),
    (r"(?:curl|wget)\b[^|]*\|\s*(?:sudo\s+)?(?:ba|z)?sh\b", "ask", "Виконання скрипта з інтернету."),
    (r"\b(?:pip|uv\s+pip)\s+install\b(?!.*-r\b)", "ask",
     "Встановлення пакета поза lock-файлом. Краще `uv add <pkg>` з поясненням, навіщо залежність."),
]

SECRET_FILE_RE = re.compile(r"(?:^|/)\.env(?:\.(?!example$)[\w-]+)?$|\.(?:pem|key|p12|pfx)$|(?:^|/)secrets?/")


def rel(path: str) -> str:
    try:
        return str(Path(path).resolve().relative_to(ROOT.resolve()))
    except Exception:
        return path


def check_shell(cmd: str) -> tuple[str, str]:
    for pattern, decision, reason in SHELL_RULES:
        if re.search(pattern, cmd, flags=re.IGNORECASE):
            return decision, reason
    return "allow", ""


def is_git_tracked(path: Path) -> bool:
    try:
        r = subprocess.run(["git", "ls-files", "--error-unmatch", str(path)], cwd=ROOT,
                           check=False, capture_output=True, timeout=5)
        return r.returncode == 0
    except Exception:
        return False


def check_edit(file_path: str) -> tuple[str, str]:
    p = rel(file_path)
    if SECRET_FILE_RE.search(p):
        return "deny", f"Редагування файлу з секретами ({p}) заборонене."
    if p.startswith("models/") and not p.endswith((".gitkeep", "README.md")):
        return "deny", "Артефакти моделей у models/ генеруються лише пайплайном навчання і не редагуються вручну (ТЗ §5.4)."
    if p.startswith(".cursor/rules/"):
        return "deny", "Файли .cursor/rules/*.mdc генеруються. Редагуйте .claude/rules/*.md і запустіть python3 scripts/sync_ai_rules.py."
    if re.match(r"apps/backend/migrations/versions/.+\.py$", p):
        full = ROOT / p
        if full.exists() and is_git_tracked(full):
            return "deny", ("Ця міграція вже закомічена (вважається застосованою). Не редагуйте її — "
                            "створіть нову: uv run alembic revision -m '...'.")
    if p == "AppInfo.md":
        return "ask", "Зміна ТЗ (AppInfo.md). Підтвердіть, що це узгоджено."
    return "allow", ""


def check_read(file_path: str) -> tuple[str, str]:
    p = rel(file_path)
    if SECRET_FILE_RE.search(p):
        return "deny", f"Читання файлу з секретами ({p}) заборонене. Використовуйте .env.example."
    return "allow", ""


def claude_respond(decision: str, reason: str) -> int:
    if decision == "deny":
        print(f"ЗАБЛОКОВАНО запобіжником проєкту: {reason}", file=sys.stderr)
        return 2
    if decision == "ask":
        print(json.dumps({"hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "ask",
            "permissionDecisionReason": reason,
        }}, ensure_ascii=False))
    return 0


def cursor_respond(decision: str, reason: str) -> int:
    out: dict[str, str] = {"permission": decision}
    if reason:
        out["userMessage"] = reason
        out["agentMessage"] = f"Заблоковано/потребує підтвердження запобіжником проєкту: {reason}"
    print(json.dumps(out, ensure_ascii=False))
    return 0


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    try:
        data = json.load(sys.stdin)
    except Exception:
        data = {}
    tool_input = data.get("tool_input") or {}

    if mode == "bash":
        return claude_respond(*check_shell(str(tool_input.get("command", ""))))
    if mode == "edit":
        path = tool_input.get("file_path") or tool_input.get("notebook_path") or ""
        return claude_respond(*check_edit(str(path)))
    if mode == "read":
        return claude_respond(*check_read(str(tool_input.get("file_path", ""))))
    if mode == "cursor-shell":
        return cursor_respond(*check_shell(str(data.get("command", ""))))
    if mode == "cursor-read":
        return cursor_respond(*check_read(str(data.get("file_path", ""))))
    print(f"guard.py: невідомий режим {mode!r}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
