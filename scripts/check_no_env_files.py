#!/usr/bin/env python3
"""Pre-commit: забороняє коміт файлів із секретами (.env*, *.pem, *.key). Дозволено лише .env.example.

Запуск: python3 scripts/check_no_env_files.py <файли…>  (pre-commit передає staged-файли).
"""

from __future__ import annotations

import sys
from pathlib import PurePosixPath

ALLOWED = {".env.example"}
FORBIDDEN_SUFFIXES = {".pem", ".key"}


def is_forbidden(path: str) -> bool:
    p = PurePosixPath(path.replace("\\", "/"))
    if p.name in ALLOWED:
        return False
    return p.name == ".env" or p.name.startswith(".env.") or p.suffix in FORBIDDEN_SUFFIXES


def main(argv: list[str]) -> int:
    bad = [f for f in argv if is_forbidden(f)]
    for f in bad:
        sys.stderr.write(f"заборонено комітити файл із секретами: {f}\n")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
