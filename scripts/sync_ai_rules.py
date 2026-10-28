#!/usr/bin/env python3
"""Генерує правила Cursor (.cursor/rules/*.mdc) з єдиного джерела .claude/rules/*.md.

Джерело істини для правил — .claude/rules/. Файли в .cursor/rules/ не редагуються вручну.
Запуск:  python3 scripts/sync_ai_rules.py          # згенерувати
         python3 scripts/sync_ai_rules.py --check  # перевірити, що Cursor-правила актуальні (для pre-commit/CI)
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml  # PyYAML

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / ".claude" / "rules"
DST = ROOT / ".cursor" / "rules"
HEADER = "<!-- ЗГЕНЕРОВАНО scripts/sync_ai_rules.py з .claude/rules/{name}. Не редагувати вручну. -->\n\n"

ALWAYS_ON = """---
description: Базові інструкції проєкту (завжди активні)
alwaysApply: true
---
<!-- ЗГЕНЕРОВАНО scripts/sync_ai_rules.py. Не редагувати вручну. -->

# Проєкт: Crypto Trading Bot (реальні гроші)

- Головні інструкції та інваріанти: @AGENTS.md. Повне ТЗ: @AppInfo.md. План і задачі: @docs/ai/TASKS.md.
- Перед початком роботи знайди свою задачу в docs/ai/TASKS.md і перевір рекомендовану модель. Якщо поточна модель слабша за рекомендовану для R3/R2 задачі — попередь користувача.
- Для задач R3 (торгове ядро, ризик, захист, звірка, Emergency Stop): спочатку план (Plan mode), потім тести, потім код; після — рев'ю skill `safety-check`.
- Ніколи не читай і не змінюй .env, не запускай нічого проти mainnet, не редагуй застосовані міграції та артефакти в models/.
"""


def split_frontmatter(text: str) -> tuple[dict, str]:
    if not text.startswith("---"):
        return {}, text
    _, fm, body = text.split("---", 2)
    return (yaml.safe_load(fm) or {}), body.lstrip("\n")


def render(src: Path) -> str:
    meta, body = split_frontmatter(src.read_text(encoding="utf-8"))
    paths = meta.get("paths") or []
    lines = ["---", f"description: {meta.get('description', src.stem)}"]
    if paths:
        lines.append(f"globs: {','.join(paths)}")
        lines.append("alwaysApply: false")
    else:
        lines.append("alwaysApply: true")
    lines.append("---")
    return "\n".join(lines) + "\n" + HEADER.format(name=src.name) + body


def build() -> dict[Path, str]:
    out = {DST / "00-project.mdc": ALWAYS_ON}
    for src in sorted(SRC.glob("*.md")):
        out[DST / f"{src.stem}.mdc"] = render(src)
    return out


def main() -> int:
    files = build()
    if "--check" in sys.argv:
        stale = [p for p, c in files.items() if not p.exists() or p.read_text(encoding="utf-8") != c]
        extra = [p for p in DST.glob("*.mdc") if p not in files]
        for p in stale + extra:
            print(f"неактуально: {p.relative_to(ROOT)}")
        if stale or extra:
            print("Запустіть: python3 scripts/sync_ai_rules.py")
            return 1
        return 0
    DST.mkdir(parents=True, exist_ok=True)
    for p in DST.glob("*.mdc"):
        if p not in files:
            p.unlink()
    for p, c in files.items():
        p.write_text(c, encoding="utf-8")
        print(f"записано {p.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
