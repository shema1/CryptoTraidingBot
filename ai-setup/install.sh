#!/usr/bin/env bash
# Розгортає налаштування Claude Code (.claude/ і .mcp.json) у корінь проєкту.
# Запуск з кореня проєкту:  bash ai-setup/install.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/ai-setup"
cd "$ROOT"

if [ -e .claude ] || [ -e .mcp.json ]; then
  ts="$(date +%Y%m%d-%H%M%S)"
  mkdir -p "ai-setup-backup-$ts"
  [ -e .claude ] && cp -R .claude "ai-setup-backup-$ts/"
  [ -e .mcp.json ] && cp .mcp.json "ai-setup-backup-$ts/"
  echo "Наявні .claude/ та .mcp.json збережено в ai-setup-backup-$ts/"
fi

mkdir -p .claude
cp -R "$SRC/dot-claude/." .claude/
cp "$SRC/mcp.json" .mcp.json
chmod +x .claude/hooks/*.py scripts/sync_ai_rules.py 2>/dev/null || true

# Перевірки
python3 -m json.tool .claude/settings.json >/dev/null
python3 -m json.tool .mcp.json >/dev/null
if echo '{"tool_input":{"command":"cat .env"}}' | CLAUDE_PROJECT_DIR="$ROOT" python3 .claude/hooks/guard.py bash 2>/dev/null; then
  echo "ПОМИЛКА: запобіжник не спрацював" >&2; exit 1
fi
if python3 -c "import yaml" 2>/dev/null; then
  python3 scripts/sync_ai_rules.py --check
else
  echo "(PyYAML не встановлено — перевірку синхронізації правил Cursor пропущено; встановіть: python3 -m pip install pyyaml)"
fi

rm -rf "$SRC"
echo "Готово: .claude/ і .mcp.json встановлено, запобіжники працюють. Тека ai-setup/ видалена."
