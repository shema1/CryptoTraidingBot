#!/bin/sh
# launchd-агенти колектора і воркера на macOS (T1.12, ТЗ §12.1.1). Без sudo: агенти користувача в ~/Library/LaunchAgents.
#
#   scripts/macos/launch_agents.sh install    # створити/оновити агенти і запустити
#   scripts/macos/launch_agents.sh uninstall  # зупинити (SIGTERM, код 0) і прибрати
#   scripts/macos/launch_agents.sh status     # стан агентів
#   scripts/macos/launch_agents.sh render collector|worker  # лише вивести plist (перевірка, тест)
#
# Агент тримає заборону сну (`caffeinate -i -s`) і перезапускає процес після ненульового коду виходу.
# Торговий процес (`python -m app`) сюди не входить: його автозапуск — T5.10, разом із HOST_ID і звіркою.
set -eu

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
# Шлях іде в sed і в рядок `/bin/sh -c` plist: символи, які там щось означають, — відмова, а не тиха підстановка.
case "$REPO" in
    *[\&\|\$\`\"\'\\]* | *"
"*)
        echo "Шлях репозиторію містить символи, небезпечні для plist ('&', '|', '\$', лапки, '\\', новий рядок): $REPO" >&2
        exit 2
        ;;
esac
BACKEND="$REPO/apps/backend"
PYTHON="$BACKEND/.venv/bin/python"
LOGS="$REPO/logs/launchd"
AGENTS="$HOME/Library/LaunchAgents"
TEMPLATE="$REPO/infra/macos/launch-agent.plist.template"
DOMAIN="gui/$(id -u)"

# ім'я | модуль Python
SERVICES="collector:app.data.collector worker:app.jobs.worker"

label() { echo "com.cryptotradingbot.$1"; }

render() {
    name="$1"
    module="$2"
    sed -e "s|__LABEL__|$(label "$name")|g" \
        -e "s|__PYTHON__|$PYTHON|g" \
        -e "s|__MODULE__|$module|g" \
        -e "s|__BACKEND__|$BACKEND|g" \
        -e "s|__LOGS__|$LOGS|g" \
        -e "s|__NAME__|$name|g" \
        "$TEMPLATE"
}

install_agents() {
    if [ ! -x "$PYTHON" ]; then
        echo "Немає $PYTHON — спершу: cd apps/backend && uv sync" >&2
        exit 1
    fi
    mkdir -p "$AGENTS" "$LOGS"
    chmod 700 "$LOGS"
    for item in $SERVICES; do
        name="${item%%:*}"
        module="${item#*:}"
        target="$AGENTS/$(label "$name").plist"
        render "$name" "$module" > "$target.tmp"
        plutil -lint "$target.tmp" >/dev/null
        mv "$target.tmp" "$target"
        # Оновлення: спершу зупинити наявний агент (SIGTERM → штатна зупинка, код 0).
        launchctl bootout "$DOMAIN/$(label "$name")" 2>/dev/null || true
        launchctl bootstrap "$DOMAIN" "$target"
        echo "запущено: $(label "$name") ($module)"
    done
}

uninstall_agents() {
    for item in $SERVICES; do
        name="${item%%:*}"
        target="$AGENTS/$(label "$name").plist"
        launchctl bootout "$DOMAIN/$(label "$name")" 2>/dev/null || true
        rm -f "$target"
        echo "прибрано: $(label "$name")"
    done
}

status_agents() {
    for item in $SERVICES; do
        name="${item%%:*}"
        if launchctl print "$DOMAIN/$(label "$name")" >/dev/null 2>&1; then
            state="$(launchctl print "$DOMAIN/$(label "$name")" | awk -F' = ' '/^\tstate/ {print $2; exit}')"
            code="$(launchctl print "$DOMAIN/$(label "$name")" | awk -F' = ' '/last exit code/ {print $2; exit}')"
            echo "$(label "$name"): state=${state:-?} last_exit=${code:-—}"
        else
            echo "$(label "$name"): не встановлено"
        fi
    done
}

render_one() {
    for item in $SERVICES; do
        if [ "${item%%:*}" = "${1:-}" ]; then
            render "${item%%:*}" "${item#*:}"
            return 0
        fi
    done
    echo "Невідома служба: ${1:-} (collector|worker)" >&2
    exit 2
}

case "${1:-}" in
    install) install_agents ;;
    render) render_one "${2:-}" ;;
    uninstall) uninstall_agents ;;
    status) status_agents ;;
    *)
        echo "Використання: $0 install|uninstall|status|render <служба>" >&2
        exit 2
        ;;
esac
