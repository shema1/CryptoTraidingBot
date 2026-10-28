#!/usr/bin/env bash
# Перевірка умов готовності T0.2 для запущеного `docker compose up -d`:
# healthy, порт лише на loopback, read-only роль не може писати, тестова БД існує.
# Використання (з кореня): PGRO_PASSWORD=<пароль ctb_readonly> bash scripts/check_db.sh
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $*" >&2; exit 1; }
dc() { docker compose "$@"; }
# psql від імені власника всередині контейнера (змінні POSTGRES_* беруться з оточення контейнера).
owner() { dc exec -T timescaledb sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAq' <<<"$1"; }

: "${PGRO_PASSWORD:?задайте PGRO_PASSWORD (пароль ctb_readonly) в оточенні}"

status=$(dc ps --format '{{.Health}}' timescaledb)
[[ "$status" == "healthy" ]] || fail "timescaledb не healthy (стан: '$status')"
echo "ok: healthy"

bind=$(dc port timescaledb 5432)
[[ "$bind" == 127.0.0.1:* ]] || fail "порт опублікований не на 127.0.0.1: $bind"
echo "ok: порт $bind"

db=$(dc exec -T timescaledb sh -c 'printf %s "$POSTGRES_DB"')
ro() {
    dc exec -T -e PGPASSWORD="$PGRO_PASSWORD" timescaledb \
        psql -h 127.0.0.1 -U ctb_readonly -d "${2:-$db}" -v ON_ERROR_STOP=1 -tAq -c "$1"
}

[[ "$(ro 'SELECT 1')" == "1" ]] || fail "ctb_readonly не може читати"
echo "ok: ctb_readonly читає"

owner 'DROP TABLE IF EXISTS _ro_probe; CREATE TABLE _ro_probe(x int); INSERT INTO _ro_probe VALUES (1);' >/dev/null
trap 'owner "DROP TABLE IF EXISTS _ro_probe" >/dev/null' EXIT

[[ "$(ro 'SELECT count(*) FROM _ro_probe')" == "1" ]] || fail "ctb_readonly не бачить нових таблиць (default privileges)"
if ro 'INSERT INTO _ro_probe VALUES (2)' >/dev/null 2>&1; then fail "ctb_readonly зміг INSERT"; fi
if ro 'CREATE TABLE _ro_denied(x int)' >/dev/null 2>&1; then fail "ctb_readonly зміг CREATE TABLE"; fi
echo "ok: ctb_readonly не може INSERT/CREATE, але бачить нові таблиці"

if ro 'SELECT 1' "${db}_test" >/dev/null 2>&1; then fail "ctb_readonly має доступ до тестової БД"; fi
has_test=$(dc exec -T timescaledb sh -c 'psql -U "$POSTGRES_USER" -d postgres -tAq -c "SELECT 1 FROM pg_database WHERE datname = current_setting('"'"'cluster_name'"'"', true) OR datname = '"'"'${POSTGRES_DB}_test'"'"'"' || true)
[[ "$has_test" == *1* ]] || fail "тестової БД ${db}_test немає"
echo "ok: тестова БД ${db}_test існує, ctb_readonly до неї не підключається"
