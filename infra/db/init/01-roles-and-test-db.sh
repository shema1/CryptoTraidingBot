#!/bin/sh
# Виконується образом postgres лише при першій ініціалізації порожнього volume.
# Створює: read-only роль ctb_readonly (для MCP `postgres-readonly`) і окрему БД для integration-тестів.
set -eu

: "${POSTGRES_READONLY_PASSWORD:?POSTGRES_READONLY_PASSWORD не задано}"
TEST_DB="${POSTGRES_DB}_test"

# Пароль передається psql-змінною (:'ro_password' екранується psql), а не вставляється в SQL-рядок shell-ом.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -v ro_password="$POSTGRES_READONLY_PASSWORD" -v main_db="$POSTGRES_DB" -v test_db="$TEST_DB" \
    -v main_user="$POSTGRES_USER" <<'SQL'
CREATE ROLE ctb_readonly LOGIN PASSWORD :'ro_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
-- Навіть якщо роль колись отримає зайві права, її сесії за замовчуванням лише для читання.
ALTER ROLE ctb_readonly SET default_transaction_read_only = on;
ALTER ROLE ctb_readonly SET statement_timeout = '30s';

CREATE DATABASE :"test_db" OWNER :"main_user";
-- Тестова БД недоступна read-only ролі; основна — доступна.
REVOKE CONNECT ON DATABASE :"test_db" FROM PUBLIC;
REVOKE CONNECT ON DATABASE :"main_db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"main_db" TO ctb_readonly;
SQL

# Основна БД: читання всіх поточних і майбутніх таблиць, створених основною роллю (міграціями).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v main_user="$POSTGRES_USER" <<'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb;
GRANT USAGE ON SCHEMA public TO ctb_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ctb_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE :"main_user" IN SCHEMA public GRANT SELECT ON TABLES TO ctb_readonly;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$TEST_DB" <<'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb;
SQL
