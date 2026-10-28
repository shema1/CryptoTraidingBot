"""Початкова схема БД (T0.4, ТЗ §4.1, §7.2, §15; рішення docs/decisions/0005-schema-t0-4.md).

Таблиці, CHECK, FK, часткові унікальні індекси — з моделей `app/*/models.py` (autogenerate).
Вручну (autogenerate цього не бачить):
- функція `ctb_unix_now_ms()` — «зараз» у мс для політик TimescaleDB на цілочисловому часі;
- hypertables `candles` і таблиць деривативів; compression `candles` після 7 днів; retention немає (ТЗ §4.1);
- тригери append-only (заборона UPDATE/DELETE/TRUNCATE) для `order_events`, `audit_log`, `ledger`,
  `experiment_trials`, `risk_config`;
- `ctb_orders_guard()`: ідентичність ордера незмінна, з термінального стану переходів немає, DELETE/TRUNCATE
  заборонені (`filled_qty`, `exchange_order_id` з NULL, `position_id`, `bot_id` з NULL лишаються змінними —
  пізні fills і звірка; `qty` ордера біржі `EXCHANGE` може лише зростати — часткові fills стоп-ордера);
- `ctb_bots_guard()`: акаунт, режим і символ бота незмінні; `STOPPED` неможливий, поки в бота є відкрита
  позиція або нетермінальні ордери (інакше другий бот на символі злився б з позицією, ТЗ §6.2);
- `ctb_bot_children_guard()`: вставка ордера чи позиції бота бере `FOR SHARE` на рядок бота — зупинка, що
  виконується паралельно, чекає на коміт вставки й бачить новий рядок (без цього гонка «перевірка → дія»);
  вхідний ордер для `STOPPED` бота відхиляється, виходи (`reduce_only`) і позиції — ні (захист понад усе);
- `ctb_positions_guard()`: відкриту позицію видалити не можна;
- `ctb_require_confirmed_audit(action, scope)`: `confirmed_audit_id` у `risk_config`, `emergency_runs`, `models`
  посилається лише на `audit_log` з `confirmed = true`, потрібною дією і тією самою ціллю (акаунт або
  `model_version`); повторне використання запису забороняють часткові `UNIQUE (confirmed_audit_id)`.

Revision ID: 0001
Revises:
Create Date: 2026-10-28 17:26:59.459132+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DAY_MS = 86_400_000
# Розмір chunk: 7 днів для свічок (compression застосовується до повністю старших за 7 днів chunk-ів),
# 30 днів для рідших рядів деривативів.
CANDLES_CHUNK_MS = 7 * DAY_MS
SERIES_CHUNK_MS = 30 * DAY_MS
CANDLES_COMPRESS_AFTER_MS = 7 * DAY_MS

SERIES_HYPERTABLES = ("open_interest", "premium_index", "mark_index_candles", "long_short_ratio", "liquidations_hourly")
APPEND_ONLY_TABLES = ("order_events", "audit_log", "ledger", "experiment_trials", "risk_config")
# Знімок на момент міграції (джерело в коді — app.trading.models.ORDER_TERMINAL_STATES).
ORDER_TERMINAL_STATES_SQL = "('FILLED', 'CANCELED', 'REJECTED', 'EXPIRED')"
# Таблиця → (дія `audit_log`, ціль підтвердження). Джерело дій у коді — app.api.models.AUDIT_ACTION_*.
CONFIRMED_AUDIT_ACTIONS = {
    "risk_config": ("risk.config.loosen", "account"),
    "emergency_runs": ("emergency.release", "account"),
    "models": ("model.approve", "model"),
}
FUNCTIONS = (
    "ctb_require_confirmed_audit()",
    "ctb_positions_guard()",
    "ctb_bot_children_guard()",
    "ctb_bots_guard()",
    "ctb_orders_guard()",
    "ctb_forbid_mutation()",
    "ctb_unix_now_ms()",
)


def upgrade() -> None:
    # Розширення ставить init-скрипт (infra/db/init); тут — на випадок БД, створеної інакше.
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    op.execute(
        """
        CREATE FUNCTION ctb_unix_now_ms() RETURNS bigint
        LANGUAGE sql STABLE AS $$ SELECT (extract(epoch FROM now()) * 1000)::bigint $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION ctb_forbid_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'append-only table %: % is forbidden', TG_TABLE_NAME, TG_OP
                USING ERRCODE = 'restrict_violation';
        END
        $$
        """
    )

    op.execute(
        f"""
        CREATE FUNCTION ctb_orders_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
                RAISE EXCEPTION 'orders: % is forbidden', TG_OP USING ERRCODE = 'restrict_violation';
            END IF;
            IF (NEW.account_id, NEW.mode, NEW.origin, NEW.symbol, NEW.side, NEW.order_type, NEW.reduce_only)
                    IS DISTINCT FROM
                    (OLD.account_id, OLD.mode, OLD.origin, OLD.symbol, OLD.side, OLD.order_type, OLD.reduce_only)
                OR NEW.client_order_id IS DISTINCT FROM OLD.client_order_id
                OR NEW.signal_id IS DISTINCT FROM OLD.signal_id
                OR (OLD.bot_id IS NOT NULL AND NEW.bot_id IS DISTINCT FROM OLD.bot_id)
                OR (OLD.exchange_order_id IS NOT NULL AND NEW.exchange_order_id IS DISTINCT FROM OLD.exchange_order_id)
                -- Кількість ордера біржі відома лише з fills: вона може зростати; решта ордерів — незмінна.
                OR (NEW.qty IS DISTINCT FROM OLD.qty AND NOT (OLD.origin = 'EXCHANGE' AND NEW.qty > OLD.qty))
            THEN
                RAISE EXCEPTION 'orders: order identity is immutable (id %)', OLD.id
                    USING ERRCODE = 'restrict_violation';
            END IF;
            IF OLD.state IN {ORDER_TERMINAL_STATES_SQL} AND NEW.state IS DISTINCT FROM OLD.state THEN
                RAISE EXCEPTION 'orders: no transitions from terminal state % (id %)', OLD.state, OLD.id
                    USING ERRCODE = 'restrict_violation';
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION ctb_bots_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF (NEW.account_id, NEW.mode, NEW.symbol) IS DISTINCT FROM (OLD.account_id, OLD.mode, OLD.symbol) THEN
                RAISE EXCEPTION 'bots: account_id, mode and symbol are immutable (id %)', OLD.id
                    USING ERRCODE = 'restrict_violation';
            END IF;
            -- Тригер BEFORE UPDATE виконується вже з блокуванням рядка бота: вставки ордерів/позицій, що тримають
            -- FOR SHARE (ctb_bot_children_guard), закомічені до цього моменту, і запити нижче їх бачать.
            IF NEW.status = 'STOPPED' AND OLD.status IS DISTINCT FROM 'STOPPED' THEN
                IF EXISTS (SELECT 1 FROM positions p WHERE p.bot_id = NEW.id AND p.status = 'OPEN') THEN
                    RAISE EXCEPTION 'bots: cannot stop bot % with an open position', NEW.id
                        USING ERRCODE = 'restrict_violation';
                END IF;
                IF EXISTS (
                    SELECT 1 FROM orders o
                    WHERE o.bot_id = NEW.id AND o.state NOT IN ('FILLED', 'CANCELED', 'REJECTED', 'EXPIRED')
                ) THEN
                    RAISE EXCEPTION 'bots: cannot stop bot % with non-terminal orders', NEW.id
                        USING ERRCODE = 'restrict_violation';
                END IF;
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION ctb_bot_children_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            bot_status text;
        BEGIN
            IF NEW.bot_id IS NULL THEN
                RETURN NEW;
            END IF;
            -- FOR SHARE конфліктує з блокуванням UPDATE бота: зупинка чекає на коміт цієї вставки (і навпаки).
            SELECT b.status INTO bot_status FROM bots b WHERE b.id = NEW.bot_id FOR SHARE;
            -- Вкладені IF: у positions немає поля reduce_only, вираз з ним не можна обчислювати для позицій.
            IF TG_TABLE_NAME = 'orders' AND bot_status = 'STOPPED' THEN
                IF NOT NEW.reduce_only THEN
                    RAISE EXCEPTION 'orders: bot % is STOPPED, entry orders are forbidden', NEW.bot_id
                        USING ERRCODE = 'restrict_violation';
                END IF;
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION ctb_positions_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.status = 'OPEN' THEN
                RAISE EXCEPTION 'positions: cannot delete open position %', OLD.id
                    USING ERRCODE = 'restrict_violation';
            END IF;
            RETURN OLD;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION ctb_require_confirmed_audit() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            ok boolean;
        BEGIN
            IF NEW.confirmed_audit_id IS NULL THEN
                RETURN NEW;
            END IF;
            -- TG_ARGV[0] — потрібна дія; TG_ARGV[1] — ціль: 'account' (той самий акаунт) або 'model' (target = model_version).
            IF TG_ARGV[1] = 'account' THEN
                SELECT EXISTS (
                    SELECT 1 FROM audit_log a
                    WHERE a.id = NEW.confirmed_audit_id AND a.action = TG_ARGV[0] AND a.confirmed
                        AND a.account_id = NEW.account_id
                ) INTO ok;
            ELSIF TG_ARGV[1] = 'model' THEN
                SELECT EXISTS (
                    SELECT 1 FROM audit_log a
                    WHERE a.id = NEW.confirmed_audit_id AND a.action = TG_ARGV[0] AND a.confirmed
                        AND a.target = NEW.model_version
                ) INTO ok;
            ELSE
                RAISE EXCEPTION 'ctb_require_confirmed_audit: unknown scope %', TG_ARGV[1];
            END IF;
            IF NOT ok THEN
                RAISE EXCEPTION '%: confirmed_audit_id % must be a confirmed audit_log entry with action % for the same %',
                    TG_TABLE_NAME, NEW.confirmed_audit_id, TG_ARGV[0], TG_ARGV[1]
                    USING ERRCODE = 'check_violation';
            END IF;
            RETURN NEW;
        END
        $$
        """
    )

    _create_tables()

    # PK кожної hypertable містить колонку часу, тож унікальність діє в межах усієї таблиці.
    op.execute(
        f"SELECT create_hypertable('candles', by_range('open_time_ms', {CANDLES_CHUNK_MS}::bigint),"
        " create_default_indexes => false)"
    )
    op.execute("SELECT set_integer_now_func('candles', 'ctb_unix_now_ms')")
    op.execute(
        "ALTER TABLE candles SET (timescaledb.compress,"
        " timescaledb.compress_segmentby = 'exchange, symbol, timeframe',"
        " timescaledb.compress_orderby = 'open_time_ms DESC')"
    )
    op.execute(f"SELECT add_compression_policy('candles', compress_after => {CANDLES_COMPRESS_AFTER_MS}::bigint)")
    for table in SERIES_HYPERTABLES:
        op.execute(
            f"SELECT create_hypertable('{table}', by_range('ts_ms', {SERIES_CHUNK_MS}::bigint),"
            " create_default_indexes => false)"
        )

    for table in APPEND_ONLY_TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table}_append_only BEFORE UPDATE OR DELETE ON {table}"
            " FOR EACH ROW EXECUTE FUNCTION ctb_forbid_mutation()"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table}_no_truncate BEFORE TRUNCATE ON {table}"
            " FOR EACH STATEMENT EXECUTE FUNCTION ctb_forbid_mutation()"
        )

    op.execute(
        "CREATE TRIGGER trg_orders_guard BEFORE UPDATE OR DELETE ON orders FOR EACH ROW EXECUTE FUNCTION ctb_orders_guard()"
    )
    op.execute(
        "CREATE TRIGGER trg_orders_no_truncate BEFORE TRUNCATE ON orders"
        " FOR EACH STATEMENT EXECUTE FUNCTION ctb_orders_guard()"
    )
    op.execute("CREATE TRIGGER trg_bots_guard BEFORE UPDATE ON bots FOR EACH ROW EXECUTE FUNCTION ctb_bots_guard()")
    op.execute(
        "CREATE TRIGGER trg_orders_bot_lock BEFORE INSERT OR UPDATE OF bot_id ON orders"
        " FOR EACH ROW EXECUTE FUNCTION ctb_bot_children_guard()"
    )
    op.execute(
        "CREATE TRIGGER trg_positions_bot_lock BEFORE INSERT ON positions"
        " FOR EACH ROW EXECUTE FUNCTION ctb_bot_children_guard()"
    )
    op.execute(
        "CREATE TRIGGER trg_positions_guard BEFORE DELETE ON positions FOR EACH ROW EXECUTE FUNCTION ctb_positions_guard()"
    )
    for table, (action, scope) in CONFIRMED_AUDIT_ACTIONS.items():
        op.execute(
            f"CREATE TRIGGER trg_{table}_confirmed_audit BEFORE INSERT OR UPDATE OF confirmed_audit_id ON {table}"
            f" FOR EACH ROW EXECUTE FUNCTION ctb_require_confirmed_audit('{action}', '{scope}')"
        )


def downgrade() -> None:
    # DROP TABLE знімає тригери, hypertable-метадані й політику compression разом із таблицею.
    _drop_tables()
    for function in FUNCTIONS:
        op.execute(f"DROP FUNCTION {function}")
    # Розширення timescaledb не видаляється: його створює init-скрипт, і воно спільне для БД.


def _create_tables() -> None:
    # Згенеровано `alembic revision --autogenerate` з моделей і перевірено вручну.
    op.create_table(
        "audit_log",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=True),
        sa.Column("bot_id", sa.Uuid(), nullable=True),
        sa.Column("target", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("confirmed", sa.Boolean(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=True),
        sa.Column("request", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("action ~ '^[a-z][a-z0-9_.]*$'", name=op.f("ck_audit_log_action_format")),
        sa.CheckConstraint("char_length(actor) > 0", name=op.f("ck_audit_log_actor_not_empty")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_audit_log")),
    )
    op.create_index("ix_audit_log_ts_ms", "audit_log", ["ts_ms"], unique=False)
    op.create_table(
        "candles",
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("timeframe", sa.Text(), nullable=False),
        sa.Column("open_time_ms", sa.BigInteger(), nullable=False),
        sa.Column("open", sa.Numeric(), nullable=False),
        sa.Column("high", sa.Numeric(), nullable=False),
        sa.Column("low", sa.Numeric(), nullable=False),
        sa.Column("close", sa.Numeric(), nullable=False),
        sa.Column("volume", sa.Numeric(), nullable=False),
        sa.Column("is_closed", sa.Boolean(), nullable=False),
        sa.CheckConstraint("exchange IN ('bybit')", name=op.f("ck_candles_exchange")),
        sa.CheckConstraint("symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_candles_symbol_format")),
        sa.CheckConstraint("timeframe ~ '^[0-9]+(m|min|h|d|w)$'", name=op.f("ck_candles_timeframe_format")),
        sa.CheckConstraint("high >= low", name=op.f("ck_candles_high_ge_low")),
        sa.CheckConstraint("high >= open AND high >= close", name=op.f("ck_candles_high_ge_open_close")),
        sa.CheckConstraint("is_closed", name=op.f("ck_candles_only_closed")),
        sa.CheckConstraint("low <= open AND low <= close", name=op.f("ck_candles_low_le_open_close")),
        sa.CheckConstraint("low > 0", name=op.f("ck_candles_low_positive")),
        sa.CheckConstraint("open_time_ms >= 0", name=op.f("ck_candles_open_time_ms_non_negative")),
        sa.CheckConstraint("volume >= 0", name=op.f("ck_candles_volume_non_negative")),
        sa.PrimaryKeyConstraint("exchange", "symbol", "timeframe", "open_time_ms", name=op.f("pk_candles")),
    )
    op.create_table(
        "data_gaps",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("timeframe", sa.Text(), nullable=False),
        sa.Column("from_ms", sa.BigInteger(), nullable=False),
        sa.Column("to_ms", sa.BigInteger(), nullable=False),
        sa.Column("detected_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("resolved_at_ms", sa.BigInteger(), nullable=True),
        sa.CheckConstraint("exchange IN ('bybit')", name=op.f("ck_data_gaps_exchange")),
        sa.CheckConstraint(
            "source IN ('candles', 'open_interest', 'premium_index', 'mark_candles', 'index_candles', 'long_short', 'liquidations')",
            name=op.f("ck_data_gaps_source"),
        ),
        sa.CheckConstraint("symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_data_gaps_symbol_format")),
        sa.CheckConstraint("timeframe ~ '^[0-9]+(m|min|h|d|w)$'", name=op.f("ck_data_gaps_timeframe_format")),
        sa.CheckConstraint("from_ms >= 0 AND to_ms > from_ms", name=op.f("ck_data_gaps_range_valid")),
        sa.CheckConstraint(
            "resolved_at_ms IS NULL OR resolved_at_ms >= detected_at_ms",
            name=op.f("ck_data_gaps_resolved_after_detected"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_data_gaps")),
    )
    op.create_index(
        "uq_data_gaps_open",
        "data_gaps",
        ["source", "exchange", "symbol", "timeframe", "from_ms"],
        unique=True,
        postgresql_where=sa.text("resolved_at_ms IS NULL"),
    )
    op.create_table(
        "exchange_accounts",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("env_key_set", sa.Text(), nullable=False),
        sa.Column("allowed_host_id", sa.Text(), nullable=False),
        sa.Column("key_check_result", sa.Text(), nullable=True),
        sa.Column("key_check_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("key_check_details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("updated_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "allowed_host_id ~ '^[a-z0-9]{2}$'", name=op.f("ck_exchange_accounts_allowed_host_id_format")
        ),
        sa.CheckConstraint("env_key_set ~ '^[A-Z][A-Z0-9_]*$'", name=op.f("ck_exchange_accounts_env_key_set_format")),
        sa.CheckConstraint("exchange IN ('bybit')", name=op.f("ck_exchange_accounts_exchange")),
        sa.CheckConstraint(
            "key_check_result IN ('OK', 'WITHDRAW_ENABLED', 'KEY_MISSING', 'UNVERIFIABLE')",
            name=op.f("ck_exchange_accounts_key_check_result"),
        ),
        sa.CheckConstraint("mode IN ('demo', 'testnet', 'live')", name=op.f("ck_exchange_accounts_mode")),
        sa.CheckConstraint(
            "(key_check_result IS NULL) = (key_check_at_ms IS NULL)",
            name=op.f("ck_exchange_accounts_key_check_result_with_time"),
        ),
        sa.CheckConstraint("char_length(name) > 0", name=op.f("ck_exchange_accounts_name_not_empty")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_exchange_accounts")),
        sa.UniqueConstraint("env_key_set", name=op.f("uq_exchange_accounts_env_key_set")),
        sa.UniqueConstraint("id", "mode", name=op.f("uq_exchange_accounts_id_mode")),
        sa.UniqueConstraint("name", name=op.f("uq_exchange_accounts_name")),
    )
    op.create_table(
        "experiments",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("research_line", sa.Text(), nullable=False),
        sa.Column("symbols", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("timeframe", sa.Text(), nullable=False),
        sa.Column("label_type", sa.Text(), nullable=False),
        sa.Column("primary_strategy", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("params", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("seed", sa.BigInteger(), nullable=False),
        sa.Column("data_from_ms", sa.BigInteger(), nullable=False),
        sa.Column("data_to_ms", sa.BigInteger(), nullable=False),
        sa.Column("n_trials_line", sa.BigInteger(), nullable=True),
        sa.Column("dsr", sa.Numeric(), nullable=True),
        sa.Column("pbo", sa.Numeric(), nullable=True),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "array_to_string(symbols, ',') ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+(,[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+)*$'",
            name=op.f("ck_experiments_symbols_format"),
        ),
        sa.CheckConstraint("label_type IN ('meta', 'direct', 'rule_only')", name=op.f("ck_experiments_label_type")),
        sa.CheckConstraint(
            "research_line ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_experiments_research_line_format")
        ),
        sa.CheckConstraint("cardinality(symbols) > 0", name=op.f("ck_experiments_symbols_not_empty")),
        sa.CheckConstraint("char_length(id) > 0", name=op.f("ck_experiments_id_not_empty")),
        sa.CheckConstraint("data_to_ms > data_from_ms", name=op.f("ck_experiments_data_range_valid")),
        sa.CheckConstraint("dsr IS NULL OR (dsr >= 0 AND dsr <= 1)", name=op.f("ck_experiments_dsr_range")),
        sa.CheckConstraint(
            "n_trials_line IS NULL OR n_trials_line > 0", name=op.f("ck_experiments_n_trials_line_positive")
        ),
        sa.CheckConstraint("pbo IS NULL OR (pbo >= 0 AND pbo <= 1)", name=op.f("ck_experiments_pbo_range")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_experiments")),
    )
    op.create_table(
        "funding_rates",
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("funding_time_ms", sa.BigInteger(), nullable=False),
        sa.Column("rate", sa.Numeric(), nullable=False),
        sa.CheckConstraint("exchange IN ('bybit')", name=op.f("ck_funding_rates_exchange")),
        sa.CheckConstraint("symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_funding_rates_symbol_format")),
        sa.CheckConstraint("funding_time_ms >= 0", name=op.f("ck_funding_rates_funding_time_ms_non_negative")),
        sa.PrimaryKeyConstraint("exchange", "symbol", "funding_time_ms", name=op.f("pk_funding_rates")),
    )
    op.create_table(
        "instruments",
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("tick_size", sa.Numeric(), nullable=False),
        sa.Column("qty_step", sa.Numeric(), nullable=False),
        sa.Column("min_qty", sa.Numeric(), nullable=False),
        sa.Column("min_notional", sa.Numeric(), nullable=False),
        sa.Column("max_qty", sa.Numeric(), nullable=True),
        sa.Column("max_leverage", sa.Numeric(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("updated_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("exchange IN ('bybit')", name=op.f("ck_instruments_exchange")),
        sa.CheckConstraint("mode IN ('demo', 'testnet', 'live')", name=op.f("ck_instruments_mode")),
        sa.CheckConstraint("symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_instruments_symbol_format")),
        sa.CheckConstraint("char_length(status) > 0", name=op.f("ck_instruments_status_not_empty")),
        sa.CheckConstraint("max_leverage >= 1", name=op.f("ck_instruments_max_leverage_ge_1")),
        sa.CheckConstraint("max_qty IS NULL OR max_qty >= min_qty", name=op.f("ck_instruments_max_qty_ge_min_qty")),
        sa.CheckConstraint("min_notional >= 0", name=op.f("ck_instruments_min_notional_non_negative")),
        sa.CheckConstraint("min_qty > 0", name=op.f("ck_instruments_min_qty_positive")),
        sa.CheckConstraint("qty_step > 0", name=op.f("ck_instruments_qty_step_positive")),
        sa.CheckConstraint("tick_size > 0", name=op.f("ck_instruments_tick_size_positive")),
        sa.PrimaryKeyConstraint("exchange", "mode", "symbol", name=op.f("pk_instruments")),
    )
    op.create_table(
        "jobs",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("priority", sa.BigInteger(), nullable=False),
        sa.Column("params", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("dedup_key", sa.Text(), nullable=True),
        sa.Column("progress", sa.Numeric(), nullable=False),
        sa.Column("progress_message", sa.Text(), nullable=True),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("attempt", sa.BigInteger(), nullable=False),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("started_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("heartbeat_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("finished_at_ms", sa.BigInteger(), nullable=True),
        sa.CheckConstraint(
            "(status IN ('DONE', 'FAILED', 'CANCELED')) = (finished_at_ms IS NOT NULL)",
            name=op.f("ck_jobs_finished_iff_terminal"),
        ),
        sa.CheckConstraint("kind ~ '^[a-z][a-z0-9_]*$'", name=op.f("ck_jobs_kind_format")),
        sa.CheckConstraint(
            "status <> 'RUNNING' OR (started_at_ms IS NOT NULL AND heartbeat_at_ms IS NOT NULL)",
            name=op.f("ck_jobs_running_has_heartbeat"),
        ),
        sa.CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'DONE', 'FAILED', 'CANCELED')", name=op.f("ck_jobs_status")
        ),
        sa.CheckConstraint("attempt >= 1", name=op.f("ck_jobs_attempt_positive")),
        sa.CheckConstraint("progress >= 0 AND progress <= 1", name=op.f("ck_jobs_progress_range")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_jobs")),
    )
    op.create_index(
        "ix_jobs_queue",
        "jobs",
        ["priority", "created_at_ms"],
        unique=False,
        postgresql_where=sa.text("status = 'QUEUED'"),
    )
    op.create_index(
        "uq_jobs_dedup_key_active",
        "jobs",
        ["dedup_key"],
        unique=True,
        postgresql_where=sa.text("status IN ('QUEUED', 'RUNNING')"),
    )
    op.create_table(
        "liquidations_hourly",
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("interval", sa.Text(), nullable=False),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.Column("long_count", sa.BigInteger(), nullable=False),
        sa.Column("long_qty", sa.Numeric(), nullable=False),
        sa.Column("long_notional", sa.Numeric(), nullable=False),
        sa.Column("short_count", sa.BigInteger(), nullable=False),
        sa.Column("short_qty", sa.Numeric(), nullable=False),
        sa.Column("short_notional", sa.Numeric(), nullable=False),
        sa.CheckConstraint("exchange IN ('bybit')", name=op.f("ck_liquidations_hourly_exchange")),
        sa.CheckConstraint("interval ~ '^[0-9]+(m|min|h|d|w)$'", name=op.f("ck_liquidations_hourly_interval_format")),
        sa.CheckConstraint(
            "symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_liquidations_hourly_symbol_format")
        ),
        sa.CheckConstraint(
            "long_count >= 0 AND short_count >= 0 AND long_qty >= 0 AND short_qty >= 0 AND long_notional >= 0 AND short_notional >= 0",
            name=op.f("ck_liquidations_hourly_non_negative"),
        ),
        sa.CheckConstraint("ts_ms >= 0", name=op.f("ck_liquidations_hourly_ts_ms_non_negative")),
        sa.PrimaryKeyConstraint("exchange", "symbol", "interval", "ts_ms", name=op.f("pk_liquidations_hourly")),
    )
    op.create_table(
        "long_short_ratio",
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("interval", sa.Text(), nullable=False),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.Column("buy_ratio", sa.Numeric(), nullable=False),
        sa.Column("sell_ratio", sa.Numeric(), nullable=False),
        sa.CheckConstraint("exchange IN ('bybit')", name=op.f("ck_long_short_ratio_exchange")),
        sa.CheckConstraint("interval ~ '^[0-9]+(m|min|h|d|w)$'", name=op.f("ck_long_short_ratio_interval_format")),
        sa.CheckConstraint(
            "symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_long_short_ratio_symbol_format")
        ),
        sa.CheckConstraint("buy_ratio >= 0 AND buy_ratio <= 1", name=op.f("ck_long_short_ratio_buy_ratio_range")),
        sa.CheckConstraint("sell_ratio >= 0 AND sell_ratio <= 1", name=op.f("ck_long_short_ratio_sell_ratio_range")),
        sa.CheckConstraint("ts_ms >= 0", name=op.f("ck_long_short_ratio_ts_ms_non_negative")),
        sa.PrimaryKeyConstraint("exchange", "symbol", "interval", "ts_ms", name=op.f("pk_long_short_ratio")),
    )
    op.create_table(
        "mark_index_candles",
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("price_type", sa.Text(), nullable=False),
        sa.Column("interval", sa.Text(), nullable=False),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.Column("open", sa.Numeric(), nullable=False),
        sa.Column("high", sa.Numeric(), nullable=False),
        sa.Column("low", sa.Numeric(), nullable=False),
        sa.Column("close", sa.Numeric(), nullable=False),
        sa.CheckConstraint("exchange IN ('bybit')", name=op.f("ck_mark_index_candles_exchange")),
        sa.CheckConstraint("interval ~ '^[0-9]+(m|min|h|d|w)$'", name=op.f("ck_mark_index_candles_interval_format")),
        sa.CheckConstraint("price_type IN ('mark', 'index')", name=op.f("ck_mark_index_candles_price_type")),
        sa.CheckConstraint(
            "symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_mark_index_candles_symbol_format")
        ),
        sa.CheckConstraint("high >= low", name=op.f("ck_mark_index_candles_high_ge_low")),
        sa.CheckConstraint("high >= open AND high >= close", name=op.f("ck_mark_index_candles_high_ge_open_close")),
        sa.CheckConstraint("low <= open AND low <= close", name=op.f("ck_mark_index_candles_low_le_open_close")),
        sa.CheckConstraint("low > 0", name=op.f("ck_mark_index_candles_low_positive")),
        sa.CheckConstraint("ts_ms >= 0", name=op.f("ck_mark_index_candles_ts_ms_non_negative")),
        sa.PrimaryKeyConstraint(
            "exchange", "symbol", "price_type", "interval", "ts_ms", name=op.f("pk_mark_index_candles")
        ),
    )
    op.create_table(
        "open_interest",
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("interval", sa.Text(), nullable=False),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.Column("open_interest", sa.Numeric(), nullable=False),
        sa.CheckConstraint("exchange IN ('bybit')", name=op.f("ck_open_interest_exchange")),
        sa.CheckConstraint("interval ~ '^[0-9]+(m|min|h|d|w)$'", name=op.f("ck_open_interest_interval_format")),
        sa.CheckConstraint("symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_open_interest_symbol_format")),
        sa.CheckConstraint("open_interest >= 0", name=op.f("ck_open_interest_open_interest_non_negative")),
        sa.CheckConstraint("ts_ms >= 0", name=op.f("ck_open_interest_ts_ms_non_negative")),
        sa.PrimaryKeyConstraint("exchange", "symbol", "interval", "ts_ms", name=op.f("pk_open_interest")),
    )
    op.create_table(
        "premium_index",
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("interval", sa.Text(), nullable=False),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.Column("open", sa.Numeric(), nullable=False),
        sa.Column("high", sa.Numeric(), nullable=False),
        sa.Column("low", sa.Numeric(), nullable=False),
        sa.Column("close", sa.Numeric(), nullable=False),
        sa.CheckConstraint("exchange IN ('bybit')", name=op.f("ck_premium_index_exchange")),
        sa.CheckConstraint("interval ~ '^[0-9]+(m|min|h|d|w)$'", name=op.f("ck_premium_index_interval_format")),
        sa.CheckConstraint("symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_premium_index_symbol_format")),
        sa.CheckConstraint("high >= low", name=op.f("ck_premium_index_high_ge_low")),
        sa.CheckConstraint("high >= open AND high >= close", name=op.f("ck_premium_index_high_ge_open_close")),
        sa.CheckConstraint("low <= open AND low <= close", name=op.f("ck_premium_index_low_le_open_close")),
        sa.CheckConstraint("ts_ms >= 0", name=op.f("ck_premium_index_ts_ms_non_negative")),
        sa.PrimaryKeyConstraint("exchange", "symbol", "interval", "ts_ms", name=op.f("pk_premium_index")),
    )
    op.create_table(
        "emergency_runs",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("group_id", sa.Uuid(), nullable=True),
        sa.Column("trigger", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'RUNNING'"), nullable=False),
        sa.Column("current_step", sa.SmallInteger(), server_default=sa.text("1"), nullable=False),
        sa.Column("report", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("completed_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("released_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("confirmed_audit_id", sa.BigInteger(), nullable=True),
        sa.CheckConstraint(
            "status <> 'RELEASED' OR (released_at_ms IS NOT NULL AND confirmed_audit_id IS NOT NULL)",
            name=op.f("ck_emergency_runs_released_by_human"),
        ),
        sa.CheckConstraint(
            "status = 'RUNNING' OR completed_at_ms IS NOT NULL", name=op.f("ck_emergency_runs_completed_has_time")
        ),
        sa.CheckConstraint("status IN ('RUNNING', 'COMPLETED', 'RELEASED')", name=op.f("ck_emergency_runs_status")),
        sa.CheckConstraint(
            "trigger IN ('UI', 'RISK_HALTED', 'CRITICAL_ERROR', 'TELEGRAM')", name=op.f("ck_emergency_runs_trigger")
        ),
        sa.CheckConstraint(
            "current_step >= 1 AND current_step <= 6", name=op.f("ck_emergency_runs_current_step_range")
        ),
        sa.ForeignKeyConstraint(
            ["account_id"], ["exchange_accounts.id"], name=op.f("fk_emergency_runs_account_id_exchange_accounts")
        ),
        sa.ForeignKeyConstraint(
            ["confirmed_audit_id"], ["audit_log.id"], name=op.f("fk_emergency_runs_confirmed_audit_id_audit_log")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_emergency_runs")),
        sa.UniqueConstraint("id", "account_id", name=op.f("uq_emergency_runs_id_account_id")),
    )
    op.create_index(
        "uq_emergency_runs_account_id_active",
        "emergency_runs",
        ["account_id"],
        unique=True,
        postgresql_where=sa.text("status <> 'RELEASED'"),
    )
    op.create_index(
        "uq_emergency_runs_confirmed_audit_id",
        "emergency_runs",
        ["confirmed_audit_id"],
        unique=True,
        postgresql_where=sa.text("confirmed_audit_id IS NOT NULL"),
    )
    op.create_table(
        "equity_snapshots",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.Column("equity", sa.Numeric(), nullable=False),
        sa.Column("wallet_balance", sa.Numeric(), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(), nullable=False),
        sa.Column("peak_equity", sa.Numeric(), nullable=False),
        sa.Column("day_start_equity", sa.Numeric(), nullable=False),
        sa.CheckConstraint(
            "peak_equity >= 0 AND day_start_equity >= 0", name=op.f("ck_equity_snapshots_base_non_negative")
        ),
        sa.CheckConstraint("peak_equity >= equity", name=op.f("ck_equity_snapshots_peak_ge_equity")),
        sa.ForeignKeyConstraint(
            ["account_id"], ["exchange_accounts.id"], name=op.f("fk_equity_snapshots_account_id_exchange_accounts")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_equity_snapshots")),
        sa.UniqueConstraint("account_id", "ts_ms", name=op.f("uq_equity_snapshots_account_id_ts_ms")),
    )
    op.create_table(
        "experiment_trials",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("experiment_id", sa.Text(), nullable=False),
        sa.Column("trial_no", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("params", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("oos_results", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metrics", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("state IN ('COMPLETE', 'PRUNED', 'FAILED')", name=op.f("ck_experiment_trials_state")),
        sa.CheckConstraint("trial_no >= 1", name=op.f("ck_experiment_trials_trial_no_positive")),
        sa.ForeignKeyConstraint(
            ["experiment_id"], ["experiments.id"], name=op.f("fk_experiment_trials_experiment_id_experiments")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_experiment_trials")),
        sa.UniqueConstraint("experiment_id", "trial_no", name=op.f("uq_experiment_trials_experiment_id_trial_no")),
    )
    op.create_table(
        "ledger",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("amount", sa.Numeric(), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("kind <> 'DEPOSIT' OR amount > 0", name=op.f("ck_ledger_deposit_positive")),
        sa.CheckConstraint("kind <> 'WITHDRAWAL' OR amount < 0", name=op.f("ck_ledger_withdrawal_negative")),
        sa.CheckConstraint(
            "kind IN ('FEE', 'FUNDING', 'REALIZED_PNL', 'DEPOSIT', 'WITHDRAWAL')", name=op.f("ck_ledger_kind")
        ),
        sa.CheckConstraint(
            "symbol IS NULL OR symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_ledger_symbol_format")
        ),
        sa.CheckConstraint("char_length(event_id) > 0", name=op.f("ck_ledger_event_id_not_empty")),
        sa.ForeignKeyConstraint(
            ["account_id"], ["exchange_accounts.id"], name=op.f("fk_ledger_account_id_exchange_accounts")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_ledger")),
        sa.UniqueConstraint("account_id", "event_id", "kind", name=op.f("uq_ledger_account_id_event_id_kind")),
    )
    op.create_index("ix_ledger_account_id_ts_ms", "ledger", ["account_id", "ts_ms"], unique=False)
    op.create_table(
        "models",
        sa.Column("model_version", sa.Text(), nullable=False),
        sa.Column("algorithm", sa.Text(), nullable=False),
        sa.Column("label_type", sa.Text(), nullable=False),
        sa.Column("trade_symbol", sa.Text(), nullable=False),
        sa.Column("timeframe", sa.Text(), nullable=False),
        sa.Column("feature_schema_hash", sa.Text(), nullable=False),
        sa.Column("preprocessing_version", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("git_commit", sa.Text(), nullable=False),
        sa.Column("git_dirty", sa.Boolean(), nullable=False),
        sa.Column("library_versions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("live_corridor", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("validated_by", sa.Text(), nullable=True),
        sa.Column("experiment_id", sa.Text(), nullable=True),
        sa.Column("artifact_path", sa.Text(), nullable=True),
        sa.Column("artifact_sha256", sa.Text(), nullable=True),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("confirmed_audit_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("status_changed_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "(label_type = 'rule_only') = (artifact_path IS NULL)", name=op.f("ck_models_artifact_path_by_label_type")
        ),
        sa.CheckConstraint(
            "NOT (git_dirty AND status IN ('CANDIDATE', 'APPROVED'))", name=op.f("ck_models_dirty_not_promoted")
        ),
        sa.CheckConstraint("label_type IN ('meta', 'direct', 'rule_only')", name=op.f("ck_models_label_type")),
        sa.CheckConstraint(
            "status <> 'APPROVED' OR confirmed_audit_id IS NOT NULL", name=op.f("ck_models_approved_by_human")
        ),
        sa.CheckConstraint("status IN ('TRAINED', 'CANDIDATE', 'APPROVED', 'RETIRED')", name=op.f("ck_models_status")),
        sa.CheckConstraint(
            "status NOT IN ('CANDIDATE', 'APPROVED') OR live_corridor IS NOT NULL",
            name=op.f("ck_models_promoted_has_corridor"),
        ),
        sa.CheckConstraint(
            "trade_symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_models_trade_symbol_format")
        ),
        sa.CheckConstraint("char_length(model_version) > 0", name=op.f("ck_models_model_version_not_empty")),
        sa.CheckConstraint(
            "validated_by IS NULL OR validated_by <> model_version", name=op.f("ck_models_validated_by_other")
        ),
        sa.ForeignKeyConstraint(
            ["confirmed_audit_id"], ["audit_log.id"], name=op.f("fk_models_confirmed_audit_id_audit_log")
        ),
        sa.ForeignKeyConstraint(
            ["experiment_id"], ["experiments.id"], name=op.f("fk_models_experiment_id_experiments")
        ),
        sa.ForeignKeyConstraint(["validated_by"], ["models.model_version"], name=op.f("fk_models_validated_by_models")),
        sa.PrimaryKeyConstraint("model_version", name=op.f("pk_models")),
    )
    op.create_index(
        "uq_models_confirmed_audit_id",
        "models",
        ["confirmed_audit_id"],
        unique=True,
        postgresql_where=sa.text("confirmed_audit_id IS NOT NULL"),
    )
    op.create_table(
        "risk_config",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column("params", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("loosening", sa.Boolean(), nullable=False),
        sa.Column("confirmed_audit_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "NOT loosening OR confirmed_audit_id IS NOT NULL", name=op.f("ck_risk_config_loosening_confirmed")
        ),
        sa.CheckConstraint("version >= 1", name=op.f("ck_risk_config_version_positive")),
        sa.ForeignKeyConstraint(
            ["account_id"], ["exchange_accounts.id"], name=op.f("fk_risk_config_account_id_exchange_accounts")
        ),
        sa.ForeignKeyConstraint(
            ["confirmed_audit_id"], ["audit_log.id"], name=op.f("fk_risk_config_confirmed_audit_id_audit_log")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_risk_config")),
        sa.UniqueConstraint("account_id", "version", name=op.f("uq_risk_config_account_id_version")),
    )
    op.create_index(
        "uq_risk_config_confirmed_audit_id",
        "risk_config",
        ["confirmed_audit_id"],
        unique=True,
        postgresql_where=sa.text("confirmed_audit_id IS NOT NULL"),
    )
    op.create_table(
        "bots",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("timeframe", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'STOPPED'"), nullable=False),
        sa.Column("pause_reasons", postgresql.ARRAY(sa.Text()), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("model_version", sa.Text(), nullable=True),
        sa.Column("pending_model_version", sa.Text(), nullable=True),
        sa.Column("config", postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("version", sa.BigInteger(), server_default=sa.text("1"), nullable=False),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("updated_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("mode IN ('demo', 'testnet', 'live')", name=op.f("ck_bots_mode")),
        sa.CheckConstraint(
            "pause_reasons <@ ARRAY['DAILY_LOSS', 'COOLDOWN', 'DATA_GAP', 'INSTRUMENT_NOT_TRADING', 'MODEL_SWAP', 'DRIFT', 'MODEL_UNDERPERFORMS', 'UNKNOWN_POSITION', 'FOREIGN_HOST_ORDERS', 'LEVERAGE_MISMATCH', 'MANUAL']::text[]",
            name=op.f("ck_bots_pause_reasons_known"),
        ),
        sa.CheckConstraint(
            "status <> 'PAUSED' OR cardinality(pause_reasons) > 0", name=op.f("ck_bots_paused_has_reason")
        ),
        sa.CheckConstraint(
            "status <> 'RUNNING' OR cardinality(pause_reasons) = 0", name=op.f("ck_bots_running_has_no_reason")
        ),
        sa.CheckConstraint(
            "status IN ('STOPPED', 'STARTING', 'RUNNING', 'PAUSED', 'RECONCILING', 'ERROR', 'EMERGENCY_STOP')",
            name=op.f("ck_bots_status"),
        ),
        sa.CheckConstraint("symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_bots_symbol_format")),
        sa.CheckConstraint("timeframe ~ '^[0-9]+(m|h|d|w)$'", name=op.f("ck_bots_timeframe_format")),
        sa.CheckConstraint(
            "pending_model_version IS NULL OR pending_model_version IS DISTINCT FROM model_version",
            name=op.f("ck_bots_pending_model_differs"),
        ),
        sa.CheckConstraint("version >= 1", name=op.f("ck_bots_version_positive")),
        sa.ForeignKeyConstraint(
            ["account_id", "mode"],
            ["exchange_accounts.id", "exchange_accounts.mode"],
            name=op.f("fk_bots_account_id_mode_exchange_accounts"),
        ),
        sa.ForeignKeyConstraint(["model_version"], ["models.model_version"], name=op.f("fk_bots_model_version_models")),
        sa.ForeignKeyConstraint(
            ["pending_model_version"], ["models.model_version"], name=op.f("fk_bots_pending_model_version_models")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_bots")),
        sa.UniqueConstraint("id", "account_id", "mode", name=op.f("uq_bots_id_account_id_mode")),
        sa.UniqueConstraint("id", "account_id", name=op.f("uq_bots_id_account_id")),
    )
    op.create_index(
        "uq_bots_account_id_symbol_active",
        "bots",
        ["account_id", "symbol"],
        unique=True,
        postgresql_where=sa.text("status <> 'STOPPED'"),
    )
    op.create_table(
        "emergency_steps",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("step_no", sa.SmallInteger(), nullable=False),
        sa.Column("attempt", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'STARTED'"), nullable=False),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("finished_at_ms", sa.BigInteger(), nullable=True),
        sa.CheckConstraint(
            "(status = 'STARTED') = (finished_at_ms IS NULL)", name=op.f("ck_emergency_steps_finished_iff_not_started")
        ),
        sa.CheckConstraint("status IN ('STARTED', 'SUCCEEDED', 'FAILED')", name=op.f("ck_emergency_steps_status")),
        sa.CheckConstraint("attempt >= 1", name=op.f("ck_emergency_steps_attempt_positive")),
        sa.CheckConstraint("step_no >= 1 AND step_no <= 6", name=op.f("ck_emergency_steps_step_no_range")),
        sa.ForeignKeyConstraint(
            ["run_id"], ["emergency_runs.id"], name=op.f("fk_emergency_steps_run_id_emergency_runs")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_emergency_steps")),
        sa.UniqueConstraint("run_id", "step_no", "attempt", name=op.f("uq_emergency_steps_run_id_step_no_attempt")),
    )
    op.create_table(
        "holdout_openings",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("research_line", sa.Text(), nullable=False),
        sa.Column("holdout_from_ms", sa.BigInteger(), nullable=False),
        sa.Column("holdout_to_ms", sa.BigInteger(), nullable=False),
        sa.Column("opening_no", sa.SmallInteger(), nullable=False),
        sa.Column("experiment_id", sa.Text(), nullable=False),
        sa.Column("model_version", sa.Text(), nullable=False),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("opened_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "research_line ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_holdout_openings_research_line_format")
        ),
        sa.CheckConstraint("holdout_to_ms > holdout_from_ms", name=op.f("ck_holdout_openings_holdout_range_valid")),
        sa.CheckConstraint("opening_no IN (1, 2)", name=op.f("ck_holdout_openings_opening_no_max_2")),
        sa.ForeignKeyConstraint(
            ["experiment_id"], ["experiments.id"], name=op.f("fk_holdout_openings_experiment_id_experiments")
        ),
        sa.ForeignKeyConstraint(
            ["model_version"], ["models.model_version"], name=op.f("fk_holdout_openings_model_version_models")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_holdout_openings")),
        sa.UniqueConstraint(
            "research_line",
            "holdout_from_ms",
            "holdout_to_ms",
            "opening_no",
            name="uq_holdout_openings_window_opening_no",
        ),
    )
    op.create_table(
        "system_state",
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("risk_status", sa.Text(), server_default=sa.text("'NORMAL'"), nullable=False),
        sa.Column("emergency_run_id", sa.Uuid(), nullable=True),
        sa.Column("last_reconcile_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("version", sa.BigInteger(), server_default=sa.text("1"), nullable=False),
        sa.Column("updated_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "risk_status IN ('NORMAL', 'RESTRICTED', 'HALTED')", name=op.f("ck_system_state_risk_status")
        ),
        sa.CheckConstraint("version >= 1", name=op.f("ck_system_state_version_positive")),
        sa.ForeignKeyConstraint(
            ["account_id"], ["exchange_accounts.id"], name=op.f("fk_system_state_account_id_exchange_accounts")
        ),
        sa.ForeignKeyConstraint(
            ["emergency_run_id", "account_id"],
            ["emergency_runs.id", "emergency_runs.account_id"],
            name=op.f("fk_system_state_emergency_run_id_account_id_emergency_runs"),
        ),
        sa.PrimaryKeyConstraint("account_id", name=op.f("pk_system_state")),
    )
    op.create_table(
        "signals",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("bot_id", sa.Uuid(), nullable=False),
        sa.Column("entry_candle_open_time_ms", sa.BigInteger(), nullable=False),
        sa.Column("features_candle_open_time_ms", sa.BigInteger(), nullable=False),
        sa.Column("computed_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("feature_schema_hash", sa.Text(), nullable=False),
        sa.Column("model_version", sa.Text(), nullable=False),
        sa.Column("primary_signal", sa.Text(), nullable=False),
        sa.Column("prediction", sa.Text(), nullable=False),
        sa.Column("probability", sa.Numeric(), nullable=True),
        sa.Column("expected_edge_atr", sa.Numeric(), nullable=True),
        sa.Column("decision", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'NEW'"), nullable=False),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("updated_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("decision IN ('ENTER', 'SKIP')", name=op.f("ck_signals_decision")),
        sa.CheckConstraint("prediction IN ('LONG', 'SHORT', 'NONE')", name=op.f("ck_signals_prediction")),
        sa.CheckConstraint("primary_signal IN ('LONG', 'SHORT')", name=op.f("ck_signals_primary_signal")),
        sa.CheckConstraint(
            "status IN ('NEW', 'EXECUTING', 'DONE', 'EXPIRED_UNEXECUTED')", name=op.f("ck_signals_status")
        ),
        sa.CheckConstraint(
            "computed_at_ms >= entry_candle_open_time_ms - 1000", name=op.f("ck_signals_computed_after_entry_open")
        ),
        sa.CheckConstraint(
            "features_candle_open_time_ms < entry_candle_open_time_ms", name=op.f("ck_signals_features_before_entry")
        ),
        sa.CheckConstraint(
            "probability IS NULL OR (probability >= 0 AND probability <= 1)", name=op.f("ck_signals_probability_range")
        ),
        sa.ForeignKeyConstraint(["bot_id"], ["bots.id"], name=op.f("fk_signals_bot_id_bots")),
        sa.ForeignKeyConstraint(
            ["model_version"], ["models.model_version"], name=op.f("fk_signals_model_version_models")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_signals")),
        sa.UniqueConstraint(
            "bot_id", "entry_candle_open_time_ms", name=op.f("uq_signals_bot_id_entry_candle_open_time_ms")
        ),
        sa.UniqueConstraint("id", "bot_id", name=op.f("uq_signals_id_bot_id")),
    )
    op.create_table(
        "positions",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("bot_id", sa.Uuid(), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'OPEN'"), nullable=False),
        sa.Column("qty", sa.Numeric(), nullable=False),
        sa.Column("entry_price", sa.Numeric(), nullable=True),
        sa.Column("sl_price", sa.Numeric(), nullable=True),
        sa.Column("tp_price", sa.Numeric(), nullable=True),
        sa.Column("sl_trigger_by", sa.Text(), nullable=True),
        sa.Column("tp_trigger_by", sa.Text(), nullable=True),
        sa.Column("protection_status", sa.Text(), nullable=True),
        sa.Column("unprotected_since_ms", sa.BigInteger(), nullable=True),
        sa.Column("entry_atr", sa.Numeric(), nullable=True),
        sa.Column("max_hold_bars", sa.BigInteger(), nullable=True),
        sa.Column("signal_id", sa.Uuid(), nullable=True),
        sa.Column("model_version", sa.Text(), nullable=True),
        sa.Column("realized_pnl", sa.Numeric(), nullable=True),
        sa.Column("close_reason", sa.Text(), nullable=True),
        sa.Column("opened_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("closed_at_ms", sa.BigInteger(), nullable=True),
        sa.Column("version", sa.BigInteger(), server_default=sa.text("1"), nullable=False),
        sa.Column("updated_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("(status = 'OPEN') = (closed_at_ms IS NULL)", name=op.f("ck_positions_closed_iff_not_open")),
        sa.CheckConstraint("mode IN ('demo', 'testnet', 'live')", name=op.f("ck_positions_mode")),
        sa.CheckConstraint(
            "protection_status IN ('ENTRY_FILLED_UNPROTECTED', 'PROTECTED')",
            name=op.f("ck_positions_protection_status"),
        ),
        sa.CheckConstraint(
            "protection_status IS DISTINCT FROM 'ENTRY_FILLED_UNPROTECTED' OR unprotected_since_ms IS NOT NULL",
            name=op.f("ck_positions_unprotected_has_since"),
        ),
        sa.CheckConstraint("side IN ('LONG', 'SHORT')", name=op.f("ck_positions_side")),
        sa.CheckConstraint(
            "status <> 'OPEN' OR protection_status IS NOT NULL", name=op.f("ck_positions_open_has_protection_status")
        ),
        sa.CheckConstraint("status <> 'OPEN' OR qty > 0", name=op.f("ck_positions_open_has_qty")),
        sa.CheckConstraint("status IN ('OPEN', 'CLOSED', 'CLOSED_EXTERNALLY')", name=op.f("ck_positions_status")),
        sa.CheckConstraint("symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_positions_symbol_format")),
        sa.CheckConstraint("entry_atr IS NULL OR entry_atr > 0", name=op.f("ck_positions_entry_atr_positive")),
        sa.CheckConstraint("entry_price IS NULL OR entry_price > 0", name=op.f("ck_positions_entry_price_positive")),
        sa.CheckConstraint(
            "max_hold_bars IS NULL OR max_hold_bars > 0", name=op.f("ck_positions_max_hold_bars_positive")
        ),
        sa.CheckConstraint("qty >= 0", name=op.f("ck_positions_qty_non_negative")),
        sa.CheckConstraint("sl_price IS NULL OR sl_price > 0", name=op.f("ck_positions_sl_price_positive")),
        sa.CheckConstraint("tp_price IS NULL OR tp_price > 0", name=op.f("ck_positions_tp_price_positive")),
        sa.CheckConstraint("version >= 1", name=op.f("ck_positions_version_positive")),
        sa.ForeignKeyConstraint(
            ["bot_id", "account_id", "mode"],
            ["bots.id", "bots.account_id", "bots.mode"],
            name=op.f("fk_positions_bot_id_account_id_mode_bots"),
        ),
        sa.ForeignKeyConstraint(
            ["model_version"], ["models.model_version"], name=op.f("fk_positions_model_version_models")
        ),
        sa.ForeignKeyConstraint(
            ["signal_id", "bot_id"],
            ["signals.id", "signals.bot_id"],
            name=op.f("fk_positions_signal_id_bot_id_signals"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_positions")),
        sa.UniqueConstraint("id", "account_id", "mode", name=op.f("uq_positions_id_account_id_mode")),
    )
    op.create_index(
        "uq_positions_account_id_symbol_open",
        "positions",
        ["account_id", "symbol"],
        unique=True,
        postgresql_where=sa.text("status = 'OPEN'"),
    )
    op.create_table(
        "risk_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("bot_id", sa.Uuid(), nullable=True),
        sa.Column("signal_id", sa.Uuid(), nullable=True),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("reason_code", sa.Text(), nullable=False),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("kind IN ('REJECT', 'LIMIT_TRIGGERED', 'STATUS_CHANGE')", name=op.f("ck_risk_events_kind")),
        sa.CheckConstraint("reason_code ~ '^[A-Z][A-Z0-9_]*$'", name=op.f("ck_risk_events_reason_code_format")),
        sa.CheckConstraint("signal_id IS NULL OR bot_id IS NOT NULL", name=op.f("ck_risk_events_signal_requires_bot")),
        sa.ForeignKeyConstraint(
            ["account_id"], ["exchange_accounts.id"], name=op.f("fk_risk_events_account_id_exchange_accounts")
        ),
        sa.ForeignKeyConstraint(
            ["bot_id", "account_id"], ["bots.id", "bots.account_id"], name=op.f("fk_risk_events_bot_id_account_id_bots")
        ),
        sa.ForeignKeyConstraint(
            ["signal_id", "bot_id"],
            ["signals.id", "signals.bot_id"],
            name=op.f("fk_risk_events_signal_id_bot_id_signals"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_risk_events")),
    )
    op.create_index("ix_risk_events_account_id_ts_ms", "risk_events", ["account_id", "ts_ms"], unique=False)
    op.create_table(
        "orders",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("bot_id", sa.Uuid(), nullable=True),
        sa.Column("origin", sa.Text(), server_default=sa.text("'BOT'"), nullable=False),
        sa.Column("signal_id", sa.Uuid(), nullable=True),
        sa.Column("position_id", sa.Uuid(), nullable=True),
        sa.Column("client_order_id", sa.Text(), nullable=True),
        sa.Column("exchange_order_id", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("order_type", sa.Text(), nullable=False),
        sa.Column("reduce_only", sa.Boolean(), nullable=False),
        sa.Column("time_in_force", sa.Text(), nullable=True),
        sa.Column("qty", sa.Numeric(), nullable=False),
        sa.Column("price", sa.Numeric(), nullable=True),
        sa.Column("sl_price", sa.Numeric(), nullable=True),
        sa.Column("tp_price", sa.Numeric(), nullable=True),
        sa.Column("state", sa.Text(), server_default=sa.text("'CREATED'"), nullable=False),
        sa.Column("filled_qty", sa.Numeric(), server_default=sa.text("0"), nullable=False),
        sa.Column("reject_code", sa.Text(), nullable=True),
        sa.Column("reject_reason", sa.Text(), nullable=True),
        sa.Column("version", sa.BigInteger(), server_default=sa.text("1"), nullable=False),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("updated_at_ms", sa.BigInteger(), nullable=False),
        sa.Column("submitted_at_ms", sa.BigInteger(), nullable=True),
        sa.CheckConstraint(
            "bot_id IS NOT NULL OR origin IN ('ORPHAN', 'EXCHANGE')", name=op.f("ck_orders_bot_required")
        ),
        sa.CheckConstraint(
            "client_order_id IS NOT NULL OR origin = 'EXCHANGE'", name=op.f("ck_orders_client_order_id_required")
        ),
        sa.CheckConstraint(
            "client_order_id ~ '^tb[a-z0-9]{2}[0-9a-f]{32}$'", name=op.f("ck_orders_client_order_id_format")
        ),
        sa.CheckConstraint("mode IN ('demo', 'testnet', 'live')", name=op.f("ck_orders_mode")),
        sa.CheckConstraint("order_type <> 'LIMIT' OR price IS NOT NULL", name=op.f("ck_orders_limit_has_price")),
        sa.CheckConstraint("order_type IN ('MARKET', 'LIMIT')", name=op.f("ck_orders_order_type")),
        sa.CheckConstraint(
            "origin <> 'BOT' OR reduce_only OR signal_id IS NOT NULL", name=op.f("ck_orders_bot_entry_has_signal")
        ),
        sa.CheckConstraint(
            "origin <> 'MANUAL_TEST' OR (mode <> 'live' AND signal_id IS NULL)",
            name=op.f("ck_orders_manual_test_not_live"),
        ),
        sa.CheckConstraint("origin IN ('BOT', 'MANUAL_TEST', 'ORPHAN', 'EXCHANGE')", name=op.f("ck_orders_origin")),
        sa.CheckConstraint(
            "origin NOT IN ('ORPHAN', 'EXCHANGE') OR (state NOT IN ('CREATED', 'SUBMITTING') AND signal_id IS NULL AND exchange_order_id IS NOT NULL)",
            name=op.f("ck_orders_imported_not_submittable"),
        ),
        sa.CheckConstraint("side IN ('BUY', 'SELL')", name=op.f("ck_orders_side")),
        sa.CheckConstraint(
            "state IN ('CREATED', 'SUBMITTING', 'OPEN', 'PARTIALLY_FILLED', 'FILLED', 'PENDING_CANCEL', 'CANCELED', 'REJECTED', 'EXPIRED', 'SUBMISSION_UNKNOWN')",
            name=op.f("ck_orders_state"),
        ),
        sa.CheckConstraint("symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_orders_symbol_format")),
        sa.CheckConstraint("filled_qty >= 0 AND filled_qty <= qty", name=op.f("ck_orders_filled_qty_range")),
        sa.CheckConstraint("price IS NULL OR price > 0", name=op.f("ck_orders_price_positive")),
        sa.CheckConstraint("qty > 0", name=op.f("ck_orders_qty_positive")),
        sa.CheckConstraint("signal_id IS NULL OR bot_id IS NOT NULL", name=op.f("ck_orders_signal_requires_bot")),
        sa.CheckConstraint("sl_price IS NULL OR sl_price > 0", name=op.f("ck_orders_sl_price_positive")),
        sa.CheckConstraint("tp_price IS NULL OR tp_price > 0", name=op.f("ck_orders_tp_price_positive")),
        sa.CheckConstraint("version >= 1", name=op.f("ck_orders_version_positive")),
        sa.ForeignKeyConstraint(
            ["account_id", "mode"],
            ["exchange_accounts.id", "exchange_accounts.mode"],
            name=op.f("fk_orders_account_id_mode_exchange_accounts"),
        ),
        sa.ForeignKeyConstraint(
            ["bot_id", "account_id", "mode"],
            ["bots.id", "bots.account_id", "bots.mode"],
            name=op.f("fk_orders_bot_id_account_id_mode_bots"),
        ),
        sa.ForeignKeyConstraint(
            ["position_id", "account_id", "mode"],
            ["positions.id", "positions.account_id", "positions.mode"],
            name=op.f("fk_orders_position_id_account_id_mode_positions"),
        ),
        sa.ForeignKeyConstraint(
            ["signal_id", "bot_id"], ["signals.id", "signals.bot_id"], name=op.f("fk_orders_signal_id_bot_id_signals")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_orders")),
        sa.UniqueConstraint("account_id", "exchange_order_id", name=op.f("uq_orders_account_id_exchange_order_id")),
        sa.UniqueConstraint("client_order_id", name=op.f("uq_orders_client_order_id")),
        sa.UniqueConstraint("id", "account_id", "mode", "symbol", "side", name="uq_orders_fill_target"),
        sa.UniqueConstraint("id", "bot_id", name=op.f("uq_orders_id_bot_id")),
    )
    op.create_index(
        "ix_orders_bot_id_active",
        "orders",
        ["bot_id"],
        unique=False,
        postgresql_where=sa.text("state NOT IN ('FILLED', 'CANCELED', 'REJECTED', 'EXPIRED')"),
    )
    op.create_index(
        "uq_orders_signal_id_entry", "orders", ["signal_id"], unique=True, postgresql_where=sa.text("NOT reduce_only")
    )
    op.create_table(
        "decision_log",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("bot_id", sa.Uuid(), nullable=False),
        sa.Column("signal_id", sa.Uuid(), nullable=True),
        sa.Column("order_id", sa.Uuid(), nullable=True),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("decision", sa.Text(), nullable=False),
        sa.Column("reason_code", sa.Text(), nullable=True),
        sa.Column("probability", sa.Numeric(), nullable=True),
        sa.Column("expected_edge_atr", sa.Numeric(), nullable=True),
        sa.Column("slippage_atr", sa.Numeric(), nullable=True),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "reason_code IS NULL OR reason_code ~ '^[A-Z][A-Z0-9_]*$'", name=op.f("ck_decision_log_reason_code_format")
        ),
        sa.CheckConstraint("stage IN ('SIGNAL', 'RISK', 'ORDER', 'RESULT')", name=op.f("ck_decision_log_stage")),
        sa.CheckConstraint(
            "probability IS NULL OR (probability >= 0 AND probability <= 1)",
            name=op.f("ck_decision_log_probability_range"),
        ),
        sa.ForeignKeyConstraint(["bot_id"], ["bots.id"], name=op.f("fk_decision_log_bot_id_bots")),
        sa.ForeignKeyConstraint(
            ["order_id", "bot_id"], ["orders.id", "orders.bot_id"], name=op.f("fk_decision_log_order_id_bot_id_orders")
        ),
        sa.ForeignKeyConstraint(
            ["signal_id", "bot_id"],
            ["signals.id", "signals.bot_id"],
            name=op.f("fk_decision_log_signal_id_bot_id_signals"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_decision_log")),
    )
    op.create_index("ix_decision_log_bot_id_ts_ms", "decision_log", ["bot_id", "ts_ms"], unique=False)
    op.create_table(
        "fills",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("exec_id", sa.Text(), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("price", sa.Numeric(), nullable=False),
        sa.Column("qty", sa.Numeric(), nullable=False),
        sa.Column("fee", sa.Numeric(), nullable=False),
        sa.Column("fee_currency", sa.Text(), nullable=False),
        sa.Column("is_maker", sa.Boolean(), nullable=True),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("mode IN ('demo', 'testnet', 'live')", name=op.f("ck_fills_mode")),
        sa.CheckConstraint("side IN ('BUY', 'SELL')", name=op.f("ck_fills_side")),
        sa.CheckConstraint("symbol ~ '^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$'", name=op.f("ck_fills_symbol_format")),
        sa.CheckConstraint("char_length(exec_id) > 0", name=op.f("ck_fills_exec_id_not_empty")),
        sa.CheckConstraint("price > 0", name=op.f("ck_fills_price_positive")),
        sa.CheckConstraint("qty > 0", name=op.f("ck_fills_qty_positive")),
        sa.ForeignKeyConstraint(
            ["order_id", "account_id", "mode", "symbol", "side"],
            ["orders.id", "orders.account_id", "orders.mode", "orders.symbol", "orders.side"],
            name="fk_fills_order",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_fills")),
        sa.UniqueConstraint("exec_id", name=op.f("uq_fills_exec_id")),
    )
    op.create_index("ix_fills_order_id", "fills", ["order_id"], unique=False)
    op.create_table(
        "order_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("from_state", sa.Text(), nullable=True),
        sa.Column("to_state", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("ts_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint(
            "from_state IS NULL OR from_state IN ('CREATED', 'SUBMITTING', 'OPEN', 'PARTIALLY_FILLED', 'FILLED', 'PENDING_CANCEL', 'CANCELED', 'REJECTED', 'EXPIRED', 'SUBMISSION_UNKNOWN')",
            name=op.f("ck_order_events_from_state"),
        ),
        sa.CheckConstraint("source IN ('WS', 'REST', 'LOCAL')", name=op.f("ck_order_events_source")),
        sa.CheckConstraint(
            "to_state IN ('CREATED', 'SUBMITTING', 'OPEN', 'PARTIALLY_FILLED', 'FILLED', 'PENDING_CANCEL', 'CANCELED', 'REJECTED', 'EXPIRED', 'SUBMISSION_UNKNOWN')",
            name=op.f("ck_order_events_to_state"),
        ),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"], name=op.f("fk_order_events_order_id_orders")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_order_events")),
    )
    op.create_index("ix_order_events_order_id_id", "order_events", ["order_id", "id"], unique=False)


def _drop_tables() -> None:
    op.drop_index("ix_order_events_order_id_id", table_name="order_events")
    op.drop_table("order_events")
    op.drop_index("ix_fills_order_id", table_name="fills")
    op.drop_table("fills")
    op.drop_index("ix_decision_log_bot_id_ts_ms", table_name="decision_log")
    op.drop_table("decision_log")
    op.drop_index("uq_orders_signal_id_entry", table_name="orders", postgresql_where=sa.text("NOT reduce_only"))
    op.drop_index(
        "ix_orders_bot_id_active",
        table_name="orders",
        postgresql_where=sa.text("state NOT IN ('FILLED', 'CANCELED', 'REJECTED', 'EXPIRED')"),
    )
    op.drop_table("orders")
    op.drop_index("ix_risk_events_account_id_ts_ms", table_name="risk_events")
    op.drop_table("risk_events")
    op.drop_index(
        "uq_positions_account_id_symbol_open", table_name="positions", postgresql_where=sa.text("status = 'OPEN'")
    )
    op.drop_table("positions")
    op.drop_table("signals")
    op.drop_table("system_state")
    op.drop_table("holdout_openings")
    op.drop_table("emergency_steps")
    op.drop_index(
        "uq_bots_account_id_symbol_active", table_name="bots", postgresql_where=sa.text("status <> 'STOPPED'")
    )
    op.drop_table("bots")
    op.drop_index(
        "uq_risk_config_confirmed_audit_id",
        table_name="risk_config",
        postgresql_where=sa.text("confirmed_audit_id IS NOT NULL"),
    )
    op.drop_table("risk_config")
    op.drop_index(
        "uq_models_confirmed_audit_id", table_name="models", postgresql_where=sa.text("confirmed_audit_id IS NOT NULL")
    )
    op.drop_table("models")
    op.drop_index("ix_ledger_account_id_ts_ms", table_name="ledger")
    op.drop_table("ledger")
    op.drop_table("experiment_trials")
    op.drop_table("equity_snapshots")
    op.drop_index(
        "uq_emergency_runs_confirmed_audit_id",
        table_name="emergency_runs",
        postgresql_where=sa.text("confirmed_audit_id IS NOT NULL"),
    )
    op.drop_index(
        "uq_emergency_runs_account_id_active",
        table_name="emergency_runs",
        postgresql_where=sa.text("status <> 'RELEASED'"),
    )
    op.drop_table("emergency_runs")
    op.drop_table("premium_index")
    op.drop_table("open_interest")
    op.drop_table("mark_index_candles")
    op.drop_table("long_short_ratio")
    op.drop_table("liquidations_hourly")
    op.drop_index(
        "uq_jobs_dedup_key_active", table_name="jobs", postgresql_where=sa.text("status IN ('QUEUED', 'RUNNING')")
    )
    op.drop_index("ix_jobs_queue", table_name="jobs", postgresql_where=sa.text("status = 'QUEUED'"))
    op.drop_table("jobs")
    op.drop_table("instruments")
    op.drop_table("funding_rates")
    op.drop_table("experiments")
    op.drop_table("exchange_accounts")
    op.drop_index("uq_data_gaps_open", table_name="data_gaps", postgresql_where=sa.text("resolved_at_ms IS NULL"))
    op.drop_table("data_gaps")
    op.drop_table("candles")
    op.drop_index("ix_audit_log_ts_ms", table_name="audit_log")
    op.drop_table("audit_log")
