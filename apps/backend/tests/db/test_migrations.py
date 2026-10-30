"""Міграції: upgrade → downgrade -1 → upgrade, відсутність дрейфу моделей, TimescaleDB, тригери (T0.4)."""

from __future__ import annotations

from collections.abc import Sequence

import pytest
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import text
from sqlalchemy.engine import URL, Connection

from app.core.db import load_models
from tests.db.dbutil import make_engine, run_alembic

pytestmark = pytest.mark.integration

DAY_MS = 86_400_000
HYPERTABLES = {
    "candles",
    "open_interest",
    "premium_index",
    "mark_index_candles",
    "long_short_ratio",
    "liquidations_hourly",
}
APPEND_ONLY = {"order_events", "audit_log", "ledger", "experiment_trials", "risk_config"}
FUNCTIONS = {
    "ctb_unix_now_ms",
    "ctb_forbid_mutation",
    "ctb_orders_guard",
    "ctb_bots_guard",
    "ctb_require_confirmed_audit",
    "ctb_api_idempotency_guard",
}
# CHECK `premium_index` про діапазон години. 0001: open теж у [low, high]; 0004 (T1.9): open — close попередньої
# години (так рахує Bybit), обмежені лише high, low і close.
PREMIUM_CHECKS_0001 = {
    "ck_premium_index_high_ge_low": "CHECK ((high >= low))",
    "ck_premium_index_high_ge_open_close": "CHECK (((high >= open) AND (high >= close)))",
    "ck_premium_index_low_le_open_close": "CHECK (((low <= open) AND (low <= close)))",
}
PREMIUM_CHECKS_0004 = {
    "ck_premium_index_high_ge_low": "CHECK ((high >= low))",
    "ck_premium_index_high_ge_close": "CHECK ((high >= close))",
    "ck_premium_index_low_le_close": "CHECK ((low <= close))",
}


async def _scalars(url: URL, sql: str) -> list[object]:
    engine = make_engine(url)
    try:
        async with engine.connect() as conn:
            return list((await conn.execute(text(sql))).scalars())
    finally:
        await engine.dispose()


async def _execute(url: URL, sql: str) -> None:
    engine = make_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(sql))
    finally:
        await engine.dispose()


async def _premium_range_checks(url: URL) -> dict[str, str]:
    """CHECK `premium_index` про high/low: ім'я → вираз, як його зберіг PostgreSQL."""
    rows = await _rows(
        url,
        "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint"
        " WHERE conrelid = 'premium_index'::regclass AND contype = 'c' AND conname ~ '_(high|low)_'",
    )
    return {str(name): str(definition) for name, definition in rows}


async def _rows(url: URL, sql: str) -> list[tuple[object, ...]]:
    engine = make_engine(url)
    try:
        async with engine.connect() as conn:
            return [tuple(row) for row in (await conn.execute(text(sql))).all()]
    finally:
        await engine.dispose()


async def _public_tables(url: URL) -> set[str]:
    rows = await _scalars(url, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    return {str(r) for r in rows} - {"alembic_version"}


async def _functions(url: URL) -> set[str]:
    rows = await _scalars(url, "SELECT proname FROM pg_proc WHERE pronamespace = 'public'::regnamespace")
    return {str(r) for r in rows}


async def _diff(url: URL) -> Sequence[object]:
    metadata = load_models()
    engine = make_engine(url)
    try:
        async with engine.connect() as conn:

            def _compare(sync_conn: Connection) -> Sequence[object]:
                ctx = MigrationContext.configure(sync_conn, opts={"compare_type": True, "compare_server_default": True})
                return list(compare_metadata(ctx, metadata))

            return await conn.run_sync(_compare)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_upgrade_downgrade_upgrade(migrated_db: URL) -> None:
    expected = set(load_models().tables)
    assert await _public_tables(migrated_db) == expected
    assert await _functions(migrated_db) >= FUNCTIONS
    assert await _premium_range_checks(migrated_db) == PREMIUM_CHECKS_0004

    # 0004 (T1.9): open premium не обмежений [low, high]. Відкат повертає старі CHECK і прибирає рядки,
    # які під ними не могли б існувати; узгоджений рядок лишається.
    await _execute(
        migrated_db,
        "INSERT INTO premium_index (exchange, symbol, interval, ts_ms, open, high, low, close) VALUES"
        " ('bybit', 'BTC/USDT:USDT', '1h', 0, 0.0006, 0.0005, 0.0004, 0.0004),"
        " ('bybit', 'BTC/USDT:USDT', '1h', 3600000, 0.00045, 0.0005, 0.0004, 0.0004)",
    )
    await run_alembic(migrated_db, "downgrade", "-1")
    assert await _scalars(migrated_db, "SELECT version_num FROM alembic_version") == ["0003"]
    assert await _premium_range_checks(migrated_db) == PREMIUM_CHECKS_0001
    assert await _scalars(migrated_db, "SELECT ts_ms FROM premium_index") == [3600000]

    # 0003 (T1.7): колонка `instruments.launch_time_ms`. Таблиці ті самі.
    await run_alembic(migrated_db, "downgrade", "-1")
    assert await _public_tables(migrated_db) == expected
    assert "ctb_api_idempotency_guard" in await _functions(migrated_db)
    assert await _scalars(migrated_db, "SELECT version_num FROM alembic_version") == ["0002"]
    launch = await _scalars(
        migrated_db,
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_name = 'instruments' AND column_name = 'launch_time_ms'",
    )
    assert launch == []

    await run_alembic(migrated_db, "downgrade", "base")
    assert await _public_tables(migrated_db) == set()
    assert FUNCTIONS.isdisjoint(await _functions(migrated_db))

    await run_alembic(migrated_db, "upgrade", "head")
    assert await _public_tables(migrated_db) == expected
    assert await _scalars(migrated_db, "SELECT version_num FROM alembic_version") == ["0004"]
    assert await _premium_range_checks(migrated_db) == PREMIUM_CHECKS_0004


@pytest.mark.asyncio
async def test_models_match_migrations(migrated_db: URL) -> None:
    """Моделі й міграції не розходяться (те саме, що `uv run alembic check`)."""
    assert await _diff(migrated_db) == []


@pytest.mark.asyncio
async def test_hypertables(migrated_db: URL) -> None:
    rows = await _scalars(migrated_db, "SELECT hypertable_name FROM timescaledb_information.hypertables")
    assert {str(r) for r in rows} == HYPERTABLES


@pytest.mark.asyncio
async def test_candles_compression_after_7_days_and_no_retention(migrated_db: URL) -> None:
    compressed = await _scalars(
        migrated_db,
        "SELECT hypertable_name FROM timescaledb_information.hypertables WHERE compression_enabled",
    )
    assert compressed == ["candles"]
    compress_after = await _scalars(
        migrated_db,
        "SELECT config->>'compress_after' FROM timescaledb_information.jobs"
        " WHERE proc_name = 'policy_compression' AND hypertable_name = 'candles'",
    )
    assert compress_after == [str(7 * DAY_MS)]
    retention = await _scalars(
        migrated_db, "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention'"
    )
    assert retention == [0]


@pytest.mark.asyncio
async def test_integer_now_func_returns_ms(migrated_db: URL) -> None:
    [value] = await _scalars(migrated_db, "SELECT ctb_unix_now_ms() - (extract(epoch FROM now()) * 1000)::bigint")
    assert value == 0


@pytest.mark.asyncio
async def test_append_only_triggers_installed(migrated_db: URL) -> None:
    rows = await _scalars(
        migrated_db,
        "SELECT event_object_table || ':' || event_manipulation FROM information_schema.triggers"
        " WHERE trigger_name LIKE 'trg_%_append_only'",
    )
    assert {str(r) for r in rows} == {f"{t}:{op}" for t in APPEND_ONLY for op in ("UPDATE", "DELETE")}
    truncate = await _scalars(
        migrated_db,
        "SELECT tgrelid::regclass::text FROM pg_trigger WHERE tgname LIKE 'trg_%_no_truncate'",
    )
    assert {str(r) for r in truncate} == APPEND_ONLY | {"orders"}


@pytest.mark.asyncio
async def test_guard_triggers_installed(migrated_db: URL) -> None:
    rows = await _scalars(
        migrated_db,
        "SELECT tgrelid::regclass::text || ':' || tgname FROM pg_trigger"
        " WHERE NOT tgisinternal AND tgname NOT LIKE 'trg_%_append_only' AND tgname NOT LIKE 'trg_%_no_truncate'"
        " AND tgrelid::regclass::text NOT LIKE '_timescaledb%'",
    )
    assert {str(r) for r in rows} >= {
        "orders:trg_orders_guard",
        "bots:trg_bots_guard",
        "risk_config:trg_risk_config_confirmed_audit",
        "emergency_runs:trg_emergency_runs_confirmed_audit",
        "models:trg_models_confirmed_audit",
    }


@pytest.mark.asyncio
async def test_no_truncated_identifiers(migrated_db: URL) -> None:
    """PostgreSQL мовчки обрізає назви до 63 символів — тоді назва в моделі й у БД розходяться."""
    rows = await _scalars(
        migrated_db,
        "SELECT conname FROM pg_constraint WHERE connamespace = 'public'::regnamespace AND length(conname) >= 63"
        " UNION ALL SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND length(indexname) >= 63",
    )
    assert rows == []
