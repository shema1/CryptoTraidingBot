"""Інваріанти схеми на рівні БД (T0.4, ТЗ §6.2, §6.3, §7.2, §15): порушення → `IntegrityError`.

Кожна перевірка порушення виконується в savepoint, тож тест продовжує працювати в тій самій транзакції,
а помилка звіряється з назвою конкретного обмеження (а не «будь-яка помилка»).
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import delete, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from app.api.models import AuditLog
from app.data.models import Candle, DataGap, MarkIndexCandle, PremiumIndex
from app.exchange.models import ExchangeAccount
from app.ml.models import Experiment, ExperimentTrial, HoldoutOpening, ModelArtifact
from app.risk.models import EmergencyRun, LedgerEntry, RiskConfig, SystemState
from app.trading.models import Bot, Fill, Order, OrderEvent, Position, Signal
from tests.db.factories import (
    HOUR_MS,
    SYMBOL,
    T0,
    coid,
    expect_violation,
    fill_values,
    make_account,
    make_audit,
    make_bot,
    make_model,
    make_order,
    make_signal,
    model_values,
    ok,
    order_values,
    position_values,
    signal_values,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ---------------------------------------------------------------- обов'язкові (T0.4 «Готово, коли»)


async def test_duplicate_client_order_id(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    first = coid()
    await make_order(conn, account, "demo", bot, client_order_id=first)
    await expect_violation(
        conn,
        insert(Order).values(order_values(account, "demo", bot, client_order_id=first)),
        "uq_orders_client_order_id",
    )


async def test_duplicate_exec_id(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    order = await make_order(conn, account, "demo", bot)
    await conn.execute(insert(Fill).values(fill_values(order, account, "demo", "exec-1")))
    await expect_violation(conn, insert(Fill).values(fill_values(order, account, "demo", "exec-1")), "uq_fills_exec_id")
    # Ідемпотентна вставка тієї самої події з іншого каналу (WS після REST) нічого не додає.
    result = await conn.execute(
        pg_insert(Fill).values(fill_values(order, account, "demo", "exec-1")).on_conflict_do_nothing()
    )
    assert result.rowcount == 0


async def test_duplicate_signal_per_candle(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    model = await make_model(conn)
    bot = await make_bot(conn, account)
    await make_signal(conn, bot, model, T0)
    await expect_violation(
        conn, insert(Signal).values(signal_values(bot, model, T0)), "uq_signals_bot_id_entry_candle_open_time_ms"
    )
    await make_signal(conn, bot, model, T0 + HOUR_MS)  # наступна свічка — можна


async def test_second_active_bot_per_symbol(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    await make_bot(conn, account, status="STOPPED")
    await make_bot(conn, account, status="STOPPED")  # зупинених може бути скільки завгодно
    await make_bot(conn, account, status="STARTING")
    stmt = insert(Bot).values(account_id=account, mode="demo", symbol=SYMBOL, timeframe="1h", status="ERROR")
    await expect_violation(conn, stmt, "uq_bots_account_id_symbol_active")
    other = await make_account(conn)
    await make_bot(conn, other, status="STARTING")  # той самий символ на іншому акаунті — можна


async def test_bot_cannot_be_activated_while_another_is_active(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    await make_bot(conn, account, status="RUNNING")
    stopped = await make_bot(conn, account, status="STOPPED")
    await expect_violation(
        conn, update(Bot).where(Bot.id == stopped).values(status="STARTING"), "uq_bots_account_id_symbol_active"
    )


# ---------------------------------------------------------------- боти


@pytest.mark.parametrize(
    ("status", "reasons", "constraint"),
    [
        ("PAUSED", [], "ck_bots_paused_has_reason"),
        ("RUNNING", ["MANUAL"], "ck_bots_running_has_no_reason"),
        ("PAUSED", ["NOT_A_REASON"], "ck_bots_pause_reasons_known"),
        ("SLEEPING", [], "ck_bots_status"),
    ],
)
async def test_bot_status_and_pause_reasons(
    conn: AsyncConnection, status: str, reasons: list[str], constraint: str
) -> None:
    account = await make_account(conn)
    stmt = insert(Bot).values(
        account_id=account, mode="demo", symbol=SYMBOL, timeframe="1h", status=status, pause_reasons=reasons
    )
    await expect_violation(conn, stmt, constraint)


async def test_paused_bot_with_reasons_is_valid(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account, status="PAUSED", pause_reasons=["DAILY_LOSS", "DRIFT"])
    reasons = await ok(conn, select(Bot.pause_reasons).where(Bot.id == bot))
    assert reasons == ["DAILY_LOSS", "DRIFT"]


async def test_bot_mode_must_match_account(conn: AsyncConnection) -> None:
    account = await make_account(conn, mode="demo")
    stmt = insert(Bot).values(account_id=account, mode="live", symbol=SYMBOL, timeframe="1h")
    await expect_violation(conn, stmt, "fk_bots_account_id_mode_exchange_accounts")


async def test_pending_model_must_differ(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    model = await make_model(conn)
    stmt = insert(Bot).values(
        account_id=account, mode="demo", symbol=SYMBOL, timeframe="1h", model_version=model, pending_model_version=model
    )
    await expect_violation(conn, stmt, "ck_bots_pending_model_differs")


# ---------------------------------------------------------------- акаунти


async def test_account_key_set_and_host_id(conn: AsyncConnection) -> None:
    await conn.execute(
        insert(ExchangeAccount).values(
            name="a1", exchange="bybit", mode="demo", env_key_set="BYBIT_DEMO", allowed_host_id="m1"
        )
    )
    same_keys = insert(ExchangeAccount).values(
        name="a2", exchange="bybit", mode="demo", env_key_set="BYBIT_DEMO", allowed_host_id="m1"
    )
    await expect_violation(conn, same_keys, "uq_exchange_accounts_env_key_set")
    bad_host = insert(ExchangeAccount).values(
        name="a3", exchange="bybit", mode="demo", env_key_set="BYBIT_X", allowed_host_id="MAC"
    )
    await expect_violation(conn, bad_host, "ck_exchange_accounts_allowed_host_id_format")
    no_time = insert(ExchangeAccount).values(
        name="a4", exchange="bybit", mode="demo", env_key_set="BYBIT_Y", allowed_host_id="m1", key_check_result="OK"
    )
    await expect_violation(conn, no_time, "ck_exchange_accounts_key_check_result_with_time")


# ---------------------------------------------------------------- сигнали


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"features_candle_open_time_ms": T0}, "ck_signals_features_before_entry"),
        ({"computed_at_ms": T0 - 1001}, "ck_signals_computed_after_entry_open"),
        ({"probability": Decimal("1.01")}, "ck_signals_probability_range"),
        ({"primary_signal": "NONE"}, "ck_signals_primary_signal"),
        ({"status": "PENDING"}, "ck_signals_status"),
    ],
)
async def test_signal_checks(conn: AsyncConnection, overrides: dict[str, Any], constraint: str) -> None:
    account = await make_account(conn)
    model = await make_model(conn)
    bot = await make_bot(conn, account)
    await expect_violation(conn, insert(Signal).values(signal_values(bot, model, **overrides)), constraint)


# ---------------------------------------------------------------- ордери


async def test_one_entry_order_per_signal(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    model = await make_model(conn)
    bot = await make_bot(conn, account)
    sig = await make_signal(conn, bot, model)
    await make_order(conn, account, "demo", bot, signal_id=sig)
    await expect_violation(
        conn, insert(Order).values(order_values(account, "demo", bot, signal_id=sig)), "uq_orders_signal_id_entry"
    )
    # Виходи (reduceOnly) з тим самим сигналом — нові ордери з новим clientOrderId (ТЗ §7.2).
    await make_order(conn, account, "demo", bot, signal_id=sig, reduce_only=True, side="SELL")
    await make_order(conn, account, "demo", bot, signal_id=sig, reduce_only=True, side="SELL")


async def test_order_signal_must_belong_to_same_bot(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    model = await make_model(conn)
    bot_a = await make_bot(conn, account, symbol="BTC/USDT:USDT")
    bot_b = await make_bot(conn, account, symbol="ETH/USDT:USDT")
    sig = await make_signal(conn, bot_a, model)
    await expect_violation(
        conn,
        insert(Order).values(order_values(account, "demo", bot_b, signal_id=sig)),
        "fk_orders_signal_id_bot_id_signals",
    )


@pytest.mark.parametrize(
    "client_order_id",
    [
        "tbaa" + uuid.uuid4().hex[:31],  # 35 символів
        "tbaa" + uuid.uuid4().hex + "0",  # 37 символів
        "xxaa" + uuid.uuid4().hex,  # не наш префікс
        "tbAA" + uuid.uuid4().hex,  # HOST_ID лише [a-z0-9]
        "tbaa" + str(uuid.uuid4()),  # з дефісами
    ],
)
async def test_client_order_id_format(conn: AsyncConnection, client_order_id: str) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    await expect_violation(
        conn,
        insert(Order).values(order_values(account, "demo", bot, client_order_id=client_order_id)),
        "ck_orders_client_order_id_format",
    )


async def test_order_mode_and_account_must_match_bot(conn: AsyncConnection) -> None:
    demo = await make_account(conn, mode="demo")
    testnet = await make_account(conn, mode="testnet")
    bot = await make_bot(conn, demo)
    await expect_violation(
        conn, insert(Order).values(order_values(demo, "testnet", bot)), "fk_orders_account_id_mode_exchange_accounts"
    )
    await expect_violation(
        conn, insert(Order).values(order_values(testnet, "testnet", bot)), "fk_orders_bot_id_account_id_mode_bots"
    )


@pytest.mark.parametrize("mode", ["demo", "testnet", "live"])
async def test_bot_entry_order_requires_signal_in_every_mode(conn: AsyncConnection, mode: str) -> None:
    account = await make_account(conn, mode=mode)
    bot = await make_bot(conn, account, mode=mode)
    await expect_violation(
        conn, insert(Order).values(order_values(account, mode, bot, origin="BOT")), "ck_orders_bot_entry_has_signal"
    )
    # Вихід (reduceOnly) без сигналу — можна в будь-якому режимі.
    await make_order(conn, account, mode, bot, origin="BOT", reduce_only=True, side="SELL")


async def test_manual_test_orders_not_in_live(conn: AsyncConnection) -> None:
    live = await make_account(conn, mode="live")
    live_bot = await make_bot(conn, live, mode="live")
    await expect_violation(
        conn, insert(Order).values(order_values(live, "live", live_bot)), "ck_orders_manual_test_not_live"
    )
    demo = await make_account(conn, mode="demo")
    await make_order(conn, demo, "demo", await make_bot(conn, demo))  # [Test Long] у Demo — можна


async def test_order_without_bot_only_when_imported(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    await expect_violation(conn, insert(Order).values(order_values(account, "demo", None)), "ck_orders_bot_required")
    await make_order(conn, account, "demo", None, origin="ORPHAN", state="OPEN", exchange_order_id="ex-1")


@pytest.mark.parametrize("origin", ["ORPHAN", "EXCHANGE"])
@pytest.mark.parametrize(
    "overrides",
    [
        {"state": "SUBMITTING", "exchange_order_id": "ex-1"},
        {"state": "CREATED", "exchange_order_id": "ex-1"},
        {"state": "OPEN", "exchange_order_id": None},
    ],
)
async def test_imported_orders_cannot_be_submitted(
    conn: AsyncConnection, origin: str, overrides: dict[str, Any]
) -> None:
    """Імпортований/біржовий ордер не можна «відправити» в обхід циклу бота, сигналу й RiskGate."""
    account = await make_account(conn)
    stmt = insert(Order).values(order_values(account, "demo", None, origin=origin, **overrides))
    await expect_violation(conn, stmt, "ck_orders_imported_not_submittable")


async def test_imported_order_cannot_carry_signal(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    model = await make_model(conn)
    bot = await make_bot(conn, account)
    sig = await make_signal(conn, bot, model)
    stmt = insert(Order).values(
        order_values(account, "demo", bot, origin="ORPHAN", state="OPEN", exchange_order_id="ex-2", signal_id=sig)
    )
    await expect_violation(conn, stmt, "ck_orders_imported_not_submittable")


async def test_exchange_created_order_and_its_fill(conn: AsyncConnection) -> None:
    """Спрацювання SL позиції створює ордер біржі без нашого clientOrderId — його fill усе одно записується."""
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    without_id = order_values(account, "demo", bot, origin="BOT", reduce_only=True, side="SELL", client_order_id=None)
    await expect_violation(conn, insert(Order).values(without_id), "ck_orders_client_order_id_required")
    order = await make_order(
        conn,
        account,
        "demo",
        bot,
        origin="EXCHANGE",
        client_order_id=None,
        exchange_order_id="sl-1",
        reduce_only=True,
        side="SELL",
        state="FILLED",
        filled_qty=Decimal("0.010"),
    )
    fill = fill_values(order, account, "demo", "exec-sl-1") | {"side": "SELL"}
    await conn.execute(insert(Fill).values(fill))


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"qty": Decimal(0)}, "ck_orders_qty_positive"),
        ({"filled_qty": Decimal("0.011")}, "ck_orders_filled_qty_range"),
        ({"order_type": "LIMIT"}, "ck_orders_limit_has_price"),
        ({"state": "DONE"}, "ck_orders_state"),
    ],
)
async def test_order_checks(conn: AsyncConnection, overrides: dict[str, Any], constraint: str) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    await expect_violation(conn, insert(Order).values(order_values(account, "demo", bot, **overrides)), constraint)


async def test_fill_mode_must_match_order(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    order = await make_order(conn, account, "demo", bot)
    await expect_violation(
        conn,
        insert(Fill).values(fill_values(order, account, "testnet", "exec-x")),
        "fk_fills_order",
    )


# ---------------------------------------------------------------- позиції


async def test_one_open_position_per_symbol(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    await conn.execute(insert(Position).values(position_values(bot, account)))
    await expect_violation(
        conn, insert(Position).values(position_values(bot, account)), "uq_positions_account_id_symbol_open"
    )
    closed = position_values(bot, account, status="CLOSED", closed_at_ms=T0 + HOUR_MS)
    await conn.execute(insert(Position).values(closed))  # закритих — скільки завгодно


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"protection_status": None}, "ck_positions_open_has_protection_status"),
        ({"protection_status": "ENTRY_FILLED_UNPROTECTED"}, "ck_positions_unprotected_has_since"),
        ({"status": "CLOSED"}, "ck_positions_closed_iff_not_open"),
        ({"closed_at_ms": T0 + 1}, "ck_positions_closed_iff_not_open"),
        ({"qty": Decimal(0)}, "ck_positions_open_has_qty"),
    ],
)
async def test_position_checks(conn: AsyncConnection, overrides: dict[str, Any], constraint: str) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    await expect_violation(conn, insert(Position).values(position_values(bot, account, **overrides)), constraint)


# ---------------------------------------------------------------- append-only


async def _append_only_rows(conn: AsyncConnection) -> dict[str, tuple[Any, Any]]:
    """Рядок у кожній append-only таблиці: {таблиця: (модель, id)}."""
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    order = await make_order(conn, account, "demo", bot)
    audit = await make_audit(conn)
    event = await ok(
        conn,
        insert(OrderEvent).values(order_id=order, to_state="CREATED", source="LOCAL").returning(OrderEvent.id),
    )
    ledger = await ok(
        conn,
        insert(LedgerEntry)
        .values(account_id=account, event_id="tx-1", kind="FEE", amount=Decimal("-0.1"), currency="USDT", ts_ms=T0)
        .returning(LedgerEntry.id),
    )
    await conn.execute(
        insert(Experiment).values(
            id="exp-0001",
            research_line=SYMBOL,
            symbols=[SYMBOL],
            timeframe="1h",
            label_type="meta",
            primary_strategy={"name": "ema_cross"},
            params={},
            seed=42,
            data_from_ms=T0,
            data_to_ms=T0 + HOUR_MS,
        )
    )
    trial = await ok(
        conn,
        insert(ExperimentTrial)
        .values(experiment_id="exp-0001", trial_no=1, state="COMPLETE", params={}, oos_results=[0.1], metrics={})
        .returning(ExperimentTrial.id),
    )
    risk = await ok(
        conn,
        insert(RiskConfig).values(account_id=account, version=1, params={}, loosening=False).returning(RiskConfig.id),
    )
    return {
        "order_events": (OrderEvent, event),
        "audit_log": (AuditLog, audit),
        "ledger": (LedgerEntry, ledger),
        "experiment_trials": (ExperimentTrial, trial),
        "risk_config": (RiskConfig, risk),
    }


APPEND_ONLY_UPDATE_COLUMN = {
    "order_events": "reason",
    "audit_log": "reason",
    "ledger": "details",
    "experiment_trials": "metrics",
    "risk_config": "confirmed_audit_id",
}


@pytest.mark.parametrize("table", sorted(APPEND_ONLY_UPDATE_COLUMN))
async def test_append_only_tables_reject_update_delete_truncate(conn: AsyncConnection, table: str) -> None:
    rows = await _append_only_rows(conn)
    model, row_id = rows[table]
    # Колонку id (IDENTITY ALWAYS) оновити не можна взагалі, тому змінюємо звичайну колонку.
    column = APPEND_ONLY_UPDATE_COLUMN[table]
    await expect_violation(conn, update(model).where(model.id == row_id).values({column: None}), "append-only")
    await expect_violation(conn, delete(model).where(model.id == row_id), "append-only")
    await expect_violation(conn, text(f"TRUNCATE {table} CASCADE"), "append-only")
    assert await ok(conn, select(func.count()).select_from(model).where(model.id == row_id)) == 1


# ---------------------------------------------------------------- ML


async def test_holdout_opened_at_most_twice(conn: AsyncConnection) -> None:
    model = await make_model(conn)
    await conn.execute(
        insert(Experiment).values(
            id="exp-h",
            research_line=SYMBOL,
            symbols=[SYMBOL],
            timeframe="1h",
            label_type="meta",
            primary_strategy={},
            params={},
            seed=1,
            data_from_ms=T0,
            data_to_ms=T0 + HOUR_MS,
        )
    )
    window = {"research_line": SYMBOL, "holdout_from_ms": T0, "holdout_to_ms": T0 + 100 * HOUR_MS}
    common = {"experiment_id": "exp-h", "model_version": model, **window}
    await conn.execute(insert(HoldoutOpening).values(opening_no=1, **common))
    await conn.execute(insert(HoldoutOpening).values(opening_no=2, **common))
    await expect_violation(
        conn, insert(HoldoutOpening).values(opening_no=3, **common), "ck_holdout_openings_opening_no_max_2"
    )
    await expect_violation(
        conn,
        insert(HoldoutOpening).values(opening_no=2, **common),
        "uq_holdout_openings_window_opening_no",
    )


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"git_dirty": True, "status": "CANDIDATE", "live_corridor": {}}, "ck_models_dirty_not_promoted"),
        ({"status": "CANDIDATE"}, "ck_models_promoted_has_corridor"),
        ({"status": "APPROVED", "live_corridor": {}}, "ck_models_approved_by_human"),
        ({"label_type": "rule_only"}, "ck_models_artifact_path_by_label_type"),
        ({"artifact_path": None}, "ck_models_artifact_path_by_label_type"),
    ],
)
async def test_model_status_rules(conn: AsyncConnection, overrides: dict[str, Any], constraint: str) -> None:
    await expect_violation(conn, insert(ModelArtifact).values(model_values("m-bad", **overrides)), constraint)


async def test_model_approved_with_audit_is_valid(conn: AsyncConnection) -> None:
    version = "m-approved"
    audit = await make_audit(conn, "model.approve", target=version)
    values = model_values(version, status="APPROVED", live_corridor={"n_trades": [10]}, confirmed_audit_id=audit)
    await conn.execute(insert(ModelArtifact).values(values))
    assert await ok(conn, select(ModelArtifact.status).where(ModelArtifact.model_version == version)) == "APPROVED"


# ---------------------------------------------------------------- ризик і Emergency Stop


async def test_risk_config_loosening_requires_audit(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    await expect_violation(
        conn,
        insert(RiskConfig).values(account_id=account, version=1, params={}, loosening=True),
        "ck_risk_config_loosening_confirmed",
    )
    audit = await make_audit(conn, "risk.config.loosen", account_id=account)
    await conn.execute(
        insert(RiskConfig).values(account_id=account, version=1, params={}, loosening=True, confirmed_audit_id=audit)
    )
    await expect_violation(
        conn,
        insert(RiskConfig).values(account_id=account, version=1, params={}, loosening=False),
        "uq_risk_config_account_id_version",
    )


async def test_one_unreleased_emergency_run_per_account(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    await conn.execute(insert(EmergencyRun).values(account_id=account, trigger="UI"))
    await expect_violation(
        conn,
        insert(EmergencyRun).values(account_id=account, trigger="RISK_HALTED"),
        "uq_emergency_runs_account_id_active",
    )
    other = await make_account(conn)
    await conn.execute(insert(EmergencyRun).values(account_id=other, trigger="UI"))  # інший акаунт — незалежно


async def test_emergency_release_requires_audit(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    stmt = insert(EmergencyRun).values(
        account_id=account, trigger="UI", status="RELEASED", completed_at_ms=T0, released_at_ms=T0
    )
    await expect_violation(conn, stmt, "ck_emergency_runs_released_by_human")


async def test_system_state_emergency_run_of_same_account(conn: AsyncConnection) -> None:
    account_a = await make_account(conn)
    account_b = await make_account(conn)
    run_b = await ok(conn, insert(EmergencyRun).values(account_id=account_b, trigger="UI").returning(EmergencyRun.id))
    await expect_violation(
        conn,
        insert(SystemState).values(account_id=account_a, emergency_run_id=run_b),
        "fk_system_state_emergency_run_id_account_id_emergency_runs",
    )
    await conn.execute(insert(SystemState).values(account_id=account_b, emergency_run_id=run_b, risk_status="HALTED"))


async def test_ledger_dedup_and_sign(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    entry = {"account_id": account, "event_id": "tx-9", "currency": "USDT", "ts_ms": T0}
    await conn.execute(insert(LedgerEntry).values(kind="DEPOSIT", amount=Decimal(100), **entry))
    # Одна транзакція біржі може дати кілька рядків різних видів.
    await conn.execute(insert(LedgerEntry).values(kind="FEE", amount=Decimal("-0.1"), **entry))
    await expect_violation(
        conn,
        insert(LedgerEntry).values(kind="DEPOSIT", amount=Decimal(100), **entry),
        "uq_ledger_account_id_event_id_kind",
    )
    other = {**entry, "event_id": "tx-10"}
    await expect_violation(
        conn, insert(LedgerEntry).values(kind="DEPOSIT", amount=Decimal(-1), **other), "ck_ledger_deposit_positive"
    )
    await expect_violation(
        conn, insert(LedgerEntry).values(kind="WITHDRAWAL", amount=Decimal(1), **other), "ck_ledger_withdrawal_negative"
    )


# ---------------------------------------------------------------- ринкові дані


def candle_values(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "exchange": "bybit",
        "symbol": SYMBOL,
        "timeframe": "1h",
        "open_time_ms": T0,
        "open": Decimal(100),
        "high": Decimal(110),
        "low": Decimal(95),
        "close": Decimal(105),
        "volume": Decimal(12),
    }
    values.update(overrides)
    return values


async def test_candles_insert_is_idempotent(conn: AsyncConnection) -> None:
    first = await conn.execute(pg_insert(Candle).values(candle_values()).on_conflict_do_nothing())
    again = await conn.execute(pg_insert(Candle).values(candle_values(close=Decimal(99))).on_conflict_do_nothing())
    assert (first.rowcount, again.rowcount) == (1, 0)
    assert await ok(conn, select(Candle.close).where(Candle.open_time_ms == T0)) == Decimal(105)
    await expect_violation(conn, insert(Candle).values(candle_values()), "pk_candles")


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"is_closed": False}, "ck_candles_only_closed"),
        ({"high": Decimal(104)}, "ck_candles_high_ge_open_close"),
        ({"low": Decimal(101)}, "ck_candles_low_le_open_close"),
        ({"volume": Decimal(-1)}, "ck_candles_volume_non_negative"),
        ({"timeframe": "hour"}, "ck_candles_timeframe_format"),
    ],
)
async def test_candle_checks(conn: AsyncConnection, overrides: dict[str, Any], constraint: str) -> None:
    await expect_violation(conn, insert(Candle).values(candle_values(**overrides)), constraint)


async def test_mark_and_index_candles_coexist(conn: AsyncConnection) -> None:
    base = {
        "exchange": "bybit",
        "symbol": SYMBOL,
        "interval": "1h",
        "ts_ms": T0,
        "open": Decimal(100),
        "high": Decimal(101),
        "low": Decimal(99),
        "close": Decimal(100),
    }
    await conn.execute(insert(MarkIndexCandle).values(price_type="mark", **base))
    await conn.execute(insert(MarkIndexCandle).values(price_type="index", **base))
    await expect_violation(conn, insert(MarkIndexCandle).values(price_type="mark", **base), "pk_mark_index_candles")


def premium_values(**overrides: Any) -> dict[str, Any]:
    # Testnet BTCUSDT 2024-06-01 00:00 UTC: open — close попередньої години, нижче за low цієї (0004, T1.9).
    values: dict[str, Any] = {
        "exchange": "bybit",
        "symbol": SYMBOL,
        "interval": "1h",
        "ts_ms": T0,
        "open": Decimal("0.00046096"),
        "high": Decimal("0.00052523"),
        "low": Decimal("0.00048958"),
        "close": Decimal("0.00048958"),
    }
    values.update(overrides)
    return values


async def test_premium_open_may_lie_outside_the_hour_range(conn: AsyncConnection) -> None:
    await conn.execute(insert(PremiumIndex).values(premium_values()))
    await conn.execute(insert(PremiumIndex).values(premium_values(ts_ms=T0 + HOUR_MS, open=Decimal("0.001"))))
    assert await ok(conn, select(func.count()).select_from(PremiumIndex)) == 2


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"close": Decimal("0.0006")}, "ck_premium_index_high_ge_close"),
        ({"close": Decimal("0.0004")}, "ck_premium_index_low_le_close"),
    ],
)
async def test_premium_checks(conn: AsyncConnection, overrides: dict[str, Any], constraint: str) -> None:
    await expect_violation(conn, insert(PremiumIndex).values(premium_values(**overrides)), constraint)


async def test_data_gap_is_recorded_once(conn: AsyncConnection) -> None:
    gap = {"source": "candles", "exchange": "bybit", "symbol": SYMBOL, "timeframe": "1h", "from_ms": T0}
    await conn.execute(insert(DataGap).values(to_ms=T0 + HOUR_MS, **gap))
    await expect_violation(
        conn,
        insert(DataGap).values(to_ms=T0 + 2 * HOUR_MS, **gap),
        "uq_data_gaps_open",
    )
    await expect_violation(
        conn, insert(DataGap).values(**{**gap, "from_ms": T0 + 5, "to_ms": T0 + 5}), "ck_data_gaps_range_valid"
    )


async def test_backfill_into_compressed_chunk_is_idempotent(conn: AsyncConnection) -> None:
    """Backfill старої історії потрапляє в уже стиснуті chunk-и: дубль не вставляється, нова свічка — так."""
    old = T0 - 400 * 24 * HOUR_MS  # далеко за межею compression (7 днів)
    await conn.execute(insert(Candle).values(candle_values(open_time_ms=old)))
    chunks = await conn.execute(text("SELECT compress_chunk(c) FROM show_chunks('candles') c"))
    assert len(chunks.all()) >= 1
    [compressed] = (
        await conn.execute(
            text(
                "SELECT count(*) FROM timescaledb_information.chunks"
                " WHERE hypertable_name = 'candles' AND is_compressed"
            )
        )
    ).scalars()
    assert compressed >= 1
    dup = await conn.execute(pg_insert(Candle).values(candle_values(open_time_ms=old)).on_conflict_do_nothing())
    new = await conn.execute(
        pg_insert(Candle).values(candle_values(open_time_ms=old + HOUR_MS)).on_conflict_do_nothing()
    )
    assert (dup.rowcount, new.rowcount) == (0, 1)
    count = await ok(
        conn, select(func.count()).select_from(Candle).where(Candle.open_time_ms.in_([old, old + HOUR_MS]))
    )
    assert count == 2
