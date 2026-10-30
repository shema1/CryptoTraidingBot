"""Тригери й складені обмеження схеми (T0.4, рев'ю `trading-safety-reviewer`): що БД забороняє і що лишає
дозволеним, щоб захист, пізні fills і звірка ніколи не блокувалися (AGENTS.md, інваріант 6)."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from app.api.models import AUDIT_ACTION_EMERGENCY_RELEASE, AUDIT_ACTION_MODEL_APPROVE, AUDIT_ACTION_RISK_LOOSEN
from app.data.models import Candle, DataGap
from app.exchange.models import Instrument
from app.jobs.models import Job
from app.ml.models import Experiment, ExperimentTrial, ModelArtifact
from app.risk.models import EmergencyRun, EmergencyStep, EquitySnapshot, LedgerEntry, RiskConfig, RiskEvent
from app.trading.models import Bot, DecisionLog, Fill, Order, Position, Signal
from tests.db.factories import (
    HOUR_MS,
    SYMBOL,
    T0,
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


async def _bot_order(conn: AsyncConnection, **overrides: Any) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    """(account, bot, order) — вхідний ордер бота з сигналом."""
    account = await make_account(conn)
    model = await make_model(conn)
    bot = await make_bot(conn, account)
    sig = await make_signal(conn, bot, model)
    order = await make_order(conn, account, "demo", bot, signal_id=sig, **overrides)
    return account, bot, order


# ---------------------------------------------------------------- orders: незмінність і термінальні стани


@pytest.mark.parametrize(
    "values",
    [
        {"reduce_only": True},
        {"side": "SELL"},
        {"qty": Decimal("0.020")},
        {"signal_id": None},
        {"client_order_id": "tbaa" + "0" * 32},
        {"symbol": "ETH/USDT:USDT"},
        {"origin": "MANUAL_TEST"},
    ],
)
async def test_order_identity_is_immutable(conn: AsyncConnection, values: dict[str, Any]) -> None:
    _, _, order = await _bot_order(conn)
    await expect_violation(conn, update(Order).where(Order.id == order).values(values), "order identity is immutable")


async def test_rejected_entry_cannot_be_reused_or_resubmitted(conn: AsyncConnection) -> None:
    """Вхідний ордер після REJECTED не повторюється (ТЗ §7.2): ні новим станом, ні «перефарбуванням» у вихід."""
    account, bot, order = await _bot_order(conn, state="REJECTED")
    await expect_violation(
        conn, update(Order).where(Order.id == order).values(state="SUBMITTING"), "no transitions from terminal state"
    )
    await expect_violation(
        conn, update(Order).where(Order.id == order).values(reduce_only=True), "order identity is immutable"
    )
    sig = await ok(conn, select(Order.signal_id).where(Order.id == order))
    await expect_violation(
        conn, insert(Order).values(order_values(account, "demo", bot, signal_id=sig)), "uq_orders_signal_id_entry"
    )


@pytest.mark.parametrize("terminal", ["FILLED", "CANCELED", "REJECTED", "EXPIRED"])
async def test_no_transitions_from_terminal_states(conn: AsyncConnection, terminal: str) -> None:
    _, _, order = await _bot_order(conn, state=terminal)
    await expect_violation(
        conn, update(Order).where(Order.id == order).values(state="OPEN"), "no transitions from terminal state"
    )


async def test_orders_cannot_be_deleted_or_truncated(conn: AsyncConnection) -> None:
    _, _, order = await _bot_order(conn)
    await expect_violation(conn, delete(Order).where(Order.id == order), "orders: DELETE is forbidden")
    await expect_violation(conn, text("TRUNCATE orders CASCADE"), "orders: TRUNCATE is forbidden")


async def test_order_allowed_updates(conn: AsyncConnection) -> None:
    """Пізні fills, звірка й легальні переходи не блокуються."""
    account, _, order = await _bot_order(conn, state="PENDING_CANCEL", filled_qty=Decimal("0.004"))
    # PENDING_CANCEL → FILLED: ордер встиг виконатися до скасування (ТЗ §7.1).
    await conn.execute(update(Order).where(Order.id == order).values(state="FILLED", filled_qty=Decimal("0.010")))
    # exchange_order_id: NULL → значення (відповідь REST після WS).
    await conn.execute(update(Order).where(Order.id == order).values(exchange_order_id="ex-9", version=2))
    # Fill на термінальний ордер, що пізно з'явився через REST.
    await conn.execute(insert(Fill).values(fill_values(order, account, "demo", "exec-late")))
    await expect_violation(
        conn, update(Order).where(Order.id == order).values(exchange_order_id="ex-10"), "order identity is immutable"
    )


async def test_canceled_with_partial_fill_and_late_fill(conn: AsyncConnection) -> None:
    account, _, order = await _bot_order(conn, state="CANCELED", filled_qty=Decimal("0.004"))
    await conn.execute(update(Order).where(Order.id == order).values(filled_qty=Decimal("0.006")))
    await conn.execute(insert(Fill).values(fill_values(order, account, "demo", "exec-c1")))


async def test_fill_while_order_is_submitting(conn: AsyncConnection) -> None:
    """Fill з WS може прийти раніше за відповідь REST, поки ордер ще SUBMITTING."""
    account, _, order = await _bot_order(conn, state="SUBMITTING")
    await conn.execute(insert(Fill).values(fill_values(order, account, "demo", "exec-early")))
    await conn.execute(update(Order).where(Order.id == order).values(state="FILLED", filled_qty=Decimal("0.010")))


async def test_orphan_order_can_be_assigned_to_bot_once(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    other = await make_bot(conn, account, symbol="ETH/USDT:USDT")
    order = await make_order(conn, account, "demo", None, origin="ORPHAN", state="OPEN", exchange_order_id="ex-o")
    await conn.execute(update(Order).where(Order.id == order).values(bot_id=bot))
    await expect_violation(
        conn, update(Order).where(Order.id == order).values(bot_id=other), "order identity is immutable"
    )


async def test_fill_must_match_order_symbol_and_side(conn: AsyncConnection) -> None:
    account, _, order = await _bot_order(conn)
    for wrong in ({"side": "SELL"}, {"symbol": "ETH/USDT:USDT"}):
        await expect_violation(
            conn, insert(Fill).values(fill_values(order, account, "demo", f"exec-{wrong}") | wrong), "fk_fills_order"
        )


async def test_duplicate_exchange_order_id_per_account(conn: AsyncConnection) -> None:
    account, bot, _ = await _bot_order(conn, exchange_order_id="ex-dup")
    await expect_violation(
        conn,
        insert(Order).values(
            order_values(account, "demo", bot, reduce_only=True, side="SELL", exchange_order_id="ex-dup")
        ),
        "uq_orders_account_id_exchange_order_id",
    )
    other = await make_account(conn)
    await make_order(conn, other, "demo", await make_bot(conn, other), exchange_order_id="ex-dup")


async def test_order_signal_requires_bot(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    model = await make_model(conn)
    sig = await make_signal(conn, await make_bot(conn, account), model)
    stmt = insert(Order).values(order_values(account, "demo", None, origin="BOT", signal_id=sig))
    # Сигнал без бота закриває й `ck_orders_signal_requires_bot` (запас на нові `origin`), і `ck_orders_bot_required`;
    # котре з них PostgreSQL назве першим, не визначено.
    with pytest.raises(IntegrityError) as excinfo:
        async with conn.begin_nested():
            await conn.execute(stmt)
    message = str(excinfo.value.orig)
    assert "ck_orders_signal_requires_bot" in message or "ck_orders_bot_required" in message, message


async def test_order_position_must_be_same_account(conn: AsyncConnection) -> None:
    account_a = await make_account(conn)
    bot_a = await make_bot(conn, account_a)
    position = await ok(conn, insert(Position).values(position_values(bot_a, account_a)).returning(Position.id))
    account_b = await make_account(conn)
    bot_b = await make_bot(conn, account_b)
    stmt = insert(Order).values(order_values(account_b, "demo", bot_b, reduce_only=True, position_id=position))
    await expect_violation(conn, stmt, "fk_orders_position_id_account_id_mode_positions")


# ---------------------------------------------------------------- bots


async def test_bot_cannot_stop_with_open_position(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account, status="RUNNING")
    position = await ok(conn, insert(Position).values(position_values(bot, account)).returning(Position.id))
    await expect_violation(conn, update(Bot).where(Bot.id == bot).values(status="STOPPED"), "cannot stop bot")
    await conn.execute(
        update(Position).where(Position.id == position).values(status="CLOSED", closed_at_ms=T0 + HOUR_MS)
    )
    await conn.execute(update(Bot).where(Bot.id == bot).values(status="STOPPED"))


async def test_bot_cannot_stop_with_active_orders(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account, status="RUNNING")
    order = await make_order(conn, account, "demo", bot, state="SUBMISSION_UNKNOWN")
    await expect_violation(conn, update(Bot).where(Bot.id == bot).values(status="STOPPED"), "non-terminal orders")
    await conn.execute(update(Order).where(Order.id == order).values(state="REJECTED"))
    await conn.execute(update(Bot).where(Bot.id == bot).values(status="STOPPED"))


@pytest.mark.parametrize("values", [{"symbol": "ETH/USDT:USDT"}, {"mode": "testnet"}])
async def test_bot_identity_is_immutable(conn: AsyncConnection, values: dict[str, Any]) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    await expect_violation(conn, update(Bot).where(Bot.id == bot).values(values), "are immutable")


async def test_bot_cannot_move_to_another_account(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    other = await make_account(conn)
    bot = await make_bot(conn, account)
    await expect_violation(conn, update(Bot).where(Bot.id == bot).values(account_id=other), "are immutable")


@pytest.mark.parametrize("status", ["RECONCILING", "ERROR", "EMERGENCY_STOP", "STOPPED"])
async def test_non_running_statuses_keep_pause_reasons(conn: AsyncConnection, status: str) -> None:
    """Причини паузи переживають RECONCILING/ERROR/EMERGENCY_STOP (вихід з паузи — лише коли знято всі)."""
    account = await make_account(conn)
    await make_bot(conn, account, status=status, pause_reasons=["DRIFT"])


async def test_position_bot_must_be_same_account(conn: AsyncConnection) -> None:
    account_a = await make_account(conn)
    account_b = await make_account(conn)
    bot_a = await make_bot(conn, account_a)
    await expect_violation(
        conn, insert(Position).values(position_values(bot_a, account_b)), "fk_positions_bot_id_account_id_mode_bots"
    )


async def test_position_signal_must_belong_to_bot(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    model = await make_model(conn)
    bot_a = await make_bot(conn, account)
    bot_b = await make_bot(conn, account, symbol="ETH/USDT:USDT")
    sig = await make_signal(conn, bot_a, model)
    stmt = insert(Position).values(position_values(bot_b, account, symbol="ETH/USDT:USDT", signal_id=sig))
    await expect_violation(conn, stmt, "fk_positions_signal_id_bot_id_signals")


async def test_signal_tolerates_clock_skew_under_1s(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    model = await make_model(conn)
    bot = await make_bot(conn, account)
    await conn.execute(insert(Signal).values(signal_values(bot, model, computed_at_ms=T0 - 1000)))


# ---------------------------------------------------------------- символи


@pytest.mark.parametrize("symbol", ["BTCUSDT", "btc/usdt:usdt", "BTC/USDT", "BTC-USDT"])
async def test_symbol_must_be_canonical(conn: AsyncConnection, symbol: str) -> None:
    account = await make_account(conn)
    await expect_violation(
        conn,
        insert(Bot).values(account_id=account, mode="demo", symbol=symbol, timeframe="1h"),
        "ck_bots_symbol_format",
    )
    candle = {"exchange": "bybit", "symbol": symbol, "timeframe": "1h", "open_time_ms": T0}
    prices = {"open": 1, "high": 1, "low": 1, "close": 1, "volume": 0}
    await expect_violation(conn, insert(Candle).values(**candle, **prices), "ck_candles_symbol_format")
    instrument = {
        "exchange": "bybit",
        "mode": "demo",
        "symbol": symbol,
        "tick_size": Decimal("0.1"),
        "qty_step": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "min_notional": Decimal(5),
        "max_leverage": Decimal(100),
        "launch_time_ms": T0,
        "status": "Trading",
        "updated_at_ms": T0,
    }
    await expect_violation(conn, insert(Instrument).values(instrument), "ck_instruments_symbol_format")


async def test_instrument_launch_time_cannot_be_negative(conn: AsyncConnection) -> None:
    instrument = {
        "exchange": "bybit",
        "mode": "demo",
        "symbol": SYMBOL,
        "tick_size": Decimal("0.1"),
        "qty_step": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "min_notional": Decimal(5),
        "max_leverage": Decimal(100),
        "launch_time_ms": -1,
        "status": "Trading",
        "updated_at_ms": T0,
    }
    await expect_violation(conn, insert(Instrument).values(instrument), "ck_instruments_launch_time_ms_non_negative")


async def test_numeric_prefixed_symbol_is_valid(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    await make_bot(conn, account, symbol="1000PEPE/USDT:USDT")


# ---------------------------------------------------------------- прогалини


async def test_mark_and_index_gaps_are_independent(conn: AsyncConnection) -> None:
    common = {"exchange": "bybit", "symbol": SYMBOL, "timeframe": "1h", "from_ms": T0, "to_ms": T0 + HOUR_MS}
    mark = await ok(conn, insert(DataGap).values(source="mark_candles", **common).returning(DataGap.id))
    await conn.execute(insert(DataGap).values(source="index_candles", **common))
    # Докачування mark не закриває прогалину index.
    await conn.execute(update(DataGap).where(DataGap.id == mark).values(resolved_at_ms=T0 + 2 * HOUR_MS))
    open_sources = await conn.execute(select(DataGap.source).where(DataGap.resolved_at_ms.is_(None)))
    assert open_sources.scalars().all() == ["index_candles"]
    await expect_violation(conn, insert(DataGap).values(source="mark_index", **common), "ck_data_gaps_source")


async def test_resolved_gap_does_not_block_new_detection(conn: AsyncConnection) -> None:
    gap = {"source": "candles", "exchange": "bybit", "symbol": SYMBOL, "timeframe": "1h", "from_ms": T0}
    await conn.execute(insert(DataGap).values(to_ms=T0 + HOUR_MS, resolved_at_ms=T0 + 2 * HOUR_MS, **gap))
    await conn.execute(insert(DataGap).values(to_ms=T0 + HOUR_MS, **gap))


# ---------------------------------------------------------------- підтвердження людиною (audit_log)


def _release_values(account: uuid.UUID, audit_id: int) -> dict[str, Any]:
    return {
        "account_id": account,
        "trigger": "UI",
        "status": "RELEASED",
        "completed_at_ms": T0,
        "released_at_ms": T0,
        "confirmed_audit_id": audit_id,
    }


@pytest.mark.parametrize(
    ("action", "confirmed", "same_account"),
    [
        (AUDIT_ACTION_EMERGENCY_RELEASE, False, True),  # не підтверджено
        ("risk.unrelated", True, True),  # інша дія
        (AUDIT_ACTION_MODEL_APPROVE, True, True),  # інша дія
        (AUDIT_ACTION_EMERGENCY_RELEASE, True, False),  # підтвердження для іншого акаунта
    ],
)
async def test_emergency_release_needs_confirmed_matching_audit(
    conn: AsyncConnection, action: str, confirmed: bool, same_account: bool
) -> None:
    account = await make_account(conn)
    audit_account = account if same_account else await make_account(conn)
    audit = await make_audit(conn, action, confirmed=confirmed, account_id=audit_account)
    stmt = insert(EmergencyRun).values(_release_values(account, audit))
    await expect_violation(conn, stmt, "must be a confirmed audit_log entry with action emergency.release")


async def test_emergency_release_with_confirmed_audit_used_once(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    audit = await make_audit(conn, AUDIT_ACTION_EMERGENCY_RELEASE, account_id=account)
    await conn.execute(insert(EmergencyRun).values(_release_values(account, audit)))
    # Те саме підтвердження не знімає наступний Emergency Stop цього акаунта.
    await expect_violation(
        conn, insert(EmergencyRun).values(_release_values(account, audit)), "uq_emergency_runs_confirmed_audit_id"
    )


async def test_emergency_run_can_restart_after_completed(conn: AsyncConnection) -> None:
    """Новий тригер при COMPLETED-запуску продовжує той самий запуск (частковий UNIQUE не дає створити другий)."""
    account = await make_account(conn)
    run = await ok(
        conn,
        insert(EmergencyRun)
        .values(account_id=account, trigger="UI", status="COMPLETED", completed_at_ms=T0)
        .returning(EmergencyRun.id),
    )
    await expect_violation(
        conn,
        insert(EmergencyRun).values(account_id=account, trigger="RISK_HALTED"),
        "uq_emergency_runs_account_id_active",
    )
    await conn.execute(update(EmergencyRun).where(EmergencyRun.id == run).values(status="RUNNING", current_step=3))


@pytest.mark.parametrize(
    ("action", "target"),
    [
        (AUDIT_ACTION_RISK_LOOSEN, "m-wrong"),
        (AUDIT_ACTION_MODEL_APPROVE, "m-other"),
        (AUDIT_ACTION_MODEL_APPROVE, None),
    ],
)
async def test_model_approval_needs_matching_audit(conn: AsyncConnection, action: str, target: str | None) -> None:
    audit = await make_audit(conn, action, target=target)
    stmt = insert(ModelArtifact).values(
        model_values("m-wrong", status="APPROVED", live_corridor={}, confirmed_audit_id=audit)
    )
    await expect_violation(conn, stmt, "with action model.approve for the same model")


async def test_model_approval_by_update_is_checked(conn: AsyncConnection) -> None:
    version = await make_model(conn, status="CANDIDATE", live_corridor={})
    unconfirmed = await make_audit(conn, AUDIT_ACTION_MODEL_APPROVE, confirmed=False, target=version)
    stmt = (
        update(ModelArtifact)
        .where(ModelArtifact.model_version == version)
        .values(status="APPROVED", confirmed_audit_id=unconfirmed)
    )
    await expect_violation(conn, stmt, "with action model.approve")


async def test_one_model_approval_approves_one_model(conn: AsyncConnection) -> None:
    audit = await make_audit(conn, AUDIT_ACTION_MODEL_APPROVE, target="m-one")
    approved = {"status": "APPROVED", "live_corridor": {}, "confirmed_audit_id": audit}
    await conn.execute(insert(ModelArtifact).values(model_values("m-one", **approved)))
    # Друга модель з тим самим записом: або чужа ціль, або повторне використання — обидва відхиляються.
    await expect_violation(conn, insert(ModelArtifact).values(model_values("m-two", **approved)), "for the same model")


@pytest.mark.parametrize(("confirmed", "same_account"), [(False, True), (True, False)])
async def test_risk_loosening_needs_confirmed_audit_of_account(
    conn: AsyncConnection, confirmed: bool, same_account: bool
) -> None:
    account = await make_account(conn)
    audit_account = account if same_account else await make_account(conn)
    audit = await make_audit(conn, AUDIT_ACTION_RISK_LOOSEN, confirmed=confirmed, account_id=audit_account)
    stmt = insert(RiskConfig).values(account_id=account, version=1, params={}, loosening=True, confirmed_audit_id=audit)
    await expect_violation(conn, stmt, "with action risk.config.loosen for the same account")


async def test_risk_loosening_audit_used_once(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    audit = await make_audit(conn, AUDIT_ACTION_RISK_LOOSEN, account_id=account)
    for version in (1, 2):
        stmt = insert(RiskConfig).values(
            account_id=account, version=version, params={}, loosening=True, confirmed_audit_id=audit
        )
        if version == 1:
            await conn.execute(stmt)
        else:
            await expect_violation(conn, stmt, "uq_risk_config_confirmed_audit_id")


# ---------------------------------------------------------------- ізоляція акаунтів


async def test_risk_event_bot_and_signal_belong_to_account(conn: AsyncConnection) -> None:
    account_a = await make_account(conn)
    account_b = await make_account(conn)
    model = await make_model(conn)
    bot_a = await make_bot(conn, account_a)
    sig = await make_signal(conn, bot_a, model)
    event = {"kind": "REJECT", "reason_code": "RISK_DAILY_LOSS"}
    await expect_violation(
        conn,
        insert(RiskEvent).values(account_id=account_b, bot_id=bot_a, **event),
        "fk_risk_events_bot_id_account_id_bots",
    )
    await expect_violation(
        conn,
        insert(RiskEvent).values(account_id=account_a, signal_id=sig, **event),
        "ck_risk_events_signal_requires_bot",
    )
    await conn.execute(insert(RiskEvent).values(account_id=account_a, bot_id=bot_a, signal_id=sig, **event))


async def test_decision_log_order_belongs_to_bot(conn: AsyncConnection) -> None:
    account, _, order = await _bot_order(conn)
    other_bot = await make_bot(conn, account, symbol="ETH/USDT:USDT")
    stmt = insert(DecisionLog).values(bot_id=other_bot, order_id=order, stage="ORDER", decision="SUBMIT")
    await expect_violation(conn, stmt, "fk_decision_log_order_id_bot_id_orders")


async def test_decision_log_signal_belongs_to_bot(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    model = await make_model(conn)
    bot_a = await make_bot(conn, account)
    bot_b = await make_bot(conn, account, symbol="ETH/USDT:USDT")
    sig = await make_signal(conn, bot_a, model)
    stmt = insert(DecisionLog).values(bot_id=bot_b, signal_id=sig, stage="SIGNAL", decision="ENTER")
    await expect_violation(conn, stmt, "fk_decision_log_signal_id_bot_id_signals")


async def test_per_account_keys_do_not_collide(conn: AsyncConnection) -> None:
    """Версія ризик-конфігу та id події ledger унікальні в межах акаунта, а не глобально."""
    for _ in range(2):
        account = await make_account(conn)
        await conn.execute(insert(RiskConfig).values(account_id=account, version=1, params={}, loosening=False))
        await conn.execute(
            insert(LedgerEntry).values(
                account_id=account, event_id="tx-1", kind="FEE", amount=Decimal("-0.1"), currency="USDT", ts_ms=T0
            )
        )


# ---------------------------------------------------------------- капітал, Emergency steps, jobs, спроби


async def test_equity_snapshot_checks(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    snap = {
        "account_id": account,
        "ts_ms": T0,
        "equity": Decimal(100),
        "wallet_balance": Decimal(100),
        "unrealized_pnl": Decimal(0),
        "peak_equity": Decimal(100),
        "day_start_equity": Decimal(100),
    }
    await conn.execute(insert(EquitySnapshot).values(snap))
    await expect_violation(conn, insert(EquitySnapshot).values(snap), "uq_equity_snapshots_account_id_ts_ms")
    later = snap | {"ts_ms": T0 + 1, "equity": Decimal(101)}
    await expect_violation(conn, insert(EquitySnapshot).values(later), "ck_equity_snapshots_peak_ge_equity")


async def test_emergency_steps_checks(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    run = await ok(conn, insert(EmergencyRun).values(account_id=account, trigger="UI").returning(EmergencyRun.id))
    await conn.execute(insert(EmergencyStep).values(run_id=run, step_no=3, attempt=1))
    await expect_violation(
        conn,
        insert(EmergencyStep).values(run_id=run, step_no=3, attempt=1),
        "uq_emergency_steps_run_id_step_no_attempt",
    )
    await expect_violation(
        conn, insert(EmergencyStep).values(run_id=run, step_no=7, attempt=1), "ck_emergency_steps_step_no_range"
    )
    await expect_violation(
        conn,
        insert(EmergencyStep).values(run_id=run, step_no=4, attempt=1, status="SUCCEEDED"),
        "ck_emergency_steps_finished_iff_not_started",
    )


async def test_jobs_checks(conn: AsyncConnection) -> None:
    await conn.execute(insert(Job).values(kind="backfill", params={}, dedup_key="bf:BTC"))
    await expect_violation(
        conn, insert(Job).values(kind="backfill", params={}, dedup_key="bf:BTC"), "uq_jobs_dedup_key_active"
    )
    await conn.execute(
        insert(Job).values(kind="backfill", params={}, dedup_key="bf:BTC", status="DONE", finished_at_ms=T0)
    )
    await expect_violation(
        conn, insert(Job).values(kind="train", params={}, status="RUNNING"), "ck_jobs_running_has_heartbeat"
    )
    await expect_violation(
        conn, insert(Job).values(kind="train", params={}, status="DONE"), "ck_jobs_finished_iff_terminal"
    )
    await expect_violation(conn, insert(Job).values(kind="Train!", params={}), "ck_jobs_kind_format")


async def test_experiment_trial_numbers_are_unique(conn: AsyncConnection) -> None:
    await conn.execute(
        insert(Experiment).values(
            id="exp-u",
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
    trial = {"experiment_id": "exp-u", "trial_no": 1, "state": "PRUNED", "params": {}, "oos_results": [], "metrics": {}}
    await conn.execute(insert(ExperimentTrial).values(trial))
    await expect_violation(conn, insert(ExperimentTrial).values(trial), "uq_experiment_trials_experiment_id_trial_no")


# ---------------------------------------------------------------- рев'ю 2: ордери біржі, зупинений бот, позиції


async def test_exchange_order_qty_grows_with_partial_fills(conn: AsyncConnection) -> None:
    """Кількість ордера біржі (SL позиції) відома лише з fills: вона може зростати, але не зменшуватися."""
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    order = await make_order(
        conn,
        account,
        "demo",
        bot,
        origin="EXCHANGE",
        client_order_id=None,
        exchange_order_id="sl-p",
        reduce_only=True,
        side="SELL",
        state="PARTIALLY_FILLED",
        qty=Decimal("0.004"),
        filled_qty=Decimal("0.004"),
    )
    await conn.execute(
        update(Order).where(Order.id == order).values(qty=Decimal("0.010"), filled_qty=Decimal("0.010"), state="FILLED")
    )
    await expect_violation(
        conn, update(Order).where(Order.id == order).values(qty=Decimal("0.009")), "order identity is immutable"
    )


async def test_open_position_cannot_be_deleted(conn: AsyncConnection) -> None:
    account = await make_account(conn)
    bot = await make_bot(conn, account)
    position = await ok(conn, insert(Position).values(position_values(bot, account)).returning(Position.id))
    await expect_violation(conn, delete(Position).where(Position.id == position), "cannot delete open position")
    await conn.execute(
        update(Position).where(Position.id == position).values(status="CLOSED", closed_at_ms=T0 + HOUR_MS)
    )
    await conn.execute(delete(Position).where(Position.id == position))


async def test_stopped_bot_accepts_exits_and_positions_but_not_entries(conn: AsyncConnection) -> None:
    """Зупинений бот не входить у ринок, але захист (вихід, пізній fill → позиція) не блокується ніколи."""
    account = await make_account(conn)
    bot = await make_bot(conn, account, status="STOPPED")
    await expect_violation(conn, insert(Order).values(order_values(account, "demo", bot)), "is STOPPED")
    await make_order(conn, account, "demo", bot, origin="BOT", reduce_only=True, side="SELL")
    await conn.execute(insert(Position).values(position_values(bot, account)))
