"""Ризик, Emergency Stop і облік капіталу — окремо для кожного акаунта (ТЗ §8, §10, §15, T0.4).

Інваріанти на рівні БД:
- `account_id` обов'язковий у всіх таблицях модуля;
- `risk_config` — версії (append-only, тригер у міграції); послаблення лімітів має підтверджений запис
  `audit_log` з дією `risk.config.loosen` (`confirmed_audit_id`, тригер `ctb_require_confirmed_audit()`);
- не більше одного незавершеного (`status <> 'RELEASED'`) Emergency Stop на акаунт;
  вихід з Emergency Stop (`RELEASED`) — лише з підтвердженим записом `audit_log` з дією `emergency.release`
  (ручне підтвердження, ТЗ §10.4);
- `risk_events`: бот належить тому самому акаунту, сигнал — тому самому боту (складені FK);
- `system_state.emergency_run_id` посилається на запуск того самого акаунта;
- `ledger` — append-only, дедуплікація за `(account_id, event_id, kind)`.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    SmallInteger,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import SYMBOL_RE, Base, CreatedMs, Money, TimeMs, enum_check

RISK_STATUSES: tuple[str, ...] = ("NORMAL", "RESTRICTED", "HALTED")
RISK_EVENT_KINDS: tuple[str, ...] = ("REJECT", "LIMIT_TRIGGERED", "STATUS_CHANGE")
REASON_CODE_RE = "^[A-Z][A-Z0-9_]*$"

# Тригери Emergency Stop (ТЗ §10.1).
EMERGENCY_TRIGGERS: tuple[str, ...] = ("UI", "RISK_HALTED", "CRITICAL_ERROR", "TELEGRAM")
# RUNNING — цикл до цільового стану; COMPLETED — 0 позицій і 0 ордерів, стан EMERGENCY_STOP триває;
# RELEASED — вихід вручну після чистої звірки та підтвердження (ТЗ §10.4).
EMERGENCY_RUN_STATUSES: tuple[str, ...] = ("RUNNING", "COMPLETED", "RELEASED")
EMERGENCY_STEP_STATUSES: tuple[str, ...] = ("STARTED", "SUCCEEDED", "FAILED")
EMERGENCY_STEPS = 6  # кроки 1–6 ТЗ §10.3

LEDGER_KINDS: tuple[str, ...] = ("FEE", "FUNDING", "REALIZED_PNL", "DEPOSIT", "WITHDRAWAL")


class RiskConfig(Base):
    """Версія лімітів ризику акаунта (ТЗ §8.1, §8.2). Чинна — з найбільшим `version`.

    `params` — схема `RiskLimits` (T2.2): писати `model_dump_json()`, читати `model_validate_json()` (strict, Decimal
    рядками). Запис версії і підтвердження послаблення — T4.1.
    """

    __tablename__ = "risk_config"
    __table_args__ = (
        CheckConstraint("version >= 1", name="version_positive"),
        CheckConstraint("NOT loosening OR confirmed_audit_id IS NOT NULL", name="loosening_confirmed"),
        UniqueConstraint("account_id", "version"),
        # Одне підтвердження — одна дія (запис audit_log не можна використати повторно).
        Index(
            "uq_risk_config_confirmed_audit_id",
            "confirmed_audit_id",
            unique=True,
            postgresql_where=text("confirmed_audit_id IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchange_accounts.id"))
    version: Mapped[int]
    params: Mapped[dict[str, Any]]
    # Хоча б один ліміт послаблено відносно попередньої версії (ТЗ §8.1: лише з підтвердженням).
    loosening: Mapped[bool]
    confirmed_audit_id: Mapped[int | None] = mapped_column(ForeignKey("audit_log.id"))
    created_at_ms: Mapped[CreatedMs]


class RiskEvent(Base):
    """Відмова RiskGate або спрацювання ліміту з кодом причини (`RISK_DAILY_LOSS`, …, ТЗ §8.1)."""

    __tablename__ = "risk_events"
    __table_args__ = (
        enum_check("kind", RISK_EVENT_KINDS),
        CheckConstraint(f"reason_code ~ '{REASON_CODE_RE}'", name="reason_code_format"),
        # Інакше складений FK (signal_id, bot_id) з NULL у bot_id не перевірявся б (MATCH SIMPLE).
        CheckConstraint("signal_id IS NULL OR bot_id IS NOT NULL", name="signal_requires_bot"),
        ForeignKeyConstraint(["bot_id", "account_id"], ["bots.id", "bots.account_id"]),
        ForeignKeyConstraint(["signal_id", "bot_id"], ["signals.id", "signals.bot_id"]),
        Index("ix_risk_events_account_id_ts_ms", "account_id", "ts_ms"),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchange_accounts.id"))
    bot_id: Mapped[uuid.UUID | None]
    signal_id: Mapped[uuid.UUID | None]
    kind: Mapped[str] = mapped_column(Text)
    reason_code: Mapped[str] = mapped_column(Text)
    details: Mapped[dict[str, Any] | None]
    ts_ms: Mapped[CreatedMs]


class EmergencyRun(Base):
    """Запуск протоколу Emergency Stop для одного акаунта (ТЗ §10). «Зупинити все» — по запуску на акаунт
    зі спільним `group_id`."""

    __tablename__ = "emergency_runs"
    __table_args__ = (
        enum_check("trigger", EMERGENCY_TRIGGERS),
        enum_check("status", EMERGENCY_RUN_STATUSES),
        CheckConstraint(f"current_step >= 1 AND current_step <= {EMERGENCY_STEPS}", name="current_step_range"),
        CheckConstraint("status = 'RUNNING' OR completed_at_ms IS NOT NULL", name="completed_has_time"),
        CheckConstraint(
            "status <> 'RELEASED' OR (released_at_ms IS NOT NULL AND confirmed_audit_id IS NOT NULL)",
            name="released_by_human",
        ),
        UniqueConstraint("id", "account_id"),
        # Одне підтвердження — одна дія (запис audit_log не можна використати повторно).
        Index(
            "uq_emergency_runs_confirmed_audit_id",
            "confirmed_audit_id",
            unique=True,
            postgresql_where=text("confirmed_audit_id IS NOT NULL"),
        ),
        Index(
            "uq_emergency_runs_account_id_active",
            "account_id",
            unique=True,
            postgresql_where=text("status <> 'RELEASED'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4, server_default=text("gen_random_uuid()")
    )
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchange_accounts.id"))
    group_id: Mapped[uuid.UUID | None]
    trigger: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="RUNNING", server_default=text("'RUNNING'"))
    current_step: Mapped[int] = mapped_column(SmallInteger, default=1, server_default=text("1"))
    report: Mapped[dict[str, Any] | None]
    error: Mapped[str | None] = mapped_column(Text)
    started_at_ms: Mapped[CreatedMs]
    completed_at_ms: Mapped[int | None]
    released_at_ms: Mapped[int | None]
    # Підтвердження виходу людиною (`audit_log.action = 'emergency.release'`, `confirmed`).
    confirmed_audit_id: Mapped[int | None] = mapped_column(ForeignKey("audit_log.id"))


class EmergencyStep(Base):
    """Спроба кроку протоколу (ТЗ §10.3–§10.4): після рестарту цикл продовжується з потрібного кроку."""

    __tablename__ = "emergency_steps"
    __table_args__ = (
        enum_check("status", EMERGENCY_STEP_STATUSES),
        CheckConstraint(f"step_no >= 1 AND step_no <= {EMERGENCY_STEPS}", name="step_no_range"),
        CheckConstraint("attempt >= 1", name="attempt_positive"),
        CheckConstraint("(status = 'STARTED') = (finished_at_ms IS NULL)", name="finished_iff_not_started"),
        UniqueConstraint("run_id", "step_no", "attempt"),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("emergency_runs.id"))
    step_no: Mapped[int] = mapped_column(SmallInteger)
    attempt: Mapped[int]
    status: Mapped[str] = mapped_column(Text, default="STARTED", server_default=text("'STARTED'"))
    details: Mapped[dict[str, Any] | None]
    error: Mapped[str | None] = mapped_column(Text)
    started_at_ms: Mapped[CreatedMs]
    finished_at_ms: Mapped[int | None]


class SystemState(Base):
    """Статус Risk Engine і Emergency Stop акаунта (ТЗ §8.5, §10.3). Рядок на акаунт.

    Emergency Stop активний ⇔ `emergency_run_id IS NOT NULL` (запис кроку 1 ТЗ §10.3).
    `last_reconcile_at_ms` — остання успішна звірка (pre-trade перевірка 6, ТЗ §8.4).
    """

    __tablename__ = "system_state"
    __table_args__ = (
        enum_check("risk_status", RISK_STATUSES),
        CheckConstraint("version >= 1", name="version_positive"),
        ForeignKeyConstraint(["emergency_run_id", "account_id"], ["emergency_runs.id", "emergency_runs.account_id"]),
    )

    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchange_accounts.id"), primary_key=True)
    risk_status: Mapped[str] = mapped_column(Text, default="NORMAL", server_default=text("'NORMAL'"))
    emergency_run_id: Mapped[uuid.UUID | None]
    last_reconcile_at_ms: Mapped[int | None]
    version: Mapped[int] = mapped_column(default=1, server_default=text("1"))
    updated_at_ms: Mapped[CreatedMs]


class EquitySnapshot(Base):
    """Знімок капіталу акаунта з біржі (ТЗ §15): раз на хвилину і після кожного fill. Значення в USDT."""

    __tablename__ = "equity_snapshots"
    __table_args__ = (
        CheckConstraint("peak_equity >= equity", name="peak_ge_equity"),
        CheckConstraint("peak_equity >= 0 AND day_start_equity >= 0", name="base_non_negative"),
        UniqueConstraint("account_id", "ts_ms"),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchange_accounts.id"))
    ts_ms: Mapped[TimeMs]
    equity: Mapped[Money]
    wallet_balance: Mapped[Money]
    unrealized_pnl: Mapped[Money]
    peak_equity: Mapped[Money]
    day_start_equity: Mapped[Money]


class LedgerEntry(Base):
    """Транзакція біржі (ТЗ §15), append-only. `amount` зі знаком: «+» — надходження на рахунок, «−» — списання."""

    __tablename__ = "ledger"
    __table_args__ = (
        enum_check("kind", LEDGER_KINDS),
        CheckConstraint("char_length(event_id) > 0", name="event_id_not_empty"),
        CheckConstraint("kind <> 'DEPOSIT' OR amount > 0", name="deposit_positive"),
        CheckConstraint("kind <> 'WITHDRAWAL' OR amount < 0", name="withdrawal_negative"),
        CheckConstraint(f"symbol IS NULL OR symbol ~ '{SYMBOL_RE}'", name="symbol_format"),
        UniqueConstraint("account_id", "event_id", "kind"),
        Index("ix_ledger_account_id_ts_ms", "account_id", "ts_ms"),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("exchange_accounts.id"))
    event_id: Mapped[str] = mapped_column(Text)
    kind: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str | None] = mapped_column(Text)
    amount: Mapped[Decimal]
    currency: Mapped[str] = mapped_column(Text)
    details: Mapped[dict[str, Any] | None]
    ts_ms: Mapped[TimeMs]
    created_at_ms: Mapped[CreatedMs]
