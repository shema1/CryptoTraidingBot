"""Таблиці торгового ядра: боти, сигнали, ордери, події ордерів, fills, позиції, журнал рішень.

ТЗ §6.1–§6.3, §7, §9, §15; T0.4. Інваріанти, які тримає сама БД (а не лише код):
- один не-`STOPPED` бот на `(account_id, symbol)` (ТЗ §6.2); `PAUSED` завжди має причину, `RUNNING` — жодної;
- один сигнал на свічку `UNIQUE(bot_id, entry_candle_open_time_ms)`; фічі — лише зі свічки до входу;
- `client_order_id UNIQUE` у форматі `tb` + `HOST_ID` + uuid4 hex (36 символів, ТЗ §7.2); порожній лише в ордерів,
  створених самою біржею (`origin = 'EXCHANGE'`: спрацювання TP/SL позиції, ліквідація, ADL);
- один вхідний ордер на сигнал (частковий `UNIQUE(signal_id) WHERE NOT reduce_only`);
- вхідний ордер бота (`origin = 'BOT'`) має сигнал у **кожному** режимі; ручні тестові входи (`MANUAL_TEST`) — лише
  не в Live; імпортовані (`ORPHAN`) та біржові (`EXCHANGE`) ордери не можуть бути у `CREATED`/`SUBMITTING`;
- `fills.exec_id UNIQUE` (дедуплікація WS/REST); fill має символ і сторону свого ордера;
- одна відкрита позиція на `(account_id, symbol)` (one-way mode, ТЗ §0); символ — канонічний (`SYMBOL_RE`);
- режим і акаунт бота, ордера, fill і позиції збігаються (складені FK до `(id, account_id, mode)`);
- тригери міграції: `order_events` — append-only; в `orders` ідентичність ордера незмінна, з термінального стану
  переходів немає, DELETE заборонений; у `bots` акаунт/режим/символ незмінні, а `STOPPED` неможливий, поки
  в бота є відкрита позиція або нетермінальні ордери.
Граф переходів станів ордера — `trading/order_state.py` (T3.1), не тут.
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
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import MODES, Base, CreatedMs, Money, TimeMs, enum_check, sql_list, symbol_check

# --- Бот (ТЗ §6.1) ---
BOT_STATUSES: tuple[str, ...] = (
    "STOPPED",
    "STARTING",
    "RUNNING",
    "PAUSED",
    "RECONCILING",
    "ERROR",
    "EMERGENCY_STOP",
)
PAUSE_REASONS: tuple[str, ...] = (
    "DAILY_LOSS",
    "COOLDOWN",
    "DATA_GAP",
    "INSTRUMENT_NOT_TRADING",
    "MODEL_SWAP",
    "DRIFT",
    "MODEL_UNDERPERFORMS",
    "UNKNOWN_POSITION",
    "FOREIGN_HOST_ORDERS",
    "LEVERAGE_MISMATCH",
    "MANUAL",
)

# --- Сигнал (ТЗ §5.5, §6.3) ---
# Допуск розбіжності локального годинника з біржею (ТЗ §4.3: більше 1 с — блок нових входів).
CLOCK_TOLERANCE_MS = 1000
DIRECTIONS: tuple[str, ...] = ("LONG", "SHORT")
PREDICTIONS: tuple[str, ...] = ("LONG", "SHORT", "NONE")
SIGNAL_DECISIONS: tuple[str, ...] = ("ENTER", "SKIP")
SIGNAL_STATUSES: tuple[str, ...] = ("NEW", "EXECUTING", "DONE", "EXPIRED_UNEXECUTED")

# --- Ордер (ТЗ §7.1) ---
ORDER_STATES: tuple[str, ...] = (
    "CREATED",
    "SUBMITTING",
    "OPEN",
    "PARTIALLY_FILLED",
    "FILLED",
    "PENDING_CANCEL",
    "CANCELED",
    "REJECTED",
    "EXPIRED",
    "SUBMISSION_UNKNOWN",
)
ORDER_TERMINAL_STATES: tuple[str, ...] = ("FILLED", "CANCELED", "REJECTED", "EXPIRED")
ORDER_SIDES: tuple[str, ...] = ("BUY", "SELL")
ORDER_TYPES: tuple[str, ...] = ("MARKET", "LIMIT")
EVENT_SOURCES: tuple[str, ...] = ("WS", "REST", "LOCAL")
# Походження ордера: BOT — цикл бота (вхід — лише з сигналом); MANUAL_TEST — `[Test Long]`/`[Test Short]` у
# Demo/Testnet (ТЗ §16); ORPHAN — ордер з нашим `HOST_ID`, знайдений при звірці (ТЗ §11.4); EXCHANGE — створений
# біржею (спрацювання TP/SL позиції, ліквідація, ADL) і відомий лише з WS/REST.
ORDER_ORIGINS: tuple[str, ...] = ("BOT", "MANUAL_TEST", "ORPHAN", "EXCHANGE")
# `tb` + HOST_ID (2 × [a-z0-9]) + uuid4().hex (32 × [0-9a-f]) = 36 символів (ТЗ §7.2).
CLIENT_ORDER_ID_RE = "^tb[a-z0-9]{2}[0-9a-f]{32}$"

# --- Позиція (ТЗ §9, §11.4) ---
POSITION_STATUSES: tuple[str, ...] = ("OPEN", "CLOSED", "CLOSED_EXTERNALLY")
PROTECTION_STATUSES: tuple[str, ...] = ("ENTRY_FILLED_UNPROTECTED", "PROTECTED")

# --- Журнал рішень (ТЗ §5.7, §6.3) ---
DECISION_STAGES: tuple[str, ...] = ("SIGNAL", "RISK", "ORDER", "RESULT")
REASON_CODE_RE = "^[A-Z][A-Z0-9_]*$"


class Bot(Base):
    """Бот: один символ на одному акаунті. `version` — optimistic concurrency (ТЗ §6.4)."""

    __tablename__ = "bots"
    __table_args__ = (
        enum_check("status", BOT_STATUSES),
        enum_check("mode", MODES),
        symbol_check(),
        CheckConstraint("timeframe ~ '^[0-9]+(m|h|d|w)$'", name="timeframe_format"),
        CheckConstraint(f"pause_reasons <@ ARRAY{list(PAUSE_REASONS)}::text[]", name="pause_reasons_known"),
        CheckConstraint("status <> 'PAUSED' OR cardinality(pause_reasons) > 0", name="paused_has_reason"),
        CheckConstraint("status <> 'RUNNING' OR cardinality(pause_reasons) = 0", name="running_has_no_reason"),
        CheckConstraint(
            "pending_model_version IS NULL OR pending_model_version IS DISTINCT FROM model_version",
            name="pending_model_differs",
        ),
        CheckConstraint("version >= 1", name="version_positive"),
        ForeignKeyConstraint(["account_id", "mode"], ["exchange_accounts.id", "exchange_accounts.mode"]),
        UniqueConstraint("id", "account_id", "mode"),
        UniqueConstraint("id", "account_id"),
        Index(
            "uq_bots_account_id_symbol_active",
            "account_id",
            "symbol",
            unique=True,
            postgresql_where=text("status <> 'STOPPED'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4, server_default=text("gen_random_uuid()")
    )
    account_id: Mapped[uuid.UUID]
    # Режим фіксується при створенні й дорівнює режиму акаунта (ТЗ §13).
    mode: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    timeframe: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="STOPPED", server_default=text("'STOPPED'"))
    pause_reasons: Mapped[list[str]] = mapped_column(ARRAY(Text), default=list, server_default=text("'{}'"))
    model_version: Mapped[str | None] = mapped_column(Text, ForeignKey("models.model_version"))
    pending_model_version: Mapped[str | None] = mapped_column(Text, ForeignKey("models.model_version"))
    config: Mapped[dict[str, Any]] = mapped_column(default=dict, server_default=text("'{}'"))
    version: Mapped[int] = mapped_column(default=1, server_default=text("1"))
    created_at_ms: Mapped[CreatedMs]
    updated_at_ms: Mapped[CreatedMs]


class Signal(Base):
    """Сигнал на свічку `t` (ТЗ §5.5). Записується лише коли первинна стратегія дала кандидата."""

    __tablename__ = "signals"
    __table_args__ = (
        enum_check("primary_signal", DIRECTIONS),
        enum_check("prediction", PREDICTIONS),
        enum_check("decision", SIGNAL_DECISIONS),
        enum_check("status", SIGNAL_STATUSES),
        # Look-ahead: фічі рахуються лише зі свічки, що закрилася до відкриття свічки входу.
        CheckConstraint("features_candle_open_time_ms < entry_candle_open_time_ms", name="features_before_entry"),
        # Розрахунок — після відкриття свічки входу, з допуском розбіжності годинника (ТЗ §4.3).
        CheckConstraint(
            f"computed_at_ms >= entry_candle_open_time_ms - {CLOCK_TOLERANCE_MS}", name="computed_after_entry_open"
        ),
        CheckConstraint("probability IS NULL OR (probability >= 0 AND probability <= 1)", name="probability_range"),
        UniqueConstraint("bot_id", "entry_candle_open_time_ms"),
        # Ціль FK ордерів/позицій: сигнал належить тому самому боту.
        UniqueConstraint("id", "bot_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4, server_default=text("gen_random_uuid()")
    )
    bot_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("bots.id"))
    entry_candle_open_time_ms: Mapped[TimeMs]
    features_candle_open_time_ms: Mapped[TimeMs]
    computed_at_ms: Mapped[TimeMs]
    feature_schema_hash: Mapped[str] = mapped_column(Text)
    model_version: Mapped[str] = mapped_column(Text, ForeignKey("models.model_version"))
    primary_signal: Mapped[str] = mapped_column(Text)
    prediction: Mapped[str] = mapped_column(Text)
    # Калібрована ймовірність TP і очікувана вигода в ATR (ТЗ §5.5.1); для `rule_only` — NULL.
    probability: Mapped[Decimal | None]
    expected_edge_atr: Mapped[Decimal | None]
    decision: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="NEW", server_default=text("'NEW'"))
    created_at_ms: Mapped[CreatedMs]
    updated_at_ms: Mapped[CreatedMs]


class Position(Base):
    """Позиція бота (ТЗ §9). Розмір, середня ціна і PnL — з `fills`; поля тут — кеш для звірки та UI."""

    __tablename__ = "positions"
    __table_args__ = (
        enum_check("mode", MODES),
        enum_check("side", DIRECTIONS),
        enum_check("status", POSITION_STATUSES),
        enum_check("protection_status", PROTECTION_STATUSES),
        CheckConstraint("qty >= 0", name="qty_non_negative"),
        CheckConstraint("status <> 'OPEN' OR qty > 0", name="open_has_qty"),
        CheckConstraint("status <> 'OPEN' OR protection_status IS NOT NULL", name="open_has_protection_status"),
        CheckConstraint(
            "protection_status IS DISTINCT FROM 'ENTRY_FILLED_UNPROTECTED' OR unprotected_since_ms IS NOT NULL",
            name="unprotected_has_since",
        ),
        # Порядку opened/closed не вимагаємо: час відкриття може бути локальним, а закриття — біржовим, і
        # невдалий UPDATE на CLOSED_EXTERNALLY залишив би позицію «відкритою» в БД.
        CheckConstraint("(status = 'OPEN') = (closed_at_ms IS NULL)", name="closed_iff_not_open"),
        symbol_check(),
        CheckConstraint("entry_price IS NULL OR entry_price > 0", name="entry_price_positive"),
        CheckConstraint("sl_price IS NULL OR sl_price > 0", name="sl_price_positive"),
        CheckConstraint("tp_price IS NULL OR tp_price > 0", name="tp_price_positive"),
        CheckConstraint("entry_atr IS NULL OR entry_atr > 0", name="entry_atr_positive"),
        CheckConstraint("max_hold_bars IS NULL OR max_hold_bars > 0", name="max_hold_bars_positive"),
        CheckConstraint("version >= 1", name="version_positive"),
        ForeignKeyConstraint(["bot_id", "account_id", "mode"], ["bots.id", "bots.account_id", "bots.mode"]),
        ForeignKeyConstraint(["signal_id", "bot_id"], ["signals.id", "signals.bot_id"]),
        UniqueConstraint("id", "account_id", "mode"),
        Index(
            "uq_positions_account_id_symbol_open",
            "account_id",
            "symbol",
            unique=True,
            postgresql_where=text("status = 'OPEN'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4, server_default=text("gen_random_uuid()")
    )
    bot_id: Mapped[uuid.UUID]
    account_id: Mapped[uuid.UUID]
    mode: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    side: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="OPEN", server_default=text("'OPEN'"))
    qty: Mapped[Money]
    entry_price: Mapped[Decimal | None]
    sl_price: Mapped[Decimal | None]
    tp_price: Mapped[Decimal | None]
    sl_trigger_by: Mapped[str | None] = mapped_column(Text)
    tp_trigger_by: Mapped[str | None] = mapped_column(Text)
    protection_status: Mapped[str | None] = mapped_column(Text)
    unprotected_since_ms: Mapped[int | None]
    # ATR(t-1), зафіксований на момент входу (ТЗ §6.6), і time-stop у барах (≤ horizon_bars моделі).
    entry_atr: Mapped[Decimal | None]
    max_hold_bars: Mapped[int | None]
    signal_id: Mapped[uuid.UUID | None]
    model_version: Mapped[str | None] = mapped_column(Text, ForeignKey("models.model_version"))
    realized_pnl: Mapped[Decimal | None]
    close_reason: Mapped[str | None] = mapped_column(Text)
    opened_at_ms: Mapped[TimeMs]
    closed_at_ms: Mapped[int | None]
    version: Mapped[int] = mapped_column(default=1, server_default=text("1"))
    updated_at_ms: Mapped[CreatedMs]


class Order(Base):
    """Ордер (ТЗ §7). Запис у `SUBMITTING` комітиться до мережевого виклику (write-ahead).

    `origin` — походження (`ORDER_ORIGINS`). `bot_id` порожній лише для `ORPHAN`/`EXCHANGE`; `client_order_id`
    порожній лише для `EXCHANGE` (біржа створює ордер сама, напр. при спрацюванні SL позиції — fill такого ордера
    все одно записується у `fills`, бо PnL рахується лише з них). `filled_qty` — кеш суми `fills.qty`.
    Обмежень на поєднання стану й `filled_qty` немає: fill з WS може прийти раніше за відповідь REST.
    """

    __tablename__ = "orders"
    __table_args__ = (
        enum_check("mode", MODES),
        enum_check("state", ORDER_STATES),
        enum_check("side", ORDER_SIDES),
        enum_check("order_type", ORDER_TYPES),
        enum_check("origin", ORDER_ORIGINS),
        symbol_check(),
        CheckConstraint(f"client_order_id ~ '{CLIENT_ORDER_ID_RE}'", name="client_order_id_format"),
        CheckConstraint("client_order_id IS NOT NULL OR origin = 'EXCHANGE'", name="client_order_id_required"),
        CheckConstraint("qty > 0", name="qty_positive"),
        CheckConstraint("filled_qty >= 0 AND filled_qty <= qty", name="filled_qty_range"),
        CheckConstraint("price IS NULL OR price > 0", name="price_positive"),
        CheckConstraint("order_type <> 'LIMIT' OR price IS NOT NULL", name="limit_has_price"),
        CheckConstraint("sl_price IS NULL OR sl_price > 0", name="sl_price_positive"),
        CheckConstraint("tp_price IS NULL OR tp_price > 0", name="tp_price_positive"),
        CheckConstraint("bot_id IS NOT NULL OR origin IN ('ORPHAN', 'EXCHANGE')", name="bot_required"),
        # Інакше складений FK (signal_id, bot_id) з NULL у bot_id не перевірявся б (MATCH SIMPLE).
        CheckConstraint("signal_id IS NULL OR bot_id IS NOT NULL", name="signal_requires_bot"),
        # Вхід бота — лише з сигналом, однаково в Demo, Testnet і Live (один код, AGENTS.md інваріант 4).
        CheckConstraint("origin <> 'BOT' OR reduce_only OR signal_id IS NOT NULL", name="bot_entry_has_signal"),
        CheckConstraint(
            "origin <> 'MANUAL_TEST' OR (mode <> 'live' AND signal_id IS NULL)", name="manual_test_not_live"
        ),
        # Імпортований/біржовий ордер уже існує на біржі: його не можна «відправити» в обхід циклу бота й RiskGate.
        CheckConstraint(
            "origin NOT IN ('ORPHAN', 'EXCHANGE')"
            " OR (state NOT IN ('CREATED', 'SUBMITTING') AND signal_id IS NULL AND exchange_order_id IS NOT NULL)",
            name="imported_not_submittable",
        ),
        CheckConstraint("version >= 1", name="version_positive"),
        ForeignKeyConstraint(["account_id", "mode"], ["exchange_accounts.id", "exchange_accounts.mode"]),
        ForeignKeyConstraint(["bot_id", "account_id", "mode"], ["bots.id", "bots.account_id", "bots.mode"]),
        ForeignKeyConstraint(["signal_id", "bot_id"], ["signals.id", "signals.bot_id"]),
        ForeignKeyConstraint(
            ["position_id", "account_id", "mode"], ["positions.id", "positions.account_id", "positions.mode"]
        ),
        UniqueConstraint("client_order_id"),
        UniqueConstraint("account_id", "exchange_order_id"),
        # Цілі складених FK: fill — того самого акаунта, режиму, символу й сторони; рішення — того самого бота.
        UniqueConstraint("id", "account_id", "mode", "symbol", "side", name="uq_orders_fill_target"),
        UniqueConstraint("id", "bot_id"),
        Index("uq_orders_signal_id_entry", "signal_id", unique=True, postgresql_where=text("NOT reduce_only")),
        Index(
            "ix_orders_bot_id_active",
            "bot_id",
            postgresql_where=text(f"state NOT IN {sql_list(ORDER_TERMINAL_STATES)}"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4, server_default=text("gen_random_uuid()")
    )
    account_id: Mapped[uuid.UUID]
    mode: Mapped[str] = mapped_column(Text)
    bot_id: Mapped[uuid.UUID | None]
    origin: Mapped[str] = mapped_column(Text, default="BOT", server_default=text("'BOT'"))
    signal_id: Mapped[uuid.UUID | None]
    position_id: Mapped[uuid.UUID | None]
    client_order_id: Mapped[str | None] = mapped_column(Text)
    exchange_order_id: Mapped[str | None] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    side: Mapped[str] = mapped_column(Text)
    order_type: Mapped[str] = mapped_column(Text)
    reduce_only: Mapped[bool]
    time_in_force: Mapped[str | None] = mapped_column(Text)
    qty: Mapped[Money]
    price: Mapped[Decimal | None]
    sl_price: Mapped[Decimal | None]
    tp_price: Mapped[Decimal | None]
    state: Mapped[str] = mapped_column(Text, default="CREATED", server_default=text("'CREATED'"))
    filled_qty: Mapped[Decimal] = mapped_column(default=Decimal(0), server_default=text("0"))
    reject_code: Mapped[str | None] = mapped_column(Text)
    reject_reason: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(default=1, server_default=text("1"))
    created_at_ms: Mapped[CreatedMs]
    updated_at_ms: Mapped[CreatedMs]
    submitted_at_ms: Mapped[int | None]


class OrderEvent(Base):
    """Append-only журнал переходів ордера (ТЗ §7.2). `from_state` порожній лише для першого запису."""

    __tablename__ = "order_events"
    __table_args__ = (
        CheckConstraint(f"from_state IS NULL OR from_state IN {sql_list(ORDER_STATES)}", name="from_state"),
        enum_check("to_state", ORDER_STATES),
        enum_check("source", EVENT_SOURCES),
        Index("ix_order_events_order_id_id", "order_id", "id"),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("orders.id"))
    from_state: Mapped[str | None] = mapped_column(Text)
    to_state: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any] | None]
    ts_ms: Mapped[CreatedMs]


class Fill(Base):
    """Виконання (ТЗ §7.2). Дедуплікація за `exec_id`: та сама подія приходить і з WS, і з REST.

    Комісія може бути від'ємною (rebate мейкера).
    """

    __tablename__ = "fills"
    __table_args__ = (
        enum_check("mode", MODES),
        enum_check("side", ORDER_SIDES),
        CheckConstraint("char_length(exec_id) > 0", name="exec_id_not_empty"),
        CheckConstraint("price > 0", name="price_positive"),
        CheckConstraint("qty > 0", name="qty_positive"),
        symbol_check(),
        ForeignKeyConstraint(
            ["order_id", "account_id", "mode", "symbol", "side"],
            ["orders.id", "orders.account_id", "orders.mode", "orders.symbol", "orders.side"],
            name="fk_fills_order",
        ),
        UniqueConstraint("exec_id"),
        Index("ix_fills_order_id", "order_id"),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    exec_id: Mapped[str] = mapped_column(Text)
    order_id: Mapped[uuid.UUID]
    account_id: Mapped[uuid.UUID]
    mode: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    side: Mapped[str] = mapped_column(Text)
    price: Mapped[Money]
    qty: Mapped[Money]
    fee: Mapped[Money]
    fee_currency: Mapped[str] = mapped_column(Text)
    is_maker: Mapped[bool | None]
    ts_ms: Mapped[TimeMs]
    created_at_ms: Mapped[CreatedMs]


class DecisionLog(Base):
    """Журнал рішень: сигнал → ризик-рішення → ордер → результат (ТЗ §5.7, §6.3)."""

    __tablename__ = "decision_log"
    __table_args__ = (
        enum_check("stage", DECISION_STAGES),
        CheckConstraint(f"reason_code IS NULL OR reason_code ~ '{REASON_CODE_RE}'", name="reason_code_format"),
        CheckConstraint("probability IS NULL OR (probability >= 0 AND probability <= 1)", name="probability_range"),
        ForeignKeyConstraint(["signal_id", "bot_id"], ["signals.id", "signals.bot_id"]),
        # Ордер рішення належить тому самому боту.
        ForeignKeyConstraint(["order_id", "bot_id"], ["orders.id", "orders.bot_id"]),
        Index("ix_decision_log_bot_id_ts_ms", "bot_id", "ts_ms"),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    bot_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("bots.id"))
    signal_id: Mapped[uuid.UUID | None]
    order_id: Mapped[uuid.UUID | None]
    stage: Mapped[str] = mapped_column(Text)
    decision: Mapped[str] = mapped_column(Text)
    reason_code: Mapped[str | None] = mapped_column(Text)
    probability: Mapped[Decimal | None]
    expected_edge_atr: Mapped[Decimal | None]
    # Фактичний slippage (вхід або вихід) у частках ATR (ТЗ §5.5.1).
    slippage_atr: Mapped[Decimal | None]
    details: Mapped[dict[str, Any] | None]
    ts_ms: Mapped[CreatedMs]
