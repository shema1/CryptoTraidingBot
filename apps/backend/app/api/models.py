"""Аудит-журнал дій користувача `audit_log` (ТЗ §14, §15, T0.4). Append-only (тригер у міграції).

Небезпечні дії (запуск Live, послаблення ризиків, вихід з `EMERGENCY_STOP`, затвердження моделі) мають запис тут.
На нього посилається `confirmed_audit_id` у `risk_config`, `emergency_runs`, `models`; тригер
`ctb_require_confirmed_audit()` (міграція 0001) перевіряє, що запис має `confirmed = true` і саме потрібну дію
(`AUDIT_ACTION_*` нижче) для того самого акаунта (`risk_config`, `emergency_runs`) або моделі (`models`: `target` =
`model_version`); один запис підтверджує лише одну дію (частковий `UNIQUE (confirmed_audit_id)` у кожній таблиці).
`confirmed = true` ставить лише API після явного підтвердження людини в UI (T0.7, T6.2).
Секрети в `request`/`result` не пишуться (ТЗ §14).
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import CheckConstraint, Identity, Index, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base, CreatedMs

# Дії, які підтверджують небезпечні зміни стану. Ті самі літерали — в аргументах тригерів міграції 0001.
AUDIT_ACTION_RISK_LOOSEN = "risk.config.loosen"
AUDIT_ACTION_EMERGENCY_RELEASE = "emergency.release"
AUDIT_ACTION_MODEL_APPROVE = "model.approve"


class AuditLog(Base):
    __tablename__ = "audit_log"
    __table_args__ = (
        CheckConstraint("char_length(actor) > 0", name="actor_not_empty"),
        CheckConstraint("action ~ '^[a-z][a-z0-9_.]*$'", name="action_format"),
        Index("ix_audit_log_ts_ms", "ts_ms"),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    actor: Mapped[str] = mapped_column(Text)
    action: Mapped[str] = mapped_column(Text)
    # Ціль дії — як дані, без FK: журнал не залежить від інших таблиць (і не створює циклу FK з `models`).
    account_id: Mapped[uuid.UUID | None]
    bot_id: Mapped[uuid.UUID | None]
    # Ідентифікатор цілі, якщо це не акаунт/бот (напр. `model_version` для `model.approve`).
    target: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    # Користувач явно підтвердив небезпечну дію в UI (ТЗ §14).
    confirmed: Mapped[bool] = mapped_column(default=False)
    idempotency_key: Mapped[str | None] = mapped_column(Text)
    request: Mapped[dict[str, Any] | None]
    result: Mapped[dict[str, Any] | None]
    ts_ms: Mapped[CreatedMs]
