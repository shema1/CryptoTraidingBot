"""Таблиці локального API: аудит-журнал `audit_log` (ТЗ §14, §15, T0.4) і ключі ідемпотентності `api_idempotency_keys`
(ТЗ §16, T0.7, рішення `docs/decisions/0007-local-api.md`).

`audit_log` — append-only (тригер у міграції 0001).

Небезпечні дії (запуск Live, послаблення ризиків, вихід з `EMERGENCY_STOP`, затвердження моделі) мають запис тут.
На нього посилається `confirmed_audit_id` у `risk_config`, `emergency_runs`, `models`; тригер
`ctb_require_confirmed_audit()` (міграція 0001) перевіряє, що запис має `confirmed = true` і саме потрібну дію
(`AUDIT_ACTION_*` нижче) для того самого акаунта (`risk_config`, `emergency_runs`) або моделі (`models`: `target` =
`model_version`); один запис підтверджує лише одну дію (частковий `UNIQUE (confirmed_audit_id)` у кожній таблиці).
`confirmed = true` ставить лише API після явного підтвердження людини в UI (T0.7, T6.2) і торговий процес при ручному
перехопленні lock після введення підтвердження в терміналі (`AUDIT_ACTION_FORCE_TAKEOVER`, T0.6).
Секрети в `request`/`result` не пишуться (ТЗ §14).
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import CheckConstraint, Identity, Index, Integer, LargeBinary, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base, CreatedMs, TimeMs, enum_check

# Дії, які підтверджують небезпечні зміни стану. Ті самі літерали — в аргументах тригерів міграції 0001.
AUDIT_ACTION_RISK_LOOSEN = "risk.config.loosen"
AUDIT_ACTION_EMERGENCY_RELEASE = "emergency.release"
AUDIT_ACTION_MODEL_APPROVE = "model.approve"
# Ручне перехоплення single-instance lock (`python -m app` з прапорцем перехоплення, ТЗ §6.4, T0.6). Пишеться
# до завершення сесії-власника; ні на що не посилається (не підтверджує зміну стану в інших таблицях).
AUDIT_ACTION_FORCE_TAKEOVER = "instance.force_takeover"


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


# Ключі ідемпотентності змінюючих запитів API (`app/api/idempotency.py`). Ті самі значення — у коді middleware.
IDEMPOTENCY_KEY_MIN_LEN = 16
IDEMPOTENCY_KEY_MAX_LEN = 128
IDEMPOTENCY_KEY_RE = f"^[A-Za-z0-9_.:-]{{{IDEMPOTENCY_KEY_MIN_LEN},{IDEMPOTENCY_KEY_MAX_LEN}}}$"
IDEMPOTENCY_METHODS: tuple[str, ...] = ("POST", "PUT", "PATCH", "DELETE")
IDEMPOTENCY_STATUSES: tuple[str, ...] = ("IN_PROGRESS", "COMPLETED", "FAILED")


class ApiIdempotencyKey(Base):
    """Змінюючий запит API з `Idempotency-Key` і його збережена відповідь (ТЗ §16, T0.7).

    Рядок вставляється (`IN_PROGRESS`) і комітиться **до** виконання обробника; повтор з тим самим ключем отримує
    збережену відповідь (`COMPLETED`) або 409, але ніколи не виконується вдруге. `FAILED` — обробник упав, результат
    невідомий. `owner` — екземпляр процесу API: `IN_PROGRESS` іншого власника = процес упав посеред обробки.
    Переходи — лише з `IN_PROGRESS` (умовний `UPDATE` у `DbIdempotencyStore`).
    """

    __tablename__ = "api_idempotency_keys"
    __table_args__ = (
        CheckConstraint(f"idempotency_key ~ '{IDEMPOTENCY_KEY_RE}'", name="key_format"),
        enum_check("method", IDEMPOTENCY_METHODS),
        CheckConstraint("request_hash ~ '^[0-9a-f]{64}$'", name="request_hash_format"),
        CheckConstraint("char_length(owner) BETWEEN 1 AND 64", name="owner_not_empty"),
        enum_check("status", IDEMPOTENCY_STATUSES),
        CheckConstraint("response_status BETWEEN 100 AND 599", name="response_status_range"),
        CheckConstraint("(status = 'IN_PROGRESS') = (completed_ms IS NULL)", name="completed_ms_iff_finished"),
        CheckConstraint(
            "(status = 'COMPLETED') = "
            "(response_status IS NOT NULL AND response_headers IS NOT NULL AND response_body IS NOT NULL)",
            name="response_iff_completed",
        ),
    )

    idempotency_key: Mapped[str] = mapped_column(Text, primary_key=True)
    method: Mapped[str] = mapped_column(Text)
    # Шлях запиту — для діагностики; ідентичність запиту — `request_hash` (метод, шлях, query, тіло).
    path: Mapped[str] = mapped_column(Text)
    request_hash: Mapped[str] = mapped_column(Text)
    owner: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    response_status: Mapped[int | None] = mapped_column(Integer)
    # Заголовки відповіді — список пар [назва, значення] без `content-length`.
    response_headers: Mapped[list[Any] | None]
    response_body: Mapped[bytes | None] = mapped_column(LargeBinary)
    created_ms: Mapped[TimeMs]
    completed_ms: Mapped[int | None]
