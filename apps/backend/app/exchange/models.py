"""Таблиці акаунтів біржі та кешу інструментів (ТЗ §6.4, §14, §15, T0.4)."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, PrimaryKeyConstraint, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import MODES, Base, CreatedMs, Money, TimeMs, enum_check, symbol_check

EXCHANGES: tuple[str, ...] = ("bybit",)

# Результат перевірки дозволів ключа при старті (ТЗ §14, T3.6):
# OK — права виводу немає; WITHDRAW_ENABLED — є право виводу (боти режиму не стартують);
# KEY_MISSING — змінних ключа немає в .env; UNVERIFIABLE — API не дозволяє перевірити (ручне підтвердження).
KEY_CHECK_RESULTS: tuple[str, ...] = ("OK", "WITHDRAW_ENABLED", "KEY_MISSING", "UNVERIFIABLE")


class ExchangeAccount(Base):
    """Акаунт біржі. Ключів тут немає — лише назва набору змінних `.env` (`env_key_set`, ТЗ §14).

    Ризик-ліміти, equity та Emergency Stop ведуться окремо для кожного акаунта (ТЗ §8.1).
    """

    __tablename__ = "exchange_accounts"
    __table_args__ = (
        enum_check("exchange", EXCHANGES),
        enum_check("mode", MODES),
        enum_check("key_check_result", KEY_CHECK_RESULTS),
        CheckConstraint("char_length(name) > 0", name="name_not_empty"),
        # Набір змінних `BYBIT_<MODE>` → BYBIT_<MODE>_API_KEY / BYBIT_<MODE>_API_SECRET (ТЗ §14).
        CheckConstraint("env_key_set ~ '^[A-Z][A-Z0-9_]*$'", name="env_key_set_format"),
        # HOST_ID: 2 символи [a-z0-9] (ТЗ §6.4).
        CheckConstraint("allowed_host_id ~ '^[a-z0-9]{2}$'", name="allowed_host_id_format"),
        CheckConstraint("(key_check_result IS NULL) = (key_check_at_ms IS NULL)", name="key_check_result_with_time"),
        UniqueConstraint("name"),
        # Два записи з тими самими ключами = один реальний акаунт з розділеними лімітами ризику. Заборонено.
        UniqueConstraint("env_key_set"),
        # Ціль складених FK (account_id, mode): режим бота/ордера/позиції/fill = режим акаунта.
        UniqueConstraint("id", "mode"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text)
    exchange: Mapped[str] = mapped_column(Text)
    mode: Mapped[str] = mapped_column(Text)
    env_key_set: Mapped[str] = mapped_column(Text)
    allowed_host_id: Mapped[str] = mapped_column(Text)
    key_check_result: Mapped[str | None] = mapped_column(Text)
    key_check_at_ms: Mapped[int | None]
    # Перелік дозволів ключа, як його повернула біржа (без самого ключа).
    key_check_details: Mapped[dict[str, Any] | None]
    created_at_ms: Mapped[CreatedMs]
    updated_at_ms: Mapped[CreatedMs]


class Instrument(Base):
    """Кеш правил інструмента (ТЗ §8.3, §15). Ключ включає `mode`: на Testnet правила можуть відрізнятися."""

    __tablename__ = "instruments"
    __table_args__ = (
        PrimaryKeyConstraint("exchange", "mode", "symbol"),
        enum_check("exchange", EXCHANGES),
        symbol_check(),
        enum_check("mode", MODES),
        CheckConstraint("tick_size > 0", name="tick_size_positive"),
        CheckConstraint("qty_step > 0", name="qty_step_positive"),
        CheckConstraint("min_qty > 0", name="min_qty_positive"),
        CheckConstraint("min_notional >= 0", name="min_notional_non_negative"),
        CheckConstraint("max_qty IS NULL OR max_qty >= min_qty", name="max_qty_ge_min_qty"),
        CheckConstraint("max_leverage >= 1", name="max_leverage_ge_1"),
        CheckConstraint("char_length(status) > 0", name="status_not_empty"),
    )

    exchange: Mapped[str] = mapped_column(Text)
    mode: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    tick_size: Mapped[Money]
    qty_step: Mapped[Money]
    min_qty: Mapped[Money]
    min_notional: Mapped[Money]
    max_qty: Mapped[Decimal | None]
    max_leverage: Mapped[Money]
    # Статус інструмента так, як його повертає біржа; «торгується чи ні» визначає адаптер (T1.7).
    status: Mapped[str] = mapped_column(Text)
    updated_at_ms: Mapped[TimeMs]
