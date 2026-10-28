"""Спільна основа БД: `Base` для SQLAlchemy-моделей, домовленості про типи, engine і сесії (ТЗ §15, T0.4).

Домовленості схеми (правила — `.claude/rules/database.md`):
- час — `BIGINT` мілісекунди UTC (`TimeMs`), значення за замовчуванням ставить код через `clock.now_ms()`
  (`server_default` на час не використовується: тести підміняють годинник);
- гроші, ціни, кількості — `NUMERIC` без обмеження точності (`Money`), у Python — `Decimal`;
- статуси — `TEXT` + `CHECK` (`enum_check()`), набір значень — як у ТЗ;
- назви обмежень — за `NAMING_CONVENTION`, щоб міграції були детермінованими.

Моделі лежать у `app/<модуль>/models.py`; Alembic (`migrations/env.py`) імпортує їх через `load_models()`.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterable
from decimal import Decimal
from typing import Annotated, Any

from sqlalchemy import BigInteger, CheckConstraint, MetaData, Numeric
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, mapped_column

from app.core.clock import now_ms

NAMING_CONVENTION: dict[str, str] = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

# Модулі з моделями. Новий `models.py` додається сюди — інакше Alembic його не побачить
# (тест `tests/db/test_models_registry.py::test_all_model_modules_are_registered`).
MODEL_MODULES: tuple[str, ...] = (
    "app.api.models",
    "app.exchange.models",
    "app.data.models",
    "app.ml.models",
    "app.jobs.models",
    "app.trading.models",
    "app.risk.models",
)

# Режими торгівлі (ТЗ §13). Поле `mode` обов'язкове в ордерах, позиціях і fills.
MODES: tuple[str, ...] = ("demo", "testnet", "live")

# Символ — лише уніфікований формат CCXT для USDT-perpetual (`BTC/USDT:USDT`, `1000PEPE/USDT:USDT`).
# Один канонічний запис потрібен, щоб `BTCUSDT` і `BTC/USDT:USDT` не обходили унікальність бота й позиції.
SYMBOL_RE = "^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$"


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    type_annotation_map = {  # noqa: RUF012 — атрибут конфігурації SQLAlchemy
        int: BigInteger(),
        Decimal: Numeric(),
        dict[str, Any]: JSONB(),
        list[Any]: JSONB(),
    }


# Час: BIGINT мс UTC. `TimeMs` — обов'язкове поле без default; `CreatedMs` — заповнюється `now_ms()` при вставці.
TimeMs = Annotated[int, mapped_column(BigInteger)]
CreatedMs = Annotated[int, mapped_column(BigInteger, default=now_ms)]
Money = Annotated[Decimal, mapped_column(Numeric)]


def sql_list(values: Iterable[str]) -> str:
    """`('A', 'B')` для SQL `IN`. Значення — лише константи з коду (не введення користувача)."""
    items = list(values)
    if not items or any("'" in v for v in items):
        raise ValueError("порожній список або лапка в значенні")
    return "(" + ", ".join(f"'{v}'" for v in items) + ")"


def enum_check(column: str, values: Iterable[str], name: str | None = None) -> CheckConstraint:
    """`CHECK (column IN (...))` — статус як `TEXT` з фіксованим набором значень."""
    return CheckConstraint(f"{column} IN {sql_list(values)}", name=name or column)


def symbol_check(column: str = "symbol") -> CheckConstraint:
    """`CHECK` канонічного формату символу (`SYMBOL_RE`); NULL дозволений там, де колонка nullable."""
    return CheckConstraint(f"{column} ~ '{SYMBOL_RE}'", name=f"{column}_format")


def load_models() -> MetaData:
    """Імпортувати всі модулі моделей і повернути спільний `MetaData`."""
    for module in MODEL_MODULES:
        importlib.import_module(module)
    return Base.metadata


def create_engine(url: URL, **kwargs: Any) -> AsyncEngine:
    """Async engine (asyncpg). Розмір пулу, таймаути — параметрами викликача."""
    return create_async_engine(url, pool_pre_ping=True, **kwargs)


def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Фабрика сесій. `expire_on_commit=False`: об'єкти лишаються читабельними після коміту.

    Транзакції короткі; мережеві виклики до біржі всередині транзакції заборонені (ТЗ §7.2, write-ahead).
    """
    return async_sessionmaker(engine, expire_on_commit=False)
