"""Реєстр моделей для Alembic і домовленості типів (T0.4). Без БД."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import BigInteger, Date, DateTime, Float, Interval, Numeric, Time

from app.core.db import MODEL_MODULES, load_models

APP_DIR = Path(__file__).resolve().parents[2] / "app"


def test_all_model_modules_are_registered() -> None:
    found = {"app." + ".".join(p.relative_to(APP_DIR).with_suffix("").parts) for p in APP_DIR.rglob("models.py")}
    assert found == set(MODEL_MODULES), (
        "новий models.py має бути в app.core.db.MODEL_MODULES (інакше Alembic його не бачить)"
    )


def test_time_columns_are_bigint_ms() -> None:
    """Час — лише BIGINT мілісекунди (ТЗ §15): колонки `*_ms` мають тип BIGINT, інших часових типів немає."""
    for table in load_models().tables.values():
        for column in table.columns:
            if column.name.endswith("_ms"):
                assert isinstance(column.type, BigInteger), f"{table.name}.{column.name}: {column.type}"
            assert not isinstance(column.type, DateTime | Date | Time | Interval), f"{table.name}.{column.name}"


def test_no_float_columns() -> None:
    """Гроші, ціни, кількості — NUMERIC, не double precision (AGENTS.md, інваріант 3)."""
    for table in load_models().tables.values():
        for column in table.columns:
            is_float = isinstance(column.type, Float) or (
                isinstance(column.type, Numeric) and not column.type.asdecimal
            )
            assert not is_float, f"{table.name}.{column.name}: {column.type}"
