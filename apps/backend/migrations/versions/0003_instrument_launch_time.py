"""Час лістингу в кеші інструментів (T1.7, ТЗ §8.3, §15).

`instruments.launch_time_ms` — Bybit `launchTime`, мс UTC. Скан прогалин свічок починається не раніше за
`max(BACKFILL_START_MS, перша година сітки ≥ launchTime)`. Кеш не є джерелом істини: рядки, що вже були
в таблиці, видаляються (без часу лістингу їх не відновити), наступний `InstrumentCache.refresh` заповнить
таблицю знову.

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-30 05:30:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("instruments", sa.Column("launch_time_ms", sa.BigInteger(), nullable=True))
    op.execute("DELETE FROM instruments")
    op.alter_column("instruments", "launch_time_ms", existing_type=sa.BigInteger(), nullable=False)
    op.create_check_constraint(
        op.f("ck_instruments_launch_time_ms_non_negative"),
        "instruments",
        "launch_time_ms >= 0",
    )


def downgrade() -> None:
    op.drop_constraint(op.f("ck_instruments_launch_time_ms_non_negative"), "instruments", type_="check")
    op.drop_column("instruments", "launch_time_ms")
