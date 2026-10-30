"""Premium index: `open` поза діапазоном години (T1.9, ТЗ §4.4, §15; рішення docs/decisions/0009-premium-open.md).

Bybit `premium-index-price-kline` віддає як `open` close попередньої години, а `high`/`low`/`close` — лише за
поточну годину. Тому `open` часто лежить поза `[low, high]` (Testnet 2026-10-30: 67–77% годин у червні 2024).
CHECK-и 0001 вимагали `low <= open <= high`, і такі години ставали прогалинами. Тепер обмежені лише `high >= low`,
`high >= close`, `low <= close`. `mark_index_candles` і `candles` не змінюються.

`premium_index` — hypertable без compression: ALTER TABLE поширюється на chunk-и. Відкат повертає CHECK-и 0001,
а рядки, які під ними не могли б існувати, видаляє (до 0004 їх у таблиці бути не могло; наступний backfill
лишить на їхньому місці прогалину).

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-30 12:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(op.f("ck_premium_index_high_ge_open_close"), "premium_index", type_="check")
    op.drop_constraint(op.f("ck_premium_index_low_le_open_close"), "premium_index", type_="check")
    op.create_check_constraint(op.f("ck_premium_index_high_ge_close"), "premium_index", "high >= close")
    op.create_check_constraint(op.f("ck_premium_index_low_le_close"), "premium_index", "low <= close")


def downgrade() -> None:
    op.execute("DELETE FROM premium_index WHERE open > high OR open < low")
    op.drop_constraint(op.f("ck_premium_index_low_le_close"), "premium_index", type_="check")
    op.drop_constraint(op.f("ck_premium_index_high_ge_close"), "premium_index", type_="check")
    op.create_check_constraint(
        op.f("ck_premium_index_high_ge_open_close"), "premium_index", "high >= open AND high >= close"
    )
    op.create_check_constraint(
        op.f("ck_premium_index_low_le_open_close"), "premium_index", "low <= open AND low <= close"
    )
