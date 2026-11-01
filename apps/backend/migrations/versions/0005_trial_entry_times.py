"""Час входу кожної угоди спроби поруч з `oos_results` (T2.6, ТЗ §5.2.5).

PBO методом CSCV вимагає, щоб рядок матриці був тим самим періодом для всіх спроб. Ряд R без часу
давав лише номер угоди, тож блоки різних спроб припадали на різні дати, а коротші ряди випадали.
`experiment_trials.oos_entry_ms` — час входу (мс UTC) кожного R у тому самому порядку.

Таблиця append-only, тож рядків до цієї міграції не перезаписати: вони отримують порожній масив. CHECK
довжини створюється `NOT VALID` і одразу валідується, якщо рядків з угодами без часу немає (чиста БД,
dev-БД T2.6); інакше лишається `NOT VALID` і перевіряє лише нові рядки. У PBO старий рядок не входить
мовчки — `trial_series_from_journal` відмовляє, і Gate G1 лінії закрита. Відкат прибирає колонку разом із часом.

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-01 18:00:00.000000+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LEN_CHECK = (
    "CASE WHEN jsonb_typeof(oos_results) = 'array'"
    " THEN cardinality(oos_entry_ms) = jsonb_array_length(oos_results) ELSE false END"
)


def upgrade() -> None:
    op.add_column(
        "experiment_trials",
        sa.Column(
            "oos_entry_ms",
            postgresql.ARRAY(sa.BigInteger()),
            server_default=sa.text("'{}'::bigint[]"),
            nullable=False,
        ),
    )
    # Default лише для наявних рядків: новий рядок передає час явно.
    op.alter_column("experiment_trials", "oos_entry_ms", server_default=None)
    op.execute(
        "ALTER TABLE experiment_trials ADD CONSTRAINT ck_experiment_trials_oos_entry_ms_len CHECK ("
        f"{_LEN_CHECK}) NOT VALID"
    )
    # Немає старих рядків з угодами — CHECK діє і на наявні рядки (`NOT VALID` лише там, де вони є).
    op.execute(
        "DO $$ BEGIN"
        " IF NOT EXISTS (SELECT 1 FROM experiment_trials WHERE CASE WHEN jsonb_typeof(oos_results) = 'array'"
        " THEN cardinality(oos_entry_ms) <> jsonb_array_length(oos_results) ELSE true END) THEN"
        " ALTER TABLE experiment_trials VALIDATE CONSTRAINT ck_experiment_trials_oos_entry_ms_len;"
        " END IF;"
        " END $$"
    )
    op.create_check_constraint(
        op.f("ck_experiment_trials_oos_entry_ms_non_negative"),
        "experiment_trials",
        "0 <= ALL (oos_entry_ms) AND array_position(oos_entry_ms, NULL) IS NULL",
    )


def downgrade() -> None:
    op.drop_constraint(op.f("ck_experiment_trials_oos_entry_ms_non_negative"), "experiment_trials", type_="check")
    op.drop_constraint(op.f("ck_experiment_trials_oos_entry_ms_len"), "experiment_trials", type_="check")
    op.drop_column("experiment_trials", "oos_entry_ms")
