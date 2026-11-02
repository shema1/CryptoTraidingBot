"""Позначки недійсних спроб `trial_invalidations` (T2.6, ТЗ §5.2.5, рішення 0015).

Одна домінантна спроба лінії (наприклад, з витоком) назавжди роздувала б розкид Sharpe для DSR і робила б PBO
лінії беззмістовним, а `experiment_trials` лише дописується. Людина позначає таку спробу недійсною: рядок тут
з причиною і `confirmed_audit_id` — підтвердженим записом `audit_log` з дією `trial.invalidate` і `target` = id
спроби. Таблиця append-only; одна позначка на спробу, один запис аудиту — одна позначка.

`ctb_require_confirmed_audit()` (міграція 0001) отримує ціль `'trial'`. Відкат прибирає таблицю (разом з тригерами)
і повертає функцію 0001.

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-02 17:08:23.180067+00:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Тіло функції 0001 з додатковою ціллю; `branch` — що вставити перед ELSE.
_FUNCTION = """
CREATE OR REPLACE FUNCTION ctb_require_confirmed_audit() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    ok boolean;
BEGIN
    IF NEW.confirmed_audit_id IS NULL THEN
        RETURN NEW;
    END IF;
    -- TG_ARGV[0] — потрібна дія; TG_ARGV[1] — ціль: 'account' (той самий акаунт), 'model' (target = model_version){scope_note}.
    IF TG_ARGV[1] = 'account' THEN
        SELECT EXISTS (
            SELECT 1 FROM audit_log a
            WHERE a.id = NEW.confirmed_audit_id AND a.action = TG_ARGV[0] AND a.confirmed
                AND a.account_id = NEW.account_id
        ) INTO ok;
    ELSIF TG_ARGV[1] = 'model' THEN
        SELECT EXISTS (
            SELECT 1 FROM audit_log a
            WHERE a.id = NEW.confirmed_audit_id AND a.action = TG_ARGV[0] AND a.confirmed
                AND a.target = NEW.model_version
        ) INTO ok;{branch}
    ELSE
        RAISE EXCEPTION 'ctb_require_confirmed_audit: unknown scope %', TG_ARGV[1];
    END IF;
    IF NOT ok THEN
        RAISE EXCEPTION '%: confirmed_audit_id % must be a confirmed audit_log entry with action % for the same %',
            TG_TABLE_NAME, NEW.confirmed_audit_id, TG_ARGV[0], TG_ARGV[1]
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END
$$
"""
_TRIAL_BRANCH = """
    ELSIF TG_ARGV[1] = 'trial' THEN
        SELECT EXISTS (
            SELECT 1 FROM audit_log a
            WHERE a.id = NEW.confirmed_audit_id AND a.action = TG_ARGV[0] AND a.confirmed
                AND a.target = NEW.trial_id::text
        ) INTO ok;"""


def upgrade() -> None:
    op.create_table(
        "trial_invalidations",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("trial_id", sa.BigInteger(), nullable=False),
        sa.Column("confirmed_audit_id", sa.BigInteger(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_at_ms", sa.BigInteger(), nullable=False),
        sa.CheckConstraint("char_length(btrim(reason)) > 0", name=op.f("ck_trial_invalidations_reason_not_empty")),
        sa.ForeignKeyConstraint(
            ["confirmed_audit_id"],
            ["audit_log.id"],
            name=op.f("fk_trial_invalidations_confirmed_audit_id_audit_log"),
        ),
        sa.ForeignKeyConstraint(
            ["trial_id"],
            ["experiment_trials.id"],
            name=op.f("fk_trial_invalidations_trial_id_experiment_trials"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_trial_invalidations")),
        sa.UniqueConstraint("confirmed_audit_id", name=op.f("uq_trial_invalidations_confirmed_audit_id")),
        sa.UniqueConstraint("trial_id", name=op.f("uq_trial_invalidations_trial_id")),
    )
    op.execute(_FUNCTION.format(scope_note=" або 'trial' (target = id спроби)", branch=_TRIAL_BRANCH))
    op.execute(
        "CREATE TRIGGER trg_trial_invalidations_append_only BEFORE UPDATE OR DELETE ON trial_invalidations"
        " FOR EACH ROW EXECUTE FUNCTION ctb_forbid_mutation()"
    )
    op.execute(
        "CREATE TRIGGER trg_trial_invalidations_no_truncate BEFORE TRUNCATE ON trial_invalidations"
        " FOR EACH STATEMENT EXECUTE FUNCTION ctb_forbid_mutation()"
    )
    op.execute(
        "CREATE TRIGGER trg_trial_invalidations_confirmed_audit BEFORE INSERT ON trial_invalidations"
        " FOR EACH ROW EXECUTE FUNCTION ctb_require_confirmed_audit('trial.invalidate', 'trial')"
    )


def downgrade() -> None:
    op.drop_table("trial_invalidations")
    op.execute(_FUNCTION.format(scope_note="", branch=""))
