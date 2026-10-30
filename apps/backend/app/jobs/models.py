"""Таблиця задач воркера `jobs` (ТЗ §5.6, §6.4, §15, T0.4). Цикл процесу — `app/jobs/worker.py` (T1.8)."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, Index, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base, CreatedMs, enum_check

JOB_STATUSES: tuple[str, ...] = ("QUEUED", "RUNNING", "DONE", "FAILED", "CANCELED")
JOB_TERMINAL_STATUSES: tuple[str, ...] = ("DONE", "FAILED", "CANCELED")


class Job(Base):
    """Задача воркера (Optuna, навчання, backfill). Беруться запитом `SELECT … FOR UPDATE SKIP LOCKED`.

    `dedup_key` — необов'язковий ключ ідемпотентності: поки задача з тим самим ключем у `QUEUED`/`RUNNING`,
    друга така сама не створюється (частковий `UNIQUE`).
    """

    __tablename__ = "jobs"
    __table_args__ = (
        enum_check("status", JOB_STATUSES),
        CheckConstraint("kind ~ '^[a-z][a-z0-9_]*$'", name="kind_format"),
        CheckConstraint("progress >= 0 AND progress <= 1", name="progress_range"),
        CheckConstraint("attempt >= 1", name="attempt_positive"),
        CheckConstraint(
            "status <> 'RUNNING' OR (started_at_ms IS NOT NULL AND heartbeat_at_ms IS NOT NULL)",
            name="running_has_heartbeat",
        ),
        CheckConstraint(
            "(status IN ('DONE', 'FAILED', 'CANCELED')) = (finished_at_ms IS NOT NULL)", name="finished_iff_terminal"
        ),
        Index(
            "uq_jobs_dedup_key_active",
            "dedup_key",
            unique=True,
            postgresql_where=text("status IN ('QUEUED', 'RUNNING')"),
        ),
        Index("ix_jobs_queue", "priority", "created_at_ms", postgresql_where=text("status = 'QUEUED'")),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4, server_default=text("gen_random_uuid()")
    )
    kind: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="QUEUED")
    # Менше значення — вища пріоритетність.
    priority: Mapped[int] = mapped_column(default=100)
    params: Mapped[dict[str, Any]]
    dedup_key: Mapped[str | None] = mapped_column(Text)
    progress: Mapped[Decimal] = mapped_column(default=Decimal(0))
    progress_message: Mapped[str | None] = mapped_column(Text)
    result: Mapped[dict[str, Any] | None]
    error: Mapped[str | None] = mapped_column(Text)
    attempt: Mapped[int] = mapped_column(default=1)
    created_at_ms: Mapped[CreatedMs]
    started_at_ms: Mapped[int | None]
    heartbeat_at_ms: Mapped[int | None]
    finished_at_ms: Mapped[int | None]
