"""Метадані моделей і журнал досліджень (ТЗ §5.2.5, §5.3, §5.4, §5А.10, §15, T0.4).

Інваріанти на рівні БД:
- модель з `git_dirty = true` не може бути `CANDIDATE`/`APPROVED` (ТЗ §5.3);
- `CANDIDATE`/`APPROVED` мають `live_corridor` (рахується при `CANDIDATE`, ТЗ §5.7);
- `APPROVED` ставить лише людина: потрібен підтверджений запис `audit_log` з дією `model.approve`
  (`confirmed_audit_id`, перевіряє тригер `ctb_require_confirmed_audit()` у міграції);
- символ торгівлі та дослідницька лінія — у канонічному форматі (`SYMBOL_RE`);
- `experiment_trials` — append-only (тригер у міграції): спроби не видаляються й не перезаписуються (DSR/PBO);
- не більше двох відкриттів одного hold-out (`holdout_openings.opening_no ∈ {1, 2}` + `UNIQUE`, ТЗ §5А.10).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, Identity, Index, SmallInteger, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import SYMBOL_RE, Base, CreatedMs, TimeMs, enum_check, symbol_check

_SYMBOL_BODY = SYMBOL_RE.removeprefix("^").removesuffix("$")
_SYMBOLS_LIST_RE = f"^{_SYMBOL_BODY}(,{_SYMBOL_BODY})*$"

MODEL_STATUSES: tuple[str, ...] = ("TRAINED", "CANDIDATE", "APPROVED", "RETIRED")
LABEL_TYPES: tuple[str, ...] = ("meta", "direct", "rule_only")
# Стан спроби Optuna/ітерації. Невдалі й обрізані спроби теж рахуються в загальну кількість (DSR).
TRIAL_STATES: tuple[str, ...] = ("COMPLETE", "PRUNED", "FAILED")


class ModelArtifact(Base):
    """Запис про артефакт `models/<model_version>/` (ТЗ §5.4). `metadata_json` — повний `model_metadata.json`."""

    __tablename__ = "models"
    __table_args__ = (
        enum_check("status", MODEL_STATUSES),
        enum_check("label_type", LABEL_TYPES),
        CheckConstraint("char_length(model_version) > 0", name="model_version_not_empty"),
        CheckConstraint("NOT (git_dirty AND status IN ('CANDIDATE', 'APPROVED'))", name="dirty_not_promoted"),
        CheckConstraint(
            "status NOT IN ('CANDIDATE', 'APPROVED') OR live_corridor IS NOT NULL", name="promoted_has_corridor"
        ),
        CheckConstraint("status <> 'APPROVED' OR confirmed_audit_id IS NOT NULL", name="approved_by_human"),
        # Одне підтвердження — одна дія (запис audit_log не можна використати повторно).
        Index(
            "uq_models_confirmed_audit_id",
            "confirmed_audit_id",
            unique=True,
            postgresql_where=text("confirmed_audit_id IS NOT NULL"),
        ),
        symbol_check("trade_symbol"),
        # У `rule_only` файлу моделі немає (ТЗ §5.4); в інших — обов'язковий.
        CheckConstraint("(label_type = 'rule_only') = (artifact_path IS NULL)", name="artifact_path_by_label_type"),
        CheckConstraint("validated_by IS NULL OR validated_by <> model_version", name="validated_by_other"),
    )

    model_version: Mapped[str] = mapped_column(Text, primary_key=True)
    algorithm: Mapped[str] = mapped_column(Text)
    label_type: Mapped[str] = mapped_column(Text)
    trade_symbol: Mapped[str] = mapped_column(Text)
    timeframe: Mapped[str] = mapped_column(Text)
    feature_schema_hash: Mapped[str] = mapped_column(Text)
    preprocessing_version: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="TRAINED")
    git_commit: Mapped[str] = mapped_column(Text)
    git_dirty: Mapped[bool]
    library_versions: Mapped[dict[str, Any]]
    live_corridor: Mapped[dict[str, Any] | None]
    # Модель, що пройшла hold-out за тим самим рецептом (ТЗ §5.4.1).
    validated_by: Mapped[str | None] = mapped_column(Text, ForeignKey("models.model_version"))
    experiment_id: Mapped[str | None] = mapped_column(Text, ForeignKey("experiments.id"))
    artifact_path: Mapped[str | None] = mapped_column(Text)
    artifact_sha256: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict[str, Any]]
    # Підтвердження людиною (`audit_log.action = 'model.approve'`, `confirmed`) — обов'язкове для `APPROVED`.
    confirmed_audit_id: Mapped[int | None] = mapped_column(ForeignKey("audit_log.id"))
    created_at_ms: Mapped[CreatedMs]
    status_changed_at_ms: Mapped[CreatedMs]


class Experiment(Base):
    """Експеримент дослідницької лінії (ТЗ §5.2.5). Лінія = торговий символ; лічильник спроб лінії не скидається.

    Загальна кількість спроб лінії рахується з `experiment_trials`; `n_trials_line` — знімок на момент DSR.
    """

    __tablename__ = "experiments"
    __table_args__ = (
        enum_check("label_type", LABEL_TYPES),
        symbol_check("research_line"),
        CheckConstraint("char_length(id) > 0", name="id_not_empty"),
        CheckConstraint("cardinality(symbols) > 0", name="symbols_not_empty"),
        # Кожен елемент масиву — канонічний символ (`SYMBOL_RE` без якорів, через кому).
        CheckConstraint(f"array_to_string(symbols, ',') ~ '{_SYMBOLS_LIST_RE}'", name="symbols_format"),
        CheckConstraint("data_to_ms > data_from_ms", name="data_range_valid"),
        CheckConstraint("n_trials_line IS NULL OR n_trials_line > 0", name="n_trials_line_positive"),
        CheckConstraint("dsr IS NULL OR (dsr >= 0 AND dsr <= 1)", name="dsr_range"),
        CheckConstraint("pbo IS NULL OR (pbo >= 0 AND pbo <= 1)", name="pbo_range"),
    )

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    research_line: Mapped[str] = mapped_column(Text)
    symbols: Mapped[list[str]] = mapped_column(ARRAY(Text))
    timeframe: Mapped[str] = mapped_column(Text)
    label_type: Mapped[str] = mapped_column(Text)
    primary_strategy: Mapped[dict[str, Any]]
    params: Mapped[dict[str, Any]]
    seed: Mapped[int]
    data_from_ms: Mapped[TimeMs]
    data_to_ms: Mapped[TimeMs]
    n_trials_line: Mapped[int | None]
    dsr: Mapped[Decimal | None]
    pbo: Mapped[Decimal | None]
    created_at_ms: Mapped[CreatedMs]


class ExperimentTrial(Base):
    """Одна спроба (append-only). `oos_results` — ряд out-of-sample результатів (для DSR і PBO)."""

    __tablename__ = "experiment_trials"
    __table_args__ = (
        enum_check("state", TRIAL_STATES),
        CheckConstraint("trial_no >= 1", name="trial_no_positive"),
        UniqueConstraint("experiment_id", "trial_no"),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    experiment_id: Mapped[str] = mapped_column(Text, ForeignKey("experiments.id"))
    trial_no: Mapped[int]
    state: Mapped[str] = mapped_column(Text)
    params: Mapped[dict[str, Any]]
    oos_results: Mapped[list[Any]]
    metrics: Mapped[dict[str, Any]]
    created_at_ms: Mapped[CreatedMs]


class HoldoutOpening(Base):
    """Відкриття hold-out після Gate G1 (ТЗ §5А.10). Третє відкриття того самого відрізка → `IntegrityError`."""

    __tablename__ = "holdout_openings"
    __table_args__ = (
        CheckConstraint("opening_no IN (1, 2)", name="opening_no_max_2"),
        symbol_check("research_line"),
        CheckConstraint("holdout_to_ms > holdout_from_ms", name="holdout_range_valid"),
        UniqueConstraint(
            "research_line",
            "holdout_from_ms",
            "holdout_to_ms",
            "opening_no",
            name="uq_holdout_openings_window_opening_no",
        ),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    research_line: Mapped[str] = mapped_column(Text)
    holdout_from_ms: Mapped[TimeMs]
    holdout_to_ms: Mapped[TimeMs]
    opening_no: Mapped[int] = mapped_column(SmallInteger)
    experiment_id: Mapped[str] = mapped_column(Text, ForeignKey("experiments.id"))
    model_version: Mapped[str] = mapped_column(Text, ForeignKey("models.model_version"))
    result: Mapped[dict[str, Any] | None]
    opened_at_ms: Mapped[CreatedMs]
