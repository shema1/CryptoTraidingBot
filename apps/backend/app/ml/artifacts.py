"""Артефакти моделі, статуси і перенавчання рецепта (ТЗ §5.3, §5.4, §5.4.1, §5.7, T2.11).

Каталог `models/<model_version>/` пише лише цей модуль: `model_metadata.json`, для `meta` ще
`model.txt` і `calibrator.json`; `direct` має той самий склад (рішення 0022). `rule_only` файлу моделі не має.

Статуси: `TRAINED → CANDIDATE → APPROVED → RETIRED`. `CANDIDATE` ще можна зняти в `RETIRED`,
не проходячи Live. `APPROVED` ставить лише людина підтвердженим `audit_log` `model.approve`.
`git_dirty` не стає `CANDIDATE`. Коридор рахується в момент переходу в `CANDIDATE` і далі не змінюється.

Hold-out перевіряє рецепт моделі допуску — тієї, що вчилась лише до hold-out розбивки експерименту
за рецептом спроби, яка пройшла Gate G1. Умова 1 — уже в записі відкриття. Умови 2–4 рахує
`record_holdout_admission` сам, зі звітів бектесту на запечатаному вікні, з часткою фолдів із G1,
лімітом просадки Risk Engine за замовчуванням і seed артефакту. Назад у walk-forward вікно не
розширюється (рішення 0018).
Другий допуск тієї самої моделі чи спроби на тому самому hold-out відхиляється.

`CANDIDATE` — лише модель, яку зареєстрував `retrain_registered_recipe`: він відтворює модель допуску
тим самим рецептом (той самий файл), читає git сам і вчить той самий рецепт на всіх даних із hold-out.
Рядки hold-out віддає лише `rows_for_opened_holdout`.

Інференс читає модель через `load_registered_model`: файли мають збігтися з рядком БД і своїми хешами.
Інференс блокується, якщо `feature_schema_hash`, `preprocessing_version` або версія `lightgbm`
(і `xgboost`, якщо вона записана) не збігаються з поточним кодом. Live стартує лише з `APPROVED`.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
import json
import os
import platform
import re
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
from pydantic import BaseModel
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.models import AUDIT_ACTION_MODEL_APPROVE, AuditLog
from app.core.clock import now_ms
from app.core.db import MODES, SYMBOL_RE
from app.core.logging import get_logger
from app.exchange.types import PositionSide
from app.ml.backtest import BacktestReport, BacktestTrade
from app.ml.decision import (
    DecisionRule,
    EntryDecision,
    ProbabilityCalibrator,
    calibrator_from_payload,
    decide_entry,
)
from app.ml.features import PREPROCESSING_VERSION, SymbolData, feature_schema_hash
from app.ml.meta_model import (
    SIDE_COLUMN,
    HoldoutGrant,
    MetaTrainError,
    _grant_holdout,
    fit_frozen_meta,
)
from app.ml.models import LABEL_TYPES, Experiment, ExperimentTrial, HoldoutOpening, ModelArtifact
from app.ml.statistics import (
    DEFAULT_BOOTSTRAP_REPLICATES,
    DSR_MIN,
    MIN_OOS_TRADES,
    PBO_MAX,
    PROFITABLE_FOLD_SHARE,
    block_bootstrap_pf,
    corridor_payload,
    live_corridor,
)
from app.ml.strategy import PRIMARY_CANDIDATES, StrategySpec, SymbolHistory
from app.ml.trade_sim import TargetParams
from app.ml.validation import (
    DIRECT_STAGE,
    ResearchSplit,
    SampleRow,
    SplitLeakError,
    _lock_line,
    _report_matches_trades,
    _signal_keys,
    costs_payload,
    is_holdout_row,
    split_from_payload,
    target_payload,
)
from app.risk.costs import CostModel
from app.risk.limits import RiskLimits
from app.risk.numbers import fixed_context, require_decimal

METADATA_FILE = "model_metadata.json"
MODEL_FILE = "model.txt"
CALIBRATOR_FILE = "calibrator.json"
GIT_TIMEOUT_S = 10

HOLDOUT_MIN_TRADES = 30
HOLDOUT_MIN_NET_PF = Decimal("1.2")
PF_LOWER_BOUND = Decimal(1)
# Пороги, спільні з Gate G1, — з `statistics.py` (одне джерело). ТЗ §5.3 п. 1: G1 з послабленими
# порогами не дає CANDIDATE.
OOS_MIN_TRADES = MIN_OOS_TRADES
FOLD_SHARE_MIN = PROFITABLE_FOLD_SHARE
CANDIDATE_MIN_DSR = DSR_MIN
CANDIDATE_MAX_PBO = PBO_MAX
# Коридор — правило зупинки live (ТЗ §5.7): число реплік не задає викликач.
CORRIDOR_REPLICATES = DEFAULT_BOOTSTRAP_REPLICATES
# Ліміт просадки допуску — значення за замовчуванням Risk Engine (ТЗ §5.3 п. 3), не аргумент викликача.
HOLDOUT_MAX_DRAWDOWN_PCT: Decimal = RiskLimits.model_fields["max_drawdown_pct"].default

REASON_MIN_TRADES = "HOLDOUT_MIN_TRADES"
REASON_NET_PF = "HOLDOUT_NET_PF"
REASON_OOS_TRADES = "HOLDOUT_OOS_TRADES"
REASON_PF_LOWER = "HOLDOUT_PF_LOWER"
REASON_DRAWDOWN = "HOLDOUT_DRAWDOWN"
REASON_FOLD_SHARE = "HOLDOUT_FOLD_SHARE"
REASON_FILTER = "HOLDOUT_FILTER"

_LOG = get_logger("app.ml.artifacts")
_TRACKED_LIBRARIES = ("lightgbm", "numpy", "pandas", "scikit-learn")
_TRANSITIONS: dict[str, frozenset[str]] = {
    "TRAINED": frozenset({"CANDIDATE"}),
    "CANDIDATE": frozenset({"APPROVED", "RETIRED"}),
    "APPROVED": frozenset({"RETIRED"}),
    "RETIRED": frozenset(),
}
_TRADABLE: dict[str, frozenset[str]] = {
    "demo": frozenset({"CANDIDATE", "APPROVED"}),
    "testnet": frozenset({"CANDIDATE", "APPROVED"}),
    "live": frozenset({"APPROVED"}),
}
_LGBM_DECIMAL = frozenset(
    {
        "learning_rate",
        "min_child_weight",
        "subsample",
        "colsample_bytree",
        "reg_alpha",
        "reg_lambda",
        "min_split_gain",
    }
)
_LGBM_INT = frozenset({"num_leaves", "max_depth", "n_estimators", "min_child_samples", "subsample_freq", "n_jobs"})
_LGBM_BOOL = frozenset({"deterministic", "force_col_wise"})
_LGBM_KEYS = _LGBM_DECIMAL | _LGBM_INT | _LGBM_BOOL | {"boosting_type", "class_weight"}
_SYMBOL = re.compile(SYMBOL_RE)
# Порядок класів моделі direct (`direct_model.CLASS_LABELS`). Тест звіряє, бо імпорт звідти дав би цикл.
_DIRECT_CLASSES: tuple[str, ...] = ("FLAT", "LONG", "SHORT")
_PROBABILITY_QUANTUM = Decimal("0.0000000001")


class ArtifactError(ValueError):
    """Артефакт, перехід або допуск зіпсовані. Новий вхід через це не відкривається."""


class InferenceBlocked(RuntimeError):
    """Схема фіч, препроцесинг або версія бібліотеки не збігаються. Входу немає."""


class ModelNotTradable(ArtifactError):
    """Цей статус моделі не торгує в заданому режимі. Live приймає лише APPROVED."""


@dataclass(frozen=True, slots=True)
class GitState:
    """Коміт, на якому зібрано артефакт. `dirty` забороняє CANDIDATE."""

    commit: str
    dirty: bool


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    """Умови 2–4 ТЗ §5.3 на запечатаному hold-out. `passed` хибний — причини в `reasons`.

    Менше `HOLDOUT_MIN_TRADES` угод — відмова, не розширення (рішення 0018). `holdout_entry_ms` і
    `holdout_r` — угоди hold-out у порядку часу: з ними `promote_to_candidate` звіряє угоди коридора.
    """

    passed: bool
    reasons: tuple[str, ...]
    window_from_ms: int
    window_to_ms: int
    holdout_trades: int
    holdout_net_pf: Decimal | None
    holdout_pf_unbounded: bool
    oos_trades: int
    max_drawdown_pct: Decimal
    max_drawdown_limit_pct: Decimal
    lower_pf_above_one: bool
    profitable_fold_share: Decimal
    filtered_net: Decimal
    unfiltered_net: Decimal | None
    scored_unfiltered_net: Decimal | None
    seed: int
    n_replicates: int
    holdout_entry_ms: tuple[int, ...]
    holdout_r: tuple[Decimal, ...]

    def payload(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "reasons": list(self.reasons),
            "window_from_ms": self.window_from_ms,
            "window_to_ms": self.window_to_ms,
            "holdout_trades": self.holdout_trades,
            "holdout_net_pf": _optional_text(self.holdout_net_pf),
            "holdout_pf_unbounded": self.holdout_pf_unbounded,
            "oos_trades": self.oos_trades,
            "max_drawdown_pct": format(self.max_drawdown_pct, "f"),
            "max_drawdown_limit_pct": format(self.max_drawdown_limit_pct, "f"),
            "lower_pf_above_one": self.lower_pf_above_one,
            "profitable_fold_share": format(self.profitable_fold_share, "f"),
            "filtered_net": format(self.filtered_net, "f"),
            "unfiltered_net": _optional_text(self.unfiltered_net),
            "scored_unfiltered_net": _optional_text(self.scored_unfiltered_net),
            "seed": self.seed,
            "n_replicates": self.n_replicates,
            "holdout_entry_ms": list(self.holdout_entry_ms),
            "holdout_r": [format(value, "f") for value in self.holdout_r],
        }


@dataclass(frozen=True, slots=True)
class LoadedModel:
    """Прочитаний каталог. Бустер і калібратор є в `meta` і `direct`; `rule_only` — ні.

    `db_status` ставить лише `load_registered_model`: статус із БД на момент читання. `load_model` лишає
    `None`, і така модель не торгує (`infer_entry`, `accept_primary`).
    """

    directory: Path
    metadata: dict[str, Any]
    booster: lgb.Booster | None
    calibrator: ProbabilityCalibrator | None
    db_status: str | None = None


def repo_models_dir() -> Path:
    """Корінь репозиторію `models/`. Тести передають свій каталог."""
    return Path(__file__).resolve().parents[4] / "models"


def read_git_state(root: Path) -> GitState:
    """`git_commit` і `git_dirty`. Якщо git не відповів або завис — dirty, щоб модель не стала CANDIDATE."""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],  # noqa: S607 — фіксована команда git без shell
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_S,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"],  # noqa: S607 — фіксована команда git без shell
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_S,
        ).stdout
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return GitState(commit="unknown", dirty=True)
    if not commit or any(character.isspace() for character in commit):
        return GitState(commit="unknown", dirty=True)
    return GitState(commit=commit, dirty=bool(status.strip()))


def current_library_versions() -> dict[str, str]:
    """Версії встановлених бібліотек. `pandas` і `xgboost` — лише якщо пакет є.

    Фічі рахуються на numpy, тож pandas у залежностях немає. Інференс звіряє lightgbm і,
    якщо версію записано у файл, xgboost.
    """
    found = {"python": platform.python_version()}
    for name in _TRACKED_LIBRARIES:
        try:
            found[name] = package_version(name)
        except PackageNotFoundError:
            if name != "pandas":
                raise ArtifactError(f"немає метаданих пакета {name}") from None
    with contextlib.suppress(PackageNotFoundError):
        found["xgboost"] = package_version("xgboost")
    return found


def hash_candles(rows: Sequence[tuple[str, int, Decimal, Decimal, Decimal, Decimal]]) -> str:
    """`sha256` свічок, на яких зібрано артефакт. Порядок не має значення."""
    lines: list[str] = []
    for symbol, time_ms, open_, high, low, close in sorted(rows, key=lambda item: (item[0], item[1])):
        if not _SYMBOL.fullmatch(symbol) or type(time_ms) is not int or time_ms < 0:
            raise ArtifactError("свічка для data_hash зіпсована")
        for price in (open_, high, low, close):
            require_decimal("price", price, positive=True)
        lines.append(
            f"{symbol},{time_ms},{format(open_, 'f')},{format(high, 'f')},{format(low, 'f')},{format(close, 'f')}"
        )
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    return f"sha256:{digest}"


def hash_inputs(histories: Sequence[SymbolHistory], feature_data: Mapping[str, SymbolData] | None) -> str:
    """`data_hash` — усі ряди, з якими вчиться модель: свічки last і mark, funding, інструмент і деривативи фіч.

    Канонічний JSON (Decimal рядком, float — hex), символи за алфавітом. Рядок викликача не приймається.
    """
    payload = {
        "histories": [_canonical(history) for history in sorted(histories, key=lambda item: item.symbol)],
        "feature_data": None
        if feature_data is None
        else {symbol: _canonical(feature_data[symbol]) for symbol in sorted(feature_data)},
    }
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


def decision_payload(rule: DecisionRule, calibrator: ProbabilityCalibrator) -> dict[str, Any]:
    """Правило входу рецепта для журналу спроби G1 (`params.decision`) і звірки допуску.

    `calibrator_sha256` — хеш того самого тексту, що `write_artifact` пише в `calibrator.json`.
    """
    if type(rule) is not DecisionRule or type(calibrator) is not ProbabilityCalibrator:
        raise ArtifactError("правило входу — DecisionRule і ProbabilityCalibrator")
    text = _json_text(calibrator.payload())
    return {
        "decision_rule": rule.metadata(),
        "calibrator_sha256": "sha256:" + hashlib.sha256(text.encode()).hexdigest(),
    }


def allocate_version(root: Path, label_type: str, *, at_ms: int | None = None) -> str:
    """`lgbm-meta-YYYY-MM-DD-001`. День — з годинника проєкту, не з локальної півночі."""
    if label_type not in LABEL_TYPES:
        raise ArtifactError("невідомий label_type")
    stamp = datetime.fromtimestamp(_millis(at_ms if at_ms is not None else now_ms()) / 1000, tz=UTC)
    prefix = {"meta": "lgbm-meta", "direct": "lgbm-direct", "rule_only": "rule-only"}[label_type]
    head = f"{prefix}-{stamp.strftime('%Y-%m-%d')}-"
    taken = {path.name for path in root.iterdir()} if root.exists() else set()
    for number in range(1, 1000):
        version = f"{head}{number:03d}"
        if version not in taken:
            return version
    raise ArtifactError("на цей день вже 999 версій")


def strategy_from_metadata(payload: Mapping[str, Any]) -> StrategySpec:
    """`primary_strategy` — `StrategySpec.metadata()` одного кандидата, не словник вибору."""
    if set(payload) != {"name", "params"} or not isinstance(payload["params"], Mapping):
        raise ArtifactError("primary_strategy — name і params одного правила")
    params = dict(payload["params"])
    if set(params) - {"fast", "slow", "lookback"}:
        raise ArtifactError("у параметрах правила зайве поле")
    spec = StrategySpec(
        name=str(payload["name"]),
        fast=params.get("fast"),
        slow=params.get("slow"),
        lookback=params.get("lookback"),
    )
    if spec.metadata() != {"name": spec.name, "params": params}:
        raise ArtifactError("primary_strategy не збігається зі StrategySpec")
    if spec not in PRIMARY_CANDIDATES:
        raise ArtifactError("артефакт лише для зафіксованого кандидата")
    return spec


def build_metadata(
    *,
    model_version: str,
    label_type: str,
    spec: StrategySpec | None,
    trade_symbol: str,
    train_symbols: Sequence[str],
    feature_columns: Sequence[str],
    target: TargetParams,
    costs: CostModel,
    rule: DecisionRule,
    experiment_id: str,
    seed: int,
    data_hash: str,
    git: GitState,
    library_versions: Mapping[str, str],
    train_range_ms: tuple[int, int],
    recipe_lgbm: Mapping[str, Any] | None,
    coverage_ms: tuple[int, int],
    train_rows: int,
    include_holdout: bool,
    calibrator_method: str | None,
    validated_by: str | None = None,
    validated_opening_id: int | None = None,
) -> dict[str, Any]:
    """`model_metadata.json` до запису файлу моделі. Статус — `TRAINED`, коридора ще немає.

    Числа — рядки Decimal. `live_corridor`, DSR, PBO і `n_trials_total` заповнює перехід у CANDIDATE
    з відкриття hold-out, не зі знімка експерименту.
    """
    if label_type not in LABEL_TYPES:
        raise ArtifactError("невідомий label_type")
    # `direct` не має первинного правила (рішення 0022): поле `primary_strategy` — `{}`, як у `experiments`.
    if label_type == "direct":
        if spec is not None:
            raise ArtifactError("direct не має первинного правила")
        primary: dict[str, Any] = {}
    else:
        if type(spec) is not StrategySpec or spec not in PRIMARY_CANDIDATES:
            raise ArtifactError("артефакт лише для зафіксованого кандидата")
        if spec.metadata()["name"] != spec.name:
            raise ArtifactError("primary_strategy зіпсований")
        primary = spec.metadata()
    columns = tuple(feature_columns)
    if not columns or len(columns) != len(set(columns)):
        raise ArtifactError("колонки фіч порожні або повторюються")
    if SIDE_COLUMN in columns:
        raise ArtifactError("side_long не входить у feature_schema_hash")
    _symbol(trade_symbol)
    symbols = tuple(train_symbols)
    if trade_symbol not in symbols or len(symbols) != len(set(symbols)):
        raise ArtifactError("торговий символ має бути серед train_symbols без повторів")
    for symbol in symbols:
        _symbol(symbol)
    if type(seed) is not int or isinstance(seed, bool) or seed < 0:
        raise ArtifactError("seed має бути невід'ємний int")
    if type(git) is not GitState or not git.commit:
        raise ArtifactError("git-стан зіпсований")
    if not str(data_hash).startswith("sha256:") or len(data_hash) != 7 + 64:
        raise ArtifactError("data_hash має бути sha256")
    start, end = train_range_ms
    if type(start) is not int or type(end) is not int or end <= start:
        raise ArtifactError("train_range_ms зіпсований")
    cover_start, cover_end = coverage_ms
    if type(cover_start) is not int or type(cover_end) is not int or cover_end <= cover_start:
        raise ArtifactError("покриття фіч зіпсоване")
    if type(train_rows) is not int or train_rows < 1:
        raise ArtifactError("train_rows має бути ціле ≥ 1")
    if type(include_holdout) is not bool:
        raise ArtifactError("include_holdout має бути bool")
    if label_type == "rule_only":
        if recipe_lgbm is not None or calibrator_method is not None:
            raise ArtifactError("rule_only без LightGBM і без калібратора")
        algorithm = "rule"
    elif label_type == "meta":
        if recipe_lgbm is None or calibrator_method not in ("isotonic", "platt"):
            raise ArtifactError("meta потребує рецепт LightGBM і калібратор")
        algorithm = "lightgbm"
    else:
        # Калібратор direct — той самий, що в журналі (`decision_payload`), файл `calibrator.json` (рішення 0022).
        if recipe_lgbm is None or calibrator_method not in ("isotonic", "platt"):
            raise ArtifactError("direct потребує рецепт LightGBM і калібратор")
        algorithm = "lightgbm"
    if validated_by is not None and (
        type(validated_by) is not str or not validated_by or validated_by == model_version
    ):
        raise ArtifactError("validated_by — інша версія")
    if validated_opening_id is not None and (type(validated_opening_id) is not int or validated_opening_id < 1):
        raise ArtifactError("validated_opening_id зіпсований")
    versions = _library_versions(library_versions)
    return {
        "model_version": model_version,
        "algorithm": algorithm,
        "label_type": label_type,
        "primary_strategy": primary,
        "trade_symbol": trade_symbol,
        "train_symbols": list(symbols),
        "timeframe": "1h",
        "feature_schema_hash": feature_schema_hash(columns),
        "preprocessing_version": PREPROCESSING_VERSION,
        "train_range_ms": [start, end],
        "target_params": _target_payload(target),
        "cost_model": _cost_payload(costs),
        "decision_rule": rule.metadata(),
        "metrics": {},
        "experiment_id": experiment_id,
        "n_trials_total": None,
        "validated_by": validated_by,
        "validated_opening_id": validated_opening_id,
        "live_corridor": None,
        "reproducibility": {
            "git_commit": git.commit,
            "git_dirty": git.dirty,
            "data_hash": data_hash,
            "seed": seed,
            "library_versions": versions,
            "model_sha256": None,
            "calibrator_sha256": None,
            "corridor_replicates": None,
        },
        "status": "TRAINED",
        "recipe": {
            "lgbm": None if recipe_lgbm is None else _lgbm_payload(recipe_lgbm),
            "feature_columns": list(columns),
            "feature_coverage_ms": [cover_start, cover_end],
            "train_rows": train_rows,
            "include_holdout": include_holdout,
            "calibrator": calibrator_method,
        },
    }


def write_artifact(
    directory: Path,
    metadata: Mapping[str, Any],
    *,
    booster: lgb.Booster | None,
    calibrator: ProbabilityCalibrator | None,
) -> Path:
    """Записати каталог версії. Повторний запис тієї самої теки відхиляється."""
    if directory.exists():
        raise ArtifactError("каталог версії вже існує")
    label = metadata.get("label_type")
    if label == "rule_only":
        if booster is not None or calibrator is not None:
            raise ArtifactError("rule_only без файлу моделі")
    elif label in ("meta", "direct"):
        # `direct` має той самий файловий склад, що `meta` (рішення 0022): бустер і калібратор.
        if not isinstance(booster, lgb.Booster) or type(calibrator) is not ProbabilityCalibrator:
            raise ArtifactError(f"{label} потребує бустер і калібратор")
    else:
        raise ArtifactError("невідомий label_type")
    if metadata.get("model_version") != directory.name or metadata.get("status") != "TRAINED":
        raise ArtifactError("версія каталогу або статус не збігаються з метаданими")
    try:
        directory.mkdir(parents=True)
    except FileExistsError as exc:
        raise ArtifactError("каталог версії вже існує") from exc
    body = json.loads(json.dumps(dict(metadata)))
    body["artifact_dir"] = str(directory)
    if booster is not None:
        booster.save_model(str(directory / MODEL_FILE))
        body["reproducibility"]["model_sha256"] = _sha256_file(directory / MODEL_FILE)
    if calibrator is not None:
        _write_json(directory / CALIBRATOR_FILE, calibrator.payload())
        body["reproducibility"]["calibrator_sha256"] = _sha256_file(directory / CALIBRATOR_FILE)
    _write_json(directory / METADATA_FILE, body)
    return directory


def load_model(directory: Path) -> LoadedModel:
    """Прочитати каталог. Розбіжність файлів, хешів і `label_type` — `ArtifactError`, не тихий fallback.

    Статус і поріг цього файлу без звірки з БД не є підставою для входу: торгівля читає модель через
    `load_registered_model`.
    """
    try:
        return _load_directory(directory)
    except ArtifactError:
        raise
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
        InvalidOperation,
        lgb.basic.LightGBMError,
    ) as exc:
        raise ArtifactError(f"каталог моделі не читається: {type(exc).__name__}") from exc


async def load_registered_model(session: AsyncSession, model_version: str) -> LoadedModel:
    """Модель для торгівлі. Рецепт у файлі має збігтися з БД, хеші — з файлами моделі й калібратора.

    Поля, які змінює перехід статусу (`status`, `live_corridor`, `n_trials_total`, `metrics`,
    `corridor_replicates`), беруться з БД: файл — лише копія, і невдалий запис файлу не робить модель
    непридатною. Решта (правило, `decision_rule`, витрати, таргет, схема, рецепт, хеші) — лише разом із БД.
    """
    row = await _model(session, model_version)
    loaded = load_model(_directory_of(row))
    stored = dict(row.metadata_json)
    if _immutable(loaded.metadata) != _immutable(stored):
        raise ArtifactError("model_metadata.json не збігається з БД")
    if stored.get("status") != row.status:
        raise ArtifactError("статус у metadata_json не збігається з рядком БД")
    if loaded.metadata["reproducibility"].get("model_sha256") != row.artifact_sha256:
        raise ArtifactError("хеш файлу моделі не збігається з БД")
    return dataclasses.replace(loaded, metadata=stored, db_status=row.status)


def assert_runtime(
    metadata: Mapping[str, Any],
    columns: Sequence[str],
    *,
    versions: Mapping[str, str] | None = None,
) -> None:
    """Зупинити інференс, якщо код або бібліотека вже не ті, з якими зібрано артефакт."""
    recipe = metadata.get("recipe")
    if not isinstance(recipe, Mapping):
        raise InferenceBlocked("у метаданих немає рецепта")
    stored_columns = recipe.get("feature_columns")
    if not isinstance(stored_columns, list) or not stored_columns:
        raise InferenceBlocked("у метаданих немає колонок фіч")
    stored_hash = metadata.get("feature_schema_hash")
    if type(stored_hash) is not str or stored_hash != feature_schema_hash(stored_columns):
        raise InferenceBlocked("feature_schema_hash не збігається з колонками артефакту")
    if stored_hash != feature_schema_hash(columns):
        raise InferenceBlocked("feature_schema_hash не збігається з поточними фічами")
    if metadata.get("preprocessing_version") != PREPROCESSING_VERSION:
        raise InferenceBlocked("preprocessing_version не збігається")
    current = dict(current_library_versions() if versions is None else versions)
    libs = metadata.get("reproducibility", {})
    if not isinstance(libs, Mapping):
        raise InferenceBlocked("немає версій бібліотек")
    recorded = libs.get("library_versions")
    if not isinstance(recorded, Mapping):
        raise InferenceBlocked("немає версій бібліотек")
    recorded_lgbm = recorded.get("lightgbm")
    if type(recorded_lgbm) is not str or recorded_lgbm != current.get("lightgbm"):
        raise InferenceBlocked("версія lightgbm не збігається")
    if "xgboost" in recorded and recorded["xgboost"] != current.get("xgboost"):
        raise InferenceBlocked("версія xgboost не збігається")
    # Калібратор meta і direct відновлюється в об'єкти scikit-learn: інша версія може змінити калібровану ймовірність.
    if metadata.get("label_type") in ("meta", "direct") and recorded.get("scikit-learn") != current.get("scikit-learn"):
        raise InferenceBlocked("версія scikit-learn (калібратор) не збігається")


def predict_probability(
    loaded: LoadedModel,
    values: Sequence[float],
    side: PositionSide,
    *,
    columns: Sequence[str],
    versions: Mapping[str, str] | None = None,
) -> Decimal:
    """Сира ймовірність TP. Середовище перевіряється до бустера.

    `columns` — колонки `values` з поточного коду (`FeatureRows.columns`), не з файлу артефакту:
    інакше схема звірялась би сама з собою. Порядок має збігтися з рецептом.
    """
    current = tuple(columns)
    assert_runtime(loaded.metadata, current, versions=versions)
    if current != tuple(loaded.metadata["recipe"]["feature_columns"]):
        raise InferenceBlocked("порядок колонок фіч не збігається з артефактом")
    if loaded.metadata["label_type"] != "meta" or loaded.booster is None:
        raise ArtifactError("ймовірність читається лише з meta")
    if type(side) is not PositionSide:
        raise ValueError("side має бути PositionSide")
    if len(values) != len(current):
        raise ValueError("довжина вектора не збігається з колонками фіч")
    vector = np.empty(len(current) + 1, dtype=np.float64)
    for index, value in enumerate(values):
        number = float(value)
        if not np.isfinite(number):
            raise ValueError("фіча не скінченна")
        vector[index] = number
    vector[-1] = 1.0 if side is PositionSide.LONG else 0.0
    raw = loaded.booster.predict(vector.reshape(1, -1))
    return _probability(float(raw[0]))


def infer_entry(
    loaded: LoadedModel,
    values: Sequence[float],
    *,
    mode: str,
    columns: Sequence[str],
    side: PositionSide,
    entry: Decimal,
    atr: Decimal,
    costs: CostModel,
    params: TargetParams,
    versions: Mapping[str, str] | None = None,
) -> EntryDecision:
    """Live-вхід meta: калібрована ймовірність і `decide_entry`, не `keep`.

    Статус має торгувати в `mode`: Live — лише `APPROVED`, Demo/Testnet — ще `CANDIDATE`.
    `loaded` — з `load_registered_model` (статус звірено з БД). Витрати і таргет — ті, що в артефакті.
    `columns` — колонки `values` з поточного коду фіч (`FeatureRows.columns`).
    """
    _require_tradable(loaded, mode)
    if loaded.calibrator is None:
        raise ArtifactError("meta без калібратора")
    if _cost_payload(costs) != dict(loaded.metadata["cost_model"]):
        raise ArtifactError("витрати входу не збігаються з артефактом")
    if params != _target(loaded.metadata["target_params"]):
        raise ArtifactError("таргет входу не збігається з артефактом")
    raw = predict_probability(loaded, values, side, columns=columns, versions=versions)
    return decide_entry(
        raw_probability=raw,
        side=side,
        entry=entry,
        atr=atr,
        costs=costs,
        params=params,
        calibrator=loaded.calibrator,
        rule=_rule(loaded.metadata["decision_rule"]),
    )


def accept_primary(
    loaded: LoadedModel,
    side: PositionSide,
    *,
    mode: str,
    versions: Mapping[str, str] | None = None,
) -> PositionSide:
    """`rule_only`: сигнал правила проходить, лише якщо статус торгує в `mode` і артефакт відповідає коду."""
    _require_tradable(loaded, mode)
    assert_runtime(loaded.metadata, loaded.metadata["recipe"]["feature_columns"], versions=versions)
    if loaded.metadata["label_type"] != "rule_only":
        raise ArtifactError("правило без фільтра приймає лише rule_only")
    if type(side) is not PositionSide:
        raise ValueError("side має бути PositionSide")
    return side


async def rows_for_opened_holdout(
    session: AsyncSession,
    rows: Sequence[SampleRow],
    *,
    experiment_id: str,
    opening_id: int,
) -> tuple[SampleRow, ...]:
    """Єдиний доступ до рядків hold-out. Відкриття читається з БД, належить експерименту і пройшло Gate G1.

    Розбивка — збережена в експерименті, не передана викликачем. Символ рядка — лише символ експерименту.
    """
    if type(opening_id) is not int or isinstance(opening_id, bool) or opening_id < 1:
        raise ArtifactError("відкриття hold-out зіпсоване")
    opening = await session.get(HoldoutOpening, opening_id)
    if opening is None:
        raise ArtifactError("немає такого відкриття hold-out")
    result = dict(opening.result or {})
    if result.get("passed") is not True or result.get("gate") != "G1":
        raise ArtifactError("рядки hold-out лише після Gate G1")
    if opening.experiment_id != experiment_id:
        raise SplitLeakError("відкриття не цього експерименту")
    experiment = await _experiment(session, experiment_id)
    if experiment.research_line != opening.research_line:
        raise SplitLeakError("відкриття іншої лінії")
    split = _stored_split(experiment)
    if (opening.holdout_from_ms, opening.holdout_to_ms) != (split.holdout_from_ms, split.holdout_to_ms):
        raise SplitLeakError("відкриття не цього hold-out")
    symbols = set(experiment.symbols)
    if any(row.symbol not in symbols for row in rows):
        raise SplitLeakError("рядок іншого символу, ніж в експерименті")
    return tuple(row for row in rows if is_holdout_row(row, split))


@fixed_context
def assess_holdout(
    *,
    label_type: str,
    trade_symbol: str,
    report: BacktestReport,
    walk_forward: Sequence[BacktestTrade],
    holdout_from_ms: int,
    holdout_to_ms: int,
    profitable_fold_share: Decimal,
    max_drawdown_limit_pct: Decimal,
    seed: int,
    n_replicates: int = DEFAULT_BOOTSTRAP_REPLICATES,
    unfiltered: BacktestReport | None = None,
    scored_unfiltered: BacktestReport | None = None,
) -> AdmissionDecision:
    """Умови 2–4 на запечатаному hold-out `[holdout_from_ms, holdout_to_ms)`. Gate G1 тут не перераховується.

    Чиста функція для перегляду. У журнал допуск пише лише `record_holdout_admission`: частку фолдів,
    ліміт, seed і число реплік він бере сам, тож перебір seed тут нічого не дає.
    Вікно не розширюється назад: модель допуску вчилась на всьому walk-forward (рішення 0018).
    Для `meta` умова 4 — звіти правила без фільтра на тих самих сигналах (рішення 0017), не голі числа.
    """
    if label_type not in LABEL_TYPES:
        raise ArtifactError("невідомий label_type")
    require_decimal("max_drawdown_limit_pct", max_drawdown_limit_pct, positive=True)
    for value in (holdout_from_ms, holdout_to_ms, seed, n_replicates):
        if type(value) is not int or isinstance(value, bool) or value < 0:
            raise ArtifactError("вікно, seed і число реплік — невід'ємні int")
    if holdout_from_ms >= holdout_to_ms or n_replicates < 1:
        raise ArtifactError("вікно hold-out порожнє або реплік немає")
    require_decimal("profitable_fold_share", profitable_fold_share)
    if profitable_fold_share < 0 or profitable_fold_share > 1:
        raise ArtifactError("частка фолдів поза [0, 1]")
    _holdout_report(report, trade_symbol, holdout_from_ms, holdout_to_ms)
    trades = report.trades
    forward = tuple(walk_forward)
    forward_times = [trade.entry_open_time_ms for trade in forward]
    if forward_times != sorted(forward_times) or len(set(forward_times)) != len(forward_times):
        raise ArtifactError("угоди walk-forward мають іти за часом без повторів")
    if any(time >= holdout_from_ms for time in forward_times):
        raise ArtifactError("угода walk-forward заходить у hold-out")
    ordered = sorted(trades, key=lambda trade: (trade.entry_open_time_ms, trade.entry_index))
    holdout_times = [trade.entry_open_time_ms for trade in ordered]
    if len(set(holdout_times)) != len(holdout_times):
        raise ArtifactError("угоди hold-out повторюються за часом")
    reasons: list[str] = []
    if len(trades) < HOLDOUT_MIN_TRADES:
        reasons.append(REASON_MIN_TRADES)
    oos_trades = len(forward) + len(trades)
    if oos_trades < OOS_MIN_TRADES:
        reasons.append(REASON_OOS_TRADES)
    observed = report.metrics.profit_factor
    unbounded = observed is None and report.metrics.total_net_usd > 0 and len(trades) > 0
    if not unbounded and (observed is None or observed < HOLDOUT_MIN_NET_PF):
        reasons.append(REASON_NET_PF)
    lower_above = False
    if trades:
        interval = block_bootstrap_pf(trades, n_replicates=n_replicates, seed=seed)
        lower_above = interval.lower_above(PF_LOWER_BOUND)
    if not lower_above:
        reasons.append(REASON_PF_LOWER)
    drawdown = report.metrics.max_drawdown_pct
    if drawdown > max_drawdown_limit_pct:
        reasons.append(REASON_DRAWDOWN)
    if profitable_fold_share < FOLD_SHARE_MIN:
        reasons.append(REASON_FOLD_SHARE)
    filtered = report.metrics.total_net_usd
    unfiltered_net: Decimal | None = None
    scored_net: Decimal | None = None
    if label_type == "meta":
        if unfiltered is None or scored_unfiltered is None:
            raise ArtifactError("meta потребує обидва звіти правила без фільтра")
        for rule_report in (unfiltered, scored_unfiltered):
            _holdout_report(rule_report, trade_symbol, holdout_from_ms, holdout_to_ms)
        for checked in (report, unfiltered, scored_unfiltered):
            if len(_signal_keys(checked)) != checked.n_signals:
                raise ArtifactError("сигнал повторюється в прогоні")
        if not _signal_keys(report) <= _signal_keys(scored_unfiltered) <= _signal_keys(unfiltered):
            raise ArtifactError("сигнали фільтра, оцінені й усі сигнали правила мають бути вкладені")
        unfiltered_net = unfiltered.metrics.total_net_usd
        scored_net = scored_unfiltered.metrics.total_net_usd
        if not (filtered > unfiltered_net and filtered > scored_net):
            reasons.append(REASON_FILTER)
    elif unfiltered is not None or scored_unfiltered is not None:
        raise ArtifactError("порівняння з фільтром лише для meta")
    return AdmissionDecision(
        passed=not reasons,
        reasons=tuple(reasons),
        window_from_ms=holdout_from_ms,
        window_to_ms=holdout_to_ms,
        holdout_trades=len(trades),
        holdout_net_pf=observed,
        holdout_pf_unbounded=unbounded,
        oos_trades=oos_trades,
        max_drawdown_pct=drawdown,
        max_drawdown_limit_pct=max_drawdown_limit_pct,
        lower_pf_above_one=lower_above,
        profitable_fold_share=profitable_fold_share,
        filtered_net=filtered,
        unfiltered_net=unfiltered_net,
        scored_unfiltered_net=scored_net,
        seed=seed,
        n_replicates=n_replicates,
        holdout_entry_ms=tuple(holdout_times),
        holdout_r=tuple(trade.r_multiple for trade in ordered),
    )


async def register_model(session: AsyncSession, directory: Path) -> str:
    """Вставити `TRAINED` моделі експерименту чи допуску. Повтор тієї самої версії — помилка БД.

    Модель, що бачила hold-out (`include_holdout`) або має `validated_by`, тут не реєструється: її пише
    лише `retrain_registered_recipe`. Інакше в `CANDIDATE` міг би піти будь-який файл.
    """
    loaded = load_model(directory)
    meta = loaded.metadata
    if meta["recipe"]["include_holdout"] is not False or meta["validated_by"] is not None:
        raise ArtifactError("перенавчену модель реєструє лише retrain_registered_recipe")
    if meta.get("validated_opening_id") is not None:
        raise ArtifactError("перенавчену модель реєструє лише retrain_registered_recipe")
    return await _insert_model(session, loaded)


async def record_holdout_admission(
    session: AsyncSession,
    *,
    opening_id: int,
    report: BacktestReport,
    walk_forward: Sequence[BacktestTrade],
    unfiltered: BacktestReport | None = None,
    scored_unfiltered: BacktestReport | None = None,
) -> AdmissionDecision:
    """Порахувати й дописати умови 2–4 в `holdout_openings.result.admission`. Один допуск на відкриття.

    Модуль бере сам: вікно — з відкриття; частку прибуткових фолдів — з результату G1; ліміт просадки —
    `HOLDOUT_MAX_DRAWDOWN_PCT` (дефолт `RiskLimits`); seed — артефакту; число реплік —
    `DEFAULT_BOOTSTRAP_REPLICATES`. Угоди walk-forward мають збігтися з рядом спроби G1 у журналі
    (час входу і R). Модель допуску — `TRAINED`, без hold-out, з рецептом спроби G1. Невдалий допуск
    теж записується, і та сама модель чи спроба на цьому hold-out другого допуску не отримає
    (ТЗ §5А.10: після перегляду не підганяють).
    """
    found = await session.get(HoldoutOpening, opening_id)
    if found is None:
        raise ArtifactError("немає такого відкриття hold-out")
    # Той самий advisory lock лінії, що в `_open_holdout`: паралельний допуск на другому відкритті
    # того самого вікна чекає, і `_refuse_second_chance` бачить перший.
    await _lock_line(session, found.research_line)
    opening = await session.get(HoldoutOpening, opening_id, with_for_update=True, populate_existing=True)
    if opening is None:
        raise ArtifactError("немає такого відкриття hold-out")
    result = dict(opening.result or {})
    if result.get("passed") is not True or result.get("gate") != "G1":
        raise ArtifactError("допуск пишеться лише на відкриття, яке пройшло Gate G1")
    if "admission" in result:
        raise ArtifactError("результат hold-out уже записаний")
    if str(result.get("model_version", opening.model_version)) != opening.model_version:
        raise ArtifactError("відкриття не збігається з моделлю")
    evidence = await _model(session, opening.model_version)
    experiment = await _experiment(session, opening.experiment_id)
    split = _stored_split(experiment)
    _require_validation_model(evidence, opening, experiment, split)
    trial = await _g1_trial(session, opening, result)
    _recipe_matches_trial(evidence.metadata_json, trial)
    await _refuse_second_chance(session, opening, evidence.model_version, trial.id)
    _walk_forward_matches(walk_forward, trial)
    for checked in (report, unfiltered, scored_unfiltered):
        if checked is not None and checked.seed != experiment.seed:
            raise ArtifactError("seed прогону hold-out не збігається з експериментом")
    decision = assess_holdout(
        label_type=str(evidence.label_type),
        trade_symbol=str(evidence.trade_symbol),
        report=report,
        walk_forward=walk_forward,
        holdout_from_ms=opening.holdout_from_ms,
        holdout_to_ms=opening.holdout_to_ms,
        profitable_fold_share=_stored_decimal(result.get("profitable_fold_share"), "profitable_fold_share"),
        max_drawdown_limit_pct=HOLDOUT_MAX_DRAWDOWN_PCT,
        seed=int(evidence.metadata_json["reproducibility"]["seed"]),
        n_replicates=DEFAULT_BOOTSTRAP_REPLICATES,
        unfiltered=unfiltered,
        scored_unfiltered=scored_unfiltered,
    )
    admission = decision.payload()
    admission["trial_id"] = trial.id
    admission["evaluated_at_ms"] = now_ms()
    result["admission"] = admission
    opening.result = result
    await session.flush()
    _LOG.info(
        "holdout_admission",
        opening_id=opening.id,
        model_version=opening.model_version,
        trial_id=trial.id,
        passed=decision.passed,
        reasons=list(decision.reasons),
    )
    return decision


async def promote_to_candidate(
    session: AsyncSession,
    *,
    model_version: str,
    oos_trades: Sequence[BacktestTrade],
) -> dict[str, Any]:
    """`TRAINED → CANDIDATE`. Коридор рахується тут (`CORRIDOR_REPLICATES` реплік, seed артефакту) і більше
    не змінюється.

    Модель — перенавчання від `retrain_registered_recipe` (`validated_by`, `include_holdout`) за рецептом,
    чий hold-out має успішний допуск. `git_dirty` залишається TRAINED. G1 відкриття — з порогами ТЗ
    (DSR ≥ 0.95, PBO < 0.5, без послаблень). DSR, PBO і N — з відкриття цієї спроби, не зі знімка
    експерименту. `oos_trades` — угоди walk-forward спроби G1 і hold-out допуску в порядку часу: час
    входу і R кожної звіряються з журналом.
    """
    row = await _model(session, model_version)
    if row.git_dirty:
        raise ArtifactError("модель з git_dirty не стає CANDIDATE")
    if row.status != "TRAINED":
        raise ArtifactError("у CANDIDATE переходить лише TRAINED")
    loaded = await load_registered_model(session, model_version)
    meta = dict(loaded.metadata)
    if meta["recipe"]["include_holdout"] is not True or row.validated_by is None:
        raise ArtifactError("CANDIDATE — модель, перенавчена після допуску")
    opening_id = meta.get("validated_opening_id")
    if type(opening_id) is not int:
        raise ArtifactError("немає відкриття, яким перевірено рецепт")
    opening = await session.get(HoldoutOpening, opening_id)
    if opening is None or opening.model_version != row.validated_by or opening.experiment_id != row.experiment_id:
        raise ArtifactError("відкриття не належить моделі, що пройшла hold-out")
    evidence = await _model(session, row.validated_by)
    _same_recipe(evidence.metadata_json, meta)
    result = dict(opening.result or {})
    admission = result.get("admission")
    if result.get("passed") is not True or result.get("gate") != "G1" or not isinstance(admission, dict):
        raise ArtifactError("немає успішного результату hold-out")
    _require_stored_admission(admission, opening, str(evidence.label_type))
    dsr = _stored_decimal(result.get("dsr"), "dsr")
    pbo = _stored_decimal(result.get("pbo"), "pbo")
    if dsr < CANDIDATE_MIN_DSR or pbo >= CANDIDATE_MAX_PBO or result.get("loosened") not in (None, []):
        raise ArtifactError("CANDIDATE лише після G1 з порогами ТЗ: DSR ≥ 0.95, PBO < 0.5, без послаблень")
    trial = await _g1_trial(session, opening, result)
    if admission.get("trial_id") != trial.id:
        raise ArtifactError("допуск не цієї спроби G1")
    trades = tuple(oos_trades)
    _corridor_trades_match(trades, trial, admission)
    seed = int(meta["reproducibility"]["seed"])
    n_trials = result.get("n_trials_line")
    if type(n_trials) is not int or isinstance(n_trials, bool) or n_trials < 1:
        raise ArtifactError("у відкритті немає N спроб лінії")
    n_replicates = CORRIDOR_REPLICATES
    corridor = corridor_payload(live_corridor(trades, n_replicates=n_replicates, seed=seed))
    meta["status"] = "CANDIDATE"
    meta["live_corridor"] = corridor
    meta["n_trials_total"] = n_trials
    # Підтвердження `model.approve`, записане до CANDIDATE, не рахується: людина мала бачити CANDIDATE і Testnet.
    meta["approval_audit_after_id"] = int(await session.scalar(select(func.coalesce(func.max(AuditLog.id), 0))) or 0)
    meta["reproducibility"] = {**meta["reproducibility"], "corridor_replicates": n_replicates}
    meta["metrics"] = {
        "holdout_net_pf": admission["holdout_net_pf"],
        "trades": int(admission["holdout_trades"]),
        "oos_trades": int(admission["oos_trades"]),
        "dsr": format(dsr, "f"),
        "pbo": format(pbo, "f"),
        "max_drawdown_pct": admission["max_drawdown_pct"],
        "g1_trial_id": trial.id,
    }
    await _transition(session, row, status="CANDIDATE", metadata=meta, live_corridor=corridor)
    return meta


async def approve_model(session: AsyncSession, *, model_version: str, confirmed_audit_id: int) -> None:
    """`CANDIDATE → APPROVED`. Лише підтверджений `model.approve` з `target` = ця версія. Коридор той самий.

    Умова 5 ТЗ §5.3 (7 днів Testnet без аномалій) — на людині, що підтверджує.
    """
    row = await _model(session, model_version)
    if row.label_type == "direct":
        raise ArtifactError("direct не стає APPROVED до T5.x (рішення 0022)")
    if row.status != "CANDIDATE":
        raise ArtifactError("APPROVED ставиться лише з CANDIDATE")
    audit = await session.get(AuditLog, confirmed_audit_id)
    if (
        audit is None
        or audit.action != AUDIT_ACTION_MODEL_APPROVE
        or audit.confirmed is not True
        or audit.target != model_version
    ):
        raise ArtifactError("APPROVED потребує підтверджений model.approve цієї версії")
    meta = dict(row.metadata_json)
    floor = meta.get("approval_audit_after_id")
    if type(floor) is not int or audit.id <= floor:
        raise ArtifactError("підтвердження model.approve старіше за перехід у CANDIDATE")
    if meta.get("live_corridor") != row.live_corridor:
        raise ArtifactError("коридор у файлі і в БД розійшовся")
    meta["status"] = "APPROVED"
    await _transition(
        session,
        row,
        status="APPROVED",
        metadata=meta,
        live_corridor=row.live_corridor,
        confirmed_audit_id=confirmed_audit_id,
    )


async def retire_model(session: AsyncSession, *, model_version: str) -> None:
    """`CANDIDATE` або `APPROVED` → `RETIRED`. Коридор не перераховується."""
    row = await _model(session, model_version)
    if row.status not in ("CANDIDATE", "APPROVED"):
        raise ArtifactError("RETIRED ставиться з CANDIDATE або APPROVED")
    meta = dict(row.metadata_json)
    meta["status"] = "RETIRED"
    await _transition(session, row, status="RETIRED", metadata=meta, live_corridor=row.live_corridor)


async def require_model_for_start(session: AsyncSession, *, model_version: str, mode: str) -> ModelArtifact:
    """Чи можна підняти бота з цією моделлю. Live — лише APPROVED. Demo і Testnet — ще й CANDIDATE.

    `direct` до T5.x не торгує в жодному режимі (рішення 0022): інференсу для нього немає.
    """
    if mode not in MODES:
        raise ArtifactError("режим — demo, testnet або live")
    row = await _model(session, model_version)
    if row.label_type == "direct":
        raise ModelNotTradable(f"{mode}: direct не торгує до T5.x (рішення 0022)")
    if row.status not in _TRADABLE[mode]:
        raise ModelNotTradable(f"{mode} не стартує з моделлю у статусі {row.status}")
    return row


async def retrain_registered_recipe(
    session: AsyncSession,
    histories: Sequence[SymbolHistory],
    *,
    validation_version: str,
    root: Path,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    feature_data: Mapping[str, SymbolData] | None = None,
    at_ms: int | None = None,
) -> str:
    """Нова модель тим самим рецептом на всіх даних. Підбору немає. Статус — TRAINED.

    Розбивка — збережена в експерименті. Спершу модель допуску відтворюється тим самим рецептом без
    hold-out і має дати той самий файл: так доведено, що вона не бачила hold-out і не з Optuna чи іншого
    рецепта. Потім той самий рецепт вчиться з hold-out. Калібратор і `decision_rule` копіюються, не
    вчаться знову. `validated_by` — версія, що пройшла hold-out. Git і `data_hash` модуль читає сам.
    Навчання і git — у `asyncio.to_thread`, event loop не блокується.
    """
    evidence = await _model(session, validation_version)
    meta = evidence.metadata_json
    if meta.get("recipe", {}).get("include_holdout") is not False:
        raise ArtifactError("перенавчання стартує з моделі, що не бачила hold-out")
    opening = await _passing_opening(session, evidence)
    experiment = await _experiment(session, opening.experiment_id)
    split = _stored_split(experiment)
    _require_validation_model(evidence, opening, experiment, split)
    # Витрати й таргет — з рецепта. `None` не означає «дефолт»: інакше мітки рахувались би іншими
    # витратами, ніж записано у файлі.
    recipe_target = _target(meta["target_params"])
    if params is not None and params != recipe_target:
        raise ArtifactError("таргет перенавчання відрізняється від рецепта")
    recipe_costs = _stored_costs(meta, costs)
    if sorted(history.symbol for history in histories) != sorted(meta["train_symbols"]):
        raise ArtifactError("символи перенавчання відрізняються від рецепта")
    git = await asyncio.to_thread(read_git_state, repo_models_dir().parent)
    data_hash = await asyncio.to_thread(hash_inputs, histories, feature_data)
    grant = _grant_holdout(opening.id, opening.holdout_from_ms, opening.holdout_to_ms)
    seed = int(meta["reproducibility"]["seed"])
    if meta["label_type"] == "rule_only":
        fit_rows = int(meta["recipe"]["train_rows"])
        fit_lgbm = None
        fit_coverage = tuple(meta["recipe"]["feature_coverage_ms"])
        fit_columns = tuple(meta["recipe"]["feature_columns"])
        booster = None
        calibrator = None
    elif meta["label_type"] == "meta":
        loaded = await load_registered_model(session, validation_version)
        if loaded.booster is None or loaded.calibrator is None:
            raise ArtifactError("немає файлу моделі допуску")
        spec = strategy_from_metadata(meta["primary_strategy"])
        recipe_lgbm = _lgbm_from_payload(meta["recipe"]["lgbm"])
        common: dict[str, Any] = {
            "spec": spec,
            "split": split,
            "trading_symbol": str(evidence.trade_symbol),
            "seed": seed,
            "costs": recipe_costs,
            "params": recipe_target,
            "feature_data": feature_data,
            "lgbm": recipe_lgbm,
            "expected_schema_hash": str(evidence.feature_schema_hash),
        }
        try:
            replay = await asyncio.to_thread(fit_frozen_meta, histories, **common)
        except (MetaTrainError, ValueError) as exc:
            raise ArtifactError(f"модель допуску не відтворюється рецептом: {exc}") from exc
        if _booster_sha256(booster_of(replay.model)) != evidence.artifact_sha256:
            raise ArtifactError("модель допуску не відтворюється рецептом: інший файл моделі")
        fit = await asyncio.to_thread(fit_frozen_meta, histories, holdout_grant=grant, **common)
        _guard_retrain(fit, meta)
        fit_rows = fit.n_rows
        fit_lgbm = fit.lgbm
        fit_coverage = fit.coverage_ms
        fit_columns = fit.model.feature_columns
        booster = booster_of(fit.model)
        calibrator = loaded.calibrator
    elif meta["label_type"] == "direct":
        # Імпорт тут: `direct_model` імпортує `artifacts` (`decision_payload`), на рівні модуля був би цикл.
        from app.ml.direct_model import DirectTrainError, fit_frozen_direct

        loaded = await load_registered_model(session, validation_version)
        if loaded.booster is None or loaded.calibrator is None:
            raise ArtifactError("немає файлу моделі допуску")
        recipe_lgbm = _lgbm_from_payload(meta["recipe"]["lgbm"])
        direct_common: dict[str, Any] = {
            "split": split,
            "trading_symbol": str(evidence.trade_symbol),
            "seed": seed,
            "costs": recipe_costs,
            "params": recipe_target,
            "feature_data": feature_data,
            "lgbm": recipe_lgbm,
            "expected_schema_hash": str(evidence.feature_schema_hash),
        }
        try:
            direct_replay = await asyncio.to_thread(fit_frozen_direct, histories, **direct_common)
        except (DirectTrainError, ValueError) as exc:
            raise ArtifactError(f"модель допуску не відтворюється рецептом: {exc}") from exc
        if _booster_sha256(booster_of(direct_replay.model)) != evidence.artifact_sha256:
            raise ArtifactError("модель допуску не відтворюється рецептом: інший файл моделі")
        direct_fit = await asyncio.to_thread(fit_frozen_direct, histories, holdout_grant=grant, **direct_common)
        _guard_retrain(direct_fit, meta)
        fit_rows = direct_fit.n_rows
        fit_lgbm = direct_fit.lgbm
        fit_coverage = direct_fit.coverage_ms
        fit_columns = direct_fit.model.feature_columns
        booster = booster_of(direct_fit.model)
        calibrator = loaded.calibrator
    else:
        raise ArtifactError("невідомий label_type для перенавчання")
    version = allocate_version(root, str(evidence.label_type), at_ms=at_ms)
    fresh = build_metadata(
        model_version=version,
        label_type=str(evidence.label_type),
        spec=None if meta["label_type"] == "direct" else strategy_from_metadata(meta["primary_strategy"]),
        trade_symbol=str(evidence.trade_symbol),
        train_symbols=tuple(meta["train_symbols"]),
        feature_columns=fit_columns,
        target=recipe_target,
        costs=recipe_costs,
        rule=_rule(meta["decision_rule"]),
        experiment_id=str(evidence.experiment_id),
        seed=seed,
        data_hash=data_hash,
        git=git,
        library_versions=current_library_versions(),
        # Свічки перенавчання — уся розбивка разом із hold-out, не діапазон моделі допуску.
        train_range_ms=(split.data_from_ms, split.data_to_ms),
        recipe_lgbm=fit_lgbm,
        coverage_ms=(int(fit_coverage[0]), int(fit_coverage[1])),
        train_rows=fit_rows,
        include_holdout=True,
        calibrator_method=meta["recipe"]["calibrator"],
        validated_by=validation_version,
        validated_opening_id=opening.id,
    )
    if fresh["decision_rule"] != meta["decision_rule"]:
        raise ArtifactError("поріг перенавчання відрізняється від моделі допуску")
    directory = write_artifact(root / version, fresh, booster=booster, calibrator=calibrator)
    registered = await _register_retrained(session, directory, grant=grant)
    _LOG.info(
        "model_retrained",
        model_version=registered,
        validated_by=validation_version,
        opening_id=opening.id,
        git_dirty=git.dirty,
    )
    return registered


def booster_of(model: Any) -> lgb.Booster:
    """Файл моделі — бустер LightGBM, не pickle класифікатора."""
    booster = model.estimator.booster_
    if not isinstance(booster, lgb.Booster):
        raise ArtifactError("очікувався бустер LightGBM")
    return booster


async def _register_retrained(session: AsyncSession, directory: Path, *, grant: HoldoutGrant) -> str:
    """Реєстрація перенавченої моделі. Лише з `retrain_registered_recipe`, з грантом того самого відкриття."""
    if type(grant) is not HoldoutGrant:
        raise ArtifactError("перенавчену модель реєструють лише з грантом hold-out")
    loaded = load_model(directory)
    meta = loaded.metadata
    if meta["recipe"]["include_holdout"] is not True or meta["validated_by"] is None:
        raise ArtifactError("це не перенавчена модель")
    if meta.get("validated_opening_id") != grant.opening_id:
        raise ArtifactError("грант не того відкриття")
    return await _insert_model(session, loaded)


async def _insert_model(session: AsyncSession, loaded: LoadedModel) -> str:
    meta = loaded.metadata
    if meta["status"] != "TRAINED" or meta["live_corridor"] is not None:
        raise ArtifactError("реєструється лише свіжий TRAINED без коридора")
    reproducibility = meta["reproducibility"]
    stamp = now_ms()
    await session.execute(
        insert(ModelArtifact).values(
            model_version=meta["model_version"],
            algorithm=meta["algorithm"],
            label_type=meta["label_type"],
            trade_symbol=meta["trade_symbol"],
            timeframe=meta["timeframe"],
            feature_schema_hash=meta["feature_schema_hash"],
            preprocessing_version=meta["preprocessing_version"],
            status="TRAINED",
            git_commit=reproducibility["git_commit"],
            git_dirty=bool(reproducibility["git_dirty"]),
            library_versions=dict(reproducibility["library_versions"]),
            live_corridor=None,
            validated_by=meta["validated_by"],
            experiment_id=meta["experiment_id"],
            artifact_path=None if meta["label_type"] == "rule_only" else str(loaded.directory / MODEL_FILE),
            artifact_sha256=reproducibility["model_sha256"],
            metadata_json=meta,
            confirmed_audit_id=None,
            created_at_ms=stamp,
            status_changed_at_ms=stamp,
        )
    )
    await session.flush()
    _LOG.info("model_registered", model_version=meta["model_version"], git_dirty=bool(reproducibility["git_dirty"]))
    return str(meta["model_version"])


def _require_tradable(loaded: LoadedModel, mode: str) -> None:
    """Статус для входу — з БД (`db_status` від `load_registered_model`), не з файлу."""
    if mode not in MODES:
        raise ArtifactError("режим — demo, testnet або live")
    if type(loaded) is not LoadedModel or loaded.db_status is None:
        raise ModelNotTradable(f"{mode}: модель не прочитана з БД (load_registered_model)")
    status = loaded.db_status
    if status not in _TRADABLE[mode] or loaded.metadata.get("status") != status:
        raise ModelNotTradable(f"{mode}: вхід не читає модель у статусі {status}")


def _require_validation_model(
    evidence: ModelArtifact,
    opening: HoldoutOpening,
    experiment: Experiment,
    split: ResearchSplit,
) -> None:
    """Модель допуску — цього експерименту, цього hold-out, вчилась лише до hold-out збереженої розбивки."""
    meta = evidence.metadata_json
    if evidence.model_version != opening.model_version or evidence.experiment_id != opening.experiment_id:
        raise ArtifactError("відкриття не належить моделі допуску")
    if evidence.status != "TRAINED" or evidence.validated_by is not None:
        raise ArtifactError("модель допуску — TRAINED без validated_by")
    if evidence.label_type != experiment.label_type or evidence.trade_symbol != experiment.research_line:
        raise ArtifactError("модель допуску не того типу чи символу, що експеримент")
    if (opening.holdout_from_ms, opening.holdout_to_ms) != (split.holdout_from_ms, split.holdout_to_ms):
        raise SplitLeakError("відкриття не hold-out збереженої розбивки")
    recipe = meta.get("recipe", {})
    if recipe.get("include_holdout") is not False:
        raise ArtifactError("модель допуску бачила hold-out")
    train_range = meta.get("train_range_ms")
    coverage = recipe.get("feature_coverage_ms")
    if (
        not isinstance(train_range, list)
        or not isinstance(coverage, list)
        or len(train_range) != 2
        or len(coverage) != 2
        or train_range[1] > split.holdout_from_ms
        or coverage[1] > split.holdout_from_ms
    ):
        raise SplitLeakError("діапазон навчання моделі допуску заходить у hold-out")
    if meta.get("primary_strategy") != experiment.primary_strategy:
        raise ArtifactError("правило моделі допуску не те, що в експерименті")
    if meta.get("reproducibility", {}).get("seed") != experiment.seed:
        raise ArtifactError("seed моделі допуску не той, що в експерименті")
    target = meta.get("target_params", {})
    if not isinstance(target, Mapping) or target.get("horizon_bars") != split.horizon_bars:
        raise ArtifactError("горизонт таргета не той, що в розбивці")
    # Таргет і витрати — частина рецепта (ТЗ §5.3). Немає в журналі — відмова, не пропуск.
    if experiment.params.get("target") != target:
        raise ArtifactError("таргет моделі допуску не той, що в журналі експерименту")
    if experiment.params.get("costs") != meta.get("cost_model"):
        raise ArtifactError("витрати моделі допуску не ті, що в журналі експерименту")
    if sorted(meta.get("train_symbols", [])) != sorted(experiment.symbols):
        raise ArtifactError("символи моделі допуску не ті, що в експерименті")


async def _g1_trial(session: AsyncSession, opening: HoldoutOpening, result: Mapping[str, Any]) -> ExperimentTrial:
    trial_id = result.get("trial_id")
    if type(trial_id) is not int:
        raise ArtifactError("у відкритті немає спроби G1")
    trial = await session.get(ExperimentTrial, trial_id)
    if trial is None or trial.experiment_id != opening.experiment_id:
        raise ArtifactError("спроба G1 не цього експерименту")
    return trial


def _recipe_matches_trial(meta: Mapping[str, Any], trial: ExperimentTrial) -> None:
    """Hold-out перевіряє саме рецепт спроби, що пройшла G1, а не інший варіант того самого експерименту."""
    params = trial.params if isinstance(trial.params, Mapping) else {}
    recipe_lgbm = meta.get("recipe", {}).get("lgbm")
    if meta.get("label_type") == "direct":
        # `direct` без первинного правила (рішення 0022): ідентичність рецепта — стадія, роль, клас і objective.
        if (
            params.get("stage") != DIRECT_STAGE
            or params.get("role") != "direct"
            or params.get("objective") != "multiclass"
            or params.get("classes") != list(_DIRECT_CLASSES)
        ):
            raise ArtifactError("спроба G1 не прямого прогнозування (stage, role, objective, classes)")
    elif params.get("strategy") != meta.get("primary_strategy"):
        raise ArtifactError("правило моделі допуску не те, що в спробі G1")
    if meta.get("label_type") == "rule_only":
        if recipe_lgbm is not None:
            raise ArtifactError("rule_only без LightGBM")
        return
    if params.get("shuffled_labels") is not False:
        raise ArtifactError("спроба G1 з перемішаними мітками або без прапорця — не рецепт моделі")
    # Правило входу — частина рецепта (ТЗ §5.3). Його пише в спробу той, хто вибрав поріг і калібратор
    # (`decision_payload`, T2.12). Немає в журналі — відмова: CANDIDATE з неперевіреним порогом не буває.
    expected_decision = {
        "decision_rule": meta.get("decision_rule"),
        "calibrator_sha256": meta.get("reproducibility", {}).get("calibrator_sha256"),
    }
    if params.get("decision") != expected_decision:
        raise ArtifactError("правило входу моделі допуску не записане в спробі G1 або інше (decision_payload)")
    # Фічі — частина рецепта: та сама схема колонок, на якій спроба пройшла G1. Немає в журналі — відмова.
    if params.get("feature_schema_hash") != meta.get("feature_schema_hash"):
        raise ArtifactError("схема фіч моделі допуску не записана в спробі G1 або інша (feature_schema_hash)")
    trial_lgbm = params.get("lgbm")
    if not isinstance(trial_lgbm, Mapping):
        raise ArtifactError("у спробі G1 немає рецепта LightGBM")
    try:
        normalized = _lgbm_payload(_lgbm_from_payload(trial_lgbm))
    except (ArtifactError, InvalidOperation) as exc:
        raise ArtifactError("рецепт LightGBM спроби G1 не читається") from exc
    if normalized != recipe_lgbm:
        raise ArtifactError("рецепт LightGBM моделі допуску не той, що в спробі G1")
    best = params.get("best_iteration")
    if best is not None and (not isinstance(best, list) or any(item != normalized["n_estimators"] for item in best)):
        # Фолди спроби вчились з ранньою зупинкою, кожен своє число дерев. Яке з них брати для моделі
        # на всіх даних — рішення методології (T2.12); поки fail-closed.
        raise ArtifactError("рецепт з ранньою зупинкою: число дерев для перенавчання ще не визначене (T2.12)")


async def _refuse_second_chance(
    session: AsyncSession, opening: HoldoutOpening, model_version: str, trial_id: int
) -> None:
    """Та сама модель чи спроба на тому самому hold-out має лише один допуск, навіть невдалий."""
    stmt = (
        select(HoldoutOpening)
        .where(
            HoldoutOpening.research_line == opening.research_line,
            HoldoutOpening.holdout_from_ms == opening.holdout_from_ms,
            HoldoutOpening.holdout_to_ms == opening.holdout_to_ms,
            HoldoutOpening.id != opening.id,
        )
        .execution_options(populate_existing=True)
    )
    for other in (await session.execute(stmt)).scalars():
        result = other.result if isinstance(other.result, dict) else {}
        if "admission" not in result:
            continue
        if other.model_version == model_version or result.get("trial_id") == trial_id:
            raise ArtifactError("цей рецепт уже перевірявся на цьому hold-out")


def _walk_forward_matches(walk_forward: Sequence[BacktestTrade], trial: ExperimentTrial) -> None:
    times = [trade.entry_open_time_ms for trade in walk_forward]
    r_values = [trade.r_multiple for trade in walk_forward]
    if times != list(trial.oos_entry_ms or []):
        raise ArtifactError("угоди walk-forward не ті, що в спробі G1")
    if r_values != [_stored_decimal(item, "oos_results") for item in trial.oos_results]:
        raise ArtifactError("R угод walk-forward не ті, що в спробі G1")


def _corridor_trades_match(
    trades: Sequence[BacktestTrade],
    trial: ExperimentTrial,
    admission: Mapping[str, Any],
) -> None:
    expected_times = [*list(trial.oos_entry_ms or []), *list(admission["holdout_entry_ms"])]
    expected_r = [
        *(_stored_decimal(item, "oos_results") for item in trial.oos_results),
        *(_stored_decimal(item, "holdout_r") for item in admission["holdout_r"]),
    ]
    if not trades or len(trades) != int(admission["oos_trades"]) or len(trades) != len(expected_times):
        raise ArtifactError("коридор рахується з усіх out-of-sample угод рецепта")
    if any(trades[index].entry_open_time_ms > trades[index + 1].entry_open_time_ms for index in range(len(trades) - 1)):
        raise ArtifactError("угоди коридора не в порядку часу")
    if [trade.entry_open_time_ms for trade in trades] != expected_times:
        raise ArtifactError("угоди коридора не ті, що в журналі і допуску")
    if [trade.r_multiple for trade in trades] != expected_r:
        raise ArtifactError("R угод коридора не ті, що в журналі і допуску")


def _require_stored_admission(admission: Mapping[str, Any], opening: HoldoutOpening, label_type: str) -> None:
    """Записаний допуск ще раз проти порогів: рядок БД теж не приймається на слово."""
    if admission.get("passed") is not True or admission.get("reasons") not in ([],):
        raise ArtifactError("немає успішного результату hold-out")
    window_from = admission.get("window_from_ms")
    window_to = admission.get("window_to_ms")
    if (
        type(window_from) is not int
        or isinstance(window_from, bool)
        or type(window_to) is not int
        or isinstance(window_to, bool)
        or (window_from, window_to) != (opening.holdout_from_ms, opening.holdout_to_ms)
    ):
        raise ArtifactError("вікно допуску не збігається з відкритим hold-out")
    trades = admission.get("holdout_trades")
    oos = admission.get("oos_trades")
    if (
        type(trades) is not int
        or isinstance(trades, bool)
        or type(oos) is not int
        or isinstance(oos, bool)
        or trades < HOLDOUT_MIN_TRADES
        or oos < OOS_MIN_TRADES
    ):
        raise ArtifactError("успішний допуск не набирає угод")
    entries = admission.get("holdout_entry_ms")
    r_values = admission.get("holdout_r")
    if (
        not isinstance(entries, list)
        or not isinstance(r_values, list)
        or len(entries) != trades
        or len(r_values) != trades
    ):
        raise ArtifactError("у допуску немає угод hold-out")
    if admission.get("lower_pf_above_one") is not True:
        raise ArtifactError("успішний допуск без нижньої межі PF")
    if _stored_decimal(admission.get("profitable_fold_share"), "profitable_fold_share") < FOLD_SHARE_MIN:
        raise ArtifactError("успішний допуск без частки прибуткових фолдів")
    drawdown = _stored_decimal(admission.get("max_drawdown_pct"), "max_drawdown_pct")
    limit = _stored_decimal(admission.get("max_drawdown_limit_pct"), "max_drawdown_limit_pct")
    if limit > HOLDOUT_MAX_DRAWDOWN_PCT or drawdown > limit:
        raise ArtifactError("успішний допуск з просадкою понад ліміт")
    pf = admission.get("holdout_net_pf")
    if pf is None:
        if admission.get("holdout_pf_unbounded") is not True:
            raise ArtifactError("успішний допуск без PF")
    elif _stored_decimal(pf, "holdout_net_pf") < HOLDOUT_MIN_NET_PF:
        raise ArtifactError("успішний допуск нижче порога PF")
    filtered = _stored_money(admission.get("filtered_net"))
    if filtered is None:
        raise ArtifactError("успішний допуск без суми фільтра")
    _require_filter_gap(
        label_type,
        filtered,
        _stored_money(admission.get("unfiltered_net")),
        _stored_money(admission.get("scored_unfiltered_net")),
    )


def _stored_money(value: object) -> Decimal | None:
    if value is None:
        return None
    return _stored_decimal(value, "admission")


def _stored_decimal(value: object, name: str) -> Decimal:
    if type(value) is Decimal:
        amount = value
    elif type(value) is str:
        try:
            amount = Decimal(value)
        except InvalidOperation as exc:
            raise ArtifactError(f"{name} зіпсований") from exc
    else:
        raise ArtifactError(f"{name} має бути рядком Decimal")
    try:
        require_decimal(name, amount)
    except (TypeError, ValueError) as exc:
        raise ArtifactError(f"{name} зіпсований") from exc
    return amount


def _require_filter_gap(
    label_type: str,
    filtered: Decimal,
    unfiltered: Decimal | None,
    scored: Decimal | None,
) -> None:
    if label_type == "meta":
        if unfiltered is None or scored is None or not (filtered > unfiltered and filtered > scored):
            raise ArtifactError("фільтр не кращий за правило")
        return
    if unfiltered is not None or scored is not None:
        raise ArtifactError("порівняння з фільтром лише для meta")


def _holdout_report(report: BacktestReport, trade_symbol: str, holdout_from_ms: int, holdout_to_ms: int) -> None:
    if type(report) is not BacktestReport:
        raise ArtifactError("звіт має бути BacktestReport")
    if report.symbol != trade_symbol:
        raise ArtifactError("звіт hold-out — лише торгового символу моделі")
    if report.cost_factor != 1:
        raise ArtifactError("звіт hold-out — з базовими витратами, не стрес")
    try:
        _report_matches_trades(report)
    except ValueError as exc:
        raise ArtifactError(f"звіт hold-out не збігається з угодами: {exc}") from exc
    for trade in report.trades:
        if trade.entry_open_time_ms < holdout_from_ms or trade.entry_open_time_ms >= holdout_to_ms:
            raise ArtifactError("угода звіту поза запечатаним hold-out")
        if trade.exit_open_time_ms >= holdout_to_ms:
            raise ArtifactError("вихід угоди за кінцем hold-out")


def _guard_retrain(fit: Any, evidence: Mapping[str, Any]) -> None:
    if fit.include_holdout is not True:
        raise ArtifactError("перенавчання має включати hold-out")
    if _lgbm_payload(fit.lgbm) != evidence["recipe"]["lgbm"]:
        raise ArtifactError("параметри LightGBM перенавчання відрізняються від рецепта")
    if fit.model.schema_hash != evidence["feature_schema_hash"]:
        raise ArtifactError("схема фіч перенавчання змінилася")


async def _passing_opening(session: AsyncSession, evidence: ModelArtifact) -> HoldoutOpening:
    """Рівно одне відкриття з успішним допуском і жодного невдалого для цієї моделі."""
    stmt = (
        select(HoldoutOpening)
        .where(
            HoldoutOpening.model_version == evidence.model_version,
            HoldoutOpening.experiment_id == evidence.experiment_id,
        )
        .execution_options(populate_existing=True)
    )
    openings = list((await session.execute(stmt)).scalars())
    passed: list[HoldoutOpening] = []
    for opening in openings:
        result = opening.result if isinstance(opening.result, dict) else {}
        admission = result.get("admission")
        if not isinstance(admission, dict):
            continue
        if admission.get("passed") is not True:
            raise ArtifactError("модель уже має невдалий допуск hold-out")
        if result.get("passed") is not True or result.get("gate") != "G1":
            raise ArtifactError("допуск на відкритті без Gate G1")
        _require_stored_admission(admission, opening, str(evidence.label_type))
        passed.append(opening)
    if len(passed) != 1:
        raise ArtifactError("перенавчання потребує рівно один успішний допуск hold-out")
    return passed[0]


def _same_recipe(evidence: Mapping[str, Any], candidate: Mapping[str, Any]) -> None:
    for key in (
        "label_type",
        "trade_symbol",
        "timeframe",
        "feature_schema_hash",
        "preprocessing_version",
        "primary_strategy",
        "decision_rule",
        "target_params",
        "cost_model",
        "experiment_id",
    ):
        if evidence.get(key) != candidate.get(key):
            raise ArtifactError(f"рецепт відрізняється полем {key}")
    if sorted(evidence.get("train_symbols", [])) != sorted(candidate.get("train_symbols", [])):
        raise ArtifactError("рецепт відрізняється полем train_symbols")
    if evidence.get("recipe", {}).get("lgbm") != candidate.get("recipe", {}).get("lgbm"):
        raise ArtifactError("рецепт LightGBM відрізняється")
    if evidence.get("reproducibility", {}).get("seed") != candidate.get("reproducibility", {}).get("seed"):
        raise ArtifactError("seed перенавчання відрізняється")
    if evidence.get("recipe", {}).get("include_holdout") is not False:
        raise ArtifactError("модель допуску вже бачила hold-out")
    evidence_calibrator = evidence.get("reproducibility", {}).get("calibrator_sha256")
    if evidence_calibrator != candidate.get("reproducibility", {}).get("calibrator_sha256"):
        raise ArtifactError("калібратор перенавчання не той, що в моделі допуску")


async def _transition(
    session: AsyncSession,
    row: ModelArtifact,
    *,
    status: str,
    metadata: dict[str, Any],
    live_corridor: dict[str, Any] | None,
    confirmed_audit_id: int | None = None,
) -> None:
    if status not in _TRANSITIONS[row.status]:
        raise ArtifactError(f"перехід {row.status} → {status} заборонений")
    previous = row.status
    values: dict[str, Any] = {
        "status": status,
        "metadata_json": metadata,
        "live_corridor": live_corridor,
        "status_changed_at_ms": now_ms(),
    }
    if confirmed_audit_id is not None:
        values["confirmed_audit_id"] = confirmed_audit_id
    result = await session.execute(
        update(ModelArtifact)
        .where(ModelArtifact.model_version == row.model_version, ModelArtifact.status == row.status)
        .values(**values)
    )
    if result.rowcount != 1:  # type: ignore[attr-defined]
        raise ArtifactError("статус моделі змінився паралельно")
    await session.flush()
    # Джерело статусу — БД. Файл — копія: його збій не блокує перехід (зокрема RETIRED), а поля переходу
    # `load_registered_model` бере з БД. Запис атомарний (tmp + rename), тож напівзаписаного файлу немає.
    try:
        _write_json(_directory_of(row) / METADATA_FILE, metadata)
    except (OSError, ArtifactError) as exc:
        _LOG.error(
            "model_metadata_write_failed",
            model_version=row.model_version,
            status=status,
            error=type(exc).__name__,
        )
    _LOG.info("model_status", model_version=row.model_version, previous=previous, status=status)


def _directory_of(row: ModelArtifact) -> Path:
    if row.artifact_path is not None:
        return Path(row.artifact_path).parent
    raw = dict(row.metadata_json).get("artifact_dir")
    if type(raw) is not str:
        raise ArtifactError("у моделі немає каталогу")
    return Path(raw)


async def _model(session: AsyncSession, model_version: str) -> ModelArtifact:
    # Статус — з БД, не з кешу сесії: сесії проєкту живуть довго (`expire_on_commit=False`).
    row = await session.get(ModelArtifact, model_version, populate_existing=True)
    if row is None:
        raise ArtifactError("немає такої моделі")
    return row


async def _experiment(session: AsyncSession, experiment_id: str) -> Experiment:
    experiment = await session.get(Experiment, experiment_id, populate_existing=True)
    if experiment is None:
        raise ArtifactError("немає такого експерименту")
    return experiment


def _stored_split(experiment: Experiment) -> ResearchSplit:
    payload = experiment.params.get("validation") if isinstance(experiment.params, Mapping) else None
    if not isinstance(payload, Mapping):
        raise SplitLeakError("в експерименту немає розбивки")
    return split_from_payload(payload)


def _load_directory(directory: Path) -> LoadedModel:
    payload = json.loads((directory / METADATA_FILE).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("model_version") != directory.name:
        raise ArtifactError("model_metadata.json не цієї версії")
    label = payload.get("label_type")
    reproducibility = payload.get("reproducibility")
    if not isinstance(reproducibility, dict):
        raise ArtifactError("у метаданих немає reproducibility")
    model_path = directory / MODEL_FILE
    calibrator_path = directory / CALIBRATOR_FILE
    booster: lgb.Booster | None = None
    calibrator: ProbabilityCalibrator | None = None
    if label == "rule_only":
        if model_path.exists() or calibrator_path.exists():
            raise ArtifactError("rule_only не має файлу моделі")
        if reproducibility.get("model_sha256") is not None or reproducibility.get("calibrator_sha256") is not None:
            raise ArtifactError("rule_only не має хеша файлу моделі")
    elif label in ("meta", "direct"):
        booster = _load_booster(model_path, payload)
        if not calibrator_path.is_file() or _sha256_file(calibrator_path) != reproducibility.get("calibrator_sha256"):
            raise ArtifactError("хеш калібратора не збігається")
        calibrator = calibrator_from_payload(json.loads(calibrator_path.read_text(encoding="utf-8")))
        if calibrator.method != payload.get("recipe", {}).get("calibrator"):
            raise ArtifactError("метод калібратора не збігається з метаданими")
    else:
        raise ArtifactError("невідомий label_type")
    _rule(payload["decision_rule"])
    _target(payload["target_params"])
    _costs(payload["cost_model"])
    return LoadedModel(directory=directory, metadata=payload, booster=booster, calibrator=calibrator)


def _load_booster(path: Path, metadata: Mapping[str, Any]) -> lgb.Booster:
    if not path.is_file():
        raise ArtifactError("немає файлу моделі")
    # Бустер читається з тих самих байтів, що хешувались: файл не підмінити між перевіркою і читанням.
    data = path.read_bytes()
    recorded = metadata.get("reproducibility", {}).get("model_sha256")
    if recorded != "sha256:" + hashlib.sha256(data).hexdigest():
        raise ArtifactError("хеш файлу моделі не збігається")
    return lgb.Booster(model_str=data.decode("utf-8"))


def _booster_sha256(booster: lgb.Booster) -> str:
    """Хеш того самого тексту, що `write_artifact` пише в `model.txt`."""
    with tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / MODEL_FILE
        booster.save_model(str(path))
        return _sha256_file(path)


def _rule(payload: Mapping[str, Any]) -> DecisionRule:
    if set(payload) != {"min_edge_atr", "min_confidence"}:
        raise ArtifactError("decision_rule — min_edge_atr і min_confidence")
    rule = DecisionRule(
        min_edge_atr=Decimal(str(payload["min_edge_atr"])),
        min_confidence=Decimal(str(payload["min_confidence"])),
    )
    if rule.metadata() != dict(payload):
        raise ArtifactError("decision_rule не збігається з DecisionRule")
    return rule


def _stored_costs(meta: Mapping[str, Any], costs: CostModel | None) -> CostModel:
    stored = _costs(meta["cost_model"])
    if costs is not None and costs != stored:
        raise ArtifactError("витрати перенавчання відрізняються від рецепта")
    return stored


def _target_payload(target: TargetParams) -> dict[str, Any]:
    try:
        return target_payload(target)
    except ValueError as exc:
        raise ArtifactError(str(exc)) from exc


def _target(payload: Mapping[str, Any]) -> TargetParams:
    return TargetParams(
        tp_atr_mult=Decimal(str(payload["tp_atr_mult"])),
        sl_atr_mult=Decimal(str(payload["sl_atr_mult"])),
        horizon_bars=int(payload["horizon_bars"]),
    )


def _costs(payload: Mapping[str, Any]) -> CostModel:
    kwargs: dict[str, Any] = {}
    for key, value in payload.items():
        kwargs[key] = None if value is None else Decimal(str(value))
    return CostModel(**kwargs)


def _cost_payload(costs: CostModel) -> dict[str, Any]:
    try:
        return costs_payload(costs)
    except ValueError as exc:
        raise ArtifactError(str(exc)) from exc


def _lgbm_payload(params: Mapping[str, Any]) -> dict[str, Any]:
    if set(params) != _LGBM_KEYS:
        raise ArtifactError("рецепт LightGBM має рівно поля журналу")
    if params.get("class_weight") is not None or params.get("deterministic") is not True or params.get("n_jobs") != 1:
        raise ArtifactError("рецепт має лишатися детермінованим на CPU з одним потоком")
    payload: dict[str, Any] = {}
    for key in sorted(_LGBM_KEYS):
        value = params[key]
        if key in _LGBM_DECIMAL:
            if type(value) is not Decimal:
                raise ArtifactError(f"{key} має бути Decimal")
            payload[key] = format(value, "f")
        elif key in _LGBM_INT:
            if type(value) is not int or isinstance(value, bool):
                raise ArtifactError(f"{key} має бути int")
            payload[key] = value
        elif key in _LGBM_BOOL:
            if type(value) is not bool:
                raise ArtifactError(f"{key} має бути bool")
            payload[key] = value
        elif key == "boosting_type":
            if value != "gbdt":
                raise ArtifactError("boosting_type лише gbdt")
            payload[key] = value
        else:
            payload[key] = None
    return payload


def _lgbm_from_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    if set(payload) != _LGBM_KEYS:
        raise ArtifactError("збережений рецепт LightGBM зіпсований")
    restored: dict[str, Any] = {}
    for key, value in payload.items():
        if key in _LGBM_DECIMAL:
            restored[key] = Decimal(str(value))
        else:
            restored[key] = value
    if _lgbm_payload(restored) != dict(payload):
        raise ArtifactError("рецепт LightGBM не читається назад")
    return restored


def _library_versions(versions: Mapping[str, str]) -> dict[str, str]:
    if "lightgbm" not in versions or "python" not in versions:
        raise ArtifactError("потрібні версії python і lightgbm")
    cleaned: dict[str, str] = {}
    for key, value in versions.items():
        if type(key) is not str or type(value) is not str or not key or not value:
            raise ArtifactError("версія бібліотеки зіпсована")
        cleaned[key] = value
    return cleaned


def _optional_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


@fixed_context
def _probability(value: float) -> Decimal:
    if not np.isfinite(value):
        raise InferenceBlocked("ймовірність моделі не скінченна")
    quantized = Decimal(format(value, ".12f")).quantize(_PROBABILITY_QUANTUM)
    if quantized < 0 or quantized > 1:
        raise InferenceBlocked("ймовірність моделі поза [0, 1]")
    return quantized


def _sha256_file(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _json_text(payload: Mapping[str, Any]) -> str:
    """Канонічний текст файлу: той самий для запису і для `decision_payload`."""
    return json.dumps(_json_ready(payload, path="metadata"), indent=2, sort_keys=True) + "\n"


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    text = _json_text(payload)
    scratch = path.with_name(f".{path.name}.tmp")
    scratch.write_text(text, encoding="utf-8")
    os.replace(scratch, path)


# Поля, які змінює перехід статусу. Решта метаданих після реєстрації не змінюється.
_MUTABLE_KEYS = frozenset({"status", "live_corridor", "n_trials_total", "metrics", "approval_audit_after_id"})


def _immutable(metadata: Mapping[str, Any]) -> dict[str, Any]:
    fixed = {key: value for key, value in metadata.items() if key not in _MUTABLE_KEYS}
    reproducibility = fixed.get("reproducibility")
    if isinstance(reproducibility, Mapping):
        fixed["reproducibility"] = {
            key: value for key, value in reproducibility.items() if key != "corridor_replicates"
        }
    return fixed


def _canonical(value: Any) -> Any:
    """Значення для `data_hash`: лише JSON-типи, Decimal рядком, float — hex."""
    if isinstance(value, Enum):
        return _canonical(value.value)
    if value is None or type(value) is bool or type(value) is int or type(value) is str:
        return value
    if type(value) is Decimal:
        return format(value, "f")
    if type(value) is float:
        return value.hex()
    if isinstance(value, np.generic):
        return _canonical(value.item())
    if isinstance(value, BaseModel):
        return {name: _canonical(getattr(value, name)) for name in type(value).model_fields}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: _canonical(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, np.ndarray):
        return [_canonical(item) for item in value.tolist()]
    raise ArtifactError(f"data_hash: несеріалізований тип {type(value).__name__}")


def _json_ready(value: Any, *, path: str) -> Any:
    if value is None or type(value) is bool or type(value) is int or type(value) is str:
        return value
    if type(value) is Decimal:
        return format(value, "f")
    if type(value) is float:
        raise ArtifactError(f"{path}: float заборонений")
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item, path=f"{path}.{key}") for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    raise ArtifactError(f"{path}: несеріалізований тип {type(value).__name__}")


def _symbol(symbol: str) -> None:
    if not _SYMBOL.fullmatch(symbol):
        raise ArtifactError("символ не канонічний")


def _millis(value: int) -> int:
    if type(value) is not int or isinstance(value, bool) or value < 0:
        raise ArtifactError("час має бути невід'ємний int мс")
    return value
