"""Діапазони LightGBM для Optuna (ТЗ §5А.7).

Окремо від `meta_model`: той модуль імпортує LightGBM, а перевірка точки потрібна і воркеру,
і тестам діапазонів без навчання. Журнальний рецепт `lgbm_journal_params` сюди не входить.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from app.risk.numbers import require_decimal

# Межі включні. `learning_rate` у пошуку логарифмічний; `reg_lambda` лінійний, бо діапазон містить 0.
SEARCH_NUM_LEAVES: tuple[int, int] = (8, 64)
SEARCH_MAX_DEPTH: tuple[int, int] = (3, 8)
SEARCH_LEARNING_RATE: tuple[Decimal, Decimal] = (Decimal("0.01"), Decimal("0.1"))
SEARCH_N_ESTIMATORS: tuple[int, int] = (200, 1500)
SEARCH_MIN_CHILD_SAMPLES: tuple[int, int] = (50, 500)
SEARCH_BAGGING: tuple[Decimal, Decimal] = (Decimal("0.6"), Decimal(1))
SEARCH_REG_LAMBDA: tuple[Decimal, Decimal] = (Decimal(0), Decimal(10))
SEARCH_PARAM_NAMES: tuple[str, ...] = (
    "num_leaves",
    "max_depth",
    "learning_rate",
    "n_estimators",
    "min_child_samples",
    "subsample",
    "colsample_bytree",
    "reg_lambda",
)
# Хвіст train для ранньої зупинки. Це не тест і не hold-out.
EARLY_STOP_FRACTION: Decimal = Decimal("0.2")
EARLY_STOPPING_ROUNDS: int = 50


def search_point(values: Mapping[str, Any]) -> dict[str, Any]:
    """Вісім параметрів пошуку. Зайві поля рецепта відкидаються: їх виставляє `lgbm_search_record`."""
    if any(name not in values for name in SEARCH_PARAM_NAMES):
        raise ValueError("пошук LightGBM приймає лише діапазони ТЗ §5А.7")
    return {name: values[name] for name in SEARCH_PARAM_NAMES}


def lgbm_search_record(values: Mapping[str, Any]) -> dict[str, Any]:
    """Точка з діапазонів ТЗ §5А.7 плюс фіксовані поля рецепта.

    Вхід — лише вісім параметрів пошуку. `device`, `n_jobs`, `deterministic` і `class_weight`
    ззовні не приймаються: інакше пошук міг би вимкнути відтворюваність або ввімкнути GPU.
    `subsample_freq` не шукається: 0 при `subsample` = 1 (як у стандарті LightGBM), інакше 1,
    щоб частка під вибірки справді застосовувалась.
    """
    point = search_point(values)
    if set(values) != set(SEARCH_PARAM_NAMES):
        raise ValueError("пошук LightGBM приймає лише діапазони ТЗ §5А.7")
    num_leaves = _search_int("num_leaves", point["num_leaves"], *SEARCH_NUM_LEAVES)
    max_depth = _search_int("max_depth", point["max_depth"], *SEARCH_MAX_DEPTH)
    learning_rate = _search_decimal("learning_rate", point["learning_rate"], *SEARCH_LEARNING_RATE)
    n_estimators = _search_int("n_estimators", point["n_estimators"], *SEARCH_N_ESTIMATORS)
    min_child_samples = _search_int("min_child_samples", point["min_child_samples"], *SEARCH_MIN_CHILD_SAMPLES)
    subsample = _search_decimal("subsample", point["subsample"], *SEARCH_BAGGING)
    colsample_bytree = _search_decimal("colsample_bytree", point["colsample_bytree"], *SEARCH_BAGGING)
    reg_lambda = _search_decimal("reg_lambda", point["reg_lambda"], *SEARCH_REG_LAMBDA)
    return {
        "boosting_type": "gbdt",
        "num_leaves": num_leaves,
        "max_depth": max_depth,
        "learning_rate": learning_rate,
        "n_estimators": n_estimators,
        "min_child_samples": min_child_samples,
        "min_child_weight": Decimal("0.001"),
        "subsample": subsample,
        "subsample_freq": 0 if subsample == Decimal(1) else 1,
        "colsample_bytree": colsample_bytree,
        "reg_alpha": Decimal(0),
        "reg_lambda": reg_lambda,
        "min_split_gain": Decimal(0),
        "deterministic": True,
        "force_col_wise": True,
        "n_jobs": 1,
        "class_weight": None,
    }


def _search_int(name: str, value: object, low: int, high: int) -> int:
    if type(value) is not int or isinstance(value, bool) or value < low or value > high:
        raise ValueError(f"{name} поза діапазоном Optuna")
    return value


def _search_decimal(name: str, value: object, low: Decimal, high: Decimal) -> Decimal:
    if type(value) is Decimal:
        number = value
    elif type(value) is float:
        if not math.isfinite(value):
            raise ValueError(f"{name} не скінченний")
        number = Decimal(format(value, ".12f"))
    else:
        raise ValueError(f"{name} має бути Decimal або float межі Optuna")
    require_decimal(name, number)
    if number < low or number > high:
        raise ValueError(f"{name} поза діапазоном Optuna")
    return number
