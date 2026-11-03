"""Діапазони, метрика і параметри задачі Optuna без навчання і без БД (T2.10)."""

from __future__ import annotations

from decimal import Context, Decimal, localcontext

import optuna
import pytest
from optuna.distributions import FloatDistribution
from optuna.samplers import TPESampler
from optuna.trial import TrialState, create_trial

from app.ml.features import TimedValue
from app.ml.lgbm_space import (
    SEARCH_BAGGING,
    SEARCH_LEARNING_RATE,
    SEARCH_MAX_DEPTH,
    SEARCH_MIN_CHILD_SAMPLES,
    SEARCH_N_ESTIMATORS,
    SEARCH_NUM_LEAVES,
    SEARCH_PARAM_NAMES,
    SEARCH_REG_LAMBDA,
    lgbm_search_record,
)
from app.ml.optuna_search import (
    OPTUNA_TRIALS_MAX,
    OPTUNA_TRIALS_MIN,
    OptunaParamsError,
    _assemble,
    _distribution_params,
    _funding_events,
    _Replay,
    _study,
    _SymbolRows,
    lgbm_distributions,
    objective_score,
    parse_optuna_job,
    suggest_lgbm,
)
from app.ml.strategy import BREAKOUT, EMA_CROSS
from app.ml.trade_sim import SimCandle
from app.ml.validation import SplitLeakError, build_split, split_from_payload, validation_payload
from tests.risk.conftest import instrument

optuna.logging.set_verbosity(optuna.logging.WARNING)

_HOUR = 3_600_000


def _params(**overrides: object) -> dict[str, object]:
    raw: dict[str, object] = {
        "strategy": "breakout",
        "n_trials": 100,
        "seed": 7,
        "symbols": ["BTC/USDT:USDT"],
        "trading_symbol": "BTC/USDT:USDT",
        "initial_equity": "10000",
        "maintenance_margin_rate": "0.005",
        "min_equity_usd": "1000",
        "max_notional_per_symbol_usd": "5000",
    }
    raw.update(overrides)
    return raw


def test_ranges_are_the_spec_and_fixed_fields_are_not_searched() -> None:
    assert SEARCH_NUM_LEAVES == (8, 64)
    assert SEARCH_MAX_DEPTH == (3, 8)
    assert (Decimal("0.01"), Decimal("0.1")) == SEARCH_LEARNING_RATE
    assert SEARCH_N_ESTIMATORS == (200, 1500)
    assert SEARCH_MIN_CHILD_SAMPLES == (50, 500)
    assert (Decimal("0.6"), Decimal(1)) == SEARCH_BAGGING
    assert (Decimal(0), Decimal(10)) == SEARCH_REG_LAMBDA
    assert set(SEARCH_PARAM_NAMES) == {
        "num_leaves",
        "max_depth",
        "learning_rate",
        "n_estimators",
        "min_child_samples",
        "subsample",
        "colsample_bytree",
        "reg_lambda",
    }
    point = lgbm_search_record(
        {
            "num_leaves": 8,
            "max_depth": 3,
            "learning_rate": Decimal("0.01"),
            "n_estimators": 200,
            "min_child_samples": 50,
            "subsample": Decimal(1),
            "colsample_bytree": Decimal("0.6"),
            "reg_lambda": Decimal(0),
        }
    )
    assert point["n_jobs"] == 1
    assert point["deterministic"] is True
    assert point["class_weight"] is None
    assert point["subsample_freq"] == 0
    assert "device" not in point
    bagged = lgbm_search_record({**_point_kwargs(), "subsample": Decimal("0.6")})
    assert bagged["subsample_freq"] == 1
    with pytest.raises(ValueError, match="діапазон"):
        lgbm_search_record({**_point_kwargs(), "device": "gpu"})


def test_suggest_uses_log_learning_rate_and_linear_lambda() -> None:
    space = lgbm_distributions()
    assert set(space) == set(SEARCH_PARAM_NAMES)
    learning_rate = space["learning_rate"]
    reg_lambda = space["reg_lambda"]
    assert isinstance(learning_rate, FloatDistribution)
    assert isinstance(reg_lambda, FloatDistribution)
    assert learning_rate.log is True
    assert reg_lambda.log is False
    study = optuna.create_study(direction="maximize", sampler=TPESampler(seed=1))
    record = suggest_lgbm(study.ask())
    assert set(record) >= set(SEARCH_PARAM_NAMES)
    assert record["n_jobs"] == 1
    assert record["class_weight"] is None


def test_same_seed_asks_the_same_first_point() -> None:
    first = _first_point(11)
    second = _first_point(11)
    assert first == second
    assert _first_point(12) != first


def test_journal_point_can_be_replayed() -> None:
    study = optuna.create_study(direction="maximize", sampler=TPESampler(seed=3))
    trial = study.ask()
    record = suggest_lgbm(trial)
    stored = {key: format(value, "f") if type(value) is Decimal else value for key, value in record.items()}
    replayed = optuna.create_study(direction="maximize", sampler=TPESampler(seed=3))
    replayed.add_trial(
        create_trial(
            state=TrialState.COMPLETE,
            value=1.5,
            params=_distribution_params(stored),
            distributions=lgbm_distributions(),
        )
    )
    assert len(replayed.trials) == 1
    assert replayed.ask().number == 1


def _journal_point(trial: optuna.trial.Trial) -> dict[str, float | int]:
    """Точка в тому вигляді, в якому її читає продовження: Decimal-рядки журналу → float розподілу."""
    record = suggest_lgbm(trial)
    stored = {key: format(value, "f") if type(value) is Decimal else value for key, value in record.items()}
    return _distribution_params(stored)


def _replay_of(study: optuna.Study, count: int) -> list[_Replay]:
    replay: list[_Replay] = []
    for number in range(count):
        trial = study.ask()
        params = _journal_point(trial)
        study.tell(trial, values=float(number))
        replay.append(_Replay(number=number, state="COMPLETE", params=params, value=float(number), steps={}))
    return replay


def test_resume_does_not_repeat_the_recorded_points() -> None:
    """Новий `TPESampler(seed)` починав випадковий старт спочатку і повторював точки 0, 1, 2… з журналу."""
    replay = _replay_of(_study(7, (), sampler=None, pruner=None), 3)
    recorded = [item.params for item in replay]
    naive = optuna.create_study(direction="maximize", sampler=TPESampler(seed=7))
    for item in replay:
        naive.add_trial(
            create_trial(
                state=TrialState.COMPLETE, value=item.value, params=item.params, distributions=lgbm_distributions()
            )
        )
    assert _journal_point(naive.ask()) == recorded[0]
    resumed = _study(7, replay, sampler=None, pruner=None)
    asked = [_journal_point(resumed.ask()) for _ in range(3)]
    assert all(point not in recorded for point in asked)
    # Перервана спроба не записується: за того самого журналу продовження питає ту саму точку.
    assert _journal_point(_study(7, replay, sampler=None, pruner=None).ask()) == asked[0]


def test_objective_is_median_minus_spread() -> None:
    assert objective_score([Decimal(10)]) == Decimal(10)
    nets = [Decimal(10), Decimal(20)]
    with localcontext(Context(prec=28)):
        spread = (Decimal(50)).sqrt()
        expected = Decimal(15) - spread
    assert objective_score(nets) == expected
    # Три фолди: медіана — середній, розкид — вибіркове відхилення (n − 1): sqrt(4) = 2.
    assert objective_score([Decimal(1), Decimal(3), Decimal(5)]) == Decimal(1)
    with pytest.raises(ValueError, match="кожного фолду"):
        objective_score([])


def test_objective_prefers_a_steady_profit_over_no_trades() -> None:
    """Штраф з множником від медіани ставив модель без угод вище за прибуткову (−60 проти −2950)."""
    profit = objective_score([Decimal(100), Decimal(120), Decimal(80)])
    flat = objective_score([Decimal(0), Decimal(0), Decimal(0)])
    loss = objective_score([Decimal(-100), Decimal(-120), Decimal(-80)])
    assert profit == Decimal(80)
    assert flat == Decimal(0)
    assert profit > flat > loss
    # Той самий розкид: вища медіана — вища оцінка.
    for shift in (Decimal(-50), Decimal(0), Decimal(50)):
        base = [Decimal(10) + shift, Decimal(30) + shift, Decimal(20) + shift]
        higher = [item + Decimal(5) for item in base]
        assert objective_score(higher) - objective_score(base) == Decimal(5)


def test_funding_price_is_the_last_closed_bar_not_the_one_that_opens_at_payment() -> None:
    bars = [
        SimCandle(open_time_ms=hour * _HOUR, open=price, high=price + 2, low=price, close=price + 1)
        for hour, price in ((0, Decimal(100)), (1, Decimal(110)), (2, Decimal(120)), (5, Decimal(150)))
    ]
    payments = [
        (0, Decimal("0.0001")),  # жодна свічка ще не закрилась
        (_HOUR, Decimal("0.0002")),  # свічка 0 закрилась рівно в момент виплати
        (2 * _HOUR, Decimal("0.0003")),  # не close свічки 2, що закриється о 3:00
        (5 * _HOUR, Decimal("0.0004")),  # після дірки 3–4 — остання закрита, свічка 2
        (8 * _HOUR, Decimal("0.0005")),
    ]
    events = _funding_events(payments, bars)
    assert [(event.time_ms, event.rate, event.mark_price) for event in events] == [
        (_HOUR, Decimal("0.0002"), Decimal(101)),
        (2 * _HOUR, Decimal("0.0003"), Decimal(111)),
        (5 * _HOUR, Decimal("0.0004"), Decimal(121)),
        (8 * _HOUR, Decimal("0.0005"), Decimal(151)),
    ]
    assert _funding_events(payments, []) == ()


def _rows(marks: list[int]) -> _SymbolRows:
    """100 годин свічок; mark — лише на годинах із `marks`; funding і деривативи щогодини."""
    price = Decimal(100)
    bars = [
        SimCandle(open_time_ms=hour * _HOUR, open=price, high=price + 2, low=price, close=price + 1)
        for hour in range(100)
    ]
    return _SymbolRows(
        symbol="BTC/USDT:USDT",
        candles=tuple((bar, Decimal(5)) for bar in bars),
        instrument=instrument(symbol="BTC/USDT:USDT"),
        marks=tuple(bar for bar in bars if bar.open_time_ms // _HOUR in marks),
        payments=tuple((hour * _HOUR, Decimal("0.0001")) for hour in range(0, 100, 8)),
        open_interest=tuple(TimedValue(hour * _HOUR, 1.0) for hour in range(100)),
        premium=(),
        long_short=(),
        liquidations=(),
        gaps={},
    )


def test_holdout_rows_do_not_decide_mark_alignment_or_funding() -> None:
    """Дірка в mark-ряді hold-out не вимикає mark для walk-forward; ряди ріжуться до вирівнювання."""
    cut = 80 * _HOUR
    full = _assemble([_rows(list(range(100)))], cut)
    holed = _assemble([_rows([hour for hour in range(100) if hour != 95])], cut)
    for histories, features in (full, holed):
        history = histories[0]
        assert history.mark_candles is not None
        assert len(history.mark_candles) == 80
        assert history.candles[-1].open_time_ms == cut - _HOUR
        assert all(event.time_ms < cut for event in history.funding)
        data = features["BTC/USDT:USDT"]
        assert len(data.candles) == 80
        assert all(point.ts_ms < cut for point in data.open_interest)
        assert all(point.ts_ms < cut for point in data.funding)
    assert full == holed
    # Дірка до hold-out, як і раніше, вимикає mark (SL за last), але лише через дані walk-forward.
    before = _assemble([_rows([hour for hour in range(100) if hour != 40])], cut)
    assert before[0][0].mark_candles is None


def test_job_budget_and_forbidden_keys() -> None:
    assert parse_optuna_job(_params()).strategy == BREAKOUT
    assert parse_optuna_job(_params(strategy="ema_cross", n_trials=200)).strategy == EMA_CROSS
    for bad in (99, 201, True):
        with pytest.raises(OptunaParamsError):
            parse_optuna_job(_params(n_trials=bad))
    with pytest.raises(OptunaParamsError):
        parse_optuna_job(_params(strategy="always_long"))
    with pytest.raises(OptunaParamsError):
        parse_optuna_job(_params(tp_atr_mult="2"))
    with pytest.raises(OptunaParamsError):
        parse_optuna_job(_params(num_leaves=16))
    assert OPTUNA_TRIALS_MIN == 100
    assert OPTUNA_TRIALS_MAX == 200


def test_split_roundtrip_rejects_a_shifted_window() -> None:
    split = build_split(
        data_from_ms=0,
        data_to_ms=8_000 * _HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        min_train_ms=4_000 * _HOUR,
        test_segment_ms=200 * _HOUR,
        test_segment_extended_ms=400 * _HOUR,
        min_trades_per_fold=1,
    )
    payload = validation_payload(split)
    restored = split_from_payload(payload)
    assert validation_payload(restored) == payload
    shifted = dict(payload)
    shifted["holdout_from_ms"] = payload["holdout_from_ms"] + _HOUR
    with pytest.raises(SplitLeakError):
        split_from_payload(shifted)


def _point_kwargs() -> dict[str, object]:
    return {
        "num_leaves": 8,
        "max_depth": 3,
        "learning_rate": Decimal("0.01"),
        "n_estimators": 200,
        "min_child_samples": 50,
        "subsample": Decimal(1),
        "colsample_bytree": Decimal(1),
        "reg_lambda": Decimal(0),
    }


def _first_point(seed: int) -> dict[str, object]:
    study = optuna.create_study(direction="maximize", sampler=TPESampler(seed=seed))
    return suggest_lgbm(study.ask())
