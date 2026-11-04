"""Артефакти, допуск hold-out, блок інференсу і перенавчання без підбору (T2.11)."""

from __future__ import annotations

import dataclasses
import json
import re
import subprocess
from decimal import Decimal
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pytest
import sklearn
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression

from app.exchange.types import PositionSide
from app.ml.artifacts import (
    CALIBRATOR_FILE,
    HOLDOUT_MAX_DRAWDOWN_PCT,
    HOLDOUT_MIN_NET_PF,
    METADATA_FILE,
    MODEL_FILE,
    REASON_DRAWDOWN,
    REASON_FILTER,
    REASON_MIN_TRADES,
    REASON_NET_PF,
    AdmissionDecision,
    ArtifactError,
    GitState,
    InferenceBlocked,
    ModelNotTradable,
    accept_primary,
    allocate_version,
    assess_holdout,
    booster_of,
    build_metadata,
    decision_payload,
    hash_candles,
    hash_inputs,
    infer_entry,
    load_model,
    predict_probability,
    read_git_state,
    strategy_from_metadata,
    write_artifact,
)
from app.ml.backtest import BacktestDecision, BacktestMetrics, BacktestReport, BacktestTrade
from app.ml.decision import DecisionRule, ProbabilityCalibrator, calibrator_from_payload, decide_entry
from app.ml.features import HOUR_MS, feature_columns, feature_schema_hash
from app.ml.meta_model import _grant_holdout, fit_frozen_meta, lgbm_journal_params
from app.ml.strategy import BREAKOUT, EMA_CROSS, SymbolHistory
from app.ml.trade_sim import ExitReason, SimCandle, StopPriceSource, TargetParams
from app.ml.validation import ResearchSplit, SplitLeakError, build_split
from app.risk.costs import CostModel, FundingEvent
from tests.risk.conftest import instrument

pytestmark = pytest.mark.unit

BTC = "BTC/USDT:USDT"
HOUR = HOUR_MS
_SEED = 7
_APP = Path(__file__).resolve().parents[2] / "app"


def test_primary_strategy_is_one_rule_not_the_selection_record() -> None:
    spec = strategy_from_metadata(EMA_CROSS.metadata())
    assert spec == EMA_CROSS
    with pytest.raises(ArtifactError, match="одного правила"):
        strategy_from_metadata({"selected": [EMA_CROSS.metadata()], "criterion": "median", "beats_flat": [True]})


def test_decision_rule_is_decimal_strings_and_live_corridor_starts_empty() -> None:
    meta = _metadata()
    assert meta["decision_rule"] == {"min_edge_atr": "0.10", "min_confidence": "0.40"}
    assert meta["primary_strategy"] == EMA_CROSS.metadata()
    assert meta["status"] == "TRAINED"
    assert meta["live_corridor"] is None
    assert meta["n_trials_total"] is None
    assert meta["label_type"] == "meta"
    assert all(isinstance(value, str) for value in meta["cost_model"].values() if value is not None)


def test_rule_only_has_no_model_file(tmp_path) -> None:  # type: ignore[no-untyped-def]
    meta = _metadata(label_type="rule_only", version="rule-only-2026-11-03-001")
    directory = write_artifact(tmp_path / meta["model_version"], meta, booster=None, calibrator=None)
    assert not (directory / MODEL_FILE).exists()
    assert not (directory / CALIBRATOR_FILE).exists()
    loaded = load_model(directory)
    assert loaded.booster is None
    versions = loaded.metadata["reproducibility"]["library_versions"]
    # Без статусу з БД модель не торгує, навіть якщо у файлі CANDIDATE.
    loaded.metadata["status"] = "CANDIDATE"
    with pytest.raises(ModelNotTradable, match="load_registered_model"):
        accept_primary(loaded, PositionSide.LONG, mode="demo", versions=versions)
    with pytest.raises(ModelNotTradable, match="TRAINED"):
        accept_primary(registered(loaded, "TRAINED"), PositionSide.LONG, mode="demo", versions=versions)
    candidate = registered(loaded, "CANDIDATE")
    assert accept_primary(candidate, PositionSide.LONG, mode="demo", versions=versions) is PositionSide.LONG
    with pytest.raises(ModelNotTradable, match="live"):
        accept_primary(candidate, PositionSide.LONG, mode="live", versions=versions)


def test_inference_blocks_a_different_lightgbm_version(tmp_path) -> None:  # type: ignore[no-untyped-def]
    loaded = _written_meta(tmp_path)
    columns = loaded.metadata["recipe"]["feature_columns"]
    versions = dict(loaded.metadata["reproducibility"]["library_versions"])
    versions["lightgbm"] = "9.9.9"
    with pytest.raises(InferenceBlocked, match="lightgbm"):
        predict_probability(loaded, [0.0] * len(columns), PositionSide.LONG, columns=columns, versions=versions)


def test_inference_blocks_a_recorded_xgboost_version_mismatch(tmp_path) -> None:  # type: ignore[no-untyped-def]
    loaded = _written_meta(tmp_path)
    columns = loaded.metadata["recipe"]["feature_columns"]
    versions = dict(loaded.metadata["reproducibility"]["library_versions"])
    loaded.metadata["reproducibility"]["library_versions"]["xgboost"] = "2.1.0"
    with pytest.raises(InferenceBlocked, match="xgboost"):
        predict_probability(loaded, [0.0] * len(columns), PositionSide.LONG, columns=columns, versions=versions)


def test_inference_blocks_a_scikit_learn_version_mismatch_for_the_calibrator(tmp_path) -> None:  # type: ignore[no-untyped-def]
    loaded = _written_meta(tmp_path)
    columns = loaded.metadata["recipe"]["feature_columns"]
    versions = {**loaded.metadata["reproducibility"]["library_versions"], "scikit-learn": "0.0.1"}
    with pytest.raises(InferenceBlocked, match="scikit-learn"):
        predict_probability(loaded, [0.0] * len(columns), PositionSide.LONG, columns=columns, versions=versions)


def test_inference_blocks_schema_and_preprocessing_mismatch(tmp_path) -> None:  # type: ignore[no-untyped-def]
    loaded = _written_meta(tmp_path)
    columns = loaded.metadata["recipe"]["feature_columns"]
    loaded.metadata["feature_schema_hash"] = "sha256:" + "0" * 64
    with pytest.raises(InferenceBlocked, match="feature_schema_hash"):
        predict_probability(loaded, [0.0] * len(columns), PositionSide.LONG, columns=columns)
    loaded.metadata["feature_schema_hash"] = feature_schema_hash(columns)
    preprocessing = loaded.metadata["preprocessing_version"]
    loaded.metadata["preprocessing_version"] = "9.9.9"
    with pytest.raises(InferenceBlocked, match="preprocessing_version"):
        predict_probability(loaded, [0.0] * len(columns), PositionSide.LONG, columns=columns)
    loaded.metadata["preprocessing_version"] = preprocessing
    del loaded.metadata["reproducibility"]["library_versions"]["lightgbm"]
    with pytest.raises(InferenceBlocked, match="lightgbm"):
        predict_probability(loaded, [0.0] * len(columns), PositionSide.LONG, columns=columns)


def test_inference_checks_the_live_columns_not_the_file_against_itself(tmp_path) -> None:  # type: ignore[no-untyped-def]
    loaded = _written_meta(tmp_path)
    stored = tuple(loaded.metadata["recipe"]["feature_columns"])
    fewer = stored[:-1]
    with pytest.raises(InferenceBlocked, match="feature_schema_hash"):
        predict_probability(loaded, [0.0] * len(fewer), PositionSide.LONG, columns=fewer)
    swapped = (stored[1], stored[0], *stored[2:])
    with pytest.raises(InferenceBlocked):
        predict_probability(loaded, [0.0] * len(swapped), PositionSide.LONG, columns=swapped)


def test_file_probability_matches_the_booster_in_memory(tmp_path) -> None:  # type: ignore[no-untyped-def]
    booster = random_booster(len(feature_columns(())) + 1)
    loaded = _written_meta(tmp_path, booster=booster)
    columns = loaded.metadata["recipe"]["feature_columns"]
    generator = np.random.default_rng(3)
    for _ in range(20):
        values = generator.random(len(columns))
        for side in (PositionSide.LONG, PositionSide.SHORT):
            probability = predict_probability(loaded, list(values), side, columns=columns)
            vector = np.append(values, 1.0 if side is PositionSide.LONG else 0.0).reshape(1, -1)
            expected = Decimal(format(float(booster.predict(vector)[0]), ".12f")).quantize(Decimal("0.0000000001"))
            assert probability == expected


def test_infer_entry_runs_decide_entry_and_respects_mode_status_costs_and_target(tmp_path) -> None:  # type: ignore[no-untyped-def]
    loaded = _written_meta(tmp_path)
    columns = loaded.metadata["recipe"]["feature_columns"]
    values = [0.1] * len(columns)
    target = TargetParams(horizon_bars=24)
    kwargs = {
        "columns": columns,
        "side": PositionSide.LONG,
        "entry": Decimal("100"),
        "atr": Decimal("2"),
        "costs": CostModel(),
        "params": target,
    }
    with pytest.raises(ModelNotTradable, match="load_registered_model"):
        infer_entry(loaded, values, mode="demo", **kwargs)  # type: ignore[arg-type]
    with pytest.raises(ModelNotTradable, match="TRAINED"):
        infer_entry(registered(loaded, "TRAINED"), values, mode="demo", **kwargs)  # type: ignore[arg-type]
    # Статус у файлі не підміняє статус із БД.
    forged = dataclasses.replace(registered(loaded, "CANDIDATE"), metadata={**loaded.metadata, "status": "APPROVED"})
    with pytest.raises(ModelNotTradable):
        infer_entry(forged, values, mode="live", **kwargs)  # type: ignore[arg-type]
    loaded = registered(loaded, "CANDIDATE")
    with pytest.raises(ModelNotTradable, match="live"):
        infer_entry(loaded, values, mode="live", **kwargs)  # type: ignore[arg-type]
    decision = infer_entry(loaded, values, mode="demo", **kwargs)  # type: ignore[arg-type]
    assert loaded.calibrator is not None
    expected = decide_entry(
        raw_probability=predict_probability(loaded, values, PositionSide.LONG, columns=columns),
        side=PositionSide.LONG,
        entry=Decimal("100"),
        atr=Decimal("2"),
        costs=CostModel(),
        params=target,
        calibrator=loaded.calibrator,
        rule=DecisionRule(),
    )
    assert decision == expected
    loaded = registered(loaded, "APPROVED")
    assert infer_entry(loaded, values, mode="live", **kwargs) == expected  # type: ignore[arg-type]
    with pytest.raises(ArtifactError, match="витрати"):
        infer_entry(loaded, values, mode="live", **{**kwargs, "costs": zero_costs()})  # type: ignore[arg-type]
    with pytest.raises(ArtifactError, match="таргет"):
        infer_entry(loaded, values, mode="live", **{**kwargs, "params": TargetParams(horizon_bars=12)})  # type: ignore[arg-type]
    with pytest.raises(ModelNotTradable, match="RETIRED"):
        infer_entry(registered(loaded, "RETIRED"), values, mode="demo", **kwargs)  # type: ignore[arg-type]


def test_tampered_or_broken_files_do_not_load(tmp_path) -> None:  # type: ignore[no-untyped-def]
    loaded = _written_meta(tmp_path)
    directory = loaded.directory
    calibrator = json.loads((directory / CALIBRATOR_FILE).read_text(encoding="utf-8"))
    calibrator["y_thresholds"] = [(0.9).hex()] * len(calibrator["y_thresholds"])
    (directory / CALIBRATOR_FILE).write_text(json.dumps(calibrator), encoding="utf-8")
    with pytest.raises(ArtifactError, match="калібратора"):
        load_model(directory)

    other = _written_meta(tmp_path / "other")
    model_text = (other.directory / MODEL_FILE).read_text(encoding="utf-8")
    (other.directory / MODEL_FILE).write_text(model_text + "\n", encoding="utf-8")
    with pytest.raises(ArtifactError, match="хеш файлу моделі"):
        load_model(other.directory)
    (other.directory / MODEL_FILE).unlink()
    with pytest.raises(ArtifactError, match="немає файлу моделі"):
        load_model(other.directory)

    broken = _written_meta(tmp_path / "broken")
    (broken.directory / METADATA_FILE).write_text("{not json", encoding="utf-8")
    with pytest.raises(ArtifactError, match="не читається"):
        load_model(broken.directory)
    with pytest.raises(ArtifactError, match="не читається"):
        load_model(tmp_path / "missing" / "lgbm-meta-2026-11-03-001")


def test_calibrator_roundtrip_matches_between_knots_and_does_not_refit(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    isotonic = IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip")
    isotonic.fit(np.array([0.1, 0.3, 0.4, 0.6, 0.7, 0.9]), np.array([0, 0, 1, 0, 1, 1]))
    original = ProbabilityCalibrator(method="isotonic", estimator=isotonic)
    platt = _platt()
    payloads = (original.payload(), platt.payload())

    def _no_fit(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("калібратор не має вчитись заново")

    monkeypatch.setattr(IsotonicRegression, "fit", _no_fit)
    monkeypatch.setattr(LogisticRegression, "fit", _no_fit)
    restored = calibrator_from_payload(payloads[0])
    restored_platt = calibrator_from_payload(payloads[1])
    points = [Decimal(index) / Decimal(100) for index in range(0, 101, 3)]
    for value in (*points, Decimal("0.05"), Decimal("0.95"), Decimal("0.35"), Decimal("0.65")):
        assert restored.calibrate(value) == original.calibrate(value)
        assert restored_platt.calibrate(value) == platt.calibrate(value)


def test_git_state_is_dirty_when_git_fails_or_hangs(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    assert read_git_state(tmp_path) == GitState(commit="unknown", dirty=True)

    def _hang(*_args: object, **kwargs: object) -> None:
        assert kwargs.get("timeout") is not None
        raise subprocess.TimeoutExpired(cmd="git", timeout=1)

    monkeypatch.setattr("app.ml.artifacts.subprocess.run", _hang)
    assert read_git_state(tmp_path) == GitState(commit="unknown", dirty=True)


def test_admission_rejects_each_failed_condition_and_accepts_a_passing_window() -> None:
    passing = _assessment()
    assert passing.passed is True
    assert passing.reasons == ()
    assert passing.holdout_entry_ms == tuple(100 * HOUR + index * HOUR for index in range(30))
    short = _assessment(wins=4, losses=6)
    assert REASON_MIN_TRADES in short.reasons
    assert short.passed is False
    weak = _assessment(win_net=Decimal("1"), loss_net=Decimal("-1"), wins=10, losses=20)
    assert REASON_NET_PF in weak.reasons
    drawn = _assessment(drawdown=Decimal("0.5"))
    assert REASON_DRAWDOWN in drawn.reasons
    tied = _assessment(rule_extra=0)
    assert REASON_FILTER in tied.reasons
    rule = _assessment(label_type="rule_only")
    assert rule.passed is True
    assert rule.unfiltered_net is None


def test_admission_checks_the_report_itself() -> None:
    trades = holdout_trades(start=100 * HOUR)
    report = backtest_report(trades)
    with pytest.raises(ArtifactError, match="торгового символу"):
        _assess(report.model_copy(update={"symbol": "ETH/USDT:USDT"}))
    with pytest.raises(ArtifactError, match="стрес"):
        _assess(report.model_copy(update={"cost_factor": Decimal(2)}))
    lying = report.model_copy(update={"metrics": report.metrics.model_copy(update={"total_net_usd": Decimal("999")})})
    with pytest.raises(ArtifactError, match="не збігається з угодами"):
        _assess(lying)
    unfiltered = backtest_report((*trades, *extra_losers(start=100 * HOUR)))
    # Ширший набір не може бути «оціненим» підмножиною фільтра.
    with pytest.raises(ArtifactError, match="вкладені"):
        _assess(unfiltered, label_type="meta", unfiltered=report, scored=report)
    with pytest.raises(ArtifactError, match="обидва звіти"):
        _assess(report, label_type="meta")


def test_short_holdout_is_refused_not_expanded_into_walk_forward() -> None:
    sealed_from = 100 * HOUR
    sealed_to = 140 * HOUR
    short = tuple(trade(sealed_from + index * HOUR, Decimal("2")) for index in range(29))
    decision = _assess(backtest_report(short), holdout_to_ms=sealed_to)
    assert decision.passed is False
    assert REASON_MIN_TRADES in decision.reasons
    assert (decision.window_from_ms, decision.window_to_ms) == (sealed_from, sealed_to)
    # Угода до запечатаного початку — in-sample для моделі допуску, у вікно оцінки її не взяти.
    borrowed = (trade(sealed_from - HOUR, Decimal("2")), *short)
    with pytest.raises(ArtifactError, match="поза запечатаним hold-out"):
        _assess(backtest_report(borrowed), holdout_to_ms=sealed_to, walk_forward=walk_forward_trades()[:-1])
    late = (*walk_forward_trades(), trade(sealed_from, Decimal(1)))
    with pytest.raises(ArtifactError, match="walk-forward заходить у hold-out"):
        _assess(backtest_report(short), holdout_to_ms=sealed_to, walk_forward=late)


def test_frozen_fit_does_not_see_holdout_without_a_grant_of_this_window(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def _forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("рання зупинка під час перенавчання")

    monkeypatch.setattr("app.ml.meta_model._fit_search", _forbidden)
    books, split = learning_books()
    blocked = fit_frozen_meta(
        books,
        spec=BREAKOUT,
        split=split,
        trading_symbol=BTC,
        seed=_SEED,
        costs=zero_costs(),
        params=TargetParams(horizon_bars=4),
    )
    assert all(time < split.holdout_from_ms for time in blocked.row_times_ms)
    assert blocked.include_holdout is False
    # Будь-які ціни hold-out не змінюють модель допуску ні на байт.
    again = fit_frozen_meta(
        (_mutate_holdout(books[0], split),),
        spec=BREAKOUT,
        split=split,
        trading_symbol=BTC,
        seed=_SEED,
        costs=zero_costs(),
        params=TargetParams(horizon_bars=4),
    )
    assert _model_text(again) == _model_text(blocked)
    with pytest.raises(TypeError):
        fit_frozen_meta(
            books,
            spec=BREAKOUT,
            split=split,
            trading_symbol=BTC,
            seed=_SEED,
            include_holdout=True,  # type: ignore[call-arg]
        )
    with pytest.raises(SplitLeakError, match="грант"):
        fit_frozen_meta(
            books,
            spec=BREAKOUT,
            split=split,
            trading_symbol=BTC,
            seed=_SEED,
            costs=zero_costs(),
            params=TargetParams(horizon_bars=4),
            holdout_grant=_grant_holdout(1, split.holdout_from_ms - HOUR, split.holdout_to_ms),
        )
    granted = fit_frozen_meta(
        books,
        spec=BREAKOUT,
        split=split,
        trading_symbol=BTC,
        seed=_SEED,
        costs=zero_costs(),
        params=TargetParams(horizon_bars=4),
        lgbm=blocked.lgbm,
        holdout_grant=_grant_holdout(1, split.holdout_from_ms, split.holdout_to_ms),
        expected_schema_hash=blocked.model.schema_hash,
    )
    assert any(time >= split.holdout_from_ms for time in granted.row_times_ms)
    assert granted.lgbm == blocked.lgbm
    assert granted.n_rows > blocked.n_rows
    assert granted.model.schema_hash == blocked.model.schema_hash
    assert granted.coverage_ms == blocked.coverage_ms


def test_holdout_grant_is_issued_only_by_artifacts() -> None:
    expected = {
        "_grant_holdout": ["ml/artifacts.py", "ml/meta_model.py"],
        "_HOLDOUT_SEAL": ["ml/meta_model.py"],
        "HoldoutGrant(": ["ml/meta_model.py"],
        "_register_retrained": ["ml/artifacts.py"],
        "_insert_model": ["ml/artifacts.py"],
        # Статус для входу ставить лише `load_registered_model`; сиру ймовірність без статусу кличе лише `infer_entry`.
        "db_status=": ["ml/artifacts.py"],
        "predict_probability(": ["ml/artifacts.py"],
    }
    for name, files in expected.items():
        users = sorted(
            str(path.relative_to(_APP)) for path in _APP.rglob("*.py") if name in path.read_text(encoding="utf-8")
        )
        assert users == files, name
    # Підміна поточних версій бібліотек (`versions=`) — лише для тестів: у `app/` її не передає ніхто, крім модуля.
    passing = sorted(
        str(path.relative_to(_APP))
        for path in _APP.rglob("*.py")
        if re.search(r"(?<![a-z_])versions=", path.read_text(encoding="utf-8"))
    )
    assert passing == ["ml/artifacts.py"]


def test_broken_metadata_values_are_artifact_errors(tmp_path) -> None:  # type: ignore[no-untyped-def]
    cases: tuple[tuple[str, object], ...] = (
        ("target_params", {"tp_atr_mult": "abc", "sl_atr_mult": "1", "horizon_bars": 24}),
        ("cost_model", {"taker_fee_rate": "not-a-number"}),
        ("cost_model", ["0.1"]),
        ("recipe", ["lgbm"]),
        ("decision_rule", {"min_edge_atr": "x", "min_confidence": "0.4"}),
    )
    for index, (key, value) in enumerate(cases):
        loaded = _written_meta(tmp_path / f"case-{index}")
        path = loaded.directory / METADATA_FILE
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload[key] = value
        path.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(ArtifactError):
            load_model(loaded.directory)


def test_data_hash_covers_every_input_series_and_ignores_symbol_order() -> None:
    books, _split = learning_books()
    history = books[0]
    base = hash_inputs(books, None)
    assert base == hash_inputs(tuple(reversed(books)), None)
    assert hash_inputs((history.model_copy(update={"mark_candles": history.candles}),), None) != base
    funding = (history.funding[0].model_copy(update={"rate": Decimal("0.0001")}),)
    assert hash_inputs((history.model_copy(update={"funding": funding}),), None) != base
    candles = (history.candles[0].model_copy(update={"close": Decimal("100.5")}), *history.candles[1:])
    assert hash_inputs((history.model_copy(update={"candles": candles}),), None) != base


def test_decision_payload_hash_is_the_calibrator_file(tmp_path) -> None:  # type: ignore[no-untyped-def]
    loaded = _written_meta(tmp_path)
    payload = decision_payload(DecisionRule(), isotonic_calibrator())
    assert payload["decision_rule"] == loaded.metadata["decision_rule"]
    assert payload["calibrator_sha256"] == loaded.metadata["reproducibility"]["calibrator_sha256"]
    other = decision_payload(DecisionRule(min_edge_atr=Decimal("0.2")), isotonic_calibrator())
    assert other["decision_rule"] != payload["decision_rule"]


def test_data_hash_ignores_order(tmp_path) -> None:  # type: ignore[no-untyped-def]
    left = (BTC, 0, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"))
    right = (BTC, HOUR, Decimal("100"), Decimal("102"), Decimal("99"), Decimal("101"))
    assert hash_candles((left, right)) == hash_candles((right, left))
    version = allocate_version(tmp_path, "meta", at_ms=1_762_000_000_000)
    assert version.startswith("lgbm-meta-")
    assert version.endswith("-001")


def test_passing_pf_meets_the_published_threshold() -> None:
    decision = _assessment()
    assert decision.holdout_net_pf is not None
    assert decision.holdout_net_pf >= HOLDOUT_MIN_NET_PF


def _metadata(*, label_type: str = "meta", version: str = "lgbm-meta-2026-11-03-001") -> dict:  # type: ignore[type-arg]
    columns = feature_columns(())
    recipe = None if label_type == "rule_only" else lgbm_journal_params()
    method = None if label_type == "rule_only" else "isotonic"
    return build_metadata(
        model_version=version,
        label_type=label_type,
        spec=EMA_CROSS,
        trade_symbol=BTC,
        train_symbols=(BTC,),
        feature_columns=columns,
        target=TargetParams(horizon_bars=24),
        costs=CostModel(),
        rule=DecisionRule(),
        experiment_id="exp-0001",
        seed=1,
        data_hash="sha256:" + "ab" * 32,
        git=GitState(commit="abc123", dirty=False),
        library_versions={
            "python": "3.11.0",
            "lightgbm": "4.6.0",
            "numpy": "2.0.0",
            "pandas": "2.2.0",
            "scikit-learn": "1.5.0",
        },
        train_range_ms=(0, 1_000),
        recipe_lgbm=recipe,
        coverage_ms=(0, 800),
        train_rows=10,
        include_holdout=False,
        calibrator_method=method,
    )


def _written_meta(root, *, booster: lgb.Booster | None = None):  # type: ignore[no-untyped-def]
    meta = _metadata()
    columns = meta["recipe"]["feature_columns"]
    meta["reproducibility"]["library_versions"] = {
        "python": "3.11.0",
        "lightgbm": lgb.__version__,
        "numpy": np.__version__,
        "pandas": "0",
        "scikit-learn": sklearn.__version__,
    }
    directory = write_artifact(
        root / meta["model_version"],
        meta,
        booster=booster if booster is not None else random_booster(len(columns) + 1),
        calibrator=isotonic_calibrator(),
    )
    loaded = load_model(directory)
    assert loaded.metadata["feature_schema_hash"] == feature_schema_hash(columns)
    return loaded


def registered(loaded, status: str):  # type: ignore[no-untyped-def]
    """Модель, як її повертає `load_registered_model`: статус із БД і та сама копія в метаданих."""
    return dataclasses.replace(loaded, db_status=status, metadata={**loaded.metadata, "status": status})


def isotonic_calibrator() -> ProbabilityCalibrator:
    return ProbabilityCalibrator(
        method="isotonic",
        estimator=IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip").fit(
            np.array([0.2, 0.8]), np.array([0, 1])
        ),
    )


def random_booster(width: int) -> lgb.Booster:
    """Бустер на випадковій матриці: файл моделі, який жоден рецепт не відтворить."""
    generator = np.random.default_rng(1)
    matrix = generator.random((8, width))
    labels = np.array([0, 1, 0, 1, 0, 1, 0, 1])
    model = lgb.LGBMClassifier(
        n_estimators=2,
        num_leaves=2,
        min_child_samples=1,
        deterministic=True,
        n_jobs=1,
        verbosity=-1,
        random_state=1,
    )
    model.fit(matrix, labels)
    booster = model.booster_
    assert isinstance(booster, lgb.Booster)
    return booster


def _platt() -> ProbabilityCalibrator:
    estimator = LogisticRegression(C=1.0, solver="lbfgs", max_iter=2000, class_weight=None, random_state=0)
    estimator.fit(np.array([[-2.0], [0.0], [2.0], [3.0]]), np.array([0, 0, 1, 1]))
    return ProbabilityCalibrator(method="platt", estimator=estimator)


def _assessment(
    *,
    label_type: str = "meta",
    wins: int = 20,
    losses: int = 10,
    win_net: Decimal = Decimal("2"),
    loss_net: Decimal = Decimal("-1"),
    drawdown: Decimal = Decimal("0.04"),
    rule_extra: int = 5,
) -> AdmissionDecision:
    start = 100 * HOUR
    trades = tuple(trade(start + index * HOUR, win_net if index < wins else loss_net) for index in range(wins + losses))
    report = backtest_report(trades, drawdown=drawdown)
    unfiltered = backtest_report((*trades, *extra_losers(start=start, count=rule_extra)))
    meta = label_type == "meta"
    return _assess(
        report,
        label_type=label_type,
        holdout_to_ms=start + 100 * HOUR,
        unfiltered=unfiltered if meta else None,
        scored=unfiltered if meta else None,
    )


def _assess(  # type: ignore[no-untyped-def]
    report: BacktestReport,
    *,
    label_type: str = "rule_only",
    holdout_to_ms: int = 200 * HOUR,
    walk_forward=None,
    unfiltered: BacktestReport | None = None,
    scored: BacktestReport | None = None,
) -> AdmissionDecision:
    return assess_holdout(
        label_type=label_type,
        trade_symbol=BTC,
        report=report,
        walk_forward=walk_forward_trades() if walk_forward is None else walk_forward,
        holdout_from_ms=100 * HOUR,
        holdout_to_ms=holdout_to_ms,
        profitable_fold_share=Decimal("0.80"),
        max_drawdown_limit_pct=HOLDOUT_MAX_DRAWDOWN_PCT,
        seed=1,
        n_replicates=20,
        unfiltered=unfiltered,
        scored_unfiltered=scored,
    )


def walk_forward_trades() -> tuple[BacktestTrade, ...]:
    """80 угод walk-forward з R = 1 на початку історії."""
    return tuple(trade(index * HOUR, Decimal("1")) for index in range(80))


def holdout_trades(*, start: int, loss: Decimal = Decimal("-1")) -> tuple[BacktestTrade, ...]:
    """30 угод hold-out: 20 по +2, 10 по `loss`."""
    return tuple(trade(start + index * HOUR, Decimal("2") if index < 20 else loss) for index in range(30))


def extra_losers(*, start: int, count: int = 5) -> tuple[BacktestTrade, ...]:
    """Сигнали, які фільтр відсіяв, а правило без фільтра торгувало (кожен −1)."""
    return tuple(trade(start + (40 + index) * HOUR, Decimal("-1")) for index in range(count))


def trade(entry_ms: int, net: Decimal) -> BacktestTrade:
    return BacktestTrade(
        side=PositionSide.LONG,
        entry_index=0,
        exit_index=1,
        entry_open_time_ms=entry_ms,
        exit_open_time_ms=entry_ms + HOUR,
        exit_reason=ExitReason.TAKE_PROFIT if net > 0 else ExitReason.STOP_LOSS,
        stop_price_source=StopPriceSource.MARK,
        qty=Decimal("1"),
        net_usd=net,
        loss_per_unit=Decimal("1"),
        r_multiple=net,
        risk_usd=Decimal("1"),
        funding_penalty_usd=Decimal("0"),
        funding_gap=False,
    )


def backtest_report(
    trades: tuple[BacktestTrade, ...],
    *,
    drawdown: Decimal = Decimal("0.04"),
    seed: int = 1,
) -> BacktestReport:
    """Звіт, узгоджений з угодами так, як його перевіряє журнал (`_report_matches_trades`)."""
    profit = sum((item.net_usd for item in trades if item.net_usd > 0), Decimal(0))
    loss = sum((-item.net_usd for item in trades if item.net_usd < 0), Decimal(0))
    wins = sum(1 for item in trades if item.net_usd > 0)
    metrics = BacktestMetrics(
        n_trades=len(trades),
        total_r=sum((item.r_multiple for item in trades), Decimal(0)),
        total_net_usd=sum((item.net_usd for item in trades), Decimal(0)),
        profit_factor=None if loss == 0 else profit / loss,
        max_drawdown_pct=drawdown,
        hit_rate=None if not trades else Decimal(wins) / Decimal(len(trades)),
        sharpe=None,
        max_consecutive_losses=0,
        n_incomplete=0,
        n_unusable=0,
        n_funding_gaps=0,
        ending_equity=Decimal("100"),
        peak_equity=Decimal("100"),
        halted=False,
    )
    decisions = tuple(
        BacktestDecision(
            ts_ms=item.entry_open_time_ms,
            entry_index=0,
            stage="RISK",
            decision="ENTER",
            reason_code=None,
            side=item.side,
        )
        for item in trades
    )
    return BacktestReport(
        symbol=BTC, seed=seed, cost_factor=Decimal(1), trades=trades, decisions=decisions, metrics=metrics
    )


def learning_books() -> tuple[tuple[SymbolHistory, ...], ResearchSplit]:
    """Синтетична історія 4000 год, на якій пробій дає обидва класи і в walk-forward, і в hold-out."""
    count = 4000
    candles = [_bar(index, "100", "101", "99", "100") for index in range(count)]
    for step, entry in enumerate(range(1400, 3100, 40)):
        decision = entry - 1
        if step % 2 == 0:
            candles[decision] = _bar(decision, "100", "102", "99.5", "101.5")
            candles[entry] = _bar(entry, "100", "110", "99.5", "100")
        else:
            candles[decision] = _bar(decision, "100", "130", "99", "128")
            candles[entry] = _bar(entry, "100", "128.5", "90", "100")
    holdout_entry = 3500
    candles[holdout_entry - 1] = _bar(holdout_entry - 1, "100", "102", "99.5", "101.5")
    candles[holdout_entry] = _bar(holdout_entry, "100", "110", "99.5", "100")
    history = SymbolHistory(
        symbol=BTC,
        candles=tuple(candles),
        instrument=instrument(symbol=BTC),
        funding=(FundingEvent(time_ms=0, rate=Decimal(0), mark_price=Decimal(100)),),
    )
    split = build_split(
        data_from_ms=0,
        data_to_ms=count * HOUR,
        horizon_bars=4,
        count_trades=lambda _start, _end: 20,
        min_train_ms=1500 * HOUR,
        test_segment_ms=200 * HOUR,
        test_segment_extended_ms=400 * HOUR,
        min_trades_per_fold=1,
    )
    return (history,), split


def _mutate_holdout(history: SymbolHistory, split: ResearchSplit) -> SymbolHistory:
    changed = tuple(
        bar
        if bar.open_time_ms < split.holdout_from_ms
        else _bar(bar.open_time_ms // HOUR, "200", "260", "150", "250" if bar.open_time_ms % (3 * HOUR) else "180")
        for bar in history.candles
    )
    return history.model_copy(update={"candles": changed})


def _model_text(fit) -> str:  # type: ignore[no-untyped-def]
    return str(booster_of(fit.model).model_to_string())


def _bar(index: int, open_: str, high: str, low: str, close: str) -> SimCandle:
    return SimCandle(
        open_time_ms=index * HOUR,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
    )


def zero_costs() -> CostModel:
    zero = Decimal(0)
    return CostModel(
        taker_fee_rate=Decimal("0.00055"),
        maker_fee_rate=Decimal("0.00055"),
        slippage_entry_atr=zero,
        slippage_entry_bps=zero,
        slippage_sl_atr=zero,
        slippage_sl_bps=zero,
        slippage_tp_atr=zero,
        slippage_tp_bps=zero,
    )
