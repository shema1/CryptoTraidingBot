"""Чисті ліміти (ТЗ §8.2, §8.5, §15, T2.2)."""

from __future__ import annotations

import inspect
from decimal import Context, Decimal, localcontext

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from app.exchange.types import PositionSide
from app.risk import limits as limits_module
from app.risk.limits import (
    RiskLimits,
    adjust_equity_anchors,
    apply_cashflows,
    check_cooldown,
    check_daily_loss,
    check_drawdown,
    check_entry_limits,
    check_entry_slippage,
    check_funding_rate,
    check_max_positions,
    check_max_trades,
    check_min_equity,
    check_time_stop,
    next_consecutive_losses,
    next_peak_equity,
)
from app.risk.reasons import RiskReason
from tests.risk.conftest import limits

_D = Decimal


def test_defaults_match_the_spec() -> None:
    cfg = limits()
    assert cfg.risk_per_trade_pct == _D("0.005")
    assert cfg.max_leverage == _D(3)
    assert cfg.max_open_positions == 3
    assert cfg.max_total_exposure_pct == _D("1.5")
    assert cfg.daily_loss_limit_pct == _D("0.02")
    assert cfg.max_drawdown_pct == _D("0.10")
    assert cfg.max_consecutive_losses == 4
    assert cfg.cooldown_bars == 8
    assert cfg.max_trades_per_day == 10
    assert cfg.min_liq_distance_sl_multiple == _D(2)
    assert cfg.max_slippage_bps == _D(15)
    assert cfg.max_funding_rate_abs is None


def test_risk_per_trade_cannot_exceed_two_percent() -> None:
    with pytest.raises(ValueError, match="risk_per_trade_pct"):
        limits(risk_per_trade_pct=_D("0.021"))


def test_min_equity_blocks_only_below_the_floor() -> None:
    assert check_min_equity(_D(100), _D(100)).ok
    assert check_min_equity(_D("99.99"), _D(100)).reason is RiskReason.RISK_MIN_EQUITY


def test_drawdown_blocks_at_the_limit_and_uses_the_given_equity() -> None:
    limit = _D("0.10")
    assert check_drawdown(equity=_D(900), peak_equity=_D(1000), limit_pct=limit).reason is RiskReason.RISK_MAX_DRAWDOWN
    assert check_drawdown(equity=_D("900.01"), peak_equity=_D(1000), limit_pct=limit).ok
    assert check_drawdown(equity=_D(1000), peak_equity=_D(1000), limit_pct=limit).ok


def test_peak_must_follow_every_observed_equity_or_a_fall_after_a_rise_is_missed() -> None:
    limit = _D("0.10")
    peak = _D(1000)
    # Зростання до 1500 без запису піку: падіння до 1100 від старого піку виглядає як прибуток.
    assert check_drawdown(equity=_D(1100), peak_equity=peak, limit_pct=limit).ok
    for observed in (_D(1500), _D(1100)):
        peak = next_peak_equity(peak, observed)
    assert peak == _D(1500)
    # Справжня просадка 26,7% від 1500.
    assert check_drawdown(equity=_D(1100), peak_equity=peak, limit_pct=limit).reason is RiskReason.RISK_MAX_DRAWDOWN
    assert next_peak_equity(_D(1500), _D(1500)) == _D(1500)
    with pytest.raises(ValueError, match="equity"):
        next_peak_equity(_D(1000), _D(-1))


def test_drawdown_and_daily_loss_are_reported_before_the_equity_floor() -> None:
    # Капітал нижче `min_equity_usd` і водночас просадка 15%: причина — просадка (з неї випливає HALTED).
    floor = limits(min_equity_usd=_D(900))
    drawdown = check_entry_limits(
        total_equity=_D(850),
        peak_equity=_D(1000),
        day_start_equity=_D(850),
        bot_open_positions=0,
        open_positions=0,
        trades_today=0,
        consecutive_losses=0,
        bars_since_last_loss=0,
        limits=floor,
    )
    assert drawdown.reason is RiskReason.RISK_MAX_DRAWDOWN
    daily = check_entry_limits(
        total_equity=_D(850),
        peak_equity=_D(850),
        day_start_equity=_D(1000),
        bot_open_positions=0,
        open_positions=0,
        trades_today=0,
        consecutive_losses=0,
        bars_since_last_loss=0,
        limits=floor,
    )
    assert daily.reason is RiskReason.RISK_DAILY_LOSS


def test_liquidation_multiple_must_be_above_one() -> None:
    # Оцінка ліквідації не покриває комісію закриття: множник 1 дозволив би ліквідацію в межах стопа.
    with pytest.raises(ValueError, match="min_liq_distance_sl_multiple"):
        limits(min_liq_distance_sl_multiple=_D(1))
    assert limits(min_liq_distance_sl_multiple=_D("1.5")).min_liq_distance_sl_multiple == _D("1.5")


def test_new_equity_high_between_snapshots_is_zero_drawdown_not_an_error() -> None:
    # Total equity з біржі вище за пік останнього знімка: нова вершина, просадки немає.
    assert check_drawdown(equity=_D(1001), peak_equity=_D(1000), limit_pct=_D("0.10")).ok
    decision = check_entry_limits(
        total_equity=_D(1001),
        peak_equity=_D(1000),
        day_start_equity=_D(1000),
        bot_open_positions=0,
        open_positions=0,
        trades_today=0,
        consecutive_losses=0,
        bars_since_last_loss=0,
        limits=limits(),
    )
    assert decision.ok


def test_daily_loss_blocks_at_the_limit() -> None:
    limit = _D("0.02")
    blocked = check_daily_loss(equity=_D(980), day_start_equity=_D(1000), limit_pct=limit)
    assert blocked.reason is RiskReason.RISK_DAILY_LOSS
    assert check_daily_loss(equity=_D("980.01"), day_start_equity=_D(1000), limit_pct=limit).ok
    assert check_daily_loss(equity=_D(1100), day_start_equity=_D(1000), limit_pct=limit).ok
    with pytest.raises(ValueError, match="day_start"):
        check_daily_loss(equity=_D(100), day_start_equity=_D(0), limit_pct=limit)


def test_one_position_and_the_account_cap_share_one_function() -> None:
    assert check_max_positions(0, 1).ok
    assert check_max_positions(1, 1).reason is RiskReason.RISK_MAX_POSITIONS
    assert check_max_positions(2, 3).ok
    assert check_max_positions(3, 3).reason is RiskReason.RISK_MAX_POSITIONS


def _entry_limits(*, bot_open_positions: int, open_positions: int) -> RiskReason | None:
    return check_entry_limits(
        total_equity=_D(1000),
        peak_equity=_D(1000),
        day_start_equity=_D(1000),
        bot_open_positions=bot_open_positions,
        open_positions=open_positions,
        trades_today=0,
        consecutive_losses=0,
        bars_since_last_loss=0,
        limits=limits(),
    ).reason


def test_entry_limits_hold_one_position_per_bot_and_the_account_cap() -> None:
    assert _entry_limits(bot_open_positions=0, open_positions=2) is None
    # Бот уже має позицію, хоча стеля акаунта (3) ще не досягнута.
    assert _entry_limits(bot_open_positions=1, open_positions=1) is RiskReason.RISK_MAX_POSITIONS
    assert _entry_limits(bot_open_positions=0, open_positions=3) is RiskReason.RISK_MAX_POSITIONS
    with pytest.raises(ValueError, match="bot_open_positions"):
        _entry_limits(bot_open_positions=2, open_positions=1)


def test_trade_count_blocks_at_the_cap() -> None:
    assert check_max_trades(9, 10).ok
    assert check_max_trades(10, 10).reason is RiskReason.RISK_MAX_TRADES_PER_DAY


def test_cooldown_boundaries() -> None:
    assert check_cooldown(consecutive_losses=3, bars_since_last_loss=0, max_consecutive_losses=4, cooldown_bars=8).ok
    assert (
        check_cooldown(consecutive_losses=4, bars_since_last_loss=0, max_consecutive_losses=4, cooldown_bars=8).reason
        is RiskReason.RISK_COOLDOWN
    )
    assert (
        check_cooldown(consecutive_losses=4, bars_since_last_loss=7, max_consecutive_losses=4, cooldown_bars=8).reason
        is RiskReason.RISK_COOLDOWN
    )
    assert check_cooldown(consecutive_losses=4, bars_since_last_loss=8, max_consecutive_losses=4, cooldown_bars=8).ok


def test_pause_does_not_reset_the_streak_so_the_next_loss_pauses_again() -> None:
    streak = 4
    assert check_cooldown(
        consecutive_losses=streak, bars_since_last_loss=8, max_consecutive_losses=4, cooldown_bars=8
    ).ok
    streak = next_consecutive_losses(streak, _D("-1"))
    assert streak == 5
    assert (
        check_cooldown(
            consecutive_losses=streak, bars_since_last_loss=0, max_consecutive_losses=4, cooldown_bars=8
        ).reason
        is RiskReason.RISK_COOLDOWN
    )
    assert check_cooldown(
        consecutive_losses=streak, bars_since_last_loss=8, max_consecutive_losses=4, cooldown_bars=8
    ).ok
    assert next_consecutive_losses(streak, _D(1)) == 0


def test_flat_trade_does_not_change_the_streak() -> None:
    assert next_consecutive_losses(2, _D("-0.01")) == 3
    assert next_consecutive_losses(2, _D(0)) == 2
    assert next_consecutive_losses(2, _D("0.01")) == 0


def test_time_stop_closes_on_the_horizon_bar_and_not_before() -> None:
    bar = 3_600_000
    horizon = 8
    opened = 1_000_000
    boundary = opened + horizon * bar
    assert check_time_stop(opened_at_ms=opened, now_ms=boundary - 1, max_hold_bars=horizon, bar_ms=bar).close is False
    due = check_time_stop(opened_at_ms=opened, now_ms=boundary, max_hold_bars=horizon, bar_ms=bar)
    assert due.close
    assert due.reason is RiskReason.TIME_STOP
    with pytest.raises(ValueError, match="раніше"):
        check_time_stop(opened_at_ms=opened, now_ms=opened - 1, max_hold_bars=horizon, bar_ms=bar)


def test_slippage_cap_allows_the_exact_limit() -> None:
    reference = _D(10000)
    assert check_entry_slippage(reference=reference, actual=_D(10015), max_slippage_bps=_D(15)).ok
    blocked = check_entry_slippage(reference=reference, actual=_D(10016), max_slippage_bps=_D(15))
    assert blocked.reason is RiskReason.RISK_MAX_SLIPPAGE


def test_funding_blocks_only_the_side_that_pays() -> None:
    cap = _D("0.0005")
    assert check_funding_rate(rate=_D("0.0005"), side=PositionSide.LONG, max_abs=cap).ok
    long_pays = check_funding_rate(rate=_D("0.00051"), side=PositionSide.LONG, max_abs=cap)
    assert long_pays.reason is RiskReason.RISK_FUNDING
    assert check_funding_rate(rate=_D("-0.01"), side=PositionSide.LONG, max_abs=cap).ok
    short_pays = check_funding_rate(rate=_D("-0.00051"), side=PositionSide.SHORT, max_abs=cap)
    assert short_pays.reason is RiskReason.RISK_FUNDING
    assert check_funding_rate(rate=_D("0.01"), side=PositionSide.SHORT, max_abs=cap).ok
    assert check_funding_rate(rate=_D("0.01"), side=PositionSide.LONG, max_abs=None).ok


def test_entry_checks_stop_at_the_first_failure_and_drawdown_wins() -> None:
    cfg = limits()
    decision = check_entry_limits(
        total_equity=_D(800),
        peak_equity=_D(1000),
        day_start_equity=_D(1000),
        bot_open_positions=1,
        open_positions=3,
        trades_today=10,
        consecutive_losses=4,
        bars_since_last_loss=0,
        limits=cfg,
    )
    assert decision.reason is RiskReason.RISK_MAX_DRAWDOWN


def test_unset_funding_limit_is_not_a_block_and_a_set_one_fails_closed_without_a_rate() -> None:
    cfg = limits()
    assert check_entry_limits(
        total_equity=_D(1000),
        peak_equity=_D(1000),
        day_start_equity=_D(1000),
        bot_open_positions=0,
        open_positions=0,
        trades_today=0,
        consecutive_losses=0,
        bars_since_last_loss=0,
        limits=cfg,
    ).ok
    strict = limits(max_funding_rate_abs=_D("0.001"))
    missing = check_entry_limits(
        total_equity=_D(1000),
        peak_equity=_D(1000),
        day_start_equity=_D(1000),
        bot_open_positions=0,
        open_positions=0,
        trades_today=0,
        consecutive_losses=0,
        bars_since_last_loss=0,
        limits=strict,
    )
    assert missing.reason is RiskReason.RISK_FUNDING


def test_deposit_does_not_erase_drawdown_or_a_losing_day() -> None:
    # До депозиту 10: капітал 90, пік 100 (просадка 10%), початок дня 100 (денний збиток 10%).
    losing = adjust_equity_anchors(equity_after=_D(100), cashflow=_D(10), peak_equity=_D(100), day_start_equity=_D(100))
    assert losing.equity == _D(100)
    assert not check_drawdown(equity=losing.equity, peak_equity=losing.peak_equity, limit_pct=_D("0.10")).ok
    # Без корекції пік лишився б 100 і просадка стала б нульовою.
    assert check_drawdown(equity=_D(100), peak_equity=_D(100), limit_pct=_D("0.10")).ok
    assert not check_daily_loss(equity=losing.equity, day_start_equity=losing.day_start_equity, limit_pct=_D("0.02")).ok


def test_withdrawal_does_not_inflate_drawdown() -> None:
    anchors = adjust_equity_anchors(
        equity_after=_D(80), cashflow=_D(-10), peak_equity=_D(100), day_start_equity=_D(100)
    )
    before = (_D(100) - _D(90)) / _D(100)
    after = (anchors.peak_equity - anchors.equity) / anchors.peak_equity
    assert after >= before
    assert after < _D("0.12")
    # Без корекції просадка була б (100-80)/100 = 20%.
    assert (anchors.peak_equity - anchors.equity) / anchors.peak_equity < _D("0.20")


def test_two_deposits_match_one_when_nothing_is_traded_between_them() -> None:
    start = {"peak_equity": _D(100), "day_start_equity": _D(100), "equity": _D(100)}
    stepped = apply_cashflows(**start, cashflows=(_D(10), _D(10)))
    once = apply_cashflows(**start, cashflows=(_D(20),))
    assert stepped == once
    assert stepped.equity == _D(120)
    assert stepped.peak_equity == _D(120)


def test_cashflow_right_after_a_new_high_starts_from_that_high() -> None:
    # Збережений пік 100, капітал до депозиту вже 110 (нова вершина), депозит 10.
    anchors = adjust_equity_anchors(
        equity_after=_D(120), cashflow=_D(10), peak_equity=_D(100), day_start_equity=_D(100)
    )
    assert anchors.peak_equity == _D(120)
    assert anchors.equity == _D(120)
    stepped = apply_cashflows(peak_equity=_D(100), day_start_equity=_D(100), equity=_D(110), cashflows=(_D(10),))
    assert stepped == anchors
    unchanged = apply_cashflows(peak_equity=_D(100), day_start_equity=_D(100), equity=_D(110), cashflows=())
    assert unchanged.peak_equity == _D(110)


def test_scaled_peak_rounds_up_so_the_drawdown_is_never_understated() -> None:
    # 110 / 90 не скінченний дріб: пік 100 → 122.22…, округлення вниз занизило б просадку.
    anchors = adjust_equity_anchors(
        equity_after=_D(110), cashflow=_D(20), peak_equity=_D(100), day_start_equity=_D(100)
    )
    with localcontext(Context(prec=200)):
        assert anchors.peak_equity * _D(90) >= _D(100) * _D(110)
        assert anchors.day_start_equity * _D(90) >= _D(100) * _D(110)


def test_limits_survive_a_json_round_trip_with_decimals_as_strings() -> None:
    # `risk_config.params` (JSONB): Decimal пишеться рядком, читається через JSON, а не з dict (strict).
    cfg = limits(max_funding_rate_abs=_D("0.0005"))
    dumped = cfg.model_dump_json()
    assert '"risk_per_trade_pct":"0.005"' in dumped
    assert RiskLimits.model_validate_json(dumped) == cfg


def test_limits_do_not_touch_the_database_or_the_clock() -> None:
    source = inspect.getsource(limits_module)
    assert "sqlalchemy" not in source
    assert "now_ms(" not in source
    assert "socket" not in source


@settings(max_examples=40, deadline=None)
@given(
    equity_units=st.integers(min_value=100, max_value=10_000),
    # Від'ємне — збережений пік нижче капіталу до потоку (нова вершина між знімками).
    drop_units=st.integers(min_value=-80, max_value=80),
    flow_units=st.integers(min_value=-40, max_value=40),
)
def test_anchors_do_not_understate_a_drawdown(equity_units: int, drop_units: int, flow_units: int) -> None:
    equity_before = _D(equity_units)
    peak = equity_before + _D(drop_units)
    flow = _D(flow_units)
    equity_after = equity_before + flow
    if equity_after <= 0 or equity_before <= 0:
        return
    anchors = adjust_equity_anchors(
        equity_after=equity_after,
        cashflow=flow,
        peak_equity=peak,
        day_start_equity=equity_before,
    )
    if peak == 0:
        return
    effective = max(peak, equity_before)
    before = (effective - equity_before) / effective
    after = (anchors.peak_equity - anchors.equity) / anchors.peak_equity
    assert after + _D("1e-18") >= before
    assert anchors.peak_equity >= anchors.equity
    # Точно, без округлення тесту: equity_after / peak_after ≤ equity_before / peak.
    with localcontext(Context(prec=200)):
        assert anchors.equity * effective <= equity_before * anchors.peak_equity


def test_risk_per_trade_field_is_required_to_build_without_the_two_configured_amounts() -> None:
    with pytest.raises(ValueError, match="min_equity_usd"):
        RiskLimits()  # type: ignore[call-arg]
