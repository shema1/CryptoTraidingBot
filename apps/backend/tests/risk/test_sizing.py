"""Розмір позиції (ТЗ §8.3, T2.2)."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, ROUND_UP, Context, Decimal, Inexact, Underflow, localcontext

import pytest
from hypothesis import event, given, settings
from hypothesis import strategies as st

from app.exchange.types import Instrument, PositionSide
from app.risk.costs import CostModel
from app.risk.limits import RiskLimits
from app.risk.numbers import div_down
from app.risk.reasons import RiskReason
from app.risk.sizing import PositionSize, calc_position_size, isolated_liq_price, sizing_equity
from tests.risk.conftest import instrument, limits

_D = Decimal
_LONG = PositionSide.LONG
_SHORT = PositionSide.SHORT
_MMR = _D("0.005")
_ENTRY = _D(100)
_STOP = _D(99)
_TARGET = _D(102)
_ATR = _D(2)
_WALLET = _D(1000)
_ZERO = _D(0)


def _size(
    *,
    side: PositionSide = _LONG,
    entry: Decimal = _ENTRY,
    stop: Decimal = _STOP,
    target: Decimal | None = _TARGET,
    atr: Decimal = _ATR,
    wallet: Decimal = _WALLET,
    equity: Decimal | None = None,
    notional: Decimal = _ZERO,
    costs: CostModel | None = None,
    cfg: RiskLimits | None = None,
    inst: Instrument | None = None,
    mmr: Decimal = _MMR,
    deduction: Decimal = _ZERO,
) -> PositionSize:
    return calc_position_size(
        side=side,
        entry_price=entry,
        stop_price=stop,
        take_profit_price=target,
        atr=atr,
        wallet_balance=wallet,
        total_equity=wallet if equity is None else equity,
        current_notional=notional,
        instrument=instrument() if inst is None else inst,
        limits=limits() if cfg is None else cfg,
        costs=CostModel() if costs is None else costs,
        maintenance_margin_rate=mmr,
        mm_deduction=deduction,
    )


def test_bybit_liquidation_example_rounds_to_the_published_price() -> None:
    price = isolated_liq_price(
        side=_LONG,
        entry=_D(40000),
        qty=_D(1),
        leverage=_D(50),
        maintenance_margin_rate=_D("0.005"),
        mm_deduction=_D(0),
        taker_fee_rate=_D("0.00055"),
        extra_margin=_D(3000),
    )
    assert price.quantize(_D("0.01"), rounding=ROUND_HALF_UP) == _D("36380.25")


@pytest.mark.parametrize("side", [_LONG, _SHORT])
def test_loss_at_the_liquidation_price_equals_margin_minus_maintenance(side: PositionSide) -> None:
    # Означення ізольованої ліквідації: збиток на ціні ліквідації = маржа позиції + extra − MM(ціна ліквідації).
    # Однаково для обох боків; ловить помилку знака в гілці шорта, якої приклад Bybit (лонг) не покриває.
    entry, qty, leverage, mmr, deduction, taker, extra = (
        _D(40000),
        _D("1.5"),
        _D(20),
        _D("0.01"),
        _D(250),
        _D("0.00055"),
        _D(1200),
    )
    liq = isolated_liq_price(
        side=side,
        entry=entry,
        qty=qty,
        leverage=leverage,
        maintenance_margin_rate=mmr,
        mm_deduction=deduction,
        taker_fee_rate=taker,
        extra_margin=extra,
    )
    with localcontext(Context(prec=50)):
        long = side is _LONG
        loss = (entry - liq) * qty if long else (liq - entry) * qty
        extra_term = extra / (1 - taker) if long else extra / (1 + taker)
        maintenance = liq * qty * mmr - deduction
        assert abs(loss - (entry * qty / leverage + extra_term - maintenance)) < _D("1e-40")
    assert (liq < entry) if side is _LONG else (liq > entry)


def test_zero_deduction_and_extra_margin_do_not_depend_on_qty() -> None:
    common = {
        "side": _LONG,
        "entry": _D(40000),
        "leverage": _D(50),
        "maintenance_margin_rate": _D("0.005"),
        "taker_fee_rate": _D("0.00055"),
    }
    assert isolated_liq_price(qty=_D(1), **common) == isolated_liq_price(qty=_D(5), **common)  # type: ignore[arg-type]


def test_hand_sized_long_and_short_stay_inside_the_risk_budget() -> None:
    long = _size()
    assert long.qty == _D("3.547")
    assert long.entry_fill == _D("100.1")
    assert long.stop_fill == _D("98.8")
    assert long.loss_per_unit == _D("1.409395")
    assert long.leverage == _D(3)
    assert long.qty * long.loss_per_unit <= long.risk_usd
    assert long.reason is None
    short = _size(side=_SHORT, stop=_D(101), target=_D(98))
    assert short.qty == _D("3.544")
    assert short.entry_fill == _D("99.9")
    assert short.stop_fill == _D("101.2")
    assert short.loss_per_unit == _D("1.410605")
    assert short.qty * short.loss_per_unit <= _D(5)


def test_unrealized_profit_does_not_increase_size_and_a_loss_does_decrease_it() -> None:
    base = _size()
    richer = _size(equity=_D(1500))
    poorer = _size(equity=_D(800))
    assert richer.qty == base.qty
    assert richer.sizing_equity == _D(1000)
    assert poorer.qty == _D("2.838")
    assert poorer.qty < base.qty
    assert sizing_equity(wallet_balance=_D(1000), total_equity=_D(1500)) == _D(1000)
    assert sizing_equity(wallet_balance=_D(1000), total_equity=_D(800)) == _D(800)


def test_leverage_is_the_lower_of_config_and_the_instrument() -> None:
    sized = _size(inst=instrument(max_leverage=_D(2)))
    assert sized.leverage == _D(2)


def test_wide_stop_lowers_leverage_until_liquidation_is_two_stops_away() -> None:
    flat = CostModel(
        slippage_entry_bps=_D(0),
        slippage_entry_atr=_D(0),
        slippage_sl_bps=_D(0),
        slippage_sl_atr=_D(0),
        slippage_tp_bps=_D(0),
        slippage_tp_atr=_D(0),
    )
    long = _size(
        entry=_D(100),
        stop=_D(60),
        target=_D(140),
        atr=_D(0),
        wallet=_D(10000),
        costs=flat,
        cfg=limits(min_equity_usd=_D(100)),
    )
    short = _size(
        side=_SHORT,
        entry=_D(100),
        stop=_D(140),
        target=_D(60),
        atr=_D(0),
        wallet=_D(10000),
        costs=flat,
        cfg=limits(min_equity_usd=_D(100)),
    )
    assert long.leverage == _D("1.24")
    assert long.qty == _D("1.247")
    assert short.leverage == _D("1.23")
    assert long.reason is None
    assert short.reason is None


def test_stop_wider_than_even_one_x_can_protect_is_rejected() -> None:
    flat = CostModel(slippage_entry_atr=_D(0), slippage_sl_atr=_D(0), slippage_tp_atr=_D(0))
    # Відстань до стопа 60, потрібно 120, а при 1× ліквідація не далі за саму ціну (~100).
    sized = _size(entry=_D(100), stop=_D(40), target=_D(160), atr=_D(0), wallet=_D(10000), costs=flat)
    assert sized.qty == _D(0)
    assert sized.reason is RiskReason.RISK_LIQ_DISTANCE


def test_min_equity_blocks_below_and_allows_the_exact_floor() -> None:
    below = _size(wallet=_D("99.99"), cfg=limits(min_equity_usd=_D(100)))
    assert below.reason is RiskReason.RISK_MIN_EQUITY
    assert below.qty == _D(0)
    exact = _size(wallet=_D(100), cfg=limits(min_equity_usd=_D(100)))
    assert exact.reason is None
    assert exact.qty > 0


def test_min_equity_in_sizing_uses_the_sizing_equity_not_unrealized_profit() -> None:
    # Гаманець уже нижче порогу, total equity вище лише завдяки плаваючому прибутку.
    sized = _size(wallet=_D(99), equity=_D(150), cfg=limits(min_equity_usd=_D(100)))
    assert sized.reason is RiskReason.RISK_MIN_EQUITY
    assert sized.qty == _D(0)


def test_min_notional_is_checked_at_the_lower_of_reference_and_fill() -> None:
    # Лонг: fill 100.1 вище за опорну 100. 1 × 100.1 ≥ 100.05, але 1 × 100 — ні: біржа могла б відхилити.
    lot = instrument(qty_step=_D(1), min_qty=_D(1), min_notional=_D("100.05"))
    long = _size(wallet=_D(400), inst=lot)
    assert long.entry_fill == _D("100.1")
    assert long.qty == _D(0)
    assert long.reason is RiskReason.RISK_BELOW_MIN_NOTIONAL
    exact = _size(wallet=_D(400), inst=instrument(qty_step=_D(1), min_qty=_D(1), min_notional=_D(100)))
    assert exact.qty == _D(1)
    # Шорт: fill 99.9 нижче за опорну — перевіряється за fill.
    short = _size(
        side=_SHORT,
        stop=_D(101),
        target=_D(98),
        wallet=_D(400),
        inst=instrument(qty_step=_D(1), min_qty=_D(1), min_notional=_D("99.95")),
    )
    assert short.reason is RiskReason.RISK_BELOW_MIN_NOTIONAL


def test_max_qty_caps_the_size_without_rejecting_it() -> None:
    sized = _size(wallet=_D(100000), inst=instrument(max_qty=_D("0.5")), cfg=limits(min_equity_usd=_D(1)))
    assert sized.reason is None
    assert sized.qty == _D("0.5")


def test_non_positive_fill_reports_the_leg_that_broke() -> None:
    target_gone = CostModel(slippage_entry_atr=_D(0), slippage_sl_atr=_D(0), slippage_tp_atr=_D(1))
    long_target = _size(entry=_D(10), stop=_D(9), target=_D(11), atr=_D(20), costs=target_gone)
    assert long_target.reason is RiskReason.RISK_TARGET_INVALID
    entry_gone = CostModel(slippage_entry_atr=_D(1), slippage_sl_atr=_D(0), slippage_tp_atr=_D(0))
    short_entry = _size(side=_SHORT, entry=_D(10), stop=_D(11), target=_D(9), atr=_D(20), costs=entry_gone)
    assert short_entry.reason is RiskReason.RISK_PRICE_INVALID
    stop_gone = CostModel(slippage_entry_atr=_D(0), slippage_sl_atr=_D(1), slippage_tp_atr=_D(0))
    long_stop = _size(entry=_D(10), stop=_D(5), target=_D(11), atr=_D(20), costs=stop_gone)
    assert long_stop.reason is RiskReason.RISK_STOP_INVALID


def test_a_level_that_rounds_to_zero_reports_its_own_leg() -> None:
    # Лонг: вхід 0.05 → 0.1, стоп 0.01 → 0.1 (угору), ціль 0.08 униз дає 0 — ламається саме ціль.
    tiny = instrument(tick_size=_D("0.1"))
    target = _size(entry=_D("0.05"), stop=_D("0.01"), target=_D("0.08"), atr=_ZERO, inst=tiny)
    assert target.reason is RiskReason.RISK_TARGET_INVALID
    # Шорт: вхід 0.05 униз дає 0 — ламається вхід.
    entry = _size(side=_SHORT, entry=_D("0.05"), stop=_D("0.2"), target=None, atr=_ZERO, inst=tiny)
    assert entry.reason is RiskReason.RISK_PRICE_INVALID


def test_size_does_not_depend_on_the_ambient_decimal_context() -> None:
    # Торговий процес ділить потік із CCXT, який міняє контекст потоку; бектест — ні. Розмір має збігатися.
    reference = _size(entry=_D("100.37"), stop=_D("99.11"), target=_D("103.3"), atr=_D("1.7"), wallet=_D("1234.56"))
    with localcontext(Context(prec=5, rounding=ROUND_UP, traps=[Underflow, Inexact])):
        hostile = _size(entry=_D("100.37"), stop=_D("99.11"), target=_D("103.3"), atr=_D("1.7"), wallet=_D("1234.56"))
    assert hostile == reference
    assert reference.qty > 0


def test_mm_deduction_shrinks_qty_until_liquidation_is_far_enough() -> None:
    # При deduction > 0 більша кількість — ближча ліквідація: 4.948 за ризиком не проходить на 10×.
    flat = CostModel(slippage_entry_atr=_D(0), slippage_sl_atr=_D(0), slippage_tp_atr=_D(0))
    cfg = limits(max_leverage=_D(10))
    sized = _size(
        entry=_D(100),
        stop=_D(90),
        target=_D(120),
        atr=_D(0),
        wallet=_D(10000),
        costs=flat,
        cfg=cfg,
        deduction=_D(1),
    )
    assert sized.reason is None
    assert sized.qty > 0
    assert sized.qty < div_down(sized.risk_usd, sized.loss_per_unit)
    assert sized.qty % _D("0.001") == 0
    required = cfg.min_liq_distance_sl_multiple * (sized.entry_fill - sized.stop_price)
    assert sized.liq_price is not None
    assert sized.entry_fill - max(sized.liq_price, _D(0)) >= required
    # Найбільша безпечна кількість на цьому плечі: ще один крок уже ближче за межу.
    one_more = isolated_liq_price(
        side=_LONG,
        entry=sized.entry_fill,
        qty=sized.qty + _D("0.001"),
        leverage=sized.leverage,
        maintenance_margin_rate=_MMR,
        mm_deduction=_D(1),
        taker_fee_rate=flat.taker_fee_rate,
    )
    assert sized.entry_fill - one_more < required


def test_no_exposure_room_rejects_before_the_lot_check() -> None:
    blocked = _size(notional=_D(1500))
    assert blocked.reason is RiskReason.RISK_EXPOSURE
    room = _size(notional=_D("1499"))
    assert room.reason is None
    assert room.qty > 0


def test_qty_below_the_minimum_lot_is_not_rounded_up() -> None:
    sized = _size(wallet=_D(100), inst=instrument(min_qty=_D(1)), cfg=limits(min_equity_usd=_D(1)))
    assert sized.qty == _D(0)
    assert sized.reason is RiskReason.RISK_BELOW_MIN_QTY


def test_symbol_cap_binds_and_notional_stays_inside_it() -> None:
    sized = _size(wallet=_D(100000), cfg=limits(min_equity_usd=_D(1), max_notional_per_symbol_usd=_D(10)))
    assert sized.qty == _D("0.099")
    assert sized.qty * sized.entry_fill <= _D(10)
    assert sized.qty * sized.entry_price <= _D(10)


def test_short_notional_stays_inside_the_cap_at_the_reference_price() -> None:
    cap = _D(100)
    sized = _size(
        side=_SHORT,
        stop=_D(101),
        target=_D(98),
        wallet=_D(100000),
        cfg=limits(min_equity_usd=_D(1), max_notional_per_symbol_usd=cap),
    )
    assert sized.reason is None
    assert sized.entry_fill < sized.entry_price
    assert sized.qty * sized.entry_price <= cap
    assert sized.qty * sized.entry_fill <= cap


def test_leverage_between_one_x_and_the_cap_is_used_when_one_x_cannot_fit_the_lot() -> None:
    flat = CostModel(
        slippage_entry_bps=_D(0),
        slippage_entry_atr=_D(0),
        slippage_sl_bps=_D(0),
        slippage_sl_atr=_D(0),
        slippage_tp_bps=_D(0),
        slippage_tp_atr=_D(0),
    )
    cfg = limits(
        min_equity_usd=_D(1),
        max_leverage=_D(50),
        risk_per_trade_pct=_D("0.02"),
        max_notional_per_symbol_usd=_D(100000),
    )
    inst = instrument(min_qty=_D(1), max_leverage=_D(50))
    sized = _size(
        entry=_D(100),
        stop=_D(99),
        target=_D(102),
        atr=_D(0),
        wallet=_D(80),
        costs=flat,
        cfg=cfg,
        inst=inst,
    )
    assert sized.reason is None
    assert sized.qty >= _D(1)
    assert _D(1) < sized.leverage < _D(50)
    one_x = _size(
        entry=_D(100),
        stop=_D(99),
        target=_D(102),
        atr=_D(0),
        wallet=_D(80),
        costs=flat,
        cfg=limits(
            min_equity_usd=_D(1),
            max_leverage=_D(1),
            risk_per_trade_pct=_D("0.02"),
            max_notional_per_symbol_usd=_D(100000),
        ),
        inst=instrument(min_qty=_D(1), max_leverage=_D(1)),
    )
    assert one_x.qty == _D(0)
    assert one_x.reason is RiskReason.RISK_BELOW_MIN_QTY
    higher = sized.leverage + _D("0.01")
    step_up = _size(
        entry=_D(100),
        stop=_D(99),
        target=_D(102),
        atr=_D(0),
        wallet=_D(80),
        costs=flat,
        cfg=limits(
            min_equity_usd=_D(1),
            max_leverage=higher,
            risk_per_trade_pct=_D("0.02"),
            max_notional_per_symbol_usd=_D(100000),
        ),
        inst=instrument(min_qty=_D(1), max_leverage=higher),
    )
    assert step_up.leverage == sized.leverage


def test_prices_round_before_the_risk_is_measured() -> None:
    sized = _size(entry=_D("100.01"), stop=_D("99.01"), target=_D("101.19"))
    assert sized.entry_price == _D("100.1")
    assert sized.stop_price == _D("99.1")
    assert sized.take_profit_price == _D("101.1")
    assert sized.entry_price % _D("0.1") == 0
    assert sized.qty % _D("0.001") == 0


def test_stop_or_target_on_the_wrong_side_rejects() -> None:
    assert _size(stop=_D(101)).reason is RiskReason.RISK_STOP_INVALID
    assert _size(target=_D(99)).reason is RiskReason.RISK_TARGET_INVALID
    assert _size(side=_SHORT, stop=_D(99), target=_D(102)).reason is RiskReason.RISK_STOP_INVALID


def test_float_prices_are_rejected() -> None:
    with pytest.raises(ValueError, match="Decimal"):
        calc_position_size(
            side=_LONG,
            entry_price=100.0,  # type: ignore[arg-type]
            stop_price=_D(99),
            atr=_D(1),
            wallet_balance=_D(1000),
            total_equity=_D(1000),
            current_notional=_D(0),
            instrument=instrument(),
            limits=limits(),
            costs=CostModel(),
            maintenance_margin_rate=_MMR,
        )


@settings(max_examples=50, deadline=None)
@given(
    long=st.booleans(),
    equity_units=st.integers(min_value=2_000, max_value=40_000),
    entry_units=st.integers(min_value=200, max_value=4_000),
    stop_ticks=st.integers(min_value=2, max_value=40),
    atr_units=st.integers(min_value=1, max_value=40),
)
def test_risk_at_the_stop_never_exceeds_the_budget(
    long: bool,
    equity_units: int,
    entry_units: int,
    stop_ticks: int,
    atr_units: int,
) -> None:
    atr = _D(min(atr_units, stop_ticks))
    entry = _D(entry_units)
    distance = _D(stop_ticks)
    side = _LONG if long else _SHORT
    stop = entry - distance if long else entry + distance
    target = entry + distance * 2 if long else entry - distance * 2
    inst = instrument(tick_size=_D(1), qty_step=_D("0.01"), min_qty=_D("0.01"), max_leverage=_D(20))
    sized = _size(
        side=side,
        entry=entry,
        stop=stop,
        target=target,
        atr=atr,
        wallet=_D(equity_units),
        inst=inst,
        cfg=limits(min_equity_usd=_D(1), max_notional_per_symbol_usd=_D("10000000")),
    )
    assert (sized.qty == 0) == (sized.reason is not None)
    if sized.qty == 0:
        return
    assert sized.qty * sized.loss_per_unit <= sized.risk_usd
    assert sized.qty % inst.qty_step == 0
    assert sized.entry_price % inst.tick_size == 0
    assert sized.stop_price % inst.tick_size == 0
    target_price = sized.take_profit_price
    assert target_price is not None
    assert target_price % inst.tick_size == 0
    assert sized.leverage <= _D(3)
    assert sized.leverage % _D("0.01") == 0
    wider = _size(
        side=side,
        entry=entry,
        stop=stop - _D(1) if long else stop + _D(1),
        target=target,
        atr=atr,
        wallet=_D(equity_units),
        inst=inst,
        cfg=limits(min_equity_usd=_D(1), max_notional_per_symbol_usd=_D("10000000")),
    )
    assert wider.qty <= sized.qty
    larger = _size(
        side=side,
        entry=entry,
        stop=stop,
        target=target,
        atr=atr,
        wallet=_D(equity_units + 1000),
        inst=inst,
        cfg=limits(min_equity_usd=_D(1), max_notional_per_symbol_usd=_D("10000000")),
    )
    assert larger.qty >= sized.qty


@st.composite
def _sizing_case(draw: st.DrawFn) -> dict[str, object]:
    long = draw(st.booleans())
    tick = draw(st.sampled_from([_D("0.01"), _D("0.5"), _D(1)]))
    step = draw(st.sampled_from([_D("0.001"), _D("0.1"), _D(1)]))
    entry_ticks = draw(st.integers(min_value=50, max_value=100_000))
    stop_ticks = min(draw(st.integers(min_value=1, max_value=60_000)), entry_ticks - 1)
    entry = tick * entry_ticks
    min_qty = step * draw(st.integers(min_value=1, max_value=5))
    lots = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=1_000)))
    equity = _D(draw(st.integers(min_value=50, max_value=200_000)))
    costs = CostModel(slippage_entry_bps=draw(st.sampled_from([_D(0), _D(2), _D(10)])))
    return {
        "side": _LONG if long else _SHORT,
        "entry": entry,
        "stop": entry - tick * stop_ticks if long else entry + tick * stop_ticks,
        "target": None,
        "atr": tick * draw(st.integers(min_value=0, max_value=50)),
        "wallet": max(_ZERO, equity + _D(draw(st.integers(min_value=-20, max_value=20)))),
        "equity": equity,
        "notional": equity * draw(st.integers(min_value=0, max_value=200)) / 100,
        "costs": costs.scaled(_D(2)) if draw(st.booleans()) else costs,
        "cfg": limits(
            min_equity_usd=_D(10),
            max_notional_per_symbol_usd=draw(st.sampled_from([_D(50), _D(1000), _D(10) ** 9])),
            max_leverage=draw(st.sampled_from([_D(1), _D(3), _D(10), _D(50)])),
            risk_per_trade_pct=draw(st.sampled_from([_D("0.001"), _D("0.005"), _D("0.02")])),
            min_liq_distance_sl_multiple=draw(st.sampled_from([_D("1.01"), _D(2), _D(3)])),
        ),
        "inst": instrument(
            tick_size=tick,
            qty_step=step,
            min_qty=min_qty,
            min_notional=draw(st.sampled_from([_D(0), _D(5), _D(100)])),
            max_qty=None if lots is None else min_qty * lots,
            max_leverage=draw(st.sampled_from([_D(1), _D(5), _D(25), _D(100)])),
        ),
        "mmr": draw(st.sampled_from([_D("0.005"), _D("0.01"), _D("0.025")])),
        "deduction": draw(st.sampled_from([_ZERO, _ZERO, _D(1), _D(5000)])),
    }


# Приблизно п'ята частина випадків доходить до прийнятого розміру (решта — відмови з причиною).
@settings(max_examples=500, deadline=None)
@given(case=_sizing_case())
def test_accepted_size_respects_every_cap_and_the_liquidation_distance(case: dict[str, object]) -> None:
    sized = _size(**case)  # type: ignore[arg-type]
    assert (sized.qty == 0) == (sized.reason is not None)
    event(f"reason={sized.reason}")
    if sized.qty == 0:
        return
    inst = case["inst"]
    cfg = case["cfg"]
    assert isinstance(inst, Instrument)
    assert isinstance(cfg, RiskLimits)
    notional = case["notional"]
    assert isinstance(notional, Decimal)
    long = case["side"] is _LONG
    price = max(sized.entry_price, sized.entry_fill)
    assert sized.qty * sized.loss_per_unit <= sized.risk_usd
    assert sized.leverage <= min(cfg.max_leverage, inst.max_leverage)
    assert sized.leverage % _D("0.01") == 0
    assert sized.qty * price <= sized.sizing_equity * sized.leverage
    assert sized.qty * price <= sized.sizing_equity * cfg.max_total_exposure_pct - notional
    assert sized.qty * price <= cfg.max_notional_per_symbol_usd
    assert sized.qty % inst.qty_step == 0
    assert sized.qty >= inst.min_qty
    assert sized.qty * min(sized.entry_price, sized.entry_fill) >= inst.min_notional
    assert inst.max_qty is None or sized.qty <= inst.max_qty
    liq = sized.liq_price
    assert liq is not None
    distance = sized.entry_fill - max(liq, _ZERO) if long else liq - sized.entry_fill
    assert distance >= cfg.min_liq_distance_sl_multiple * abs(sized.entry_fill - sized.stop_price)
