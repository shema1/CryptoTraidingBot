"""Один і той самий вхід дає один розмір і одні й ті самі ліміти (ТЗ §8.3, §18.1)."""

from __future__ import annotations

from decimal import Decimal

from app.exchange.types import PositionSide
from app.risk.costs import CostModel
from app.risk.limits import check_entry_limits
from app.risk.reasons import RiskReason
from app.risk.sizing import assess_entry, calc_position_size
from tests.risk.conftest import instrument, limits

_D = Decimal


def _kwargs() -> dict[str, object]:
    return {
        "side": PositionSide.LONG,
        "entry_price": _D(100),
        "stop_price": _D(99),
        "take_profit_price": _D(102),
        "atr": _D(2),
        "wallet_balance": _D(1000),
        "total_equity": _D(1000),
        "peak_equity": _D(1000),
        "day_start_equity": _D(1000),
        "current_notional": _D(0),
        "bot_open_positions": 0,
        "open_positions": 0,
        "trades_today": 0,
        "consecutive_losses": 0,
        "bars_since_last_loss": 0,
        "instrument": instrument(),
        "limits": limits(),
        "costs": CostModel(),
        "maintenance_margin_rate": _D("0.005"),
    }


def test_repeated_call_is_the_same_decision() -> None:
    kwargs = _kwargs()
    assert assess_entry(**kwargs) == assess_entry(**kwargs)  # type: ignore[arg-type]


def test_composition_matches_the_pieces_backtest_and_the_gate_will_share() -> None:
    kwargs = _kwargs()
    decision = assess_entry(**kwargs)  # type: ignore[arg-type]
    blocked = check_entry_limits(
        total_equity=kwargs["total_equity"],  # type: ignore[arg-type]
        peak_equity=kwargs["peak_equity"],  # type: ignore[arg-type]
        day_start_equity=kwargs["day_start_equity"],  # type: ignore[arg-type]
        bot_open_positions=0,
        open_positions=0,
        trades_today=0,
        consecutive_losses=0,
        bars_since_last_loss=0,
        limits=kwargs["limits"],  # type: ignore[arg-type]
        side=PositionSide.LONG,
    )
    assert blocked.ok
    size = calc_position_size(
        side=PositionSide.LONG,
        entry_price=_D(100),
        stop_price=_D(99),
        take_profit_price=_D(102),
        atr=_D(2),
        wallet_balance=_D(1000),
        total_equity=_D(1000),
        current_notional=_D(0),
        instrument=instrument(),
        limits=limits(),
        costs=CostModel(),
        maintenance_margin_rate=_D("0.005"),
    )
    assert decision.size == size
    assert decision.reason is None
    assert assess_entry.__module__ == "app.risk.sizing"
    assert calc_position_size.__module__ == "app.risk.sizing"


def test_daily_loss_uses_total_equity_not_the_wallet() -> None:
    # Гаманець цілий, total equity вже на денному ліміті: блок, хоча sizing бачив би гаманець.
    kwargs = _kwargs()
    kwargs["wallet_balance"] = _D(1000)
    kwargs["total_equity"] = _D(980)
    kwargs["peak_equity"] = _D(980)
    kwargs["day_start_equity"] = _D(1000)
    decision = assess_entry(**kwargs)  # type: ignore[arg-type]
    assert decision.reason is RiskReason.RISK_DAILY_LOSS
    assert decision.size is None

    # Плаваючий прибуток: денний результат за total equity нульовий, розмір — з гаманця.
    kwargs["total_equity"] = _D(1000)
    kwargs["peak_equity"] = _D(1000)
    kwargs["wallet_balance"] = _D(900)
    allowed = assess_entry(**kwargs)  # type: ignore[arg-type]
    full = assess_entry(**_kwargs())  # type: ignore[arg-type]
    assert allowed.ok
    assert allowed.size is not None
    assert full.size is not None
    assert allowed.size.sizing_equity == _D(900)
    assert allowed.size.qty < full.size.qty


def test_open_position_of_the_bot_blocks_a_new_entry_below_the_account_cap() -> None:
    kwargs = _kwargs()
    kwargs["bot_open_positions"] = 1
    kwargs["open_positions"] = 1
    decision = assess_entry(**kwargs)  # type: ignore[arg-type]
    assert decision.reason is RiskReason.RISK_MAX_POSITIONS
    assert decision.size is None


def test_drawdown_blocks_before_a_position_that_would_otherwise_fit() -> None:
    kwargs = _kwargs()
    kwargs["total_equity"] = _D(900)
    kwargs["peak_equity"] = _D(1000)
    kwargs["day_start_equity"] = _D(900)
    decision = assess_entry(**kwargs)  # type: ignore[arg-type]
    assert decision.reason is RiskReason.RISK_MAX_DRAWDOWN
    assert decision.size is None
