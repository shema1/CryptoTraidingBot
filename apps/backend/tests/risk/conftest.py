"""Спільні фікстури розміру і лімітів (T2.2)."""

from __future__ import annotations

from decimal import Decimal

from app.exchange.types import Instrument
from app.risk.limits import RiskLimits


def instrument(**overrides: Decimal | str | int | None) -> Instrument:
    data: dict[str, Decimal | str | int | None] = {
        "symbol": "BTC/USDT:USDT",
        "tick_size": Decimal("0.1"),
        "qty_step": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "min_notional": Decimal(0),
        "max_qty": None,
        "max_leverage": Decimal("100"),
        "launch_time_ms": 0,
        "status": "Trading",
    }
    data.update(overrides)
    return Instrument(**data)  # type: ignore[arg-type]


def limits(**overrides: Decimal | int | None) -> RiskLimits:
    data: dict[str, Decimal | int | None] = {
        "min_equity_usd": Decimal("100"),
        "max_notional_per_symbol_usd": Decimal("1000000"),
    }
    data.update(overrides)
    return RiskLimits(**data)  # type: ignore[arg-type]
