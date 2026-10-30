"""Коди причин відмови входу і примусового виходу (ТЗ §5.2.3, §8.1).

Рядок коду збігається зі значенням enum і проходить CHECK `risk_events.reason_code`
(`^[A-Z][A-Z0-9_]*$`). Запис у `risk_events` і `decision_log` робить RiskGate (T4.1);
тут лише стабільні коди.
"""

from __future__ import annotations

from enum import StrEnum


class RiskReason(StrEnum):
    """Чому вхід не береться або позицію треба закрити."""

    RISK_MIN_EQUITY = "RISK_MIN_EQUITY"
    RISK_MAX_DRAWDOWN = "RISK_MAX_DRAWDOWN"
    RISK_DAILY_LOSS = "RISK_DAILY_LOSS"
    RISK_MAX_POSITIONS = "RISK_MAX_POSITIONS"
    RISK_MAX_TRADES_PER_DAY = "RISK_MAX_TRADES_PER_DAY"
    RISK_COOLDOWN = "RISK_COOLDOWN"
    RISK_FUNDING = "RISK_FUNDING"
    RISK_MAX_SLIPPAGE = "RISK_MAX_SLIPPAGE"
    RISK_EXPOSURE = "RISK_EXPOSURE"
    RISK_LIQ_DISTANCE = "RISK_LIQ_DISTANCE"
    RISK_QTY_ZERO = "RISK_QTY_ZERO"
    RISK_BELOW_MIN_QTY = "RISK_BELOW_MIN_QTY"
    RISK_BELOW_MIN_NOTIONAL = "RISK_BELOW_MIN_NOTIONAL"
    RISK_PRICE_INVALID = "RISK_PRICE_INVALID"
    RISK_STOP_INVALID = "RISK_STOP_INVALID"
    RISK_TARGET_INVALID = "RISK_TARGET_INVALID"
    # Не відмова входу: позицію треба закрити (ТЗ §6.6).
    TIME_STOP = "TIME_STOP"
