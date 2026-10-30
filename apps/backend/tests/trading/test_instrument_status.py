"""Перехід паузи за статусом інструмента (ТЗ §6.1, T1.7). Запис у БД — у тесті кешу."""

from __future__ import annotations

import pytest

from app.trading.instrument_status import INSTRUMENT_NOT_TRADING, instrument_pause_transition


@pytest.mark.parametrize(
    ("status", "reasons", "tradable", "expected_status", "expected_reasons"),
    [
        ("RUNNING", (), False, "PAUSED", (INSTRUMENT_NOT_TRADING,)),
        ("RUNNING", (INSTRUMENT_NOT_TRADING,), False, "PAUSED", (INSTRUMENT_NOT_TRADING,)),
        ("PAUSED", ("MANUAL",), False, "PAUSED", ("MANUAL", INSTRUMENT_NOT_TRADING)),
        ("PAUSED", ("MANUAL", INSTRUMENT_NOT_TRADING), False, "PAUSED", ("MANUAL", INSTRUMENT_NOT_TRADING)),
        ("RECONCILING", (), False, "RECONCILING", (INSTRUMENT_NOT_TRADING,)),
        ("EMERGENCY_STOP", (), False, "EMERGENCY_STOP", (INSTRUMENT_NOT_TRADING,)),
        ("ERROR", ("DRIFT",), False, "ERROR", ("DRIFT", INSTRUMENT_NOT_TRADING)),
        ("STOPPED", (), False, "STOPPED", (INSTRUMENT_NOT_TRADING,)),
        ("STARTING", (), False, "STARTING", (INSTRUMENT_NOT_TRADING,)),
        ("PAUSED", (INSTRUMENT_NOT_TRADING,), True, "RUNNING", ()),
        ("PAUSED", ("MANUAL", INSTRUMENT_NOT_TRADING), True, "PAUSED", ("MANUAL",)),
        ("RECONCILING", (INSTRUMENT_NOT_TRADING,), True, "RECONCILING", ()),
        ("EMERGENCY_STOP", (INSTRUMENT_NOT_TRADING,), True, "EMERGENCY_STOP", ()),
        ("STOPPED", (INSTRUMENT_NOT_TRADING,), True, "STOPPED", ()),
        ("RUNNING", (), True, "RUNNING", ()),
        ("ERROR", ("DRIFT", INSTRUMENT_NOT_TRADING), True, "ERROR", ("DRIFT",)),
    ],
)
def test_pause_transition(
    status: str,
    reasons: tuple[str, ...],
    tradable: bool,
    expected_status: str,
    expected_reasons: tuple[str, ...],
) -> None:
    assert instrument_pause_transition(status=status, pause_reasons=reasons, tradable=tradable) == (
        expected_status,
        expected_reasons,
    )


def test_unknown_pause_reason_is_rejected() -> None:
    with pytest.raises(ValueError, match="невідому"):
        instrument_pause_transition(status="PAUSED", pause_reasons=("NOPE",), tradable=False)
