"""Чисті правила кешу: торгованість, блок входу, початок скану, коди відхилення (T1.7)."""

from __future__ import annotations

import pytest

from app.exchange.instruments import (
    INSTRUMENT_MAX_AGE_MS,
    INSTRUMENT_REFRESH_REJECTION_CODES,
    InstrumentEntryBlock,
    candle_scan_start_ms,
    entry_block,
    is_instrument_refresh_rejection,
    is_tradable,
)

HOUR = 3_600_000
START = 1_577_836_800_000  # 2020-01-01T00:00:00Z, кратно 1h


@pytest.mark.parametrize(
    ("status", "tradable"),
    [
        ("Trading", True),
        ("PreLaunch", False),
        ("PendingOpen", False),
        ("Closed", False),
        ("Absent", False),
        ("", False),
    ],
)
def test_only_trading_status_is_tradable(status: str, tradable: bool) -> None:
    assert is_tradable(status) is tradable


def test_entry_block_fails_closed() -> None:
    now = 10_000
    assert entry_block(status=None, updated_at_ms=None, now_ms_value=now) is InstrumentEntryBlock.MISSING
    assert entry_block(status="PreLaunch", updated_at_ms=now, now_ms_value=now) is InstrumentEntryBlock.NOT_TRADABLE
    assert entry_block(status="Trading", updated_at_ms=now, now_ms_value=now) is None
    assert (
        entry_block(status="Trading", updated_at_ms=now - INSTRUMENT_MAX_AGE_MS, now_ms_value=now)
        is InstrumentEntryBlock.STALE
    )
    assert entry_block(status="Trading", updated_at_ms=now - INSTRUMENT_MAX_AGE_MS + 1, now_ms_value=now) is None
    # Годинник відкотився: мітка з майбутнього нічого не підтверджує.
    assert entry_block(status="Trading", updated_at_ms=now + 1, now_ms_value=now) is InstrumentEntryBlock.STALE
    # Неторгованість важливіша за вік: причина паузи, а не «застаріло».
    assert entry_block(status="Closed", updated_at_ms=0, now_ms_value=now) is InstrumentEntryBlock.NOT_TRADABLE


def test_candle_scan_starts_at_the_first_bar_on_or_after_listing() -> None:
    assert candle_scan_start_ms(START, START - HOUR, step_ms=HOUR) == START
    assert candle_scan_start_ms(START, START, step_ms=HOUR) == START
    later = START + 10 * HOUR
    assert candle_scan_start_ms(START, later, step_ms=HOUR) == later
    assert candle_scan_start_ms(START, later + 1, step_ms=HOUR) == later + HOUR
    assert candle_scan_start_ms(START, later + HOUR - 1, step_ms=HOUR) == later + HOUR


def test_filter_rejection_codes_are_an_allowlist() -> None:
    assert is_instrument_refresh_rejection("110003") is True
    assert is_instrument_refresh_rejection("110094") is True
    assert is_instrument_refresh_rejection("30228") is True
    assert is_instrument_refresh_rejection("10001") is False
    assert "10001" not in INSTRUMENT_REFRESH_REJECTION_CODES
    with pytest.raises(TypeError):
        is_instrument_refresh_rejection(110003)  # type: ignore[arg-type]
