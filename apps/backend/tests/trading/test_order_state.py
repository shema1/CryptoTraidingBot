"""Граф станів ордера (ТЗ §7.1, рішення 0027; T3.1): лише чиста логіка, без БД."""

from __future__ import annotations

import itertools

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from structlog.testing import capture_logs

from app.exchange.types import ExchangeOrderStatus
from app.trading.models import EVENT_SOURCES, ORDER_STATES, ORDER_TERMINAL_STATES
from app.trading.order_state import (
    ALLOWED_TRANSITIONS,
    TRANSITION_SOURCES,
    InvariantViolation,
    check_transition,
    exchange_target,
)

EXCHANGE = frozenset({"WS", "REST"})
LOCAL = frozenset({"LOCAL"})
ANY = frozenset(EVENT_SOURCES)

# Таблиця ТЗ §7.1 + три ребра «скасовано біржею» (рішення 0027), з джерелом події для кожного ребра:
# факти біржі — лише WS/REST, рішення бота — лише LOCAL (інваріант 1: біржа — джерело істини для фактів).
# `SUBMISSION_UNKNOWN → REJECTED` — і те, і те: «не знайдено» вирішує бот, «знайдено відхиленим» каже біржа.
# `OPEN → EXPIRED` — лише факт біржі: тайм-аут входу бот оформлює скасуванням через PENDING_CANCEL (рев'ю T3.1).
SPEC: dict[tuple[str, str], frozenset[str]] = {
    ("CREATED", "SUBMITTING"): LOCAL,
    ("SUBMITTING", "OPEN"): EXCHANGE,
    ("SUBMITTING", "REJECTED"): EXCHANGE,
    ("SUBMITTING", "SUBMISSION_UNKNOWN"): LOCAL,
    ("SUBMISSION_UNKNOWN", "OPEN"): EXCHANGE,
    ("SUBMISSION_UNKNOWN", "PARTIALLY_FILLED"): EXCHANGE,
    ("SUBMISSION_UNKNOWN", "FILLED"): EXCHANGE,
    ("SUBMISSION_UNKNOWN", "REJECTED"): ANY,
    ("SUBMISSION_UNKNOWN", "CANCELED"): EXCHANGE,
    ("OPEN", "PARTIALLY_FILLED"): EXCHANGE,
    ("OPEN", "FILLED"): EXCHANGE,
    ("OPEN", "PENDING_CANCEL"): LOCAL,
    ("OPEN", "EXPIRED"): EXCHANGE,
    ("OPEN", "CANCELED"): EXCHANGE,
    ("PARTIALLY_FILLED", "FILLED"): EXCHANGE,
    ("PARTIALLY_FILLED", "PENDING_CANCEL"): LOCAL,
    ("PARTIALLY_FILLED", "CANCELED"): EXCHANGE,
    ("PENDING_CANCEL", "CANCELED"): EXCHANGE,
    ("PENDING_CANCEL", "FILLED"): EXCHANGE,
    ("PENDING_CANCEL", "PARTIALLY_FILLED"): EXCHANGE,
}


# Переходи з першим фактом біржі про ордер: id ордера біржі обов'язковий (без нього звірка не знайде ордер).
EXCHANGE_ID_EDGES = frozenset(
    {
        ("SUBMITTING", "OPEN"),
        ("SUBMISSION_UNKNOWN", "OPEN"),
        ("SUBMISSION_UNKNOWN", "PARTIALLY_FILLED"),
        ("SUBMISSION_UNKNOWN", "FILLED"),
        ("SUBMISSION_UNKNOWN", "CANCELED"),
    }
)


def test_the_graph_is_exactly_the_spec_table_and_lives_in_one_place() -> None:
    edges = {(origin, target) for origin, targets in ALLOWED_TRANSITIONS.items() for target in targets}
    assert edges == set(SPEC)
    assert set(ALLOWED_TRANSITIONS) <= set(ORDER_STATES)
    assert dict(TRANSITION_SOURCES) == SPEC
    for state in ORDER_TERMINAL_STATES:
        assert not ALLOWED_TRANSITIONS.get(state), f"з термінального {state} переходів немає"


@pytest.mark.parametrize(("edge", "sources"), sorted(SPEC.items()))
def test_every_allowed_transition_passes_with_its_sources_only(edge: tuple[str, str], sources: frozenset[str]) -> None:
    origin, target = edge
    for source in EVENT_SOURCES:
        if source in sources:
            check_transition(origin, target, source=source)
        else:
            with pytest.raises(InvariantViolation, match="джерело"):
                check_transition(origin, target, source=source)


def test_every_other_pair_of_states_is_an_invariant_violation_and_is_logged() -> None:
    forbidden = [pair for pair in itertools.product(ORDER_STATES, ORDER_STATES) if pair not in SPEC]
    assert len(forbidden) == len(ORDER_STATES) ** 2 - len(SPEC)
    for origin, target in forbidden:
        for source in EVENT_SOURCES:
            with capture_logs() as logs, pytest.raises(InvariantViolation) as raised:
                check_transition(origin, target, source=source)
            assert (raised.value.from_state, raised.value.to_state, raised.value.source) == (origin, target, source)
            violations = [entry for entry in logs if entry["event"] == "order_invariant_violation"]
            assert len(violations) == 1
            assert violations[0]["log_level"] == "error"
            assert (violations[0]["from_state"], violations[0]["to_state"]) == (origin, target)


def test_unknown_states_and_sources_are_violations_not_silent_passes() -> None:
    with pytest.raises(InvariantViolation):
        check_transition("NEW", "OPEN", source="WS")
    with pytest.raises(InvariantViolation):
        check_transition("OPEN", "FILLED", source="TELEGRAM")


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (ExchangeOrderStatus.NEW, "OPEN"),
        (ExchangeOrderStatus.UNTRIGGERED, "OPEN"),
        (ExchangeOrderStatus.TRIGGERED, "OPEN"),
        (ExchangeOrderStatus.PARTIALLY_FILLED, "PARTIALLY_FILLED"),
        (ExchangeOrderStatus.FILLED, "FILLED"),
        (ExchangeOrderStatus.CANCELED, "CANCELED"),
        (ExchangeOrderStatus.DEACTIVATED, "CANCELED"),
        (ExchangeOrderStatus.REJECTED, "REJECTED"),
        (ExchangeOrderStatus.UNKNOWN, None),
    ],
)
def test_exchange_status_maps_to_a_state_or_to_no_transition(status: ExchangeOrderStatus, expected: str | None) -> None:
    assert exchange_target(status) == expected


def test_every_exchange_status_is_mapped() -> None:
    for status in ExchangeOrderStatus:
        target = exchange_target(status)
        assert target is None or target in ORDER_STATES
        # Термінальний статус біржі — термінальний стан (або «не знаю»), не активний ордер.
        if status.is_terminal:
            assert target in ORDER_TERMINAL_STATES


@settings(max_examples=300, deadline=None)
@given(
    st.lists(
        st.tuples(st.sampled_from(ORDER_STATES), st.sampled_from(EVENT_SOURCES)),
        max_size=40,
    )
)
def test_no_sequence_of_events_leaves_the_graph(events: list[tuple[str, str]]) -> None:
    """Довільні події: стан завжди з `ORDER_STATES`, відхилена подія його не змінює, термінальний — поглинає."""
    state = "CREATED"
    for target, source in events:
        before = state
        try:
            check_transition(state, target, source=source)
        except InvariantViolation:
            assert state == before
            continue
        assert (before, target) in SPEC
        assert source in SPEC[(before, target)]
        assert before not in ORDER_TERMINAL_STATES
        state = target
        assert state in ORDER_STATES


def test_only_the_state_machine_writes_order_states_and_events() -> None:
    """Рев'ю T3.1: обійти `check_transition` можна лише записом в `orders`/`order_events` поза модулем — його немає."""
    import re
    from pathlib import Path

    app = Path(__file__).resolve().parents[2] / "app"
    writes = re.compile(r"update\(Order\)|insert\(Order\)|insert\(OrderEvent\)|\bOrderEvent\(|update\(OrderEvent\)")
    offenders = [
        str(path.relative_to(app))
        for path in app.rglob("*.py")
        if path.name != "order_state.py"
        and path.relative_to(app).as_posix() != "trading/models.py"
        and writes.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []
