"""Машина станів ордера (ТЗ §7.1–7.2, рішення 0027; T3.1).

Граф — **лише тут** (`TRANSITION_SOURCES` / `ALLOWED_TRANSITIONS`): таблиця ТЗ §7.1 і три ребра «скасовано біржею»
(рішення 0027). Для кожного ребра задано, звідки може прийти подія: факти біржі (прийнято, виконано, скасовано,
відхилено біржею) — лише `WS`/`REST`; рішення бота (почати відправку, запит на скасування, «стан невідомий»,
«не знайдено після перевірок») — лише `LOCAL` (AGENTS.md, інваріант 1: біржа — джерело істини для фактів).
Будь-який інший перехід чи джерело — `InvariantViolation` і лог рівня error.

Перехід у БД — `transition_order`: `UPDATE orders … WHERE id AND version AND state` (OCC) і запис в `order_events`
у **тій самій** транзакції сесії викликача (коміт — за викликачем). Програна гонка — `OrderConflict`, не порушення
інваріанта: викликач перечитує стан і вирішує. З термінального стану переходів немає ні тут, ні в БД (тригер
`ctb_orders_guard()`): повтор — це **новий** ордер з новим `clientOrderId` (`create_order`), і лише для виходів
(алгоритм — T3.3).

Write-ahead (ТЗ §7.2) — `begin_submission`: `CREATED → SUBMITTING` у власній транзакції, яка комітиться до
повернення. Мережевий виклик `place_order` іде лише після нього і поза будь-якою транзакцією.

`exchange_target` — у який стан веде статус ордера біржі (`ExchangeOrderStatus`). Як застосувати статус, що
«перестрибує» стан (напр. `SUBMITTING` + `FILLED` — через `OPEN`) або застарів, вирішує той, хто обробляє події
біржі (T3.3, T3.4).
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Final, NoReturn

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.clock import now_ms
from app.core.logging import get_logger
from app.exchange.types import ExchangeOrderStatus, new_client_order_id
from app.trading.models import EVENT_SOURCES, ORDER_STATES, ORDER_TERMINAL_STATES, Order, OrderEvent

_LOG = get_logger("app.trading.order_state")

_EXCHANGE: Final = frozenset({"WS", "REST"})
_LOCAL: Final = frozenset({"LOCAL"})
_ANY: Final = frozenset(EVENT_SOURCES)

# Ребро → дозволені джерела події. Єдине місце графа (ТЗ §7.1 + рішення 0027).
TRANSITION_SOURCES: Final[Mapping[tuple[str, str], frozenset[str]]] = MappingProxyType(
    {
        ("CREATED", "SUBMITTING"): _LOCAL,  # write-ahead перед мережевим викликом
        ("SUBMITTING", "OPEN"): _EXCHANGE,  # біржа підтвердила прийняття
        ("SUBMITTING", "REJECTED"): _EXCHANGE,  # біржа відхилила (код зберігається)
        ("SUBMITTING", "SUBMISSION_UNKNOWN"): _LOCAL,  # таймаут, обрив, 5xx
        ("SUBMISSION_UNKNOWN", "OPEN"): _EXCHANGE,  # знайдено за clientOrderId
        ("SUBMISSION_UNKNOWN", "PARTIALLY_FILLED"): _EXCHANGE,
        ("SUBMISSION_UNKNOWN", "FILLED"): _EXCHANGE,
        # Бот: не знайдено після вікна перевірки; біржа: знайдено відхиленим (рішення 0027, рев'ю T3.1).
        ("SUBMISSION_UNKNOWN", "REJECTED"): _ANY,
        ("SUBMISSION_UNKNOWN", "CANCELED"): _EXCHANGE,  # знайдено вже скасованим біржею (рішення 0027)
        ("OPEN", "PARTIALLY_FILLED"): _EXCHANGE,
        ("OPEN", "FILLED"): _EXCHANGE,
        ("OPEN", "PENDING_CANCEL"): _LOCAL,  # бот відправив скасування
        # Лише факт біржі: тайм-аут входу бот оформлює скасуванням через PENDING_CANCEL — інакше живий на біржі ордер
        # вибув би з активних, і новий вхід дав би подвійну позицію (рев'ю T3.1, рішення 0027).
        ("OPEN", "EXPIRED"): _EXCHANGE,
        ("OPEN", "CANCELED"): _EXCHANGE,  # скасовано біржею (рішення 0027)
        ("PARTIALLY_FILLED", "FILLED"): _EXCHANGE,
        ("PARTIALLY_FILLED", "PENDING_CANCEL"): _LOCAL,
        ("PARTIALLY_FILLED", "CANCELED"): _EXCHANGE,  # залишок скасовано біржею (рішення 0027)
        ("PENDING_CANCEL", "CANCELED"): _EXCHANGE,  # біржа підтвердила скасування
        ("PENDING_CANCEL", "FILLED"): _EXCHANGE,  # встиг виконатися до скасування
        ("PENDING_CANCEL", "PARTIALLY_FILLED"): _EXCHANGE,
    }
)
# Перший факт біржі про ордер: id ордера біржі обов'язковий — без нього звірка не зіставить ордер (ТЗ §11).
EXCHANGE_ID_EDGES: Final = frozenset(
    {
        ("SUBMITTING", "OPEN"),
        ("SUBMISSION_UNKNOWN", "OPEN"),
        ("SUBMISSION_UNKNOWN", "PARTIALLY_FILLED"),
        ("SUBMISSION_UNKNOWN", "FILLED"),
        ("SUBMISSION_UNKNOWN", "CANCELED"),
    }
)
ALLOWED_TRANSITIONS: Final[Mapping[str, frozenset[str]]] = MappingProxyType(
    {state: frozenset(target for origin, target in TRANSITION_SOURCES if origin == state) for state in ORDER_STATES}
)

_EXCHANGE_TARGETS: Final[Mapping[ExchangeOrderStatus, str | None]] = MappingProxyType(
    {
        ExchangeOrderStatus.NEW: "OPEN",
        ExchangeOrderStatus.UNTRIGGERED: "OPEN",
        ExchangeOrderStatus.TRIGGERED: "OPEN",
        ExchangeOrderStatus.PARTIALLY_FILLED: "PARTIALLY_FILLED",
        ExchangeOrderStatus.FILLED: "FILLED",
        ExchangeOrderStatus.CANCELED: "CANCELED",
        ExchangeOrderStatus.DEACTIVATED: "CANCELED",
        ExchangeOrderStatus.REJECTED: "REJECTED",
        # Статус, якого адаптер не знає: переходу немає, звірка — CRITICAL (T3.3, ТЗ §11.4).
        ExchangeOrderStatus.UNKNOWN: None,
    }
)


def _check_graph() -> None:
    """Граф узгоджений з таблицею станів БД. Порушення — помилка коду, а не стан ринку: модуль не імпортується."""
    for origin, target in TRANSITION_SOURCES:
        if origin not in ORDER_STATES or target not in ORDER_STATES:
            raise RuntimeError(f"стан {origin} → {target} не з ORDER_STATES")
        if origin in ORDER_TERMINAL_STATES:
            raise RuntimeError(f"з термінального {origin} переходів немає")
    for sources in TRANSITION_SOURCES.values():
        if not sources or not sources <= _ANY:
            raise RuntimeError("джерела переходу не з EVENT_SOURCES")
    if set(_EXCHANGE_TARGETS) != set(ExchangeOrderStatus):
        raise RuntimeError("не кожен статус біржі має мапінг")
    for edge in EXCHANGE_ID_EDGES:
        if not TRANSITION_SOURCES[edge] <= _EXCHANGE:
            raise RuntimeError("id ордера біржі приходить лише з подією біржі")


_check_graph()


class InvariantViolation(Exception):
    """Перехід чи джерело події поза графом ТЗ §7.1. Помилка інваріанта: бот не має робити вигляд, що все гаразд."""

    def __init__(self, message: str, *, from_state: str, to_state: str, source: str) -> None:
        super().__init__(message)
        self.from_state = from_state
        self.to_state = to_state
        self.source = source


class OrderConflict(Exception):
    """OCC: стан або версія ордера вже не ті, з яких викликач рахував перехід. Перечитати і вирішити заново."""

    def __init__(self, order_id: uuid.UUID, *, state: str, version: int) -> None:
        super().__init__(f"ордер {order_id} уже в стані {state} (версія {version})")
        self.order_id = order_id
        self.state = state
        self.version = version


@dataclass(frozen=True, slots=True)
class NewOrder:
    """Новий ордер до відправки. Інваріанти походження (сигнал, режим, `reduce_only`) тримають CHECK-и БД."""

    account_id: uuid.UUID
    mode: str
    bot_id: uuid.UUID | None
    origin: str
    symbol: str
    side: str
    order_type: str
    reduce_only: bool
    qty: Decimal
    signal_id: uuid.UUID | None = None
    position_id: uuid.UUID | None = None
    price: Decimal | None = None
    time_in_force: str | None = None
    sl_price: Decimal | None = None
    tp_price: Decimal | None = None


@dataclass(frozen=True, slots=True)
class OrderRef:
    """Що викликач знає про ордер: з цих `state` і `version` рахується наступний перехід (OCC)."""

    id: uuid.UUID
    client_order_id: str | None
    state: str
    version: int


def check_transition(
    from_state: str,
    to_state: str,
    *,
    source: str,
    order_id: uuid.UUID | None = None,
    client_order_id: str | None = None,
) -> None:
    """Чи дозволений перехід і джерело події (ТЗ §7.1, рішення 0027). Ні — лог error і `InvariantViolation`."""
    problem: str | None = None
    if source not in EVENT_SOURCES:
        problem = f"невідоме джерело події {source!r}"
    elif from_state not in ORDER_STATES or to_state not in ORDER_STATES:
        problem = f"невідомий стан ордера {from_state!r} → {to_state!r}"
    elif to_state not in ALLOWED_TRANSITIONS[from_state]:
        problem = f"перехід {from_state} → {to_state} не дозволений графом ТЗ §7.1"
    elif source not in TRANSITION_SOURCES[(from_state, to_state)]:
        problem = f"джерело {source} не дозволене для {from_state} → {to_state}"
    if problem is None:
        return
    _LOG.error(
        "order_invariant_violation",
        from_state=from_state,
        to_state=to_state,
        source=source,
        order_id=None if order_id is None else str(order_id),
        client_order_id=client_order_id,
        problem=problem,
    )
    raise InvariantViolation(problem, from_state=from_state, to_state=to_state, source=source)


def exchange_target(status: ExchangeOrderStatus) -> str | None:
    """Стан, у який веде статус ордера біржі; `None` — статус невідомий, переходу немає."""
    return _EXCHANGE_TARGETS[status]


def _require_decimal(name: str, value: Decimal | None, *, optional: bool) -> None:
    if value is None and optional:
        return
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise ValueError(f"{name} має бути додатний скінченний Decimal")


def _json_default(value: object) -> str:
    if type(value) is Decimal and value.is_finite():
        return format(value, "f")
    raise TypeError(f"тип {type(value).__name__} не серіалізується")


def _payload(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """`payload` події — JSON без втрат: `Decimal` → рядок. Перевіряється до будь-якого запису (рев'ю T3.1)."""
    if payload is None:
        return None
    if not isinstance(payload, Mapping):
        raise ValueError("payload має бути Mapping")
    try:
        text = json.dumps(dict(payload), default=_json_default, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"payload не серіалізується в JSON: {exc}") from exc
    loaded = json.loads(text)
    if not isinstance(loaded, dict):
        raise ValueError("payload має бути об'єктом JSON")
    return loaded


async def _insert_event(
    session: AsyncSession,
    *,
    order_id: uuid.UUID,
    from_state: str | None,
    to_state: str,
    source: str,
    reason: str | None,
    payload: dict[str, Any] | None,
    ts_ms: int,
) -> None:
    await session.execute(
        insert(OrderEvent).values(
            order_id=order_id,
            from_state=from_state,
            to_state=to_state,
            source=source,
            reason=reason,
            payload=payload,
            ts_ms=ts_ms,
        )
    )


async def create_order(session: AsyncSession, order: NewOrder, *, host_id: str) -> OrderRef:
    """Новий ордер у `CREATED` з новим `clientOrderId` (`tb` + `HOST_ID` + uuid4 hex) і подією `None → CREATED`.

    Пише в сесію викликача і не комітить. Відправка — лише через `begin_submission`.
    """
    if type(order) is not NewOrder:
        raise ValueError("order має бути NewOrder")
    _require_decimal("qty", order.qty, optional=False)
    for name in ("price", "sl_price", "tp_price"):
        _require_decimal(name, getattr(order, name), optional=True)
    client_order_id = new_client_order_id(host_id)
    now = now_ms()
    # Ордер і його перша подія — одне ціле: savepoint відкочує обидва, якщо друга вставка впала.
    async with session.begin_nested():
        order_id = await _insert_order(session, order, client_order_id=client_order_id, now=now)
        await _insert_event(
            session,
            order_id=order_id,
            from_state=None,
            to_state="CREATED",
            source="LOCAL",
            reason=None,
            payload=None,
            ts_ms=now,
        )
    _LOG.info(
        "order_created",
        order_id=str(order_id),
        client_order_id=client_order_id,
        bot_id=None if order.bot_id is None else str(order.bot_id),
        symbol=order.symbol,
        origin=order.origin,
        reduce_only=order.reduce_only,
    )
    return OrderRef(id=order_id, client_order_id=client_order_id, state="CREATED", version=1)


async def _insert_order(session: AsyncSession, order: NewOrder, *, client_order_id: str, now: int) -> uuid.UUID:
    inserted: uuid.UUID = (
        await session.execute(
            insert(Order)
            .values(
                account_id=order.account_id,
                mode=order.mode,
                bot_id=order.bot_id,
                origin=order.origin,
                signal_id=order.signal_id,
                position_id=order.position_id,
                client_order_id=client_order_id,
                symbol=order.symbol,
                side=order.side,
                order_type=order.order_type,
                reduce_only=order.reduce_only,
                time_in_force=order.time_in_force,
                qty=order.qty,
                price=order.price,
                sl_price=order.sl_price,
                tp_price=order.tp_price,
                state="CREATED",
                version=1,
                created_at_ms=now,
                updated_at_ms=now,
            )
            .returning(Order.id)
        )
    ).scalar_one()
    return inserted


async def transition_order(
    session: AsyncSession,
    ref: OrderRef,
    to_state: str,
    *,
    source: str,
    reason: str | None = None,
    payload: Mapping[str, Any] | None = None,
    exchange_order_id: str | None = None,
    reject_code: str | None = None,
    reject_reason: str | None = None,
) -> OrderRef:
    """Перехід `ref.state → to_state` з OCC і подією в `order_events` у сесії викликача (коміт — за викликачем).

    Недозволений перехід чи джерело — `InvariantViolation` до будь-якого запису. Стан чи версія вже інші —
    `OrderConflict` без запису. `reject_code` — рівно для `→ REJECTED` (ТЗ §7.1: код помилки зберігається).
    `exchange_order_id` — лише з подією біржі й обов'язковий на першому факті біржі (`EXCHANGE_ID_EDGES`).
    `ref.client_order_id` має бути саме цього ордера (інакше `ValueError`): write-ahead і відправка — з одним id.
    `payload` перевіряється до запису (`Decimal` → рядок). UPDATE і подія — в одному savepoint: падіння запису події
    відкочує й новий стан, а сесія лишається придатною.
    """
    if type(ref) is not OrderRef:
        raise ValueError("ref має бути OrderRef")
    check_transition(ref.state, to_state, source=source, order_id=ref.id, client_order_id=ref.client_order_id)
    rejected = to_state == "REJECTED"
    if rejected != (isinstance(reject_code, str) and reject_code != ""):
        raise ValueError("reject_code обов'язковий рівно для переходу в REJECTED")
    if reject_reason is not None and not rejected:
        raise ValueError("reject_reason — лише для переходу в REJECTED")
    if exchange_order_id is not None:
        if not isinstance(exchange_order_id, str) or exchange_order_id == "":
            raise ValueError("exchange_order_id має бути непорожнім рядком")
        if source not in _EXCHANGE:
            raise ValueError("exchange_order_id — лише з подією біржі (WS/REST)")
    elif (ref.state, to_state) in EXCHANGE_ID_EDGES:
        raise ValueError(f"exchange_order_id обов'язковий для першого факту біржі {ref.state} → {to_state}")
    event_payload = _payload(payload)
    now = now_ms()
    values: dict[str, Any] = {"state": to_state, "version": Order.version + 1, "updated_at_ms": now}
    if to_state == "SUBMITTING":
        values["submitted_at_ms"] = now
    if exchange_order_id is not None:
        # Змінити вже записаний id не дасть тригер `ctb_orders_guard()` (ідентичність ордера незмінна).
        values["exchange_order_id"] = exchange_order_id
    if rejected:
        values["reject_code"] = reject_code
        values["reject_reason"] = reject_reason
    async with session.begin_nested():
        row = (
            await session.execute(
                update(Order)
                .where(
                    Order.id == ref.id,
                    Order.version == ref.version,
                    Order.state == ref.state,
                    Order.client_order_id.is_not_distinct_from(ref.client_order_id),
                )
                .values(**values)
                .returning(Order.version, Order.bot_id, Order.symbol)
                .execution_options(synchronize_session=False)
            )
        ).one_or_none()
        if row is None:
            await _refuse_stale(session, ref, to_state=to_state, source=source)
        await _insert_event(
            session,
            order_id=ref.id,
            from_state=ref.state,
            to_state=to_state,
            source=source,
            reason=reason,
            payload=event_payload,
            ts_ms=now,
        )
    _LOG.info(
        "order_transition",
        order_id=str(ref.id),
        client_order_id=ref.client_order_id,
        bot_id=None if row.bot_id is None else str(row.bot_id),
        symbol=row.symbol,
        from_state=ref.state,
        to_state=to_state,
        source=source,
        reason=reason,
    )
    return OrderRef(id=ref.id, client_order_id=ref.client_order_id, state=to_state, version=int(row.version))


async def _refuse_stale(session: AsyncSession, ref: OrderRef, *, to_state: str, source: str) -> NoReturn:
    """UPDATE не знайшов рядка з цими `version`/`state`/`clientOrderId`: розібратись чому і відмовити (завжди кидає)."""
    current = (
        await session.execute(select(Order.state, Order.version, Order.client_order_id).where(Order.id == ref.id))
    ).one_or_none()
    context = {"order_id": str(ref.id), "client_order_id": ref.client_order_id, "to_state": to_state, "source": source}
    if current is None:
        _LOG.error("order_not_found", **context)
        raise LookupError(f"ордера {ref.id} немає")
    if current.client_order_id != ref.client_order_id:
        _LOG.error("order_ref_mismatch", stored_client_order_id=current.client_order_id, **context)
        raise ValueError("clientOrderId у ref не цього ордера")
    _LOG.warning(
        "order_version_conflict",
        expected_state=ref.state,
        expected_version=ref.version,
        state=current.state,
        version=current.version,
        **context,
    )
    raise OrderConflict(ref.id, state=current.state, version=current.version)


async def begin_submission(sessions: async_sessionmaker[AsyncSession], ref: OrderRef) -> OrderRef:
    """Write-ahead (ТЗ §7.2): `CREATED → SUBMITTING` у власній транзакції, закомічений до повернення.

    Мережевий виклик `place_order` — лише після цього і поза транзакцією. Падіння між комітом і викликом лишає
    в БД `SUBMITTING`: відправка могла відбутися, і розв'язання йде як `SUBMISSION_UNKNOWN` (T3.3).
    **Будь-який виняток звідси — `place_order` не викликати**: write-ahead не відбувся.

    Нова сесія з фабрики — окреме з'єднання. Якщо той самий корутин тримає інше з'єднання з незакоміченим UPDATE
    цього ордера, виклик чекатиме на блокування без кінця (PostgreSQL не бачить циклу): такого не робити.
    Ізоляція — READ COMMITTED (як у проєкті): програна гонка — `OrderConflict`; під REPEATABLE READ / SERIALIZABLE
    вона прийшла б як помилка серіалізації 40001.
    """
    if type(ref) is not OrderRef or ref.client_order_id is None:
        raise ValueError("відправляється лише ордер бота з clientOrderId")
    async with sessions() as session, session.begin():
        return await transition_order(session, ref, "SUBMITTING", source="LOCAL", reason="write_ahead")
