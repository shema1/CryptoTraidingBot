"""Словник приватного API Bybit v5: шляхи, маршрути CCXT, мапінг кодів відповіді й статусів (ТЗ §3, §7; T3.2).

Факти — з офіційної документації v5 (context7, 2026-11-10) і коду CCXT 4.5.91; непідтверджене позначено
`TODO(verify-bybit)` і перевіряється на Testnet (T3.7).

**Коди — allowlist** (`app/exchange/errors.py`): `RequestRejected` і підкласи — лише для кодів, що за документацією
означають «запит не виконано» для цієї операції. Будь-який інший код, зокрема «внутрішня помилка», —
`ExchangeOutcomeUnknown`: «біржа відповіла» ще не означає «точно не виконала». Збій сервера (`10000`, `10016`,
`110079`, `-1`, retCode `429`) — `ExchangeUnavailable`: читання адаптер повторює, запис — ні.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final, Literal

from app.exchange.errors import (
    AdapterConfigError,
    DuplicateClientOrderId,
    ExchangeAuthError,
    ExchangeError,
    ExchangeOutcomeUnknown,
    ExchangeUnavailable,
    InsufficientMargin,
    MalformedResponse,
    OrderNotActive,
    OrderRejected,
    RateLimited,
    RequestRejected,
)
from app.exchange.types import ExchangeOrderStatus, ExecType, MarginMode, StopOrderType

# --- Шляхи й маршрути CCXT (`ccxt/abstract/bybit.py`) ---
PATH_ORDER_CREATE: Final = "v5/order/create"
PATH_ORDER_CANCEL: Final = "v5/order/cancel"
PATH_ORDER_REALTIME: Final = "v5/order/realtime"
PATH_ORDER_HISTORY: Final = "v5/order/history"
PATH_POSITION_LIST: Final = "v5/position/list"
PATH_EXECUTION_LIST: Final = "v5/execution/list"
PATH_WALLET_BALANCE: Final = "v5/account/wallet-balance"
PATH_ACCOUNT_INFO: Final = "v5/account/info"
PATH_SET_LEVERAGE: Final = "v5/position/set-leverage"
PATH_TRADING_STOP: Final = "v5/position/trading-stop"
PATH_SET_MARGIN_MODE: Final = "v5/account/set-margin-mode"

HttpMethod = Literal["GET", "POST"]

# Шлях → (HTTP-метод, implicit-метод CCXT). Інший шлях приватний клієнт не відправить.
PRIVATE_ROUTES: Final[Mapping[str, tuple[HttpMethod, str]]] = MappingProxyType(
    {
        PATH_ORDER_CREATE: ("POST", "privatePostV5OrderCreate"),
        PATH_ORDER_CANCEL: ("POST", "privatePostV5OrderCancel"),
        PATH_ORDER_REALTIME: ("GET", "privateGetV5OrderRealtime"),
        PATH_ORDER_HISTORY: ("GET", "privateGetV5OrderHistory"),
        PATH_POSITION_LIST: ("GET", "privateGetV5PositionList"),
        PATH_EXECUTION_LIST: ("GET", "privateGetV5ExecutionList"),
        PATH_WALLET_BALANCE: ("GET", "privateGetV5AccountWalletBalance"),
        PATH_ACCOUNT_INFO: ("GET", "privateGetV5AccountInfo"),
        PATH_SET_LEVERAGE: ("POST", "privatePostV5PositionSetLeverage"),
        PATH_TRADING_STOP: ("POST", "privatePostV5PositionTradingStop"),
        PATH_SET_MARGIN_MODE: ("POST", "privatePostV5AccountSetMarginMode"),
    }
)

# --- Коди відповіді ---
Operation = Literal["read", "place", "cancel", "leverage", "protection", "margin"]

RECV_WINDOW_CODE: Final = "10002"
RATE_LIMIT_CODE: Final = "10006"
DUPLICATE_CLIENT_ORDER_ID_CODE: Final = "110072"

# Автентифікація й права ключа: 10003 невалідний ключ, 10004 підпис, 10005 немає дозволу, 10010 IP не в білому списку.
_AUTH: Final = frozenset({"10003", "10004", "10005", "10010"})
# Збій сервера: 10000 таймаут сервера, 10016 внутрішня помилка / рестарт, 110079 «ордер обробляється, спробуйте
# пізніше», -1 «запит застарів», retCode 429 «високе навантаження». Результат запису невідомий.
_SERVER: Final = frozenset({"10000", "10016", "110079", "-1", "429"})
# 10017 — маршрут не знайдено (або категорія не відповідає режиму акаунта): помилка конфігурації, не ринку.
_CONFIG: Final = frozenset({"10017"})

# Відмови за операцією: код → клас. Лише коди, що за документацією означають «запит не виконано».
_REJECTIONS: Final[Mapping[Operation, Mapping[str, type[RequestRejected]]]] = MappingProxyType(
    {
        "read": MappingProxyType({"10001": RequestRejected}),
        "place": MappingProxyType(
            {
                "10001": OrderRejected,  # помилка параметрів
                "10027": OrderRejected,  # торгівлю заборонено
                "10029": OrderRejected,  # символ не в білому списку ключа
                "110003": OrderRejected,  # ціна поза дозволеним діапазоном
                "110017": OrderRejected,  # кількість округлюється до нуля
                "110020": OrderRejected,  # понад 500 активних ордерів
                "110094": OrderRejected,  # номінал нижче мінімуму
                "110004": InsufficientMargin,  # брак балансу гаманця
                "110007": InsufficientMargin,  # брак доступного балансу
                "110012": InsufficientMargin,  # брак доступного балансу
            }
        ),
        "cancel": MappingProxyType({"10001": RequestRejected}),
        "leverage": MappingProxyType({"10001": RequestRejected, "110013": RequestRejected}),
        "protection": MappingProxyType({"10001": RequestRejected}),
        # 110073 — «не вдалося змінити режим маржі» (замінив 3400045, документація pilot-feature).
        "margin": MappingProxyType({"10001": RequestRejected, "110073": RequestRejected, "3400045": RequestRejected}),
    }
)
# Скасування: ордера немає серед активних (110001 не існує, 110008 виконаний або скасований, 110010 скасований).
_NOT_ACTIVE: Final = frozenset({"110001", "110008", "110010"})
# «Не змінено» — успіх (ідемпотентність запису): 110043 плече вже таке.
_NOT_MODIFIED: Final[Mapping[Operation, frozenset[str]]] = MappingProxyType({"leverage": frozenset({"110043"})})


def error_for(op: Operation, code: str, message: str, *, client_order_id: str | None = None) -> ExchangeError | None:
    """Виняток для ненульового `retCode` операції `op`; `None` — «не змінено», тобто успіх.

    `message` — `retMsg` біржі, обрізаний викликачем (секретів у ньому немає: біржа не повертає ключ).
    """
    if code == "0":
        raise ValueError("retCode 0 — успіх, не помилка")
    if code in _NOT_MODIFIED.get(op, frozenset()):
        return None
    if op == "place" and code == DUPLICATE_CLIENT_ORDER_ID_CODE and client_order_id is not None:
        # Дубль `orderLinkId` — ордер з таким id уже існує: це не відмова (ТЗ §7.2, `errors.py`).
        return DuplicateClientOrderId(client_order_id)
    if op == "cancel" and code in _NOT_ACTIVE:
        return OrderNotActive(f"bybit {code}: {message}")
    rejection = _REJECTIONS[op].get(code)
    if rejection is not None:
        return rejection(code, message)
    if code in _AUTH:
        return ExchangeAuthError(f"bybit {code}: {message}")
    if code in _CONFIG:
        return AdapterConfigError(f"bybit {code}: {message}")
    if code == RATE_LIMIT_CODE:
        return RateLimited(f"bybit {code}")
    if code in _SERVER:
        return ExchangeUnavailable(f"bybit {code}: {message}")
    # Зокрема 10002 (recvWindow): де саме Bybit його перевіряє, не підтверджено — запит міг дійти (T3.7).
    return ExchangeOutcomeUnknown(f"bybit retCode {code}: {message}")


# --- Статуси ордера, типи виконань, режим маржі ---
_ORDER_STATUS: Final[Mapping[str, ExchangeOrderStatus]] = MappingProxyType(
    {
        "New": ExchangeOrderStatus.NEW,
        "Created": ExchangeOrderStatus.NEW,  # синонім початкового стану (T1.1)
        "PartiallyFilled": ExchangeOrderStatus.PARTIALLY_FILLED,
        "Untriggered": ExchangeOrderStatus.UNTRIGGERED,
        "Triggered": ExchangeOrderStatus.TRIGGERED,
        "Filled": ExchangeOrderStatus.FILLED,
        # У деривативах `Cancelled` буває з `cumExecQty > 0`; `PartiallyFilledCanceled` — spot, але мапиться так само.
        "Cancelled": ExchangeOrderStatus.CANCELED,
        "PartiallyFilledCanceled": ExchangeOrderStatus.CANCELED,
        "Rejected": ExchangeOrderStatus.REJECTED,
        "Deactivated": ExchangeOrderStatus.DEACTIVATED,
    }
)


def order_status(raw: str) -> ExchangeOrderStatus:
    """Статус ордера Bybit → домен. Невідомий — `UNKNOWN` (не термінальний), а не нерозбірна відповідь."""
    return _ORDER_STATUS.get(raw, ExchangeOrderStatus.UNKNOWN)


_STOP_ORDER_TYPE: Final[Mapping[str, StopOrderType]] = MappingProxyType(
    {
        "TakeProfit": StopOrderType.TAKE_PROFIT,
        "PartialTakeProfit": StopOrderType.TAKE_PROFIT,
        "StopLoss": StopOrderType.STOP_LOSS,
        "PartialStopLoss": StopOrderType.STOP_LOSS,
        "TrailingStop": StopOrderType.TRAILING_STOP,
        "Stop": StopOrderType.CONDITIONAL,
    }
)


def stop_order_type(raw: str) -> StopOrderType | None:
    """`stopOrderType` → домен; `""` / `UNKNOWN` — не умовний ордер; решта (`tpslOrder`, `OcoOrder`, …) — `OTHER`."""
    if raw in ("", "UNKNOWN"):
        return None
    return _STOP_ORDER_TYPE.get(raw, StopOrderType.OTHER)


_EXEC_TYPE: Final[Mapping[str, ExecType | None]] = MappingProxyType(
    {
        "Trade": ExecType.TRADE,
        "AdlTrade": ExecType.ADL_TRADE,
        "BustTrade": ExecType.BUST_TRADE,
        "Delivery": ExecType.DELIVERY,
        "Funding": None,  # funding — у `ledger` (T3.4), не fill
    }
)


def exec_type(raw: str) -> ExecType | None:
    """`execType` → домен; `None` — funding (не fill). Інший тип — `MalformedResponse`: невідоме виконання не
    пропускається мовчки, бо від нього залежать позиція й PnL."""
    if raw not in _EXEC_TYPE:
        raise MalformedResponse(f"bybit: невідомий execType {raw[:40]!r}")
    return _EXEC_TYPE[raw]


# `/v5/account/info` `marginMode` (UTA — на рівні акаунта). Портфельна маржа — крос за суттю.
_MARGIN_MODE: Final[Mapping[str, MarginMode]] = MappingProxyType(
    {
        "ISOLATED_MARGIN": MarginMode.ISOLATED,
        "REGULAR_MARGIN": MarginMode.CROSS,
        "PORTFOLIO_MARGIN": MarginMode.CROSS,
    }
)
MARGIN_MODE_REQUEST: Final[Mapping[MarginMode, str]] = MappingProxyType(
    {MarginMode.ISOLATED: "ISOLATED_MARGIN", MarginMode.CROSS: "REGULAR_MARGIN"}
)


def margin_mode(raw: str) -> MarginMode:
    """`marginMode` акаунта → домен. Невідомий — `MalformedResponse` (режим маржі не вгадується)."""
    found = _MARGIN_MODE.get(raw)
    if found is None:
        raise MalformedResponse(f"bybit: невідомий marginMode {raw[:40]!r}")
    return found


# `unifiedMarginStatus`: 5 / 6 — UTA 2.0 (pro). Інші (класичний акаунт, UTA 1.0) мають іншу семантику маржі й
# ордерів (`switch-isolated`, `openOnly`), тож адаптер їх не підтримує. TODO(verify-bybit): статус акаунта Testnet.
SUPPORTED_UNIFIED_STATUSES: Final = frozenset({5, 6})
