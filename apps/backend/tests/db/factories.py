"""Фабрики рядків для integration-тестів схеми (T0.4). Значення — мінімально валідні; перекриваються kwargs."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import Executable, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from app.api.models import AuditLog
from app.exchange.models import ExchangeAccount
from app.ml.models import ModelArtifact
from app.trading.models import Bot, Order, Signal

HOUR_MS = 3_600_000
T0 = 1_767_225_600_000  # 2026-01-01T00:00:00Z
SYMBOL = "BTC/USDT:USDT"


async def expect_violation(conn: AsyncConnection, stmt: Executable, constraint: str) -> None:
    with pytest.raises(IntegrityError) as excinfo:
        async with conn.begin_nested():
            await conn.execute(stmt)
    assert constraint in str(excinfo.value.orig), str(excinfo.value.orig)


async def ok(conn: AsyncConnection, stmt: Executable) -> Any:
    return (await conn.execute(stmt)).scalar_one_or_none()


def coid(host: str = "aa") -> str:
    return "tb" + host + uuid.uuid4().hex


async def make_account(conn: AsyncConnection, mode: str = "demo") -> uuid.UUID:
    suffix = uuid.uuid4().hex[:8].upper()
    return await ok(  # type: ignore[no-any-return]
        conn,
        insert(ExchangeAccount)
        .values(name=f"acc-{suffix}", exchange="bybit", mode=mode, env_key_set=f"BYBIT_{suffix}", allowed_host_id="aa")
        .returning(ExchangeAccount.id),
    )


async def make_audit(
    conn: AsyncConnection,
    action: str = "test.action",
    *,
    confirmed: bool = True,
    account_id: uuid.UUID | None = None,
    target: str | None = None,
) -> int:
    values = {"actor": "user", "action": action, "confirmed": confirmed, "account_id": account_id, "target": target}
    return await ok(conn, insert(AuditLog).values(values).returning(AuditLog.id))  # type: ignore[no-any-return]


def model_values(version: str, **overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "model_version": version,
        "algorithm": "lightgbm",
        "label_type": "meta",
        "trade_symbol": SYMBOL,
        "timeframe": "1h",
        "feature_schema_hash": "sha256:x",
        "preprocessing_version": "1.0.0",
        "git_commit": "a1b2c3d",
        "git_dirty": False,
        "library_versions": {"lightgbm": "4.5.0"},
        "artifact_path": f"models/{version}/model.txt",
        "metadata_json": {},
    }
    values.update(overrides)
    return values


async def make_model(conn: AsyncConnection, **overrides: Any) -> str:
    version = f"m-{uuid.uuid4().hex[:8]}"
    await conn.execute(insert(ModelArtifact).values(model_values(version, **overrides)))
    return version


async def make_bot(
    conn: AsyncConnection, account_id: uuid.UUID, mode: str = "demo", status: str = "RUNNING", **kw: Any
) -> uuid.UUID:
    return await ok(  # type: ignore[no-any-return]
        conn,
        insert(Bot)
        .values(account_id=account_id, mode=mode, symbol=kw.pop("symbol", SYMBOL), timeframe="1h", status=status, **kw)
        .returning(Bot.id),
    )


def signal_values(bot_id: uuid.UUID, model_version: str, entry_ms: int = T0, **overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "bot_id": bot_id,
        "entry_candle_open_time_ms": entry_ms,
        "features_candle_open_time_ms": entry_ms - HOUR_MS,
        "computed_at_ms": entry_ms + 420,
        "feature_schema_hash": "sha256:x",
        "model_version": model_version,
        "primary_signal": "LONG",
        "prediction": "LONG",
        "probability": Decimal("0.47"),
        "expected_edge_atr": Decimal("0.14"),
        "decision": "ENTER",
    }
    values.update(overrides)
    return values


async def make_signal(conn: AsyncConnection, bot_id: uuid.UUID, model_version: str, entry_ms: int = T0) -> uuid.UUID:
    return await ok(  # type: ignore[no-any-return]
        conn, insert(Signal).values(signal_values(bot_id, model_version, entry_ms)).returning(Signal.id)
    )


def order_values(account_id: uuid.UUID, mode: str, bot_id: uuid.UUID | None, **overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "account_id": account_id,
        "mode": mode,
        "bot_id": bot_id,
        "client_order_id": coid(),
        # За замовчуванням: з сигналом — ордер бота, без сигналу — ручний тестовий вхід у Demo (ТЗ §16).
        "origin": "BOT" if overrides.get("signal_id") else "MANUAL_TEST",
        "symbol": SYMBOL,
        "side": "BUY",
        "order_type": "MARKET",
        "reduce_only": False,
        "qty": Decimal("0.010"),
    }
    values.update(overrides)
    return values


async def make_order(
    conn: AsyncConnection, account_id: uuid.UUID, mode: str, bot_id: uuid.UUID | None, **overrides: Any
) -> uuid.UUID:
    return await ok(  # type: ignore[no-any-return]
        conn, insert(Order).values(order_values(account_id, mode, bot_id, **overrides)).returning(Order.id)
    )


def fill_values(order_id: uuid.UUID, account_id: uuid.UUID, mode: str, exec_id: str) -> dict[str, Any]:
    return {
        "exec_id": exec_id,
        "order_id": order_id,
        "account_id": account_id,
        "mode": mode,
        "symbol": SYMBOL,
        "side": "BUY",
        "price": Decimal("65000.5"),
        "qty": Decimal("0.010"),
        "fee": Decimal("0.357"),
        "fee_currency": "USDT",
        "ts_ms": T0,
    }


def position_values(bot: uuid.UUID, account: uuid.UUID, **overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "bot_id": bot,
        "account_id": account,
        "mode": "demo",
        "symbol": SYMBOL,
        "side": "LONG",
        "qty": Decimal("0.010"),
        "protection_status": "PROTECTED",
        "opened_at_ms": T0,
    }
    values.update(overrides)
    return values
