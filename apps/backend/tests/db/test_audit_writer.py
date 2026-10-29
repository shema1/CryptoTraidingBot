"""`AuditWriter` (T0.7): запис `audit_log` у транзакції викликача, маскування секретів, формат дії.

Підтверджені записи небезпечних дій (T6.2) мають бути видимі тригеру `ctb_require_confirmed_audit()` у тій самій
транзакції — тому writer не комітить сам.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.api.audit import AuditWriter
from app.api.models import AUDIT_ACTION_RISK_LOOSEN, AuditLog
from app.core.logging import MASK, SecretMasker
from app.risk.models import RiskConfig
from tests.db.factories import make_account

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

REGISTERED = "registered-secret-value-777"


async def _row(session: AsyncSession, audit_id: int) -> AuditLog:
    return (await session.execute(select(AuditLog).where(AuditLog.id == audit_id))).scalar_one()


async def test_write_records_all_fields(conn: AsyncConnection) -> None:
    writer = AuditWriter(SecretMasker([REGISTERED]))
    account_id, bot_id = uuid.uuid4(), uuid.uuid4()
    async with AsyncSession(bind=conn) as session:
        audit_id = await writer.write(
            session,
            action="bot.start",
            account_id=account_id,
            bot_id=bot_id,
            target="BTC/USDT:USDT",
            reason="ручний старт",
            idempotency_key="6f1c2a9e-4b7d-4c1e-9a0b-1d2e3f405162",
            request={"qty": Decimal("0.001"), "mode": "demo"},
            result={"status": "RUNNING"},
        )
        row = await _row(session, audit_id)
    assert (row.actor, row.action, row.account_id, row.bot_id, row.target) == (
        "api",
        "bot.start",
        account_id,
        bot_id,
        "BTC/USDT:USDT",
    )
    assert row.reason == "ручний старт"
    assert row.confirmed is False
    assert row.idempotency_key == "6f1c2a9e-4b7d-4c1e-9a0b-1d2e3f405162"
    assert row.request == {"qty": "0.001", "mode": "demo"}  # Decimal — рядком, без втрати точності
    assert row.result == {"status": "RUNNING"}


async def test_secrets_are_masked_in_request_and_result(conn: AsyncConnection) -> None:
    writer = AuditWriter(SecretMasker([REGISTERED]))
    async with AsyncSession(bind=conn) as session:
        audit_id = await writer.write(
            session,
            action="test.secrets",
            request={"api_key": "unregistered-key-AAA", "note": f"value {REGISTERED} here", "nested": [{"token": "t"}]},
            result={"password": "pw-BBB"},
        )
        row = await _row(session, audit_id)
    text = repr(row.request) + repr(row.result)
    for secret in ("unregistered-key-AAA", REGISTERED, "pw-BBB"):
        assert secret not in text
    assert row.request is not None
    assert row.request["api_key"] == MASK
    assert row.request["nested"] == [{"token": MASK}]


@pytest.mark.parametrize("action", ["", "Bot.Start", "1bot", "bot start", "bot-start", "bot/start"])
async def test_malformed_action_is_rejected_before_db(conn: AsyncConnection, action: str) -> None:
    writer = AuditWriter(SecretMasker([]))
    async with AsyncSession(bind=conn) as session:
        with pytest.raises(ValueError, match="action"):
            await writer.write(session, action=action)
        assert not session.new


async def test_empty_actor_is_rejected(conn: AsyncConnection) -> None:
    async with AsyncSession(bind=conn) as session:
        with pytest.raises(ValueError, match="actor"):
            await AuditWriter(SecretMasker([])).write(session, action="bot.start", actor="")


async def test_confirmed_record_authorizes_dangerous_change_in_same_transaction(conn: AsyncConnection) -> None:
    # T6.2: підтверджений запис і небезпечна зміна — в одній транзакції; тригер бачить запис до коміту.
    account_id = await make_account(conn)
    async with AsyncSession(bind=conn) as session:
        audit_id = await AuditWriter(SecretMasker([])).write(
            session, action=AUDIT_ACTION_RISK_LOOSEN, account_id=account_id, confirmed=True, reason="тест"
        )
        session.add(
            RiskConfig(
                account_id=account_id,
                version=1,
                params={"max_daily_loss_pct": "3"},
                loosening=True,
                confirmed_audit_id=audit_id,
            )
        )
        await session.flush()


async def test_reason_and_target_are_masked_and_uuid_is_plain(conn: AsyncConnection) -> None:
    # Рев'ю T0.7: секрет у reason/target не повинен потрапити в журнал; UUID — рядком, а не repr.
    writer = AuditWriter(SecretMasker([REGISTERED]))
    ref = uuid.uuid4()
    async with AsyncSession(bind=conn) as session:
        audit_id = await writer.write(
            session, action="test.text", reason=f"bad {REGISTERED}", target=REGISTERED, request={"ref": ref}
        )
        row = await _row(session, audit_id)
    assert REGISTERED not in (row.reason or "") + (row.target or "")
    assert row.request == {"ref": str(ref)}
