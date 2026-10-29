"""Запис дій користувача в `audit_log` з обробників API (ТЗ §14, §15, T0.7).

Запис додається в **сесію викликача** без коміту: небезпечна зміна (T6.2) і її підтверджений запис аудиту мають
бути в одній транзакції — тригер `ctb_require_confirmed_audit()` перевіряє запис до коміту, а відкат дії відкочує
й запис. `confirmed = True` — лише після явного підтвердження людини в UI (T6.2). `request`/`result` проходять
маскування секретів (`SecretMasker.mask`) — секрети в журнал не потрапляють (ТЗ §14).
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from typing import Any, Final

from sqlalchemy.ext.asyncio import AsyncSession

from app.api.models import AuditLog
from app.core.config import Settings
from app.core.logging import SecretMasker

# Той самий формат, що й CHECK `action_format` таблиці (`app/api/models.py`): перевірка до БД дає зрозумілу помилку.
_ACTION_RE: Final = re.compile(r"[a-z][a-z0-9_.]*")
# Хто діє через API: власник `API_TOKEN` (UI). `cli` — ручне перехоплення lock у терміналі (T0.6).
API_ACTOR: Final = "api"


class AuditWriter:
    def __init__(self, masker: SecretMasker) -> None:
        self._masker = masker

    @classmethod
    def from_settings(cls, settings: Settings) -> AuditWriter:
        """Маскер з усіма секретами `Settings` (ті самі, що реєструє логер)."""
        return cls(SecretMasker(settings.secret_values()))

    async def write(
        self,
        session: AsyncSession,
        *,
        action: str,
        actor: str = API_ACTOR,
        account_id: uuid.UUID | None = None,
        bot_id: uuid.UUID | None = None,
        target: str | None = None,
        reason: str | None = None,
        confirmed: bool = False,
        idempotency_key: str | None = None,
        request: Mapping[str, Any] | None = None,
        result: Mapping[str, Any] | None = None,
    ) -> int:
        """Додати запис у транзакцію `session` (flush, без коміту) і повернути його `id`."""
        if _ACTION_RE.fullmatch(action) is None:
            raise ValueError(f"audit action має формат {_ACTION_RE.pattern}: {action!r}")
        if not actor:
            raise ValueError("audit actor не може бути порожнім")
        entry = AuditLog(
            actor=actor,
            action=action,
            account_id=account_id,
            bot_id=bot_id,
            target=self._masked_text(target),
            reason=self._masked_text(reason),
            confirmed=confirmed,
            idempotency_key=idempotency_key,
            request=self._masked(request),
            result=self._masked(result),
        )
        session.add(entry)
        await session.flush()
        return entry.id

    def _masked_text(self, text: str | None) -> str | None:
        return None if text is None else self._masker.scrub_text(text)

    def _masked(self, payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if payload is None:
            return None
        masked = self._masker.mask(dict(payload))
        if not isinstance(masked, dict):
            raise TypeError("маскований payload аудиту має лишитися словником")
        return masked
