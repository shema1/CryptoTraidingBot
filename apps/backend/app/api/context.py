"""Залежності обробників API (T0.7): контекст процесу, lock торгового процесу, ping БД."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

from fastapi import Request

from app.api.audit import AuditWriter
from app.api.errors import ApiError, ErrorCode
from app.api.idempotency import IdempotencyStore
from app.core.clock import monotonic_ms, now_ms
from app.core.config import Settings
from app.core.instance_lock import InstanceLockLost

# Перевірка БД для health: `SELECT 1`; помилка або виняток — БД недоступна.
DbPing = Callable[[], Awaitable[None]]


class LockState(Protocol):
    """Стан single-instance lock (`app.core.instance_lock.InstanceLock`)."""

    @property
    def held(self) -> bool: ...

    def ensure_held(self) -> None: ...


@dataclass(frozen=True, slots=True, kw_only=True)
class ApiContext:
    """Усе, що API отримує від процесу. Створює `serve_api()` (`app/api/server.py`), у тестах — фейки."""

    settings: Settings
    lock: LockState
    db_ping: DbPing
    idempotency: IdempotencyStore
    audit: AuditWriter
    # Власник записів `api_idempotency_keys`: новий на кожен запуск процесу.
    instance_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_ms: int = field(default_factory=now_ms)
    started_monotonic_ms: int = field(default_factory=monotonic_ms)


def context(request: Request) -> ApiContext:
    ctx = request.app.state.ctx
    if not isinstance(ctx, ApiContext):
        raise TypeError("app.state.ctx не налаштовано (create_app)")
    return ctx


def require_instance_lock(request: Request) -> None:
    """Залежність для ендпоінтів, що збільшують експозицію (T6.1/T6.2): обробники запитів працюють поза
    `TaskGroup` роботи під lock (контракт `run_exclusive`, T0.6), тож перевіряють lock самі перед дією.
    """
    try:
        context(request).lock.ensure_held()
    except InstanceLockLost as exc:
        raise ApiError(
            503, ErrorCode.INSTANCE_LOCK_LOST, "Торговий процес втратив single-instance lock і завершується"
        ) from exc
