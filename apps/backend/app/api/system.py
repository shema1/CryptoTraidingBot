"""Група `/system` (ТЗ §16). T0.7 — лише `GET /system/health`; emergency-stop, reconcile, ключі — T6.1/T6.2."""

from __future__ import annotations

import asyncio
from typing import Final, Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.api.context import DbPing, context
from app.api.errors import ErrorResponse
from app.core.clock import monotonic_ms, now_ms
from app.core.logging import get_logger

_log = get_logger("app.api")

# Скільки чекати `SELECT 1`, перш ніж вважати БД недоступною (health не повинен висіти).
HEALTH_DB_TIMEOUT_MS: Final = 2000

router = APIRouter(prefix="/system", tags=["system"])


class HealthChecks(BaseModel):
    # Торговий процес досі тримає single-instance lock (ТЗ §6.4); `false` — процес завершується.
    instance_lock: bool
    # `SELECT 1` у БД пройшов за HEALTH_DB_TIMEOUT_MS.
    database: bool


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    # Поточний час сервера, мс UTC.
    time_ms: int
    # Скільки працює API, мс (монотонний годинник).
    uptime_ms: int
    checks: HealthChecks


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Стан торгового процесу",
    responses={
        503: {"model": HealthResponse, "description": "Деградація: lock втрачено або БД недоступна"},
        401: {"model": ErrorResponse, "description": "Немає або неправильний токен"},
        403: {"model": ErrorResponse, "description": "Недозволений Host або Origin"},
    },
)
async def health(request: Request) -> JSONResponse:
    ctx = context(request)
    checks = HealthChecks(instance_lock=ctx.lock.held, database=await _database_ok(ctx.db_ping))
    healthy = checks.instance_lock and checks.database
    body = HealthResponse(
        status="ok" if healthy else "degraded",
        time_ms=now_ms(),
        uptime_ms=monotonic_ms() - ctx.started_monotonic_ms,
        checks=checks,
    )
    if not healthy:
        _log.warning("api_health_degraded", instance_lock=checks.instance_lock, database=checks.database)
    return JSONResponse(body.model_dump(mode="json"), status_code=200 if healthy else 503)


async def _database_ok(ping: DbPing) -> bool:
    try:
        async with asyncio.timeout(HEALTH_DB_TIMEOUT_MS / 1000):
            await ping()
    except Exception as exc:  # noqa: BLE001 — health лише звітує: текст помилки (DSN, хост) назовні не віддається
        _log.warning("api_health_db_failed", error_type=type(exc).__name__)
        return False
    return True
