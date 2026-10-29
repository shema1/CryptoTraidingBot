"""Єдиний формат помилок локального API (ТЗ §16: «зрозумілі коди помилок», T0.7).

Тіло будь-якої помилки: `{"error": {"code": "<ErrorCode>", "message": "<текст українською>", "details": ...}}`;
`details` — лише якщо є що показати (напр. поля, що не пройшли валідацію). Коди — стабільні ідентифікатори для UI,
текст — для людини. Внутрішні подробиці (текст винятку, трасування) назовні не віддаються.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from enum import StrEnum
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.logging import get_logger

_log = get_logger("app.api")


class ErrorCode(StrEnum):
    HOST_NOT_ALLOWED = "HOST_NOT_ALLOWED"
    ORIGIN_NOT_ALLOWED = "ORIGIN_NOT_ALLOWED"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    AUTH_INVALID = "AUTH_INVALID"
    IDEMPOTENCY_KEY_REQUIRED = "IDEMPOTENCY_KEY_REQUIRED"
    IDEMPOTENCY_KEY_INVALID = "IDEMPOTENCY_KEY_INVALID"
    IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED"
    IDEMPOTENCY_REQUEST_IN_PROGRESS = "IDEMPOTENCY_REQUEST_IN_PROGRESS"
    IDEMPOTENCY_RESULT_UNKNOWN = "IDEMPOTENCY_RESULT_UNKNOWN"
    IDEMPOTENCY_STORE_UNAVAILABLE = "IDEMPOTENCY_STORE_UNAVAILABLE"
    REQUEST_TOO_LARGE = "REQUEST_TOO_LARGE"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    METHOD_NOT_ALLOWED = "METHOD_NOT_ALLOWED"
    HTTP_ERROR = "HTTP_ERROR"
    INSTANCE_LOCK_LOST = "INSTANCE_LOCK_LOST"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class ErrorBody(BaseModel):
    code: ErrorCode
    message: str
    details: Any | None = None


class ErrorResponse(BaseModel):
    """Схема помилки в OpenAPI (для згенерованих TS-типів UI)."""

    error: ErrorBody


class ApiError(Exception):
    """Помилка обробника або залежності з кодом; перетворюється на відповідь `install_error_handlers()`."""

    def __init__(
        self,
        status: int,
        code: ErrorCode,
        message: str,
        *,
        details: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(code.value)
        self.status = status
        self.code = code
        self.message = message
        self.details = details
        self.headers = headers


def error_content(code: ErrorCode, message: str, details: Any | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {"code": code.value, "message": message}
    if details is not None:
        body["details"] = details
    return {"error": body}


def error_json_response(
    status: int, code: ErrorCode, message: str, *, details: Any | None = None, headers: dict[str, str] | None = None
) -> JSONResponse:
    return JSONResponse(error_content(code, message, details), status_code=status, headers=headers)


async def send_error(
    send: Send,
    status: int,
    code: ErrorCode,
    message: str,
    *,
    headers: Iterable[tuple[bytes, bytes]] = (),
) -> None:
    """Відповідь-помилка напряму з ASGI-middleware (до роутера FastAPI)."""
    body = json.dumps(error_content(code, message), ensure_ascii=False).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
                *headers,
            ],
        }
    )
    await send({"type": "http.response.body", "body": body, "more_body": False})


# Тексти для стандартних HTTP-помилок роутера (404, 405 тощо).
_HTTP_CODES: dict[int, tuple[ErrorCode, str]] = {
    404: (ErrorCode.NOT_FOUND, "Ресурс не знайдено"),
    405: (ErrorCode.METHOD_NOT_ALLOWED, "Метод не дозволений для цього ресурсу"),
}

INTERNAL_ERROR_MESSAGE = "Внутрішня помилка сервера"


def install_error_handlers(app: FastAPI) -> None:
    """Усі помилки FastAPI/Starlette — у єдиному форматі `ErrorResponse`."""

    async def on_api_error(request: Request, exc: Exception) -> JSONResponse:
        if not isinstance(exc, ApiError):
            raise TypeError(type(exc).__name__)
        return error_json_response(exc.status, exc.code, exc.message, details=exc.details, headers=exc.headers)

    async def on_http_error(request: Request, exc: Exception) -> JSONResponse:
        if not isinstance(exc, StarletteHTTPException):
            raise TypeError(type(exc).__name__)
        code, message = _HTTP_CODES.get(exc.status_code, (ErrorCode.HTTP_ERROR, "Помилка запиту"))
        headers = dict(exc.headers) if exc.headers else None
        return error_json_response(exc.status_code, code, message, headers=headers)

    async def on_validation_error(request: Request, exc: Exception) -> JSONResponse:
        if not isinstance(exc, RequestValidationError):
            raise TypeError(type(exc).__name__)
        # Лише місце й тип помилки — без введених значень (`input`) і контексту pydantic.
        details = [{"loc": list(e.get("loc", ())), "type": e.get("type"), "msg": e.get("msg")} for e in exc.errors()]
        return error_json_response(422, ErrorCode.VALIDATION_ERROR, "Некоректні параметри запиту", details=details)

    async def on_unhandled(request: Request, exc: Exception) -> JSONResponse:
        # Запасний шлях для збою в зовнішніх middleware (основний — `CatchUnhandled`). Трасування — у маскований лог;
        # клієнт бачить лише код. ServerErrorMiddleware далі перекидає виняток серверу.
        _log.error(
            "api_unhandled_exception",
            method=request.method,
            path=request.url.path,
            error_type=type(exc).__name__,
            exc_info=exc,
        )
        return error_json_response(500, ErrorCode.INTERNAL_ERROR, INTERNAL_ERROR_MESSAGE)

    app.add_exception_handler(ApiError, on_api_error)
    app.add_exception_handler(StarletteHTTPException, on_http_error)
    app.add_exception_handler(RequestValidationError, on_validation_error)
    app.add_exception_handler(Exception, on_unhandled)


class CatchUnhandled:
    """Необроблений виняток обробника → 500 `INTERNAL_ERROR` **усередині** CORS і `LocalOriginGuard`.

    Без цього 500 формував би зовнішній `ServerErrorMiddleware`: відповідь без CORS-заголовків браузер показав би UI як
    мережеву помилку («запит не дійшов»), і UI не відрізнив би її від «результат невідомий» (ключ `FAILED`).
    Виняток не перекидається далі: трасування вже записане маскованим логом, а повторний запис uvicorn не потрібен.
    """

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        started = False

        async def tracking_send(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self._app(scope, receive, tracking_send)
        except Exception as exc:  # noqa: BLE001 — записується маскованим логом із трасуванням, клієнту — 500
            _log.error(
                "api_unhandled_exception",
                method=scope.get("method"),
                path=scope.get("path"),
                error_type=type(exc).__name__,
                response_started=started,
                exc_info=exc,
            )
            if not started:
                await send_error(send, 500, ErrorCode.INTERNAL_ERROR, INTERNAL_ERROR_MESSAGE)
