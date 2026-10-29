"""Застосунок FastAPI локального API (ТЗ §14, §16, T0.7).

Порядок middleware (зовні → всередину; `add_middleware` додає зовнішній шар, тому виклики нижче — у зворотному
порядку):

1. `ServerErrorMiddleware` (Starlette, завжди зовнішній) — запасний 500 для збою самих зовнішніх middleware;
2. `LocalOriginGuard` — `Host` / `Origin` → 403, заголовки `no-store` / `nosniff`;
3. `CORSMiddleware` — суворий CORS лише для `API_ALLOWED_ORIGINS`, без credentials; відповідає на preflight;
4. `CatchUnhandled` — необроблений виняток обробника → 500 `INTERNAL_ERROR` з CORS і `no-store` (UI бачить код);
5. `BearerAuth` — токен → 401 (усередині CORS, щоб UI міг прочитати 401);
6. `IdempotencyMiddleware` — `Idempotency-Key` для змінюючих запитів (після автентифікації: чужий запит не займе ключ);
7. роутер і обробники помилок (`app/api/errors.py`).

Swagger UI і ReDoc вимкнені (вантажать JS з CDN в origin API); `/openapi.json` — лише з токеном. Схему для
TS-типів UI (T6.4) можна отримати офлайн: `create_app(ctx).openapi()`.
"""

from __future__ import annotations

from typing import Any, Final

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import system
from app.api.context import ApiContext
from app.api.errors import CatchUnhandled, install_error_handlers
from app.api.idempotency import (
    IDEMPOTENCY_HEADER,
    KEY_MAX_LEN,
    KEY_MIN_LEN,
    MUTATING_METHODS,
    REPLAYED_HEADER,
    IdempotencyMiddleware,
)
from app.api.models import IDEMPOTENCY_KEY_RE
from app.api.security import BearerAuth, LocalOriginGuard, RejectionLog

API_TITLE: Final = "Crypto Trading Bot — локальний API"
CORS_METHODS: Final = ("GET", "POST", "PUT", "PATCH", "DELETE")
CORS_HEADERS: Final = ("Authorization", "Content-Type", IDEMPOTENCY_HEADER)
CORS_MAX_AGE_S: Final = 600

_ERROR_REF: Final = {"application/json": {"schema": {"$ref": "#/components/schemas/ErrorResponse"}}}
_COMMON_ERRORS: Final = {
    "401": "Немає або неправильний токен (AUTH_REQUIRED / AUTH_INVALID)",
    "403": "Недозволений Host або Origin (HOST_NOT_ALLOWED / ORIGIN_NOT_ALLOWED)",
}
_MUTATION_ERRORS: Final = {
    "400": "Немає або некоректний Idempotency-Key",
    "409": "Запит з цим ключем ще виконується або його результат невідомий",
    "413": "Тіло запиту завелике",
    "422": "Некоректні параметри (VALIDATION_ERROR) або ключ використано для іншого запиту",
    "503": "Сховище ключів недоступне — дію не виконано",
}


class LocalApi(FastAPI):
    """FastAPI з OpenAPI, що описує реальний захист: bearer-токен і обов'язковий `Idempotency-Key`."""

    def openapi(self) -> dict[str, Any]:
        built = self.openapi_schema is not None
        schema = super().openapi()
        if not built:
            _document_security(schema)
        return schema


def create_app(ctx: ApiContext) -> FastAPI:
    token = ctx.settings.api_token
    if token is None:
        raise ValueError("API_TOKEN не задано: локальний API без токена не запускається")
    app = LocalApi(
        title=API_TITLE,
        version="0.0.0",
        docs_url=None,
        redoc_url=None,
        swagger_ui_oauth2_redirect_url=None,
    )
    app.state.ctx = ctx
    install_error_handlers(app)
    app.include_router(system.router)

    app.add_middleware(IdempotencyMiddleware, store=ctx.idempotency, owner=ctx.instance_id)
    rejections = RejectionLog()
    app.add_middleware(BearerAuth, token=token, rejections=rejections)
    app.add_middleware(CatchUnhandled)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(ctx.settings.api_allowed_origins),
        allow_methods=list(CORS_METHODS),
        allow_headers=list(CORS_HEADERS),
        expose_headers=[REPLAYED_HEADER],
        allow_credentials=False,
        allow_private_network=False,
        max_age=CORS_MAX_AGE_S,
    )
    app.add_middleware(
        LocalOriginGuard,
        port=ctx.settings.api_port,
        allowed_origins=ctx.settings.api_allowed_origins,
        rejections=rejections,
    )
    return app


def _document_security(schema: dict[str, Any]) -> None:
    components = schema.setdefault("components", {})
    components.setdefault("securitySchemes", {})["bearerAuth"] = {"type": "http", "scheme": "bearer"}
    schema["security"] = [{"bearerAuth": []}]
    for operations in schema.get("paths", {}).values():
        for method, operation in operations.items():
            responses = operation.setdefault("responses", {})
            errors = dict(_COMMON_ERRORS)
            if method.upper() in MUTATING_METHODS:
                errors.update(_MUTATION_ERRORS)
                _add_idempotency_header(operation)
            for status, description in errors.items():
                if status not in responses or status == "422":
                    # 422 FastAPI описує своєю схемою HTTPValidationError, а реальне тіло — ErrorResponse.
                    responses[status] = {"description": description, "content": _ERROR_REF}
    schemas = components.get("schemas", {})
    for unused in ("HTTPValidationError", "ValidationError"):
        schemas.pop(unused, None)


def _add_idempotency_header(operation: dict[str, Any]) -> None:
    parameters = operation.setdefault("parameters", [])
    if any(p.get("in") == "header" and str(p.get("name", "")).lower() == "idempotency-key" for p in parameters):
        return
    parameters.append(
        {
            "name": IDEMPOTENCY_HEADER,
            "in": "header",
            "required": True,
            "description": "Унікальний ключ дії (UUID); повтор з тим самим ключем не виконує дію вдруге",
            "schema": {
                "type": "string",
                "minLength": KEY_MIN_LEN,
                "maxLength": KEY_MAX_LEN,
                "pattern": IDEMPOTENCY_KEY_RE,
            },
        }
    )
