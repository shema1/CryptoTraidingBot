"""Захист локального API від сторінок у браузері (ТЗ §14, T0.7).

Будь-яка сторінка в браузері може надсилати запити на `127.0.0.1`, тож «локальність» сама не захищає:
- **`LocalOriginGuard`** — `Host` лише `127.0.0.1:<порт>` / `localhost:<порт>` (DNS rebinding: домен атакувальника,
  що резолвиться в 127.0.0.1, приходить зі своїм `Host`); `Origin`, якщо є, — точно з allowlist (CSRF, CSWSH);
  крос-сайтовий запит браузера без `Origin` (`Sec-Fetch-Site: cross-site|same-site`, напр. `<img src>`) — теж 403.
  Усі відповіді — `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`.
- **`BearerAuth`** — `Authorization: Bearer <API_TOKEN>`; браузер не додає цей заголовок сам (на відміну від кук),
  тож чужа сторінка не може «позичити» автентифікацію. Без токена проходить лише CORS-preflight.

Чисті ASGI-middleware (не `BaseHTTPMiddleware`): працюють і для WebSocket, не буферизують відповідь.
Порядок у застосунку — `app/api/app.py`. Значення заголовків авторизації ніколи не логуються.
"""

from __future__ import annotations

import hashlib
import hmac
from collections import Counter
from collections.abc import Collection
from typing import Final

from pydantic import SecretStr
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.api.errors import ErrorCode, send_error
from app.core.clock import monotonic_ms
from app.core.logging import get_logger

_log = get_logger("app.api")

# Значення `Sec-Fetch-Site`, з якими запит без `Origin` не крос-сайтовий: той самий origin або URL, введений людиною.
SAFE_FETCH_SITES: Final = frozenset({"same-origin", "none"})
SECURITY_HEADERS: Final[tuple[tuple[bytes, bytes], ...]] = (
    (b"cache-control", b"no-store"),
    (b"x-content-type-options", b"nosniff"),
)
_SECURITY_HEADER_NAMES: Final = frozenset(name for name, _ in SECURITY_HEADERS)
# Код закриття WebSocket «порушення політики» (RFC 6455); до accept сервер відповідає HTTP 403.
WS_POLICY_VIOLATION: Final = 1008
# Скільки символів чужих заголовків (`Host`, `Origin`) писати в лог.
_MAX_LOGGED_CHARS: Final = 200

_MESSAGES: Final[dict[ErrorCode, str]] = {
    ErrorCode.HOST_NOT_ALLOWED: "Запити до API дозволені лише на 127.0.0.1 або localhost з портом API",
    ErrorCode.ORIGIN_NOT_ALLOWED: "Origin не дозволений для локального API (API_ALLOWED_ORIGINS)",
    ErrorCode.AUTH_REQUIRED: "Потрібен заголовок Authorization: Bearer <API_TOKEN>",
    ErrorCode.AUTH_INVALID: "Неправильний токен API",
}


def allowed_hosts(port: int) -> frozenset[str]:
    """Дозволені значення `Host` (у нижньому регістрі): API слухає лише 127.0.0.1 (`API_BIND_HOST`)."""
    return frozenset({f"127.0.0.1:{port}", f"localhost:{port}"})


def header_values(scope: Scope, name: bytes) -> list[bytes]:
    """Усі значення заголовка (назви в ASGI — у нижньому регістрі)."""
    return [value for key, value in scope.get("headers", ()) if key == name]


def _clip(values: list[bytes]) -> list[str]:
    return [v.decode("latin-1")[:_MAX_LOGGED_CHARS] for v in values]


# Логи відмов: будь-яка сторінка в браузері може слати запити на 127.0.0.1 у циклі; кожен WARNING — синхронний запис у
# файл у циклі подій торгового процесу. Тому не більше REJECTION_LOG_BURST записів за вікно, решта — лічильником.
REJECTION_LOG_WINDOW_MS: Final = 60_000
REJECTION_LOG_BURST: Final = 20


class RejectionLog:
    """Обмежений лог відмов (`api_request_rejected`): перші `REJECTION_LOG_BURST` за вікно — повністю, решта
    рахується за кодом причини й підсумовується одним `api_request_rejected_suppressed` на першій відмові нового вікна.
    Один екземпляр на застосунок (спільний для `LocalOriginGuard` і `BearerAuth`)."""

    def __init__(self, *, window_ms: int = REJECTION_LOG_WINDOW_MS, burst: int = REJECTION_LOG_BURST) -> None:
        self._window_ms = window_ms
        self._burst = burst
        self._window_start_ms: int | None = None
        self._logged = 0
        self._suppressed: Counter[str] = Counter()

    def tick(self) -> None:
        """Закрити вікно, що минуло, і записати підсумок придушених відмов. Викликається на кожному запиті (зокрема
        дозволеному), тож підсумок не чекає наступної відмови, якщо флуд припинився, а UI далі опитує API."""
        now = monotonic_ms()
        if self._window_start_ms is not None and now - self._window_start_ms < self._window_ms:
            return
        if self._suppressed:
            _log.warning("api_request_rejected_suppressed", counts=dict(self._suppressed), window_ms=self._window_ms)
        self._window_start_ms = now
        self._logged = 0
        self._suppressed.clear()

    def record(self, scope: Scope, reason: ErrorCode, **details: object) -> None:
        self.tick()
        if self._logged >= self._burst:
            self._suppressed[reason.value] += 1
            return
        self._logged += 1
        client = scope.get("client") or ("?", 0)
        _log.warning(
            "api_request_rejected",
            reason=reason.value,
            scope_type=scope["type"],
            method=scope.get("method", "WEBSOCKET"),
            path=str(scope.get("path", ""))[:_MAX_LOGGED_CHARS],
            client=client[0],
            **details,
        )


async def _reject(scope: Scope, receive: Receive, send: Send, status: int, reason: ErrorCode) -> None:
    if scope["type"] == "websocket":
        # Закрити до accept: сервер відповідає на handshake 403, обробник не викликається.
        await receive()
        await send({"type": "websocket.close", "code": WS_POLICY_VIOLATION})
        return
    # `Connection: close`: відхилений клієнт не тримає keep-alive сокет (чужа сторінка не накопичує з'єднань).
    extra = [(b"connection", b"close")]
    if status == 401:
        extra.append((b"www-authenticate", b"Bearer"))
    await send_error(send, status, reason, _MESSAGES[reason], headers=extra)


def _with_security_headers(send: Send, scope: Scope) -> Send:
    started_ms = monotonic_ms()
    status: list[int] = []

    async def wrapped(message: Message) -> None:
        if message["type"] == "http.response.start":
            status.append(message["status"])
            headers = [(k, v) for k, v in message.get("headers", ()) if k.lower() not in _SECURITY_HEADER_NAMES]
            message = {**message, "headers": [*headers, *SECURITY_HEADERS]}
        await send(message)
        if message["type"] == "http.response.body" and not message.get("more_body", False):
            _log.debug(
                "api_request",
                method=scope["method"],
                path=scope["path"],
                status=status[0] if status else None,
                duration_ms=monotonic_ms() - started_ms,
            )

    return wrapped


class LocalOriginGuard:
    """`Host` і `Origin` (DNS rebinding, CSRF, CSWSH) → 403 / закриття WebSocket 1008. Зовнішній шар захисту."""

    def __init__(
        self, app: ASGIApp, *, port: int, allowed_origins: Collection[str], rejections: RejectionLog | None = None
    ) -> None:
        self._app = app
        self._hosts = allowed_hosts(port)
        self._origins = frozenset(allowed_origins)
        self._rejections = rejections or RejectionLog()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self._app(scope, receive, send)
            return
        self._rejections.tick()
        if scope["type"] == "http":
            send = _with_security_headers(send, scope)
        reason = self._check_host(scope) or self._check_origin(scope)
        if reason is not None:
            self._rejections.record(
                scope,
                reason,
                host=_clip(header_values(scope, b"host")),
                origin=_clip(header_values(scope, b"origin")),
                sec_fetch_site=_clip(header_values(scope, b"sec-fetch-site")),
            )
            await _reject(scope, receive, send, 403, reason)
            return
        await self._app(scope, receive, send)

    def _check_host(self, scope: Scope) -> ErrorCode | None:
        hosts = header_values(scope, b"host")
        if len(hosts) != 1 or hosts[0].decode("latin-1").lower() not in self._hosts:
            return ErrorCode.HOST_NOT_ALLOWED
        return None

    def _check_origin(self, scope: Scope) -> ErrorCode | None:
        origins = header_values(scope, b"origin")
        if origins:
            # Точний збіг: браузер надсилає origin у канонічному вигляді (нижній регістр, без шляху).
            if len(origins) != 1 or origins[0].decode("latin-1") not in self._origins:
                return ErrorCode.ORIGIN_NOT_ALLOWED
            return None
        sites = header_values(scope, b"sec-fetch-site")
        if any(site.decode("latin-1").strip().lower() not in SAFE_FETCH_SITES for site in sites):
            return ErrorCode.ORIGIN_NOT_ALLOWED
        return None


def is_cors_preflight(scope: Scope) -> bool:
    """CORS-preflight: OPTIONS з `Origin` і `Access-Control-Request-Method` (так його розпізнає `CORSMiddleware`)."""
    return (
        scope["type"] == "http"
        and scope["method"] == "OPTIONS"
        and bool(header_values(scope, b"origin"))
        and bool(header_values(scope, b"access-control-request-method"))
    )


class BearerAuth:
    """`Authorization: Bearer <API_TOKEN>` → інакше 401 (`WWW-Authenticate: Bearer`) / закриття WebSocket 1008.

    Порівняння — `hmac.compare_digest` SHA-256 дайджестів (стала тривалість, незалежна і від довжини). Токен у query
    не приймається (потрапляв би в історію браузера й логи). Без токена проходить лише CORS-preflight — його
    `Origin` уже перевірив `LocalOriginGuard`.
    """

    def __init__(self, app: ASGIApp, *, token: SecretStr, rejections: RejectionLog | None = None) -> None:
        self._app = app
        self._expected = hashlib.sha256(token.get_secret_value().encode()).digest()
        self._rejections = rejections or RejectionLog()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket") or is_cors_preflight(scope):
            await self._app(scope, receive, send)
            return
        reason = self._check(scope)
        if reason is not None:
            self._rejections.record(scope, reason)
            await _reject(scope, receive, send, 401, reason)
            return
        await self._app(scope, receive, send)

    def _check(self, scope: Scope) -> ErrorCode | None:
        values = header_values(scope, b"authorization")
        if not values:
            return ErrorCode.AUTH_REQUIRED
        if len(values) != 1:
            return ErrorCode.AUTH_INVALID
        parts = values[0].split(b" ")
        if len(parts) != 2 or parts[0].lower() != b"bearer" or not parts[1]:
            return ErrorCode.AUTH_INVALID
        if not hmac.compare_digest(hashlib.sha256(parts[1]).digest(), self._expected):
            return ErrorCode.AUTH_INVALID
        return None
