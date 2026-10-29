"""`Idempotency-Key` для змінюючих запитів API (ТЗ §16, T0.7, рішення `docs/decisions/0007-local-api.md`).

Кожен `POST`/`PUT`/`PATCH`/`DELETE` мусить мати заголовок `Idempotency-Key` (16–128 символів `[A-Za-z0-9_.:-]`,
UI ставить UUID на кожну дію людини). Запис ключа (`IN_PROGRESS`) комітиться в БД **до** виконання обробника
(write-ahead, як `SUBMITTING` для ордерів), тож дія не виконується двічі навіть після рестарту процесу:

| Наявний запис з цим ключем | Відповідь |
| --- | --- |
| немає | обробник виконується; відповідь зберігається (`COMPLETED`) |
| інший метод / шлях / query / тіло | 422 `IDEMPOTENCY_KEY_REUSED` |
| `COMPLETED` | збережена відповідь (статус, заголовки, тіло) + `Idempotency-Replayed: true` |
| `IN_PROGRESS`, обробник якого виконується зараз у цьому процесі | 409 `IDEMPOTENCY_REQUEST_IN_PROGRESS` |
| `FAILED` або будь-який інший `IN_PROGRESS` (упалий процес, не записаний підсумок) | 409 `IDEMPOTENCY_RESULT_UNKNOWN` |

Невідомий результат **ніколи** не виконується повторно з тим самим ключем: людина перевіряє стан і робить нову
дію (новий ключ). Fail-closed: сховище недоступне до виконання → 503 без виконання; результат не вдалося
записати → клієнт отримує відповідь, а повтор — 409.
"""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final, Protocol

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.api.errors import ErrorCode, send_error
from app.api.models import (
    IDEMPOTENCY_KEY_MAX_LEN,
    IDEMPOTENCY_KEY_MIN_LEN,
    IDEMPOTENCY_KEY_RE,
    IDEMPOTENCY_METHODS,
    ApiIdempotencyKey,
)
from app.api.security import header_values
from app.core.clock import now_ms
from app.core.logging import get_logger

_log = get_logger("app.api")

IDEMPOTENCY_HEADER: Final = "Idempotency-Key"
REPLAYED_HEADER: Final = "Idempotency-Replayed"
MUTATING_METHODS: Final = frozenset(IDEMPOTENCY_METHODS)
# Без ключа — лише безпечні методи; будь-який інший (`TRACE`, `post`, …) не доходить до обробника.
SAFE_METHODS: Final = frozenset({"GET", "HEAD", "OPTIONS"})
KEY_MIN_LEN: Final = IDEMPOTENCY_KEY_MIN_LEN
KEY_MAX_LEN: Final = IDEMPOTENCY_KEY_MAX_LEN
_KEY_RE: Final = re.compile(IDEMPOTENCY_KEY_RE)
# Межі тіла запиту (читається повністю для хешу) і відповіді, що зберігається для повтору.
MAX_REQUEST_BODY_BYTES: Final = 1024 * 1024
MAX_STORED_RESPONSE_BYTES: Final = 1024 * 1024
_MAX_STORED_PATH_CHARS: Final = 1024

_KEY_REQUIRED_MESSAGE: Final = "Змінюючий запит потребує заголовка Idempotency-Key (UUID на кожну дію)"
_KEY_INVALID_MESSAGE: Final = "Idempotency-Key: один заголовок, 16–128 символів [A-Za-z0-9_.:-]"


class IdempotencyStatus(StrEnum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class StoredResponse:
    status: int
    # Пари (назва, значення) у порядку відповіді, без `content-length`.
    headers: tuple[tuple[str, str], ...]
    body: bytes


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    key: str
    method: str
    path: str
    request_hash: str
    owner: str
    status: IdempotencyStatus
    created_ms: int
    response: StoredResponse | None


class IdempotencyStore(Protocol):
    async def begin(self, record: IdempotencyRecord) -> IdempotencyRecord | None:
        """Вставити й закомітити запис `IN_PROGRESS`. `None` — вставлено (виконує цей запит); інакше — наявний запис."""
        ...

    async def complete(self, key: str, owner: str, response: StoredResponse, *, now_ms: int) -> bool:
        """`IN_PROGRESS` цього власника → `COMPLETED` з відповіддю. `False` — запис не в цьому стані."""
        ...

    async def fail(self, key: str, owner: str, *, now_ms: int) -> bool:
        """`IN_PROGRESS` цього власника → `FAILED` (результат невідомий)."""
        ...


def request_fingerprint(method: str, path: str, query: bytes, body: bytes) -> str:
    """SHA-256 (hex) методу, шляху, query і тіла; кожне поле з префіксом довжини — межі полів не зсуваються."""
    digest = hashlib.sha256()
    for part in (method.encode(), path.encode("utf-8", "surrogatepass"), query, body):
        digest.update(len(part).to_bytes(8, "big"))
        digest.update(part)
    return digest.hexdigest()


class _RequestTooLarge(Exception):
    pass


class _ClientDisconnected(Exception):
    pass


async def _read_body(receive: Receive, limit: int) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            raise _ClientDisconnected
        if message["type"] != "http.request":
            continue
        chunk: bytes = message.get("body", b"")
        size += len(chunk)
        if size > limit:
            raise _RequestTooLarge
        chunks.append(chunk)
        if not message.get("more_body", False):
            return b"".join(chunks)


def _replay_body(body: bytes, receive: Receive) -> Receive:
    """Тіло вже прочитане для хешу: віддати його обробнику одним повідомленням, далі — справжній `receive`."""
    delivered = False

    async def wrapped() -> Message:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": body, "more_body": False}
        return await receive()

    return wrapped


class _ResponseCapture:
    """Пропускає відповідь клієнту без змін і паралельно збирає копію (до `limit` байтів тіла)."""

    def __init__(self, send: Send, limit: int) -> None:
        self._send = send
        self._limit = limit
        self._status: int | None = None
        self._headers: tuple[tuple[str, str], ...] = ()
        self._chunks: list[bytes] = []
        self._size = 0
        self._complete = False

    async def send(self, message: Message) -> None:
        if message["type"] == "http.response.start":
            self._status = int(message["status"])
            self._headers = tuple(
                (name.decode("latin-1"), value.decode("latin-1"))
                for name, value in message.get("headers", ())
                if name.lower() != b"content-length"
            )
        elif message["type"] == "http.response.body":
            chunk: bytes = message.get("body", b"")
            self._size += len(chunk)
            if self._size <= self._limit:
                self._chunks.append(chunk)
            if not message.get("more_body", False):
                self._complete = True
        await self._send(message)

    def problem(self) -> str | None:
        if self._status is None or not self._complete:
            return "response_incomplete"
        if self._size > self._limit:
            return "response_too_large"
        return None

    def response(self) -> StoredResponse:
        if self._status is None:
            raise RuntimeError("відповідь не почалася")
        return StoredResponse(status=self._status, headers=self._headers, body=b"".join(self._chunks))


async def _send_stored(send: Send, response: StoredResponse) -> None:
    headers = [(name.encode("latin-1"), value.encode("latin-1")) for name, value in response.headers]
    headers += [(b"content-length", str(len(response.body)).encode()), (b"idempotency-replayed", b"true")]
    await send({"type": "http.response.start", "status": response.status, "headers": headers})
    await send({"type": "http.response.body", "body": response.body, "more_body": False})


class IdempotencyMiddleware:
    """Обов'язковий `Idempotency-Key` і повтор збереженої відповіді (таблиця в docstring модуля).

    Стоїть після `BearerAuth`: неавтентифікований запит не може зайняти ключ. Ключ доступний обробнику як
    `request.state.idempotency_key` (для `audit_log`). Скасування обробника (зупинка процесу) лишає запис
    `IN_PROGRESS` — після рестарту він «невідомий» (інший `owner`).
    """

    def __init__(self, app: ASGIApp, *, store: IdempotencyStore, owner: str) -> None:
        self._app = app
        self._store = store
        self._owner = owner
        # Ключі, обробник яких виконується зараз у цьому процесі. `IN_PROGRESS` цього `owner` поза множиною — підсумок
        # не записано (збій БД) або клієнт не дочекався коміту `begin`: результат невідомий, а не «ще виконується».
        self._executing: set[str] = set()
        # Скільки запитів з ключем зараз усередині `begin()`: переможець уже закомітив запис, але ще не повернувся
        # (прибирання сесії, пул) — його паралельний повтор має отримати «ще виконується», а не «невідомо».
        self._beginning: Counter[str] = Counter()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] in SAFE_METHODS:
            await self._app(scope, receive, send)
            return
        method: str = scope["method"]
        if method not in MUTATING_METHODS:
            _log.warning("api_method_not_allowed", method=method[:16], path=str(scope.get("path", ""))[:200])
            await send_error(send, 405, ErrorCode.METHOD_NOT_ALLOWED, "Метод не дозволений")
            return
        path: str = scope["path"]
        log = _log.bind(method=method, path=path[:_MAX_STORED_PATH_CHARS])

        keys = header_values(scope, b"idempotency-key")
        if not keys:
            log.warning("idempotency_key_missing")
            await send_error(send, 400, ErrorCode.IDEMPOTENCY_KEY_REQUIRED, _KEY_REQUIRED_MESSAGE)
            return
        key = keys[0].decode("latin-1")
        if len(keys) != 1 or _KEY_RE.fullmatch(key) is None:
            log.warning("idempotency_key_invalid", headers=len(keys))
            await send_error(send, 400, ErrorCode.IDEMPOTENCY_KEY_INVALID, _KEY_INVALID_MESSAGE)
            return
        log = log.bind(idempotency_key=key)

        try:
            _check_declared_length(scope)
            body = await _read_body(receive, MAX_REQUEST_BODY_BYTES)
        except _RequestTooLarge:
            log.warning("idempotency_request_too_large", limit_bytes=MAX_REQUEST_BODY_BYTES)
            await send_error(
                send, 413, ErrorCode.REQUEST_TOO_LARGE, f"Тіло запиту більше за {MAX_REQUEST_BODY_BYTES} байтів"
            )
            return
        except _ClientDisconnected:
            log.info("idempotency_client_disconnected")
            return

        record = IdempotencyRecord(
            key=key,
            method=method,
            path=path[:_MAX_STORED_PATH_CHARS].replace("\x00", "�"),
            request_hash=request_fingerprint(method, path, scope.get("query_string", b""), body),
            owner=self._owner,
            status=IdempotencyStatus.IN_PROGRESS,
            created_ms=now_ms(),
            response=None,
        )
        self._beginning[key] += 1
        try:
            existing = await self._store.begin(record)
        except Exception as exc:  # fail-closed: без запису ключа дія не виконується
            log.error("idempotency_store_unavailable", error_type=type(exc).__name__, exc_info=exc)
            await send_error(
                send, 503, ErrorCode.IDEMPOTENCY_STORE_UNAVAILABLE, "Сховище ключів недоступне; дію не виконано"
            )
            return
        finally:
            # Без await між цим і `_executing.add` нижче: переможець не випадає з обох множин.
            self._beginning[key] -= 1
            if self._beginning[key] <= 0:
                del self._beginning[key]
        if existing is not None:
            await self._answer_existing(existing, record, send, log)
            return

        self._executing.add(key)
        try:
            await self._execute(scope, receive, send, key, body, log)
        finally:
            self._executing.discard(key)

    async def _execute(self, scope: Scope, receive: Receive, send: Send, key: str, body: bytes, log: Any) -> None:
        scope.setdefault("state", {})["idempotency_key"] = key
        capture = _ResponseCapture(send, MAX_STORED_RESPONSE_BYTES)
        try:
            await self._app(scope, _replay_body(body, receive), capture.send)
        except Exception:
            await self._finish(log, "handler_failed", lambda: self._store.fail(key, self._owner, now_ms=now_ms()))
            raise
        problem = capture.problem()
        if problem is not None:
            log.error("idempotency_response_not_storable", reason=problem, limit_bytes=MAX_STORED_RESPONSE_BYTES)
            await self._finish(log, problem, lambda: self._store.fail(key, self._owner, now_ms=now_ms()))
            return
        response = capture.response()
        await self._finish(log, "completed", lambda: self._store.complete(key, self._owner, response, now_ms=now_ms()))

    async def _finish(self, log: Any, outcome: str, action: Callable[[], Awaitable[bool]]) -> None:
        """Записати підсумок; збій лише логується (запис лишається `IN_PROGRESS` → повтор отримає 409)."""
        try:
            changed = await action()
        except Exception as exc:  # відповідь уже віддана/виняток іде далі; повтор буде fail-closed
            log.error("idempotency_result_not_stored", outcome=outcome, error_type=type(exc).__name__, exc_info=exc)
            return
        if not changed:
            log.error("idempotency_result_not_stored", outcome=outcome, reason="record_not_in_progress")

    async def _answer_existing(
        self, existing: IdempotencyRecord, record: IdempotencyRecord, send: Send, log: Any
    ) -> None:
        log = log.bind(stored_status=existing.status.value)
        if existing.request_hash != record.request_hash:
            log.warning("idempotency_key_reused", stored_method=existing.method, stored_path=existing.path)
            await send_error(
                send,
                422,
                ErrorCode.IDEMPOTENCY_KEY_REUSED,
                "Idempotency-Key уже використано для іншого запиту; нова дія потребує нового ключа",
            )
        elif existing.status is IdempotencyStatus.COMPLETED and existing.response is not None:
            log.info("idempotency_replayed", response_status=existing.response.status)
            await _send_stored(send, existing.response)
        elif (
            existing.status is IdempotencyStatus.IN_PROGRESS
            and existing.owner == self._owner
            and (existing.key in self._executing or self._beginning[existing.key] > 0)
        ):
            log.warning("idempotency_request_in_progress")
            await send_error(send, 409, ErrorCode.IDEMPOTENCY_REQUEST_IN_PROGRESS, "Запит з цим ключем ще виконується")
        else:
            log.warning("idempotency_result_unknown", stored_owner=existing.owner)
            await send_error(
                send,
                409,
                ErrorCode.IDEMPOTENCY_RESULT_UNKNOWN,
                "Результат попереднього запиту з цим ключем невідомий: перевірте стан і повторіть дію з новим ключем",
            )


def _check_declared_length(scope: Scope) -> None:
    for value in header_values(scope, b"content-length"):
        try:
            declared = int(value)
        except ValueError:
            continue  # некоректний заголовок відхиляє сервер; межу все одно перевіряє _read_body
        if declared > MAX_REQUEST_BODY_BYTES:
            raise _RequestTooLarge


# ---------------------------------------------------------------- сховище в БД


def _to_record(row: ApiIdempotencyKey) -> IdempotencyRecord:
    response: StoredResponse | None = None
    if row.response_status is not None and row.response_headers is not None and row.response_body is not None:
        response = StoredResponse(
            status=row.response_status,
            headers=tuple((str(name), str(value)) for name, value in row.response_headers),
            body=bytes(row.response_body),
        )
    return IdempotencyRecord(
        key=row.idempotency_key,
        method=row.method,
        path=row.path,
        request_hash=row.request_hash,
        owner=row.owner,
        status=IdempotencyStatus(row.status),
        created_ms=row.created_ms,
        response=response,
    )


class DbIdempotencyStore:
    """`IdempotencyStore` на таблиці `api_idempotency_keys`. Кожна операція — окрема коротка закомічена транзакція.

    `begin`: `INSERT … ON CONFLICT DO NOTHING` — паралельна вставка того самого ключа чекає на коміт першої й
    отримує наявний запис (PK). `complete`/`fail`: умовний `UPDATE … WHERE status = 'IN_PROGRESS' AND owner = …`.
    """

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def begin(self, record: IdempotencyRecord) -> IdempotencyRecord | None:
        table = ApiIdempotencyKey
        insert_stmt = (
            pg_insert(table)
            .values(
                idempotency_key=record.key,
                method=record.method,
                path=record.path,
                request_hash=record.request_hash,
                owner=record.owner,
                status=IdempotencyStatus.IN_PROGRESS.value,
                created_ms=record.created_ms,
            )
            .on_conflict_do_nothing(index_elements=[table.idempotency_key])
            .returning(table.idempotency_key)
        )
        async with self._sessions() as session, session.begin():
            if (await session.execute(insert_stmt)).scalar_one_or_none() is not None:
                return None
            row = (await session.execute(select(table).where(table.idempotency_key == record.key))).scalar_one()
            return _to_record(row)

    async def complete(self, key: str, owner: str, response: StoredResponse, *, now_ms: int) -> bool:
        return await self._finish(
            key,
            owner,
            status=IdempotencyStatus.COMPLETED.value,
            response_status=response.status,
            response_headers=[[name, value] for name, value in response.headers],
            response_body=response.body,
            completed_ms=now_ms,
        )

    async def fail(self, key: str, owner: str, *, now_ms: int) -> bool:
        return await self._finish(key, owner, status=IdempotencyStatus.FAILED.value, completed_ms=now_ms)

    async def _finish(self, key: str, owner: str, **values: object) -> bool:
        table = ApiIdempotencyKey
        stmt = (
            update(table)
            .where(
                table.idempotency_key == key,
                table.owner == owner,
                table.status == IdempotencyStatus.IN_PROGRESS.value,
            )
            .values(**values)
            .returning(table.idempotency_key)
        )
        async with self._sessions() as session, session.begin():
            return (await session.execute(stmt)).scalar_one_or_none() is not None
