"""Структуроване логування (ТЗ §12, §14): structlog → stdlib `logging`, JSON, ротація, маскування секретів.

Канали (окремі файли поверх загального `app.log`):
- `trading` — торгові рішення (`trading.log`);
- `orders` — ордери (`orders.log`);
- `errors.log` — усі записи рівня ERROR і вище з будь-якого логера, зокрема сторонніх бібліотек.

Файли: `<log_dir>/<process>/{app,trading,orders,errors}.log`. Кожен процес (торговий, воркер, колектор) пише
у власну підтеку, бо `RotatingFileHandler` не розрахований на кілька процесів в одному файлі.

Маскування (`SecretMasker`) застосовується до **кожного** запису після рендера трасування винятку:
structlog-записи маскуються ще до передачі в stdlib (тож і чужі хендлери бачать лише маски), а в спільному
форматері наших хендлерів — повторно разом із записами сторонніх бібліотек (ccxt, sqlalchemy, asyncpg), до рендера
JSON і ще раз у готовому рядку. Форматер рендерить запис один раз і повторно використовує результат для всіх
своїх хендлерів.

Fail-closed: збій маскування → `log_masking_failed`, збій форматування → `log_format_failed`, збій хендлера
(диск, права) → `log_handler_failed` у stderr; у жодній заглушці немає вихідного тексту запису.

`configure_logging()` — перше, що робить кожен процес: він забирає root-логер (знімає чужі хендлери) і ставить
хуки неперехоплених винятків. До цього виклику логування не маскується.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import re
import sys
import threading
from collections.abc import Iterable, Mapping
from decimal import Decimal
from enum import Enum
from types import TracebackType
from typing import Any, Final, Literal

import structlog
from pydantic import SecretBytes, SecretStr
from structlog.typing import EventDict, Processor, WrappedLogger

from app.core import clock
from app.core.config import Settings

Process = Literal["backend", "worker", "collector"]

APP: Final = "app"
TRADING: Final = "trading"
ORDERS: Final = "orders"

MASK: Final = "***"
# Так само, як `str(SecretStr(...))` у pydantic.
SECRET_MASK: Final = "**********"  # noqa: S105 — маска, а не пароль
# Коротші значення не маскуються за збігом тексту (інакше «ctb» чи «1234» нищили б логи);
# вони все одно маскуються за типом `SecretStr` і за назвою поля.
MIN_SECRET_LEN: Final = 6
# Довші рядки обрізаються (після заміни відомих секретів): маскування синхронне і не повинне блокувати цикл подій.
MAX_STR_LEN: Final = 64 * 1024

_MAX_DEPTH: Final = 20
_HANDLER_MARK: Final = "_ctb_handler"
_FORMATTED_ATTR: Final = "_ctb_formatted"
_LEVELS: Final = frozenset({"debug", "info", "warning", "error", "critical", "exception"})

# Назва поля чутлива, якщо злита назва в нижньому регістрі (`X-BAPI-API-KEY` → `xbapiapikey`, `DBPassword` →
# `dbpassword`) містить одне з цих слів. `sign` — окремо: лише як ціле слово або в кінці (`x_bapi_sign`), бо
# підрядок `sign` є в `signal_id`, `designation`, `assignee`.
_SENSITIVE_SUBSTRINGS: Final = (
    "secret",
    "password",
    "passwd",
    "pwd",
    "token",
    "signature",
    "authorization",
    "cookie",
    "passphrase",
    "credential",
    "apikey",
    "privatekey",
    "accesskey",
)
_SENSITIVE_WORDS: Final = frozenset({"sign"})

# Чутлива назва в довільному тексті (query string, заголовки HTTP, repr об'єктів, повідомлення винятків):
# `name=value`, `name: value`, `"name": "value"`, `name='value'`, `Authorization: Bearer value`. Назва починається
# з чутливого слова (префікс на кшталт `X-BAPI-` лишається поза збігом і не змінюється), далі допускаються
# множина/цифри і суфікс через `_`/`-`. Без вкладених необмежених повторів — час лінійний.
_SENSITIVE_NAME: Final = (
    r"(?:api[_-]?keys?|api[_-]?secrets?|secrets?|passwords?|passwd|pwd|tokens?|signatures?|sign|passphrases?"
    r"|authorization|cookies?|private[_-]?keys?|credentials?)"
)
_INLINE_SECRET: Final = re.compile(
    r"(?P<name>" + _SENSITIVE_NAME + r"\d{0,3}(?:[_-][\w-]{0,40})?)"
    r"(?P<sep>[\"']?\s*[:=]\s*[\"']?)"
    r"(?P<scheme>(?:bearer|basic)\s+)?"
    r"(?P<value>[^\s\"'&,;}\])]+)",
    re.IGNORECASE,
)
# Пароль у DSN: scheme://user:password@host
_DSN_PASSWORD: Final = re.compile(
    r"(?P<prefix>\b[a-z][a-z0-9+.-]{0,30}://[^:/@\s]{1,256}:)[^@\s/]{1,512}(?=@)", re.IGNORECASE
)
_NON_ALNUM: Final = re.compile(r"[^a-z0-9]+")


def _is_sensitive_key(key: str) -> bool:
    words = [w for w in _NON_ALNUM.split(key.lower()) if w]
    compact = "".join(words)
    if any(part in compact for part in _SENSITIVE_SUBSTRINGS):
        return True
    return bool(_SENSITIVE_WORDS.intersection(words)) or compact.endswith("sign")


def _inline_replacement(match: re.Match[str]) -> str:
    return f"{match['name']}{match['sep']}{match['scheme'] or ''}{MASK}"


def _replace_all(text: str, needles: Iterable[str]) -> str:
    for needle in needles:
        # Цикл, бо заміна може «зшити» нове входження; кожна заміна скорочує рядок (len(needle) > len(MASK)).
        while needle in text:
            text = text.replace(needle, MASK)
    return text


class SecretMasker:
    """structlog-процесор маскування секретів. Значення секретів тримає екземпляр, а не модуль."""

    def __init__(self, secrets: Iterable[str] = ()) -> None:
        if isinstance(secrets, str | bytes):
            # Рядок — теж Iterable[str]: розклався б на символи коротші за MIN_SECRET_LEN і мовчки нічого б не маскував.
            raise TypeError("secrets має бути колекцією рядків, а не рядком")
        raw = {s for s in secrets if len(s) >= MIN_SECRET_LEN}
        # Секрет у тексті може бути в «сирому» вигляді або екранованим як у repr()/ascii().
        text_forms = set(raw)
        for secret in raw:
            text_forms.add(repr(secret)[1:-1])
            text_forms.add(ascii(secret)[1:-1])
        # У готовому JSON — ще й JSON-екрановані форми.
        rendered_forms = set(text_forms)
        for form in text_forms:
            rendered_forms.add(json.dumps(form)[1:-1])
            rendered_forms.add(json.dumps(form, ensure_ascii=False)[1:-1])
        # Довші — першими: секрет, що містить інший секрет, маскується цілим.
        self._text_forms = sorted(text_forms, key=len, reverse=True)
        self._rendered_forms = sorted(rendered_forms, key=len, reverse=True)

    def scrub_text(self, text: str) -> str:
        # 1. Відомі значення — першими: шаблони нижче маскують значення лише до роздільника (`;`, `,`, `@`, пробіл),
        #    і хвіст зареєстрованого секрету лишився б, якби вони спрацювали раніше.
        text = _replace_all(text, self._text_forms)
        # 2. Обрізання — після заміни відомих значень, тож межа не розріже цілий зареєстрований секрет.
        if len(text) > MAX_STR_LEN:
            text = f"{text[:MAX_STR_LEN]}…<truncated {len(text) - MAX_STR_LEN} chars>"
        # 3. Шаблони для незареєстрованих секретів.
        text = _DSN_PASSWORD.sub(rf"\g<prefix>{MASK}", text)
        text = _INLINE_SECRET.sub(_inline_replacement, text)
        # 4. І ще раз відомі значення: жодна попередня заміна не повинна лишити їх у тексті.
        return _replace_all(text, self._text_forms)

    def __call__(self, logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
        try:
            return self._mask_mapping(event_dict, 0)
        except Exception as exc:  # noqa: BLE001 — fail-closed: краще втратити запис, ніж злити секрет
            return _masking_failed(event_dict, method_name, exc)

    def scrub_rendered(self, logger: WrappedLogger, method_name: str, rendered: EventDict | str) -> str:
        """Остання лінія захисту: прибрати відомі секрети з готового JSON-рядка."""
        try:
            if not isinstance(rendered, str):
                raise TypeError("scrub_rendered очікує відрендерений рядок")
            return _replace_all(rendered, self._rendered_forms)
        except Exception as exc:  # noqa: BLE001 — fail-closed
            return json.dumps({"event": "log_masking_failed", "error_type": type(exc).__name__})

    def _mask_mapping(self, mapping: Mapping[Any, Any], depth: int) -> dict[Any, Any]:
        result: dict[Any, Any] = {}
        for key, value in mapping.items():
            if isinstance(value, SecretStr | SecretBytes):
                masked: object = SECRET_MASK
            elif isinstance(key, str) and _is_sensitive_key(key) and value is not None:
                # Уже замасковане значення (повторний прохід у форматері) лишається як є.
                masked = value if isinstance(value, str) and value in (MASK, SECRET_MASK) else MASK
            else:
                masked = self._mask_value(value, depth + 1)
            result[self._mask_key(key, depth)] = masked
        return result

    def _mask_key(self, key: object, depth: int) -> str | int | float | bool | None:
        if isinstance(key, str):
            return self.scrub_text(key)
        if key is None or isinstance(key, bool):
            return key
        return str(self._mask_value(key, depth + 1))

    def _mask_value(self, value: object, depth: int = 0) -> object:
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, int | float):
            # Числовий секрет (напр. ID) маскується до рендера, а не в готовому JSON, де заміна зламала б синтаксис.
            return MASK if self._text_forms and self.scrub_text(str(value)) != str(value) else value
        if isinstance(value, str):
            return self.scrub_text(value)
        if isinstance(value, SecretStr | SecretBytes):
            return SECRET_MASK
        if isinstance(value, Decimal):
            return self.scrub_text(str(value))  # рядком — без втрати точності (інваріант 3)
        if isinstance(value, Enum):
            return self._mask_value(value.value, depth + 1)
        if depth > _MAX_DEPTH:
            return f"<too deep {type(value).__name__}>"
        if isinstance(value, Mapping):
            return self._mask_mapping(value, depth)
        if isinstance(value, list | tuple | set | frozenset):
            return [self._mask_value(item, depth + 1) for item in value]
        # Довільний об'єкт: repr тут, а не в JSON-рендері, щоб результат пройшов маскування.
        try:
            text = repr(value)
        except Exception:  # noqa: BLE001 — repr стороннього об'єкта може впасти; повідомлення винятку не беремо
            text = f"<unrepresentable {type(value).__name__}>"
        return self.scrub_text(text)


def _masking_failed(event_dict: EventDict, method_name: str, exc: Exception) -> EventDict:
    level = event_dict.get("level")
    ts_ms = event_dict.get("ts_ms")
    return {
        "event": "log_masking_failed",
        "level": level if isinstance(level, str) and level in _LEVELS else method_name,
        "ts_ms": ts_ms if type(ts_ms) is int else clock.now_ms(),
        "error_type": type(exc).__name__,
    }


def _stub(event: str, record: logging.LogRecord, exc: BaseException | None, masker: SecretMasker) -> str:
    """Заглушка без тексту запису: лише назва логера, рівень, час і тип помилки."""
    return json.dumps(
        {
            "event": event,
            "logger": masker.scrub_text(str(record.name)),
            "level": str(record.levelname).lower(),
            "ts_ms": clock.now_ms(),
            "error_type": type(exc).__name__ if exc is not None else None,
        }
    )


class _SafeFormatter(structlog.stdlib.ProcessorFormatter):
    """ProcessorFormatter, що рендерить запис один раз на форматер і не втрачає запис при збої."""

    def __init__(self, *, masker: SecretMasker, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._masker = masker

    def format(self, record: logging.LogRecord) -> str:
        # Кешування: RotatingFileHandler викликає format() і в shouldRollover, і в emit, а запис проходить кілька
        # хендлерів зі спільним форматером — маскування дороге, рахуємо його один раз.
        cached = record.__dict__.get(_FORMATTED_ATTR)
        if isinstance(cached, tuple) and cached[0] is self:
            return str(cached[1])
        try:
            text = super().format(record)
        except Exception as exc:  # noqa: BLE001 — напр. `%d` з рядком у сторонній бібліотеці; запис не губимо
            text = _stub("log_format_failed", record, exc, self._masker)
        record.__dict__[_FORMATTED_ATTR] = (self, text)
        return text


def _report_handler_error(handler: logging.Handler, record: logging.LogRecord) -> None:
    # Замість вбудованого звіту stdlib (друкує `msg`/`args` запису повз маскування) — рядок без тексту запису.
    exc = sys.exc_info()[1]
    try:
        line = json.dumps(
            {
                "event": "log_handler_failed",
                "handler": type(handler).__name__,
                "logger": str(record.name),
                "level": str(record.levelname).lower(),
                "ts_ms": clock.now_ms(),
                "error_type": type(exc).__name__ if exc is not None else None,
            }
        )
        sys.stderr.write(line + "\n")
        sys.stderr.flush()
    except Exception:  # noqa: BLE001, S110 — stderr недоступний; іншого каналу немає, процес працює далі
        pass


class _RotatingFileHandler(logging.handlers.RotatingFileHandler):
    def handleError(self, record: logging.LogRecord) -> None:
        _report_handler_error(self, record)


class _StreamHandler(logging.StreamHandler):  # type: ignore[type-arg]
    def handleError(self, record: logging.LogRecord) -> None:
        _report_handler_error(self, record)


def _add_ts_ms(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    # Час запису — з єдиного годинника (ТЗ §4.3): UTC мс, у тестах — фейковий.
    event_dict["ts_ms"] = clock.now_ms()
    return event_dict


def _mark(handler: logging.Handler) -> logging.Handler:
    setattr(handler, _HANDLER_MARK, True)
    return handler


# Неперехоплені винятки інакше друкуються в stderr сирим traceback-ом повз маскування.
def _excepthook(exc_type: type[BaseException], exc: BaseException, tb: TracebackType | None) -> None:
    get_logger(APP).critical("uncaught_exception", exc_info=(exc_type, exc, tb))


def _threading_excepthook(args: threading.ExceptHookArgs) -> None:
    if args.exc_type is SystemExit:
        return
    thread = args.thread.name if args.thread is not None else None
    if args.exc_value is None:
        get_logger(APP).critical("uncaught_thread_exception", thread=thread, error_type=args.exc_type.__name__)
        return
    get_logger(APP).critical(
        "uncaught_thread_exception",
        thread=thread,
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )


def _unraisablehook(unraisable: Any) -> None:
    exc_value = unraisable.exc_value
    get_logger(APP).error(
        "unraisable_exception",
        err_msg=unraisable.err_msg,
        object=unraisable.object,
        exc_info=(unraisable.exc_type, exc_value, unraisable.exc_traceback) if exc_value is not None else None,
    )


# Стан stdlib до першого configure_logging — щоб shutdown_logging повернув його (тести, завершення процесу).
_saved_globals: dict[str, Any] = {}


def _take_over_globals() -> None:
    if not _saved_globals:
        _saved_globals.update(
            excepthook=sys.excepthook,
            threading_excepthook=threading.excepthook,
            unraisablehook=sys.unraisablehook,
            raise_exceptions=logging.raiseExceptions,
        )
    sys.excepthook = _excepthook
    threading.excepthook = _threading_excepthook
    sys.unraisablehook = _unraisablehook
    # Чужі хендлери (не наші) вбудований звіт stdlib про помилку не друкують: він показує сирі `msg`/`args`.
    logging.raiseExceptions = False
    logging.captureWarnings(True)


def _restore_globals() -> None:
    if _saved_globals:
        sys.excepthook = _saved_globals["excepthook"]
        threading.excepthook = _saved_globals["threading_excepthook"]
        sys.unraisablehook = _saved_globals["unraisablehook"]
        logging.raiseExceptions = _saved_globals["raise_exceptions"]
        _saved_globals.clear()
    logging.captureWarnings(False)


def configure_logging(settings: Settings, *, process: Process, extra_secrets: Iterable[str] = ()) -> None:
    """Налаштувати логування процесу. Ідемпотентно: повторний виклик замінює попередні хендлери.

    `extra_secrets` — значення, яких немає в `Settings` (напр. отримані під час роботи), але які теж треба маскувати.
    Root-логер переходить під повний контроль: чужі хендлери на ньому знімаються. Логери з власними хендлерами і
    `propagate=False` (напр. стандартний log_config uvicorn) обходять ці хендлери — їх треба вимикати (T0.7:
    `log_config=None`).
    """
    masker = SecretMasker([*settings.secret_values(), *_as_secret_list(extra_secrets)])
    shutdown_logging()

    shared: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        _add_ts_ms,
    ]
    formatter = _SafeFormatter(
        masker=masker,
        # Записи сторонніх бібліотек проходять ті самі кроки, що й structlog-записи.
        foreign_pre_chain=[*shared, structlog.processors.format_exc_info],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            masker,
            structlog.processors.JSONRenderer(ensure_ascii=False),
            masker.scrub_rendered,
        ],
    )

    process_dir = settings.log_dir / process
    process_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(process_dir, 0o700)  # mkdir не змінює права вже наявної теки

    def file_handler(name: str, level: int = logging.NOTSET) -> logging.Handler:
        handler = _RotatingFileHandler(
            process_dir / name,
            maxBytes=settings.log_max_bytes,
            backupCount=settings.log_backup_count,
            encoding="utf-8",
            delay=True,
        )
        handler.setLevel(level)
        handler.setFormatter(formatter)
        return _mark(handler)

    root = logging.getLogger()
    for foreign in list(root.handlers):
        root.removeHandler(foreign)
    root.setLevel(settings.log_level)
    root.addHandler(file_handler("app.log"))
    root.addHandler(file_handler("errors.log", logging.ERROR))
    if settings.log_console:
        console = _StreamHandler(sys.stderr)
        console.setFormatter(formatter)
        root.addHandler(_mark(console))
    logging.getLogger(TRADING).addHandler(file_handler("trading.log"))
    logging.getLogger(ORDERS).addHandler(file_handler("orders.log"))

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *shared,
            structlog.processors.StackInfoRenderer(),
            # Трасування — звичайний текст без локальних змінних (locals могли б містити секрети).
            structlog.processors.format_exc_info,
            # Маскування ще до передачі в stdlib: сторонні хендлери теж бачать лише маски.
            # У форматері воно повторюється (ідемпотентно) — там проходять і записи сторонніх бібліотек.
            masker,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )
    _take_over_globals()


def _as_secret_list(values: Iterable[str]) -> list[str]:
    if isinstance(values, str | bytes):
        raise TypeError("extra_secrets має бути колекцією рядків, а не рядком")
    return list(values)


def shutdown_logging() -> None:
    """Зняти й закрити наші хендлери і повернути хуки/прапорці stdlib (завершення процесу, тести)."""
    for logger in (logging.getLogger(), logging.getLogger(TRADING), logging.getLogger(ORDERS)):
        for handler in list(logger.handlers):
            if getattr(handler, _HANDLER_MARK, False):
                logger.removeHandler(handler)
                handler.close()
    logging.getLogger().setLevel(logging.WARNING)
    _restore_globals()


def get_logger(name: str = APP, **initial_values: Any) -> structlog.stdlib.BoundLogger:
    """Логер каналу `name` (`APP`, `TRADING`, `ORDERS` або `app.<модуль>`) з початковим контекстом."""
    logger: structlog.stdlib.BoundLogger = structlog.stdlib.get_logger(name, **initial_values)
    return logger


def bind_context(**values: Any) -> None:
    """Додати контекст (`bot_id`, `symbol`, `client_order_id`, `signal_id`) до всіх записів поточної задачі asyncio."""
    structlog.contextvars.bind_contextvars(**values)


def clear_context() -> None:
    structlog.contextvars.clear_contextvars()
