"""Логування `app.core.logging`: JSON, канали, ротація і маскування секретів (ТЗ §12, §14; T0.3).

Головна вимога: секрет у полі, винятку чи `repr` не потрапляє в жоден лог-файл і в stderr.
"""

from __future__ import annotations

import json
import logging
import stat
import sys
import threading
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import SecretStr

from app.core.clock import FakeClock
from app.core.config import Settings
from app.core.logging import (
    MASK,
    MAX_STR_LEN,
    ORDERS,
    TRADING,
    SecretMasker,
    bind_context,
    clear_context,
    configure_logging,
    get_logger,
    shutdown_logging,
)

# Вигадані значення; реальних ключів у тестах немає.
DB_PASSWORD = "db-Pa55word-XYZ-987"
API_KEY = "AKfake0123456789key"
API_SECRET = "SKfake9876543210secret"
SIGNATURE = "f00dbabe1234567890abcdef"
LOG_FILES = ("app.log", "trading.log", "orders.log", "errors.log")


@pytest.fixture(autouse=True)
def _reset_logging() -> Iterator[None]:
    yield
    shutdown_logging()
    clear_context()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        log_dir=tmp_path,
        log_console=False,
        postgres_password=SecretStr(DB_PASSWORD),
    )


@pytest.fixture
def log_dir(settings: Settings) -> Path:
    configure_logging(settings, process="backend", extra_secrets=[API_KEY, API_SECRET])
    return settings.log_dir / "backend"


def _lines(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _all_text(log_dir: Path) -> str:
    return "".join(p.read_text(encoding="utf-8") for p in sorted(log_dir.glob("*.log*")))


def _assert_no_secrets(text: str, *secrets: str) -> None:
    for secret in secrets:
        assert secret not in text
        assert json.dumps(secret)[1:-1] not in text


# --------------------------------------------------------------------------- секрет у полі


def test_secret_in_sensitive_fields_is_masked(log_dir: Path) -> None:
    unregistered_key = "unregistered-key-AAA111"
    get_logger(ORDERS).info(
        "order_sent",
        api_key=unregistered_key,
        password=SecretStr("pw-unregistered-BBB222"),
        headers={"X-BAPI-SIGN": SIGNATURE, "X-BAPI-API-KEY": "hdr-key-CCC333", "Content-Type": "application/json"},
        nested=[{"apiSecret": "nested-secret-DDD444"}, ("token", {"access_token": "tok-EEE555"})],
    )
    _assert_no_secrets(
        _all_text(log_dir),
        unregistered_key,
        "pw-unregistered-BBB222",
        SIGNATURE,
        "hdr-key-CCC333",
        "nested-secret-DDD444",
        "tok-EEE555",
    )
    (record,) = _lines(log_dir / "orders.log")
    assert record["api_key"] == MASK
    assert record["password"] == "**********"
    assert record["headers"]["X-BAPI-SIGN"] == MASK
    assert record["headers"]["Content-Type"] == "application/json"
    assert record["nested"][0]["apiSecret"] == MASK


def test_secret_str_value_under_neutral_key_is_masked(log_dir: Path) -> None:
    get_logger().info("connect", value=SecretStr("neutral-key-secret-FFF666"))
    _assert_no_secrets(_all_text(log_dir), "neutral-key-secret-FFF666")


def test_registered_secret_in_free_text_is_masked(log_dir: Path) -> None:
    log = get_logger()
    log.info(f"connecting with {DB_PASSWORD} and {API_KEY}", note=f"secret={API_SECRET}; done")
    log.info("plain", items=[f"x{DB_PASSWORD}y"], mapping={f"key-{API_KEY}": 1})
    _assert_no_secrets(_all_text(log_dir), DB_PASSWORD, API_KEY, API_SECRET)


@pytest.mark.parametrize(
    ("text", "secret"),
    [
        ("GET /v5/order?api_key=qs-key-111222&sign=qs-sign-333444&symbol=BTCUSDT", "qs-key-111222"),
        ("GET /v5/order?api_key=qs-key-111222&sign=qs-sign-333444&symbol=BTCUSDT", "qs-sign-333444"),
        ('{"apiKey": "json-key-555666", "x": 1}', "json-key-555666"),
        ("Authorization: Bearer bearer-tok-777888", "bearer-tok-777888"),
        ("X-BAPI-SIGN: hdr-sign-999000", "hdr-sign-999000"),
        ("postgresql://ctb:dsn-pass-121212@127.0.0.1:5432/ctb", "dsn-pass-121212"),
        ("secret_key='repr-secret-343434'", "repr-secret-343434"),
    ],
)
def test_inline_patterns_are_masked_without_registration(log_dir: Path, text: str, secret: str) -> None:
    get_logger().warning(text, detail=text)
    _assert_no_secrets(_all_text(log_dir), secret)


def test_non_secret_fields_are_preserved(log_dir: Path) -> None:
    class Side(Enum):
        BUY = "Buy"

    bind_context(bot_id=7, symbol="BTCUSDT")
    get_logger(TRADING).info(
        "decision",
        signal_id="sig-42",
        client_order_id="tbaa0123456789abcdef0123456789abcd",
        price=Decimal("65000.123456789"),
        qty=Decimal("0.001"),
        side=Side.BUY,
        url="https://api-testnet.bybit.com/v5/market/time",
        designation="plain",
    )
    (record,) = _lines(log_dir / "trading.log")
    assert record["signal_id"] == "sig-42"
    assert record["client_order_id"] == "tbaa0123456789abcdef0123456789abcd"
    assert record["price"] == "65000.123456789"
    assert record["qty"] == "0.001"
    assert record["side"] == "Buy"
    assert record["bot_id"] == 7
    assert record["symbol"] == "BTCUSDT"
    assert record["url"] == "https://api-testnet.bybit.com/v5/market/time"
    assert record["event"] == "decision"
    assert record["level"] == "info"
    assert record["logger"] == TRADING


# --------------------------------------------------------------------------- секрет у винятку


def test_secret_in_exception_and_cause_is_masked(log_dir: Path) -> None:
    log = get_logger(ORDERS)
    try:
        try:
            raise ValueError(f"bad key {API_KEY}")
        except ValueError as exc:
            raise RuntimeError(f"request failed: api_secret={API_SECRET} pw={DB_PASSWORD}") from exc
    except RuntimeError:
        log.exception("place_order_failed")
    text = _all_text(log_dir)
    _assert_no_secrets(text, API_KEY, API_SECRET, DB_PASSWORD)
    (record,) = _lines(log_dir / "errors.log")
    assert record["event"] == "place_order_failed"
    assert record["level"] == "error"
    assert "Traceback" in record["exception"]
    assert "RuntimeError" in record["exception"]
    assert "ValueError" in record["exception"]


def test_secret_in_unregistered_exception_with_inline_pattern(log_dir: Path) -> None:
    try:
        raise ConnectionError("POST /v5/order/create headers={'X-BAPI-API-KEY': 'exc-key-565656'}")
    except ConnectionError:
        get_logger().exception("http_error")
    _assert_no_secrets(_all_text(log_dir), "exc-key-565656")


# --------------------------------------------------------------------------- секрет у repr


@dataclass
class _Credentials:
    api_key: str
    api_secret: str


class _Opaque:
    def __init__(self, secret: str) -> None:
        self._secret = secret

    def __repr__(self) -> str:
        return f"<Opaque holding {self._secret}>"


def test_secret_in_repr_of_object_is_masked(log_dir: Path) -> None:
    get_logger().info(
        "creds_loaded",
        creds=_Credentials(api_key="repr-key-787878", api_secret="repr-sec-909090"),
        opaque=_Opaque(API_SECRET),
        as_text=repr(_Credentials(api_key="repr-key-787878", api_secret="repr-sec-909090")),
    )
    _assert_no_secrets(_all_text(log_dir), "repr-key-787878", "repr-sec-909090", API_SECRET)


def test_object_with_failing_repr_does_not_break_logging(log_dir: Path) -> None:
    class _Broken:
        def __repr__(self) -> str:
            raise RuntimeError(f"repr leaks {API_SECRET}")

    get_logger().info("odd", obj=_Broken(), keep="yes")
    (record,) = _lines(log_dir / "app.log")
    assert record["keep"] == "yes"
    assert "_Broken" in record["obj"]
    _assert_no_secrets(_all_text(log_dir), API_SECRET)


# --------------------------------------------------------------------------- сторонні логери і stderr


def test_foreign_stdlib_logger_is_masked_and_routed(log_dir: Path) -> None:
    logging.getLogger("ccxt.base.exchange").error(
        "bybit POST failed: %s", f"apiKey={API_KEY}&sign={SIGNATURE} pw {DB_PASSWORD}"
    )
    _assert_no_secrets(_all_text(log_dir), API_KEY, SIGNATURE, DB_PASSWORD)
    (record,) = _lines(log_dir / "errors.log")
    assert record["logger"] == "ccxt.base.exchange"
    assert record["level"] == "error"


def test_foreign_logger_exception_is_masked(log_dir: Path) -> None:
    try:
        raise OSError(f"socket closed, token={API_KEY}")
    except OSError:
        logging.getLogger("sqlalchemy.pool").exception("pool error with %s", DB_PASSWORD)
    _assert_no_secrets(_all_text(log_dir), API_KEY, DB_PASSWORD)
    (record,) = _lines(log_dir / "errors.log")
    assert "OSError" in record["exception"]


def test_console_output_is_masked(settings: Settings, capsys: pytest.CaptureFixture[str]) -> None:
    console_settings = settings.model_copy(update={"log_console": True})
    configure_logging(console_settings, process="backend")
    get_logger().info(f"pw {DB_PASSWORD}", api_key="console-key-131313")
    err = capsys.readouterr().err
    assert '"event": "pw ***"' in err
    _assert_no_secrets(err, DB_PASSWORD, "console-key-131313")


def test_console_disabled_writes_nothing_to_stderr(log_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    get_logger().info("quiet")
    assert capsys.readouterr().err == ""


def test_json_escaped_secret_is_masked(settings: Settings) -> None:
    tricky = 'q"uo\\te-secret-Ж-1'
    configure_logging(settings, process="backend", extra_secrets=[tricky])
    get_logger().info(f"value {tricky}", field=tricky, other={"x": f"<{tricky}>"})
    _assert_no_secrets(_all_text(settings.log_dir / "backend"), tricky)


def test_short_secrets_are_not_registered_but_still_masked_by_key(settings: Settings) -> None:
    configure_logging(settings, process="backend", extra_secrets=["abc"])
    get_logger().info("abc is a common substring", password="abc")
    (record,) = _lines(settings.log_dir / "backend" / "app.log")
    assert record["event"] == "abc is a common substring"
    assert record["password"] == MASK


def test_foreign_handler_on_root_sees_only_masked_values(log_dir: Path, caplog: pytest.LogCaptureFixture) -> None:
    # Хендлер не з configure_logging (тут — pytest caplog) отримує запис без нашого форматера.
    with caplog.at_level(logging.INFO):
        get_logger(ORDERS).info(f"sent with {API_KEY}", api_secret="caplog-secret-151515")
    assert caplog.records
    _assert_no_secrets(caplog.text, API_KEY, "caplog-secret-151515")
    _assert_no_secrets(str([r.msg for r in caplog.records]), API_KEY, "caplog-secret-151515")


# --------------------------------------------------------------------------- fail-closed


def test_masking_failure_drops_the_event(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(self: SecretMasker, value: object, depth: int = 0) -> object:
        raise RuntimeError("masker bug")

    monkeypatch.setattr(SecretMasker, "_mask_value", _boom)
    get_logger().info(f"would leak {API_KEY}", api_key=API_KEY)
    (record,) = _lines(log_dir / "app.log")
    assert record["event"] == "log_masking_failed"
    assert record["level"] == "info"
    assert record["error_type"] == "RuntimeError"
    _assert_no_secrets(_all_text(log_dir), API_KEY)


# --------------------------------------------------------------------------- канали, час, ротація


def test_channels_are_routed_to_their_files(log_dir: Path) -> None:
    get_logger(TRADING).info("signal_accepted")
    get_logger(ORDERS).info("order_submitting")
    get_logger("app.data").warning("gap_detected")
    get_logger(TRADING).error("risk_veto_failed")

    def events(name: str) -> list[str]:
        return [r["event"] for r in _lines(log_dir / name)]

    assert events("trading.log") == ["signal_accepted", "risk_veto_failed"]
    assert events("orders.log") == ["order_submitting"]
    assert events("app.log") == ["signal_accepted", "order_submitting", "gap_detected", "risk_veto_failed"]
    assert events("errors.log") == ["risk_veto_failed"]


def test_level_filter_applies(settings: Settings) -> None:
    configure_logging(settings.model_copy(update={"log_level": "WARNING"}), process="backend")
    get_logger().info("hidden")
    get_logger().warning("shown")
    assert [r["event"] for r in _lines(settings.log_dir / "backend" / "app.log")] == ["shown"]


def test_processes_write_to_separate_directories(settings: Settings) -> None:
    configure_logging(settings, process="worker")
    get_logger().info("from_worker")
    assert [r["event"] for r in _lines(settings.log_dir / "worker" / "app.log")] == ["from_worker"]
    assert not (settings.log_dir / "backend").exists()


def test_timestamp_is_int_ms_from_clock(log_dir: Path, fake_clock: FakeClock) -> None:
    get_logger().info("first")
    fake_clock.advance(1_234)
    logging.getLogger("uvicorn").warning("second")
    first, second = _lines(log_dir / "app.log")
    assert type(first["ts_ms"]) is int
    assert second["ts_ms"] - first["ts_ms"] == 1_234
    assert first["ts_ms"] == 1_767_225_600_000


def test_rotation_creates_backup_files(settings: Settings) -> None:
    small = settings.model_copy(update={"log_max_bytes": 1024 * 1024, "log_backup_count": 2})
    configure_logging(small, process="backend")
    chunk = "x" * 64 * 1024
    for i in range(40):  # ~2.5 MiB > 2 × max_bytes
        get_logger().info("bulk", i=i, payload=chunk)
    backend = small.log_dir / "backend"
    assert (backend / "app.log.1").exists()
    assert (backend / "app.log.2").exists()
    assert not (backend / "app.log.3").exists()
    assert (backend / "app.log").stat().st_size <= 1024 * 1024


def test_reconfigure_does_not_duplicate_handlers(settings: Settings) -> None:
    configure_logging(settings, process="backend")
    configure_logging(settings, process="backend")
    get_logger(TRADING).info("once")
    backend = settings.log_dir / "backend"
    assert len(_lines(backend / "app.log")) == 1
    assert len(_lines(backend / "trading.log")) == 1


def test_context_binding_and_clearing(log_dir: Path) -> None:
    bind_context(bot_id=1, client_order_id="tbaa00")
    get_logger().info("with_ctx")
    clear_context()
    get_logger().info("without_ctx")
    with_ctx, without_ctx = _lines(log_dir / "app.log")
    assert with_ctx["bot_id"] == 1
    assert with_ctx["client_order_id"] == "tbaa00"
    assert "bot_id" not in without_ctx


# --------------------------------------------------------------------------- property-based


def _string_values(obj: object) -> list[str]:
    if isinstance(obj, str):
        return [obj]
    if isinstance(obj, dict):
        return [s for k, v in obj.items() for s in (*_string_values(k), *_string_values(v))]
    if isinstance(obj, list | tuple):
        return [s for item in obj for s in _string_values(item)]
    return []


@given(
    secret=st.text(st.characters(codec="utf-8", exclude_categories=("Cs", "Cc", "Zs")), min_size=6, max_size=40),
    prefix=st.text(max_size=20),
    suffix=st.text(max_size=20),
)
def test_registered_secret_never_survives_masking(secret: str, prefix: str, suffix: str) -> None:
    masker = SecretMasker([secret])
    text = f"{prefix}{secret}{suffix}"
    event = masker(None, "info", {"event": text, "nested": {"k": [text, (text,)]}, text: 1})
    for value in _string_values(event):
        assert secret not in value
    rendered = masker.scrub_rendered(None, "info", json.dumps(event, ensure_ascii=False))
    assert secret not in rendered


# --------------------------------------------------------------------------- регресії з рев'ю T0.3


def _fourgrams(text: str) -> set[str]:
    return {text[i : i + 4] for i in range(len(text) - 3)}


@pytest.mark.parametrize("secret", ["Abc;def123", "Xy,zz-123456", "S3cr3t)value)", "p@ssw0rd!x", "with space 98"])
@pytest.mark.parametrize(
    "template",
    ["db connect failed: password={s} host=db", "postgresql://ctb:{s}@127.0.0.1/ctb", "Settings(api_secret={s})"],
)
def test_registered_secret_with_delimiters_leaves_no_fragment(settings: Settings, secret: str, template: str) -> None:
    # Блокер рев'ю: шаблон маскував значення лише до першого `;`/`,`/`)`/`@`/пробілу, і хвіст секрету лишався.
    configure_logging(settings, process="backend", extra_secrets=[secret])
    get_logger(ORDERS).error(template.format(s=secret), detail=template.format(s=secret))
    text = _all_text(settings.log_dir / "backend")
    leaked = {gram for gram in _fourgrams(secret) if gram in text}
    assert leaked == set(), f"фрагменти секрету в лозі: {leaked}"


@given(
    secret=st.text(st.characters(min_codepoint=33, max_codepoint=126), min_size=6, max_size=30),
    prefix=st.text(st.characters(min_codepoint=0x430, max_codepoint=0x44F), max_size=10),
    template=st.sampled_from(["password={s}", "api_key: {s}", "pg://u:{s}@h", '"token": "{s}"', "{s}"]),
)
def test_registered_secret_leaves_no_fragment_property(secret: str, prefix: str, template: str) -> None:
    masker = SecretMasker([secret])
    out = masker.scrub_text(f"{prefix} {template.format(s=secret)} {prefix}")
    allowed = _fourgrams(template.format(s=MASK))
    leaked = {gram for gram in _fourgrams(secret) if gram in out and gram not in allowed}
    assert leaked == set()


def test_uncaught_exception_is_logged_masked(log_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    try:
        raise RuntimeError(f"cannot connect with {API_SECRET}")
    except RuntimeError as exc:
        sys.excepthook(type(exc), exc, exc.__traceback__)
    _assert_no_secrets(_all_text(log_dir) + capsys.readouterr().err, API_SECRET)
    (record,) = _lines(log_dir / "errors.log")
    assert record["event"] == "uncaught_exception"
    assert record["level"] == "critical"
    assert "RuntimeError" in record["exception"]


def test_uncaught_thread_exception_is_logged_masked(settings: Settings, capsys: pytest.CaptureFixture[str]) -> None:
    def _fail() -> None:
        raise RuntimeError(f"thread failed with {API_SECRET}")

    # Конфігуруємо в тілі тесту: pytest ставить власний threading.excepthook на фазу виконання тесту.
    configure_logging(settings, process="backend", extra_secrets=[API_SECRET])
    log_dir = settings.log_dir / "backend"
    thread = threading.Thread(target=_fail, name="probe")
    thread.start()
    thread.join()
    shutdown_logging()  # відновити хук pytest до виходу з його контексту
    _assert_no_secrets(_all_text(log_dir) + capsys.readouterr().err, API_SECRET)
    (record,) = _lines(log_dir / "errors.log")
    assert record["event"] == "uncaught_thread_exception"
    assert record["thread"] == "probe"


def test_shutdown_restores_hooks_and_flags(settings: Settings) -> None:
    before = (sys.excepthook, threading.excepthook, sys.unraisablehook, logging.raiseExceptions)
    configure_logging(settings, process="backend")
    assert sys.excepthook is not before[0]
    shutdown_logging()
    assert (sys.excepthook, threading.excepthook, sys.unraisablehook, logging.raiseExceptions) == before


def test_format_failure_writes_stub_instead_of_dropping(log_dir: Path) -> None:
    logging.getLogger("ccxt.base").error("request failed %d", f"x {API_KEY}")
    (record,) = _lines(log_dir / "errors.log")
    assert record["event"] == "log_format_failed"
    assert record["logger"] == "ccxt.base"
    assert record["level"] == "error"
    assert record["error_type"] == "TypeError"
    _assert_no_secrets(_all_text(log_dir), API_KEY)


def test_handler_failure_is_reported_without_message(
    log_dir: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def _disk_full(self: logging.Handler, record: logging.LogRecord) -> bool:
        raise OSError(f"No space left on device {API_KEY}")

    monkeypatch.setattr(logging.handlers.RotatingFileHandler, "shouldRollover", _disk_full)
    get_logger().info(f"payload {API_SECRET}")
    err = capsys.readouterr().err
    assert '"event": "log_handler_failed"' in err
    assert '"error_type": "OSError"' in err
    _assert_no_secrets(err, API_KEY, API_SECRET)


def test_preexisting_root_handlers_are_replaced(settings: Settings, capsys: pytest.CaptureFixture[str]) -> None:
    foreign = logging.StreamHandler(sys.stderr)  # як після logging.basicConfig()
    logging.getLogger().addHandler(foreign)
    try:
        configure_logging(settings, process="backend")
        logging.getLogger("asyncpg").error("connect failed %s", f"password {DB_PASSWORD}")
        assert foreign not in logging.getLogger().handlers
        _assert_no_secrets(capsys.readouterr().err, DB_PASSWORD)
    finally:
        logging.getLogger().removeHandler(foreign)


def test_numeric_secret_keeps_json_valid(settings: Settings) -> None:
    configure_logging(settings, process="backend", extra_secrets=["12345678"])
    get_logger().info("order", order_link=12345678, price=12345678.5, other=42)
    (record,) = _lines(settings.log_dir / "backend" / "app.log")  # json.loads не падає
    assert record["order_link"] == MASK
    assert record["price"] == MASK
    assert record["other"] == 42


@pytest.mark.parametrize(
    "key",
    [
        "DBPassword",
        "HMACSignature",
        "JWTToken",
        "AWSSecret",
        "XBAPISign",
        "passwords",
        "tokens",
        "password2",
        "apiKeys",
    ],
)
def test_sensitive_key_variants_are_masked(key: str) -> None:
    assert SecretMasker()(None, "info", {key: "unreg-value-123"})[key] == MASK


@pytest.mark.parametrize("key", ["signal_id", "designation", "symbol", "client_order_id", "assignee"])
def test_neutral_keys_are_not_masked(key: str) -> None:
    assert SecretMasker()(None, "info", {key: "value-123"})[key] == "value-123"


@pytest.mark.parametrize(
    ("text", "secret"),
    [("tokens=tok-AAA111", "tok-AAA111"), ("password2=pw-BBB222", "pw-BBB222"), ("apiKeys: k-CCC333", "k-CCC333")],
)
def test_inline_plural_and_numbered_names_are_masked(text: str, secret: str) -> None:
    assert secret not in SecretMasker().scrub_text(text)


def test_extra_secrets_as_plain_string_is_rejected(settings: Settings) -> None:
    with pytest.raises(TypeError):
        configure_logging(settings, process="backend", extra_secrets="token-value-XYZ")


def test_existing_process_dir_gets_owner_only_permissions(settings: Settings) -> None:
    process_dir = settings.log_dir / "backend"
    process_dir.mkdir(mode=0o755)
    configure_logging(settings, process="backend")
    assert stat.S_IMODE(process_dir.stat().st_mode) == 0o700


def test_long_strings_are_truncated_without_secret_prefix(settings: Settings) -> None:
    secret = "LongSecret-0123456789"
    masker = SecretMasker([secret])
    # Секрет перетинає межу обрізання; відомі значення замінюються до обрізання, тож половина не лишається.
    text = "a" * (MAX_STR_LEN - 6) + secret + "b" * 100
    out = masker.scrub_text(text)
    assert len(out) < MAX_STR_LEN + 100
    assert "truncated" in out
    assert "LongSe" not in out
    pathological = masker.scrub_text("token-" * 200_000)
    assert len(pathological) < MAX_STR_LEN + 100


def test_record_is_masked_once_per_formatter(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    original = SecretMasker.__call__

    def _counting(self: SecretMasker, logger: object, method_name: str, event_dict: Any) -> Any:
        nonlocal calls
        calls += 1
        return original(self, logger, method_name, event_dict)

    monkeypatch.setattr(SecretMasker, "__call__", _counting)
    get_logger(ORDERS).error("rejected", reason="x")
    # один прохід у structlog + один у спільному форматері для orders.log, app.log, errors.log
    assert calls == 2
    assert len(_lines(log_dir / "orders.log")) == len(_lines(log_dir / "errors.log")) == 1


def test_warnings_stack_info_and_positional_args_are_masked(log_dir: Path) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("always")
        logging.captureWarnings(True)
        warnings.warn(f"deprecated with {API_KEY}", UserWarning, stacklevel=1)
    get_logger().info("with stack %s", API_SECRET, stack_info=True)
    logging.getLogger("ccxt").warning("stack %s", DB_PASSWORD, stack_info=True)
    _assert_no_secrets(_all_text(log_dir), API_KEY, API_SECRET, DB_PASSWORD)


def test_secret_bytes_and_deep_structures_are_handled(log_dir: Path) -> None:
    from pydantic import SecretBytes

    loop: list[Any] = []
    loop.append(loop)
    deep: list[Any] = [1]
    for _ in range(30):
        deep = [deep]
    get_logger().info("odd", raw=SecretBytes(b"bytes-secret-XYZ"), loop=loop, deep=deep)
    (record,) = _lines(log_dir / "app.log")
    assert record["raw"] == "**********"
    assert "bytes-secret-XYZ" not in _all_text(log_dir)
