"""Кадри публічного потоку ліквідацій Bybit (T1.10). Приклад події — документація v5 allLiquidation; відповіді на
підписку й ping — записані з публічного mainnet 2026-10-30 (без ключів)."""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import aiohttp
import pytest

from app.exchange.bybit import ExchangeMode
from app.exchange.errors import ExchangeUnavailable
from app.exchange.liquidations import (
    MAINNET_LINEAR_WS,
    TESTNET_LINEAR_WS,
    AiohttpLiquidationConnector,
    liquidation_stream_url,
    liquidation_topic,
    parse_liquidation_message,
)

ROSE = "ROSE/USDT:USDT"
# Документація v5, All Liquidation, приклад відповіді (context7, 2026-10-30). S=Sell — ліквідація short.
OFFICIAL_SELL = """
{
    "topic": "allLiquidation.ROSEUSDT",
    "type": "snapshot",
    "ts": 1739502303204,
    "data": [
        {
            "T": 1739502302929,
            "s": "ROSEUSDT",
            "S": "Sell",
            "v": "20000",
            "p": "0.04499"
        }
    ]
}
"""
# Записано з wss://stream.bybit.com/v5/public/linear: окремий запит на невідому тему і JSON-ping з `req_id`.
MAINNET_REJECTED = (
    '{"success":false,"ret_msg":"error:handler not found,topic:allLiquidation.NOSUCHCOINUSDT",'
    '"conn_id":"db293j69806pvcveqh40-7s6mm","req_id":"allLiquidation.NOSUCHCOINUSDT","op":"subscribe"}'
)
MAINNET_PONG = '{"success":true,"ret_msg":"pong","conn_id":"db293j69806pvcveqh40-7s6mm","req_id":"p1","op":"ping"}'
# Справжні ліквідації з публічного mainnet 2026-10-30 12:29 UTC (за 4 хв на 16 символах — лише ці три: ринок тихий).
AVAX = "AVAX/USDT:USDT"
PEPE = "1000PEPE/USDT:USDT"
MAINNET_EVENTS = (
    '{"topic":"allLiquidation.AVAXUSDT","type":"snapshot","ts":1793363346256,'
    '"data":[{"T":1793363345991,"s":"AVAXUSDT","S":"Buy","v":"1783.2","p":"8.135"}]}',
    '{"topic":"allLiquidation.AVAXUSDT","type":"snapshot","ts":1793363354756,'
    '"data":[{"T":1793363354301,"s":"AVAXUSDT","S":"Buy","v":"6.2","p":"8.113"}]}',
    '{"topic":"allLiquidation.1000PEPEUSDT","type":"snapshot","ts":1793363367035,'
    '"data":[{"T":1793363366923,"s":"1000PEPEUSDT","S":"Buy","v":"2000","p":"0.0051200"}]}',
)
MAINNET_SYMBOLS = {"AVAXUSDT": AVAX, "1000PEPEUSDT": PEPE}
# Стара тема `liquidation.{symbol}` на mainnet більше не існує.
MAINNET_LEGACY_REJECTED = (
    '{"success":false,"ret_msg":"error:handler not found,topic:liquidation.BTCUSDT",'
    '"conn_id":"db293t3h7csfeki7llu0-7rxir","req_id":"liquidation.BTCUSDT","op":"subscribe"}'
)
_SYMBOLS = {"ROSEUSDT": ROSE, "BTCUSDT": "BTC/USDT:USDT", "NOSUCHCOINUSDT": "NOSUCHCOIN/USDT:USDT"}


def _frame(side: str, qty: str, price: str, ts: int = 1739502303000) -> str:
    return json.dumps(
        {
            "topic": "allLiquidation.ROSEUSDT",
            "type": "snapshot",
            "ts": ts,
            "data": [{"T": ts, "s": "ROSEUSDT", "S": side, "v": qty, "p": price}],
        }
    )


def test_official_sell_snapshot_is_a_short_liquidation() -> None:
    message = parse_liquidation_message(OFFICIAL_SELL, symbols=_SYMBOLS)
    assert message.kind == "events"
    assert message.exchange_ts_ms == 1_739_502_303_204
    assert len(message.events) == 1
    event = message.events[0]
    assert event.symbol == ROSE
    assert event.position_side == "short"
    assert event.time_ms == 1_739_502_302_929
    assert event.qty == Decimal("20000")
    assert event.price == Decimal("0.04499")
    assert event.notional == Decimal("899.8")


def test_recorded_mainnet_events_parse_exactly() -> None:
    parsed = [parse_liquidation_message(raw, symbols=MAINNET_SYMBOLS) for raw in MAINNET_EVENTS]
    assert [message.kind for message in parsed] == ["events", "events", "events"]
    events = [message.events[0] for message in parsed]
    assert [(e.symbol, e.position_side, e.time_ms, e.qty, e.price) for e in events] == [
        (AVAX, "long", 1_793_363_345_991, Decimal("1783.2"), Decimal("8.135")),
        (AVAX, "long", 1_793_363_354_301, Decimal("6.2"), Decimal("8.113")),
        (PEPE, "long", 1_793_363_366_923, Decimal("2000"), Decimal("0.0051200")),
    ]
    assert [message.exchange_ts_ms for message in parsed] == [1_793_363_346_256, 1_793_363_354_756, 1_793_363_367_035]


def test_legacy_topic_rejection_is_not_ours() -> None:
    # Колектор підписує лише `allLiquidation.*`: відповідь на чужий `req_id` нічого не змінює.
    assert parse_liquidation_message(MAINNET_LEGACY_REJECTED, symbols=_SYMBOLS).kind == "ignore"


def test_buy_snapshot_is_a_long_liquidation() -> None:
    message = parse_liquidation_message(_frame("Buy", "1.5", "0.04"), symbols=_SYMBOLS)
    event = message.events[0]
    assert event.position_side == "long"
    assert event.qty == Decimal("1.5")
    assert event.notional == Decimal("0.06")


def test_one_snapshot_can_carry_both_sides() -> None:
    raw = json.dumps(
        {
            "topic": "allLiquidation.ROSEUSDT",
            "type": "snapshot",
            "ts": 1739502303204,
            "data": [
                {"T": 1739502302929, "s": "ROSEUSDT", "S": "Sell", "v": "20000", "p": "0.04499"},
                {"T": 1739502303000, "s": "ROSEUSDT", "S": "Buy", "v": "1.5", "p": "0.04"},
            ],
        }
    )
    message = parse_liquidation_message(raw, symbols=_SYMBOLS)
    assert [event.position_side for event in message.events] == ["short", "long"]


@pytest.mark.parametrize(
    "raw",
    [
        '{"topic":"allLiquidation.ROSEUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"ROSEUSDT","S":"Hold","v":"1","p":"1"}]}',
        '{"topic":"allLiquidation.ROSEUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"ROSEUSDT","S":"Buy","v":"0","p":"1"}]}',
        '{"topic":"allLiquidation.ROSEUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"ROSEUSDT","S":"Buy","v":"1","p":"1"}]}'.replace(
            '"v":"1"', '"v":1'
        ),
        '{"topic":"allLiquidation.ROSEUSDT","type":"delta","ts":1,"data":[]}',
        "not-json",
        "",
    ],
)
def test_broken_frame_of_our_topic_is_unusable(raw: str) -> None:
    message = parse_liquidation_message(raw, symbols=_SYMBOLS)
    assert message.kind == "unusable"
    assert message.events == ()


def test_foreign_symbol_is_ignored() -> None:
    raw = OFFICIAL_SELL.replace("ROSEUSDT", "DOGEUSDT")
    assert parse_liquidation_message(raw, symbols=_SYMBOLS).kind == "ignore"


def test_subscribe_reply_is_matched_to_the_symbol_by_req_id() -> None:
    ack = '{"success":true,"ret_msg":"","conn_id":"c1","req_id":"allLiquidation.ROSEUSDT","op":"subscribe"}'
    message = parse_liquidation_message(ack, symbols=_SYMBOLS)
    assert message.kind == "subscribed"
    assert message.symbol == ROSE


def test_recorded_mainnet_rejection_names_the_symbol() -> None:
    message = parse_liquidation_message(MAINNET_REJECTED, symbols=_SYMBOLS)
    assert message.kind == "rejected"
    assert message.symbol == "NOSUCHCOIN/USDT:USDT"
    assert message.detail == "error:handler not found,topic:allLiquidation.NOSUCHCOINUSDT"


def test_success_with_our_topic_in_fail_topics_is_a_rejection() -> None:
    raw = (
        '{"success":true,"ret_msg":"","req_id":"allLiquidation.ROSEUSDT","op":"subscribe",'
        '"data":{"failTopics":["allLiquidation.ROSEUSDT"],"successTopics":[]}}'
    )
    message = parse_liquidation_message(raw, symbols=_SYMBOLS)
    assert message.kind == "rejected"
    assert message.symbol == ROSE


@pytest.mark.parametrize(
    "raw",
    [
        # Спільний запит (як до T1.10-фіксу): `req_id` не тема — не наша відповідь.
        MAINNET_REJECTED.replace('"req_id":"allLiquidation.NOSUCHCOINUSDT"', '"req_id":"all"'),
        '{"success":true,"ret_msg":"","op":"subscribe"}',
        '{"success":true,"ret_msg":"","req_id":"allLiquidation.DOGEUSDT","op":"subscribe"}',
    ],
    ids=["combined-request", "no-req-id", "foreign-topic"],
)
def test_subscribe_reply_without_our_req_id_is_ignored(raw: str) -> None:
    message = parse_liquidation_message(raw, symbols=_SYMBOLS)
    assert message.kind == "ignore"
    assert message.symbol is None


def test_recorded_mainnet_pong() -> None:
    assert parse_liquidation_message(MAINNET_PONG, symbols=_SYMBOLS).kind == "pong"
    assert parse_liquidation_message('{"op":"ping","success":true,"ret_msg":"pong"}', symbols=_SYMBOLS).kind == "pong"


@pytest.mark.asyncio
async def test_hanging_handshake_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    async def silent_upgrade(self: aiohttp.ClientSession, *args: object, **kwargs: object) -> object:
        # TCP і TLS є, а відповіді на upgrade немає: без таймауту `connect` чекав би вічно.
        await asyncio.Event().wait()
        raise AssertionError("upgrade не мав завершитись")

    monkeypatch.setattr(aiohttp.ClientSession, "ws_connect", silent_upgrade)
    connector = AiohttpLiquidationConnector(handshake_timeout_s=0.05)
    try:
        with pytest.raises(ExchangeUnavailable, match="connect failed"):
            await asyncio.wait_for(connector.connect(MAINNET_LINEAR_WS), 5)
    finally:
        await connector.aclose()


def test_handshake_timeout_must_be_positive_seconds() -> None:
    with pytest.raises(ValueError, match="handshake_timeout_s"):
        AiohttpLiquidationConnector(handshake_timeout_s=0.0)
    with pytest.raises(ValueError, match="handshake_timeout_s"):
        AiohttpLiquidationConnector(handshake_timeout_s=10)  # type: ignore[arg-type]


def test_topic_and_public_url() -> None:
    assert liquidation_topic("BTC/USDT:USDT") == "allLiquidation.BTCUSDT"
    assert liquidation_stream_url(ExchangeMode.DEMO) == MAINNET_LINEAR_WS
    assert liquidation_stream_url(ExchangeMode.LIVE) == MAINNET_LINEAR_WS
    assert liquidation_stream_url(ExchangeMode.TESTNET) == TESTNET_LINEAR_WS
