"""Ринкові дані: свічки, прогалини, funding, дані деривативів (ТЗ §4.1, §4.2, §4.4, §15, T0.4).

`candles` і таблиці деривативів — hypertables TimescaleDB (створюються в міграції через `create_hypertable`).
Вставка — `ON CONFLICT DO NOTHING` (повторне завантаження ідемпотентне). Інтерполяція заборонена:
пропуски лише записуються в `data_gaps`.
"""

from __future__ import annotations

from sqlalchemy import CheckConstraint, Identity, Index, PrimaryKeyConstraint, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base, CreatedMs, Money, TimeMs, enum_check, symbol_check
from app.exchange.models import EXCHANGES

# Джерело прогалини (ТЗ §15): ряд даних, у якому її виявлено. Mark- і index-свічки — окремі ряди з окремими
# прогалинами (`docs/decisions/0005-schema-t0-4.md`): докачування одного не закриває прогалину іншого.
GAP_SOURCES: tuple[str, ...] = (
    "candles",
    "open_interest",
    "premium_index",
    "mark_candles",
    "index_candles",
    "long_short",
    "liquidations",
)

# Частковий UNIQUE відкритих прогалин ряду. `gaps.is_open_gap_conflict` за цим іменем відрізняє гонку двох процесів,
# що зводять один ряд, від інших порушень цілісності.
OPEN_GAP_INDEX = "uq_data_gaps_open"

# Ряди `mark_index_candles`: mark-ціна (тригер SL, симулятор угоди) та index-ціна — окремі ендпоінти Bybit.
PRICE_TYPES: tuple[str, ...] = ("mark", "index")

# Таймфрейм/інтервал: число + одиниця (`1h`, `4h`, `1d`, `5min` для long/short ratio).
# Той самий вираз, що в CHECK схеми; `gaps.py` звіряє рядок із ним до запису.
INTERVAL_RE = "^[0-9]+(m|min|h|d|w)$"


def _interval_check(column: str) -> CheckConstraint:
    return CheckConstraint(f"{column} ~ '{INTERVAL_RE}'", name=f"{column}_format")


def _ohlc_checks(positive: bool) -> tuple[CheckConstraint, ...]:
    checks = (
        CheckConstraint("high >= low", name="high_ge_low"),
        CheckConstraint("high >= open AND high >= close", name="high_ge_open_close"),
        CheckConstraint("low <= open AND low <= close", name="low_le_open_close"),
    )
    if positive:
        return (*checks, CheckConstraint("low > 0", name="low_positive"))
    return checks


class Candle(Base):
    """Закрита свічка (ТЗ §4.1). Незакриті свічки в БД не потрапляють (`is_closed` завжди true)."""

    __tablename__ = "candles"
    __table_args__ = (
        PrimaryKeyConstraint("exchange", "symbol", "timeframe", "open_time_ms"),
        enum_check("exchange", EXCHANGES),
        symbol_check(),
        _interval_check("timeframe"),
        CheckConstraint("open_time_ms >= 0", name="open_time_ms_non_negative"),
        CheckConstraint("is_closed", name="only_closed"),
        CheckConstraint("volume >= 0", name="volume_non_negative"),
        *_ohlc_checks(positive=True),
    )

    exchange: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    timeframe: Mapped[str] = mapped_column(Text)
    open_time_ms: Mapped[TimeMs]
    open: Mapped[Money]
    high: Mapped[Money]
    low: Mapped[Money]
    close: Mapped[Money]
    volume: Mapped[Money]
    is_closed: Mapped[bool] = mapped_column(default=True)


class DataGap(Base):
    """Прогалина в даних (ТЗ §4.2). Інтервал напіввідкритий: `[from_ms, to_ms)`.

    Повторне виявлення тієї самої **відкритої** прогалини не створює дубля (частковий `UNIQUE ... WHERE
    resolved_at_ms IS NULL`, AGENTS.md інваріант 11): відкритий рядок зберігає id і `detected_at_ms`, а його межі
    оновлюються, коли дірка росте чи скорочується (`gaps.sync_open_gaps`); закрита прогалина не заважає
    зареєструвати нову з тим самим `from_ms`.
    `resolved_at_ms` — коли дані докачано й перевірено; прогалина, якої на біржі немає, лишається відкритою.
    """

    __tablename__ = "data_gaps"
    __table_args__ = (
        enum_check("source", GAP_SOURCES),
        enum_check("exchange", EXCHANGES),
        symbol_check(),
        _interval_check("timeframe"),
        CheckConstraint("from_ms >= 0 AND to_ms > from_ms", name="range_valid"),
        CheckConstraint("resolved_at_ms IS NULL OR resolved_at_ms >= detected_at_ms", name="resolved_after_detected"),
        Index(
            OPEN_GAP_INDEX,
            "source",
            "exchange",
            "symbol",
            "timeframe",
            "from_ms",
            unique=True,
            postgresql_where=text("resolved_at_ms IS NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    source: Mapped[str] = mapped_column(Text)
    exchange: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    timeframe: Mapped[str] = mapped_column(Text)
    from_ms: Mapped[TimeMs]
    to_ms: Mapped[TimeMs]
    detected_at_ms: Mapped[CreatedMs]
    resolved_at_ms: Mapped[int | None]


class FundingRate(Base):
    """Історія funding rate (ТЗ §4.1, §4.4). Ставка може бути від'ємною."""

    __tablename__ = "funding_rates"
    __table_args__ = (
        PrimaryKeyConstraint("exchange", "symbol", "funding_time_ms"),
        enum_check("exchange", EXCHANGES),
        symbol_check(),
        CheckConstraint("funding_time_ms >= 0", name="funding_time_ms_non_negative"),
    )

    exchange: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    funding_time_ms: Mapped[TimeMs]
    rate: Mapped[Money]


def _series_args(*extra: object, price_type: bool = False) -> tuple[object, ...]:
    pk = (
        ("exchange", "symbol", "price_type", "interval", "ts_ms")
        if price_type
        else ("exchange", "symbol", "interval", "ts_ms")
    )
    return (
        PrimaryKeyConstraint(*pk),
        enum_check("exchange", EXCHANGES),
        symbol_check(),
        _interval_check("interval"),
        CheckConstraint("ts_ms >= 0", name="ts_ms_non_negative"),
        *extra,
    )


class OpenInterest(Base):
    """Open interest (ТЗ §4.4). `ts_ms` — час точки, як його повертає біржа."""

    __tablename__ = "open_interest"
    __table_args__ = _series_args(CheckConstraint("open_interest >= 0", name="open_interest_non_negative"))

    exchange: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    interval: Mapped[str] = mapped_column(Text)
    ts_ms: Mapped[TimeMs]
    open_interest: Mapped[Money]


class PremiumIndex(Base):
    """Свічки premium index (ТЗ §4.4). Значення можуть бути від'ємними. `ts_ms` — час відкриття свічки."""

    __tablename__ = "premium_index"
    __table_args__ = _series_args(*_ohlc_checks(positive=False))

    exchange: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    interval: Mapped[str] = mapped_column(Text)
    ts_ms: Mapped[TimeMs]
    open: Mapped[Money]
    high: Mapped[Money]
    low: Mapped[Money]
    close: Mapped[Money]


class MarkIndexCandle(Base):
    """Mark- та index-свічки (ТЗ §4.4). `price_type` входить у PK (`docs/decisions/0005-schema-t0-4.md`)."""

    __tablename__ = "mark_index_candles"
    __table_args__ = _series_args(enum_check("price_type", PRICE_TYPES), *_ohlc_checks(positive=True), price_type=True)

    exchange: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    price_type: Mapped[str] = mapped_column(Text)
    interval: Mapped[str] = mapped_column(Text)
    ts_ms: Mapped[TimeMs]
    open: Mapped[Money]
    high: Mapped[Money]
    low: Mapped[Money]
    close: Mapped[Money]


class LongShortRatio(Base):
    """Long/short ratio (ТЗ §4.4): частки рахунків у long і short, кожна в [0, 1]."""

    __tablename__ = "long_short_ratio"
    __table_args__ = _series_args(
        CheckConstraint("buy_ratio >= 0 AND buy_ratio <= 1", name="buy_ratio_range"),
        CheckConstraint("sell_ratio >= 0 AND sell_ratio <= 1", name="sell_ratio_range"),
    )

    exchange: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    interval: Mapped[str] = mapped_column(Text)
    ts_ms: Mapped[TimeMs]
    buy_ratio: Mapped[Money]
    sell_ratio: Mapped[Money]


class LiquidationsHourly(Base):
    """Ліквідації, агреговані по годинах окремо для long і short (ТЗ §4.4). Лише вперед — колектор T1.10.

    `*_qty` — у базовій валюті, `*_notional` — у USDT. `ts_ms` — початок години.
    """

    __tablename__ = "liquidations_hourly"
    __table_args__ = _series_args(
        CheckConstraint(
            "long_count >= 0 AND short_count >= 0 AND long_qty >= 0 AND short_qty >= 0"
            " AND long_notional >= 0 AND short_notional >= 0",
            name="non_negative",
        ),
    )

    exchange: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    interval: Mapped[str] = mapped_column(Text)
    ts_ms: Mapped[TimeMs]
    long_count: Mapped[int]
    long_qty: Mapped[Money]
    long_notional: Mapped[Money]
    short_count: Mapped[int]
    short_qty: Mapped[Money]
    short_notional: Mapped[Money]
