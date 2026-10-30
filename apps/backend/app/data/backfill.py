"""Інкрементальний backfill закритих свічок 1h і funding (ТЗ §4.1, §4.4, T1.4).

Символи — торговий плюс `research_symbols` (без дублів). Повторне завантаження нічого не змінює:
вставка `ON CONFLICT DO NOTHING`. Незакрита за **локальним** годинником свічка не записується, навіть якщо
біржа вже віддала її (годинник біржі може випереджати локальний до того, як T1.6 це зловить).

Короткий простій (не більше `INLINE_MAX_BARS` годин від курсора до останньої закритої свічки, і так само для
funding) докачується одразу — це виклик при старті і за розкладом (`MarketDataSync.run_if_due`). Довша історія
лише ставиться в `jobs` (`kind=backfill_candles`): її виконує `python -m app.jobs.worker` через `run_backfill_job`,
зі своєю часткою rate limit (ТЗ §6.5). `BackfillIncomplete` воркер записує як `FAILED`. Хвіст історії не
записується раніше за початок: курсор ряду — це `max(час)`, інакше дірка попереду стала б невидимою.

Мережа — поза транзакцією. Курсор ряду читається з БД на початку докачування, далі ряд іде сторінками свічок і
вікнами funding (`FUNDING_WINDOW_MS`), і кожна сторінка чи вікно комітиться окремо: обірване завантаження
продовжується з місця, де коміт уже відбувся. Вставка ділиться на порції `INSERT_CHUNK_ROWS` рядків — один запит
asyncpg не приймає більше 32767 параметрів.

Збій біржі на одному символі не зупиняє решту: прохід розкладу повертає його в `SyncResult.failed_symbols`, задача
воркера докачує інші символи і наприкінці кидає `BackfillIncomplete`. Розклад іде за монотонним годинником;
невдалий прохід повторюється через паузу `SYNC_RETRY_BASE_MS`, що подвоюється до інтервалу розкладу (з jitter).

Прогалини не інтерполюються. Після проходу свічок (і для символу, який цей прохід не качав) викликається
`detect_candle_gaps` по сітці до `settled_end_ms(now)`: дірка пишеться в `data_gaps`, а щойно закрита свічка в межах
`CANDLE_SETTLE_MS` ще не вважається діркою. Початок сітки — не раніше за `start_ms` і не раніше за першу годину
≥ `launch_time_ms` з кешу `instruments` (T1.7): період до лістингу діркою не є. Якщо такий рядок уже відкритий
(скан раніше йшов з `BACKFILL_START_MS`), наступний прохід закриває або обрізає його: авторитетне вікно
починається з конфігу, а пропущені свічки шукаються лише від лістингу. Якщо в кеші кілька режимів,
береться найраніший лістинг, щоб пізніший Testnet не відрізав історію mainnet (demo і live ділять одні свічки).
Немає рядка — сітка лишається з `start_ms`. Курсор `max(час)` за діркою позаду не повертається — закрити її повторним
проходом цей модуль не може, рядок лишається відкритим (T1.5). Зіткнення з іншим процесом на тому самому ряді
прогалин повторюється один раз, далі ряд пропускається до наступного проходу — решта символів докачується.
"""

from __future__ import annotations

import random
import re
import uuid
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Final, Protocol

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import InstrumentedAttribute

from app.core.clock import monotonic_ms, now_ms
from app.core.config import H1_MS, Settings
from app.core.db import SYMBOL_RE, Base
from app.core.logging import get_logger
from app.data.gaps import detect_candle_gaps, is_open_gap_conflict, settled_end_ms
from app.data.models import Candle as StoredCandle
from app.data.models import FundingRate as StoredFunding
from app.exchange.errors import ExchangeError
from app.exchange.instruments import candle_scan_start_ms
from app.exchange.models import Instrument as StoredInstrument
from app.exchange.rate_limit import Jitter
from app.exchange.types import Candle, FundingRate, Timeframe
from app.jobs.models import Job

EXCHANGE: Final = "bybit"
BACKFILL_TIMEFRAME: Final = Timeframe.H1
# Сторінка свічок за один виклик адаптера (Bybit kline — 1000; фейк віддає `limit` найраніших).
CANDLE_FETCH_LIMIT: Final = 1000
# Вікно funding за один виклик адаптера — 60 діб: при 8-годинному funding це 180 записів, менше за сторінку
# Bybit (200), тобто один запит; густіший інтервал адаптер ділить сам. Кожне вікно комітиться окремо.
FUNDING_WINDOW_MS: Final = 60 * 24 * H1_MS
# Рядків в одному INSERT. asyncpg: не більше 32767 параметрів на запит; свічка — 10 колонок, тобто 10 000.
INSERT_CHUNK_ROWS: Final = 1000
# Перша повторна спроба після невдалого проходу розкладу; далі пауза подвоюється, але не довша за інтервал.
SYNC_RETRY_BASE_MS: Final = 30_000
# Тиждень годинних свічок: коротший простій закриває цей процес, довша історія — задача воркера.
INLINE_MAX_BARS: Final = 168
JOB_KIND_BACKFILL: Final = "backfill_candles"
BACKFILL_DEDUP_KEY: Final = "backfill_candles"
# Більше число — пізніше в черзі воркера (модель `Job.priority`).
BACKFILL_JOB_PRIORITY: Final = 200
_ACTIVE_JOB_STATUSES: Final = ("QUEUED", "RUNNING")
# Спроб звести ряд прогалин, коли інший процес (воркер і торговий процес) зводить його одночасно.
GAP_SYNC_ATTEMPTS: Final = 2
_SYMBOL: Final = re.compile(SYMBOL_RE)
_LOG = get_logger("app.data")

SessionFactory = async_sessionmaker[AsyncSession]


class MarketDataSource(Protocol):
    """Читання свічок і funding. Повний `ExchangeAdapter` цей протокол задовольняє."""

    async def fetch_candles(
        self,
        *,
        symbol: str,
        timeframe: Timeframe,
        start_ms: int,
        end_ms: int | None = None,
        limit: int | None = None,
    ) -> list[Candle]: ...

    async def fetch_funding_history(
        self, *, symbol: str, start_ms: int, end_ms: int | None = None
    ) -> list[FundingRate]: ...


@dataclass(frozen=True)
class BackfillStats:
    """Скільки рядків реально додано (конфлікт ключа — не додавання)."""

    candles_inserted: int = 0
    funding_inserted: int = 0

    def __add__(self, other: BackfillStats) -> BackfillStats:
        return BackfillStats(
            candles_inserted=self.candles_inserted + other.candles_inserted,
            funding_inserted=self.funding_inserted + other.funding_inserted,
        )


@dataclass(frozen=True)
class SyncResult:
    """Підсумок одного проходу старту або розкладу.

    `failed_symbols` — символи, на яких біржа відповіла помилкою: їхні ряди продовжаться з курсора на наступному
    проході, решту символів прохід докачав.
    """

    inline: BackfillStats
    job_id: uuid.UUID | None
    job_symbols: tuple[str, ...]
    failed_symbols: tuple[str, ...]


class BackfillIncomplete(Exception):
    """Задачу воркера докачано не для всіх символів: на `failed_symbols` біржа відповіла помилкою.

    Решту символів докачано й закомічено. Воркер позначить задачу FAILED, наступний прохід розкладу поставить нову,
    і вона продовжить з курсорів.
    """

    def __init__(self, failed_symbols: tuple[str, ...], stats: BackfillStats) -> None:
        super().__init__(f"backfill не завершено для: {', '.join(failed_symbols)}")
        self.failed_symbols = failed_symbols
        self.stats = stats


def forming_open_ms(current_ms: int, timeframe: Timeframe = BACKFILL_TIMEFRAME) -> int:
    """`open_time_ms` свічки, що ще формується. Закрита історія — це `[..., цей час)`."""
    _require_ms("now_ms", current_ms)
    return (current_ms // timeframe.duration_ms) * timeframe.duration_ms


def candles_closed_at(candles: Iterable[Candle], *, now: int) -> list[Candle]:
    """Свічки з `close_time_ms ≤ now`. Незакрита за локальним годинником у БД не пишеться."""
    _require_ms("now", now)
    return [candle for candle in candles if candle.close_time_ms <= now]


def funding_known_at(rates: Iterable[FundingRate], *, now: int, start_ms: int) -> list[FundingRate]:
    """Funding із `funding_time_ms` у `[start_ms, now]` — подія в майбутньому ще не відома."""
    _require_ms("now", now)
    _require_ms("start_ms", start_ms)
    return [rate for rate in rates if start_ms <= rate.funding_time_ms <= now]


def backfill_symbols(*, trading_symbol: str, research_symbols: Sequence[str]) -> tuple[str, ...]:
    """Торговий символ першим, далі символи навчання, кожен один раз."""
    symbols: list[str] = []
    for symbol in (trading_symbol, *research_symbols):
        if symbol not in symbols:
            symbols.append(symbol)
    if not symbols:
        raise ValueError("немає символів для backfill")
    return tuple(symbols)


def backfill_is_due(*, last_run_ms: int | None, now: int, interval_ms: int) -> bool:
    """Перший виклик (старт) — завжди; далі — коли минув інтервал.

    `MarketDataSync` передає сюди монотонний час (`monotonic_ms()`), тож стрибок системного годинника розклад не
    зсуває. Для будь-якого джерела часу `now < last_run_ms` — не час.
    """
    _require_ms("now", now)
    if type(interval_ms) is not int or interval_ms < 1:
        raise ValueError("interval_ms має бути int ≥ 1")
    if last_run_ms is None:
        return True
    _require_ms("last_run_ms", last_run_ms)
    return now - last_run_ms >= interval_ms


def backfill_job_params(symbols: Sequence[str], start_ms: int) -> dict[str, Any]:
    """Параметри задачі `backfill_candles`. `end` немає: його визначає годинник у момент виконання."""
    return {"symbols": list(symbols), "timeframe": BACKFILL_TIMEFRAME.value, "start_ms": start_ms}


def parse_backfill_job_params(params: Mapping[str, Any]) -> tuple[tuple[str, ...], int]:
    """Розібрати `jobs.params`. Чужий набір полів або не 1h — `ValueError` (воркер позначить задачу FAILED)."""
    if set(params) != {"symbols", "timeframe", "start_ms"}:
        raise ValueError("params backfill: очікуються рівно symbols, timeframe, start_ms")
    if params["timeframe"] != BACKFILL_TIMEFRAME.value:
        raise ValueError("backfill підтримує лише таймфрейм 1h")
    raw_symbols = params["symbols"]
    if type(raw_symbols) is not list or not raw_symbols or any(type(symbol) is not str for symbol in raw_symbols):
        raise ValueError("symbols має бути непорожнім списком рядків")
    symbols = tuple(raw_symbols)
    if len(set(symbols)) != len(symbols):
        raise ValueError("symbols: дублікати")
    for symbol in symbols:
        if _SYMBOL.fullmatch(symbol) is None:
            raise ValueError(f"symbol не в канонічному форматі CCXT: {symbol}")
    start_ms = params["start_ms"]
    if type(start_ms) is not int or start_ms < 0:
        raise ValueError("start_ms має бути int мс ≥ 0")
    if start_ms % H1_MS != 0:
        raise ValueError("start_ms не вирівняний за 1h")
    return symbols, start_ms


async def backfill_symbol(
    sessions: SessionFactory,
    source: MarketDataSource,
    *,
    symbol: str,
    start_ms: int,
    candles: bool = True,
    funding: bool = True,
    fetch_limit: int = CANDLE_FETCH_LIMIT,
    funding_window_ms: int = FUNDING_WINDOW_MS,
) -> BackfillStats:
    """Докачати один символ від курсора БД (не раніше `start_ms`) до останньої закритої свічки і відомого funding.

    `candles=False` / `funding=False` — цей ряд не чіпати (його докачає задача воркера, щоб не перестрибнути курсор).
    Збій біржі (`ExchangeError`) летить далі; сторінки й вікна до нього вже закомічені.
    """
    _require_ms("start_ms", start_ms)
    if type(fetch_limit) is not int or fetch_limit < 1:
        raise ValueError("fetch_limit має бути int ≥ 1")
    if type(funding_window_ms) is not int or funding_window_ms < 1:
        raise ValueError("funding_window_ms має бути int ≥ 1")
    if _SYMBOL.fullmatch(symbol) is None:
        raise ValueError(f"symbol не в канонічному форматі CCXT: {symbol}")
    current = now_ms()
    inserted_candles = 0
    inserted_funding = 0
    if candles:
        async with _transaction(sessions) as session:
            candle_floor = await _candle_floor(session, symbol=symbol, floor_ms=start_ms)
        try:
            inserted_candles = await _backfill_candles(
                sessions, source, symbol=symbol, floor_ms=candle_floor, now=current, fetch_limit=fetch_limit
            )
        except ExchangeError:
            # Сторінки до збою вже в БД: дірка до курсора має бути видна live, навіть якщо хвіст не докачано.
            await _record_candle_gaps(sessions, symbol=symbol, floor_ms=start_ms, now=current)
            raise
        await _record_candle_gaps(sessions, symbol=symbol, floor_ms=start_ms, now=current)
    if funding:
        inserted_funding = await _backfill_funding(
            sessions, source, symbol=symbol, floor_ms=start_ms, now=current, window_ms=funding_window_ms
        )
    if inserted_candles or inserted_funding:
        _LOG.info(
            "backfill_symbol",
            symbol=symbol,
            candles_inserted=inserted_candles,
            funding_inserted=inserted_funding,
        )
    return BackfillStats(candles_inserted=inserted_candles, funding_inserted=inserted_funding)


async def run_backfill_job(
    sessions: SessionFactory, source: MarketDataSource, params: Mapping[str, Any]
) -> BackfillStats:
    """Виконати задачу `backfill_candles`. Статус рядка `jobs` не змінює — це робить воркер (T1.8).

    Збій біржі на одному символі не зупиняє інші: їх докачано, а наприкінці летить `BackfillIncomplete`.
    """
    symbols, start_ms = parse_backfill_job_params(params)
    total, failed = await _backfill_each(
        sessions,
        source,
        [_Series(symbol=symbol, candles=True, funding=True) for symbol in symbols],
        start_ms=start_ms,
        fetch_limit=CANDLE_FETCH_LIMIT,
    )
    if failed:
        raise BackfillIncomplete(failed, total)
    return total


async def enqueue_backfill_job(session: AsyncSession, *, symbols: Sequence[str], start_ms: int) -> uuid.UUID:
    """Поставити задачу, якщо такої активної ще немає. Повертає id існуючої або нової.

    Поки задача в `QUEUED`/`RUNNING`, друга з тим самим `dedup_key` не створюється. Параметри вже поставленої
    не переписуються: після її завершення наступний прохід поставить актуальний список символів.
    """
    if not symbols:
        raise ValueError("немає символів для задачі backfill")
    _require_ms("start_ms", start_ms)
    existing = await _active_job_id(session)
    if existing is not None:
        return existing
    job = Job(
        kind=JOB_KIND_BACKFILL,
        priority=BACKFILL_JOB_PRIORITY,
        params=backfill_job_params(symbols, start_ms),
        dedup_key=BACKFILL_DEDUP_KEY,
        progress_message=None,
        result=None,
        error=None,
    )
    try:
        async with session.begin_nested():
            session.add(job)
            await session.flush()
    except IntegrityError:
        found = await _active_job_id(session)
        if found is None:
            raise
        return found
    return job.id


async def sync_market_data(
    sessions: SessionFactory,
    source: MarketDataSource,
    *,
    symbols: Sequence[str],
    start_ms: int,
    inline_max_bars: int = INLINE_MAX_BARS,
    fetch_limit: int = CANDLE_FETCH_LIMIT,
) -> SyncResult:
    """Один прохід: короткі ряди докачати зараз, довгі — однією задачею воркера.

    Задача ставиться до мережі (в одній транзакції з читанням курсорів), тож збій біржі її не скасовує. Символ, на
    якому біржа відповіла помилкою, потрапляє в `failed_symbols`, решта докачується. Інші винятки летять далі.
    """
    if not symbols:
        raise ValueError("немає символів для backfill")
    if type(inline_max_bars) is not int or inline_max_bars < 1:
        raise ValueError("inline_max_bars має бути int ≥ 1")
    _require_ms("start_ms", start_ms)
    current = now_ms()
    end = forming_open_ms(current)
    step = BACKFILL_TIMEFRAME.duration_ms
    inline: list[_Series] = []
    queued: list[str] = []
    async with _transaction(sessions) as session:
        for symbol in symbols:
            candle_floor = await _candle_floor(session, symbol=symbol, floor_ms=start_ms)
            candle_at = await _candle_resume_ms(session, symbol, floor_ms=candle_floor, step=step)
            funding_at = await _funding_resume_ms(session, symbol, floor_ms=start_ms)
            series = _Series(
                symbol=symbol,
                candles=candle_at < end and (end - candle_at) // step <= inline_max_bars,
                funding=funding_at <= current and current - funding_at <= inline_max_bars * step,
            )
            if series.candles or series.funding:
                inline.append(series)
            candle_far = candle_at < end and not series.candles
            funding_far = funding_at <= current and not series.funding
            if candle_far or funding_far:
                queued.append(symbol)
        job_id: uuid.UUID | None = None
        if queued:
            job_id = await enqueue_backfill_job(session, symbols=queued, start_ms=start_ms)
    if job_id is not None:
        _LOG.info("backfill_enqueued", job_id=str(job_id), symbols=queued, start_ms=start_ms)
    total, failed = await _backfill_each(sessions, source, inline, start_ms=start_ms, fetch_limit=fetch_limit)
    # Символ, чиї свічки цей прохід качав, уже звірений у `backfill_symbol`. Решта (черга воркера, лише funding,
    # ряд що наздогнав) теж звіряється: інакше дірка позаду курсора лишилась би невидимою.
    recorded = {item.symbol for item in inline if item.candles}
    for symbol in symbols:
        if symbol not in recorded:
            await _record_candle_gaps(sessions, symbol=symbol, floor_ms=start_ms, now=current)
    return SyncResult(inline=total, job_id=job_id, job_symbols=tuple(queued), failed_symbols=failed)


class MarketDataSync:
    """Старт і розклад: перший `run_if_due` виконується одразу, наступні — після `interval_ms`.

    Час розкладу — монотонний (`monotonic_ms()`): стрибок системного годинника його не зсуває. Невдалий прохід
    (виняток або непорожній `failed_symbols`) повторюється не на кожному виклику, а через паузу: `SYNC_RETRY_BASE_MS`,
    далі вдвічі довше, не довше за `interval_ms`; фактична пауза — jitter у `[пауза / 2, пауза]`. Успішний прохід
    повертає звичайний інтервал.
    """

    def __init__(
        self,
        sessions: SessionFactory,
        source: MarketDataSource,
        *,
        symbols: Sequence[str],
        start_ms: int,
        interval_ms: int,
        inline_max_bars: int = INLINE_MAX_BARS,
        fetch_limit: int = CANDLE_FETCH_LIMIT,
        jitter: Jitter | None = None,
    ) -> None:
        if type(interval_ms) is not int or interval_ms < 1:
            raise ValueError("interval_ms має бути int ≥ 1")
        self._sessions = sessions
        self._source = source
        self._symbols = tuple(symbols)
        self._start_ms = start_ms
        self._interval_ms = interval_ms
        self._inline_max_bars = inline_max_bars
        self._fetch_limit = fetch_limit
        self._jitter = jitter if jitter is not None else _system_jitter()
        # Монотонний час початку останнього проходу і пауза до наступного.
        self._last_run_ms: int | None = None
        self._delay_ms = interval_ms
        self._failures = 0

    @classmethod
    def from_settings(cls, settings: Settings, sessions: SessionFactory, source: MarketDataSource) -> MarketDataSync:
        """Символи, початок історії й інтервал — з `Settings`."""
        return cls(
            sessions,
            source,
            symbols=backfill_symbols(
                trading_symbol=settings.trading_symbol, research_symbols=settings.research_symbols
            ),
            start_ms=settings.backfill_start_ms,
            interval_ms=settings.backfill_interval_s * 1000,
        )

    async def run_if_due(self) -> SyncResult | None:
        """Виконати прохід, якщо настав час. `None` — ще рано. Виняток проходу летить далі, повтор — після паузи."""
        started = monotonic_ms()
        if not backfill_is_due(last_run_ms=self._last_run_ms, now=started, interval_ms=self._delay_ms):
            return None
        # До першого `await`: паралельний виклик побачить, що прохід уже йде, і не запустить другий.
        self._last_run_ms = started
        try:
            result = await sync_market_data(
                self._sessions,
                self._source,
                symbols=self._symbols,
                start_ms=self._start_ms,
                inline_max_bars=self._inline_max_bars,
                fetch_limit=self._fetch_limit,
            )
        except Exception as exc:
            self._schedule_retry(reason=type(exc).__name__)
            raise
        if result.failed_symbols:
            self._schedule_retry(reason="failed_symbols")
        else:
            self._failures = 0
            self._delay_ms = self._interval_ms
        return result

    def _schedule_retry(self, *, reason: str) -> None:
        self._failures += 1
        # Зсув замість `2 ** n`: лишається int; 16 кроків — уже більше за будь-який інтервал розкладу.
        step = SYNC_RETRY_BASE_MS << min(self._failures - 1, 16)
        high = min(self._interval_ms, step)
        low = high // 2
        chosen = self._jitter(low, high)
        self._delay_ms = chosen if type(chosen) is int and low <= chosen <= high else high
        _LOG.warning("backfill_sync_retry", reason=reason, failures=self._failures, retry_in_ms=self._delay_ms)


@dataclass(frozen=True)
class _Series:
    symbol: str
    candles: bool
    funding: bool


def _require_ms(name: str, value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} має бути int мс ≥ 0")
    return value


def _align_forward(ms: int, step: int) -> int:
    remainder = ms % step
    if remainder == 0:
        return ms
    return ms + (step - remainder)


async def _candle_floor(session: AsyncSession, *, symbol: str, floor_ms: int) -> int:
    """Не раніше за `floor_ms` і не раніше за найраніший лістинг символу в кеші.

    Кілька режимів (demo/live — одні свічки mainnet, testnet може відрізнятися): береться мінімум, щоб пізніший
    лістинг не сховав справжню історію. Немає рядка — `floor_ms` (період до лістингу лишиться діркою, поки кеш
    не заповнить `InstrumentCache.refresh`).
    """
    launches = list(
        await session.scalars(
            select(StoredInstrument.launch_time_ms).where(
                StoredInstrument.exchange == EXCHANGE, StoredInstrument.symbol == symbol
            )
        )
    )
    if not launches:
        return floor_ms
    launch = min(launches)
    if len(set(launches)) > 1:
        _LOG.info("instrument_launch_modes_differ", symbol=symbol, launch_time_ms=launch, rows=len(launches))
    return candle_scan_start_ms(floor_ms, launch, step_ms=BACKFILL_TIMEFRAME.duration_ms)


def _system_jitter() -> Jitter:
    rng = random.Random()  # noqa: S311 — jitter паузи повтору backfill, не секрет і не ключ

    def jitter(low: int, high: int) -> int:
        if high <= low:
            return low
        return low + rng.randrange(high - low + 1)

    return jitter


async def _backfill_each(
    sessions: SessionFactory,
    source: MarketDataSource,
    series: Sequence[_Series],
    *,
    start_ms: int,
    fetch_limit: int,
) -> tuple[BackfillStats, tuple[str, ...]]:
    """Докачати ряди по черзі. Збій біржі на символі не зупиняє наступні; повертає ще й символи, що впали."""
    total = BackfillStats()
    failed: list[str] = []
    for item in series:
        try:
            total += await backfill_symbol(
                sessions,
                source,
                symbol=item.symbol,
                start_ms=start_ms,
                candles=item.candles,
                funding=item.funding,
                fetch_limit=fetch_limit,
            )
        except ExchangeError:
            # Уже залоговано з типом помилки (`backfill_*_failed`); ряд продовжиться з курсора наступного разу.
            failed.append(item.symbol)
    if failed:
        _LOG.warning("backfill_incomplete", failed_symbols=failed)
    return total, tuple(failed)


@asynccontextmanager
async def _transaction(sessions: SessionFactory) -> AsyncIterator[AsyncSession]:
    async with sessions() as session, session.begin():
        yield session


async def _record_candle_gaps(sessions: SessionFactory, *, symbol: str, floor_ms: int, now: int) -> None:
    """Записати дірки свічок `[перша сітка ≥ floor, settled_end_ms(now))`. Ряд свічок не змінюється.

    Зіткнення з іншим процесом на `uq_data_gaps_open` повторюється (`GAP_SYNC_ATTEMPTS`). Якщо й повтор зіткнувся,
    ряд щойно звів той процес: цей прохід іде далі, наступний зведе ряд знову. Інакше гонка зупиняла б backfill
    решти символів. Інші помилки летять далі.
    """
    step = BACKFILL_TIMEFRAME.duration_ms
    configured = _align_forward(floor_ms, step)
    async with _transaction(sessions) as session:
        listed = await _candle_floor(session, symbol=symbol, floor_ms=floor_ms)
    listed = _align_forward(listed, step)
    end = settled_end_ms(now, BACKFILL_TIMEFRAME)
    if configured >= end:
        return
    # Дірки шукаємо від лістингу. Авторитетний скан починається раніше — з конфігу: уже відкритий
    # рядок `[BACKFILL_START, launch)` тоді закривається, а не лишається «поза сканом».
    candle_from = min(listed, end)
    for attempt in range(1, GAP_SYNC_ATTEMPTS + 1):
        try:
            async with _transaction(sessions) as session:
                await detect_candle_gaps(
                    session,
                    exchange=EXCHANGE,
                    symbol=symbol,
                    timeframe=BACKFILL_TIMEFRAME,
                    start_ms=candle_from,
                    end_ms=end,
                    history_from_ms=configured,
                )
        except IntegrityError as exc:
            if not is_open_gap_conflict(exc):
                raise
            _LOG.warning("data_gap_sync_conflict", symbol=symbol, attempt=attempt, attempts=GAP_SYNC_ATTEMPTS)
            continue
        return


async def _active_job_id(session: AsyncSession) -> uuid.UUID | None:
    found = await session.scalar(
        select(Job.id).where(Job.dedup_key == BACKFILL_DEDUP_KEY, Job.status.in_(_ACTIVE_JOB_STATUSES))
    )
    if found is None:
        return None
    # Драйвер може віддати свій підклас UUID: порівнювати треба значення, не клас.
    if isinstance(found, uuid.UUID):
        return uuid.UUID(int=found.int)
    try:
        return uuid.UUID(str(found))
    except ValueError as exc:
        raise TypeError(f"jobs.id: очікувався UUID, отримано {type(found).__name__}") from exc


async def _candle_resume_ms(session: AsyncSession, symbol: str, *, floor_ms: int, step: int) -> int:
    stored = await session.scalar(
        select(func.max(StoredCandle.open_time_ms)).where(
            StoredCandle.exchange == EXCHANGE,
            StoredCandle.symbol == symbol,
            StoredCandle.timeframe == BACKFILL_TIMEFRAME.value,
        )
    )
    if stored is None:
        return _align_forward(floor_ms, step)
    if type(stored) is not int:
        raise TypeError(f"open_time_ms: очікувався int, отримано {type(stored).__name__}")
    return max(stored + step, _align_forward(floor_ms, step))


async def _funding_resume_ms(session: AsyncSession, symbol: str, *, floor_ms: int) -> int:
    stored = await session.scalar(
        select(func.max(StoredFunding.funding_time_ms)).where(
            StoredFunding.exchange == EXCHANGE, StoredFunding.symbol == symbol
        )
    )
    if stored is None:
        return floor_ms
    if type(stored) is not int:
        raise TypeError(f"funding_time_ms: очікувався int, отримано {type(stored).__name__}")
    return max(stored + 1, floor_ms)


async def _backfill_candles(
    sessions: SessionFactory,
    source: MarketDataSource,
    *,
    symbol: str,
    floor_ms: int,
    now: int,
    fetch_limit: int,
) -> int:
    step = BACKFILL_TIMEFRAME.duration_ms
    end = forming_open_ms(now)
    async with _transaction(sessions) as session:
        cursor = await _candle_resume_ms(session, symbol, floor_ms=floor_ms, step=step)
    inserted = 0
    while cursor < end:
        try:
            batch = await source.fetch_candles(
                symbol=symbol,
                timeframe=BACKFILL_TIMEFRAME,
                start_ms=cursor,
                end_ms=end,
                limit=fetch_limit,
            )
        except ExchangeError as exc:
            _LOG.warning("backfill_candles_failed", symbol=symbol, start_ms=cursor, error=type(exc).__name__)
            raise
        accepted = _accept_candles(batch, symbol=symbol, cursor=cursor, end=end, now=now)
        if not accepted:
            break
        async with _transaction(sessions) as session:
            inserted += await _insert_candles(session, accepted)
        advanced = accepted[-1].open_time_ms + step
        if advanced <= cursor:
            break
        cursor = advanced
        if len(batch) < fetch_limit:
            break
    return inserted


def _accept_candles(batch: Sequence[Candle], *, symbol: str, cursor: int, end: int, now: int) -> list[Candle]:
    closed = candles_closed_at(
        (
            candle
            for candle in batch
            if candle.symbol == symbol
            and candle.timeframe is BACKFILL_TIMEFRAME
            and cursor <= candle.open_time_ms < end
        ),
        now=now,
    )
    by_open: dict[int, Candle] = {}
    for candle in closed:
        by_open.setdefault(candle.open_time_ms, candle)
    return [by_open[open_ms] for open_ms in sorted(by_open)]


async def _insert_candles(session: AsyncSession, candles: Sequence[Candle]) -> int:
    rows = [
        {
            "exchange": EXCHANGE,
            "symbol": candle.symbol,
            "timeframe": candle.timeframe.value,
            "open_time_ms": candle.open_time_ms,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "is_closed": True,
        }
        for candle in candles
    ]
    return await _insert_new(session, StoredCandle, rows, key=StoredCandle.open_time_ms)


async def _backfill_funding(
    sessions: SessionFactory, source: MarketDataSource, *, symbol: str, floor_ms: int, now: int, window_ms: int
) -> int:
    async with _transaction(sessions) as session:
        cursor = await _funding_resume_ms(session, symbol, floor_ms=floor_ms)
    inserted = 0
    # Вікна `[cursor, window_end)` йдуть впритул до `now` включно; кожне комітиться окремо.
    while cursor <= now:
        window_end = min(cursor + window_ms, now + 1)
        try:
            batch = await source.fetch_funding_history(symbol=symbol, start_ms=cursor, end_ms=window_end)
        except ExchangeError as exc:
            _LOG.warning("backfill_funding_failed", symbol=symbol, start_ms=cursor, error=type(exc).__name__)
            raise
        accepted = _accept_funding(batch, symbol=symbol, cursor=cursor, end=window_end, now=now)
        if accepted:
            async with _transaction(sessions) as session:
                inserted += await _insert_funding(session, accepted)
        cursor = window_end
    return inserted


def _accept_funding(batch: Sequence[FundingRate], *, symbol: str, cursor: int, end: int, now: int) -> list[FundingRate]:
    known = funding_known_at(
        (rate for rate in batch if rate.symbol == symbol and rate.funding_time_ms < end), now=now, start_ms=cursor
    )
    by_time: dict[int, FundingRate] = {}
    for rate in known:
        by_time.setdefault(rate.funding_time_ms, rate)
    return [by_time[ts] for ts in sorted(by_time)]


async def _insert_funding(session: AsyncSession, rates: Sequence[FundingRate]) -> int:
    rows = [
        {
            "exchange": EXCHANGE,
            "symbol": rate.symbol,
            "funding_time_ms": rate.funding_time_ms,
            "rate": rate.rate,
        }
        for rate in rates
    ]
    return await _insert_new(session, StoredFunding, rows, key=StoredFunding.funding_time_ms)


async def _insert_new(
    session: AsyncSession, table: type[Base], rows: Sequence[dict[str, Any]], *, key: InstrumentedAttribute[int]
) -> int:
    """`INSERT … ON CONFLICT DO NOTHING` порціями `INSERT_CHUNK_ROWS`; повертає кількість реально доданих рядків."""
    inserted = 0
    for offset in range(0, len(rows), INSERT_CHUNK_ROWS):
        chunk = list(rows[offset : offset + INSERT_CHUNK_ROWS])
        statement = pg_insert(table).values(chunk).on_conflict_do_nothing().returning(key)
        result = await session.execute(statement)
        inserted += len(result.scalars().all())
    return inserted
