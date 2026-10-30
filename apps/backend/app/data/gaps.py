"""Виявлення прогалин без інтерполяції (ТЗ §4.2, T1.5).

Прогалина — напіввідкритий інтервал `[from_ms, to_ms)` пропущених точок сітки. Значення між точками не
вигадуються: детектор лише порівнює наявні мітки часу з очікуваною сіткою і записує дірки в `data_gaps`.

Повторний прохід тієї самої дірки не створює другого відкритого рядка (частковий UNIQUE). Нова дірка зводиться з
відкритими рядками, які вона перекриває: рядок зберігає id і час першого виявлення, а межі оновлюються (дірка
виросла, скоротилась з будь-якого боку). Середину докачано — друга частина стає новим рядком із тим самим часом
виявлення. Заповнена дірка отримує `resolved_at_ms`. `warning` — лише коли пропущених слотів побільшало; дірка, що
лише скоротилась, — `info`. Дірка, якої на біржі немає, лишається відкритою — наступний прохід знову її бачить.
Закритий рядок не заважає зареєструвати нову дірку з тією самою `from_ms`.

Остання свічка перевіряється лише через `CANDLE_SETTLE_MS` після закриття (`settled_end_ms`): адаптер віддає її з
запасом остаточності за часом біржі, а локальний годинник може розходитись із біржею до `CLOCK_DRIFT_LIMIT_MS`.

Запит «чи є прогалина у вікні» читає **відкриті** рядки. Вікно теж напіввідкрите `[start_ms, end_ms)`: дотик
межею (дірка закінчується рівно на `start` або починається рівно на `end`) — це не перетин. `False` означає
«відкритого рядка в цьому вікні немає», а не «ряд уже перевіряли». Перш ніж трактувати `False` як дозвіл
торгувати, вікно має пройти `detect_candle_gaps` (backfill робить це після проходу свічок).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Final

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import CLOCK_DRIFT_LIMIT_MS, now_ms
from app.core.db import SYMBOL_RE
from app.core.logging import get_logger
from app.data.models import GAP_SOURCES, INTERVAL_RE, OPEN_GAP_INDEX, DataGap
from app.data.models import Candle as StoredCandle
from app.exchange.bybit import KLINE_FINALITY_MARGIN_MS
from app.exchange.models import EXCHANGES
from app.exchange.types import Timeframe

# Джерело ряду свічок у `data_gaps.source` (ТЗ §15). Інші джерела пишуть T1.9 і T1.10 тим самим `sync_open_gaps`.
CANDLE_SOURCE: Final = "candles"
# Свічка, закрита менше ніж стільки тому за локальним годинником, ще не прогалина: адаптер віддає її лише через
# `KLINE_FINALITY_MARGIN_MS` після закриття за часом біржі, а годинник може відставати від біржі до
# `CLOCK_DRIFT_LIMIT_MS` (`app/core/clock.py`; більша розбіжність блокує нові входи — `app/data/time_sync.py`).
CANDLE_SETTLE_MS: Final = KLINE_FINALITY_MARGIN_MS + CLOCK_DRIFT_LIMIT_MS
_UNIQUE_VIOLATION: Final = "23505"
_LOG = get_logger("app.data")
_SYMBOL = re.compile(SYMBOL_RE)
_TIMEFRAME = re.compile(INTERVAL_RE)


@dataclass(frozen=True, slots=True)
class GapRange:
    """Напіввідкритий інтервал `[from_ms, to_ms)`: початок входить, кінець — ні."""

    from_ms: int
    to_ms: int

    def __post_init__(self) -> None:
        if type(self.from_ms) is not int or type(self.to_ms) is not int:
            raise ValueError("межі прогалини мають бути int мс")
        if self.from_ms < 0 or self.to_ms <= self.from_ms:
            raise ValueError("прогалина має задовольняти 0 ≤ from_ms < to_ms")


@dataclass(frozen=True, slots=True)
class GapSync:
    """Що змінилось у відкритих прогалинах одного ряду за один прохід.

    `opened` — дірка там, де відкритих рядків не було; `extended` — відома дірка, у якій пропущених слотів побільшало;
    `shrunk` — відома дірка лише скоротилась (або розпалась на частини); `resolved` — дані з'явились, рядок закрито;
    `merged` — рядок закрито, бо його дірка злилась з іншою (між ними зник слот).
    """

    opened: tuple[GapRange, ...]
    extended: tuple[GapRange, ...]
    shrunk: tuple[GapRange, ...]
    resolved: tuple[GapRange, ...]
    merged: tuple[GapRange, ...] = ()

    @property
    def changed(self) -> bool:
        return bool(self.opened or self.extended or self.shrunk or self.resolved or self.merged)


@dataclass(frozen=True, slots=True)
class _StoredGap:
    id: int
    gap: GapRange
    detected_at_ms: int


def find_gaps(present_ms: Iterable[int], *, start_ms: int, end_ms: int, step_ms: int) -> tuple[GapRange, ...]:
    """Дірки сітки `[start_ms, end_ms)` з кроком `step_ms`. Точки поза сіткою дірку не закривають.

    Порожній діапазон (`start_ms == end_ms`) — жодної дірки. Мітки поза `[start, end)`, дублікати і мітки не
    на кроці ігноруються: слот сітки закриває лише точна мітка цього слота. Нічого не добудовується.
    """
    start, end = _bounds("start_ms", start_ms, "end_ms", end_ms)
    step = _require_step(step_ms)
    if start % step != 0 or end % step != 0:
        raise ValueError("межі сітки не вирівняні за кроком")
    if start == end:
        return ()

    on_grid: set[int] = set()
    for raw in present_ms:
        ts = _require_ms("present_ms", raw)
        if ts < start or ts >= end or (ts - start) % step != 0:
            continue
        on_grid.add(ts)

    gaps: list[GapRange] = []
    expected = start
    for ts in sorted(on_grid):
        if ts > expected:
            gaps.append(GapRange(expected, ts))
        expected = ts + step
    if expected < end:
        gaps.append(GapRange(expected, end))
    return tuple(gaps)


def settled_end_ms(current_ms: int, timeframe: Timeframe = Timeframe.H1) -> int:
    """Кінець сітки, яку вже можна перевіряти на прогалини: відкриття першої свічки, що ще не остаточна з запасом.

    Свічка `[t, t + крок)` входить у сітку, лише коли `current_ms ≥ t + крок + CANDLE_SETTLE_MS`. До того її відсутність
    у БД — не прогалина: біржа могла ще не віддати її (запас остаточності, розбіжність годинників).
    """
    current = _require_ms("current_ms", current_ms)
    step = timeframe.duration_ms
    return (max(current - CANDLE_SETTLE_MS, 0) // step) * step


def is_open_gap_conflict(exc: IntegrityError) -> bool:
    """Чи це зіткнення двох процесів, що одночасно зводять один ряд (`uq_data_gaps_open`).

    Інше порушення цілісності (CHECK, інший ключ) — не гонка, а помилка: його не можна гасити повтором.
    """
    orig = exc.orig
    if orig is None or getattr(orig, "sqlstate", None) != _UNIQUE_VIOLATION:
        return False
    return getattr(orig.__cause__, "constraint_name", None) == OPEN_GAP_INDEX


def window_has_gap(gaps: Sequence[GapRange], *, start_ms: int, end_ms: int) -> bool:
    """Чи перетинає якась прогалина вікно `[start_ms, end_ms)`.

    Дотик межею не рахується: `[0, 10)` і вікно `[10, 20)` не перетинаються. Порожнє вікно — ні.
    """
    start, end = _bounds("start_ms", start_ms, "end_ms", end_ms)
    if start == end:
        return False
    return any(gap.from_ms < end and start < gap.to_ms for gap in gaps)


def plan_open_gaps(
    existing: Sequence[GapRange],
    detected: Sequence[GapRange],
    *,
    scan_start_ms: int,
    scan_end_ms: int,
) -> tuple[GapRange, ...]:
    """Відкриті прогалини після авторитетного скану `[scan_start_ms, scan_end_ms)`.

    Усередині скану істина — `detected`. Шматки наявних прогалин поза сканом зберігаються. Сусідні інтервали
    зливаються, тож дірка, що виросла за межу скану, лишається одним інтервалом із тією самою `from_ms`.
    Порожній скан нічого не змінює: він не авторитетний.
    """
    scan_start, scan_end = _bounds("scan_start_ms", scan_start_ms, "scan_end_ms", scan_end_ms)
    if scan_start == scan_end:
        if detected:
            raise ValueError("порожній скан не може містити прогалину")
        return tuple(sorted(existing, key=lambda gap: (gap.from_ms, gap.to_ms)))
    for gap in detected:
        if gap.from_ms < scan_start or gap.to_ms > scan_end:
            raise ValueError("виявлена прогалина поза межами скану")
    pieces: list[GapRange] = []
    for gap in existing:
        if gap.from_ms < scan_start:
            pieces.append(GapRange(gap.from_ms, min(gap.to_ms, scan_start)))
        if gap.to_ms > scan_end:
            right_from = max(gap.from_ms, scan_end)
            if right_from < gap.to_ms:
                pieces.append(GapRange(right_from, gap.to_ms))
    pieces.extend(detected)
    return _merge(pieces)


async def sync_open_gaps(
    session: AsyncSession,
    *,
    source: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    scan_start_ms: int,
    scan_end_ms: int,
    gaps: Sequence[GapRange],
    detected_at_ms: int | None = None,
) -> GapSync:
    """Звести відкриті рядки `data_gaps` цього ряду до `plan_open_gaps` і залогувати зміни.

    Кожна дірка плану бере відкритий рядок, який перекриває (спершу — з тією самою `from_ms`): id і `detected_at_ms`
    зберігаються, межі оновлюються. Друга частина розпалої дірки — новий рядок із часом виявлення батьківського.
    Рядок, який не перекриває жодна дірка, закривається (`resolved_at_ms`); перекритий, але не взятий — злитий.
    Повторний виклик із тим самим набором нічого не пише і не логує. `warning` — `data_gap` (нова дірка) і
    `data_gap_extended` (пропущених слотів побільшало); `info` — `data_gap_shrunk`, `data_gap_merged`,
    `data_gap_resolved`. Зіткнення з іншим процесом на `uq_data_gaps_open` — `IntegrityError`, див.
    `is_open_gap_conflict`.
    """
    _validate_series(source=source, exchange=exchange, symbol=symbol, timeframe=timeframe)
    detected_at = now_ms() if detected_at_ms is None else _require_ms("detected_at_ms", detected_at_ms)
    stored = await _load_open_gaps(session, source=source, exchange=exchange, symbol=symbol, timeframe=timeframe)
    planned = plan_open_gaps(
        tuple(row.gap for row in stored),
        gaps,
        scan_start_ms=scan_start_ms,
        scan_end_ms=scan_end_ms,
    )
    by_from = {row.gap.from_ms: row for row in stored}
    if len(by_from) != len(stored):
        raise RuntimeError("відкриті прогалини ряду мають різні from_ms")

    opened: list[GapRange] = []
    extended: list[tuple[GapRange, GapRange]] = []
    shrunk: list[tuple[GapRange, GapRange]] = []
    merged: list[GapRange] = []
    resolved: list[tuple[GapRange, int]] = []
    taken: set[int] = set()
    key = {"source": source, "exchange": exchange, "symbol": symbol, "timeframe": timeframe}

    for gap in planned:
        overlapping = [row for row in stored if _overlaps(row.gap, gap)]
        # Нові пропущені слоти — ті, яких не покривав жоден відкритий рядок. Лише вони варті `warning`.
        grew = _covered_ms(gap, [row.gap for row in overlapping]) < gap.to_ms - gap.from_ms
        # Рядок з тією самою `from_ms` завжди перекриває дірку і не може бути взятий попередньою: план не має
        # дотичних інтервалів. Тож перенесення меж іншого рядка ніколи не зіткнеться з ним на UNIQUE.
        row = by_from.get(gap.from_ms)
        if row is None or row.id in taken:
            row = next((candidate for candidate in overlapping if candidate.id not in taken), None)
        if row is None:
            # Друга частина розпалої дірки відома з моменту виявлення батьківської.
            known_since = min((candidate.detected_at_ms for candidate in overlapping), default=detected_at)
            session.add(DataGap(from_ms=gap.from_ms, to_ms=gap.to_ms, detected_at_ms=known_since, **key))
            if not overlapping:
                opened.append(gap)
            else:
                (extended if grew else shrunk).append((gap, overlapping[0].gap))
            continue
        taken.add(row.id)
        if row.gap == gap:
            continue
        await session.execute(update(DataGap).where(DataGap.id == row.id).values(from_ms=gap.from_ms, to_ms=gap.to_ms))
        (extended if grew else shrunk).append((gap, row.gap))

    for row in stored:
        if row.id in taken:
            continue
        # Годинник міг відкотитись назад (T1.6): закриття не раніше виявлення, інакше CHECK
        # `resolved_after_detected` зупинив би весь прохід синхронізації.
        closed_at = max(detected_at, row.detected_at_ms)
        await session.execute(update(DataGap).where(DataGap.id == row.id).values(resolved_at_ms=closed_at))
        if any(_overlaps(row.gap, gap) for gap in planned):
            merged.append(row.gap)
        else:
            resolved.append((row.gap, closed_at))

    await session.flush()
    for gap in opened:
        _LOG.warning("data_gap", from_ms=gap.from_ms, to_ms=gap.to_ms, **key)
    for gap, previous in extended:
        _LOG.warning(
            "data_gap_extended",
            from_ms=gap.from_ms,
            to_ms=gap.to_ms,
            previous_from_ms=previous.from_ms,
            previous_to_ms=previous.to_ms,
            **key,
        )
    for gap, previous in shrunk:
        _LOG.info(
            "data_gap_shrunk",
            from_ms=gap.from_ms,
            to_ms=gap.to_ms,
            previous_from_ms=previous.from_ms,
            previous_to_ms=previous.to_ms,
            **key,
        )
    for gap in merged:
        _LOG.info("data_gap_merged", from_ms=gap.from_ms, to_ms=gap.to_ms, **key)
    for gap, closed_at in resolved:
        _LOG.info("data_gap_resolved", from_ms=gap.from_ms, to_ms=gap.to_ms, resolved_at_ms=closed_at, **key)
    return GapSync(
        opened=tuple(opened),
        extended=tuple(gap for gap, _previous in extended),
        shrunk=tuple(gap for gap, _previous in shrunk),
        resolved=tuple(gap for gap, _closed_at in resolved),
        merged=tuple(merged),
    )


async def detect_candle_gaps(
    session: AsyncSession,
    *,
    exchange: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    timeframe: Timeframe = Timeframe.H1,
    history_from_ms: int | None = None,
) -> GapSync:
    """Знайти дірки закритих свічок у `[start_ms, end_ms)` і записати їх. Свічки не змінюються.

    `end_ms` — зазвичай `settled_end_ms(now)`: свічка, що формується, і щойно закрита в межах `CANDLE_SETTLE_MS` у
    сітку не входять.
    Мітка не на кроці таймфрейму слот не закриває і логується (`data_gap_off_grid`).
    `history_from_ms`, якщо раніше за `start_ms`, розширює авторитетний скан ліворуч: у `[history_from_ms, start_ms)`
    дірок свічок немає навмисно (період до лістингу). Відкритий рядок, що починається там, закривається або
    обрізається до `start_ms`. Без цього аргумента скан лишається `[start_ms, end_ms)`.
    """
    step = timeframe.duration_ms
    start, end = _bounds("start_ms", start_ms, "end_ms", end_ms)
    history = start if history_from_ms is None else _require_ms("history_from_ms", history_from_ms)
    if history > start:
        raise ValueError("history_from_ms пізніший за початок пошуку дірок")
    if history % step != 0 or start % step != 0 or end % step != 0:
        raise ValueError("межі сітки не вирівняні за кроком")
    if start == end:
        gaps: tuple[GapRange, ...] = ()
    else:
        present = await _candle_opens(
            session, exchange=exchange, symbol=symbol, timeframe=timeframe.value, start_ms=start, end_ms=end
        )
        off_grid = [ts for ts in present if (ts - start) % step != 0]
        if off_grid:
            _LOG.warning(
                "data_gap_off_grid",
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe.value,
                count=len(off_grid),
                sample_ms=off_grid[:3],
            )
        gaps = find_gaps(present, start_ms=start, end_ms=end, step_ms=step)
    return await sync_open_gaps(
        session,
        source=CANDLE_SOURCE,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe.value,
        scan_start_ms=history,
        scan_end_ms=end,
        gaps=gaps,
    )


async def open_gaps_in_window(
    session: AsyncSession,
    *,
    source: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
) -> tuple[GapRange, ...]:
    """Відкриті прогалини ряду, що перетинають `[start_ms, end_ms)`, за `from_ms`."""
    _validate_series(source=source, exchange=exchange, symbol=symbol, timeframe=timeframe)
    start, end = _bounds("start_ms", start_ms, "end_ms", end_ms)
    if start == end:
        return ()
    rows = await session.execute(
        select(DataGap.from_ms, DataGap.to_ms)
        .where(
            DataGap.source == source,
            DataGap.exchange == exchange,
            DataGap.symbol == symbol,
            DataGap.timeframe == timeframe,
            DataGap.resolved_at_ms.is_(None),
            DataGap.from_ms < end,
            DataGap.to_ms > start,
        )
        .order_by(DataGap.from_ms)
    )
    return tuple(GapRange(_as_int("from_ms", from_ms), _as_int("to_ms", to_ms)) for from_ms, to_ms in rows.all())


async def has_open_gap(
    session: AsyncSession,
    *,
    source: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
) -> bool:
    """Чи є відкрита прогалина у вікні `[start_ms, end_ms)` (ТЗ §4.2: навчання і live).

    Той самий предикат, що `window_has_gap`: `from_ms < end` і `to_ms > start`. Закрита прогалина не рахується.
    """
    found = await open_gaps_in_window(
        session,
        source=source,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    return bool(found)


def _overlaps(left: GapRange, right: GapRange) -> bool:
    return left.from_ms < right.to_ms and right.from_ms < left.to_ms


def _covered_ms(gap: GapRange, ranges: Sequence[GapRange]) -> int:
    """Скільки мс `gap` покрито `ranges` (перетини зливаються — перекриття не рахується двічі)."""
    clipped = [
        GapRange(max(item.from_ms, gap.from_ms), min(item.to_ms, gap.to_ms)) for item in ranges if _overlaps(item, gap)
    ]
    return sum(item.to_ms - item.from_ms for item in _merge(clipped))


def _merge(ranges: Sequence[GapRange]) -> tuple[GapRange, ...]:
    """Злити інтервали, що перетинаються або торкаються (`[0, 10)` + `[10, 20)` → `[0, 20)`)."""
    ordered = sorted(ranges, key=lambda gap: (gap.from_ms, gap.to_ms))
    merged: list[GapRange] = []
    for gap in ordered:
        if not merged or gap.from_ms > merged[-1].to_ms:
            merged.append(gap)
        elif gap.to_ms > merged[-1].to_ms:
            merged[-1] = GapRange(merged[-1].from_ms, gap.to_ms)
    return tuple(merged)


def _validate_series(*, source: str, exchange: str, symbol: str, timeframe: str) -> None:
    if source not in GAP_SOURCES:
        raise ValueError(f"невідоме джерело прогалини: {source}")
    if exchange not in EXCHANGES:
        raise ValueError(f"невідома біржа: {exchange}")
    if _SYMBOL.fullmatch(symbol) is None:
        raise ValueError(f"symbol не в канонічному форматі CCXT: {symbol}")
    if _TIMEFRAME.fullmatch(timeframe) is None:
        raise ValueError(f"timeframe не в форматі інтервалу: {timeframe}")


def _require_ms(name: str, value: object) -> int:
    # bool — підклас int: відхиляється. Секунди як float теж.
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} має бути int мс ≥ 0")
    return value


def _require_step(step_ms: object) -> int:
    if type(step_ms) is not int or step_ms < 1:
        raise ValueError("step_ms має бути int ≥ 1")
    return step_ms


def _bounds(start_name: str, start: object, end_name: str, end: object) -> tuple[int, int]:
    start_ms = _require_ms(start_name, start)
    end_ms = _require_ms(end_name, end)
    if end_ms < start_ms:
        raise ValueError(f"{end_name} раніше за {start_name}")
    return start_ms, end_ms


def _as_int(name: str, value: object) -> int:
    if type(value) is not int:
        raise TypeError(f"{name}: очікувався int, отримано {type(value).__name__}")
    return value


async def _load_open_gaps(
    session: AsyncSession, *, source: str, exchange: str, symbol: str, timeframe: str
) -> tuple[_StoredGap, ...]:
    rows = await session.execute(
        select(DataGap.id, DataGap.from_ms, DataGap.to_ms, DataGap.detected_at_ms)
        .where(
            DataGap.source == source,
            DataGap.exchange == exchange,
            DataGap.symbol == symbol,
            DataGap.timeframe == timeframe,
            DataGap.resolved_at_ms.is_(None),
        )
        .order_by(DataGap.from_ms)
    )
    stored: list[_StoredGap] = []
    for gap_id, from_ms, to_ms, detected_at in rows.all():
        stored.append(
            _StoredGap(
                id=_as_int("id", gap_id),
                gap=GapRange(_as_int("from_ms", from_ms), _as_int("to_ms", to_ms)),
                detected_at_ms=_as_int("detected_at_ms", detected_at),
            )
        )
    return tuple(stored)


async def _candle_opens(
    session: AsyncSession, *, exchange: str, symbol: str, timeframe: str, start_ms: int, end_ms: int
) -> list[int]:
    rows = await session.scalars(
        select(StoredCandle.open_time_ms).where(
            StoredCandle.exchange == exchange,
            StoredCandle.symbol == symbol,
            StoredCandle.timeframe == timeframe,
            StoredCandle.open_time_ms >= start_ms,
            StoredCandle.open_time_ms < end_ms,
        )
    )
    return [_as_int("open_time_ms", ts) for ts in rows.all()]
