"""Постановка задач воркера з командного рядка (T1.11, рішення `docs/decisions/0023-market-data-bootstrap.md`).

`python -m app.jobs.enqueue market-data` ставить у чергу три задачі, які виконує воркер (`python -m app.jobs.worker`):

- `refresh_instruments` — кеш правил інструментів режиму `demo` (крок лота, тік, мінімальний номінал, лістинг) для
  розміру позиції в дослідженнях; лише поки ботів цього режиму немає (паузи ботів зводить торговий процес, T1.7);
- `backfill_candles` — свічки 1h і funding для торгового символу і всіх `RESEARCH_SYMBOLS`;
- `backfill_derivatives` — open interest, premium, mark/index і long/short для `RESEARCH_SYMBOLS`.

Backfill — від `BACKFILL_START_MS`. Задачі інкрементальні (курсор — `max(час)` у БД, вставка `ON CONFLICT DO NOTHING`),
тож повторний запуск докачує лише нове й не створює дублів. Поки задача з тим самим `dedup_key` у `QUEUED`/`RUNNING`,
друга не ставиться: повертається id наявної.

Команда працює лише з БД: мережі, ключів біржі й rate limiter у неї немає. До біржі ходить воркер — зі своєю
часткою IP-ліміту (ТЗ §6.5). Свічки за розкладом і далі качатиме торговий процес (T5, `MarketDataSync`); ця команда —
спосіб отримати історію для етапу 2 раніше.

Коди виходу: 0 — задачі в черзі; 1 — необроблений виняток; 2 — аргументи (argparse); 11 — БД недоступна;
15 — конфіг невалідний (немає `POSTGRES_PASSWORD`).
"""

from __future__ import annotations

import argparse
import asyncio
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.db import create_engine, session_factory
from app.core.logging import configure_logging, get_logger, shutdown_logging
from app.data.backfill import backfill_symbols, enqueue_backfill_job
from app.data.derivatives import derivative_symbols, enqueue_derivatives_job
from app.jobs.worker import enqueue_instruments_job

EXIT_OK: Final = 0
EXIT_UNCAUGHT: Final = 1
EXIT_DB_UNAVAILABLE: Final = 11
EXIT_CONFIG_INVALID: Final = 15

COMMAND_MARKET_DATA: Final = "market-data"

_LOG = get_logger("app.jobs.enqueue")


@dataclass(frozen=True, slots=True)
class MarketDataJobs:
    """Id задач у черзі (нових або вже активних) і символи, для яких їх просили."""

    instruments_job_id: uuid.UUID
    candles_job_id: uuid.UUID
    candle_symbols: tuple[str, ...]
    derivatives_job_id: uuid.UUID
    derivative_symbols: tuple[str, ...]
    start_ms: int


async def enqueue_market_data(session: AsyncSession, settings: Settings) -> MarketDataJobs:
    """`refresh_instruments`, `backfill_candles` і `backfill_derivatives` у транзакції викликача. Мережі немає."""
    candles = backfill_symbols(trading_symbol=settings.trading_symbol, research_symbols=settings.research_symbols)
    derivatives = derivative_symbols(settings.research_symbols)
    start_ms = settings.backfill_start_ms
    instruments_job = await enqueue_instruments_job(session, symbols=candles)
    candles_job = await enqueue_backfill_job(session, symbols=candles, start_ms=start_ms)
    derivatives_job = await enqueue_derivatives_job(session, symbols=derivatives, start_ms=start_ms)
    return MarketDataJobs(
        instruments_job_id=instruments_job,
        candles_job_id=candles_job,
        candle_symbols=candles,
        derivatives_job_id=derivatives_job,
        derivative_symbols=derivatives,
        start_ms=start_ms,
    )


async def run(settings: Settings) -> int:
    """Одна транзакція: усі задачі або жодної."""
    try:
        url = settings.database_url()
    except ValueError:
        _LOG.error("enqueue_config_invalid", reason="POSTGRES_PASSWORD")
        return EXIT_CONFIG_INVALID
    engine = create_engine(url)
    try:
        sessions = session_factory(engine)
        async with sessions() as session, session.begin():
            jobs = await enqueue_market_data(session, settings)
    except (OSError, SQLAlchemyError) as exc:
        _LOG.error("enqueue_db_unavailable", error_type=type(exc).__name__)
        return EXIT_DB_UNAVAILABLE
    finally:
        await engine.dispose()
    _LOG.info(
        "market_data_enqueued",
        instruments_job_id=str(jobs.instruments_job_id),
        candles_job_id=str(jobs.candles_job_id),
        candle_symbols=list(jobs.candle_symbols),
        derivatives_job_id=str(jobs.derivatives_job_id),
        derivative_symbols=list(jobs.derivative_symbols),
        start_ms=jobs.start_ms,
        hint="виконає воркер: python -m app.jobs.worker",
    )
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входу `python -m app.jobs.enqueue`."""
    _parse_args(argv)
    try:
        settings = Settings()
    except ValidationError:
        # Текст помилки без значень (`hide_input_in_errors`), але логера ще немає: лише код виходу.
        return EXIT_CONFIG_INVALID
    configure_logging(settings, process="enqueue")
    try:
        return asyncio.run(run(settings))
    except (SystemExit, KeyboardInterrupt):
        raise
    except BaseException:  # noqa: BLE001 — маскований лог і код 1; текст винятку в stderr без маски не потрапляє
        _LOG.critical("uncaught_exception", exc_info=True)
        return EXIT_UNCAUGHT
    finally:
        shutdown_logging()


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m app.jobs.enqueue",
        description="Поставити задачі воркера. Лише БД: мережі й ключів біржі немає.",
        allow_abbrev=False,
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        COMMAND_MARKET_DATA,
        help="кеш інструментів і історія свічок 1h, funding і деривативів від BACKFILL_START_MS (виконає воркер)",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


if __name__ == "__main__":
    raise SystemExit(main())
