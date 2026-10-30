"""Дочірній процес integration-тесту T1.8: той самий цикл, обробник не завершується сам.

`python -m tests.jobs.hold_worker` з каталогу `apps/backend`. Задачу `kind=hold` тримає, доки процес
не отримає SIGTERM (тоді цикл повертає її в `QUEUED`) або його не вб'ють (`RUNNING` лишається).
"""

from __future__ import annotations

import asyncio

from app.core.config import Settings
from app.core.logging import configure_logging, get_logger, shutdown_logging
from app.exchange.fake import FakeExchangeAdapter
from app.jobs.worker import EXIT_UNCAUGHT, JobContext, serve

_LOG = get_logger("app.jobs.worker")


async def _hold(_ctx: JobContext) -> None:
    await asyncio.Event().wait()


def main() -> int:
    settings = Settings()
    configure_logging(settings, process="worker")
    try:
        return asyncio.run(serve(settings, handlers={"hold": _hold}, source=FakeExchangeAdapter()))
    except (SystemExit, KeyboardInterrupt):
        raise
    except BaseException:  # noqa: BLE001 — як у бойовій точці входу: маскований лог і код 1
        _LOG.critical("uncaught_exception", exc_info=True)
        return EXIT_UNCAUGHT
    finally:
        shutdown_logging()


if __name__ == "__main__":
    raise SystemExit(main())
