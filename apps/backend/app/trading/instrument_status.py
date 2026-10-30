"""Пауза бота, коли інструмент не торгується (ТЗ §6.1, §15, T1.7).

`INSTRUMENT_NOT_TRADING` додається до `pause_reasons`. `RUNNING` стає `PAUSED`. Інші статуси не змінюються:
входи там і так заборонені, а `EMERGENCY_STOP` / `ERROR` / `STOPPED` цей модуль не знімає. Причина лишається
в масиві, тож вихід у `RUNNING` (T5.1) неможливий, поки її не знято — CHECK `running_has_no_reason`.

Коли інструмент знову `Trading`, причину знімають. `PAUSED` без жодної причини, що лишилась, повертається в
`RUNNING` (вихід автоматичний, ТЗ §6.1). Інші причини й інші статуси не чіпаються. SL цей модуль не знімає.
Тому при старті процесу знімок викликається лише після переведення ботів у `STARTING` (T5.1): інакше `PAUSED`
лише з цією причиною став би `RUNNING` до стартової звірки (ТЗ §11.1).
Лог пише `log_instrument_pauses` після коміту знімка: відкат не лишає запису про паузу, якої в БД немає.

Telegram для `instrument_not_trading` — T5.6. Зараз попередження пишеться в лог.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import now_ms
from app.core.logging import get_logger
from app.trading.models import PAUSE_REASONS, Bot

_LOG = get_logger("app.trading")

INSTRUMENT_NOT_TRADING = "INSTRUMENT_NOT_TRADING"
_PAUSE_ATTEMPTS = 2


class InstrumentPauseConflict(Exception):
    """`bots.version` змінився під час запису паузи. Транзакцію треба відкотити й повторити знімок."""

    def __init__(self, bot_id: uuid.UUID) -> None:
        super().__init__(f"bots.version змінився під час паузи інструмента: {bot_id}")
        self.bot_id = bot_id


@dataclass(frozen=True)
class PauseChange:
    """Один бот, чий статус або причини змінила синхронізація інструмента."""

    bot_id: uuid.UUID
    symbol: str
    status_before: str
    status_after: str
    reasons_before: tuple[str, ...]
    reasons_after: tuple[str, ...]


def instrument_pause_transition(
    *, status: str, pause_reasons: Sequence[str], tradable: bool
) -> tuple[str, tuple[str, ...]]:
    """Наступні `status` і `pause_reasons`. Без запису в БД.

    Неторгованість: додати `INSTRUMENT_NOT_TRADING`; з `RUNNING` перейти в `PAUSED`.
    Торгованість: прибрати цю причину; `PAUSED` без причин, що лишились, — знову `RUNNING`.
    """
    if type(status) is not str or not status:
        raise ValueError("status має бути непорожній рядок")
    if type(tradable) is not bool:
        raise TypeError("tradable має бути bool")
    reasons = tuple(pause_reasons)
    if any(type(reason) is not str or reason not in PAUSE_REASONS for reason in reasons):
        raise ValueError("pause_reasons містить невідому причину")
    has = INSTRUMENT_NOT_TRADING in reasons
    if not tradable:
        updated = reasons if has else (*reasons, INSTRUMENT_NOT_TRADING)
        # `RUNNING` з причиною заборонений CHECK-ом. Навіть якщо причина вже стоїть — у `PAUSED`.
        if status == "RUNNING":
            return "PAUSED", updated
        return status, updated
    if not has:
        return status, reasons
    updated = tuple(reason for reason in reasons if reason != INSTRUMENT_NOT_TRADING)
    if status == "PAUSED" and not updated:
        return "RUNNING", updated
    return status, updated


async def apply_instrument_pauses(
    session: AsyncSession,
    *,
    mode: str,
    tradable_by_symbol: Mapping[str, bool],
    now_ms_value: int | None = None,
) -> tuple[PauseChange, ...]:
    """Записати паузу інструмента ботам цього `mode` і символів зі мапи. Ідемпотентно.

    Бот, чий перехід нічого не міняє, не отримує новий `version`. Конфлікт версії повторюється один раз;
    другий — `InstrumentPauseConflict` (відкат транзакції знімка).
    """
    if not tradable_by_symbol:
        return ()
    if any(type(flag) is not bool for flag in tradable_by_symbol.values()):
        raise TypeError("tradable_by_symbol: значення мають бути bool")
    current = now_ms() if now_ms_value is None else now_ms_value
    if type(current) is not int or isinstance(current, bool) or current < 0:
        raise ValueError("now_ms має бути int мс ≥ 0")
    symbols = tuple(tradable_by_symbol)
    bots = list((await session.scalars(select(Bot).where(Bot.mode == mode, Bot.symbol.in_(symbols)))).all())
    changes: list[PauseChange] = []
    for bot in bots:
        change = await _apply_one(session, bot_id=bot.id, tradable=tradable_by_symbol[bot.symbol], now_ms_value=current)
        if change is not None:
            changes.append(change)
    return tuple(changes)


async def _apply_one(
    session: AsyncSession, *, bot_id: uuid.UUID, tradable: bool, now_ms_value: int
) -> PauseChange | None:
    for _attempt in range(_PAUSE_ATTEMPTS):
        bot = await session.get(Bot, bot_id, populate_existing=True)
        if bot is None:
            return None
        before = tuple(bot.pause_reasons)
        status, reasons = instrument_pause_transition(status=bot.status, pause_reasons=before, tradable=tradable)
        if status == bot.status and reasons == before:
            return None
        updated_id = (
            await session.execute(
                update(Bot)
                .where(Bot.id == bot.id, Bot.version == bot.version)
                .values(
                    status=status,
                    pause_reasons=list(reasons),
                    version=bot.version + 1,
                    updated_at_ms=now_ms_value,
                )
                .returning(Bot.id)
            )
        ).scalar_one_or_none()
        if updated_id is not None:
            return PauseChange(
                bot_id=bot.id,
                symbol=bot.symbol,
                status_before=bot.status,
                status_after=status,
                reasons_before=before,
                reasons_after=reasons,
            )
    raise InstrumentPauseConflict(bot_id)


def log_instrument_pauses(changes: Sequence[PauseChange], *, mode: str, tradable_by_symbol: Mapping[str, bool]) -> None:
    """Лог після коміту знімка: відкат транзакції не лишає запису про паузу, якої в БД немає."""
    for change in changes:
        _log_change(change, mode=mode, tradable=tradable_by_symbol[change.symbol])


def _log_change(change: PauseChange, *, mode: str, tradable: bool) -> None:
    payload = {
        "bot_id": str(change.bot_id),
        "symbol": change.symbol,
        "mode": mode,
        "status": change.status_after,
        "pause_reasons": list(change.reasons_after),
    }
    if tradable:
        _LOG.info("instrument_trading_resumed", **payload)
    else:
        _LOG.warning("instrument_not_trading", **payload)
