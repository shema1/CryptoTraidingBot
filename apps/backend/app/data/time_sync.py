"""Контроль часу (ТЗ §4.3, T1.6): розбіжність з часом біржі та стрибки системного годинника.

Обидва механізми блокують лише **нові входи** (fail-closed, AGENTS.md інваріант 6): стан читається перед входом,
а захист і закриття позицій його не питають.

1. **Розбіжність з біржею** — `check_drift()` при старті (ТЗ §11.1 п. 6) і кожні `DRIFT_CHECK_INTERVAL_MS` (10 хв).
   Один вимір — `fetch_time` між двома читаннями годинника: локальний час — середина запиту (системний на початку +
   половина RTT за монотонним), похибка — половина RTT, округлена вгору, плюс розбіжність швидкостей годинників з
   віком виміру (`CLOCK_RATE_TOLERANCE_PPM`). З `DRIFT_SAMPLES` вимірів береться той, що з найменшим RTT; вимір з RTT
   понад `MAX_SAMPLE_RTT_MS` нічого не доводить і відкидається; уся перевірка — не довше `DRIFT_CHECK_TIMEOUT_MS`.
   Межа розбіжності = |зсув| + похибка; не менше `CLOCK_DRIFT_LIMIT_MS` — `CLOCK_DRIFT` і попередження в лог (ТЗ:
   «більше 1 с» — блок, повернення — лише коли «менша за 1 с»; дрейф міг бути й меншим, але підтвердити це не
   вдалося). Невдала перевірка лишає попередній результат, доки йому не більше `DRIFT_RESULT_MAX_AGE_MS`; далі —
   `CLOCK_UNVERIFIED`. Поки годинник не підтверджено (дрейф, збій, після стрибка чи `request_recheck`), перевірка
   повторюється кожні `DRIFT_RECHECK_MS`. Між перевірками зсув оцінюється безперервно: якщо системний годинник
   зрушив відносно монотонного (кроки NTP, ручна зміна до 1 с), оцінка це враховує без нового запиту.

2. **Стрибок годинника** — `tick()` щосекунди: системний годинник між двома тіками пройшов інакше, ніж монотонний,
   більш ніж на `CLOCK_JUMP_LIMIT_MS` (пробудження після сну на macOS / Linux — монотонний годинник ОС у сні стоїть;
   ручна зміна; крок NTP). Це нова «епоха» і `ClockJump` для переходу ботів у `RECONCILING` (ТЗ §6.1, T5.1). Вимір
   з попередньої епохи вже не чинний (`CLOCK_UNVERIFIED` до нового виміру), а `CLOCK_JUMP_UNRECONCILED` для області
   звірки (акаунта) знімає лише `confirm_reconciled(ticket)` з квитком `begin_reconcile(scope)`, узятим до старту
   успішної звірки цієї області. Так правило ТЗ «повертається в `RUNNING`, лише коли розбіжність < 1 с і звірка
   пройшла» тримає сам годинник, навіть якщо обробник стрибка впав, і окремо для кожного акаунта (ТЗ §8.1).

Обмеження: пауза віртуальної машини (WSL2 під час сну Windows), коли обидва локальні годинники стоять разом, для
`tick()` непомітна — її ловить лише час біржі: планова перевірка або `request_recheck()` від торгового клієнта
(відмова за `recvWindow`, реконект WS; T3.2, T3.5). Перевірка на реальній WSL2 — T5.10.

Годинник не коригується: Bybit радить підписувати запити локальним часом із синхронізацією NTP, розбіжність
виправляє ОС. `recvWindow` приватних запитів — `Settings.recv_window_ms` (T3.2).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final, Protocol

from app.core.clock import CLOCK_DRIFT_LIMIT_MS, monotonic_ms, now_ms
from app.core.logging import get_logger
from app.exchange.errors import ExchangeError, MalformedResponse

# ТЗ §4.3: «при стрибку понад 1 с» — той самий допуск, що й для розбіжності.
CLOCK_JUMP_LIMIT_MS: Final = CLOCK_DRIFT_LIMIT_MS
# ТЗ §4.3: монотонний і системний годинники порівнюються щосекунди, час біржі — при старті й кожні 10 хв.
JUMP_CHECK_INTERVAL_MS: Final = 1_000
DRIFT_CHECK_INTERVAL_MS: Final = 600_000
# Поки годинник не підтверджено, повтор частіше: після сну ОС синхронізує годинник за секунди–хвилини, а нові входи
# весь цей час заблоковані. Три `fetch_time` раз на 30 с — дрібниця проти IP-ліміту (`RECONCILE`, група `MARKET`).
DRIFT_RECHECK_MS: Final = 30_000
# Уся перевірка (черга лімітера, повтори адаптера) — не довше; інакше завислий запит зупинив би нові перевірки.
DRIFT_CHECK_TIMEOUT_MS: Final = DRIFT_RECHECK_MS
# Скільки чинний вимір без підтвердження (усі планові перевірки падають) — дві планові перевірки.
DRIFT_RESULT_MAX_AGE_MS: Final = 2 * DRIFT_CHECK_INTERVAL_MS
DRIFT_SAMPLES: Final = 3
# Похибка виміру — половина RTT. Довший запит (черга лімітера, повтори адаптера) не відрізнить 0 від 1 с.
MAX_SAMPLE_RTT_MS: Final = CLOCK_DRIFT_LIMIT_MS
# Монотонний годинник (кварц, NTP його не підправляє) і системний (його підправляє NTP) ідуть з різною швидкістю:
# оцінка зсуву між вимірами старіє на стільки мільйонних часток віку (100 ppm — 120 мс за 20 хв).
CLOCK_RATE_TOLERANCE_PPM: Final = 100

_log = get_logger(__name__)


class TimeSource(Protocol):
    """Межа біржі для контролю часу: `ExchangeAdapter.fetch_time` (у т.ч. `BybitAdapter` до T3.2)."""

    async def fetch_time(self) -> int: ...


class ClockBlockReason(StrEnum):
    """Чому годинник блокує нові входи (коди для журналу рішень і UI)."""

    # Немає чинного виміру: ще не перевірено, вимір застарів, зроблений до стрибка чи до `request_recheck`.
    UNVERIFIED = "CLOCK_UNVERIFIED"
    # Межа розбіжності з біржею (|зсув| + похибка) не менша за `CLOCK_DRIFT_LIMIT_MS`.
    DRIFT = "CLOCK_DRIFT"
    # Був стрибок, а успішну звірку цієї області після нього ще не підтверджено.
    JUMP_UNRECONCILED = "CLOCK_JUMP_UNRECONCILED"


@dataclass(frozen=True, slots=True)
class DriftSample:
    """Один вимір: `offset_ms` = час біржі − локальний (> 0 — локальний годинник відстає)."""

    offset_ms: int
    rtt_ms: int
    # Середина запиту за системним і монотонним годинниками.
    wall_ms: int
    mono_ms: int
    # Покоління на початку виміру: стрибок і `request_recheck` його збільшують, і старіший вимір стає не чинним.
    generation: int

    @property
    def uncertainty_ms(self) -> int:
        return (self.rtt_ms + 1) // 2


@dataclass(frozen=True, slots=True)
class ClockJump:
    """Стрибок системного годинника: `skew_ms` = Δ системного − Δ монотонного (> 0 — уперед)."""

    skew_ms: int
    wall_delta_ms: int
    mono_delta_ms: int
    # Номер епохи, що почалася з цим стрибком (1, 2, …).
    epoch: int


@dataclass(frozen=True, slots=True)
class ReconcileTicket:
    """Квиток звірки області `scope` (акаунта), виданий до її старту: `epoch` — епоха стрибків на той момент."""

    scope: str
    epoch: int


@dataclass(frozen=True, slots=True)
class ClockState:
    """Знімок стану для області `scope` (`None` — область, що ще не звірялась).

    `offset_ms` — поточна оцінка за останнім виміром (`None` — вимірів ще не було).
    """

    block_reasons: tuple[ClockBlockReason, ...]
    offset_ms: int | None
    uncertainty_ms: int | None
    drift_bound_ms: int | None
    sample_age_ms: int | None
    jump_epoch: int
    scope: str | None
    reconciled_epoch: int

    @property
    def entries_allowed(self) -> bool:
        return not self.block_reasons


JumpHandler = Callable[[ClockJump], Awaitable[None]]
SleepMs = Callable[[int], Awaitable[None]]


class ClockGuard:
    """Стан годинника процесу. Один на процес (торговий; колектор ліквідацій — для меж години); див. docstring модуля.

    Перед новим входом — `state(scope).entries_allowed` для області звірки бота (акаунта). Звірка: квиток
    `begin_reconcile(scope)` до її старту, після успіху — `confirm_reconciled(ticket)`.
    """

    def __init__(
        self, source: TimeSource, *, samples: int = DRIFT_SAMPLES, check_timeout_ms: int = DRIFT_CHECK_TIMEOUT_MS
    ) -> None:
        if type(samples) is not int or samples < 1:
            raise ValueError("samples має бути int ≥ 1")
        if type(check_timeout_ms) is not int or check_timeout_ms < 1:
            raise ValueError("check_timeout_ms має бути int ≥ 1")
        self._source = source
        self._samples = samples
        self._check_timeout_ms = check_timeout_ms
        self._sample: DriftSample | None = None
        self._generation = 0
        self._jump_epoch = 0
        self._reconciled: dict[str, int] = {}
        self._checks_in_flight = 0
        self._last_check_mono: int | None = None
        self._recheck_requested = False
        # Попередній тік детектора стрибків (перший тік порівнюється з моментом створення).
        self._tick_wall = now_ms()
        self._tick_mono = monotonic_ms()

    def state(self, scope: str | None = None) -> ClockState:
        """Стан для області звірки `scope`. Для рішення про вхід — лише з `scope`: без нього кожен стрибок блокує."""
        wall, mono = now_ms(), monotonic_ms()
        reasons: list[ClockBlockReason] = []
        offset: int | None = None
        uncertainty: int | None = None
        bound: int | None = None
        age: int | None = None
        sample = self._sample
        if sample is None:
            reasons.append(ClockBlockReason.UNVERIFIED)
        else:
            # Скільки системний годинник зрушив відносно монотонного з моменту виміру.
            shift = (wall - mono) - (sample.wall_ms - sample.mono_ms)
            offset = sample.offset_ms - shift
            age = mono - sample.mono_ms
            uncertainty = sample.uncertainty_ms + age * CLOCK_RATE_TOLERANCE_PPM // 1_000_000
            bound = abs(offset) + uncertainty
            if sample.generation != self._generation or age > DRIFT_RESULT_MAX_AGE_MS:
                reasons.append(ClockBlockReason.UNVERIFIED)
            elif bound >= CLOCK_DRIFT_LIMIT_MS:
                reasons.append(ClockBlockReason.DRIFT)
        reconciled = 0 if scope is None else self._reconciled.get(scope, 0)
        if reconciled < self._jump_epoch:
            reasons.append(ClockBlockReason.JUMP_UNRECONCILED)
        return ClockState(
            block_reasons=tuple(reasons),
            offset_ms=offset,
            uncertainty_ms=uncertainty,
            drift_bound_ms=bound,
            sample_age_ms=age,
            jump_epoch=self._jump_epoch,
            scope=scope,
            reconciled_epoch=reconciled,
        )

    def tick(self) -> ClockJump | None:
        """Порівняти системний і монотонний годинники з попереднім тіком. Викликати щосекунди (`run`)."""
        wall, mono = now_ms(), monotonic_ms()
        wall_delta, mono_delta = wall - self._tick_wall, mono - self._tick_mono
        self._tick_wall, self._tick_mono = wall, mono
        skew = wall_delta - mono_delta
        if abs(skew) <= CLOCK_JUMP_LIMIT_MS:
            return None
        self._jump_epoch += 1
        self._generation += 1
        self._recheck_requested = True
        jump = ClockJump(skew_ms=skew, wall_delta_ms=wall_delta, mono_delta_ms=mono_delta, epoch=self._jump_epoch)
        _log.warning(
            "clock_jump_detected",
            skew_ms=skew,
            wall_delta_ms=wall_delta,
            mono_delta_ms=mono_delta,
            epoch=jump.epoch,
            limit_ms=CLOCK_JUMP_LIMIT_MS,
            hint="нові входи заблоковано до свіжої перевірки часу й звірки",
        )
        return jump

    async def check_drift(self) -> ClockState:
        """Виміряти розбіжність з біржею зараз. Не кидає винятків, крім скасування: збій — у лог і в стан."""
        self._checks_in_flight += 1
        self._recheck_requested = False
        self._last_check_mono = monotonic_ms()
        try:
            samples, rtts, error = await self._measure()
        finally:
            self._checks_in_flight -= 1
        # Вимір, початий до стрибка чи `request_recheck`, годинник уже не підтверджує.
        fresh = [sample for sample in samples if sample.generation == self._generation]
        if fresh:
            best = min(fresh, key=lambda sample: sample.rtt_ms)
            # Паралельна перевірка могла вже записати новіший вимір.
            if self._sample is None or best.mono_ms >= self._sample.mono_ms:
                self._sample = best
            state = self.state()
            self._log_result(state, rtt_ms=best.rtt_ms, error=error)
            return state
        if error is not None:
            failure: dict[str, Any] = {"error_type": type(error).__name__}
            if not isinstance(error, ExchangeError | TimeoutError):
                # Не збій біржі чи дедлайн, а несподіваний виняток — потрібне трасування.
                failure["exc_info"] = error
            _log.warning("clock_drift_check_failed", **failure, hint="нові входи заблоковано, якщо вимір застарів")
        elif samples:
            # Стрибок або `request_recheck` посеред перевірки — повтор уже запитано.
            _log.warning("clock_drift_check_inconclusive", reason="invalidated_during_check", rtts_ms=rtts)
        else:
            _log.warning(
                "clock_drift_check_inconclusive", reason="high_rtt", rtts_ms=rtts, max_rtt_ms=MAX_SAMPLE_RTT_MS
            )
        return self.state()

    def drift_check_due(self) -> bool:
        """Чи пора `check_drift()`: старт, запит повтору, 10 хв після чинного виміру, 30 с — якщо не підтверджено."""
        if self._checks_in_flight:
            return False
        if self._last_check_mono is None or self._recheck_requested:
            return True
        reasons = self.state().block_reasons
        verified = ClockBlockReason.UNVERIFIED not in reasons and ClockBlockReason.DRIFT not in reasons
        pause = DRIFT_CHECK_INTERVAL_MS if verified else DRIFT_RECHECK_MS
        return monotonic_ms() - self._last_check_mono >= pause

    def request_recheck(self, *, reason: str) -> None:
        """Годинник під сумнівом: чинний вимір скасовується (`CLOCK_UNVERIFIED`), перевірка — якнайшвидше.

        Для торгового клієнта: біржа відхилила запит за `recvWindow` (retCode 10002, T3.2), реконект приватного WS
        (T3.5). Звірки не вимагає — це не стрибок.
        """
        self._generation += 1
        self._recheck_requested = True
        _log.warning("clock_recheck_requested", reason=reason, hint="нові входи заблоковано до свіжого виміру")

    def begin_reconcile(self, scope: str) -> ReconcileTicket:
        """Квиток звірки області `scope` (акаунта). Брати до старту звірки, щоб стрибок під час неї не загубився."""
        if type(scope) is not str or not scope:
            raise ValueError("scope має бути непорожнім рядком")
        return ReconcileTicket(scope=scope, epoch=self._jump_epoch)

    def confirm_reconciled(self, ticket: ReconcileTicket) -> None:
        """Звірку за квитком завершено успішно: стрибки до `ticket.epoch` для цієї області підтверджено.

        Стрибок під час звірки лишається непідтвердженим; старіший квиток нічого не змінює; чужий — `ValueError`.
        """
        if type(ticket) is not ReconcileTicket or not 0 <= ticket.epoch <= self._jump_epoch:
            raise ValueError("ticket має бути квитком begin_reconcile цього годинника")
        if ticket.epoch <= self._reconciled.get(ticket.scope, 0):
            return
        self._reconciled[ticket.scope] = ticket.epoch
        _log.info(
            "clock_jump_reconciled",
            scope=ticket.scope,
            epoch=ticket.epoch,
            unreconciled_jumps=self._jump_epoch - ticket.epoch,
        )

    async def run(self, *, on_jump: JumpHandler | None = None, sleep: SleepMs | None = None) -> None:
        """Цикл торгового процесу: `tick()` щосекунди, `check_drift()` за розкладом (T5.1 запускає й зупиняє).

        Перевірка й обробник стрибка — окремі задачі: повільна біржа чи завислий обробник не зупиняють детектор.
        Збій обробника пишеться в лог (`clock_jump_handler_failed`) — нові входи однаково заблоковані. Завершується
        лише скасуванням або винятком (`sleep`, помилка в коді) — і тоді контролю часу більше немає: викликач має
        блокувати нові входи.
        """
        pause = sleep if sleep is not None else _sleep_ms
        check: asyncio.Task[ClockState] | None = None
        async with asyncio.TaskGroup() as tasks:
            while True:
                jump = self.tick()
                if jump is not None and on_jump is not None:
                    tasks.create_task(_notify(on_jump, jump))
                if (check is None or check.done()) and self.drift_check_due():
                    check = tasks.create_task(self.check_drift())
                await pause(JUMP_CHECK_INTERVAL_MS)

    async def _measure(self) -> tuple[list[DriftSample], list[int], Exception | None]:
        """До `samples` вимірів підряд за `check_timeout_ms`; після першого збою решту не робить."""
        samples: list[DriftSample] = []
        rtts: list[int] = []
        deadline = asyncio.get_running_loop().time() + self._check_timeout_ms / 1000
        for _ in range(self._samples):
            generation = self._generation
            wall_start, mono_start = now_ms(), monotonic_ms()
            try:
                async with asyncio.timeout_at(deadline):
                    server: object = await self._source.fetch_time()
            except Exception as exc:  # noqa: BLE001 — повертається викликачу, той пише в лог; годинник не підтверджено
                return samples, rtts, exc
            rtt = monotonic_ms() - mono_start
            if type(server) is not int or server <= 0:
                return samples, rtts, MalformedResponse("fetch_time: час біржі не є int мс UTC")
            rtts.append(rtt)
            if rtt > MAX_SAMPLE_RTT_MS:
                continue
            half = rtt // 2
            samples.append(
                DriftSample(
                    offset_ms=server - (wall_start + half),
                    rtt_ms=rtt,
                    wall_ms=wall_start + half,
                    mono_ms=mono_start + half,
                    generation=generation,
                )
            )
        return samples, rtts, None

    @staticmethod
    def _log_result(state: ClockState, *, rtt_ms: int, error: Exception | None) -> None:
        offset = state.offset_ms
        fields: dict[str, Any] = {
            "offset_ms": offset,
            "rtt_ms": rtt_ms,
            "bound_ms": state.drift_bound_ms,
            "limit_ms": CLOCK_DRIFT_LIMIT_MS,
        }
        if error is not None:
            # Частина вимірів пройшла, наступний упав: результат є, але збій видно.
            fields["partial_error_type"] = type(error).__name__
        if ClockBlockReason.DRIFT in state.block_reasons:
            _log.warning(
                "clock_drift_exceeded",
                **fields,
                local_clock="behind" if offset is not None and offset > 0 else "ahead",
                hint="нові входи заблоковано; перевірте синхронізацію годинника ОС (NTP)",
            )
        else:
            _log.info("clock_drift_ok", **fields)


async def _notify(handler: JumpHandler, jump: ClockJump) -> None:
    try:
        await handler(jump)
    except Exception:  # noqa: BLE001 — лог; нові входи однаково заблоковані до свіжого виміру й звірки
        _log.error("clock_jump_handler_failed", epoch=jump.epoch, exc_info=True)


async def _sleep_ms(ms: int) -> None:
    await asyncio.sleep(ms / 1000)
