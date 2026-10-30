"""Бектест із тим самим виконанням і тими самими лімітами, що live (ТЗ §5.2.3, §6.6).

Закриття — лише `simulate_trade` / `close_broken_path` / `close_at_market`. Розмір і відмова входу —
`assess_entry`. Пік капіталу — `next_peak_equity` на кожному спостереженні (відкриття бару, збиток свічки,
реалізований вихід). Серія збитків — `next_consecutive_losses`. Депозит і виведення — `adjust_equity_anchors`.
Стрес витрат — `CostModel.scaled` лише у виконанні: розмір, ліміт slippage і бюджет стопа (знаменник R)
беруть базову модель, як live. Інакше розмір поглинав би подвоєні витрати стопу, а ліміт slippage
викидав би зі стресу найдорожчі входи.

Одна позиція бота. Поки вона відкрита, новий сигнал лише записується (розвороту й доливання немає).
Просадка, що дійшла ліміту, лишає рахунок зупиненим до кінця прогону (`HALTED`, вихід лише вручну) і
закриває відкриту позицію market (Emergency Stop, ТЗ §8.5, §10.3): за `open`, якщо ліміт видно вже на
ньому, інакше за екстремумом last проти позиції. Денний ліміт знімається на першому барі наступної доби
UTC, позиція під ним лишається під SL. Виходи ліміти не блокують.

Затримка входу — `entry_is_stale`: за замовчуванням сигнал пораховано на `open` свічки входу (вік 0).
Випадкового проковзування немає, тож `seed` лише записується у звіт для журналу спроб (T2.5).

Кожен сигнал має мати `horizon_bars` свічок після входу, інакше — помилка прогону: кінець ряду не дірка
в даних, а рішення за індексом не залежить від результату угоди. Дірка на шляху ще відкритої угоди не
відкидається: `close_broken_path` бере бюджет стопа. Дірка після вже спрацьованого бар'єра на суцільному
префіксі угоду не змінює (`strict_path=False`). Funding сітки 8 год, якого немає в історії, не стає нулем:
до PnL додається песимістична виплата, угода позначається.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from decimal import Decimal
from enum import StrEnum
from itertools import pairwise

from pydantic import model_validator

from app.exchange.types import Instrument, PositionSide
from app.ml.features import HOUR_MS
from app.ml.trade_sim import (
    ExitReason,
    IncompletePath,
    MarkSeries,
    NoAtr,
    SimCandle,
    SimulatedTrade,
    StopPriceSource,
    TargetParams,
    UnusableExit,
    atr_before_entry,
    barrier_prices,
    close_at_market,
    close_broken_path,
    simulate_trade,
)
from app.risk.costs import (
    DEFAULT_MAX_SIGNAL_AGE_MS,
    CostModel,
    FundingEvent,
    entry_is_stale,
    funding_over_hold,
)
from app.risk.limits import (
    RiskLimits,
    adjust_equity_anchors,
    check_daily_loss,
    check_drawdown,
    check_entry_slippage,
    next_consecutive_losses,
    next_peak_equity,
)
from app.risk.numbers import FrozenModel, fixed_context, require_decimal
from app.risk.reasons import RiskReason
from app.risk.sizing import assess_entry

# Той самий формат, що CHECK `decision_log.reason_code` (T0.4).
_REASON_CODE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_DAY_MS = 86_400_000
_FUNDING_INTERVAL_MS = 8 * HOUR_MS
_YEAR_MS = 365 * 24 * 60 * 60 * 1000


class BacktestReason(StrEnum):
    """Відмови, яких немає серед `RiskReason`: сигнал, дані, обірваний шлях."""

    SIGNAL_STALE = "SIGNAL_STALE"
    NO_ATR = "NO_ATR"
    UNUSABLE_EXIT = "UNUSABLE_EXIT"


class FundingCoverageError(ValueError):
    """Утримання перетинає годину funding, а ставки для песимістичної оцінки немає."""


class EntrySignal(FrozenModel):
    """Кандидат на свічку входу. Стратегію обирає викликач (T2.7), не бектест.

    `computed_at_ms is None` — сигнал на відкритті свічки входу, вік 0.
    """

    entry_index: int
    side: PositionSide
    computed_at_ms: int | None = None

    @model_validator(mode="after")
    def _fields(self) -> EntrySignal:
        if type(self.entry_index) is not int or self.entry_index < 0:
            raise ValueError("entry_index має бути невід'ємний int")
        if type(self.side) is not PositionSide:
            raise ValueError("side має бути PositionSide")
        if self.computed_at_ms is not None and (type(self.computed_at_ms) is not int or self.computed_at_ms < 0):
            raise ValueError("computed_at_ms має бути невід'ємний int")
        return self


class Cashflow(FrozenModel):
    """Депозит (> 0) або виведення (< 0) у валюті рахунку. Час — мс UTC."""

    time_ms: int
    amount: Decimal

    @model_validator(mode="after")
    def _fields(self) -> Cashflow:
        if type(self.time_ms) is not int or self.time_ms < 0:
            raise ValueError("time_ms має бути невід'ємний int")
        require_decimal("amount", self.amount)
        if self.amount == 0:
            raise ValueError("нульовий грошовий потік")
        return self


class BacktestTrade(FrozenModel):
    """Закрита угода. `r_multiple` = чистий результат / (`loss_per_unit` × `qty`); на гепі і в стресі буває < −1.

    `loss_per_unit` — запланований бюджет стопа з `PositionSize` (базова модель витрат, округлені рівні).
    """

    side: PositionSide
    entry_index: int
    exit_index: int
    entry_open_time_ms: int
    exit_open_time_ms: int
    exit_reason: ExitReason
    stop_price_source: StopPriceSource
    qty: Decimal
    net_usd: Decimal
    loss_per_unit: Decimal
    r_multiple: Decimal
    risk_usd: Decimal
    funding_penalty_usd: Decimal
    funding_gap: bool

    @model_validator(mode="after")
    def _signed(self) -> BacktestTrade:
        require_decimal("qty", self.qty, positive=True)
        require_decimal("loss_per_unit", self.loss_per_unit, positive=True)
        require_decimal("net_usd", self.net_usd)
        require_decimal("r_multiple", self.r_multiple)
        require_decimal("risk_usd", self.risk_usd, positive=True)
        require_decimal("funding_penalty_usd", self.funding_penalty_usd)
        if self.funding_penalty_usd > 0:
            raise ValueError("штраф funding не може збільшувати результат")
        return self


class BacktestDecision(FrozenModel):
    """Один рядок журналу, як `decision_log`: код причини або вхід без причини."""

    ts_ms: int
    entry_index: int
    stage: str
    decision: str
    reason_code: str | None
    side: PositionSide

    @model_validator(mode="after")
    def _code(self) -> BacktestDecision:
        if self.stage not in ("SIGNAL", "RISK", "RESULT"):
            raise ValueError("stage журналу бектесту")
        if self.decision not in ("ENTER", "SKIP", "CLOSE"):
            raise ValueError("decision журналу бектесту")
        if self.reason_code is None:
            if self.decision != "ENTER":
                raise ValueError("відмова або вихід без причини")
            return self
        if _REASON_CODE.fullmatch(self.reason_code) is None:
            raise ValueError("reason_code не проходить формат decision_log")
        if self.decision == "ENTER":
            raise ValueError("вхід не має коду відмови")
        return self


class BacktestMetrics(FrozenModel):
    """Метрики після витрат. Sharpe — річний, з бар-доходів без депозитів; просадка — з тінями."""

    n_trades: int
    total_r: Decimal
    total_net_usd: Decimal
    profit_factor: Decimal | None
    max_drawdown_pct: Decimal
    hit_rate: Decimal | None
    sharpe: Decimal | None
    max_consecutive_losses: int
    n_incomplete: int
    n_unusable: int
    n_funding_gaps: int
    ending_equity: Decimal
    peak_equity: Decimal
    halted: bool


class BacktestReport(FrozenModel):
    """Повторюваний результат прогону. `seed` лише записується: генератора немає."""

    symbol: str
    seed: int
    cost_factor: Decimal
    trades: tuple[BacktestTrade, ...]
    decisions: tuple[BacktestDecision, ...]
    metrics: BacktestMetrics


class _Open:
    """Позиція, яку ще не реалізовано. `cash_base` — гаманець до цієї угоди, без її PnL.

    `loss_per_unit` — бюджет стопа з `PositionSize`, знаменник R. `trade` замінюється при Emergency Stop.
    """

    __slots__ = ("cash_base", "entry_fee", "loss_per_unit", "notional", "qty", "risk_usd", "trade")

    def __init__(
        self,
        *,
        trade: SimulatedTrade,
        qty: Decimal,
        cash_base: Decimal,
        entry_fee: Decimal,
        risk_usd: Decimal,
        loss_per_unit: Decimal,
        notional: Decimal,
    ) -> None:
        self.trade = trade
        self.qty = qty
        self.cash_base = cash_base
        self.entry_fee = entry_fee
        self.risk_usd = risk_usd
        self.loss_per_unit = loss_per_unit
        self.notional = notional


class _Book:
    """Стан рахунку одного прогону. Не живе між викликами."""

    def __init__(
        self,
        *,
        candles: tuple[SimCandle, ...],
        signals: dict[int, EntrySignal],
        cashflows: tuple[Cashflow, ...],
        funding: tuple[FundingEvent, ...],
        marks: MarkSeries | None,
        instrument: Instrument,
        limits: RiskLimits,
        costs: CostModel,
        exec_costs: CostModel,
        params: TargetParams,
        wallet: Decimal,
        maintenance_margin_rate: Decimal,
        mm_deduction: Decimal,
        other_open_positions: int,
        other_notional: Decimal,
        bar_ms: int,
        max_signal_age_ms: int,
        funding_interval_ms: int,
    ) -> None:
        self.candles = candles
        self.signals = signals
        self.cashflows = cashflows
        self.funding = funding
        self.funding_times = frozenset(event.time_ms for event in funding)
        self.marks = marks
        self.instrument = instrument
        self.limits = limits
        # Рішення входу (розмір, ліміт slippage) — базова модель; виконання — `exec_costs` (стрес).
        self.costs = costs
        self.exec_costs = exec_costs
        self.params = params
        self.wallet = wallet
        self.peak = wallet
        self.day_start = wallet
        self.maintenance_margin_rate = maintenance_margin_rate
        self.mm_deduction = mm_deduction
        self.other_open_positions = other_open_positions
        self.other_notional = other_notional
        self.bar_ms = bar_ms
        self.max_signal_age_ms = max_signal_age_ms
        self.funding_interval_ms = funding_interval_ms
        self.position: _Open | None = None
        self.trades: list[BacktestTrade] = []
        self.decisions: list[BacktestDecision] = []
        self.day: int | None = None
        self.trades_today = 0
        self.consecutive_losses = 0
        self.max_consecutive_losses = 0
        self.last_loss_index: int | None = None
        self.halted = False
        self.daily_paused = False
        self.max_dd = Decimal(0)
        self.external_cash = Decimal(0)
        self.cash_cursor = 0
        self.now_ms = 0
        self.atr_cache: dict[int, Decimal | None] = {}
        self.n_unusable = 0
        self.path: list[Decimal] = [wallet]

    def run(self) -> None:
        first = self.candles[0].open_time_ms
        if first > 0:
            self._drain_cashflows(first - 1, bar=None)
        for index, bar in enumerate(self.candles):
            self.now_ms = bar.open_time_ms
            self._drain_cashflows(bar.open_time_ms, bar=bar)
            equity = self._equity(self._price(bar, adverse=False))
            self._roll_day(equity)
            self._observe(equity)
            if (
                self.position is not None
                and self.position.trade.exit_index == index
                and self._exit_is_known_at_open(self.position.trade)
            ):
                self._realize()
            # Ліміт просадки видно вже на open: Emergency Stop закриває позицію market за цим open.
            self._emergency_close(index, bar.open)
            signal = self.signals.get(index)
            if signal is not None:
                self._on_signal(signal, bar)
            if self.position is not None and self.position.trade.exit_index == index:
                # Тінь проти позиції на свічці TP була, поки позиція ще відкрита. Порядок тіні й TP
                # невідомий: песимістично тінь перша, і якщо вона зупинила рахунок, TP вже не буде.
                if self.position.trade.exit_reason is ExitReason.TAKE_PROFIT:
                    self._observe(self._equity(self._price(bar, adverse=True)))
                    self._emergency_close(index, self._adverse_last(bar))
                if self.position is not None:
                    self._realize()
            self.path.append(self._equity(self._price(bar, adverse=False)) - self.external_cash)
            if self.position is not None:
                self._observe(self._equity(self._price(bar, adverse=True)))
                self._emergency_close(index, self._adverse_last(bar))
        if self.position is None and self.cashflows:
            self._drain_cashflows(self.cashflows[-1].time_ms, bar=None)

    def _on_signal(self, signal: EntrySignal, bar: SimCandle) -> None:
        computed = bar.open_time_ms if signal.computed_at_ms is None else signal.computed_at_ms
        if entry_is_stale(
            computed_at_ms=computed,
            entry_candle_open_time_ms=bar.open_time_ms,
            max_signal_age_ms=self.max_signal_age_ms,
        ):
            self._skip(signal, "SIGNAL", BacktestReason.SIGNAL_STALE)
            return
        if self.halted:
            self._skip(signal, "RISK", RiskReason.RISK_MAX_DRAWDOWN)
            return
        if self.daily_paused:
            self._skip(signal, "RISK", RiskReason.RISK_DAILY_LOSS)
            return
        atr = self._atr(signal.entry_index)
        if atr is None:
            self._skip(signal, "SIGNAL", BacktestReason.NO_ATR)
            return
        try:
            stop, take = barrier_prices(signal.side, bar.open, atr, self.params)
        except UnusableExit:
            self.n_unusable += 1
            self._skip(signal, "RISK", BacktestReason.UNUSABLE_EXIT)
            return
        decision = assess_entry(
            side=signal.side,
            entry_price=bar.open,
            stop_price=stop,
            take_profit_price=take,
            atr=atr,
            wallet_balance=self._wallet_mark(),
            total_equity=self._equity(self._price(bar, adverse=False)),
            peak_equity=self.peak,
            day_start_equity=self.day_start,
            current_notional=self._notional(),
            bot_open_positions=0 if self.position is None else 1,
            open_positions=self._open_positions(),
            trades_today=self.trades_today,
            consecutive_losses=self.consecutive_losses,
            bars_since_last_loss=self._bars_since(signal.entry_index),
            instrument=self.instrument,
            limits=self.limits,
            costs=self.costs,
            maintenance_margin_rate=self.maintenance_margin_rate,
            mm_deduction=self.mm_deduction,
            entry_is_maker=False,
            funding_rate=self._latest_funding_rate(bar.open_time_ms),
        )
        if not decision.ok or decision.size is None:
            self._skip(signal, "RISK", decision.reason if decision.reason is not None else RiskReason.RISK_QTY_ZERO)
            return
        if self.position is not None:
            raise RuntimeError("assess_entry дозволив другу позицію бота")
        size = decision.size
        slipped = check_entry_slippage(
            reference=size.entry_price,
            actual=size.entry_fill,
            max_slippage_bps=self.limits.max_slippage_bps,
        )
        if not slipped.ok:
            self._skip(signal, "RISK", RiskReason.RISK_MAX_SLIPPAGE)
            return
        try:
            trade = simulate_trade(
                self.candles,
                entry_index=signal.entry_index,
                side=signal.side,
                atr=atr,
                params=self.params,
                costs=self.exec_costs,
                mark_candles=self.marks,
                funding=self.funding,
                bar_ms=self.bar_ms,
                entry_is_maker=False,
                strict_path=False,
            )
        except IncompletePath as exc:
            last = signal.entry_index if exc.last_index is None else exc.last_index
            if last >= len(self.candles) - 1:
                # `_as_signals` гарантує свічку горизонту: обрив тут — лише дірка, не кінець ряду.
                raise RuntimeError("шлях угоди обірвався на кінці ряду") from exc
            try:
                trade = close_broken_path(
                    self.candles,
                    entry_index=signal.entry_index,
                    last_index=last,
                    side=signal.side,
                    atr=atr,
                    params=self.params,
                    costs=self.exec_costs,
                    funding=self.funding,
                    bar_ms=self.bar_ms,
                )
            except UnusableExit:
                self.n_unusable += 1
                self._skip(signal, "RISK", BacktestReason.UNUSABLE_EXIT)
                return
        except UnusableExit:
            self.n_unusable += 1
            self._skip(signal, "RISK", BacktestReason.UNUSABLE_EXIT)
            return
        fee = self.exec_costs.entry_fee_rate(maker=False) * trade.entry_fill * size.qty
        self.position = _Open(
            trade=trade,
            qty=size.qty,
            cash_base=self.wallet,
            entry_fee=fee,
            risk_usd=size.risk_usd,
            loss_per_unit=size.loss_per_unit,
            notional=size.qty * max(trade.reference_entry, trade.entry_fill),
        )
        self.trades_today += 1
        self.decisions.append(
            BacktestDecision(
                ts_ms=bar.open_time_ms,
                entry_index=signal.entry_index,
                stage="RISK",
                decision="ENTER",
                reason_code=None,
                side=signal.side,
            )
        )

    def _realize(self) -> None:
        pos = self.position
        if pos is None:
            return
        penalty, gap = self._funding_penalty(pos.trade, pos.qty, pos.trade.exit_open_time_ms)
        net = pos.trade.net_per_unit * pos.qty + penalty
        self.wallet = pos.cash_base + net
        self.position = None
        self.consecutive_losses = next_consecutive_losses(self.consecutive_losses, net)
        if net < 0:
            self.last_loss_index = pos.trade.exit_index
        if self.consecutive_losses > self.max_consecutive_losses:
            self.max_consecutive_losses = self.consecutive_losses
        # Бюджет стопа, з яким рахували розмір: у стресі виконання дорожче, і стоп гірший за −1R.
        loss_budget = pos.loss_per_unit * pos.qty
        self.trades.append(
            BacktestTrade(
                side=pos.trade.side,
                entry_index=pos.trade.entry_index,
                exit_index=pos.trade.exit_index,
                entry_open_time_ms=pos.trade.entry_open_time_ms,
                exit_open_time_ms=pos.trade.exit_open_time_ms,
                exit_reason=pos.trade.exit_reason,
                stop_price_source=pos.trade.stop_price_source,
                qty=pos.qty,
                net_usd=net,
                loss_per_unit=pos.loss_per_unit,
                r_multiple=net / loss_budget,
                risk_usd=pos.risk_usd,
                funding_penalty_usd=penalty,
                funding_gap=gap,
            )
        )
        self.decisions.append(
            BacktestDecision(
                ts_ms=pos.trade.exit_open_time_ms,
                entry_index=pos.trade.exit_index,
                stage="RESULT",
                decision="CLOSE",
                reason_code=pos.trade.exit_reason.value,
                side=pos.trade.side,
            )
        )
        self._observe(self.wallet)

    def _emergency_close(self, index: int, reference: Decimal) -> None:
        """`HALTED` з відкритою позицією — Emergency Stop: market-закриття за `reference` (ТЗ §8.5, §10.3).

        `HALTED` липкий, а позицію після нього бот уже не відкриває, тож тут вона завжди та, що
        тримали в момент зупинки.
        """
        pos = self.position
        if not self.halted or pos is None:
            return
        pos.trade = close_at_market(
            self.candles,
            pos.trade,
            exit_index=index,
            reference_exit=reference,
            costs=self.exec_costs,
            funding=self.funding,
        )
        self._realize()

    def _adverse_last(self, bar: SimCandle) -> Decimal:
        """Екстремум last проти позиції: market-закриття виконується за last, навіть коли тригер — mark."""
        pos = self.position
        if pos is None or pos.trade.side is PositionSide.LONG:
            return bar.low
        return bar.high

    def _equity(self, price: Decimal) -> Decimal:
        pos = self.position
        if pos is None:
            return self.wallet
        sign = Decimal(1) if pos.trade.side is PositionSide.LONG else Decimal(-1)
        unrealized = sign * (price - pos.trade.entry_fill) * pos.qty
        funded = funding_over_hold(
            self.funding,
            side=pos.trade.side,
            qty=pos.qty,
            opened_at_ms=pos.trade.entry_open_time_ms,
            closed_at_ms=self.now_ms,
        )
        penalty, _gap = self._funding_penalty(pos.trade, pos.qty, self.now_ms)
        return pos.cash_base + unrealized - pos.entry_fee + funded + penalty

    def _wallet_mark(self) -> Decimal:
        """Гаманець для розміру: під час угоди готівка ще без її реалізованого PnL, уже з комісією входу."""
        pos = self.position
        if pos is None:
            return self.wallet
        return pos.cash_base - pos.entry_fee

    def _price(self, bar: SimCandle, *, adverse: bool) -> Decimal:
        source = bar
        pos = self.position
        if pos is not None and pos.trade.stop_price_source is StopPriceSource.MARK and self.marks is not None:
            mark = self.marks.get(bar.open_time_ms)
            if mark is not None:
                source = mark
        if not adverse:
            return source.open
        if pos is None or pos.trade.side is PositionSide.LONG:
            return source.low
        return source.high

    def _observe(self, equity: Decimal) -> None:
        self.peak = next_peak_equity(self.peak, equity)
        if self.peak > 0 and equity < self.peak:
            drawdown = (self.peak - equity) / self.peak
            if drawdown > self.max_dd:
                self.max_dd = drawdown
        if not check_drawdown(equity=equity, peak_equity=self.peak, limit_pct=self.limits.max_drawdown_pct).ok:
            self.halted = True
        if not check_daily_loss(
            equity=equity, day_start_equity=self.day_start, limit_pct=self.limits.daily_loss_limit_pct
        ).ok:
            self.daily_paused = True

    def _roll_day(self, equity: Decimal) -> None:
        day = self.now_ms // _DAY_MS
        if self.day is None:
            self.day = day
            return
        if day == self.day:
            return
        self.day = day
        self.trades_today = 0
        self.daily_paused = False
        self.day_start = equity

    def _drain_cashflows(self, until_ms: int, *, bar: SimCandle | None) -> None:
        while self.cash_cursor < len(self.cashflows) and self.cashflows[self.cash_cursor].time_ms <= until_ms:
            flow = self.cashflows[self.cash_cursor]
            self.cash_cursor += 1
            if self.position is None or bar is None:
                equity = self.wallet
            else:
                equity = self._equity(self._price(bar, adverse=False))
            anchors = adjust_equity_anchors(
                equity_after=equity + flow.amount,
                cashflow=flow.amount,
                peak_equity=self.peak,
                day_start_equity=self.day_start,
            )
            self.peak = anchors.peak_equity
            self.day_start = anchors.day_start_equity
            self.external_cash += flow.amount
            if self.position is None:
                self.wallet += flow.amount
            else:
                self.position.cash_base += flow.amount

    def _funding_penalty(self, trade: SimulatedTrade, qty: Decimal, until_ms: int) -> tuple[Decimal, bool]:
        opened = trade.entry_open_time_ms
        closed = min(until_ms, trade.exit_open_time_ms)
        missing = [
            stamp
            for stamp in _funding_slots(opened, closed, self.funding_interval_ms)
            if stamp not in self.funding_times
        ]
        if not missing:
            return Decimal(0), False
        penalty = Decimal(0)
        for stamp in missing:
            penalty -= abs(self._pessimistic_funding_rate(stamp)) * qty * self._open_price_at(stamp)
        gap = any(
            stamp not in self.funding_times
            for stamp in _funding_slots(opened, trade.exit_open_time_ms, self.funding_interval_ms)
        )
        return penalty, gap

    def _pessimistic_funding_rate(self, until_ms: int) -> Decimal:
        """Найбільший модуль ставки, відомої не пізніше за слот. Майбутня ставка прогін не змінює."""
        known = [abs(event.rate) for event in self.funding if event.time_ms <= until_ms]
        if known:
            return max(known)
        if self.limits.max_funding_rate_abs is not None:
            return self.limits.max_funding_rate_abs
        raise FundingCoverageError(
            "утримання перетинає funding, історії ставки немає і max_funding_rate_abs не заданий"
        )

    def _open_price_at(self, time_ms: int) -> Decimal:
        found = self.candles[0].open
        for bar in self.candles:
            if bar.open_time_ms > time_ms:
                break
            found = bar.open
        return found

    def _latest_funding_rate(self, time_ms: int) -> Decimal | None:
        if self.limits.max_funding_rate_abs is None:
            return None
        latest: FundingEvent | None = None
        for event in self.funding:
            if event.time_ms >= time_ms:
                break
            latest = event
        if latest is None:
            return None
        return latest.rate

    def _atr(self, index: int) -> Decimal | None:
        if index not in self.atr_cache:
            try:
                self.atr_cache[index] = atr_before_entry(self.candles, index, bar_ms=self.bar_ms)
            except NoAtr:
                self.atr_cache[index] = None
        return self.atr_cache[index]

    def _exit_is_known_at_open(self, trade: SimulatedTrade) -> bool:
        """Вихід відомий з `open` бару: time-stop, геп через SL або TP, свічка горизонту. Тінь бару — ще ні."""
        if trade.exit_reason is ExitReason.DATA_GAP:
            return False
        if trade.exit_index == trade.entry_index + self.params.horizon_bars:
            return True
        if trade.exit_reason is ExitReason.TAKE_PROFIT:
            # Геп через TP: біржовий TP виконано на open, тінь після нього позицію вже не застала.
            opened = self.candles[trade.exit_index].open
            if trade.side is PositionSide.LONG:
                return opened > trade.reference_take_profit
            return opened < trade.reference_take_profit
        return trade.exit_reason is ExitReason.STOP_LOSS and trade.reference_exit != trade.reference_stop

    def _bars_since(self, index: int) -> int:
        if self.last_loss_index is None:
            return 0
        return index - self.last_loss_index

    def _open_positions(self) -> int:
        return self.other_open_positions + (0 if self.position is None else 1)

    def _notional(self) -> Decimal:
        if self.position is None:
            return self.other_notional
        return self.other_notional + self.position.notional

    def _skip(self, signal: EntrySignal, stage: str, reason: BacktestReason | RiskReason) -> None:
        self.decisions.append(
            BacktestDecision(
                ts_ms=self.candles[signal.entry_index].open_time_ms,
                entry_index=signal.entry_index,
                stage=stage,
                decision="SKIP",
                reason_code=reason.value,
                side=signal.side,
            )
        )


def _funding_slots(opened_ms: int, closed_ms: int, interval_ms: int) -> tuple[int, ...]:
    """Мітки funding строго після входу і не пізніше за вихід. Мережа — від епохи UTC."""
    if closed_ms <= opened_ms:
        return ()
    if opened_ms % interval_ms == 0:
        start = opened_ms + interval_ms
    else:
        start = opened_ms + (interval_ms - opened_ms % interval_ms)
    stamps: list[int] = []
    stamp = start
    while stamp <= closed_ms:
        stamps.append(stamp)
        stamp += interval_ms
    return tuple(stamps)


def _sharpe(levels: Sequence[Decimal], bar_ms: int) -> Decimal | None:
    """Річний Sharpe бар-доходів. Менше двох доходностей або нульовий розкид — метрики немає."""
    if len(levels) < 3:
        return None
    returns: list[Decimal] = []
    for previous, current in pairwise(levels):
        if previous <= 0:
            return None
        returns.append((current - previous) / previous)
    if len(returns) < 2:
        return None
    count = Decimal(len(returns))
    mean = sum(returns, Decimal(0)) / count
    variance = sum((value - mean) ** 2 for value in returns) / Decimal(len(returns) - 1)
    if variance == 0:
        return None
    annual = (Decimal(_YEAR_MS) / Decimal(bar_ms)).sqrt()
    return mean / variance.sqrt() * annual


def _metrics(book: _Book) -> BacktestMetrics:
    trades = book.trades
    total_net = sum((trade.net_usd for trade in trades), Decimal(0))
    total_r = sum((trade.r_multiple for trade in trades), Decimal(0))
    gross_profit = sum((trade.net_usd for trade in trades if trade.net_usd > 0), Decimal(0))
    gross_loss = sum((-trade.net_usd for trade in trades if trade.net_usd < 0), Decimal(0))
    wins = sum(1 for trade in trades if trade.net_usd > 0)
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    hit_rate = None if not trades else Decimal(wins) / Decimal(len(trades))
    return BacktestMetrics(
        n_trades=len(trades),
        total_r=total_r,
        total_net_usd=total_net,
        profit_factor=profit_factor,
        max_drawdown_pct=book.max_dd,
        hit_rate=hit_rate,
        sharpe=_sharpe(book.path, book.bar_ms),
        max_consecutive_losses=book.max_consecutive_losses,
        n_incomplete=sum(1 for trade in trades if trade.exit_reason is ExitReason.DATA_GAP),
        n_unusable=book.n_unusable,
        n_funding_gaps=sum(1 for trade in trades if trade.funding_gap),
        ending_equity=book.wallet,
        peak_equity=book.peak,
        halted=book.halted,
    )


def _mark_index(mark_candles: Sequence[SimCandle] | MarkSeries | None) -> MarkSeries | None:
    if mark_candles is None or isinstance(mark_candles, MarkSeries):
        return mark_candles
    return MarkSeries(mark_candles)


def _as_candles(candles: Sequence[SimCandle]) -> tuple[SimCandle, ...]:
    if not isinstance(candles, Sequence) or isinstance(candles, (str, bytes)) or not candles:
        raise ValueError("свічки мають бути непорожньою послідовністю")
    out: list[SimCandle] = []
    previous: int | None = None
    for bar in candles:
        if type(bar) is not SimCandle:
            raise ValueError("свічка має бути SimCandle")
        if previous is not None and bar.open_time_ms <= previous:
            raise ValueError("свічки мають йти за зростанням часу")
        previous = bar.open_time_ms
        out.append(bar)
    return tuple(out)


def _as_signals(signals: Sequence[EntrySignal], n_candles: int, horizon_bars: int) -> dict[int, EntrySignal]:
    """Кожен сигнал — з `horizon_bars` свічками після входу. Рішення за індексом, а не за результатом угоди.

    Без цього угода біля кінця ряду закривалась би як дірка в даних, або (з відкиданням) лишались би
    лише ранні виходи. Ряд подовжують за останній сигнал, а не обрізають сигнали за результатом.
    """
    if not isinstance(signals, Sequence) or isinstance(signals, (str, bytes)):
        raise ValueError("сигнали мають бути послідовністю")
    found: dict[int, EntrySignal] = {}
    for signal in signals:
        if type(signal) is not EntrySignal:
            raise ValueError("сигнал має бути EntrySignal")
        if signal.entry_index + horizon_bars >= n_candles:
            raise ValueError(
                f"сигнал на свічці {signal.entry_index} без horizon_bars={horizon_bars} свічок після входу: "
                "подовжте ряд за останній сигнал"
            )
        if signal.entry_index in found:
            raise ValueError("два сигнали на одну свічку")
        found[signal.entry_index] = signal
    return found


def _as_funding(funding: Sequence[FundingEvent]) -> tuple[FundingEvent, ...]:
    if not isinstance(funding, Sequence) or isinstance(funding, (str, bytes)):
        raise ValueError("funding має бути послідовністю")
    events = tuple(funding)
    for event in events:
        if type(event) is not FundingEvent:
            raise ValueError("подія funding має бути FundingEvent")
    ordered = tuple(sorted(events, key=lambda event: event.time_ms))
    for previous, current in pairwise(ordered):
        if previous.time_ms == current.time_ms:
            raise ValueError("дві події funding на одну мітку")
    return ordered


def _as_cashflows(cashflows: Sequence[Cashflow]) -> tuple[Cashflow, ...]:
    if not isinstance(cashflows, Sequence) or isinstance(cashflows, (str, bytes)):
        raise ValueError("грошові потоки мають бути послідовністю")
    flows = tuple(cashflows)
    for flow in flows:
        if type(flow) is not Cashflow:
            raise ValueError("грошовий потік має бути Cashflow")
    return tuple(sorted(flows, key=lambda flow: flow.time_ms))


@fixed_context
def run_backtest(
    candles: Sequence[SimCandle],
    signals: Sequence[EntrySignal],
    *,
    symbol: str,
    instrument: Instrument,
    limits: RiskLimits,
    initial_equity: Decimal,
    maintenance_margin_rate: Decimal,
    seed: int,
    costs: CostModel | None = None,
    params: TargetParams | None = None,
    mark_candles: Sequence[SimCandle] | MarkSeries | None = None,
    funding: Sequence[FundingEvent] = (),
    cashflows: Sequence[Cashflow] = (),
    other_open_positions: int = 0,
    other_notional: Decimal = Decimal(0),
    mm_deduction: Decimal = Decimal(0),
    bar_ms: int = HOUR_MS,
    max_signal_age_ms: int = DEFAULT_MAX_SIGNAL_AGE_MS,
    cost_factor: Decimal = Decimal(1),
    funding_interval_ms: int = _FUNDING_INTERVAL_MS,
) -> BacktestReport:
    """Прогнати сигнали одного символу. Mark, funding і свічки — цього самого символу.

    `cost_factor` > 1 — стрес (`CostModel.scaled`). `other_open_positions` — позиції акаунта
    поза цим ботом; сам бот тримає не більше однієї. `max_hold` коротший за горизонт задається
    меншим `TargetParams.horizon_bars`: бектест його не подовжує.
    """
    series = _as_candles(candles)
    if type(symbol) is not str or not symbol:
        raise ValueError("symbol має бути непорожній рядок")
    if type(instrument) is not Instrument or type(limits) is not RiskLimits:
        raise ValueError("instrument має бути Instrument, limits — RiskLimits")
    if instrument.symbol != symbol:
        raise ValueError("інструмент іншого символу")
    require_decimal("initial_equity", initial_equity, positive=True)
    require_decimal("maintenance_margin_rate", maintenance_margin_rate, positive=True)
    require_decimal("mm_deduction", mm_deduction, non_negative=True)
    require_decimal("other_notional", other_notional, non_negative=True)
    require_decimal("cost_factor", cost_factor, positive=True)
    if type(seed) is not int or seed < 0:
        raise ValueError("seed має бути невід'ємний int")
    if type(other_open_positions) is not int or other_open_positions < 0:
        raise ValueError("other_open_positions має бути невід'ємний int")
    if type(bar_ms) is not int or bar_ms < 1:
        raise ValueError("bar_ms має бути int ≥ 1")
    if type(max_signal_age_ms) is not int or max_signal_age_ms < 0:
        raise ValueError("max_signal_age_ms має бути невід'ємний int")
    if type(funding_interval_ms) is not int or funding_interval_ms < 1:
        raise ValueError("funding_interval_ms має бути int ≥ 1")
    model = costs if costs is not None else CostModel()
    if type(model) is not CostModel:
        raise ValueError("costs має бути CostModel")
    spec = params if params is not None else TargetParams()
    if type(spec) is not TargetParams:
        raise ValueError("params має бути TargetParams")
    priced = model if cost_factor == 1 else model.scaled(cost_factor)
    marks = _mark_index(mark_candles)
    book = _Book(
        candles=series,
        signals=_as_signals(signals, len(series), spec.horizon_bars),
        cashflows=_as_cashflows(cashflows),
        funding=_as_funding(funding),
        marks=marks,
        instrument=instrument,
        limits=limits,
        costs=model,
        exec_costs=priced,
        params=spec,
        wallet=initial_equity,
        maintenance_margin_rate=maintenance_margin_rate,
        mm_deduction=mm_deduction,
        other_open_positions=other_open_positions,
        other_notional=other_notional,
        bar_ms=bar_ms,
        max_signal_age_ms=max_signal_age_ms,
        funding_interval_ms=funding_interval_ms,
    )
    book.run()
    if book.position is not None:
        raise RuntimeError("позиція лишилась відкритою: вихід не потрапив у ряд свічок")
    return BacktestReport(
        symbol=symbol,
        seed=seed,
        cost_factor=cost_factor,
        trades=tuple(book.trades),
        decisions=tuple(book.decisions),
        metrics=_metrics(book),
    )
