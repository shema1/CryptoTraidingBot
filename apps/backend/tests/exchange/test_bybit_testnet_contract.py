"""Contract-набір `ExchangeAdapter` проти Bybit Testnet з ключем (T3.2): ордери, позиція, SL/TP, fills.

Запуск: `uv run pytest -q -m testnet tests/exchange/test_bybit_testnet_contract.py`.
Ключ — `BYBIT_TESTNET_API_KEY` / `BYBIT_TESTNET_API_SECRET` у `.env` (лише торгівля, без права виводу; як створити —
`docs/runbooks/getting-started.md`). Немає ключа — тест **падає** з інструкцією, а не пропускається.

Безпека набору: хости (публічний і приватний) перевіряються до першого запиту — лише Testnet; набір відкриває
мінімальну позицію за `BTC/USDT:USDT` і закриває лише власний вхід (`contract.py`). Перед набором акаунт Testnet
переводиться в `ISOLATED_MARGIN` (на UTA — для всього акаунта Testnet) і проходить `verify_account_config`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio

from app.core.clock import SystemClock, use_clock
from app.core.config import Settings
from app.exchange.adapter import ExchangeAdapter
from app.exchange.bybit import PRIVATE_HOSTS, BybitAdapter, ExchangeMode
from app.exchange.bybit_private import BybitCredentials
from app.exchange.errors import AdapterConfigError
from app.exchange.rate_limit import ProcessRole, RateLimitConfig, RateLimiter, RequestPriority
from app.exchange.types import MarginMode, Timeframe
from tests.exchange.contract import ContractMarket, ExchangeAdapterContract

_SYMBOL = "BTC/USDT:USDT"
_TESTNET_PRIVATE = f"https://{PRIVATE_HOSTS[ExchangeMode.TESTNET]}"
_MISSING_KEY = (
    "немає ключа Bybit Testnet: створіть на testnet.bybit.com API-ключ лише з правом торгівлі (без виводу) і додайте "
    "в .env BYBIT_TESTNET_API_KEY і BYBIT_TESTNET_API_SECRET (docs/runbooks/getting-started.md)"
)


@pytest.fixture(autouse=True)
def fake_clock() -> Iterator[None]:
    """Реальний час: `fetch_time`, підпис запиту й вікна виконань порівнюються з біржею."""
    with use_clock(SystemClock()):
        yield


def _as_adapter(adapter: BybitAdapter) -> ExchangeAdapter:
    return adapter


@pytest_asyncio.fixture
async def adapter() -> AsyncIterator[ExchangeAdapter]:
    settings = Settings()
    key, secret = settings.bybit_testnet_api_key, settings.bybit_testnet_api_secret
    if key is None or secret is None:
        pytest.fail(_MISSING_KEY)
    limiter = RateLimiter(RateLimitConfig(role=ProcessRole.TRADING, share_pct=settings.rate_limit_share_trading_pct))
    bybit = BybitAdapter(
        mode=ExchangeMode.TESTNET,
        limiter=limiter,
        credentials=BybitCredentials(api_key=key, api_secret=secret),
        recv_window_ms=settings.recv_window_ms,
    )
    try:
        # До першого запиту: обидва хости — Testnet, ніколи mainnet чи demo.
        public = bybit.public_origin
        if "testnet" not in public or "demo" in public:
            pytest.fail(f"публічний хост не testnet: {public}")
        if bybit.private_origin != _TESTNET_PRIVATE:
            pytest.fail(f"приватний хост не testnet: {bybit.private_origin}")
        await bybit.set_margin_mode(priority=RequestPriority.RECONCILE, symbol=_SYMBOL, mode=MarginMode.ISOLATED)
        await bybit.verify_account_config([_SYMBOL])
        yield _as_adapter(bybit)
    finally:
        await bybit.aclose()
        await limiter.aclose()


@pytest.fixture
def contract_market() -> ContractMarket:
    # `max_time_skew_ms` — з урахуванням RTT до Testnet; `host_id` — окремий для набору, не збігається з ботами.
    return ContractMarket(
        symbol=_SYMBOL, host_id="tn", mode="testnet", timeframe=Timeframe.H1, history_bars=5, max_time_skew_ms=5_000
    )


@pytest.mark.testnet
class TestBybitTestnetContract(ExchangeAdapterContract):
    """Contract-набір на Bybit Testnet."""

    @pytest.mark.xfail(
        raises=AdapterConfigError, strict=True, reason="T3.5: приватний WebSocket Bybit ще не реалізовано"
    )
    @pytest.mark.asyncio
    async def test_private_stream_starts_with_stream_connected(self, adapter: ExchangeAdapter) -> None:
        await super().test_private_stream_starts_with_stream_connected(adapter)
