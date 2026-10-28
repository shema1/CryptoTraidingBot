---
name: risk-engine-change
description: Використовуй при будь-якій зміні Risk Engine, RiskGate, calc_position_size, лімітів ризику, equity/drawdown розрахунків або Position Protection Manager (apps/backend/app/risk/, apps/backend/app/protection/).
paths:
  - "apps/backend/app/risk/**"
  - "apps/backend/app/protection/**"
---

# Зміна Risk Engine / захисту позицій

Вимоги: ТЗ §8 (ризик), §9 (захист), §15 (equity, ledger, instruments).

## Червоні лінії (якщо зміна їх торкається — зупинись і запитай)
- Будь-який шлях збільшення експозиції в обхід `RiskGate`.
- Блокування `reduceOnly`-виходів або аварійного закриття.
- Зміна значень за замовчуванням ТЗ §8.2 у бік більшого ризику.
- Округлення кількості вгору; вхід при `qty < minOrderQty`.
- Віддалення SL від ціни входу.
- Збільшення `protection_timeout_s` або кількості повторів без явного запиту.

## Порядок
1. Визнач, яку перевірку/ліміт/формулу змінюєш, і знайди її в ТЗ. Процитуй рядок ТЗ у описі зміни.
2. Тести спочатку (`tests/risk/`, `tests/protection/`):
   - граничні значення: рівно на ліміті, на 1 крок вище/нижче; LONG і SHORT;
   - `qty` → 0 після округлення → входу немає, причина в `decision_log`;
   - property-based: `qty * |entry - sl| ≤ risk_usd` (з урахуванням запасу на комісію/slippage); `qty % qtyStep == 0`; ціни кратні `tickSize`;
   - `sizing_equity = min(wallet_balance, total_equity)`; нереалізований прибуток не збільшує розмір;
   - денний ліміт і просадка рахуються з `total_equity`; депозит/виведення коригують `peak_equity` і `day_start_equity`;
   - parity: той самий вхід → той самий розмір і ті самі рішення `risk/limits.py` у бектестері та `RiskGate`;
   - захист: немає підтвердженого SL за `protection_timeout_s` → 3 повтори → `reduceOnly` market → `ERROR`; нові входи заблоковані весь цей час;
   - SL можна змінити лише в бік зменшення ризику.
3. Реалізуй з `Decimal`. Кожна відмова — код причини (`RISK_*`) у `risk_events` і `decision_log`.
4. `uv run pytest -q tests/risk tests/protection && uv run mypy app/risk app/protection`.
5. Обов'язкове незалежне рев'ю: субагент `trading-safety-reviewer`.
