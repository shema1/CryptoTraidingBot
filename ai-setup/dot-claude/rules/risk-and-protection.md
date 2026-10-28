---
description: Правила для Risk Engine, розміру позиції та Position Protection Manager (SL/TP)
paths:
  - "apps/backend/app/risk/**"
  - "apps/backend/app/protection/**"
---

# Risk Engine та захист позицій (ТЗ §8, §9)

## Risk Engine
- `RiskGate` — єдина точка дозволу на збільшення експозиції. Немає параметрів/прапорців «bypass», `force`, `skip_risk` — і не додавай їх.
- **Усе окремо для кожного акаунта** (`account_id`): `risk_config`, equity, денний ліміт, просадка, лічильники, статус `NORMAL/RESTRICTED/HALTED`, Emergency Stop. Стан Demo ніколи не впливає на Live і навпаки (тест обов'язковий).
- Pre-trade перевірки — усі шість з ТЗ §8.4, кожна з власним кодом причини (`RISK_DAILY_LOSS`, `RISK_MAX_POSITIONS`, …). Кожна відмова логується в `risk_events` і `decision_log`.
- Розмір позиції — **єдина** функція `calc_position_size()` у `risk/sizing.py`, яку викликають і `RiskGate`, і бектестер. Формула — ТЗ §8.3. Запас `sl_dist` на slippage — з параметра `slippage_sl` моделі витрат `risk/costs.py`.
- Перевірки лімітів (одна позиція, cooldown, денний збиток, `max_trades_per_day`, просадка, time-stop) — **чисті функції** `risk/limits.py` без БД, мережі й годинника (час передається параметром). Їх викликають і `RiskGate`, і бектестер; дублювати цю логіку деінде заборонено.
- `sizing_equity = min(wallet_balance, total_equity)`; для денного ліміту та просадки — `total_equity`. Значення з `fetch_balance` біржі, не з кешу.
- Якщо після округлення `qty < minOrderQty` або нижче мінімального номіналу — входу немає. **Ніколи не округлювати вгору заради мінімального лота.**
- Перевірка ліквідаційної відстані `min_liq_distance_sl_multiple` перед відправкою.
- Поповнення/виведення з `ledger` коригують `peak_equity` та `day_start_equity`.
- Значення за замовчуванням (ТЗ §8.2) — консервативні. Не змінюй їх у бік більшого ризику. Послаблення конфігу — нова версія `risk_config` + підтвердження в UI + `audit_log`.
- `HALTED` автоматично запускає Emergency Stop; вихід лише вручну.

## Position Protection Manager
- Основний режим — нативні TP/SL Bybit на рівні позиції (`tpslMode=Full`), SL trigger `MarkPrice`, TP trigger `LastPrice`.
- Інваріант: позиція без **підтвердженого** (прочитаного з біржі) ненульового `stopLoss` існує не довше `protection_timeout_s`. Далі — 3 повтори з backoff → примусове `reduceOnly` market закриття → бот `ERROR` → CRITICAL у Telegram.
- Поки є `ENTRY_FILLED_UNPROTECTED`, нові входи заборонені.
- SL можна рухати лише в бік зменшення ризику (для LONG — лише вгору, для SHORT — лише вниз). Перевірка — в коді, з тестом.
- Після закриття позиції — перевірити й скасувати залишкові умовні ордери.

## Тести (обов'язково для будь-якої зміни тут)
- Граничні значення: нульовий/мінімальний лот, `qty` рівно на межі, equity ≤ `min_equity_usd`, LONG і SHORT.
- Property-based тести (hypothesis) для `calc_position_size`: ризик за SL ніколи не перевищує `risk_usd`; `qty` кратний `qtyStep`; монотонність.
- Parity-тест: бектестер і `RiskGate` дають однаковий розмір і однакові рішення лімітів на однакових даних.
