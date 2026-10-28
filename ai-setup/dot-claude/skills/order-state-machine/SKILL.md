---
name: order-state-machine
description: Використовуй при будь-якій зміні машини станів ордера, order manager, обробки fills, SUBMISSION_UNKNOWN, clientOrderId або order_events (apps/backend/app/trading/). Містить таблицю переходів і чекліст безпечної зміни.
paths:
  - "apps/backend/app/trading/**"
---

# Машина станів ордера

Джерело вимог: ТЗ §7 (`AppInfo.md`). Ця skill — чекліст, а не заміна ТЗ.

## Таблиця переходів (єдине джерело в коді: `trading/order_state.py`, константа `ALLOWED_TRANSITIONS`)

| З | В |
|---|---|
| CREATED | SUBMITTING |
| SUBMITTING | OPEN, REJECTED, SUBMISSION_UNKNOWN |
| SUBMISSION_UNKNOWN | OPEN, PARTIALLY_FILLED, FILLED, REJECTED |
| OPEN | PARTIALLY_FILLED, FILLED, PENDING_CANCEL, EXPIRED |
| PARTIALLY_FILLED | FILLED, PENDING_CANCEL |
| PENDING_CANCEL | CANCELED, FILLED, PARTIALLY_FILLED |

Термінальні: FILLED, CANCELED, REJECTED, EXPIRED. Частковий fill + скасування залишку = CANCELED з `filled_qty > 0`.

## Порядок роботи

1. Прочитай ТЗ §7.1–7.2 і поточний `order_state.py`.
2. Опиши зміну в термінах «який перехід додається/змінюється і чому». Якщо зміна не випливає з ТЗ — зупинись і запитай користувача.
3. **Спочатку тести** (`tests/trading/test_order_state.py`):
   - кожен дозволений перехід проходить;
   - кожен НЕдозволений перехід (повний декартів добуток станів мінус дозволені) кидає `InvariantViolation` і логується;
   - перехід пише рівно один запис в `order_events` з джерелом (WS/REST/LOCAL);
   - конкурентне оновлення з тим самим `version` — один успішний, другий отримує конфлікт;
   - property-based тест (hypothesis): довільна послідовність подій ніколи не виводить зі множини валідних станів.
4. Реалізуй. Перевір:
   - write-ahead: `SUBMITTING` комітиться ДО мережевого виклику; мережевий виклик поза транзакцією;
   - таймаут/обрив/5xx → `SUBMISSION_UNKNOWN`, не `REJECTED`;
   - повторна відправка — лише з тим самим `clientOrderId` і лише після розв'язання UNKNOWN;
   - `clientOrderId = "tb" + uuid4().hex` (34 символи, без дефісів);
   - fills дедуплікуються за `exec_id` (UNIQUE у БД + `ON CONFLICT DO NOTHING`); подія з WS і REST — одна й та сама;
   - `filled_qty`, середня ціна, комісії — лише з `fills`.
5. Алгоритм `SUBMISSION_UNKNOWN` (ТЗ §7.2): блок нових ордерів бота → `fetch_order_by_client_id` + WS → до 3 перевірок з інтервалом 2–3 с + звірка позиції → знайдено: відповідний стан / не знайдено: REJECTED → ескалація через `unknown_escalation_s` (бот `ERROR`, CRITICAL у Telegram). Покрий тестами кожну гілку з `FakeExchangeAdapter`.
6. Запусти `uv run pytest -q tests/trading && uv run mypy app/trading`.
7. Запусти skill `safety-check` (або субагента `trading-safety-reviewer`).
