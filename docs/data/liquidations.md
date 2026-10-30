# Публічний потік ліквідацій Bybit v5

Перевірено 2026-10-30 для задачі T1.10 (ТЗ §4.4). Джерело — документація v5
[All Liquidation](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation)
(context7 `/websites/bybit-exchange_github_io/v5`). REST-історії ліквідацій немає: підписка не відтворює пропущені
події, `type = snapshot` — пачка нових ліквідацій за останні 500 мс, не знімок години і не повтор при підключенні.
Тому колектор має працювати безперервно. Відповіді на підписку, ping і закриття без ping перевірено на публічному
mainnet 2026-10-30 (лише читання, без ключів; записані кадри — у `tests/exchange/test_liquidation_messages.py`).
З підтвердження власника ТЗ §19 переносить цей формат у блок «Уже перевірено»
(`docs/decisions/0012-liquidation-stream-verified.md`).

## З'єднання

| | |
| --- | --- |
| Mainnet (demo і live, публічні дані) | `wss://stream.bybit.com/v5/public/linear` |
| Testnet | `wss://stream-testnet.bybit.com/v5/public/linear` |
| Тема | `allLiquidation.{symbol}`, біржовий id: `BTC/USDT:USDT` → `allLiquidation.BTCUSDT` |
| Підписка | окремий запит на кожен символ: `{"req_id":"allLiquidation.BTCUSDT","op":"subscribe","args":["allLiquidation.BTCUSDT"]}` |
| Успіх | `success: true`, `op: subscribe`, той самий `req_id` |
| Відхилення | `success: false` (або наша тема в `data.failTopics`), той самий `req_id`; mainnet: `ret_msg: "error:handler not found,topic:allLiquidation.…"` |
| Ping | JSON `{"op":"ping"}` кожні 20 с. Відповідь публічного linear: `op = ping`, `ret_msg = pong` (без мітки часу) |

Чому окремі запити: якщо в одному запиті хоч одна тема невідома (опечатка чи знятий з торгів символ), mainnet
відхиляє запит цілком — `success: false`, без `failTopics`, і не видно, чи решта тем підписалась. З окремим запитом
невідомий символ дає лише `liquidation_topic_rejected` (error, з `ret_msg`), а решта працює; його години стають
прогалинами. Відхилено всі символи — нове з'єднання з backoff. Відповідь без нашого `req_id` ігнорується.

Ключі не використовуються. Без ping mainnet закриває з'єднання приблизно за 60 с (виміряно: 60,7 с, код 1006); з ping
кожні 20 с з'єднання тримається. Колектор вважає потік мертвим після 60 с без жодного кадру, а підписку — після 10 с,
якщо хоч на один символ немає відповіді (`liquidation_subscribe_timeout` з переліком).

`aiohttp` 3.14: `receive(timeout)` на `TimeoutError` ставить сокету abnormal closure. Читання йде без таймауту;
ping і перевірку тиші шле окрема задача. Відкриття з'єднання (DNS, TCP, TLS, upgrade) обмежене `HANDSHAKE_TIMEOUT_S`
(15 с): `ClientTimeout` колектора не покриває відповідь на upgrade, і без цього `connect` міг чекати вічно.

## Кадр

Приклад з документації (ROSEUSDT, `S = Sell` — ліквідація short):

```json
{
    "topic": "allLiquidation.ROSEUSDT",
    "type": "snapshot",
    "ts": 1739502303204,
    "data": [
        {
            "T": 1739502302929,
            "s": "ROSEUSDT",
            "S": "Sell",
            "v": "20000",
            "p": "0.04499"
        }
    ]
}
```

| Поле | Зміст |
| --- | --- |
| `T` | час ліквідації, мс UTC |
| `s` | символ |
| `S` | сторона **позиції**: `Buy` — ліквідовано long («When you receive a Buy update, this means that a long position has been liquidated»); `Sell` — short |
| `v` | виконаний розмір, рядок, базова валюта |
| `p` | bankruptcy price, рядок, USDT |
| `ts` | час кадру, мс |

`v` і `p` приймаються лише як додатні скінченні десяткові рядки (не JSON-число). Обсяг USDT — `v × p` без округлення
(у прикладі `20000 × 0.04499 = 899.8`). У `liquidations_hourly` це `short_qty` / `short_notional` або
`long_qty` / `long_notional`, плюс лічильник. Id виконання немає: два однакові кадри в ще відкритій годині рахуються
обидва. Уже записану годину пізній кадр не змінює (`ON CONFLICT DO NOTHING`, лог `liquidation_late_event`).

Чужий символ ігнорується. Зламаний кадр нашої теми псує цю годину і поточну: нулем їх не закриваємо.

Справжній кадр з mainnet (2026-10-30 12:29 UTC) має ту саму форму, що й приклад з документації:

```json
{"topic":"allLiquidation.AVAXUSDT","type":"snapshot","ts":1793363346256,"data":[{"T":1793363345991,"s":"AVAXUSDT","S":"Buy","v":"1783.2","p":"8.135"}]}
```

Стара тема `liquidation.{symbol}` на mainnet відповідає `handler not found`, тож `allLiquidation` — єдине джерело.

Ліквідації на великих символах бувають рідко. За ~20 хв проб вісім `research_symbols` не дали жодної, хоча угоди
BTCUSDT ішли сотнями кадрів на хвилину, а інші символи (AVAX, 1000PEPE) події доставляли. Тож години з нулями — це
нормальні дані. Чого потік не показує: біржа могла б підтвердити підписку й мовчки не слати події, тоді нулі були б
хибними. Ознак цього не видно, але окремої перевірки в коді немає (`docs/decisions/0012-liquidation-stream-verified.md`).

## Коли година потрапляє в таблицю

Година символу пишеться, якщо одночасно:

- його підписку підтверджено щонайменше за `CLOCK_MARGIN_MS` (1 с) до початку години;
- кадр прийшов щонайменше через 1 с після кінця години, а від кінця минуло `SEAL_MARGIN_MS` (30 с, довше за ping 20 с);
- увесь цей час годиннику вірили.

Запас 1 с потрібен, бо межі години рахуються за локальним годинником, а події — за часом біржі `T`. Колектор тримає
власний `ClockGuard` (ТЗ §4.3): `fetch_time` при старті, кожні 10 хв і щосекундна перевірка стрибків. Годиннику
вірять, коли є свіжий вимір і |зсув| + похибка < 1 с. Немає виміру, зсув ≥ 1 с або стрибок → відкриті години
відкидаються (`liquidation_clock_untrusted`, warning), уже готові лишаються. Після цього відлік повних годин іде
заново з моменту, коли годиннику знову вірять (`liquidation_clock_trusted`).

Тиха година, прослухана цілком, пишеться нулями: інакше детектор прогалин прийняв би мовчання за простій.
Недослухана година в таблицю не йде.

Запис у БД іде окремою задачею, а цикл читання її не чекає. Після помилки БД наступна спроба — не раніше ніж через
1 с, далі ×2 до 60 с (`liquidation_save_failed` / `liquidation_gap_sync_failed` з `retry_in_ms`); відновлення —
`liquidation_db_recovered`. Готові години чекають у пам'яті. При зупинці — останній запис, не довше 3 с
(`liquidation_final_flush_timeout`).

Прогалина — `data_gaps` з `source = liquidations`, `timeframe = 1h`, `exchange = bybit`, інтервал `[from_ms, to_ms)`.
Сітка — від `max(BACKFILL_START_MS, перша година ≥ launch_time_ms)`, якщо в `instruments` є лістинг. Немає рядка
кешу — від `BACKFILL_START_MS`. Період до лістингу діркою не є. Префікс від цієї підлоги до першої повністю
прослуханої години лишається відкритим назавжди: докачати його не можна. Це очікувано. Дірка вже всередині зібраного
хвоста — втрата.

## Як перевірити добу

Колектор: `cd apps/backend && uv run python -m app.data.collector`. Логи — `logs/collector/`. На одній БД — один
процес (lock `COLLECTOR_LOCK_ID` = 3).

Відкриті прогалини:

```sql
SELECT symbol, from_ms, to_ms
FROM data_gaps
WHERE source = 'liquidations' AND resolved_at_ms IS NULL
ORDER BY symbol, from_ms;
```

Добова перевірка пройдена, коли за останні 24 години немає відкритої дірки, крім ще не закритої поточної години
і постійного історичного префікса до першої повної години. Рядки `liquidations_hourly` за ті самі години мають бути
на кожен `research_symbols`, включно з нульовими.
