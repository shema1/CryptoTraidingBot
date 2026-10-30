# `app/ml` — Фічі, симулятор угоди, мітки, валідація, бектест, інференс

## Призначення

ML-конвеєр: `compute_features()` (один для навчання і live), єдиний симулятор угоди `trade_sim.py`, meta-мітки й ваги унікальності, walk-forward з embargo, DSR/PBO, бектест, калібрування і правило EV, артефакти моделей, інференс.

> Заплановано: T2.3, T2.4, T2.5, T2.6, T2.7, T2.8, T2.9, T2.11. Фічі й буфер — T2.1. Моделі БД — T0.4.

## Розділи ТЗ

- ТЗ §5 — ML-пайплайн
- ТЗ §5А — практичний посібник з навчання

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `ModelArtifact`, `Experiment`, `ExperimentTrial`, `HoldoutOpening` | `models.py` | `models`, `experiments`, `experiment_trials`, `holdout_openings` |
| `MODEL_STATUSES`, `LABEL_TYPES`, `TRIAL_STATES` | `models.py` | статуси моделі, `meta`/`direct`/`rule_only`, `COMPLETE`/`PRUNED`/`FAILED` |
| `compute_features`, `FeatureBuffer`, `FeatureRows` | `features.py` | фічі свічки `t` з даних, відомих на закриття `t-1`; буфер останнього рядка |
| `feature_columns`, `feature_schema_hash`, `PREPROCESSING_VERSION` | `features.py` | склад вектора і `sha256` опису; версія формул `1.0.0` |
| `sources_covering`, `funding_gap_ranges`, `load_feature_gaps` | `features.py` | які джерела деривативів покривають навчальне вікно; відкриті дірки з БД |
| `derivative_window`, `DERIVATIVE_LAG_MS` | `features.py` | години `[start, end)`, які читає фіча дериватива для рядка `T`; лаг годинних деривативів |

## Фічі (T2.1, ТЗ §5.1, §5.5, §5А.4)

Рядок з `entry_open_time_ms = T` бере свічки лише з `[T - FEATURE_LOOKBACK_BARS·1h, T)`. Годинна свічка входить, якщо `open_time_ms < T`. Старші 4h і 1d збираються з цих годин; береться рівно `H4_BARS` і `D1_BARS` останніх повністю закритих (`початок + крок <= T`), межі — від півночі UTC. Кількість — найбільша, що вміщається у вікно за будь-якої години рішення. Вона стала: якби в 1d бралось усе, що є у вікні, діб було б то на одну більше, то менше, і EMA/ATR на 1d зсувались би о 05:00 UTC без нової денної свічки. Тож старші фічі змінюються лише тоді, коли закривається нова старша свічка.

Деривативи. Година `ts_ms` закривається в `ts_ms + 1h`, але live отримує її пізніше за рішення: ліквідації запечатуються через `SEAL_MARGIN_MS` (`app/data/liquidations.py`), OI, premium і long/short колектор докачує раз на `BACKFILL_INTERVAL_S`, а сигнал старіє за `max_signal_age_ms`. Тому фіча читає лише години, що закрились не пізніше `T - DERIVATIVE_LAG_MS`: за лагу в годину остання — `[T-2h, T-1h)`. Офлайн правило те саме, тож навчання не бачить того, чого live не матиме вчасно. Premium — це `close` години, не `open` (рішення `docs/decisions/0009-premium-open.md`). Funding пише прохід свічок, лагу в нього немає: остання подія з `funding_time_ms < T` і віком не більшим за `FUNDING_MAX_STALE_MS`. Ставка з міткою рівно `T` не входить, бо в live вона може ще не бути в БД. Попередня ставка віком рівно 8 год лишається придатною.

Прогалини. Рядок не рахується (усі колонки NaN, `valid` хибний), якщо дірка перетинає години, які він читає. Для свічок це все вікно: EMA і ATR ідуть по ньому цілком. Для дериватива — лише його `derivative_window`: зміна OI — `OI_CHANGE_BARS + 1` годин, ліквідації — `LIQ_BARS`, premium і long/short — одна година, funding — `[T - FUNDING_MAX_STALE_MS, T)` для явних дірок плюс свіжість самої ставки. Тож одна втрачена година ліквідацій (кожен перезапуск колектора) вибиває `LIQ_BARS` рядків, а не все вікно. Дірка сітки — `find_gaps`, явний відкритий інтервал — `window_has_gap` (обидва з `app/data/gaps.py`, межі `[start, end)`). `load_feature_gaps` читає лише відкриті рядки через `open_gaps_in_window` для `candles`, `open_interest`, `premium_index`, `long_short`, `liquidations`. У `data_gaps` немає джерела funding: покриття навчального вікна для нього дає `funding_gap_ranges`. Інтерполяції немає.

Колонки — `PRICE_COLUMNS`, потім увімкнені з `DERIVATIVE_COLUMNS`, потім `TIME_COLUMNS`. Ідентифікатора символу серед них немає. З усіма п'ятьма деривативами колонок 30. З прикладів ТЗ §5А.4 не входять корельовані вікна: log-return за 3 і 12, std за 12, зміна OI за 1 і 6. `ema_spread_atr` — контекст сили правила (відстань EMA(20) і EMA(50) поділена на ATR). Джерело дериватива зникає з вектора, якщо `coverage_start_ms`/`coverage_end_ms` задані і хоч один символ має дірку на цьому вікні: інакше ліквідації з постійним історичним префіксом (T1.10) зробили б NaN кожен рядок навчання. Це вікно має бути навчальним відрізком фолду, без hold-out (T2.5). Буфер не звіряє годинник: у нього кладуть лише закриті свічки.

`FeatureBuffer` тримає останні `FEATURE_LOOKBACK_BARS` годин (1300 — запас під EMA(50) на 1d за будь-якого зсуву від півночі UTC) і викликає той самий `compute_features()`. Старше за вікно відкидається, коли приходить новіша свічка. Точка дериватива, що прийшла раніше за свою свічку (колектор і прохід свічок — різні процеси), лишається і увійде у фічу, коли до неї дійде вікно. Числа вікон, лаг і насінина EMA живуть у константах `features.py`; вони входять у `feature_schema_hash`. Зміна формули без зміни цих чисел вимагає підняти `PREPROCESSING_VERSION`.

Розрахунок на numpy, без pandas: немає `shift(-n)`, `bfill` і `resample`. Вікно рядка береться зрізом за `searchsorted`, тож вартість рядка не залежить від довжини історії (~0,8 мс; 5 років 1h на символ — близько 35 с). Ціни на межі ML — float; у торговому шарі лишається Decimal. numpy обмежено `<2.5`, щоб навчання й live під будь-яким підтримуваним Python мали одну версію.

## Таблиці БД

- `models` — PK `model_version`; поля з `model_metadata.json` (ТЗ §5.4) + повний JSON у `metadata_json`;
  `status` `TRAINED`/`CANDIDATE`/`APPROVED`/`RETIRED`. Обмеження: `git_dirty` → не `CANDIDATE`/`APPROVED`;
  `CANDIDATE`/`APPROVED` мають `live_corridor`; `APPROVED` має `confirmed_audit_id` — підтверджений запис `audit_log` з дією `model.approve` і `target = model_version`
  (тригер; один запис — одна модель; ставить лише людина); `trade_symbol` і `research_line` — канонічний символ;
  `artifact_path` порожній ⇔ `label_type = 'rule_only'`; `validated_by` — інша модель.
- `experiments` — `id` (`exp-0042`), `research_line` (= торговий символ), `symbols` (кожен — канонічний символ), первинна стратегія, параметри,
  seed, діапазон даних, `n_trials_line` (знімок кількості спроб лінії на момент DSR), `dsr`, `pbo` ∈ [0, 1].
- `experiment_trials` — **append-only** (тригер): `UNIQUE (experiment_id, trial_no)`, `state`, `params`,
  `oos_results` (ряд OOS-результатів для DSR/PBO), `metrics`. Невдалі й обрізані спроби теж записуються.
- `holdout_openings` — кожне відкриття hold-out; `opening_no ∈ {1, 2}` + `UNIQUE (research_line, holdout_from_ms,
  holdout_to_ms, opening_no)`: третє відкриття того самого відрізка → `IntegrityError` (ТЗ §5А.10, рішення 0005).

Схема створюється міграцією `migrations/versions/0001_initial_schema.py`; обмеження й тригери перевіряють `tests/db/test_constraints.py`, `tests/db/test_guards.py` і `tests/db/test_migrations.py`.


## Стан реалізації

| Задача | Статус | Що зроблено |
| --- | --- | --- |
| T0.4 | `[x]` | таблиці `models`, `experiments`, `experiment_trials`, `holdout_openings` |
| T2.1 | `[x]` | `compute_features()` + кільцевий буфер, schema hash, прогалини |
| T2.3 | `[ ]` | Симулятор угоди, розмітка, ваги |
| T2.4 | `[ ]` | Бектестер |
| T2.5 | `[ ]` | Валідація |
| T2.6 | `[ ]` | Статистика і Gate G1 |
| T2.7 | `[ ]` | Бейзлайни і первинна стратегія |
| T2.8 | `[ ]` | Meta-модель LightGBM |
| T2.9 | `[ ]` | Калібрування і правило EV |
| T2.11 | `[ ]` | Артефакти і статуси моделі |
