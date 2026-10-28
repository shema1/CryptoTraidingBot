# `app/ml` — Фічі, симулятор угоди, мітки, валідація, бектест, інференс

## Призначення

ML-конвеєр: `compute_features()` (один для навчання і live), єдиний симулятор угоди `trade_sim.py`, meta-мітки й ваги унікальності, walk-forward з embargo, DSR/PBO, бектест, калібрування і правило EV, артефакти моделей, інференс.

> Заплановано: T2.1, T2.3, T2.4, T2.5, T2.6, T2.7, T2.8, T2.9, T2.11. Зараз є лише моделі БД (T0.4).

## Розділи ТЗ

- ТЗ §5 — ML-пайплайн
- ТЗ §5А — практичний посібник з навчання

## Публічний інтерфейс

| Що | Де | Опис |
| --- | --- | --- |
| `ModelArtifact`, `Experiment`, `ExperimentTrial`, `HoldoutOpening` | `models.py` | `models`, `experiments`, `experiment_trials`, `holdout_openings` |
| `MODEL_STATUSES`, `LABEL_TYPES`, `TRIAL_STATES` | `models.py` | статуси моделі, `meta`/`direct`/`rule_only`, `COMPLETE`/`PRUNED`/`FAILED` |

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
| T2.1 | `[ ]` | `compute_features()` + буфер |
| T2.3 | `[ ]` | Симулятор угоди, розмітка, ваги |
| T2.4 | `[ ]` | Бектестер |
| T2.5 | `[ ]` | Валідація |
| T2.6 | `[ ]` | Статистика і Gate G1 |
| T2.7 | `[ ]` | Бейзлайни і первинна стратегія |
| T2.8 | `[ ]` | Meta-модель LightGBM |
| T2.9 | `[ ]` | Калібрування і правило EV |
| T2.11 | `[ ]` | Артефакти і статуси моделі |
