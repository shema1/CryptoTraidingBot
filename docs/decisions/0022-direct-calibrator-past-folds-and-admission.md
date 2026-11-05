# 0022. Допуск, артефакт і перенавчання для `direct`: калібратор у файлі, правило лише з минулих фолдів, без первинного правила

- **Дата:** 2026-11-05
- **Статус:** прийнято (2026-11-05, підтвердив користувач: калібратор у файлі, `primary_strategy` = `{}`, правило минулих фолдів — блокер цієї задачі); реалізовано в T2.11-розширення.
- **Рішення користувача щодо G1 (2026-11-05):** варіант (a) — `direct` не проходить Gate G1 на порогах ТЗ; пороги G1 не змінюються; варіанти (b) «прогрів фолдів 0–1» і (c) «послабити мінімум угод на фолд для direct» відхилено і не реалізовані. Причина: фолди 0 і 1 мають 0 угод за правилом минулих фолдів, а `_decide` вимагає `n_trades ≥ min_trades_per_fold` на кожному фолді. Код допуску, артефакту і перенавчання лишається, але `CANDIDATE` для `direct` не досягається, поки G1 не проходить. Hold-out для `direct` не відкривався.
- **Зачеплені розділи ТЗ:** §5.1 п. 2, §5.4, §5.4.1, §5.5.1, §5.3, §5А.10 (Gate G1), §5А.3 п. 13
- **Зачеплені задачі:** T2.11 (розширення на `direct`), T2.13 (зміна рішення 0021)
- **Змінює:** `docs/decisions/0021-direct-three-class-entry-and-g1.md` — пункти «Обмеження» (правило минулих фолдів) і «Не зроблено» (допуск, перенавчання, fail-closed); твердження «артефакт `direct` без калібратора» знято.

## Контекст

0021 фіксувало, що `direct` не має калібратора в артефакті, що hold-out для `direct` відмовляє fail-closed до допуску, і що перед будь-яким відкриттям hold-out за `direct` має бути перевірка «правило входу лише з минулих фолдів». Для реалізації допуску і перенавчання з'ясувалось:

1. Правило входу (`params.decision`, `decision_payload`) містить sha256 калібратора. Допуск і перенавчання мають звіряти його з журналом. Без файлу калібратора перенавчання не могло б його скопіювати, а звірка з журналом була б перевіркою без змісту.
2. Експеримент `direct` має `primary_strategy={}` (колонка `experiments.primary_strategy` не `NULL`), а `build_metadata` вимагав `StrategySpec` з `PRIMARY_CANDIDATES`.
3. Перехресна оцінка T2.13 брала для фолду k моделі й рядки інших фолдів, у тому числі пізніших. Вимога 0021 — правило фолду k бачить лише фолди `< k` — не була виконана.

## Рішення

**1. Артефакт `direct` має той самий склад, що `meta`.** `models/<version>/` = `model_metadata.json`, `model.txt` і `calibrator.json` (hex, без pickle), хеші sha256 обох у метаданих. Відновлення калібратора не викликає `fit`. `load_model` і `write_artifact` звіряють обидва файли й метод калібратора з `recipe.calibrator`.

**2. `primary_strategy` для `direct` — `{}`, не `null`.** Обмеження БД: колонка `experiments.primary_strategy` не приймає `NULL`. Той самий порожній словник пишеться в артефакт, тож звірка `_require_validation_model` (`meta.primary_strategy == experiment.primary_strategy`) проходить без винятку. Передача `StrategySpec` для `direct` — `ArtifactError`. Перевірка рецепта в `_recipe_matches_trial` для `direct` замінює `strategy` на ідентичність спроби: `stage == direct_model`, `role == direct`, `objective == multiclass`, `classes == [FLAT, LONG, SHORT]`, `shuffled_labels == false`. Список класів в `artifacts.py` дублює `direct_model.CLASS_LABELS` (імпорт дав би цикл); тест `test_artifact_class_order_is_the_model_class_order` звіряє їх.

**3. Правило входу для журналу G1 — лише минулими фолдами.** Правило фолду k підігнане `fit_decision` на OOF-рядках фолдів `< k`. Пізніші фолди не входять ні в пул, ні в калібратор. Фолди `< PAST_FOLDS_MIN = 2` мають правило `None` і угод не мають. Ці два фолди — наслідок вимоги: `fit_decision` калібрується щонайменше на двох фолдах, тож правила для фолду 0 і 1 не існує. Це свідомий наслідок, не підбір. Позначка `params.entry_evaluation = "past_folds"` (`ENTRY_EVALUATION_PAST_FOLDS`), `params.entry_rules[k]` містить `fit_on_folds`. Якщо жодного правила немає або калібрування фолду k ≥ 2 не вдалось — `cross_error`, угод немає, G1 відмовляє (`G1_ENTRY_RULE`). Правило, яке йде в допуск і live (`params.decision`), лишається підігнаним на всіх OOF-рядках, як у meta (рішення 0020).

**4. Fail-closed для hold-out `direct` знято.** `open_holdout_after_gate` для `direct` іде тим самим шляхом, що для `meta`: G1 з правилом минулих фолдів і стресом з журналу, без умови `G1_FILTER`. Без `model_version` — `ValueError`; відмова G1 — `GateRejected` без запису.

**5. Допуск і перенавчання `direct`.** Модель допуску — `fit_frozen_direct` без гранту (лише до hold-out, `reject_holdout`). Вона має відтворити файл `model.txt` з допуску (sha256), інакше `ArtifactError`. Перенавчання — `fit_frozen_direct` з `HoldoutGrant` того самого вікна. Калібратор і `decision_rule` копіюються з файлу допуску, не вчаться. Підбору немає.

**6. Сигнали hold-out для `direct`.** `_holdout_signals` застосовує правило журналу (`decide_entry`) до рядків вікна hold-out тією самою моделлю допуску. Звіт бектесту будує викликач, як для meta. Модель з грантом сигналів не рахує.

## Наслідки

- **Код:** `app/ml/decision.py` (`ENTRY_EVALUATION_PAST_FOLDS`), `app/ml/direct_model.py` (`_fit_past`, `PAST_FOLDS_MIN`, `_entry_rule_record`, `fit_frozen_direct`, `FrozenDirectFit`, `_holdout_signals`, `_bars` з параметрами відрізання), `app/ml/artifacts.py` (`build_metadata`, `write_artifact`, `_load_directory`, `_recipe_matches_trial`, `retrain_registered_recipe`, `assert_runtime`), `app/ml/statistics.py` (`entry_rule_evaluated` для `direct`, знято fail-closed). Схема БД не змінена, міграції немає.
- **Тести:** `tests/ml/test_direct_admission.py` (нові: склад артефакту, калібратор, допуск, перенавчання, кандидат, Live лише з APPROVED, відмова G1 без запису), `tests/ml/test_direct_model.py` (правило минулих фолдів, `test_flipping_the_labels_of_a_fold_never_changes_a_rule_fit_before_it`; тест `test_direct_holdout_never_opens_fail_closed` видалено — його передумова знята цією задачею, замість нього в `test_direct_admission.py` тест відмови G1).
- **Live-інференс `direct`:** не реалізовано. `predict_probability` і `infer_entry` приймають лише `meta` (T5.x). `direct` не стає `APPROVED` для торгівлі, доки T5.x не додасть інференс.

## Обмеження

- Пул правила фолду k — рядки минулих фолдів, чия мітка (до `horizon_bars` свічок) закінчується до початку тесту фолду k (`_past_pool`, purge як у walk-forward). Без purge мітки останніх свічок минулого фолду читали б ціни тесту.
- Звіт hold-out для допуску `direct` подає викликач (як для meta, відомий борг T2.12); `_holdout_signals` — приватна, без відкриття не викликається.

- **G1 для `direct` на порогах ТЗ не проходить і зараз.** Фолди 0 і 1 мають 0 угод (правило минулих фолдів), а `_decide` (`statistics.py`) відмовляє, якщо будь-який фолд має `n_trades < min_trades_per_fold`. Мінімум фолду — з розбивки (≥ 1), `max(limits, split_min)` його не знижує. Частка прибуткових фолдів при цьому не перевищує (n−2)/n. Отже допуск, перенавчання і `CANDIDATE` для `direct` законним шляхом недосяжні; знятий fail-closed лишився формальним. Пом'якшити G1 або змінити правило фолдів 0–1 може лише людина (AGENTS, інваріант 14); цю задачу не чіпаємо. Варіанти для рішення: (a) прийняти, що direct не проходить до переробки; (b) зробити правило фолдів 0–1 з окремого прогріву, що не входить у G1; (c) змінити вимогу мінімуму угод на фолд для direct — лише з явним рішенням людини.
- Схема фіч допуску — повне покриття `[data_from, holdout_from)`, схема в журналі — схема останнього фолду. Так само, як у meta: розбіжність покриття — відкрите питання T2.11, не змінюється цим рішенням.
- Прохід G1 і відкриття hold-out через `open_holdout_after_gate` для `direct` не перевірені ні на сценарних, ні на реальних даних: на порогах ТЗ G1 не проходить (див. вище). Тести `test_direct_admission.py` покривають відмову G1 і ланцюг допуску через приватне `_open_holdout` з синтетичним G1.

## Перевірка (2026-11-05)

- Нові тести `test_direct_admission.py` (7) і оновлені `test_direct_model.py` зелені; `tests/ml` — зелені.
- Рев'ю `ml-leakage-auditor` і `trading-safety-reviewer` — див. `docs/ai/TASKS.md`, задача T2.11-розширення.
