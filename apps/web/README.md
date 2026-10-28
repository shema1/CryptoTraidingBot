# `apps/web` — веб-інтерфейс

React + Vite + TypeScript. Лише відображення й команди: бізнес-логіки (розмір позиції, ризики, PnL)
у фронтенді немає — усе приходить з API (ТЗ §16, `.claude/rules/frontend.md`). Сервер розробки слухає лише `127.0.0.1`.

## Команди

| Команда | Що робить |
| --- | --- |
| `npm install` | встановити залежності (перший раз; створює `package-lock.json` — комітиться) |
| `npm ci` | встановити рівно за `package-lock.json` |
| `npm run dev` | dev-сервер на `http://127.0.0.1:5173` |
| `npm run typecheck` | перевірка типів (`tsc --noEmit`) — обов'язкова перевірка фронтенду |
| `npm test` | vitest; поки тестів немає. Єдиний плановий тест — покриття підказок (T6.5) |
| `npm run gen:api` | генерація TS-типів з OpenAPI — **поки заглушка** |
| `npm run build` | перевірка типів + production-збірка в `dist/` |

> Заплановано: T6.4 — `gen:api` через openapi-typescript; T6.5 — `<FieldHelp>` і тест покриття довідки;
> T6.7–T6.10 — екрани; T6.8 — Lightweight Charts.

## Тести

Лише `npm run typecheck` і (з T6.5) тест покриття підказок. E2E, snapshot- і компонентних тестів не пишемо
(`.claude/rules/testing.md`).
