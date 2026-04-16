# DataSUS ETL — Web UI

Vite + React + TypeScript frontend for the DataSUS ETL backend.

## Stack

- Vite 5, React 18, TypeScript 5
- Tailwind CSS 3 + shadcn-style primitives (Radix UI + CVA)
- TanStack Query v5, TanStack Router v1
- Recharts, date-fns, sonner, Monaco (via `@monaco-editor/react`)
- Bun as the package manager / runtime

The dev server proxies `/api/*` to the FastAPI backend at
`http://127.0.0.1:8787`. Production builds emit into
`../src/datasus_etl/web/static/` so the FastAPI app can serve them directly.

## Commands

```bash
# install deps
bun install

# dev server at http://localhost:5173 (proxies /api to http://127.0.0.1:8787)
bun run dev

# type-check
bun run lint

# production build -> ../src/datasus_etl/web/static/
bun run build

# install Playwright's chromium browser (run once)
bun run test:e2e:install

# end-to-end tests (spawns uvicorn + vite automatically)
bun run test:e2e
```

## Running alongside the backend

Start the FastAPI backend in one terminal:

```bash
uvicorn datasus_etl.web.server:create_app --factory --port 8787
```

Then in another:

```bash
cd web-ui
bun run dev
```

Open <http://localhost:5173>.

## Project layout

```
src/
  main.tsx
  router.tsx
  globals.css
  lib/
    api.ts     — typed fetch wrappers + TS interfaces
    sse.ts     — pipeline progress EventSource helper
    format.ts  — formatBytes / formatNumber / formatRelative
    query.ts   — TanStack Query client
    utils.ts   — cn() helper
  components/
    ui/        — shadcn-style primitives
    Layout, Sidebar, TopBar, StatCard, SubsystemCard,
    VolumeChart, BrazilMap, EmptyState, ThemeProvider, ThemeToggle
  pages/
    Dashboard, DownloadWizard (steps 1–4), Query, Settings
  hooks/       — useStats, useSettings, usePipelineRun, useSqlQuery
tests/         — Playwright end-to-end tests
```

## Notes

- `BrazilMap` currently renders a grid of 27 labelled UF cells as a
  placeholder. The component API (`valuesByUf`, `selected`, `onToggleUf`)
  is stable — dropping in real SVG geometry is a drop-in change.
- Dark mode is the default. Toggle via the top bar.
- SQL queries are validated server-side (SELECT/WITH only); the editor
  sends Ctrl+Enter to run and supports CSV / XLSX export.
