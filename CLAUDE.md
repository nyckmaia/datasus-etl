# CLAUDE.md

Guidance for Claude Code (and compatible agents) working in this repository.

## What this project is

`datasus-etl` is a Python ETL for Brazilian public-health data (DATASUS).
It downloads DBC files from the DATASUS FTP, converts them through DBF into
partitioned Parquet, and exposes the result via a CLI, a Python API, a
DuckDB shell, and a packaged FastAPI + React web UI.

## Layout at a glance

```
src/datasus_etl/
  cli.py                      # Typer CLI entrypoints: pipeline, update, status, db, ui
  config.py                   # PipelineConfig.create(...)
  core/context.py             # PipelineContext: shared state + progress callback
  datasets/
    base.py                   # DatasetConfig ABC + ViewSpec (schema, parse_filename, file_prefix, views)
    sihsus/, sim/             # Per-subsystem schema + filename parser + SQL
  download/ftp_downloader.py  # FTP client + date-range helpers
  pipeline/
    base_pipeline.py          # Orchestration template
    sihsus_pipeline.py        # SIHSUS-specific pipeline
    sim_pipeline.py           # SIM-specific pipeline
    estimate.py               # "How many files / bytes?" preview for the wizard
  storage/
    paths.py                  # resolve_parquet_dir — single source of truth (see "Paths" below)
    migration.py              # Detects + migrates the legacy double-nested layout
    parquet_manager.py        # Reads/writes partitioned parquet (union_by_name + filename flags), builds DuckDB VIEWs
    duckdb_query_engine.py    # SQL facade over the parquet store
  transform/
    converters/dbc_to_dbf.py  # Pure-Python DBC → DBF
    converters/dbf_to_parquet.py
    sql/                      # Subsystem-specific enrichment / normalisation SQL
  web/
    server.py                 # FastAPI app factory. Mounts /api + SPA fallback.
    runtime.py                # In-process pipeline runner bridging to SSE
    routes/                   # settings, stats, pipeline, query, export
    static/                   # Built SPA — GITIGNORED, see "Frontend workflow"
tests/
  unit/                       # test_paths.py, test_migration.py, etc.
  integration/
web-ui/
  src/                        # Vite + React + TS + Tailwind + shadcn
  package.json                # Bun scripts: dev / build / lint / test:e2e
  tests/                      # Playwright e2e
```

## Frontend workflow (critical)

The SPA bundle lives in `src/datasus_etl/web/static/` and is served by
FastAPI. That directory is **gitignored** and **pre-built**. Two pitfalls
follow:

1. Editing files under `web-ui/src/` will NOT change what `datasus ui`
   serves until you run:
   ```bash
   cd web-ui && bun run build
   ```
   The build emits into `../src/datasus_etl/web/static/` (configured in
   `web-ui/vite.config.ts`).

2. Vite uses content-hashed filenames (`index-<hash>.js`). Browsers can
   still serve a stale file after a rebuild — always suggest a **hard
   refresh** (Ctrl+Shift+R) when validating UI changes via the FastAPI
   server. For iterative development prefer `bun run dev` on port 5173
   (proxies `/api/*` to `http://127.0.0.1:8787`).

`bun run lint` runs `tsc --noEmit` — use it to validate type changes
before claiming frontend work is done.

## Paths — one rule, centralised

`storage/paths.py::resolve_parquet_dir` is the *only* function that
decides where parquet data lives. Every other caller (`config`,
`ParquetManager`, web routes) delegates here. The rules, in order:

1. If `base_dir.name` is already `datasus_db` or `parquet`
   (case-insensitive), append only the subsystem.
2. If a legacy `{base_dir}/parquet/{subsystem}` exists, return it
   unchanged (pre-0.1 layout).
3. Otherwise, return `{base_dir}/datasus_db/{subsystem}`.

Do **not** re-implement this logic elsewhere. The original bug that
motivated this module produced a double-nested
`{base}/datasus_db/datasus_db/` layout; `storage/migration.py` detects
and migrates it at CLI startup.

## SIM filename parsing — a landmine to respect

`datasets/sim/config.py::parse_filename` disambiguates CID9 vs. CID10
**by stem length**, not by prefix:

- `DOUFYYYY.dbc` — 8-char stem → CID10 (1996+)
- `DORUFYY.dbc`  — 7-char stem → CID9  (1979-1995)

Checking "starts with `DOR`" first (the previous implementation) silently
dropped every CID10 death record from RJ, RN, RO, RR, and RS. Preserve
the length-based check if you refactor that function.

SIM also publishes with a ~2-year lag — the web UI warns about this in
Step 2 of the wizard. Don't assume "missing files" means a bug.

## Backend stack highlights

- **FastAPI** app factory in `web/server.py`. Mounts `/api/settings`,
  `/api/stats`, `/api/pipeline`, `/api/query` (with `/schema` for the catalog tree), `/api/export`.
- **SSE** progress: `web/runtime.py` bridges the synchronous
  `PipelineContext` progress callback to an asyncio queue, streamed from
  `/api/pipeline/progress`. The React hook `usePipelineRun` consumes it.
- **Native folder picker**: `settings.py::/api/settings/pick-directory`
  spawns a one-shot Python subprocess that runs tkinter. This avoids the
  "Tk must run on the main thread" failure mode when called from uvicorn
  workers (especially on macOS). Keep that subprocess pattern if you
  touch it.
- **SQL editor is read-only**: `web/routes/query.py::_validate_sql` strips
  block (`/* */`) and line (`-- ...`) comments before checking, then
  enforces an allowlist (`SELECT`/`WITH` only) and a denylist of mutating
  keywords (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, ATTACH, COPY,
  PRAGMA, EXPORT, IMPORT, REPLACE, TRUNCATE, MERGE, UPSERT, CALL, INSTALL,
  LOAD, SET, USE, DETACH, CHECKPOINT, VACUUM, GRANT, REVOKE, BEGIN,
  COMMIT, ROLLBACK). Applied to both `/api/query/sql` and `/api/export`.
- **Hierarchical schema endpoint**: `GET /api/query/schema` returns the
  full SUBSYSTEM → VIEWS → COLUMNS tree consumed by the `/query` page's
  left sidebar. Discovery convention: main view = subsystem name, dim
  views = `{subsystem}_dim_*`, raw `{subsystem}_all` is hidden. Override
  via `DatasetConfig.views: list[ViewSpec]` (see `datasets/base.py`).
- **Path validation**: `/api/settings/validate-path` returns metadata
  (exists, writable, has data) so the UI can warn before saving.
- **Settings persistence**: `~/.config/datasus-etl/config.toml` via
  `web/user_config.py`.

## CLI quick reference

```bash
datasus pipeline -s sihsus --start-date 2023-01-01 -d ./data
datasus update   -s sihsus --start-date 2023-01-01 -d ./data
datasus status   -s sihsus -d ./data
datasus db       -d ./data                    # DuckDB interactive shell
datasus ui                                    # FastAPI on :8787, opens browser
```

All CLI entrypoints share the migration check from
`storage/migration.py` — they'll prompt before rewriting a legacy layout
unless you pass `--yes`.

## Testing conventions

- `pytest` at repo root. Unit tests live in `tests/unit/`, integration
  in `tests/integration/`.
- Notable suites: `test_paths.py` + `test_migration.py` cover every
  documented rule of the path/migration logic — regressions there break
  storage for every user.
- Frontend: `cd web-ui && bun run test:e2e` (Playwright). The e2e
  harness spawns uvicorn and vite automatically.
- `bun run lint` = `tsc --noEmit`. There's one pre-existing unused-React
  import warning in `Step1Subsystem.tsx`; ignore it unless you're
  touching that file.

## Common pitfalls

- **Don't edit `src/datasus_etl/web/static/`** — it's a build artifact.
  Edit `web-ui/src/` and rebuild.
- **Don't reimplement path logic** — always call
  `resolve_parquet_dir`.
- **SIM parser length check** — see the landmine above.
- **TanStack Router search params**: when adding navigations that carry
  data (e.g. the Dashboard → Scope shortcut), declare
  `validateSearch` on the route and use `<Link to=... search={...} />`
  rather than string concatenation. An optional field must be typed
  `subsystem?: string` (not `string | undefined`), otherwise the router
  treats it as required at every call site.
- **SPA fallback**: `/api/*` paths that miss a real route return 404 —
  they must not fall through to `index.html`. See the guard in
  `_register_spa_routes`.

## Useful external references

- DATASUS FTP: `ftp.datasus.gov.br/dissemin/publicos/`
- Monaco, TanStack Router, shadcn/ui, Tailwind — all documented on
  their official sites; the `web-ui/README.md` lists versions.
