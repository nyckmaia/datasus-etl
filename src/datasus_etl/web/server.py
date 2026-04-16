"""FastAPI application factory for the DataSUS ETL web UI.

The ASGI app serves two surfaces from a single process:

* ``/api/*`` — JSON REST endpoints for stats, pipeline control, query,
  export, and settings.
* ``/``     — the compiled React SPA (Vite build output placed under
  ``src/datasus_etl/web/static/`` by the Hatch build hook). A catch-all
  route returns ``index.html`` so client-side routing works without 404s.

The application is instantiated via :func:`create_app` — usage:

.. code-block:: bash

    uvicorn datasus_etl.web.server:create_app --factory --port 8787
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request

STATIC_DIR = Path(__file__).parent / "static"


def create_app(data_dir: Path | None = None) -> FastAPI:
    """Build the FastAPI app.

    Args:
        data_dir: Optional user data directory. If omitted, the factory reads
            the ``DATASUS_DATA_DIR`` environment variable (set by the CLI
            ``datasus ui`` command). If both are missing, endpoints that need
            a data directory will return HTTP 400 until the user sets one via
            the settings page.
    """
    resolved_data_dir = _resolve_data_dir(data_dir)

    app = FastAPI(
        title="DataSUS ETL",
        description="Web interface for downloading, querying, and exporting DataSUS data.",
        version="0.1.0",
        docs_url="/api/docs",
        redoc_url=None,
        openapi_url="/api/openapi.json",
    )
    app.state.data_dir = resolved_data_dir

    # Vite dev server runs on :5173 during local development.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    _register_api_routes(app)
    _register_spa_routes(app)

    return app


def _resolve_data_dir(explicit: Path | None) -> Path | None:
    if explicit is not None:
        return Path(explicit).expanduser().resolve()
    env = os.environ.get("DATASUS_DATA_DIR")
    if env:
        return Path(env).expanduser().resolve()
    return None


def _register_api_routes(app: FastAPI) -> None:
    """Attach the /api/* routers."""
    from .routes import export, pipeline, query, settings, stats

    app.include_router(stats.router, prefix="/api/stats", tags=["stats"])
    app.include_router(pipeline.router, prefix="/api/pipeline", tags=["pipeline"])
    app.include_router(query.router, prefix="/api/query", tags=["query"])
    app.include_router(export.router, prefix="/api/export", tags=["export"])
    app.include_router(settings.router, prefix="/api/settings", tags=["settings"])

    @app.get("/api/health", tags=["meta"])
    def health() -> dict[str, str]:
        return {"status": "ok"}


def _register_spa_routes(app: FastAPI) -> None:
    """Mount the compiled SPA, if it has been built."""
    if not STATIC_DIR.exists() or not (STATIC_DIR / "index.html").exists():
        @app.get("/", include_in_schema=False)
        async def _spa_missing() -> JSONResponse:
            return JSONResponse(
                status_code=503,
                content={
                    "error": "SPA not built",
                    "hint": "Run `make ui-build` (or `cd web-ui && bun run build`).",
                },
            )
        return

    assets_dir = STATIC_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    # Common root-level static files (favicon, manifest, etc.) + SPA fallback.
    # Any path that falls through to this handler is either a static asset or
    # a client-routed SPA path. Unknown /api/* paths must 404 rather than
    # return the SPA shell — they would mask typos silently.
    @app.get("/{filename:path}", include_in_schema=False)
    async def spa_fallback(request: Request, filename: str) -> Response:
        if filename.startswith("api/"):
            return JSONResponse(status_code=404, content={"detail": "Not found"})
        candidate = STATIC_DIR / filename
        if filename and candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(STATIC_DIR / "index.html")
