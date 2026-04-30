"""Ad-hoc SQL query endpoints.

DuckDB is opened in-memory per request. For each registered subsystem we expose
two VIEWs — a raw `{name}_all` over the parquet tree, and an enriched `{name}`
that LEFT JOINs `ibge_locais` to add `uf_res`, `municipio_res`, `rg_imediata_res`,
and `rg_intermediaria_res`. The JOIN column is taken from
`DatasetConfig.RESIDENCE_MUNICIPALITY_COLUMN` (SIHSUS → `munic_res`, SIM →
`codmunres`). This mirrors the `datasus db` CLI shell so manual queries and web
queries see the same view shape. Only read-only statements are accepted — see
:func:`_validate_sql`.
"""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any

import duckdb
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from datasus_etl.datasets.base import DatasetRegistry
from datasus_etl.storage.parquet_manager import ParquetManager

from .settings import _resolve_data_dir

router = APIRouter()

MAX_LIMIT = 100_000
DEFAULT_LIMIT = 10_000

_SAFE_SQL = re.compile(r"^\s*(with|select)\b", re.IGNORECASE)
_FORBIDDEN = re.compile(
    r"\b("
    r"insert|update|delete|drop|create|alter|attach|copy|pragma|export|import|replace|"
    r"truncate|merge|upsert|call|install|load|set|use|detach|checkpoint|vacuum|"
    r"grant|revoke|begin|commit|rollback"
    r")\b",
    re.IGNORECASE,
)
# Strip line and block comments before running the regex denylist. Block
# comments don't nest in standard SQL, but DuckDB tolerates them, so the
# pattern is the simplest non-greedy match. Done in a single pass — the
# stripped string is only used for validation, never for execution.
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT = re.compile(r"--[^\n]*")


def _strip_sql_comments(sql: str) -> str:
    """Remove block and line comments. Used only by the validator — the
    original SQL with comments preserved is what gets sent to DuckDB."""
    return _LINE_COMMENT.sub("", _BLOCK_COMMENT.sub("", sql))


class SqlRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    limit: int | None = Field(default=None, ge=1, le=MAX_LIMIT)


class SqlResult(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    truncated: bool
    elapsed_ms: float
    limit_applied: int
    # Total rows the un-limited query would return. Only populated when the
    # result is truncated — running COUNT(*) on a non-truncated query is
    # wasteful, since the row_count is already exact in that case.
    total_rows: int | None = None


class TemplateItem(BaseModel):
    subsystem: str
    name: str
    sql: str


def _validate_sql(sql: str) -> None:
    cleaned = _strip_sql_comments(sql)
    stripped = cleaned.strip().rstrip(";").strip()
    if ";" in stripped:
        raise HTTPException(
            status_code=400, detail="Multi-statement SQL is not allowed."
        )
    if not _SAFE_SQL.match(stripped):
        raise HTTPException(
            status_code=400, detail="Only SELECT and WITH queries are allowed."
        )
    if _FORBIDDEN.search(stripped):
        raise HTTPException(
            status_code=400, detail="Write statements are not allowed."
        )


def _ensure_limit(sql: str, limit: int) -> tuple[str, int]:
    """Return (sql_with_limit, effective_limit). Idempotent if a LIMIT already exists."""
    stripped = sql.strip().rstrip(";").strip()
    if re.search(r"\blimit\s+\d+\s*$", stripped, re.IGNORECASE):
        # Respect the user's limit; we still clamp the returned row count below.
        return stripped, limit
    return f"{stripped}\nLIMIT {limit}", limit


def _data_dir_or_400(request: Request) -> Path:
    d = _resolve_data_dir(request)
    if d is None:
        raise HTTPException(
            status_code=400,
            detail="No data directory configured.",
        )
    return d


def _connect_with_views(data_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")

    # Expose the IBGE parquet (single file, not Hive-partitioned) as a VIEW so
    # the per-subsystem enriched views below can LEFT JOIN against it. Mirrors
    # the CLI's `datasus db` interactive shell (cli.py:2168-2177).
    ibge_mgr = ParquetManager(data_dir, "ibge")
    ibge_parquet = ibge_mgr.parquet_dir / "ibge_locais.parquet"
    has_ibge = ibge_parquet.exists()
    if has_ibge:
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW ibge_locais AS "
                f"SELECT * FROM read_parquet('{ibge_parquet}')"
            )
        except Exception:  # noqa: BLE001 — best-effort; fall back to no enrichment
            has_ibge = False

    for name in DatasetRegistry.list_available():
        mgr = ParquetManager(data_dir, name)
        if not mgr.parquet_dir.exists():
            continue
        if not any(mgr.parquet_dir.rglob("*.parquet")):
            continue
        try:
            # Raw view named `{name}_all` (e.g. `sihsus_all`, `sim_all`).
            mgr.create_duckdb_view(con, f"{name}_all")
            config = DatasetRegistry.get(name)
            join_col = config.RESIDENCE_MUNICIPALITY_COLUMN if config else None
            if has_ibge and join_col:
                # Enriched view named `{name}` (e.g. `sihsus`, `sim`) with the
                # uf_res / municipio_res / rg_imediata_res / rg_intermediaria_res
                # columns from the IBGE join — same shape as the CLI's `db`
                # shell. The JOIN column is per-dataset (SIHSUS uses munic_res,
                # SIM uses codmunres).
                con.execute(
                    f"""
                    CREATE OR REPLACE VIEW {name} AS
                    SELECT
                        s.*,
                        i.sigla_uf AS uf_res,
                        i.nome_municipio AS municipio_res,
                        i.nome_regiao_geografica_imediata AS rg_imediata_res,
                        i.nome_regiao_geografica_intermediaria AS rg_intermediaria_res
                    FROM {name}_all s
                    LEFT JOIN ibge_locais i
                        ON s.{join_col} = i.codigo_municipio_6_digitos
                    """
                )
            else:
                con.execute(
                    f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM {name}_all"
                )
        except Exception:  # noqa: BLE001 — the view is best-effort per subsystem
            continue
    return con


@router.post("/sql", response_model=SqlResult)
async def run_sql(payload: SqlRequest, request: Request) -> SqlResult:
    _validate_sql(payload.sql)
    limit = payload.limit or DEFAULT_LIMIT
    data_dir = _data_dir_or_400(request)

    sql_with_limit, effective_limit = _ensure_limit(payload.sql, limit)

    start = time.perf_counter()
    con = _connect_with_views(data_dir)
    try:
        try:
            rel = con.sql(sql_with_limit)
        except duckdb.Error as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        columns = [c for c in rel.columns]
        fetched = rel.fetchall()
    finally:
        con.close()
    elapsed_ms = (time.perf_counter() - start) * 1000

    truncated = len(fetched) >= effective_limit

    # Compute total_rows only when truncated — the user wants to know how
    # many rows are NOT being shown, so we wrap the original SQL (without
    # the LIMIT we just appended) in COUNT(*) and run that as a second
    # cheap query. Best-effort: if the wrapping fails (e.g. SQL with a
    # trailing semicolon that survived the strip), we leave total_rows
    # null and the UI gracefully falls back to "Truncated".
    total_rows: int | None = None
    if truncated:
        original_sql = payload.sql.strip().rstrip(";").strip()
        # Strip an inner LIMIT if the user wrote one — otherwise the
        # COUNT(*) reflects only the limited output, which would be useless.
        without_limit = re.sub(
            r"\blimit\s+\d+\s*$", "", original_sql, flags=re.IGNORECASE
        ).rstrip()
        try:
            con2 = _connect_with_views(data_dir)
            try:
                count_rel = con2.sql(f"SELECT COUNT(*) FROM ({without_limit})")
                row = count_rel.fetchone()
                if row and row[0] is not None:
                    total_rows = int(row[0])
            finally:
                con2.close()
        except duckdb.Error:
            total_rows = None

    return SqlResult(
        columns=columns,
        rows=[list(row) for row in fetched],
        row_count=len(fetched),
        truncated=truncated,
        elapsed_ms=round(elapsed_ms, 2),
        limit_applied=effective_limit,
        total_rows=total_rows,
    )


@router.get("/templates", response_model=list[TemplateItem])
async def list_templates() -> list[TemplateItem]:
    from datasus_etl.web import templates as T

    out: list[TemplateItem] = []
    for attr in dir(T):
        if not attr.endswith("_TEMPLATES"):
            continue
        subsystem = attr.removesuffix("_TEMPLATES").lower()
        value = getattr(T, attr)
        if not isinstance(value, dict):
            continue
        for name, sql in value.items():
            out.append(TemplateItem(subsystem=subsystem, name=name, sql=sql))
    return out


# Map: enriched-view alias → source column in `ibge_locais`. Mirrors the
# JOIN aliases used by `_connect_with_views` and the CLI's `db` shell. The
# distinct-count of each enriched column is bounded above by the distinct
# count of its source IBGE column, so we surface that bound in the UI as
# a `~`-prefixed approximate badge.
IBGE_ENRICHED_TO_SOURCE: dict[str, str] = {
    "uf_res": "sigla_uf",
    "municipio_res": "nome_municipio",
    "rg_imediata_res": "nome_regiao_geografica_imediata",
    "rg_intermediaria_res": "nome_regiao_geografica_intermediaria",
}


def _ibge_distinct_counts(data_dir: Path) -> dict[str, int]:
    """Return distinct-value counts for the 4 IBGE columns we expose.

    `ibge_locais.parquet` is small (~5570 rows / a few hundred KB) and
    stable, so we just run a single COUNT(DISTINCT …) per column on every
    call — the query is sub-100ms and avoiding cache machinery keeps the
    code path obvious. Empty dict if the parquet is missing or the query
    fails for any reason (best-effort: the dictionary endpoint stays useful
    and the UI falls back to the muted `?` badge).
    """
    ibge_mgr = ParquetManager(data_dir, "ibge")
    ibge_parquet = ibge_mgr.parquet_dir / "ibge_locais.parquet"
    if not ibge_parquet.exists():
        return {}
    try:
        import duckdb

        con = duckdb.connect(":memory:")
        try:
            row = con.execute(
                f"""
                SELECT
                    COUNT(DISTINCT sigla_uf)                              AS d_uf,
                    COUNT(DISTINCT nome_municipio)                         AS d_mun,
                    COUNT(DISTINCT nome_regiao_geografica_imediata)        AS d_rgi,
                    COUNT(DISTINCT nome_regiao_geografica_intermediaria)   AS d_rgx
                FROM read_parquet('{ibge_parquet}')
                """
            ).fetchone()
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        import logging

        logging.getLogger(__name__).warning(
            "ibge_locais distinct-count query failed: %s", exc
        )
        return {}

    if row is None:
        return {}
    return {
        "sigla_uf": int(row[0]),
        "nome_municipio": int(row[1]),
        "nome_regiao_geografica_imediata": int(row[2]),
        "nome_regiao_geografica_intermediaria": int(row[3]),
    }


def _column_metadata_cache(
    request: Request, subsystem: str
) -> dict:
    """Best-effort lookup of the column-stats cache for `subsystem`.

    Reads `_column_stats.json` if fresh; recomputes via `parquet_metadata` +
    `approx_count_distinct` if stale or missing. Any failure (no data dir,
    parquet folder doesn't exist, recomputation throws) returns an empty
    dict — the dictionary endpoint stays useful even when stats can't be
    produced, and the UI renders muted "?" badges for the missing fields.
    """
    try:
        from .settings import _resolve_data_dir

        data_dir = _resolve_data_dir(request)
        if data_dir is None:
            return {}

        mgr = ParquetManager(data_dir, subsystem)
        if not mgr.exists():
            return {}

        cache: dict | None = None
        if mgr.column_stats_are_fresh():
            cache = mgr.load_column_stats()
        if cache is None:
            cache = mgr.compute_column_stats()
            mgr.save_column_stats(cache)
    except Exception as exc:  # noqa: BLE001 — best-effort, don't break the dictionary
        import logging

        logging.getLogger(__name__).warning(
            "column-stats lookup failed for %s: %s", subsystem, exc
        )
        return {}

    return cache.get("columns", {}) if isinstance(cache, dict) else {}


# ─────────────────────────────────────────────────────────────────────────
# Hierarchical schema endpoint — feeds the /query LeftSidebar tree.
# ─────────────────────────────────────────────────────────────────────────


class SchemaColumn(BaseModel):
    """Mirrors `DictionaryEntry` — the columns visuals on /query depend on
    this exact shape (fill_pct, distinct_count, type, description)."""

    column: str
    description: str = ""
    type: str = ""
    fill_pct: float | None = None
    fill_pct_approx: bool = False
    distinct_count: int | None = None
    distinct_count_approx: bool = False


class SchemaView(BaseModel):
    """One VIEW under a subsystem.

    `role` is ``"main"`` for the enriched aggregate view (named exactly like
    the subsystem) and ``"dim"`` for any dimension lookup view (currently
    discovered by ``{subsystem}_dim_*`` convention, overridable per dataset
    via ``DatasetConfig.views``).
    """

    name: str
    role: str  # "main" | "dim"
    label_pt: str | None = None
    label_en: str | None = None
    description: str | None = None
    columns: list[SchemaColumn]


class SchemaSubsystem(BaseModel):
    name: str
    label: str
    views: list[SchemaView]


class SchemaTree(BaseModel):
    subsystems: list[SchemaSubsystem]


def _columns_for_view(
    request: Request,
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    subsystem: str,
) -> list[SchemaColumn]:
    """Run DESCRIBE on the view and merge with the dictionary + column-stats
    cache — same data the legacy `/dictionary` endpoint exposed, just keyed
    on the view name instead of the subsystem name."""
    try:
        describe_rows = con.execute(f"DESCRIBE {view_name}").fetchall()
    except duckdb.Error:
        return []

    try:
        from datasus_etl.web import dictionary as D
    except ImportError:
        D = None  # type: ignore[assignment]

    descriptions: dict[str, str] = {}
    if D is not None:
        for attr in (f"{subsystem.upper()}_DICTIONARY", f"{subsystem.upper()}_COLUMNS"):
            if hasattr(D, attr):
                candidate = getattr(D, attr)
                if isinstance(candidate, dict):
                    descriptions = candidate
                    break

    cache_columns = _column_metadata_cache(request, subsystem)

    data_dir = _resolve_data_dir(request)
    ibge_distincts = _ibge_distinct_counts(data_dir) if data_dir else {}

    # The IBGE-enriched columns inherit fill_pct from whichever residence-
    # municipality column the subsystem joined on. Look it up once.
    cfg = DatasetRegistry.get(subsystem)
    join_col = cfg.RESIDENCE_MUNICIPALITY_COLUMN if cfg else None
    join_fill: float | None = None
    if join_col:
        entry = cache_columns.get(join_col)
        if isinstance(entry, dict):
            v = entry.get("fill_pct")
            if isinstance(v, (int, float)):
                join_fill = float(v)

    out: list[SchemaColumn] = []
    for row in describe_rows:
        col_name = row[0]
        col_type = (row[1] or "").upper() if len(row) > 1 else ""
        if col_name in IBGE_ENRICHED_TO_SOURCE:
            source_col = IBGE_ENRICHED_TO_SOURCE[col_name]
            ibge_distinct = ibge_distincts.get(source_col)
            out.append(
                SchemaColumn(
                    column=col_name,
                    description=descriptions.get(col_name, ""),
                    type=col_type or "VARCHAR",
                    fill_pct=join_fill,
                    fill_pct_approx=join_fill is not None,
                    distinct_count=ibge_distinct,
                    distinct_count_approx=ibge_distinct is not None,
                )
            )
            continue
        cache_entry = cache_columns.get(col_name)
        fill = cache_entry.get("fill_pct") if isinstance(cache_entry, dict) else None
        distinct = cache_entry.get("distinct_count") if isinstance(cache_entry, dict) else None
        out.append(
            SchemaColumn(
                column=col_name,
                description=descriptions.get(col_name, ""),
                type=col_type,
                fill_pct=float(fill) if isinstance(fill, (int, float)) else None,
                distinct_count=int(distinct)
                if isinstance(distinct, int) and not isinstance(distinct, bool)
                else None,
            )
        )
    return out


def _discover_views_for_subsystem(
    con: duckdb.DuckDBPyConnection, subsystem: str
) -> list[tuple[str, str]]:
    """Return [(view_name, role), ...] following the convention.

    Convention:
      * exact match `{subsystem}` → role="main"
      * prefix match `{subsystem}_dim_*` → role="dim"
      * anything ending in `_all` is hidden
    """
    rows = con.execute(
        "SELECT view_name FROM duckdb_views() "
        "WHERE schema_name='main' AND NOT internal"
    ).fetchall()
    all_names = {r[0] for r in rows if r and r[0]}
    out: list[tuple[str, str]] = []
    if subsystem in all_names:
        out.append((subsystem, "main"))
    dim_prefix = f"{subsystem}_dim_"
    for name in sorted(all_names):
        if name.startswith(dim_prefix):
            out.append((name, "dim"))
    return out


@router.get("/schema", response_model=SchemaTree)
async def schema(request: Request) -> SchemaTree:
    """Return the full SUBSYSTEM → VIEWS → COLUMNS tree.

    The DuckDB session is built the same way as the SQL endpoint
    (`_connect_with_views`), so every subsystem visible here is queryable
    in the editor with the same view names — including cross-subsystem
    JOIN/UNION queries.
    """
    data_dir = _data_dir_or_400(request)

    subsystems_out: list[SchemaSubsystem] = []
    con = _connect_with_views(data_dir)
    try:
        for name in DatasetRegistry.list_available():
            mgr = ParquetManager(data_dir, name)
            if not mgr.exists():
                continue
            cfg = DatasetRegistry.get(name)

            if cfg is not None and cfg.views is not None:
                pairs = [(spec.name, spec.role) for spec in cfg.views]
                spec_by_name = {spec.name: spec for spec in cfg.views}
            else:
                pairs = _discover_views_for_subsystem(con, name)
                spec_by_name = {}

            views_out: list[SchemaView] = []
            for view_name, role in pairs:
                cols = _columns_for_view(request, con, view_name, name)
                spec = spec_by_name.get(view_name)
                views_out.append(
                    SchemaView(
                        name=view_name,
                        role=role,
                        label_pt=spec.label_pt if spec else None,
                        label_en=spec.label_en if spec else None,
                        description=spec.description if spec else None,
                        columns=cols,
                    )
                )

            subsystems_out.append(
                SchemaSubsystem(
                    name=name,
                    label=name.upper(),
                    views=views_out,
                )
            )
    finally:
        con.close()

    return SchemaTree(subsystems=subsystems_out)
