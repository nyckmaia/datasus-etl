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


class DictionaryEntry(BaseModel):
    column: str
    description: str
    # DuckDB SQL type as declared in the dataset's schema (e.g. "INTEGER",
    # "VARCHAR", "VARCHAR[]", "DATE"). The frontend abbreviates this for the
    # "Colunas" panel type tags. Empty string when the column lives only in
    # the dictionary file (no schema entry); the IBGE-enriched columns are
    # always VARCHAR (joined from `ibge_locais`).
    type: str = ""
    # Percentage of NON-NULL values for this column, computed from parquet
    # column-chunk footers. Sourced from `_column_stats.json` (recomputed
    # on staleness via `parquet_metadata`). `null` when the cache hasn't
    # been computed yet OR when a column-chunk lacks statistics — UI falls
    # back to the muted "?" badge in those cases.
    fill_pct: float | None = None
    # True for the 4 IBGE-enriched columns (uf_res / municipio_res /
    # rg_imediata_res / rg_intermediaria_res). Their fill_pct is INHERITED
    # from the subsystem's residence-municipality join column (e.g.
    # codmunres for SIM, munic_res for SIHSUS) — that's an upper bound on
    # the actual JOIN success rate, since some non-NULL codes may not
    # match a row in `ibge_locais`. The frontend renders these badges with
    # a `~` prefix so the user knows the value is derived.
    fill_pct_approx: bool = False
    # Approximate count of distinct (non-NULL) values in this column,
    # computed via DuckDB's HyperLogLog (`approx_count_distinct`). `null`
    # when the cache hasn't been computed yet OR when the column lives in
    # the dictionary file only (no schema entry to scan). The UI renders
    # this as a clickable badge that opens a histogram query.
    distinct_count: int | None = None
    # True when distinct_count comes from a JOINED reference table rather
    # than a direct measurement of the subsystem's parquet — currently only
    # the 4 IBGE-enriched columns. The number is the absolute upper bound
    # (e.g. `uf_res` ≤ 27 because `ibge_locais` carries 27 distinct UFs);
    # the UI prefixes the badge with `~` to signal the bound semantics.
    distinct_count_approx: bool = False


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


def _column_fill_pcts(
    request: Request, subsystem: str, schema_columns: list[str]
) -> dict[str, float | None]:
    """Compatibility shim — returns just fill_pct per column. Used as one of
    two views over the same underlying cache (the other being distinct_count
    via `_column_distinct_counts`)."""
    cache_columns = _column_metadata_cache(request, subsystem)
    out: dict[str, float | None] = {}
    for col in schema_columns:
        entry = cache_columns.get(col)
        if isinstance(entry, dict):
            value = entry.get("fill_pct")
            if isinstance(value, (int, float)):
                out[col] = float(value)
    return out


@router.get("/dictionary", response_model=list[DictionaryEntry])
async def dictionary(
    request: Request, subsystem: str = Query(..., min_length=2)
) -> list[DictionaryEntry]:
    """Return the full column listing for a subsystem's enriched VIEW.

    The schema is the source of truth — every column that lives in the
    parquet (and therefore would show up in `DESCRIBE {subsystem}`) is
    listed here, even when no human description has been written for it.
    Descriptions come from `web.dictionary.{NAME}_COLUMNS` when available;
    otherwise the field is left empty and the UI shows just the column
    name + type tag.

    The 4 IBGE-enriched columns (uf_res / municipio_res / rg_imediata_res
    / rg_intermediaria_res) are appended at the end for any subsystem
    that declares `RESIDENCE_MUNICIPALITY_COLUMN`, mirroring the JOIN
    that `_connect_with_views` adds at query time.
    """
    try:
        from datasus_etl.web import dictionary as D
    except ImportError:
        return []

    # Description map (may be missing entirely for an unknown subsystem).
    descriptions: dict[str, str] = {}
    for attr in (f"{subsystem.upper()}_DICTIONARY", f"{subsystem.upper()}_COLUMNS"):
        if hasattr(D, attr):
            candidate = getattr(D, attr)
            if isinstance(candidate, dict):
                descriptions = candidate
                break

    config = DatasetRegistry.get(subsystem)
    if config is None:
        # No registered dataset → fall back to whatever descriptions exist
        # (no schema means no types).
        return [
            DictionaryEntry(column=name, description=desc, type="")
            for name, desc in descriptions.items()
        ]

    # Schema preserves declaration order — the same order users see in
    # `DESCRIBE {subsystem}_all`.
    schema: dict[str, str] = config.get_schema()

    # One cache read serves both fill_pct and distinct_count per column.
    # Best-effort: an empty dict on any failure means every column ships
    # with both fields = None and the UI shows muted "?" badges.
    cache_columns = _column_metadata_cache(request, subsystem)

    def _fill_pct_for(name: str) -> float | None:
        entry = cache_columns.get(name)
        if isinstance(entry, dict):
            v = entry.get("fill_pct")
            if isinstance(v, (int, float)):
                return float(v)
        return None

    def _distinct_for(name: str) -> int | None:
        entry = cache_columns.get(name)
        if isinstance(entry, dict):
            v = entry.get("distinct_count")
            if isinstance(v, int) and not isinstance(v, bool):
                return v
        return None

    entries = [
        DictionaryEntry(
            column=name,
            description=descriptions.get(name, ""),
            type=dtype,
            fill_pct=_fill_pct_for(name),
            distinct_count=_distinct_for(name),
        )
        for name, dtype in schema.items()
    ]

    # Append IBGE-enriched columns last (they only exist in the enriched
    # `{subsystem}` VIEW, not in `{subsystem}_all`). They are always VARCHAR.
    # Their fill_pct cannot be measured from the subsystem's parquet footers,
    # but it is bounded above by the fill_pct of the join column — every IBGE
    # column is NULL whenever the JOIN failed, and the JOIN can only succeed
    # when codmunres/munic_res is non-NULL. We surface that upper bound and
    # mark `fill_pct_approx=True` so the UI prefixes the badge with `~`.
    join_col = config.RESIDENCE_MUNICIPALITY_COLUMN
    if join_col:
        ibge_extra: dict[str, str] = getattr(D, "IBGE_ENRICHED_COLUMNS", {})
        join_col_fill = _fill_pct_for(join_col)
        # Look up the upper-bound distinct counts from `ibge_locais.parquet`.
        # Empty dict if the IBGE parquet doesn't exist yet — entries gracefully
        # degrade to the muted "?" badge in that case.
        from .settings import _resolve_data_dir

        data_dir = _resolve_data_dir(request)
        ibge_distincts = _ibge_distinct_counts(data_dir) if data_dir else {}

        for alias, desc in ibge_extra.items():
            source_col = IBGE_ENRICHED_TO_SOURCE.get(alias)
            ibge_distinct = (
                ibge_distincts.get(source_col) if source_col else None
            )
            entries.append(
                DictionaryEntry(
                    column=alias,
                    description=desc,
                    type="VARCHAR",
                    fill_pct=join_col_fill,
                    fill_pct_approx=join_col_fill is not None,
                    distinct_count=ibge_distinct,
                    distinct_count_approx=ibge_distinct is not None,
                )
            )

    return entries
