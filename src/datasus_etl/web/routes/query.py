"""Ad-hoc SQL query endpoints.

DuckDB is opened in-memory per request. For each registered subsystem we expose
two VIEWs — a raw `{name}_all` over the parquet tree, and an enriched `{name}`
that LEFT JOINs `ibge_locais` to add `uf_res`, `municipio_res`, `rg_imediata_res`,
and `rg_intermediaria_res`. This mirrors the `datasus db` CLI shell so manual
queries and web queries see the same view shape. Only read-only statements are
accepted — see :func:`_validate_sql`.
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

MAX_LIMIT = 10_000
DEFAULT_LIMIT = 1_000

_SAFE_SQL = re.compile(r"^\s*(with|select)\b", re.IGNORECASE)
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|create|alter|attach|copy|pragma|export|import|replace)\b",
    re.IGNORECASE,
)


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


class TemplateItem(BaseModel):
    subsystem: str
    name: str
    sql: str


class DictionaryEntry(BaseModel):
    column: str
    description: str


def _validate_sql(sql: str) -> None:
    stripped = sql.strip().rstrip(";").strip()
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
            # Raw view named `{name}_all` (e.g. `sihsus_all`).
            mgr.create_duckdb_view(con, f"{name}_all")
            if has_ibge:
                # Enriched view named `{name}` (e.g. `sihsus`) with the
                # uf_res / municipio_res / rg_imediata_res / rg_intermediaria_res
                # columns from the IBGE join — same shape as cli.py:2172-2177.
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
                        ON s.munic_res = i.codigo_municipio_6_digitos
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
    return SqlResult(
        columns=columns,
        rows=[list(row) for row in fetched],
        row_count=len(fetched),
        truncated=truncated,
        elapsed_ms=round(elapsed_ms, 2),
        limit_applied=effective_limit,
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


@router.get("/dictionary", response_model=list[DictionaryEntry])
async def dictionary(subsystem: str = Query(..., min_length=2)) -> list[DictionaryEntry]:
    try:
        from datasus_etl.web import dictionary as D
    except ImportError:
        return []

    source: dict[str, str] | None = None
    for attr in (f"{subsystem.upper()}_DICTIONARY", f"{subsystem.upper()}_COLUMNS"):
        if hasattr(D, attr):
            candidate = getattr(D, attr)
            if isinstance(candidate, dict):
                source = candidate
                break
    if source is None:
        return []
    return [DictionaryEntry(column=k, description=v) for k, v in source.items()]
