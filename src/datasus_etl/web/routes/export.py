"""Stream query results as CSV or Excel.

CSV streams row-by-row; XLSX generates a workbook in a tempfile (openpyxl
does not support streaming into the response body) and then streams the file.
"""

from __future__ import annotations

import csv
import io
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Iterator, Literal

import duckdb
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from datasus_etl.web.user_config import EXPORT_MAX_BYTES_DEFAULT, EXPORT_MAX_ROWS_DEFAULT

from . import settings as _settings_mod
from .query import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    _connect_with_views,
    _ensure_limit,
    _validate_sql,
)

router = APIRouter()


class ExportRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    format: Literal["csv", "xlsx"] = "csv"
    limit: int | None = Field(default=None, ge=1, le=MAX_LIMIT)
    filename: str | None = None
    unlimited: bool = False


def _get_export_max_rows(request: Request) -> int:
    """Read the user-configured cap on rows for unlimited CSV export."""
    from datasus_etl.web import user_config as _uc

    cfg = _uc.load()
    return int(cfg.export_max_rows)


def _get_export_max_bytes(request: Request) -> int:
    """Read the user-configured cap on bytes for unlimited CSV export."""
    from datasus_etl.web import user_config as _uc

    cfg = _uc.load()
    return int(cfg.export_max_bytes)


def _safe_filename(raw: str | None, default: str) -> str:
    base = raw or default
    base = re.sub(r"[^A-Za-z0-9_.-]+", "_", base).strip("_")
    return base or default


def _csv_stream(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    max_bytes: int | None = None,
) -> Iterator[bytes]:
    try:
        rel = con.sql(sql)
        columns = list(rel.columns)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(columns)
        sent = 0
        chunk = buf.getvalue().encode("utf-8")
        sent += len(chunk)
        yield chunk
        batch_size = 100 if max_bytes is not None else 5000
        while True:
            if max_bytes is not None and sent >= max_bytes:
                break
            batch = rel.fetchmany(batch_size)
            if not batch:
                break
            buf.seek(0)
            buf.truncate()
            for row in batch:
                writer.writerow(row)
            chunk = buf.getvalue().encode("utf-8")
            sent += len(chunk)
            yield chunk
    finally:
        con.close()


def _xlsx_response(con: duckdb.DuckDBPyConnection, sql: str, filename: str) -> StreamingResponse:
    from openpyxl import Workbook

    try:
        rel = con.sql(sql)
        columns = list(rel.columns)
        rows = rel.fetchall()
    finally:
        con.close()

    tmp = Path(tempfile.mkstemp(prefix="datasus-export-", suffix=".xlsx")[1])
    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Results")
    ws.append(columns)
    for row in rows:
        ws.append(list(row))
    wb.save(tmp)

    def _iter() -> Iterator[bytes]:
        try:
            with tmp.open("rb") as fp:
                while chunk := fp.read(64 * 1024):
                    yield chunk
        finally:
            tmp.unlink(missing_ok=True)

    return StreamingResponse(
        _iter(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("")
@router.post("/", include_in_schema=False)
async def export(payload: ExportRequest, request: Request) -> StreamingResponse:
    _validate_sql(payload.sql)

    data_dir = _settings_mod._resolve_data_dir(request)
    if data_dir is None:
        raise HTTPException(
            status_code=400,
            detail="No data directory configured.",
        )

    if payload.unlimited:
        if payload.format == "xlsx":
            raise HTTPException(
                status_code=400, detail="unlimited mode is CSV-only"
            )
        row_cap = _get_export_max_rows(request)
        byte_cap: int | None = _get_export_max_bytes(request)
        sql, _ = _ensure_limit(payload.sql, row_cap)
    else:
        limit = payload.limit or DEFAULT_LIMIT
        sql, _ = _ensure_limit(payload.sql, limit)
        byte_cap = None

    con = _connect_with_views(data_dir)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    if payload.format == "csv":
        filename = _safe_filename(payload.filename, f"datasus-{stamp}.csv")
        return StreamingResponse(
            _csv_stream(con, sql, max_bytes=byte_cap),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    filename = _safe_filename(payload.filename, f"datasus-{stamp}.xlsx")
    return _xlsx_response(con, sql, filename)
