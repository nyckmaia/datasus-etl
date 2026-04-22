"""Storage statistics endpoints.

Powered by :class:`ParquetManager` for file/partition metadata and DuckDB for
row counts. Results are cached in-process keyed by the parquet directory's
mtime so dashboard polls don't re-scan on every request.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from datasus_etl.datasets.base import DatasetRegistry
from datasus_etl.storage.parquet_manager import ParquetManager

from .settings import _resolve_data_dir

router = APIRouter()


class SubsystemSummary(BaseModel):
    subsystem: str
    files: int
    size_bytes: int
    ufs: list[str]
    row_count: int | None = None
    first_period: str | None = None  # "YYYY-MM"
    last_period: str | None = None
    last_updated: float | None = None  # Unix timestamp


class UfBreakdown(BaseModel):
    uf: str
    files: int
    size_bytes: int
    row_count: int | None = None
    first_period: str | None = None  # "YYYY-MM"
    last_period: str | None = None  # "YYYY-MM"


class TimelinePoint(BaseModel):
    period: str  # "YYYY-MM"
    files: int
    size_bytes: int


class SubsystemDetail(BaseModel):
    subsystem: str
    files: int
    size_bytes: int
    ufs: list[str]
    row_count: int | None
    per_uf: list[UfBreakdown]
    timeline: list[TimelinePoint]
    last_updated: float | None


# --------------------------------------------------------------------------- #
# Simple (subsystem -> (mtime, payload)) cache for row counts.                 #
# --------------------------------------------------------------------------- #

@dataclass
class _CacheEntry:
    mtime: float
    payload: object


_cache: dict[str, _CacheEntry] = {}


def _cached(key: str, dir_path: Path, compute):
    mtime = dir_path.stat().st_mtime if dir_path.exists() else 0.0
    entry = _cache.get(key)
    if entry is not None and entry.mtime == mtime:
        return entry.payload
    value = compute()
    _cache[key] = _CacheEntry(mtime=mtime, payload=value)
    return value


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _parse_period_from_filename(filename: str, prefix: str) -> str | None:
    """Return 'YYYY-MM' from a DataSUS filename like ``RDSP2401.parquet``."""
    stem = Path(filename).stem.upper()
    if not stem.startswith(prefix.upper()):
        return None
    rest = stem[len(prefix):]
    if len(rest) < 6:
        return None
    try:
        yy = int(rest[2:4])
        mm = int(rest[4:6])
    except ValueError:
        return None
    if mm < 1 or mm > 12:
        return None
    year = 2000 + yy if yy < 50 else 1900 + yy
    return f"{year:04d}-{mm:02d}"


def _data_dir_or_400(request: Request) -> Path:
    d = _resolve_data_dir(request)
    if d is None:
        raise HTTPException(
            status_code=400,
            detail="No data directory configured.",
        )
    return d


def _count_rows(manager: ParquetManager) -> int | None:
    """Return the total row count across the subsystem's parquet files, or None if empty."""
    if not manager.parquet_dir.exists():
        return None
    glob = manager.get_glob_pattern()
    try:
        con = duckdb.connect(database=":memory:")
        try:
            result = con.sql(
                f"SELECT COUNT(*) FROM read_parquet('{glob}', union_by_name=true)"
            ).fetchone()
            return int(result[0]) if result else 0
        finally:
            con.close()
    except duckdb.Error:
        return None


def _build_summary(subsystem: str, data_dir: Path, with_rows: bool = True) -> SubsystemSummary:
    mgr = ParquetManager(data_dir, subsystem)
    stats = mgr.get_storage_stats()
    cfg_cls = DatasetRegistry.get(subsystem)
    prefix = cfg_cls.FILE_PREFIX if cfg_cls is not None else ""

    periods: list[str] = []
    for file in mgr.list_parquet_files():
        p = _parse_period_from_filename(file.name, prefix)
        if p is not None:
            periods.append(p)
    periods.sort()

    last_updated: float | None = None
    if mgr.manifest_path.exists():
        last_updated = mgr.manifest_path.stat().st_mtime

    row_count: int | None = None
    if with_rows and stats.total_files > 0:
        row_count = _cached(f"rows:{subsystem}", mgr.parquet_dir, lambda: _count_rows(mgr))

    return SubsystemSummary(
        subsystem=subsystem,
        files=stats.total_files,
        size_bytes=stats.total_size_bytes,
        ufs=stats.partitions,
        row_count=row_count,
        first_period=periods[0] if periods else None,
        last_period=periods[-1] if periods else None,
        last_updated=last_updated,
    )


# --------------------------------------------------------------------------- #
# Endpoints                                                                   #
# --------------------------------------------------------------------------- #

@router.get("/overview", response_model=list[SubsystemSummary])
async def overview(
    request: Request,
    with_rows: bool = Query(default=True, description="Include row counts (slower)."),
) -> list[SubsystemSummary]:
    data_dir = _data_dir_or_400(request)
    result: list[SubsystemSummary] = []
    for name in DatasetRegistry.list_available():
        result.append(_build_summary(name, data_dir, with_rows=with_rows))
    return result


@router.get("/subsystem/{name}", response_model=SubsystemDetail)
async def subsystem_detail(name: str, request: Request) -> SubsystemDetail:
    cfg_cls = DatasetRegistry.get(name)
    if cfg_cls is None:
        raise HTTPException(status_code=404, detail=f"Unknown subsystem: {name}")

    data_dir = _data_dir_or_400(request)
    mgr = ParquetManager(data_dir, name)
    stats = mgr.get_storage_stats()
    prefix = cfg_cls.FILE_PREFIX or ""

    # Per-UF breakdown
    per_uf: list[UfBreakdown] = []
    for uf in stats.partitions:
        files = mgr.list_parquet_files(uf=uf)
        size = sum(f.stat().st_size for f in files)
        uf_periods = sorted(
            p
            for p in (_parse_period_from_filename(f.name, prefix) for f in files)
            if p is not None
        )
        per_uf.append(
            UfBreakdown(
                uf=uf,
                files=len(files),
                size_bytes=size,
                row_count=None,
                first_period=uf_periods[0] if uf_periods else None,
                last_period=uf_periods[-1] if uf_periods else None,
            )
        )

    # Timeline (files & size per YYYY-MM, summed across all UFs)
    timeline_acc: dict[str, tuple[int, int]] = {}
    for f in mgr.list_parquet_files():
        period = _parse_period_from_filename(f.name, prefix)
        if period is None:
            continue
        files_count, size = timeline_acc.get(period, (0, 0))
        timeline_acc[period] = (files_count + 1, size + f.stat().st_size)
    timeline = [
        TimelinePoint(period=p, files=c, size_bytes=s)
        for p, (c, s) in sorted(timeline_acc.items())
    ]

    row_count = (
        _cached(f"rows:{name}", mgr.parquet_dir, lambda: _count_rows(mgr))
        if stats.total_files
        else None
    )
    last_updated = mgr.manifest_path.stat().st_mtime if mgr.manifest_path.exists() else None

    return SubsystemDetail(
        subsystem=name,
        files=stats.total_files,
        size_bytes=stats.total_size_bytes,
        ufs=stats.partitions,
        row_count=row_count,
        per_uf=per_uf,
        timeline=timeline,
        last_updated=last_updated,
    )


@router.get("/timeline", response_model=list[TimelinePoint])
async def timeline(
    request: Request,
    subsystem: str = Query(..., min_length=2),
) -> list[TimelinePoint]:
    cfg_cls = DatasetRegistry.get(subsystem)
    if cfg_cls is None:
        raise HTTPException(status_code=404, detail=f"Unknown subsystem: {subsystem}")
    data_dir = _data_dir_or_400(request)
    mgr = ParquetManager(data_dir, subsystem)
    prefix = cfg_cls.FILE_PREFIX or ""

    acc: dict[str, tuple[int, int]] = {}
    for f in mgr.list_parquet_files():
        period = _parse_period_from_filename(f.name, prefix)
        if period is None:
            continue
        files_count, size = acc.get(period, (0, 0))
        acc[period] = (files_count + 1, size + f.stat().st_size)

    return [
        TimelinePoint(period=p, files=c, size_bytes=s)
        for p, (c, s) in sorted(acc.items())
    ]
