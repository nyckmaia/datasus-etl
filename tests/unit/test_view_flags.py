"""Verify the per-subsystem `_all` view tolerates schema drift across partitions
and exposes the parquet source path via the `filename` virtual column.

Regression target: a single Hive partition with a different column set should
not fail the view; `read_parquet` is invoked with `union_by_name=true` to
reconcile schemas by name and `filename=true` to surface row provenance.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from datasus_etl.storage.parquet_manager import ParquetManager


def _write_parquet(path: Path, sql: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


@pytest.fixture
def drifted_subsystem(tmp_path: Path) -> Path:
    """Two partitions with INCOMPATIBLE schemas — uf=SP has an extra column."""
    base = tmp_path / "datasus_db" / "sihsus"
    _write_parquet(
        base / "uf=SP" / "rows.parquet",
        "SELECT 'A' AS munic_res, 100 AS val_tot, 'extra' AS sp_only_col",
    )
    _write_parquet(
        base / "uf=RJ" / "rows.parquet",
        "SELECT 'B' AS munic_res, 200 AS val_tot",
    )
    return tmp_path


def test_view_unions_drifted_schemas_by_name(drifted_subsystem: Path) -> None:
    mgr = ParquetManager(drifted_subsystem, "sihsus")
    con = duckdb.connect(":memory:")
    try:
        mgr.create_duckdb_view(con, "sihsus_all")
        cols = {row[0] for row in con.execute("DESCRIBE sihsus_all").fetchall()}
        # The SP-only column is present and nullable for RJ rows — that's the
        # whole point of union_by_name. Without the flag DuckDB would fail.
        assert "sp_only_col" in cols
        assert "munic_res" in cols
        assert "val_tot" in cols
    finally:
        con.close()


def test_view_exposes_filename_virtual_column(drifted_subsystem: Path) -> None:
    mgr = ParquetManager(drifted_subsystem, "sihsus")
    con = duckdb.connect(":memory:")
    try:
        mgr.create_duckdb_view(con, "sihsus_all")
        cols = {row[0] for row in con.execute("DESCRIBE sihsus_all").fetchall()}
        assert "filename" in cols, (
            "filename=true should add a virtual column with the parquet path"
        )
        non_null = con.execute(
            "SELECT COUNT(*) FROM sihsus_all WHERE filename IS NOT NULL"
        ).fetchone()[0]
        assert non_null > 0
    finally:
        con.close()


def test_uf_filtered_view_keeps_flags(drifted_subsystem: Path) -> None:
    """The uf_filter branch must also include union_by_name + filename."""
    mgr = ParquetManager(drifted_subsystem, "sihsus")
    con = duckdb.connect(":memory:")
    try:
        mgr.create_duckdb_view(con, "sihsus_sp_only", uf_filter=["SP"])
        cols = {row[0] for row in con.execute("DESCRIBE sihsus_sp_only").fetchall()}
        assert "sp_only_col" in cols
        assert "filename" in cols
        # The uf filter applied — no RJ rows.
        ufs = {row[0] for row in con.execute("SELECT DISTINCT uf FROM sihsus_sp_only").fetchall()}
        assert ufs == {"SP"}
    finally:
        con.close()
