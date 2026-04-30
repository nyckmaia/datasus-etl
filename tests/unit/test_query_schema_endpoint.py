"""End-to-end test for /api/query/schema using a fake parquet layout.

The endpoint returns the SUBSYSTEM → VIEWS → COLUMNS tree. The convention
hides `_all` views, calls the subsystem-named view "main", and surfaces any
view whose name matches `{subsystem}_dim_*` as a "dim" view. Column entries
preserve the ColumnFillBadge + ColumnDistinctBadge inputs the existing
`/dictionary` endpoint already returns (the columns section visuals don't
change — only the wrapping changes).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from datasus_etl.web.server import create_app


def _write_parquet(path: Path, sql: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


@pytest.fixture
def populated(tmp_path: Path, monkeypatch) -> Path:
    """SIHSUS + SIM with one row each, IBGE included so enrichment kicks in."""
    base = tmp_path / "datasus_db"
    _write_parquet(
        base / "ibge" / "ibge_locais.parquet",
        "SELECT 355030 AS codigo_municipio_6_digitos, 'SP' AS sigla_uf, "
        "'Sao Paulo' AS nome_municipio, "
        "'Sao Paulo' AS nome_regiao_geografica_imediata, "
        "'Sao Paulo' AS nome_regiao_geografica_intermediaria",
    )
    _write_parquet(
        base / "sihsus" / "uf=SP" / "rows.parquet",
        "SELECT 355030 AS munic_res, 100.0 AS val_tot",
    )
    _write_parquet(
        base / "sim" / "uf=SP" / "rows.parquet",
        "SELECT 355030 AS codmunres, 'A00' AS causabas",
    )
    # `query.py` does `from .settings import _resolve_data_dir`, which creates
    # a local binding — patching the settings module won't reach query.py's
    # copy. Patch BOTH modules.
    from datasus_etl.web.routes import query as query_mod
    from datasus_etl.web.routes import settings as settings_mod

    monkeypatch.setattr(query_mod, "_resolve_data_dir", lambda _request: tmp_path)
    monkeypatch.setattr(settings_mod, "_resolve_data_dir", lambda _request: tmp_path)
    return tmp_path


@pytest.fixture
def client(populated: Path) -> TestClient:
    return TestClient(create_app())


def test_schema_returns_all_subsystems(client: TestClient) -> None:
    res = client.get("/api/query/schema")
    assert res.status_code == 200, res.text
    body = res.json()
    names = {s["name"] for s in body["subsystems"]}
    assert {"sihsus", "sim"} <= names


def test_schema_hides_all_views(client: TestClient) -> None:
    body = client.get("/api/query/schema").json()
    for sub in body["subsystems"]:
        view_names = {v["name"] for v in sub["views"]}
        assert all(not n.endswith("_all") for n in view_names), view_names


def test_schema_marks_main_view(client: TestClient) -> None:
    body = client.get("/api/query/schema").json()
    sihsus = next(s for s in body["subsystems"] if s["name"] == "sihsus")
    main = [v for v in sihsus["views"] if v["role"] == "main"]
    assert len(main) == 1
    assert main[0]["name"] == "sihsus"


def test_schema_columns_match_describe(client: TestClient) -> None:
    """The main view's columns include the IBGE-enriched 4 + the raw cols."""
    body = client.get("/api/query/schema").json()
    sim = next(s for s in body["subsystems"] if s["name"] == "sim")
    main = next(v for v in sim["views"] if v["role"] == "main")
    cols = {c["column"] for c in main["columns"]}
    assert {"codmunres", "causabas", "uf_res", "municipio_res"} <= cols


def test_schema_columns_carry_fill_and_type(client: TestClient) -> None:
    """Each column entry preserves the same shape as /api/query/dictionary —
    the LeftSidebar's columns visuals depend on these fields."""
    body = client.get("/api/query/schema").json()
    sihsus = next(s for s in body["subsystems"] if s["name"] == "sihsus")
    main = next(v for v in sihsus["views"] if v["role"] == "main")
    sample = next(c for c in main["columns"] if c["column"] == "munic_res")
    assert "type" in sample
    assert "fill_pct" in sample
    assert "distinct_count" in sample


def test_filename_column_has_exact_stats(client: TestClient) -> None:
    """The DuckDB-virtual `filename` column is always populated and has
    one distinct value per source parquet — surface those exact numbers
    so the badges show real values instead of the muted '?' marker."""
    body = client.get("/api/query/schema").json()
    sihsus = next(s for s in body["subsystems"] if s["name"] == "sihsus")
    main = next(v for v in sihsus["views"] if v["role"] == "main")
    filename_entry = next(c for c in main["columns"] if c["column"] == "filename")
    assert filename_entry["fill_pct"] == 100.0
    assert filename_entry["fill_pct_approx"] is False
    # Fixture writes one parquet per subsystem under uf=SP/.
    assert filename_entry["distinct_count"] == 1
    assert filename_entry["distinct_count_approx"] is False
