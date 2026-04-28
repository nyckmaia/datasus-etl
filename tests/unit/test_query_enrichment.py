"""Tests for the per-subsystem IBGE enrichment used by /api/query/sql.

The web SQL endpoint and the `datasus db` CLI both build an in-memory DuckDB
session that exposes a raw `{name}_all` view and an enriched `{name}` view.
The enriched view LEFT JOINs `ibge_locais` and adds 4 IBGE columns. The JOIN
column differs by subsystem (SIHSUS uses `munic_res`, SIM uses `codmunres`),
so this test fakes the parquet layout for both subsystems and verifies the
enriched view is built correctly for each.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from datasus_etl.web.routes.query import _connect_with_views


IBGE_COLUMNS = ("uf_res", "municipio_res", "rg_imediata_res", "rg_intermediaria_res")


def _write_ibge_parquet(data_dir: Path) -> None:
    """Write a minimal ibge_locais.parquet under {data_dir}/datasus_db/ibge/."""
    ibge_dir = data_dir / "datasus_db" / "ibge"
    ibge_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = ibge_dir / "ibge_locais.parquet"
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE ibge_locais AS SELECT * FROM (VALUES "
            "(355030, 'SP', 'Sao Paulo', 'Sao Paulo', 'Sao Paulo'), "
            "(330455, 'RJ', 'Rio de Janeiro', 'Rio de Janeiro', 'Rio de Janeiro') "
            ") AS t(codigo_municipio_6_digitos, sigla_uf, nome_municipio, "
            "       nome_regiao_geografica_imediata, nome_regiao_geografica_intermediaria)"
        )
        con.execute(f"COPY ibge_locais TO '{parquet_path}' (FORMAT PARQUET)")
    finally:
        con.close()


def _write_subsystem_parquet(
    data_dir: Path, subsystem: str, join_col: str, codes: list[int]
) -> None:
    """Write a single hive-partitioned parquet for `subsystem` under uf=SP/."""
    sub_dir = data_dir / "datasus_db" / subsystem / "uf=SP"
    sub_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = sub_dir / "rows.parquet"

    values = ", ".join(f"({c})" for c in codes)
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"CREATE TABLE rows AS "
            f"SELECT * FROM (VALUES {values}) AS t({join_col})"
        )
        con.execute(f"COPY rows TO '{parquet_path}' (FORMAT PARQUET)")
    finally:
        con.close()


@pytest.fixture
def populated_data_dir(tmp_path: Path) -> Path:
    """Build a fake data dir with ibge_locais + sihsus + sim parquets."""
    _write_ibge_parquet(tmp_path)
    _write_subsystem_parquet(tmp_path, "sihsus", "munic_res", [355030, 330455])
    _write_subsystem_parquet(tmp_path, "sim", "codmunres", [355030, 330455])
    return tmp_path


def _columns_of(con: duckdb.DuckDBPyConnection, view: str) -> list[str]:
    return [row[0] for row in con.execute(f"DESCRIBE {view}").fetchall()]


def test_sihsus_enriched_view_has_ibge_columns(populated_data_dir: Path) -> None:
    con = _connect_with_views(populated_data_dir)
    try:
        cols = _columns_of(con, "sihsus")
        for c in IBGE_COLUMNS:
            assert c in cols, f"sihsus VIEW missing enriched column {c!r}"
        # And the raw column from sihsus_all is preserved.
        assert "munic_res" in cols
    finally:
        con.close()


def test_sim_enriched_view_has_ibge_columns(populated_data_dir: Path) -> None:
    """Regression test: SIM uses `codmunres`, not `munic_res`. Hardcoding
    `s.munic_res` in the JOIN would silently fail to create the view."""
    con = _connect_with_views(populated_data_dir)
    try:
        cols = _columns_of(con, "sim")
        for c in IBGE_COLUMNS:
            assert c in cols, f"sim VIEW missing enriched column {c!r}"
        assert "codmunres" in cols
    finally:
        con.close()


def test_sim_enriched_view_actually_joins(populated_data_dir: Path) -> None:
    """SP code 355030 must resolve to municipio_res = 'Sao Paulo'."""
    con = _connect_with_views(populated_data_dir)
    try:
        rows = con.execute(
            "SELECT codmunres, uf_res, municipio_res FROM sim "
            "WHERE codmunres = 355030"
        ).fetchall()
        assert rows == [(355030, "SP", "Sao Paulo")]
    finally:
        con.close()


def test_raw_views_are_also_exposed(populated_data_dir: Path) -> None:
    """Both `{name}_all` raw views must remain available alongside enriched."""
    con = _connect_with_views(populated_data_dir)
    try:
        sihsus_all_cols = _columns_of(con, "sihsus_all")
        sim_all_cols = _columns_of(con, "sim_all")
        assert "munic_res" in sihsus_all_cols
        assert "codmunres" in sim_all_cols
        # Raw views do NOT carry the enrichment columns.
        for c in IBGE_COLUMNS:
            assert c not in sihsus_all_cols
            assert c not in sim_all_cols
    finally:
        con.close()


def test_sim_alias_when_no_ibge_parquet(tmp_path: Path) -> None:
    """If ibge_locais.parquet is missing, the `sim` VIEW falls back to a plain
    alias of `sim_all` — querying `uf_res` then must fail loudly. This pins
    the contract: enrichment requires the IBGE parquet."""
    _write_subsystem_parquet(tmp_path, "sim", "codmunres", [355030])
    # Note: we intentionally do NOT call _write_ibge_parquet.
    con = _connect_with_views(tmp_path)
    try:
        cols = _columns_of(con, "sim")
        for c in IBGE_COLUMNS:
            assert c not in cols
        assert "codmunres" in cols
    finally:
        con.close()


def test_dictionary_endpoint_returns_ibge_columns_for_sim() -> None:
    """The /api/query/dictionary endpoint must merge IBGE_ENRICHED_COLUMNS
    on top of SIM_COLUMNS, so the UI 'Colunas' tab lists uf_res / municipio_res /
    rg_imediata_res / rg_intermediaria_res for SIM (not just for SIHSUS)."""
    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    from datasus_etl.web.routes.query import router

    app = FastAPI()
    app.include_router(router, prefix="/api/query")

    with TestClient(app) as client:
        for sub in ("sim", "sihsus"):
            resp = client.get(f"/api/query/dictionary?subsystem={sub}")
            assert resp.status_code == 200, resp.text
            cols = {entry["column"] for entry in resp.json()}
            for c in IBGE_COLUMNS:
                assert c in cols, f"/dictionary?subsystem={sub} missing {c!r}"


def test_dictionary_has_no_ghost_columns() -> None:
    """Every entry in `web.dictionary.{NAME}_COLUMNS` must reference a real
    column in the corresponding `DatasetConfig.get_schema()`. Otherwise the
    UI's 'Colunas' panel shows ghost columns with `?` types that don't exist
    in the parquet (e.g. `DESCRIBE sim` won't return them) — which is what
    happened with `baires`, `endres`, and `graession` (typo of `semagestac`)
    before this guard was added.
    """
    from datasus_etl.datasets.base import DatasetRegistry
    from datasus_etl.web import dictionary as D
    from datasus_etl.web.dictionary import IBGE_ENRICHED_COLUMNS

    ibge = set(IBGE_ENRICHED_COLUMNS)

    for name in DatasetRegistry.list_available():
        config = DatasetRegistry.get(name)
        if config is None:
            continue
        attr = f"{name.upper()}_COLUMNS"
        if not hasattr(D, attr):
            continue
        dict_cols = set(getattr(D, attr))
        schema_cols = set(config.get_schema())
        ghosts = dict_cols - schema_cols - ibge
        assert ghosts == set(), (
            f"{attr} references columns missing from {name}'s schema: "
            f"{sorted(ghosts)}. Either fix the typo or remove the entry."
        )


def test_dictionary_endpoint_returns_all_schema_columns() -> None:
    """The /api/query/dictionary endpoint must list every column declared in
    the dataset schema — even columns without a human-written description.
    Otherwise users see "missing" columns in the 'Colunas' panel that DO
    show up in `DESCRIBE sim` (e.g. idade_valor, escmae, versaosist)."""
    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    from datasus_etl.datasets.sim.schema import SIM_DUCKDB_SCHEMA
    from datasus_etl.web.routes.query import router

    app = FastAPI()
    app.include_router(router, prefix="/api/query")

    with TestClient(app) as client:
        resp = client.get("/api/query/dictionary?subsystem=sim")
        assert resp.status_code == 200, resp.text
        by_col = {e["column"]: e for e in resp.json()}

        # Every schema column must be present.
        for col, dtype in SIM_DUCKDB_SCHEMA.items():
            assert col in by_col, f"missing schema column {col!r}"
            assert by_col[col]["type"] == dtype, (
                f"{col} type mismatch: got {by_col[col]['type']!r}, "
                f"expected {dtype!r}"
            )

        # Spot-check columns previously missing from SIM_COLUMNS: they must
        # now both appear AND carry a human description (regression guard
        # against shipping bare schema columns to the UI).
        for col in ("idade_valor", "idade_unidade", "escmae", "versaosist", "acidtrab"):
            assert col in by_col
            assert by_col[col]["description"], (
                f"{col} should have a non-empty description in SIM_COLUMNS"
            )

        # IBGE columns still come last and remain VARCHAR.
        for c in IBGE_COLUMNS:
            assert by_col[c]["type"] == "VARCHAR"


def test_dictionary_endpoint_returns_column_types() -> None:
    """Each dictionary entry carries a `type` field (DuckDB SQL type) so the
    UI can render type tags. IBGE-enriched columns are always VARCHAR."""
    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    from datasus_etl.web.routes.query import router

    app = FastAPI()
    app.include_router(router, prefix="/api/query")

    with TestClient(app) as client:
        resp = client.get("/api/query/dictionary?subsystem=sihsus")
        assert resp.status_code == 200, resp.text
        by_col = {e["column"]: e for e in resp.json()}

        # Schema-driven types reach the UI.
        assert by_col["munic_res"]["type"] == "INTEGER"
        assert by_col["dt_inter"]["type"] == "DATE"
        assert by_col["morte"]["type"] == "BOOLEAN"

        # All 4 IBGE-enriched columns are VARCHAR.
        for c in IBGE_COLUMNS:
            assert by_col[c]["type"] == "VARCHAR", f"{c} should be VARCHAR"

        # The new fill_pct field must be present on every entry. Value can be
        # None (cache cold or column absent from parquet); the contract is
        # that the FIELD is always there so the frontend can rely on it.
        for entry in resp.json():
            assert "fill_pct" in entry, f"{entry['column']} missing fill_pct field"

        # SIM ships VARCHAR[] arrays — these must reach the UI as-is so the
        # frontend can render them as the `list` abbrev.
        resp = client.get("/api/query/dictionary?subsystem=sim")
        sim_cols = {e["column"]: e for e in resp.json()}
        # causabas exists in SIM_COLUMNS, but linhaa/linhab arrays don't —
        # so we test a column that's both in SIM_COLUMNS and the schema.
        assert sim_cols["causabas"]["type"] == "VARCHAR[]"
        assert sim_cols["dtobito"]["type"] == "DATE"
