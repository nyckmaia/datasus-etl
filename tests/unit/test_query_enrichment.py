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


def test_schema_endpoint_returns_sim_columns_with_metadata(
    tmp_path: Path,
) -> None:
    """The /api/query/schema endpoint must expose parquet columns with their
    type, description, and statistics fields so the SchemaTree can render
    the ColumnFillBadge and ColumnDistinctBadge visuals.

    The endpoint uses DESCRIBE on the actual DuckDB view (not the dataset's
    Python schema), so only columns present in the fake parquet are asserted.
    IBGE-enriched columns are added by the JOIN and must be VARCHAR.

    Migrated from test_dictionary_endpoint_returns_all_schema_columns.
    """
    import duckdb as _duckdb
    from fastapi.testclient import TestClient

    from datasus_etl.web.server import create_app
    from datasus_etl.web.routes import query as query_mod
    from datasus_etl.web.routes import settings as settings_mod

    # Write a minimal SIM parquet + IBGE so the enriched view is created.
    ibge_dir = tmp_path / "datasus_db" / "ibge"
    ibge_dir.mkdir(parents=True, exist_ok=True)
    con = _duckdb.connect(":memory:")
    try:
        con.execute(
            "COPY (SELECT 355030 AS codigo_municipio_6_digitos, 'SP' AS sigla_uf, "
            "'Sao Paulo' AS nome_municipio, 'Sao Paulo' AS nome_regiao_geografica_imediata, "
            "'Sao Paulo' AS nome_regiao_geografica_intermediaria) "
            f"TO '{ibge_dir / 'ibge_locais.parquet'}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    sim_dir = tmp_path / "datasus_db" / "sim" / "uf=SP"
    sim_dir.mkdir(parents=True, exist_ok=True)
    con = _duckdb.connect(":memory:")
    try:
        con.execute(
            f"COPY (SELECT 355030 AS codmunres, 'A00' AS causabas) "
            f"TO '{sim_dir / 'rows.parquet'}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    import pytest as _pytest

    monkeypatch = _pytest.MonkeyPatch()
    monkeypatch.setattr(query_mod, "_resolve_data_dir", lambda _request: tmp_path)
    monkeypatch.setattr(settings_mod, "_resolve_data_dir", lambda _request: tmp_path)

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/api/query/schema")
        assert resp.status_code == 200, resp.text
        body = resp.json()

    monkeypatch.undo()

    sim_sub = next(s for s in body["subsystems"] if s["name"] == "sim")
    main_view = next(v for v in sim_sub["views"] if v["role"] == "main")
    by_col = {c["column"]: c for c in main_view["columns"]}

    # Columns we wrote must be present with the right types.
    assert "codmunres" in by_col
    assert by_col["codmunres"]["type"] == "INTEGER"
    # causabas is a VARCHAR[] in the SIM schema (CID code array).
    assert "causabas" in by_col

    # Every present column must carry the required metadata fields.
    for col_entry in main_view["columns"]:
        assert "type" in col_entry, f"{col_entry['column']} missing type"
        assert "fill_pct" in col_entry, f"{col_entry['column']} missing fill_pct"
        assert "distinct_count" in col_entry, f"{col_entry['column']} missing distinct_count"

    # IBGE enriched columns come from the LEFT JOIN and must be VARCHAR.
    for c in IBGE_COLUMNS:
        assert c in by_col, f"missing IBGE column {c!r}"
        assert by_col[c]["type"] == "VARCHAR", f"{c} should be VARCHAR"


def test_schema_endpoint_column_fields_always_present(
    tmp_path: Path,
) -> None:
    """Each schema column entry carries `type`, `fill_pct`, and `distinct_count`
    fields so the SchemaTree's ColumnFillBadge and ColumnDistinctBadge visuals
    can rely on them being present (value may be None when cache is cold).

    IBGE-enriched columns must always be VARCHAR. Typed parquet columns must
    carry the DuckDB SQL type as-is.

    Migrated from test_dictionary_endpoint_returns_column_types.
    """
    import duckdb as _duckdb
    from fastapi.testclient import TestClient

    from datasus_etl.web.server import create_app
    from datasus_etl.web.routes import query as query_mod
    from datasus_etl.web.routes import settings as settings_mod

    # Write IBGE parquet.
    ibge_dir = tmp_path / "datasus_db" / "ibge"
    ibge_dir.mkdir(parents=True, exist_ok=True)
    con = _duckdb.connect(":memory:")
    try:
        con.execute(
            "COPY (SELECT 355030 AS codigo_municipio_6_digitos, 'SP' AS sigla_uf, "
            "'Sao Paulo' AS nome_municipio, 'Sao Paulo' AS nome_regiao_geografica_imediata, "
            "'Sao Paulo' AS nome_regiao_geografica_intermediaria) "
            f"TO '{ibge_dir / 'ibge_locais.parquet'}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    # Write SIHSUS and SIM parquets with typed columns we can assert on.
    sihsus_p = tmp_path / "datasus_db" / "sihsus" / "uf=SP"
    sihsus_p.mkdir(parents=True, exist_ok=True)
    con = _duckdb.connect(":memory:")
    try:
        con.execute(
            f"COPY (SELECT 355030::INTEGER AS munic_res, 100.0::DOUBLE AS val_tot) "
            f"TO '{sihsus_p / 'rows.parquet'}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    sim_p = tmp_path / "datasus_db" / "sim" / "uf=SP"
    sim_p.mkdir(parents=True, exist_ok=True)
    con = _duckdb.connect(":memory:")
    try:
        con.execute(
            f"COPY (SELECT 355030::INTEGER AS codmunres, 'A00'::VARCHAR AS causabas) "
            f"TO '{sim_p / 'rows.parquet'}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    import pytest as _pytest

    monkeypatch = _pytest.MonkeyPatch()
    monkeypatch.setattr(query_mod, "_resolve_data_dir", lambda _request: tmp_path)
    monkeypatch.setattr(settings_mod, "_resolve_data_dir", lambda _request: tmp_path)

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/api/query/schema")
        assert resp.status_code == 200, resp.text
        body = resp.json()

    monkeypatch.undo()

    def _cols_for(subsystem: str) -> dict:
        sub = next(s for s in body["subsystems"] if s["name"] == subsystem)
        main = next(v for v in sub["views"] if v["role"] == "main")
        return {c["column"]: c for c in main["columns"]}

    sihsus_cols = _cols_for("sihsus")

    # Typed parquet columns carry the right DuckDB SQL types.
    assert sihsus_cols["munic_res"]["type"] == "INTEGER"
    assert sihsus_cols["val_tot"]["type"] == "DOUBLE"

    # All 4 IBGE-enriched columns are VARCHAR.
    for c in IBGE_COLUMNS:
        assert sihsus_cols[c]["type"] == "VARCHAR", f"{c} should be VARCHAR"

    # The fill_pct, distinct_count fields must be present on every entry.
    # Value can be None (cache cold); the contract is that the FIELD is always
    # there so the frontend can rely on it.
    sub = next(s for s in body["subsystems"] if s["name"] == "sihsus")
    main = next(v for v in sub["views"] if v["role"] == "main")
    for entry in main["columns"]:
        assert "fill_pct" in entry, f"{entry['column']} missing fill_pct field"
        assert "distinct_count" in entry, f"{entry['column']} missing distinct_count field"

    # SIM columns.
    sim_cols = _cols_for("sim")
    assert sim_cols["codmunres"]["type"] == "INTEGER"
    assert sim_cols["causabas"]["type"] == "VARCHAR"
