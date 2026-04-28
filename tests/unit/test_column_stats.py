"""Tests for ParquetManager's column-statistics cache.

Covers:
  - `compute_column_stats` produces accurate fill_pct from parquet footers
    (DuckDB writes `null_count` per column-chunk by default).
  - `save_column_stats` / `load_column_stats` round-trip.
  - `column_stats_are_fresh` correctly compares the cache mtime against the
    newest parquet file mtime.
  - The /api/query/dictionary endpoint surfaces `fill_pct` per column and
    serves it from the cache on the hot path.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import duckdb
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from datasus_etl.storage.parquet_manager import ParquetManager
from datasus_etl.web.routes.query import router as query_router


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures: a small fake "sim" parquet store under tmp_path with controlled
# null counts per column. We use the same writer the production pipeline uses
# (DuckDB COPY ... TO ... PARQUET), so the parquet footer carries the same
# `null_count` statistics that real subsystems carry.
# ─────────────────────────────────────────────────────────────────────────────


def _write_sim_parquet(data_dir: Path, uf: str, rows: list[tuple]) -> Path:
    """Write a single Hive-partitioned parquet under
    ``data_dir/datasus_db/sim/uf=<UF>/rows.parquet``.

    Schema mirrors the SIM columns we exercise in the test:
      - codmunres INTEGER  (nulls allowed → exercises NULL stats)
      - dtobito   DATE     (no nulls → exercises 100% fill)
      - causabas  VARCHAR  (some nulls)
    """
    folder = data_dir / "datasus_db" / "sim" / f"uf={uf}"
    folder.mkdir(parents=True, exist_ok=True)
    parquet_path = folder / "rows.parquet"
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE rows (codmunres INTEGER, dtobito DATE, causabas VARCHAR)"
        )
        con.executemany(
            "INSERT INTO rows VALUES (?, ?, ?)",
            rows,
        )
        con.execute(f"COPY rows TO '{parquet_path}' (FORMAT PARQUET)")
    finally:
        con.close()
    return parquet_path


@pytest.fixture
def populated_sim(tmp_path: Path) -> Path:
    """10 rows: 2 with codmunres=NULL, 0 with dtobito=NULL, 4 with causabas=NULL.

    Expected null_pct: codmunres=20%, dtobito=0%, causabas=40%.
    Expected fill_pct: 80%, 100%, 60%.
    """
    rows = [
        (355030, "2024-01-01", "I64"),
        (355030, "2024-01-02", None),
        (None,   "2024-01-03", "J18"),
        (330455, "2024-01-04", None),
        (330455, "2024-01-05", "C50"),
        (None,   "2024-01-06", "I21"),
        (350600, "2024-01-07", None),
        (350600, "2024-01-08", None),
        (310620, "2024-01-09", "K35"),
        (310620, "2024-01-10", "G35"),
    ]
    _write_sim_parquet(tmp_path, "SP", rows)
    return tmp_path


# ─────────────────────────────────────────────────────────────────────────────
# compute_column_stats
# ─────────────────────────────────────────────────────────────────────────────


def test_compute_returns_expected_fill_pcts(populated_sim: Path) -> None:
    mgr = ParquetManager(populated_sim, "sim")
    stats = mgr.compute_column_stats()

    assert stats["row_count"] == 10
    assert "computed_at" in stats
    cols = stats["columns"]

    assert cols["codmunres"]["null_pct"] == 20.0
    assert cols["codmunres"]["fill_pct"] == 80.0
    assert cols["dtobito"]["null_pct"] == 0.0
    assert cols["dtobito"]["fill_pct"] == 100.0
    assert cols["causabas"]["null_pct"] == 40.0
    assert cols["causabas"]["fill_pct"] == 60.0


def test_compute_returns_empty_when_no_parquets(tmp_path: Path) -> None:
    mgr = ParquetManager(tmp_path, "sim")
    stats = mgr.compute_column_stats()
    assert stats["row_count"] == 0
    assert stats["columns"] == {}


def test_compute_aggregates_across_partitions(tmp_path: Path) -> None:
    """codmunres should aggregate null counts across uf=SP and uf=RJ files."""
    sp_rows = [(None, "2024-01-01", "I64"), (1, "2024-01-02", "I65")]  # 50% null
    rj_rows = [(2, "2024-02-01", "I66"), (3, "2024-02-02", "I67")]      # 0% null
    _write_sim_parquet(tmp_path, "SP", sp_rows)
    _write_sim_parquet(tmp_path, "RJ", rj_rows)

    mgr = ParquetManager(tmp_path, "sim")
    stats = mgr.compute_column_stats()

    # 1 null out of 4 → 25% null, 75% fill
    assert stats["row_count"] == 4
    assert stats["columns"]["codmunres"]["null_pct"] == 25.0
    assert stats["columns"]["codmunres"]["fill_pct"] == 75.0


# ─────────────────────────────────────────────────────────────────────────────
# load / save / freshness
# ─────────────────────────────────────────────────────────────────────────────


def test_save_and_load_round_trip(populated_sim: Path) -> None:
    mgr = ParquetManager(populated_sim, "sim")
    stats = mgr.compute_column_stats()
    mgr.save_column_stats(stats)

    assert mgr.column_stats_path.exists()
    loaded = mgr.load_column_stats()
    assert loaded == stats


def test_load_returns_none_when_missing(tmp_path: Path) -> None:
    mgr = ParquetManager(tmp_path, "sim")
    assert mgr.load_column_stats() is None


def test_load_returns_none_when_corrupt(populated_sim: Path) -> None:
    mgr = ParquetManager(populated_sim, "sim")
    mgr.parquet_dir.mkdir(parents=True, exist_ok=True)
    mgr.column_stats_path.write_text("{not valid json", encoding="utf-8")
    assert mgr.load_column_stats() is None


def test_freshness_true_when_cache_newer(populated_sim: Path) -> None:
    mgr = ParquetManager(populated_sim, "sim")
    mgr.save_column_stats(mgr.compute_column_stats())
    assert mgr.column_stats_are_fresh()


def test_freshness_false_when_parquet_newer(populated_sim: Path) -> None:
    mgr = ParquetManager(populated_sim, "sim")
    mgr.save_column_stats(mgr.compute_column_stats())

    # Touch a parquet to a future mtime — simulates the user adding more data
    # without re-running the cache.
    parquets = list(mgr.parquet_dir.rglob("*.parquet"))
    assert parquets
    future = time.time() + 60
    os.utime(parquets[0], (future, future))

    assert not mgr.column_stats_are_fresh()


def test_freshness_false_when_cache_missing(populated_sim: Path) -> None:
    mgr = ParquetManager(populated_sim, "sim")
    assert not mgr.column_stats_are_fresh()


# ─────────────────────────────────────────────────────────────────────────────
# /api/query/dictionary integration
# ─────────────────────────────────────────────────────────────────────────────


def _make_app(data_dir: Path) -> TestClient:
    app = FastAPI()
    app.include_router(query_router, prefix="/api/query")
    app.state.data_dir = data_dir
    return TestClient(app)


def test_dictionary_endpoint_populates_fill_pct(populated_sim: Path) -> None:
    """First call writes the cache; the response carries fill_pct per column."""
    client = _make_app(populated_sim)
    resp = client.get("/api/query/dictionary?subsystem=sim")
    assert resp.status_code == 200, resp.text

    by_col = {e["column"]: e for e in resp.json()}
    # SIM columns the test parquet covers
    assert by_col["codmunres"]["fill_pct"] == 80.0
    assert by_col["dtobito"]["fill_pct"] == 100.0
    assert by_col["causabas"]["fill_pct"] == 60.0
    # Schema columns NOT present in our small parquet (e.g. linhaa) — there's
    # no chunk for them, so fill_pct stays None and the UI shows a "?" badge.
    # We only assert the field exists.
    assert "fill_pct" in by_col["linhaa"]


def test_dictionary_writes_cache_on_first_call(populated_sim: Path) -> None:
    """Verifies the lazy-recompute path persists `_column_stats.json`."""
    mgr = ParquetManager(populated_sim, "sim")
    assert not mgr.column_stats_path.exists()

    client = _make_app(populated_sim)
    client.get("/api/query/dictionary?subsystem=sim")

    assert mgr.column_stats_path.exists()
    cache = json.loads(mgr.column_stats_path.read_text(encoding="utf-8"))
    assert cache["columns"]["codmunres"]["fill_pct"] == 80.0


def test_compute_returns_distinct_count_per_column(populated_sim: Path) -> None:
    """approx_count_distinct (HyperLogLog) is exact on tiny inputs but has
    bounded variance — DuckDB's docs cite ~1.6% relative error. We assert
    tight bounds rather than exact equality so the suite isn't flaky.

    Fixture truth: 4 distinct codmunres, 10 distinct dtobito, 6 distinct
    causabas (NULLs excluded by HLL convention).
    """
    mgr = ParquetManager(populated_sim, "sim")
    stats = mgr.compute_column_stats()

    cols = stats["columns"]

    def _within(actual: int | None, truth: int) -> bool:
        return actual is not None and abs(actual - truth) <= 1

    assert _within(cols["codmunres"]["distinct_count"], 4)
    assert _within(cols["dtobito"]["distinct_count"], 10)
    assert _within(cols["causabas"]["distinct_count"], 6)


def test_compute_writes_version_field(populated_sim: Path) -> None:
    """Cache JSON carries the schema version so future migrations can
    invalidate stale formats without leaving partial data on screen."""
    mgr = ParquetManager(populated_sim, "sim")
    stats = mgr.compute_column_stats()
    assert stats["version"] == ParquetManager.COLUMN_STATS_VERSION


def test_freshness_invalidates_old_version(populated_sim: Path) -> None:
    """A cache JSON with no `version` (legacy) or with the wrong version is
    treated as stale, even if its mtime is newer than every parquet."""
    mgr = ParquetManager(populated_sim, "sim")
    # Write a "v1" cache — the format that shipped before distinct_count.
    legacy_cache = {
        "computed_at": "2026-04-01T00:00:00",
        "row_count": 10,
        "columns": {
            "codmunres": {"null_pct": 20.0, "fill_pct": 80.0},
        },
    }
    mgr.save_column_stats(legacy_cache)
    assert not mgr.column_stats_are_fresh()


def test_compute_array_columns_use_row_level_fill_pct(
    tmp_path: Path,
) -> None:
    """Array columns get a ROW-LEVEL fill_pct, not the leaf-element-level
    one parquet's footer would give. A row counts as null only when the
    array is explicitly NULL or an empty list `[]`; everything else
    (including `[NULL]`) counts as filled. This matches the user's mental
    model of "% of rows whose list has values".
    """
    folder = tmp_path / "datasus_db" / "sim" / "uf=SP"
    folder.mkdir(parents=True, exist_ok=True)
    parquet_path = folder / "rows.parquet"

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE rows (causabas VARCHAR[])")
        con.executemany(
            "INSERT INTO rows VALUES (?)",
            [
                (["I64", "J18"],),  # filled
                (None,),             # null (explicit NULL)
                (["C50"],),          # filled
                (None,),             # null (explicit NULL)
                ([],),               # null (empty list)
                (["X"],),            # filled
            ],
        )
        con.execute(f"COPY rows TO '{parquet_path}' (FORMAT PARQUET)")
    finally:
        con.close()

    mgr = ParquetManager(tmp_path, "sim")
    stats = mgr.compute_column_stats()
    cols = stats["columns"]

    # 6 rows total, 3 filled, 3 null (2 explicit NULL + 1 empty list).
    # → fill_pct = 50.0% exactly. NOT 60% (which would be the leaf-level
    # parquet footer interpretation: 4 non-null leaves out of 5 = 80%, or
    # similar — the point is that we want row semantics, not leaf semantics).
    assert "causabas" in cols
    assert cols["causabas"]["fill_pct"] == 50.0
    assert cols["causabas"]["null_pct"] == 50.0
    # distinct_count is still computed (HLL on the array values).
    assert cols["causabas"]["distinct_count"] is not None


def test_compute_array_with_no_nulls_or_empties_reports_100pct(
    tmp_path: Path,
) -> None:
    """Sanity: when every row has a non-empty list, fill_pct is 100%."""
    folder = tmp_path / "datasus_db" / "sim" / "uf=SP"
    folder.mkdir(parents=True, exist_ok=True)
    parquet_path = folder / "rows.parquet"

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE rows (causabas VARCHAR[])")
        con.executemany(
            "INSERT INTO rows VALUES (?)",
            [(["I64"],), (["J18", "K20"],), (["C50"],)],
        )
        con.execute(f"COPY rows TO '{parquet_path}' (FORMAT PARQUET)")
    finally:
        con.close()

    mgr = ParquetManager(tmp_path, "sim")
    stats = mgr.compute_column_stats()
    assert stats["columns"]["causabas"]["fill_pct"] == 100.0
    assert stats["columns"]["causabas"]["null_pct"] == 0.0


def test_dictionary_endpoint_populates_distinct_count(
    populated_sim: Path,
) -> None:
    """Smoke check that the new field threads through from cache to JSON."""
    client = _make_app(populated_sim)
    resp = client.get("/api/query/dictionary?subsystem=sim").json()
    by_col = {e["column"]: e for e in resp}

    # HLL variance: ±1 around the truth (4 unique codmunres, 10 unique dtobito).
    assert abs(by_col["codmunres"]["distinct_count"] - 4) <= 1
    assert abs(by_col["dtobito"]["distinct_count"] - 10) <= 1

    # IBGE columns can't be measured (they live in a different parquet),
    # so distinct_count stays None and the UI renders the muted "?" badge.
    for ibge_col in ("uf_res", "municipio_res", "rg_imediata_res", "rg_intermediaria_res"):
        assert by_col[ibge_col]["distinct_count"] is None


def test_dictionary_ibge_columns_inherit_join_column_fill_pct(
    populated_sim: Path,
) -> None:
    """The 4 IBGE-enriched columns aren't in the parquet footers (they come
    from a LEFT JOIN against ibge_locais), so their fill_pct is the JOIN
    success upper bound — same value as codmunres' fill_pct for SIM. UI
    renders these with a `~` prefix; backend marks `fill_pct_approx=True`."""
    client = _make_app(populated_sim)
    resp = client.get("/api/query/dictionary?subsystem=sim").json()

    by_col = {e["column"]: e for e in resp}
    join_fill = by_col["codmunres"]["fill_pct"]
    assert join_fill == 80.0  # from the populated_sim fixture

    for ibge_col in (
        "uf_res",
        "municipio_res",
        "rg_imediata_res",
        "rg_intermediaria_res",
    ):
        assert ibge_col in by_col, f"{ibge_col} should be appended for SIM"
        assert by_col[ibge_col]["fill_pct"] == join_fill
        assert by_col[ibge_col]["fill_pct_approx"] is True

    # Schema columns are NOT marked approx — only the 4 IBGE columns are.
    assert by_col["codmunres"]["fill_pct_approx"] is False
    assert by_col["dtobito"]["fill_pct_approx"] is False


def _seed_ibge(tmp_path: Path) -> Path:
    """Write a tiny ibge_locais.parquet with known distinct counts:
       sigla_uf = 2 (SP, RJ)
       nome_municipio = 3 (São Paulo, Rio, Niterói)
       nome_regiao_geografica_imediata = 2
       nome_regiao_geografica_intermediaria = 2
    """
    folder = tmp_path / "datasus_db" / "ibge"
    folder.mkdir(parents=True, exist_ok=True)
    parquet_path = folder / "ibge_locais.parquet"
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            """
            CREATE TABLE ibge_locais (
                codigo_municipio_6_digitos INTEGER,
                sigla_uf VARCHAR,
                nome_municipio VARCHAR,
                nome_regiao_geografica_imediata VARCHAR,
                nome_regiao_geografica_intermediaria VARCHAR
            )
            """
        )
        con.executemany(
            "INSERT INTO ibge_locais VALUES (?, ?, ?, ?, ?)",
            [
                (355030, "SP", "São Paulo", "São Paulo", "São Paulo"),
                (330455, "RJ", "Rio de Janeiro", "Rio de Janeiro", "Rio de Janeiro"),
                (330330, "RJ", "Niterói", "Rio de Janeiro", "Rio de Janeiro"),
            ],
        )
        con.execute(f"COPY ibge_locais TO '{parquet_path}' (FORMAT PARQUET)")
    finally:
        con.close()
    return parquet_path


def test_dictionary_ibge_columns_get_distinct_counts_from_ibge_parquet(
    populated_sim: Path,
) -> None:
    """The 4 IBGE-enriched columns should have distinct_count populated from
    `ibge_locais.parquet` (upper bound on JOIN result), with
    `distinct_count_approx=True` so the UI prefixes the badge with `~`."""
    _seed_ibge(populated_sim)

    client = _make_app(populated_sim)
    resp = client.get("/api/query/dictionary?subsystem=sim").json()
    by_col = {e["column"]: e for e in resp}

    # 2 distinct UFs in the seeded ibge → uf_res inherits.
    assert by_col["uf_res"]["distinct_count"] == 2
    assert by_col["uf_res"]["distinct_count_approx"] is True
    # 3 distinct municipalities.
    assert by_col["municipio_res"]["distinct_count"] == 3
    assert by_col["municipio_res"]["distinct_count_approx"] is True
    # 2 distinct geographic regions (immediate and intermediate).
    assert by_col["rg_imediata_res"]["distinct_count"] == 2
    assert by_col["rg_imediata_res"]["distinct_count_approx"] is True
    assert by_col["rg_intermediaria_res"]["distinct_count"] == 2
    assert by_col["rg_intermediaria_res"]["distinct_count_approx"] is True

    # Schema columns are NOT marked approx — only IBGE inherits.
    assert by_col["codmunres"]["distinct_count_approx"] is False


def test_dictionary_ibge_columns_fallback_when_ibge_parquet_missing(
    populated_sim: Path,
) -> None:
    """No `ibge_locais.parquet` ⇒ distinct_count for the 4 IBGE columns
    stays None and approx stays False — the UI shows the muted `?` badge."""
    # Note: deliberately do NOT call _seed_ibge here.
    client = _make_app(populated_sim)
    resp = client.get("/api/query/dictionary?subsystem=sim").json()
    by_col = {e["column"]: e for e in resp}

    for ibge_col in ("uf_res", "municipio_res", "rg_imediata_res", "rg_intermediaria_res"):
        assert by_col[ibge_col]["distinct_count"] is None
        assert by_col[ibge_col]["distinct_count_approx"] is False


def test_dictionary_uses_cache_on_warm_path(populated_sim: Path) -> None:
    """On the second call the cache is hot — corrupt the parquets and
    confirm the endpoint still returns the cached values (no recompute)."""
    client = _make_app(populated_sim)
    # First call: populates cache.
    first = client.get("/api/query/dictionary?subsystem=sim").json()
    cached_codmunres = next(e for e in first if e["column"] == "codmunres")["fill_pct"]
    assert cached_codmunres == 80.0

    # Sabotage the parquet — but keep mtime older than the cache so freshness
    # check passes. Then assert response is still served from cache.
    mgr = ParquetManager(populated_sim, "sim")
    parquets = list(mgr.parquet_dir.rglob("*.parquet"))
    for p in parquets:
        p.write_bytes(b"corrupted")
        # Set parquet mtime to before the cache mtime (1 hour earlier).
        cache_mtime = mgr.column_stats_path.stat().st_mtime
        os.utime(p, (cache_mtime - 3600, cache_mtime - 3600))

    second = client.get("/api/query/dictionary?subsystem=sim").json()
    cached_codmunres_again = next(
        e for e in second if e["column"] == "codmunres"
    )["fill_pct"]
    assert cached_codmunres_again == 80.0
