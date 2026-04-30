"""Verify the `unlimited=true` export path enforces row + byte caps and
streams CSV correctly under both branches (cap hit vs not hit).
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
def populated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """5000 rows in sihsus, no IBGE join needed."""
    base = tmp_path / "datasus_db"
    _write_parquet(
        base / "sihsus" / "uf=SP" / "rows.parquet",
        "SELECT i AS munic_res, i * 10 AS val_tot FROM range(5000) t(i)",
    )
    from datasus_etl.web.routes import export as export_mod
    from datasus_etl.web.routes import query as query_mod
    from datasus_etl.web.routes import settings as settings_mod

    monkeypatch.setattr(query_mod, "_resolve_data_dir", lambda _r: tmp_path)
    monkeypatch.setattr(settings_mod, "_resolve_data_dir", lambda _r: tmp_path)
    return tmp_path


@pytest.fixture
def client(populated: Path) -> TestClient:
    return TestClient(create_app())


def test_unlimited_streams_all_rows_under_cap(client: TestClient) -> None:
    res = client.post(
        "/api/export",
        json={
            "sql": "SELECT * FROM sihsus_all",
            "format": "csv",
            "unlimited": True,
        },
    )
    assert res.status_code == 200, res.text
    body = res.content.decode("utf-8")
    line_count = body.count("\n")
    # 5000 data rows + 1 header
    assert line_count >= 5000


def test_unlimited_respects_row_cap(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cap at 100 rows via patched setting; expect ~100 + header."""
    from datasus_etl.web.routes import export as export_mod

    monkeypatch.setattr(export_mod, "_get_export_max_rows", lambda _r: 100)
    monkeypatch.setattr(export_mod, "_get_export_max_bytes", lambda _r: 10**9)

    res = client.post(
        "/api/export",
        json={
            "sql": "SELECT * FROM sihsus_all",
            "format": "csv",
            "unlimited": True,
        },
    )
    assert res.status_code == 200, res.text
    body = res.content.decode("utf-8")
    line_count = body.count("\n")
    assert 100 <= line_count <= 101  # header + 100 data rows


def test_unlimited_respects_byte_cap(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cap at 1KB via patched setting; the stream stops well short of 5000 rows."""
    from datasus_etl.web.routes import export as export_mod

    monkeypatch.setattr(export_mod, "_get_export_max_rows", lambda _r: 10**6)
    monkeypatch.setattr(export_mod, "_get_export_max_bytes", lambda _r: 1024)

    res = client.post(
        "/api/export",
        json={
            "sql": "SELECT * FROM sihsus_all",
            "format": "csv",
            "unlimited": True,
        },
    )
    assert res.status_code == 200, res.text
    body = res.content
    # Stream may overshoot the cap by up to one batch (5000 rows in
    # _csv_stream), but should be far short of the full 5000-row payload.
    assert len(body) < 50 * 1024


def test_limited_export_unchanged(client: TestClient) -> None:
    """Without unlimited=true the existing limit pathway still works."""
    res = client.post(
        "/api/export",
        json={
            "sql": "SELECT * FROM sihsus_all",
            "format": "csv",
            "limit": 50,
        },
    )
    assert res.status_code == 200, res.text
    body = res.content.decode("utf-8")
    line_count = body.count("\n")
    assert 50 <= line_count <= 51


def test_unlimited_xlsx_rejected(client: TestClient) -> None:
    """unlimited=true is CSV-only; XLSX requests must 400."""
    res = client.post(
        "/api/export",
        json={
            "sql": "SELECT * FROM sihsus_all",
            "format": "xlsx",
            "unlimited": True,
        },
    )
    assert res.status_code == 400
