"""Tests for POST /api/settings/reset-storage.

The endpoint is destructive and gated by a typed-back 4-digit code on the
client side; the server still has to:
  - reject calls when no data dir is configured,
  - reject unknown subsystem names,
  - never delete anything that resolves outside the configured storage root,
  - report skipped items rather than 500-ing on missing folders,
  - return the freed byte count for the UI to surface to the user.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from datasus_etl.web.routes.settings import router


def _make_app(data_dir: Path) -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/settings")
    # Mirror how datasus_etl.web.server initialises app.state — the route
    # handler reads the data dir off there before falling back to user config.
    app.state.data_dir = data_dir
    return TestClient(app)


def _seed(data_dir: Path, subsystem: str, payload: bytes = b"hello") -> Path:
    """Create a small fake parquet folder so the endpoint has something to delete."""
    folder = data_dir / "datasus_db" / subsystem / "uf=SP"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "rows.parquet").write_bytes(payload)
    return folder


def test_reset_storage_deletes_selected_subsystem(tmp_path: Path) -> None:
    target = _seed(tmp_path, "sim", b"x" * 1024)
    # Seed a column-stats cache file too (sibling of `_manifest.json` inside
    # the subsystem dir), to confirm it vanishes along with the subsystem
    # folder. _seed returns the `uf=SP` folder; the cache lives one level up.
    column_stats_path = target.parent / "_column_stats.json"
    column_stats_path.write_text('{"columns": {}}', encoding="utf-8")

    client = _make_app(tmp_path)
    resp = client.post("/api/settings/reset-storage", json={"subsystems": ["sim"]})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["deleted"]) == 1
    assert body["deleted"][0]["name"] == "sim"
    assert body["deleted"][0]["freed_bytes"] >= 1024
    assert not target.exists()
    assert not target.parent.exists()  # the sim/ folder itself is gone
    assert not column_stats_path.exists()  # sibling stats cache is gone too


def test_reset_storage_only_deletes_what_was_selected(tmp_path: Path) -> None:
    sihsus_target = _seed(tmp_path, "sihsus")
    sim_target = _seed(tmp_path, "sim")

    client = _make_app(tmp_path)
    resp = client.post("/api/settings/reset-storage", json={"subsystems": ["sim"]})

    assert resp.status_code == 200
    assert sihsus_target.exists(), "sihsus must be untouched"
    assert not sim_target.exists()


def test_reset_storage_handles_ibge_special_case(tmp_path: Path) -> None:
    ibge_dir = tmp_path / "datasus_db" / "ibge"
    ibge_dir.mkdir(parents=True)
    (ibge_dir / "ibge_locais.parquet").write_bytes(b"data")

    client = _make_app(tmp_path)
    resp = client.post("/api/settings/reset-storage", json={"subsystems": ["ibge"]})

    assert resp.status_code == 200
    assert resp.json()["deleted"][0]["name"] == "ibge"
    assert not ibge_dir.exists()


def test_reset_storage_rejects_unknown_subsystem(tmp_path: Path) -> None:
    _seed(tmp_path, "sim")

    client = _make_app(tmp_path)
    resp = client.post(
        "/api/settings/reset-storage", json={"subsystems": ["bogus"]}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["deleted"] == []
    assert len(body["skipped"]) == 1
    assert body["skipped"][0]["name"] == "bogus"
    assert body["skipped"][0]["skipped_reason"] == "unknown subsystem"


def test_reset_storage_skips_missing_folder(tmp_path: Path) -> None:
    """A subsystem that was never downloaded should be reported as skipped,
    not raise. Mirrors a user toggling an empty subsystem in the dialog."""
    (tmp_path / "datasus_db").mkdir()

    client = _make_app(tmp_path)
    resp = client.post(
        "/api/settings/reset-storage", json={"subsystems": ["sihsus"]}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["deleted"] == []
    assert body["skipped"][0]["skipped_reason"] == "no data on disk"


def test_reset_storage_dedups_repeated_names(tmp_path: Path) -> None:
    _seed(tmp_path, "sim")

    client = _make_app(tmp_path)
    resp = client.post(
        "/api/settings/reset-storage", json={"subsystems": ["sim", "SIM", "sim"]}
    )
    assert resp.status_code == 200
    # Only one entry — the duplicate "SIM" / "sim" should be deduplicated
    # (case-insensitive normalization happens server-side).
    assert len(resp.json()["deleted"]) + len(resp.json()["skipped"]) == 1


def test_reset_storage_requires_at_least_one_subsystem(tmp_path: Path) -> None:
    client = _make_app(tmp_path)
    resp = client.post("/api/settings/reset-storage", json={"subsystems": []})
    # FastAPI/pydantic 422 for the empty list — protects against UI bugs that
    # might submit an empty selection.
    assert resp.status_code == 422


def test_reset_storage_400_when_no_data_dir_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When no data_dir is set on app.state AND the persisted user config
    has no data_dir, the endpoint must 400 — it can't know what to delete."""
    # Isolate from the real ~/.config/datasus-etl/config.toml: monkeypatch the
    # module's config_path() to point at a tmp file that has no data_dir.
    from datasus_etl.web import user_config

    fake_config = tmp_path / "config.toml"  # doesn't exist → load() returns defaults
    monkeypatch.setattr(user_config, "config_path", lambda: fake_config)

    app = FastAPI()
    app.include_router(router, prefix="/api/settings")
    # Don't set app.state.data_dir.

    client = TestClient(app)
    resp = client.post(
        "/api/settings/reset-storage", json={"subsystems": ["sim"]}
    )
    assert resp.status_code == 400
    assert "data directory" in resp.json()["detail"].lower()


@pytest.mark.skipif(
    os.name == "nt",
    reason="Symlink path-traversal guard test needs a UNIX symlink "
    "(Windows would require admin privileges).",
)
def test_reset_storage_rejects_symlink_outside_root(tmp_path: Path) -> None:
    """If a malicious symlink points the per-subsystem folder outside the
    storage root, the endpoint must refuse to delete and report skipped."""
    storage_root = tmp_path / "datasus_db"
    storage_root.mkdir()

    # The "sensitive" target sits OUTSIDE storage_root.
    outside = tmp_path / "outside_data"
    outside.mkdir()
    (outside / "important.txt").write_text("must survive")

    # Hand-craft a symlink at the canonical sim/ path that escapes the root.
    sim_link = storage_root / "sim"
    sim_link.symlink_to(outside, target_is_directory=True)

    client = _make_app(tmp_path)
    resp = client.post("/api/settings/reset-storage", json={"subsystems": ["sim"]})

    assert resp.status_code == 200
    body = resp.json()
    assert body["deleted"] == []
    assert body["skipped"][0]["skipped_reason"] == "outside storage root"
    assert (outside / "important.txt").exists(), "outside data must survive"
