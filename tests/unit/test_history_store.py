"""Tests for the per-subsystem query history store."""

from __future__ import annotations

from pathlib import Path

import pytest

from datasus_etl.web import history_store, user_config


@pytest.fixture
def isolated_config(monkeypatch, tmp_path: Path) -> Path:
    """Redirect XDG_CONFIG_HOME so reads/writes hit a temp dir."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    return tmp_path


def _entry(i: int) -> dict:
    return {
        "id": f"id-{i}",
        "sql": f"SELECT {i}",
        "ts": 1_700_000_000_000 + i,
        "rows": i,
        "elapsed_ms": float(i),
    }


def test_read_empty_returns_empty_list(isolated_config: Path) -> None:
    assert history_store.read("sihsus") == []


def test_append_then_read_returns_newest_first(isolated_config: Path) -> None:
    history_store.append("sihsus", _entry(1))
    history_store.append("sihsus", _entry(2))
    history_store.append("sihsus", _entry(3))

    out = history_store.read("sihsus")

    assert [e["id"] for e in out] == ["id-3", "id-2", "id-1"]


def test_per_subsystem_isolation(isolated_config: Path) -> None:
    history_store.append("sihsus", _entry(1))
    history_store.append("sim", _entry(2))

    sihsus = history_store.read("sihsus")
    sim = history_store.read("sim")

    assert [e["id"] for e in sihsus] == ["id-1"]
    assert [e["id"] for e in sim] == ["id-2"]


def test_fifo_truncate_drops_oldest(isolated_config: Path, monkeypatch) -> None:
    # Force a tiny ceiling so we don't have to write 1000 entries.
    monkeypatch.setattr(history_store, "_max_entries", lambda: 3)

    for i in range(5):
        history_store.append("sihsus", _entry(i))

    out = history_store.read("sihsus")
    # Newest-first: 4, 3, 2 — entries 0 and 1 dropped FIFO.
    assert [e["id"] for e in out] == ["id-4", "id-3", "id-2"]


def test_default_history_size_k_is_2(isolated_config: Path) -> None:
    cfg = user_config.load()
    assert cfg.history_size_k == user_config.DEFAULT_HISTORY_SIZE_K == 2


def test_history_size_k_clamped_below_min(isolated_config: Path) -> None:
    cfg = user_config.UserConfig(data_dir=None, history_size_k=0)
    user_config.save(cfg)
    loaded = user_config.load()
    assert loaded.history_size_k == user_config.MIN_HISTORY_SIZE_K


def test_history_size_k_clamped_above_max(isolated_config: Path) -> None:
    cfg = user_config.UserConfig(data_dir=None, history_size_k=999)
    user_config.save(cfg)
    loaded = user_config.load()
    assert loaded.history_size_k == user_config.MAX_HISTORY_SIZE_K


def test_clear_removes_file(isolated_config: Path) -> None:
    history_store.append("sihsus", _entry(1))
    assert history_store.history_file("sihsus").exists()

    history_store.clear("sihsus")

    assert not history_store.history_file("sihsus").exists()
    assert history_store.read("sihsus") == []


def test_invalid_subsystem_name_rejected() -> None:
    with pytest.raises(ValueError):
        history_store.history_file("../escape")
    with pytest.raises(ValueError):
        history_store.history_file("ABC")


def test_read_with_limit(isolated_config: Path) -> None:
    for i in range(5):
        history_store.append("sihsus", _entry(i))

    out = history_store.read("sihsus", limit=2)
    assert [e["id"] for e in out] == ["id-4", "id-3"]


def test_corrupt_line_skipped(isolated_config: Path) -> None:
    history_store.append("sihsus", _entry(1))
    f = history_store.history_file("sihsus")
    f.write_text(f.read_text() + "this is not json\n", encoding="utf-8")

    out = history_store.read("sihsus")
    assert [e["id"] for e in out] == ["id-1"]


def test_update_renames_and_favorites(isolated_config: Path) -> None:
    history_store.append("sihsus", _entry(1))

    updated = history_store.update("sihsus", "id-1", {"name": "my query", "favorite": True})

    assert updated is not None
    assert updated["name"] == "my query"
    assert updated["favorite"] is True

    after = history_store.read("sihsus")
    assert after[0]["name"] == "my query"
    assert after[0]["favorite"] is True


def test_update_unknown_id_returns_none(isolated_config: Path) -> None:
    history_store.append("sihsus", _entry(1))
    assert history_store.update("sihsus", "nope", {"name": "x"}) is None


def test_remove_deletes_one_entry(isolated_config: Path) -> None:
    history_store.append("sihsus", _entry(1))
    history_store.append("sihsus", _entry(2))

    removed = history_store.remove("sihsus", "id-1")

    assert removed is True
    after = history_store.read("sihsus")
    assert [e["id"] for e in after] == ["id-2"]


def test_remove_unknown_id_returns_false(isolated_config: Path) -> None:
    assert history_store.remove("sihsus", "nope") is False


def test_fifo_preserves_favorites(isolated_config: Path, monkeypatch) -> None:
    monkeypatch.setattr(history_store, "_max_entries", lambda: 3)

    # Star the very first entry so we can confirm it survives FIFO pressure.
    history_store.append("sihsus", {**_entry(0), "favorite": True})
    for i in range(1, 6):
        history_store.append("sihsus", _entry(i))

    out = history_store.read("sihsus")
    ids = [e["id"] for e in out]
    # Favorite (id-0) preserved despite being the oldest. Cap = 3 with the
    # favorite always kept means we keep favorite + 2 newest non-favorites.
    assert "id-0" in ids
    assert ids[0] == "id-5"  # newest at top
