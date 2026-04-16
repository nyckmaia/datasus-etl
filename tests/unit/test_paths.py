"""Tests for the parquet path resolver — single source of truth.

Regression tests for the historical double-nested 'datasus_db/datasus_db/' bug
caused by both config.get_parquet_dir() and ParquetManager.__init__ independently
prepending the 'datasus_db' folder name.
"""

from pathlib import Path

from datasus_etl.storage.paths import resolve_parquet_dir


class TestResolveParquetDir:
    """Tests for resolve_parquet_dir — the one function that decides where parquet lives."""

    def test_base_dir_does_not_end_in_datasus_db(self, temp_dir: Path) -> None:
        """User convention: --data-dir is the parent; tool adds one 'datasus_db/'."""
        assert resolve_parquet_dir(temp_dir, "sihsus") == temp_dir / "datasus_db" / "sihsus"

    def test_base_dir_ends_in_datasus_db_does_not_double(self, temp_dir: Path) -> None:
        """Edge case: user already included 'datasus_db/'. Must NOT be doubled."""
        base = temp_dir / "datasus_db"
        base.mkdir()
        assert resolve_parquet_dir(base, "sihsus") == base / "sihsus"

    def test_legacy_parquet_folder_preserved(self, temp_dir: Path) -> None:
        """Legacy 'parquet/{subsystem}' folder is reused if it exists (backwards-compat)."""
        legacy = temp_dir / "parquet" / "sihsus"
        legacy.mkdir(parents=True)
        assert resolve_parquet_dir(temp_dir, "sihsus") == legacy

    def test_base_dir_is_legacy_parquet_folder(self, temp_dir: Path) -> None:
        """If the base_dir itself is the legacy 'parquet' folder, append subsystem only."""
        base = temp_dir / "parquet"
        base.mkdir()
        assert resolve_parquet_dir(base, "sim") == base / "sim"

    def test_case_insensitive_base_dir_name_match(self, temp_dir: Path) -> None:
        """Folder name comparison is case-insensitive (Mac/Windows filesystem quirks)."""
        base = temp_dir / "Datasus_DB"
        base.mkdir()
        assert resolve_parquet_dir(base, "sim") == base / "sim"

    def test_subsystem_is_lowercased(self, temp_dir: Path) -> None:
        """Subsystem names are always lowercased in the returned path."""
        assert resolve_parquet_dir(temp_dir, "SIHSUS") == temp_dir / "datasus_db" / "sihsus"

    def test_accepts_string_base_dir(self, temp_dir: Path) -> None:
        """Accepts str or Path for base_dir."""
        result = resolve_parquet_dir(str(temp_dir), "sim")
        assert result == temp_dir / "datasus_db" / "sim"
        assert isinstance(result, Path)
