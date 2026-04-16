"""Tests for the legacy double-nested layout migration."""

from pathlib import Path

from datasus_etl.storage.migration import (
    MigrationReport,
    detect_legacy_layout,
    migrate_legacy_layout,
)


def _make_legacy_fixture(base: Path) -> None:
    """Create a legacy layout: {base}/datasus_db/datasus_db/{sihsus,sim}/uf=SP/*.parquet."""
    for subsystem, uf, fname in [
        ("sihsus", "SP", "RDSP2401.parquet"),
        ("sihsus", "RJ", "RDRJ2401.parquet"),
        ("sim", "SP", "DOSP2301.parquet"),
    ]:
        legacy_file = base / "datasus_db" / "datasus_db" / subsystem / f"uf={uf}" / fname
        legacy_file.parent.mkdir(parents=True, exist_ok=True)
        legacy_file.write_bytes(b"PARQUET_STUB")


class TestDetectLegacyLayout:
    def test_returns_none_when_no_legacy(self, temp_dir: Path) -> None:
        assert detect_legacy_layout(temp_dir) is None

    def test_returns_path_when_legacy_present(self, temp_dir: Path) -> None:
        nested = temp_dir / "datasus_db" / "datasus_db"
        nested.mkdir(parents=True)
        assert detect_legacy_layout(temp_dir) == nested

    def test_accepts_string_path(self, temp_dir: Path) -> None:
        (temp_dir / "datasus_db" / "datasus_db").mkdir(parents=True)
        assert detect_legacy_layout(str(temp_dir)) is not None

    def test_ignores_a_file_at_the_legacy_location(self, temp_dir: Path) -> None:
        """A bare file named ``datasus_db/datasus_db`` must not trip detection."""
        (temp_dir / "datasus_db").mkdir()
        (temp_dir / "datasus_db" / "datasus_db").write_bytes(b"not a dir")
        assert detect_legacy_layout(temp_dir) is None


class TestMigrateLegacyLayout:
    def test_noop_when_no_legacy(self, temp_dir: Path) -> None:
        report = migrate_legacy_layout(temp_dir)
        assert report.files_moved == 0
        assert report.subsystems_migrated == []
        assert report.conflicts == []

    def test_moves_files_up_one_level(self, temp_dir: Path) -> None:
        _make_legacy_fixture(temp_dir)

        report = migrate_legacy_layout(temp_dir)

        assert report.files_moved == 3
        assert set(report.subsystems_migrated) == {"sihsus", "sim"}
        assert report.conflicts == []
        # Files are at the correct location:
        assert (temp_dir / "datasus_db" / "sihsus" / "uf=SP" / "RDSP2401.parquet").is_file()
        assert (temp_dir / "datasus_db" / "sihsus" / "uf=RJ" / "RDRJ2401.parquet").is_file()
        assert (temp_dir / "datasus_db" / "sim" / "uf=SP" / "DOSP2301.parquet").is_file()
        # Legacy tree was cleaned up:
        assert not (temp_dir / "datasus_db" / "datasus_db").exists()

    def test_dry_run_touches_nothing(self, temp_dir: Path) -> None:
        _make_legacy_fixture(temp_dir)

        report = migrate_legacy_layout(temp_dir, dry_run=True)

        assert report.files_moved == 3
        # Nothing was actually moved:
        assert (
            temp_dir / "datasus_db" / "datasus_db" / "sihsus" / "uf=SP" / "RDSP2401.parquet"
        ).is_file()
        assert not (temp_dir / "datasus_db" / "sihsus").exists()

    def test_reports_conflicts_without_overwriting(self, temp_dir: Path) -> None:
        _make_legacy_fixture(temp_dir)
        # Pre-populate a conflicting target with different content.
        existing = temp_dir / "datasus_db" / "sihsus" / "uf=SP" / "RDSP2401.parquet"
        existing.parent.mkdir(parents=True, exist_ok=True)
        existing.write_bytes(b"EXISTING_TARGET")

        report = migrate_legacy_layout(temp_dir)

        # The conflict is reported and the target stays untouched.
        assert len(report.conflicts) == 1
        assert existing.read_bytes() == b"EXISTING_TARGET"
        # Non-conflicting files still moved.
        assert (temp_dir / "datasus_db" / "sihsus" / "uf=RJ" / "RDRJ2401.parquet").is_file()
        # The conflicting source is left in place for manual resolution.
        assert (
            temp_dir / "datasus_db" / "datasus_db" / "sihsus" / "uf=SP" / "RDSP2401.parquet"
        ).is_file()

    def test_preserves_manifest_files(self, temp_dir: Path) -> None:
        _make_legacy_fixture(temp_dir)
        manifest = temp_dir / "datasus_db" / "datasus_db" / "sihsus" / "_manifest.json"
        manifest.write_text('{"processed_files": {}}', encoding="utf-8")

        migrate_legacy_layout(temp_dir)

        target_manifest = temp_dir / "datasus_db" / "sihsus" / "_manifest.json"
        assert target_manifest.is_file()
        assert target_manifest.read_text(encoding="utf-8") == '{"processed_files": {}}'

    def test_returns_migrationreport(self, temp_dir: Path) -> None:
        report = migrate_legacy_layout(temp_dir)
        assert isinstance(report, MigrationReport)
