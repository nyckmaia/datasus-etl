"""Parquet storage manager for partitioned Hive-style data.

Manages partitioned Parquet files with Hive-style structure:
- Structure: {base_dir}/datasus_db/{subsystem}/uf={UF}/{filename}.parquet
- Manifest tracking: _manifest.json for processed files
- DuckDB integration: creates VIEWs from Parquet glob patterns
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb


@dataclass
class ParquetStorageStats:
    """Statistics about Parquet storage."""

    total_files: int
    total_size_bytes: int
    partitions: list[str]  # List of UF partitions
    file_count_by_partition: dict[str, int]


class ParquetManager:
    """Manager for partitioned Parquet storage.

    Provides:
    - Manifest tracking of processed source files
    - Listing of partitions and files
    - Glob patterns for read_parquet()
    - DuckDB VIEW creation for querying

    Example:
        >>> manager = ParquetManager(Path("data"), "sihsus")
        >>> manager.mark_processed("RDSP2401.dbc")
        >>> if "RDSP2401.dbc" not in manager.get_processed_files():
        ...     # Process file
        >>> manager.create_duckdb_view(conn, "sihsus_raw")
    """

    MANIFEST_FILENAME = "_manifest.json"

    def __init__(self, base_dir: Path, subsystem: str) -> None:
        """Initialize the Parquet manager.

        Path resolution is delegated to
        :func:`datasus_etl.storage.paths.resolve_parquet_dir` so the result
        matches :meth:`PipelineConfig.get_parquet_dir` exactly.

        Args:
            base_dir: The user's configured data directory
                (e.g. what ``--data-dir`` points at).
            subsystem: DataSUS subsystem name (``sihsus``, ``sim``, etc.).
        """
        from .paths import resolve_parquet_dir

        self.base_dir = Path(base_dir)
        self.subsystem = subsystem.lower()
        self.parquet_dir = resolve_parquet_dir(self.base_dir, self.subsystem)
        self.manifest_path = self.parquet_dir / self.MANIFEST_FILENAME
        self.logger = logging.getLogger(__name__)

        # Ensure directory exists
        self.parquet_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _extract_uf_from_filename(filename: str) -> str:
        """Extract UF code from filename in RDUFAAMM.dbc format.

        Args:
            filename: Source file name (e.g., "RDSP2401.dbc")

        Returns:
            UF code (e.g., "SP") or "UNKNOWN" if cannot parse
        """
        name = filename.upper().replace('.DBC', '')
        if len(name) >= 4:
            return name[2:4]
        return "UNKNOWN"

    @staticmethod
    def _sort_files_by_date(files: list[str]) -> list[str]:
        """Sort files by date (oldest to newest).

        Files are in RDUFAAMM.dbc format where AA is year and MM is month.

        Args:
            files: List of filenames

        Returns:
            Sorted list of filenames
        """
        def parse_date(filename: str) -> tuple[int, int]:
            name = filename.upper().replace('.DBC', '')
            if len(name) >= 8:
                try:
                    year = int(name[4:6])
                    month = int(name[6:8])
                    # Convert 2-digit year: 92-99 -> 1992-1999, 00-91 -> 2000-2091
                    full_year = 1900 + year if year >= 92 else 2000 + year
                    return (full_year, month)
                except ValueError:
                    pass
            return (9999, 99)

        return sorted(files, key=parse_date)

    def get_processed_files(self) -> set[str]:
        """Get set of source files that have been processed.

        Handles both old format (flat list) and new format (dict by UF).

        Returns:
            Set of source_file values (e.g., {"RDSP2401.dbc", "RDSP2402.dbc"})
        """
        manifest = self._load_manifest()
        processed = manifest.get("processed_files", {})

        # Handle old format (flat list) for backwards compatibility
        if isinstance(processed, list):
            return set(processed)

        # New format: dict with UF keys
        all_files = set()
        for uf_files in processed.values():
            all_files.update(uf_files)
        return all_files

    def get_processed_files_by_uf(self) -> dict[str, list[str]]:
        """Get processed files organized by UF.

        Returns:
            Dict mapping UF codes to list of processed files for that UF
        """
        manifest = self._load_manifest()
        processed = manifest.get("processed_files", {})

        # Handle old format (flat list) - convert to dict
        if isinstance(processed, list):
            result: dict[str, list[str]] = {}
            for filename in processed:
                uf = self._extract_uf_from_filename(filename)
                if uf not in result:
                    result[uf] = []
                result[uf].append(filename)
            # Sort files within each UF
            for uf in result:
                result[uf] = self._sort_files_by_date(result[uf])
            return result

        return processed

    def mark_processed(self, source_file: str) -> None:
        """Mark a source file as processed in the manifest.

        Files are organized by UF and sorted by date within each UF.

        Args:
            source_file: Source file name (e.g., "RDSP2401.dbc")
        """
        manifest = self._load_manifest()
        uf = self._extract_uf_from_filename(source_file)

        # Ensure processed_files is a dict (migrate from old format if needed)
        processed = manifest.get("processed_files", {})
        if isinstance(processed, list):
            # Migrate from old format
            processed = self._migrate_to_uf_format(processed)

        if uf not in processed:
            processed[uf] = []

        if source_file not in processed[uf]:
            processed[uf].append(source_file)
            # Keep sorted by date
            processed[uf] = self._sort_files_by_date(processed[uf])
            manifest["processed_files"] = processed
            manifest["last_updated"] = datetime.now().isoformat()
            self._save_manifest(manifest)

        self.logger.debug(f"Marked as processed: {source_file} (UF: {uf})")

    def _migrate_to_uf_format(self, flat_list: list[str]) -> dict[str, list[str]]:
        """Migrate old flat list format to new UF-based dict format.

        Args:
            flat_list: Old format list of filenames

        Returns:
            New format dict with UF keys
        """
        result: dict[str, list[str]] = {}
        for filename in flat_list:
            uf = self._extract_uf_from_filename(filename)
            if uf not in result:
                result[uf] = []
            result[uf].append(filename)

        # Sort files within each UF and sort UF keys
        sorted_result: dict[str, list[str]] = {}
        for uf in sorted(result.keys()):
            sorted_result[uf] = self._sort_files_by_date(result[uf])

        return sorted_result

    def remove_processed(self, source_file: str) -> None:
        """Remove a source file from the processed list.

        Useful for reprocessing files.

        Args:
            source_file: Source file name to remove
        """
        manifest = self._load_manifest()
        uf = self._extract_uf_from_filename(source_file)

        processed = manifest.get("processed_files", {})

        # Handle old format
        if isinstance(processed, list):
            if source_file in processed:
                processed.remove(source_file)
                manifest["processed_files"] = processed
                manifest["last_updated"] = datetime.now().isoformat()
                self._save_manifest(manifest)
                self.logger.debug(f"Removed from processed: {source_file}")
            return

        # New format
        if uf in processed and source_file in processed[uf]:
            processed[uf].remove(source_file)
            # Remove UF key if empty
            if not processed[uf]:
                del processed[uf]
            manifest["processed_files"] = processed
            manifest["last_updated"] = datetime.now().isoformat()
            self._save_manifest(manifest)
            self.logger.debug(f"Removed from processed: {source_file} (UF: {uf})")

    def clear_manifest(self) -> None:
        """Clear all processed files from manifest."""
        manifest = {
            "subsystem": self.subsystem,
            "processed_files": {},
            "last_updated": datetime.now().isoformat(),
        }
        self._save_manifest(manifest)
        self.logger.info("Manifest cleared")

    def get_glob_pattern(self) -> str:
        """Get glob pattern for read_parquet() with Hive partitioning.

        Returns:
            Glob pattern string (e.g., "data/parquet/sihsus/uf=*/*.parquet")
        """
        return str(self.parquet_dir / "uf=*" / "*.parquet")

    def get_partitions(self) -> list[str]:
        """Get list of UF partitions.

        Returns:
            List of UF codes (e.g., ["SP", "RJ", "MG"])
        """
        partitions = []
        if self.parquet_dir.exists():
            for partition_dir in self.parquet_dir.iterdir():
                if partition_dir.is_dir() and partition_dir.name.startswith("uf="):
                    uf = partition_dir.name.replace("uf=", "")
                    partitions.append(uf)
        return sorted(partitions)

    def get_storage_stats(self) -> ParquetStorageStats:
        """Get statistics about Parquet storage.

        Returns:
            ParquetStorageStats with file counts, sizes, and partitions
        """
        total_files = 0
        total_size = 0
        file_count_by_partition = {}

        if self.parquet_dir.exists():
            for parquet_file in self.parquet_dir.rglob("*.parquet"):
                total_files += 1
                total_size += parquet_file.stat().st_size

                # Extract partition from path
                partition_dir = parquet_file.parent.name
                if partition_dir.startswith("uf="):
                    uf = partition_dir.replace("uf=", "")
                    file_count_by_partition[uf] = file_count_by_partition.get(uf, 0) + 1

        return ParquetStorageStats(
            total_files=total_files,
            total_size_bytes=total_size,
            partitions=self.get_partitions(),
            file_count_by_partition=file_count_by_partition,
        )

    def list_parquet_files(self, uf: Optional[str] = None) -> list[Path]:
        """List all Parquet files, optionally filtered by UF.

        Args:
            uf: Optional UF code to filter by

        Returns:
            List of Parquet file paths
        """
        if not self.parquet_dir.exists():
            return []

        if uf:
            partition_dir = self.parquet_dir / f"uf={uf.upper()}"
            if partition_dir.exists():
                return sorted(partition_dir.glob("*.parquet"))
            return []
        else:
            return sorted(self.parquet_dir.rglob("*.parquet"))

    def create_duckdb_view(
        self,
        conn: duckdb.DuckDBPyConnection,
        view_name: str,
        uf_filter: Optional[list[str]] = None,
    ) -> None:
        """Create a DuckDB VIEW from Parquet files.

        Creates a VIEW that reads from the Parquet files using
        read_parquet() with hive_partitioning enabled.

        Args:
            conn: DuckDB connection
            view_name: Name for the VIEW to create
            uf_filter: Optional list of UF codes to filter

        Raises:
            ValueError: If no Parquet files exist
        """
        glob_pattern = self.get_glob_pattern()

        # Check if files exist
        files = list(self.parquet_dir.rglob("*.parquet"))
        if not files:
            raise ValueError(f"No Parquet files found in {self.parquet_dir}")

        if uf_filter:
            # Create VIEW with UF filter
            uf_values = ", ".join([f"'{uf.upper()}'" for uf in uf_filter])
            sql = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM read_parquet(
                    '{glob_pattern}',
                    hive_partitioning=true
                )
                WHERE uf IN ({uf_values})
            """
        else:
            # Create VIEW without filter
            sql = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM read_parquet(
                    '{glob_pattern}',
                    hive_partitioning=true
                )
            """

        conn.execute(sql)
        self.logger.info(f"Created VIEW {view_name} from {len(files)} Parquet files")

    def create_enriched_view(
        self,
        conn: duckdb.DuckDBPyConnection,
        raw_view_name: str,
        enriched_view_name: str,
    ) -> None:
        """Create an enriched VIEW with dimension table joins.

        Creates a VIEW that joins the raw Parquet data with
        dimension tables (like dim_municipios) if they exist.

        Args:
            conn: DuckDB connection
            raw_view_name: Name of the raw data VIEW
            enriched_view_name: Name for the enriched VIEW
        """
        # Check if dim_municipios exists and has data
        try:
            dim_count = conn.execute(
                "SELECT COUNT(*) FROM dim_municipios"
            ).fetchone()[0]
        except duckdb.CatalogException:
            dim_count = 0

        if dim_count > 0:
            # Create enriched VIEW with IBGE join
            sql = f"""
                CREATE OR REPLACE VIEW {enriched_view_name} AS
                SELECT
                    r.*,
                    m.nome AS municipio_res,
                    m.uf AS uf_res,
                    m.regiao_imediata AS rg_imediata_res,
                    m.regiao_intermediaria AS rg_intermediaria_res
                FROM {raw_view_name} r
                LEFT JOIN dim_municipios m ON r.munic_res = m.codigo_municipio
            """
        else:
            # Create simple VIEW without enrichment
            sql = f"""
                CREATE OR REPLACE VIEW {enriched_view_name} AS
                SELECT * FROM {raw_view_name}
            """

        conn.execute(sql)
        self.logger.info(f"Created enriched VIEW {enriched_view_name}")

    def exists(self) -> bool:
        """Check if Parquet storage exists and has files.

        Returns:
            True if parquet directory exists and contains .parquet files
        """
        if not self.parquet_dir.exists():
            return False
        return any(self.parquet_dir.rglob("*.parquet"))

    def _load_manifest(self) -> dict:
        """Load manifest from disk."""
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                self.logger.warning(f"Failed to load manifest: {e}")
                return {"subsystem": self.subsystem, "processed_files": {}}
        return {"subsystem": self.subsystem, "processed_files": {}}

    def _save_manifest(self, manifest: dict) -> None:
        """Save manifest to disk."""
        try:
            with open(self.manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, ensure_ascii=False)
        except IOError as e:
            self.logger.error(f"Failed to save manifest: {e}")

    def __repr__(self) -> str:
        """String representation."""
        stats = self.get_storage_stats()
        return (
            f"ParquetManager(subsystem={self.subsystem}, "
            f"files={stats.total_files}, partitions={len(stats.partitions)})"
        )
