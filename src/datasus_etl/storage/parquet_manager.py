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
    # Sibling JSON cache that holds per-column NULL/fill statistics + an
    # approximate distinct-value count. The `/api/query/dictionary` endpoint
    # reads this to seed the "Colunas" panel badges; falls back to
    # recomputing via `parquet_metadata()` + `approx_count_distinct()` if
    # the file is missing or older than any source parquet.
    COLUMN_STATS_FILENAME = "_column_stats.json"
    # Bump this whenever the JSON shape changes OR the computation logic
    # changes in a way that should invalidate caches written by older code.
    # `column_stats_are_fresh()` treats any cache with a different version
    # as stale and recomputes silently.
    #
    # Version history:
    #   1 — initial cache: null_pct, fill_pct (footer-only), no version field
    #   2 — added distinct_count via approx_count_distinct (batched)
    #   3 — array columns get row-level fill_pct ("NULL or empty list" = null);
    #       distinct-count fallback per-column when the batched query fails
    COLUMN_STATS_VERSION = 3

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
        self.column_stats_path = self.parquet_dir / self.COLUMN_STATS_FILENAME
        self.logger = logging.getLogger(__name__)

        # Intentionally NOT creating parquet_dir here. The dashboard
        # instantiates one ParquetManager per registered subsystem just to
        # query stats; eagerly creating folders would litter the data dir
        # with empty `<subsystem>/` placeholders before the user has
        # downloaded anything. All read paths already guard with
        # `.exists()`. The only writer (`mark_processed`) creates the dir
        # itself, and `DbfToParquetStage` mkdirs the partition tree at
        # download time.

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
                    union_by_name=true,
                    hive_partitioning=true,
                    filename=true
                )
                WHERE uf IN ({uf_values})
            """
        else:
            # Create VIEW without filter
            sql = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM read_parquet(
                    '{glob_pattern}',
                    union_by_name=true,
                    hive_partitioning=true,
                    filename=true
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

    # ──────────────────────────────────────────────────────────────────────
    # Column statistics (NULL / fill percentages)
    #
    # Computed from parquet column-chunk footers via DuckDB's
    # `parquet_metadata(<glob>)` — a metadata-only read that scales to 40+ GB
    # subsystems in seconds. Persisted as a sibling JSON of `_manifest.json`
    # so the dictionary endpoint serves the values instantly on the hot path.
    # ──────────────────────────────────────────────────────────────────────

    def compute_column_stats(
        self, conn: Optional[duckdb.DuckDBPyConnection] = None
    ) -> dict:
        """Aggregate per-column null/fill percentages from parquet footers.

        Uses ``parquet_metadata()`` to read each parquet file's footer (no
        row scan) and sums ``null_count`` / ``num_values`` over column-chunks
        per column.

        Args:
            conn: Optional DuckDB connection to reuse. If None, a one-shot
                in-memory connection is created and closed before returning.

        Returns:
            A dict shaped like::

                {
                    "computed_at": "2026-04-27T20:15:00",
                    "row_count": 12_345_678,
                    "columns": {
                        "uf": {"null_pct": 0.0, "fill_pct": 100.0},
                        "codmunres": {"null_pct": 0.04, "fill_pct": 99.96},
                        ...
                    },
                }

            Returns ``{"computed_at": ..., "row_count": 0, "columns": {}}``
            when there are no parquet files.
        """
        files = list(self.parquet_dir.rglob("*.parquet")) if self.parquet_dir.exists() else []
        if not files:
            return {
                "version": self.COLUMN_STATS_VERSION,
                "computed_at": datetime.now().isoformat(timespec="seconds"),
                "row_count": 0,
                "columns": {},
            }

        glob_pattern = self.get_glob_pattern()
        owns_conn = conn is None
        if owns_conn:
            conn = duckdb.connect(":memory:")
        try:
            # `parquet_metadata` exposes one row per (file, column-chunk).
            # Aggregate across all chunks per column. Any column-chunk that
            # was written without statistics surfaces as `null_count = NULL`
            # — we exclude those from the divisor so a single missing chunk
            # doesn't poison the whole percentage.
            # Column name in DuckDB's `parquet_metadata` is `stats_null_count`
            # (it's a flattened version of the parquet column-chunk stats).
            #
            # In DuckDB 1.5+, `path_in_schema` is a VARCHAR. For scalar
            # columns it's just the name (``codmunres``); for array
            # columns the nesting levels are comma-space separated
            # (e.g. ``causabas, list, element``). We aggregate by the
            # first segment so array columns roll up under their top-level
            # name and the cache key matches the schema name.
            rows = conn.execute(
                f"""
                SELECT
                    split_part(path_in_schema, ', ', 1) AS column,
                    SUM(num_values) AS total_values,
                    SUM(CASE WHEN stats_null_count IS NOT NULL
                             THEN stats_null_count ELSE 0 END) AS total_nulls,
                    BOOL_AND(stats_null_count IS NOT NULL) AS all_chunks_have_stats
                FROM parquet_metadata('{glob_pattern}')
                GROUP BY split_part(path_in_schema, ', ', 1)
                """
            ).fetchall()

            # Total row count (per the first parquet's row groups). Cheaper
            # than COUNT(*) — we sum num_rows from `parquet_file_metadata`
            # over all files, which is also a footer-only read.
            row_count_row = conn.execute(
                f"SELECT COALESCE(SUM(num_rows), 0) FROM parquet_file_metadata('{glob_pattern}')"
            ).fetchone()
            row_count = int(row_count_row[0]) if row_count_row else 0

            # Detect array columns via DESCRIBE so we can give them a
            # row-level fill_pct (parquet's footer `null_count` for arrays
            # is reported at the LEAF level, which has different semantics
            # from "% of rows with a non-empty list"). We learn the column
            # types from the parquet's logical schema; `LIMIT 0` keeps the
            # plan free from any actual row scan.
            col_types: dict[str, str] = {}
            try:
                describe_rows = conn.execute(
                    f"""
                    DESCRIBE
                    SELECT * FROM read_parquet('{glob_pattern}',
                                               hive_partitioning=true)
                    """
                ).fetchall()
                # row[0] = column_name, row[1] = column_type
                for r in describe_rows:
                    if r and r[0] is not None:
                        col_types[r[0]] = (r[1] or "").upper()
            except duckdb.Error as exc:  # noqa: BLE001
                self.logger.warning("DESCRIBE failed for %s: %s", self.subsystem, exc)

            def _is_array(name: str) -> bool:
                # DuckDB renders array types as `<base>[]` (e.g. `VARCHAR[]`).
                return col_types.get(name, "").endswith("[]")

            col_list = [r[0] for r in rows if r[0]]
            array_cols = [c for c in col_list if _is_array(c)]

            # ── Distinct counts + (for arrays) row-level fill_pct ─────────
            # We try a single batched SELECT first — it reads each column
            # once and is the fastest path. If it fails (e.g. one column
            # rejects approx_count_distinct on this DuckDB version, or the
            # batched plan exhausts memory), we fall back to per-column
            # queries so a single bad column can't poison every metric.
            distinct_counts: dict[str, int | None] = {}
            array_fill_pcts: dict[str, float | None] = {}

            def _array_fill_sql(col: str) -> str:
                # Per the user contract: a row counts as null ONLY when the
                # array column is explicitly NULL or an empty list `[]`.
                # Anything else (including `[NULL, NULL]`) counts as filled.
                return (
                    f'100.0 * SUM(CASE WHEN "{col}" IS NULL OR len("{col}") = 0 '
                    f"THEN 0 ELSE 1 END) / NULLIF(COUNT(*), 0)"
                )

            if col_list:
                select_parts: list[str] = []
                for i, c in enumerate(col_list):
                    select_parts.append(f'approx_count_distinct("{c}") AS _d{i}')
                for i, c in enumerate(array_cols):
                    select_parts.append(f"{_array_fill_sql(c)} AS _af{i}")

                batched_ok = False
                try:
                    result_row = conn.execute(
                        f"""
                        SELECT {", ".join(select_parts)}
                        FROM read_parquet('{glob_pattern}',
                                          hive_partitioning=true)
                        """
                    ).fetchone()
                    if result_row is not None:
                        for i, c in enumerate(col_list):
                            v = result_row[i]
                            distinct_counts[c] = (
                                int(v) if v is not None else None
                            )
                        offset = len(col_list)
                        for i, c in enumerate(array_cols):
                            v = result_row[offset + i]
                            array_fill_pcts[c] = (
                                round(float(v), 2) if v is not None else None
                            )
                        batched_ok = True
                except duckdb.Error as exc:  # noqa: BLE001
                    self.logger.warning(
                        "Batched compute failed for %s; falling back per-column: %s",
                        self.subsystem,
                        exc,
                    )

                if not batched_ok:
                    # Per-column fallback — slower but resilient. Each
                    # column gets its own query so a single problematic
                    # column doesn't lose us the metrics for the others.
                    for c in col_list:
                        try:
                            v = conn.execute(
                                f"""
                                SELECT approx_count_distinct("{c}")
                                FROM read_parquet('{glob_pattern}',
                                                  hive_partitioning=true)
                                """
                            ).fetchone()
                            if v is not None and v[0] is not None:
                                distinct_counts[c] = int(v[0])
                        except duckdb.Error as exc:
                            self.logger.warning(
                                "distinct count failed for %s.%s: %s",
                                self.subsystem,
                                c,
                                exc,
                            )
                    for c in array_cols:
                        try:
                            v = conn.execute(
                                f"""
                                SELECT {_array_fill_sql(c)}
                                FROM read_parquet('{glob_pattern}',
                                                  hive_partitioning=true)
                                """
                            ).fetchone()
                            if v is not None and v[0] is not None:
                                array_fill_pcts[c] = round(float(v[0]), 2)
                        except duckdb.Error as exc:
                            self.logger.warning(
                                "array fill_pct failed for %s.%s: %s",
                                self.subsystem,
                                c,
                                exc,
                            )
        finally:
            if owns_conn:
                conn.close()

        columns: dict[str, dict[str, float | int | None]] = {}
        for col_name, total_values, total_nulls, all_have_stats in rows:
            if not col_name:
                continue
            distinct = distinct_counts.get(col_name)

            # Array columns: prefer the row-level fill_pct we computed in
            # the SQL above. Parquet's footer `null_count` for arrays is at
            # the leaf-element level — counts each array slot, not each row
            # — so the user-visible "% of rows whose list has values" is
            # better served by the explicit CASE/len computation.
            if col_name in array_fill_pcts:
                fill_pct = array_fill_pcts[col_name]
                if fill_pct is None:
                    columns[col_name] = {
                        "null_pct": None,
                        "fill_pct": None,
                        "distinct_count": distinct,
                    }
                    continue
                null_pct = round(100.0 - fill_pct, 2)
                columns[col_name] = {
                    "null_pct": null_pct,
                    "fill_pct": fill_pct,
                    "distinct_count": distinct,
                }
                continue

            if not all_have_stats or total_values is None or total_values == 0:
                # We can't compute a reliable percentage — surface as None
                # so the UI shows the muted "?" badge instead of a bogus 0%.
                columns[col_name] = {
                    "null_pct": None,
                    "fill_pct": None,
                    "distinct_count": distinct,
                }
                continue
            null_pct = float(total_nulls) / float(total_values) * 100.0
            # Round to 2 decimals server-side so JSON stays compact and the
            # UI never has to decide between 99.96 and 99.96000000000001.
            null_pct = round(null_pct, 2)
            fill_pct = round(100.0 - null_pct, 2)
            columns[col_name] = {
                "null_pct": null_pct,
                "fill_pct": fill_pct,
                "distinct_count": distinct,
            }

        return {
            "version": self.COLUMN_STATS_VERSION,
            "computed_at": datetime.now().isoformat(timespec="seconds"),
            "row_count": row_count,
            "columns": columns,
        }

    def load_column_stats(self) -> Optional[dict]:
        """Read the persisted column-stats JSON, or return None if missing/corrupt."""
        if not self.column_stats_path.exists():
            return None
        try:
            with open(self.column_stats_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Failed to load column stats: {e}")
            return None

    def save_column_stats(self, stats: dict) -> None:
        """Persist the column-stats JSON next to ``_manifest.json``."""
        try:
            self.parquet_dir.mkdir(parents=True, exist_ok=True)
            with open(self.column_stats_path, "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
        except IOError as e:
            self.logger.error(f"Failed to save column stats: {e}")

    def column_stats_are_fresh(self) -> bool:
        """True if `_column_stats.json` exists, has the current schema version,
        and is newer than every parquet.

        The check walks the same `rglob('*.parquet')` we already use elsewhere
        — slow only if the partition tree is enormous, but no slower than
        `get_storage_stats()` which the dashboard already calls.

        Caches written by an older code version (e.g. before the
        `distinct_count` field existed) are treated as stale and recomputed
        silently — this avoids users seeing partial data after an upgrade.
        """
        if not self.column_stats_path.exists():
            return False
        try:
            cache_mtime = self.column_stats_path.stat().st_mtime
        except OSError:
            return False

        # Schema-version gate: out-of-date JSON ⇒ stale, no need to read mtimes.
        cached = self.load_column_stats()
        if not isinstance(cached, dict):
            return False
        if cached.get("version") != self.COLUMN_STATS_VERSION:
            return False

        if not self.parquet_dir.exists():
            # No data → trivially "fresh" (nothing to recompute against).
            return True
        for parquet_file in self.parquet_dir.rglob("*.parquet"):
            try:
                if parquet_file.stat().st_mtime > cache_mtime:
                    return False
            except OSError:
                continue
        return True

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
            # Lazy mkdir — the constructor no longer creates parquet_dir,
            # so the first real write needs to materialize it.
            self.parquet_dir.mkdir(parents=True, exist_ok=True)
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
