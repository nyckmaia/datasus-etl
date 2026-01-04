"""Memory-aware parallel processor for large DataSUS datasets.

This module provides a processor that handles large volumes of DBC files
(e.g., 400GB+ for all 27 Brazilian states) without exhausting RAM.

Key features:
- Processes one DBC file at a time, exporting immediately
- Uses independent DuckDB in-memory connections per worker
- Automatically adjusts parallelism based on available RAM
- Exports to persistent DuckDB database (one file per subsystem)
- Supports graceful cancellation via context
"""

import logging
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Optional

import duckdb
import psutil
from tqdm import tqdm

from datasus_etl.config import PipelineConfig
from datasus_etl.exceptions import PipelineCancelled


@dataclass
class ProcessingResult:
    """Result from processing a single DBC file."""

    source_file: str
    uf: str
    rows_exported: int
    output_file: Path
    success: bool
    error: Optional[str] = None


@dataclass
class MemoryEstimate:
    """Memory usage estimates for processing."""

    available_ram_gb: float
    estimated_per_file_gb: float
    recommended_workers: int
    processing_mode: Literal["parallel", "serial"]


class MemoryAwareProcessor:
    """Processes DBC files with automatic memory management.

    This processor is designed for large-scale data processing where
    loading all data into memory is not feasible. It processes each
    DBC file independently:

    1. Decompress DBC → DBF (in temp directory)
    2. Stream DBF → DuckDB (in-memory, independent connection)
    3. Apply SQL transformations
    4. Export to persistent DuckDB database
    5. Clean up DBF and release memory

    Each worker uses its own DuckDB in-memory connection to avoid
    contention and allow true parallel processing.
    """

    # Estimated RAM usage multipliers (based on empirical testing)
    DBF_TO_RAM_MULTIPLIER = 3.0  # DBF file size → RAM when loaded in DuckDB
    SAFETY_MARGIN = 0.7  # Use only 70% of available RAM
    MIN_WORKERS = 1
    MAX_WORKERS = 8

    def __init__(
        self,
        config: PipelineConfig,
        num_workers: int = 4,
        ibge_data_path: Optional[Path] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
        progress_callback: Optional[Callable[[float, str], None]] = None,
    ) -> None:
        """Initialize the memory-aware processor.

        Args:
            config: Pipeline configuration
            num_workers: Maximum number of parallel workers (default: 4)
            ibge_data_path: Optional path to IBGE data for municipality enrichment
            cancel_check: Optional callback to check if cancellation was requested
            progress_callback: Optional callback to report progress (progress: 0-1, message: str)
        """
        self.config = config
        self.num_workers = max(self.MIN_WORKERS, min(num_workers, self.MAX_WORKERS))
        self.ibge_data_path = ibge_data_path
        self.logger = logging.getLogger("datasus_etl.MemoryAwareProcessor")
        self._cancel_check = cancel_check
        self._progress_callback = progress_callback
        self._cancelled = threading.Event()

        # Track results
        self._results: list[ProcessingResult] = []
        self._total_rows = 0

    def _is_cancelled(self) -> bool:
        """Check if processing should be cancelled."""
        if self._cancelled.is_set():
            return True
        if self._cancel_check and self._cancel_check():
            self._cancelled.set()
            return True
        return False

    def estimate_memory_usage(self, dbc_files: list[Path]) -> MemoryEstimate:
        """Estimate memory requirements and recommend worker count.

        Args:
            dbc_files: List of DBC files to process

        Returns:
            MemoryEstimate with recommendations
        """
        # Get available RAM
        mem_info = psutil.virtual_memory()
        available_ram_gb = mem_info.available / (1024**3)

        # Estimate RAM per file (based on largest file)
        if dbc_files:
            max_file_size = max(f.stat().st_size for f in dbc_files)
            # DBF is ~10x larger than DBC, RAM usage is ~3x DBF size
            estimated_dbf_size = max_file_size * 10  # DBC → DBF expansion
            estimated_per_file_gb = (estimated_dbf_size * self.DBF_TO_RAM_MULTIPLIER) / (1024**3)
        else:
            estimated_per_file_gb = 0.5  # Default estimate

        # Calculate safe worker count
        usable_ram = available_ram_gb * self.SAFETY_MARGIN
        max_safe_workers = max(1, int(usable_ram / max(estimated_per_file_gb, 0.1)))

        # Determine recommended workers (min of user setting and safe limit)
        recommended_workers = min(self.num_workers, max_safe_workers)

        # Determine processing mode
        # Serial mode if: very low RAM, or user requested 1 worker
        if recommended_workers <= 1 or available_ram_gb < 2.0:
            processing_mode: Literal["parallel", "serial"] = "serial"
            recommended_workers = 1
        else:
            processing_mode = "parallel"

        self.logger.debug(
            f"Memory estimate: {available_ram_gb:.1f}GB available, "
            f"~{estimated_per_file_gb:.2f}GB per file, "
            f"recommending {recommended_workers} workers ({processing_mode} mode)"
        )

        return MemoryEstimate(
            available_ram_gb=available_ram_gb,
            estimated_per_file_gb=estimated_per_file_gb,
            recommended_workers=recommended_workers,
            processing_mode=processing_mode,
        )

    def process_all(
        self,
        dbc_files: list[Path],
        output_dir: Path,
    ) -> list[ProcessingResult]:
        """Process all DBC files with memory-aware parallelism.

        Args:
            dbc_files: List of DBC files to process
            output_dir: Base output directory for DuckDB database

        Returns:
            List of ProcessingResult for each file
        """
        if not dbc_files:
            self.logger.warning("No DBC files to process")
            return []

        # Estimate memory and get recommendations
        estimate = self.estimate_memory_usage(dbc_files)

        output_dir.mkdir(parents=True, exist_ok=True)

        tqdm.write(
            f"       RAM: {estimate.available_ram_gb:.1f} GB disponível, "
            f"{estimate.recommended_workers} workers ({estimate.processing_mode})"
        )

        # Process based on mode
        if estimate.processing_mode == "serial":
            results = self._process_serial(dbc_files, output_dir)
        else:
            results = self._process_parallel(dbc_files, output_dir, estimate.recommended_workers)

        # Summary
        successful = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        total_rows = sum(r.rows_exported for r in results if r.success)

        # Calculate output size
        db_files = list(output_dir.rglob("*.duckdb"))
        total_size_mb = sum(f.stat().st_size for f in db_files) / (1024 * 1024)

        tqdm.write(
            f"       Processamento concluido: {total_rows:,} linhas, "
            f"{len(db_files)} arquivo(s) DuckDB ({total_size_mb:.1f} MB)"
        )

        if failed > 0:
            tqdm.write(f"       [!] {failed} arquivos falharam")

        self._results = results
        self._total_rows = total_rows

        return results

    def _process_serial(
        self,
        dbc_files: list[Path],
        output_dir: Path,
    ) -> list[ProcessingResult]:
        """Process files one at a time (low memory mode).

        Args:
            dbc_files: List of DBC files
            output_dir: Output directory

        Returns:
            List of results
        """
        results = []
        total_files = len(dbc_files)

        for i, dbc_file in enumerate(tqdm(dbc_files, desc="DBC->DuckDB", leave=False)):
            # Check for cancellation
            if self._is_cancelled():
                tqdm.write("\n[CANCELLED] Processamento cancelado pelo usuário")
                break

            result = self._process_single_file(dbc_file, output_dir)
            results.append(result)

            # Report progress
            if self._progress_callback:
                progress = (i + 1) / total_files
                self._progress_callback(progress, f"{i + 1}/{total_files} arquivos")

            if not result.success:
                tqdm.write(f"[ERROR] {result.source_file}: {result.error}")
            else:
                self.logger.debug(f"Processed {result.source_file}: {result.rows_exported:,} rows")

        return results

    def _process_parallel(
        self,
        dbc_files: list[Path],
        output_dir: Path,
        num_workers: int,
    ) -> list[ProcessingResult]:
        """Process files in parallel with independent DuckDB connections.

        Args:
            dbc_files: List of DBC files
            output_dir: Output directory
            num_workers: Number of parallel workers

        Returns:
            List of results
        """
        results = []
        total_files = len(dbc_files)
        completed_count = 0

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self._process_single_file, dbc_file, output_dir): dbc_file
                for dbc_file in dbc_files
            }

            # Process completed tasks
            for future in tqdm(
                as_completed(future_to_file),
                total=total_files,
                desc=f"DBC->DuckDB ({num_workers}w)",
                leave=False,
            ):
                # Check for cancellation
                if self._is_cancelled():
                    tqdm.write("\n[CANCELLED] Processamento cancelado pelo usuário")
                    # Cancel pending futures
                    for f in future_to_file:
                        f.cancel()
                    break

                dbc_file = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    completed_count += 1

                    # Report progress
                    if self._progress_callback:
                        progress = completed_count / total_files
                        self._progress_callback(progress, f"{completed_count}/{total_files} arquivos")

                    if not result.success:
                        tqdm.write(f"[ERROR] {result.source_file}: {result.error}")
                    else:
                        self.logger.debug(
                            f"Processed {result.source_file}: {result.rows_exported:,} rows"
                        )

                except Exception as e:
                    self.logger.error(f"Worker failed for {dbc_file.name}: {e}")
                    results.append(
                        ProcessingResult(
                            source_file=dbc_file.name,
                            uf="??",
                            rows_exported=0,
                            output_file=Path(),
                            success=False,
                            error=str(e),
                        )
                    )
                    completed_count += 1

                    # Report progress even on failure
                    if self._progress_callback:
                        progress = completed_count / total_files
                        self._progress_callback(progress, f"{completed_count}/{total_files} arquivos")

        return results

    def _process_single_file(
        self,
        dbc_file: Path,
        output_dir: Path,
    ) -> ProcessingResult:
        """Process a single DBC file end-to-end.

        Each call creates its own DuckDB in-memory connection,
        ensuring complete isolation between workers.

        Args:
            dbc_file: Path to DBC file
            output_dir: Base output directory

        Returns:
            ProcessingResult with status and stats
        """
        source_name = dbc_file.stem
        uf = "??"

        try:
            import datasus_dbc

            # Create independent in-memory DuckDB connection for this worker
            conn = duckdb.connect(":memory:")

            # Configure memory limit for this connection (smaller per-connection limit)
            available_ram_gb = psutil.virtual_memory().available / (1024**3)
            per_conn_limit_gb = max(1, int(available_ram_gb * 0.3))
            conn.execute(f"SET memory_limit = '{per_conn_limit_gb}GB'")

            # Configure temp directory for spilling
            temp_dir = Path(tempfile.gettempdir()) / "datasus_etl_worker"
            temp_dir.mkdir(parents=True, exist_ok=True)
            conn.execute(f"SET temp_directory = '{temp_dir}'")

            try:
                # Step 1: Decompress DBC to temporary DBF
                with tempfile.TemporaryDirectory() as temp_dbf_dir:
                    dbf_file = Path(temp_dbf_dir) / f"{source_name}.dbf"
                    datasus_dbc.decompress(str(dbc_file), str(dbf_file))

                    # Step 2: Extract UF from filename (e.g., RDSP2301.dbc → SP)
                    uf = self._extract_uf_from_filename(source_name)

                    # Step 3: Stream DBF to DuckDB staging table
                    staging_table = f"staging_{source_name}"
                    rows_loaded = self._stream_dbf_to_duckdb(conn, dbf_file, staging_table, uf)

                    if rows_loaded == 0:
                        return ProcessingResult(
                            source_file=dbc_file.name,
                            uf=uf,
                            rows_exported=0,
                            output_file=Path(),
                            success=True,
                            error="Empty file (0 rows)",
                        )

                    # Step 4: Transform using SQL
                    view_name = f"canonical_{source_name}"
                    self._apply_sql_transforms(conn, staging_table, view_name)

                    # Step 5: Export to persistent DuckDB database
                    output_file = self._export_to_duckdb(
                        conn, view_name, source_name, uf, output_dir
                    )

                    return ProcessingResult(
                        source_file=dbc_file.name,
                        uf=uf,
                        rows_exported=rows_loaded,
                        output_file=output_file,
                        success=True,
                    )

            finally:
                # Always close connection to release memory
                conn.close()

        except Exception as e:
            self.logger.error(f"Failed to process {dbc_file.name}: {e}")
            return ProcessingResult(
                source_file=dbc_file.name,
                uf=uf,
                rows_exported=0,
                output_file=Path(),
                success=False,
                error=str(e),
            )

    def _extract_uf_from_filename(self, filename: str) -> str:
        """Extract UF code from DATASUS filename.

        Examples:
            RDSP2301 → SP
            DOSP2023 → SP
            RDMG2312 → MG

        Args:
            filename: Filename without extension

        Returns:
            2-letter UF code
        """
        # Standard DATASUS format: XX[UF]YYMM or XX[UF]YYYY
        # Position 2-4 typically contains UF
        if len(filename) >= 4:
            potential_uf = filename[2:4].upper()
            from datasus_etl.constants import ALL_UFS

            if potential_uf in ALL_UFS:
                return potential_uf

        return "??"

    def _stream_dbf_to_duckdb(
        self,
        conn: duckdb.DuckDBPyConnection,
        dbf_file: Path,
        table_name: str,
        uf: str,
    ) -> int:
        """Stream DBF file to DuckDB table with chunked reading.

        Args:
            conn: DuckDB connection
            dbf_file: Path to DBF file
            table_name: Target table name
            uf: UF code to add as column

        Returns:
            Number of rows loaded
        """
        from dbfread import DBF

        chunk_size = self.config.database.chunk_size
        total_rows = 0

        # Read DBF with streaming
        dbf = DBF(str(dbf_file), encoding="latin-1", ignore_missing_memofile=True)

        # Get field names and create table
        field_names = dbf.field_names
        columns_sql = ", ".join([f'"{name}" VARCHAR' for name in field_names])
        columns_sql += ', "UF" VARCHAR, "SOURCE_FILE" VARCHAR'

        conn.execute(f"CREATE TABLE {table_name} ({columns_sql})")

        # Stream in chunks
        chunk = []
        source_file_name = dbf_file.stem + ".dbc"

        for record in dbf:
            row = list(record.values()) + [uf, source_file_name]
            chunk.append(row)

            if len(chunk) >= chunk_size:
                self._insert_chunk(conn, table_name, field_names, chunk)
                total_rows += len(chunk)
                chunk = []

        # Insert remaining rows
        if chunk:
            self._insert_chunk(conn, table_name, field_names, chunk)
            total_rows += len(chunk)

        return total_rows

    def _insert_chunk(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        field_names: list[str],
        chunk: list[list],
    ) -> None:
        """Insert a chunk of rows into DuckDB table.

        Args:
            conn: DuckDB connection
            table_name: Target table name
            field_names: List of column names (without UF and SOURCE_FILE)
            chunk: List of rows to insert
        """
        all_columns = [f'"{name}"' for name in field_names] + ['"UF"', '"SOURCE_FILE"']
        columns_str = ", ".join(all_columns)
        placeholders = ", ".join(["?" for _ in all_columns])

        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        conn.executemany(insert_sql, chunk)

    def _apply_sql_transforms(
        self,
        conn: duckdb.DuckDBPyConnection,
        source_table: str,
        target_view: str,
    ) -> None:
        """Apply SQL transformations to create canonical view.

        Args:
            conn: DuckDB connection
            source_table: Source staging table name
            target_view: Target view name
        """
        from datasus_etl.storage.sql_transformer import SQLTransformer

        transformer = SQLTransformer(
            conn,
            subsystem=self.config.subsystem,
            raw_mode=self.config.raw_mode,
        )

        transformer.transform_to_canonical_view(
            source_table=source_table,
            target_view=target_view,
            ibge_data_path=self.ibge_data_path,
        )

    def _export_to_duckdb(
        self,
        conn: duckdb.DuckDBPyConnection,
        view_name: str,
        source_name: str,
        uf: str,
        output_dir: Path,
    ) -> Path:
        """Export data to persistent DuckDB database.

        Args:
            conn: In-memory DuckDB connection with transformed data
            view_name: View to export
            source_name: Original source file name (without extension)
            uf: UF code
            output_dir: Base output directory

        Returns:
            Path to DuckDB database file
        """
        import threading

        # Get database path from config
        db_path = self.config.get_database_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Use a lock to prevent concurrent writes to the same database
        lock_key = str(db_path)
        if not hasattr(self, '_db_locks'):
            self._db_locks = {}
        if lock_key not in self._db_locks:
            self._db_locks[lock_key] = threading.Lock()

        with self._db_locks[lock_key]:
            # Open persistent connection
            persistent_conn = duckdb.connect(str(db_path))

            try:
                # Get schema for this subsystem
                from datasus_etl.storage.sql_transformer import SUBSYSTEM_SCHEMAS
                from datasus_etl.storage.duckdb_manager import DuckDBManager
                from datasus_etl.config import DatabaseConfig

                schema = SUBSYSTEM_SCHEMAS.get(self.config.subsystem, {})
                raw_table = f"{self.config.subsystem}_raw"

                # Initialize raw table if needed (thread-safe via lock)
                temp_config = DatabaseConfig(db_path=db_path)
                persistent_manager = DuckDBManager(temp_config)
                persistent_manager._conn = persistent_conn
                persistent_manager.initialize_raw_table(self.config.subsystem, schema)

                # Check if source file already exists
                source_file = f"{source_name}.dbc"
                existing = persistent_conn.execute(
                    f"SELECT COUNT(*) FROM {raw_table} WHERE source_file = ?",
                    [source_file]
                ).fetchone()[0]

                if existing > 0:
                    self.logger.debug(f"Skipping {source_name} - already processed")
                    return db_path

                # Transfer data from in-memory to persistent DB
                # Use fetchall() + executemany() to avoid DuckDB lock conflicts
                data = conn.execute(f"SELECT * FROM {view_name}").fetchall()

                # Get column names for INSERT
                columns = [desc[0] for desc in conn.execute(
                    f"SELECT * FROM {view_name} LIMIT 0"
                ).description]

                # Insert into persistent connection
                placeholders = ", ".join(["?" for _ in columns])
                col_list = ", ".join(columns)
                persistent_conn.executemany(
                    f"INSERT INTO {raw_table} ({col_list}) VALUES ({placeholders})",
                    data
                )

            finally:
                persistent_conn.close()

        return db_path

    @property
    def results(self) -> list[ProcessingResult]:
        """Get processing results."""
        return self._results

    @property
    def total_rows_exported(self) -> int:
        """Get total rows exported across all files."""
        return self._total_rows
