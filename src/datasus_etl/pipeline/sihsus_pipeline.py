"""Complete SIHSUS data processing pipeline."""

import logging
import os
import tempfile
from pathlib import Path
from datetime import datetime

from datasus_etl.config import PipelineConfig
from datasus_etl.constants import SYM_CHECK, SYM_ARROW, SYM_FILE, SYM_CHART
from datasus_etl.core.pipeline import Pipeline
from datasus_etl.core.stage import Stage
from datasus_etl.core.context import PipelineContext
from datasus_etl.download.ftp_downloader import FTPDownloader
from datasus_etl.transform.converters.dbc_to_dbf import DbcToDbfConverter
from datasus_etl.transform.converters.dbf_to_duckdb import DbfToDuckDBConverter
from datasus_etl.storage.sql_transformer import SQLTransformer
from datasus_etl.storage.duckdb_manager import DuckDBManager
from tqdm import tqdm

try:
    import humanize
except ImportError:
    humanize = None


class DownloadStage(Stage):
    """Stage for downloading DBC files from DATASUS FTP."""

    def __init__(self, config: PipelineConfig, subsystem: str = "sihsus") -> None:
        super().__init__(f"Download {subsystem.upper()} Data")
        self.config = config
        self.subsystem = subsystem

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute download stage."""
        downloader = FTPDownloader(self.config.download, subsystem=self.subsystem)
        files = downloader.download()

        context.set("downloaded_files", files)
        context.set_metadata("download_count", len(files))

        self.logger.info(f"Downloaded {len(files)} files")
        return context


class DbcToDbfStage(Stage):
    """Stage for converting DBC to DBF files."""

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Convert DBC to DBF")
        self.config = config

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute DBC to DBF conversion stage."""
        converter = DbcToDbfConverter(self.config.conversion)
        stats = converter.convert_directory()

        context.set("dbc_conversion_stats", stats)
        context.set_metadata("dbc_converted_count", stats["converted"])

        self.logger.info(f"Converted {stats['converted']} DBC files to DBF")
        return context


class DbfToDbStage(Stage):
    """Stage for streaming DBF directly to DuckDB."""

    def __init__(self, config: PipelineConfig, subsystem: str = "sihsus") -> None:
        super().__init__("Stream DBF to DuckDB")
        self.config = config
        self.subsystem = subsystem

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Stream all DBF files to DuckDB staging tables with parallel processing."""
        import multiprocessing
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Get or create DuckDB manager (shared across stages)
        db_manager = context.get("db_manager")
        if db_manager is None:
            db_manager = DuckDBManager(self.config.database)
            db_manager.__enter__()  # Open connection
            context.set("db_manager", db_manager)

        converter = DbfToDuckDBConverter(
            conn=db_manager._conn,
            chunk_size=self.config.database.chunk_size,
            dataframe_threshold_mb=self.config.database.dataframe_threshold_mb
        )

        # Find DBF files filtered by subsystem prefix
        from datasus_etl.datasets import DatasetRegistry

        dataset_config = DatasetRegistry.get(self.subsystem)
        file_prefix = dataset_config.FILE_PREFIX.upper() if dataset_config else ""

        all_dbf_files = list(self.config.conversion.dbf_dir.rglob("*.dbf"))

        # Filter by subsystem prefix to avoid mixing data from different subsystems
        if file_prefix:
            dbf_files = [
                f for f in all_dbf_files
                if f.stem.upper().startswith(file_prefix)
            ]
            self.logger.debug(
                f"Filtered {len(all_dbf_files)} DBF files to {len(dbf_files)} "
                f"matching prefix '{file_prefix}'"
            )
        else:
            dbf_files = all_dbf_files

        if not dbf_files:
            self.logger.warning(
                f"No DBF files found in {self.config.conversion.dbf_dir} "
                f"matching prefix '{file_prefix}'"
            )
            return context

        # Import tqdm for terminal-visible logging
        from tqdm import tqdm

        # Use single worker to avoid DuckDB connection contention
        # ThreadPoolExecutor with multiple workers causes race conditions
        # when all threads try to INSERT into the same DuckDB connection
        max_workers = 1
        total_rows = 0
        staging_tables = []

        self.logger.info(f"Streaming {len(dbf_files)} DBF files using {max_workers} threads")

        # Log all files to be processed (use tqdm.write for terminal visibility)
        tqdm.write(f"\nStreaming {len(dbf_files)} DBF files using {max_workers} threads:")
        for dbf_file in dbf_files:
            file_size_mb = dbf_file.stat().st_size / (1024 * 1024)
            tqdm.write(f"  - {dbf_file.name} ({file_size_mb:.1f}MB)")
        tqdm.write("")  # Empty line before progress bar

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(
                    converter.stream_dbf_to_table,
                    dbf_file,
                    f"staging_{dbf_file.stem}",
                    True
                ): dbf_file
                for dbf_file in dbf_files
            }

            # Process completed tasks with progress bar
            for future in tqdm(as_completed(future_to_file), total=len(dbf_files), desc=f"Streaming DBF{SYM_ARROW}DuckDB"):
                dbf_file = future_to_file[future]
                try:
                    rows = future.result()
                    total_rows += rows
                    staging_tables.append(f"staging_{dbf_file.stem}")
                    # Log completion with row count
                    tqdm.write(f"[OK] Completed {dbf_file.name}: {rows:,} rows")
                except Exception as e:
                    self.logger.error(f"Failed to stream {dbf_file.name}: {e}")
                    continue

        context.set("staging_tables", staging_tables)
        context.set_metadata("total_rows_loaded", total_rows)

        tqdm.write(f"\n[DONE] Streamed {total_rows:,} rows from {len(dbf_files)} DBF files\n")

        return context


class SqlTransformStage(Stage):
    """Stage for SQL-based data transformation and DuckDB export.

    Transforms staging tables and inserts into persistent DuckDB database.
    Creates enrichment VIEW with IBGE dimension data.
    """

    def __init__(self, config: PipelineConfig, ibge_data_path: Path = None) -> None:
        super().__init__("Transform Data (SQL) and Export to DuckDB")
        self.config = config
        self.ibge_data_path = ibge_data_path

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Transform staging tables using SQL and insert into persistent DuckDB."""
        import duckdb
        from datasus_etl.storage.dimension_loader import DimensionLoader

        # Get shared DuckDB manager from context (in-memory, used for staging)
        staging_db_manager = context.get("db_manager")
        if staging_db_manager is None:
            raise ValueError("DuckDB manager not found in context. DBF stage must run first.")

        # Get list of staging tables
        staging_tables = context.get("staging_tables", [])
        if not staging_tables:
            self.logger.warning("No staging tables found")
            return context

        tqdm.write(f"\n[TRANSFORM] Transforming {len(staging_tables)} tables {SYM_ARROW} DuckDB...")

        # Get the persistent database path
        db_path = self.config.get_database_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Open persistent DuckDB connection
        persistent_conn = duckdb.connect(str(db_path))

        try:
            # Get schema for SIHSUS
            from datasus_etl.storage.sql_transformer import SUBSYSTEM_SCHEMAS
            schema = SUBSYSTEM_SCHEMAS.get("sihsus", SUBSYSTEM_SCHEMAS["sihsus"])

            # Initialize raw table in persistent DB
            from datasus_etl.config import DatabaseConfig
            temp_config = DatabaseConfig(db_path=db_path)
            persistent_manager = DuckDBManager(temp_config)
            persistent_manager._conn = persistent_conn

            # Initialize schema (table + dimensions)
            persistent_manager.initialize_raw_table("sihsus", schema)
            persistent_manager.initialize_dimension_tables()

            # Load IBGE dimension data
            dim_loader = DimensionLoader(persistent_conn)
            try:
                dim_loader.load_ibge_municipios(replace=False)
                tqdm.write(f"[OK] Dados IBGE carregados na tabela dim_municipios")
            except Exception as e:
                self.logger.warning(f"Failed to load IBGE data: {e}")

            total_rows_inserted = 0
            raw_table = "sihsus_raw"

            # Create transformer for staging DB
            transformer = SQLTransformer(
                staging_db_manager._conn,
                subsystem="sihsus",
                raw_mode=self.config.raw_mode,
            )

            # Process each staging table
            for i, staging_table in enumerate(staging_tables, 1):
                original_name = staging_table.replace("staging_", "")
                tqdm.write(f"\n[{i}/{len(staging_tables)}] Transforming {original_name}...")

                try:
                    # Transform in staging DB (creates a view)
                    view_name = f"canonical_{original_name}"
                    transformer.transform_to_canonical_view(
                        source_table=staging_table,
                        target_view=view_name,
                        enable_ibge_enrichment=False,
                    )

                    # Get row count
                    row_count = staging_db_manager._conn.execute(
                        f"SELECT COUNT(*) FROM {view_name}"
                    ).fetchone()[0]

                    if row_count == 0:
                        self.logger.debug(f"Skipping empty view: {view_name}")
                        continue

                    # Check if source_file already exists in persistent DB (deduplication)
                    source_file_value = staging_db_manager._conn.execute(
                        f"SELECT DISTINCT source_file FROM {view_name} LIMIT 1"
                    ).fetchone()

                    if source_file_value:
                        source_file = source_file_value[0]
                        existing = persistent_conn.execute(
                            f"SELECT COUNT(*) FROM {raw_table} WHERE source_file = ?",
                            [source_file]
                        ).fetchone()[0]

                        if existing > 0:
                            tqdm.write(f"[SKIP] {original_name} ja processado")
                            continue

                    # Transfer data from staging to persistent DB using Parquet
                    # This is much faster than fetchall() + executemany()
                    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                        tmp_path = tmp.name

                    try:
                        # Export from staging DB to parquet (fast bulk export)
                        staging_db_manager._conn.execute(
                            f"COPY (SELECT * FROM {view_name}) TO '{tmp_path}' (FORMAT PARQUET)"
                        )

                        # Import from parquet to persistent DB (fast bulk insert)
                        persistent_conn.execute(
                            f"INSERT INTO {raw_table} SELECT * FROM read_parquet('{tmp_path}')"
                        )
                    finally:
                        # Cleanup temp file
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)

                    total_rows_inserted += row_count
                    tqdm.write(f"[OK] Inserted {original_name}: {row_count:,} rows")

                    # Cleanup staging view
                    staging_db_manager._conn.execute(f"DROP VIEW IF EXISTS {view_name}")

                except Exception as e:
                    tqdm.write(f"[ERROR] Failed: {original_name}: {e}")
                    self.logger.error(f"Failed to transform/insert {staging_table}: {e}")
                    raise

            # Create enrichment VIEW in persistent DB
            persistent_manager.create_enrichment_view("sihsus")

            # Get final stats
            total_rows = persistent_conn.execute(
                f"SELECT COUNT(*) FROM {raw_table}"
            ).fetchone()[0]

            # Get database file size
            db_size_bytes = db_path.stat().st_size
            db_size_mb = db_size_bytes / (1024 * 1024)

            context.set_metadata("total_rows_exported", total_rows_inserted)
            context.set_metadata("total_rows_in_db", total_rows)
            context.set("database_path", str(db_path))
            context.set("database_size_mb", db_size_mb)

            tqdm.write(f"\n[DONE] Export complete:")
            tqdm.write(f"  - Inserted: {total_rows_inserted:,} rows")
            tqdm.write(f"  - Total in DB: {total_rows:,} rows")
            tqdm.write(f"  - Database: {db_path.name} ({db_size_mb:.1f} MB)\n")

        finally:
            persistent_conn.close()

        return context


class RawCsvExportStage(Stage):
    """Optional stage: Export raw DBF data to CSV before transformations."""

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Export Raw CSV")
        self.config = config

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Export all staging tables to CSV files."""
        if not self.config.storage.export_raw_csv:
            self.logger.info("Raw CSV export disabled, skipping...")
            return context

        # Get output directory
        csv_dir = self.config.storage.csv_dir or (
            self.config.storage.database_dir / "csv_raw"
        )
        csv_dir.mkdir(parents=True, exist_ok=True)

        # Get DuckDB connection from context
        db_manager: DuckDBManager = context.get("db_manager")
        staging_tables: list[str] = context.get("staging_tables", [])

        self.logger.info(f"Exporting {len(staging_tables)} tables to CSV (raw)")

        # Export each staging table
        for table in staging_tables:
            output_file = csv_dir / f"{table}.csv"

            db_manager._conn.execute(f"""
                COPY (SELECT * FROM {table})
                TO '{output_file}'
                (HEADER true, DELIMITER ';', ENCODING 'UTF-8')
            """)

            self.logger.info(f"Exported {table} {SYM_ARROW} {output_file.name}")

        context.set("raw_csv_files", list(csv_dir.glob("*.csv")))
        self.logger.info(f"Raw CSV export complete: {csv_dir}")
        return context


class CleanedCsvExportStage(Stage):
    """Optional stage: Export cleaned/transformed data to CSV."""

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Export Cleaned CSV")
        self.config = config

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Export transformed view to CSV."""
        if not self.config.storage.export_cleaned_csv:
            self.logger.info("Cleaned CSV export disabled, skipping...")
            return context

        # Get output directory
        csv_dir = self.config.storage.csv_dir or (
            self.config.storage.database_dir / "csv_cleaned"
        )
        csv_dir.mkdir(parents=True, exist_ok=True)

        # Get DuckDB connection
        db_manager: DuckDBManager = context.get("db_manager")

        # Export cleaned data
        output_file = csv_dir / "sihsus_cleaned.csv"

        self.logger.info(f"Exporting cleaned data to CSV...")

        db_manager._conn.execute(f"""
            COPY (SELECT * FROM sihsus_processed ORDER BY dt_inter ASC NULLS LAST)
            TO '{output_file}'
            (HEADER true, DELIMITER ';', ENCODING 'UTF-8')
        """)

        self.logger.info(f"Exported {SYM_ARROW} {output_file}")

        context.set("cleaned_csv_file", output_file)
        return context


class SihsusPipeline(Pipeline[PipelineConfig]):
    """Complete SIHSUS data processing pipeline.

    Orchestrates the entire data pipeline from download to DuckDB database:
    1. Download DBC files from DATASUS FTP
    2. Convert DBC to DBF (decompression)
    3. Stream DBF to in-memory DuckDB staging tables
    4. Apply SQL transformations
    5. Export to persistent DuckDB database with enrichment VIEW

    Example:
        ```python
        from datasus_etl.config import PipelineConfig, DownloadConfig, ConversionConfig
        from datasus_etl.pipeline import SihsusPipeline

        config = PipelineConfig(
            download=DownloadConfig(...),
            conversion=ConversionConfig(...),
            processing=ProcessingConfig(...),
            storage=StorageConfig(...),
            database=DatabaseConfig(...),
        )

        pipeline = SihsusPipeline(config)
        result = pipeline.run()
        ```
    """

    def __init__(self, config: PipelineConfig, ibge_data_path: Path = None) -> None:
        """Initialize the pipeline.

        Args:
            config: Complete pipeline configuration
            ibge_data_path: Optional path to IBGE data file
        """
        super().__init__(config)
        self.ibge_data_path = ibge_data_path

    def setup_stages(self) -> None:
        """Set up optimized pipeline stages.

        Pipeline stages:
        1. Download DBC files from DATASUS FTP
        2. Decompress DBC → DBF (using TABWIN)
        3. Stream DBF → DuckDB in-memory staging tables
        4. [Optional] Export raw CSV (if configured)
        5. Transform in SQL and export to persistent DuckDB database
        6. [Optional] Export cleaned CSV (if configured)

        This replaces the old multi-stage pipeline:
        - Removed: DbfToCsvStage (replaced by DbfToDbStage)
        - Removed: ProcessStage (replaced by SqlTransformStage)
        - Removed: EnrichStage (merged into SqlTransformStage)
        - All data exported to persistent DuckDB with enrichment VIEW
        """
        # Add stages in order
        self.add_stage(DownloadStage(self.config))
        self.add_stage(DbcToDbfStage(self.config))
        self.add_stage(DbfToDbStage(self.config))

        # Optional: Export raw CSV (before transformations)
        self.add_stage(RawCsvExportStage(self.config))

        self.add_stage(SqlTransformStage(self.config, self.ibge_data_path))

        # Optional: Export cleaned CSV (after transformations)
        self.add_stage(CleanedCsvExportStage(self.config))

        self.logger.info(f"Optimized pipeline configured with {len(self._stages)} stages")

    def _generate_completion_report(self, context: PipelineContext) -> None:
        """Generate and log pipeline completion statistics.

        Args:
            context: Pipeline context with metadata
        """
        self.logger.info("=" * 70)
        self.logger.info("PIPELINE COMPLETION REPORT")
        self.logger.info("=" * 70)

        # 1. Files processed
        staging_tables = context.get("staging_tables", [])
        dbf_files = context.get("dbf_files", [])
        num_files = len(dbf_files) or len(staging_tables)

        self.logger.info(f"{SYM_FILE} Files Processed: {num_files} DBC {SYM_ARROW} DuckDB")

        # 2. Total rows (from context metadata)
        total_rows = context.get_metadata("total_rows_in_db", 0) or context.get_metadata("total_rows_exported", 0)
        if total_rows:
            self.logger.info(f"{SYM_CHART} Total Rows: {total_rows:,}")

        # 3. Output database path and size
        db_path = context.get("database_path")
        if db_path:
            db_path = Path(db_path)
            if db_path.exists():
                db_size_bytes = db_path.stat().st_size
                if humanize:
                    size_human = humanize.naturalsize(db_size_bytes, binary=True)
                else:
                    size_human = f"{db_size_bytes / (1024**3):.2f} GB"

                self.logger.info(f"Database: {db_path}")
                self.logger.info(f"Database Size: {size_human}")

        # 4. Processing duration
        start_time = context.get_metadata("start_time")
        end_time = datetime.now()

        if start_time:
            duration = end_time - start_time
            if humanize:
                duration_human = humanize.naturaldelta(duration)
            else:
                duration_human = str(duration).split('.')[0]  # Remove microseconds
            self.logger.info(f"Duration: {duration_human}")

        # 5. CSV exports (if enabled)
        if self.config.storage.export_raw_csv:
            raw_csv_files = context.get("raw_csv_files", [])
            if raw_csv_files:
                self.logger.info(f"Raw CSV Files: {len(raw_csv_files)} exported")

        if self.config.storage.export_cleaned_csv:
            cleaned_csv = context.get("cleaned_csv_file")
            if cleaned_csv and cleaned_csv.exists():
                csv_size_bytes = cleaned_csv.stat().st_size
                if humanize:
                    csv_size = humanize.naturalsize(csv_size_bytes, binary=True)
                else:
                    csv_size = f"{csv_size_bytes / (1024**3):.2f} GB"
                self.logger.info(f"Cleaned CSV: {csv_size}")

        # 6. Errors (if any)
        if context.has_errors:
            self.logger.warning(f"Errors: {len(context.errors)} encountered")
            for error in context.errors[:5]:  # Show first 5 errors
                self.logger.warning(f"   - {error}")

        self.logger.info("=" * 70)

    def _cleanup_temporary_files(self, success: bool = True) -> None:
        """Delete temporary DBC and DBF files and directories after successful DuckDB export.

        Args:
            success: Whether the pipeline completed successfully
        """
        if self.config.keep_temp_files:
            self.logger.info("Keeping temporary files (--keep-temp-files enabled)")
            return

        if not success:
            self.logger.info("Skipping cleanup due to pipeline failure")
            return

        deleted_dbc = 0
        deleted_dbf = 0
        deleted_dirs = 0

        # Delete DBF files (recursively, as files may be in UF subdirectories)
        dbf_dir = self.config.conversion.dbf_dir
        if dbf_dir.exists():
            for dbf_file in dbf_dir.rglob("*.dbf"):
                try:
                    dbf_file.unlink()
                    deleted_dbf += 1
                except Exception as e:
                    self.logger.warning(f"Failed to delete {dbf_file}: {e}")

            # Delete dbf directory and subdirectories if empty
            deleted_dirs += self._remove_empty_dirs(dbf_dir)

        # Delete DBC files (recursively, as files may be in UF subdirectories)
        dbc_dir = self.config.download.output_dir
        if dbc_dir.exists():
            for dbc_file in dbc_dir.rglob("*.dbc"):
                try:
                    dbc_file.unlink()
                    deleted_dbc += 1
                except Exception as e:
                    self.logger.warning(f"Failed to delete {dbc_file}: {e}")

            # Delete dbc directory and subdirectories if empty
            deleted_dirs += self._remove_empty_dirs(dbc_dir)

        if deleted_dbc > 0 or deleted_dbf > 0 or deleted_dirs > 0:
            self.logger.info(
                f"Cleanup: deleted {deleted_dbc} DBC files, {deleted_dbf} DBF files, "
                f"and {deleted_dirs} empty directories"
            )

    def _remove_empty_dirs(self, root_dir: Path) -> int:
        """Remove empty directories recursively, starting from deepest level.

        Args:
            root_dir: Root directory to clean up

        Returns:
            Number of directories removed
        """
        deleted_count = 0

        if not root_dir.exists():
            return 0

        # Get all subdirectories, sorted by depth (deepest first)
        all_dirs = sorted(
            [d for d in root_dir.rglob("*") if d.is_dir()],
            key=lambda p: len(p.parts),
            reverse=True,
        )

        # Delete empty subdirectories (deepest first)
        for dir_path in all_dirs:
            try:
                if dir_path.exists() and not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    deleted_count += 1
                    self.logger.debug(f"Removed empty directory: {dir_path}")
            except Exception as e:
                self.logger.warning(f"Failed to delete directory {dir_path}: {e}")

        # Finally, try to delete the root directory itself if empty
        try:
            if root_dir.exists() and not any(root_dir.iterdir()):
                root_dir.rmdir()
                deleted_count += 1
                self.logger.debug(f"Removed empty directory: {root_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to delete directory {root_dir}: {e}")

        return deleted_count

    def run(self) -> "PipelineContext":
        """Run the pipeline and cleanup resources.

        Returns:
            Pipeline context with results

        Raises:
            PyInmetError: If pipeline execution fails
        """
        # Track start time
        start_time = datetime.now()
        self.context.set_metadata("start_time", start_time)

        success = False
        try:
            # Run parent implementation
            result = super().run()

            # Generate completion report
            self._generate_completion_report(result)

            success = True
            return result
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            # Always cleanup DuckDB connection
            db_manager = self.context.get("db_manager")
            if db_manager is not None:
                try:
                    db_manager.__exit__(None, None, None)
                    self.logger.debug("DuckDB connection closed")
                except Exception as e:
                    self.logger.warning(f"Error closing DuckDB connection: {e}")

            # Cleanup temporary files (only on success)
            self._cleanup_temporary_files(success=success)
