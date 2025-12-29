"""Complete SIHSUS data processing pipeline."""

import logging
from pathlib import Path
from datetime import datetime

from pydatasus.config import PipelineConfig
from pydatasus.core.pipeline import Pipeline
from pydatasus.core.stage import Stage
from pydatasus.core.context import PipelineContext
from pydatasus.download.ftp_downloader import FTPDownloader
from pydatasus.transform.converters.dbc_to_dbf import DbcToDbfConverter
from pydatasus.transform.converters.dbf_to_duckdb import DbfToDuckDBConverter
from pydatasus.storage.sql_transformer import SQLTransformer
from pydatasus.storage.duckdb_manager import DuckDBManager
from tqdm import tqdm

try:
    import humanize
except ImportError:
    humanize = None


class DownloadStage(Stage):
    """Stage for downloading DBC files from DATASUS FTP."""

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Download SIHSUS Data")
        self.config = config

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute download stage."""
        downloader = FTPDownloader(self.config.download)
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

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Stream DBF to DuckDB")
        self.config = config

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

        # Find all DBF files
        dbf_files = list(self.config.conversion.dbf_dir.rglob("*.dbf"))
        if not dbf_files:
            self.logger.warning(f"No DBF files found in {self.config.conversion.dbf_dir}")
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
            for future in tqdm(as_completed(future_to_file), total=len(dbf_files), desc="Streaming DBF→DuckDB"):
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
    """Stage for SQL-based data transformation and Parquet export.

    Supports two export modes:
    1. Individual files (legacy): One Parquet file per DBF
    2. Partitioned (default): Hive-partitioned directory with file size control

    Partitioned mode uses:
    - PARTITION_BY (uf): Creates uf=SP/, uf=RJ/ structure
    - FILE_SIZE_BYTES: Controls max file size (default 512MB)
    - APPEND: Accumulates data from multiple DBFs
    - Canonical schema: All columns from SIHSUS_PARQUET_SCHEMA
    """

    def __init__(self, config: PipelineConfig, ibge_data_path: Path = None) -> None:
        super().__init__("Transform Data (SQL) and Export Parquet")
        self.config = config
        self.ibge_data_path = ibge_data_path
        # Use config option, defaults to True (partitioned export)
        self.use_partitioned_export = config.storage.use_partitioned_export

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Transform staging tables using SQL and export to Parquet.

        When use_partitioned_export=True (default):
        - Uses canonical schema (all columns from SIHSUS_PARQUET_SCHEMA)
        - Exports to Hive-partitioned directory structure
        - Uses APPEND to accumulate data from multiple DBFs
        - Controls file size with FILE_SIZE_BYTES

        When use_partitioned_export=False:
        - Legacy mode: one Parquet file per DBF
        """
        if self.use_partitioned_export:
            return self._execute_partitioned(context)
        else:
            return self._execute_individual(context)

    def _execute_partitioned(self, context: PipelineContext) -> PipelineContext:
        """Transform and export to Hive-partitioned Parquet with canonical schema."""
        from tqdm import tqdm

        # Get shared DuckDB manager from context
        db_manager = context.get("db_manager")
        if db_manager is None:
            raise ValueError("DuckDB manager not found in context. DBF stage must run first.")

        transformer = SQLTransformer(
            db_manager._conn,
            subsystem="sihsus",
            raw_mode=self.config.raw_mode,
        )

        # Get list of staging tables
        staging_tables = context.get("staging_tables", [])
        if not staging_tables:
            self.logger.warning("No staging tables found")
            return context

        tqdm.write(f"\n[TRANSFORM] Transforming {len(staging_tables)} tables with canonical schema...")
        tqdm.write(f"[PARTITION] Exporting to Hive-partitioned Parquet (partition_by={self.config.storage.partition_cols})")

        # Ensure output directory exists
        parquet_dir = self.config.storage.parquet_dir
        parquet_dir.mkdir(parents=True, exist_ok=True)

        # Build partition columns string
        partition_cols = ", ".join(self.config.storage.partition_cols)

        # Get file size limit from config (in bytes, convert to human-readable)
        max_file_size_bytes = self.config.storage.max_file_size
        max_file_size_mb = max_file_size_bytes // (1024 * 1024)

        total_rows_exported = 0
        is_first_export = True

        # Process each staging table
        for i, staging_table in enumerate(staging_tables, 1):
            original_name = staging_table.replace("staging_", "")
            tqdm.write(f"\n[{i}/{len(staging_tables)}] Transforming {original_name} with canonical schema...")

            try:
                # Transform using canonical schema (all SIHSUS columns)
                view_name = f"canonical_{original_name}"
                transformer.transform_to_canonical_view(
                    source_table=staging_table,
                    target_view=view_name,
                    ibge_data_path=self.ibge_data_path
                )

                # Get row count before export
                row_count = db_manager._conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]

                # Determine if we should use APPEND or OVERWRITE
                # First export: use OVERWRITE_OR_IGNORE to clean any existing data
                # Subsequent exports: use APPEND to add to existing partitions
                if is_first_export:
                    append_mode = "OVERWRITE_OR_IGNORE"
                    is_first_export = False
                else:
                    append_mode = "APPEND"

                # Export with partitioning (FILE_SIZE_BYTES not compatible with PARTITION_BY in DuckDB)
                export_sql = f"""
                    COPY (
                        SELECT * FROM {view_name}
                        ORDER BY dt_inter ASC NULLS LAST
                    )
                    TO '{parquet_dir}' (
                        FORMAT PARQUET,
                        PARTITION_BY ({partition_cols}),
                        {append_mode},
                        COMPRESSION '{self.config.storage.compression}',
                        ROW_GROUP_SIZE {self.config.storage.row_group_size}
                    )
                """
                db_manager._conn.execute(export_sql)

                total_rows_exported += row_count
                tqdm.write(f"[OK] Exported {original_name}: {row_count:,} rows (partitioned by {partition_cols})")

                # Cleanup: drop the view to free memory
                db_manager._conn.execute(f"DROP VIEW IF EXISTS {view_name}")

            except Exception as e:
                tqdm.write(f"[ERROR] Failed to transform/export {original_name}: {e}")
                self.logger.error(f"Failed to transform/export {staging_table}: {e}")
                raise

        # Calculate total size of all Parquet files
        total_size_bytes = sum(f.stat().st_size for f in parquet_dir.rglob("*.parquet"))
        total_size_mb = total_size_bytes / (1024 * 1024)

        # Count partition directories and files
        partition_dirs = list(parquet_dir.glob("uf=*"))
        parquet_files = list(parquet_dir.rglob("*.parquet"))

        context.set_metadata("total_rows_exported", total_rows_exported)
        context.set("parquet_dir", str(parquet_dir))
        context.set("partition_count", len(partition_dirs))
        context.set("file_count", len(parquet_files))
        context.set("exported_parquet_files", [str(f) for f in parquet_files])

        tqdm.write(f"\n[DONE] Partitioned export complete:")
        tqdm.write(f"  - Total rows: {total_rows_exported:,}")
        tqdm.write(f"  - Partitions: {len(partition_dirs)} (by {partition_cols})")
        tqdm.write(f"  - Files: {len(parquet_files)} ({total_size_mb:.1f}MB total)\n")

        return context

    def _execute_individual(self, context: PipelineContext) -> PipelineContext:
        """Legacy mode: Transform and export one Parquet file per DBF."""
        from tqdm import tqdm

        # Get shared DuckDB manager from context
        db_manager = context.get("db_manager")
        if db_manager is None:
            raise ValueError("DuckDB manager not found in context. DBF stage must run first.")

        transformer = SQLTransformer(
            db_manager._conn,
            subsystem="sihsus",
            raw_mode=self.config.raw_mode,
        )

        # Get list of staging tables
        staging_tables = context.get("staging_tables", [])
        if not staging_tables:
            self.logger.warning("No staging tables found")
            return context

        tqdm.write(f"\n[TRANSFORM] Transforming {len(staging_tables)} tables with SQL (individual files)...")

        # Ensure output directory exists
        parquet_dir = self.config.storage.parquet_dir
        parquet_dir.mkdir(parents=True, exist_ok=True)

        total_rows_exported = 0
        exported_files = []

        # Process each staging table individually
        for i, staging_table in enumerate(staging_tables, 1):
            # Extract original filename from staging table name (staging_RDSP2301 → RDSP2301)
            original_name = staging_table.replace("staging_", "")

            tqdm.write(f"\n[{i}/{len(staging_tables)}] Transforming {original_name}...")

            try:
                # Transform staging table (clean column names already applied at DBF load)
                view_name = f"transformed_{original_name}"
                transformer.transform_sihsus_data(
                    source_table=staging_table,
                    target_view=view_name
                )

                # Export transformed view to Parquet
                parquet_file = parquet_dir / f"{original_name}.parquet"

                # Check if dt_inter exists for sorting (now lowercase due to column cleaning)
                columns = db_manager._conn.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{view_name}'"
                ).fetchall()
                column_names = [col[0] for col in columns]

                # Build ORDER BY clause if dt_inter exists (lowercase)
                order_by = "ORDER BY dt_inter ASC NULLS LAST" if 'dt_inter' in column_names else ""

                # Export with transformations applied
                db_manager._conn.execute(
                    f"""
                    COPY (
                        SELECT * FROM {view_name}
                        {order_by}
                    )
                    TO '{parquet_file}' (
                        FORMAT PARQUET,
                        COMPRESSION '{self.config.storage.compression}',
                        ROW_GROUP_SIZE {self.config.storage.row_group_size}
                    )
                """
                )

                # Get row count
                row_count = db_manager._conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
                total_rows_exported += row_count

                file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
                tqdm.write(f"[OK] Exported {original_name}.parquet: {row_count:,} rows ({file_size_mb:.1f}MB)")

                exported_files.append(str(parquet_file))

            except Exception as e:
                tqdm.write(f"[ERROR] Failed to transform/export {original_name}: {e}")
                self.logger.error(f"Failed to transform/export {staging_table}: {e}")
                raise  # Stop on first error to see the issue

        context.set_metadata("total_rows_exported", total_rows_exported)
        context.set("exported_parquet_files", exported_files)

        tqdm.write(f"\n[DONE] Exported {len(exported_files)} Parquet files ({total_rows_exported:,} total rows)\n")

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
            self.config.storage.parquet_dir.parent / "csv_raw"
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

            self.logger.info(f"Exported {table} → {output_file.name}")

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
            self.config.storage.parquet_dir.parent / "csv_cleaned"
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

        self.logger.info(f"Exported → {output_file}")

        context.set("cleaned_csv_file", output_file)
        return context


class SihsusPipeline(Pipeline[PipelineConfig]):
    """Complete SIHSUS data processing pipeline.

    Orchestrates the entire data pipeline from download to database:
    1. Download DBC files from DATASUS FTP
    2. Convert DBC to DBF (decompression)
    3. Convert DBF to CSV
    4. Process and clean CSV data
    5. Enrich with IBGE geographic data
    6. Convert to Parquet format
    7. Load into DuckDB database

    Example:
        ```python
        from pydatasus.config import PipelineConfig, DownloadConfig, ConversionConfig
        from pydatasus.pipeline import SihsusPipeline

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
        3. Stream DBF → DuckDB (new, replaces CSV step)
        4. [Optional] Export raw CSV (if configured)
        5. Transform in SQL and export to Parquet (new, combines processing/enrichment/export)
        6. [Optional] Export cleaned CSV (if configured)

        This replaces the old 7-stage pipeline:
        - Removed: DbfToCsvStage (replaced by DbfToDbStage)
        - Removed: ProcessStage (replaced by SqlTransformStage)
        - Removed: EnrichStage (merged into SqlTransformStage)
        - Removed: ParquetStage (merged into SqlTransformStage)
        - Removed: DatabaseStage (no longer needed, Parquet can be queried directly)
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

        self.logger.info(f"📄 Files Processed: {num_files} DBC → Parquet")

        # 2. Total rows (from context metadata)
        total_rows = context.get_metadata("total_rows_exported", 0)
        if total_rows:
            self.logger.info(f"📊 Total Rows: {total_rows:,}")

        # 3. Output directory and size
        parquet_dir = self.config.storage.parquet_dir

        if parquet_dir.exists():
            # Calculate total size of Parquet files
            total_size = sum(
                f.stat().st_size
                for f in parquet_dir.rglob("*.parquet")
            )

            if humanize:
                size_human = humanize.naturalsize(total_size, binary=True)
            else:
                size_human = f"{total_size / (1024**3):.2f} GB"

            self.logger.info(f"📁 Output Directory: {parquet_dir}")
            self.logger.info(f"💾 Parquet Database Size: {size_human}")

            # Count partition directories
            partition_dirs = [
                d for d in parquet_dir.rglob("*")
                if d.is_dir() and "=" in d.name
            ]
            if partition_dirs:
                self.logger.info(f"📂 Partitions: {len(partition_dirs)} directories")

        # 4. Processing duration
        start_time = context.get_metadata("start_time")
        end_time = datetime.now()

        if start_time:
            duration = end_time - start_time
            if humanize:
                duration_human = humanize.naturaldelta(duration)
            else:
                duration_human = str(duration).split('.')[0]  # Remove microseconds
            self.logger.info(f"⏱️  Duration: {duration_human}")

        # 5. CSV exports (if enabled)
        if self.config.storage.export_raw_csv:
            raw_csv_files = context.get("raw_csv_files", [])
            if raw_csv_files:
                self.logger.info(f"📝 Raw CSV Files: {len(raw_csv_files)} exported")

        if self.config.storage.export_cleaned_csv:
            cleaned_csv = context.get("cleaned_csv_file")
            if cleaned_csv and cleaned_csv.exists():
                csv_size_bytes = cleaned_csv.stat().st_size
                if humanize:
                    csv_size = humanize.naturalsize(csv_size_bytes, binary=True)
                else:
                    csv_size = f"{csv_size_bytes / (1024**3):.2f} GB"
                self.logger.info(f"📝 Cleaned CSV: {csv_size}")

        # 6. Errors (if any)
        if context.has_errors:
            self.logger.warning(f"⚠️  Errors: {len(context.errors)} encountered")
            for error in context.errors[:5]:  # Show first 5 errors
                self.logger.warning(f"   - {error}")

        self.logger.info("=" * 70)

    def _cleanup_temporary_files(self, success: bool = True) -> None:
        """Delete temporary DBC and DBF files and directories after successful Parquet export.

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
