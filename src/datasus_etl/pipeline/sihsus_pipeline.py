"""Complete SIHSUS data processing pipeline."""

import gc
import logging
import os
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
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
from datasus_etl.transform.converters.dbf_to_parquet import (
    DbfToParquetConverter,
    convert_dbf_to_parquet,
    ParquetConversionResult,
)
from datasus_etl.storage.sql_transformer import SQLTransformer, SUBSYSTEM_SCHEMAS
from datasus_etl.storage.duckdb_manager import DuckDBManager
from datasus_etl.storage.parquet_manager import ParquetManager
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
        from tqdm import tqdm

        # Get or create DuckDB manager (shared across stages)
        # Use the final database directly for staging (prevents RAM exhaustion)
        db_manager = context.get("db_manager")
        if db_manager is None:
            # Open connection directly to the final database file
            db_path = self.config.get_database_path()
            db_path.parent.mkdir(parents=True, exist_ok=True)

            from datasus_etl.config import DatabaseConfig
            db_config = DatabaseConfig(db_path=db_path)
            db_manager = DuckDBManager(db_config)
            db_manager.connect()

            context.set("db_manager", db_manager)
            context.set("database_path", str(db_path))
            tqdm.write(f"\n[DATABASE] Using final database directly: {db_path}")

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
        """Transform staging tables using SQL and insert into raw table.

        Since DbfToDbStage now uses the final database directly, both staging
        tables and raw table are in the same database. No Parquet transfer needed.
        """
        from datasus_etl.storage.dimension_loader import DimensionLoader

        # Get shared DuckDB manager from context (already connected to final database)
        db_manager = context.get("db_manager")
        if db_manager is None:
            raise ValueError("DuckDB manager not found in context. DBF stage must run first.")

        conn = db_manager._conn

        # Get list of staging tables
        staging_tables = context.get("staging_tables", [])
        if not staging_tables:
            self.logger.warning("No staging tables found")
            return context

        tqdm.write(f"\n[TRANSFORM] Transforming {len(staging_tables)} tables {SYM_ARROW} raw table...")

        # Get the database path
        db_path = self.config.get_database_path()

        # Get schema for SIHSUS
        from datasus_etl.storage.sql_transformer import SUBSYSTEM_SCHEMAS
        schema = SUBSYSTEM_SCHEMAS.get("sihsus", SUBSYSTEM_SCHEMAS["sihsus"])

        # Initialize raw table and dimensions (same database as staging)
        db_manager.initialize_raw_table("sihsus", schema)
        db_manager.initialize_dimension_tables()

        # Load IBGE dimension data
        dim_loader = DimensionLoader(conn)
        try:
            dim_loader.load_ibge_municipios(replace=False)
            tqdm.write(f"[OK] Dados IBGE carregados na tabela dim_municipios")
        except Exception as e:
            self.logger.warning(f"Failed to load IBGE data: {e}")

        total_rows_inserted = 0
        raw_table = "sihsus_raw"

        # Create transformer (same connection for staging and raw)
        transformer = SQLTransformer(
            conn,
            subsystem="sihsus",
            raw_mode=self.config.raw_mode,
        )

        # Process each staging table
        for i, staging_table in enumerate(staging_tables, 1):
            original_name = staging_table.replace("staging_", "")
            tqdm.write(f"\n[{i}/{len(staging_tables)}] Transforming {original_name}...")

            try:
                # Transform staging table (creates a view)
                view_name = f"canonical_{original_name}"
                transformer.transform_to_canonical_view(
                    source_table=staging_table,
                    target_view=view_name,
                    enable_ibge_enrichment=False,
                )

                # Get row count
                row_count = conn.execute(
                    f"SELECT COUNT(*) FROM {view_name}"
                ).fetchone()[0]

                if row_count == 0:
                    self.logger.debug(f"Skipping empty view: {view_name}")
                    # Cleanup empty staging table and view
                    conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                    conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
                    continue

                # Check if source_file already exists (deduplication)
                source_file_value = conn.execute(
                    f"SELECT DISTINCT source_file FROM {view_name} LIMIT 1"
                ).fetchone()

                if source_file_value:
                    source_file = source_file_value[0]
                    # Use EXISTS instead of COUNT (faster - stops at first match)
                    existing = conn.execute(
                        f"SELECT EXISTS(SELECT 1 FROM {raw_table} WHERE source_file = ? LIMIT 1)",
                        [source_file]
                    ).fetchone()[0]

                    if existing:
                        tqdm.write(f"[SKIP] {original_name} ja processado")
                        # Cleanup staging table and view
                        conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                        conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
                        continue

                # Insert directly from view to raw table (same database, no Parquet needed)
                conn.execute(f"INSERT INTO {raw_table} SELECT * FROM {view_name}")

                total_rows_inserted += row_count
                tqdm.write(f"[OK] Inserted {original_name}: {row_count:,} rows")

                # Cleanup staging view and table to free disk space
                conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                conn.execute(f"DROP TABLE IF EXISTS {staging_table}")

                # Periodic CHECKPOINT and GC to free memory and disk space
                if i % 20 == 0:
                    conn.execute("CHECKPOINT")
                    gc.collect()
                    self.logger.debug(f"Checkpoint and GC after {i} tables")

            except Exception as e:
                tqdm.write(f"[ERROR] Failed: {original_name}: {e}")
                self.logger.error(f"Failed to transform/insert {staging_table}: {e}")
                raise

        # Create enrichment VIEW
        db_manager.create_enrichment_view("sihsus")

        # Get final stats
        total_rows = conn.execute(
            f"SELECT COUNT(*) FROM {raw_table}"
        ).fetchone()[0]

        # Get database file size
        db_size_bytes = db_path.stat().st_size
        db_size_mb = db_size_bytes / (1024 * 1024)

        context.set_metadata("total_rows_exported", total_rows_inserted)
        context.set_metadata("total_rows_in_db", total_rows)
        context.set("database_path", str(db_path))
        context.set("database_size_mb", db_size_mb)

        tqdm.write(f"\n[DONE] Transform complete:")
        tqdm.write(f"  - Inserted: {total_rows_inserted:,} rows")
        tqdm.write(f"  - Total in DB: {total_rows:,} rows")
        tqdm.write(f"  - Database: {db_path.name} ({db_size_mb:.1f} MB)\n")

        return context


class DbfToParquetStage(Stage):
    """Stage for converting DBF files to partitioned Parquet format.

    Converts DBF files to Hive-style partitioned Parquet files:
    - Structure: {base_dir}/parquet/{subsystem}/uf={UF}/{filename}.parquet
    - Uses ProcessPoolExecutor for true parallel processing
    - Each worker has its own DuckDB in-memory connection
    - Tracks processed files via manifest to enable incremental updates
    """

    def __init__(self, config: PipelineConfig, subsystem: str = "sihsus") -> None:
        super().__init__("Convert DBF to Parquet")
        self.config = config
        self.subsystem = subsystem

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Convert DBF files to Parquet with parallel processing."""
        from datasus_etl.datasets import DatasetRegistry

        # Get dataset configuration
        dataset_config = DatasetRegistry.get(self.subsystem)
        file_prefix = dataset_config.FILE_PREFIX.upper() if dataset_config else ""
        schema = SUBSYSTEM_SCHEMAS.get(self.subsystem, SUBSYSTEM_SCHEMAS["sihsus"])

        # Find DBF files filtered by subsystem prefix
        all_dbf_files = list(self.config.conversion.dbf_dir.rglob("*.dbf"))

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

        # Initialize Parquet manager for manifest tracking
        parquet_manager = ParquetManager(
            self.config.storage.database_dir,
            self.subsystem,
        )

        # Filter out already processed files
        processed_files = parquet_manager.get_processed_files()
        pending_files = [
            f for f in dbf_files
            if f"{f.stem.upper()}.dbc" not in processed_files
        ]

        if not pending_files:
            tqdm.write(f"\n[OK] All {len(dbf_files)} DBF files already processed")
            context.set("parquet_results", [])
            context.set_metadata("files_converted", 0)
            return context

        tqdm.write(f"\nConverting {len(pending_files)} DBF files to Parquet "
                   f"(skipping {len(processed_files)} already processed)")

        # Setup output directory
        output_dir = self.config.get_parquet_dir()
        output_dir.mkdir(parents=True, exist_ok=True)

        # Configure parallel processing
        num_workers = min(self.config.database.num_workers, len(pending_files))
        compression = self.config.storage.parquet_compression
        raw_mode = self.config.raw_mode

        tqdm.write(f"Using {num_workers} parallel workers, compression={compression}")

        # Process files in parallel using ProcessPoolExecutor
        results: list[ParquetConversionResult] = []
        total_rows = 0
        errors = 0

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(
                    convert_dbf_to_parquet,
                    dbf_file,
                    output_dir,
                    self.subsystem,
                    schema,
                    raw_mode,
                    compression,
                ): dbf_file
                for dbf_file in pending_files
            }

            # Process completed tasks with progress bar
            for future in tqdm(
                as_completed(future_to_file),
                total=len(pending_files),
                desc=f"DBF{SYM_ARROW}Parquet",
            ):
                dbf_file = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)

                    if result.success:
                        total_rows += result.rows
                        parquet_manager.mark_processed(result.source_file)
                        tqdm.write(f"[OK] {result.source_file}: {result.rows:,} rows -> uf={result.uf}")
                    else:
                        errors += 1
                        tqdm.write(f"[ERROR] {result.source_file}: {result.error}")

                except Exception as e:
                    errors += 1
                    self.logger.error(f"Failed to convert {dbf_file.name}: {e}")
                    tqdm.write(f"[ERROR] {dbf_file.name}: {e}")

        # Store results in context
        context.set("parquet_results", results)
        context.set_metadata("files_converted", len(results) - errors)
        context.set_metadata("total_rows_converted", total_rows)
        context.set_metadata("conversion_errors", errors)

        # Get storage stats
        stats = parquet_manager.get_storage_stats()
        context.set("parquet_stats", stats)

        tqdm.write(f"\n[DONE] Converted {len(results) - errors} files, "
                   f"{total_rows:,} total rows, {errors} errors")
        tqdm.write(f"Storage: {stats.total_files} Parquet files, "
                   f"{stats.total_size_bytes / (1024*1024):.1f} MB, "
                   f"{len(stats.partitions)} UF partitions\n")

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

        Pipeline stages depend on output_format configuration:

        For Parquet mode (default):
        1. Download DBC files from DATASUS FTP
        2. Decompress DBC → DBF
        3. Convert DBF → Parquet (partitioned by UF)

        For DuckDB mode (legacy):
        1. Download DBC files from DATASUS FTP
        2. Decompress DBC → DBF
        3. Stream DBF → DuckDB in-memory staging tables
        4. [Optional] Export raw CSV (if configured)
        5. Transform in SQL and export to persistent DuckDB database
        6. [Optional] Export cleaned CSV (if configured)
        """
        # Add common stages
        self.add_stage(DownloadStage(self.config))
        self.add_stage(DbcToDbfStage(self.config))

        # Choose storage-specific stages based on output_format
        if self.config.is_parquet_mode():
            # Parquet mode: convert directly to partitioned Parquet files
            self.add_stage(DbfToParquetStage(self.config, subsystem="sihsus"))
            self.logger.info("Pipeline configured for Parquet output (Hive-partitioned)")
        else:
            # DuckDB mode: legacy flow with staging tables
            self.add_stage(DbfToDbStage(self.config))

            # Optional: Export raw CSV (before transformations)
            self.add_stage(RawCsvExportStage(self.config))

            self.add_stage(SqlTransformStage(self.config, self.ibge_data_path))

            # Optional: Export cleaned CSV (after transformations)
            self.add_stage(CleanedCsvExportStage(self.config))
            self.logger.info("Pipeline configured for DuckDB output (single file)")

        self.logger.info(f"Pipeline configured with {len(self._stages)} stages")

    def _generate_completion_report(self, context: PipelineContext) -> None:
        """Generate and log pipeline completion statistics.

        Args:
            context: Pipeline context with metadata
        """
        self.logger.info("=" * 70)
        self.logger.info("PIPELINE COMPLETION REPORT")
        self.logger.info("=" * 70)

        # Check if Parquet mode
        is_parquet = self.config.is_parquet_mode()

        # 1. Files processed
        if is_parquet:
            parquet_results = context.get("parquet_results", [])
            num_files = context.get_metadata("files_converted", len(parquet_results))
            self.logger.info(f"{SYM_FILE} Files Processed: {num_files} DBC {SYM_ARROW} Parquet")
        else:
            staging_tables = context.get("staging_tables", [])
            dbf_files = context.get("dbf_files", [])
            num_files = len(dbf_files) or len(staging_tables)
            self.logger.info(f"{SYM_FILE} Files Processed: {num_files} DBC {SYM_ARROW} DuckDB")

        # 2. Total rows (from context metadata)
        if is_parquet:
            total_rows = context.get_metadata("total_rows_converted", 0)
        else:
            total_rows = context.get_metadata("total_rows_in_db", 0) or context.get_metadata("total_rows_exported", 0)
        if total_rows:
            self.logger.info(f"{SYM_CHART} Total Rows: {total_rows:,}")

        # 3. Output path and size
        if is_parquet:
            parquet_stats = context.get("parquet_stats")
            if parquet_stats:
                parquet_dir = self.config.get_parquet_dir()
                if humanize:
                    size_human = humanize.naturalsize(parquet_stats.total_size_bytes, binary=True)
                else:
                    size_human = f"{parquet_stats.total_size_bytes / (1024**3):.2f} GB"

                self.logger.info(f"Parquet Directory: {parquet_dir}")
                self.logger.info(f"Storage Size: {size_human}")
                self.logger.info(f"Partitions: {len(parquet_stats.partitions)} UFs ({', '.join(parquet_stats.partitions[:10])}{'...' if len(parquet_stats.partitions) > 10 else ''})")
        else:
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
