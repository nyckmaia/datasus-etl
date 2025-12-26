"""Complete SIHSUS data processing pipeline."""

import logging
from pathlib import Path

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
            chunk_size=self.config.database.chunk_size
        )

        # Find all DBF files
        dbf_files = list(self.config.conversion.dbf_dir.rglob("*.dbf"))
        if not dbf_files:
            self.logger.warning(f"No DBF files found in {self.config.conversion.dbf_dir}")
            return context

        # Use multi-threading for I/O-bound DBF loading (max 4 workers)
        max_workers = min(4, multiprocessing.cpu_count())
        total_rows = 0
        staging_tables = []

        self.logger.info(f"Streaming {len(dbf_files)} DBF files using {max_workers} threads")

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
                except Exception as e:
                    self.logger.error(f"Failed to stream {dbf_file.name}: {e}")
                    continue

        context.set("staging_tables", staging_tables)
        context.set_metadata("total_rows_loaded", total_rows)

        self.logger.info(f"Streamed {total_rows:,} rows from {len(dbf_files)} DBF files")

        return context


class SqlTransformStage(Stage):
    """Stage for SQL-based data transformation and Parquet export."""

    def __init__(self, config: PipelineConfig, ibge_data_path: Path = None) -> None:
        super().__init__("Transform Data (SQL) and Export Parquet")
        self.config = config
        self.ibge_data_path = ibge_data_path

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Apply all transformations using SQL and export to Parquet."""
        # Get shared DuckDB manager from context
        db_manager = context.get("db_manager")
        if db_manager is None:
            raise ValueError("DuckDB manager not found in context. DBF stage must run first.")

        transformer = SQLTransformer(db_manager._conn)

        # Get list of staging tables
        staging_tables = context.get("staging_tables", [])
        if not staging_tables:
            self.logger.warning("No staging tables found")
            return context

        # Create UNION of all staging tables
        union_query = " UNION ALL ".join([
            f"SELECT * FROM {table}"
            for table in staging_tables
        ])

        db_manager._conn.execute(
            f"""
            CREATE OR REPLACE VIEW unified_staging AS
            {union_query}
        """
        )

        self.logger.info(f"Unified {len(staging_tables)} staging tables")

        # Apply all transformations
        transformer.transform_sihsus_data(
            source_table="unified_staging",
            target_view="sihsus_processed",
            ibge_data_path=self.ibge_data_path
        )

        # Export directly to Parquet (zero-copy)
        parquet_dir = self.config.storage.parquet_dir
        parquet_dir.mkdir(parents=True, exist_ok=True)

        # Export with partitioning and compression
        export_path = parquet_dir / "data.parquet"
        self.logger.info("Exporting to Parquet...")

        # Build partition columns from config (respect user's choice)
        partition_cols = self.config.storage.partition_cols
        # Filter only columns that exist in transformed view
        valid_partition_cols = [
            col.lower() for col in partition_cols
            if col.upper() in ['ANO_INTER', 'MES_INTER', 'UF_ZI', 'MUNIC_RES']
        ]
        partition_cols_sql = ', '.join(valid_partition_cols) if valid_partition_cols else 'ano_inter'
        self.logger.info(f"Partitioning Parquet by: {partition_cols_sql}")

        db_manager._conn.execute(
            f"""
            COPY (SELECT * FROM sihsus_processed)
            TO '{export_path}' (
                FORMAT PARQUET,
                PARTITION_BY ({partition_cols_sql}),
                COMPRESSION '{self.config.storage.compression}',
                ROW_GROUP_SIZE {self.config.storage.row_group_size},
                OVERWRITE_OR_IGNORE true
            )
        """
        )

        # Get row count AFTER export (faster - reads Parquet metadata)
        count_result = db_manager._conn.execute(
            f"""
            SELECT COUNT(*) as cnt
            FROM read_parquet('{parquet_dir}/**/*.parquet', hive_partitioning=true)
            """
        ).fetchone()
        total_rows = count_result[0] if count_result else 0

        context.set("parquet_path", parquet_dir)
        context.set_metadata("total_rows_exported", total_rows)

        self.logger.info(f"Exported {total_rows:,} rows to {parquet_dir}")

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
        4. Transform in SQL and export to Parquet (new, combines processing/enrichment/export)

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
        self.add_stage(SqlTransformStage(self.config, self.ibge_data_path))

        self.logger.info(f"Optimized pipeline configured with {len(self._stages)} stages")

    def run(self) -> "PipelineContext":
        """Run the pipeline and cleanup resources.

        Returns:
            Pipeline context with results

        Raises:
            PyInmetError: If pipeline execution fails
        """
        try:
            # Run parent implementation
            result = super().run()
            return result
        finally:
            # Always cleanup DuckDB connection
            db_manager = self.context.get("db_manager")
            if db_manager is not None:
                try:
                    db_manager.__exit__(None, None, None)
                    self.logger.debug("DuckDB connection closed")
                except Exception as e:
                    self.logger.warning(f"Error closing DuckDB connection: {e}")
