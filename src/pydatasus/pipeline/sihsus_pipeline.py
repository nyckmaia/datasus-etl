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
        """Stream all DBF files to DuckDB staging tables."""
        db_manager = DuckDBManager(self.config.database)

        with db_manager:
            converter = DbfToDuckDBConverter(
                conn=db_manager._conn,
                chunk_size=self.config.database.chunk_size
            )

            # Find all DBF files
            dbf_files = list(self.config.conversion.dbf_dir.rglob("*.dbf"))
            if not dbf_files:
                self.logger.warning(f"No DBF files found in {self.config.conversion.dbf_dir}")
                return context

            total_rows = 0
            for dbf_file in tqdm(dbf_files, desc="Streaming DBF→DuckDB"):
                table_name = f"staging_{dbf_file.stem}"

                try:
                    rows = converter.stream_dbf_to_table(
                        dbf_path=dbf_file,
                        table_name=table_name,
                        create_table=True
                    )
                    total_rows += rows
                except Exception as e:
                    self.logger.error(f"Failed to stream {dbf_file.name}: {e}")
                    continue

            context.set("staging_tables", [f"staging_{f.stem}" for f in dbf_files])
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
        db_manager = DuckDBManager(self.config.database)

        with db_manager:
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

            # Check if we have partition columns
            # First, let's count rows to provide feedback
            count_result = db_manager._conn.execute(
                "SELECT COUNT(*) as cnt FROM sihsus_processed"
            ).fetchone()
            total_rows = count_result[0] if count_result else 0

            self.logger.info(f"Exporting {total_rows:,} rows to Parquet...")

            # Export with partitioning and compression
            export_path = parquet_dir / "data.parquet"
            db_manager._conn.execute(
                f"""
                COPY (SELECT * FROM sihsus_processed)
                TO '{export_path}' (
                    FORMAT PARQUET,
                    PARTITION_BY (ANO_INTER, UF_ZI),
                    COMPRESSION '{self.config.storage.compression}',
                    ROW_GROUP_SIZE {self.config.storage.row_group_size}
                )
            """
            )

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
