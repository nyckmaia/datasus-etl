"""Complete SIHSUS data processing pipeline."""

import logging
from pathlib import Path

from pydatasus.config import PipelineConfig
from pydatasus.core.pipeline import Pipeline
from pydatasus.core.stage import Stage
from pydatasus.core.context import PipelineContext
from pydatasus.download.ftp_downloader import FTPDownloader
from pydatasus.transform.converters.dbc_to_dbf import DbcToDbfConverter
from pydatasus.transform.converters.dbf_to_csv import DbfToCsvConverter
from pydatasus.transform.processors.sihsus_processor import SihsusProcessor
from pydatasus.transform.enrichers.ibge_enricher import IbgeEnricher
from pydatasus.storage.parquet_writer import ParquetWriter
from pydatasus.storage.duckdb_manager import DuckDBManager


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


class DbfToCsvStage(Stage):
    """Stage for converting DBF to CSV files."""

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Convert DBF to CSV")
        self.config = config

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute DBF to CSV conversion stage."""
        converter = DbfToCsvConverter(self.config.conversion)
        stats = converter.convert_directory()

        context.set("dbf_conversion_stats", stats)
        context.set_metadata("csv_converted_count", stats["converted"])

        self.logger.info(f"Converted {stats['converted']} DBF files to CSV")
        return context


class ProcessStage(Stage):
    """Stage for processing and cleaning CSV data."""

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Process and Clean Data")
        self.config = config

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute processing stage."""
        processor = SihsusProcessor(self.config.processing)
        stats = processor.process_directory()

        context.set("processing_stats", stats)
        context.set_metadata("processed_count", stats["processed"])

        self.logger.info(f"Processed {stats['processed']} CSV files")
        return context


class EnrichStage(Stage):
    """Stage for enriching data with IBGE information."""

    def __init__(self, config: PipelineConfig, ibge_data_path: Path = None) -> None:
        super().__init__("Enrich with IBGE Data")
        self.config = config
        self.ibge_data_path = ibge_data_path

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute enrichment stage."""
        enricher = IbgeEnricher(ibge_data_path=self.ibge_data_path)

        # Enrich from processed dir to a temp enriched dir
        enriched_dir = self.config.processing.output_dir.parent / "enriched"
        enriched_dir.mkdir(parents=True, exist_ok=True)

        stats = enricher.enrich_directory(
            input_dir=self.config.processing.output_dir,
            output_dir=enriched_dir,
        )

        context.set("enrichment_stats", stats)
        context.set("enriched_dir", enriched_dir)
        context.set_metadata("enriched_count", stats["enriched"])

        self.logger.info(f"Enriched {stats['enriched']} files with IBGE data")
        return context


class ParquetStage(Stage):
    """Stage for converting data to Parquet format."""

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Convert to Parquet")
        self.config = config

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute Parquet conversion stage."""
        writer = ParquetWriter(self.config.storage)

        # Use enriched dir if available, otherwise processed dir
        input_dir = context.get("enriched_dir", self.config.processing.output_dir)

        stats = writer.convert_directory(input_dir)

        context.set("parquet_stats", stats)
        context.set_metadata("parquet_count", stats["converted"])

        self.logger.info(f"Converted {stats['converted']} files to Parquet")
        return context


class DatabaseStage(Stage):
    """Stage for loading data into DuckDB."""

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Load into DuckDB")
        self.config = config

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute database loading stage."""
        db_manager = DuckDBManager(self.config.database)

        with db_manager:
            # Register all parquet directories as tables
            parquet_dirs = list(self.config.storage.parquet_dir.glob("*"))
            parquet_tables = [d for d in parquet_dirs if d.is_dir()]

            table_count = 0
            for table_dir in parquet_tables:
                table_name = table_dir.name
                db_manager.register_parquet(table_dir, table_name)
                table_count += 1

            context.set("database_tables", table_count)
            context.set_metadata("tables_loaded", table_count)

            self.logger.info(f"Loaded {table_count} tables into DuckDB")

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
        """Set up all pipeline stages."""
        # Add stages in order
        self.add_stage(DownloadStage(self.config))
        self.add_stage(DbcToDbfStage(self.config))
        self.add_stage(DbfToCsvStage(self.config))
        self.add_stage(ProcessStage(self.config))
        self.add_stage(EnrichStage(self.config, self.ibge_data_path))
        self.add_stage(ParquetStage(self.config))
        self.add_stage(DatabaseStage(self.config))

        self.logger.info(f"Pipeline configured with {len(self._stages)} stages")
