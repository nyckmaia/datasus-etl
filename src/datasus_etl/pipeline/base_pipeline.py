"""Base pipeline class for DataSUS subsystems.

This module provides a base class that all subsystem pipelines inherit from,
containing common functionality for download, conversion, transformation,
and export stages.
"""

import logging
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from datasus_etl.config import PipelineConfig
from datasus_etl.constants import SYM_CHECK, SYM_ARROW
from datasus_etl.core.context import PipelineContext
from datasus_etl.core.pipeline import Pipeline
from datasus_etl.core.stage import Stage
from datasus_etl.download.ftp_downloader import FTPDownloader
from datasus_etl.storage.duckdb_manager import DuckDBManager
from datasus_etl.storage.memory_aware_processor import MemoryAwareProcessor
from datasus_etl.storage.parquet_manager import ParquetManager
from datasus_etl.storage.sql_transformer import SQLTransformer, SUBSYSTEM_SCHEMAS
from datasus_etl.transform.converters.dbc_to_dbf import DbcToDbfConverter
from datasus_etl.transform.converters.dbf_to_duckdb import DbfToDuckDBConverter
from datasus_etl.transform.converters.dbf_to_parquet import (
    convert_dbf_to_parquet,
    ParquetConversionResult,
)

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
        from tqdm import tqdm

        current = context.get_metadata("current_stage", 1)
        total = context.get_metadata("total_stages", 1)

        tqdm.write(f"\n[{current}/{total}] Download: Baixando arquivos DBC do FTP...")

        # Report start of download
        context.update_stage_progress("download", 0.1, "Conectando ao FTP...")

        downloader = FTPDownloader(self.config.download, subsystem=self.subsystem)

        # Report download in progress
        context.update_stage_progress("download", 0.5, "Baixando arquivos...")

        files = downloader.download()

        context.set("downloaded_files", files)
        context.set_metadata("download_count", len(files))

        # Calculate total size
        total_size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
        tqdm.write(f"       {SYM_CHECK} Download concluido: {len(files)} arquivos ({total_size_mb:.1f} MB)")

        # Mark stage progress complete
        context.mark_stage_progress_complete("download")

        return context


class DbcToDbfStage(Stage):
    """Stage for converting DBC to DBF files."""

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Convert DBC to DBF")
        self.config = config

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute DBC to DBF conversion stage."""
        from tqdm import tqdm

        current = context.get_metadata("current_stage", 1)
        total = context.get_metadata("total_stages", 1)

        tqdm.write(f"\n[{current}/{total}] Conversao: DBC {SYM_ARROW} DBF...")

        converter = DbcToDbfConverter(self.config.conversion)
        stats = converter.convert_directory()

        context.set("dbc_conversion_stats", stats)
        context.set_metadata("dbc_converted_count", stats["converted"])

        tqdm.write(f"       {SYM_CHECK} Conversao concluida: {stats['converted']} arquivos")

        # Mark stage progress complete
        context.mark_stage_progress_complete("dbc_to_dbf")

        return context


class DbfToDbStage(Stage):
    """Stage for streaming DBF directly to DuckDB."""

    def __init__(self, config: PipelineConfig, subsystem: str = "sihsus") -> None:
        super().__init__("Stream DBF to DuckDB")
        self.config = config
        self.subsystem = subsystem

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Stream all DBF files to DuckDB staging tables with parallel processing."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from tqdm import tqdm

        current = context.get_metadata("current_stage", 1)
        total = context.get_metadata("total_stages", 1)

        # Get or create DuckDB manager (shared across stages)
        db_manager = context.get("db_manager")
        if db_manager is None:
            db_manager = DuckDBManager(self.config.database)
            db_manager.__enter__()  # Open connection
            context.set("db_manager", db_manager)

        converter = DbfToDuckDBConverter(
            conn=db_manager._conn,
            chunk_size=self.config.database.chunk_size,
            dataframe_threshold_mb=self.config.database.dataframe_threshold_mb,
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
        max_workers = 1
        total_rows = 0
        staging_tables = []

        tqdm.write(f"\n[{current}/{total}] Streaming: DBF {SYM_ARROW} DuckDB ({len(dbf_files)} arquivos)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(
                    converter.stream_dbf_to_table, dbf_file, f"staging_{dbf_file.stem}", True
                ): dbf_file
                for dbf_file in dbf_files
            }

            # Process completed tasks with progress bar
            for future in tqdm(
                as_completed(future_to_file), total=len(dbf_files), desc=f"DBF{SYM_ARROW}DuckDB", leave=False
            ):
                # Check for cancellation before processing next result
                context.check_cancelled()

                dbf_file = future_to_file[future]
                try:
                    rows = future.result()
                    total_rows += rows
                    staging_tables.append(f"staging_{dbf_file.stem}")
                    self.logger.debug(f"Streamed {dbf_file.name}: {rows:,} rows")
                except Exception as e:
                    self.logger.error(f"Failed to stream {dbf_file.name}: {e}")
                    continue

        context.set("staging_tables", staging_tables)
        context.set_metadata("total_rows_loaded", total_rows)

        tqdm.write(f"       {SYM_CHECK} Streaming concluido: {total_rows:,} linhas de {len(dbf_files)} arquivos")

        # Mark stage progress complete
        context.mark_stage_progress_complete("dbf_to_db")

        return context


class SqlTransformStage(Stage):
    """Stage for SQL-based data transformation and DuckDB export."""

    def __init__(
        self, config: PipelineConfig, ibge_data_path: Path = None, subsystem: str = "sihsus"
    ) -> None:
        super().__init__("Transform Data (SQL) and Export to DuckDB")
        self.config = config
        self.ibge_data_path = ibge_data_path
        self.subsystem = subsystem

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Transform staging tables using SQL and insert into persistent DuckDB."""
        return self._execute_duckdb_insert(context)

    def _execute_duckdb_insert(self, context: PipelineContext) -> PipelineContext:
        """Transform staging tables and insert into persistent DuckDB database."""
        import duckdb
        from tqdm import tqdm

        from datasus_etl.storage.dimension_loader import DimensionLoader

        current = context.get_metadata("current_stage", 1)
        total = context.get_metadata("total_stages", 1)

        # Get shared DuckDB manager from context (in-memory, used for staging)
        staging_db_manager = context.get("db_manager")
        if staging_db_manager is None:
            raise ValueError("DuckDB manager not found in context. DBF stage must run first.")

        # Get list of staging tables
        staging_tables = context.get("staging_tables", [])
        if not staging_tables:
            self.logger.warning("No staging tables found")
            return context

        tqdm.write(
            f"\n[{current}/{total}] Transformacao + Export: "
            f"{len(staging_tables)} tabelas {SYM_ARROW} DuckDB..."
        )

        # Get the persistent database path
        db_path = self.config.get_database_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Open persistent DuckDB connection
        persistent_conn = duckdb.connect(str(db_path))

        try:
            # Get schema for this subsystem
            from datasus_etl.storage.sql_transformer import SUBSYSTEM_SCHEMAS
            schema = SUBSYSTEM_SCHEMAS.get(self.subsystem, SUBSYSTEM_SCHEMAS["sihsus"])

            # Initialize raw table in persistent DB
            from datasus_etl.storage.duckdb_manager import DuckDBManager
            from datasus_etl.config import DatabaseConfig

            temp_config = DatabaseConfig(db_path=db_path)
            persistent_manager = DuckDBManager(temp_config)
            persistent_manager._conn = persistent_conn

            # Initialize schema (table + dimensions)
            persistent_manager.initialize_raw_table(self.subsystem, schema)
            persistent_manager.initialize_dimension_tables()

            # Load IBGE dimension data
            dim_loader = DimensionLoader(persistent_conn)
            try:
                dim_loader.load_ibge_municipios(replace=False)
                tqdm.write(f"       {SYM_CHECK} Dados IBGE carregados na tabela dim_municipios")
            except Exception as e:
                self.logger.warning(f"Failed to load IBGE data: {e}")

            total_rows_inserted = 0
            raw_table = f"{self.subsystem}_raw"

            # Create transformer for staging DB
            transformer = SQLTransformer(
                staging_db_manager._conn,
                subsystem=self.subsystem,
                raw_mode=self.config.raw_mode,
            )

            # Process each staging table
            for staging_table in tqdm(staging_tables, desc="Transform+Insert", leave=False):
                # Check for cancellation
                context.check_cancelled()

                original_name = staging_table.replace("staging_", "")

                try:
                    # Transform in staging DB (creates a view)
                    view_name = f"canonical_{original_name}"
                    transformer.transform_to_canonical_view(
                        source_table=staging_table,
                        target_view=view_name,
                        enable_ibge_enrichment=False,  # IBGE enrichment will be in VIEW
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
                            tqdm.write(f"       [SKIP] {original_name} ja processado")
                            continue

                    # Transfer data from staging to persistent DB
                    # Use fetchall() + executemany() to avoid DuckDB lock conflicts
                    data = staging_db_manager._conn.execute(
                        f"SELECT * FROM {view_name}"
                    ).fetchall()

                    # Get column names for INSERT
                    columns = [desc[0] for desc in staging_db_manager._conn.execute(
                        f"SELECT * FROM {view_name} LIMIT 0"
                    ).description]

                    # Insert into persistent connection
                    # Quote column names to handle SQL reserved words (e.g., 'natural')
                    placeholders = ", ".join(["?" for _ in columns])
                    col_list = ", ".join([f'"{col}"' for col in columns])
                    persistent_conn.executemany(
                        f"INSERT INTO {raw_table} ({col_list}) VALUES ({placeholders})",
                        data
                    )

                    total_rows_inserted += row_count
                    self.logger.debug(f"Inserted {original_name}: {row_count:,} rows")

                    # Cleanup staging view
                    staging_db_manager._conn.execute(f"DROP VIEW IF EXISTS {view_name}")

                except Exception as e:
                    tqdm.write(f"[ERROR] Failed: {original_name}: {e}")
                    self.logger.error(f"Failed to transform/insert {staging_table}: {e}")
                    raise

            # Create enrichment VIEW in persistent DB
            persistent_manager.create_enrichment_view(self.subsystem)

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

            tqdm.write(
                f"       {SYM_CHECK} Export concluido: {total_rows_inserted:,} linhas inseridas"
            )
            tqdm.write(
                f"       {SYM_CHECK} Database: {db_path.name} ({db_size_mb:.1f} MB, {total_rows:,} linhas total)"
            )

            # Mark stage progress complete
            context.mark_stage_progress_complete("sql_transform")

        finally:
            persistent_conn.close()

        return context

    def _get_order_column(self) -> str:
        """Get the date column to use for ordering based on subsystem."""
        order_columns = {
            "sihsus": "dt_inter",
            "sim": "dtobito",
            "siasus": "dt_atend",
        }
        return order_columns.get(self.subsystem, "uf")


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
        from tqdm import tqdm

        current = context.get_metadata("current_stage", 1)
        total = context.get_metadata("total_stages", 1)

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
            tqdm.write(f"\n[{current}/{total}] {SYM_CHECK} All {len(dbf_files)} DBF files already processed")
            context.set("parquet_results", [])
            context.set_metadata("files_converted", 0)
            context.mark_stage_progress_complete("dbf_to_parquet")
            return context

        tqdm.write(f"\n[{current}/{total}] Parquet: Converting {len(pending_files)} DBF files "
                   f"(skipping {len(processed_files)} already processed)")

        # Setup output directory
        output_dir = self.config.get_parquet_dir()
        output_dir.mkdir(parents=True, exist_ok=True)

        # Configure parallel processing
        num_workers = min(self.config.database.num_workers, len(pending_files))
        compression = self.config.storage.parquet_compression
        raw_mode = self.config.raw_mode

        self.logger.info(f"Using {num_workers} parallel workers, compression={compression}")

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
                leave=False,
            ):
                # Check for cancellation
                context.check_cancelled()

                dbf_file = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)

                    if result.success:
                        total_rows += result.rows
                        parquet_manager.mark_processed(result.source_file)
                        self.logger.debug(f"Converted {result.source_file}: {result.rows:,} rows")
                    else:
                        errors += 1
                        tqdm.write(f"       [ERROR] {result.source_file}: {result.error}")

                except Exception as e:
                    errors += 1
                    self.logger.error(f"Failed to convert {dbf_file.name}: {e}")
                    tqdm.write(f"       [ERROR] {dbf_file.name}: {e}")

        # Store results in context
        context.set("parquet_results", results)
        context.set_metadata("files_converted", len(results) - errors)
        context.set_metadata("total_rows_converted", total_rows)
        context.set_metadata("conversion_errors", errors)

        # Get storage stats
        stats = parquet_manager.get_storage_stats()
        context.set("parquet_stats", stats)

        tqdm.write(f"       {SYM_CHECK} Converted {len(results) - errors} files, "
                   f"{total_rows:,} rows, {errors} errors")
        tqdm.write(f"       Storage: {stats.total_files} Parquet files, "
                   f"{stats.total_size_bytes / (1024*1024):.1f} MB")

        # Mark stage progress complete
        context.mark_stage_progress_complete("dbf_to_parquet")

        return context


class MemoryAwareProcessingStage(Stage):
    """Stage for memory-aware parallel processing of DBC files.

    This stage replaces DbcToDbfStage, DbfToDbStage, and SqlTransformStage
    when memory_aware_mode is enabled. It processes each DBC file independently:

    1. Decompress DBC -> temporary DBF
    2. Stream DBF -> independent DuckDB in-memory connection
    3. Apply SQL transformations
    4. Export to persistent DuckDB database
    5. Clean up and release memory

    Each worker uses its own DuckDB connection, allowing true parallelism
    while preventing RAM exhaustion on large datasets.
    """

    def __init__(
        self,
        config: PipelineConfig,
        ibge_data_path: Path = None,
        subsystem: str = "sihsus",
    ) -> None:
        super().__init__("Memory-Aware DBC Processing")
        self.config = config
        self.ibge_data_path = ibge_data_path
        self.subsystem = subsystem

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute memory-aware processing of all DBC files."""
        from tqdm import tqdm

        current = context.get_metadata("current_stage", 1)
        total = context.get_metadata("total_stages", 1)

        # Find all DBC files
        dbc_dir = self.config.download.output_dir
        dbc_files = list(dbc_dir.rglob("*.dbc")) + list(dbc_dir.rglob("*.DBC"))

        if not dbc_files:
            self.logger.warning(f"No DBC files found in {dbc_dir}")
            return context

        format_name = self.config.output_format.upper()
        tqdm.write(
            f"\n[{current}/{total}] Processamento Memory-Aware: "
            f"{len(dbc_files)} arquivos DBC {SYM_ARROW} {format_name}..."
        )

        # Create progress updater for memory-aware processing
        def update_processing_progress(pct: float, msg: str = "") -> None:
            context.update_stage_progress("memory_aware_processing", pct, msg)

        # Create processor with configured workers
        processor = MemoryAwareProcessor(
            config=self.config,
            num_workers=self.config.database.num_workers,
            ibge_data_path=self.ibge_data_path,
            cancel_check=context.is_cancelled,
            progress_callback=update_processing_progress,
        )

        # Process all files
        output_dir = self.config.storage.database_dir
        results = processor.process_all(dbc_files, output_dir)

        # Store results in context
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        context.set("processing_results", results)
        context.set("exported_database_files", [str(r.output_file) for r in successful])
        context.set_metadata("total_rows_exported", processor.total_rows_exported)
        context.set_metadata("files_processed", len(successful))
        context.set_metadata("files_failed", len(failed))

        # Count partitions
        partition_dirs = list(output_dir.glob("uf=*"))
        context.set("partition_count", len(partition_dirs))

        if failed:
            for r in failed:
                context.add_error(f"Failed: {r.source_file}: {r.error}")

        # Mark stage progress complete
        context.mark_stage_progress_complete("memory_aware_processing")

        return context


class DatasusPipeline(Pipeline[PipelineConfig], ABC):
    """Base class for all DataSUS subsystem pipelines.

    Provides common functionality:
    - Standard stages (download, convert, transform, export)
    - Completion report generation
    - Temporary file cleanup
    - DuckDB connection management

    Subclasses must implement:
    - subsystem_name: Return the subsystem identifier
    - schema: Return the DuckDB schema for the subsystem
    """

    def __init__(self, config: PipelineConfig, ibge_data_path: Path = None) -> None:
        """Initialize the pipeline.

        Args:
            config: Complete pipeline configuration
            ibge_data_path: Optional path to IBGE data file
        """
        super().__init__(config)
        self.ibge_data_path = ibge_data_path

    @property
    @abstractmethod
    def subsystem_name(self) -> str:
        """Return subsystem name (e.g., 'sihsus', 'sim')."""
        pass

    @property
    @abstractmethod
    def schema(self) -> dict[str, str]:
        """Return DuckDB schema for this subsystem."""
        pass

    def setup_stages(self) -> None:
        """Set up standard pipeline stages.

        Pipeline stages depend on output_format and memory_aware_mode settings:

        For Parquet mode (default):
        1. Download DBC files from DATASUS FTP
        2. Decompress DBC -> DBF
        3. Convert DBF -> Parquet (partitioned by UF)

        For DuckDB mode (legacy):
        1. Download DBC files from DATASUS FTP
        2. Decompress DBC -> DBF
        3. Stream DBF -> DuckDB staging tables
        4. Transform in SQL and export to persistent DuckDB

        Memory-aware mode processes one DBC file at a time with parallel workers.
        """
        # Define stage weights for progress tracking based on mode
        if self.config.is_parquet_mode():
            # Parquet mode: 3 stages
            stage_weights = {
                "download": 0.20,
                "dbc_to_dbf": 0.10,
                "dbf_to_parquet": 0.70,
            }
        elif self.config.database.memory_aware_mode:
            # Memory-aware mode: 2 stages
            stage_weights = {
                "download": 0.20,
                "memory_aware_processing": 0.80,
            }
        else:
            # Standard DuckDB mode: 4 stages
            stage_weights = {
                "download": 0.25,
                "dbc_to_dbf": 0.10,
                "dbf_to_db": 0.25,
                "sql_transform": 0.40,
            }

        # Always start with download
        download_stage = DownloadStage(self.config, subsystem=self.subsystem_name)
        self.add_stage(download_stage)
        self.context.register_stage("download", stage_weights["download"])

        # Choose pipeline path based on configuration
        if self.config.is_parquet_mode():
            # Parquet mode: DBC -> DBF -> Parquet
            dbc_stage = DbcToDbfStage(self.config)
            self.add_stage(dbc_stage)
            self.context.register_stage("dbc_to_dbf", stage_weights["dbc_to_dbf"])

            parquet_stage = DbfToParquetStage(self.config, subsystem=self.subsystem_name)
            self.add_stage(parquet_stage)
            self.context.register_stage("dbf_to_parquet", stage_weights["dbf_to_parquet"])

            self.logger.info(f"{self.subsystem_name.upper()} pipeline configured for Parquet output")

        elif self.config.database.memory_aware_mode:
            # Memory-aware mode: single stage handles DBC->DuckDB directly
            processing_stage = MemoryAwareProcessingStage(
                self.config,
                self.ibge_data_path,
                subsystem=self.subsystem_name,
            )
            self.add_stage(processing_stage)
            self.context.register_stage(
                "memory_aware_processing", stage_weights["memory_aware_processing"]
            )
            self.logger.info(f"{self.subsystem_name.upper()} pipeline configured for memory-aware DuckDB")

        else:
            # Standard DuckDB mode: separate stages
            dbc_stage = DbcToDbfStage(self.config)
            self.add_stage(dbc_stage)
            self.context.register_stage("dbc_to_dbf", stage_weights["dbc_to_dbf"])

            dbf_stage = DbfToDbStage(self.config, subsystem=self.subsystem_name)
            self.add_stage(dbf_stage)
            self.context.register_stage("dbf_to_db", stage_weights["dbf_to_db"])

            transform_stage = SqlTransformStage(
                self.config, self.ibge_data_path, subsystem=self.subsystem_name
            )
            self.add_stage(transform_stage)
            self.context.register_stage("sql_transform", stage_weights["sql_transform"])
            self.logger.info(f"{self.subsystem_name.upper()} pipeline configured for DuckDB output")

        self.logger.debug(
            f"{self.subsystem_name.upper()} pipeline configured with {len(self._stages)} stages"
        )

    def _generate_completion_report(self, context: PipelineContext) -> None:
        """Generate and log pipeline completion statistics."""
        from tqdm import tqdm

        # Calculate duration
        start_time = context.get_metadata("start_time")
        end_time = datetime.now()
        duration_str = ""

        if start_time:
            duration = end_time - start_time
            if humanize:
                duration_str = humanize.naturaldelta(duration)
            else:
                duration_str = str(duration).split(".")[0]

        # Check if Parquet mode
        is_parquet = self.config.is_parquet_mode()

        tqdm.write("")
        tqdm.write(f"[PIPELINE] {SYM_CHECK} {self.subsystem_name.upper()} concluido em {duration_str}")

        if is_parquet:
            # Parquet mode statistics
            total_rows = context.get_metadata("total_rows_converted", 0)
            files_converted = context.get_metadata("files_converted", 0)
            parquet_stats = context.get("parquet_stats")

            tqdm.write(f"           Arquivos convertidos: {files_converted}")
            tqdm.write(f"           Linhas processadas: {total_rows:,}")

            if parquet_stats:
                if humanize:
                    size_human = humanize.naturalsize(parquet_stats.total_size_bytes, binary=True)
                else:
                    size_human = f"{parquet_stats.total_size_bytes / (1024**2):.1f} MB"

                parquet_dir = self.config.get_parquet_dir()
                tqdm.write(f"           Tamanho: {size_human}")
                tqdm.write(f"           Particoes: {len(parquet_stats.partitions)} UFs")
                tqdm.write(f"           Caminho: {parquet_dir}")
        else:
            # DuckDB mode statistics
            total_rows = context.get_metadata("total_rows_exported", 0)
            total_rows_in_db = context.get_metadata("total_rows_in_db", total_rows)

            # Output size - DuckDB database file
            db_path = self.config.get_database_path()

            if db_path.exists():
                total_size = db_path.stat().st_size

                if humanize:
                    size_human = humanize.naturalsize(total_size, binary=True)
                else:
                    size_human = f"{total_size / (1024**2):.1f} MB"
            else:
                size_human = "0 MB"

            tqdm.write(f"           Linhas inseridas: {total_rows:,}")
            tqdm.write(f"           Total no banco: {total_rows_in_db:,}")
            tqdm.write(f"           Database: {db_path.name} ({size_human})")
            tqdm.write(f"           Caminho: {db_path}")

        # Errors
        if context.has_errors:
            tqdm.write(f"           [!] Erros: {len(context.errors)}")
            for error in context.errors[:3]:
                tqdm.write(f"               - {error}")

        tqdm.write("")

    def _cleanup_temporary_files(self, success: bool = True) -> None:
        """Delete temporary DBC and DBF files and directories after successful DuckDB export."""
        if self.config.keep_temp_files:
            self.logger.debug("Keeping temporary files (--keep-temp-files enabled)")
            return

        if not success:
            self.logger.debug("Skipping cleanup due to pipeline failure")
            return

        deleted_dbc = 0
        deleted_dbf = 0
        deleted_dirs = 0

        # Delete DBF files
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

        # Delete DBC files
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

        if deleted_dbc > 0 or deleted_dbf > 0:
            self.logger.debug(
                f"Cleanup: deleted {deleted_dbc} DBC, {deleted_dbf} DBF files"
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
        """Run the pipeline and cleanup resources."""
        from tqdm import tqdm

        start_time = datetime.now()
        self.context.set_metadata("start_time", start_time)

        # Build UF list string
        uf_list = getattr(self.config.download, "uf_list", None) or []
        uf_str = ", ".join(uf_list[:5]) if uf_list else "todos"
        if len(uf_list) > 5:
            uf_str += f" (+{len(uf_list) - 5})"

        # Date range
        start_date = getattr(self.config.download, "start_date", "")
        end_date = getattr(self.config.download, "end_date", "")
        date_str = f"{start_date} a {end_date}" if start_date else ""

        # Mode
        mode_str = "memory-aware" if self.config.database.memory_aware_mode else "standard"

        tqdm.write(f"\n[PIPELINE] Iniciando {self.subsystem_name.upper()} ({uf_str})")
        if date_str:
            tqdm.write(f"           Período: {date_str}")
        tqdm.write(f"           Modo: {mode_str}")

        success = False
        try:
            result = super().run()
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

            # Cleanup temporary files
            self._cleanup_temporary_files(success=success)
