"""Base pipeline class for DataSUS subsystems.

This module provides a base class that all subsystem pipelines inherit from,
containing common functionality for download, conversion, transformation,
and export stages.
"""

import logging
from abc import ABC, abstractmethod
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
from datasus_etl.storage.sql_transformer import SQLTransformer
from datasus_etl.transform.converters.dbc_to_dbf import DbcToDbfConverter
from datasus_etl.transform.converters.dbf_to_duckdb import DbfToDuckDBConverter

try:
    import humanize
except ImportError:
    humanize = None


class DownloadStage(Stage):
    """Stage for downloading DBC files from DATASUS FTP."""

    def __init__(self, config: PipelineConfig, subsystem_name: str = "Data") -> None:
        super().__init__(f"Download {subsystem_name} Data")
        self.config = config

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Execute download stage."""
        from tqdm import tqdm

        current = context.get_metadata("current_stage", 1)
        total = context.get_metadata("total_stages", 1)

        tqdm.write(f"\n[{current}/{total}] Download: Baixando arquivos DBC do FTP...")

        # Report start of download
        context.update_stage_progress("download", 0.1, "Conectando ao FTP...")

        downloader = FTPDownloader(self.config.download)

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

    def __init__(self, config: PipelineConfig) -> None:
        super().__init__("Stream DBF to DuckDB")
        self.config = config

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

        # Find all DBF files
        dbf_files = list(self.config.conversion.dbf_dir.rglob("*.dbf"))
        if not dbf_files:
            self.logger.warning(f"No DBF files found in {self.config.conversion.dbf_dir}")
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
    """Stage for SQL-based data transformation and Parquet export."""

    def __init__(
        self, config: PipelineConfig, ibge_data_path: Path = None, subsystem: str = "sihsus"
    ) -> None:
        super().__init__("Transform Data (SQL) and Export Parquet")
        self.config = config
        self.ibge_data_path = ibge_data_path
        self.subsystem = subsystem
        self.use_partitioned_export = config.storage.use_partitioned_export

    def _execute(self, context: PipelineContext) -> PipelineContext:
        """Transform staging tables using SQL and export to Parquet."""
        if self.use_partitioned_export:
            return self._execute_partitioned(context)
        else:
            return self._execute_individual(context)

    def _export_csv(
        self,
        db_manager,
        view_name: str,
        original_name: str,
        output_dir: Path,
        order_col: str,
    ) -> None:
        """Export data to CSV file with Hive partitioning.

        Creates CSV files in partition directories: uf=XX/{original_name}.csv

        Args:
            db_manager: DuckDB manager instance
            view_name: Name of the view to export
            original_name: Original DBC filename (without extension)
            output_dir: Base output directory
            order_col: Column to order by
        """
        # Get UF value from the data to create partition directory
        uf_result = db_manager._conn.execute(
            f"SELECT DISTINCT uf FROM {view_name} LIMIT 1"
        ).fetchone()

        if uf_result is None:
            self.logger.warning(f"No data in view {view_name}, skipping CSV export")
            return

        uf_value = uf_result[0]

        # Create partition directory
        partition_dir = output_dir / f"uf={uf_value}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Output CSV file path (named after DBC source)
        csv_path = partition_dir / f"{original_name}.csv"

        # Export to CSV
        delimiter = self.config.csv_delimiter
        encoding = self.config.csv_encoding

        export_sql = f"""
            COPY (
                SELECT * FROM {view_name}
                ORDER BY {order_col} ASC NULLS LAST
            )
            TO '{csv_path}' (
                FORMAT CSV,
                HEADER true,
                DELIMITER '{delimiter}',
                ENCODING '{encoding}'
            )
        """
        db_manager._conn.execute(export_sql)

    def _export_parquet(
        self,
        db_manager,
        view_name: str,
        original_name: str,
        output_dir: Path,
        order_col: str,
    ) -> None:
        """Export data to Parquet file with Hive partitioning and original name.

        Creates Parquet files in partition directories: uf=XX/{original_name}.parquet
        This replaces DuckDB's automatic naming (data_0.parquet) with meaningful names.

        Args:
            db_manager: DuckDB manager instance
            view_name: Name of the view to export
            original_name: Original DBC filename (without extension)
            output_dir: Base output directory
            order_col: Column to order by
        """
        # Get UF value from the data to create partition directory
        uf_result = db_manager._conn.execute(
            f"SELECT DISTINCT uf FROM {view_name} LIMIT 1"
        ).fetchone()

        if uf_result is None:
            self.logger.warning(f"No data in view {view_name}, skipping Parquet export")
            return

        uf_value = uf_result[0]

        # Create partition directory
        partition_dir = output_dir / f"uf={uf_value}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Output Parquet file path (named after DBC source)
        parquet_path = partition_dir / f"{original_name}.parquet"

        # Export to Parquet with compression and settings
        compression = self.config.storage.compression
        row_group_size = self.config.storage.row_group_size

        export_sql = f"""
            COPY (
                SELECT * FROM {view_name}
                ORDER BY {order_col} ASC NULLS LAST
            )
            TO '{parquet_path}' (
                FORMAT PARQUET,
                COMPRESSION '{compression}',
                ROW_GROUP_SIZE {row_group_size}
            )
        """
        db_manager._conn.execute(export_sql)

    def _execute_partitioned(self, context: PipelineContext) -> PipelineContext:
        """Transform and export to Hive-partitioned Parquet with canonical schema."""
        from tqdm import tqdm

        current = context.get_metadata("current_stage", 1)
        total = context.get_metadata("total_stages", 1)

        # Get shared DuckDB manager from context
        db_manager = context.get("db_manager")
        if db_manager is None:
            raise ValueError("DuckDB manager not found in context. DBF stage must run first.")

        transformer = SQLTransformer(
            db_manager._conn,
            subsystem=self.subsystem,
            raw_mode=self.config.raw_mode,
        )

        # Get list of staging tables
        staging_tables = context.get("staging_tables", [])
        if not staging_tables:
            self.logger.warning("No staging tables found")
            return context

        # Ensure output directory exists
        parquet_dir = self.config.storage.parquet_dir
        parquet_dir.mkdir(parents=True, exist_ok=True)

        # Build partition columns string
        partition_cols = ", ".join(self.config.storage.partition_cols)
        format_name = self.config.output_format.upper()

        tqdm.write(
            f"\n[{current}/{total}] Transformacao + Export: "
            f"{len(staging_tables)} tabelas {SYM_ARROW} {format_name}..."
        )

        total_rows_exported = 0

        # Process each staging table with progress bar
        for staging_table in tqdm(staging_tables, desc="Transform+Export", leave=False):
            # Check for cancellation before processing next table
            context.check_cancelled()

            original_name = staging_table.replace("staging_", "")

            try:
                # Transform using canonical schema
                view_name = f"canonical_{original_name}"
                transformer.transform_to_canonical_view(
                    source_table=staging_table,
                    target_view=view_name,
                    ibge_data_path=self.ibge_data_path,
                )

                # Get row count before export
                row_count = db_manager._conn.execute(
                    f"SELECT COUNT(*) FROM {view_name}"
                ).fetchone()[0]

                # Get the date column for ordering (different per subsystem)
                order_col = self._get_order_column()

                # Export based on output format
                if self.config.output_format == "csv":
                    self._export_csv(
                        db_manager=db_manager,
                        view_name=view_name,
                        original_name=original_name,
                        output_dir=parquet_dir,
                        order_col=order_col,
                    )
                else:
                    self._export_parquet(
                        db_manager=db_manager,
                        view_name=view_name,
                        original_name=original_name,
                        output_dir=parquet_dir,
                        order_col=order_col,
                    )

                total_rows_exported += row_count
                self.logger.debug(f"Exported {original_name}: {row_count:,} rows")

                # Cleanup: drop the view to free memory
                db_manager._conn.execute(f"DROP VIEW IF EXISTS {view_name}")

            except Exception as e:
                tqdm.write(f"[ERROR] Failed: {original_name}: {e}")
                self.logger.error(f"Failed to transform/export {staging_table}: {e}")
                raise

        # Calculate total size based on output format
        file_ext = "*.csv" if self.config.output_format == "csv" else "*.parquet"
        output_files = list(parquet_dir.rglob(file_ext))
        total_size_bytes = sum(f.stat().st_size for f in output_files)
        total_size_mb = total_size_bytes / (1024 * 1024)

        # Count partition directories and files
        partition_dirs = list(parquet_dir.glob("uf=*"))

        context.set_metadata("total_rows_exported", total_rows_exported)
        context.set("parquet_dir", str(parquet_dir))
        context.set("partition_count", len(partition_dirs))
        context.set("file_count", len(output_files))
        context.set("exported_parquet_files", [str(f) for f in output_files])

        tqdm.write(
            f"       {SYM_CHECK} Export concluido: {total_rows_exported:,} linhas, "
            f"{len(output_files)} arquivos {format_name} ({total_size_mb:.1f} MB)"
        )

        # Mark stage progress complete
        context.mark_stage_progress_complete("sql_transform")

        return context

    def _execute_individual(self, context: PipelineContext) -> PipelineContext:
        """Legacy mode: Transform and export one Parquet file per DBF."""
        from tqdm import tqdm

        current = context.get_metadata("current_stage", 1)
        total = context.get_metadata("total_stages", 1)

        db_manager = context.get("db_manager")
        if db_manager is None:
            raise ValueError("DuckDB manager not found in context. DBF stage must run first.")

        transformer = SQLTransformer(
            db_manager._conn,
            subsystem=self.subsystem,
            raw_mode=self.config.raw_mode,
        )

        staging_tables = context.get("staging_tables", [])
        if not staging_tables:
            self.logger.warning("No staging tables found")
            return context

        parquet_dir = self.config.storage.parquet_dir
        parquet_dir.mkdir(parents=True, exist_ok=True)

        tqdm.write(
            f"\n[{current}/{total}] Transformacao + Export: "
            f"{len(staging_tables)} tabelas {SYM_ARROW} PARQUET (individual)..."
        )

        total_rows_exported = 0
        exported_files = []

        for staging_table in tqdm(staging_tables, desc="Transform+Export", leave=False):
            # Check for cancellation before processing next table
            context.check_cancelled()

            original_name = staging_table.replace("staging_", "")

            try:
                view_name = f"transformed_{original_name}"
                transformer.transform_to_canonical_view(
                    source_table=staging_table,
                    target_view=view_name,
                    ibge_data_path=self.ibge_data_path,
                )

                parquet_file = parquet_dir / f"{original_name}.parquet"
                order_col = self._get_order_column()

                db_manager._conn.execute(
                    f"""
                    COPY (
                        SELECT * FROM {view_name}
                        ORDER BY {order_col} ASC NULLS LAST
                    )
                    TO '{parquet_file}' (
                        FORMAT PARQUET,
                        COMPRESSION '{self.config.storage.compression}',
                        ROW_GROUP_SIZE {self.config.storage.row_group_size}
                    )
                """
                )

                row_count = db_manager._conn.execute(
                    f"SELECT COUNT(*) FROM {view_name}"
                ).fetchone()[0]
                total_rows_exported += row_count

                self.logger.debug(f"Exported {original_name}: {row_count:,} rows")

                exported_files.append(str(parquet_file))

            except Exception as e:
                tqdm.write(f"[ERROR] Failed: {original_name}: {e}")
                self.logger.error(f"Failed to transform/export {staging_table}: {e}")
                raise

        # Calculate total size
        total_size_mb = sum(
            Path(f).stat().st_size for f in exported_files
        ) / (1024 * 1024)

        context.set_metadata("total_rows_exported", total_rows_exported)
        context.set("exported_parquet_files", exported_files)

        tqdm.write(
            f"       {SYM_CHECK} Export concluido: {total_rows_exported:,} linhas, "
            f"{len(exported_files)} arquivos PARQUET ({total_size_mb:.1f} MB)"
        )

        # Mark stage progress complete
        context.mark_stage_progress_complete("sql_transform")

        return context

    def _get_order_column(self) -> str:
        """Get the date column to use for ordering based on subsystem."""
        order_columns = {
            "sihsus": "dt_inter",
            "sim": "dtobito",
            "siasus": "dt_atend",
        }
        return order_columns.get(self.subsystem, "uf")


class MemoryAwareProcessingStage(Stage):
    """Stage for memory-aware parallel processing of DBC files.

    This stage replaces DbcToDbfStage, DbfToDbStage, and SqlTransformStage
    when memory_aware_mode is enabled. It processes each DBC file independently:

    1. Decompress DBC → temporary DBF
    2. Stream DBF → independent DuckDB in-memory connection
    3. Apply SQL transformations
    4. Export to Parquet/CSV with Hive partitioning (by UF)
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
        output_dir = self.config.storage.parquet_dir
        results = processor.process_all(dbc_files, output_dir)

        # Store results in context
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]

        context.set("processing_results", results)
        context.set("exported_parquet_files", [str(r.output_file) for r in successful])
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
    - schema: Return the Parquet schema for the subsystem
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
        """Return Parquet schema for this subsystem."""
        pass

    def setup_stages(self) -> None:
        """Set up standard pipeline stages.

        Uses memory-aware mode if enabled in config, which processes
        one DBC file at a time with parallel workers to prevent RAM exhaustion.
        """
        # Define stage weights for progress tracking
        if self.config.database.memory_aware_mode:
            # Memory-aware mode: 2 stages
            stage_weights = {
                "download": 0.20,
                "memory_aware_processing": 0.80,
            }
        else:
            # Standard mode: 4 stages
            stage_weights = {
                "download": 0.25,
                "dbc_to_dbf": 0.10,
                "dbf_to_db": 0.25,
                "sql_transform": 0.40,
            }

        # Always start with download
        download_stage = DownloadStage(self.config, self.subsystem_name.upper())
        self.add_stage(download_stage)
        self.context.register_stage("download", stage_weights["download"])

        # Check if memory-aware mode is enabled
        if self.config.database.memory_aware_mode:
            # Memory-aware mode: single stage handles DBC→Parquet directly
            processing_stage = MemoryAwareProcessingStage(
                self.config,
                self.ibge_data_path,
                subsystem=self.subsystem_name,
            )
            self.add_stage(processing_stage)
            self.context.register_stage(
                "memory_aware_processing", stage_weights["memory_aware_processing"]
            )
        else:
            # Standard mode: separate stages
            dbc_stage = DbcToDbfStage(self.config)
            self.add_stage(dbc_stage)
            self.context.register_stage("dbc_to_dbf", stage_weights["dbc_to_dbf"])

            dbf_stage = DbfToDbStage(self.config)
            self.add_stage(dbf_stage)
            self.context.register_stage("dbf_to_db", stage_weights["dbf_to_db"])

            transform_stage = SqlTransformStage(
                self.config, self.ibge_data_path, subsystem=self.subsystem_name
            )
            self.add_stage(transform_stage)
            self.context.register_stage("sql_transform", stage_weights["sql_transform"])

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

        # Total rows
        total_rows = context.get_metadata("total_rows_exported", 0)

        # Output size
        parquet_dir = self.config.storage.parquet_dir
        format_ext = "*.csv" if self.config.output_format == "csv" else "*.parquet"
        format_name = self.config.output_format.upper()

        if parquet_dir.exists():
            output_files = list(parquet_dir.rglob(format_ext))
            total_size = sum(f.stat().st_size for f in output_files)

            if humanize:
                size_human = humanize.naturalsize(total_size, binary=True)
            else:
                size_human = f"{total_size / (1024**2):.1f} MB"
        else:
            output_files = []
            size_human = "0 MB"

        # Print summary
        tqdm.write("")
        tqdm.write(f"[PIPELINE] {SYM_CHECK} {self.subsystem_name.upper()} concluido em {duration_str}")
        tqdm.write(f"           Linhas exportadas: {total_rows:,}")
        tqdm.write(f"           Arquivos {format_name}: {len(output_files)} ({size_human})")
        tqdm.write(f"           Diretório: {parquet_dir}")

        # Errors
        if context.has_errors:
            tqdm.write(f"           [!] Erros: {len(context.errors)}")
            for error in context.errors[:3]:
                tqdm.write(f"               - {error}")

        tqdm.write("")

    def _cleanup_temporary_files(self, success: bool = True) -> None:
        """Delete temporary DBC and DBF files and directories after successful Parquet export."""
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
