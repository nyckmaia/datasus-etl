"""Base pipeline class for DataSUS subsystems.

This module provides a base class that all subsystem pipelines inherit from,
containing common functionality for download, conversion, transformation,
and export stages.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from pydatasus.config import PipelineConfig
from pydatasus.core.context import PipelineContext
from pydatasus.core.pipeline import Pipeline
from pydatasus.core.stage import Stage
from pydatasus.download.ftp_downloader import FTPDownloader
from pydatasus.storage.duckdb_manager import DuckDBManager
from pydatasus.storage.sql_transformer import SQLTransformer
from pydatasus.transform.converters.dbc_to_dbf import DbcToDbfConverter
from pydatasus.transform.converters.dbf_to_duckdb import DbfToDuckDBConverter

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
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from tqdm import tqdm

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

        self.logger.info(f"Streaming {len(dbf_files)} DBF files using {max_workers} threads")

        # Log all files to be processed
        tqdm.write(f"\nStreaming {len(dbf_files)} DBF files using {max_workers} threads:")
        for dbf_file in dbf_files:
            file_size_mb = dbf_file.stat().st_size / (1024 * 1024)
            tqdm.write(f"  - {dbf_file.name} ({file_size_mb:.1f}MB)")
        tqdm.write("")  # Empty line before progress bar

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
                as_completed(future_to_file), total=len(dbf_files), desc="Streaming DBF->DuckDB"
            ):
                dbf_file = future_to_file[future]
                try:
                    rows = future.result()
                    total_rows += rows
                    staging_tables.append(f"staging_{dbf_file.stem}")
                    tqdm.write(f"[OK] Completed {dbf_file.name}: {rows:,} rows")
                except Exception as e:
                    self.logger.error(f"Failed to stream {dbf_file.name}: {e}")
                    continue

        context.set("staging_tables", staging_tables)
        context.set_metadata("total_rows_loaded", total_rows)

        tqdm.write(f"\n[DONE] Streamed {total_rows:,} rows from {len(dbf_files)} DBF files\n")

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

    def _execute_partitioned(self, context: PipelineContext) -> PipelineContext:
        """Transform and export to Hive-partitioned Parquet with canonical schema."""
        from tqdm import tqdm

        # Get shared DuckDB manager from context
        db_manager = context.get("db_manager")
        if db_manager is None:
            raise ValueError("DuckDB manager not found in context. DBF stage must run first.")

        transformer = SQLTransformer(db_manager._conn, subsystem=self.subsystem)

        # Get list of staging tables
        staging_tables = context.get("staging_tables", [])
        if not staging_tables:
            self.logger.warning("No staging tables found")
            return context

        tqdm.write(
            f"\n[TRANSFORM] Transforming {len(staging_tables)} tables with canonical schema..."
        )
        tqdm.write(
            f"[PARTITION] Exporting to Hive-partitioned Parquet "
            f"(partition_by={self.config.storage.partition_cols})"
        )

        # Ensure output directory exists
        parquet_dir = self.config.storage.parquet_dir
        parquet_dir.mkdir(parents=True, exist_ok=True)

        # Build partition columns string
        partition_cols = ", ".join(self.config.storage.partition_cols)

        total_rows_exported = 0
        is_first_export = True

        # Process each staging table
        for i, staging_table in enumerate(staging_tables, 1):
            original_name = staging_table.replace("staging_", "")
            tqdm.write(
                f"\n[{i}/{len(staging_tables)}] Transforming {original_name} with canonical schema..."
            )

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

                # Determine append mode
                if is_first_export:
                    append_mode = "OVERWRITE_OR_IGNORE"
                    is_first_export = False
                else:
                    append_mode = "APPEND"

                # Get the date column for ordering (different per subsystem)
                order_col = self._get_order_column()

                # Export with partitioning
                export_sql = f"""
                    COPY (
                        SELECT * FROM {view_name}
                        ORDER BY {order_col} ASC NULLS LAST
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
                tqdm.write(
                    f"[OK] Exported {original_name}: {row_count:,} rows "
                    f"(partitioned by {partition_cols})"
                )

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

        db_manager = context.get("db_manager")
        if db_manager is None:
            raise ValueError("DuckDB manager not found in context. DBF stage must run first.")

        transformer = SQLTransformer(db_manager._conn, subsystem=self.subsystem)

        staging_tables = context.get("staging_tables", [])
        if not staging_tables:
            self.logger.warning("No staging tables found")
            return context

        tqdm.write(
            f"\n[TRANSFORM] Transforming {len(staging_tables)} tables with SQL (individual files)..."
        )

        parquet_dir = self.config.storage.parquet_dir
        parquet_dir.mkdir(parents=True, exist_ok=True)

        total_rows_exported = 0
        exported_files = []

        for i, staging_table in enumerate(staging_tables, 1):
            original_name = staging_table.replace("staging_", "")
            tqdm.write(f"\n[{i}/{len(staging_tables)}] Transforming {original_name}...")

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

                file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
                tqdm.write(
                    f"[OK] Exported {original_name}.parquet: {row_count:,} rows ({file_size_mb:.1f}MB)"
                )

                exported_files.append(str(parquet_file))

            except Exception as e:
                tqdm.write(f"[ERROR] Failed to transform/export {original_name}: {e}")
                self.logger.error(f"Failed to transform/export {staging_table}: {e}")
                raise

        context.set_metadata("total_rows_exported", total_rows_exported)
        context.set("exported_parquet_files", exported_files)

        tqdm.write(
            f"\n[DONE] Exported {len(exported_files)} Parquet files "
            f"({total_rows_exported:,} total rows)\n"
        )

        return context

    def _get_order_column(self) -> str:
        """Get the date column to use for ordering based on subsystem."""
        order_columns = {
            "sihsus": "dt_inter",
            "sim": "dtobito",
            "siasus": "dt_atend",
        }
        return order_columns.get(self.subsystem, "uf")


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
        """Set up standard pipeline stages."""
        self.add_stage(DownloadStage(self.config, self.subsystem_name.upper()))
        self.add_stage(DbcToDbfStage(self.config))
        self.add_stage(DbfToDbStage(self.config))
        self.add_stage(
            SqlTransformStage(self.config, self.ibge_data_path, subsystem=self.subsystem_name)
        )

        self.logger.info(f"{self.subsystem_name.upper()} pipeline configured with {len(self._stages)} stages")

    def _generate_completion_report(self, context: PipelineContext) -> None:
        """Generate and log pipeline completion statistics."""
        self.logger.info("=" * 70)
        self.logger.info(f"{self.subsystem_name.upper()} PIPELINE COMPLETION REPORT")
        self.logger.info("=" * 70)

        # Files processed
        staging_tables = context.get("staging_tables", [])
        dbf_files = context.get("dbf_files", [])
        num_files = len(dbf_files) or len(staging_tables)

        self.logger.info(f"Files Processed: {num_files} DBC -> Parquet")

        # Total rows
        total_rows = context.get_metadata("total_rows_exported", 0)
        if total_rows:
            self.logger.info(f"Total Rows: {total_rows:,}")

        # Output directory and size
        parquet_dir = self.config.storage.parquet_dir

        if parquet_dir.exists():
            total_size = sum(f.stat().st_size for f in parquet_dir.rglob("*.parquet"))

            if humanize:
                size_human = humanize.naturalsize(total_size, binary=True)
            else:
                size_human = f"{total_size / (1024**3):.2f} GB"

            self.logger.info(f"Output Directory: {parquet_dir}")
            self.logger.info(f"Parquet Database Size: {size_human}")

            # Count partition directories
            partition_dirs = [d for d in parquet_dir.rglob("*") if d.is_dir() and "=" in d.name]
            if partition_dirs:
                self.logger.info(f"Partitions: {len(partition_dirs)} directories")

        # Processing duration
        start_time = context.get_metadata("start_time")
        end_time = datetime.now()

        if start_time:
            duration = end_time - start_time
            if humanize:
                duration_human = humanize.naturaldelta(duration)
            else:
                duration_human = str(duration).split(".")[0]
            self.logger.info(f"Duration: {duration_human}")

        # Errors
        if context.has_errors:
            self.logger.warning(f"Errors: {len(context.errors)} encountered")
            for error in context.errors[:5]:
                self.logger.warning(f"   - {error}")

        self.logger.info("=" * 70)

    def _cleanup_temporary_files(self, success: bool = True) -> None:
        """Delete temporary DBC and DBF files after successful Parquet export."""
        if self.config.keep_temp_files:
            self.logger.info("Keeping temporary files (--keep-temp-files enabled)")
            return

        if not success:
            self.logger.info("Skipping cleanup due to pipeline failure")
            return

        deleted_dbc = 0
        deleted_dbf = 0

        # Delete DBF files
        dbf_dir = self.config.conversion.dbf_dir
        if dbf_dir.exists():
            for dbf_file in dbf_dir.rglob("*.dbf"):
                try:
                    dbf_file.unlink()
                    deleted_dbf += 1
                except Exception as e:
                    self.logger.warning(f"Failed to delete {dbf_file}: {e}")

        # Delete DBC files
        dbc_dir = self.config.download.output_dir
        if dbc_dir.exists():
            for dbc_file in dbc_dir.rglob("*.dbc"):
                try:
                    dbc_file.unlink()
                    deleted_dbc += 1
                except Exception as e:
                    self.logger.warning(f"Failed to delete {dbc_file}: {e}")

        if deleted_dbc > 0 or deleted_dbf > 0:
            self.logger.info(
                f"Cleanup: deleted {deleted_dbc} DBC and {deleted_dbf} DBF temporary files"
            )

    def run(self) -> "PipelineContext":
        """Run the pipeline and cleanup resources."""
        start_time = datetime.now()
        self.context.set_metadata("start_time", start_time)

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
