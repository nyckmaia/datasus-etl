"""Converter from DBF to Parquet with DuckDB in-memory processing.

This module converts DBF files to Parquet format using DuckDB in-memory
for transformations, enabling partitioned Hive-style storage:
- Structure: {base_dir}/parquet/{subsystem}/uf={UF}/{filename}.parquet
- Memory efficient: releases resources after each file conversion
- Parallel processing: each worker uses its own DuckDB connection
"""

import gc
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import duckdb
import psutil

from datasus_etl.exceptions import ConversionError
from datasus_etl.transform.sql import TransformPipeline


@dataclass
class ParquetConversionResult:
    """Result of a single DBF to Parquet conversion."""

    source_file: str  # Original DBC filename (e.g., "RDSP2401.dbc")
    output_file: Path  # Path to output Parquet file
    rows: int  # Number of rows written
    uf: str  # UF code extracted from filename
    success: bool  # Whether conversion succeeded
    error: Optional[str] = None  # Error message if failed


class DbfToParquetConverter:
    """Convert DBF files to Parquet format with DuckDB in-memory processing.

    Uses DuckDB in-memory for streaming and transformations, then exports
    to Parquet format. Each conversion creates its own DuckDB connection
    to enable parallel processing.

    Example:
        >>> converter = DbfToParquetConverter(
        ...     subsystem="sihsus",
        ...     schema=SIHSUS_DUCKDB_SCHEMA,
        ... )
        >>> result = converter.convert(
        ...     Path("data/sihsus/dbf/RDSP2401.dbf"),
        ...     Path("data/parquet/sihsus"),
        ... )
        >>> print(f"Converted {result.rows} rows to {result.output_file}")
    """

    def __init__(
        self,
        subsystem: str,
        schema: dict[str, str],
        chunk_size: int = 10000,
        raw_mode: bool = False,
        compression: str = "zstd",
        dataframe_threshold_mb: int = 250,
    ) -> None:
        """Initialize the converter.

        Args:
            subsystem: DataSUS subsystem name (sihsus, sim, etc.)
            schema: DuckDB schema dict mapping column names to types
            chunk_size: Number of rows per chunk for streaming (default: 10000)
            raw_mode: If True, skip type conversions (default: False)
            compression: Parquet compression algorithm (default: zstd)
            dataframe_threshold_mb: File size threshold for full DataFrame loading
        """
        self.subsystem = subsystem
        self.schema = schema
        self.chunk_size = chunk_size
        self.raw_mode = raw_mode
        self.compression = compression.upper()
        self.dataframe_threshold_mb = dataframe_threshold_mb
        self.logger = logging.getLogger(__name__)

    def convert(
        self,
        dbf_path: Path,
        output_dir: Path,
        enable_ibge_enrichment: bool = False,
    ) -> ParquetConversionResult:
        """Convert a single DBF file to Parquet format.

        Creates a DuckDB in-memory connection, streams the DBF file,
        applies transformations, and exports to Parquet in the
        appropriate UF partition directory.

        Args:
            dbf_path: Path to input DBF file
            output_dir: Base directory for Parquet output
                       (files will be saved to output_dir/uf={UF}/)
            enable_ibge_enrichment: Whether to enrich with IBGE data

        Returns:
            ParquetConversionResult with conversion details

        Raises:
            ConversionError: If conversion fails critically
        """
        # Extract metadata from filename (e.g., RDSP2401.dbf -> uf=SP)
        filename = dbf_path.stem.upper()
        uf_code = filename[2:4] if len(filename) >= 4 else "XX"
        source_file = f"{filename}.dbc"

        conn = None
        try:
            # Create in-memory DuckDB connection
            conn = duckdb.connect(":memory:")

            # Configure memory limits based on available RAM
            ram_gb = psutil.virtual_memory().available / (1024**3)
            memory_limit_gb = max(1, int(ram_gb * 0.3))
            conn.execute(f"SET memory_limit = '{memory_limit_gb}GB'")
            self.logger.debug(f"DuckDB memory limit: {memory_limit_gb}GB")

            # Stream DBF to staging table
            staging_table = f"staging_{dbf_path.stem}"
            rows = self._stream_dbf_to_staging(conn, dbf_path, staging_table, uf_code, source_file)

            if rows == 0:
                self.logger.warning(f"No rows in {dbf_path.name}, skipping")
                return ParquetConversionResult(
                    source_file=source_file,
                    output_file=output_dir / f"uf={uf_code}" / f"{dbf_path.stem}.parquet",
                    rows=0,
                    uf=uf_code,
                    success=True,
                )

            # Apply transformations using TransformPipeline
            view_name = f"canonical_{dbf_path.stem}"
            pipeline = TransformPipeline(
                conn=conn,
                schema=self.schema,
                subsystem=self.subsystem,
                raw_mode=self.raw_mode,
                enable_ibge=enable_ibge_enrichment,
            )
            pipeline.execute_transform(staging_table, view_name)

            # Create output directory with Hive-style partitioning
            partition_dir = output_dir / f"uf={uf_code}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            output_path = partition_dir / f"{dbf_path.stem}.parquet"

            # Export to Parquet
            conn.execute(f"""
                COPY (SELECT * FROM {view_name})
                TO '{output_path}'
                (FORMAT PARQUET, COMPRESSION '{self.compression}')
            """)

            self.logger.info(f"Converted {dbf_path.name}: {rows:,} rows -> {output_path.name}")

            return ParquetConversionResult(
                source_file=source_file,
                output_file=output_path,
                rows=rows,
                uf=uf_code,
                success=True,
            )

        except Exception as e:
            self.logger.error(f"Failed to convert {dbf_path.name}: {e}")
            return ParquetConversionResult(
                source_file=source_file,
                output_file=output_dir / f"uf={uf_code}" / f"{dbf_path.stem}.parquet",
                rows=0,
                uf=uf_code,
                success=False,
                error=str(e),
            )

        finally:
            # Cleanup to release memory
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            gc.collect()

    def _stream_dbf_to_staging(
        self,
        conn: duckdb.DuckDBPyConnection,
        dbf_path: Path,
        table_name: str,
        uf_code: str,
        source_file: str,
    ) -> int:
        """Stream DBF file to DuckDB staging table.

        Uses adaptive strategy based on file size:
        - Small files: Full DataFrame insertion (faster)
        - Large files: Chunked streaming (memory efficient)

        Args:
            conn: DuckDB connection
            dbf_path: Path to DBF file
            table_name: Target staging table name
            uf_code: UF code to add as column
            source_file: Original source filename

        Returns:
            Number of rows inserted
        """
        file_size_mb = dbf_path.stat().st_size / (1024 * 1024)
        available_ram_gb = psutil.virtual_memory().available / (1024**3)
        threshold_mb = min(self.dataframe_threshold_mb, available_ram_gb * 1024 * 0.2)

        if file_size_mb < threshold_mb:
            return self._insert_full_dataframe(conn, dbf_path, table_name, uf_code, source_file)
        else:
            return self._stream_chunks(conn, dbf_path, table_name, uf_code, source_file)

    def _clean_column_name(self, name: str) -> str:
        """Clean DBF column name by removing invisible characters.

        Args:
            name: Raw column name from DBF file

        Returns:
            Cleaned column name (lowercase, trimmed, no invisible characters)
        """
        cleaned = name.replace('\0', '').replace('\t', '').replace('\n', '').replace('\r', '')
        return cleaned.strip().lower()

    def _insert_full_dataframe(
        self,
        conn: duckdb.DuckDBPyConnection,
        dbf_path: Path,
        table_name: str,
        uf_code: str,
        source_file: str,
    ) -> int:
        """Insert entire DBF as DataFrame (fast for small files)."""
        try:
            from simpledbf import Dbf5
            from dbfread import DBF

            # Load entire DBF into Pandas DataFrame
            df = Dbf5(str(dbf_path), codec="latin-1").to_dataframe()

            # Clean column names
            df.columns = [self._clean_column_name(col) for col in df.columns]

            # Add UF and source_file columns at the beginning
            df.insert(0, 'uf', uf_code)
            df.insert(1, 'source_file', source_file)

            # Create table with explicit VARCHAR types
            dbf = DBF(
                str(dbf_path),
                load=False,
                encoding="latin-1",
                ignore_missing_memofile=True,
            )
            self._create_staging_table(conn, dbf, table_name, uf_code, source_file)

            # Register DataFrame and insert
            conn.register("temp_full_df", df)
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_full_df")

            rows = len(df)

            # Cleanup
            conn.unregister("temp_full_df")
            del df
            gc.collect()

            return rows

        except ImportError:
            # Fallback to chunked streaming if simpledbf not available
            self.logger.warning("simpledbf not available, falling back to chunked streaming")
            return self._stream_chunks(conn, dbf_path, table_name, uf_code, source_file)

    def _stream_chunks(
        self,
        conn: duckdb.DuckDBPyConnection,
        dbf_path: Path,
        table_name: str,
        uf_code: str,
        source_file: str,
    ) -> int:
        """Stream DBF in chunks (memory efficient for large files)."""
        import pyarrow as pa
        from dbfread import DBF

        dbf = DBF(
            str(dbf_path),
            load=False,
            encoding="latin-1",
            ignore_missing_memofile=True,
        )

        # Create staging table
        self._create_staging_table(conn, dbf, table_name, uf_code, source_file)

        # Stream records in chunks
        chunk = []
        total_rows = 0

        for record in dbf:
            row = {
                self._clean_column_name(k): self._encode_value(v)
                for k, v in record.items()
            }
            chunk.append(row)

            if len(chunk) >= self.chunk_size:
                self._insert_chunk(conn, chunk, table_name, uf_code, source_file)
                total_rows += len(chunk)
                chunk = []

        # Insert remaining records
        if chunk:
            self._insert_chunk(conn, chunk, table_name, uf_code, source_file)
            total_rows += len(chunk)

        return total_rows

    def _create_staging_table(
        self,
        conn: duckdb.DuckDBPyConnection,
        dbf,
        table_name: str,
        uf_code: str,
        source_file: str,
    ) -> None:
        """Create staging table with VARCHAR columns."""
        columns = ['"uf" VARCHAR', '"source_file" VARCHAR']

        for field in dbf.fields:
            clean_name = self._clean_column_name(field.name)
            columns.append(f'"{clean_name}" VARCHAR')

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
        """
        conn.execute(create_sql)

    def _insert_chunk(
        self,
        conn: duckdb.DuckDBPyConnection,
        chunk: list[dict],
        table_name: str,
        uf_code: str,
        source_file: str,
    ) -> None:
        """Insert chunk of records into staging table."""
        import pyarrow as pa

        # Add uf and source_file to each record
        for record in chunk:
            record['uf'] = uf_code
            record['source_file'] = source_file

        # Convert to Arrow Table for zero-copy transfer
        arrow_table = pa.Table.from_pylist(chunk)
        conn.register("temp_chunk", arrow_table)
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_chunk")
        conn.unregister("temp_chunk")
        del arrow_table

    def _encode_value(self, value) -> Optional[str]:
        """Encode value from latin-1 to UTF-8."""
        if isinstance(value, bytes):
            decoded = value.decode("latin-1", errors="ignore")
            return None if decoded.strip() == "" else decoded
        elif isinstance(value, str):
            encoded = (
                value.encode("latin-1", errors="ignore")
                .decode("utf-8", errors="ignore")
            )
            return None if encoded.strip() == "" else encoded
        return value


def convert_dbf_to_parquet(
    dbf_path: Path,
    output_dir: Path,
    subsystem: str,
    schema: dict[str, str],
    raw_mode: bool = False,
    compression: str = "zstd",
) -> ParquetConversionResult:
    """Standalone function for parallel processing with ProcessPoolExecutor.

    This function is pickle-able and can be used as a target for
    ProcessPoolExecutor workers.

    Args:
        dbf_path: Path to input DBF file
        output_dir: Base directory for Parquet output
        subsystem: DataSUS subsystem name
        schema: DuckDB schema dict
        raw_mode: If True, skip type conversions
        compression: Parquet compression algorithm

    Returns:
        ParquetConversionResult with conversion details
    """
    converter = DbfToParquetConverter(
        subsystem=subsystem,
        schema=schema,
        raw_mode=raw_mode,
        compression=compression,
    )
    return converter.convert(dbf_path, output_dir)
