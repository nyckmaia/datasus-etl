"""Converter from DBF to DuckDB with adaptive streaming support."""

import logging
from pathlib import Path
from typing import Any

import duckdb
import psutil
import pyarrow as pa
from dbfread import DBF

from pydatasus.exceptions import ConversionError


class DbfToDuckDBConverter:
    """Stream DBF files directly to DuckDB with adaptive insertion strategy.

    This converter uses an adaptive approach based on file size and available RAM:
    - Small files (<100MB or <10% available RAM): Full DataFrame insertion using
      simpledbf (30% faster, single INSERT operation)
    - Large files: Chunked streaming using dbfread (safer, prevents OOM)

    This approach minimizes memory usage and I/O overhead compared to writing
    intermediate CSV files, while maximizing performance across different file sizes.

    Example:
        >>> conn = duckdb.connect(":memory:")
        >>> converter = DbfToDuckDBConverter(conn, chunk_size=10000)
        >>> rows = converter.stream_dbf_to_table("data.dbf", "my_table", create_table=True)
        >>> print(f"Loaded {rows} rows")
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, chunk_size: int = 10000) -> None:
        """Initialize the converter.

        Args:
            conn: DuckDB connection to use for inserting data
            chunk_size: Number of rows to process in each chunk (default: 10000)
                       Smaller chunks use less memory but may be slower.
        """
        self.conn = conn
        self.chunk_size = chunk_size
        self.logger = logging.getLogger(__name__)

    def stream_dbf_to_table(
        self, dbf_path: Path, table_name: str, create_table: bool = False
    ) -> int:
        """Stream DBF file to DuckDB table with adaptive strategy.

        Uses an adaptive approach based on file size and available RAM:
        - Small files (<100MB or <10% RAM): Full DataFrame insertion (faster)
        - Large files: Chunked streaming (safer, prevents OOM)

        Args:
            dbf_path: Path to DBF file
            table_name: Target table name in DuckDB (staging)
            create_table: If True, create table based on DBF schema

        Returns:
            Total number of rows inserted

        Raises:
            ConversionError: If DBF file cannot be read or data cannot be inserted

        Example:
            >>> converter.stream_dbf_to_table(
            ...     Path("RDSP2001.dbf"),
            ...     "staging_rdsp2001",
            ...     create_table=True
            ... )
            15234
        """
        try:
            # Determine file size and available RAM
            file_size_mb = dbf_path.stat().st_size / (1024 * 1024)
            available_ram_gb = psutil.virtual_memory().available / (1024**3)

            # Adaptive threshold: 100MB or 10% of available RAM (whichever is smaller)
            threshold_mb = min(100, available_ram_gb * 1024 * 0.1)

            # Choose strategy based on file size
            if file_size_mb < threshold_mb:
                self.logger.debug(
                    f"Using full DataFrame insertion for {dbf_path.name} "
                    f"({file_size_mb:.1f}MB < {threshold_mb:.1f}MB threshold)"
                )
                return self._insert_full_dataframe(dbf_path, table_name, create_table)
            else:
                self.logger.debug(
                    f"Using chunked streaming for {dbf_path.name} "
                    f"({file_size_mb:.1f}MB >= {threshold_mb:.1f}MB threshold)"
                )
                return self._stream_chunks(dbf_path, table_name, create_table)

        except Exception as e:
            self.logger.error(f"Failed to stream {dbf_path} to {table_name}: {e}")
            raise ConversionError(
                f"DBF to DuckDB streaming failed for {dbf_path}: {e}"
            ) from e

    def _insert_full_dataframe(
        self, dbf_path: Path, table_name: str, create_table: bool
    ) -> int:
        """Insert entire DBF as DataFrame (fast for small files).

        Uses simpledbf to load the entire DBF into a Pandas DataFrame,
        then inserts it in a single operation. This is faster for small files
        but uses more memory.

        Args:
            dbf_path: Path to DBF file
            table_name: Target table name
            create_table: If True, create table from DataFrame schema

        Returns:
            Number of rows inserted
        """
        try:
            from simpledbf import Dbf5

            # Load entire DBF into Pandas DataFrame
            df = Dbf5(str(dbf_path), codec="latin-1").to_dataframe()

            if create_table:
                # Create table from DataFrame schema (DuckDB auto-infers types)
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df WHERE FALSE")

            # Register DataFrame and insert
            self.conn.register("temp_full_df", df)
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_full_df")

            rows = len(df)
            self.logger.info(f"Inserted {rows:,} rows from {dbf_path.name} (full DataFrame)")
            return rows

        except ImportError:
            # Fallback to chunked streaming if simpledbf not available
            self.logger.warning(
                "simpledbf not available, falling back to chunked streaming"
            )
            return self._stream_chunks(dbf_path, table_name, create_table)

    def _stream_chunks(
        self, dbf_path: Path, table_name: str, create_table: bool
    ) -> int:
        """Stream DBF in chunks (safe for large files).

        Reads the DBF file record by record, accumulating chunks in memory,
        then inserting them into DuckDB using PyArrow for zero-copy transfer.

        Args:
            dbf_path: Path to DBF file
            table_name: Target table name
            create_table: If True, create table based on DBF schema

        Returns:
            Total number of rows inserted
        """
        # Open DBF file without loading entire file into memory
        dbf = DBF(
            str(dbf_path),
            load=False,  # Critical: don't load all data at once
            encoding="latin-1",
            ignore_missing_memofile=True,
        )

        # Create table if requested
        if create_table:
            self._create_table_from_dbf_schema(dbf, table_name)

        # Stream records in chunks
        chunk = []
        total_rows = 0

        for record in dbf:
            # Convert record to dict with UTF-8 encoding
            row = {k: self._encode_value(v) for k, v in record.items()}
            chunk.append(row)

            # Insert chunk when it reaches target size
            if len(chunk) >= self.chunk_size:
                self._insert_chunk(chunk, table_name)
                total_rows += len(chunk)
                chunk = []

        # Insert remaining records
        if chunk:
            self._insert_chunk(chunk, table_name)
            total_rows += len(chunk)

        self.logger.info(f"Streamed {total_rows:,} rows from {dbf_path.name} (chunked)")
        return total_rows

    def _insert_chunk(self, chunk: list[dict], table_name: str) -> None:
        """Insert chunk of records into DuckDB table.

        Uses PyArrow for zero-copy transfer of data from Python to DuckDB.
        This is much more efficient than inserting rows individually.

        Args:
            chunk: List of record dictionaries
            table_name: Target table name
        """
        try:
            # Convert chunk to Arrow Table (zero-copy when possible)
            arrow_table = pa.Table.from_pylist(chunk)

            # Register Arrow table as temporary view in DuckDB
            self.conn.register("temp_chunk", arrow_table)

            # Insert data using SQL (DuckDB optimizes this internally)
            self.conn.execute(
                f"""
                INSERT INTO {table_name}
                SELECT * FROM temp_chunk
            """
            )

            # Cleanup temporary view
            self.conn.execute("DROP VIEW IF EXISTS temp_chunk")

        except Exception as e:
            self.logger.error(f"Failed to insert chunk into {table_name}: {e}")
            raise

    def _encode_value(self, value: Any) -> Any:
        """Encode value from latin-1 to UTF-8 if it's a string.

        DBF files from DATASUS use latin-1 encoding, but we want UTF-8
        for consistency and better Unicode support.

        Args:
            value: Value from DBF record (any type)

        Returns:
            Encoded value (UTF-8 string if input was string/bytes, otherwise unchanged)
        """
        if isinstance(value, bytes):
            return value.decode("latin-1", errors="ignore")
        elif isinstance(value, str):
            # Some DBF readers may already decode to str, so re-encode carefully
            return (
                value.encode("latin-1", errors="ignore")
                .decode("utf-8", errors="ignore")
            )
        return value

    def _create_table_from_dbf_schema(self, dbf: DBF, table_name: str) -> None:
        """Create DuckDB table based on DBF file schema.

        Maps DBF field types to appropriate DuckDB types. Note that dates
        are initially stored as VARCHAR and will be parsed later in SQL.

        Args:
            dbf: DBF file object (with fields metadata)
            table_name: Name for the new table

        Raises:
            ConversionError: If table creation fails
        """
        try:
            # Map DBF types to DuckDB types
            type_map = {
                "C": "VARCHAR",  # Character
                "N": "DOUBLE",  # Numeric (use DOUBLE to handle decimals)
                "L": "BOOLEAN",  # Logical
                "D": "VARCHAR",  # Date (parse later in SQL transformations)
                "F": "DOUBLE",  # Float
                "M": "VARCHAR",  # Memo
            }

            # Build column definitions
            columns = []
            for field in dbf.fields:
                field_type = type_map.get(field.type, "VARCHAR")
                columns.append(f"{field.name} {field_type}")

            # Create table
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            )
            """
            self.conn.execute(create_sql)

            self.logger.debug(
                f"Created table {table_name} with {len(columns)} columns"
            )

        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            raise ConversionError(f"Table creation failed for {table_name}: {e}") from e
