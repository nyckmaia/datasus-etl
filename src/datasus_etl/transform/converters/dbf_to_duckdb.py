"""Converter from DBF to DuckDB with adaptive streaming support."""

import logging
from pathlib import Path
from typing import Any

import duckdb
import psutil
import pyarrow as pa
from dbfread import DBF

from datasus_etl.exceptions import ConversionError


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

    def __init__(self, conn: duckdb.DuckDBPyConnection, chunk_size: int = 10000, dataframe_threshold_mb: int = 250) -> None:
        """Initialize the converter.

        Args:
            conn: DuckDB connection to use for inserting data
            chunk_size: Number of rows to process in each chunk (default: 10000)
                       Smaller chunks use less memory but may be slower.
            dataframe_threshold_mb: File size threshold (MB) for using full DataFrame vs chunked streaming (default: 250MB)
        """
        self.conn = conn
        self.chunk_size = chunk_size
        self.dataframe_threshold_mb = dataframe_threshold_mb
        self.logger = logging.getLogger(__name__)

    def _clean_column_name(self, name: str) -> str:
        """Clean DBF column name by removing invisible characters and normalizing.

        DBF files from DATASUS may contain corrupted column names with invisible
        characters like null bytes (\0), tabs (\t), newlines (\n), and carriage
        returns (\r) embedded in the field names.

        Example:
            'VAL_UCI\0CI' → 'val_uci'
            'MUNIC_RES\t' → 'munic_res'
            ' DIAG_PRINC \n' → 'diag_princ'

        Args:
            name: Raw column name from DBF file

        Returns:
            Cleaned column name (lowercase, trimmed, no invisible characters)
        """
        # Remove invisible characters
        cleaned = name.replace('\0', '')  # Null bytes
        cleaned = cleaned.replace('\t', '')  # Tabs
        cleaned = cleaned.replace('\n', '')  # Newlines
        cleaned = cleaned.replace('\r', '')  # Carriage returns

        # Convert to lowercase and trim whitespace
        cleaned = cleaned.strip().lower()

        return cleaned

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
        from tqdm import tqdm
        try:
            tqdm.write(f"\n[DEBUG] ========== STARTING {dbf_path.name} ==========")

            # Extract UF code and source_file from filename (e.g., RDSP2401.dbf → "SP", "RDSP2401.dbc")
            filename = dbf_path.stem.upper()  # "RDSP2401"
            uf_code = filename[2:4] if len(filename) >= 4 else None  # "SP"
            source_file = f"{filename}.dbc"  # Original DBC filename (e.g., "RDSP2401.dbc")

            if uf_code:
                self.logger.debug(f"Extracted UF code from filename: {uf_code}")
                self.logger.debug(f"Source file: {source_file}")

            # Determine file size and available RAM
            file_size_mb = dbf_path.stat().st_size / (1024 * 1024)
            available_ram_gb = psutil.virtual_memory().available / (1024**3)
            tqdm.write(f"[DEBUG] File: {file_size_mb:.1f}MB, Available RAM: {available_ram_gb:.1f}GB")

            # Adaptive threshold: configured threshold or 20% of available RAM (whichever is smaller)
            threshold_mb = min(self.dataframe_threshold_mb, available_ram_gb * 1024 * 0.2)

            # Choose strategy based on file size
            if file_size_mb < threshold_mb:
                # self.logger.info(
                #     f"Strategy: Full DataFrame insertion for {dbf_path.name} "
                #     f"({file_size_mb:.1f}MB, threshold={threshold_mb:.1f}MB)"
                # )
                result = self._insert_full_dataframe(dbf_path, table_name, create_table, uf_code, source_file)
                # tqdm.write(f"[DEBUG] ========== COMPLETED {dbf_path.name} ({result:,} rows) ==========\n")
                return result
            else:
                self.logger.info(
                    f"Strategy: Chunked streaming for {dbf_path.name} "
                    f"({file_size_mb:.1f}MB, threshold={threshold_mb:.1f}MB, {self.chunk_size:,} rows/chunk)"
                )
                result = self._stream_chunks(dbf_path, table_name, create_table, uf_code, source_file)
                # tqdm.write(f"[DEBUG] ========== COMPLETED {dbf_path.name} ({result:,} rows) ==========\n")
                return result

        except Exception as e:
            self.logger.error(f"Failed to stream {dbf_path} to {table_name}: {e}")
            raise ConversionError(
                f"DBF to DuckDB streaming failed for {dbf_path}: {e}"
            ) from e

    def _insert_full_dataframe(
        self, dbf_path: Path, table_name: str, create_table: bool, uf_code: str = None, source_file: str = None
    ) -> int:
        """Insert entire DBF as DataFrame (fast for small files).

        Uses simpledbf to load the entire DBF into a Pandas DataFrame,
        then inserts it in a single operation. This is faster for small files
        but uses more memory.

        Args:
            dbf_path: Path to DBF file
            table_name: Target table name
            create_table: If True, create table from DataFrame schema
            uf_code: UF state code to add as column
            source_file: Original DBC filename to add as column

        Returns:
            Number of rows inserted
        """
        try:
            from simpledbf import Dbf5
            from tqdm import tqdm

            # Load entire DBF into Pandas DataFrame
            file_size_mb = dbf_path.stat().st_size / (1024 * 1024)
            self.logger.info(f"Loading {dbf_path.name} into DataFrame ({file_size_mb:.1f}MB)...")
            tqdm.write(f"[DEBUG] Starting Dbf5 read for {dbf_path.name}...")
            df = Dbf5(str(dbf_path), codec="latin-1").to_dataframe()
            tqdm.write(f"[DEBUG] Dbf5 read complete: {len(df):,} rows loaded")

            # Clean column names immediately after loading
            df.columns = [self._clean_column_name(col) for col in df.columns]
            tqdm.write(f"[DEBUG] Cleaned {len(df.columns)} column names")

            # Add UF column at the beginning
            if uf_code:
                # tqdm.write(f"[DEBUG] Adding UF column: {uf_code}")
                df.insert(0, 'uf', uf_code)  # lowercase 'uf' to match cleaning convention

            # Add source_file column after UF
            if source_file:
                df.insert(1, 'source_file', source_file)  # Original DBC filename

            if create_table:
                # Create table with explicit VARCHAR types to match chunked path
                # This avoids DuckDB auto-inferring numeric types from Pandas
                tqdm.write(f"[DEBUG] Creating table {table_name} with explicit VARCHAR types...")
                # Open DBF to get field metadata
                from dbfread import DBF
                dbf = DBF(
                    str(dbf_path),
                    load=False,
                    encoding="latin-1",
                    ignore_missing_memofile=True,
                )
                self._create_table_from_dbf_schema(dbf, table_name, uf_code, source_file)
                # tqdm.write(f"[DEBUG] Table {table_name} created")

            # Register DataFrame and insert
            # tqdm.write(f"[DEBUG] Registering DataFrame temp_full_df...")
            self.conn.register("temp_full_df", df)
            # tqdm.write(f"[DEBUG] Executing INSERT INTO {table_name}...")
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_full_df")
            # tqdm.write(f"[DEBUG] INSERT completed for {table_name}")

            rows = len(df)
            self.logger.info(f"Inserted {rows:,} rows from {dbf_path.name} (full DataFrame)")
            # tqdm.write(f"[DEBUG] Cleaning up temp_full_df...")
            # Cleanup
            self.conn.unregister("temp_full_df")
            # tqdm.write(f"[DEBUG] Cleanup complete for {dbf_path.name}")
            return rows

        except ImportError:
            # Fallback to chunked streaming if simpledbf not available
            self.logger.warning(
                "simpledbf not available, falling back to chunked streaming"
            )
            return self._stream_chunks(dbf_path, table_name, create_table, uf_code)

    def _stream_chunks(
        self, dbf_path: Path, table_name: str, create_table: bool, uf_code: str = None, source_file: str = None
    ) -> int:
        """Stream DBF in chunks (safe for large files).

        Reads the DBF file record by record, accumulating chunks in memory,
        then inserting them into DuckDB using PyArrow for zero-copy transfer.

        Args:
            dbf_path: Path to DBF file
            table_name: Target table name
            create_table: If True, create table based on DBF schema
            uf_code: UF state code to add as column
            source_file: Original DBC filename to add as column

        Returns:
            Total number of rows inserted
        """
        from tqdm import tqdm

        # Open DBF file without loading entire file into memory
        tqdm.write(f"[DEBUG] Opening DBF file {dbf_path.name} for chunked streaming...")
        dbf = DBF(
            str(dbf_path),
            load=False,  # Critical: don't load all data at once
            encoding="latin-1",
            ignore_missing_memofile=True,
        )
        tqdm.write(f"[DEBUG] DBF file opened, {len(dbf.fields)} fields detected")

        # Create table if requested
        if create_table:
            tqdm.write(f"[DEBUG] Creating table {table_name} from DBF schema...")
            self._create_table_from_dbf_schema(dbf, table_name, uf_code, source_file)
            tqdm.write(f"[DEBUG] Table {table_name} created")

        # Stream records in chunks
        chunk = []
        total_rows = 0
        tqdm.write(f"[DEBUG] Starting record iteration for {dbf_path.name}...")

        for record in dbf:
            # Convert record to dict with UTF-8 encoding AND clean column names
            row = {
                self._clean_column_name(k): self._encode_value(v)
                for k, v in record.items()
            }
            chunk.append(row)

            # Insert chunk when it reaches target size
            if len(chunk) >= self.chunk_size:
                tqdm.write(f"[DEBUG] Chunk ready ({len(chunk):,} rows), inserting into {table_name}...")
                self._insert_chunk(chunk, table_name, uf_code, source_file)
                total_rows += len(chunk)

                # Log progress every 5 chunks (50K rows with default chunk_size)
                if (total_rows // self.chunk_size) % 5 == 0:
                    self.logger.info(f"  Processed {total_rows:,} rows from {dbf_path.name}...")

                chunk = []

        # Insert remaining records
        if chunk:
            tqdm.write(f"[DEBUG] Inserting final chunk ({len(chunk):,} rows)...")
            self._insert_chunk(chunk, table_name, uf_code, source_file)
            total_rows += len(chunk)
            self.logger.info(f"  Inserted final {len(chunk):,} rows")

        self.logger.info(f"Streamed {total_rows:,} rows from {dbf_path.name} (chunked)")
        tqdm.write(f"[DEBUG] Chunked streaming complete for {dbf_path.name}")
        return total_rows

    def _insert_chunk(self, chunk: list[dict], table_name: str, uf_code: str = None, source_file: str = None) -> None:
        """Insert chunk of records into DuckDB table.

        Uses PyArrow for zero-copy transfer of data from Python to DuckDB.
        This is much more efficient than inserting rows individually.

        Args:
            chunk: List of record dictionaries
            table_name: Target table name
            uf_code: UF state code to add to all records
            source_file: Original DBC filename to add to all records
        """
        from tqdm import tqdm
        try:
            # Add UF column to each record
            if uf_code:
                tqdm.write(f"[DEBUG] Adding uf={uf_code} to {len(chunk):,} records...")
                for record in chunk:
                    record['uf'] = uf_code  # lowercase 'uf' for consistency

            # Add source_file column to each record
            if source_file:
                for record in chunk:
                    record['source_file'] = source_file

            # Convert chunk to Arrow Table (zero-copy when possible)
            tqdm.write(f"[DEBUG] Converting {len(chunk):,} records to Arrow Table...")
            arrow_table = pa.Table.from_pylist(chunk)
            tqdm.write(f"[DEBUG] Arrow Table created: {arrow_table.num_rows:,} rows, {arrow_table.num_columns} columns")

            # Register Arrow table as temporary view in DuckDB
            tqdm.write(f"[DEBUG] Registering temp_chunk in DuckDB...")
            self.conn.register("temp_chunk", arrow_table)

            # Insert data using SQL (DuckDB optimizes this internally)
            tqdm.write(f"[DEBUG] Executing INSERT INTO {table_name} SELECT * FROM temp_chunk...")
            self.conn.execute(
                f"""
                INSERT INTO {table_name}
                SELECT * FROM temp_chunk
            """
            )
            tqdm.write(f"[DEBUG] INSERT completed")

            # Cleanup temporary view
            tqdm.write(f"[DEBUG] Cleaning up temp_chunk view...")
            self.conn.execute("DROP VIEW IF EXISTS temp_chunk")
            tqdm.write(f"[DEBUG] temp_chunk view dropped")

        except Exception as e:
            # Enhanced error logging with schema information
            if chunk:
                sample_record = chunk[0]
                column_info = []
                for col_name, col_value in sample_record.items():
                    value_type = type(col_value).__name__
                    value_preview = str(col_value)[:50] if col_value is not None else "NULL"
                    column_info.append(f"{col_name}={value_preview} ({value_type})")

                self.logger.error(
                    f"Failed to insert chunk into {table_name}: {e}\n"
                    f"Sample record columns: {', '.join(column_info[:5])}..."
                )
            else:
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
            Empty strings are converted to None for safe type casting
        """
        if isinstance(value, bytes):
            decoded = value.decode("latin-1", errors="ignore")
            # Convert empty strings to None for safe NULL insertion
            return None if decoded.strip() == "" else decoded
        elif isinstance(value, str):
            # Some DBF readers may already decode to str, so re-encode carefully
            encoded = (
                value.encode("latin-1", errors="ignore")
                .decode("utf-8", errors="ignore")
            )
            # Convert empty strings to None for safe NULL insertion
            return None if encoded.strip() == "" else encoded
        return value

    def _create_table_from_dbf_schema(self, dbf: DBF, table_name: str, uf_code: str = None, source_file: str = None) -> None:
        """Create DuckDB table based on DBF file schema.

        Maps DBF field types to appropriate DuckDB types. Note that dates
        are initially stored as VARCHAR and will be parsed later in SQL.

        Args:
            dbf: DBF file object (with fields metadata)
            table_name: Name for the new table
            uf_code: UF state code to add as column
            source_file: Original DBC filename to add as column

        Raises:
            ConversionError: If table creation fails
        """
        try:
            # Map DBF types to DuckDB types
            # IMPORTANT: Use VARCHAR for most types to avoid conversion errors during DBF import.
            # The SQL transformation stage will handle proper type conversion with TRY_CAST.
            type_map = {
                "C": "VARCHAR",  # Character
                "N": "VARCHAR",  # Numeric (stored as VARCHAR, converted later in SQL)
                "L": "VARCHAR",  # Logical (stored as VARCHAR, converted later in SQL)
                "D": "VARCHAR",  # Date (parse later in SQL transformations)
                "F": "VARCHAR",  # Float (stored as VARCHAR, converted later in SQL)
                "M": "VARCHAR",  # Memo
            }

            # Build column definitions
            columns = []

            # Add UF column at the beginning if provided
            if uf_code:
                columns.append("uf VARCHAR")  # lowercase 'uf' for consistency

            # Add source_file column after UF
            if source_file:
                columns.append("source_file VARCHAR")  # Original DBC filename

            for field in dbf.fields:
                field_type = type_map.get(field.type, "VARCHAR")
                clean_name = self._clean_column_name(field.name)  # Clean column name
                columns.append(f"{clean_name} {field_type}")

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
