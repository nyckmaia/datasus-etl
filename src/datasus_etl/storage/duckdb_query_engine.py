"""Query engine for DuckDB persistent databases.

Provides a simple interface for users to query DuckDB database files
using SQL, with results returned as Polars DataFrames.
"""

import logging
from pathlib import Path
from typing import Optional, Union

import duckdb
import polars as pl

from datasus_etl.exceptions import PyInmetError


class DuckDBQueryEngine:
    """Query engine for DuckDB persistent database files.

    Opens a connection to a DuckDB database file and provides SQL query interface.
    Uses the enrichment VIEW by default for queries (includes dimension JOINs).

    Example:
        >>> engine = DuckDBQueryEngine("data/sihsus.duckdb")
        >>> df = engine.sql('''
        ...     SELECT ano_cmpt, COUNT(*) as total
        ...     FROM sihsus
        ...     WHERE uf = 'SP'
        ...     GROUP BY ano_cmpt
        ...     ORDER BY ano_cmpt
        ... ''')
        >>> print(df)
        shape: (10, 2)
        ┌──────────┬───────┐
        │ ano_cmpt ┆ total │
        │ ---      ┆ ---   │
        │ i64      ┆ i64   │
        ╞══════════╪═══════╡
        │ 2023     ┆ 45678 │
        │ 2024     ┆ 47234 │
        └──────────┴───────┘

    Attributes:
        db_path: Path to the DuckDB database file
        subsystem: Name of the subsystem (derived from filename)
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        read_only: bool = True,
    ) -> None:
        """Initialize query engine.

        Args:
            db_path: Path to the DuckDB database file (e.g., "sihsus.duckdb")
            read_only: Open database in read-only mode (default: True)

        Raises:
            ValueError: If db_path doesn't exist
            PyInmetError: If connection fails
        """
        self.db_path = Path(db_path)
        self._read_only = read_only
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self.logger = logging.getLogger(__name__)

        if not self.db_path.exists():
            raise ValueError(f"Database file not found: {self.db_path}")

        # Derive subsystem name from filename (e.g., "sihsus" from "sihsus.duckdb")
        self._subsystem = self.db_path.stem
        self._view_name = self._subsystem
        self._raw_table = f"{self._subsystem}_raw"

        # Open connection
        self._connect()

    def _connect(self) -> None:
        """Open connection to DuckDB database.

        Raises:
            PyInmetError: If connection fails
        """
        try:
            self.logger.info(f"Connecting to DuckDB: {self.db_path}")
            self._conn = duckdb.connect(
                str(self.db_path),
                read_only=self._read_only
            )
            self.logger.info("Connected to DuckDB successfully")

        except Exception as e:
            self.logger.error(f"Failed to connect to DuckDB: {e}")
            raise PyInmetError(f"Database connection failed: {e}") from e

    @property
    def subsystem(self) -> str:
        """Get the subsystem name."""
        return self._subsystem

    def sql(
        self, query: str, read_mode: bool = True
    ) -> Optional[pl.DataFrame]:
        """Execute SQL query on the database.

        Args:
            query: SQL query string
            read_mode: If True, return results as DataFrame.
                      If False, execute only (for DDL/DML).

        Returns:
            Polars DataFrame with query results (if read_mode=True),
            None otherwise

        Raises:
            PyInmetError: If query execution fails

        Example:
            >>> df = engine.sql('''
            ...     SELECT
            ...         ano_cmpt,
            ...         uf,
            ...         COUNT(*) as total_internacoes,
            ...         AVG(val_tot) as valor_medio
            ...     FROM sihsus
            ...     WHERE ano_cmpt >= 2023
            ...     GROUP BY ano_cmpt, uf
            ...     ORDER BY ano_cmpt, uf
            ... ''')
        """
        if not self._conn:
            raise PyInmetError("DuckDB connection not initialized")

        try:
            self.logger.debug(f"Executing query: {query[:100]}...")

            if read_mode:
                result = self._conn.execute(query).pl()
                self.logger.debug(f"Query returned {len(result)} rows")
                return result
            else:
                self._conn.execute(query)
                self.logger.debug("Query executed successfully")
                return None

        except Exception as e:
            self.logger.error(f"Query failed: {e}")
            raise PyInmetError(f"Query execution failed: {e}") from e

    def tables(self) -> list[str]:
        """List available tables/views in the database.

        Returns:
            List of table/view names

        Example:
            >>> engine.tables()
            ['sihsus_raw', 'dim_municipios', 'sihsus']
        """
        if not self._conn:
            raise PyInmetError("DuckDB connection not initialized")

        result = self._conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def schema(self, table_name: Optional[str] = None) -> pl.DataFrame:
        """Get schema of table/view.

        Args:
            table_name: Table name (defaults to main view)

        Returns:
            DataFrame with column information (name, type, null, key, default, extra)

        Example:
            >>> schema = engine.schema()
            >>> print(schema)
            shape: (50, 6)
            ┌─────────────┬──────────┬─────┬─────┬─────────┬───────┐
            │ column_name ┆ type     ┆ ... │
        """
        if not self._conn:
            raise PyInmetError("DuckDB connection not initialized")

        table = table_name or self._view_name
        return self._conn.execute(f"DESCRIBE {table}").pl()

    def count(self, use_raw: bool = False) -> int:
        """Get total row count.

        Args:
            use_raw: If True, count from raw table. If False, count from view.

        Returns:
            Total number of rows

        Example:
            >>> engine.count()
            1234567
        """
        table = self._raw_table if use_raw else self._view_name
        result = self.sql(f"SELECT COUNT(*) as count FROM {table}")
        return result["count"][0]

    def sample(self, n: int = 10, use_raw: bool = False) -> pl.DataFrame:
        """Get random sample of rows.

        Args:
            n: Number of rows to sample (default: 10)
            use_raw: If True, sample from raw table. If False, sample from view.

        Returns:
            DataFrame with sampled rows

        Example:
            >>> sample = engine.sample(5)
        """
        table = self._raw_table if use_raw else self._view_name
        return self.sql(
            f"SELECT * FROM {table} USING SAMPLE {n} ROWS"
        )

    def get_processed_source_files(self) -> set[str]:
        """Get set of source files already in the database.

        This method queries the 'source_file' column from the raw table
        to find which DBC files have already been processed.
        Useful for incremental updates.

        Returns:
            Set of source file names (e.g., {"RDSP2301.dbc", "RDRJ2301.dbc"})

        Raises:
            PyInmetError: If query fails
        """
        if not self._conn:
            raise PyInmetError("DuckDB connection not initialized")

        try:
            result = self.sql(
                f"SELECT DISTINCT source_file FROM {self._raw_table} "
                f"WHERE source_file IS NOT NULL"
            )

            if result is None or len(result) == 0:
                return set()

            return set(result["source_file"].to_list())

        except Exception as e:
            self.logger.warning(f"Failed to get processed source files: {e}")
            return set()

    def get_file_row_counts(self) -> dict[str, int]:
        """Get row count per source file.

        Returns:
            Dictionary mapping source_file -> row count

        Example:
            >>> engine.get_file_row_counts()
            {'RDSP2301.dbc': 12345, 'RDRJ2301.dbc': 67890}
        """
        if not self._conn:
            raise PyInmetError("DuckDB connection not initialized")

        try:
            result = self.sql(
                f"""
                SELECT source_file, COUNT(*) as row_count
                FROM {self._raw_table}
                WHERE source_file IS NOT NULL
                GROUP BY source_file
                ORDER BY source_file
                """
            )

            if result is None or len(result) == 0:
                return {}

            return dict(zip(result["source_file"].to_list(), result["row_count"].to_list()))

        except Exception as e:
            self.logger.error(f"Failed to get file row counts: {e}")
            return {}

    def get_uf_stats(self) -> pl.DataFrame:
        """Get statistics by UF (state).

        Returns:
            DataFrame with row counts per UF

        Example:
            >>> engine.get_uf_stats()
            shape: (27, 2)
            ┌─────┬───────────┐
            │ uf  ┆ registros │
            ├─────┼───────────┤
            │ SP  ┆ 1234567   │
            │ RJ  ┆  987654   │
            └─────┴───────────┘
        """
        return self.sql(
            f"""
            SELECT uf, COUNT(*) as registros
            FROM {self._raw_table}
            GROUP BY uf
            ORDER BY registros DESC
            """
        )

    def get_dimension_status(self) -> dict[str, int]:
        """Get row counts for all dimension tables.

        Returns:
            Dict mapping dimension table name to row count (-1 if not exists)

        Example:
            >>> engine.get_dimension_status()
            {'dim_municipios': 5571, 'dim_procedimentos': -1}
        """
        status = {}
        dimension_tables = [
            "dim_municipios",
            "dim_procedimentos",
            "dim_cid10",
            "dim_ocupacoes",
        ]

        for table in dimension_tables:
            try:
                result = self._conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()
                status[table] = result[0] if result else 0
            except duckdb.CatalogException:
                status[table] = -1
            except Exception as e:
                self.logger.warning(f"Error checking {table}: {e}")
                status[table] = -1

        return status

    def get_database_info(self) -> dict:
        """Get information about the database.

        Returns:
            Dictionary with database metadata

        Example:
            >>> engine.get_database_info()
            {'path': 'sihsus.duckdb', 'size_mb': 123.4, 'subsystem': 'sihsus', ...}
        """
        info = {
            "path": str(self.db_path),
            "size_bytes": self.db_path.stat().st_size,
            "size_mb": self.db_path.stat().st_size / (1024 * 1024),
            "subsystem": self._subsystem,
            "view_name": self._view_name,
            "raw_table": self._raw_table,
            "tables": self.tables(),
            "row_count": self.count(use_raw=True),
            "dimensions": self.get_dimension_status(),
        }
        return info

    def close(self) -> None:
        """Close DuckDB connection.

        Called automatically when object is deleted, but can be called
        explicitly to free resources earlier.
        """
        if self._conn:
            self._conn.close()
            self._conn = None
            self.logger.info("DuckDB connection closed")

    def __enter__(self) -> "DuckDBQueryEngine":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def __repr__(self) -> str:
        """String representation."""
        status = "connected" if self._conn else "closed"
        return f"DuckDBQueryEngine(db={self.db_path.name}, subsystem={self._subsystem}, status={status})"

    def __del__(self) -> None:
        """Cleanup connection on deletion."""
        self.close()
