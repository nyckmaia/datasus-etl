"""Query engine for Parquet data using DuckDB.

Provides a simple interface for users to query partitioned Parquet files
using SQL, with results returned as Polars DataFrames.
"""

import logging
from pathlib import Path
from typing import Optional, Union

import duckdb
import polars as pl

from pydatasus.exceptions import PyInmetError


class ParquetQueryEngine:
    """Query engine for Parquet data using DuckDB.

    Creates an in-memory DuckDB database with a VIEW pointing to Parquet files.
    Users can execute SQL queries and get results as Polars DataFrames.

    The engine uses DuckDB's zero-copy Parquet reading for maximum performance.
    Queries benefit from partition pruning and columnar execution.

    Example:
        >>> engine = ParquetQueryEngine("data/parquet")
        >>> df = engine.sql('''
        ...     SELECT ano_inter, COUNT(*) as total
        ...     FROM sihsus
        ...     WHERE uf_zi = 'SP'
        ...     GROUP BY ano_inter
        ...     ORDER BY ano_inter
        ... ''')
        >>> print(df)
        shape: (10, 2)
        ┌───────────┬───────┐
        │ ano_inter ┆ total │
        │ ---       ┆ ---   │
        │ i64       ┆ i64   │
        ╞═══════════╪═══════╡
        │ 2015      ┆ 45678 │
        │ 2016      ┆ 47234 │
        └───────────┴───────┘

    Attributes:
        parquet_dir: Path to directory with partitioned Parquet files
    """

    def __init__(
        self, parquet_dir: Union[str, Path], view_name: str = "sihsus"
    ) -> None:
        """Initialize query engine.

        Args:
            parquet_dir: Path to directory with partitioned Parquet files
            view_name: Name for the DuckDB view (default: "sihsus")

        Raises:
            ValueError: If parquet_dir doesn't exist
            PyInmetError: If VIEW creation fails
        """
        self.parquet_dir = Path(parquet_dir)
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._view_name = view_name
        self.logger = logging.getLogger(__name__)

        if not self.parquet_dir.exists():
            raise ValueError(f"Parquet directory not found: {self.parquet_dir}")

        # Create in-memory DuckDB connection
        self.logger.info("Initializing DuckDB connection")
        self._conn = duckdb.connect(":memory:")

        # Register VIEW pointing to Parquet files
        self._register_parquet_view()

    def _register_parquet_view(self) -> None:
        """Register Parquet files as DuckDB VIEW.

        Creates a VIEW that reads all Parquet files in the directory tree.
        The VIEW is lazy and won't load data until queried.

        Uses hive_partitioning=true to leverage partition columns for pruning.

        Raises:
            PyInmetError: If VIEW creation fails
        """
        try:
            # Pattern to match all parquet files recursively
            parquet_pattern = str(self.parquet_dir / "**/*.parquet")

            # Create VIEW with hive partitioning support
            self._conn.execute(
                f"""
                CREATE OR REPLACE VIEW {self._view_name} AS
                SELECT * FROM read_parquet(
                    '{parquet_pattern}',
                    hive_partitioning=true
                )
            """
            )

            self.logger.info(f"Registered view '{self._view_name}' for {parquet_pattern}")

        except Exception as e:
            self.logger.error(f"Failed to register Parquet view: {e}")
            raise PyInmetError(f"Parquet VIEW creation failed: {e}") from e

    def sql(
        self, query: str, read_mode: bool = True
    ) -> Optional[pl.DataFrame]:
        """Execute SQL query on Parquet data.

        Args:
            query: SQL query string (can reference the view name)
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
            ...         ano_inter,
            ...         uf_zi,
            ...         COUNT(*) as total_internacoes,
            ...         AVG(val_tot) as valor_medio,
            ...         SUM(qt_diarias) as total_diarias
            ...     FROM sihsus
            ...     WHERE ano_inter BETWEEN 2015 AND 2020
            ...         AND sexo_descr = 'F'
            ...     GROUP BY ano_inter, uf_zi
            ...     ORDER BY ano_inter, uf_zi
            ... ''')
        """
        if not self._conn:
            raise PyInmetError("DuckDB connection not initialized")

        try:
            self.logger.debug(f"Executing query: {query[:100]}...")

            if read_mode:
                # Execute query and return as Polars DataFrame
                result = self._conn.execute(query).pl()
                self.logger.debug(f"Query returned {len(result)} rows")
                return result
            else:
                # Execute only (for DDL/DML statements)
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
            ['sihsus']
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

    def count(self) -> int:
        """Get total row count in the main view.

        Returns:
            Total number of rows

        Example:
            >>> engine.count()
            1234567
        """
        result = self.sql(f"SELECT COUNT(*) as count FROM {self._view_name}")
        return result["count"][0]

    def sample(self, n: int = 10) -> pl.DataFrame:
        """Get random sample of rows.

        Args:
            n: Number of rows to sample (default: 10)

        Returns:
            DataFrame with sampled rows

        Example:
            >>> sample = engine.sample(5)
        """
        return self.sql(
            f"SELECT * FROM {self._view_name} USING SAMPLE {n} ROWS"
        )

    def close(self) -> None:
        """Close DuckDB connection.

        Called automatically when object is deleted, but can be called
        explicitly to free resources earlier.
        """
        if self._conn:
            self._conn.close()
            self._conn = None
            self.logger.info("DuckDB connection closed")

    def __enter__(self) -> "ParquetQueryEngine":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def __repr__(self) -> str:
        """String representation."""
        status = "connected" if self._conn else "closed"
        return f"ParquetQueryEngine(dir={self.parquet_dir}, view={self._view_name}, status={status})"

    def __del__(self) -> None:
        """Cleanup connection on deletion."""
        self.close()
