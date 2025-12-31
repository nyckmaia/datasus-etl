"""DuckDB database manager for analytical queries."""

import logging
import tempfile
from pathlib import Path
from typing import Any, Optional

import duckdb
import polars as pl
import psutil

from datasus_etl.config import DatabaseConfig
from datasus_etl.exceptions import PyInmetError


class DuckDBManager:
    """Manage DuckDB database for SIHSUS data.

    Provides SQL query interface, views creation, and data export
    capabilities using DuckDB's analytical query engine.
    """

    def __init__(self, config: DatabaseConfig) -> None:
        """Initialize the database manager.

        Args:
            config: Database configuration
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> None:
        """Connect to DuckDB database.

        Creates a new database file or connects to existing one.
        If db_path is None, creates an in-memory database.
        """
        if self._conn is not None:
            self.logger.warning("Already connected to database")
            return

        db_path_str = str(self.config.db_path) if self.config.db_path else ":memory:"

        self.logger.info(f"Connecting to DuckDB: {db_path_str}")

        try:
            self._conn = duckdb.connect(
                database=db_path_str,
                read_only=self.config.read_only,
            )

            # Configure memory limits to prevent OOM
            available_ram_gb = psutil.virtual_memory().available / (1024**3)
            # Use 60% of available RAM, minimum 2GB
            memory_limit_gb = max(2, int(available_ram_gb * 0.6))
            self._conn.execute(f"SET memory_limit = '{memory_limit_gb}GB'")
            self.logger.info(f"DuckDB memory limit set to {memory_limit_gb}GB")

            # Configure threads
            threads = self.config.threads or psutil.cpu_count()
            self._conn.execute(f"SET threads TO {threads}")
            self.logger.info(f"DuckDB threads set to {threads}")

            # Configure temp directory for spilling
            temp_dir = Path(tempfile.gettempdir()) / "datasus_etl_duckdb_temp"
            temp_dir.mkdir(parents=True, exist_ok=True)
            self._conn.execute(f"SET temp_directory = '{temp_dir}'")
            self.logger.info(f"DuckDB temp directory: {temp_dir}")

            self.logger.info("Connected to DuckDB successfully")

        except Exception as e:
            self.logger.error(f"Failed to connect to DuckDB: {e}")
            raise PyInmetError(f"Database connection failed: {e}") from e

    def disconnect(self) -> None:
        """Disconnect from database."""
        if self._conn:
            self._conn.close()
            self._conn = None
            self.logger.info("Disconnected from DuckDB")

    def __enter__(self) -> "DuckDBManager":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.disconnect()

    def execute(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query.

        Args:
            query: SQL query string

        Returns:
            Query result relation

        Raises:
            PyInmetError: If query execution fails
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        self.logger.debug(f"Executing query: {query[:100]}...")

        try:
            return self._conn.execute(query)
        except Exception as e:
            self.logger.error(f"Query failed: {e}")
            raise PyInmetError(f"Query execution failed: {e}") from e

    def query(self, sql: str) -> pl.DataFrame:
        """Execute query and return result as Polars DataFrame.

        Args:
            sql: SQL query string

        Returns:
            Query result as Polars DataFrame

        Raises:
            PyInmetError: If query fails
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        self.logger.debug(f"Querying: {sql[:100]}...")

        try:
            result = self._conn.execute(sql).pl()
            self.logger.debug(f"Query returned {len(result)} rows")
            return result
        except Exception as e:
            self.logger.error(f"Query failed: {e}")
            raise PyInmetError(f"Query failed: {e}") from e

    def register_parquet(
        self,
        parquet_path: Path,
        table_name: str,
        replace: bool = True,
    ) -> None:
        """Register a Parquet file/directory as a table.

        Args:
            parquet_path: Path to Parquet file or directory
            table_name: Name for the table
            replace: Replace existing table if True

        Raises:
            PyInmetError: If registration fails
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        if not parquet_path.exists():
            raise PyInmetError(f"Parquet path does not exist: {parquet_path}")

        self.logger.info(f"Registering Parquet as table '{table_name}'")

        try:
            # Determine if it's a file or directory
            if parquet_path.is_file():
                parquet_pattern = str(parquet_path)
            else:
                parquet_pattern = str(parquet_path / "**/*.parquet")

            # Create or replace view
            action = "CREATE OR REPLACE" if replace else "CREATE"
            query = f"""
            {action} VIEW {table_name} AS
            SELECT * FROM read_parquet('{parquet_pattern}')
            """

            self._conn.execute(query)
            self.logger.info(f"Registered table: {table_name}")

        except Exception as e:
            self.logger.error(f"Failed to register Parquet: {e}")
            raise PyInmetError(f"Parquet registration failed: {e}") from e

    def register_csv(
        self,
        csv_path: Path,
        table_name: str,
        replace: bool = True,
        delimiter: str = ";",
    ) -> None:
        """Register a CSV file as a table.

        Args:
            csv_path: Path to CSV file
            table_name: Name for the table
            replace: Replace existing table if True
            delimiter: CSV delimiter

        Raises:
            PyInmetError: If registration fails
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        if not csv_path.exists():
            raise PyInmetError(f"CSV file does not exist: {csv_path}")

        self.logger.info(f"Registering CSV as table '{table_name}'")

        try:
            action = "CREATE OR REPLACE" if replace else "CREATE"
            query = f"""
            {action} VIEW {table_name} AS
            SELECT * FROM read_csv('{csv_path}', delim='{delimiter}', header=true)
            """

            self._conn.execute(query)
            self.logger.info(f"Registered table: {table_name}")

        except Exception as e:
            self.logger.error(f"Failed to register CSV: {e}")
            raise PyInmetError(f"CSV registration failed: {e}") from e

    def list_tables(self) -> list[str]:
        """List all tables in the database.

        Returns:
            List of table names
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        result = self._conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def table_info(self, table_name: str) -> pl.DataFrame:
        """Get information about a table.

        Args:
            table_name: Name of the table

        Returns:
            DataFrame with column information
        """
        query = f"DESCRIBE {table_name}"
        return self.query(query)

    def table_stats(self, table_name: str) -> dict[str, Any]:
        """Get statistics about a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table statistics
        """
        # Row count
        count_result = self.query(f"SELECT COUNT(*) as count FROM {table_name}")
        row_count = count_result["count"][0]

        # Column count
        info = self.table_info(table_name)
        col_count = len(info)

        return {
            "table_name": table_name,
            "row_count": row_count,
            "column_count": col_count,
            "columns": info["column_name"].to_list(),
        }

    def export_query_to_csv(
        self,
        query: str,
        output_file: Path,
        delimiter: str = ";",
    ) -> None:
        """Execute query and export results to CSV.

        Args:
            query: SQL query
            output_file: Output CSV file path
            delimiter: CSV delimiter
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        self.logger.info(f"Exporting query results to {output_file}")

        try:
            export_query = f"""
            COPY ({query})
            TO '{output_file}'
            (HEADER, DELIMITER '{delimiter}')
            """

            self._conn.execute(export_query)
            self.logger.info(f"Exported to {output_file}")

        except Exception as e:
            self.logger.error(f"Export failed: {e}")
            raise PyInmetError(f"Export to CSV failed: {e}") from e

    def __repr__(self) -> str:
        """String representation."""
        db_path = self.config.db_path or ":memory:"
        status = "connected" if self._conn else "disconnected"
        return f"DuckDBManager(db_path={db_path}, status={status})"
