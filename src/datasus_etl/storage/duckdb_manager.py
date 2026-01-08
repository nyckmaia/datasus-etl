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
from datasus_etl.transform.sql.sim_descriptive_mappings import (
    SIM_DESCRIPTIVE_MAPPINGS,
    get_descriptive_case_sql,
)


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

    def initialize_raw_table(
        self,
        subsystem: str,
        schema: dict[str, str],
    ) -> None:
        """Initialize the raw data table for a subsystem.

        Creates the table if it doesn't exist with the specified schema.
        Includes 'uf' and 'source_file' columns for tracking.

        Args:
            subsystem: DataSUS subsystem name (sihsus, sim, siasus, etc)
            schema: Dictionary mapping column names to DuckDB SQL types

        Raises:
            PyInmetError: If table creation fails
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        table_name = f"{subsystem}_raw"
        self.logger.info(f"Initializing raw table: {table_name}")

        try:
            # Build column definitions from schema
            # Quote column names to handle SQL reserved words (e.g., 'natural')
            columns = []
            for col_name, col_type in schema.items():
                columns.append(f'"{col_name}" {col_type}')

            columns_sql = ",\n    ".join(columns)

            # Create table if not exists
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_sql}
            )
            """
            self._conn.execute(create_sql)

            # Create index on source_file for deduplication
            index_sql = f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_source_file
            ON {table_name}(source_file)
            """
            self._conn.execute(index_sql)

            self.logger.info(f"Raw table '{table_name}' initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize raw table: {e}")
            raise PyInmetError(f"Raw table initialization failed: {e}") from e

    def initialize_dimension_tables(self) -> None:
        """Initialize dimension tables for enrichment.

        Creates the following dimension tables if they don't exist:
        - dim_municipios: IBGE municipality codes and names

        Additional dimension tables can be added in the future.

        Raises:
            PyInmetError: If table creation fails
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        self.logger.info("Initializing dimension tables")

        try:
            # dim_municipios - IBGE municipality data
            self._conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_municipios (
                codigo_municipio INTEGER PRIMARY KEY,
                nome VARCHAR,
                uf VARCHAR,
                regiao_imediata VARCHAR,
                regiao_intermediaria VARCHAR
            )
            """)
            self.logger.info("Dimension table 'dim_municipios' initialized")

            # Future dimension tables can be added here:
            # - dim_procedimentos (SIGTAP)
            # - dim_cid10 (ICD-10 codes)
            # - dim_ocupacoes (CBO occupations)

        except Exception as e:
            self.logger.error(f"Failed to initialize dimension tables: {e}")
            raise PyInmetError(f"Dimension table initialization failed: {e}") from e

    def create_enrichment_view(self, subsystem: str) -> None:
        """Create or replace the enrichment VIEW for a subsystem.

        Creates a VIEW that joins the raw table with dimension tables
        to provide enriched data with descriptive columns.

        Args:
            subsystem: DataSUS subsystem name (sihsus, sim, siasus, etc)

        Raises:
            PyInmetError: If VIEW creation fails
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        raw_table = f"{subsystem}_raw"
        view_name = subsystem

        self.logger.info(f"Creating enrichment VIEW: {view_name}")

        try:
            # For SIM subsystem, create VIEW with descriptive columns
            if subsystem.lower() == "sim":
                self.logger.info(
                    f"Creating SIM enrichment VIEW with descriptive columns"
                )
                # Get existing columns in sim_raw
                existing_cols = {
                    row[0].lower()
                    for row in self._conn.execute(
                        f"SELECT column_name FROM information_schema.columns "
                        f"WHERE table_name = '{raw_table}'"
                    ).fetchall()
                }

                # Build descriptive column expressions only for existing columns
                desc_columns = []
                for source_col in SIM_DESCRIPTIVE_MAPPINGS:
                    if source_col in existing_cols:
                        desc_col = f"{source_col}_desc"
                        sql_expr = get_descriptive_case_sql(source_col, f'r."{source_col}"')
                        desc_columns.append(f'{sql_expr} AS "{desc_col}"')

                if desc_columns:
                    desc_columns_sql = ",\n                    ".join(desc_columns)
                    view_sql = f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT
                        r.*,
                        {desc_columns_sql}
                    FROM {raw_table} r
                    """
                else:
                    # No descriptive columns to add
                    view_sql = f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT * FROM {raw_table}
                    """

                self._conn.execute(view_sql)
                self.logger.info(f"SIM VIEW '{view_name}' created with {len(desc_columns)} descriptive columns")
                return

            # Check if dim_municipios has data
            dim_count = self._conn.execute(
                "SELECT COUNT(*) FROM dim_municipios"
            ).fetchone()[0]

            if dim_count > 0:
                # Create VIEW with IBGE enrichment
                # These columns are added via JOIN and not stored in the raw table
                view_sql = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT
                    r.*,
                    m.nome AS municipio_res,
                    m.uf AS uf_res,
                    m.regiao_imediata AS rg_imediata_res,
                    m.regiao_intermediaria AS rg_intermediaria_res
                FROM {raw_table} r
                LEFT JOIN dim_municipios m ON r.munic_res = m.codigo_municipio
                """
            else:
                # Create simple VIEW without joins (no dimension data yet)
                self.logger.warning(
                    "dim_municipios is empty. Creating VIEW without enrichment."
                )
                view_sql = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM {raw_table}
                """

            self._conn.execute(view_sql)
            self.logger.info(f"Enrichment VIEW '{view_name}' created successfully")

        except Exception as e:
            self.logger.error(f"Failed to create enrichment VIEW: {e}")
            raise PyInmetError(f"Enrichment VIEW creation failed: {e}") from e

    def get_processed_source_files(self, subsystem: str) -> set[str]:
        """Get the set of source files already processed for a subsystem.

        Used for incremental updates to avoid reprocessing files.

        Args:
            subsystem: DataSUS subsystem name

        Returns:
            Set of source_file values already in the database
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        table_name = f"{subsystem}_raw"

        try:
            # Check if table exists
            tables = self.list_tables()
            if table_name not in tables:
                return set()

            result = self._conn.execute(
                f"SELECT DISTINCT source_file FROM {table_name}"
            ).fetchall()
            return {row[0] for row in result if row[0]}

        except Exception as e:
            self.logger.warning(f"Failed to get processed files: {e}")
            return set()

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists, False otherwise
        """
        if not self._conn:
            raise PyInmetError("Not connected to database. Call connect() first.")

        try:
            result = self._conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_name = '{table_name}'"
            ).fetchone()
            return result[0] > 0
        except Exception:
            return False

    def __repr__(self) -> str:
        """String representation."""
        db_path = self.config.db_path or ":memory:"
        status = "connected" if self._conn else "disconnected"
        return f"DuckDBManager(db_path={db_path}, status={status})"
