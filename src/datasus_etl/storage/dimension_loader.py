"""Dimension table loader for DuckDB persistent storage.

Loads dimension tables from various sources (IBGE Excel, CSV files)
into DuckDB tables for data enrichment.
"""

import logging
from pathlib import Path
from typing import Optional

import duckdb

from datasus_etl.exceptions import PyInmetError
from datasus_etl.utils.ibge_loader import load_ibge_municipalities

logger = logging.getLogger(__name__)


class DimensionLoader:
    """Loads dimension tables into DuckDB database.

    Supports loading from:
    - Bundled IBGE municipality data (Excel)
    - CSV files for custom dimension tables

    Example:
        >>> conn = duckdb.connect("sihsus.duckdb")
        >>> loader = DimensionLoader(conn)
        >>> loader.load_ibge_municipios()
        >>> loader.load_from_csv("dim_procedimentos", Path("procedimentos.csv"))
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Initialize the dimension loader.

        Args:
            conn: Active DuckDB connection
        """
        self.conn = conn
        self.logger = logging.getLogger(__name__)

    def load_ibge_municipios(self, replace: bool = True) -> int:
        """Load IBGE municipality data into dim_municipios table.

        Uses the bundled IBGE DTB 2024 Excel file to populate
        the dim_municipios dimension table.

        Args:
            replace: If True, truncates existing data before loading.
                    If False, appends to existing data.

        Returns:
            Number of rows loaded

        Raises:
            PyInmetError: If loading fails
        """
        self.logger.info("Loading IBGE municipality data into dim_municipios")

        try:
            # Ensure table exists
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS dim_municipios (
                    codigo_municipio INTEGER PRIMARY KEY,
                    nome VARCHAR,
                    uf VARCHAR,
                    regiao_imediata VARCHAR,
                    regiao_intermediaria VARCHAR
                )
            """)

            # Clear existing data if replacing
            if replace:
                self.conn.execute("DELETE FROM dim_municipios")
                self.logger.debug("Cleared existing dim_municipios data")

            # Load municipality data from bundled Excel
            municipalities = load_ibge_municipalities()

            # Insert data in batches
            batch_size = 1000
            rows_inserted = 0
            batch = []

            for codigo, data in municipalities.items():
                batch.append((
                    codigo,
                    data.get("municipio"),
                    data.get("uf"),
                    data.get("rg_imediata"),
                    data.get("rg_intermediaria"),
                ))

                if len(batch) >= batch_size:
                    self._insert_municipios_batch(batch)
                    rows_inserted += len(batch)
                    batch = []

            # Insert remaining rows
            if batch:
                self._insert_municipios_batch(batch)
                rows_inserted += len(batch)

            self.logger.info(
                f"Loaded {rows_inserted} municipalities into dim_municipios"
            )
            return rows_inserted

        except Exception as e:
            self.logger.error(f"Failed to load IBGE data: {e}")
            raise PyInmetError(f"IBGE municipality loading failed: {e}") from e

    def _insert_municipios_batch(
        self, batch: list[tuple[int, str, str, str, str]]
    ) -> None:
        """Insert a batch of municipality records.

        Args:
            batch: List of tuples (codigo, nome, uf, rg_imediata, rg_intermediaria)
        """
        # Use INSERT OR REPLACE to handle duplicates
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO dim_municipios
            (codigo_municipio, nome, uf, regiao_imediata, regiao_intermediaria)
            VALUES (?, ?, ?, ?, ?)
            """,
            batch,
        )

    def load_from_csv(
        self,
        table_name: str,
        csv_path: Path,
        replace: bool = True,
        delimiter: str = ";",
        header: bool = True,
    ) -> int:
        """Load a dimension table from a CSV file.

        The CSV file's column names become the table columns.
        The table is created if it doesn't exist.

        Args:
            table_name: Target table name (e.g., "dim_procedimentos")
            csv_path: Path to the CSV file
            replace: If True, drops and recreates the table.
                    If False, appends to existing table.
            delimiter: CSV delimiter character
            header: Whether CSV has header row

        Returns:
            Number of rows loaded

        Raises:
            PyInmetError: If loading fails
            FileNotFoundError: If CSV file doesn't exist
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        self.logger.info(f"Loading {csv_path} into {table_name}")

        try:
            # Drop table if replacing
            if replace:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.logger.debug(f"Dropped existing table {table_name}")

            # Create table from CSV
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS
                SELECT * FROM read_csv(
                    '{csv_path}',
                    delim='{delimiter}',
                    header={str(header).lower()}
                )
            """
            self.conn.execute(create_sql)

            # Get row count
            result = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()
            row_count = result[0] if result else 0

            self.logger.info(f"Loaded {row_count} rows into {table_name}")
            return row_count

        except Exception as e:
            self.logger.error(f"Failed to load CSV into {table_name}: {e}")
            raise PyInmetError(f"CSV loading failed: {e}") from e

    def get_dimension_status(self) -> dict[str, int]:
        """Get row counts for all dimension tables.

        Returns:
            Dict mapping table name to row count
        """
        status = {}

        # Check known dimension tables
        dimension_tables = [
            "dim_municipios",
            "dim_procedimentos",
            "dim_cid10",
            "dim_ocupacoes",
        ]

        for table in dimension_tables:
            try:
                result = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()
                status[table] = result[0] if result else 0
            except duckdb.CatalogException:
                # Table doesn't exist
                status[table] = -1
            except Exception as e:
                self.logger.warning(f"Error checking {table}: {e}")
                status[table] = -1

        return status

    def list_available_dimensions(self) -> list[str]:
        """List dimension tables that have data loaded.

        Returns:
            List of table names with data
        """
        status = self.get_dimension_status()
        return [table for table, count in status.items() if count > 0]
