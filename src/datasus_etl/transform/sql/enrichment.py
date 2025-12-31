"""IBGE enrichment transformation for SQL data processing.

This module provides the IbgeEnrichmentTransform class that adds geographic
information from IBGE municipality data.
"""

import logging
from pathlib import Path
from typing import Optional

import duckdb

from datasus_etl.transform.sql.base import BaseTransform
from datasus_etl.utils.ibge_loader import create_ibge_lookup_csv


class IbgeEnrichmentTransform(BaseTransform):
    """Transform that enriches data with IBGE municipality information.

    Adds geographic columns via LEFT JOIN on municipality code:
    - municipio_res: Municipality name
    - uf_res: State (UF) code
    - rg_imediata_res: Immediate geographic region
    - rg_intermediaria_res: Intermediate geographic region

    The join is performed on MUNIC_RES = codigo_municipio.
    Uses bundled IBGE data by default, or accepts custom CSV path.

    Example:
        >>> conn = duckdb.connect(":memory:")
        >>> transform = IbgeEnrichmentTransform(conn)
        >>> transform.load_ibge_data()
        True
        >>> transform.get_join_sql(["munic_res", "nome"])
        'LEFT JOIN ibge_data AS ibge ON typed.munic_res = ibge.codigo_municipio'
    """

    # Columns added by IBGE enrichment
    IBGE_COLUMNS = ["municipio_res", "uf_res", "rg_imediata_res", "rg_intermediaria_res"]

    def __init__(
        self,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        ibge_data_path: Optional[Path] = None,
    ) -> None:
        """Initialize IBGE enrichment transform.

        Args:
            conn: DuckDB connection for loading IBGE data
            ibge_data_path: Optional custom path to IBGE CSV file
        """
        self.conn = conn
        self.ibge_data_path = ibge_data_path
        self._ibge_loaded = False
        self.logger = logging.getLogger(__name__)

    @property
    def name(self) -> str:
        """Return transform name."""
        return "ibge_enrichment"

    def get_columns(self) -> list[str]:
        """Return list of columns added by this transform."""
        return self.IBGE_COLUMNS

    def get_sql(self, column: str, columns: list[str], schema: Optional[dict[str, str]] = None) -> str:
        """Generate SQL for IBGE enrichment column.

        Args:
            column: Column name (should be one of IBGE_COLUMNS)
            columns: List of all available source columns
            schema: Optional schema dict

        Returns:
            SQL expression for the IBGE column (from JOIN)
        """
        col_lower = column.lower()

        if col_lower not in [c.lower() for c in self.IBGE_COLUMNS]:
            return f"{column} AS {col_lower}"

        # Check if munic_res exists in source (required for JOIN)
        if "munic_res" not in [c.lower() for c in columns]:
            # No join key available, return NULL
            return f"NULL AS {col_lower}"

        # Return reference to IBGE join alias
        return f"ibge.{col_lower}"

    def get_select_sql(self) -> str:
        """Generate SQL SELECT clause for all IBGE enrichment columns.

        Returns:
            SQL column list for IBGE data from JOIN
        """
        return """ibge.municipio_res,
        ibge.uf_res,
        ibge.rg_imediata_res,
        ibge.rg_intermediaria_res"""

    def get_join_sql(self, columns: list[str]) -> str:
        """Generate SQL LEFT JOIN clause for IBGE data.

        Args:
            columns: List of available source columns

        Returns:
            SQL JOIN clause, or empty string if munic_res not available
        """
        # Check if munic_res exists in source
        if "munic_res" not in [c.lower() for c in columns]:
            return ""

        return "\nLEFT JOIN ibge_data AS ibge\n    ON typed.munic_res = ibge.codigo_municipio"

    def load_ibge_data(self) -> bool:
        """Load IBGE municipality data into DuckDB temp view.

        Creates a temp view 'ibge_data' from bundled IBGE data or custom path.

        Returns:
            True if IBGE data was loaded successfully, False otherwise
        """
        if self.conn is None:
            self.logger.warning("No DuckDB connection provided for IBGE data loading")
            return False

        try:
            if self.ibge_data_path and self.ibge_data_path.exists():
                # Use provided CSV path
                csv_path = self.ibge_data_path
                self.logger.info(f"Loading IBGE data from {csv_path}")
            else:
                # Create CSV from bundled Excel file
                csv_path = create_ibge_lookup_csv()
                self.logger.info(f"Loading IBGE data from bundled file: {csv_path}")

            self.conn.execute(f"""
                CREATE OR REPLACE TEMP VIEW ibge_data AS
                SELECT
                    CAST(codigo_municipio AS INTEGER) AS codigo_municipio,
                    municipio_res,
                    uf_res,
                    rg_imediata_res,
                    rg_intermediaria_res
                FROM read_csv('{csv_path}', header=true)
            """)

            # Get count for logging
            count = self.conn.execute("SELECT COUNT(*) FROM ibge_data").fetchone()[0]
            self.logger.info(f"Loaded {count} municipalities from IBGE data")

            self._ibge_loaded = True
            return True

        except Exception as e:
            self.logger.warning(f"Failed to load IBGE data: {e}")
            return False

    @property
    def is_loaded(self) -> bool:
        """Check if IBGE data has been loaded."""
        return self._ibge_loaded

    def get_canonical_columns_sql(
        self,
        actual_columns: list[str],
        schema: dict[str, str],
    ) -> list[str]:
        """Generate SQL for IBGE columns in canonical schema.

        For columns that exist via JOIN, references ibge.column_name.
        For columns without JOIN (no munic_res), returns NULL with correct type.

        Args:
            actual_columns: Columns available in source table
            schema: Schema dict with column types

        Returns:
            List of SQL expressions for IBGE columns
        """
        result = []
        has_munic_res = "munic_res" in [c.lower() for c in actual_columns]

        for col in self.IBGE_COLUMNS:
            if col in schema:
                if self._ibge_loaded and has_munic_res:
                    result.append(f"ibge.{col}")
                else:
                    col_type = schema.get(col, "VARCHAR")
                    result.append(f"NULL::{col_type} AS {col}")

        return result
