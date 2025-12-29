"""Transform pipeline orchestrator for SQL data processing.

This module provides the TransformPipeline class that orchestrates
multiple transforms in sequence to build complete SQL transformation queries.
"""

import logging
from typing import Optional

import duckdb

from pydatasus.transform.sql.base import BaseTransform
from pydatasus.transform.sql.cleaning import CleaningTransform
from pydatasus.transform.sql.dates import DateParsingTransform
from pydatasus.transform.sql.categorical import SexoTransform, RacaCorTransform
from pydatasus.transform.sql.types import TypeCastTransform
from pydatasus.transform.sql.enrichment import IbgeEnrichmentTransform


class TransformPipeline:
    """Orchestrates multiple SQL transforms into a complete transformation query.

    The pipeline applies transforms in a specific order:
    1. CleaningTransform - Remove invisible characters, trim whitespace
    2. DateParsingTransform - Parse date strings with format fallback
    3. TypeCastTransform - Convert strings to target types
    4. SexoTransform - Map SEXO codes to labels
    5. RacaCorTransform - Map RACA_COR codes to labels
    6. IbgeEnrichmentTransform - Add geographic data via JOIN

    The pipeline generates a complete SQL query with CTEs for each stage.

    Attributes:
        conn: DuckDB connection
        schema: Column type schema
        transforms: List of transform instances
        raw_mode: If True, only apply CleaningTransform

    Example:
        >>> conn = duckdb.connect(":memory:")
        >>> schema = {"idade": "INTEGER", "sexo": "VARCHAR"}
        >>> pipeline = TransformPipeline(conn, schema)
        >>> sql = pipeline.build_transform_sql(
        ...     source_table="staging",
        ...     target_view="processed",
        ...     columns=["idade", "sexo", "nome"]
        ... )
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: dict[str, str],
        subsystem: str = "sihsus",
        raw_mode: bool = False,
        enable_ibge: bool = True,
    ) -> None:
        """Initialize transform pipeline.

        Args:
            conn: DuckDB connection
            schema: Column type schema (column_name -> DuckDB type)
            subsystem: DataSUS subsystem name (sihsus, sim)
            raw_mode: If True, only apply cleaning (no type conversions)
            enable_ibge: Whether to enable IBGE enrichment (default: True)
        """
        self.conn = conn
        self.schema = schema
        self.subsystem = subsystem
        self.raw_mode = raw_mode
        self.enable_ibge = enable_ibge
        self.logger = logging.getLogger(__name__)

        # Date columns by subsystem
        self._date_columns_map = {
            "sihsus": ["dt_inter", "dt_saida", "nasc", "gestor_dt"],
            "sim": ["dtobito", "dtnasc", "dtinvestig", "dtcadastro", "dtrecebim",
                   "dtatestado", "dtregcart", "dtcadinf"],
        }

        # Initialize transforms
        self._init_transforms()

    def _init_transforms(self) -> None:
        """Initialize transform instances based on configuration."""
        self.cleaning = CleaningTransform()

        if self.raw_mode:
            # Raw mode: only cleaning
            self.date_parsing = None
            self.type_cast = None
            self.sexo = None
            self.raca_cor = None
            self.ibge = None
        else:
            # Full mode: all transforms
            date_columns = self._date_columns_map.get(self.subsystem, [])
            self.date_parsing = DateParsingTransform(date_columns=date_columns)
            self.type_cast = TypeCastTransform(self.schema)
            self.sexo = SexoTransform()
            self.raca_cor = RacaCorTransform()

            if self.enable_ibge:
                self.ibge = IbgeEnrichmentTransform(self.conn)
            else:
                self.ibge = None

    @property
    def transforms(self) -> list[BaseTransform]:
        """Return list of active transforms."""
        result = [self.cleaning]

        if not self.raw_mode:
            if self.date_parsing:
                result.append(self.date_parsing)
            if self.type_cast:
                result.append(self.type_cast)
            if self.sexo:
                result.append(self.sexo)
            if self.raca_cor:
                result.append(self.raca_cor)
            if self.ibge:
                result.append(self.ibge)

        return result

    def load_ibge_data(self) -> bool:
        """Load IBGE data for enrichment.

        Returns:
            True if IBGE data was loaded successfully
        """
        if self.ibge is None:
            return False
        return self.ibge.load_ibge_data()

    def build_cleaned_columns_sql(self, columns: list[str]) -> str:
        """Build SQL for cleaned columns (CTE: cleaned).

        Args:
            columns: List of source columns

        Returns:
            SQL column list with cleaning applied
        """
        cleaned = []
        for col in columns:
            cleaned.append(self.cleaning.get_sql(col, columns, self.schema))
        return ",\n        ".join(cleaned)

    def build_date_parsing_sql(self, columns: list[str]) -> str:
        """Build SQL for date parsing (part of cleaned CTE).

        Args:
            columns: List of source columns

        Returns:
            SQL expressions for date parsing
        """
        if self.date_parsing is None:
            return "NULL AS _no_dates"

        date_columns = self.date_parsing.get_columns()
        columns_lower = [c.lower() for c in columns]

        parts = []
        for date_col in date_columns:
            if date_col.lower() in columns_lower:
                parts.append(self.date_parsing.get_sql(date_col, columns, self.schema))

        return ",\n        ".join(parts) if parts else "NULL AS _no_dates"

    def build_typed_columns_sql(self, columns: list[str]) -> str:
        """Build SQL for typed columns (CTE: typed).

        Applies type conversions, categorical mappings, and date assignments.

        Args:
            columns: List of source columns

        Returns:
            SQL column list with type conversions
        """
        if self.raw_mode:
            # Raw mode: just reference cleaned columns
            return ",\n        ".join([f"cleaned.{c.lower()}" for c in columns])

        typed = []
        columns_lower = [c.lower() for c in columns]

        # Map date columns to their parsed versions
        date_column_mapping = {}
        if self.date_parsing:
            for date_col in self.date_parsing.get_columns():
                date_column_mapping[date_col.lower()] = f"{date_col.lower()}_parsed"

        for col in columns:
            col_lower = col.lower()

            # Check for special transforms
            if col_lower == "sexo" and self.sexo:
                typed.append(f"{self.sexo.get_sql_expression(col_lower)} AS {col_lower}")
            elif col_lower == "raca_cor" and self.raca_cor:
                typed.append(f"{self.raca_cor.get_sql_expression(col_lower)} AS {col_lower}")
            elif col_lower in date_column_mapping:
                # Use parsed date version
                parsed_col = date_column_mapping[col_lower]
                typed.append(f"cleaned.{parsed_col} AS {col_lower}")
            elif col_lower in self.schema:
                # Apply type cast
                target_type = self.schema[col_lower]
                if target_type == "VARCHAR":
                    typed.append(f"cleaned.{col_lower}")
                elif target_type == "BOOLEAN":
                    typed.append(
                        f"CASE WHEN cleaned.{col_lower} IN ('1', 'true', 'TRUE') THEN TRUE "
                        f"WHEN cleaned.{col_lower} IN ('0', 'false', 'FALSE') THEN FALSE "
                        f"ELSE NULL END AS {col_lower}"
                    )
                elif target_type == "DATE":
                    # Date should already be handled above
                    typed.append(f"cleaned.{col_lower}")
                else:
                    # Numeric types
                    typed.append(f"TRY_CAST(cleaned.{col_lower} AS {target_type}) AS {col_lower}")
            else:
                # Not in schema, keep as VARCHAR
                typed.append(f"cleaned.{col_lower}")

        return ",\n        ".join(typed)

    def build_where_clause(self, columns: list[str]) -> str:
        """Build WHERE clause for row filtering.

        Filters out rows where all date columns are NULL.

        Args:
            columns: List of source columns

        Returns:
            SQL WHERE clause
        """
        if self.raw_mode:
            return "1=1"

        # Get subsystem date columns for filtering
        date_columns = self._date_columns_map.get(self.subsystem, [])
        columns_lower = [c.lower() for c in columns]

        conditions = []
        for date_col in date_columns:
            if date_col.lower() in columns_lower:
                conditions.append(f"cleaned.{date_col.lower()}_parsed IS NULL")

        if conditions:
            return f"NOT ({' AND '.join(conditions)})"
        else:
            return "1=1"

    def build_canonical_columns_sql(self, columns: list[str]) -> str:
        """Build SQL for canonical schema columns (CTE: canonical).

        Generates all columns from schema, with NULL for missing columns.

        Args:
            columns: List of source columns

        Returns:
            SQL column list for canonical schema
        """
        columns_lower = {c.lower() for c in columns}
        canonical = []

        # IBGE columns come from JOIN
        ibge_columns = set()
        if self.ibge and self.ibge.is_loaded:
            ibge_columns = set(IbgeEnrichmentTransform.IBGE_COLUMNS)

        for col_name, col_type in self.schema.items():
            if col_name in ibge_columns:
                # IBGE column - from JOIN if available
                if self.ibge and self.ibge.is_loaded and "munic_res" in columns_lower:
                    canonical.append(f"ibge.{col_name}")
                else:
                    canonical.append(f"NULL::{col_type} AS {col_name}")
            elif col_name in columns_lower:
                # Column exists in source
                canonical.append(f"typed.{col_name}")
            else:
                # Missing column - NULL with correct type
                canonical.append(f"NULL::{col_type} AS {col_name}")

        return ",\n        ".join(canonical)

    def build_ibge_join_sql(self, columns: list[str]) -> str:
        """Build IBGE LEFT JOIN clause.

        Args:
            columns: List of source columns

        Returns:
            SQL JOIN clause, or empty string if not applicable
        """
        if self.ibge is None or not self.ibge.is_loaded:
            return ""
        return self.ibge.get_join_sql(columns)

    def build_transform_sql(
        self,
        source_table: str,
        target_view: str,
        columns: list[str],
    ) -> str:
        """Build complete transformation SQL query.

        Generates a CREATE VIEW statement with CTEs for each transformation stage:
        - cleaned: Column cleaning and date parsing
        - typed: Type conversions and categorical mappings
        - canonical: All schema columns (missing as NULL)

        Args:
            source_table: Name of source table in DuckDB
            target_view: Name for the target view to create
            columns: List of source columns

        Returns:
            Complete SQL CREATE VIEW statement
        """
        return f"""
CREATE OR REPLACE VIEW {target_view} AS
WITH cleaned AS (
    SELECT
        -- Original columns (cleaned)
        {self.build_cleaned_columns_sql(columns)},

        -- Parse dates with fallback to multiple formats
        {self.build_date_parsing_sql(columns)}
    FROM {source_table}
),
typed AS (
    SELECT
        -- Apply type conversions
        {self.build_typed_columns_sql(columns)}
    FROM cleaned
    WHERE
        -- Remove empty rows
        {self.build_where_clause(columns)}
),
canonical AS (
    SELECT
        -- All columns from canonical schema
        {self.build_canonical_columns_sql(columns)}
    FROM typed{self.build_ibge_join_sql(columns)}
)
SELECT * FROM canonical
"""

    def execute_transform(
        self,
        source_table: str,
        target_view: str,
    ) -> None:
        """Execute transformation and create target view.

        Args:
            source_table: Name of source table in DuckDB
            target_view: Name for the target view to create

        Raises:
            Exception: If transformation fails
        """
        # Get actual columns from source table
        actual_columns = self.conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{source_table}' ORDER BY ordinal_position"
        ).fetchall()
        columns = [col[0] for col in actual_columns]

        self.logger.info(
            f"Building transform: {len(columns)} source columns -> "
            f"{len(self.schema)} schema columns"
        )

        # Load IBGE data if enabled
        if self.enable_ibge and not self.raw_mode:
            self.load_ibge_data()

        # Build and execute SQL
        sql = self.build_transform_sql(source_table, target_view, columns)
        self.logger.debug(f"Transform SQL:\n{sql}")

        try:
            self.conn.execute(sql)
            self.logger.info(f"Created view: {target_view}")
        except Exception as e:
            self.logger.error(f"Transform failed: {e}")
            self.logger.error(f"SQL:\n{sql}")
            raise
