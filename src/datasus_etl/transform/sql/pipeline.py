"""Transform pipeline orchestrator for SQL data processing.

This module provides the TransformPipeline class that orchestrates
multiple transforms in sequence to build complete SQL transformation queries.
"""

import logging
from typing import Optional

import duckdb

from datasus_etl.transform.sql.base import BaseTransform
from datasus_etl.transform.sql.cleaning import CleaningTransform
from datasus_etl.transform.sql.dates import DateParsingTransform
from datasus_etl.transform.sql.categorical import SexoTransform, RacaCorTransform
from datasus_etl.transform.sql.types import TypeCastTransform
from datasus_etl.transform.sql.validation import CidValidationTransform
from datasus_etl.transform.sql.enrichment import IbgeEnrichmentTransform
from datasus_etl.transform.sql.idade import IdadeTransform
from datasus_etl.transform.sql.cid_array import CidArrayTransform
from datasus_etl.transform.sql.boolean_mappings import SIM_BOOLEAN_MAPPINGS, get_boolean_case_sql

# CID (ICD-10) columns by subsystem
SIHSUS_CID_COLUMNS = [
    "diag_princ", "diag_secun", "cid_asso", "cid_morte", "cid_notif",
    "diagsec1", "diagsec2", "diagsec3", "diagsec4", "diagsec5",
    "diagsec6", "diagsec7", "diagsec8", "diagsec9"
]


def quote_column(col: str) -> str:
    """Quote a column name to handle SQL reserved words.

    Args:
        col: Column name to quote

    Returns:
        Quoted column name (e.g., '"natural"')
    """
    return f'"{col}"'


class TransformPipeline:
    """Orchestrates multiple SQL transforms into a complete transformation query.

    The pipeline applies transforms in a specific order:
    1. CleaningTransform - Remove invisible characters, trim whitespace
    2. DateParsingTransform - Parse date strings with format fallback
    3. TypeCastTransform - Convert strings to target types
    4. CidValidationTransform - Validate ICD-10 (CID) code format
    5. SexoTransform - Map SEXO codes to labels
    6. RacaCorTransform - Map RACA_COR codes to labels
    7. IbgeEnrichmentTransform - Add geographic data via JOIN
    8. IdadeTransform - Decode IDADE field for SIM subsystem

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
            "sim": [
                "dtobito", "dtnasc", "dtinvestig", "dtcadastro", "dtrecebim",
                "dtatestado", "dtcadinf",
                # Additional date columns (may have 7-digit format)
                "dtrecoriga", "dtcadinv", "dtconinv", "dtconcaso",
            ],
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
            self.cid_validation = None
            self.sexo = None
            self.raca_cor = None
            self.ibge = None
            self.idade = None
            self.cid_array = None
        else:
            # Full mode: all transforms
            date_columns = self._date_columns_map.get(self.subsystem, [])
            self.date_parsing = DateParsingTransform(date_columns=date_columns)
            self.type_cast = TypeCastTransform(self.schema)

            # CID validation for SIHSUS subsystem
            cid_columns = SIHSUS_CID_COLUMNS if self.subsystem == "sihsus" else []
            self.cid_validation = CidValidationTransform(cid_columns=cid_columns) if cid_columns else None

            self.sexo = SexoTransform(subsystem=self.subsystem)
            self.raca_cor = RacaCorTransform(subsystem=self.subsystem)

            if self.enable_ibge:
                self.ibge = IbgeEnrichmentTransform(self.conn)
            else:
                self.ibge = None

            # IDADE transform for SIM subsystem
            if self.subsystem == "sim":
                self.idade = IdadeTransform()
            else:
                self.idade = None

            # CID array transform for SIM subsystem (handles asterisk-separated CIDs)
            if self.subsystem == "sim":
                self.cid_array = CidArrayTransform()
            else:
                self.cid_array = None

    @property
    def transforms(self) -> list[BaseTransform]:
        """Return list of active transforms."""
        result = [self.cleaning]

        if not self.raw_mode:
            if self.date_parsing:
                result.append(self.date_parsing)
            if self.type_cast:
                result.append(self.type_cast)
            if self.cid_validation:
                result.append(self.cid_validation)
            if self.sexo:
                result.append(self.sexo)
            if self.raca_cor:
                result.append(self.raca_cor)
            if self.ibge:
                result.append(self.ibge)
            if self.idade:
                result.append(self.idade)
            if self.cid_array:
                result.append(self.cid_array)

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
            # Raw mode: just reference cleaned columns (quoted for reserved words)
            return ",\n        ".join([f'cleaned.{quote_column(c.lower())}' for c in columns])

        typed = []
        columns_lower = [c.lower() for c in columns]

        # Map date columns to their parsed versions
        date_column_mapping = {}
        if self.date_parsing:
            for date_col in self.date_parsing.get_columns():
                date_column_mapping[date_col.lower()] = f"{date_col.lower()}_parsed"

        for col in columns:
            col_lower = col.lower()
            col_quoted = quote_column(col_lower)

            # Check for special transforms
            if col_lower == "sexo" and self.sexo:
                typed.append(f"{self.sexo.get_sql_expression(col_lower)} AS {col_quoted}")
            elif col_lower in ("raca_cor", "racacor") and self.raca_cor:
                typed.append(f"{self.raca_cor.get_sql_expression(col_lower)} AS {col_quoted}")
            elif self.cid_array and self.cid_array.applies_to(col_lower):
                # SIM CID columns: convert to array (handles asterisk-separated values)
                typed.append(f"{self.cid_array.get_sql_expression(col_lower)} AS {col_quoted}")
            elif self.cid_validation and self.cid_validation.applies_to(col_lower):
                typed.append(f"{self.cid_validation.get_sql_expression(col_lower)} AS {col_quoted}")
            elif col_lower in date_column_mapping:
                # Use parsed date version
                parsed_col = date_column_mapping[col_lower]
                typed.append(f'cleaned.{quote_column(parsed_col)} AS {col_quoted}')
            elif col_lower in self.schema:
                # Apply type cast
                target_type = self.schema[col_lower]
                if target_type == "VARCHAR":
                    typed.append(f'cleaned.{col_quoted}')
                elif target_type == "BOOLEAN":
                    # Check for custom BOOLEAN mappings (SIM subsystem)
                    if self.subsystem == "sim" and col_lower in SIM_BOOLEAN_MAPPINGS:
                        bool_sql = get_boolean_case_sql(col_lower, f'cleaned.{col_quoted}')
                        typed.append(f'{bool_sql} AS {col_quoted}')
                    else:
                        # Default BOOLEAN mapping (1/true -> TRUE, 0/false -> FALSE)
                        typed.append(
                            f'CASE WHEN cleaned.{col_quoted} IN (\'1\', \'true\', \'TRUE\') THEN TRUE '
                            f'WHEN cleaned.{col_quoted} IN (\'0\', \'false\', \'FALSE\') THEN FALSE '
                            f'ELSE NULL END AS {col_quoted}'
                        )
                elif target_type == "TIME":
                    # TIME conversion for SIM horaobito (HHMM -> HH:MM)
                    typed.append(
                        f'CASE '
                        f'WHEN cleaned.{col_quoted} IS NULL OR TRIM(cleaned.{col_quoted}) = \'\' THEN NULL '
                        f'WHEN LENGTH(TRIM(cleaned.{col_quoted})) = 4 '
                        f'AND TRY_CAST(TRIM(cleaned.{col_quoted}) AS INTEGER) IS NOT NULL '
                        f'THEN CAST(TRY_STRPTIME('
                        f'SUBSTRING(TRIM(cleaned.{col_quoted}), 1, 2) || \':\' || '
                        f'SUBSTRING(TRIM(cleaned.{col_quoted}), 3, 2), \'%H:%M\') AS TIME) '
                        f'ELSE NULL END AS {col_quoted}'
                    )
                elif target_type == "DATE":
                    # Date should already be handled above
                    typed.append(f'cleaned.{col_quoted}')
                else:
                    # Numeric types
                    typed.append(f'TRY_CAST(cleaned.{col_quoted} AS {target_type}) AS {col_quoted}')
            else:
                # Not in schema, keep as VARCHAR
                typed.append(f'cleaned.{col_quoted}')

        return ",\n        ".join(typed)

    def build_where_clause(self, columns: list[str]) -> str:
        """Build WHERE clause for row filtering.

        Currently returns 1=1 (no filtering) to preserve all original records.

        Args:
            columns: List of source columns

        Returns:
            SQL WHERE clause (always "1=1" - no filtering)
        """
        return "1=1"

    def build_order_by_clause(self, columns: list[str]) -> str:
        """Build ORDER BY clause for consistent row ordering.

        Ordering varies by subsystem:
        - SIM: ORDER BY dtobito ASC (date of death)
        - SIHSUS: No specific ordering (empty string)

        Args:
            columns: List of source columns

        Returns:
            SQL ORDER BY clause, or empty string if no ordering needed
        """
        columns_lower = [c.lower() for c in columns]

        if self.subsystem == "sim":
            # SIM: order by date of death
            if "dtobito" in columns_lower:
                return "\nORDER BY dtobito ASC"

        # Default: no ordering
        return ""

    def build_canonical_columns_sql(self, columns: list[str]) -> str:
        """Build SQL for canonical schema columns (CTE: canonical).

        Generates all columns from schema, with NULL for missing columns.
        Includes derived columns (IBGE enrichment, IDADE decoding for SIM).

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

        # IDADE derived columns (for SIM subsystem)
        idade_columns = set()
        if self.idade:
            idade_columns = set(IdadeTransform.IDADE_COLUMNS)

        for col_name, col_type in self.schema.items():
            col_quoted = quote_column(col_name)
            if col_name in ibge_columns:
                # IBGE column - from JOIN if available
                if self.ibge and self.ibge.is_loaded and "munic_res" in columns_lower:
                    canonical.append(f'ibge.{col_quoted}')
                else:
                    canonical.append(f'NULL::{col_type} AS {col_quoted}')
            elif col_name in idade_columns:
                # IDADE derived column - decode from source idade field
                if "idade" in columns_lower:
                    if col_name == "idade_valor":
                        canonical.append(
                            f'{self.idade.get_idade_valor_sql("typed." + quote_column("idade"))} AS {col_quoted}'
                        )
                    elif col_name == "idade_unidade":
                        canonical.append(
                            f'{self.idade.get_idade_unidade_sql("typed." + quote_column("idade"))} AS {col_quoted}'
                        )
                else:
                    canonical.append(f'NULL::{col_type} AS {col_quoted}')
            elif col_name in columns_lower:
                # Column exists in source
                canonical.append(f'typed.{col_quoted}')
            else:
                # Missing column - NULL with correct type
                canonical.append(f'NULL::{col_type} AS {col_quoted}')

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
SELECT * FROM canonical{self.build_order_by_clause(columns)}
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
