"""Type casting transformation for SQL data processing.

This module provides the TypeCastTransform class that converts string columns
to their target types based on a schema definition.
"""

from typing import Optional

from datasus_etl.transform.sql.base import BaseTransform


class TypeCastTransform(BaseTransform):
    """Transform that casts columns to their target types based on schema.

    Uses TRY_CAST for safe conversion - invalid values become NULL instead
    of causing errors.

    Supported types:
    - VARCHAR: No conversion needed
    - INTEGER, BIGINT, SMALLINT, TINYINT: Numeric conversion
    - FLOAT, DOUBLE: Floating point conversion
    - BOOLEAN: Converts '1'/'true' to TRUE, '0'/'false' to FALSE
    - DATE: Handled by DateParsingTransform (skipped here)

    Example:
        >>> schema = {"idade": "INTEGER", "val_tot": "DOUBLE"}
        >>> transform = TypeCastTransform(schema)
        >>> transform.get_sql("idade", ["idade", "nome"], schema)
        'TRY_CAST(idade AS INTEGER) AS idade'
    """

    def __init__(self, schema: Optional[dict[str, str]] = None) -> None:
        """Initialize type casting transform.

        Args:
            schema: Dictionary mapping column names to DuckDB SQL types.
                   If None, columns are kept as VARCHAR.
        """
        self._schema = schema or {}

    @property
    def name(self) -> str:
        """Return transform name."""
        return "type_cast"

    def get_columns(self) -> list[str]:
        """Return empty list - applies based on schema."""
        return []  # Applies to all columns that are in schema

    def get_sql(self, column: str, columns: list[str], schema: Optional[dict[str, str]] = None) -> str:
        """Generate SQL for type casting based on schema.

        Args:
            column: Column name to cast
            columns: List of all available columns
            schema: Schema dict (overrides instance schema if provided)

        Returns:
            SQL expression with appropriate type cast
        """
        effective_schema = schema or self._schema
        col_lower = column.lower()

        # Check if column is in schema
        if col_lower not in effective_schema:
            return f"{col_lower}"

        target_type = effective_schema[col_lower]

        # Skip DATE columns - handled by DateParsingTransform
        if target_type == "DATE":
            return f"{col_lower}"

        # Skip categorical columns - handled by SexoTransform/RacaCorTransform
        if col_lower in ("sexo", "raca_cor"):
            return f"{col_lower}"

        # VARCHAR - no conversion needed
        if target_type == "VARCHAR":
            return f"{col_lower}"

        # BOOLEAN - special handling
        if target_type == "BOOLEAN":
            return f"""CASE WHEN {col_lower} IN ('1', 'true', 'TRUE') THEN TRUE
            WHEN {col_lower} IN ('0', 'false', 'FALSE') THEN FALSE
            ELSE NULL END AS {col_lower}"""

        # Numeric types - TRY_CAST for safe conversion
        return f"TRY_CAST({col_lower} AS {target_type}) AS {col_lower}"

    def get_sql_expression(self, column: str, schema: Optional[dict[str, str]] = None) -> str:
        """Generate SQL expression without AS alias.

        Args:
            column: Column name to cast
            schema: Schema dict (overrides instance schema if provided)

        Returns:
            SQL expression with type cast (without AS alias)
        """
        effective_schema = schema or self._schema
        col_lower = column.lower()

        if col_lower not in effective_schema:
            return col_lower

        target_type = effective_schema[col_lower]

        if target_type in ("DATE", "VARCHAR") or col_lower in ("sexo", "raca_cor"):
            return col_lower

        if target_type == "BOOLEAN":
            return f"""CASE WHEN {col_lower} IN ('1', 'true', 'TRUE') THEN TRUE
            WHEN {col_lower} IN ('0', 'false', 'FALSE') THEN FALSE
            ELSE NULL END"""

        return f"TRY_CAST({col_lower} AS {target_type})"
