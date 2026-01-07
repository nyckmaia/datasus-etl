"""IDADE field transformation for SIM (Mortality) data.

This module provides the IdadeTransform class that decodes the IDADE field
from SIM mortality records into separate valor and unidade columns.

The IDADE field in SIM data uses a 3-digit encoded format:
- First digit: unit of age (1=min, 2=hr, 3=months, 4=years, 5=>100 years, 9=ignored)
- Next 2 digits: numeric value

Example encodings:
- '401' -> idade_valor=1, idade_unidade='anos' (1 year old)
- '311' -> idade_valor=11, idade_unidade='meses' (11 months old)
- '505' -> idade_valor=105, idade_unidade='anos' (105 years old)
- '999' -> idade_valor=NULL, idade_unidade='ignorado' (ignored/unknown)

Reference: SIM Technical Documentation (DATASUS)
"""

from typing import Optional

from datasus_etl.transform.sql.base import BaseTransform


class IdadeTransform(BaseTransform):
    """Transform that decodes IDADE field into valor and unidade columns.

    The IDADE field in SIM mortality data is encoded as a 3-character string:
    - Position 0: Unit indicator (1-5, 9)
    - Positions 1-2: Numeric value

    This transform generates SQL expressions to decode the field into:
    - idade_valor: INTEGER - The numeric age value (or NULL if invalid)
    - idade_unidade: VARCHAR - The unit name in Portuguese

    Unit mapping (per official SIM documentation):
    - '1' = minutos (minutes, value 01-59)
    - '2' = horas (hours, value 01-23)
    - '3' = meses (months, value 01-11)
    - '4' = anos (years, value 00-99)
    - '5' = anos (years, value + 100 for ages > 100)
    - '9' = ignorado (ignored/unknown)

    Example:
        >>> transform = IdadeTransform()
        >>> transform.get_idade_valor_sql("idade")
        "CASE WHEN idade IS NULL OR LENGTH(TRIM(idade)) < 3 THEN NULL..."
    """

    # Columns produced by this transform
    IDADE_COLUMNS = ["idade_valor", "idade_unidade"]

    @property
    def name(self) -> str:
        """Return transform name."""
        return "idade_transform"

    def get_columns(self) -> list[str]:
        """Return list of columns produced by this transform."""
        return self.IDADE_COLUMNS

    def get_sql(
        self, column: str, columns: list[str], schema: Optional[dict[str, str]] = None
    ) -> str:
        """Generate SQL for idade derived columns.

        Args:
            column: Column name (should be idade_valor or idade_unidade)
            columns: List of all available source columns
            schema: Optional schema dict

        Returns:
            SQL expression for the derived column
        """
        col_lower = column.lower()

        # Check if source idade column exists
        has_idade = "idade" in [c.lower() for c in columns]

        if col_lower == "idade_valor":
            if has_idade:
                return f"{self.get_idade_valor_sql('idade')} AS idade_valor"
            return "NULL::INTEGER AS idade_valor"

        if col_lower == "idade_unidade":
            if has_idade:
                return f"{self.get_idade_unidade_sql('idade')} AS idade_unidade"
            return "NULL::VARCHAR AS idade_unidade"

        # Not an idade column, return as-is
        return f"{column} AS {col_lower}"

    def get_idade_valor_sql(self, source_column: str = "idade") -> str:
        """Generate SQL expression to decode idade_valor.

        The value is extracted from positions 1-2 of the IDADE field.
        For unit '5' (>100 years), adds 100 to the value.

        Args:
            source_column: Name of the source IDADE column

        Returns:
            SQL CASE expression that evaluates to INTEGER or NULL
        """
        return f"""CASE
            WHEN {source_column} IS NULL OR LENGTH(TRIM({source_column})) < 3 THEN NULL
            WHEN SUBSTRING({source_column}, 1, 1) IN ('0', '9') THEN NULL
            WHEN SUBSTRING({source_column}, 1, 1) = '5' THEN
                TRY_CAST(SUBSTRING({source_column}, 2, 2) AS INTEGER) + 100
            ELSE TRY_CAST(SUBSTRING({source_column}, 2, 2) AS INTEGER)
        END"""

    def get_idade_unidade_sql(self, source_column: str = "idade") -> str:
        """Generate SQL expression to decode idade_unidade.

        Maps the first digit of IDADE to a Portuguese unit name.

        Args:
            source_column: Name of the source IDADE column

        Returns:
            SQL CASE expression that evaluates to VARCHAR
        """
        return f"""CASE
            WHEN {source_column} IS NULL OR LENGTH(TRIM({source_column})) < 3 THEN NULL
            WHEN SUBSTRING({source_column}, 1, 1) = '1' THEN 'minutos'
            WHEN SUBSTRING({source_column}, 1, 1) = '2' THEN 'horas'
            WHEN SUBSTRING({source_column}, 1, 1) = '3' THEN 'meses'
            WHEN SUBSTRING({source_column}, 1, 1) = '4' THEN 'anos'
            WHEN SUBSTRING({source_column}, 1, 1) = '5' THEN 'anos'
            WHEN SUBSTRING({source_column}, 1, 1) = '9' THEN 'ignorado'
            ELSE NULL
        END"""

    def applies_to(self, column: str) -> bool:
        """Check if this transform applies to a specific column.

        This transform only produces idade_valor and idade_unidade columns.

        Args:
            column: Column name to check

        Returns:
            True if this is an idade derived column
        """
        return column.lower() in [c.lower() for c in self.IDADE_COLUMNS]

    def get_canonical_columns_sql(
        self,
        actual_columns: list[str],
        schema: dict[str, str],
    ) -> list[str]:
        """Generate SQL for idade columns in canonical schema.

        If source 'idade' column exists, generates decoding expressions.
        Otherwise, returns NULL with correct types.

        Args:
            actual_columns: Columns available in source table
            schema: Schema dict with column types

        Returns:
            List of SQL expressions for idade derived columns
        """
        result = []
        has_idade = "idade" in [c.lower() for c in actual_columns]

        for col in self.IDADE_COLUMNS:
            if col in schema:
                if has_idade:
                    if col == "idade_valor":
                        result.append(
                            f"{self.get_idade_valor_sql('idade')} AS {col}"
                        )
                    elif col == "idade_unidade":
                        result.append(
                            f"{self.get_idade_unidade_sql('idade')} AS {col}"
                        )
                else:
                    col_type = schema.get(col, "VARCHAR")
                    result.append(f"NULL::{col_type} AS {col}")

        return result
