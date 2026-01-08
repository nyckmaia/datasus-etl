"""CID array transformation for SIM subsystem.

This module provides the CidArrayTransform class that converts CID columns
containing asterisk-separated values into VARCHAR[] arrays.
"""

from typing import Optional

from datasus_etl.transform.sql.base import BaseTransform


class CidArrayTransform(BaseTransform):
    """Transform that converts CID columns to arrays, handling multiple delimiters.

    SIM data often contains multiple CID codes in a single field, separated
    by asterisks (*) or slashes (/). This transform:
    1. Removes leading/trailing whitespace
    2. Normalizes delimiters (converts '/' to '*')
    3. Splits by asterisk separator
    4. Filters out empty values
    5. Validates each CID format (letter + 2-3 digits)
    6. Returns a VARCHAR[] array

    Examples:
        '*A01'              -> ['A01']
        '*A01*J128'         -> ['A01', 'J128']
        'A001/I12/H890'     -> ['A001', 'I12', 'H890']
        'A001/I12*H890'     -> ['A001', 'I12', 'H890']  (mixed delimiters)
        ''                  -> NULL
        NULL                -> NULL

    Attributes:
        SIM_CID_COLUMNS: List of column names this transform applies to
    """

    SIM_CID_COLUMNS = ["linhaa", "linhab", "linhac", "linhad", "linhaii", "causabas", "atestado"]

    @property
    def name(self) -> str:
        """Return transform name."""
        return "cid_array"

    def get_columns(self) -> list[str]:
        """Return list of columns this transform applies to."""
        return self.SIM_CID_COLUMNS

    def applies_to(self, column: str) -> bool:
        """Check if this transform applies to a given column.

        Args:
            column: Column name to check

        Returns:
            True if this transform should be applied to the column
        """
        return column.lower() in [c.lower() for c in self.SIM_CID_COLUMNS]

    def get_sql(self, column: str, columns: list[str], schema: Optional[dict[str, str]] = None) -> str:
        """Generate SQL for CID array transformation.

        Args:
            column: Column name to transform
            columns: List of all available columns
            schema: Optional schema dict (unused)

        Returns:
            SQL expression for CID array transformation with AS alias
        """
        col_lower = column.lower()
        if not self.applies_to(col_lower):
            return f'"{col_lower}" AS "{col_lower}"'

        return f'{self.get_sql_expression(col_lower)} AS "{col_lower}"'

    def get_sql_expression(self, column: str) -> str:
        """Generate SQL expression for CID array transformation without AS alias.

        The expression:
        1. Handles NULL and empty strings
        2. Trims whitespace and converts to uppercase
        3. Splits by asterisk
        4. Filters empty values and validates CID format
        5. Returns NULL if no valid CIDs found

        Args:
            column: Column name to transform

        Returns:
            SQL expression for CID array transformation (without AS alias)
        """
        col_lower = column.lower()
        # Note: Using double braces {{ }} to escape braces in f-string for regex
        return f"""CASE
            WHEN cleaned."{col_lower}" IS NULL OR TRIM(cleaned."{col_lower}") = ''
            THEN NULL
            ELSE (
                SELECT CASE WHEN LEN(arr) = 0 THEN NULL ELSE arr END
                FROM (
                    SELECT LIST_FILTER(
                        STRING_SPLIT(REPLACE(UPPER(TRIM(cleaned."{col_lower}")), '/', '*'), '*'),
                        x -> x != '' AND REGEXP_MATCHES(x, '^[A-Z][0-9]{{2,3}}$')
                    ) AS arr
                )
            )
        END"""
