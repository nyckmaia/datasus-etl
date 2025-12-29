"""Cleaning transformation for SQL data processing.

This module provides the CleaningTransform class that removes invisible
characters, trims whitespace, and normalizes string data.
"""

from typing import Optional

from pydatasus.transform.sql.base import BaseTransform


class CleaningTransform(BaseTransform):
    """Transform that cleans string data by removing invisible characters.

    Cleaning steps applied:
    1. CAST to VARCHAR
    2. Remove invisible characters (tabs, newlines, carriage returns, null bytes, form feeds)
    3. TRIM whitespace (leading and trailing)
    4. Convert empty strings to NULL using NULLIF

    Removes these invisible characters:
    - CHR(9): Tabs
    - CHR(10): Line feeds
    - CHR(13): Carriage returns
    - CHR(0): Null bytes
    - CHR(12): Form feeds

    Example:
        >>> transform = CleaningTransform()
        >>> transform.get_sql("nome", ["nome", "cpf"])
        'NULLIF(TRIM(REPLACE(REPLACE(...))), '') AS nome'
    """

    @property
    def name(self) -> str:
        """Return transform name."""
        return "cleaning"

    def get_columns(self) -> list[str]:
        """Return empty list to apply to all columns."""
        return []  # Applies to all columns

    def get_sql(self, column: str, columns: list[str], schema: Optional[dict[str, str]] = None) -> str:
        """Generate SQL to clean a column value.

        Args:
            column: Column name to clean
            columns: List of all available columns (unused)
            schema: Optional schema dict (unused)

        Returns:
            SQL expression that removes invisible chars, trims, and converts empty to NULL
        """
        return f"""NULLIF(
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(CAST({column} AS VARCHAR), CHR(9), ''),
                                CHR(10), ''),
                            CHR(13), ''),
                        CHR(0), ''),
                    CHR(12), '')
            ),
            ''
        ) AS {column.lower()}"""

    def get_sql_expression(self, column: str) -> str:
        """Generate SQL expression without AS alias (for nesting).

        Args:
            column: Column name to clean

        Returns:
            SQL expression that cleans the value (without AS alias)
        """
        return f"""NULLIF(
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(CAST({column} AS VARCHAR), CHR(9), ''),
                                CHR(10), ''),
                            CHR(13), ''),
                        CHR(0), ''),
                    CHR(12), '')
            ),
            ''
        )"""
