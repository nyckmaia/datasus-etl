"""Date parsing transformation for SQL data processing.

This module provides the DateParsingTransform class that parses date strings
in multiple formats with optional future date validation.
"""

from typing import Optional

from pydatasus.transform.sql.base import BaseTransform


class DateParsingTransform(BaseTransform):
    """Transform that parses date strings with multiple format fallback.

    Tries date formats in order:
    1. YYYYMMDD (e.g., "20200131")
    2. DDMMYYYY (e.g., "31012020")
    3. YYYY-MM-DD (e.g., "2020-01-31")
    4. Direct cast (fallback)

    When allow_future=False (default), validates that parsed dates are not
    in the future. This prevents ambiguous dates like "20260115" from being
    parsed as 2026-01-15 when they might actually be 2015-01-26.

    Attributes:
        date_columns: List of column names to treat as dates
        allow_future: Whether to allow dates in the future (default: False)

    Example:
        >>> transform = DateParsingTransform(
        ...     date_columns=["dt_inter", "dt_saida"],
        ...     allow_future=False
        ... )
        >>> transform.get_sql("dt_inter", ["dt_inter", "nome"])
        'COALESCE(CASE WHEN TRY_CAST(...) <= CURRENT_DATE ...) AS dt_inter'
    """

    def __init__(
        self,
        date_columns: Optional[list[str]] = None,
        allow_future: bool = False,
    ) -> None:
        """Initialize date parsing transform.

        Args:
            date_columns: List of column names to parse as dates.
                         If None, defaults to common DataSUS date columns.
            allow_future: Whether to allow dates in the future (default: False)
        """
        self._date_columns = date_columns or [
            "dt_inter", "dt_saida", "nasc", "gestor_dt",  # SIHSUS
            "dtobito", "dtnasc", "dtinvestig", "dtcadastro",  # SIM
            "dtrecebim", "dtatestado", "dtregcart", "dtcadinf",  # SIM additional
        ]
        self.allow_future = allow_future

    @property
    def name(self) -> str:
        """Return transform name."""
        return "date_parsing"

    def get_columns(self) -> list[str]:
        """Return list of date columns this transform applies to."""
        return self._date_columns

    def get_sql(self, column: str, columns: list[str], schema: Optional[dict[str, str]] = None) -> str:
        """Generate SQL for parsing a date column.

        Args:
            column: Column name to parse
            columns: List of all available columns
            schema: Optional schema dict

        Returns:
            SQL expression with multi-format date parsing and optional validation
        """
        col_lower = column.lower()

        # Only parse columns that are in our date list
        if col_lower not in [c.lower() for c in self._date_columns]:
            return f"{column} AS {col_lower}"

        # Check if column exists in source
        if col_lower not in [c.lower() for c in columns]:
            return f"NULL AS {col_lower}_parsed"

        if self.allow_future:
            return self._get_simple_parse_sql(column)
        else:
            return self._get_validated_parse_sql(column)

    def _get_simple_parse_sql(self, column: str) -> str:
        """Generate simple date parsing SQL without future validation.

        Args:
            column: Column name to parse

        Returns:
            SQL expression for date parsing
        """
        col_lower = column.lower()
        return f"""COALESCE(
            TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y%m%d') AS DATE),
            TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%d%m%Y') AS DATE),
            TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y-%m-%d') AS DATE),
            TRY_CAST(NULLIF({col_lower}, '') AS DATE)
        ) AS {col_lower}_parsed"""

    def _get_validated_parse_sql(self, column: str) -> str:
        """Generate date parsing SQL with future date validation.

        Args:
            column: Column name to parse

        Returns:
            SQL expression for date parsing with future date rejection
        """
        col_lower = column.lower()
        return f"""COALESCE(
            -- Try format 1: YYYYMMDD with date validation
            CASE
                WHEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y%m%d') AS DATE) IS NOT NULL
                     AND TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y%m%d') AS DATE) <= CURRENT_DATE
                THEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y%m%d') AS DATE)
                ELSE NULL
            END,
            -- Try format 2: DDMMYYYY with date validation
            CASE
                WHEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%d%m%Y') AS DATE) IS NOT NULL
                     AND TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%d%m%Y') AS DATE) <= CURRENT_DATE
                THEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%d%m%Y') AS DATE)
                ELSE NULL
            END,
            -- Try format 3: YYYY-MM-DD with date validation
            CASE
                WHEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y-%m-%d') AS DATE) IS NOT NULL
                     AND TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y-%m-%d') AS DATE) <= CURRENT_DATE
                THEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y-%m-%d') AS DATE)
                ELSE NULL
            END,
            -- Fallback: direct cast with validation
            CASE
                WHEN TRY_CAST(NULLIF({col_lower}, '') AS DATE) IS NOT NULL
                     AND TRY_CAST(NULLIF({col_lower}, '') AS DATE) <= CURRENT_DATE
                THEN TRY_CAST(NULLIF({col_lower}, '') AS DATE)
                ELSE NULL
            END
        ) AS {col_lower}_parsed"""

    def get_sql_expression(self, column: str) -> str:
        """Generate date parsing SQL expression without AS alias.

        Args:
            column: Column name to parse

        Returns:
            SQL expression for date parsing (without AS alias)
        """
        col_lower = column.lower()
        if self.allow_future:
            return f"""COALESCE(
                TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y%m%d') AS DATE),
                TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%d%m%Y') AS DATE),
                TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y-%m-%d') AS DATE),
                TRY_CAST(NULLIF({col_lower}, '') AS DATE)
            )"""
        else:
            return f"""COALESCE(
                CASE
                    WHEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y%m%d') AS DATE) IS NOT NULL
                         AND TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y%m%d') AS DATE) <= CURRENT_DATE
                    THEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y%m%d') AS DATE)
                    ELSE NULL
                END,
                CASE
                    WHEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%d%m%Y') AS DATE) IS NOT NULL
                         AND TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%d%m%Y') AS DATE) <= CURRENT_DATE
                    THEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%d%m%Y') AS DATE)
                    ELSE NULL
                END,
                CASE
                    WHEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y-%m-%d') AS DATE) IS NOT NULL
                         AND TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y-%m-%d') AS DATE) <= CURRENT_DATE
                    THEN TRY_CAST(STRPTIME(NULLIF({col_lower}, ''), '%Y-%m-%d') AS DATE)
                    ELSE NULL
                END,
                CASE
                    WHEN TRY_CAST(NULLIF({col_lower}, '') AS DATE) IS NOT NULL
                         AND TRY_CAST(NULLIF({col_lower}, '') AS DATE) <= CURRENT_DATE
                    THEN TRY_CAST(NULLIF({col_lower}, '') AS DATE)
                    ELSE NULL
                END
            )"""
