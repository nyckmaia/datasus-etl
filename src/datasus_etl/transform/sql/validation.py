"""Validation transformation classes for SQL data processing.

This module provides transforms for data validation:
- CidValidationTransform: Validates ICD-10 (CID) code format
"""

from typing import Optional

from datasus_etl.transform.sql.base import BaseTransform


class CidValidationTransform(BaseTransform):
    """Transform that validates CID (ICD-10) code format.

    Valid CID format: Single uppercase letter followed by 2 or 3 digits
    - Valid examples: A01, B123, Z999
    - Invalid examples: ABC (only letters), 123 (only numbers), A1 (too short),
                       A1234 (too long), a01 (lowercase)

    Invalid values are converted to NULL.

    Attributes:
        cid_columns: List of column names to validate as CID codes

    Example:
        >>> transform = CidValidationTransform(cid_columns=["cid_morte"])
        >>> sql = transform.get_sql("cid_morte", ["cid_morte", "sexo"])
        >>> # Returns: CASE WHEN ... regexp_matches(...) THEN cid_morte ELSE NULL END AS cid_morte
    """

    def __init__(self, cid_columns: Optional[list[str]] = None) -> None:
        """Initialize CID validation transform.

        Args:
            cid_columns: List of column names to validate as CID codes.
                        Defaults to ["cid_morte"] if None.
        """
        self._cid_columns = cid_columns or ["cid_morte"]

    @property
    def name(self) -> str:
        """Return transform name."""
        return "cid_validation"

    def get_columns(self) -> list[str]:
        """Return list of columns this transform applies to."""
        return self._cid_columns

    def get_sql(
        self,
        column: str,
        columns: list[str],
        schema: Optional[dict[str, str]] = None,
    ) -> str:
        """Generate SQL for CID validation.

        Args:
            column: Column name to validate
            columns: List of all available columns
            schema: Optional schema dict (unused)

        Returns:
            SQL CASE expression that validates CID format
        """
        col_lower = column.lower()

        # Only validate columns in our CID list
        if col_lower not in [c.lower() for c in self._cid_columns]:
            return f"{column} AS {col_lower}"

        # CID validation regex: ^[A-Z][0-9]{2,3}$
        # - ^ start of string
        # - [A-Z] exactly one uppercase letter
        # - [0-9]{2,3} exactly 2 or 3 digits
        # - $ end of string
        return f"""CASE
            WHEN {col_lower} IS NOT NULL
                 AND regexp_matches({col_lower}, '^[A-Z][0-9]{{2,3}}$')
            THEN {col_lower}
            ELSE NULL
        END AS {col_lower}"""

    def get_sql_expression(self, column: str) -> str:
        """Generate SQL expression without AS alias.

        This method is used when the validation is applied within
        a SELECT statement where the column alias is added separately.

        Args:
            column: Column name to validate

        Returns:
            SQL CASE expression (without AS alias)
        """
        col_lower = column.lower()
        return f"""CASE
            WHEN {col_lower} IS NOT NULL
                 AND regexp_matches({col_lower}, '^[A-Z][0-9]{{2,3}}$')
            THEN {col_lower}
            ELSE NULL
        END"""
