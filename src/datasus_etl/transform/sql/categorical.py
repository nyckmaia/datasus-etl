"""Categorical transformation classes for SQL data processing.

This module provides transforms for categorical data mappings:
- SexoTransform: Maps sex codes to labels (subsystem-specific)
- RacaCorTransform: Maps race/color codes to labels (subsystem-specific)
"""

from typing import Optional

from datasus_etl.transform.sql.base import BaseTransform
from datasus_etl.transform.sql.mappings import (
    SEXO_MAPPINGS,
    SEXO_DEFAULT,
    RACACOR_MAPPINGS,
    RACACOR_DEFAULT,
)


class SexoTransform(BaseTransform):
    """Transform that maps SEXO codes to human-readable labels.

    Mapping varies by subsystem:
    - SIHSUS: '0' -> 'I', '1' -> 'M', '3' -> 'F', other -> NULL
    - SIM: 'M'/'1' -> 'M', 'F'/'2' -> 'F', other -> 'I'

    Attributes:
        subsystem: DataSUS subsystem name (sihsus, sim)

    Example:
        >>> transform = SexoTransform(subsystem="sim")
        >>> transform.get_sql("sexo", ["sexo", "nome"])
        "CASE sexo WHEN 'M' THEN 'M' WHEN '1' THEN 'M' ... END AS sexo"
    """

    def __init__(self, subsystem: str = "sihsus") -> None:
        """Initialize SEXO transform.

        Args:
            subsystem: DataSUS subsystem name (default: 'sihsus')
        """
        self.subsystem = subsystem.lower()
        self.mapping = SEXO_MAPPINGS.get(self.subsystem, SEXO_MAPPINGS["sihsus"])
        self.default = SEXO_DEFAULT.get(self.subsystem, "NULL")

    @property
    def name(self) -> str:
        """Return transform name."""
        return "sexo_mapping"

    def get_columns(self) -> list[str]:
        """Return list of columns this transform applies to."""
        return ["sexo"]

    def _build_case_sql(self, col_ref: str) -> str:
        """Build CASE expression from mapping.

        Args:
            col_ref: Column reference (e.g., 'sexo' or 'cleaned."sexo"')

        Returns:
            SQL CASE expression
        """
        when_clauses = []
        for code, label in self.mapping.items():
            when_clauses.append(f"WHEN '{code}' THEN '{label}'")

        clauses_str = "\n            ".join(when_clauses)
        return f"""CASE {col_ref}
            {clauses_str}
            ELSE {self.default}
        END"""

    def get_sql(self, column: str, columns: list[str], schema: Optional[dict[str, str]] = None) -> str:
        """Generate SQL for SEXO categorical mapping.

        Args:
            column: Column name (should be 'sexo')
            columns: List of all available columns
            schema: Optional schema dict (unused)

        Returns:
            SQL CASE expression for SEXO mapping
        """
        col_lower = column.lower()
        if col_lower != "sexo":
            return f"{column} AS {col_lower}"

        return f'{self._build_case_sql(col_lower)} AS "{col_lower}"'

    def get_sql_expression(self, column: str = "sexo") -> str:
        """Generate SQL expression without AS alias.

        Args:
            column: Column name (default: 'sexo')

        Returns:
            SQL CASE expression (without AS alias)
        """
        col_lower = column.lower()
        return self._build_case_sql(f'cleaned."{col_lower}"')


class RacaCorTransform(BaseTransform):
    """Transform that maps RACACOR codes to human-readable labels.

    Mapping varies by subsystem:
    - SIHSUS: 2-digit codes ('01' -> 'Branca', '02' -> 'Preta', etc.)
    - SIM: 1-digit codes ('1' -> 'Branca', '2' -> 'Preta', etc.)

    Note: SIM uses different order: 3=Amarela, 4=Parda (vs SIHSUS 3=Parda, 4=Amarela)

    Attributes:
        subsystem: DataSUS subsystem name (sihsus, sim)

    Example:
        >>> transform = RacaCorTransform(subsystem="sim")
        >>> transform.get_sql("racacor", ["racacor", "nome"])
        "CASE racacor WHEN '1' THEN 'Branca' ... END AS racacor"
    """

    def __init__(self, subsystem: str = "sihsus") -> None:
        """Initialize RACACOR transform.

        Args:
            subsystem: DataSUS subsystem name (default: 'sihsus')
        """
        self.subsystem = subsystem.lower()
        self.mapping = RACACOR_MAPPINGS.get(self.subsystem, RACACOR_MAPPINGS["sihsus"])

    @property
    def name(self) -> str:
        """Return transform name."""
        return "racacor_mapping"

    def get_columns(self) -> list[str]:
        """Return list of columns this transform applies to."""
        return ["racacor", "raca_cor"]

    def _build_case_sql(self, col_ref: str) -> str:
        """Build CASE expression from mapping.

        Args:
            col_ref: Column reference (e.g., 'racacor' or 'cleaned."racacor"')

        Returns:
            SQL CASE expression
        """
        when_clauses = []
        for code, label in self.mapping.items():
            when_clauses.append(f"WHEN '{code}' THEN '{label}'")

        clauses_str = "\n            ".join(when_clauses)
        return f"""CASE {col_ref}
            {clauses_str}
            ELSE {RACACOR_DEFAULT}
        END"""

    def get_sql(self, column: str, columns: list[str], schema: Optional[dict[str, str]] = None) -> str:
        """Generate SQL for RACACOR categorical mapping.

        Args:
            column: Column name (should be 'racacor' or 'raca_cor')
            columns: List of all available columns
            schema: Optional schema dict (unused)

        Returns:
            SQL CASE expression for RACACOR mapping
        """
        col_lower = column.lower()
        if col_lower not in ("racacor", "raca_cor"):
            return f"{column} AS {col_lower}"

        return f'{self._build_case_sql(col_lower)} AS "{col_lower}"'

    def get_sql_expression(self, column: str = "racacor") -> str:
        """Generate SQL expression without AS alias.

        Args:
            column: Column name (default: 'racacor')

        Returns:
            SQL CASE expression (without AS alias)
        """
        col_lower = column.lower()
        return self._build_case_sql(f'cleaned."{col_lower}"')
