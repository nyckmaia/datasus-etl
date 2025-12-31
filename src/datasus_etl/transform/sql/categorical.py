"""Categorical transformation classes for SQL data processing.

This module provides transforms for categorical data mappings:
- SexoTransform: Maps sex codes to labels (0->I, 1->M, 3->F)
- RacaCorTransform: Maps race/color codes to labels
"""

from typing import Optional

from datasus_etl.transform.sql.base import BaseTransform


class SexoTransform(BaseTransform):
    """Transform that maps SEXO codes to human-readable labels.

    Mapping:
    - '0' -> 'I' (Ignorado/Unknown)
    - '1' -> 'M' (Masculino/Male)
    - '3' -> 'F' (Feminino/Female)
    - Other -> NULL

    Example:
        >>> transform = SexoTransform()
        >>> transform.get_sql("sexo", ["sexo", "nome"])
        "CASE sexo WHEN '0' THEN 'I' WHEN '1' THEN 'M' WHEN '3' THEN 'F' ELSE NULL END AS sexo"
    """

    @property
    def name(self) -> str:
        """Return transform name."""
        return "sexo_mapping"

    def get_columns(self) -> list[str]:
        """Return list of columns this transform applies to."""
        return ["sexo"]

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

        return f"""CASE {col_lower}
            WHEN '0' THEN 'I'
            WHEN '1' THEN 'M'
            WHEN '3' THEN 'F'
            ELSE NULL
        END AS {col_lower}"""

    def get_sql_expression(self, column: str = "sexo") -> str:
        """Generate SQL expression without AS alias.

        Args:
            column: Column name (default: 'sexo')

        Returns:
            SQL CASE expression (without AS alias)
        """
        col_lower = column.lower()
        return f"""CASE {col_lower}
            WHEN '0' THEN 'I'
            WHEN '1' THEN 'M'
            WHEN '3' THEN 'F'
            ELSE NULL
        END"""


class RacaCorTransform(BaseTransform):
    """Transform that maps RACA_COR codes to human-readable labels.

    Mapping (IBGE classification):
    - '01' -> 'Branca'
    - '02' -> 'Preta'
    - '03' -> 'Parda'
    - '04' -> 'Amarela'
    - '05' -> 'Indigena'
    - Other -> 'Ignorado'

    Example:
        >>> transform = RacaCorTransform()
        >>> transform.get_sql("raca_cor", ["raca_cor", "nome"])
        "CASE raca_cor WHEN '01' THEN 'Branca' ... END AS raca_cor"
    """

    @property
    def name(self) -> str:
        """Return transform name."""
        return "raca_cor_mapping"

    def get_columns(self) -> list[str]:
        """Return list of columns this transform applies to."""
        return ["raca_cor"]

    def get_sql(self, column: str, columns: list[str], schema: Optional[dict[str, str]] = None) -> str:
        """Generate SQL for RACA_COR categorical mapping.

        Args:
            column: Column name (should be 'raca_cor')
            columns: List of all available columns
            schema: Optional schema dict (unused)

        Returns:
            SQL CASE expression for RACA_COR mapping
        """
        col_lower = column.lower()
        if col_lower != "raca_cor":
            return f"{column} AS {col_lower}"

        return f"""CASE {col_lower}
            WHEN '01' THEN 'Branca'
            WHEN '02' THEN 'Preta'
            WHEN '03' THEN 'Parda'
            WHEN '04' THEN 'Amarela'
            WHEN '05' THEN 'Indigena'
            ELSE 'Ignorado'
        END AS {col_lower}"""

    def get_sql_expression(self, column: str = "raca_cor") -> str:
        """Generate SQL expression without AS alias.

        Args:
            column: Column name (default: 'raca_cor')

        Returns:
            SQL CASE expression (without AS alias)
        """
        col_lower = column.lower()
        return f"""CASE {col_lower}
            WHEN '01' THEN 'Branca'
            WHEN '02' THEN 'Preta'
            WHEN '03' THEN 'Parda'
            WHEN '04' THEN 'Amarela'
            WHEN '05' THEN 'Indigena'
            ELSE 'Ignorado'
        END"""
