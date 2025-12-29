"""Base class for SQL transformations.

This module defines the abstract base class for all SQL transformation classes.
Each transformation class encapsulates a specific data transformation operation
that can be composed into a pipeline.
"""

from abc import ABC, abstractmethod
from typing import Optional


class BaseTransform(ABC):
    """Abstract base class for SQL transformations.

    Each transform encapsulates a specific transformation operation that
    generates SQL expressions. Transforms can be composed into a pipeline
    for sequential application.

    Attributes:
        name: Human-readable name for the transform (used in logging)

    Example:
        >>> class MyTransform(BaseTransform):
        ...     @property
        ...     def name(self) -> str:
        ...         return "my_transform"
        ...
        ...     def get_sql(self, column: str, columns: list[str]) -> str:
        ...         return f"UPPER({column})"
        ...
        ...     def get_columns(self) -> list[str]:
        ...         return []  # applies to all columns
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of this transform.

        Returns:
            Human-readable name for logging and debugging
        """
        pass

    @abstractmethod
    def get_sql(self, column: str, columns: list[str], schema: Optional[dict[str, str]] = None) -> str:
        """Generate SQL expression for transforming a column.

        Args:
            column: Name of the column to transform
            columns: List of all available columns in the source table
            schema: Optional schema dict mapping column names to DuckDB types

        Returns:
            SQL expression string for the transformation
        """
        pass

    @abstractmethod
    def get_columns(self) -> list[str]:
        """Return list of columns this transform applies to.

        Returns:
            List of column names this transform should be applied to.
            Empty list means the transform applies to all columns.
        """
        pass

    def applies_to(self, column: str) -> bool:
        """Check if this transform applies to a specific column.

        Args:
            column: Column name to check

        Returns:
            True if transform should be applied to this column
        """
        target_columns = self.get_columns()
        if not target_columns:
            # Empty list means applies to all columns
            return True
        return column.lower() in [c.lower() for c in target_columns]

    def __repr__(self) -> str:
        """Return string representation."""
        return f"{self.__class__.__name__}(name={self.name!r})"
