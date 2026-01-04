"""Base configuration class for DataSUS datasets.

All subsystem configurations inherit from DatasetConfig and provide
their specific schema, FTP paths, and file patterns.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, Optional


@dataclass
class DatasetConfig(ABC):
    """Base configuration for a DataSUS dataset/subsystem.

    Each subsystem (SIHSUS, SIM, SIASUS, etc.) must implement this interface
    to provide its specific configuration for downloading, parsing, and
    transforming data.

    Attributes:
        name: Short name of the subsystem (e.g., "sihsus", "sim")
        description: Human-readable description
        ftp_dirs: List of FTP directory configurations
        file_prefix: Prefix for data files (e.g., "RD" for SIHSUS)
        schema: Dictionary mapping column names to DuckDB types
    """

    # Class-level constants (to be overridden in subclasses)
    NAME: ClassVar[str] = ""
    DESCRIPTION: ClassVar[str] = ""
    FILE_PREFIX: ClassVar[str] = ""

    @classmethod
    @abstractmethod
    def get_ftp_dirs(cls) -> list[dict]:
        """Get FTP directory configurations.

        Returns:
            List of dicts with keys: path, start_year, end_year
        """
        ...

    @classmethod
    @abstractmethod
    def get_schema(cls) -> dict[str, str]:
        """Get DuckDB schema mapping column names to types.

        Returns:
            Dictionary mapping column name (lowercase) to DuckDB SQL type
        """
        ...

    @classmethod
    def get_file_pattern(cls) -> str:
        """Get file pattern for matching DBC files.

        Returns:
            Glob pattern like "RD*.dbc" for SIHSUS
        """
        return f"{cls.FILE_PREFIX}*.dbc"

    @classmethod
    def get_numeric_columns(cls) -> list[str]:
        """Get list of numeric column names from schema.

        Returns:
            List of column names that are numeric types
        """
        numeric_types = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE"}
        return [
            col
            for col, dtype in cls.get_schema().items()
            if dtype in numeric_types
        ]

    @classmethod
    def get_date_columns(cls) -> list[str]:
        """Get list of date column names from schema.

        Returns:
            List of column names that are DATE type
        """
        return [
            col for col, dtype in cls.get_schema().items() if dtype == "DATE"
        ]

    @classmethod
    def parse_filename(cls, filename: str) -> Optional[dict]:
        """Parse filename to extract metadata (UF, year, month).

        Default implementation handles standard DATASUS naming:
        PREFIX + UF(2) + YY(2) + MM(2) + .dbc
        Example: RDSP2301.dbc -> {uf: "SP", year: 2023, month: 1}

        Args:
            filename: Name of the DBC file

        Returns:
            Dict with parsed metadata, or None if parsing fails
        """
        try:
            # Remove extension
            stem = filename.replace(".dbc", "").replace(".DBC", "")

            # Extract components (PREFIX + UF + YYMM)
            prefix_len = len(cls.FILE_PREFIX)
            if not stem.startswith(cls.FILE_PREFIX):
                return None

            remaining = stem[prefix_len:]
            if len(remaining) < 6:
                return None

            uf = remaining[:2]
            yy = int(remaining[2:4])
            mm = int(remaining[4:6])

            if mm < 1 or mm > 12:
                return None

            # Determine century based on year value
            # Most DATASUS data is from 2000+
            year = 2000 + yy if yy < 50 else 1900 + yy

            return {
                "uf": uf,
                "year": year,
                "month": mm,
                "source_file": filename,
            }

        except (ValueError, IndexError):
            return None


class DatasetRegistry:
    """Registry of available dataset configurations.

    Provides a centralized way to look up dataset configurations by name.
    """

    _datasets: dict[str, type[DatasetConfig]] = {}

    @classmethod
    def register(cls, config_class: type[DatasetConfig]) -> type[DatasetConfig]:
        """Register a dataset configuration class.

        Can be used as a decorator:
            @DatasetRegistry.register
            class MyDatasetConfig(DatasetConfig):
                ...

        Args:
            config_class: The configuration class to register

        Returns:
            The same config_class (for decorator usage)
        """
        cls._datasets[config_class.NAME.lower()] = config_class
        return config_class

    @classmethod
    def get(cls, name: str) -> Optional[type[DatasetConfig]]:
        """Get a dataset configuration by name.

        Args:
            name: Name of the dataset (case-insensitive)

        Returns:
            Configuration class, or None if not found
        """
        return cls._datasets.get(name.lower())

    @classmethod
    def list_available(cls) -> list[str]:
        """List all registered dataset names.

        Returns:
            List of dataset names
        """
        return list(cls._datasets.keys())

    @classmethod
    def get_all(cls) -> dict[str, type[DatasetConfig]]:
        """Get all registered datasets.

        Returns:
            Dictionary mapping name to configuration class
        """
        return cls._datasets.copy()
