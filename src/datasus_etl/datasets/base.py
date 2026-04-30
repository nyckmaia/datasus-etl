"""Base configuration class for DataSUS datasets.

All subsystem configurations inherit from DatasetConfig and provide
their specific schema, FTP paths, and file patterns.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, Optional


@dataclass(frozen=True)
class ViewSpec:
    """Metadata describing a single VIEW exposed by a subsystem.

    Used by ``/api/query/schema`` to render the SUBSYSTEM → VIEWS → COLUMNS
    tree. When ``DatasetConfig.views`` is set, the endpoint reads its specs
    verbatim instead of running the default discovery convention (main view
    = subsystem name, ``{subsystem}_dim_*`` = dim views, ``_all`` = hidden).
    Each spec carries optional PT/EN labels and a free-text description for
    the future tooltip.

    Attributes:
        name: DuckDB view name (e.g. ``sihsus_dim_diag``).
        role: ``"main"`` (the enriched aggregate view) or ``"dim"`` (a
            dimension lookup view, typically two columns: code + decoded
            value).
        label_pt: Optional Portuguese display label. UI falls back to
            ``name`` when None.
        label_en: Optional English display label. UI falls back to ``name``
            when None.
        description: Optional one-line description shown as a tooltip when
            the user hovers the view header.
    """

    name: str
    role: str  # "main" | "dim"
    label_pt: Optional[str] = None
    label_en: Optional[str] = None
    description: Optional[str] = None


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

    # Column in the raw `{name}_all` view holding the 6-digit IBGE residence
    # municipality code. Used by the enriched `{name}` view to LEFT JOIN
    # `ibge_locais.codigo_municipio_6_digitos` and add uf_res / municipio_res /
    # rg_imediata_res / rg_intermediaria_res. Subsystems whose schema has no
    # such column should leave this as None — the enriched view falls back to
    # a plain alias of the raw view.
    RESIDENCE_MUNICIPALITY_COLUMN: ClassVar[Optional[str]] = None

    # Optional explicit list of views exposed by this subsystem. When None,
    # the schema endpoint falls back to the convention: main view = exact
    # subsystem name; dim views = names matching `{subsystem}_dim_*`; the
    # raw `{subsystem}_all` view is hidden. Set this attribute to override
    # the convention, add PT/EN labels, or surface views whose names don't
    # follow the prefix scheme.
    views: ClassVar[Optional[list[ViewSpec]]] = None

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
