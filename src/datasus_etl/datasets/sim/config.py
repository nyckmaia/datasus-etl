"""SIM dataset configuration.

Defines FTP paths, file patterns, and configuration specific to
the SIM (Sistema de Informacoes sobre Mortalidade) subsystem.
"""

from typing import ClassVar, Optional

from datasus_etl.datasets.base import DatasetConfig, DatasetRegistry
from datasus_etl.datasets.sim.schema import SIM_DUCKDB_SCHEMA


@DatasetRegistry.register
class SIMConfig(DatasetConfig):
    """Configuration for SIM (Mortality Information System) dataset.

    SIM contains death/mortality records from death certificates
    (DO - Declaracao de Obito). Data files are named with the pattern
    DOUFYYYY.dbc where:
    - DO = Fixed prefix for death declarations
    - UF = 2-letter state code (e.g., SP, RJ)
    - YYYY = 4-digit year

    Note: SIM files are organized by year, not by month like SIHSUS.
    """

    NAME: ClassVar[str] = "sim"
    DESCRIPTION: ClassVar[str] = "Sistema de Informacoes sobre Mortalidade"
    FILE_PREFIX: ClassVar[str] = "DO"

    # FTP directory structure for SIM data
    # SIM uses a different structure with CID9 (before 1996) and CID10 (1996+)
    FTP_DIRS: ClassVar[list[dict]] = [
        {
            "path": "/dissemin/publicos/SIM/CID9/DORES/",
            "start_year": 1979,
            "end_year": 1995,
        },
        {
            "path": "/dissemin/publicos/SIM/CID10/DORES/",
            "start_year": 1996,
            "end_year": 9999,  # No upper limit
        },
    ]

    @classmethod
    def get_ftp_dirs(cls) -> list[dict]:
        """Get FTP directory configurations for SIM.

        Returns:
            List of dicts with keys: path, start_year, end_year
        """
        return cls.FTP_DIRS

    @classmethod
    def get_schema(cls) -> dict[str, str]:
        """Get DuckDB schema for SIM.

        Returns:
            Dictionary mapping column name (lowercase) to DuckDB SQL type
        """
        return SIM_DUCKDB_SCHEMA

    @classmethod
    def parse_filename(cls, filename: str) -> Optional[dict]:
        """Parse SIM filename to extract metadata.

        SIM files follow the pattern: DOUFYYYY.dbc
        Example: DOSP2023.dbc -> {uf: "SP", year: 2023}

        Note: Unlike SIHSUS, SIM files are yearly, not monthly.

        Args:
            filename: Name of the DBC file

        Returns:
            Dict with parsed metadata, or None if parsing fails
        """
        try:
            # Remove extension
            stem = filename.replace(".dbc", "").replace(".DBC", "")

            # Must start with DO
            if not stem.startswith("DO"):
                return None

            if len(stem) < 8:
                return None

            uf = stem[2:4]
            year = int(stem[4:8])

            # Validate year range
            if year < 1979 or year > 2100:
                return None

            return {
                "uf": uf,
                "year": year,
                "month": None,  # SIM is yearly, not monthly
                "source_file": filename,
            }

        except (ValueError, IndexError):
            return None

    @classmethod
    def get_file_pattern(cls) -> str:
        """Get file pattern for matching SIM DBC files.

        Returns:
            Glob pattern for SIM files
        """
        return "DO*.dbc"
