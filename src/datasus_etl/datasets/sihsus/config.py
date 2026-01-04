"""SIHSUS dataset configuration.

Defines FTP paths, file patterns, and configuration specific to
the SIHSUS (Sistema de Informacoes Hospitalares) subsystem.
"""

from typing import ClassVar, Optional

from datasus_etl.datasets.base import DatasetConfig, DatasetRegistry
from datasus_etl.datasets.sihsus.schema import SIHSUS_DUCKDB_SCHEMA


@DatasetRegistry.register
class SIHSUSConfig(DatasetConfig):
    """Configuration for SIHSUS (Hospital Information System) dataset.

    SIHSUS contains hospital admission records (AIH - Autorizacao de Internacao
    Hospitalar). Data files are named with the pattern RDUFYYMM.dbc where:
    - RD = Fixed prefix for hospital admission records
    - UF = 2-letter state code (e.g., SP, RJ)
    - YY = 2-digit year
    - MM = 2-digit month
    """

    NAME: ClassVar[str] = "sihsus"
    DESCRIPTION: ClassVar[str] = "Sistema de Informacoes Hospitalares do SUS"
    FILE_PREFIX: ClassVar[str] = "RD"

    # FTP directory structure for SIHSUS data
    FTP_DIRS: ClassVar[list[dict]] = [
        {
            "path": "/dissemin/publicos/SIHSUS/199201_200712/Dados/",
            "start_year": 1992,
            "end_year": 2007,
        },
        {
            "path": "/dissemin/publicos/SIHSUS/200801_/Dados/",
            "start_year": 2008,
            "end_year": 9999,  # No upper limit
        },
    ]

    @classmethod
    def get_ftp_dirs(cls) -> list[dict]:
        """Get FTP directory configurations for SIHSUS.

        Returns:
            List of dicts with keys: path, start_year, end_year
        """
        return cls.FTP_DIRS

    @classmethod
    def get_schema(cls) -> dict[str, str]:
        """Get DuckDB schema for SIHSUS.

        Returns:
            Dictionary mapping column name (lowercase) to DuckDB SQL type
        """
        return SIHSUS_DUCKDB_SCHEMA

    @classmethod
    def parse_filename(cls, filename: str) -> Optional[dict]:
        """Parse SIHSUS filename to extract metadata.

        SIHSUS files follow the pattern: RDUFYYMM.dbc
        Example: RDSP2301.dbc -> {uf: "SP", year: 2023, month: 1}

        Args:
            filename: Name of the DBC file

        Returns:
            Dict with parsed metadata, or None if parsing fails
        """
        try:
            # Remove extension
            stem = filename.replace(".dbc", "").replace(".DBC", "")

            # Must start with RD
            if not stem.startswith("RD"):
                return None

            if len(stem) < 8:
                return None

            uf = stem[2:4]
            yy = int(stem[4:6])
            mm = int(stem[6:8])

            if mm < 1 or mm > 12:
                return None

            # Determine century based on year value
            # SIHSUS data from 1992 onwards
            if yy >= 92:
                year = 1900 + yy
            else:
                year = 2000 + yy

            return {
                "uf": uf,
                "year": year,
                "month": mm,
                "source_file": filename,
            }

        except (ValueError, IndexError):
            return None
