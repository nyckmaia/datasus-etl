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
    (DO - Declaracao de Obito). Data files have two patterns:

    CID10 (1996+): DOUFYYYY.dbc
    - DO = Fixed prefix for death declarations
    - UF = 2-letter state code (e.g., SP, RJ)
    - YYYY = 4-digit year
    - Example: DOSP2023.dbc

    CID9 (1979-1995): DORUFYY.dbc
    - DOR = Fixed prefix for CID9 death declarations
    - UF = 2-letter state code
    - YY = 2-digit year
    - Example: DORSP80.dbc (1980)

    Note: SIM files are organized by year, not by month like SIHSUS.
    """

    NAME: ClassVar[str] = "sim"
    DESCRIPTION: ClassVar[str] = "Sistema de Informacoes sobre Mortalidade"
    FILE_PREFIX: ClassVar[str] = "DO"  # Primary prefix (also matches DOR)

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

        Supports two patterns:
        - CID10 (1996+): DOUFYYYY.dbc (e.g., DOSP2023.dbc)
        - CID9 (1979-1995): DORUFYY.dbc (e.g., DORSP80.dbc)

        Note: Unlike SIHSUS, SIM files are yearly, not monthly.

        Args:
            filename: Name of the DBC file

        Returns:
            Dict with parsed metadata, or None if parsing fails
        """
        try:
            # Remove extension
            stem = filename.replace(".dbc", "").replace(".DBC", "")
            upper_stem = stem.upper()

            # CID9 pattern: DORUFYY (DOR + UF + 2 digit year)
            # Must check DOR first since it also starts with DO
            if upper_stem.startswith("DOR"):
                if len(stem) >= 7:
                    uf = stem[3:5].upper()
                    yy = int(stem[5:7])
                    # CID9 data is from 1979-1995
                    # Years 79-99 are 1979-1999, years 00-78 would be 2000s (but CID9 stops at 1995)
                    year = 1900 + yy if yy >= 79 else 2000 + yy
                    if 1979 <= year <= 1995:
                        return {
                            "uf": uf,
                            "year": year,
                            "month": None,  # SIM is yearly
                            "source_file": filename,
                            "cid_version": "CID9",
                        }

            # CID10 pattern: DOUFYYYY (DO + UF + 4 digit year)
            if upper_stem.startswith("DO") and not upper_stem.startswith("DOR"):
                if len(stem) >= 8:
                    uf = stem[2:4].upper()
                    year = int(stem[4:8])
                    if 1996 <= year <= 2100:
                        return {
                            "uf": uf,
                            "year": year,
                            "month": None,  # SIM is yearly
                            "source_file": filename,
                            "cid_version": "CID10",
                        }

            return None

        except (ValueError, IndexError):
            return None

    @classmethod
    def get_file_pattern(cls) -> str:
        """Get file pattern for matching SIM DBC files.

        Returns:
            Glob pattern for SIM files
        """
        return "DO*.dbc"
