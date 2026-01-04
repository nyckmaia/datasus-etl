"""Constants for DataSUS ETL.

This module contains general constants for the DataSUS ETL package.
For SIHSUS schema definitions, see datasus_etl.constants.sihsus_schema.
"""

import os
import sys


def _can_use_unicode() -> bool:
    """Check if the current environment supports Unicode output."""
    # Check for NO_COLOR or TERM=dumb (set by Web Interface subprocess)
    if os.environ.get("NO_COLOR") or os.environ.get("TERM") == "dumb":
        return False

    # Check if stdout is a terminal
    if not sys.stdout.isatty():
        return False

    # On Windows, check encoding
    if sys.platform == "win32":
        try:
            # Try to encode a Unicode character
            "✓".encode(sys.stdout.encoding or "utf-8")
            return True
        except (UnicodeEncodeError, LookupError):
            return False

    return True


# Unicode symbols with ASCII fallbacks
USE_UNICODE = _can_use_unicode()
SYM_CHECK = "✓" if USE_UNICODE else "[OK]"
SYM_ARROW = "→" if USE_UNICODE else "->"
SYM_FILE = "📄" if USE_UNICODE else ""
SYM_CHART = "📊" if USE_UNICODE else ""
SYM_FOLDER = "📁" if USE_UNICODE else ""


# DATASUS FTP Configuration
DATASUS_FTP_HOST = "ftp.datasus.gov.br"
DATASUS_FTP_USER = ""  # Anonymous
DATASUS_FTP_PASS = ""  # Anonymous

# SIHSUS FTP Directories (hospital admission data)
SIHSUS_DIRS = [
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

# SIM FTP Directories (mortality data)
# Uses different directories for CID-9 (before 1996) and CID-10 (1996+)
SIM_DIRS = [
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

# All Brazilian states (UF codes)
ALL_UFS = [
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]

# File name patterns
DBC_FILE_PATTERN = "RD*.dbc"  # SIHSUS files start with RD

# Column mappings for SIHSUS data
SEXO_MAP = {"0": "I", "1": "M", "3": "F"}  # I=Ignorado, M=Masculino, F=Feminino

RACA_COR_MAP = {
    "01": "Branca",
    "02": "Preta",
    "03": "Amarela",
    "04": "Parda",
    "05": "Indígena",
    "99": "Indeterminado",
}

# Date format patterns
DATE_FORMATS = [
    "%Y%m%d",
    "%d%m%Y",
    "%d%m%y",
    "%d/%m/%y",
    "%d/%m/%Y",
]

# IBGE municipality codes
IBGE_7_DIGITS = 7  # Full municipality code
IBGE_6_DIGITS = 6  # Municipality code without check digit
