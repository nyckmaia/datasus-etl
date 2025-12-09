"""Constants for PyDataSUS."""

# DATASUS FTP Configuration
DATASUS_FTP_HOST = "ftp.datasus.gov.br"
DATASUS_FTP_USER = ""  # Anonymous
DATASUS_FTP_PASS = ""  # Anonymous

# SIHSUS FTP Directories (historical data)
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
