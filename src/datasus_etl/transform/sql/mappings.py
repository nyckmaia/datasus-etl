"""Categorical value mappings by subsystem.

This module contains mapping dictionaries for categorical fields
that differ between DataSUS subsystems (SIHSUS, SIM, etc.).
"""

# SEXO (Sex) mappings by subsystem
# Output: 'M' (Male), 'F' (Female), 'I' (Unknown/Ignored)
SEXO_MAPPINGS: dict[str, dict[str, str]] = {
    "sihsus": {
        "0": "I",
        "1": "M",
        "3": "F",
    },
    "sim": {
        # SIM accepts both letter codes and numeric codes
        "M": "M",
        "1": "M",
        "F": "F",
        "2": "F",
        # All other values map to Ignored
        "I": "I",
        "0": "I",
        "9": "I",
    },
}

# Default value for SEXO when code not found in mapping
SEXO_DEFAULT: dict[str, str] = {
    "sihsus": "NULL",  # SIHSUS: unknown codes become NULL
    "sim": "'I'",      # SIM: unknown codes become 'I' (Ignored)
}


# RACACOR (Race/Color) mappings by subsystem
# SIHSUS uses 2-digit codes, SIM uses 1-digit codes
RACACOR_MAPPINGS: dict[str, dict[str, str]] = {
    "sihsus": {
        "01": "Branca",
        "02": "Preta",
        "03": "Parda",
        "04": "Amarela",
        "05": "Indigena",
    },
    "sim": {
        "1": "Branca",
        "2": "Preta",
        "3": "Amarela",
        "4": "Parda",
        "5": "Indigena",
    },
}

# Default value for RACACOR when code not found in mapping
RACACOR_DEFAULT = "'Ignorado'"
