"""Constants and schema definitions for DataSUS ETL.

This module provides access to constants and schemas.
For new code, prefer using the datasus_etl.datasets module directly:

    from datasus_etl.datasets import SIHSUSConfig, SIMConfig
    schema = SIHSUSConfig.get_schema()
"""

# Import general constants
from datasus_etl.constants.general import (
    ALL_UFS,
    DATE_FORMATS,
    DATASUS_FTP_HOST,
    DATASUS_FTP_PASS,
    DATASUS_FTP_USER,
    DBC_FILE_PATTERN,
    IBGE_6_DIGITS,
    IBGE_7_DIGITS,
    RACA_COR_MAP,
    SEXO_MAP,
    SIHSUS_DIRS,
    SIM_DIRS,
    # Unicode symbols with ASCII fallbacks
    USE_UNICODE,
    SYM_CHECK,
    SYM_ARROW,
    SYM_FILE,
    SYM_CHART,
    SYM_FOLDER,
)

from datasus_etl.constants.sihsus_schema import (
    SIHSUS_DUCKDB_SCHEMA,
    generate_column_cleaning_sql,
    generate_type_validation_sql,
    get_columns_by_type,
    get_numeric_columns,
    get_sql_cast_expression,
)

__all__ = [
    # Schema constants
    "SIHSUS_DUCKDB_SCHEMA",
    # Schema helper functions
    "get_sql_cast_expression",
    "generate_column_cleaning_sql",
    "generate_type_validation_sql",
    "get_columns_by_type",
    "get_numeric_columns",
    # General constants
    "ALL_UFS",
    "DATASUS_FTP_HOST",
    "DATASUS_FTP_USER",
    "DATASUS_FTP_PASS",
    "SIHSUS_DIRS",
    "SIM_DIRS",
    "DBC_FILE_PATTERN",
    "SEXO_MAP",
    "RACA_COR_MAP",
    "DATE_FORMATS",
    "IBGE_7_DIGITS",
    "IBGE_6_DIGITS",
    # Unicode symbols
    "USE_UNICODE",
    "SYM_CHECK",
    "SYM_ARROW",
    "SYM_FILE",
    "SYM_CHART",
    "SYM_FOLDER",
]
