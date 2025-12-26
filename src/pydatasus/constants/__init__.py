"""Constants and schema definitions for PyDataSUS."""

# Import general constants
from pydatasus.constants.general import (
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
)

from pydatasus.constants.sihsus_schema import (
    DUCKDB_TO_POLARS_TYPE_MAP,
    SIHSUS_PARQUET_SCHEMA,
    generate_column_cleaning_sql,
    generate_type_validation_sql,
    get_columns_by_type,
    get_numeric_columns,
    get_polars_schema,
    get_sql_cast_expression,
)

__all__ = [
    # Schema constants
    "SIHSUS_PARQUET_SCHEMA",
    "DUCKDB_TO_POLARS_TYPE_MAP",
    # Schema helper functions
    "get_sql_cast_expression",
    "get_polars_schema",
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
    "DBC_FILE_PATTERN",
    "SEXO_MAP",
    "RACA_COR_MAP",
    "DATE_FORMATS",
    "IBGE_7_DIGITS",
    "IBGE_6_DIGITS",
]
