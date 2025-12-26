"""Schema definition for SIHSUS data (SIH - Sistema de Informações Hospitalares).

This module defines the formal schema for SIHSUS Parquet output files.
Column types are specified using DuckDB SQL types, as all transformations
are performed in DuckDB before Parquet export.

Important Notes:
-----------------
1. All columns are initially read as TEXT/VARCHAR during DBF import
2. Transformations and type validation are applied in DuckDB SQL queries
3. This schema defines the FINAL types after all transformations
4. Column names in output Parquet are lowercase (transformed during SQL processing)
5. Some columns have data quality issues in source files, requiring validation
"""

# DuckDB SQL type mapping for SIHSUS Parquet schema
# Maps column name (lowercase) -> DuckDB SQL type
SIHSUS_PARQUET_SCHEMA: dict[str, str] = {
    # ========================================================================
    # Geographic and temporal identification
    # ========================================================================
    "uf": "VARCHAR",  # UF state code extracted from filename (e.g., "SP", "RJ")
    "uf_zi": "INTEGER",  # UF location code (some records have 6 digits: 130000)
    "ano_cmpt": "SMALLINT",  # Competency year
    "mes_cmpt": "TINYINT",  # Competency month (1-12)
    "espec": "SMALLINT",  # Medical specialty
    "cgc_hosp": "BIGINT",  # Hospital CNPJ (tax ID)
    "n_aih": "VARCHAR",  # AIH number (hospital admission authorization)
    "ident": "TINYINT",  # Identification
    "cep": "VARCHAR",  # ZIP code
    "munic_res": "INTEGER",  # Residence municipality code (IBGE)
    "municipio_res": "VARCHAR",  # Residence municipality name
    "uf_res": "VARCHAR",  # Residence state (abbreviation)
    "rg_imediata_res": "VARCHAR",  # Immediate residence region
    "rg_intermediaria_res": "VARCHAR",  # Intermediate residence region
    # ========================================================================
    # Personal data
    # ========================================================================
    "nasc": "DATE",  # Date of birth
    "sexo": "VARCHAR",  # Sex (M/F/I - male/female/unknown)
    # ========================================================================
    # ICU - Months (time in months in ICU)
    # ========================================================================
    "uti_mes_in": "TINYINT",  # ICU months - type IN
    "uti_mes_an": "TINYINT",  # ICU months - type AN
    "uti_mes_al": "TINYINT",  # ICU months - type AL
    "uti_mes_to": "TINYINT",  # ICU months - total
    "marca_uti": "TINYINT",  # ICU marker
    # ========================================================================
    # ICU - Admissions (number of ICU admissions)
    # ========================================================================
    "uti_int_in": "TINYINT",  # ICU admissions - type IN
    "uti_int_an": "TINYINT",  # ICU admissions - type AN
    "uti_int_al": "TINYINT",  # ICU admissions - type AL
    "uti_int_to": "TINYINT",  # ICU admissions - total
    # ========================================================================
    # Procedure and costs
    # ========================================================================
    "proc_rea": "VARCHAR",  # Procedure performed (code)
    "val_sh": "FLOAT",  # Hospital services cost
    "val_sp": "FLOAT",  # Professional services cost
    "val_sadt": "FLOAT",  # SADT cost (diagnostic and therapeutic support)
    "val_rn": "FLOAT",  # Newborn cost
    "val_ortp": "FLOAT",  # Orthoses and prostheses cost
    "val_sangue": "FLOAT",  # Blood cost
    "val_sadtsr": "FLOAT",  # SADT cost without registration
    "val_transp": "FLOAT",  # Transportation cost
    "val_obsang": "FLOAT",  # Blood observation cost
    "val_ped1ac": "FLOAT",  # Pediatrics 1st year complement cost
    "val_tot": "FLOAT",  # Total cost
    "val_uti": "FLOAT",  # ICU cost
    "us_tot": "FLOAT",  # Total US
    # ========================================================================
    # Dates
    # ========================================================================
    "dt_inter": "DATE",  # Admission date
    "dt_saida": "DATE",  # Discharge date
    # ========================================================================
    # Diagnoses
    # ========================================================================
    "diag_princ": "VARCHAR",  # Primary diagnosis (ICD-10)
    "diag_secun": "VARCHAR",  # Secondary diagnosis (ICD-10)
    # ========================================================================
    # Administrative management
    # ========================================================================
    "cobranca": "TINYINT",  # Billing type
    "natureza": "TINYINT",  # Unit nature
    "gestao": "TINYINT",  # Management (without tilde)
    "gestão": "TINYINT",  # Management (with tilde) - duplicate column in some files
    "munic_mov": "INTEGER",  # Movement municipality
    # ========================================================================
    # Age
    # ========================================================================
    "cod_idade": "TINYINT",  # Age unit code (1=years, 2=months, 3=days)
    "idade": "TINYINT",  # Age
    "dias_perm": "SMALLINT",  # Days of stay
    # ========================================================================
    # Outcome
    # ========================================================================
    "morte": "BOOLEAN",  # Death (true/false)
    # ========================================================================
    # File
    # ========================================================================
    "cod_arq": "TINYINT",  # File code
    "cont": "TINYINT",  # Continuation
    "nacional": "SMALLINT",  # National
    # ========================================================================
    # Procedures
    # ========================================================================
    "num_proc": "SMALLINT",  # Number of procedures
    "car_int": "TINYINT",  # Admission character
    "tot_pt_sp": "SMALLINT",  # Total SP points
    # ========================================================================
    # Extra identification
    # ========================================================================
    "cpf_aut": "VARCHAR",  # Authorizer CPF (tax ID)
    "homonimo": "TINYINT",  # Homonym
    "num_filhos": "TINYINT",  # Number of children
    "instru": "TINYINT",  # Education level
    "cid_notif": "VARCHAR",  # Notification ICD
    "contracep1": "TINYINT",  # Contraceptive 1
    "contracep2": "TINYINT",  # Contraceptive 2
    "gestrisco": "BOOLEAN",  # High-risk pregnancy
    # ========================================================================
    # Federal/management costs
    # ========================================================================
    "val_sh_fed": "FLOAT",  # Federal SH cost
    "val_sp_fed": "FLOAT",  # Federal SP cost
    "val_sh_ges": "FLOAT",  # Management SH cost
    "val_sp_ges": "FLOAT",  # Management SP cost
    "val_uci": "FLOAT",  # ICU cost
    # ========================================================================
    # Daily rates
    # ========================================================================
    "diar_acom": "SMALLINT",  # Companion daily rates
    "qt_diarias": "SMALLINT",  # Number of daily rates
    # ========================================================================
    # Medical classification
    # ========================================================================
    "cbor": "SMALLINT",  # CBO (Brazilian Occupation Classification)
    "cnaer": "SMALLINT",  # CNAE
    "etnia": "SMALLINT",  # Ethnicity
    "raca_cor": "VARCHAR",  # Race/color
    # ========================================================================
    # Complementary ICDs
    # ========================================================================
    "cid_asso": "VARCHAR",  # Associated ICD
    "cid_morte": "VARCHAR",  # Death ICD
    "diagsec1": "VARCHAR",  # Secondary diagnosis 1
    "diagsec2": "VARCHAR",  # Secondary diagnosis 2
    "diagsec3": "VARCHAR",  # Secondary diagnosis 3
    "diagsec4": "VARCHAR",  # Secondary diagnosis 4
    "diagsec5": "VARCHAR",  # Secondary diagnosis 5
    "diagsec6": "VARCHAR",  # Secondary diagnosis 6
    "diagsec7": "VARCHAR",  # Secondary diagnosis 7
    "diagsec8": "VARCHAR",  # Secondary diagnosis 8
    "diagsec9": "VARCHAR",  # Secondary diagnosis 9
    # ========================================================================
    # Secondary diagnosis types
    # ========================================================================
    "tpdisec1": "TINYINT",  # Secondary diagnosis type 1
    "tpdisec2": "TINYINT",  # Secondary diagnosis type 2
    "tpdisec3": "TINYINT",  # Secondary diagnosis type 3
    "tpdisec4": "TINYINT",  # Secondary diagnosis type 4
    "tpdisec5": "TINYINT",  # Secondary diagnosis type 5
    "tpdisec6": "TINYINT",  # Secondary diagnosis type 6
    "tpdisec7": "TINYINT",  # Secondary diagnosis type 7
    "tpdisec8": "TINYINT",  # Secondary diagnosis type 8
    "tpdisec9": "TINYINT",  # Secondary diagnosis type 9
    # ========================================================================
    # Extra columns (present only in some files)
    # ========================================================================
    "insc_pn": "BIGINT",  # PN registration
    "seq_aih5": "SMALLINT",  # AIH sequence 5
    "vincprev": "TINYINT",  # Social security link
    "gestor_cod": "SMALLINT",  # Manager code
    "gestor_cpf": "BIGINT",  # Manager CPF (tax ID)
    "gestor_dt": "DATE",  # Manager date
    "cnes": "INTEGER",  # CNES (National Registry of Health Establishments)
    "cgc_mant": "BIGINT",  # Maintainer CNPJ (tax ID)
    "complex": "TINYINT",  # Complexity
    "faec_tp": "INTEGER",  # FAEC type
    "financ": "TINYINT",  # Financing
    "gestor_tp": "TINYINT",  # Manager type
    "regct": "SMALLINT",  # CT registry
    "remessa": "VARCHAR",  # Remittance
    "sequencia": "SMALLINT",  # Sequence
    "aud_just": "VARCHAR",  # Audit justification
    "nat_jur": "SMALLINT",  # Legal nature
    "sis_just": "VARCHAR",  # System justification
    "marca_uci": "SMALLINT",  # UCI marker
}


# Helper function to generate SQL CAST expressions
def get_sql_cast_expression(column_name: str) -> str:
    """Generate SQL CAST expression for a column based on schema.

    Args:
        column_name: Name of the column (lowercase)

    Returns:
        SQL CAST expression, e.g., "CAST(col AS INTEGER)"
        If column not in schema, returns column name as-is (VARCHAR)

    Example:
        >>> get_sql_cast_expression("idade")
        'CAST(idade AS TINYINT)'
        >>> get_sql_cast_expression("unknown_col")
        'unknown_col'
    """
    if column_name not in SIHSUS_PARQUET_SCHEMA:
        return column_name  # Return as-is if not in schema (stays VARCHAR)

    sql_type = SIHSUS_PARQUET_SCHEMA[column_name]

    # Special handling for DATE columns (need TRY_CAST for validation)
    if sql_type == "DATE":
        return f"TRY_CAST({column_name} AS DATE)"

    # Special handling for BOOLEAN (convert 0/1 to true/false)
    if sql_type == "BOOLEAN":
        return f"CAST({column_name} AS BOOLEAN)"

    # Regular CAST for numeric types
    return f"CAST({column_name} AS {sql_type})"


# Mapping from DuckDB types to Polars types (for Parquet export compatibility)
DUCKDB_TO_POLARS_TYPE_MAP = {
    "TINYINT": "Int8",
    "SMALLINT": "Int16",
    "INTEGER": "Int32",
    "BIGINT": "Int64",
    "FLOAT": "Float32",
    "DOUBLE": "Float64",
    "BOOLEAN": "Boolean",
    "DATE": "Date",
    "VARCHAR": "Utf8",
}


def get_polars_schema() -> dict[str, str]:
    """Convert DuckDB schema to Polars schema for Parquet export.

    Returns:
        Dictionary mapping column name -> Polars type string

    Example:
        >>> schema = get_polars_schema()
        >>> schema["idade"]
        'Int8'
        >>> schema["nasc"]
        'Date'
    """
    return {
        col: DUCKDB_TO_POLARS_TYPE_MAP.get(dtype, "Utf8")
        for col, dtype in SIHSUS_PARQUET_SCHEMA.items()
    }


def generate_column_cleaning_sql() -> str:
    """Generate SQL for cleaning all columns (TRIM + UPPER + remove invisible chars).

    Generates comprehensive cleaning for all columns:
    - Removes tabs, newlines, carriage returns, null bytes, form feeds
    - Trims leading/trailing whitespace
    - Converts to uppercase

    Returns:
        SQL column list with cleaning transformations, comma-separated

    Example output:
        TRIM(UPPER(REPLACE(...CAST(uf_zi AS VARCHAR)...))) AS UF_ZI,
        TRIM(UPPER(REPLACE(...CAST(ano_cmpt AS VARCHAR)...))) AS ANO_CMPT,
        ...
    """
    cleaned = []
    for col in SIHSUS_PARQUET_SCHEMA.keys():
        col_upper = col.upper()  # DBF columns are uppercase initially

        # Build nested REPLACE operations for invisible character removal
        clean_expr = f"""TRIM(UPPER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(CAST({col_upper} AS VARCHAR), CHR(9), ' '),
                            CHR(10), ' '),
                        CHR(13), ' '),
                    CHR(0), ''),
                CHR(12), '')
        ))"""
        cleaned.append(f"{clean_expr} AS {col_upper}")

    return ",\n                ".join(cleaned)


def generate_type_validation_sql(suffix: str = "_typed") -> str:
    """Generate SQL for type validation and conversion of all columns.

    Uses TRY_CAST to safely convert string values to target types.
    Invalid values become NULL instead of causing errors.

    Args:
        suffix: Suffix to append to column names (e.g., "_typed", "_num", "_int")
                If empty string, uses original column name

    Returns:
        SQL column list with type conversions, comma-separated

    Example output (suffix="_typed"):
        TRY_CAST(uf_zi AS INTEGER) AS uf_zi_typed,
        TRY_CAST(idade AS TINYINT) AS idade_typed,
        TRY_CAST(val_tot AS FLOAT) AS val_tot_typed,
        ...

    Example output (suffix=""):
        TRY_CAST(uf_zi AS INTEGER) AS uf_zi,
        TRY_CAST(idade AS TINYINT) AS idade,
        ...
    """
    conversions = []
    for col, dtype in SIHSUS_PARQUET_SCHEMA.items():
        col_upper = col.upper()
        target_name = f"{col_upper}{suffix}" if suffix else col_upper

        # Use TRY_CAST for safe conversion (returns NULL on error)
        if dtype == "DATE":
            # Dates require special handling with STRPTIME
            conversions.append(f"TRY_CAST({col_upper} AS DATE) AS {target_name}")
        elif dtype == "BOOLEAN":
            # Boolean conversion from 0/1
            conversions.append(
                f"CASE WHEN {col_upper} IN ('1', 'true', 'TRUE') THEN TRUE "
                f"WHEN {col_upper} IN ('0', 'false', 'FALSE') THEN FALSE "
                f"ELSE NULL END AS {target_name}"
            )
        else:
            conversions.append(f"TRY_CAST({col_upper} AS {dtype}) AS {target_name}")

    return ",\n                ".join(conversions)


def get_columns_by_type(sql_type: str) -> list[str]:
    """Get all column names of a specific SQL type.

    Args:
        sql_type: DuckDB SQL type (e.g., "INTEGER", "FLOAT", "DATE", "VARCHAR")

    Returns:
        List of column names (lowercase) with the specified type

    Example:
        >>> get_columns_by_type("FLOAT")
        ['val_sh', 'val_sp', 'val_tot', ...]
        >>> get_columns_by_type("DATE")
        ['nasc', 'dt_inter', 'dt_saida', 'gestor_dt']
    """
    return [col for col, dtype in SIHSUS_PARQUET_SCHEMA.items() if dtype == sql_type]


def get_numeric_columns() -> list[str]:
    """Get all numeric column names.

    Returns:
        List of column names (lowercase) that are numeric types
        (TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE)

    Example:
        >>> numeric = get_numeric_columns()
        >>> 'idade' in numeric
        True
        >>> 'val_tot' in numeric
        True
        >>> 'sexo' in numeric
        False
    """
    numeric_types = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE"}
    return [
        col for col, dtype in SIHSUS_PARQUET_SCHEMA.items() if dtype in numeric_types
    ]
