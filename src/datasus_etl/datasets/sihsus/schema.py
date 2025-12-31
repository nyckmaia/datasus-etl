"""Schema definition for SIHSUS data (SIH - Sistema de Informacoes Hospitalares).

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
    # Source identification
    # ========================================================================
    "uf": "VARCHAR",  # UF state code extracted from filename (e.g., "SP", "RJ")
    "source_file": "VARCHAR",  # Original DBC filename (e.g., "RDSP2301.dbc")
    # ========================================================================
    # Geographic and temporal identification
    # ========================================================================
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
    """
    return {
        col: DUCKDB_TO_POLARS_TYPE_MAP.get(dtype, "Utf8")
        for col, dtype in SIHSUS_PARQUET_SCHEMA.items()
    }
