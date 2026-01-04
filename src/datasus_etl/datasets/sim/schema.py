"""Schema definition for SIM data (Sistema de Informacoes sobre Mortalidade).

This module defines the formal schema for SIM DuckDB output tables.
SIM contains death/mortality records from death certificates (DO - Declaracao de Obito).

Column types are specified using DuckDB SQL types.

Important Notes:
-----------------
1. All columns are initially read as TEXT/VARCHAR during DBF import
2. Transformations and type validation are applied in DuckDB SQL queries
3. This schema defines the FINAL types after all transformations
4. Column names in output are lowercase
5. SIM files have yearly data (not monthly like SIHSUS)

References:
- ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/
- DATASUS SIM documentation
"""

# DuckDB SQL type mapping for SIM schema
# Maps column name (lowercase) -> DuckDB SQL type
SIM_DUCKDB_SCHEMA: dict[str, str] = {
    # ========================================================================
    # Source identification
    # ========================================================================
    "uf": "VARCHAR",  # UF state code extracted from filename (e.g., "SP", "RJ")
    "source_file": "VARCHAR",  # Original DBC filename (e.g., "DOSP2023.dbc")
    # ========================================================================
    # Death certificate identification
    # ========================================================================
    "numerodo": "VARCHAR",  # Death certificate number
    "tipobito": "TINYINT",  # Type of death (1=fetal, 2=non-fetal)
    "dtobito": "DATE",  # Date of death
    "horaobito": "VARCHAR",  # Time of death (HHMM)
    "natural": "VARCHAR",  # Naturalness (place of birth)
    "dtnasc": "DATE",  # Date of birth
    # ========================================================================
    # Demographics
    # ========================================================================
    "idade": "VARCHAR",  # Age code (format varies by age unit)
    "sexo": "VARCHAR",  # Sex (1=M, 2=F, 0/9=Unknown)
    "racacor": "VARCHAR",  # Race/color (1-5 + 9=unknown)
    "estciv": "VARCHAR",  # Marital status
    "esc": "VARCHAR",  # Education level (old coding)
    "esc2010": "VARCHAR",  # Education level (2010 coding)
    "ocup": "VARCHAR",  # Occupation (CBO)
    # ========================================================================
    # Residence information
    # ========================================================================
    "codmunres": "INTEGER",  # Residence municipality (IBGE code)
    "lococor": "TINYINT",  # Place of occurrence (1=hospital, 2=other health, etc.)
    "codestab": "VARCHAR",  # Health establishment code (CNES)
    "codmunocor": "INTEGER",  # Occurrence municipality (IBGE code)
    "idademae": "TINYINT",  # Mother's age (for fetal/infant deaths)
    "escmae": "VARCHAR",  # Mother's education
    "escmae2010": "VARCHAR",  # Mother's education (2010 coding)
    "ocupmae": "VARCHAR",  # Mother's occupation
    # ========================================================================
    # Cause of death (ICD-10)
    # ========================================================================
    "causabas": "VARCHAR",  # Underlying cause of death (ICD-10)
    "linhaa": "VARCHAR",  # Line A - immediate cause
    "linhab": "VARCHAR",  # Line B - intermediate cause
    "linhac": "VARCHAR",  # Line C - intermediate cause
    "linhad": "VARCHAR",  # Line D - underlying cause
    "linhaii": "VARCHAR",  # Part II - contributing conditions
    # ========================================================================
    # Medical certification
    # ========================================================================
    "circobito": "TINYINT",  # Circumstance of death
    "acidtrab": "TINYINT",  # Work accident (1=yes, 2=no, 9=unknown)
    "fonte": "TINYINT",  # Source of information
    "tppos": "TINYINT",  # Type of position/certification
    "dtinvestig": "DATE",  # Investigation date
    "causabas_o": "VARCHAR",  # Original underlying cause
    "dtcadastro": "DATE",  # Registration date
    "atession": "TINYINT",  # Medical care
    "fonteinv": "TINYINT",  # Investigation source
    "dtrecebim": "DATE",  # Receipt date
    # ========================================================================
    # Pregnancy and childbirth (for maternal/fetal deaths)
    # ========================================================================
    "tpmorteoco": "TINYINT",  # Type of death occurrence
    "semagestac": "TINYINT",  # Weeks of gestation
    "tpgravid": "TINYINT",  # Type of pregnancy
    "gestacao": "TINYINT",  # Gestation period
    "parto": "TINYINT",  # Type of delivery
    "obitoparto": "TINYINT",  # Death during delivery
    "peso": "SMALLINT",  # Birth weight (grams)
    "obitograv": "TINYINT",  # Death during pregnancy
    "obitopuerp": "TINYINT",  # Death during puerperium
    "assistmed": "TINYINT",  # Medical assistance
    "exame": "TINYINT",  # Medical examination
    "cirurgia": "TINYINT",  # Surgery
    "necropsia": "TINYINT",  # Autopsy
    # ========================================================================
    # Violence and external causes
    # ========================================================================
    "dtatestado": "DATE",  # Certificate date
    "circobito": "TINYINT",  # Circumstance (duplicate for external causes context)
    "tppos": "TINYINT",  # Type of position
    # ========================================================================
    # Additional fields (may vary by year)
    # ========================================================================
    "codinst": "VARCHAR",  # Institution code
    "numerolote": "VARCHAR",  # Batch number
    "versaosist": "VARCHAR",  # System version
    "versaoscb": "VARCHAR",  # SCB version
    "codcart": "VARCHAR",  # Registry office code
    "numregcart": "VARCHAR",  # Registry number
    "dtregcart": "DATE",  # Registry date
    "contador": "INTEGER",  # Counter
    "difdata": "SMALLINT",  # Date difference
    "dtcadinf": "DATE",  # Information registration date
    "stcodifica": "VARCHAR",  # Coding status
    "codificado": "VARCHAR",  # Coded
    "cb_pre": "VARCHAR",  # Pre-coding underlying cause
    "comunsvoam": "VARCHAR",  # Common SVO
    "comundinf": "VARCHAR",  # Common information
    "tpassam": "VARCHAR",  # Type of assistance (detailed)
    "altcausa": "VARCHAR",  # Altered cause
    "ufinform": "VARCHAR",  # Informing state
    "nudissam": "VARCHAR",  # Dissemination number
}
