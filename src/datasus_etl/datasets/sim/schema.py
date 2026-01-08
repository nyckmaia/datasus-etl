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
    "tipobito": "TINYINT",  # Type of death (1=fetal, 2=non-fetal)
    "dtobito": "DATE",  # Date of death
    "horaobito": "TIME",  # Time of death (HHMM -> HH:MM)
    "natural": "SMALLINT",  # Naturalness (place of birth) - 16 bits
    "dtnasc": "DATE",  # Date of birth
    # ========================================================================
    # Demographics
    # ========================================================================
    "idade": "VARCHAR",  # Age code (format varies by age unit) - original encoded value
    # Derived age columns (decoded from IDADE field):
    # Format: first digit = unit (1=min, 2=hr, 3=months, 4=yrs, 5=>100yrs, 9=ignored)
    #         next 2 digits = value
    "idade_valor": "INTEGER",  # Numeric age value (NULL if idade is invalid/ignored)
    "idade_unidade": "VARCHAR",  # Age unit: 'minutos', 'horas', 'meses', 'anos', 'ignorado'
    "sexo": "VARCHAR",  # Sex (1=M, 2=F, 0/9=Unknown)
    "racacor": "VARCHAR",  # Race/color (1-5 + 9=unknown)
    "estciv": "TINYINT",  # Marital status - 8 bits
    "esc": "TINYINT",  # Education level (old coding) - 8 bits
    "esc2010": "TINYINT",  # Education level (2010 coding) - 8 bits
    "ocup": "INTEGER",  # Occupation (CBO) - 32 bits
    # ========================================================================
    # Residence information
    # ========================================================================
    "codmunres": "INTEGER",  # Residence municipality (IBGE code)
    "lococor": "TINYINT",  # Place of occurrence (1=hospital, 2=other health, etc.)
    "codestab": "INTEGER",  # Health establishment code (CNES) - 32 bits
    "codmunocor": "INTEGER",  # Occurrence municipality (IBGE code)
    "idademae": "TINYINT",  # Mother's age (for fetal/infant deaths)
    "escmae": "TINYINT",  # Mother's education - 8 bits
    "escmae2010": "TINYINT",  # Mother's education (2010 coding) - 8 bits
    "ocupmae": "INTEGER",  # Mother's occupation - 32 bits
    # ========================================================================
    # Cause of death (ICD-10)
    # CID columns are stored as VARCHAR[] arrays because:
    # 1. Raw data contains asterisks: *A01*J128 = multiple CIDs
    # 2. Array preserves all CIDs without data loss
    # 3. Each element is validated (letter + 2-3 digits)
    # ========================================================================
    "causabas": "VARCHAR[]",  # Underlying cause of death (ICD-10 array)
    "linhaa": "VARCHAR[]",  # Line A - immediate cause (array)
    "linhab": "VARCHAR[]",  # Line B - intermediate cause (array)
    "linhac": "VARCHAR[]",  # Line C - intermediate cause (array)
    "linhad": "VARCHAR[]",  # Line D - underlying cause (array)
    "linhaii": "VARCHAR[]",  # Part II - contributing conditions (array)
    # ========================================================================
    # Medical certification
    # ========================================================================
    "circobito": "TINYINT",  # Circumstance of death
    "acidtrab": "BOOLEAN",  # Work accident (1=true, 2=false, 9=null)
    "fonte": "TINYINT",  # Source of information
    "tppos": "BOOLEAN",  # Type of position/certification (1=true, 2=false)
    "dtinvestig": "DATE",  # Investigation date
    "causabas_o": "VARCHAR",  # Original underlying cause
    "dtcadastro": "DATE",  # Registration date
    "atestado": "VARCHAR[]",  # Medical care
    "atestante": "TINYINT",  # Doctor condition of the certifying doctor
    "fonteinv": "TINYINT",  # Investigation source
    "dtrecebim": "DATE",  # Receipt date
    # ========================================================================
    # Pregnancy and childbirth (for maternal/fetal deaths)
    # ========================================================================
    "tpmorteoco": "TINYINT",  # Type of death occurrence
    "semagestac": "TINYINT",  # Weeks of gestation
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
    # ========================================================================
    # Additional fields (may vary by year)
    # ========================================================================
    "numerolote": "INTEGER",  # Batch number - 32 bits
    "versaosist": "VARCHAR",  # System version
    "versaoscb": "VARCHAR",  # SCB version
    "contador": "INTEGER",  # Counter
    "difdata": "SMALLINT",  # Date difference
    "dtcadinf": "DATE",  # Information registration date
    "dtrecoriga": "DATE",  # Original receipt date
    "dtcadinv": "DATE",  # Investigation registration date
    "dtconinv": "DATE",  # Investigation conclusion date
    "dtconcaso": "DATE",  # Case conclusion date
    "stcodifica": "BOOLEAN",  # Coding status (S=true, N=false)
    "codificado": "BOOLEAN",  # Coded (S=true, N=false)
    "cb_pre": "VARCHAR",  # Pre-coding underlying cause
    "altcausa": "BOOLEAN",  # Altered cause (1=true, 2=false)
    "comunsvoim": "INTEGER",  # City code of the SVO or IML municipality
    # ========================================================================
    # Optional fields (may not exist in all files)
    # ========================================================================
    "stdoepidem": "BOOLEAN",  # Epidemiological status (1=true, 0=false)
    "stdonova": "BOOLEAN",  # New DO status (1=true, 0=false)
}
