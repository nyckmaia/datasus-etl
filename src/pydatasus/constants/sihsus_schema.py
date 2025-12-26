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
    # Identificação geográfica e temporal
    # ========================================================================
    "uf_zi": "INTEGER",  # UF de localização (alguns registros têm 6 dígitos: 130000)
    "ano_cmpt": "SMALLINT",  # Ano de competência
    "mes_cmpt": "TINYINT",  # Mês de competência (1-12)
    "espec": "SMALLINT",  # Especialidade
    "cgc_hosp": "BIGINT",  # CNPJ do hospital
    "n_aih": "VARCHAR",  # Número da AIH (autorização de internação hospitalar)
    "ident": "TINYINT",  # Identificação
    "cep": "VARCHAR",  # CEP
    "munic_res": "INTEGER",  # Código do município de residência (IBGE)
    "municipio_res": "VARCHAR",  # Nome do município de residência
    "uf_res": "VARCHAR",  # UF de residência (sigla)
    "rg_imediata_res": "VARCHAR",  # Região imediata de residência
    "rg_intermediaria_res": "VARCHAR",  # Região intermediária de residência
    # ========================================================================
    # Dados pessoais
    # ========================================================================
    "nasc": "DATE",  # Data de nascimento
    "sexo": "VARCHAR",  # Sexo (M/F/I - masculino/feminino/ignorado)
    # ========================================================================
    # UTI - Meses (tempo em meses de UTI)
    # ========================================================================
    "uti_mes_in": "TINYINT",  # Meses de UTI - tipo IN
    "uti_mes_an": "TINYINT",  # Meses de UTI - tipo AN
    "uti_mes_al": "TINYINT",  # Meses de UTI - tipo AL
    "uti_mes_to": "TINYINT",  # Meses de UTI - total
    "marca_uti": "TINYINT",  # Marcador de UTI
    # ========================================================================
    # UTI - Internações (número de internações em UTI)
    # ========================================================================
    "uti_int_in": "TINYINT",  # Internações UTI - tipo IN
    "uti_int_an": "TINYINT",  # Internações UTI - tipo AN
    "uti_int_al": "TINYINT",  # Internações UTI - tipo AL
    "uti_int_to": "TINYINT",  # Internações UTI - total
    # ========================================================================
    # Procedimento e valores
    # ========================================================================
    "proc_rea": "VARCHAR",  # Procedimento realizado (código)
    "val_sh": "FLOAT",  # Valor serviços hospitalares
    "val_sp": "FLOAT",  # Valor serviços profissionais
    "val_sadt": "FLOAT",  # Valor SADT (apoio diagnóstico e terapêutico)
    "val_rn": "FLOAT",  # Valor recém-nascido
    "val_ortp": "FLOAT",  # Valor órteses e próteses
    "val_sangue": "FLOAT",  # Valor sangue
    "val_sadtsr": "FLOAT",  # Valor SADT sem registro
    "val_transp": "FLOAT",  # Valor transporte
    "val_obsang": "FLOAT",  # Valor observação sangue
    "val_ped1ac": "FLOAT",  # Valor pediatria 1o ano complemento
    "val_tot": "FLOAT",  # Valor total
    "val_uti": "FLOAT",  # Valor UTI
    "us_tot": "FLOAT",  # US total
    # ========================================================================
    # Datas
    # ========================================================================
    "dt_inter": "DATE",  # Data de internação
    "dt_saida": "DATE",  # Data de saída
    # ========================================================================
    # Diagnósticos
    # ========================================================================
    "diag_princ": "VARCHAR",  # Diagnóstico principal (CID-10)
    "diag_secun": "VARCHAR",  # Diagnóstico secundário (CID-10)
    # ========================================================================
    # Gestão administrativa
    # ========================================================================
    "cobranca": "TINYINT",  # Tipo de cobrança
    "natureza": "TINYINT",  # Natureza da unidade
    "gestao": "TINYINT",  # Gestão (sem til)
    "gestão": "TINYINT",  # Gestão (com til) - coluna duplicada em alguns arquivos
    "munic_mov": "INTEGER",  # Município de movimentação
    # ========================================================================
    # Idade
    # ========================================================================
    "cod_idade": "TINYINT",  # Código da unidade de idade (1=anos, 2=meses, 3=dias)
    "idade": "TINYINT",  # Idade
    "dias_perm": "SMALLINT",  # Dias de permanência
    # ========================================================================
    # Desfecho
    # ========================================================================
    "morte": "BOOLEAN",  # Óbito (true/false)
    # ========================================================================
    # Arquivo
    # ========================================================================
    "cod_arq": "TINYINT",  # Código do arquivo
    "cont": "TINYINT",  # Continuação
    "nacional": "SMALLINT",  # Nacional
    # ========================================================================
    # Procedimentos
    # ========================================================================
    "num_proc": "SMALLINT",  # Número de procedimentos
    "car_int": "TINYINT",  # Caráter de internação
    "tot_pt_sp": "SMALLINT",  # Total de pontos SP
    # ========================================================================
    # Identificação extra
    # ========================================================================
    "cpf_aut": "VARCHAR",  # CPF autorizador
    "homonimo": "TINYINT",  # Homônimo
    "num_filhos": "TINYINT",  # Número de filhos
    "instru": "TINYINT",  # Instrução
    "cid_notif": "VARCHAR",  # CID de notificação
    "contracep1": "TINYINT",  # Contraceptivo 1
    "contracep2": "TINYINT",  # Contraceptivo 2
    "gestrisco": "BOOLEAN",  # Gestação de risco
    # ========================================================================
    # Valores federais/gestão
    # ========================================================================
    "val_sh_fed": "FLOAT",  # Valor SH federal
    "val_sp_fed": "FLOAT",  # Valor SP federal
    "val_sh_ges": "FLOAT",  # Valor SH gestão
    "val_sp_ges": "FLOAT",  # Valor SP gestão
    "val_uci": "FLOAT",  # Valor UCI
    # ========================================================================
    # Diárias
    # ========================================================================
    "diar_acom": "SMALLINT",  # Diárias de acompanhante
    "qt_diarias": "SMALLINT",  # Quantidade de diárias
    # ========================================================================
    # Classificação médica
    # ========================================================================
    "cbor": "SMALLINT",  # CBO (Classificação Brasileira de Ocupações)
    "cnaer": "SMALLINT",  # CNAE
    "etnia": "SMALLINT",  # Etnia
    "raca_cor": "VARCHAR",  # Raça/cor
    # ========================================================================
    # CIDs complementares
    # ========================================================================
    "cid_asso": "VARCHAR",  # CID associado
    "cid_morte": "VARCHAR",  # CID de morte
    "diagsec1": "VARCHAR",  # Diagnóstico secundário 1
    "diagsec2": "VARCHAR",  # Diagnóstico secundário 2
    "diagsec3": "VARCHAR",  # Diagnóstico secundário 3
    "diagsec4": "VARCHAR",  # Diagnóstico secundário 4
    "diagsec5": "VARCHAR",  # Diagnóstico secundário 5
    "diagsec6": "VARCHAR",  # Diagnóstico secundário 6
    "diagsec7": "VARCHAR",  # Diagnóstico secundário 7
    "diagsec8": "VARCHAR",  # Diagnóstico secundário 8
    "diagsec9": "VARCHAR",  # Diagnóstico secundário 9
    # ========================================================================
    # Tipos de diagnósticos secundários
    # ========================================================================
    "tpdisec1": "TINYINT",  # Tipo diagnóstico secundário 1
    "tpdisec2": "TINYINT",  # Tipo diagnóstico secundário 2
    "tpdisec3": "TINYINT",  # Tipo diagnóstico secundário 3
    "tpdisec4": "TINYINT",  # Tipo diagnóstico secundário 4
    "tpdisec5": "TINYINT",  # Tipo diagnóstico secundário 5
    "tpdisec6": "TINYINT",  # Tipo diagnóstico secundário 6
    "tpdisec7": "TINYINT",  # Tipo diagnóstico secundário 7
    "tpdisec8": "TINYINT",  # Tipo diagnóstico secundário 8
    "tpdisec9": "TINYINT",  # Tipo diagnóstico secundário 9
    # ========================================================================
    # Colunas extras (presentes apenas em alguns arquivos)
    # ========================================================================
    "insc_pn": "BIGINT",  # Inscrição PN
    "seq_aih5": "SMALLINT",  # Sequência AIH 5
    "vincprev": "TINYINT",  # Vínculo previdenciário
    "gestor_cod": "SMALLINT",  # Código do gestor
    "gestor_cpf": "BIGINT",  # CPF do gestor
    "gestor_dt": "DATE",  # Data do gestor
    "cnes": "INTEGER",  # CNES (Cadastro Nacional de Estabelecimentos de Saúde)
    "cgc_mant": "BIGINT",  # CNPJ mantenedora
    "complex": "TINYINT",  # Complexidade
    "faec_tp": "INTEGER",  # Tipo FAEC
    "financ": "TINYINT",  # Financiamento
    "gestor_tp": "TINYINT",  # Tipo de gestor
    "regct": "SMALLINT",  # Registro CT
    "remessa": "VARCHAR",  # Remessa
    "sequencia": "SMALLINT",  # Sequência
    "aud_just": "VARCHAR",  # Auditoria justificativa
    "nat_jur": "SMALLINT",  # Natureza jurídica
    "sis_just": "VARCHAR",  # Sistema justificativa
    "marca_uci": "SMALLINT",  # Marcador UCI
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
