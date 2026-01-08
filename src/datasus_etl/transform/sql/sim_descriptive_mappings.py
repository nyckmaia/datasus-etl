"""Descriptive column mappings for SIM subsystem categorical fields.

This module defines value mappings for generating _desc columns in the SIM VIEW.
Each mapping converts numeric codes to human-readable Portuguese descriptions.

These columns are VIEW-only (not part of the physical schema) and are added
in the canonical CTE of the TransformPipeline.

References:
- TODO.md for mapping definitions
- DATASUS SIM documentation
"""

# Type alias for clarity
DescMapping = dict[str, str]

# All SIM descriptive mappings
# Key = source column name (without _desc suffix)
# Value = dict mapping source values (as strings) to descriptions
SIM_DESCRIPTIVE_MAPPINGS: dict[str, DescMapping] = {
    "tipobito": {
        "1": "Fetal",
        "2": "Não Fetal",
    },
    "estciv": {
        "1": "Solteiro",
        "2": "Casado",
        "3": "Viúvo",
        "4": "Separado judicialmente/divorciado",
        "5": "União estável",
        "9": "Ignorado",
    },
    "esc2010": {
        "0": "Sem escolaridade",
        "1": "Fundamental I (1ª a 4ª série)",
        "2": "Fundamental II (5ª a 8ª série)",
        "3": "Médio (antigo 2º Grau)",
        "4": "Superior incompleto",
        "5": "Superior completo",
        "9": "Ignorado",
    },
    "lococor": {
        "1": "hospital",
        "2": "outros estabelecimentos de saúde",
        "3": "domicílio",
        "4": "via pública",
        "5": "outros",
        "6": "aldeia indígena",
        "9": "ignorado",
    },
    "escmae": {
        "1": "Nenhuma",
        "2": "de 1 a 3 anos",
        "3": "de 4 a 7 anos",
        "4": "de 8 a 11 anos",
        "5": "12 anos e mais",
        "9": "Ignorado",
    },
    "escmae2010": {
        "0": "Sem escolaridade",
        "1": "Fundamental I (1ª a 4ª série)",
        "2": "Fundamental II (5ª a 8ª série)",
        "3": "Médio (antigo 2º Grau)",
        "4": "Superior incompleto",
        "5": "Superior completo",
        "9": "Ignorado",
    },
    "circobito": {
        "1": "acidente",
        "2": "suicídio",
        "3": "homicídio",
        "4": "outros",
        "9": "ignorado",
    },
    "fonte": {
        "1": "ocorrência policial",
        "2": "hospital",
        "3": "família",
        "4": "outra",
        "9": "ignorado",
    },
    "esc": {
        "1": "Nenhuma",
        "2": "de 1 a 3 anos",
        "3": "de 4 a 7 anos",
        "4": "de 8 a 11 anos",
        "5": "12 anos e mais",
        "9": "Ignorado",
    },
    "atestante": {
        "1": "Assistente",
        "2": "Substituto",
        "3": "IML",
        "4": "SVO",
        "5": "Outro",
    },
    "gestacao": {
        "1": "Menos de 22 semanas",
        "2": "22 a 27 semanas",
        "3": "28 a 31 semanas",
        "4": "32 a 36 semanas",
        "5": "37 a 41 semanas",
        "6": "42 e + semanas",
    },
    "origem": {
        "1": "Oracle",
        "2": "Banco estadual diponibilizado via FTP",
        "3": "Banco SEADE",
        "9": "Ignorado",
    },
}

# List of all descriptive column names (for easy iteration)
SIM_DESCRIPTIVE_COLUMNS: list[str] = [
    f"{col}_desc" for col in SIM_DESCRIPTIVE_MAPPINGS.keys()
]


def get_descriptive_case_sql(column: str, source_expr: str) -> str:
    """Generate a CASE WHEN SQL expression for descriptive mapping.

    Creates a SQL CASE expression that maps numeric codes to their
    human-readable descriptions. Unmapped values return NULL.

    Args:
        column: Source column name (without _desc suffix).
                Must be a key in SIM_DESCRIPTIVE_MAPPINGS.
        source_expr: SQL expression for the source value
                     (e.g., 'typed."tipobito"')

    Returns:
        SQL CASE WHEN expression that maps values to descriptions.
        The source is cast to VARCHAR to handle both numeric and string inputs.

    Raises:
        ValueError: If column is not in SIM_DESCRIPTIVE_MAPPINGS.

    Example:
        >>> get_descriptive_case_sql("tipobito", 'typed."tipobito"')
        "CASE CAST(typed.\"tipobito\" AS VARCHAR)\\n            WHEN '1' THEN 'Fetal'..."
    """
    if column not in SIM_DESCRIPTIVE_MAPPINGS:
        raise ValueError(f"No descriptive mapping for column: {column}")

    mapping = SIM_DESCRIPTIVE_MAPPINGS[column]
    when_clauses = [f"WHEN '{code}' THEN '{desc}'" for code, desc in mapping.items()]
    clauses_str = "\n            ".join(when_clauses)

    # Cast source to VARCHAR to handle both numeric (TINYINT) and string inputs
    return f"""CASE CAST({source_expr} AS VARCHAR)
            {clauses_str}
            ELSE NULL
        END"""
