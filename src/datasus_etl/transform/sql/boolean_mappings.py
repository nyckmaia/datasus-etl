"""Custom BOOLEAN mappings for SIM subsystem columns.

This module defines custom true/false value mappings for BOOLEAN columns
in the SIM (Sistema de Informacoes sobre Mortalidade) subsystem.

These mappings are used when the standard '1'/'0' or 'true'/'false' patterns
don't apply to specific columns.
"""

# Custom BOOLEAN mappings for SIM subsystem
# Each column maps to a dict with:
#   - true_values: list of string values that map to TRUE
#   - false_values: list of string values that map to FALSE
# Any value not in true_values or false_values will become NULL

SIM_BOOLEAN_MAPPINGS: dict[str, dict[str, list[str]]] = {
    # Work accident: 1=yes, 2=no, 9=unknown/ignored
    "acidtrab": {
        "true_values": ["1"],
        "false_values": ["2"],
    },
    # Type of position/certification: 1=yes, 2=no, S=yes, N=no
    "tppos": {
        "true_values": ["1", "S"],
        "false_values": ["2", "N"],
    },
    # Coding status: S=yes, N=no
    "stcodifica": {
        "true_values": ["S"],
        "false_values": ["N"],
    },
    # Coded: S=yes, N=no
    "codificado": {
        "true_values": ["S"],
        "false_values": ["N"],
    },
    # Altered cause: 1=yes, 2=no
    "altcausa": {
        "true_values": ["1"],
        "false_values": ["2"],
    },
    # Epidemiological status (optional): 1=yes, 0=no
    "stdoepidem": {
        "true_values": ["1"],
        "false_values": ["0"],
    },
    # New DO status (optional): 1=yes, 0=no
    "stdonova": {
        "true_values": ["1"],
        "false_values": ["0"],
    },
}


def get_boolean_case_sql(column: str, source_expr: str) -> str:
    """Generate a CASE WHEN SQL expression for custom BOOLEAN mapping.

    Args:
        column: Column name (lowercase)
        source_expr: SQL expression for the source value (e.g., 'cleaned."column"')

    Returns:
        SQL CASE WHEN expression that maps values to TRUE/FALSE/NULL

    Example:
        >>> get_boolean_case_sql("acidtrab", 'cleaned."acidtrab"')
        "CASE WHEN cleaned.\"acidtrab\" IN ('1') THEN TRUE ..."
    """
    if column not in SIM_BOOLEAN_MAPPINGS:
        raise ValueError(f"No custom BOOLEAN mapping for column: {column}")

    mapping = SIM_BOOLEAN_MAPPINGS[column]
    true_vals = ", ".join(f"'{v}'" for v in mapping["true_values"])
    false_vals = ", ".join(f"'{v}'" for v in mapping["false_values"])

    return (
        f"CASE WHEN {source_expr} IN ({true_vals}) THEN TRUE "
        f"WHEN {source_expr} IN ({false_vals}) THEN FALSE "
        f"ELSE NULL END"
    )
