"""SQL query templates for common health research analyses.

Provides pre-built SQL queries for epidemiological analyses,
organized by DataSUS subsystem.
"""

# SIHSUS - Hospital Information System Templates
SIHSUS_TEMPLATES = {
    "Internacoes por UF": """SELECT uf, COUNT(*) as total, SUM(val_tot) as valor_total
FROM sihsus
GROUP BY uf
ORDER BY total DESC""",

    "Serie Temporal Mensal": """SELECT
    EXTRACT(YEAR FROM dt_inter) as ano,
    EXTRACT(MONTH FROM dt_inter) as mes,
    COUNT(*) as internacoes,
    SUM(val_tot) as valor_total
FROM sihsus
WHERE dt_inter IS NOT NULL
GROUP BY ano, mes
ORDER BY ano, mes""",

    "Top 10 Diagnosticos (CID-10)": """SELECT diag_princ, COUNT(*) as total
FROM sihsus
WHERE diag_princ IS NOT NULL
GROUP BY diag_princ
ORDER BY total DESC
LIMIT 10""",

    "Top 10 Procedimentos": """SELECT proc_rea, COUNT(*) as total, SUM(val_tot) as valor
FROM sihsus
WHERE proc_rea IS NOT NULL
GROUP BY proc_rea
ORDER BY total DESC
LIMIT 10""",

    "Internacoes por Sexo": """SELECT
    sexo,
    COUNT(*) as total,
    ROUND(AVG(dias_perm), 1) as media_dias
FROM sihsus
WHERE sexo IS NOT NULL
GROUP BY sexo
ORDER BY total DESC""",

    "Internacoes por Faixa Etaria": """SELECT
    CASE
        WHEN idade < 1 THEN '0-1 ano'
        WHEN idade BETWEEN 1 AND 4 THEN '1-4 anos'
        WHEN idade BETWEEN 5 AND 14 THEN '5-14 anos'
        WHEN idade BETWEEN 15 AND 24 THEN '15-24 anos'
        WHEN idade BETWEEN 25 AND 44 THEN '25-44 anos'
        WHEN idade BETWEEN 45 AND 64 THEN '45-64 anos'
        ELSE '65+ anos'
    END as faixa_etaria,
    COUNT(*) as total
FROM sihsus
WHERE idade IS NOT NULL
GROUP BY faixa_etaria
ORDER BY MIN(idade)""",

    "Media de Permanencia por UF": """SELECT
    uf,
    COUNT(*) as internacoes,
    ROUND(AVG(dias_perm), 1) as media_dias,
    MAX(dias_perm) as max_dias
FROM sihsus
WHERE dias_perm IS NOT NULL
GROUP BY uf
ORDER BY media_dias DESC""",

    "Valor Total por Mes": """SELECT
    EXTRACT(YEAR FROM dt_inter) as ano,
    EXTRACT(MONTH FROM dt_inter) as mes,
    SUM(val_tot) as valor_total,
    COUNT(*) as internacoes
FROM sihsus
WHERE dt_inter IS NOT NULL
GROUP BY ano, mes
ORDER BY ano, mes""",
}

# SIM - Mortality Information System Templates
SIM_TEMPLATES = {
    "Obitos por UF": """SELECT uf, COUNT(*) as total
FROM sim
GROUP BY uf
ORDER BY total DESC""",

    "Serie Temporal Mensal": """SELECT
    EXTRACT(YEAR FROM dtobito) as ano,
    EXTRACT(MONTH FROM dtobito) as mes,
    COUNT(*) as obitos
FROM sim
WHERE dtobito IS NOT NULL
GROUP BY ano, mes
ORDER BY ano, mes""",

    "Top 10 Causas Basicas (CID-10)": """SELECT causabas, COUNT(*) as total
FROM sim
WHERE causabas IS NOT NULL
GROUP BY causabas
ORDER BY total DESC
LIMIT 10""",

    "Obitos por Sexo": """SELECT
    sexo,
    COUNT(*) as total
FROM sim
WHERE sexo IS NOT NULL
GROUP BY sexo
ORDER BY total DESC""",

    "Obitos por Faixa Etaria": """SELECT
    CASE
        WHEN idade < 1 THEN '0-1 ano'
        WHEN idade BETWEEN 1 AND 4 THEN '1-4 anos'
        WHEN idade BETWEEN 5 AND 14 THEN '5-14 anos'
        WHEN idade BETWEEN 15 AND 24 THEN '15-24 anos'
        WHEN idade BETWEEN 25 AND 44 THEN '25-44 anos'
        WHEN idade BETWEEN 45 AND 64 THEN '45-64 anos'
        ELSE '65+ anos'
    END as faixa_etaria,
    COUNT(*) as total
FROM sim
WHERE idade IS NOT NULL
GROUP BY faixa_etaria
ORDER BY MIN(idade)""",

    "Obitos por Local de Ocorrencia": """SELECT
    lococor,
    COUNT(*) as total
FROM sim
WHERE lococor IS NOT NULL
GROUP BY lococor
ORDER BY total DESC""",

    "Obitos por Escolaridade": """SELECT
    esc,
    COUNT(*) as total
FROM sim
WHERE esc IS NOT NULL
GROUP BY esc
ORDER BY total DESC""",

    "Obitos por Raca/Cor": """SELECT
    racacor,
    COUNT(*) as total
FROM sim
WHERE racacor IS NOT NULL
GROUP BY racacor
ORDER BY total DESC""",
}

# Template mapping by subsystem
TEMPLATES = {
    "sihsus": SIHSUS_TEMPLATES,
    "sim": SIM_TEMPLATES,
    "siasus": SIHSUS_TEMPLATES,  # Fallback to SIHSUS patterns
}


def get_templates(subsystem: str) -> dict[str, str]:
    """Get SQL templates for a specific subsystem.

    Args:
        subsystem: DataSUS subsystem name (sihsus, sim, siasus)

    Returns:
        Dictionary of template name -> SQL query
    """
    return TEMPLATES.get(subsystem.lower(), SIHSUS_TEMPLATES)
