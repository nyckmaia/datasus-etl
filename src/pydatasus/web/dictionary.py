"""Data dictionary for DataSUS columns.

Provides human-readable descriptions of columns in each subsystem,
helping health researchers understand the data structure.
"""

# SIHSUS - Hospital Information System Column Descriptions
SIHSUS_COLUMNS = {
    # Identification
    "n_aih": "Numero da AIH (Autorizacao de Internacao Hospitalar)",
    "ident": "Identificador do tipo de AIH",
    "cnes": "Codigo CNES do estabelecimento de saude",
    "uf": "UF de internacao (sigla do estado)",
    "source_file": "Arquivo DBC de origem",

    # Patient Demographics
    "nasc": "Data de nascimento do paciente",
    "idade": "Idade do paciente (em anos)",
    "sexo": "Sexo do paciente (M/F)",
    "raca_cor": "Raca/cor declarada",
    "instru": "Grau de instrucao",
    "nacional": "Nacionalidade",

    # Geographic
    "munic_res": "Codigo IBGE do municipio de residencia",
    "municipio_res": "Nome do municipio de residencia (enriquecido)",
    "uf_res": "UF de residencia (enriquecido)",
    "rg_imediata_res": "Regiao geografica imediata (enriquecido)",
    "rg_intermediaria_res": "Regiao geografica intermediaria (enriquecido)",
    "munic_mov": "Municipio de ocorrencia da internacao",
    "cep": "CEP do paciente",

    # Dates
    "dt_inter": "Data de internacao",
    "dt_saida": "Data de saida/alta",
    "gestor_dt": "Data do gestor",

    # Clinical Information
    "diag_princ": "Diagnostico principal (CID-10)",
    "diag_secun": "Diagnostico secundario (CID-10)",
    "proc_solic": "Procedimento solicitado",
    "proc_rea": "Procedimento realizado",
    "car_int": "Carater da internacao (eletiva/urgencia)",
    "espec": "Especialidade do leito",
    "complex": "Complexidade (alta/media/baixa)",
    "cobranca": "Motivo de cobranca",
    "morte": "Indicador de obito",
    "marca_uti": "Indicador de UTI",
    "uti_mes_to": "Total de dias em UTI",

    # Hospitalization
    "dias_perm": "Dias de permanencia",
    "qt_diarias": "Quantidade de diarias",

    # Financial
    "val_sh": "Valor dos servicos hospitalares",
    "val_sp": "Valor dos servicos profissionais",
    "val_sadt": "Valor de SADT",
    "val_rn": "Valor do recem-nascido",
    "val_acomp": "Valor do acompanhante",
    "val_uti": "Valor da UTI",
    "val_tot": "Valor total da AIH",
    "us_tot": "Total de pontos/unidades de servico",

    # Administrative
    "ano_cmpt": "Ano de competencia",
    "mes_cmpt": "Mes de competencia",
    "gestor_cod": "Codigo do gestor",
    "cnpj_mant": "CNPJ da mantenedora",
    "nat_jur": "Natureza juridica",
}

# SIM - Mortality Information System Column Descriptions
SIM_COLUMNS = {
    # Identification
    "numerodo": "Numero da Declaracao de Obito (DO)",
    "uf": "UF de ocorrencia",
    "source_file": "Arquivo DBC de origem",

    # Deceased Information
    "dtnasc": "Data de nascimento do falecido",
    "dtobito": "Data do obito",
    "idade": "Idade ao falecer",
    "sexo": "Sexo (M/F)",
    "racacor": "Raca/cor",
    "estciv": "Estado civil",
    "esc": "Escolaridade",
    "ocup": "Ocupacao (CBO)",
    "natural": "Naturalidade",

    # Geographic - Residence
    "codmunres": "Codigo IBGE do municipio de residencia",
    "baires": "Bairro de residencia",
    "endres": "Endereco de residencia",

    # Geographic - Occurrence
    "codmunocor": "Codigo IBGE do municipio de ocorrencia",
    "lococor": "Local de ocorrencia (hospital, domicilio, via publica, etc)",
    "codestab": "Codigo do estabelecimento de saude",

    # Death Information
    "tipobito": "Tipo de obito (fetal/nao-fetal)",
    "causabas": "Causa basica (CID-10)",
    "linhaa": "Linha A da DO - causa terminal",
    "linhab": "Linha B da DO - causa consequencial",
    "linhac": "Linha C da DO - causa consequencial",
    "linhad": "Linha D da DO - causa antecedente",
    "linhaii": "Linha II - outras condicoes",
    "circobito": "Circunstancia do obito (acidente, suicidio, etc)",
    "fonte": "Fonte de investigacao",

    # Maternal/Fetal
    "graession": "Semanas de gestacao",
    "gestacao": "Duracao da gestacao",
    "parto": "Tipo de parto",
    "obitoparto": "Obito em relacao ao parto (antes/durante/depois)",
    "peso": "Peso ao nascer (obitos fetais/infantis)",
    "obitograv": "Obito durante gravidez",
    "obitopuerp": "Obito no puerperio",

    # Medical Care
    "assistmed": "Houve assistencia medica",
    "necropsia": "Foi realizada necropsia",
    "exame": "Tipo de exame",

    # Administrative
    "dtinvestig": "Data de investigacao",
    "dtcadastro": "Data de cadastro no sistema",
    "dtrecebim": "Data de recebimento",
    "dtatestado": "Data do atestado",
    "dtregcart": "Data de registro em cartorio",
    "dtcadinf": "Data de cadastro de informacoes",
}

# Column descriptions by subsystem
COLUMN_DESCRIPTIONS = {
    "sihsus": SIHSUS_COLUMNS,
    "sim": SIM_COLUMNS,
    "siasus": SIHSUS_COLUMNS,  # Fallback to SIHSUS descriptions
}


def get_column_descriptions(subsystem: str) -> dict[str, str]:
    """Get column descriptions for a specific subsystem.

    Args:
        subsystem: DataSUS subsystem name (sihsus, sim, siasus)

    Returns:
        Dictionary of column_name -> description
    """
    return COLUMN_DESCRIPTIONS.get(subsystem.lower(), SIHSUS_COLUMNS)


def get_column_description(subsystem: str, column: str) -> str:
    """Get description for a specific column.

    Args:
        subsystem: DataSUS subsystem name
        column: Column name (case-insensitive)

    Returns:
        Column description or empty string if not found
    """
    descriptions = get_column_descriptions(subsystem)
    return descriptions.get(column.lower(), "")
