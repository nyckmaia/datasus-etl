"""Data dictionary for DataSUS columns.

Provides human-readable descriptions of columns in each subsystem,
helping health researchers understand the data structure.
"""

# SIHSUS - Hospital Information System Column Descriptions
# Based on SIHSUS_DUCKDB_SCHEMA from constants/sihsus_schema.py
SIHSUS_COLUMNS = {
    # Source identification
    "uf": "UF de internacao (sigla do estado)",
    "source_file": "Arquivo DBC de origem",

    # Geographic and temporal identification
    "uf_zi": "Codigo UF do local",
    "ano_cmpt": "Ano de competencia",
    "mes_cmpt": "Mes de competencia (1-12)",
    "espec": "Especialidade medica",
    "cgc_hosp": "CNPJ do hospital",
    "n_aih": "Numero da AIH (Autorizacao de Internacao Hospitalar)",
    "ident": "Identificador do tipo de AIH",
    "cep": "CEP do paciente",
    "munic_res": "Codigo IBGE do municipio de residencia",
    "municipio_res": "Nome do municipio de residencia (enriquecido)",
    "uf_res": "UF de residencia (enriquecido)",
    "rg_imediata_res": "Regiao geografica imediata (enriquecido)",
    "rg_intermediaria_res": "Regiao geografica intermediaria (enriquecido)",

    # Personal data
    "nasc": "Data de nascimento do paciente",
    "sexo": "Sexo do paciente (M/F/I)",

    # ICU - Months (time in months in ICU)
    "uti_mes_in": "Meses em UTI - tipo IN",
    "uti_mes_an": "Meses em UTI - tipo AN",
    "uti_mes_al": "Meses em UTI - tipo AL",
    "uti_mes_to": "Meses em UTI - total",
    "marca_uti": "Indicador de UTI",

    # ICU - Admissions
    "uti_int_in": "Internacoes em UTI - tipo IN",
    "uti_int_an": "Internacoes em UTI - tipo AN",
    "uti_int_al": "Internacoes em UTI - tipo AL",
    "uti_int_to": "Internacoes em UTI - total",

    # Procedure and costs
    "proc_rea": "Procedimento realizado (codigo)",
    "val_sh": "Valor dos servicos hospitalares",
    "val_sp": "Valor dos servicos profissionais",
    "val_sadt": "Valor de SADT (apoio diagnostico/terapeutico)",
    "val_rn": "Valor do recem-nascido",
    "val_ortp": "Valor de orteses e proteses",
    "val_sangue": "Valor de sangue",
    "val_sadtsr": "Valor SADT sem registro",
    "val_transp": "Valor de transporte",
    "val_obsang": "Valor observacao de sangue",
    "val_ped1ac": "Valor complemento pediatria 1o ano",
    "val_tot": "Valor total da AIH",
    "val_uti": "Valor da UTI",
    "us_tot": "Total de pontos/unidades de servico",

    # Dates
    "dt_inter": "Data de internacao",
    "dt_saida": "Data de saida/alta",

    # Diagnoses
    "diag_princ": "Diagnostico principal (CID-10)",
    "diag_secun": "Diagnostico secundario (CID-10)",

    # Administrative management
    "cobranca": "Tipo de cobranca",
    "natureza": "Natureza da unidade",
    "gestao": "Gestao",
    "munic_mov": "Municipio de movimento",

    # Age
    "cod_idade": "Codigo unidade de idade (1=anos, 2=meses, 3=dias)",
    "idade": "Idade do paciente",
    "dias_perm": "Dias de permanencia",

    # Outcome
    "morte": "Indicador de obito (true/false)",

    # File
    "cod_arq": "Codigo do arquivo",
    "cont": "Continuacao",
    "nacional": "Nacionalidade",

    # Procedures
    "num_proc": "Numero de procedimentos",
    "car_int": "Carater da internacao (eletiva/urgencia)",
    "tot_pt_sp": "Total de pontos SP",

    # Extra identification
    "cpf_aut": "CPF do autorizador",
    "homonimo": "Homonimo",
    "num_filhos": "Numero de filhos",
    "instru": "Grau de instrucao",
    "cid_notif": "CID de notificacao",
    "contracep1": "Contraceptivo 1",
    "contracep2": "Contraceptivo 2",
    "gestrisco": "Gestacao de risco",

    # Federal/management costs
    "val_sh_fed": "Valor SH federal",
    "val_sp_fed": "Valor SP federal",
    "val_sh_ges": "Valor SH gestao",
    "val_sp_ges": "Valor SP gestao",
    "val_uci": "Valor UCI",

    # Daily rates
    "diar_acom": "Diarias de acompanhante",
    "qt_diarias": "Quantidade de diarias",

    # Medical classification
    "cbor": "CBO (Classificacao Brasileira de Ocupacoes)",
    "cnaer": "CNAE",
    "etnia": "Etnia",
    "raca_cor": "Raca/cor declarada",

    # Complementary ICDs
    "cid_asso": "CID associado",
    "cid_morte": "CID de morte",
    "diagsec1": "Diagnostico secundario 1",
    "diagsec2": "Diagnostico secundario 2",
    "diagsec3": "Diagnostico secundario 3",
    "diagsec4": "Diagnostico secundario 4",
    "diagsec5": "Diagnostico secundario 5",
    "diagsec6": "Diagnostico secundario 6",
    "diagsec7": "Diagnostico secundario 7",
    "diagsec8": "Diagnostico secundario 8",
    "diagsec9": "Diagnostico secundario 9",

    # Secondary diagnosis types
    "tpdisec1": "Tipo diagnostico secundario 1",
    "tpdisec2": "Tipo diagnostico secundario 2",
    "tpdisec3": "Tipo diagnostico secundario 3",
    "tpdisec4": "Tipo diagnostico secundario 4",
    "tpdisec5": "Tipo diagnostico secundario 5",
    "tpdisec6": "Tipo diagnostico secundario 6",
    "tpdisec7": "Tipo diagnostico secundario 7",
    "tpdisec8": "Tipo diagnostico secundario 8",
    "tpdisec9": "Tipo diagnostico secundario 9",

    # Extra columns
    "insc_pn": "Inscricao PN",
    "seq_aih5": "Sequencia AIH 5",
    "vincprev": "Vinculo previdenciario",
    "gestor_cod": "Codigo do gestor",
    "gestor_cpf": "CPF do gestor",
    "gestor_dt": "Data do gestor",
    "cnes": "Codigo CNES do estabelecimento de saude",
    "cgc_mant": "CNPJ da mantenedora",
    "complex": "Complexidade (alta/media/baixa)",
    "faec_tp": "Tipo FAEC",
    "financ": "Financiamento",
    "gestor_tp": "Tipo de gestor",
    "regct": "Registro CT",
    "remessa": "Remessa",
    "sequencia": "Sequencia",
    "aud_just": "Justificativa de auditoria",
    "nat_jur": "Natureza juridica",
    "sis_just": "Justificativa do sistema",
    "marca_uci": "Marcador UCI",
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
