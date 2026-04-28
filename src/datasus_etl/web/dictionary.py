"""Data dictionary for DataSUS columns.

Provides human-readable descriptions of columns in each subsystem,
helping health researchers understand the data structure.
"""

# 4 IBGE columns appended to the enriched `{subsystem}` VIEW (the one served
# by /api/query/sql and `datasus db`). Any subsystem that declares
# `RESIDENCE_MUNICIPALITY_COLUMN` on its DatasetConfig gets these columns at
# query time; the dictionary endpoint merges them on top of the per-subsystem
# *_COLUMNS dict so the "Colunas" UI tab also shows them.
IBGE_ENRICHED_COLUMNS = {
    "uf_res": "UF de residencia (enriquecido via IBGE)",
    "municipio_res": "Nome do municipio de residencia (enriquecido via IBGE)",
    "rg_imediata_res": "Regiao geografica imediata (enriquecido via IBGE)",
    "rg_intermediaria_res": "Regiao geografica intermediaria (enriquecido via IBGE)",
}

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
    # uf_res / municipio_res / rg_imediata_res / rg_intermediaria_res are
    # appended dynamically via IBGE_ENRICHED_COLUMNS — see dictionary endpoint.

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
# Descriptions are derived from DATASUS SIM official documentation and the
# inline annotations in datasets/sim/schema.py. They cover every column in
# SIM_DUCKDB_SCHEMA so the UI's 'Colunas' panel never shows blank entries.
# Note: DATASUS does not publish bairro/endereco fields for SIM (privacy).
SIM_COLUMNS = {
    # ── Identification ──────────────────────────────────────────────
    "uf": "UF de ocorrencia (sigla extraida do nome do arquivo)",
    "source_file": "Arquivo DBC de origem",
    "origem": "Origem do registro",

    # ── Death certificate (DO) ───────────────────────────────────────
    "tipobito": "Tipo de obito (1=fetal, 2=nao-fetal)",
    "dtobito": "Data do obito",
    "horaobito": "Hora do obito (HH:MM)",
    "natural": "Naturalidade (local de nascimento)",
    "codmunnatu": "Codigo IBGE do municipio de naturalidade",
    "dtnasc": "Data de nascimento do falecido",

    # ── Demographics ────────────────────────────────────────────────
    "idade": "Idade codificada (formato original DATASUS)",
    "idade_valor": "Idade decodificada - valor numerico",
    "idade_unidade": "Unidade da idade (minutos/horas/meses/anos/ignorado)",
    "sexo": "Sexo (1=M, 2=F, 0/9=desconhecido)",
    "racacor": "Raca/cor (1-5, 9=ignorado)",
    "estciv": "Estado civil",
    "esc": "Escolaridade (codificacao antiga)",
    "esc2010": "Escolaridade (codificacao 2010)",
    "seriescfal": "Serie escolar do falecido",
    "ocup": "Ocupacao do falecido (CBO)",

    # ── Residence / occurrence ──────────────────────────────────────
    "codmunres": "Codigo IBGE do municipio de residencia",
    "lococor": "Local de ocorrencia (1=hospital, 2=outro, etc)",
    "codestab": "Codigo CNES do estabelecimento de saude",
    "estabdescr": "Descricao do estabelecimento (campo nao oficial, presente em alguns DBC)",
    "codmunocor": "Codigo IBGE do municipio de ocorrencia",

    # ── Mother (fetal/infant deaths) ────────────────────────────────
    "idademae": "Idade da mae (obitos fetais/infantis)",
    "escmae": "Escolaridade da mae",
    "escmae2010": "Escolaridade da mae (codificacao 2010)",
    "seriescmae": "Serie escolar da mae",
    "ocupmae": "Ocupacao da mae (CBO)",
    "qtdfilvivo": "Quantidade de filhos vivos",
    "qtdfilmort": "Quantidade de filhos mortos",
    "gravidez": "Tipo de gravidez (unica/dupla/tripla)",

    # ── Cause of death (CID-10) ─────────────────────────────────────
    "causabas": "Causa basica do obito (CID-10) - array",
    "linhaa": "Linha A - causa imediata (CID-10) - array",
    "linhab": "Linha B - causa intermediaria (CID-10) - array",
    "linhac": "Linha C - causa intermediaria (CID-10) - array",
    "linhad": "Linha D - causa antecedente (CID-10) - array",
    "linhaii": "Parte II - condicoes contribuintes (CID-10) - array",

    # ── Medical certification ───────────────────────────────────────
    "circobito": "Circunstancia do obito (acidente, suicidio, etc)",
    "acidtrab": "Acidente de trabalho (1=sim, 2=nao, 9=ignorado)",
    "fonte": "Fonte de informacao",
    "tppos": "Tipo de posicao/certificacao (1=sim, 2=nao)",
    "dtinvestig": "Data da investigacao",
    "causabas_o": "Causa basica original (antes da codificacao)",
    "dtcadastro": "Data de cadastro no sistema",
    "atestado": "Atestado medico (array de itens)",
    "atestante": "Condicao do medico atestante",
    "fonteinv": "Fonte da investigacao",
    "dtrecebim": "Data de recebimento",

    # ── Pregnancy / childbirth ──────────────────────────────────────
    "tpmorteoco": "Tipo de morte ocorrida (em relacao a gestacao)",
    "semagestac": "Semanas de gestacao",
    "gestacao": "Duracao da gestacao",
    "parto": "Tipo de parto",
    "obitoparto": "Obito em relacao ao parto (antes/durante/depois)",
    "peso": "Peso ao nascer em gramas (obitos fetais/infantis)",
    "obitograv": "Obito durante a gravidez",
    "obitopuerp": "Obito no puerperio",
    "assistmed": "Houve assistencia medica",
    "exame": "Houve exame medico",
    "cirurgia": "Houve cirurgia",
    "necropsia": "Foi realizada necropsia",

    # ── External causes / dates ─────────────────────────────────────
    "dtatestado": "Data do atestado",

    # ── Lot / system metadata ───────────────────────────────────────
    "numerolote": "Numero do lote",
    "versaosist": "Versao do sistema",
    "versaoscb": "Versao do SCB",
    "contador": "Contador",
    "difdata": "Diferenca de datas",
    "nudiasobco": "Dias entre obito e comunicacao",
    "nudiasobin": "Dias entre obito e investigacao",
    "dtcadinf": "Data de cadastro de informacoes",
    "morteparto": "Morte em relacao ao parto",
    "dtrecoriga": "Data original de recebimento",
    "causamat": "Causa materna (array de CIDs)",
    "escmaeagr1": "Escolaridade da mae agregada",
    "escfalagr1": "Escolaridade do falecido agregada",
    "dtcadinv": "Data de cadastro da investigacao",
    "tpobitocor": "Tipo de obito ocorrido",
    "dtconinv": "Data de conclusao da investigacao",
    "fontes": "Fontes de informacao",
    "tpresginfo": "Tipo de recuperacao da informacao",
    "tpnivelinv": "Tipo/nivel da investigacao",
    "nudiasinf": "Dias de informacao",
    "dtconcaso": "Data de conclusao do caso",
    "fontesinf": "Fontes adicionais de informacao (campo nao oficial)",
    "stcodifica": "Status de codificacao (S=sim, N=nao)",
    "codificado": "Codificado (S=sim, N=nao)",
    "cb_pre": "Causa basica pre-codificacao",
    "altcausa": "Causa alterada (1=sim, 2=nao)",
    "comunsvoim": "Codigo do municipio do SVO/IML",

    # ── Optional fields ─────────────────────────────────────────────
    "stdoepidem": "DO de notificacao epidemiologica (1=sim, 0=nao)",
    "stdonova": "Nova versao da DO (1=sim, 0=nao)",
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
