# Disseminação de Informações do Sistema de Informações Hospitalares (SIH)

**Ministério da Saúde / Secretaria de Gestão Estratégica e Participativa / DATASUS**  
**DIDIS – Divisão de Disseminação de Informações em Saúde**

*Informe Técnico referente ao processamento 2016-03*

---

## 1. Alterações na estrutura e conteúdo de campos dos arquivos RD*.dbf

Não houve alterações na estrutura dos arquivos RD*.dbf

## 2. Layout dos arquivos RD*.dbf

**Tabela 1** – Layout dos arquivos RD*.dbf para janeiro de 2008 em diante.

| SEQ | NOME DO CAMPO | TIPO E TAM | Descrição/Observações |
| --- | --- | --- | --- |
| 1 | UF_ZI | char(6) | Município Gestor. |
| 2 | ANO_CMPT | char(4) | Ano de processamento da AIH, no formato aaaa. |
| 3 | MÊS_CMPT | char(2) | Mês de processamento da AIH, no formato mm. |
| 4 | ESPEC | char(2) | Especialidade do Leito |
| 5 | CGC_HOSP | char(14) | CNPJ do Estabelecimento. |
| 6 | N_AIH | char(13) | Número da AIH. |
| 7 | IDENT | char(1) | Identificação do tipo da AIH. |
| 8 | CEP | char(8) | CEP do paciente. |
| 9 | MUNIC_RES | char(6) | Município de Residência do Paciente |
| 10 | NASC | char(8) | Data de nascimento do paciente (aaaammdd). |
| 11 | SEXO | char(1) | Sexo do paciente. |
| 12 | UTI_MES_IN | numeric(2) | Zerado |
| 13 | UTI_MES_AN | numeric(2) | Zerado |
| 14 | UTI_MES_AL | numeric(2) | Zerado |
| 15 | UTI_MES_TO | numeric(3) | Quantidade de dias de UTI no mês. |
| 16 | MARCA_UTI | char(2) | Indica qual o tipo de UTI utilizada pelo paciente. |
| 17 | UTI_INT_IN | numeric(2) | Zerado |
| 18 | UTI_INT_AN | numeric(2) | Zerado |
| 19 | UTI_INT_AL | numeric(2) | Zerado |
| 20 | UTI_INT_TO | numeric(3) | Quantidade de diárias em unidade intermediaria. |
| 21 | DIAR_ACOM | numeric(3) | Quantidade de diárias de acompanhante. |
| 22 | QT_DIARIAS | numeric(3) | Quantidade de diárias. |
| 23 | PROC_SOLIC | char(10) | Procedimento solicitado. |
| 24 | PROC_REA | char(10) | Procedimento realizado. |
| 25 | VAL_SH | numeric(13,2) | Valor de serviços hospitalares. |
| 26 | VAL_SP | numeric(13,2) | Valor de serviços profissionais. |
| 27 | VAL_SADT | numeric(13,2) | Zerado |
| 28 | VAL_RN | numeric(13,2) | Zerado |
| 29 | VAL_ACOMP | numeric(13,2) | Zerado |
| 30 | VAL_ORTP | numeric(13,2) | Zerado |
| 31 | VAL_SANGUE | numeric(13,2) | Zerado |
| 32 | VAL_SADTSR | numeric(11,2) | Zerado |
| 33 | VAL_TRANSP | numeric(13,2) | Zerado |
| 34 | VAL_OBSANG | numeric(11,2) | Zerado |
| 35 | VAL_PED1AC | numeric(11,2) | Zerado |
| 36 | VAL_TOT | numeric(14,2) | Valor total da AIH. |
| 37 | VAL_UTI | numeric(8,2) | Valor de UTI. |
| 38 | US_TOT | numeric(10,2) | Valor total, em dólar. |
| 39 | DI_INTER | char(8) | Data de internação no formato aaammdd. |
| 40 | DT_SAIDA | char(8) | Data de saída, no formato aaaammdd. |
| 41 | DIAG_PRINC | char(4) | Código do diagnóstico principal (CID10). |
| 42 | DIAG_SECUN | char(4) | Código do diagnostico secundário (CID10). Preenchido com zeros a partir de 201501. |
| 43 | COBRANCA | char(2) | Motivo de Saída/Permanência |
| 44 | NATUREZA | char(2) | Natureza jurídica do hospital (com conteúdo até maio/12). Era utilizada a classificação de Regime e Natureza. |
| 45 | NAT_JUR | char(4) | Natureza jurídica do Estabelecimento, conforme a Comissão Nacional de Classificação - CONCLA |
| 46 | DESTAO | char(1) | Indica o tipo de gestão do hospital. |
| 47 | RUBRICA | numeric(5) | Zerado |
| 48 | IND_VDRL | char(1) | Indica exame VDRL. |
| 49 | MUNIC_MOV | char(6) | Município do Estabelecimento. |
| 50 | COD_IDADE | char(1) | Unidade de medida da idade. |
| 51 | IDADE | numeric(2) | Idade. |
| 52 | DIAS_PERM | numeric(5) | Dias de Permanência. |
| 53 | MORTE | numeric(1) | Indica Óbito |
| 54 | NACIONAL | char(2) | Código da nacionalidade do paciente. |
| 55 | NUM_PROC | char(4) | Zerado |
| 56 | CAR_INT | char(2) | Caráter da internação. |
| 57 | TOT_PT_SP | numeric(6) | Zerado |
| 58 | CPF_AUT | char(11) | Zerado |
| 59 | HOMONIMO | char(1) | Indicador se o paciente da AIH é homônimo do paciente de outra AIH. |
| 60 | NUM_FILHOS | numeric(2) | Número de filhos do paciente. |
| 61 | INSTRU | char(1) | Grau de instrução do paciente. |
| 62 | CID_NOTIF | char(4) | CID de Notificação. |
| 63 | CONTRACEP1 | char(2) | Tipo de contraceptivo utilizado. |
| 64 | CONTRACEP2 | char(2) | Segundo tipo de contraceptivo utilizado. |
| 65 | GESTRISCO | char(1) | Indicador se é gestante de risco. |
| 66 | INSC_PN | char(12) | Número da gestante no pré-natal. |
| 67 | SEQ_AIH5 | char(3) | Sequencial de longa permanência (AIH tipo 5). |
| 68 | CBOR | char(3) | Ocupação do paciente, segundo a Classificação Brasileira de Ocupações – CBO. |
| 69 | CNAER | char(3) | Código de acidente de trabalho. |
| 70 | VINCPREV | char(1) | Vínculo com a Previdência. |
| 71 | GESTOR_COD | char(3) | Motivo de autorização da AIH pelo Gestor. |
| 72 | GESTOR_TP | char(1) | Tipo de gestor. |
| 73 | GESTOR_CPF | char(11) | Número do CPF do Gestor. |
| 74 | GESTOR_DT | char(8) | Data da autorização dada pelo Gestor (aaaammdd). |
| 75 | CNES | char(7) | Código CNES do hospital. |
| 76 | CNPJ_MANT | char(14) | CNPJ da mantenedora. |
| 77 | INFEHOSP | char(1) | Status de infecção hospitalar. |
| 78 | CID_ASSO | char(4) | CID causa. |
| 79 | CID_MORTE | char(4) | CID da morte. |
| 80 | COMPLEX | char(2) | Complexidade. |
| 81 | FINANC | char(2) | Tipo de financiamento. |
| 82 | FAEC_TP | char(6) | Subtipo de financiamento FAEC. |
| 83 | REGCT | char(4) | Regra contratual. |
| 84 | RACA_COR | char(4) | Raça/Cor do paciente. |
| 85 | ETNIA | char(4) | Etnia do paciente, se raça cor for indígena. |
| 86 | SEQUENCIA | numeric(9) | Sequencial da AIH na remessa. |
| 87 | REMESSA | char(21) | Número da remessa. |
| 88 | AUD_JUST | char (50) | Justificativa do auditor para aceitação da AIH sem o número do Cartão Nacional de Saúde. |
| 89 | SIS_JUST | char (50) | Justificativa do estabelecimento para aceitação da AIH sem o número do Cartão Nacional de Saúde. |
| 90 | VAL_SH_FED | numeric (10, 2) | Valor do complemento federal de serviços hospitalares. Está incluído no valor total da AIH. |
| 91 | VAL_SP_FED | numeric (10, 2) | Valor do complemento federal de serviços profissionais. Está incluído no valor total da AIH. |
| 92 | VAL_SH_GES | numeric (10, 2) | Valor do complemento do gestor (estadual ou municipal) de serviços hospitalares. Está incluído no valor total da AIH. |
| 93 | VAL_SP_GES | numeric (10, 2) | Valor do complemento do gestor (estadual ou municipal) de serviços profissionais. Está incluído no valor total da AIH. |
| 94 | VAL_UCI | numeric (10, 2) | Valor de UCI. |
| 95 | MARCA_UCI | char (2) | Tipo de UCI utilizada pelo paciente. |
| 96 | DIAGSEC1 | char (4) | Diagnóstico secundário1. |
| 97 | DIAGSEC2 | char (4) | Diagnóstico secundário 2. |
| 98 | DIAGSEC3 | char (4) | Diagnóstico secundário 3. |
| 99 | DIAGSEC4 | char (4) | Diagnóstico secundário 4. |
| 100 | DIAGSEC5 | char (4) | Diagnóstico secundário 5. |
| 101 | DIAGSEC6 | char (4) | Diagnóstico secundário 6. |
| 102 | DIAGSEC7 | char (4) | Diagnóstico secundário 7. |
| 103 | DIAGSEC8 | char (4) | Diagnóstico secundário 8. |
| 104 | DIAGSEC9 | char (4) | Diagnóstico secundário 9. |
| 105 | TPDISEC1 | char(1) | Tipo de diagnóstico secundário 1. |
| 107 | TPDISEC2 | char(1) | Tipo de diagnóstico secundário 2. |
| 108 | TPDISEC3 | char(1) | Tipo de diagnóstico secundário 3. |
| 109 | TPDISEC4 | char(1) | Tipo de diagnóstico secundário 4. |
| 110 | TPDISEC5 | char(1) | Tipo de diagnóstico secundário 5. |
| 111 | TPDISEC6 | char(1) | Tipo de diagnóstico secundário 6. |
| 112 | TPDISEC7 | char(1) | Tipo de diagnóstico secundário 7. |
| 113 | TPDISEC8 | char(1) | Tipo de diagnóstico secundário 8. |
| 114 | TPDISEC9 | char(1) | Tipo de diagnóstico secundário 9. |

## 3. Layout dos arquivos SP*.dbf

**Tabela 2** – Layout dos arquivos SP*.dbf para janeiro de 2008 em diante.

| SEQ | NOME DO CAMPO | TIPO E TAM | Descrição/Observações |
| --- | --- | --- | --- |
| 1 | SP_GESTOR | char(6) | Unidade de Federação + Código Município de Gestão ou UF0000 se o Estabelecimento Executante está sob Gestão Estadual. |
| 2 | SP_UF | char (2) | Unidade da Federação. |
| 3 | SP_AA | char(4) | Ano da internação. |
| 4 | SP_MM | char(2) | Mês da internação. |
| 5 | SP_CNES | char(7) | Código do CNES do Estabelecimento de Saúde executante da AIH. |
| 6 | SP_NAIH | char(13) | Número da AIH. |
| 7 | SP_PROCREA | char (10) | Procedimento principal realizado na AIH. |
| 8 | SP_DTINTER | char (8) | Data da internação. |
| 9 | SP_DTSAIDA | char (8) | Data de saída. |
| 10 | SP_NUM_PR | char (8) | Zerado |
| 11 | SP_TIPO | char (2) | Zerado |
| 12 | SP_CPFCGC | char (14) | CNES, CPF ou CNPJ do prestador do serviço do ato profissional. |
| 13 | SP_ATOPROF | char (10) | Procedimento referente ao ato profissional. |
| 14 | SP_TP_ATO | char (2) | Zerado. |
| 15 | SP_QTD_ATO | numeric (4) | Quantidade de atos profissionais. |
| 16 | SP_PTSP | char (6) | Quantidade de pontos. |
| 17 | SP_NF | char (8) | Nota fiscal do material empregado quando órtese/prótese, quando não, o campo representa a data do ato. |
| 18 | SP_VALATO | numeric (14,2) | Valor do ato profissional. |
| 19 | SP_M_HOSP | char (6) | Município de localização do Estabelecimento Executante da AIH. |
| 20 | SP_M_PAC | char (6) | Município de residência do paciente. |
| 21 | SP_DES_HOS | char (1) | Indica se a UF de residência do paciente é diferente da UF de localização do estabelecimento. |
| 22 | SP_DES_PAC | char (1) | Indica se a UF de residência do paciente é diferente da UF de localização do estabelecimento. |
| 23 | SP_COMPLEX | char (2) | Complexidade do ato profissional. |
| 24 | SP_FINANC | char (2) | Tipo de financiamento do ato profissional. |
| 25 | SP_CO_FAEC | char (6) | Tipo de financiamento (04-FAEC) + Subtipo de financiamento relacionado ao tipo de financiamento (04-FAEC) do ato profissional. |
| 26 | SP_PF_CBO | char (6) | Código de Ocupação Brasileira do Profissional que realizou o ato ou “00000” caso não tenha sido. |
| 27 | SP_PF_DOC | char (15) | Documento de pessoa jurídica. |
| 28 | SP_PJ_DOC | char (14) | Documento de pessoa física. |
| 29 | IN_TP_VAL | char (1) | Tipo de valor: 1 - SP /2 –SH. |
| 30 | SEQUENCIA | char (9) | Código sequencial. |
| 31 | REMESSA | char (21) | Nome da remessa. |
| 32 | SERV_CLA | char (6) | Serviço/Classificação. |
| 33 | SP_CIDPRI | char (4) | CID Principal. |
| 34 | SP_CIDSEC | char (4) | CID Secundário. |
| 35 | SP_QT_PROC | numeric(4) | Quantidade de procedimentos realizados. |
| 36 | SP_U_AIH | char(1) | Indicador único da AIH. Contabiliza a AIH sem repetições. |

## 4. Formas de contato com o DATASUS

- **Por correspondência ou ofício:**  
  Ministério da Saúde  
  Secretaria de Gestão Estratégica e Participativa  
  Departamento de Informática do SUS  
  Coordenação Geral de Informações de Saúde (CGDIS)  
  Divisão de Disseminação de Informações em Saúde (DIDIS)  
  Rua México, 128, 8º andar CEP 20.031-142 - Castelo  
  Rio de Janeiro - RJ

- **Pela página do DATASUS**, através do link Fale conosco.

- **Pelo e-mail:** didis.atende@listas.datasus.gov.br