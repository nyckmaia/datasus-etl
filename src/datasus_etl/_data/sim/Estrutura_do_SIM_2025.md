# Estrutura do SIM - Sistema de Informação sobre Mortalidade

**Coordenação-Geral de Informações e Análise Epidemiológicas - CGIAE**  
**Departamento de Análise em Saúde e Vigilância de Doenças Não Transmissíveis – DASNT**  
**Secretaria de Vigilância em Saúde – SVS**  
**Ministério da Saúde – MS**

*Arquivo atualizado em 07/2025*

## Campos do Arquivo

| Nome do campo | Nome do campo no DBF | Tipo | Tam | Valores válidos | Descrição | Características |
| --- | --- | --- | --- | --- | --- | --- |
| 1- Tipo do óbito | TIPOBITO | Caracter | 1 | 1-Fetal; 2-Não Fetal | Tipo do óbito Óbito fetal: morte antes da expulsão ou da extração completa do corpo da Mãe, independentemente da duração da gravidez. Indica o óbito o fato de o feto, depois da expulsão do corpo materno, não respirar nem apresentar nenhum outro sinal de vida, como batimentos do coração, pulsações do cordão umbilical ou movimentos efetivos dos músculos de contração voluntária | Campo obrigatório |
| 2- Data do Óbito | DTOBITO | Caracter | 8 | Data no padrão ddmmaaaa | Data em que occoreu o óbito. | Campo obrigatório |
| 2- Hora | HORAOBITO | Caracter | 5 | Números (padrão 24 horas 00:00) | Horário do óbito |  |
| 4- Naturalidade | NATURAL | Caracter | 3 | Números | Município e Unidade da Federação onde o falecido nasceu. Se estrangeiro informar País |  |
| 4 - Código do município de naturalidade | CODMUNNATU | Caracter | 7 | Números | Código do município de naturalidade do falecido. |  |
| 8- Data de Nascimento | DTNASC | Caracter | 8 | Data no padrão ddmmaaaa | Data do nascimento do falecido. Em caso de óbito fetal as datas de óbito e nascimento deverão ser iguais. |  |
| 9- Idade | IDADE | Caracter | 3 | Idade: composto de dois subcampos. - O primeiro, de 1 dígito, indica a unidade da idade (se 1 = minuto, se 2 = hora, se 3 = mês, se 4 = ano, se = 5 idade maior que 100 anos). - O segundo, de dois dígitos, indica a quantidade de unidades: Idade menor de 1 hora: subcampo varia de 01 e 59 (minutos); De 1 a 23 Horas: subcampo varia de 01 a 23 (horas); De 24 horas e 29 dias: subcampo varia de 01 a 29 (dias); De 1 a menos de 12 meses completos: subcampo varia de 01 a 11 (meses); Anos - subcampo varia de 00 a 99; - 9 - ignorado | Idade do falecido em minutos, horas, dias, meses ou anos | Campo obrigatório. Se 1 = 1 (óbito fetal) campo não deve ser preenchido. |
| 10- Sexo | SEXO | Caracter | 1 | M,1 – masculino; F,2 – feminino; I,0,9 - ignorado | Sexo do falecido. “Ignorado” selecionada em casos especiais como cadáveres mutilados, em estado avançado de decomposição, genitália indefinida ou hermafroditismo | Campo obrigatório |
| 11- Raça Cor | RACACOR | Caracter | 1 | 1 – Branca; 2 – Preta; 3 – Amarela; 4 – Parda; 5 – Indígena | Cor informada pelo responsável pelas informações do falecido |  |
| 12- Situação Conjugal | ESTCIV | Caracter | 1 | 1 – Solteiro; 2 – Casado; 3 – Viúvo; 4 – Separado judicialmente/divorciado; 5 – União estável; 9 – Ignorado | Situação conjugal do falecido informada pelos familiares |  |
| 13- Escolaridade (última série concluída) - nível | ESC2010 | Caracter | 1 | 0 – Sem escolaridade; 1 – Fundamental I (1ª a 4ª série); 2 – Fundamental II (5ª a 8ª série); 3 – Médio (antigo 2º Grau); 4 – Superior incompleto; 5 – Superior completo; 9 – Ignorado | Escolaridade 2010. Nível da última série concluída pelo falecido |  |
| 13- Escolaridade (última série concluída) - série | SERIESCFAL | Caracter |  | Números de 1 a 8 | Última série escolar concluída pelo falecido |  |
| 14- Ocupação habitual (Código CBO 2002) | OCUP | Caracter | 6 | Números | Tipo de trabalho que o falecido desenvolveu na maior parte de sua vida produtiva. Preenchimento de acordo com Classificação Brasileira de Ocupações – CBO 2002 | Campo preenchido se 9 > ou = 5 (idade a partir de 5 anos |
| 18 - Município de residência (Código) | CODMUNRES | Caracter | 7 | Números | Código do município de residência. Em caso de óbito fetal, considerar o município de residência da mãe | Campo obrigatório |
| 20- Local de Ocorrência do Óbito | LOCOCOR | Caracter | 1 | 1 – hospital; 2 – outros estabelecimentos de saúde; 3 – domicílio; 4 – via pública; 5 – outros; 6 - aldeia indígena; 9 – ignorado. | Local de ocorrência do óbito | Campo obrigatório. |
| 21- Estabelecimento (Código CNES) | CODESTAB | Caracter | 8 | Números | Código do estabelecimento de saúde constante do Cadastro Nacional de Estabelecimento de Saúde | Se 20 = 1 ou 2, campo 21 obrigatório |
| 25- Código do Município de ocorrência | CODMUNOCOR | Caracter | 8 | Números | Código relativo ao município onde ocorreu o óbito | Campo obrigatório |
| 27- Idade (anos) | IDADEMAE | Caracter | 2 | Números | Idade da mãe |  |
| 28- Escolaridade (última série concluída) - nível | ESCMAE2010 | Caracter | 1 | 0 – Sem escolaridade; 1 – Fundamental I (1ª a 4ª série); 2 – Fundamental II (5ª a 8ª série); 3 – Médio (antigo 2º Grau); 4 – Superior incompleto; 5 – Superior completo; 9 – Ignorado | Escolaridade 2010. Nível da última série concluída pela mãe |  |
| 28- Escolaridade (última série concluída) - série | SERIESCMAE | Caracter | 1 | Números de 1 a 8 | Última série escolar concluída pela mãe | Campo preenchido se 28 = 1, 2 ou 3 |
| 29 - Ocupação habitual (Código CBO 2002) | OCUPMAE | Caracter | 6 |  | Tipo de trabalho exercido habitualmente pela Mãe, de acordo com Classificação Brasileira de Ocupações – CBO 2002. No caso da mãe do falecido(a) ser “aposentada”, preencher com a ocupação habitual anterior. |  |
| 30- Número de filhos tidos (nascidos vivos) | QTDFILVIVO | Caracter | 2 | Número; 9 - igonorado | Número de filhos vivos |  |
| 30- Número de filhos tidos (perdas fetais/aborto) | QTDFILMORT | Caracter | 2 | Número; 9 - igonorado | Número de filhos mortos. Não incluir a criança cujo óbito se notifica na respectiva DO |  |
| 31- Nº de semanas de gestação | SEMAGESTAC | Caracter | 3 | Números com dois algarismos; 9 - igonorado | Semanas de gestação com dois algarismos |  |
| 32- Tipo de Gravidez | GRAVIDEZ | Caracter | 1 | 1 – única; 2 – dupla; 3 – tripla e mais; 9 – ignorada | Tipo de gravidez |  |
| 33- Tipo de Parto | PARTO | Caracter | 1 | 1 – vaginal; 2 – cesáreo; 9 – ignorado | Tipo de parto |  |
| 34- Morte em relação ao Parto | OBITOPARTO | Caracter | 1 | 1 - antes; 2– durante; 3–depois; 9– Ignorado | Momento do óbito em relação ao parto |  |
| 35- Peso ao Nascer | PESO | Caracter | 4 | Número (quatro algarismos) | Peso ao nascer em gramas |  |
| 37- A morte ocorreu | TPMORTEOCO | Caracter | 1 | 1 – na gravidez; 2 – no parto; 3 – no abortamento; 4 – até 42 dias após o término do parto; 5 – de 43 dias a 1 ano após o término da gestação ; 8 – não ocorreu nestes períodos; 9 – ignorado. | Situação gestacional ou pósgestacional em que ocorreu o óbito | Deve ser preenchido em caso de óbito de mulher fértil |
| 38- Recebeu assist. médica durante a doença que ocasionou a morte? | ASSISTMED | Caracter | 1 | 1 – sim; 2 – não; 9 – ignorado | Se refere ao atendimento médico continuado que o paciente recebeu, ou não, durante a enfermidade que ocasionou o óbito |  |
| 39- Necrópsia | NECROPSIA | Caracter | 1 | 1 – sim; 2 – não; 9 – ignorado | Refere-se a execução ou não de necropsia para confirmação do diagnóstico |  |
| 40- Causas da Morte- Parte I - CID | LINHAA | Caracter |  | *;letras; números; | CIDs informados na Linha A da DO referente ao diagnóstico na Linha A da DO (causa terminal - doença ou estado mórbido que causou diretamente a morte) | Campo obrigatório |
| 40- Causas da Morte- Parte I - CID | LINHAB | Caracter | 20 | *;letras; números; | CIDs informados na Linha B da DO referente ao diagnóstico na Linha B da DO (causa antecedente ou conseqüencial - estado mórbido, se existir, que produziu a causa direta da morte registrada na linha A) |  |
| 40- Causas da Morte- Parte I - CID | LINHAC | Caracter | 20 | *;letras; números; | CIDs informados na Linha C da DO referente ao diagnóstico na Linha C da DO (causa antecedente ou conseqüencial - estado mórbido, se existir, que produziu a causa |  |
|  |  |  |  |  | direta da morte registrada na linha A) |  |
| 40- Causas da Morte- Parte I - CID | LINHAD | Caracter | 20 | letras; números; * | CIDs informados na Linha D da DO referente ao diagnóstico na Linha D da DO (causa básica – estado mórbido, se existir, que produziu a causa direta da morte registrada na linha A) |  |
| 40- Causas da Morte- Parte II - CID | LINHAII |  | 45 |  | CIDs informados na Parte II da DO |  |
| Causa básica da Morte | CAUSABAS | Caracter | 4 | *;letras; números; | Causa básica da DO |  |
| 43- Óbito atestado por Médico | ATESTANTE | Caracter | 1 | 1 - Assistente; 2 – Substituto; 3 – IML; 4 – SVO; 5 – Outro | Condição do médico atestante |  |
| 44- Município e UF do SVO ou IML | COMUNSVOIM | Caracter | 7 | Números | Código do município do SVO ou do IML | Campo deve ser preenchido se 43 = 3 ou 4 (SVO ou IML) |
| 46 - Data do atestado | DTATESTADO | Caracter | 8 | Data no padrão ddmmaaaa | Data em que o atestado foi assinado |  |
| 48 - Tipo | CIRCOBITO | Caracter | 1 | 1 – acidente; 2 – suicídio; 3 – homicídio; 4 – outros; 9 – ignorado | Tipo de morte violenta ou circunstâncias em que se deu a morte não natural |  |
| 49 - Acidente do trabalho | ACIDTRAB | Caracter | 1 | 1 – sim; 2 – não; 9 – ignorado | Indica se o evento que desencadeou o óbito está relacionado ao processo de trabalho |  |
| 50 - Fonte da Informação | FONTE | Caracter | 1 | 1 – ocorrência policial; 2 – hospital; 3 – família; 4 – outra; 9 – ignorado | fonte de informação utilizada para o preenchimento dos campos 48 e 49 |  |
|  | ORIGEM | Caracter | 1 | 01- Oracle, 02 Banco estadual diponibilizado via FTP, 03 Banco SEADE 9 Ignorado |  |  |
|  | ESC | Caracter | 1 | 1 – Nenhuma; 2 – de 1 a 3 anos; 3 – de 4 a 7 anos; 4 – de 8 a 11 anos; 5 – 12 anos e mais; 9 – Ignorado. | Escolaridade em anos |  |
|  | ESCMAE | Caracter | 1 | 1 – Nenhuma; 2 – de 1 a 3 anos; 3 – de 4 a 7 anos; 4 – de 8 a 11 anos; 5 – 12 anos e mais; 9 – Ignorado | Escolaridade da mãe em anos |  |
|  | OBITOGRAV | Caracter | 1 | 1 – sim; 2 – não; 9 – ignorado | Óbito na gravidez |  |
|  | OBITOPUERP | Caracter | 1 | 1 – Sim, até 42 dias após o parto; 2 – Sim, de 43 dias a 1 ano; 3 – Não; 9 – Ignorado. | Óbito no puerpério |  |
|  | EXAME | Caracter | 1 | 1 – sim; 2 – não; 9 – ignorado | Realização de exame |  |
|  | CIRURGIA | Caracter | 1 | 1 – sim; 2 – não; 9 – ignorado | Realização de cirurgia |  |
|  | CAUSABAS_O | Caracter | 4 | *; letras;números | Causa básica informada antes da resseleção |  |
|  | NUMEROLOTE | Caracter | 8 | Números | Número do lote |  |
|  | DTINVESTIG | Caracter | 8 | Data no padrão ddmmaaaa | Data da investigação do óbito |  |
|  | DTCADASTRO | Caracter | 8 | Data no padrão ddmmaaaa | Data do cadastro do óbito |  |
|  | STCODIFICA | Caracter | 1 | Se codificadora (valor: S) ou não (valor: N) | Status de instalação |  |
|  | CODIFICADO | Caracter | 1 | Se estiver codificado (valor: S) ou não (valor: N) | Informa se formulario foi codificado |  |
|  | VERSAOSIST | Caracter | 7 | Números; . | Versão do sistema |  |
|  | VERSAOSCB | Caracter | 7 | Números; . | Versão do seletor de causa básica |  |
|  | FONTEINV | Caracter | 8 | 1 – Comitê de Morte Materna e/ou Infantil; 2 – Visita domiciliar / Entrevista família; 3 – Estabelecimento de Saúde / Prontuário; 4 – Relacionado com outros bancos de dados; 5 – S V O; 6 – I M L; 7 – Outra fonte; 8 – Múltiplas fontes; 9 – Ignorado | Fonte de investigação |  |
|  | DTRECEBIM | Caracter | 8 | Data no padrão ddmmaaaa | Data do recebimento |  |
|  | ATESTADO | Caracter | 70 | Letras; números; * ; / | CIDs informados no atestado |  |
|  | DTRECORIGA | Caracter |  | Data no padrão ddmmaaaa | Campo Criado no Tratamento para Data do recebimento original |  |
|  | OPOR_DO | Caracter | 4 | Números | Campo Criado no Tratamento para calcular Oportunidades |  |
|  | CAUSAMAT | Caracter | 4 | Letras; números; * ; / | CID da causa externa associada a uma causa materna |  |
|  | ESCMAEAGR1 | Caracter | 2 | 00 – Sem Escolaridade; 01 – Fundamental I Incompleto; 02 – Fundamental I Completo; 03 – Fundamental II Incompleto; 04 – Fundamental II Completo; 05 – Ensino Médio Incompleto; 06 – Ensino Médio Completo; 07 – Superior Incompleto; 08 – Superior Completo; 09 – Ignorado; 10 – Fundamental I Incompleto ou Inespecífico; 11 – Fundamental II Incompleto ou Inespecífico; 12 – Ensino Médio Incompleto ou Inespecífico | Escolaridade da mãe agregada (ormulário a partir de 2010) |  |
|  | ESCFALAGR1 | Caracter | 1 | 00 – Sem Escolaridade; 01 – Fundamental I Incompleto; 02 – Fundamental I Completo; 03 – Fundamental II Incompleto; 04 – Fundamental II Completo; 05 – Ensino Médio Incompleto; 06 – Ensino Médio Completo; 07 – Superior Incompleto; 08 – Superior Completo; 09 – Ignorado; 10 – Fundamental I Incompleto ou Inespecífico; 11 – Fundamental II Incompleto ou Inespecífico; 12 – Ensino Médio Incompleto ou Inespecífico | Escolaridade do falecido agregada (formulário a partir de 2010) |  |
|  | STDOEPIDEM | Caracter | 1 | 1 - Sim; 0 - Não | Status de DO Epidemiológica |  |
|  | STDONOVA | Caracter | 1 | 1 - Sim; 0 - Não | Status de DO Nova |  |
|  | DIFDATA | Caracter | 8 | Números | Diferença entre a data de óbito e data do recebimento original da DO ([DTOBITO] – [DTRECORIG]) |  |
|  | NUDIASOBCO | Caracter | 4 | Números | Diferença entre a data óbito e a data conclusão da investigação, em dias. |  |
|  | DTCADINV | Caracter | 8 | Data no padrão ddmmaaaa | Data do cadastro de investigação |  |
|  | TPOBITOCOR | Caracter | 1 | 1-Durante a gestação, 2- Durante o abortamento, 3- Após o abortamento , 4- No parto ou até 1 hora após o parto, 5- No puerpério - até 42 dias após o parto, 6- Entre 43 dias e até 1 ano após o parto, 7- A investigação não identificou o momento do óbito, 8- Mais de um ano após o parto , 9- O óbito não ocorreu nas circunstancias anteriores, Branco - Não investigado |  |  |
|  | DTCONINV | Caracter | 8 | Data no padrão ddmmaaaa | Data da conclusão da investigação |  |
|  | FONTES | Caracter | 6 | Letras | Combinado de caracteres conforme o preenchimento dos campos de fontes (FONTENTREV, FONTEAMBUL, FONTEPRONT, FONTESVO, FONTEIML, FONTEPROF): se preenchido caractere “S”, se o campo estiver vazio caractere “X” |  |
|  | TPRESGINFO | Caracter | 2 | 01 - Não acrescentou nem corrigiu informação; 02 - Sim, permitiu o resgate de novas informações; 03 - Sim, permitiu a correção de alguma das causas informadas originalmente. | Informa se a investigação permitiu o resgate de alguma causa de óbito não informado, ou a correção de alguma antes informada |  |
|  | TPNIVELINV | Caracter | 1 | E – estadual; R- regional; M- Municipal | Tipo de nível investigador |  |
|  | DTCADINF | Caracter | 8 | Data no padrão ddmmaaaa | Quando preenchido indica se a investigação foi realizada |  |
|  | MORTEPARTO | Caracter | 1 | 1 - antes; 2– durante; 3–após; 9– Ignorado | Momento do óbito em relação ao parto após investigação |  |
|  | DTCONCASO | Caracter | 8 | Data no padrão ddmmaaaa | Data de conclusão do caso |  |
|  | ALTCAUSA | Caracter | 1 | 1- Sim; 2 – Não | Indica se houve correção ou alteração da causa do óbito após investigação |  |
|  | CAUSABAS_O | Caracter | 4 | Letras; números; * | Causa básica Original |  |
|  | TPPOS | Caracter | 1 | 1 – sim; 2 – não | Óbito investigado |  |
|  | TP_ALTERA | Caracter |  |  | 02 = CausaBas em branco 03 = CausaBas com ausência do 4 caractere 04 = Causas Asterisco 05 = CID não pode ser CausaBas 06 = CausaBas inválida para o Sexo Feminino 07 = CausaBas inválida para o Sexo Masculino 08 = CID Implausíveis 09 = Causas Erradicadas ou Causa U 10 = Causas Triviais 11 = Causas Improváveis 12 = Óbito Não Fetal com causa Fetal 13 = Óbito Fetal com causa Não Fetal 14 = Óbito Materno duvidoso 15 = Óbito possível de ser materno 16 = Óbito com restrição de idade (TP_MSG_5) 17 = Óbito com restrição de idade (TP_MSG_6) |  |
| Semanas de gestação (formulário antigo) | GESTACAO | Caracter | 1 | 1 - Menos de 22 semanas; 2 - 22 a 27 semanas; 3 - 28 a 31 semanas; 4 - 32 a 36 semanas; 5 - 37 a 41 semanas; 6 - 42 e + semanas | Faixas de semanas de gestação |  |
|  | CB_ALT | Caracter |  |  | Variável de sistema |  |