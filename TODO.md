**I1. renomear projeto para "datasus-etl"**
- Para aprimorar o nome do projeto, quero que ele mude de nome de `pydatasus` para `datasus-etl`
- Quero que a ferramenta CLI continue chamando apenas `datasus` por simplicidade
- Altere pyproject.toml e todos os lugares necessários, inclusive a Web Interface.

**I2. Streamlit: Não perguntar o email do usuário na primeira vez**
- Ao executar o `datasus ui` pela primeira vez, a biblioteca gráfica `streamlit` pergunta no Terminal para o usuário se ele quer digitar o seu email (opcional).
- A tela no terminal fica travada até o usuário tomar uma ação e não abre o browser.
- Ao apertar a tecla ENTER (com o campo do email vazio) o Terminal sai dessa opção e abre o browser com sucesso.
- Para evitar que o usuário execute a primeira vez e não veja o browser aberto, existe alguma forma do `streamlit` não perguntar o email do usuário na primeira vez e abrir o browser diretamente?

**I3. No CLI, aprimorar a opção de conversão de dados DBC**
- No CLI, aprimorar a opção `convert` de arquivos *.DBC de modo que o usuário possa escolher o formato de saída: *.dbf ou *.csv
- Além de passar um arquivo *.dbc específico, o usuário também poderá passar como parâmetro de entrada uma pasta contendo multiplos arquivos *.dbc, os quais também devem ser convertidos para o formato final escolhido pelo usuário: *.dbf ou *.csv

**I4. No CLI, calcula o tamanho do download e do banco em parquet e csv**
- No CLI, criar uma opção `download-estimate` o qual calcula o tamanho em disco a ser usado REALMENTE para download dos arquivos originais `*.dbc` que estão no servidor FTP
- Calcular também o tamanho em disco desses dados *.dbc convertidos para `parquet` e também para `*.csv` individualmente
- Avisar o usuário que (por padrão) os arquivos `*.dbc` são deletados durante o processo da pipeline de processamento, não ocupando disco no final da pipeline.

**I5. No CLI, renomear 2 opções e adicionar exemplo de execução**
- Renomear 2 opções no CLI: 
  - `run` para `pipeline`
  - `download` para `download-only`
- Ao executar o `datasus --help`, logo após a descrição da ferramenta, adicione o exemplo de execução da `pipeline` com os principais argumentos para fácil entendimento do usuário.

**I6. Web Interface: Aprimorar a tela de Status**
1. Na tela de "Status" da Web Interface, substituir o gráfico do plotly chamada "Registros por Estado (UF)" por uma tabela contendo as colunas:
  - uf: Contendo a sigla dos Estados (UF)
  - Registros: Quantidade de registros do Estado (UF)
  - Uso do Banco: Quantidade de dados do UF dado em porcentagem (%)
  - Data Inicial: Data onde inicia os registros daquele estado (UF)
  - Data Final: Data onde finaliza os registros daquele estado (UF)
2. Aprimore a tabela chamada "Estatísticas das Colunas Numéricas"
  - As colunas devem ser:
    - Coluna: Nome da coluna
    - Descrição: Descrição breve da coluna
    - Tipo de Dado: Tipo do dado SQL (VARCHAR, INTEGER, etc)
    - Mínimo: Valor mínimo
    - Máximo: Valor máximo
    - Porcentagem de Nulos: Porcentagem de registros nulos (NULL) com 1 casa decimal
  - Na coluna "Coluna" coloque todas as colunas so schema canônico do sub-sistema selecionado (sihsus, sim, etc)
  - Mova a tabela para fora do "breadcrumb" e expanda ela verticalmente para exibir todos as colunas sem utilizar a rolagem vertical.
3. Na tela "Status", remova a seção chamada "Arquivos Fonte" juntamente com a sua tabela contendo as colunas "Arquivo" e "linhas"

**I7. Web Interface: Aprimorar a tela de Consultar**
- Na tela "Consultar", no campo "Dicionario de Dados" é apresentado uma tabela "Descricao das colunas do subsistema SIHSUS:", contendo 2 colunas: "Coluna", "Descrição".
Nesta tabela, adicione as colunas:
- Tipo de Dado: (Os tipos do SQL como VARCHAR, INTEGER, BOOLEAN, etc)
- Porcentagem de Nulos (%): Porcentagem com 1 casa decimal dos valores NULL da coluna

**I8. Aprimorar consumo de RAM no processo de stream de dados**
- Atualmente, no processo de stream de dados do arquivo *.dbf para uma tabela do banco de dados duckdb, ocorre a criação de várias tabelas temporárias, uma para cada arquivo *.dbf.
- Essas tabelas ficam na memória RAM do duckdb até que o processo de stream finalize o e duckdb possa exportar todas as tabelas para arquivos *.parquet particionados pela coluna 'uf'.
- Esse acúmulo de multiplas tabelas temporárias em memória faz o uso crescente e acumulado de RAM crescer no tempo até o momento da exportação.
- Para evitar que haja estouro de memória RAM, para cada arquivo *.dbf que foi feito stream para o duckdb em uma tabela temporária, deve-se aplicar o conjunto das transformações da pipeline e depois já exportar os dados em memória para um arquivo *.parquet particionado pela coluna 'uf'.
- Desse modo, será possível liberar a memória RAM a cada arquivo *.dbf -> *.parquet processado, evitando o acúmulo do uso de memória RAM do PC do usuário.
- Esse processo faz sentido? Quais são as vantagens e desvantagens dele comparado com a implementação atual?
- Não implemente nada. Apenas me mostre um relatório da sua análise desse assunto.
