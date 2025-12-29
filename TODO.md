**Melhoria 01: Para pesquisar - Melhoria de Performace do pipeline**
- O duckdb não aceita mais de 1 thread /acesso ao mesmo tempo, certo?
- No meu processo de stream dos dados do formato DBF para uma tabela temporária no Duckdb, atualemente isso está sendo feito em single-thread senquencialmente.
- Existiria alguma forma de paralelizar esse processo para diminuir o tempo de processamento? Quais são as melhores opções para ganhar tempo neste caso?
- Não implemente nada, apenas me mostre as opções e as vantagens e desvantagens de cada uma delas.

**Melhoria 02: Aprimorar o processo de deleção dos arquivos temporários DBC e DBF**
- Atualmente, a pipeline segue esse encadeamento:
  1. Faz o download de todos os arquivos originais DBC
  2. Converte cada arquivo DBC para DBF
  3. Mantem ambos DBC e DBF armazenados em disco durante o stream dos dados DBF para a tabela temporária do duckdb
  4. Exporta os dados do duckdb para o formato de saida (parquet)
  5. Após concluir a exportação, deleta os arquivos temporários DBC e DBF. Por fim deleta as pastas dbc/ e dbf/ onde esses arquivos temporários estavam sendo armazenados.
- Analise a possibilidade de evitar arquivos temporários em disco por mais tempo do que o necessário. Exemplo a ser analisado por você:
  1. Faz o download de todos os arquivos originais DBC e os armazena na pasta `dbc` atual
  2. Itera em cada arquivo DBC de modo a:
    - 2.1 Converte o arquivo DBC para DBF
    - Faz o stream do arquivo DBF para a tabela temporária no duckdb
    - Exporta os dados da tabela temporária para o formato de saída (parquet)
    - Após concluir a exportação deleta o arquivo DBF especifico
  3. Ao final da iteração de todos os arquivos DBC, deleta-se a pasta dbc/ e a pasta dbf/ completamente pois todos os dados já estarão exportados para o formato final (parquet)
- Isso é viável? quais as vantagens e desvantagens de se aplicar esse método ao invés do que já está implementado atualmente?

**Melhoria 03: No CLI do datasus adicionar o parametro opcional output-format**
- O valor padrão do novo parâmetro `output-format` será `parquet`
- Os valores possíveis serão: `parquet` ou `csv`
- Se o usuário passar o parametro `--output-format csv` então haverá uma dupla consequência:
  1. O stream de dados do DBF vai para o duckdb para ser tratado e transformado corretamente, mas não vai ser exportado para o formato `parquet` e sim para o formato `csv` particionado (hive) pela coluna `uf`, usando o encoding do arquivo de saída `utf-8` e delimitador `,`.
  2. No CLI, no processo de `update` do banco de dados, como não haverá arquivos `parquet` para consultar, será necessário ver que os arquivos do banco atual estão em formato `csv` e automaticamente fazer modo em que, ao invés de criar uma VIEW temporaria no duckdb a partir dos arquivos parquet (read_parquet), será feito uma VIEW temporária a partir do conjunto dos arquivos CSV com o comando `read_csv` do Duckdb.
    - Para a leitura correta dos CSVs o Duckdb tem parametros do comando `read_csv` importantes no link: https://duckdb.org/docs/stable/data/csv/overview
    - Exemplo: `header=true`, `parallel=true`, `union_by_name=true`, `hive_partitioning=true`

**Melhoria 04: Renomear os arquivos de saida de acordo com o nome do arquivo dbc original**
- No final da pipeline, os arquivos de saída podem estar no formato `*.parquet` ou em `*.csv`
- Como cada arquivo `*.dbc` den entrada original está gerando 1 arquivo de saída (parquet ou csv), seria bom que o nome do arquivo de saída fosse o mesmo do arquivo DBC de entrada.
- O duckdb gera nomes hexadeciamais automaticamente no exportação, por isso seria necessário adicionar mais uma etapa para a renomeação dos arquivos de saída (parquet ou csv) dentro da partição pela coluna 'uf'. 

**Melhoria 05: No CLI adicionar uma opção para não aplicar nenhuma transformação nos dados originais**
- adicionar a opção com um nome parecido com: `--not-transform-any-data`. Escolher um nome apropriado.
- Todas as colunas serão lidas e exportadas como strings (tipo VARCHAR)
- As únicas 2 transformações aplicadas nesse opção são:
  - Remover os caracteres inválidos e nulos do cabeçalho e dos registros. Exemplos: \t, \0, \n, \r, etc.
  - Colocar os dados dentro do schema canônico (colunas pré-definidas)
- Os dados de saída podem ser no formato `*.parquet` ou `*.csv` de acordo com a opção escolhida pelo usuário

**Melhoria 06: Aprimorar as tags do Pypi do projeto**
- Aprimorar as tags do pypi para que pesquisadores consigam localizar com facidadade este pacote Python na internet

**Melhoria 07: Individualizar as transformações dos dados**
- Após o stream dos dados do arquivo *.dbf para o banco de dados Duckdb ocorre um processo de transformação e enriquecimentos dos dados tabulares.
- No momento atual, todas as transformações e enriquecimentos estão sendo realizados juntos dentro da mesma função.
- Quero que cada transformação ou enriquecimento seja modular e individualizado, para que ele possa ser reaproveitado em outros pipelines de outros sub sistemas do DataSUS (como o sub sistema SIM, SIASUS, etc).
- Com essa modularização, será possivel adicionar, remover e alterar a ordem das transformações e enriquecimentos em cada pipeline específico
- Ao ser executado, o pipeline deve mostrar no terminal um print com o nome da transformação ou enriquecimento que está sendo executado naquele momento. 
- No Exemplo abaixo mostro 4 sub etapas (3 de transformação + 1 de enriquecimento de dados):
[INFO][Data Transform][1/3] Removing invalid and null characters
[INFO][Data Transform][2/3] Decoding 'sexo' column
[INFO][Data Transform][3/3] Decoding 'raca_cor' column
[INFO][Data Enrichment][1/1] Adding 4 IBGE columns via 'munic_res' column

**Melhoria 08: Aprimorar os scripts de exemplos**
- Na pasta `examples` estou sempre usando o `basic_usage.py` como referência. Eu gosto dele. Mantenha ele assim.
- Analise os demais scripts Python: veja se eles estão de acordo com a nova arquitetura, com os novos parâmetros das classes, etc.
- Quero poucos exemplos nessa pasta. Analise e refaça eles de modo que sejam bem claros e objetivos em relação ao projeto.

**Melhoria 09: Pesquisar online a possibilidade de renomear esse projeto de pydatasus para apenas datasus**
- Verificar diponibilidade do nome `datasus` no Pypi
- Verificar se há algum outro impedimento para que este pacote possa se chamar `datasus` oficialmente no Python
- Quais seriam outras sugestões de nomes compatíveis com esse projeto? datasus-pipeline? Outros?

**Melhoria 10: Aprimorar a Web Interface**
- Sugerir possíveis aprimoramentos para a Web Interface
- Os principais usuários são pesquisadores de graduação, pós-graduação e pesquisadores profissionais da área da saúde
- Além disso, algumas vezes, ao fazer uma query pesada, a UI gera uma mensagem falando para não transportar tantos dados do backend para a Web Interface. Existe alguma maneira de diminuir esse impacto e possibilitar a execução de queries com muitos resultados na Web Interface?
- Não implemente nada. Me mostre suas sugestões, vantagens e desvantagens.