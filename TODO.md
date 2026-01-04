**1. Substituir o formato do banco de dados atual: Parquet para DuckDB**
- Atualmente, o banco de dados de saída consiste em um conjunto de arquivos *.parquet particionado pela coluna 'uf'.
- Quero substituir esse formato por um conjunto de arquivos de banco de dados Duckdb (*.duckdb) conforme a estrutura de pastas abaixo.
  - Cada arquivo *.duckdb será responsável por armazenar os dados de um único sub-sistema do DataSUS (SIHSUS, SIM, SIASUS, etc), o qual irá conter:
    - 1 tabela 'sub-sistema_raw'
      - Exemplo: Dentro do arquivo 'sihsus.duckdb': tabela 'sihsus_raw' contendo os dados processados pela pipeline, porém sem o enriquecimento (colunas adicionais) dos dados externos. Outros exemplos para outros arquivos *.duckdb são: 
      - sim.duckdb -> tabela 'sim_raw'
      - siasus.duckdb -> tabela 'siasus_raw'
    - Múltiplas tabela dimensões do sub-sistema específico: Cada sib-sistema do DataSUS contém arquivos *.csv próprios com tabelas dimensões, as quais tem as relações entre as colunas codificadas e o seu valor textual semântico. Vamos usar essas múltiplas tabelas para construir uma VIEW completa com os dados originais + as colunas enriquecedoras com os dados semâniticos decodificados realizando JOIN's específicos.
    - 1 única VIEW chamada 'sub-sistema'.
      - Exemplo para sihsus.duckdb: sihsus. Essa VIEW irá conter os dados da tabela 'sihsus_raw' + as colunas de enriquecimento provindas das fontes externas (DataSUS + IBGE).
- Planejar essa mudança de modo a ficar correspondente com essa estrutura de pastas abaixo. Veja que a pasta 'datasus-db' é a pasta de saída dos arquivos processados pela pipeline do 'datasus-etl' e ela fica posicionada fora do projeto, em qualquer lugar no disco que o usuário definir no parâmetro `--data-dir` do CLI:

datasus-db/
├─ siasus.duckdb
├─ sihsus.duckdb
├─ sim.duckdb
datasus-etl/
├─ datasus_etl/
│  ├─ _data/
│  │  ├─ datasus/
│  │  │  ├─ sihsus/
│  │  │  │  ├─ *.csv
│  │  │  │  ├─ IT_SIHSUS_1603.pdf
│  │  │  ├─ sim/
│  │  │  │  ├─ cid10-tables/
│  │  │  │  │  ├─ cadmun.csv
│  │  │  │  │  ├─ cid10.csv
│  │  │  │  │  ├─ cidcap10.csv
│  │  │  │  │  ├─ tabocup.csv
│  │  │  │  │  ├─ tabpais.csv
│  │  │  │  │  ├─ tabuf.csv
│  │  │  │  ├─ cid9-tables/
│  │  │  │  │  ├─ cid9.csv
│  │  │  │  │  ├─ cidbr.csv
│  │  │  │  │  ├─ cidbr2.csv
│  │  │  │  │  ├─ cidcap.csv
│  │  │  │  │  ├─ tabetnia.csv
│  │  │  │  │  ├─ tabmun.csv
│  │  │  │  │  ├─ tabocup.csv
│  │  │  │  │  ├─ tabpais.csv
│  │  │  │  │  ├─ tabuf.csv
│  │  │  │  ├─ Estrutura_do_SIM_2025.pdf
│  │  ├─ ibge/
│  │  │  ├─ DTB_2024/
│  │  │  │  ├─ *.xls
│  │  │  ├─ *.csv

- Perceba que dentro da pastas `datasus-etl/datasus_etl/_data/` terá as fontes de dados externos que pertencem ao projeto e devem ser instaladas junto com o pacote `datasus-etl` para funcionar.
  - A principio, temos apenas 2 fontes externas: IBGE e DataSUS
  - A pasta `datasus` terá sub-pastas, uma para cada `sub-sistema` do DataSUS (SIHSUS, SIM, SIASUS, etc), onde dentro dessa sub-pasta terá os arquivos *.csv específicos de cada sub-sistema. Esses arquivos serão usados para criar VIEWs dos dados enriquecidas com outras colunas provindas desses arquivos *.csv.
    - Exemplos dessas novas colunas enriquecidas podem ser as colunas que descrevem os dados codificados, como por exemplo: CID_MORTE (que está no dado original) pode ser enriquecido com CID_MORTE_DESC (que é a descrição semântica do código do CID, a qual está em um dos arquivos *.csv da pasta do sub-sistema específico)
- Crie uma nova branch para essa implementação
- Essa nova feature pode ser implementada de modo a não manter retro-compatibilidade com a implementação atual. Ou seja, essa nova implementação deve ser a única implementação no projeto todo.
- No futuro eu vou adicionar os arquivos *.csv específicos de cada sub-sistema nas devidas pastas conforme a estrtura de pastas desenhada acima.