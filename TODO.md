**1. Adicionar read_only na opção db**
- No CLI, na opção db, adicionar a flag opcional: --read_only para previnir modificações indesejadas no banco de dados duckdb.
- Essa opção pode ser modificada pelo usuário na chamada do CLI de modo a habilitar a edição dos dados do banco.

**2. Aprimorar a Web Interface - Status**
- Na seção `Status`, no campo `Estatísticas das Colunas`, há uma tabela mostrando apenas "30 resultados de 117 colunas".
- Retire essa restrição vertical e exiba sempre todas as colunas dessa tabela.

**3. Adicionar opção export no CLI**
- No CLI, adicionar uma opção `export` para exportar os dados do banco de dados DuckDB para outros formatos de dados.
- Os formatos possíveis serão:
  - CSV (padrão)
  - Parquet
  - SQL
- Os dados exportados serão particionados (divididos em arquivos) de acordo com a coluna desejada pelo usuário. Para manter uma comtabilidade com os dados *.dbc originais, a coluna padrão para ser utilizada no processo de partição é a coluna 'source_file'.
  - Os arquivos de saída terão o mesmo nome do registro da coluna 'source_file' porém com a extensão solicitada pelo usuário (CSV, parquet ou SQL)
- Todoso os parametros são obrigatórios com exeção do: --format csv
- Exemplos:
  - datasus export --source sihsus --start-date 2000-01-01 --end-date 2002-02-03 --format csv --output-dir ./csv-data --partition-column source_file