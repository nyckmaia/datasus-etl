# datasus-etl

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Pipeline profissional para download, processamento e consulta de dados do **DATASUS** (Departamento de Informatica do SUS).

Suporta multiplos subsistemas:
- **SIHSUS** - Sistema de Informacoes Hospitalares
- **SIM** - Sistema de Informacoes sobre Mortalidade
- **SIASUS** - Sistema de Informacoes Ambulatoriais (em desenvolvimento)

## Caracteristicas

- **Download FTP automatico** do DATASUS
- **Conversao DBC para DBF** com Python puro (datasus-dbc)
- **Streaming DBF para DuckDB** (sem CSV intermediario)
- **Transformacoes SQL** otimizadas em DuckDB
- **Enriquecimento com dados IBGE** integrado (5571 municipios)
- **Armazenamento em Parquet** particionado por UF
- **Interface Web** (FastAPI + React) para usuarios nao-tecnicos
- **CLI completa** para automacao
- **Python API** para integracao em pipelines
- **Shell interativo DuckDB** para consultas SQL
- **Limpeza automatica** de arquivos temporarios (DBC/DBF)

## Instalacao

```bash
pip install datasus-etl
```

Ou para desenvolvimento:

```bash
git clone https://github.com/nyckmaia/datasus-etl.git
cd datasus-etl
pip install -e ".[dev]"
```

## Uso

O datasus-etl oferece 4 formas de uso:

### 1. CLI (Command Line Interface)

```bash
# Pipeline completo: download -> convert -> transform -> export
datasus pipeline --source sihsus --start-date 2023-01-01 --end-date 2023-12-31 --data-dir ./data/datasus --uf SP,RJ

# Atualizacao incremental (apenas arquivos novos)
datasus update --source sihsus --start-date 2023-01-01 --data-dir ./data/datasus

# Ver status do banco de dados
datasus status --source sihsus --data-dir ./data/datasus

# Shell interativo DuckDB para consultas SQL
datasus db --data-dir ./data/datasus

# Abrir interface web
datasus ui
datasus ui --port 8080
```

**Opcoes do comando `pipeline`:**

| Opcao | Descricao | Padrao |
|-------|-----------|--------|
| `--source`, `-s` | Subsistema (sihsus, sim, siasus) | - |
| `--start-date` | Data inicial (YYYY-MM-DD) | - |
| `--end-date` | Data final (YYYY-MM-DD) | hoje |
| `--uf` | Estados separados por virgula | todos |
| `--data-dir`, `-d` | Diretorio de dados | - |
| `--compression`, `-c` | Compressao Parquet | zstd |
| `--memory-aware`, `-m` | Modo otimizado para RAM | False |
| `--num-workers`, `-w` | Workers paralelos (1-8) | 4 |
| `--keep-temp-files` | Manter arquivos DBC/DBF | False |

### 2. Shell Interativo DuckDB

```bash
# Abre shell com VIEWs automaticas para cada subsistema
datasus db --data-dir ./data/datasus

# Filtrar por subsistema especifico
datasus db --data-dir ./data/datasus --source sihsus
```

**Comandos do shell:**

| Comando | Descricao |
|---------|-----------|
| `.tables` | Lista VIEWs disponiveis |
| `.schema <view>` | Mostra colunas da VIEW |
| `.count <view>` | Conta registros |
| `.sample <view> [n]` | Mostra N registros aleatorios |
| `.csv <arquivo>` | Exporta ultimo resultado para CSV |
| `.maxrows [n]` | Define max linhas exibidas |
| `.exit` | Sai do shell |

**Exemplo de sessao:**

```sql
datasus> SELECT COUNT(*) FROM sihsus;
datasus> SELECT uf, COUNT(*) as total FROM sihsus GROUP BY uf ORDER BY total DESC;
datasus> .csv resultado.csv
```

### 3. Python API

```python
from datasus_etl.config import PipelineConfig
from datasus_etl.pipeline.sihsus_pipeline import SihsusPipeline

# Criar configuracao usando factory method
config = PipelineConfig.create(
    base_dir="./data/datasus",
    subsystem="sihsus",
    start_date="2023-01-01",
    end_date="2023-12-31",
    uf_list=["SP", "RJ", "MG"],
    compression="zstd",
)

# Executar pipeline
pipeline = SihsusPipeline(config)
result = pipeline.run()

print(f"Linhas exportadas: {result.get_metadata('total_rows_exported'):,}")
```

**Consultar dados com SQL:**

```python
from datasus_etl.storage.parquet_query_engine import ParquetQueryEngine

# Conectar ao banco Parquet
engine = ParquetQueryEngine("./data/datasus/sihsus/parquet", view_name="sihsus")

# Executar query SQL
df = engine.sql("""
    SELECT
        uf,
        municipio_res,
        COUNT(*) as internacoes,
        SUM(val_tot) as valor_total
    FROM sihsus
    WHERE ano_cmpt = 2023
    GROUP BY uf, municipio_res
    ORDER BY internacoes DESC
    LIMIT 10
""")

print(df.to_pandas())

# Ver schema
print(engine.schema())

# Contar registros
print(f"Total: {engine.count():,} registros")

engine.close()
```

### 4. Interface Web (FastAPI + React)

```bash
datasus ui                                  # abre http://localhost:8787
datasus ui --data-dir /media/Dados/dados    # define o diretorio base
datasus ui --port 8080 --no-open            # porta customizada, sem abrir browser
```

A interface e um SPA em ingles, servido pelo proprio pacote Python (sem
dependencias externas para o usuario final). Paginas:

- **Dashboard**: estatisticas agregadas, mapa de cobertura por UF e serie
  temporal de volume de dados.
- **Download**: wizard em 4 passos (subsistema -> escopo -> estimativa ->
  execucao com progresso ao vivo via SSE).
- **Query**: editor SQL (Monaco), templates pre-definidos e dicionario de
  colunas. Exportacao para CSV ou Excel.
- **Settings**: diretorio de dados persistido em
  `~/.config/datasus-etl/config.toml`.

Desenvolvedores que quiserem trabalhar no frontend: veja `web-ui/README.md`
(requer [Bun](https://bun.sh)).

## Estrutura de Dados

Apos o processamento, os dados sao organizados em:

```
data/datasus/
├── sihsus/                    # Sistema de Informacoes Hospitalares
│   ├── dbc/                   # Arquivos originais (deletados apos processamento)
│   ├── dbf/                   # Arquivos convertidos (deletados apos processamento)
│   └── parquet/               # Dados finais
│       ├── uf=SP/
│       │   └── data_0.parquet
│       ├── uf=RJ/
│       │   └── data_0.parquet
│       └── uf=MG/
│           └── data_0.parquet
├── sim/                       # Sistema de Informacoes sobre Mortalidade
│   └── parquet/
│       └── ...
└── siasus/                    # Sistema de Informacoes Ambulatoriais
    └── parquet/
        └── ...
```

## Colunas Enriquecidas

O pipeline adiciona automaticamente informacoes geograficas do IBGE:

| Coluna | Descricao | Exemplo |
|--------|-----------|---------|
| `municipio_res` | Nome do municipio de residencia | Sao Paulo |
| `uf_res` | Nome do estado de residencia | Sao Paulo |
| `rg_imediata_res` | Regiao geografica imediata | Sao Paulo |
| `rg_intermediaria_res` | Regiao geografica intermediaria | Sao Paulo |

Alem das transformacoes existentes:

| Coluna | Transformacao |
|--------|---------------|
| `sexo` | Codigo para texto (M/F/I) |
| `raca_cor` | Codigo para texto (Branca, Preta, Parda, etc) |

## Subsistemas Suportados

| Subsistema | Descricao | Status |
|------------|-----------|--------|
| SIHSUS | Sistema de Informacoes Hospitalares | Completo |
| SIM | Sistema de Informacoes sobre Mortalidade | Completo |
| SIASUS | Sistema de Informacoes Ambulatoriais | Planejado |

## Performance

O pipeline e otimizado para processar grandes volumes:

- **Streaming DBF**: Processa arquivos maiores que a RAM
- **Memory-aware mode**: Processa 1 arquivo por vez com workers paralelos
- **Chunked processing**: Tamanho de chunk configuravel
- **Partition pruning**: DuckDB le apenas particoes necessarias
- **Parquet compressao**: zstd oferece melhor compressao

## Configuracao

### Modo Memory-Aware (Recomendado para Grandes Datasets)

```bash
# Processa todos os 27 estados sem estourar a RAM
datasus pipeline -s sihsus --start-date 2023-01-01 -d ./data/datasus --memory-aware -w 4
```

```python
from datasus_etl.config import PipelineConfig

config = PipelineConfig.create(
    base_dir="./data/datasus",
    subsystem="sihsus",
    start_date="2023-01-01",
    memory_aware_mode=True,
    num_workers=4,
)
```

### Ajustar para RAM Limitada

```python
from datasus_etl.config import PipelineConfig

config = PipelineConfig.create(
    base_dir="./data/datasus",
    subsystem="sihsus",
    start_date="2023-01-01",
    chunk_size=5000,  # Reduzir para menos RAM
)
```

### Manter Arquivos Temporarios

```bash
datasus pipeline --source sihsus --start-date 2023-01-01 -d ./data/datasus --keep-temp-files
```

Ou via Python:

```python
from datasus_etl.config import PipelineConfig

config = PipelineConfig.create(
    base_dir="./data/datasus",
    subsystem="sihsus",
    start_date="2023-01-01",
    keep_temp_files=True,
)
```

## Testes

```bash
# Executar todos os testes
pytest

# Com coverage
pytest --cov=datasus_etl --cov-report=html

# Testes especificos
pytest tests/unit/test_config.py
pytest tests/integration/
```

## Dependencias Principais

- **DuckDB**: Banco analitico SQL
- **Polars**: DataFrames de alta performance
- **PyArrow**: Formato Parquet
- **FastAPI + uvicorn**: API HTTP + servidor ASGI
- **React + Vite + shadcn/ui**: interface web (bundled com o pacote)
- **Typer**: CLI framework

## Licenca

Apache License 2.0

## Contribuindo

1. Fork o repositorio
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudancas (`git commit -m 'feat: adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request
