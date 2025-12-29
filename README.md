# PyDataSUS

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Pipeline profissional para download, processamento e consulta de dados do **DATASUS** (Sistema de Informacoes Hospitalares do SUS).

## Caracteristicas

- **Download FTP automatico** do DATASUS
- **Conversao DBC para DBF** com Python puro (datasus-dbc)
- **Streaming DBF para DuckDB** (sem CSV intermediario)
- **Transformacoes SQL** otimizadas em DuckDB
- **Enriquecimento com dados IBGE** integrado (5571 municipios)
- **Armazenamento em Parquet** particionado por UF
- **Interface Web** (Streamlit) para usuarios nao-tecnicos
- **CLI completa** para automacao
- **Python API** para integracao em pipelines
- **Limpeza automatica** de arquivos temporarios (DBC/DBF)

## Instalacao

```bash
pip install pydatasus
```

Ou para desenvolvimento:

```bash
git clone https://github.com/user/pydatasus.git
cd pydatasus
pip install -e ".[dev]"
```

## Uso

O PyDataSUS oferece 3 formas de uso:

### 1. CLI (Command Line Interface)

```bash
# Pipeline completo: download -> convert -> transform -> export
datasus run --source sihsus --start-date 2023-01-01 --end-date 2023-12-31 --uf SP,RJ

# Atualizacao incremental (apenas arquivos novos)
datasus update --source sihsus --data-dir ./data/datasus

# Ver status do banco de dados
datasus status --source sihsus --data-dir ./data/datasus

# Abrir interface web
datasus ui
datasus ui --port 8080
```

**Opcoes do comando `run`:**

| Opcao | Descricao | Padrao |
|-------|-----------|--------|
| `--source`, `-s` | Subsistema (sihsus, sim, siasus) | sihsus |
| `--start-date` | Data inicial (YYYY-MM-DD) | 2023-01-01 |
| `--end-date` | Data final (YYYY-MM-DD) | hoje |
| `--uf` | Estados separados por virgula | todos |
| `--data-dir`, `-d` | Diretorio de dados | ./data/datasus |
| `--compression`, `-c` | Compressao Parquet | zstd |
| `--keep-temp-files` | Manter arquivos DBC/DBF | False |

### 2. Python API

```python
from pydatasus.config import PipelineConfig
from pydatasus.pipeline.sihsus_pipeline import SihsusPipeline

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
from pydatasus.storage.parquet_query_engine import ParquetQueryEngine

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

### 3. Interface Web (Streamlit)

```bash
datasus ui
```

Abre http://localhost:8501 com:

- **Status**: Ver estatisticas do banco de dados
- **Download**: Baixar e processar novos dados
- **Consultar**: Executar queries SQL interativas
- **Exportar**: Exportar dados para CSV/Excel

## Estrutura de Dados

Apos o processamento, os dados sao organizados em:

```
data/datasus/
└── sihsus/                    # Subsistema
    ├── dbc/                   # Arquivos originais (deletados apos processamento)
    ├── dbf/                   # Arquivos convertidos (deletados apos processamento)
    └── parquet/               # Dados finais
        ├── uf=SP/
        │   └── data_0.parquet
        ├── uf=RJ/
        │   └── data_0.parquet
        └── uf=MG/
            └── data_0.parquet
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
| SIM | Sistema de Informacoes sobre Mortalidade | Em desenvolvimento |
| SIASUS | Sistema de Informacoes Ambulatoriais | Planejado |

## Performance

O pipeline e otimizado para processar grandes volumes:

- **Streaming DBF**: Processa arquivos maiores que a RAM
- **Chunked processing**: Configurable chunk size
- **Partition pruning**: DuckDB le apenas particoes necessarias
- **Parquet compressao**: zstd oferece melhor compressao

## Configuracao

### Ajustar para RAM Limitada

```python
config = PipelineConfig.create(
    base_dir="./data/datasus",
    subsystem="sihsus",
    chunk_size=5000,  # Reduzir para menos RAM
)
```

### Manter Arquivos Temporarios

```bash
datasus run --source sihsus --keep-temp-files
```

Ou via Python:

```python
config = PipelineConfig.create(
    base_dir="./data/datasus",
    subsystem="sihsus",
    keep_temp_files=True,
)
```

## Testes

```bash
# Executar todos os testes
pytest

# Com coverage
pytest --cov=pydatasus --cov-report=html

# Testes especificos
pytest tests/unit/test_config.py
pytest tests/integration/
```

## Dependencias Principais

- **DuckDB**: Banco analitico SQL
- **Polars**: DataFrames de alta performance
- **PyArrow**: Formato Parquet
- **Streamlit**: Interface web
- **Typer**: CLI framework

## Licenca

Apache License 2.0

## Contribuindo

1. Fork o repositorio
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudancas (`git commit -m 'feat: adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request
