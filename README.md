# PyDataSUS 🏥

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Pipeline profissional para download, conversão e processamento de dados do **DATASUS** (Sistema de Informações Hospitalares do SUS).

## ✨ Características

- 🔽 **Download FTP automático** do DATASUS
- 🔄 **Conversão DBC→DBF→CSV** com TABWIN
- 🧹 **Processamento e limpeza** de dados hospitalares
- 📊 **Enriquecimento com dados IBGE**
- 💾 **Armazenamento em Parquet** particionado
- 🗄️ **DuckDB** para consultas SQL
- ⚡ **Processamento paralelo** otimizado
- 🧪 **Testes abrangentes** (50+ testes unitários)

## 📦 Instalação

```bash
git clone https://github.com/user/pydatasus.git
cd pydatasus
pip install -e ".[dev]"
```

**Requisito**: TABWIN instalado para conversão DBC→DBF

## 🚀 Uso Rápido

### Pipeline Completo

```bash
# Executar pipeline completo (download → processamento → DuckDB)
pydatasus pipeline --start-date 2020-01-01 --end-date 2020-12-31 --ufs SP,RJ
```

### Comandos Individuais

```bash
# Download do FTP DATASUS
pydatasus download --start-date 2020-01-01 --end-date 2020-12-31 --ufs SP,RJ

# Converter DBC para DBF
pydatasus convert-dbc ./data/dbc ./data/dbf

# Converter DBF para CSV
pydatasus convert-dbf ./data/dbf ./data/csv

# Versão
pydatasus version
```

### Python API

#### Pipeline Completo

```python
from pydatasus import SihsusPipeline
from pydatasus.config import (
    PipelineConfig,
    DownloadConfig,
    ConversionConfig,
    ProcessingConfig,
    StorageConfig,
    DatabaseConfig,
)
from pathlib import Path

# Configurar pipeline
config = PipelineConfig(
    download=DownloadConfig(
        output_dir=Path("./data/dbc"),
        start_date="2020-01-01",
        end_date="2020-12-31",
        uf_list=["SP"],
    ),
    conversion=ConversionConfig(
        dbc_dir=Path("./data/dbc"),
        dbf_dir=Path("./data/dbf"),
        csv_dir=Path("./data/csv"),
    ),
    processing=ProcessingConfig(
        input_dir=Path("./data/csv"),
        output_dir=Path("./data/processed"),
    ),
    storage=StorageConfig(
        parquet_dir=Path("./data/parquet"),
    ),
    database=DatabaseConfig(
        db_path=Path("./data/sihsus.duckdb"),
    ),
)

# Executar pipeline
pipeline = SihsusPipeline(config)
result = pipeline.run()

print(f"Pipeline concluído! {result.get_metadata('tables_loaded', 0)} tabelas no DuckDB")
```

#### Componentes Individuais

```python
# Download
from pydatasus.download import FTPDownloader
from pydatasus.config import DownloadConfig

config = DownloadConfig(
    output_dir="./data/dbc",
    start_date="2020-01-01",
    uf_list=["SP"],
)

downloader = FTPDownloader(config)
files = downloader.download()

# Processamento
from pydatasus.transform.processors import SihsusProcessor
from pydatasus.config import ProcessingConfig

proc_config = ProcessingConfig(
    input_dir="./data/csv",
    output_dir="./data/processed",
)

processor = SihsusProcessor(proc_config)
stats = processor.process_directory()

# DuckDB
from pydatasus.storage import DuckDBManager
from pydatasus.config import DatabaseConfig

db_config = DatabaseConfig(db_path="./data/sihsus.duckdb")

with DuckDBManager(db_config) as db:
    # Registrar Parquet
    db.register_parquet(Path("./data/parquet/sihsus"), "sihsus")

    # Consultar
    df = db.query("SELECT uf, COUNT(*) as total FROM sihsus GROUP BY uf")
    print(df)

    # Exportar
    db.export_query_to_csv(
        "SELECT * FROM sihsus WHERE uf = 'SP'",
        Path("./sp_data.csv"),
    )
```

## 📊 Pipeline Completo

```
FTP Download → DBC Files → DBF Files → CSV Raw →
CSV Processed → IBGE Enriched → Parquet → DuckDB
```

## 🏗️ Arquitetura

### Componentes Principais

- **FTPDownloader**: Download do FTP DATASUS
- **DbcToDbfConverter**: Conversão usando TABWIN
- **DbfToCsvConverter**: Conversão para CSV
- **SihsusProcessor**: Limpeza e validação
- **IbgeEnricher**: Enriquecimento geográfico
- **ParquetWriter**: Armazenamento otimizado
- **DuckDBManager**: Banco analítico
- **DataExporter**: Exportação de dados

### Design Patterns

- **Template Method**: Pipeline base
- **Strategy**: Conversores diferentes
- **Chain of Responsibility**: Stages sequenciais
- **Factory**: Criação de componentes (preparado)
- **Singleton**: Conexões de banco (DuckDB)

## 📝 Dados Disponíveis

- **SIHSUS**: Dados de internações hospitalares
- **Período**: 1992-01 até presente
- **Cobertura**: Todos os estados brasileiros
- **Formato final**: Parquet particionado + DuckDB

## 🧪 Testes

```bash
# Executar todos os testes
pytest

# Com coverage
pytest --cov=src/pydatasus --cov-report=html

# Testes específicos
pytest tests/unit/test_config.py
```

**Cobertura**: 50+ testes unitários (~85% de cobertura)

## 📁 Estrutura do Projeto

```
pydatasus/
├── src/pydatasus/
│   ├── __init__.py
│   ├── cli.py                    # CLI com Typer
│   ├── config.py                 # Configurações Pydantic
│   ├── constants.py              # Constantes
│   ├── core/                     # Pipeline framework
│   ├── download/                 # FTP downloader
│   ├── transform/
│   │   ├── converters/           # DBC/DBF/CSV
│   │   ├── processors/           # SihsusProcessor
│   │   └── enrichers/            # IbgeEnricher
│   ├── storage/                  # Parquet, DuckDB
│   └── pipeline/                 # SihsusPipeline
├── tests/                        # 50+ testes
├── pyproject.toml
└── README.md
```

## 📚 Documentação Adicional

- [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) - Resumo da implementação
- [tests/README.md](tests/README.md) - Guia de testes

## 📄 Licença

Apache License 2.0

---

**PyDataSUS** - Pipeline profissional para dados do SUS
