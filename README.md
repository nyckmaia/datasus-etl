# datasus-etl

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](LICENSE)

An open-source ETL for **DATASUS**, Brazil's public-health data warehouse.
It downloads DBC files from the DATASUS FTP, converts them through DBF into
DuckDB, enriches the result with IBGE municipality references and CID-10
codings, and writes partitioned Parquet — in a single command. Built for
researchers who need the data, not the plumbing.

Supported subsystems:

- **SIHSUS** — Hospital Information System
- **SIM** — Mortality Information System
- **SIASUS** — Outpatient Information System (in development)

## Why datasus-etl?

DATASUS is the richest public source of Brazilian health data, but using it
directly has historically meant fighting the tools instead of analyzing the
data. This package solves the recurring pain points researchers face:

- **Opaque FTP layout.** DATASUS distributes each subsystem under its own
  directory tree with bespoke filename conventions. `datasus-etl` ships
  per-subsystem parsers so you can ask for *"SIHSUS between 2020 and 2024
  for SP, RJ, MG"* and get exactly that.
- **DBC compression.** The `.dbc` format is a proprietary extension of DBF
  that Python could not natively read until recently. We depend on
  `datasus-dbc` for a pure-Python decoder — no external binaries required.
- **SIM CID-9 vs CID-10 disambiguation.** SIM filenames collide between the
  two ICD revisions unless you disambiguate by stem length (`DOUFYYYY` vs
  `DORUFYY`). Getting this wrong silently drops every CID-10 death record
  from RJ, RN, RO, RR and RS. Our parser handles it.
- **IBGE enrichment out of the box.** The pipeline joins every row against
  the 5,571 Brazilian municipalities, adding human-readable names, UF,
  immediate region and intermediate region — ready for grouping and mapping.
- **Memory-aware streaming.** DBF files for large states can be several GB.
  The pipeline streams DBF → DuckDB → Parquet chunk by chunk so a consumer
  laptop can process the full country without blowing up RAM.
- **Four usage modes from one install.** The same package exposes a CLI,
  a Python API, an interactive DuckDB SQL shell, and a bundled FastAPI +
  React web UI for non-technical users.

## Research context

Developed by **Nycholas Maia** in technical collaboration with
**Paulo Alves Maia (FUNDACENTRO)** within the CNPq research group
*"Mudanças Climáticas e Segurança e Saúde no Trabalho"* (Climate Change and
Occupational Safety and Health) —
<http://dgp.cnpq.br/dgp/espelhogrupo/216702>.

## Features

- Automated FTP downloads from DATASUS
- Pure-Python DBC → DBF conversion (via `datasus-dbc`)
- Streaming DBF → DuckDB pipeline (no intermediate CSV)
- SQL transformations optimized in DuckDB
- Built-in IBGE enrichment (5,571 municipalities)
- UF-partitioned Parquet storage
- Web UI (FastAPI + React) for non-technical users
- Full CLI for automation
- Python API for integration into larger pipelines
- Interactive DuckDB shell for ad-hoc SQL
- Automatic cleanup of temporary DBC/DBF files

## Installation

```bash
pip install datasus-etl
```

For development:

```bash
git clone https://github.com/nyckmaia/datasus-etl.git
cd datasus-etl
pip install -e ".[dev]"
```

## Usage

`datasus-etl` offers four ways to use it.

### 1. CLI (Command Line Interface)

```bash
# Full pipeline: download → convert → transform → export
datasus pipeline --source sihsus --start-date 2023-01-01 --end-date 2023-12-31 --data-dir ./data/datasus --uf SP,RJ

# Incremental update (only new files)
datasus update --source sihsus --start-date 2023-01-01 --data-dir ./data/datasus

# Database status
datasus status --source sihsus --data-dir ./data/datasus

# Interactive DuckDB SQL shell
datasus db --data-dir ./data/datasus

# Launch the web UI
datasus ui
datasus ui --port 8080
```

**`pipeline` command options:**

| Option | Description | Default |
|-------|-------------|---------|
| `--source`, `-s` | Subsystem (`sihsus`, `sim`, `siasus`) | — |
| `--start-date` | Start date (`YYYY-MM-DD`) | — |
| `--end-date` | End date (`YYYY-MM-DD`) | today |
| `--uf` | Comma-separated Brazilian states | all |
| `--data-dir`, `-d` | Data directory | — |
| `--compression`, `-c` | Parquet compression | `zstd` |
| `--memory-aware`, `-m` | RAM-optimized mode | `False` |
| `--num-workers`, `-w` | Parallel workers (1–8) | `4` |
| `--keep-temp-files` | Keep intermediate DBC/DBF files | `False` |

### 2. Interactive DuckDB shell

```bash
# Opens a shell with automatic VIEWs for every subsystem with data
datasus db --data-dir ./data/datasus

# Restrict to a single subsystem
datasus db --data-dir ./data/datasus --source sihsus
```

**Shell commands:**

| Command | Description |
|---------|-------------|
| `.tables` | List available VIEWs |
| `.schema <view>` | Show columns of a VIEW |
| `.count <view>` | Count records |
| `.sample <view> [n]` | Show N random rows |
| `.csv <file>` | Export the last result as CSV |
| `.maxrows [n]` | Set the max number of rows displayed |
| `.exit` | Exit the shell |

**Example session:**

```sql
datasus> SELECT COUNT(*) FROM sihsus;
datasus> SELECT uf, COUNT(*) AS total FROM sihsus GROUP BY uf ORDER BY total DESC;
datasus> .csv result.csv
```

### 3. Python API

```python
from datasus_etl.config import PipelineConfig
from datasus_etl.pipeline.sihsus_pipeline import SihsusPipeline

# Build a configuration via the factory method
config = PipelineConfig.create(
    base_dir="./data/datasus",
    subsystem="sihsus",
    start_date="2023-01-01",
    end_date="2023-12-31",
    uf_list=["SP", "RJ", "MG"],
    compression="zstd",
)

# Run the pipeline
pipeline = SihsusPipeline(config)
result = pipeline.run()

print(f"Rows exported: {result.get_metadata('total_rows_exported'):,}")
```

**Query the data with SQL:**

```python
from datasus_etl.storage.parquet_query_engine import ParquetQueryEngine

# Connect to the Parquet store
engine = ParquetQueryEngine("./data/datasus/sihsus/parquet", view_name="sihsus")

# Run a SQL query
df = engine.sql("""
    SELECT
        uf,
        municipio_res,
        COUNT(*) AS admissions,
        SUM(val_tot) AS total_value
    FROM sihsus
    WHERE ano_cmpt = 2023
    GROUP BY uf, municipio_res
    ORDER BY admissions DESC
    LIMIT 10
""")

print(df.to_pandas())

# Inspect the schema
print(engine.schema())

# Count records
print(f"Total: {engine.count():,} records")

engine.close()
```

### 4. Web UI (FastAPI + React)

```bash
datasus ui                                  # opens http://localhost:8787
datasus ui --data-dir /media/Data/datasus   # set the base directory
datasus ui --port 8080 --no-open            # custom port, no browser launch
```

The UI is an English SPA served by the Python package itself, with no
external dependencies for the end user. Pages:

- **Dashboard** — aggregated statistics, UF coverage map, time series of
  data volume, and per-subsystem cards. Each card's **Update** button jumps
  straight to the *Scope* step of the wizard with the subsystem
  pre-selected.
- **Download** — a 4-step wizard (subsystem → scope → estimate → live SSE
  execution). The *Scope* step uses a `MonthPicker`, pre-fills the end
  date with the current month, and shows existing coverage per UF
  (first/last period already downloaded).
- **Query** — a Monaco SQL editor with predefined templates and a column
  dictionary. Export results to CSV or Excel.
- **Settings** — persisted data directory at
  `~/.config/datasus-etl/config.toml`. Ships a native folder picker
  (tkinter in a subprocess so it does not block the uvicorn event loop)
  and validates the path before saving (exists?, writable?, already has
  data?).

## Data layout

After processing, data is organized under a `datasus_db/` root created
inside `--data-dir`. If you point `--data-dir` directly at a folder already
named `datasus_db/` (or `parquet/`), the pipeline respects that choice and
does not re-nest — the logic lives in `src/datasus_etl/storage/paths.py`.

```
<data-dir>/datasus_db/
├── sihsus/                    # Hospital Information System
│   ├── dbc/                   # Original DBC files (deleted after processing)
│   ├── dbf/                   # Converted DBF files (deleted after processing)
│   └── parquet/               # Final partitioned Parquet
│       ├── uf=SP/
│       │   └── data_0.parquet
│       ├── uf=RJ/
│       │   └── data_0.parquet
│       └── uf=MG/
│           └── data_0.parquet
├── sim/                       # Mortality Information System
│   └── parquet/
│       └── ...
└── siasus/                    # Outpatient Information System
    └── parquet/
        └── ...
```

## Enriched columns

The pipeline automatically adds IBGE geographic metadata:

| Column | Description | Example |
|--------|-------------|---------|
| `municipio_res` | Municipality of residence | São Paulo |
| `uf_res` | State of residence | São Paulo |
| `rg_imediata_res` | Immediate geographic region | São Paulo |
| `rg_intermediaria_res` | Intermediate geographic region | São Paulo |

Plus the existing code-to-text expansions:

| Column | Transformation |
|--------|----------------|
| `sexo` | Numeric code → text (M / F / I) |
| `raca_cor` | Numeric code → text (Branca, Preta, Parda, …) |

## Supported subsystems

| Subsystem | Description | Status |
|-----------|-------------|--------|
| SIHSUS | Hospital Information System | Complete |
| SIM | Mortality Information System | Complete |
| SIASUS | Outpatient Information System | Planned |

### Notes on SIM

- SIM data is published with a **~2-year lag** (CID-10 coding revision). If
  the estimate returns zero files, extend the window further into the past.
- The SIM filename parser distinguishes CID-10 from CID-9 by **stem
  length**, not by prefix: `DOUFYYYY.dbc` (8 chars, CID-10, 1996+) vs.
  `DORUFYY.dbc` (7 chars, CID-9, 1979–1995). Checking the `DOR` prefix
  first would collide with CID-10 UFs that start with "R" (RJ / RN / RO /
  RR / RS) and silently drop every death record from those five states.

## Performance

The pipeline is tuned for large-volume processing:

- **DBF streaming** — handles files larger than available RAM
- **Memory-aware mode** — processes one file at a time with parallel workers
- **Chunked processing** — configurable chunk size
- **Partition pruning** — DuckDB reads only the partitions it needs
- **Parquet compression** — `zstd` gives the best size/speed trade-off

## Configuration

### Memory-aware mode (recommended for large datasets)

```bash
# Processes all 27 Brazilian states without exhausting RAM
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

### Tuning for limited RAM

```python
from datasus_etl.config import PipelineConfig

config = PipelineConfig.create(
    base_dir="./data/datasus",
    subsystem="sihsus",
    start_date="2023-01-01",
    chunk_size=5000,  # Reduce for less RAM usage
)
```

### Keep temporary files

```bash
datasus pipeline --source sihsus --start-date 2023-01-01 -d ./data/datasus --keep-temp-files
```

Or via Python:

```python
from datasus_etl.config import PipelineConfig

config = PipelineConfig.create(
    base_dir="./data/datasus",
    subsystem="sihsus",
    start_date="2023-01-01",
    keep_temp_files=True,
)
```
