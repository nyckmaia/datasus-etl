# Análise de Consumo de RAM no Pipeline DataSUS ETL

## Resumo Executivo

Este documento descreve a implementação do modo memory-aware para processamento
de grandes datasets do DataSUS sem esgotar a memória RAM.

---

## 1. Implementação do Modo Memory-Aware

### 1.1 Ativação

```bash
# Ativar modo memory-aware com 4 workers paralelos
datasus run -s sihsus --start-date 2023-01-01 -d ./data --memory-aware -w 4

# Usar alias curto
datasus run -s sihsus --start-date 2023-01-01 -d ./data -m -w 4
```

### 1.2 Parâmetros

| Parâmetro | Alias | Default | Descrição |
|-----------|-------|---------|-----------|
| `--memory-aware` | `-m` | False | Ativa o modo memory-aware |
| `--num-workers` | `-w` | 4 | Número de workers paralelos (1-8) |

---

## 2. Como Funciona

### 2.1 Fluxo de Processamento

```
Para cada arquivo DBC (em paralelo com N workers):
    1. Descomprime DBC → DBF temporário
    2. Cria conexão DuckDB in-memory independente
    3. Stream DBF → tabela staging
    4. Aplica transformações SQL
    5. Exporta para Parquet/CSV particionado (uf=XX/)
    6. Fecha conexão DuckDB (libera RAM)
    7. Deleta DBF temporário
```

### 2.2 Isolamento de Workers

Cada worker usa sua própria conexão DuckDB in-memory:

```python
# Cada worker cria conexão independente
conn = duckdb.connect(":memory:")  # Não compartilha dados

# Configura limite de RAM por conexão
conn.execute(f"SET memory_limit = '{per_conn_limit_gb}GB'")
```

Isso permite:
- Verdadeiro paralelismo (sem locks)
- Isolamento de memória
- Falha de um worker não afeta outros

### 2.3 Gerenciamento Automático de RAM

O processador estima automaticamente o uso de RAM:

```python
# Estima RAM por arquivo
estimated_dbf_size = dbc_size * 10  # DBC → DBF: ~10x
estimated_ram = estimated_dbf_size * 3  # DBF → RAM: ~3x

# Calcula workers seguros
usable_ram = available_ram * 0.7  # 70% safety margin
max_safe_workers = usable_ram / estimated_ram

# Se RAM insuficiente, usa modo serial
if max_safe_workers < 2:
    processing_mode = "serial"
```

---

## 3. Estrutura de Saída

### 3.1 Particionamento Hive

Os arquivos são organizados por UF:

```
output_dir/
├── uf=AC/
│   ├── RDAC2301.parquet
│   └── RDAC2302.parquet
├── uf=SP/
│   ├── RDSP2301.parquet
│   └── RDSP2302.parquet
└── uf=RJ/
    ├── RDRJ2301.parquet
    └── RDRJ2302.parquet
```

### 3.2 Nomenclatura

- Cada arquivo Parquet/CSV mantém o nome do DBC original
- Particionado por coluna `uf`
- Facilita queries por estado:

```sql
SELECT * FROM read_parquet('output_dir/uf=SP/*.parquet')
```

---

## 4. Comparativo de Modos

| Aspecto | Modo Padrão | Modo Memory-Aware |
|---------|-------------|-------------------|
| RAM mínima | ~8GB+ | ~2GB |
| Paralelismo | Single DuckDB | Multi-conexão |
| Arquivo/vez | Todos carregados | 1 por worker |
| Tolerância a falha | Baixa (tudo perdido) | Alta (por arquivo) |
| Velocidade | Mais rápido (se RAM) | Ligeiramente menor |
| Uso de disco | Menor (compactação) | ~20% maior |

---

## 5. Recomendações de Uso

### 5.1 Quando Usar Memory-Aware

- Processamento de todos os 27 estados
- Períodos longos (vários anos)
- Máquinas com RAM limitada (<16GB)
- Ambientes compartilhados

### 5.2 Configuração de Workers

| RAM Disponível | Workers Recomendados |
|----------------|----------------------|
| < 4 GB | 1 (serial) |
| 4-8 GB | 2 |
| 8-16 GB | 4 |
| > 16 GB | 8 |

### 5.3 Exemplo para Dataset Completo

```bash
# Processar todos os estados de 2020-2023
datasus run \
    -s sihsus \
    --start-date 2020-01-01 \
    --end-date 2023-12-31 \
    -d ./data \
    --memory-aware \
    --num-workers 4 \
    --yes
```

---

## 6. Arquivos de Implementação

| Arquivo | Descrição |
|---------|-----------|
| `storage/memory_aware_processor.py` | Processador principal |
| `pipeline/base_pipeline.py` | Stage MemoryAwareProcessingStage |
| `config.py` | Parâmetros num_workers, memory_aware_mode |
| `cli.py` | Flags --memory-aware e --num-workers |

---

## 7. Métricas de Saída

O processador exibe métricas durante a execução:

```
============================================================
Memory-Aware Processing: 324 DBC files
============================================================
Available RAM: 15.2 GB
Est. RAM/file: 0.45 GB
Workers: 4 (parallel mode)
Output: ./data/sihsus/parquet
Format: PARQUET
============================================================

Processing DBC files (4 workers): 100%|████████| 324/324
[OK] RDSP2301.dbc → uf=SP: 145,234 rows
[OK] RDRJ2301.dbc → uf=RJ: 98,765 rows
...

============================================================
Processing Complete
============================================================
Successful: 324/324 files
Failed: 0
Total rows: 12,345,678
============================================================
```

---

*Documento atualizado após implementação.*
