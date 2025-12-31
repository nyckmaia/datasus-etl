# Análise de Consumo de RAM no Pipeline DataSUS ETL

## Resumo Executivo

Este documento analisa o consumo de memória RAM durante o processamento de dados do DataSUS e propõe melhorias para otimizar o uso em máquinas com recursos limitados.

---

## 1. Processo Atual de Streaming

### 1.1 Fluxo de Dados

```
DBC (comprimido) → DBF (descomprimido) → DuckDB Staging → Parquet
```

O pipeline atual utiliza streaming DBF→DuckDB para processar arquivos sem carregar tudo em memória:

1. **Descompressão DBC→DBF**: Arquivo por arquivo usando `datasus-dbc`
2. **Leitura DBF**: Streaming via `dbfread` com chunks de ~10.000 registros
3. **Staging DuckDB**: Inserção batch em tabela temporária
4. **Transformação**: SQL queries no DuckDB
5. **Export Parquet**: Via DuckDB's Parquet writer

### 1.2 Componentes que Usam RAM

| Componente | Uso de RAM | Descrição |
|------------|------------|-----------|
| DBF Reader | Baixo (~50MB) | Leitura streaming com chunks |
| DuckDB Staging | **Alto** | Tabelas temporárias acumulam em memória |
| SQL Transforms | Médio | Depende da complexidade das queries |
| Parquet Writer | Baixo | Streaming direto do DuckDB |

---

## 2. Problema: Acúmulo em Tabelas Staging

### 2.1 Causa Raiz

O DuckDB mantém tabelas staging em memória até o export final. Para grandes volumes de dados (ex: todos os estados de um ano), a RAM pode se esgotar:

```python
# Atual: todos os arquivos vão para mesma tabela staging
for dbf_file in dbf_files:
    stream_to_staging(dbf_file, staging_table)  # Acumula em RAM

# Só depois exporta tudo
export_to_parquet(staging_table)  # Precisa de toda RAM disponível
```

### 2.2 Consumo Estimado

| Volume | Registros | RAM Estimada |
|--------|-----------|--------------|
| 1 UF, 1 mês | ~100K | ~200MB |
| 1 UF, 1 ano | ~1.2M | ~2.4GB |
| Todos UFs, 1 mês | ~2.5M | ~5GB |
| Todos UFs, 1 ano | ~30M | **~60GB** |

---

## 3. Proposta: Export Incremental por Arquivo

### 3.1 Novo Fluxo

```python
# Proposta: exportar Parquet por arquivo DBF
for dbf_file in dbf_files:
    # 1. Stream para staging temporária
    stream_to_staging(dbf_file, temp_table)

    # 2. Transforma
    transformed = apply_transforms(temp_table)

    # 3. Exporta imediatamente
    export_to_parquet(transformed, output_file)

    # 4. Limpa staging (libera RAM)
    drop_table(temp_table)
```

### 3.2 Vantagens

- **RAM Constante**: Uso de memória proporcional a 1 arquivo (~200MB)
- **Resistente a Falhas**: Arquivos já exportados não são perdidos
- **Incremental Nativo**: Facilita reprocessamento parcial
- **Paralelização**: Múltiplos workers podem processar simultaneamente

### 3.3 Desvantagens

- **Mais Arquivos Parquet**: Um arquivo por DBF fonte
- **Compactação Menor**: Parquet funciona melhor com mais dados
- **Queries Mais Lentas**: DuckDB precisa abrir múltiplos arquivos

---

## 4. Comparativo de Abordagens

| Aspecto | Batch Atual | Incremental Proposto |
|---------|-------------|---------------------|
| RAM mínima | ~8GB | ~512MB |
| Tempo total | Menor | Ligeiramente maior (+10%) |
| Resistência a falhas | Baixa | Alta |
| Tamanho Parquet final | Menor | ~20% maior |
| Complexidade código | Menor | Média |
| Queries SQL | Mais rápidas | Ligeiramente mais lentas |

---

## 5. Modo Híbrido Recomendado

### 5.1 Estratégia Adaptativa

Detectar RAM disponível e escolher modo automaticamente:

```python
import psutil

def get_processing_mode() -> str:
    available_ram_gb = psutil.virtual_memory().available / (1024**3)

    if available_ram_gb >= 16:
        return "batch"      # RAM suficiente para batch completo
    elif available_ram_gb >= 4:
        return "chunked"    # Processa N arquivos por vez
    else:
        return "streaming"  # 1 arquivo por vez
```

### 5.2 Configuração Sugerida

```python
class ProcessingConfig:
    mode: Literal["auto", "batch", "chunked", "streaming"] = "auto"
    chunk_size: int = 10  # Arquivos por chunk no modo chunked
    max_memory_mb: Optional[int] = None  # Limite manual de RAM
```

---

## 6. Implementação Futura

### 6.1 Prioridades

1. **Fase 1**: Implementar modo `streaming` para sistemas com pouca RAM
2. **Fase 2**: Adicionar detecção automática de RAM
3. **Fase 3**: Implementar modo `chunked` como intermediário
4. **Fase 4**: Otimizar compactação Parquet pós-processamento

### 6.2 Arquivos a Modificar

- `src/datasus_etl/config.py`: Adicionar opções de modo
- `src/datasus_etl/storage/duckdb_manager.py`: Lógica de streaming
- `src/datasus_etl/storage/parquet_writer.py`: Export incremental
- `src/datasus_etl/cli.py`: Opção `--low-memory`

---

## 7. Conclusão

O modo híbrido baseado em RAM disponível é a melhor abordagem:

- **Máquinas robustas (16GB+)**: Batch para máxima performance
- **Máquinas médias (4-16GB)**: Chunked para balancear RAM e performance
- **Máquinas modestas (<4GB)**: Streaming para garantir execução

A implementação do modo streaming deve ser priorizada para permitir uso em máquinas com recursos limitados, especialmente em ambientes acadêmicos e de pesquisa.

---

*Documento gerado para análise técnica. Implementação sujeita a revisão.*
