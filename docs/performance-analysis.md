# Performance Analysis - Melhoria 01

## Overview

Analysis of parallelization opportunities in the DataSUS-ETL pipeline.

## Current Architecture

The pipeline processes data in these stages:
1. **Download** (FTP) - Downloads DBC files from DataSUS
2. **Convert** (DBC→DBF) - Decompresses DBC to DBF format
3. **Load** (DBF→DuckDB) - Streams DBF data into DuckDB tables
4. **Transform** (SQL) - Applies transformations in DuckDB
5. **Export** (Parquet/CSV) - Writes final output files

## Parallelization Opportunities

### 1. Download Stage
**Current**: Sequential FTP downloads
**Opportunity**: Multiple concurrent FTP connections

```python
# Potential implementation with asyncio/aiofiles
async def download_parallel(files: list[str], max_concurrent: int = 5):
    semaphore = asyncio.Semaphore(max_concurrent)
    async with semaphore:
        await download_file(file)
```

**Considerations**:
- FTP server may limit connections per IP
- Network bandwidth is often the bottleneck
- Rate limiting to avoid being blocked

### 2. Conversion Stage (DBC→DBF)
**Current**: Sequential conversion using `datasus-dbc` library
**Opportunity**: Parallel conversion using multiprocessing

```python
from concurrent.futures import ProcessPoolExecutor

def convert_parallel(dbc_files: list[Path], max_workers: int = 4):
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(convert_single_file, dbc_files)
```

**Considerations**:
- CPU-bound operation
- Memory usage per process
- Ideal `max_workers = os.cpu_count()`

### 3. Load Stage (DBF→DuckDB)
**Current**: Sequential streaming per file
**Opportunity**: Limited - DuckDB handles parallelism internally

**Considerations**:
- DuckDB already uses multiple threads for queries
- Parallel inserts may cause contention
- Better to let DuckDB optimize internally

### 4. Transform Stage (SQL)
**Current**: Sequential view creation per staging table
**Opportunity**: Limited - SQL execution benefits from DuckDB parallelism

**Considerations**:
- DuckDB's query optimizer handles parallelism
- Memory management is automatic
- Transform is typically fast (SQL operations on in-memory data)

### 5. Export Stage
**Current**: Sequential file export
**Opportunity**: Parallel export for multiple UFs/partitions

```python
# Export different UFs in parallel
with ThreadPoolExecutor(max_workers=4) as executor:
    for uf in ['SP', 'RJ', 'MG', 'BA']:
        executor.submit(export_partition, uf)
```

**Considerations**:
- I/O-bound operation
- Disk write speed is often the bottleneck
- SSD vs HDD performance difference

## Recommendations

### Priority 1: Conversion Parallelization
- Highest impact for large datasets
- Easy to implement with ProcessPoolExecutor
- No external dependencies

### Priority 2: Download Parallelization
- Good impact for initial downloads
- Requires async implementation
- Need to respect FTP server limits

### Priority 3: Export Parallelization
- Moderate impact
- Only beneficial for multi-UF exports
- Disk I/O may be the limit

## Benchmarks Needed

1. Measure current stage durations (sample: 1 month, all UFs)
2. Test conversion with 2, 4, 8 workers
3. Test download with 3, 5, 10 concurrent connections
4. Compare SSD vs HDD export times

## Implementation Plan

1. Add `max_workers` parameter to ConversionConfig (already exists)
2. Implement parallel conversion in DbcToDbfConverter
3. Add connection limit parameter to DownloadConfig
4. Implement async download with configurable concurrency
5. Benchmark and document optimal settings
