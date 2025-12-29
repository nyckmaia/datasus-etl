# Temp File Optimization - Melhoria 02

## Overview

Analysis of temporary file handling in PyDataSUS pipeline and optimization opportunities.

## Current Behavior

The pipeline creates temporary files during processing:

1. **DBC files** - Downloaded from FTP to `dbc/` directory
2. **DBF files** - Converted from DBC to `dbf/` directory
3. **DuckDB database** - In-memory or file-based during processing

### File Flow

```
FTP Server → dbc/*.dbc → dbf/*.dbf → DuckDB (memory) → parquet/*.parquet
                ↓             ↓
           (deleted)     (deleted)
```

## Current Cleanup Implementation

```python
# In base_pipeline.py
def _cleanup_temp_files(self):
    """Remove temporary DBC and DBF files after successful export."""
    dbc_dir = self.config.storage.dbc_dir
    dbf_dir = self.config.storage.dbf_dir

    for f in dbc_dir.glob("*.dbc"):
        f.unlink()
    for f in dbf_dir.glob("*.dbf"):
        f.unlink()
```

## Optimization Opportunities

### 1. Stream-Delete Pattern

**Idea**: Delete DBC immediately after DBF conversion

```python
def convert_and_cleanup(dbc_path: Path) -> Path:
    dbf_path = convert_dbc_to_dbf(dbc_path)
    dbc_path.unlink()  # Delete immediately
    return dbf_path
```

**Pros**:
- Reduces peak disk usage by ~50%
- Useful for processing large datasets

**Cons**:
- Cannot retry conversion on failure
- Complicates error recovery

### 2. Memory-Mapped DBF Reading

**Idea**: Use mmap for DBF reading to reduce memory footprint

```python
import mmap

def read_dbf_mmap(dbf_path: Path):
    with open(dbf_path, 'rb') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            # Process DBF directly from mmap
            pass
```

**Pros**:
- OS manages memory efficiently
- Works well with large files

**Cons**:
- Requires dbf library changes
- Platform-specific behavior

### 3. Streaming Download → Convert

**Idea**: Pipe FTP download directly to DBC converter

```python
# Hypothetical - would require library changes
async def stream_convert(ftp_url: str) -> Path:
    async with ftp_stream(ftp_url) as stream:
        dbf_path = convert_stream_to_dbf(stream)
    return dbf_path
```

**Pros**:
- No DBC files on disk at all
- Minimal disk usage

**Cons**:
- Requires datasus-dbc library changes
- Complex error handling
- Cannot resume partial downloads

### 4. Configurable Cleanup Strategy

**Idea**: Let users choose cleanup behavior

```python
class CleanupStrategy(Enum):
    KEEP_ALL = "keep"        # Keep all intermediate files
    KEEP_DBF = "keep_dbf"    # Keep DBF, delete DBC
    DELETE_ALL = "delete"    # Delete all temp files (default)

config = PipelineConfig(
    cleanup_strategy=CleanupStrategy.DELETE_ALL
)
```

**Use Cases**:
- `KEEP_ALL`: Debugging, data inspection
- `KEEP_DBF`: Faster re-processing (skip download+convert)
- `DELETE_ALL`: Production, minimal disk usage

### 5. Chunked Processing

**Idea**: Process files in batches to limit disk usage

```python
def process_chunked(files: list[str], chunk_size: int = 10):
    for chunk in batched(files, chunk_size):
        # Download chunk
        dbc_files = download_files(chunk)
        # Convert chunk
        dbf_files = convert_files(dbc_files)
        # Load to DuckDB
        load_files(dbf_files)
        # Cleanup chunk
        cleanup_files(dbc_files + dbf_files)
```

**Pros**:
- Predictable disk usage
- Works with limited storage

**Cons**:
- Slower overall (less parallelism)
- More complex orchestration

## Disk Usage Analysis

### Typical File Sizes (SIHSUS, 1 month, 1 UF)

| Stage | Size | Cumulative |
|-------|------|------------|
| DBC | ~5 MB | 5 MB |
| DBF | ~50 MB | 55 MB |
| Parquet | ~15 MB | 70 MB |

### All UFs (27), 1 Month

| Stage | Size | Cumulative |
|-------|------|------------|
| DBC | ~135 MB | 135 MB |
| DBF | ~1.35 GB | 1.5 GB |
| Parquet | ~400 MB | 1.9 GB |

### All UFs, 12 Months

| Stage | Size | Cumulative |
|-------|------|------------|
| DBC | ~1.6 GB | 1.6 GB |
| DBF | ~16 GB | 17.6 GB |
| Parquet | ~5 GB | 22.6 GB |

## Recommendations

### Priority 1: Configurable Cleanup Strategy
- Low effort, high flexibility
- Add `cleanup_strategy` to config
- Default to DELETE_ALL

### Priority 2: Immediate DBC Cleanup
- Delete DBC right after successful conversion
- Reduces peak usage by ~10%

### Priority 3: Chunked Processing Mode
- Add `--max-concurrent-files` option
- Process in batches for limited storage

### Lower Priority
- Memory-mapped reading (library changes needed)
- Streaming download (significant refactor)

## Implementation Notes

Current cleanup already deletes temp files after successful export. Main improvements would be:

1. More granular control over what to keep
2. Earlier cleanup (per-file vs batch)
3. Better error recovery with partial cleanup

## Related Files

- `src/pydatasus/pipeline/base_pipeline.py` - cleanup logic
- `src/pydatasus/download/` - FTP download
- `src/pydatasus/convert/` - DBC→DBF conversion
