"""Pipeline components for PyDataSUS.

This module provides pipeline classes for processing DataSUS subsystem data:

- DatasusPipeline: Base class for all subsystem pipelines
- SihsusPipeline: Pipeline for SIHSUS (Hospital Information System)
- SIMPipeline: Pipeline for SIM (Mortality Information System)

Each pipeline handles the complete data processing workflow:
1. Download DBC files from DATASUS FTP
2. Convert DBC to DBF (decompression)
3. Stream DBF to DuckDB
4. Transform and enrich data using SQL
5. Export to Hive-partitioned Parquet

Example:
    ```python
    from pydatasus.config import PipelineConfig
    from pydatasus.pipeline import SihsusPipeline, SIMPipeline

    # SIHSUS example
    config = PipelineConfig.create(
        base_dir="./data/datasus",
        subsystem="sihsus",
        start_date="2023-01-01",
    )
    pipeline = SihsusPipeline(config)
    result = pipeline.run()

    # SIM example
    config = PipelineConfig.create(
        base_dir="./data/datasus",
        subsystem="sim",
        start_date="2020-01-01",
    )
    pipeline = SIMPipeline(config)
    result = pipeline.run()
    ```
"""

from pydatasus.pipeline.base_pipeline import DatasusPipeline
from pydatasus.pipeline.sihsus_pipeline import SihsusPipeline
from pydatasus.pipeline.sim_pipeline import SIMPipeline

__all__ = [
    "DatasusPipeline",
    "SihsusPipeline",
    "SIMPipeline",
]
