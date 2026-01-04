"""Pipeline components for DataSUS-ETL.

This module provides pipeline classes for processing DataSUS subsystem data:

- DatasusPipeline: Base class for all subsystem pipelines
- SihsusPipeline: Pipeline for SIHSUS (Hospital Information System)
- SIMPipeline: Pipeline for SIM (Mortality Information System)

Each pipeline handles the complete data processing workflow:
1. Download DBC files from DATASUS FTP
2. Convert DBC to DBF (decompression)
3. Stream DBF to DuckDB
4. Transform and enrich data using SQL
5. Export to persistent DuckDB database

Example:
    ```python
    from datasus_etl.config import PipelineConfig
    from datasus_etl.pipeline import SihsusPipeline, SIMPipeline

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

from datasus_etl.pipeline.base_pipeline import DatasusPipeline
from datasus_etl.pipeline.sihsus_pipeline import SihsusPipeline
from datasus_etl.pipeline.sim_pipeline import SIMPipeline

__all__ = [
    "DatasusPipeline",
    "SihsusPipeline",
    "SIMPipeline",
]
