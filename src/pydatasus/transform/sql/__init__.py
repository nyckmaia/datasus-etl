"""SQL transformation module for PyDataSUS.

This module provides modular SQL transformations for DataSUS data processing.
Each transformation is encapsulated in its own class and can be composed
into a pipeline for sequential application.

Transform Classes:
- BaseTransform: Abstract base class for all transforms
- CleaningTransform: Removes invisible characters and trims whitespace
- DateParsingTransform: Parses date strings with multiple format fallback
- TypeCastTransform: Converts columns to target types based on schema
- SexoTransform: Maps SEXO codes to labels (M/F/I)
- RacaCorTransform: Maps RACA_COR codes to labels
- IbgeEnrichmentTransform: Adds geographic data via IBGE lookup

Pipeline:
- TransformPipeline: Orchestrates multiple transforms into a complete SQL query

Example:
    ```python
    import duckdb
    from pydatasus.transform.sql import TransformPipeline

    conn = duckdb.connect(":memory:")
    schema = {"idade": "INTEGER", "sexo": "VARCHAR", "dt_inter": "DATE"}

    pipeline = TransformPipeline(
        conn=conn,
        schema=schema,
        subsystem="sihsus",
        raw_mode=False,
    )

    pipeline.execute_transform(
        source_table="staging_data",
        target_view="processed_data",
    )
    ```
"""

from pydatasus.transform.sql.base import BaseTransform
from pydatasus.transform.sql.cleaning import CleaningTransform
from pydatasus.transform.sql.dates import DateParsingTransform
from pydatasus.transform.sql.categorical import SexoTransform, RacaCorTransform
from pydatasus.transform.sql.types import TypeCastTransform
from pydatasus.transform.sql.enrichment import IbgeEnrichmentTransform
from pydatasus.transform.sql.pipeline import TransformPipeline

__all__ = [
    "BaseTransform",
    "CleaningTransform",
    "DateParsingTransform",
    "TypeCastTransform",
    "SexoTransform",
    "RacaCorTransform",
    "IbgeEnrichmentTransform",
    "TransformPipeline",
]
