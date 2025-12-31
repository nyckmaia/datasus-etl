"""SQL transformation module for PyDataSUS.

This module provides modular SQL transformations for DataSUS data processing.
Each transformation is encapsulated in its own class and can be composed
into a pipeline for sequential application.

Transform Classes:
- BaseTransform: Abstract base class for all transforms
- CleaningTransform: Removes invisible characters and trims whitespace
- DateParsingTransform: Parses date strings with multiple format fallback
- TypeCastTransform: Converts columns to target types based on schema
- CidValidationTransform: Validates ICD-10 (CID) code format
- SexoTransform: Maps SEXO codes to labels (M/F/I)
- RacaCorTransform: Maps RACA_COR codes to labels
- IbgeEnrichmentTransform: Adds geographic data via IBGE lookup

Pipeline:
- TransformPipeline: Orchestrates multiple transforms into a complete SQL query

Example:
    ```python
    import duckdb
    from datasus_etl.transform.sql import TransformPipeline

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

from datasus_etl.transform.sql.base import BaseTransform
from datasus_etl.transform.sql.cleaning import CleaningTransform
from datasus_etl.transform.sql.dates import DateParsingTransform
from datasus_etl.transform.sql.categorical import SexoTransform, RacaCorTransform
from datasus_etl.transform.sql.types import TypeCastTransform
from datasus_etl.transform.sql.validation import CidValidationTransform
from datasus_etl.transform.sql.enrichment import IbgeEnrichmentTransform
from datasus_etl.transform.sql.pipeline import TransformPipeline

__all__ = [
    "BaseTransform",
    "CleaningTransform",
    "DateParsingTransform",
    "TypeCastTransform",
    "CidValidationTransform",
    "SexoTransform",
    "RacaCorTransform",
    "IbgeEnrichmentTransform",
    "TransformPipeline",
]
