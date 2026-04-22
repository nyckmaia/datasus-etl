"""SIHSUS (Sistema de Informacoes Hospitalares) data processing pipeline.

Thin subclass of :class:`DatasusPipeline` — only declares the subsystem
name and DuckDB schema. All download, conversion, and reporting logic
lives in the generic base pipeline and is shared with every other
DataSUS subsystem (SIM, future SIASUS/CNES/SINASC).
"""

from pathlib import Path
from typing import Optional

from datasus_etl.config import PipelineConfig
from datasus_etl.constants.sihsus_schema import SIHSUS_DUCKDB_SCHEMA
from datasus_etl.pipeline.base_pipeline import DatasusPipeline


class SihsusPipeline(DatasusPipeline):
    """Pipeline for SIHSUS hospital information data."""

    def __init__(self, config: PipelineConfig, ibge_data_path: Optional[Path] = None) -> None:
        super().__init__(config, ibge_data_path)

    @property
    def subsystem_name(self) -> str:
        return "sihsus"

    @property
    def schema(self) -> dict[str, str]:
        return SIHSUS_DUCKDB_SCHEMA
