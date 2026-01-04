"""Dataset modules for DataSUS-ETL subsystems.

This package contains configuration and schema definitions for
different DataSUS subsystems:

- SIHSUS: Sistema de Informacoes Hospitalares (Hospital Information System)
- SIM: Sistema de Informacoes sobre Mortalidade (Mortality Information System)
- SIASUS: Sistema de Informacoes Ambulatoriais (Ambulatory Information System)

Each subsystem module contains:
- schema.py: DuckDB schema definition with column types
- config.py: FTP paths, file patterns, and subsystem-specific configuration
- transforms.py: SQL transformations specific to the subsystem (if needed)
"""

from datasus_etl.datasets.base import DatasetConfig, DatasetRegistry
from datasus_etl.datasets.sihsus import SIHSUSConfig, SIHSUS_DUCKDB_SCHEMA
from datasus_etl.datasets.sim import SIMConfig, SIM_DUCKDB_SCHEMA

__all__ = [
    "DatasetConfig",
    "DatasetRegistry",
    "SIHSUSConfig",
    "SIMConfig",
    "SIHSUS_DUCKDB_SCHEMA",
    "SIM_DUCKDB_SCHEMA",
]
