"""SIM (Sistema de Informacoes sobre Mortalidade) dataset configuration.

SIM contains death/mortality records from death certificates.
Data files are prefixed with "DO" (Declaracao de Obito).

Example: DOSP2023.dbc for Sao Paulo, year 2023
"""

from datasus_etl.datasets.sim.config import SIMConfig
from datasus_etl.datasets.sim.schema import SIM_DUCKDB_SCHEMA

__all__ = ["SIMConfig", "SIM_DUCKDB_SCHEMA"]
