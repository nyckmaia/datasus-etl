"""SIM (Sistema de Informacoes sobre Mortalidade) dataset configuration.

SIM contains death/mortality records from death certificates.
Data files are prefixed with "DO" (Declaracao de Obito).

Example: DOSP2023.dbc for Sao Paulo, year 2023
"""

from pydatasus.datasets.sim.config import SIMConfig
from pydatasus.datasets.sim.schema import SIM_PARQUET_SCHEMA

__all__ = ["SIMConfig", "SIM_PARQUET_SCHEMA"]
