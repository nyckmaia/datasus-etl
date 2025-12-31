"""SIHSUS (Sistema de Informacoes Hospitalares) dataset configuration.

SIHSUS contains hospital admission records (AIH - Autorizacao de Internacao Hospitalar).
Data files are prefixed with "RD" (e.g., RDSP2301.dbc for Sao Paulo, January 2023).
"""

from datasus_etl.datasets.sihsus.config import SIHSUSConfig
from datasus_etl.datasets.sihsus.schema import SIHSUS_PARQUET_SCHEMA

__all__ = ["SIHSUSConfig", "SIHSUS_PARQUET_SCHEMA"]
