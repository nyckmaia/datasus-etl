"""PyDataSUS - Pipeline profissional para dados do DATASUS.

PyDataSUS é um pacote Python para download, conversão e processamento
de dados do DATASUS (Sistema de Informações Hospitalares do SUS).
"""

from datasus_etl.__version__ import __version__
from datasus_etl.config import PipelineConfig
from datasus_etl.pipeline.sihsus_pipeline import SihsusPipeline

__all__ = [
    "__version__",
    "PipelineConfig",
    "SihsusPipeline",
]
