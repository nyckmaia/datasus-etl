"""PyDataSUS - Pipeline profissional para dados do DATASUS.

PyDataSUS é um pacote Python para download, conversão e processamento
de dados do DATASUS (Sistema de Informações Hospitalares do SUS).
"""

from pydatasus.__version__ import __version__
from pydatasus.config import PipelineConfig
from pydatasus.pipeline.sihsus_pipeline import SihsusPipeline

__all__ = [
    "__version__",
    "PipelineConfig",
    "SihsusPipeline",
]
