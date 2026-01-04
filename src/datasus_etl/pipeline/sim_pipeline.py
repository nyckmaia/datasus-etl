"""SIM (Sistema de Informacoes sobre Mortalidade) data processing pipeline.

This pipeline processes death/mortality records from the Brazilian
SIM system. It downloads DBC files from DATASUS FTP, converts them
to DBF, loads into DuckDB, applies SQL transformations, and exports
to persistent DuckDB database.

SIM data includes:
- Death certificates (Declaracao de Obito - DO)
- Cause of death (ICD-10 coding)
- Demographics of deceased
- Location of death and residence
- Maternal/fetal death information

Note: SIM files are organized by year (not monthly like SIHSUS).
File naming pattern: DOUFYYYY.dbc (e.g., DOSP2023.dbc)
"""

from pathlib import Path

from datasus_etl.config import PipelineConfig
from datasus_etl.datasets.sim.schema import SIM_DUCKDB_SCHEMA
from datasus_etl.pipeline.base_pipeline import DatasusPipeline


class SIMPipeline(DatasusPipeline):
    """Pipeline for SIM (Sistema de Informacoes sobre Mortalidade) data.

    Inherits from DatasusPipeline and provides SIM-specific configuration.

    Example:
        ```python
        from datasus_etl.config import PipelineConfig
        from datasus_etl.pipeline import SIMPipeline

        config = PipelineConfig.create(
            base_dir="./data/datasus",
            subsystem="sim",
            start_date="2020-01-01",
            end_date="2023-12-31",
            uf_list=["SP", "RJ"],
        )

        pipeline = SIMPipeline(config)
        result = pipeline.run()
        ```
    """

    def __init__(self, config: PipelineConfig, ibge_data_path: Path = None) -> None:
        """Initialize the SIM pipeline.

        Args:
            config: Complete pipeline configuration
            ibge_data_path: Optional path to IBGE data file for enrichment
        """
        super().__init__(config, ibge_data_path)

    @property
    def subsystem_name(self) -> str:
        """Return subsystem name."""
        return "sim"

    @property
    def schema(self) -> dict[str, str]:
        """Return DuckDB schema for SIM."""
        return SIM_DUCKDB_SCHEMA
