"""IBGE data enricher for geographic information.

.. deprecated:: 1.1.0
    This module is deprecated and will be removed in v2.0.
    IBGE enrichment is now integrated into
    :class:`~pydatasus.storage.sql_transformer.SQLTransformer`.
"""

import logging
import warnings
from pathlib import Path
from typing import Optional

import polars as pl

from pydatasus.exceptions import PyInmetError


class IbgeEnricher:
    """Enrich SIHSUS data with IBGE geographic information.

    .. deprecated:: 1.1.0
        IBGE enrichment is now handled by
        :class:`~pydatasus.storage.sql_transformer.SQLTransformer`.
        The new approach:

        - Integrated into pipeline (no separate enrichment step)
        - SQL LEFT JOIN (more efficient than Polars join)
        - Processed in a single pass with other transformations
        - Better memory efficiency

        This class will be removed in v2.0.

    Adds municipality names, state codes, and regional information
    based on IBGE municipality codes.
    """

    def __init__(
        self,
        ibge_data_path: Optional[Path] = None,
        override: bool = False,
    ) -> None:
        """Initialize the enricher.

        Args:
            ibge_data_path: Path to IBGE municipality data CSV (optional)
            override: Override existing enriched files

        .. deprecated:: 1.1.0
            Use SihsusPipeline with ibge_data_path parameter instead.
        """
        warnings.warn(
            "IbgeEnricher is deprecated and will be removed in v2.0. "
            "IBGE enrichment is now integrated into the optimized SihsusPipeline. "
            "Pass ibge_data_path to SihsusPipeline instead of using this class directly. "
            "See examples/basic_usage.py for migration guide.",
            DeprecationWarning,
            stacklevel=2
        )
        self.ibge_data_path = ibge_data_path
        self.override = override
        self.logger = logging.getLogger(__name__)
        self._ibge_df: Optional[pl.DataFrame] = None

    def load_ibge_data(self) -> None:
        """Load IBGE municipality data.

        If no data path is provided, creates a basic mapping from code to UF.
        """
        if self.ibge_data_path and self.ibge_data_path.exists():
            self.logger.info(f"Loading IBGE data from {self.ibge_data_path}")
            self._ibge_df = pl.read_csv(self.ibge_data_path)
        else:
            # Create basic IBGE data with UF codes from municipality codes
            self.logger.info("Creating basic IBGE mapping from municipality codes")
            self._ibge_df = self._create_basic_ibge_mapping()

    def _create_basic_ibge_mapping(self) -> pl.DataFrame:
        """Create basic IBGE municipality to UF mapping.

        Returns:
            DataFrame with IBGE code to UF mapping
        """
        # IBGE municipality codes: first 2 digits = UF code
        uf_codes = {
            "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
            "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
            "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
            "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
            "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
            "52": "GO", "53": "DF",
        }

        data = []
        for uf_code, uf in uf_codes.items():
            # Generate sample municipalities for each UF
            # In production, this would load real IBGE data
            for i in range(1, 100):
                mun_code = f"{uf_code}{i:05d}"
                data.append({
                    "cod_municipio": mun_code,
                    "uf": uf,
                    "nome_municipio": f"Município {i} - {uf}",
                    "regiao": self._get_region(uf),
                })

        return pl.DataFrame(data)

    def _get_region(self, uf: str) -> str:
        """Get Brazilian region from UF code.

        Args:
            uf: State code

        Returns:
            Region name
        """
        regions = {
            "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
            "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
            "Centro-Oeste": ["DF", "GO", "MT", "MS"],
            "Sudeste": ["ES", "MG", "RJ", "SP"],
            "Sul": ["PR", "RS", "SC"],
        }

        for region, ufs in regions.items():
            if uf in ufs:
                return region

        return "Desconhecido"

    def enrich_file(self, input_file: Path, output_file: Path) -> Path:
        """Enrich a single CSV file with IBGE data.

        Args:
            input_file: Path to input CSV file
            output_file: Path to output enriched CSV file

        Returns:
            Path to enriched file

        Raises:
            PyInmetError: If enrichment fails
        """
        # Skip if already enriched
        if output_file.exists() and not self.override:
            self.logger.debug(f"Skipping (already enriched): {input_file.name}")
            return output_file

        self.logger.info(f"Enriching: {input_file.name}")

        try:
            # Ensure IBGE data is loaded
            if self._ibge_df is None:
                self.load_ibge_data()

            # Read input file
            df = pl.read_csv(input_file, separator=";", encoding="utf-8")

            # Enrich with IBGE data
            df = self._enrich_dataframe(df)

            # Write enriched file
            df.write_csv(output_file, separator=";")

            self.logger.info(f"Enriched: {output_file.name} ({len(df):,} rows)")
            return output_file

        except Exception as e:
            self.logger.error(f"Failed to enrich {input_file}: {e}")
            raise PyInmetError(f"Enrichment failed for {input_file}: {e}") from e

    def _enrich_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Enrich dataframe with IBGE data.

        Args:
            df: Input dataframe

        Returns:
            Enriched dataframe
        """
        if self._ibge_df is None:
            raise PyInmetError("IBGE data not loaded")

        # Find municipality code column
        munic_cols = [col for col in df.columns if "MUNIC" in col.upper()]

        if not munic_cols:
            self.logger.warning("No municipality column found, skipping enrichment")
            return df

        munic_col = munic_cols[0]
        self.logger.debug(f"Using municipality column: {munic_col}")

        # Ensure municipality code is string with proper padding
        df = df.with_columns(
            pl.col(munic_col).cast(pl.Utf8).str.zfill(7).alias(f"{munic_col}_CODE")
        )

        # Join with IBGE data
        df = df.join(
            self._ibge_df,
            left_on=f"{munic_col}_CODE",
            right_on="cod_municipio",
            how="left",
        )

        # Extract UF from municipality code if not in IBGE data
        if "uf" not in df.columns and f"{munic_col}_CODE" in df.columns:
            df = df.with_columns(
                pl.col(f"{munic_col}_CODE").str.slice(0, 2).alias("uf_codigo")
            )

        return df

    def enrich_directory(
        self,
        input_dir: Path,
        output_dir: Path,
        skip_errors: bool = True,
    ) -> dict[str, int]:
        """Enrich all CSV files in a directory.

        Args:
            input_dir: Input directory with CSV files
            output_dir: Output directory for enriched files
            skip_errors: Skip files that fail enrichment

        Returns:
            Statistics dict with counts
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        csv_files = list(input_dir.glob("*.csv"))

        if not csv_files:
            self.logger.warning(f"No CSV files found in {input_dir}")
            return {"total": 0, "enriched": 0, "skipped": 0, "failed": 0}

        self.logger.info(f"Found {len(csv_files)} CSV files to enrich")

        # Load IBGE data once
        self.load_ibge_data()

        stats = {"total": len(csv_files), "enriched": 0, "skipped": 0, "failed": 0}

        for csv_file in csv_files:
            output_file = output_dir / csv_file.name

            # Check if already enriched
            if output_file.exists() and not self.override:
                stats["skipped"] += 1
                continue

            try:
                self.enrich_file(csv_file, output_file)
                stats["enriched"] += 1
            except Exception as e:
                stats["failed"] += 1
                if not skip_errors:
                    raise
                self.logger.error(f"Skipped {csv_file.name}: {e}")

        self.logger.info(
            f"Enrichment complete: {stats['enriched']} enriched, "
            f"{stats['skipped']} skipped, {stats['failed']} failed"
        )

        return stats

    def __repr__(self) -> str:
        """String representation."""
        return f"IbgeEnricher(ibge_data_path={self.ibge_data_path})"
