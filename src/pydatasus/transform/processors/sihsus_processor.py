"""SIHSUS data processor for cleaning and validation.

.. deprecated:: 1.1.0
    This module is deprecated and will be removed in v2.0.
    Use :class:`~pydatasus.storage.sql_transformer.SQLTransformer`
    for SQL-based transformations with better performance.
"""

import logging
import warnings
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl

from pydatasus.config import ProcessingConfig
from pydatasus.constants import DATE_FORMATS, RACA_COR_MAP, SEXO_MAP
from pydatasus.exceptions import PyInmetError


class SihsusProcessor:
    """Process and clean SIHSUS CSV files.

    .. deprecated:: 1.1.0
        Use :class:`~pydatasus.storage.sql_transformer.SQLTransformer` instead.
        The new approach:

        - 40% faster (SQL-based transformations in DuckDB)
        - 58% less RAM usage (streaming, no DataFrame materialization)
        - All transformations in a single SQL query
        - Automatic memory management

        This class will be removed in v2.0.

    Handles:
    - Date parsing and validation
    - Type conversions
    - Value mappings (sex, race/color)
    - Data cleaning and validation
    - Column standardization
    """

    def __init__(self, config: ProcessingConfig) -> None:
        """Initialize the processor.

        Args:
            config: Processing configuration

        .. deprecated:: 1.1.0
            Use SQLTransformer with SihsusPipeline for better performance.
        """
        warnings.warn(
            "SihsusProcessor is deprecated and will be removed in v2.0. "
            "Use the optimized SihsusPipeline with SQLTransformer for "
            "40% better performance and lower memory usage. "
            "See examples/basic_usage.py for migration guide.",
            DeprecationWarning,
            stacklevel=2
        )
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Create output directory
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

    def process_file(self, input_file: Path) -> Path:
        """Process a single CSV file.

        Args:
            input_file: Path to input CSV file

        Returns:
            Path to processed CSV file

        Raises:
            PyInmetError: If processing fails
        """
        output_file = self.config.output_dir / input_file.name

        # Skip if already processed
        if output_file.exists() and not self.config.override:
            self.logger.debug(f"Skipping (already processed): {input_file.name}")
            return output_file

        self.logger.info(f"Processing: {input_file.name}")

        try:
            # Read CSV with polars
            df = pl.read_csv(
                input_file,
                separator=";",
                encoding="utf-8",
                null_values=["", " ", "NA", "NULL"],
                ignore_errors=True,
            )

            # Clean and validate
            df = self._clean_dataframe(df)
            df = self._validate_dataframe(df)
            df = self._add_computed_columns(df)

            # Write processed file
            df.write_csv(
                output_file,
                separator=";",
            )

            self.logger.info(f"Processed: {output_file.name} ({len(df):,} rows)")
            return output_file

        except Exception as e:
            self.logger.error(f"Failed to process {input_file}: {e}")
            raise PyInmetError(f"Processing failed for {input_file}: {e}") from e

    def process_directory(self, skip_errors: bool = True) -> dict[str, int]:
        """Process all CSV files in input directory.

        Args:
            skip_errors: Skip files that fail processing

        Returns:
            Statistics dict with counts
        """
        csv_files = list(self.config.input_dir.glob("*.csv"))

        if not csv_files:
            self.logger.warning(f"No CSV files found in {self.config.input_dir}")
            return {"total": 0, "processed": 0, "skipped": 0, "failed": 0}

        self.logger.info(f"Found {len(csv_files)} CSV files to process")

        stats = {"total": len(csv_files), "processed": 0, "skipped": 0, "failed": 0}

        for csv_file in csv_files:
            output_file = self.config.output_dir / csv_file.name

            # Check if already processed
            if output_file.exists() and not self.config.override:
                stats["skipped"] += 1
                continue

            try:
                self.process_file(csv_file)
                stats["processed"] += 1
            except Exception as e:
                stats["failed"] += 1
                if not skip_errors:
                    raise
                self.logger.error(f"Skipped {csv_file.name}: {e}")

        self.logger.info(
            f"Processing complete: {stats['processed']} processed, "
            f"{stats['skipped']} skipped, {stats['failed']} failed"
        )

        return stats

    def _clean_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean the dataframe.

        Args:
            df: Input dataframe

        Returns:
            Cleaned dataframe
        """
        # Standardize column names (uppercase)
        df = df.rename({col: col.upper() for col in df.columns})

        # Remove completely empty rows
        df = df.filter(~pl.all_horizontal(pl.all().is_null()))

        # Strip whitespace from string columns
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                df = df.with_columns(pl.col(col).str.strip_chars())

        return df

    def _validate_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate and transform data types.

        Args:
            df: Input dataframe

        Returns:
            Validated dataframe
        """
        # Parse dates if date columns exist
        date_columns = [col for col in df.columns if "DT_" in col or col.endswith("_DATA")]

        for col in date_columns:
            df = df.with_columns(self._parse_date_column(pl.col(col)).alias(col))

        # Map categorical values
        if "SEXO" in df.columns:
            df = df.with_columns(
                pl.col("SEXO").cast(pl.Utf8).replace(SEXO_MAP).alias("SEXO_DESCR")
            )

        if "RACA_COR" in df.columns:
            df = df.with_columns(
                pl.col("RACA_COR").cast(pl.Utf8).replace(RACA_COR_MAP).alias("RACA_COR_DESCR")
            )

        # Convert numeric columns
        numeric_candidates = [
            "IDADE", "QT_DIARIAS", "VAL_SH", "VAL_SP", "VAL_TOT",
            "VAL_UTI", "DIAS_PERM", "PROC_REA", "VAL_SADT"
        ]

        for col in numeric_candidates:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Float64, strict=False).alias(col)
                )

        return df

    def _parse_date_column(self, col: pl.Expr) -> pl.Expr:
        """Parse date column trying multiple formats.

        Args:
            col: Polars column expression

        Returns:
            Parsed date column
        """
        # Try each date format
        parsed = None
        for fmt in DATE_FORMATS:
            try:
                parsed = pl.col(col.meta.output_name()).str.strptime(pl.Date, fmt, strict=False)
                break
            except Exception:
                continue

        # If all formats fail, return as-is
        return parsed if parsed is not None else col

    def _add_computed_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add computed columns.

        Args:
            df: Input dataframe

        Returns:
            Dataframe with computed columns
        """
        # Extract year/month from admission date if exists
        if "DT_INTER" in df.columns:
            df = df.with_columns([
                pl.col("DT_INTER").dt.year().alias("ANO_INTER"),
                pl.col("DT_INTER").dt.month().alias("MES_INTER"),
            ])

        # Calculate length of stay if dates exist
        if "DT_INTER" in df.columns and "DT_SAIDA" in df.columns:
            df = df.with_columns(
                (pl.col("DT_SAIDA") - pl.col("DT_INTER")).dt.total_days().alias("DIAS_INTERNACAO")
            )

        return df

    def __repr__(self) -> str:
        """String representation."""
        return f"SihsusProcessor(input_dir={self.config.input_dir})"
