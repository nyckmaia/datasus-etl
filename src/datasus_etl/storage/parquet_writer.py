"""Parquet writer for efficient data storage."""

import logging
from pathlib import Path
from typing import Optional

import polars as pl

from datasus_etl.config import StorageConfig
from datasus_etl.exceptions import PyInmetError


class ParquetWriter:
    """Write SIHSUS data to partitioned Parquet files.

    Provides efficient columnar storage with compression and partitioning
    for optimal query performance.
    """

    def __init__(self, config: StorageConfig) -> None:
        """Initialize the writer.

        Args:
            config: Storage configuration
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Create output directory
        self.config.parquet_dir.mkdir(parents=True, exist_ok=True)

    def write_csv_to_parquet(
        self,
        csv_file: Path,
        table_name: Optional[str] = None,
    ) -> Path:
        """Convert CSV file to partitioned Parquet.

        Args:
            csv_file: Path to input CSV file
            table_name: Optional table name (defaults to filename stem)

        Returns:
            Path to parquet directory

        Raises:
            PyInmetError: If conversion fails
        """
        if table_name is None:
            table_name = csv_file.stem

        output_dir = self.config.parquet_dir / table_name

        self.logger.info(f"Converting {csv_file.name} to Parquet")

        try:
            # Read CSV
            df = pl.read_csv(csv_file, separator=";", encoding="utf-8")

            # Ensure partition columns exist
            df = self._ensure_partition_columns(df)

            # Write partitioned parquet
            self._write_partitioned(df, output_dir)

            self.logger.info(
                f"Wrote Parquet: {output_dir.name} ({len(df):,} rows, "
                f"{df.estimated_size('mb'):.2f} MB)"
            )

            return output_dir

        except Exception as e:
            self.logger.error(f"Failed to convert {csv_file}: {e}")
            raise PyInmetError(f"Parquet conversion failed for {csv_file}: {e}") from e

    def write_dataframe_to_parquet(
        self,
        df: pl.DataFrame,
        table_name: str,
    ) -> Path:
        """Write DataFrame to partitioned Parquet.

        Args:
            df: Polars DataFrame
            table_name: Table name for output directory

        Returns:
            Path to parquet directory

        Raises:
            PyInmetError: If write fails
        """
        output_dir = self.config.parquet_dir / table_name

        self.logger.info(f"Writing DataFrame to Parquet: {table_name}")

        try:
            # Ensure partition columns exist
            df = self._ensure_partition_columns(df)

            # Write partitioned parquet
            self._write_partitioned(df, output_dir)

            self.logger.info(
                f"Wrote Parquet: {table_name} ({len(df):,} rows, "
                f"{df.estimated_size('mb'):.2f} MB)"
            )

            return output_dir

        except Exception as e:
            self.logger.error(f"Failed to write DataFrame: {e}")
            raise PyInmetError(f"Parquet write failed for {table_name}: {e}") from e

    def convert_directory(
        self,
        input_dir: Path,
        skip_errors: bool = True,
    ) -> dict[str, int]:
        """Convert all CSV files in directory to Parquet.

        Args:
            input_dir: Directory with CSV files
            skip_errors: Skip files that fail conversion

        Returns:
            Statistics dict with counts
        """
        csv_files = list(input_dir.glob("*.csv"))

        if not csv_files:
            self.logger.warning(f"No CSV files found in {input_dir}")
            return {"total": 0, "converted": 0, "failed": 0}

        self.logger.info(f"Found {len(csv_files)} CSV files to convert")

        stats = {"total": len(csv_files), "converted": 0, "failed": 0}

        for csv_file in csv_files:
            try:
                self.write_csv_to_parquet(csv_file)
                stats["converted"] += 1
            except Exception as e:
                stats["failed"] += 1
                if not skip_errors:
                    raise
                self.logger.error(f"Skipped {csv_file.name}: {e}")

        self.logger.info(
            f"Conversion complete: {stats['converted']} converted, "
            f"{stats['failed']} failed"
        )

        return stats

    def _ensure_partition_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Ensure partition columns exist in dataframe.

        Args:
            df: Input dataframe

        Returns:
            Dataframe with partition columns
        """
        # Add partition columns if they don't exist
        for col in self.config.partition_cols:
            if col not in df.columns:
                # Try to derive from other columns
                if col == "uf" and "UF_ZI" in df.columns:
                    df = df.with_columns(pl.col("UF_ZI").alias("uf"))
                elif col == "year" and "ANO_INTER" in df.columns:
                    df = df.with_columns(pl.col("ANO_INTER").alias("year"))
                elif col == "month" and "MES_INTER" in df.columns:
                    df = df.with_columns(pl.col("MES_INTER").alias("month"))
                elif col == "year" and "DT_INTER" in df.columns:
                    df = df.with_columns(pl.col("DT_INTER").dt.year().alias("year"))
                elif col == "month" and "DT_INTER" in df.columns:
                    df = df.with_columns(pl.col("DT_INTER").dt.month().alias("month"))
                else:
                    # Add default value if can't derive
                    df = df.with_columns(pl.lit(None).alias(col))

        return df

    def _write_partitioned(self, df: pl.DataFrame, output_dir: Path) -> None:
        """Write dataframe as partitioned Parquet.

        Args:
            df: Dataframe to write
            output_dir: Output directory
        """
        # Get valid partition columns (only those that exist in df)
        valid_partition_cols = [
            col for col in self.config.partition_cols if col in df.columns
        ]

        if valid_partition_cols:
            # Write with partitioning
            df.write_parquet(
                output_dir,
                compression=self.config.compression,
                use_pyarrow=True,
                pyarrow_options={
                    "partition_cols": valid_partition_cols,
                    "max_rows_per_group": self.config.row_group_size,
                },
            )
        else:
            # Write without partitioning
            self.logger.warning(
                f"No valid partition columns found in {valid_partition_cols}, "
                "writing unpartitioned"
            )
            output_file = output_dir / "data.parquet"
            output_file.parent.mkdir(parents=True, exist_ok=True)

            df.write_parquet(
                output_file,
                compression=self.config.compression,
                use_pyarrow=True,
                row_group_size=self.config.row_group_size,
            )

    def __repr__(self) -> str:
        """String representation."""
        return f"ParquetWriter(output_dir={self.config.parquet_dir})"
