"""Data exporter for various output formats."""

import logging
from pathlib import Path
from typing import Optional

import polars as pl

from pydatasus.exceptions import PyInmetError


class DataExporter:
    """Export data to various formats (CSV, Excel, JSON).

    Provides flexible data export capabilities with customizable
    options for delimiters, encoding, and format-specific settings.
    """

    def __init__(self) -> None:
        """Initialize the exporter."""
        self.logger = logging.getLogger(__name__)

    def export_to_csv(
        self,
        df: pl.DataFrame,
        output_file: Path,
        delimiter: str = ";",
        include_header: bool = True,
    ) -> Path:
        """Export DataFrame to CSV.

        Args:
            df: Polars DataFrame to export
            output_file: Output file path
            delimiter: CSV delimiter
            include_header: Include header row

        Returns:
            Path to exported file

        Raises:
            PyInmetError: If export fails
        """
        self.logger.info(f"Exporting to CSV: {output_file}")

        try:
            # Ensure parent directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Write CSV
            df.write_csv(
                output_file,
                separator=delimiter,
                include_header=include_header,
            )

            self.logger.info(
                f"Exported {len(df):,} rows to {output_file} "
                f"({output_file.stat().st_size / 1024:.2f} KB)"
            )

            return output_file

        except Exception as e:
            self.logger.error(f"CSV export failed: {e}")
            raise PyInmetError(f"Failed to export to CSV: {e}") from e

    def export_parquet_to_csv(
        self,
        parquet_path: Path,
        output_file: Path,
        delimiter: str = ";",
        filter_expr: Optional[str] = None,
    ) -> Path:
        """Export Parquet file(s) to CSV.

        Args:
            parquet_path: Path to Parquet file or directory
            output_file: Output CSV file path
            delimiter: CSV delimiter
            filter_expr: Optional Polars filter expression

        Returns:
            Path to exported file

        Raises:
            PyInmetError: If export fails
        """
        self.logger.info(f"Exporting Parquet to CSV: {parquet_path} -> {output_file}")

        try:
            # Read Parquet
            if parquet_path.is_file():
                df = pl.read_parquet(parquet_path)
            else:
                # Read all parquet files in directory
                parquet_files = list(parquet_path.glob("**/*.parquet"))
                if not parquet_files:
                    raise PyInmetError(f"No Parquet files found in {parquet_path}")

                df = pl.concat([pl.read_parquet(f) for f in parquet_files])

            # Apply filter if provided
            if filter_expr:
                df = df.filter(filter_expr)

            # Export to CSV
            return self.export_to_csv(df, output_file, delimiter)

        except Exception as e:
            self.logger.error(f"Parquet to CSV export failed: {e}")
            raise PyInmetError(f"Failed to export Parquet to CSV: {e}") from e

    def export_to_json(
        self,
        df: pl.DataFrame,
        output_file: Path,
        orient: str = "records",
    ) -> Path:
        """Export DataFrame to JSON.

        Args:
            df: Polars DataFrame to export
            output_file: Output file path
            orient: JSON orientation ('records', 'columns', etc.)

        Returns:
            Path to exported file

        Raises:
            PyInmetError: If export fails
        """
        self.logger.info(f"Exporting to JSON: {output_file}")

        try:
            # Ensure parent directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Write JSON
            with open(output_file, "w", encoding="utf-8") as f:
                if orient == "records":
                    f.write(df.write_json(row_oriented=True))
                else:
                    f.write(df.write_json(row_oriented=False))

            self.logger.info(
                f"Exported {len(df):,} rows to {output_file} "
                f"({output_file.stat().st_size / 1024:.2f} KB)"
            )

            return output_file

        except Exception as e:
            self.logger.error(f"JSON export failed: {e}")
            raise PyInmetError(f"Failed to export to JSON: {e}") from e

    def export_sample(
        self,
        parquet_path: Path,
        output_file: Path,
        n_rows: int = 1000,
        delimiter: str = ";",
    ) -> Path:
        """Export sample of data from Parquet to CSV.

        Args:
            parquet_path: Path to Parquet file or directory
            output_file: Output CSV file path
            n_rows: Number of rows to sample
            delimiter: CSV delimiter

        Returns:
            Path to exported file

        Raises:
            PyInmetError: If export fails
        """
        self.logger.info(f"Exporting {n_rows} sample rows to {output_file}")

        try:
            # Read Parquet (just sample)
            if parquet_path.is_file():
                df = pl.read_parquet(parquet_path, n_rows=n_rows)
            else:
                # Read from first available file
                parquet_files = list(parquet_path.glob("**/*.parquet"))
                if not parquet_files:
                    raise PyInmetError(f"No Parquet files found in {parquet_path}")

                df = pl.read_parquet(parquet_files[0], n_rows=n_rows)

            # Export to CSV
            return self.export_to_csv(df, output_file, delimiter)

        except Exception as e:
            self.logger.error(f"Sample export failed: {e}")
            raise PyInmetError(f"Failed to export sample: {e}") from e

    def export_summary(
        self,
        parquet_path: Path,
        output_file: Path,
    ) -> Path:
        """Export data summary statistics to CSV.

        Args:
            parquet_path: Path to Parquet file or directory
            output_file: Output CSV file path

        Returns:
            Path to exported file

        Raises:
            PyInmetError: If export fails
        """
        self.logger.info(f"Generating summary statistics for {parquet_path}")

        try:
            # Read Parquet
            if parquet_path.is_file():
                df = pl.read_parquet(parquet_path)
            else:
                parquet_files = list(parquet_path.glob("**/*.parquet"))
                if not parquet_files:
                    raise PyInmetError(f"No Parquet files found in {parquet_path}")

                df = pl.concat([pl.read_parquet(f) for f in parquet_files])

            # Generate summary statistics
            summary = df.describe()

            # Export to CSV
            return self.export_to_csv(summary, output_file)

        except Exception as e:
            self.logger.error(f"Summary export failed: {e}")
            raise PyInmetError(f"Failed to export summary: {e}") from e

    def __repr__(self) -> str:
        """String representation."""
        return "DataExporter()"
