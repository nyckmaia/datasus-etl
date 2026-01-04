"""Data exporter for various output formats."""

import logging
from pathlib import Path
from typing import Optional, Union

import duckdb
import polars as pl

from datasus_etl.exceptions import PyInmetError


class DataExporter:
    """Export data to various formats (CSV, JSON) from DuckDB databases.

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

    def export_duckdb_to_csv(
        self,
        db_path: Union[str, Path],
        output_file: Path,
        table_name: Optional[str] = None,
        query: Optional[str] = None,
        delimiter: str = ";",
    ) -> Path:
        """Export DuckDB table or query results to CSV.

        Args:
            db_path: Path to DuckDB database file
            output_file: Output CSV file path
            table_name: Table name to export (uses enriched VIEW by default)
            query: Custom SQL query (overrides table_name if provided)
            delimiter: CSV delimiter

        Returns:
            Path to exported file

        Raises:
            PyInmetError: If export fails
        """
        db_path = Path(db_path)
        self.logger.info(f"Exporting DuckDB to CSV: {db_path} -> {output_file}")

        try:
            conn = duckdb.connect(str(db_path), read_only=True)

            try:
                # Determine what to export
                if query:
                    sql = query
                elif table_name:
                    sql = f"SELECT * FROM {table_name}"
                else:
                    # Default to enriched VIEW (subsystem name derived from db file)
                    subsystem = db_path.stem
                    sql = f"SELECT * FROM {subsystem}"

                # Ensure parent directory exists
                output_file.parent.mkdir(parents=True, exist_ok=True)

                # Export directly from DuckDB
                conn.execute(f"""
                    COPY ({sql})
                    TO '{output_file}' (
                        FORMAT CSV,
                        HEADER true,
                        DELIMITER '{delimiter}'
                    )
                """)

                file_size = output_file.stat().st_size
                self.logger.info(f"Exported to {output_file} ({file_size / 1024:.2f} KB)")

                return output_file

            finally:
                conn.close()

        except Exception as e:
            self.logger.error(f"DuckDB to CSV export failed: {e}")
            raise PyInmetError(f"Failed to export DuckDB to CSV: {e}") from e

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
        db_path: Union[str, Path],
        output_file: Path,
        n_rows: int = 1000,
        delimiter: str = ";",
        table_name: Optional[str] = None,
    ) -> Path:
        """Export sample of data from DuckDB to CSV.

        Args:
            db_path: Path to DuckDB database file
            output_file: Output CSV file path
            n_rows: Number of rows to sample
            delimiter: CSV delimiter
            table_name: Table name (uses enriched VIEW by default)

        Returns:
            Path to exported file

        Raises:
            PyInmetError: If export fails
        """
        db_path = Path(db_path)
        self.logger.info(f"Exporting {n_rows} sample rows to {output_file}")

        try:
            # Determine table name
            if not table_name:
                table_name = db_path.stem  # Default to enriched VIEW

            query = f"SELECT * FROM {table_name} USING SAMPLE {n_rows} ROWS"
            return self.export_duckdb_to_csv(db_path, output_file, query=query, delimiter=delimiter)

        except Exception as e:
            self.logger.error(f"Sample export failed: {e}")
            raise PyInmetError(f"Failed to export sample: {e}") from e

    def export_summary(
        self,
        db_path: Union[str, Path],
        output_file: Path,
        table_name: Optional[str] = None,
    ) -> Path:
        """Export data summary statistics to CSV.

        Args:
            db_path: Path to DuckDB database file
            output_file: Output CSV file path
            table_name: Table name (uses raw table by default)

        Returns:
            Path to exported file

        Raises:
            PyInmetError: If export fails
        """
        db_path = Path(db_path)
        self.logger.info(f"Generating summary statistics for {db_path}")

        try:
            conn = duckdb.connect(str(db_path), read_only=True)

            try:
                # Determine table name
                if not table_name:
                    table_name = f"{db_path.stem}_raw"

                # Get summary using DuckDB's SUMMARIZE
                result = conn.execute(f"SUMMARIZE {table_name}").pl()

                # Export to CSV
                return self.export_to_csv(result, output_file)

            finally:
                conn.close()

        except Exception as e:
            self.logger.error(f"Summary export failed: {e}")
            raise PyInmetError(f"Failed to export summary: {e}") from e

    def __repr__(self) -> str:
        """String representation."""
        return "DataExporter()"
