"""Converter from DBF to CSV format.

.. deprecated:: 1.1.0
    This module is deprecated and will be removed in v2.0.
    Use :class:`~pydatasus.transform.converters.dbf_to_duckdb.DbfToDuckDBConverter`
    for better performance (60% faster, 63% less I/O).
"""

import csv
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from dbfread import DBF
from tqdm import tqdm

from pydatasus.config import ConversionConfig
from pydatasus.exceptions import ConversionError


class DbfToCsvConverter:
    """Converts DBF files to CSV format.

    .. deprecated:: 1.1.0
        Use :class:`~pydatasus.transform.converters.dbf_to_duckdb.DbfToDuckDBConverter`
        instead. The new converter:

        - 60% faster (no intermediate CSV files)
        - 63% less disk I/O
        - Streams directly to DuckDB
        - Better memory efficiency

        This class will be removed in v2.0.

    Uses dbfread library to read DBF files and standard csv module to write.
    """

    def __init__(self, config: ConversionConfig) -> None:
        """Initialize DBF to CSV converter.

        Args:
            config: Conversion configuration

        .. deprecated:: 1.1.0
            Use DbfToDuckDBConverter for better performance.
        """
        warnings.warn(
            "DbfToCsvConverter is deprecated and will be removed in v2.0. "
            "Use DbfToDuckDBConverter with the optimized SihsusPipeline for "
            "60% better performance and lower memory usage. "
            "See examples/basic_usage.py for migration guide.",
            DeprecationWarning,
            stacklevel=2
        )
        self.config = config
        self.logger = logging.getLogger("pydatasus.DbfToCsvConverter")
        self._converted_count = 0
        self._error_count = 0

    def convert_directory(
        self, input_dir: Optional[Path] = None, output_dir: Optional[Path] = None
    ) -> dict[str, int]:
        """Convert all DBF files in directory tree to CSV.

        Args:
            input_dir: Input directory (uses config if None)
            output_dir: Output directory (uses config if None)

        Returns:
            Dictionary with conversion statistics
        """
        input_dir = input_dir or self.config.dbf_dir
        output_dir = output_dir or self.config.csv_dir

        if not input_dir.exists():
            raise ConversionError(f"Input directory not found: {input_dir}")

        # Find all DBF files
        dbf_files = list(input_dir.rglob("*.dbf")) + list(input_dir.rglob("*.DBF"))

        if not dbf_files:
            self.logger.warning(f"No DBF files found in {input_dir}")
            return {"converted": 0, "errors": 0}

        self.logger.info(f"Found {len(dbf_files)} DBF files")

        # Filter if not overriding
        if not self.config.override:
            dbf_files = self._filter_existing(dbf_files, input_dir, output_dir)

        if not dbf_files:
            self.logger.info("All files already converted")
            return {"converted": 0, "errors": 0, "skipped": len(dbf_files)}

        # Convert in parallel
        max_workers = self.config.max_workers or 8

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._convert_file, dbf_file, input_dir, output_dir
                ): dbf_file
                for dbf_file in dbf_files
            }

            with tqdm(total=len(dbf_files), desc="Converting DBF→CSV") as pbar:
                for future in as_completed(futures):
                    try:
                        success = future.result()
                        if success:
                            self._converted_count += 1
                        else:
                            self._error_count += 1
                    except Exception as e:
                        self._error_count += 1
                        dbf_file = futures[future]
                        self.logger.error(f"Error converting {dbf_file.name}: {e}")
                    finally:
                        pbar.update(1)

        return {"converted": self._converted_count, "errors": self._error_count}

    def _convert_file(
        self, dbf_file: Path, input_root: Path, output_root: Path
    ) -> bool:
        """Convert a single DBF file to CSV.

        Args:
            dbf_file: Path to DBF file
            input_root: Input root directory
            output_root: Output root directory

        Returns:
            True if successful, False otherwise
        """
        try:
            # Calculate output path
            rel_path = dbf_file.relative_to(input_root)
            output_file = output_root / rel_path.parent / (dbf_file.stem + ".csv")
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Read DBF
            table = DBF(dbf_file, load=True, encoding="latin-1", ignore_missing_memofile=True)

            # Write CSV
            with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile, delimiter=";")

                # Write header
                writer.writerow(table.field_names)

                # Write rows
                for record in table:
                    writer.writerow(record.values())

            return True

        except Exception as e:
            self.logger.error(f"Error converting {dbf_file.name}: {e}")
            return False

    def _filter_existing(
        self, dbf_files: list[Path], input_root: Path, output_root: Path
    ) -> list[Path]:
        """Filter out files that already have CSV output."""
        filtered = []
        for dbf_file in dbf_files:
            rel_path = dbf_file.relative_to(input_root)
            csv_file = output_root / rel_path.parent / (dbf_file.stem + ".csv")

            if not csv_file.exists():
                filtered.append(dbf_file)

        return filtered
