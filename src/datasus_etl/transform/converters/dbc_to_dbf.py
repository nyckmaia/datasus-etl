"""Converter from DBC (compressed) to DBF or CSV format using datasus-dbc library."""

import csv
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Literal, Optional

from tqdm import tqdm

from datasus_etl.config import ConversionConfig
from datasus_etl.exceptions import ConversionError


class DbcToDbfConverter:
    """Converts DBC files to DBF or CSV using datasus-dbc Python library.

    DBC files are compressed DBF files used by DATASUS.
    This converter uses the pure Python datasus-dbc library, eliminating
    the need for TABWIN (Windows-only executable).

    Supports:
    - Single file or directory as input
    - Output formats: DBF (default) or CSV
    """

    def __init__(self, config: ConversionConfig) -> None:
        """Initialize DBC to DBF/CSV converter.

        Args:
            config: Conversion configuration
        """
        self.config = config
        self.logger = logging.getLogger("datasus_etl.DbcToDbfConverter")
        self._converted_count = 0
        self._error_count = 0

    def convert(
        self,
        input_path: Optional[Path] = None,
        output_dir: Optional[Path] = None,
        output_format: Optional[Literal["dbf", "csv"]] = None,
    ) -> dict[str, int]:
        """Convert DBC file(s) to DBF or CSV.

        Accepts a single DBC file or a directory containing DBC files.

        Args:
            input_path: Input file or directory (uses config if None)
            output_dir: Output directory (uses config if None)
            output_format: Output format 'dbf' or 'csv' (uses config if None)

        Returns:
            Dictionary with conversion statistics

        Raises:
            ConversionError: If input path not found
        """
        input_path = input_path or self.config.dbc_dir
        output_dir = output_dir or self.config.dbf_dir
        output_format = output_format or self.config.output_format

        if not input_path.exists():
            raise ConversionError(f"Input path not found: {input_path}")

        # Determine if input is file or directory
        if input_path.is_file():
            return self._convert_single_file(input_path, output_dir, output_format)
        else:
            return self._convert_directory(input_path, output_dir, output_format)

    def convert_directory(
        self, input_dir: Optional[Path] = None, output_dir: Optional[Path] = None
    ) -> dict[str, int]:
        """Convert all DBC files in directory tree to DBF.

        This method is kept for backward compatibility.
        Use convert() for new code.

        Args:
            input_dir: Input directory (uses config if None)
            output_dir: Output directory (uses config if None)

        Returns:
            Dictionary with conversion statistics
        """
        return self.convert(input_dir, output_dir)

    def _convert_single_file(
        self,
        input_file: Path,
        output_dir: Path,
        output_format: Literal["dbf", "csv"],
    ) -> dict[str, int]:
        """Convert a single DBC file.

        Args:
            input_file: Path to DBC file
            output_dir: Output directory
            output_format: Output format ('dbf' or 'csv')

        Returns:
            Dictionary with conversion statistics
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        # Check if output already exists
        extension = ".csv" if output_format == "csv" else ".dbf"
        output_file = output_dir / (input_file.stem + extension)

        if output_file.exists() and not self.config.override:
            self.logger.info(f"Skipped {input_file.name} (already exists)")
            return {"converted": 0, "errors": 0, "skipped": 1}

        success = self._convert_file(input_file, input_file.parent, output_dir, output_format)

        if success:
            return {"converted": 1, "errors": 0, "skipped": 0}
        else:
            return {"converted": 0, "errors": 1, "skipped": 0}

    def _convert_directory(
        self,
        input_dir: Path,
        output_dir: Path,
        output_format: Literal["dbf", "csv"],
    ) -> dict[str, int]:
        """Convert all DBC files in directory tree.

        Args:
            input_dir: Input directory
            output_dir: Output directory
            output_format: Output format ('dbf' or 'csv')

        Returns:
            Dictionary with conversion statistics
        """
        # Find all DBC files
        dbc_files = list(input_dir.rglob("*.dbc")) + list(input_dir.rglob("*.DBC"))

        if not dbc_files:
            self.logger.warning(f"No DBC files found in {input_dir}")
            return {"converted": 0, "errors": 0, "skipped": 0}

        self.logger.info(f"Found {len(dbc_files)} DBC files")

        # Filter files if not overriding
        original_count = len(dbc_files)
        if not self.config.override:
            dbc_files = self._filter_existing(dbc_files, input_dir, output_dir, output_format)

        skipped = original_count - len(dbc_files)

        if not dbc_files:
            self.logger.info("All files already converted")
            return {"converted": 0, "errors": 0, "skipped": skipped}

        # Reset counters
        self._converted_count = 0
        self._error_count = 0

        # Convert in parallel
        max_workers = self.config.max_workers or 4

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._convert_file, dbc_file, input_dir, output_dir, output_format
                ): dbc_file
                for dbc_file in dbc_files
            }

            format_label = "CSV" if output_format == "csv" else "DBF"
            with tqdm(total=len(dbc_files), desc=f"Converting DBC→{format_label}") as pbar:
                for future in as_completed(futures):
                    try:
                        success = future.result()
                        if success:
                            self._converted_count += 1
                        else:
                            self._error_count += 1
                    except Exception as e:
                        self._error_count += 1
                        dbc_file = futures[future]
                        self.logger.error(f"Error converting {dbc_file.name}: {e}")
                    finally:
                        pbar.update(1)

        return {
            "converted": self._converted_count,
            "errors": self._error_count,
            "skipped": skipped,
        }

    def _convert_file(
        self,
        dbc_file: Path,
        input_root: Path,
        output_root: Path,
        output_format: Literal["dbf", "csv"] = "dbf",
    ) -> bool:
        """Convert a single DBC file to DBF or CSV.

        Args:
            dbc_file: Path to DBC file
            input_root: Input root directory
            output_root: Output root directory
            output_format: Output format ('dbf' or 'csv')

        Returns:
            True if successful, False otherwise
        """
        try:
            import datasus_dbc

            # Calculate output path
            rel_path = dbc_file.relative_to(input_root)
            output_dir = output_root / rel_path.parent
            output_dir.mkdir(parents=True, exist_ok=True)

            if output_format == "csv":
                return self._convert_to_csv(dbc_file, output_dir, datasus_dbc)
            else:
                return self._convert_to_dbf(dbc_file, output_dir, datasus_dbc)

        except Exception as e:
            self.logger.error(f"Error converting {dbc_file.name}: {e}")
            return False

    def _convert_to_dbf(self, dbc_file: Path, output_dir: Path, datasus_dbc: object) -> bool:
        """Convert DBC to DBF format.

        Args:
            dbc_file: Path to DBC file
            output_dir: Output directory
            datasus_dbc: The datasus_dbc module

        Returns:
            True if successful, False otherwise
        """
        output_file = output_dir / (dbc_file.stem + ".dbf")
        datasus_dbc.decompress(str(dbc_file), str(output_file))
        self.logger.debug(f"Converted {dbc_file.name} → {output_file.name}")
        return True

    def _convert_to_csv(self, dbc_file: Path, output_dir: Path, datasus_dbc: object) -> bool:
        """Convert DBC to CSV format.

        First decompresses DBC to DBF, then converts DBF to CSV using dbfread.

        Args:
            dbc_file: Path to DBC file
            output_dir: Output directory
            datasus_dbc: The datasus_dbc module

        Returns:
            True if successful, False otherwise
        """
        import tempfile
        from dbfread import DBF

        # Create temporary DBF file
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dbf = Path(temp_dir) / (dbc_file.stem + ".dbf")

            # Decompress DBC to temporary DBF
            datasus_dbc.decompress(str(dbc_file), str(temp_dbf))

            # Read DBF and write CSV
            output_file = output_dir / (dbc_file.stem + ".csv")

            dbf = DBF(str(temp_dbf), encoding='latin-1', ignore_missing_memofile=True)

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)

                # Write header
                writer.writerow(dbf.field_names)

                # Write rows
                for record in dbf:
                    writer.writerow(record.values())

        self.logger.debug(f"Converted {dbc_file.name} → {output_file.name}")
        return True

    def _filter_existing(
        self,
        dbc_files: list[Path],
        input_root: Path,
        output_root: Path,
        output_format: Literal["dbf", "csv"] = "dbf",
    ) -> list[Path]:
        """Filter out files that already have output.

        Args:
            dbc_files: List of DBC files
            input_root: Input root directory
            output_root: Output root directory
            output_format: Output format to check

        Returns:
            Filtered list of DBC files
        """
        extension = ".csv" if output_format == "csv" else ".dbf"
        filtered = []

        for dbc_file in dbc_files:
            rel_path = dbc_file.relative_to(input_root)
            output_file = output_root / rel_path.parent / (dbc_file.stem + extension)

            if not output_file.exists():
                filtered.append(dbc_file)

        return filtered
