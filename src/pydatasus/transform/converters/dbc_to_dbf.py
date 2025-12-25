"""Converter from DBC (compressed) to DBF format using datasus-dbc library."""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from pydatasus.config import ConversionConfig
from pydatasus.exceptions import ConversionError


class DbcToDbfConverter:
    """Converts DBC files to DBF using datasus-dbc Python library.

    DBC files are compressed DBF files used by DATASUS.
    This converter uses the pure Python datasus-dbc library, eliminating
    the need for TABWIN (Windows-only executable).
    """

    def __init__(self, config: ConversionConfig) -> None:
        """Initialize DBC to DBF converter.

        Args:
            config: Conversion configuration
        """
        self.config = config
        self.logger = logging.getLogger("pydatasus.DbcToDbfConverter")
        self._converted_count = 0
        self._error_count = 0

    def convert_directory(
        self, input_dir: Optional[Path] = None, output_dir: Optional[Path] = None
    ) -> dict[str, int]:
        """Convert all DBC files in directory tree to DBF.

        Args:
            input_dir: Input directory (uses config if None)
            output_dir: Output directory (uses config if None)

        Returns:
            Dictionary with conversion statistics

        Raises:
            ConversionError: If input directory not found
        """
        input_dir = input_dir or self.config.dbc_dir
        output_dir = output_dir or self.config.dbf_dir

        if not input_dir.exists():
            raise ConversionError(f"Input directory not found: {input_dir}")

        # Find all DBC files
        dbc_files = list(input_dir.rglob("*.dbc")) + list(input_dir.rglob("*.DBC"))

        if not dbc_files:
            self.logger.warning(f"No DBC files found in {input_dir}")
            return {"converted": 0, "errors": 0, "skipped": 0}

        self.logger.info(f"Found {len(dbc_files)} DBC files")

        # Filter files if not overriding
        if not self.config.override:
            dbc_files = self._filter_existing(dbc_files, input_dir, output_dir)

        if not dbc_files:
            self.logger.info("All files already converted")
            return {"converted": 0, "errors": 0, "skipped": len(dbc_files)}

        # Convert in parallel
        max_workers = self.config.max_workers or 4

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._convert_file, dbc_file, input_dir, output_dir
                ): dbc_file
                for dbc_file in dbc_files
            }

            with tqdm(total=len(dbc_files), desc="Converting DBC→DBF") as pbar:
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
            "skipped": len(dbc_files) if not self.config.override else 0,
        }

    def _convert_file(
        self, dbc_file: Path, input_root: Path, output_root: Path
    ) -> bool:
        """Convert a single DBC file to DBF using datasus-dbc.

        Args:
            dbc_file: Path to DBC file
            input_root: Input root directory
            output_root: Output root directory

        Returns:
            True if successful, False otherwise
        """
        try:
            import datasus_dbc

            # Calculate output path
            rel_path = dbc_file.relative_to(input_root)
            output_dir = output_root / rel_path.parent
            output_dir.mkdir(parents=True, exist_ok=True)

            # Output DBF file (same name, different extension)
            output_file = output_dir / (dbc_file.stem + ".dbf")

            # Decompress DBC to DBF using datasus-dbc library
            datasus_dbc.decompress_file(str(dbc_file), str(output_file))

            self.logger.debug(f"Converted {dbc_file.name} → {output_file.name}")
            return True

        except Exception as e:
            self.logger.error(f"Error converting {dbc_file.name}: {e}")
            return False

    def _filter_existing(
        self, dbc_files: list[Path], input_root: Path, output_root: Path
    ) -> list[Path]:
        """Filter out files that already have DBF output.

        Args:
            dbc_files: List of DBC files
            input_root: Input root directory
            output_root: Output root directory

        Returns:
            Filtered list of DBC files
        """
        filtered = []
        for dbc_file in dbc_files:
            rel_path = dbc_file.relative_to(input_root)
            dbf_file = output_root / rel_path.parent / (dbc_file.stem + ".dbf")

            if not dbf_file.exists():
                filtered.append(dbc_file)

        return filtered
