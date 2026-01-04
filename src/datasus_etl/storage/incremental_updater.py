"""Incremental update support for DataSUS-ETL datasets.

This module provides functionality to update existing DuckDB databases
by processing only new files from the FTP server, avoiding reprocessing
of already-imported data.
"""

import logging
from pathlib import Path
from typing import Optional

from datasus_etl.config import PipelineConfig
from datasus_etl.storage.duckdb_query_engine import DuckDBQueryEngine


class IncrementalUpdater:
    """Manages incremental updates for a DuckDB database.

    Compares source files in existing DuckDB database with files available
    on the FTP server to determine which files need to be processed.

    Example:
        >>> updater = IncrementalUpdater(config)
        >>> new_files = updater.get_new_files()
        >>> print(f"Found {len(new_files)} new files to process")
        >>> if new_files:
        ...     result = updater.update()
    """

    def __init__(
        self,
        config: PipelineConfig,
        database_path: Optional[Path] = None,
    ) -> None:
        """Initialize incremental updater.

        Args:
            config: Pipeline configuration
            database_path: Path to DuckDB database file (default: from config)
        """
        self.config = config
        self.database_path = database_path or config.get_database_path()
        self.logger = logging.getLogger(__name__)

        # Track state
        self._processed_files: set[str] = set()
        self._available_files: set[str] = set()

    def get_processed_files(self) -> set[str]:
        """Get list of files already in DuckDB database.

        Returns:
            Set of source_file values from existing database
        """
        if self._processed_files:
            return self._processed_files

        if not self.database_path.exists():
            self.logger.info("Database doesn't exist yet - full import needed")
            return set()

        try:
            engine = DuckDBQueryEngine(self.database_path, read_only=True)
            self._processed_files = engine.get_processed_source_files()
            engine.close()

            self.logger.info(f"Found {len(self._processed_files)} files already processed")
            return self._processed_files

        except Exception as e:
            self.logger.error(f"Failed to query processed files: {e}")
            return set()

    def get_available_files_from_ftp(self) -> set[str]:
        """Get list of files available on FTP server.

        Returns:
            Set of filenames available on FTP matching date range and UF criteria
        """
        import datetime
        from ftplib import FTP

        from datasus_etl.constants import ALL_UFS, DATASUS_FTP_HOST
        from datasus_etl.datasets import DatasetRegistry

        if self._available_files:
            return self._available_files

        # Get dataset configuration
        dataset_config = DatasetRegistry.get(self.config.subsystem)
        if not dataset_config:
            self.logger.error(f"Unknown subsystem: {self.config.subsystem}")
            return set()

        ftp_dirs = dataset_config.get_ftp_dirs()
        file_prefix = dataset_config.FILE_PREFIX

        # Parse dates from config
        start_date = datetime.datetime.strptime(
            self.config.download.start_date, "%Y-%m-%d"
        )
        end_date = (
            datetime.datetime.strptime(self.config.download.end_date, "%Y-%m-%d")
            if self.config.download.end_date
            else datetime.datetime.now()
        )

        # Get UF list
        uf_list = self.config.download.uf_list or ALL_UFS

        available_files = set()

        for dir_info in ftp_dirs:
            try:
                self.logger.debug(f"Checking FTP: {dir_info['path']}")
                ftp = FTP(DATASUS_FTP_HOST, timeout=30)
                ftp.login()
                ftp.cwd(dir_info["path"])

                files = ftp.nlst()

                for filename in files:
                    # Filter by prefix
                    if not filename.upper().startswith(file_prefix):
                        continue

                    # Parse filename to check date and UF
                    parsed = dataset_config.parse_filename(filename)
                    if not parsed:
                        continue

                    uf = parsed.get("uf")
                    year = parsed.get("year")
                    month = parsed.get("month", 1)  # Default to January for yearly files

                    if uf not in uf_list:
                        continue

                    # Check date range
                    try:
                        file_date = datetime.datetime(year, month, 1)
                        if not (start_date <= file_date <= end_date):
                            continue

                        # Check directory date range
                        if not (
                            dir_info["start_year"]
                            <= year
                            <= dir_info.get("end_year", 9999)
                        ):
                            continue

                        available_files.add(filename.upper())

                    except ValueError:
                        continue

                ftp.quit()

            except Exception as e:
                self.logger.error(f"Error accessing {dir_info['path']}: {e}")
                try:
                    ftp.quit()
                except Exception:
                    pass

        self._available_files = available_files
        self.logger.info(f"Found {len(available_files)} files available on FTP")
        return available_files

    def get_new_files(self) -> set[str]:
        """Get list of new files that need to be downloaded and processed.

        Returns:
            Set of filenames that exist on FTP but not in DuckDB
        """
        processed = self.get_processed_files()
        available = self.get_available_files_from_ftp()

        # Normalize case for comparison (FTP may return different case)
        processed_upper = {f.upper() for f in processed}
        available_upper = {f.upper() for f in available}

        new_files = available_upper - processed_upper

        if new_files:
            self.logger.info(f"Found {len(new_files)} new files to process:")
            for f in sorted(new_files)[:10]:  # Show first 10
                self.logger.info(f"  - {f}")
            if len(new_files) > 10:
                self.logger.info(f"  ... and {len(new_files) - 10} more")
        else:
            self.logger.info("No new files to process - database is up to date")

        return new_files

    def get_update_summary(self) -> dict:
        """Get summary of update status.

        Returns:
            Dictionary with update statistics
        """
        processed = self.get_processed_files()
        available = self.get_available_files_from_ftp()

        processed_upper = {f.upper() for f in processed}
        available_upper = {f.upper() for f in available}

        new_files = available_upper - processed_upper

        return {
            "processed_count": len(processed),
            "available_count": len(available),
            "new_count": len(new_files),
            "new_files": sorted(new_files),
            "is_up_to_date": len(new_files) == 0,
        }

    def create_incremental_config(self) -> Optional[PipelineConfig]:
        """Create a new config for processing only new files.

        Returns:
            Modified PipelineConfig with filter for new files only,
            or None if no update is needed
        """
        new_files = self.get_new_files()

        if not new_files:
            return None

        # Create a copy of the config with incremental settings
        # The download stage will be modified to only download these files
        from copy import deepcopy

        incremental_config = deepcopy(self.config)

        # Store the list of files to download in the config
        # The downloader will use this to filter
        incremental_config.download.incremental_files = new_files

        return incremental_config
