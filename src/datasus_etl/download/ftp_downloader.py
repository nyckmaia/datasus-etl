"""FTP downloader for DATASUS data (SIHSUS, SIM, etc)."""

import datetime
import logging
from ftplib import FTP
from pathlib import Path
from typing import Callable, Optional

from tqdm import tqdm

from datasus_etl.config import DownloadConfig
from datasus_etl.constants import ALL_UFS, DATASUS_FTP_HOST
from datasus_etl.datasets import DatasetRegistry
from datasus_etl.exceptions import DownloadError

# Messages starting with this tag are surfaced by the web UI as a
# "Preparing download" state (no determinate progress bar yet).
PREPARE_TAG = "[prepare] "

ProgressCallback = Callable[[float, str], None]


class FTPDownloader:
    """Downloads data from DATASUS FTP server for any supported subsystem.

    Supports multiple subsystems (SIHSUS, SIM, etc) by using the DatasetRegistry
    to get subsystem-specific FTP directories, file prefixes, and filename parsing.
    """

    def __init__(
        self,
        config: DownloadConfig,
        subsystem: str = "sihsus",
        progress_callback: Optional[ProgressCallback] = None,
    ) -> None:
        """Initialize FTP downloader.

        Args:
            config: Download configuration
            subsystem: Subsystem name (sihsus, sim). Default: sihsus for backward compatibility.
            progress_callback: Optional ``(progress, message)`` callback invoked
                during FTP listing and per-file downloads. ``progress`` is 0-1
                *within the download stage*; messages tagged with
                :data:`PREPARE_TAG` mark the pre-download listing phase.
        """
        self.config = config
        self.subsystem = subsystem.lower()
        self.logger = logging.getLogger("datasus_etl.FTPDownloader")
        self.ftp_host = DATASUS_FTP_HOST
        self._downloaded_files: list[Path] = []
        self._skipped_files: list[Path] = []
        self._failed_files: list[tuple[str, str]] = []
        self._progress_callback = progress_callback

        # Get dataset configuration from registry
        self._dataset_config = DatasetRegistry.get(self.subsystem)
        if not self._dataset_config:
            raise ValueError(
                f"Unknown subsystem: {subsystem}. "
                f"Available: {DatasetRegistry.list_available()}"
            )

    def _report(self, progress: float, message: str) -> None:
        if self._progress_callback is not None:
            try:
                self._progress_callback(progress, message)
            except Exception:  # noqa: BLE001 — callbacks must never break downloads
                self.logger.debug("Progress callback raised", exc_info=True)

    def download(self) -> list[Path]:
        """Download files from FTP.

        Returns:
            List of downloaded file paths

        Raises:
            DownloadError: If download fails critically
        """
        self.logger.info(f"Starting {self.subsystem.upper()} download from DATASUS FTP")

        # Parse dates
        start_date = datetime.datetime.strptime(self.config.start_date, "%Y-%m-%d")
        end_date = (
            datetime.datetime.strptime(self.config.end_date, "%Y-%m-%d")
            if self.config.end_date
            else datetime.datetime.now()
        )

        if end_date < start_date:
            raise DownloadError("end_date must be >= start_date")

        # Ensure output directory
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

        # Get UF list
        uf_list = self.config.uf_list or ALL_UFS

        self.logger.info(
            f"Downloading data from {start_date.date()} to {end_date.date()} "
            f"for {len(uf_list)} states"
        )

        # Collect files from both FTP directories
        self._report(0.0, f"{PREPARE_TAG}Connecting to DATASUS FTP…")
        selected_files = self._collect_files_from_ftp(start_date, end_date, uf_list)

        if not selected_files:
            self.logger.warning("No files found matching criteria")
            self._report(0.0, f"{PREPARE_TAG}No files matched the selected scope.")
            return []

        self.logger.info(f"Found {len(selected_files)} files to download")
        total_mb = sum(size for _, _, _, size in selected_files) / (1024 * 1024)
        self._report(
            0.0,
            f"{PREPARE_TAG}Found {len(selected_files)} files ({total_mb:,.1f} MB). Starting downloads…",
        )

        # Download files
        self._download_files(selected_files)

        # Summary
        self.logger.info(
            f"Download complete: {len(self._downloaded_files)} downloaded, "
            f"{len(self._skipped_files)} skipped, "
            f"{len(self._failed_files)} failed"
        )

        return self._downloaded_files

    def _collect_files_from_ftp(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        uf_list: list[str],
    ) -> list[tuple[str, str, str, int]]:
        """Collect files from FTP directories.

        Uses DatasetRegistry to get subsystem-specific FTP directories,
        file prefixes, and filename parsing logic.

        Returns:
            List of tuples (ftp_path, filename, uf, size)
        """
        selected_files = []
        ftp_dirs = self._dataset_config.get_ftp_dirs()

        for idx, dir_info in enumerate(ftp_dirs, start=1):
            ftp = None
            try:
                self.logger.debug(f"Connecting to FTP: {dir_info['path']}")
                self._report(
                    0.0,
                    f"{PREPARE_TAG}Listing {dir_info['path']} "
                    f"(directory {idx}/{len(ftp_dirs)})…",
                )
                ftp = FTP(self.ftp_host, timeout=self.config.timeout)
                ftp.login()
                ftp.cwd(dir_info["path"])

                files = ftp.nlst()
                self._report(
                    0.0,
                    f"{PREPARE_TAG}{len(files)} entries in {dir_info['path']}; "
                    "filtering by scope…",
                )

                for filename in files:
                    # Filter by file extension
                    if not filename.lower().endswith(".dbc"):
                        continue

                    # Use DatasetRegistry to parse filename
                    parsed = self._dataset_config.parse_filename(filename)
                    if not parsed:
                        continue

                    # Check UF
                    uf = parsed.get("uf", "")
                    if uf not in uf_list:
                        continue

                    # Build file date for range checking
                    year = parsed.get("year")
                    month = parsed.get("month") or 1  # Default to January for yearly datasets
                    if not year:
                        continue

                    try:
                        file_date = datetime.datetime(year, month, 1)
                    except (ValueError, TypeError):
                        continue

                    # Check if within date range
                    if not (start_date <= file_date <= end_date):
                        continue

                    # Check if within directory's year range
                    if not (
                        dir_info["start_year"]
                        <= year
                        <= dir_info.get("end_year", 9999)
                    ):
                        continue

                    # Check incremental filter (if set, only download specified files)
                    if self.config.incremental_files is not None:
                        if filename.upper() not in {f.upper() for f in self.config.incremental_files}:
                            continue

                    # Get file size
                    try:
                        size = ftp.size(filename) or 0
                    except Exception:
                        size = 0

                    selected_files.append((dir_info["path"], filename, uf, int(size)))

                ftp.quit()

            except Exception as e:
                self.logger.error(f"Error accessing {dir_info['path']}: {e}")
                if ftp:
                    try:
                        ftp.quit()
                    except Exception:
                        pass

        return selected_files

    def _download_files(
        self, files: list[tuple[str, str, str, int]]
    ) -> None:
        """Download files with progress bar.

        Args:
            files: List of (ftp_path, filename, uf, size)
        """
        total_size = sum(size for _, _, _, size in files)
        total_count = len(files)
        bytes_done = 0

        def _report_file(index: int, filename: str, uf: str, size: int, action: str) -> None:
            # Bytes-based progress when total_size is known, else file-count.
            done = bytes_done if total_size > 0 else index
            total = total_size if total_size > 0 else total_count
            frac = (done / total) if total > 0 else 0.0
            size_mb = size / (1024 * 1024) if size else 0.0
            self._report(
                frac,
                f"{action} {filename} (UF={uf}, {size_mb:.1f} MB) — "
                f"{index}/{total_count}",
            )

        with tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Downloading {self.subsystem.upper()}",
        ) as pbar:
            for i, (ftp_path, filename, uf, size) in enumerate(files, start=1):
                # Setup output path
                uf_dir = self.config.output_dir / uf
                uf_dir.mkdir(parents=True, exist_ok=True)
                local_path = uf_dir / filename

                # Skip if exists and not overriding
                if not self.config.override and local_path.exists():
                    local_size = local_path.stat().st_size
                    if local_size == size and size > 0:
                        self._skipped_files.append(local_path)
                        pbar.update(size)
                        bytes_done += size
                        _report_file(i, filename, uf, size, "Skipped")
                        continue

                _report_file(i, filename, uf, size, "Downloading")

                # Download file
                retry_count = 0
                while retry_count < self.config.max_retries:
                    try:
                        ftp = FTP(self.ftp_host, timeout=self.config.timeout)
                        ftp.login()
                        ftp.cwd(ftp_path)

                        with open(local_path, "wb") as f:

                            def callback(data: bytes) -> None:
                                f.write(data)
                                pbar.update(len(data))

                            ftp.retrbinary(f"RETR {filename}", callback)

                        ftp.quit()
                        self._downloaded_files.append(local_path)
                        bytes_done += size
                        _report_file(i, filename, uf, size, "Downloaded")
                        break

                    except Exception as e:
                        retry_count += 1
                        self.logger.warning(
                            f"Error downloading {filename} (attempt {retry_count}): {e}"
                        )
                        if retry_count >= self.config.max_retries:
                            self._failed_files.append((filename, str(e)))

                        try:
                            ftp.quit()
                        except Exception:
                            pass

    @property
    def summary(self) -> dict[str, int]:
        """Get download summary.

        Returns:
            Dictionary with statistics
        """
        return {
            "downloaded": len(self._downloaded_files),
            "skipped": len(self._skipped_files),
            "failed": len(self._failed_files),
        }

    def get_file_info(self) -> dict[str, object]:
        """Get information about files available for download without downloading.

        Returns:
            Dictionary with file information:
            - file_count: Number of files
            - files: List of (filename, uf, size_bytes)
            - total_size_bytes: Total size in bytes
            - estimated_duckdb_bytes: Estimated DuckDB size (~60% of DBC)
            - estimated_csv_bytes: Estimated CSV size (~300% of DBC)
        """
        self.logger.info("Fetching file information from DATASUS FTP")

        # Parse dates
        start_date = datetime.datetime.strptime(self.config.start_date, "%Y-%m-%d")
        end_date = (
            datetime.datetime.strptime(self.config.end_date, "%Y-%m-%d")
            if self.config.end_date
            else datetime.datetime.now()
        )

        if end_date < start_date:
            raise DownloadError("end_date must be >= start_date")

        # Get UF list
        uf_list = self.config.uf_list or ALL_UFS

        # Collect files from FTP
        files = self._collect_files_from_ftp(start_date, end_date, uf_list)

        # Calculate totals
        total_size = sum(size for _, _, _, size in files)
        file_list = [(filename, uf, size) for _, filename, uf, size in files]

        return {
            "file_count": len(files),
            "files": file_list,
            "total_size_bytes": total_size,
            "estimated_duckdb_bytes": int(total_size * 0.6),
            "estimated_csv_bytes": int(total_size * 3.0),
        }
