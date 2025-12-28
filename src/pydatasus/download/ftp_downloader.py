"""FTP downloader for DATASUS/SIHSUS data."""

import datetime
import logging
from ftplib import FTP
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from pydatasus.config import DownloadConfig
from pydatasus.constants import ALL_UFS, DATASUS_FTP_HOST, SIHSUS_DIRS
from pydatasus.exceptions import DownloadError


class FTPDownloader:
    """Downloads SIHSUS data from DATASUS FTP server.

    Downloads RD*.dbc files (hospital admission records) from two historical folders:
    - 199201_200712: Data from 1992-01 to 2007-12
    - 200801_: Data from 2008-01 onwards
    """

    def __init__(self, config: DownloadConfig) -> None:
        """Initialize FTP downloader.

        Args:
            config: Download configuration
        """
        self.config = config
        self.logger = logging.getLogger("pydatasus.FTPDownloader")
        self.ftp_host = DATASUS_FTP_HOST
        self._downloaded_files: list[Path] = []
        self._skipped_files: list[Path] = []
        self._failed_files: list[tuple[str, str]] = []

    def download(self) -> list[Path]:
        """Download SIHSUS files from FTP.

        Returns:
            List of downloaded file paths

        Raises:
            DownloadError: If download fails critically
        """
        self.logger.info("Starting SIHSUS download from DATASUS FTP")

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
        selected_files = self._collect_files_from_ftp(start_date, end_date, uf_list)

        if not selected_files:
            self.logger.warning("No files found matching criteria")
            return []

        self.logger.info(f"Found {len(selected_files)} files to download")

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

        Returns:
            List of tuples (ftp_path, filename, uf, size)
        """
        selected_files = []

        for dir_info in SIHSUS_DIRS:
            try:
                self.logger.debug(f"Connecting to FTP: {dir_info['path']}")
                ftp = FTP(self.ftp_host, timeout=self.config.timeout)
                ftp.login()
                ftp.cwd(dir_info["path"])

                files = ftp.nlst()

                for filename in files:
                    # Filter RD*.dbc files
                    if not (filename.startswith("RD") and filename.lower().endswith(".dbc")):
                        continue

                    # Extract UF and date from filename (RDxxYYMM.dbc)
                    try:
                        uf = filename[2:4]
                        yy = int(filename[4:6])
                        mm = int(filename[6:8])

                        if mm < 1 or mm > 12:
                            continue

                        # Interpret year based on directory
                        if "199201_200712" in dir_info["path"]:
                            year = 1900 + yy if yy >= 92 else 2000 + yy
                        else:
                            year = 2000 + yy

                        file_date = datetime.datetime(year, mm, 1)

                    except (ValueError, IndexError):
                        continue

                    # Check if within date range and directory range
                    if not (start_date <= file_date <= end_date):
                        continue

                    if not (
                        dir_info["start_year"]
                        <= file_date.year
                        <= dir_info.get("end_year", 9999)
                    ):
                        continue

                    # Check UF
                    if uf not in uf_list:
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

        with tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc="Downloading SIHSUS",
        ) as pbar:
            for ftp_path, filename, uf, size in files:
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
                        continue

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
