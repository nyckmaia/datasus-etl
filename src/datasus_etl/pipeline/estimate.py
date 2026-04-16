"""Pure-function download estimator — shared by the CLI and the web API.

Wraps :class:`FTPDownloader.get_file_info` so callers don't have to know about
``DownloadConfig`` internals.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from datasus_etl.config import DownloadConfig
from datasus_etl.download.ftp_downloader import FTPDownloader


@dataclass
class EstimateResult:
    subsystem: str
    file_count: int
    total_download_bytes: int
    estimated_duckdb_bytes: int
    estimated_csv_bytes: int


def estimate_download(
    subsystem: str,
    start_date: str,
    end_date: str | None = None,
    ufs: list[str] | None = None,
) -> EstimateResult:
    """Ask the FTP server how much data matches the request.

    Args:
        subsystem: ``sihsus``, ``sim``, ``siasus``, …
        start_date: ``YYYY-MM-DD`` lower bound.
        end_date: ``YYYY-MM-DD`` upper bound, defaults to today.
        ufs: Optional list of two-letter state codes.

    Returns:
        An :class:`EstimateResult` with the totals.

    Raises:
        ValueError: If ``subsystem`` is not recognised.
    """
    cfg = DownloadConfig(
        output_dir=Path("."),  # unused for estimation
        start_date=start_date,
        end_date=end_date,
        uf_list=ufs,
    )
    downloader = FTPDownloader(cfg)
    info = downloader.get_file_info()

    total = int(info["total_size_bytes"])
    duckdb_bytes = int(info.get("estimated_duckdb_bytes", total * 0.6))
    csv_bytes = int(info["estimated_csv_bytes"])

    return EstimateResult(
        subsystem=subsystem.lower(),
        file_count=int(info["file_count"]),
        total_download_bytes=total,
        estimated_duckdb_bytes=duckdb_bytes,
        estimated_csv_bytes=csv_bytes,
    )
