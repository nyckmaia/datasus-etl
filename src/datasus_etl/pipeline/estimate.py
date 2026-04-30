"""Pure-function download estimator — shared by the CLI and the web API.

Wraps :class:`FTPDownloader.get_file_info` so callers don't have to know about
``DownloadConfig`` internals.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from datasus_etl.config import DownloadConfig
from datasus_etl.datasets import DatasetRegistry
from datasus_etl.download.ftp_downloader import FTPDownloader


@dataclass
class UfEstimate:
    uf: str
    file_count: int
    download_bytes: int
    storage_bytes: int  # approx share of the total storage estimate
    ftp_first_period: str | None  # "YYYY-MM"
    ftp_last_period: str | None


@dataclass
class EstimateResult:
    subsystem: str
    file_count: int
    total_download_bytes: int
    estimated_duckdb_bytes: int
    estimated_csv_bytes: int
    per_uf: list[UfEstimate] = field(default_factory=list)


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
        An :class:`EstimateResult` with the totals and per-UF breakdown.

    Raises:
        ValueError: If ``subsystem`` is not recognised.
    """
    cfg = DownloadConfig(
        output_dir=Path("."),  # unused for estimation
        start_date=start_date,
        end_date=end_date,
        uf_list=ufs,
    )
    downloader = FTPDownloader(cfg, subsystem=subsystem)
    info = downloader.get_file_info()

    total = int(info["total_size_bytes"])
    duckdb_bytes = int(info.get("estimated_duckdb_bytes", total * 0.6))
    csv_bytes = int(info["estimated_csv_bytes"])

    # Build per-UF breakdown from the raw file list.
    # Each entry in info["files"] is (filename, uf, size_bytes).
    dataset_cfg = DatasetRegistry.get(subsystem)
    uf_groups: dict[str, list[tuple[str, int]]] = {}
    for filename, uf, size in info.get("files", []):
        if uf not in uf_groups:
            uf_groups[uf] = []
        uf_groups[uf].append((filename, int(size)))

    per_uf_list: list[UfEstimate] = []
    for uf in sorted(uf_groups.keys()):
        group = uf_groups[uf]
        dl_bytes = sum(sz for _, sz in group)
        storage = (
            int(dl_bytes / total * duckdb_bytes) if total > 0 else 0
        )

        # Parse year/month from each filename to find the period range.
        periods: list[tuple[int, int]] = []
        for filename, _ in group:
            parsed = dataset_cfg.parse_filename(filename) if dataset_cfg else None
            if parsed is None:
                continue
            year = parsed.get("year")
            month = parsed.get("month")
            if year is not None and month is not None:
                periods.append((int(year), int(month)))
            elif year is not None:
                # Yearly dataset (e.g. SIM) — no month; use month=1 for ordering
                periods.append((int(year), 1))

        periods.sort()
        ftp_first: str | None = None
        ftp_last: str | None = None
        if periods:
            ftp_first = f"{periods[0][0]:04d}-{periods[0][1]:02d}"
            ftp_last = f"{periods[-1][0]:04d}-{periods[-1][1]:02d}"

        per_uf_list.append(
            UfEstimate(
                uf=uf,
                file_count=len(group),
                download_bytes=dl_bytes,
                storage_bytes=storage,
                ftp_first_period=ftp_first,
                ftp_last_period=ftp_last,
            )
        )

    return EstimateResult(
        subsystem=subsystem.lower(),
        file_count=int(info["file_count"]),
        total_download_bytes=total,
        estimated_duckdb_bytes=duckdb_bytes,
        estimated_csv_bytes=csv_bytes,
        per_uf=per_uf_list,
    )
