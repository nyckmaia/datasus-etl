"""Single source of truth for parquet storage paths.

Historically, both `config.PipelineConfig.get_parquet_dir()` and
`ParquetManager.__init__` independently decided whether to prepend a
`datasus_db/` folder segment. When one caller anchored on the user's
`--data-dir` and the other on an already-resolved path, the two answers
diverged and files landed under `{base}/datasus_db/datasus_db/{subsystem}/`.

This module centralises the logic so there is exactly one rule for every
caller. Both `config` and `ParquetManager` delegate here.
"""

from __future__ import annotations

from pathlib import Path

DATA_ROOT_FOLDER = "datasus_db"
LEGACY_FOLDER = "parquet"


def resolve_parquet_dir(base_dir: str | Path, subsystem: str) -> Path:
    """Return the canonical parquet directory for a subsystem.

    Rules (first match wins):

    1. If ``base_dir.name`` already equals ``datasus_db`` or ``parquet``
       (case-insensitive), the user has pointed directly at the storage
       root — just append the subsystem.
    2. If a legacy ``{base_dir}/parquet/{subsystem}`` directory exists,
       return it unchanged (backwards compatibility with the pre-0.1 layout).
    3. Otherwise, return ``{base_dir}/datasus_db/{subsystem}``.

    The subsystem name is always lowercased.

    Args:
        base_dir: The user's configured data directory (``--data-dir``).
        subsystem: DataSUS subsystem name (e.g. ``sihsus``, ``sim``, ``siasus``).

    Returns:
        The absolute-or-relative Path where parquet partitions live.
    """
    base = Path(base_dir)
    sub = subsystem.lower()

    if base.name.lower() in (DATA_ROOT_FOLDER, LEGACY_FOLDER):
        return base / sub

    legacy = base / LEGACY_FOLDER / sub
    if legacy.exists():
        return legacy

    return base / DATA_ROOT_FOLDER / sub
