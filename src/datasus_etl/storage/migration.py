"""One-time migration for the legacy double-nested ``datasus_db/datasus_db/`` layout.

Older revisions of the tool accidentally wrote parquet files to
``{data_dir}/datasus_db/datasus_db/{subsystem}/uf=XX/*.parquet`` because two
independent code paths both prepended a ``datasus_db/`` segment. The path
resolver in :mod:`datasus_etl.storage.paths` fixes the root cause going
forward, but existing data on disk still needs to be hoisted up one level.

:func:`detect_legacy_layout` is a cheap read-only probe suitable to call on
every CLI startup; :func:`migrate_legacy_layout` performs the actual move.
Conflicts are never overwritten — they are returned for the caller to report.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

_LEGACY_INNER = "datasus_db"


@dataclass
class MigrationReport:
    """Outcome of a single migration attempt."""

    base_dir: Path
    legacy_root: Path
    target_root: Path
    subsystems_migrated: list[str] = field(default_factory=list)
    files_moved: int = 0
    conflicts: list[Path] = field(default_factory=list)

    @property
    def needed(self) -> bool:
        """True if a legacy layout was detected (moves happened or would happen)."""
        return self.legacy_root.exists() or self.files_moved > 0


def detect_legacy_layout(base_dir: str | Path) -> Path | None:
    """Return the legacy nested root if one exists, else ``None``.

    The legacy path is ``{base_dir}/datasus_db/datasus_db``. The function does
    not touch the filesystem beyond a single ``is_dir`` stat call.
    """
    candidate = Path(base_dir) / _LEGACY_INNER / _LEGACY_INNER
    return candidate if candidate.is_dir() else None


def migrate_legacy_layout(base_dir: str | Path, *, dry_run: bool = False) -> MigrationReport:
    """Move ``{base_dir}/datasus_db/datasus_db/*`` up to ``{base_dir}/datasus_db/*``.

    Per-subsystem directories are merged: files are moved into the existing
    target tree file-by-file. If a destination file already exists, it is
    recorded as a conflict and skipped — the legacy copy is left in place for
    the user to resolve manually.

    Args:
        base_dir: The user's ``--data-dir`` (the parent of ``datasus_db/``).
        dry_run: If True, no filesystem changes are made; the report still
            reflects what *would* be moved.

    Returns:
        A :class:`MigrationReport` summarising the work.
    """
    base = Path(base_dir)
    legacy = base / _LEGACY_INNER / _LEGACY_INNER
    target = base / _LEGACY_INNER

    report = MigrationReport(base_dir=base, legacy_root=legacy, target_root=target)
    if not legacy.is_dir():
        return report

    for sub_dir in sorted(legacy.iterdir()):
        if not sub_dir.is_dir():
            continue
        report.subsystems_migrated.append(sub_dir.name)
        dest_sub = target / sub_dir.name

        for src in sub_dir.rglob("*"):
            if src.is_dir():
                continue
            rel = src.relative_to(sub_dir)
            dst = dest_sub / rel
            if dst.exists():
                report.conflicts.append(dst)
                continue
            if not dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dst))
            report.files_moved += 1

        if not dry_run:
            # Remove the now-empty subsystem dir (best-effort).
            _remove_if_empty(sub_dir)

    if not dry_run:
        _remove_if_empty(legacy)

    return report


def _remove_if_empty(path: Path) -> None:
    """Remove ``path`` if it has no remaining files, recursing into subdirs.

    Empty directories left behind after the move are cleaned up. Non-empty
    directories (conflicts, other files) are preserved.
    """
    if not path.is_dir():
        return
    # Remove empty subdirectories bottom-up.
    for child in sorted(path.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if child.is_dir() and not any(child.iterdir()):
            try:
                child.rmdir()
            except OSError:
                pass
    if not any(path.iterdir()):
        try:
            path.rmdir()
        except OSError as exc:
            logger.debug("Could not remove empty legacy dir %s: %s", path, exc)
