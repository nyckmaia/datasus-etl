"""Per-subsystem persistent query history stored as JSONL.

One file per subsystem at ``~/.config/datasus-etl/history/{subsystem}.jsonl``.
Each line is a JSON object with ``id``, ``sql``, ``ts``, ``rows``, ``elapsed_ms``.

FIFO truncation: when the user appends past the configured ceiling
(``history_size_k * 1000``), the oldest entries are dropped.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from . import user_config


_SUBSYSTEM_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")


def _validate_subsystem(name: str) -> str:
    """Reject anything that could be a path traversal attempt."""
    if not _SUBSYSTEM_RE.match(name):
        raise ValueError(f"invalid subsystem: {name!r}")
    return name


def history_dir() -> Path:
    return user_config.config_path().parent / "history"


def history_file(subsystem: str) -> Path:
    return history_dir() / f"{_validate_subsystem(subsystem)}.jsonl"


def _max_entries() -> int:
    cfg = user_config.load()
    return cfg.history_size_k * 1000


def read(subsystem: str, limit: int | None = None) -> list[dict[str, Any]]:
    """Return entries newest-first."""
    path = history_file(subsystem)
    out = _read_raw(path)
    out.reverse()
    if limit is not None:
        out = out[:limit]
    return out


def _read_raw(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    out: list[dict[str, Any]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def _write_all(path: Path, entries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(json.dumps(e, ensure_ascii=False) for e in entries)
    if payload:
        payload += "\n"
    tmp = path.with_suffix(".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)


def append(subsystem: str, entry: dict[str, Any]) -> None:
    """Append entry and FIFO-truncate to the configured ceiling.

    Favorited entries are preserved across truncation — only non-favorite
    entries (the bulk of the history) participate in the FIFO drop. This
    mirrors the user's mental model: starring a query means "keep this".
    """
    path = history_file(subsystem)
    cap = _max_entries()
    entries = _read_raw(path)
    entries.append(entry)
    if len(entries) > cap:
        # Keep all favorites; drop oldest non-favorites until we're under cap.
        # Stable order: walk from the front (oldest) and drop the first few
        # non-favorite entries needed to reach cap.
        overflow = len(entries) - cap
        kept: list[dict[str, Any]] = []
        dropped = 0
        for e in entries:
            if dropped < overflow and not e.get("favorite"):
                dropped += 1
                continue
            kept.append(e)
        entries = kept
    _write_all(path, entries)


def update(
    subsystem: str, entry_id: str, patch: dict[str, Any]
) -> dict[str, Any] | None:
    """Apply a partial update to one entry by id. Returns the updated entry."""
    path = history_file(subsystem)
    entries = _read_raw(path)
    updated: dict[str, Any] | None = None
    for e in entries:
        if e.get("id") == entry_id:
            for k, v in patch.items():
                if v is None:
                    e.pop(k, None)
                else:
                    e[k] = v
            updated = e
            break
    if updated is not None:
        _write_all(path, entries)
    return updated


def remove(subsystem: str, entry_id: str) -> bool:
    """Remove the entry with the given id. Returns True if it existed."""
    path = history_file(subsystem)
    entries = _read_raw(path)
    new_entries = [e for e in entries if e.get("id") != entry_id]
    if len(new_entries) == len(entries):
        return False
    _write_all(path, new_entries)
    return True


def clear(subsystem: str) -> None:
    path = history_file(subsystem)
    if path.is_file():
        path.unlink()


def clear_all() -> None:
    d = history_dir()
    if not d.is_dir():
        return
    for f in d.glob("*.jsonl"):
        f.unlink()


def known_subsystems() -> Iterable[str]:
    d = history_dir()
    if not d.is_dir():
        return []
    return [f.stem for f in d.glob("*.jsonl")]
