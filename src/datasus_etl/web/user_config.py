"""Persistent user configuration for the web UI.

Stored at ``~/.config/datasus-etl/config.toml`` (respecting ``XDG_CONFIG_HOME``).
Currently stores only ``data_dir``, but the format is forward-compatible.
"""

from __future__ import annotations

import os
import sys
import tomllib
from dataclasses import dataclass, asdict
from pathlib import Path

import tomli_w


DEFAULT_HISTORY_SIZE_K = 2  # 2K = 2000 queries per subsystem
MIN_HISTORY_SIZE_K = 1
MAX_HISTORY_SIZE_K = 100


@dataclass
class UserConfig:
    """User-scoped settings persisted across sessions."""

    data_dir: str | None = None
    history_size_k: int = DEFAULT_HISTORY_SIZE_K


def config_path() -> Path:
    """Return the path where user config lives (creates parents on write)."""
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "datasus-etl" / "config.toml"


def _coerce_history_size_k(value: object) -> int:
    try:
        n = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return DEFAULT_HISTORY_SIZE_K
    if n < MIN_HISTORY_SIZE_K:
        return MIN_HISTORY_SIZE_K
    if n > MAX_HISTORY_SIZE_K:
        return MAX_HISTORY_SIZE_K
    return n


def load() -> UserConfig:
    """Read user config, returning defaults if the file is missing or invalid."""
    path = config_path()
    if not path.is_file():
        return UserConfig()
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return UserConfig()
    return UserConfig(
        data_dir=data.get("data_dir"),
        history_size_k=_coerce_history_size_k(
            data.get("history_size_k", DEFAULT_HISTORY_SIZE_K)
        ),
    )


def save(cfg: UserConfig) -> Path:
    """Persist ``cfg`` atomically. Returns the config path."""
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {k: v for k, v in asdict(cfg).items() if v is not None}
    payload["history_size_k"] = _coerce_history_size_k(
        payload.get("history_size_k", DEFAULT_HISTORY_SIZE_K)
    )
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(tomli_w.dumps(payload).encode("utf-8"))
    tmp.replace(path)
    return path


def python_version() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
