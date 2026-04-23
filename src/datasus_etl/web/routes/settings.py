"""User-configurable settings endpoints.

- ``GET  /api/settings``                 — full settings object + environment info.
- ``PUT  /api/settings/data-dir``        — change the active data directory.
- ``POST /api/settings/pick-directory``  — open the OS-native folder picker on
                                           the machine running the server. Only
                                           makes sense for the local
                                           ``datasus ui`` launcher (server and
                                           user share a display).
- ``POST /api/settings/validate-path``   — return path metadata so the UI can
                                           warn the user (will be created, not
                                           writable, etc.) before saving.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import sys
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from datasus_etl import __version__
from datasus_etl.datasets.base import DatasetRegistry
from datasus_etl.storage.paths import resolve_storage_root
from datasus_etl.web import user_config

router = APIRouter()


# Picker runs in a one-shot subprocess so tkinter gets its own main thread —
# avoids the "Tk must run on main thread" failure mode that hits when called
# from inside uvicorn's worker thread (especially on macOS). The subprocess
# is dispatched through the CLI's hidden `_pick-folder` subcommand so the
# same invocation path works under a plain `pip install` (where
# ``sys.executable`` is the system Python) and under a Nuitka-compiled
# binary (where ``sys.executable`` is the packaged executable — Nuitka
# explicitly supports re-invocation with subcommands).


def _pick_folder_cmd() -> list[str]:
    # Under a normal pip install, sys.executable is the Python interpreter —
    # we have to invoke the CLI via ``python -m datasus_etl``. Under a
    # Nuitka-compiled binary, sys.executable IS the CLI, so we invoke it
    # directly with the hidden subcommand.
    exe = Path(sys.executable).stem.lower()
    if exe.startswith("python") or exe == "pypy" or exe.startswith("pypy"):
        return [sys.executable, "-m", "datasus_etl", "_pick-folder"]
    return [sys.executable, "_pick-folder"]


class SubsystemInfo(BaseModel):
    name: str
    description: str
    file_prefix: str


class SettingsResponse(BaseModel):
    data_dir: str | None
    data_dir_resolved: str | None = Field(
        None,
        description="Absolute path where parquet files actually live, after path resolution.",
    )
    free_disk_bytes: int | None = None
    total_disk_bytes: int | None = None
    version: str = __version__
    python_version: str = user_config.python_version()
    subsystems: list[SubsystemInfo]
    config_file: str


class UpdateDataDirRequest(BaseModel):
    data_dir: str = Field(..., min_length=1, description="Absolute or ~ path to data directory.")


class PickDirectoryResponse(BaseModel):
    path: str | None = None
    cancelled: bool = False
    error: str | None = None


class ValidatePathRequest(BaseModel):
    path: str = Field(..., min_length=1)


class ValidatePathResponse(BaseModel):
    normalized: str
    exists: bool
    is_dir: bool
    will_be_created: bool
    writable: bool
    error: str | None = None


def _subsystems() -> list[SubsystemInfo]:
    items: list[SubsystemInfo] = []
    for name, cfg_cls in DatasetRegistry.get_all().items():
        items.append(
            SubsystemInfo(
                name=name,
                description=getattr(cfg_cls, "DESCRIPTION", "") or "",
                file_prefix=getattr(cfg_cls, "FILE_PREFIX", "") or "",
            )
        )
    return sorted(items, key=lambda x: x.name)


def _resolve_data_dir(request: Request) -> Path | None:
    explicit = getattr(request.app.state, "data_dir", None)
    if explicit is not None:
        return Path(explicit)
    persisted = user_config.load().data_dir
    return Path(persisted).expanduser() if persisted else None


def _disk_usage(path: Path) -> tuple[int | None, int | None]:
    probe = path
    while not probe.exists():
        if probe.parent == probe:
            return None, None
        probe = probe.parent
    try:
        du = shutil.disk_usage(probe)
        return du.free, du.total
    except OSError:
        return None, None


@router.get("", response_model=SettingsResponse)
@router.get("/", response_model=SettingsResponse, include_in_schema=False)
async def get_settings(request: Request) -> SettingsResponse:
    data_dir = _resolve_data_dir(request)
    free, total = (None, None)
    resolved: str | None = None
    if data_dir is not None:
        free, total = _disk_usage(data_dir)
        resolved = str(resolve_storage_root(data_dir).resolve()) if data_dir.exists() else None
    return SettingsResponse(
        data_dir=str(data_dir) if data_dir else None,
        data_dir_resolved=resolved,
        free_disk_bytes=free,
        total_disk_bytes=total,
        subsystems=_subsystems(),
        config_file=str(user_config.config_path()),
    )


@router.put("/data-dir", response_model=SettingsResponse)
async def set_data_dir(payload: UpdateDataDirRequest, request: Request) -> SettingsResponse:
    new_dir = Path(payload.data_dir).expanduser()
    try:
        new_dir = new_dir.resolve()
    except OSError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid path: {exc}") from exc

    if new_dir.exists() and not new_dir.is_dir():
        raise HTTPException(
            status_code=400,
            detail=f"Path exists but is not a directory: {new_dir}",
        )
    if not new_dir.exists():
        try:
            new_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise HTTPException(status_code=400, detail=f"Cannot create directory: {exc}") from exc

    user_config.save(user_config.UserConfig(data_dir=str(new_dir)))
    request.app.state.data_dir = new_dir
    return await get_settings(request)


@router.post("/pick-directory", response_model=PickDirectoryResponse)
async def pick_directory() -> PickDirectoryResponse:
    """Open the OS-native folder picker and return the chosen path.

    Spawns a fresh Python interpreter that runs tkinter's ``askdirectory``
    dialog, then prints the chosen path to stdout (empty string if the user
    cancels). The dialog is forced topmost so it surfaces above the browser.

    Assumes the server and user share a display — this endpoint is meant for
    the local ``datasus ui`` launcher and has no use in remote deployments.
    """
    try:
        proc = await asyncio.to_thread(
            subprocess.run,
            _pick_folder_cmd(),
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        return PickDirectoryResponse(error="Folder picker timed out")
    except OSError as exc:
        return PickDirectoryResponse(error=f"Could not launch folder picker: {exc}")

    if proc.returncode == 2 and "TK_MISSING" in proc.stderr:
        return PickDirectoryResponse(
            error=(
                "Native folder picker is unavailable because tkinter is not "
                "installed. On Ubuntu/Debian: sudo apt install python3-tk"
            ),
        )
    if proc.returncode != 0:
        return PickDirectoryResponse(
            error=(proc.stderr.strip() or "Folder picker failed"),
        )

    chosen = proc.stdout.strip()
    if not chosen:
        return PickDirectoryResponse(cancelled=True)
    return PickDirectoryResponse(path=chosen)


@router.post("/validate-path", response_model=ValidatePathResponse)
async def validate_path(payload: ValidatePathRequest) -> ValidatePathResponse:
    """Return metadata about a candidate data-dir path.

    Pure metadata — never enumerates directory contents. Used by the Settings
    page to give the user inline feedback (e.g. "this folder will be created"
    or "not writable") before they click Save.
    """
    raw = payload.path.strip()
    try:
        p = Path(raw).expanduser().resolve(strict=False)
    except (OSError, ValueError) as exc:
        return ValidatePathResponse(
            normalized="",
            exists=False,
            is_dir=False,
            will_be_created=False,
            writable=False,
            error=f"Invalid path: {exc}",
        )

    exists = p.exists()
    is_dir = p.is_dir() if exists else False

    if exists:
        writable = os.access(p, os.W_OK)
    else:
        # Walk up to the first existing ancestor and check its writability —
        # that's the directory mkdir(parents=True) will actually create into.
        ancestor = p.parent
        while not ancestor.exists() and ancestor != ancestor.parent:
            ancestor = ancestor.parent
        writable = ancestor.exists() and os.access(ancestor, os.W_OK)

    return ValidatePathResponse(
        normalized=str(p),
        exists=exists,
        is_dir=is_dir,
        will_be_created=not exists,
        writable=writable,
    )
