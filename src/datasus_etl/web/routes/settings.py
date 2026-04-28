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
from datasus_etl.storage.paths import resolve_parquet_dir, resolve_storage_root
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
    # Windows 10 throws WinError 6 (invalid handle) when capture_output is set
    # but stdin is left to inherit from a parent process that has no console
    # (the Nuitka-built binary launched from Explorer). DEVNULL gives the
    # child a valid handle in every case. CREATE_NO_WINDOW also suppresses a
    # cmd.exe flash on Windows. Other platforms ignore creationflags.
    spawn_kwargs: dict = {
        "capture_output": True,
        "text": True,
        "timeout": 120,
        "stdin": subprocess.DEVNULL,
    }
    if sys.platform == "win32":
        spawn_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

    try:
        proc = await asyncio.to_thread(
            subprocess.run,
            _pick_folder_cmd(),
            **spawn_kwargs,
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


# ──────────────────────────────────────────────────────────────────────────
# Danger zone — destructive deletion of the per-subsystem parquet folders
# ──────────────────────────────────────────────────────────────────────────


class ResetStorageRequest(BaseModel):
    # Lowercase subsystem names (e.g. "sihsus", "sim") + the special value
    # "ibge" for the IBGE reference parquet. The frontend gates this call
    # behind a 4-digit confirmation code typed by the user; the server stays
    # defensive about which paths it will rmtree (see _validate_target).
    subsystems: list[str] = Field(..., min_length=1)


class ResetStorageItem(BaseModel):
    name: str
    path: str | None = None
    freed_bytes: int = 0
    skipped_reason: str | None = None


class ResetStorageResponse(BaseModel):
    deleted: list[ResetStorageItem]
    skipped: list[ResetStorageItem]


def _allowed_subsystem_names() -> set[str]:
    # IBGE is not a registered DatasetConfig (it's reference data, not an
    # ETL pipeline), but it lives under datasus_db/ibge/ and the user can
    # opt to wipe it from the same UI. Everything else must come from the
    # registry so we never invent a path the rest of the app can't see.
    return set(DatasetRegistry.list_available()) | {"ibge"}


def _resolve_target_for(name: str, data_dir: Path, storage_root: Path) -> Path | None:
    """Map a subsystem name to its on-disk folder, or None if outside root.

    The resolved path is checked to live strictly inside ``storage_root``
    (which is itself ``resolve_storage_root(data_dir).resolve()``). This is
    a paranoid path-traversal guard — even though the input names come from
    a fixed allow-list, future code paths that might tweak resolution
    shouldn't be able to point this endpoint at, say, ``/`` or the user's
    home directory.
    """
    if name == "ibge":
        target = storage_root / "ibge"
    else:
        target = resolve_parquet_dir(data_dir, name)
    try:
        resolved = target.resolve()
    except OSError:
        return None
    try:
        resolved.relative_to(storage_root)
    except ValueError:
        return None
    return resolved


def _dir_size_bytes(path: Path) -> int:
    total = 0
    for f in path.rglob("*"):
        try:
            if f.is_file():
                total += f.stat().st_size
        except OSError:
            continue
    return total


@router.post("/reset-storage", response_model=ResetStorageResponse)
async def reset_storage(
    payload: ResetStorageRequest, request: Request
) -> ResetStorageResponse:
    """Delete the per-subsystem parquet folders the user selected.

    Irreversible. The frontend confirms intent with a typed-back 4-digit
    code; this endpoint only enforces structural safety:

    * a data dir must be configured;
    * each name must be on the allow-list (registered subsystems + "ibge");
    * each resolved target must live inside the configured storage root;
    * non-existent targets are reported as skipped, not 500'd.
    """
    data_dir = _resolve_data_dir(request)
    if data_dir is None:
        raise HTTPException(
            status_code=400, detail="No data directory configured."
        )
    storage_root = resolve_storage_root(data_dir).resolve()
    if not storage_root.is_dir():
        raise HTTPException(
            status_code=400,
            detail=f"Storage root does not exist: {storage_root}",
        )

    allowed = _allowed_subsystem_names()
    deleted: list[ResetStorageItem] = []
    skipped: list[ResetStorageItem] = []

    seen: set[str] = set()
    for raw_name in payload.subsystems:
        name = raw_name.strip().lower()
        if name in seen:
            continue
        seen.add(name)

        if name not in allowed:
            skipped.append(
                ResetStorageItem(name=name, skipped_reason="unknown subsystem")
            )
            continue

        target = _resolve_target_for(name, data_dir, storage_root)
        if target is None:
            skipped.append(
                ResetStorageItem(name=name, skipped_reason="outside storage root")
            )
            continue

        if not target.exists():
            skipped.append(
                ResetStorageItem(
                    name=name, path=str(target), skipped_reason="no data on disk"
                )
            )
            continue

        freed = _dir_size_bytes(target)
        try:
            shutil.rmtree(target)
        except OSError as exc:
            skipped.append(
                ResetStorageItem(
                    name=name,
                    path=str(target),
                    skipped_reason=f"could not delete: {exc}",
                )
            )
            continue

        deleted.append(
            ResetStorageItem(name=name, path=str(target), freed_bytes=freed)
        )

    return ResetStorageResponse(deleted=deleted, skipped=skipped)
