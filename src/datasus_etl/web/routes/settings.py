"""User-configurable settings endpoints.

- ``GET  /api/settings``            — full settings object + environment info.
- ``PUT  /api/settings/data-dir``   — change the active data directory.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from datasus_etl import __version__
from datasus_etl.datasets.base import DatasetRegistry
from datasus_etl.web import user_config

router = APIRouter()


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
        resolved = str((data_dir / "datasus_db").resolve()) if data_dir.exists() else None
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
