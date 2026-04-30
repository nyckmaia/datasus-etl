"""Per-subsystem query history endpoints.

Persisted at ``~/.config/datasus-etl/history/{subsystem}.jsonl`` with a
FIFO ceiling controlled by the ``history_size_k`` user setting.

* ``GET    /api/query/history/{subsystem}`` — newest-first list
* ``POST   /api/query/history/{subsystem}`` — append one entry
* ``DELETE /api/query/history/{subsystem}`` — clear that subsystem's history
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from datasus_etl.datasets.base import DatasetRegistry
from datasus_etl.web import history_store

router = APIRouter()


class HistoryEntry(BaseModel):
    id: str = Field(..., min_length=1, max_length=128)
    sql: str = Field(..., min_length=1)
    ts: int = Field(..., ge=0, description="Unix milliseconds.")
    rows: int = Field(0, ge=0)
    elapsed_ms: float = Field(0.0, ge=0)
    name: str | None = Field(default=None, max_length=200)
    favorite: bool = False


class HistoryEntryPatch(BaseModel):
    name: str | None = Field(default=None, max_length=200)
    favorite: bool | None = None


class HistoryListResponse(BaseModel):
    entries: list[dict[str, Any]]


def _validate_subsystem(name: str) -> str:
    name = name.strip().lower()
    if name not in DatasetRegistry.list_available():
        raise HTTPException(status_code=404, detail=f"unknown subsystem: {name}")
    return name


@router.get("/{subsystem}", response_model=HistoryListResponse)
async def list_history(subsystem: str, limit: int | None = None) -> HistoryListResponse:
    name = _validate_subsystem(subsystem)
    return HistoryListResponse(entries=history_store.read(name, limit))


@router.post("/{subsystem}", response_model=HistoryListResponse)
async def append_history(
    subsystem: str, entry: HistoryEntry
) -> HistoryListResponse:
    name = _validate_subsystem(subsystem)
    history_store.append(name, entry.model_dump())
    return HistoryListResponse(entries=history_store.read(name))


@router.patch("/{subsystem}/{entry_id}", response_model=HistoryListResponse)
async def patch_history_entry(
    subsystem: str, entry_id: str, patch: HistoryEntryPatch
) -> HistoryListResponse:
    name = _validate_subsystem(subsystem)
    # Pydantic .model_dump(exclude_unset=True) keeps only fields the client
    # actually supplied — letting us distinguish "rename to empty string"
    # (clear the name) from "don't touch the name".
    payload = patch.model_dump(exclude_unset=True)
    updated = history_store.update(name, entry_id, payload)
    if updated is None:
        raise HTTPException(status_code=404, detail="history entry not found")
    return HistoryListResponse(entries=history_store.read(name))


@router.delete("/{subsystem}/{entry_id}", response_model=HistoryListResponse)
async def delete_history_entry(
    subsystem: str, entry_id: str
) -> HistoryListResponse:
    name = _validate_subsystem(subsystem)
    if not history_store.remove(name, entry_id):
        raise HTTPException(status_code=404, detail="history entry not found")
    return HistoryListResponse(entries=history_store.read(name))


@router.delete("/{subsystem}", response_model=HistoryListResponse)
async def clear_history(subsystem: str) -> HistoryListResponse:
    name = _validate_subsystem(subsystem)
    history_store.clear(name)
    return HistoryListResponse(entries=[])
