"""Pipeline orchestration endpoints.

- ``POST /api/pipeline/estimate``        — ask the FTP how much data matches.
- ``POST /api/pipeline/start``           — launch a download; returns ``run_id``.
- ``GET  /api/pipeline/progress/{id}``   — SSE stream of progress events.
- ``POST /api/pipeline/cancel/{id}``     — best-effort cancel.
- ``GET  /api/pipeline/runs``            — list known runs.
- ``GET  /api/pipeline/runs/{id}``       — run status snapshot.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from datasus_etl.datasets.base import DatasetRegistry
from datasus_etl.pipeline.estimate import estimate_download
from datasus_etl.web.runtime import REGISTRY, Run

from .settings import _resolve_data_dir

router = APIRouter()


class EstimateRequest(BaseModel):
    subsystem: str = Field(..., min_length=2)
    start_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    end_date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    ufs: list[str] | None = None


class UfEstimate(BaseModel):
    uf: str
    file_count: int
    download_bytes: int
    storage_bytes: int
    ftp_first_period: str | None
    ftp_last_period: str | None


class EstimateResponse(BaseModel):
    subsystem: str
    file_count: int
    total_download_bytes: int
    estimated_duckdb_bytes: int
    estimated_csv_bytes: int
    per_uf: list[UfEstimate] = []


class StartRequest(EstimateRequest):
    override: bool = False


class StartResponse(BaseModel):
    run_id: str


class RunSnapshot(BaseModel):
    id: str
    subsystem: str
    status: str
    progress: float
    message: str
    started_at: str
    finished_at: str | None


def _snapshot(run: Run) -> RunSnapshot:
    return RunSnapshot(
        id=run.id,
        subsystem=run.subsystem,
        status=run.status,
        progress=run.progress,
        message=run.message,
        started_at=run.started_at,
        finished_at=run.finished_at,
    )


def _check_subsystem(name: str) -> None:
    if DatasetRegistry.get(name) is None:
        raise HTTPException(status_code=400, detail=f"Unknown subsystem: {name}")


@router.post("/estimate", response_model=EstimateResponse)
async def estimate(payload: EstimateRequest) -> EstimateResponse:
    _check_subsystem(payload.subsystem)
    try:
        result = await asyncio.to_thread(
            estimate_download,
            payload.subsystem,
            payload.start_date,
            payload.end_date,
            payload.ufs,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"FTP query failed: {exc}") from exc
    return EstimateResponse(
        subsystem=result.subsystem,
        file_count=result.file_count,
        total_download_bytes=result.total_download_bytes,
        estimated_duckdb_bytes=result.estimated_duckdb_bytes,
        estimated_csv_bytes=result.estimated_csv_bytes,
        per_uf=[
            UfEstimate(
                uf=u.uf,
                file_count=u.file_count,
                download_bytes=u.download_bytes,
                storage_bytes=u.storage_bytes,
                ftp_first_period=u.ftp_first_period,
                ftp_last_period=u.ftp_last_period,
            )
            for u in result.per_uf
        ],
    )


@router.post("/start", response_model=StartResponse)
async def start_run(payload: StartRequest, request: Request) -> StartResponse:
    _check_subsystem(payload.subsystem)
    data_dir = _resolve_data_dir(request)
    if data_dir is None:
        raise HTTPException(
            status_code=400,
            detail="No data directory configured.",
        )
    params: dict[str, Any] = {
        "start_date": payload.start_date,
        "end_date": payload.end_date,
        "ufs": payload.ufs,
        "override": payload.override,
    }
    try:
        run = await REGISTRY.launch(payload.subsystem, params, data_dir)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return StartResponse(run_id=run.id)


@router.post("/cancel/{run_id}")
async def cancel_run(run_id: str) -> dict[str, bool]:
    ok = await REGISTRY.cancel(run_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Run not found or already finished")
    return {"cancelled": True}


@router.get("/runs", response_model=list[RunSnapshot])
async def list_runs() -> list[RunSnapshot]:
    return [_snapshot(r) for r in REGISTRY.all()]


@router.get("/runs/{run_id}", response_model=RunSnapshot)
async def run_snapshot(run_id: str) -> RunSnapshot:
    run = REGISTRY.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return _snapshot(run)


@router.get("/progress/{run_id}")
async def progress_stream(run_id: str, request: Request) -> EventSourceResponse:
    run = REGISTRY.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")

    async def _events():
        yield {"event": "snapshot", "data": json.dumps(_snapshot(run).model_dump())}
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(run.queue.get(), timeout=15.0)
            except asyncio.TimeoutError:
                yield {"event": "ping", "data": ""}
                continue
            yield {"event": event.type, "data": json.dumps(event.as_dict())}
            if event.type in ("done", "error", "cancelled"):
                break

    return EventSourceResponse(_events())
