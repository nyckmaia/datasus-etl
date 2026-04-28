"""In-process pipeline runner with an async event queue for SSE progress.

The web UI kicks off a long-running pipeline via
:meth:`RunRegistry.launch`. The pipeline itself is synchronous and runs in a
worker thread; progress callbacks are bridged to the event loop so an SSE
endpoint can ``await queue.get()`` without blocking.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from datasus_etl.config import PipelineConfig
from datasus_etl.exceptions import PipelineCancelled
from datasus_etl.pipeline.sihsus_pipeline import SihsusPipeline
from datasus_etl.pipeline.sim_pipeline import SIMPipeline

logger = logging.getLogger(__name__)

EventType = Literal["start", "progress", "log", "done", "error", "cancelled"]

_PIPELINES = {
    "sihsus": SihsusPipeline,
    "sim": SIMPipeline,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RunEvent:
    """A single SSE event emitted by a running pipeline."""

    type: EventType
    ts: str
    data: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"type": self.type, "ts": self.ts, **self.data}


@dataclass
class Run:
    """Tracks one pipeline execution."""

    id: str
    subsystem: str
    params: dict[str, Any]
    status: Literal["pending", "running", "done", "error", "cancelled"] = "pending"
    progress: float = 0.0
    message: str = ""
    queue: asyncio.Queue[RunEvent] = field(default_factory=lambda: asyncio.Queue(maxsize=1000))
    task: asyncio.Task[None] | None = None
    started_at: str = field(default_factory=_now)
    finished_at: str | None = None
    context: Any | None = None  # PipelineContext, populated when running


class RunRegistry:
    """Process-wide registry of in-flight and completed runs."""

    def __init__(self) -> None:
        self._runs: dict[str, Run] = {}

    def create(self, subsystem: str, params: dict[str, Any]) -> Run:
        run_id = uuid.uuid4().hex[:12]
        run = Run(id=run_id, subsystem=subsystem.lower(), params=params)
        self._runs[run_id] = run
        return run

    def get(self, run_id: str) -> Run | None:
        return self._runs.get(run_id)

    def all(self) -> list[Run]:
        return list(self._runs.values())

    async def launch(self, subsystem: str, params: dict[str, Any], data_dir: Path) -> Run:
        if subsystem.lower() not in _PIPELINES:
            raise ValueError(f"Unsupported subsystem: {subsystem}")
        run = self.create(subsystem, params)
        run.task = asyncio.create_task(_run_pipeline(run, data_dir))
        return run

    async def cancel(self, run_id: str) -> bool:
        run = self.get(run_id)
        if run is None:
            return False
        if run.status not in ("pending", "running"):
            return False
        if run.context is not None and hasattr(run.context, "request_cancel"):
            run.context.request_cancel()
        if run.task is not None and not run.task.done():
            run.task.cancel()
        return True


REGISTRY = RunRegistry()


async def _run_pipeline(run: Run, data_dir: Path) -> None:
    """Coroutine that owns one pipeline execution."""
    loop = asyncio.get_running_loop()

    def on_progress(progress: float, message: str) -> None:
        run.progress = progress
        run.message = message
        event = RunEvent(
            type="progress",
            ts=_now(),
            data={"progress": progress, "message": message},
        )
        try:
            asyncio.run_coroutine_threadsafe(run.queue.put(event), loop)
        except RuntimeError:
            pass  # Loop closing; drop the event.

    def on_stage_progress(
        stage_name: str,
        stage_frac: float,
        message: str,
        global_frac: float,
    ) -> None:
        run.progress = global_frac
        run.message = message
        event = RunEvent(
            type="progress",
            ts=_now(),
            data={
                "progress": global_frac,
                "message": message,
                "stage": stage_name,
                "stage_progress": stage_frac,
            },
        )
        try:
            asyncio.run_coroutine_threadsafe(run.queue.put(event), loop)
        except RuntimeError:
            pass  # Loop closing; drop the event.

    await run.queue.put(
        RunEvent(
            type="start",
            ts=_now(),
            data={"subsystem": run.subsystem, "params": run.params},
        )
    )
    run.status = "running"

    def _work() -> None:
        # Ensure the IBGE municipalities parquet is present before any FTP
        # work begins — the post-pipeline `ibge_locais` VIEW depends on it.
        # Surfaced to the UI through the existing "[prepare] " message tag so
        # the wizard's "Preparing download…" panel covers the status.
        from datasus_etl.download.ftp_downloader import PREPARE_TAG
        from datasus_etl.utils.ibge_loader import ensure_ibge_parquet

        on_progress(0.0, f"{PREPARE_TAG}Verificando dados do IBGE...")
        ensure_ibge_parquet(
            data_dir,
            on_progress=lambda msg: on_progress(0.0, f"{PREPARE_TAG}{msg}"),
        )

        pipeline_cls = _PIPELINES[run.subsystem]
        cfg = PipelineConfig.create(
            base_dir=data_dir,
            subsystem=run.subsystem,
            start_date=run.params["start_date"],
            end_date=run.params.get("end_date"),
            uf_list=run.params.get("ufs"),
            override=run.params.get("override", False),
        )
        pipeline_obj = pipeline_cls(cfg)
        run.context = pipeline_obj.context
        pipeline_obj.context.set_progress_callback(on_progress)
        pipeline_obj.context.set_stage_progress_callback(on_stage_progress)
        pipeline_obj.run()

    try:
        await asyncio.to_thread(_work)
    except asyncio.CancelledError:
        run.status = "cancelled"
        run.finished_at = _now()
        await run.queue.put(RunEvent(type="cancelled", ts=run.finished_at))
        return
    except PipelineCancelled:
        run.status = "cancelled"
        run.finished_at = _now()
        await run.queue.put(RunEvent(type="cancelled", ts=run.finished_at))
        return
    except Exception as exc:  # noqa: BLE001 — we want to surface every failure
        run.status = "error"
        run.finished_at = _now()
        tb = traceback.format_exc()
        logger.exception("Pipeline run %s failed", run.id)
        # Always print to stderr too — datasus_etl loggers may not be wired up
        # to a handler when running under uvicorn, so the traceback would be
        # invisible otherwise (which leaves the UI showing a blank "Pipeline
        # error" with no clue about the underlying cause).
        print(
            f"[runtime] Pipeline run {run.id} failed: {type(exc).__name__}: {exc}\n{tb}",
            file=sys.stderr,
            flush=True,
        )
        # Fall back to the exception type name when str(exc) is empty so the
        # UI never has to render the generic "Pipeline error" placeholder.
        message = str(exc).strip() or type(exc).__name__
        await run.queue.put(
            RunEvent(
                type="error",
                ts=run.finished_at,
                data={
                    "message": message,
                    "type": type(exc).__name__,
                    "traceback": tb,
                },
            )
        )
        return

    run.status = "done"
    run.progress = 1.0
    run.finished_at = _now()
    await run.queue.put(RunEvent(type="done", ts=run.finished_at, data={"progress": 1.0}))
