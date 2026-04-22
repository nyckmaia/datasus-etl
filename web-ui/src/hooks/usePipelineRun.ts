import * as React from "react";

import { subscribeProgress } from "@/lib/sse";
import type { RunSnapshot } from "@/lib/api";

// Backend-side marker on messages while still listing FTP directories. See
// `src/datasus_etl/download/ftp_downloader.py:PREPARE_TAG`.
const PREPARE_TAG = "[prepare] ";

export type PipelinePhase = "idle" | "preparing" | "running" | "finished";

export interface PipelineLogEntry {
  ts: number;
  type: string;
  message: string;
}

// Pipeline stage IDs the backend emits via the `stage` field of progress
// events. Kept as a string union so future stages (e.g. memory_aware_processing)
// flow through unchanged — the UI just won't render them by default.
export type PipelineStageId =
  | "download"
  | "dbc_to_dbf"
  | "dbf_to_parquet"
  | string;

export interface PipelineRunState {
  progress: number;
  message: string;
  status: string;
  phase: PipelinePhase;
  logs: PipelineLogEntry[];
  snapshot: RunSnapshot | null;
  finished: boolean;
  error: string | null;
  // Per-stage progress as reported by backend `update_stage_progress` calls.
  // Keys are stage IDs ("download", "dbc_to_dbf", "dbf_to_parquet"); values
  // are 0..1 fractions of the individual stage. The UI renders one
  // <Progress> per known stage from this map.
  stageProgress: Record<string, number>;
  // Which stage emitted the most recent progress event — used to highlight
  // the currently active bar. `null` while preparing or before the first
  // stage event.
  activeStage: string | null;
}

const initialState: PipelineRunState = {
  progress: 0,
  message: "Connecting...",
  status: "pending",
  phase: "idle",
  logs: [],
  snapshot: null,
  finished: false,
  error: null,
  stageProgress: {},
  activeStage: null,
};

function parseMessage(raw: string): { message: string; preparing: boolean } {
  if (raw.startsWith(PREPARE_TAG)) {
    return { message: raw.slice(PREPARE_TAG.length), preparing: true };
  }
  return { message: raw, preparing: false };
}

// Preparation is the *first* phase of a run and never resumes once real work
// starts. Once we've observed progress > 0 or a non-prepare message, latch the
// phase to "running" so a later prepare-tagged log line can't snap the UI
// back to the spinner.
function phaseFrom(
  prev: PipelinePhase,
  status: string,
  preparing: boolean,
  progress: number,
): PipelinePhase {
  if (status === "done" || status === "error" || status === "cancelled") return "finished";
  if (prev === "running" || prev === "finished") return prev === "finished" ? "finished" : "running";
  if (progress > 0) return "running";
  if (preparing) return "preparing";
  if (status === "running") return "running";
  return "idle";
}

export function usePipelineRun(runId: string | null): PipelineRunState {
  const [state, setState] = React.useState<PipelineRunState>(initialState);

  React.useEffect(() => {
    if (!runId) {
      setState(initialState);
      return;
    }
    setState(initialState);

    // Dedupe consecutive identical messages — the FTP downloader emits one
    // event per integer percent of total bytes, but the message string only
    // changes when a new file starts. Without dedupe the log scroll would be
    // ~100 copies of "Downloading X.dbc — 1/4" before "Downloading Y.dbc"
    // appears. Same applies to converter stages emitting per-file messages.
    const append = (log: PipelineLogEntry) =>
      setState((s) => {
        const last = s.logs[s.logs.length - 1];
        if (
          last &&
          last.type === log.type &&
          last.message === log.message
        ) {
          return s;
        }
        return { ...s, logs: [...s.logs, log] };
      });

    const dispose = subscribeProgress(runId, {
      onEvent: (event) => {
        if (event.type === "snapshot") {
          const parsed = parseMessage(event.snapshot.message || "");
          setState((s) => ({
            ...s,
            snapshot: event.snapshot,
            status: event.snapshot.status,
            progress: event.snapshot.progress,
            message: parsed.message,
            phase: phaseFrom(
              s.phase,
              event.snapshot.status,
              parsed.preparing,
              event.snapshot.progress,
            ),
            finished: ["done", "error", "cancelled"].includes(event.snapshot.status),
          }));
          append({
            ts: Date.now(),
            type: "snapshot",
            message: parsed.message || event.snapshot.status,
          });
          return;
        }
        if (event.type === "progress") {
          const parsed = parseMessage(event.message || "");
          setState((s) => {
            // Merge per-stage progress when the backend tags the event.
            // When a NEW stage starts emitting we lock the previous one to
            // 1.0 so its bar visibly completes (the backend only fires
            // mark_stage_progress_complete once per stage and the rounding
            // can otherwise leave a bar at 99%).
            let stageProgress = s.stageProgress;
            let activeStage = s.activeStage;
            if (event.stage) {
              const next = { ...s.stageProgress };
              if (
                activeStage &&
                activeStage !== event.stage &&
                next[activeStage] !== 1
              ) {
                next[activeStage] = 1;
              }
              if (typeof event.stageProgress === "number") {
                next[event.stage] = event.stageProgress;
              }
              stageProgress = next;
              activeStage = event.stage;
            }
            return {
              ...s,
              progress: event.progress,
              message: parsed.message,
              status: "running",
              phase: phaseFrom(s.phase, "running", parsed.preparing, event.progress),
              stageProgress,
              activeStage,
            };
          });
          if (parsed.message) {
            append({
              ts: Date.now(),
              type: parsed.preparing ? "prepare" : "progress",
              message: parsed.message,
            });
          }
          return;
        }
        if (event.type === "start") {
          setState((s) => ({
            ...s,
            status: "running",
            phase: s.phase === "idle" ? "preparing" : s.phase,
          }));
          append({ ts: Date.now(), type: "start", message: "Run started" });
          return;
        }
        if (event.type === "done") {
          setState((s) => {
            // Pin every stage we've seen to 100% so no bar ends visually
            // short (rounding or a missing final emit would otherwise leave
            // it at 99%).
            const stageProgress: Record<string, number> = {};
            for (const stage of Object.keys(s.stageProgress)) {
              stageProgress[stage] = 1;
            }
            return {
              ...s,
              status: "done",
              progress: 1,
              message: "Completed",
              phase: "finished",
              finished: true,
              stageProgress,
              activeStage: null,
            };
          });
          append({ ts: Date.now(), type: "done", message: "Completed" });
          return;
        }
        if (event.type === "cancelled") {
          setState((s) => ({
            ...s,
            status: "cancelled",
            phase: "finished",
            finished: true,
            message: "Cancelled",
          }));
          append({ ts: Date.now(), type: "cancelled", message: "Cancelled" });
          return;
        }
        if (event.type === "error") {
          const msg =
            (event.data &&
              typeof event.data === "object" &&
              "message" in (event.data as Record<string, unknown>) &&
              typeof (event.data as Record<string, unknown>).message === "string"
              ? ((event.data as Record<string, unknown>).message as string)
              : "Pipeline error") || "Pipeline error";
          // Defense in depth: if the run already completed cleanly (done
          // or cancelled), ignore any late-arriving error event. This can
          // happen because the SSE protocol reuses the "error" event name
          // for browser-side connection issues — a clean close after
          // `done` shouldn't downgrade a successful run.
          let suppressed = false;
          setState((s) => {
            if (s.finished) {
              suppressed = true;
              return s;
            }
            return {
              ...s,
              status: "error",
              phase: "finished",
              finished: true,
              error: msg,
              message: msg,
            };
          });
          if (!suppressed) {
            append({ ts: Date.now(), type: "error", message: msg });
          }
          return;
        }
        // ping — ignore
      },
      onError: () => {
        setState((s) =>
          s.finished
            ? s
            : {
                ...s,
                error: "Connection lost",
                message: "Connection lost",
              },
        );
      },
    });

    return dispose;
  }, [runId]);

  return state;
}
