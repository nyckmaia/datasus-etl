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

export interface PipelineRunState {
  progress: number;
  message: string;
  status: string;
  phase: PipelinePhase;
  logs: PipelineLogEntry[];
  snapshot: RunSnapshot | null;
  finished: boolean;
  error: string | null;
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

    const append = (log: PipelineLogEntry) =>
      setState((s) => ({ ...s, logs: [...s.logs, log] }));

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
          setState((s) => ({
            ...s,
            progress: event.progress,
            message: parsed.message,
            status: "running",
            phase: phaseFrom(s.phase, "running", parsed.preparing, event.progress),
          }));
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
          setState((s) => ({
            ...s,
            status: "done",
            progress: 1,
            message: "Completed",
            phase: "finished",
            finished: true,
          }));
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
          setState((s) => ({
            ...s,
            status: "error",
            phase: "finished",
            finished: true,
            error: msg,
            message: msg,
          }));
          append({ ts: Date.now(), type: "error", message: msg });
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
