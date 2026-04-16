import * as React from "react";

import { subscribeProgress } from "@/lib/sse";
import type { RunSnapshot } from "@/lib/api";

export interface PipelineLogEntry {
  ts: number;
  type: string;
  message: string;
}

export interface PipelineRunState {
  progress: number;
  message: string;
  status: string;
  logs: PipelineLogEntry[];
  snapshot: RunSnapshot | null;
  finished: boolean;
  error: string | null;
}

const initialState: PipelineRunState = {
  progress: 0,
  message: "Connecting...",
  status: "pending",
  logs: [],
  snapshot: null,
  finished: false,
  error: null,
};

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
          setState((s) => ({
            ...s,
            snapshot: event.snapshot,
            status: event.snapshot.status,
            progress: event.snapshot.progress,
            message: event.snapshot.message,
            finished: ["done", "error", "cancelled"].includes(event.snapshot.status),
          }));
          append({
            ts: Date.now(),
            type: "snapshot",
            message: event.snapshot.message || event.snapshot.status,
          });
          return;
        }
        if (event.type === "progress") {
          setState((s) => ({
            ...s,
            progress: event.progress,
            message: event.message,
            status: "running",
          }));
          if (event.message) {
            append({ ts: Date.now(), type: "progress", message: event.message });
          }
          return;
        }
        if (event.type === "start") {
          setState((s) => ({ ...s, status: "running" }));
          append({ ts: Date.now(), type: "start", message: "Run started" });
          return;
        }
        if (event.type === "done") {
          setState((s) => ({
            ...s,
            status: "done",
            progress: 1,
            message: "Completed",
            finished: true,
          }));
          append({ ts: Date.now(), type: "done", message: "Completed" });
          return;
        }
        if (event.type === "cancelled") {
          setState((s) => ({
            ...s,
            status: "cancelled",
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
