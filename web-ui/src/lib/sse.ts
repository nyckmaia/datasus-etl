// Server-Sent Events helper for pipeline progress streaming.

import type { RunSnapshot } from "./api";

export type SseEventType =
  | "snapshot"
  | "start"
  | "progress"
  | "ping"
  | "done"
  | "error"
  | "cancelled";

export interface SseProgressEvent {
  type: "progress";
  progress: number;
  message: string;
  // Set when the event was emitted by `update_stage_progress(...)` on the
  // backend. Indicates which pipeline stage moved (download / dbc_to_dbf /
  // dbf_to_parquet / …) and how far through that stage it is. Older events
  // without these fields fall back to the single global bar.
  stage?: string;
  stageProgress?: number;
}

export interface SseSnapshotEvent {
  type: "snapshot";
  snapshot: RunSnapshot;
}

export interface SseGenericEvent {
  type: Exclude<SseEventType, "progress" | "snapshot">;
  data: unknown;
}

export type SseEvent = SseProgressEvent | SseSnapshotEvent | SseGenericEvent;

export interface SubscribeOptions {
  onEvent: (event: SseEvent) => void;
  onError?: (err: Event) => void;
}

export function subscribeProgress(
  runId: string,
  { onEvent, onError }: SubscribeOptions,
): () => void {
  const url = `/api/pipeline/progress/${encodeURIComponent(runId)}`;
  const es = new EventSource(url);

  const parse = (raw: string): unknown => {
    try {
      return JSON.parse(raw);
    } catch {
      return raw;
    }
  };

  const register = (type: SseEventType) => {
    es.addEventListener(type, (e) => {
      const evt = e as MessageEvent<string>;
      // The browser fires a synthetic `error` event on EventSource when the
      // connection closes (which happens cleanly after we yield `done` and
      // break the SSE generator). That synthetic event is a plain Event
      // without a `data` field — distinct from a backend-sent
      // `event: error\ndata: {...}` frame, which always carries data.
      // Without this guard the hook would interpret connection-close as a
      // failed pipeline and overwrite the just-received `done` with a
      // generic "Pipeline error". The dedicated `es.onerror` handler below
      // is responsible for actual connection-loss UX.
      if (type === "error" && (evt.data === undefined || evt.data === null)) {
        return;
      }
      const data = parse(evt.data ?? "");
      if (type === "progress" && data && typeof data === "object") {
        const d = data as Record<string, unknown>;
        onEvent({
          type: "progress",
          progress: typeof d.progress === "number" ? d.progress : 0,
          message: typeof d.message === "string" ? d.message : "",
          stage: typeof d.stage === "string" ? d.stage : undefined,
          stageProgress:
            typeof d.stage_progress === "number" ? d.stage_progress : undefined,
        });
      } else if (type === "snapshot" && data && typeof data === "object") {
        onEvent({ type: "snapshot", snapshot: data as RunSnapshot });
      } else {
        onEvent({ type, data } as SseGenericEvent);
      }

      // Hard-close the EventSource on terminal events. Otherwise the
      // browser's default reconnect behaviour fires ~3s after the server
      // closes the stream, re-runs `_events()` from the top, and replays
      // the initial snapshot — which surfaces in the run-page log as a
      // duplicate `[OK] dbf_to_parquet` line right after `done`.
      if (
        type === "done" ||
        type === "cancelled" ||
        (type === "error" && evt.data)
      ) {
        es.close();
      }
    });
  };

  (["snapshot", "start", "progress", "ping", "done", "error", "cancelled"] as SseEventType[]).forEach(
    register,
  );

  es.onerror = (err) => {
    if (onError) onError(err);
  };

  return () => {
    es.close();
  };
}
