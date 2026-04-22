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
      const data = parse((e as MessageEvent<string>).data ?? "");
      if (type === "progress" && data && typeof data === "object") {
        const d = data as Record<string, unknown>;
        onEvent({
          type: "progress",
          progress: typeof d.progress === "number" ? d.progress : 0,
          message: typeof d.message === "string" ? d.message : "",
        });
      } else if (type === "snapshot" && data && typeof data === "object") {
        onEvent({ type: "snapshot", snapshot: data as RunSnapshot });
      } else {
        onEvent({ type, data } as SseGenericEvent);
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
