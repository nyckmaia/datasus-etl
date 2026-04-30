// Typed fetch wrappers for the DataSUS ETL backend.
//
// All endpoints live under /api. When running through Vite's dev server the
// proxy in vite.config.ts forwards these to the FastAPI backend. In
// production the FastAPI app mounts this bundle under /, so relative paths
// work in both environments.

export interface SubsystemInfo {
  name: string;
  description: string;
  file_prefix: string;
}

export interface SettingsResponse {
  data_dir: string | null;
  data_dir_resolved: string | null;
  free_disk_bytes: number | null;
  total_disk_bytes: number | null;
  version: string;
  python_version: string;
  subsystems: SubsystemInfo[];
  config_file: string;
  history_size_k: number;
}

export interface QueryHistoryEntry {
  id: string;
  sql: string;
  ts: number;
  rows: number;
  elapsed_ms: number;
  name?: string | null;
  favorite?: boolean;
}

export interface QueryHistoryPatch {
  name?: string | null;
  favorite?: boolean;
}

export interface VersionCheckResponse {
  current: string;
  latest: string | null;
  update_available: boolean;
  release_url?: string;
  error?: string;
}

export interface SubsystemSummary {
  subsystem: string;
  files: number;
  size_bytes: number;
  ufs: string[];
  row_count: number | null;
  first_period: string | null;
  last_period: string | null;
  last_updated: number | null;
}

export interface UfBreakdown {
  uf: string;
  files: number;
  size_bytes: number;
  row_count: number | null;
  first_period: string | null;
  last_period: string | null;
}

export interface TimelinePoint {
  period: string;
  files: number;
  size_bytes: number;
}

export interface SubsystemDetail {
  subsystem: string;
  files: number;
  size_bytes: number;
  ufs: string[];
  row_count: number | null;
  per_uf: UfBreakdown[];
  timeline: TimelinePoint[];
  last_updated: number | null;
}

export interface EstimateRequest {
  subsystem: string;
  start_date: string;
  end_date?: string | null;
  ufs?: string[] | null;
}

export interface EstimateResponse {
  subsystem: string;
  file_count: number;
  total_download_bytes: number;
  estimated_duckdb_bytes: number;
  estimated_csv_bytes: number;
}

export interface StartRequest extends EstimateRequest {
  override?: boolean;
}

export interface StartResponse {
  run_id: string;
}

export type RunStatus =
  | "pending"
  | "running"
  | "done"
  | "error"
  | "cancelled"
  | string;

export interface RunSnapshot {
  id: string;
  subsystem: string;
  status: RunStatus;
  progress: number;
  message: string;
  started_at: string;
  finished_at: string | null;
}

export interface SqlRequest {
  sql: string;
  limit?: number | null;
}

export type SqlValue = string | number | boolean | null;

export interface SqlResult {
  columns: string[];
  rows: SqlValue[][];
  row_count: number;
  truncated: boolean;
  elapsed_ms: number;
  limit_applied: number;
  total_rows?: number | null;
}

export interface TemplateItem {
  subsystem: string;
  name: string;
  sql: string;
}

// --- Hierarchical schema (used by the SchemaTree on /query) -----------------

export interface SchemaColumn {
  column: string;
  description: string;
  type: string;
  fill_pct?: number | null;
  fill_pct_approx?: boolean;
  distinct_count?: number | null;
  distinct_count_approx?: boolean;
}

export interface SchemaView {
  name: string;
  role: "main" | "dim";
  label_pt: string | null;
  label_en: string | null;
  description: string | null;
  columns: SchemaColumn[];
}

export interface SchemaSubsystem {
  name: string;
  label: string;
  views: SchemaView[];
}

export interface SchemaTree {
  subsystems: SchemaSubsystem[];
}

export interface ExportRequest {
  sql: string;
  format: "csv" | "xlsx";
  limit?: number | null;
  filename?: string | null;
}

export interface PickDirectoryResponse {
  path: string | null;
  cancelled: boolean;
  error?: string;
}

export interface ValidatePathResponse {
  normalized: string;
  exists: boolean;
  is_dir: boolean;
  will_be_created: boolean;
  writable: boolean;
  error?: string;
}

export interface ResetStorageItem {
  name: string;
  path?: string | null;
  freed_bytes: number;
  skipped_reason?: string | null;
}

export interface ResetStorageResponse {
  deleted: ResetStorageItem[];
  skipped: ResetStorageItem[];
}

export class ApiError extends Error {
  status: number;
  detail: string;
  constructor(status: number, detail: string) {
    super(detail);
    this.status = status;
    this.detail = detail;
  }
}

async function request<T>(input: string, init?: RequestInit): Promise<T> {
  const res = await fetch(input, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      if (body && typeof body.detail === "string") detail = body.detail;
    } catch {
      // fall through
    }
    throw new ApiError(res.status, detail);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

// --------------------------------------------------------------------------- //
// Health & settings                                                            //
// --------------------------------------------------------------------------- //

export const api = {
  health(): Promise<{ status: string }> {
    return request("/api/health");
  },

  settings(): Promise<SettingsResponse> {
    return request("/api/settings");
  },

  updateDataDir(data_dir: string): Promise<SettingsResponse> {
    return request("/api/settings/data-dir", {
      method: "PUT",
      body: JSON.stringify({ data_dir }),
    });
  },

  updateHistorySize(history_size_k: number): Promise<SettingsResponse> {
    return request("/api/settings/history-size", {
      method: "PUT",
      body: JSON.stringify({ history_size_k }),
    });
  },

  listHistory(subsystem: string): Promise<{ entries: QueryHistoryEntry[] }> {
    return request(`/api/query/history/${encodeURIComponent(subsystem)}`);
  },

  appendHistory(
    subsystem: string,
    entry: QueryHistoryEntry,
  ): Promise<{ entries: QueryHistoryEntry[] }> {
    return request(`/api/query/history/${encodeURIComponent(subsystem)}`, {
      method: "POST",
      body: JSON.stringify(entry),
    });
  },

  patchHistoryEntry(
    subsystem: string,
    id: string,
    patch: QueryHistoryPatch,
  ): Promise<{ entries: QueryHistoryEntry[] }> {
    return request(
      `/api/query/history/${encodeURIComponent(subsystem)}/${encodeURIComponent(id)}`,
      { method: "PATCH", body: JSON.stringify(patch) },
    );
  },

  deleteHistoryEntry(
    subsystem: string,
    id: string,
  ): Promise<{ entries: QueryHistoryEntry[] }> {
    return request(
      `/api/query/history/${encodeURIComponent(subsystem)}/${encodeURIComponent(id)}`,
      { method: "DELETE" },
    );
  },

  clearHistory(subsystem: string): Promise<{ entries: QueryHistoryEntry[] }> {
    return request(`/api/query/history/${encodeURIComponent(subsystem)}`, {
      method: "DELETE",
    });
  },

  pickDirectory(): Promise<PickDirectoryResponse> {
    return request("/api/settings/pick-directory", { method: "POST" });
  },

  validatePath(path: string): Promise<ValidatePathResponse> {
    return request("/api/settings/validate-path", {
      method: "POST",
      body: JSON.stringify({ path }),
    });
  },

  resetStorage(subsystems: string[]): Promise<ResetStorageResponse> {
    return request("/api/settings/reset-storage", {
      method: "POST",
      body: JSON.stringify({ subsystems }),
    });
  },

  versionCheck(): Promise<VersionCheckResponse> {
    return request("/api/version/check");
  },

  // ------------------------------------------------------------------------ //
  // Stats                                                                     //
  // ------------------------------------------------------------------------ //

  statsOverview(with_rows = true): Promise<SubsystemSummary[]> {
    const qs = new URLSearchParams({ with_rows: String(with_rows) });
    return request(`/api/stats/overview?${qs.toString()}`);
  },

  statsSubsystem(name: string): Promise<SubsystemDetail> {
    return request(`/api/stats/subsystem/${encodeURIComponent(name)}`);
  },

  statsTimeline(subsystem: string): Promise<TimelinePoint[]> {
    const qs = new URLSearchParams({ subsystem });
    return request(`/api/stats/timeline?${qs.toString()}`);
  },

  // ------------------------------------------------------------------------ //
  // Pipeline                                                                  //
  // ------------------------------------------------------------------------ //

  estimate(payload: EstimateRequest): Promise<EstimateResponse> {
    return request("/api/pipeline/estimate", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },

  startPipeline(payload: StartRequest): Promise<StartResponse> {
    return request("/api/pipeline/start", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },

  cancelPipeline(run_id: string): Promise<{ cancelled: boolean }> {
    return request(`/api/pipeline/cancel/${encodeURIComponent(run_id)}`, {
      method: "POST",
    });
  },

  listRuns(): Promise<RunSnapshot[]> {
    return request("/api/pipeline/runs");
  },

  getRun(run_id: string): Promise<RunSnapshot> {
    return request(`/api/pipeline/runs/${encodeURIComponent(run_id)}`);
  },

  // ------------------------------------------------------------------------ //
  // Query                                                                     //
  // ------------------------------------------------------------------------ //

  runSql(payload: SqlRequest): Promise<SqlResult> {
    return request("/api/query/sql", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  },

  templates(): Promise<TemplateItem[]> {
    return request("/api/query/templates");
  },

  schema(): Promise<SchemaTree> {
    return request("/api/query/schema");
  },

  // ------------------------------------------------------------------------ //
  // Export                                                                    //
  // ------------------------------------------------------------------------ //

  async exportQuery(payload: ExportRequest): Promise<Blob> {
    const res = await fetch("/api/export", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      let detail = res.statusText;
      try {
        const body = await res.json();
        if (body && typeof body.detail === "string") detail = body.detail;
      } catch {
        // ignore
      }
      throw new ApiError(res.status, detail);
    }
    return res.blob();
  },
};
