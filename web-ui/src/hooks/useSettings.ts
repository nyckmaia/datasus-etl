import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api } from "@/lib/api";
import type { SettingsResponse } from "@/lib/api";

export function useSettings() {
  return useQuery<SettingsResponse>({
    queryKey: ["settings"],
    queryFn: () => api.settings(),
  });
}

export function useUpdateDataDir() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data_dir: string) => api.updateDataDir(data_dir),
    onSuccess: (data) => {
      qc.setQueryData(["settings"], data);
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}

export function useUpdateHistorySize() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (history_size_k: number) => api.updateHistorySize(history_size_k),
    onSuccess: (data) => {
      qc.setQueryData(["settings"], data);
    },
  });
}

export function useUpdateExportCaps() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: { export_max_rows: number; export_max_bytes: number }) =>
      api.updateExportCaps(payload),
    onSuccess: (data) => {
      qc.setQueryData(["settings"], data);
    },
  });
}

export function usePickDirectory() {
  return useMutation({
    mutationFn: () => api.pickDirectory(),
  });
}

export function useResetStorage() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (subsystems: string[]) => api.resetStorage(subsystems),
    // After deletion every Dashboard / Query view that depends on file
    // counts is stale — invalidate `stats` so totals/per-UF maps refresh.
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["stats"] });
    },
  });
}
