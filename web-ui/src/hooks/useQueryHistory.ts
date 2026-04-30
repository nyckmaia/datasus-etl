import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api } from "@/lib/api";
import type { QueryHistoryEntry, QueryHistoryPatch } from "@/lib/api";

const KEY = (subsystem: string | null) => ["query", "history", subsystem];

export function useQueryHistory(subsystem: string | null) {
  return useQuery({
    queryKey: KEY(subsystem),
    queryFn: () =>
      subsystem
        ? api.listHistory(subsystem).then((r) => r.entries)
        : Promise.resolve([] as QueryHistoryEntry[]),
    enabled: Boolean(subsystem),
    staleTime: 5_000,
  });
}

export function useAppendQueryHistory(subsystem: string | null) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (entry: QueryHistoryEntry) =>
      subsystem
        ? api.appendHistory(subsystem, entry)
        : Promise.resolve({ entries: [] as QueryHistoryEntry[] }),
    onSuccess: (data) => {
      if (subsystem) qc.setQueryData(KEY(subsystem), data.entries);
    },
  });
}

export function useClearQueryHistory(subsystem: string | null) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      subsystem
        ? api.clearHistory(subsystem)
        : Promise.resolve({ entries: [] as QueryHistoryEntry[] }),
    onSuccess: () => {
      if (subsystem) qc.setQueryData(KEY(subsystem), []);
    },
  });
}

export function usePatchQueryHistory(subsystem: string | null) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, patch }: { id: string; patch: QueryHistoryPatch }) =>
      subsystem
        ? api.patchHistoryEntry(subsystem, id, patch)
        : Promise.resolve({ entries: [] as QueryHistoryEntry[] }),
    onSuccess: (data) => {
      if (subsystem) qc.setQueryData(KEY(subsystem), data.entries);
    },
  });
}

export function useDeleteQueryHistoryEntry(subsystem: string | null) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) =>
      subsystem
        ? api.deleteHistoryEntry(subsystem, id)
        : Promise.resolve({ entries: [] as QueryHistoryEntry[] }),
    onSuccess: (data) => {
      if (subsystem) qc.setQueryData(KEY(subsystem), data.entries);
    },
  });
}
