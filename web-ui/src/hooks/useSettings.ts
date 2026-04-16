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
