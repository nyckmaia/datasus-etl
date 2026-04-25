import { useQuery } from "@tanstack/react-query";

import { api } from "@/lib/api";
import type { VersionCheckResponse } from "@/lib/api";

export function useVersionCheck() {
  return useQuery<VersionCheckResponse>({
    queryKey: ["version", "check"],
    queryFn: () => api.versionCheck(),
    staleTime: 24 * 60 * 60 * 1000,
    gcTime: 24 * 60 * 60 * 1000,
    retry: false,
  });
}
