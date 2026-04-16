import { useQuery } from "@tanstack/react-query";

import { api } from "@/lib/api";
import type { SubsystemDetail, SubsystemSummary, TimelinePoint } from "@/lib/api";

export function useStatsOverview(withRows = true) {
  return useQuery<SubsystemSummary[]>({
    queryKey: ["stats", "overview", { withRows }],
    queryFn: () => api.statsOverview(withRows),
  });
}

export function useSubsystemDetail(name: string | null) {
  return useQuery<SubsystemDetail>({
    queryKey: ["stats", "subsystem", name],
    queryFn: () => api.statsSubsystem(name as string),
    enabled: Boolean(name),
  });
}

export function useTimeline(subsystem: string | null) {
  return useQuery<TimelinePoint[]>({
    queryKey: ["stats", "timeline", subsystem],
    queryFn: () => api.statsTimeline(subsystem as string),
    enabled: Boolean(subsystem),
  });
}
