import * as React from "react";
import { useMutation } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { ArrowLeft, Play, AlertTriangle } from "lucide-react";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api";
import type { EstimateResponse } from "@/lib/api";
import { formatBytes, formatNumber } from "@/lib/format";
import { useSettings } from "@/hooks/useSettings";
import { useWizard } from "../DownloadWizard";

export function Step3EstimatePage() {
  const { t } = useTranslation();
  const { state, update } = useWizard();
  const settings = useSettings();
  const navigate = useNavigate();
  const [estimate, setEstimate] = React.useState<EstimateResponse | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  const estimateMutation = useMutation({
    mutationFn: () => {
      if (!state.subsystem) throw new Error("subsystem missing");
      return api.estimate({
        subsystem: state.subsystem,
        start_date: state.start_date,
        end_date: state.end_date || null,
        ufs: state.ufs.length ? state.ufs : null,
      });
    },
    onSuccess: (data) => {
      setEstimate(data);
      setError(null);
    },
    onError: (err: Error) => {
      setError(err.message);
    },
  });

  const startMutation = useMutation({
    mutationFn: () => {
      if (!state.subsystem) throw new Error("subsystem missing");
      return api.startPipeline({
        subsystem: state.subsystem,
        start_date: state.start_date,
        end_date: state.end_date || null,
        ufs: state.ufs.length ? state.ufs : null,
        override: false,
      });
    },
    onSuccess: (data) => {
      update({ runId: data.run_id });
      navigate({ to: "/download/step-4" });
    },
    onError: (err: Error) => {
      toast.error(t("step3.failedToStart"), { description: err.message });
    },
  });

  // Auto-estimate on mount
  React.useEffect(() => {
    if (!state.subsystem || !state.start_date) return;
    estimateMutation.mutate();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const free = settings.data?.free_disk_bytes ?? null;
  const needed = estimate?.estimated_duckdb_bytes ?? 0;
  const enoughDisk = free == null ? true : free > needed;

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-xl font-semibold">{t("step3.title")}</h2>
        <p className="text-sm text-muted-foreground">{t("step3.subtitle")}</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t("step3.selection")}</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-3 text-sm md:grid-cols-2">
          <div>
            <div className="text-xs uppercase tracking-wide text-muted-foreground">
              {t("step3.subsystem")}
            </div>
            <div className="font-mono font-semibold uppercase">{state.subsystem}</div>
          </div>
          <div>
            <div className="text-xs uppercase tracking-wide text-muted-foreground">
              {t("step3.dateRange")}
            </div>
            <div className="font-mono">
              {state.start_date} → {state.end_date || t("step3.latest")}
            </div>
          </div>
          <div className="md:col-span-2">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">
              {t("step3.ufs")}
            </div>
            <div className="mt-1 flex flex-wrap gap-1">
              {state.ufs.length === 0 ? (
                <Badge variant="secondary">{t("common.all")}</Badge>
              ) : (
                state.ufs.map((u) => (
                  <Badge key={u} variant="secondary">
                    {u}
                  </Badge>
                ))
              )}
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t("step3.downloadEstimate")}</CardTitle>
        </CardHeader>
        <CardContent>
          {estimateMutation.isPending ? (
            <div className="grid gap-3 md:grid-cols-3">
              <Skeleton className="h-16" />
              <Skeleton className="h-16" />
              <Skeleton className="h-16" />
            </div>
          ) : error ? (
            <div className="rounded-md border border-destructive/40 bg-destructive/10 p-4 text-sm text-destructive">
              {error}
            </div>
          ) : estimate ? (
            <div className="grid gap-4 md:grid-cols-3">
              <Stat label={t("step3.files")} value={formatNumber(estimate.file_count)} />
              <Stat
                label={t("step3.downloadSize")}
                value={formatBytes(estimate.total_download_bytes)}
              />
              <Stat
                label={t("step3.storageOnDisk")}
                value={formatBytes(estimate.estimated_duckdb_bytes)}
              />
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">{t("step3.noEstimate")}</p>
          )}

          {!enoughDisk && estimate ? (
            <div className="mt-4 flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-600 dark:text-amber-400">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <div>
                {t("step3.diskWarning", {
                  needed: formatBytes(needed),
                  free: free != null ? formatBytes(free) : t("step3.diskUnknown"),
                })}
              </div>
            </div>
          ) : null}
        </CardContent>
      </Card>

      <div className="flex items-center justify-between">
        <Button variant="ghost" onClick={() => navigate({ to: "/download/step-2" })}>
          <ArrowLeft className="h-4 w-4" />
          {t("common.back")}
        </Button>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            disabled={estimateMutation.isPending}
            onClick={() => estimateMutation.mutate()}
          >
            {t("step3.reEstimate")}
          </Button>
          <Button
            disabled={!estimate || startMutation.isPending}
            onClick={() => startMutation.mutate()}
          >
            <Play className="h-4 w-4" />
            {startMutation.isPending ? t("step3.starting") : t("step3.startDownload")}
          </Button>
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border p-4">
      <div className="text-xs uppercase tracking-wide text-muted-foreground">
        {label}
      </div>
      <div className="mt-1 font-mono text-2xl font-semibold tabular-nums">
        {value}
      </div>
    </div>
  );
}
