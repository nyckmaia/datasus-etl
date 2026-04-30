import * as React from "react";
import { useMutation } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { ArrowLeft, Play, AlertTriangle, ChevronRight } from "lucide-react";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { api } from "@/lib/api";
import type { EstimateResponse } from "@/lib/api";
import { formatBytes, formatNumber, formatYearsMonths, monthsBetween } from "@/lib/format";
import { useSettings } from "@/hooks/useSettings";
import { useSubsystemDetail } from "@/hooks/useStats";
import { cn } from "@/lib/utils";
import { useWizard } from "../DownloadWizard";

export function Step3EstimatePage() {
  const { t } = useTranslation();
  const { state, update } = useWizard();
  const settings = useSettings();
  const navigate = useNavigate();
  const [estimate, setEstimate] = React.useState<EstimateResponse | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [perUfOpen, setPerUfOpen] = React.useState(false);

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

  const subsystemDetail = useSubsystemDetail(state.subsystem);
  const localPerUf = subsystemDetail.data?.per_uf ?? [];
  const ftpPerUf = estimate?.per_uf ?? [];

  // Combines local missing-months gaps (from /api/stats/subsystem/{name}) with
  // FTP availability per UF (from /api/pipeline/estimate.per_uf).
  const perUfRows = React.useMemo(() => {
    const ufSet = new Set<string>();
    for (const r of localPerUf) ufSet.add(r.uf);
    for (const r of ftpPerUf) ufSet.add(r.uf);
    const rows = Array.from(ufSet).sort().map((uf) => {
      const local = localPerUf.find((r) => r.uf === uf);
      const ftp = ftpPerUf.find((r) => r.uf === uf);
      const startDate = local?.first_period ?? null;
      const endDate = local?.last_period ?? null;
      const dataGapMonths = local?.missing_months ?? 0;
      // Period to Download is the FTP-side span being added.
      const periodToDownloadMonths = monthsBetween(
        ftp?.ftp_first_period ?? null,
        ftp?.ftp_last_period ?? null,
      );
      // Updated End Date = max(local end, ftp end). Lexicographic comparison
      // works because both are "YYYY-MM" strings.
      const candidates = [endDate, ftp?.ftp_last_period ?? null].filter(Boolean) as string[];
      const updatedEndDate = candidates.length ? candidates.sort().at(-1)! : null;
      return {
        uf,
        startDate,
        endDate,
        dataGapMonths,
        periodToDownloadMonths,
        updatedEndDate,
      };
    });
    return rows;
  }, [localPerUf, ftpPerUf]);

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

      {/* Collapsible per-UF detail table — combines local missing-months gaps
          (from /api/stats/subsystem/{name}) with FTP availability per UF
          (from /api/pipeline/estimate.per_uf). Default collapsed. */}
      <Card className="overflow-hidden">
        <button
          type="button"
          onClick={() => setPerUfOpen((v) => !v)}
          className="flex w-full items-center justify-between border-b px-4 py-3 text-left transition-colors hover:bg-secondary/30"
        >
          <span className="flex items-center gap-2 text-sm font-semibold">
            <ChevronRight
              className={cn(
                "h-4 w-4 transition-transform",
                perUfOpen && "rotate-90",
              )}
            />
            {t("step3.perUf.title")}
          </span>
          <span className="text-xs text-muted-foreground">
            {perUfRows.length} {t("step3.perUf.rowCountSuffix", "UF(s)")}
          </span>
        </button>
        {perUfOpen ? (
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>{t("step3.perUf.tableUf")}</TableHead>
                  <TableHead>{t("step3.perUf.tableStart")}</TableHead>
                  <TableHead>{t("step3.perUf.tableEnd")}</TableHead>
                  <TableHead>{t("step3.perUf.tableDataGap")}</TableHead>
                  <TableHead>{t("step3.perUf.tablePeriodToDownload")}</TableHead>
                  <TableHead>{t("step3.perUf.tableUpdatedEnd")}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {perUfRows.map((row) => (
                  <TableRow
                    key={row.uf}
                    className={cn(
                      row.dataGapMonths > 0
                        && "bg-amber-500/10 hover:bg-amber-500/15",
                    )}
                    title={
                      row.dataGapMonths > 0
                        ? t("step3.perUf.warnGap")
                        : undefined
                    }
                  >
                    <TableCell className="font-mono font-medium">{row.uf}</TableCell>
                    <TableCell className="font-mono text-xs">
                      {row.startDate ?? "-"}
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {row.endDate ?? "-"}
                    </TableCell>
                    <TableCell
                      className={cn(
                        "text-xs",
                        row.dataGapMonths > 0 && "font-medium text-amber-700 dark:text-amber-400",
                      )}
                    >
                      {formatYearsMonths(row.dataGapMonths)}
                    </TableCell>
                    <TableCell className="text-xs">
                      {formatYearsMonths(row.periodToDownloadMonths)}
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {row.updatedEndDate ?? "-"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ) : null}
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
