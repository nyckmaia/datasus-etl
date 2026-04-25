import * as React from "react";
import { useNavigate } from "@tanstack/react-router";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";
import {
  Download,
  Database,
  HardDrive,
  Files,
  Rows3,
  FolderOpen,
} from "lucide-react";

import { StatCard } from "@/components/StatCard";
import { SubsystemCard } from "@/components/SubsystemCard";
import { VolumeChart } from "@/components/VolumeChart";
import { BrazilMap } from "@/components/BrazilMap";
import { EmptyState } from "@/components/EmptyState";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useStatsOverview, useTimeline } from "@/hooks/useStats";
import {
  useSettings,
  useUpdateDataDir,
  usePickDirectory,
} from "@/hooks/useSettings";
import { formatBytes, formatCompact, formatNumber } from "@/lib/format";

export function DashboardPage() {
  const { t } = useTranslation();
  const settings = useSettings();
  const overview = useStatsOverview(true);
  const updateDataDir = useUpdateDataDir();
  const pickDir = usePickDirectory();
  const navigate = useNavigate();

  const data = overview.data ?? [];
  const totalFiles = data.reduce((acc, d) => acc + d.files, 0);
  const totalSize = data.reduce((acc, d) => acc + d.size_bytes, 0);
  const totalRows = data.reduce((acc, d) => acc + (d.row_count ?? 0), 0);
  const distinctUfs = new Set<string>();
  data.forEach((d) => d.ufs.forEach((u) => distinctUfs.add(u)));

  const valuesByUf: Record<string, number> = {};
  data.forEach((d) => {
    d.ufs.forEach((u) => {
      valuesByUf[u] = (valuesByUf[u] ?? 0) + d.files;
    });
  });

  // Pick the first populated subsystem for the headline chart.
  const [chartSubsystem, setChartSubsystem] = React.useState<string | null>(null);
  React.useEffect(() => {
    if (chartSubsystem) return;
    const first = data.find((d) => d.files > 0);
    if (first) setChartSubsystem(first.subsystem);
  }, [data, chartSubsystem]);

  const timeline = useTimeline(chartSubsystem);

  const hasDataDir = !!settings.data?.data_dir_resolved;
  const noData = !overview.isLoading && data.every((d) => d.files === 0);

  const handlePick = () => {
    pickDir.mutate(undefined, {
      onSuccess: (res) => {
        if (res.error) {
          toast.error(res.error);
          return;
        }
        if (res.cancelled || !res.path) return;
        updateDataDir.mutate(res.path, {
          onSuccess: (data) =>
            toast.success(t("dashboard.dataDirSet"), {
              description: data.data_dir_resolved ?? data.data_dir ?? "",
            }),
          onError: (err: Error) =>
            toast.error(t("dashboard.setDataDirFailed"), {
              description: err.message,
            }),
        });
      },
      onError: (err: Error) =>
        toast.error(t("dashboard.folderPickerFailed"), { description: err.message }),
    });
  };

  const goToDownload = () => {
    if (!hasDataDir) {
      toast.error(t("dashboard.configureFirstTitle"), {
        description: t("dashboard.configureFirstDesc"),
      });
      return;
    }
    navigate({ to: "/download" });
  };

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{t("dashboard.title")}</h1>
          <p className="text-sm text-muted-foreground">{t("dashboard.subtitle")}</p>
        </div>
        <Button onClick={goToDownload}>
          <Download className="h-4 w-4" />
          {t("dashboard.newDownload")}
        </Button>
      </div>

      {!settings.isLoading && !hasDataDir ? (
        <Card className="border-amber-500/40 bg-amber-500/5">
          <CardHeader>
            <CardTitle className="text-base">{t("dashboard.noDataDirTitle")}</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-4 text-sm sm:flex-row sm:items-center sm:justify-between">
            <p
              className="text-muted-foreground"
              dangerouslySetInnerHTML={{
                __html: t("dashboard.noDataDirBody", {
                  folder: '<code class="font-mono">datasus_db/</code>',
                  interpolation: { escapeValue: false },
                }),
              }}
            />
            <Button
              onClick={handlePick}
              disabled={pickDir.isPending || updateDataDir.isPending}
            >
              <FolderOpen className="h-4 w-4" />
              {pickDir.isPending
                ? t("common.opening")
                : updateDataDir.isPending
                  ? t("common.saving")
                  : t("dashboard.setDataDir")}
            </Button>
          </CardContent>
        </Card>
      ) : null}

      {overview.isLoading ? (
        <div className="grid gap-4 md:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-28" />
          ))}
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-4">
          <StatCard
            label={t("dashboard.stats.subsystems")}
            value={formatNumber(data.filter((d) => d.files > 0).length)}
            icon={Database}
            hint={t("dashboard.stats.subsystemsHint", { count: data.length })}
          />
          <StatCard
            label={t("dashboard.stats.totalFiles")}
            value={formatCompact(totalFiles)}
            icon={Files}
            hint={t("dashboard.stats.totalFilesHint", { count: totalFiles })}
          />
          <StatCard
            label={t("dashboard.stats.onDisk")}
            value={formatBytes(totalSize)}
            icon={HardDrive}
            hint={
              settings.data?.free_disk_bytes != null
                ? t("dashboard.stats.onDiskHintFree", {
                    free: formatBytes(settings.data.free_disk_bytes),
                  })
                : "—"
            }
          />
          <StatCard
            label={t("dashboard.stats.rows")}
            value={formatCompact(totalRows)}
            icon={Rows3}
            hint={t("dashboard.stats.rowsHint", { count: distinctUfs.size })}
          />
        </div>
      )}

      {overview.error && hasDataDir ? (
        <Card>
          <CardContent className="p-6 text-sm text-destructive">
            {t("dashboard.loadFailed", {
              error:
                overview.error instanceof Error
                  ? overview.error.message
                  : t("common.unknownError"),
            })}
          </CardContent>
        </Card>
      ) : null}

      {noData ? (
        <EmptyState
          icon={Download}
          title={t("dashboard.noDatasetsTitle")}
          description={t("dashboard.noDatasetsDesc")}
          action={
            <Button onClick={goToDownload}>
              <Download className="h-4 w-4" />
              {t("dashboard.downloadFirst")}
            </Button>
          }
        />
      ) : (
        <>
          <section>
            <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-muted-foreground">
              {t("dashboard.subsystemsHeading")}
            </h2>
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {overview.isLoading
                ? Array.from({ length: 3 }).map((_, i) => (
                    <Skeleton key={i} className="h-56" />
                  ))
                : data.map((summary) => {
                    const info = settings.data?.subsystems.find(
                      (s) => s.name === summary.subsystem,
                    );
                    return (
                      <SubsystemCard
                        key={summary.subsystem}
                        summary={summary}
                        description={info?.description}
                      />
                    );
                  })}
            </div>
          </section>

          <section className="grid gap-4 lg:grid-cols-3">
            <Card className="lg:col-span-2">
              <CardHeader className="flex flex-row items-center justify-between space-y-0">
                <CardTitle className="text-base">
                  {t("dashboard.volumeOverTime")}
                  {chartSubsystem ? (
                    <span className="ml-2 font-mono text-xs font-normal uppercase text-muted-foreground">
                      {chartSubsystem}
                    </span>
                  ) : null}
                </CardTitle>
                <div className="flex gap-1">
                  {data
                    .filter((d) => d.files > 0)
                    .map((d) => (
                      <button
                        key={d.subsystem}
                        onClick={() => setChartSubsystem(d.subsystem)}
                        className={`rounded px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide transition-colors ${
                          chartSubsystem === d.subsystem
                            ? "bg-secondary text-foreground"
                            : "text-muted-foreground hover:bg-secondary/50"
                        }`}
                      >
                        {d.subsystem}
                      </button>
                    ))}
                </div>
              </CardHeader>
              <CardContent>
                {!chartSubsystem || !timeline.data || timeline.data.length === 0 ? (
                  <p className="py-8 text-center text-sm text-muted-foreground">
                    {t("dashboard.populateTimeline")}
                  </p>
                ) : (
                  <VolumeChart data={timeline.data} />
                )}
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">{t("dashboard.coverageByUf")}</CardTitle>
              </CardHeader>
              <CardContent>
                <BrazilMap valuesByUf={valuesByUf} readOnly />
              </CardContent>
            </Card>
          </section>
        </>
      )}
    </div>
  );
}
