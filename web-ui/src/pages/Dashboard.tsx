import * as React from "react";
import { Link } from "@tanstack/react-router";
import { Download, Database, HardDrive, Files, Rows3 } from "lucide-react";

import { StatCard } from "@/components/StatCard";
import { SubsystemCard } from "@/components/SubsystemCard";
import { VolumeChart } from "@/components/VolumeChart";
import { BrazilMap } from "@/components/BrazilMap";
import { EmptyState } from "@/components/EmptyState";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useStatsOverview, useTimeline } from "@/hooks/useStats";
import { useSettings } from "@/hooks/useSettings";
import { formatBytes, formatCompact, formatNumber } from "@/lib/format";

export function DashboardPage() {
  const settings = useSettings();
  const overview = useStatsOverview(true);

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

  const noData = !overview.isLoading && data.every((d) => d.files === 0);

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>
          <p className="text-sm text-muted-foreground">
            Overview of your local DataSUS parquet storage.
          </p>
        </div>
        <Button asChild>
          <Link to="/download">
            <Download className="h-4 w-4" />
            New download
          </Link>
        </Button>
      </div>

      {overview.isLoading ? (
        <div className="grid gap-4 md:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-28" />
          ))}
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-4">
          <StatCard
            label="Subsystems"
            value={formatNumber(data.length)}
            icon={Database}
            hint={`${data.filter((d) => d.files > 0).length} with data`}
          />
          <StatCard
            label="Total files"
            value={formatCompact(totalFiles)}
            icon={Files}
            hint={`${formatNumber(totalFiles)} parquet files`}
          />
          <StatCard
            label="On disk"
            value={formatBytes(totalSize)}
            icon={HardDrive}
            hint={
              settings.data?.free_disk_bytes != null
                ? `${formatBytes(settings.data.free_disk_bytes)} free`
                : "—"
            }
          />
          <StatCard
            label="Rows"
            value={formatCompact(totalRows)}
            icon={Rows3}
            hint={`${distinctUfs.size} UFs covered`}
          />
        </div>
      )}

      {overview.error ? (
        <Card>
          <CardContent className="p-6 text-sm text-destructive">
            Failed to load overview:{" "}
            {overview.error instanceof Error ? overview.error.message : "unknown error"}
          </CardContent>
        </Card>
      ) : null}

      {noData ? (
        <EmptyState
          icon={Download}
          title="No datasets yet"
          description="Once you download DataSUS data into your local store, this dashboard will show file counts, storage size, and a coverage map."
          action={
            <Button asChild>
              <Link to="/download">
                <Download className="h-4 w-4" />
                Download your first dataset
              </Link>
            </Button>
          }
        />
      ) : (
        <>
          <section>
            <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-muted-foreground">
              Subsystems
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
                  Volume over time
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
                    Download a subsystem to populate the timeline.
                  </p>
                ) : (
                  <VolumeChart data={timeline.data} />
                )}
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Coverage by UF</CardTitle>
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
