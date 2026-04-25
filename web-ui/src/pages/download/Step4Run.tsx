import * as React from "react";
import { useMutation } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { CheckCircle2, Loader2, XCircle, Square, Home } from "lucide-react";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { api } from "@/lib/api";
import { usePipelineRun } from "@/hooks/usePipelineRun";
import { cn } from "@/lib/utils";
import { useWizard } from "../DownloadWizard";

export function Step4RunPage() {
  const { t } = useTranslation();
  const { state, reset } = useWizard();
  const navigate = useNavigate();
  const run = usePipelineRun(state.runId);

  const logsContainerRef = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => {
    const viewport = logsContainerRef.current?.querySelector<HTMLDivElement>(
      "[data-radix-scroll-area-viewport]",
    );
    if (viewport) {
      viewport.scrollTop = viewport.scrollHeight;
    }
  }, [run.logs.length]);

  const cancelMutation = useMutation({
    mutationFn: () => {
      if (!state.runId) throw new Error("no runId");
      return api.cancelPipeline(state.runId);
    },
    onSuccess: () => toast.success(t("step4.cancelRequested")),
    onError: (err: Error) =>
      toast.error(t("step4.cancelFailed"), { description: err.message }),
  });

  if (!state.runId) {
    return (
      <Card>
        <CardContent className="p-6 text-sm text-muted-foreground">
          {t("step4.noActiveRun")}
        </CardContent>
      </Card>
    );
  }

  const isPreparing = run.phase === "preparing";

  // Stage manifest — order matches the Parquet-mode pipeline. Backend stage
  // IDs come from PipelineContext.register_stage(...) calls in
  // base_pipeline.py:setup_stages.
  const STAGES: { id: string; label: string }[] = [
    { id: "download", label: t("step4.stages.download") },
    { id: "dbc_to_dbf", label: t("step4.stages.dbcToDbf") },
    { id: "dbf_to_parquet", label: t("step4.stages.dbfToParquet") },
  ];
  const statusBadge = (() => {
    if (isPreparing) return <Badge variant="secondary">{t("step4.status.preparing")}</Badge>;
    switch (run.status) {
      case "running":
        return <Badge>{t("step4.status.running")}</Badge>;
      case "done":
        return <Badge variant="success">{t("step4.status.done")}</Badge>;
      case "error":
        return <Badge variant="destructive">{t("step4.status.error")}</Badge>;
      case "cancelled":
        return <Badge variant="secondary">{t("step4.status.cancelled")}</Badge>;
      default:
        return <Badge variant="outline">{run.status || t("step4.status.pending")}</Badge>;
    }
  })();

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-xl font-semibold">{t("step4.title")}</h2>
        <p className="text-sm text-muted-foreground">{t("step4.subtitle")}</p>
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0">
          <CardTitle className="text-base">
            {t("step4.runLabel", {
              subsystem: state.subsystem?.toUpperCase() ?? "",
              id: state.runId.slice(0, 8),
            })}
          </CardTitle>
          {statusBadge}
        </CardHeader>
        <CardContent className="space-y-4">
          {isPreparing ? (
            <div className="flex items-start gap-3 rounded-md border border-dashed border-primary/40 bg-primary/5 p-4">
              <Loader2 className="mt-0.5 h-5 w-5 shrink-0 animate-spin text-primary" />
              <div className="space-y-1">
                <div className="text-sm font-medium">{t("step4.preparingTitle")}</div>
                <div className="text-xs text-muted-foreground">{t("step4.preparingBody")}</div>
                <div className="pt-1 font-mono text-xs text-foreground/80">
                  {run.message || t("step4.connecting")}
                </div>
              </div>
            </div>
          ) : (
            <div className="space-y-3">
              <div className="truncate text-xs text-muted-foreground">
                {run.message || t("step4.waiting")}
              </div>
              {STAGES.map(({ id, label }) => {
                const frac = run.stageProgress[id] ?? 0;
                const pct = Math.round(frac * 100);
                const isActive = run.activeStage === id && !run.finished;
                return (
                  <div key={id}>
                    <div className="mb-1 flex items-center justify-between text-xs">
                      <span
                        className={cn(
                          "truncate pr-2",
                          isActive
                            ? "font-medium text-foreground"
                            : "text-muted-foreground",
                        )}
                      >
                        {label}
                      </span>
                      <span
                        className={cn(
                          "tabular-nums",
                          isActive ? "text-foreground" : "text-muted-foreground",
                        )}
                      >
                        {pct}%
                      </span>
                    </div>
                    <Progress value={pct} />
                  </div>
                );
              })}
            </div>
          )}

          <ScrollArea
            ref={logsContainerRef}
            className="h-64 rounded-md border bg-muted/30"
          >
            <div className="p-3 font-mono text-xs leading-relaxed">
              {run.logs.length === 0 ? (
                <div className="text-muted-foreground">{t("step4.noEvents")}</div>
              ) : (
                run.logs.map((log, i) => (
                  <div
                    key={i}
                    className={cn(
                      "flex gap-3",
                      log.type === "error" && "text-destructive",
                      log.type === "done" && "text-primary",
                      log.type === "prepare" && "text-muted-foreground",
                    )}
                  >
                    <span className="text-muted-foreground">
                      {new Date(log.ts).toLocaleTimeString()}
                    </span>
                    <span className="w-16 shrink-0 uppercase text-muted-foreground">
                      {log.type}
                    </span>
                    <span className="break-all">{log.message}</span>
                  </div>
                ))
              )}
            </div>
          </ScrollArea>

          {run.error ? (
            <div className="rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">
              {run.error}
            </div>
          ) : null}
        </CardContent>
      </Card>

      <div className="flex items-center justify-between">
        <Button
          variant="outline"
          onClick={() => {
            reset();
            navigate({ to: "/" });
          }}
        >
          <Home className="h-4 w-4" />
          {t("step4.backToDashboard")}
        </Button>
        {run.finished ? (
          <Button onClick={() => navigate({ to: "/query" })}>
            {run.status === "done" ? (
              <CheckCircle2 className="h-4 w-4" />
            ) : (
              <XCircle className="h-4 w-4" />
            )}
            {t("step4.goToQuery")}
          </Button>
        ) : (
          <Button
            variant="destructive"
            disabled={cancelMutation.isPending}
            onClick={() => cancelMutation.mutate()}
          >
            <Square className="h-4 w-4" />
            {t("step4.cancelRun")}
          </Button>
        )}
      </div>
    </div>
  );
}
