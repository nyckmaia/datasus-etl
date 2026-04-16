import * as React from "react";
import { useMutation } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { CheckCircle2, Loader2, XCircle, Square, Home } from "lucide-react";
import { toast } from "sonner";

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
  const { state, reset } = useWizard();
  const navigate = useNavigate();
  const run = usePipelineRun(state.runId);

  const cancelMutation = useMutation({
    mutationFn: () => {
      if (!state.runId) throw new Error("no runId");
      return api.cancelPipeline(state.runId);
    },
    onSuccess: () => toast.success("Cancellation requested"),
    onError: (err: Error) =>
      toast.error("Cancel failed", { description: err.message }),
  });

  if (!state.runId) {
    return (
      <Card>
        <CardContent className="p-6 text-sm text-muted-foreground">
          No active run. Start one from the previous step.
        </CardContent>
      </Card>
    );
  }

  const pct = Math.round((run.progress || 0) * 100);
  const isPreparing = run.phase === "preparing";
  const statusBadge = (() => {
    if (isPreparing) return <Badge variant="secondary">Preparing</Badge>;
    switch (run.status) {
      case "running":
        return <Badge>Running</Badge>;
      case "done":
        return <Badge variant="success">Done</Badge>;
      case "error":
        return <Badge variant="destructive">Error</Badge>;
      case "cancelled":
        return <Badge variant="secondary">Cancelled</Badge>;
      default:
        return <Badge variant="outline">{run.status || "pending"}</Badge>;
    }
  })();

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-xl font-semibold">Running</h2>
        <p className="text-sm text-muted-foreground">
          Live progress stream from the backend.
        </p>
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0">
          <CardTitle className="text-base">
            {state.subsystem?.toUpperCase()} — run {state.runId.slice(0, 8)}
          </CardTitle>
          {statusBadge}
        </CardHeader>
        <CardContent className="space-y-4">
          {isPreparing ? (
            <div className="flex items-start gap-3 rounded-md border border-dashed border-primary/40 bg-primary/5 p-4">
              <Loader2 className="mt-0.5 h-5 w-5 shrink-0 animate-spin text-primary" />
              <div className="space-y-1">
                <div className="text-sm font-medium">Preparing download…</div>
                <div className="text-xs text-muted-foreground">
                  Listing files on the DATASUS FTP and resolving the scope. For
                  large date/UF ranges this can take a couple of minutes. The
                  progress bar starts moving once transfers begin.
                </div>
                <div className="pt-1 font-mono text-xs text-foreground/80">
                  {run.message || "Connecting…"}
                </div>
              </div>
            </div>
          ) : (
            <div>
              <div className="mb-1 flex items-center justify-between text-xs text-muted-foreground">
                <span className="truncate pr-2">{run.message || "Waiting..."}</span>
                <span className="tabular-nums">{pct}%</span>
              </div>
              <Progress value={pct} />
            </div>
          )}

          <ScrollArea className="h-64 rounded-md border bg-muted/30">
            <div className="p-3 font-mono text-xs leading-relaxed">
              {run.logs.length === 0 ? (
                <div className="text-muted-foreground">No events yet.</div>
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
          Back to dashboard
        </Button>
        {run.finished ? (
          <Button onClick={() => navigate({ to: "/query" })}>
            {run.status === "done" ? (
              <CheckCircle2 className="h-4 w-4" />
            ) : (
              <XCircle className="h-4 w-4" />
            )}
            Go to query
          </Button>
        ) : (
          <Button
            variant="destructive"
            disabled={cancelMutation.isPending}
            onClick={() => cancelMutation.mutate()}
          >
            <Square className="h-4 w-4" />
            Cancel run
          </Button>
        )}
      </div>
    </div>
  );
}
