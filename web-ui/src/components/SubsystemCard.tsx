import { Link } from "@tanstack/react-router";
import { Database, Download, ArrowRight } from "lucide-react";

import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { formatBytes, formatCompact, formatRelative } from "@/lib/format";
import { ALL_UFS } from "./BrazilMap";
import type { SubsystemSummary } from "@/lib/api";

interface SubsystemCardProps {
  summary: SubsystemSummary;
  description?: string;
}

export function SubsystemCard({ summary, description }: SubsystemCardProps) {
  const coveredUfs = new Set(summary.ufs);
  const coverage = coveredUfs.size / ALL_UFS.length;

  return (
    <Card className="group relative overflow-hidden transition-colors hover:border-primary/50">
      <CardContent className="p-5">
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-2">
              <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                Subsystem
              </span>
              {summary.files === 0 ? (
                <Badge variant="outline">Empty</Badge>
              ) : (
                <Badge variant="success">Active</Badge>
              )}
            </div>
            <h3 className="mt-1 font-mono text-xl font-semibold uppercase tracking-wide">
              {summary.subsystem}
            </h3>
            {description ? (
              <p className="mt-1 line-clamp-2 text-sm text-muted-foreground">
                {description}
              </p>
            ) : null}
          </div>
          <Database className="h-4 w-4 text-muted-foreground" />
        </div>

        <div className="mt-5 grid grid-cols-3 gap-3 text-sm">
          <div>
            <div className="text-[11px] uppercase tracking-wide text-muted-foreground">
              Files
            </div>
            <div className="mt-0.5 font-mono text-base font-semibold tabular-nums">
              {formatCompact(summary.files)}
            </div>
          </div>
          <div>
            <div className="text-[11px] uppercase tracking-wide text-muted-foreground">
              Size
            </div>
            <div className="mt-0.5 font-mono text-base font-semibold tabular-nums">
              {formatBytes(summary.size_bytes)}
            </div>
          </div>
          <div>
            <div className="text-[11px] uppercase tracking-wide text-muted-foreground">
              Rows
            </div>
            <div className="mt-0.5 font-mono text-base font-semibold tabular-nums">
              {summary.row_count != null ? formatCompact(summary.row_count) : "—"}
            </div>
          </div>
        </div>

        <div className="mt-4">
          <div className="flex items-center justify-between text-[11px] text-muted-foreground">
            <span>UF coverage</span>
            <span className="tabular-nums">
              {coveredUfs.size} / {ALL_UFS.length}
            </span>
          </div>
          <Progress value={coverage * 100} className="mt-1 h-1.5" />
        </div>

        <div className="mt-4 flex items-center justify-between text-xs text-muted-foreground">
          <span>
            {summary.first_period && summary.last_period
              ? `${summary.first_period} → ${summary.last_period}`
              : "No periods"}
          </span>
          <span>Updated {formatRelative(summary.last_updated)}</span>
        </div>

        <div className="mt-4 flex items-center gap-2">
          <Button asChild size="sm" variant="secondary" className="flex-1">
            <Link to="/query" search={{ subsystem: summary.subsystem }}>
              <Database className="h-3.5 w-3.5" />
              Query
            </Link>
          </Button>
          <Button asChild size="sm" className="flex-1">
            <Link
              to="/download/step-2"
              search={{ subsystem: summary.subsystem }}
            >
              <Download className="h-3.5 w-3.5" />
              Update
              <ArrowRight className="h-3.5 w-3.5" />
            </Link>
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
