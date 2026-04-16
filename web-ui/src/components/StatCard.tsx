import * as React from "react";

import { cn } from "@/lib/utils";
import { Card, CardContent } from "@/components/ui/card";

interface StatCardProps {
  label: string;
  value: React.ReactNode;
  icon?: React.ElementType;
  hint?: React.ReactNode;
  trend?: {
    value: string;
    direction: "up" | "down" | "neutral";
  };
  className?: string;
}

export function StatCard({
  label,
  value,
  icon: Icon,
  hint,
  trend,
  className,
}: StatCardProps) {
  return (
    <Card className={cn("relative overflow-hidden", className)}>
      <CardContent className="p-5">
        <div className="flex items-start justify-between gap-2">
          <div className="text-sm font-medium text-muted-foreground">{label}</div>
          {Icon ? (
            <Icon className="h-4 w-4 text-muted-foreground" aria-hidden />
          ) : null}
        </div>
        <div className="mt-2 font-mono text-3xl font-semibold tabular-nums tracking-tight">
          {value}
        </div>
        <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
          {trend ? (
            <span
              className={cn(
                "font-medium",
                trend.direction === "up" && "text-primary",
                trend.direction === "down" && "text-destructive",
              )}
            >
              {trend.value}
            </span>
          ) : null}
          {hint ? <span>{hint}</span> : null}
        </div>
      </CardContent>
    </Card>
  );
}
