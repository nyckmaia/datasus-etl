import * as React from "react";
import { useTranslation } from "react-i18next";

import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { formatBytes, formatPercent } from "@/lib/format";
import { cn } from "@/lib/utils";

const LOW_FREE_BYTES = 5 * 1024 ** 3;

const LABEL_VISIBLE_RATIO = 0.18;

type Tier = "ok" | "warn" | "danger";

function tierFor(usedRatio: number, freeBytes: number): Tier {
  if (usedRatio >= 0.9 || freeBytes < LOW_FREE_BYTES) return "danger";
  if (usedRatio >= 0.7) return "warn";
  return "ok";
}

const USED_BG: Record<Tier, string> = {
  ok: "bg-primary",
  warn: "bg-amber-500",
  danger: "bg-destructive",
};

const USED_FG: Record<Tier, string> = {
  ok: "text-primary-foreground",
  warn: "text-amber-50",
  danger: "text-destructive-foreground",
};

interface Props {
  freeBytes: number;
  totalBytes: number;
  className?: string;
}

export function DiskUsageBar({ freeBytes, totalBytes, className }: Props) {
  const { t } = useTranslation();

  if (totalBytes <= 0) return null;

  const usedBytes = Math.max(0, totalBytes - freeBytes);
  const usedRatio = Math.min(1, Math.max(0, usedBytes / totalBytes));
  const freeRatio = 1 - usedRatio;
  const tier = tierFor(usedRatio, freeBytes);

  // Animate from 0 → final ratio on mount so the bar fills in instead of
  // snapping; re-trigger when the data dir (and therefore totalBytes) changes.
  const [animatedRatio, setAnimatedRatio] = React.useState(0);
  React.useEffect(() => {
    setAnimatedRatio(0);
    const id = window.requestAnimationFrame(() => setAnimatedRatio(usedRatio));
    return () => window.cancelAnimationFrame(id);
  }, [totalBytes, usedRatio]);

  const showUsedLabel = usedRatio >= LABEL_VISIBLE_RATIO;
  const showFreeLabel = freeRatio >= LABEL_VISIBLE_RATIO;

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div
          role="img"
          aria-label={t("topBar.disk.aria", {
            used: formatBytes(usedBytes),
            free: formatBytes(freeBytes),
            total: formatBytes(totalBytes),
            percent: formatPercent(usedRatio),
          })}
          className={cn(
            "flex h-5 w-[240px] shrink-0 select-none overflow-hidden rounded-full border border-border/60 bg-muted/40 shadow-inner",
            className,
          )}
        >
          <div
            className={cn(
              "flex items-center justify-center px-2 transition-[width] duration-700 ease-out",
              USED_BG[tier],
            )}
            style={{ width: `${animatedRatio * 100}%` }}
          >
            {showUsedLabel ? (
              <span
                className={cn(
                  "truncate text-xs font-semibold leading-none tabular-nums",
                  USED_FG[tier],
                )}
              >
                {formatBytes(usedBytes)}
              </span>
            ) : null}
          </div>
          <div
            className="flex flex-1 items-center justify-center px-2 transition-[width] duration-700 ease-out"
            style={{ width: `${(1 - animatedRatio) * 100}%` }}
          >
            {showFreeLabel ? (
              <span className="truncate text-xs font-semibold leading-none tabular-nums text-foreground/70">
                {formatBytes(freeBytes)} {t("topBar.disk.free")}
              </span>
            ) : null}
          </div>
        </div>
      </TooltipTrigger>
      <TooltipContent side="bottom" align="start" className="px-3 py-2">
        <div className="mb-1.5 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          {t("topBar.disk.tooltipTitle")}
        </div>
        <BreakdownRow
          label={t("topBar.disk.used")}
          value={formatBytes(usedBytes)}
          hint={formatPercent(usedRatio)}
          dotClass={USED_BG[tier]}
        />
        <BreakdownRow
          label={t("topBar.disk.free")}
          value={formatBytes(freeBytes)}
          hint={formatPercent(freeRatio)}
          dotClass="bg-muted-foreground/30"
        />
        <BreakdownRow
          label={t("topBar.disk.total")}
          value={formatBytes(totalBytes)}
          dotClass="bg-transparent border border-border"
        />
      </TooltipContent>
    </Tooltip>
  );
}

function BreakdownRow({
  label,
  value,
  hint,
  dotClass,
}: {
  label: string;
  value: string;
  hint?: string;
  dotClass: string;
}) {
  return (
    <div className="flex items-center gap-3 py-0.5 text-sm">
      <span className={cn("h-2 w-2 shrink-0 rounded-full", dotClass)} />
      <span className="flex-1 text-muted-foreground">{label}</span>
      <span className="font-semibold tabular-nums">{value}</span>
      {hint ? (
        <span className="w-12 text-right tabular-nums text-muted-foreground">
          {hint}
        </span>
      ) : (
        <span className="w-12" />
      )}
    </div>
  );
}
