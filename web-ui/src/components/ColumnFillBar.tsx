import { fillPctMeta } from "@/lib/columnFillPct";
import { cn } from "@/lib/utils";

interface ColumnFillBarProps {
  fillPct: number | null | undefined;
  className?: string;
}

/**
 * Thin (2 px) horizontal bar at the bottom of a column row showing the
 * non-NULL proportion at a glance. Reads as a "ruler" running underneath
 * each row — the user scans the column of bars and finds gaps without
 * looking at numbers. The numeric reading lives on the sibling
 * `<ColumnFillBadge>`; the bar's job is purely visual rhythm.
 *
 * Unknown (`fillPct = null`) renders an empty muted track so the row's
 * bottom edge stays consistent.
 */
export function ColumnFillBar({ fillPct, className }: ColumnFillBarProps) {
  const { barClassName, barWidthPct, bucket } = fillPctMeta(fillPct);

  return (
    <div
      aria-hidden
      data-bucket={bucket}
      className={cn(
        "mt-1.5 h-[2px] w-full overflow-hidden rounded-full bg-muted/40",
        className,
      )}
    >
      <div
        className={cn("h-full transition-all duration-500 ease-out", barClassName)}
        style={{ width: `${barWidthPct}%` }}
      />
    </div>
  );
}
