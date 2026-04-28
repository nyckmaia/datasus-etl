// Visual mapping for the per-column "fill percentage" (% of non-NULL values)
// shown in the /query Columns panel. Mirrors lib/columnType.ts in shape and
// in spirit — same low-saturation tinted backgrounds, same tone-pair pattern
// for light/dark themes, same `?` fallback for unknown.
//
// We frame it as **fill** (% present), not nulls — the user picked the
// positive framing during planning. Higher is better, color goes from
// muted-emerald (basically full) to rose (mostly empty).

export type FillBucket = "full" | "high" | "mid" | "low" | "unknown";

export interface FillPctMeta {
  bucket: FillBucket;
  /** Number to display in the badge: "99.6%", "14.3%", "0%", "?". */
  label: string;
  /** Tailwind classes for the pill badge (background + text + ring). */
  badgeClassName: string;
  /** Tailwind classes for the thin progress bar fill. */
  barClassName: string;
  /** Width of the bar fill (0–100, where 0 = empty visual). */
  barWidthPct: number;
}

/** Distinct tone pairs per bucket — matches the type-tag palette tone. */
const BUCKET_STYLES: Record<FillBucket, { badge: string; bar: string }> = {
  full: {
    badge:
      "bg-emerald-500/10 text-emerald-700 ring-1 ring-inset ring-emerald-500/30 " +
      "dark:bg-emerald-400/15 dark:text-emerald-300 dark:ring-emerald-400/30",
    bar: "bg-emerald-500/70 dark:bg-emerald-400/70",
  },
  high: {
    badge:
      "bg-sky-500/10 text-sky-700 ring-1 ring-inset ring-sky-500/30 " +
      "dark:bg-sky-400/15 dark:text-sky-300 dark:ring-sky-400/30",
    bar: "bg-sky-500/70 dark:bg-sky-400/70",
  },
  mid: {
    badge:
      "bg-amber-500/10 text-amber-700 ring-1 ring-inset ring-amber-500/30 " +
      "dark:bg-amber-400/15 dark:text-amber-300 dark:ring-amber-400/30",
    bar: "bg-amber-500/70 dark:bg-amber-400/70",
  },
  low: {
    badge:
      "bg-rose-500/10 text-rose-700 ring-1 ring-inset ring-rose-500/30 " +
      "dark:bg-rose-400/15 dark:text-rose-300 dark:ring-rose-400/30",
    bar: "bg-rose-500/70 dark:bg-rose-400/70",
  },
  unknown: {
    badge: "bg-muted text-muted-foreground ring-1 ring-inset ring-border/60",
    bar: "bg-muted-foreground/20",
  },
};

/** Round a percentage to one decimal place; 100 stays 100, 0 stays 0. */
function formatPct(pct: number): string {
  // Prefer integer formatting for the edges so the badge isn't "100.0%" /
  // "0.0%" — keeps the chrome quiet at the extremes where it matters most.
  if (pct >= 100) return "100%";
  if (pct <= 0) return "0%";
  return `${pct.toFixed(1)}%`;
}

/**
 * Map a numeric fill percentage to its display metadata.
 *
 * @param pct    Measured (or derived) fill percentage in [0, 100].
 * @param approx When true, the value was inherited from a join column rather
 *               than measured from the parquet footer (IBGE-enriched columns).
 *               The label gains a `~` prefix so the user can spot the
 *               difference at a glance; bucket and color stay the same.
 */
export function fillPctMeta(
  pct: number | null | undefined,
  approx = false,
): FillPctMeta {
  if (pct == null || Number.isNaN(pct)) {
    return {
      bucket: "unknown",
      label: "?",
      badgeClassName: BUCKET_STYLES.unknown.badge,
      barClassName: BUCKET_STYLES.unknown.bar,
      barWidthPct: 0,
    };
  }

  const clamped = Math.max(0, Math.min(100, pct));
  let bucket: FillBucket;
  if (clamped >= 99) bucket = "full";
  else if (clamped >= 90) bucket = "high";
  else if (clamped >= 50) bucket = "mid";
  else bucket = "low";

  const baseLabel = formatPct(clamped);
  return {
    bucket,
    label: approx ? `~${baseLabel}` : baseLabel,
    badgeClassName: BUCKET_STYLES[bucket].badge,
    barClassName: BUCKET_STYLES[bucket].bar,
    barWidthPct: clamped,
  };
}
