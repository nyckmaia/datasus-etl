// Visual mapping for the per-column "distinct value count" shown next to
// each entry in the /query Columns panel. Mirrors columnType / columnFillPct
// in shape so the three badges line up with consistent height and rhythm,
// but commits to a single tonal lane (indigo) — the number itself conveys
// magnitude, color would just compete with the other two metadata badges.
//
// The badge is **clickable**: tapping it writes a histogram query to the
// SQL editor (see Query.tsx). The hover state hints at this with a
// foreground tint and a cursor change.

export interface DistinctCountMeta {
  /** Compact label suitable for a 7-character-wide badge: "12", "1.2K", "5,2M". */
  label: string;
  /** Tailwind classes for the (clickable) pill. */
  className: string;
  /** True when the value is missing/unknown — the badge shows "?" and is non-clickable. */
  isUnknown: boolean;
}

const KNOWN_STYLES =
  "bg-indigo-500/10 text-indigo-700 ring-1 ring-inset ring-indigo-500/30 " +
  "hover:bg-indigo-500/20 hover:ring-indigo-500/50 active:bg-indigo-500/30 " +
  "dark:bg-indigo-400/15 dark:text-indigo-300 dark:ring-indigo-400/30 " +
  "dark:hover:bg-indigo-400/25 dark:hover:ring-indigo-400/50 " +
  "dark:active:bg-indigo-400/35 " +
  "cursor-pointer transition-colors";

const UNKNOWN_STYLES =
  "bg-muted text-muted-foreground ring-1 ring-inset ring-border/60";

/**
 * Format a count compactly for the badge:
 *   847       → "847"
 *   1234      → "1.2K"
 *   12_345    → "12K"
 *   1_234_567 → "1.2M"
 *   0         → "0"
 *
 * We keep "0" intentionally — a column reporting 0 distinct values is
 * surprising and worth surfacing, not hiding under a "?" badge.
 */
function formatCompact(n: number): string {
  if (!Number.isFinite(n) || n < 0) return "?";
  if (n < 1000) return String(n);
  if (n < 10_000) {
    // 1.2K, 9.9K — keep one decimal for resolution under 10K
    const v = n / 1000;
    return `${v.toFixed(1).replace(/\.0$/, "")}K`;
  }
  if (n < 1_000_000) {
    return `${Math.round(n / 1000)}K`;
  }
  if (n < 10_000_000) {
    return `${(n / 1_000_000).toFixed(1).replace(/\.0$/, "")}M`;
  }
  return `${Math.round(n / 1_000_000)}M`;
}

/**
 * Map a distinct-value count to its display metadata.
 *
 * @param count  Measured (or inherited) distinct count.
 * @param approx When true, the value is an UPPER BOUND inherited from a
 *               JOINed reference table (IBGE-enriched columns). The label
 *               swaps the `#` prefix for `~` so the user can spot the
 *               difference at a glance; tone stays the same.
 */
export function distinctCountMeta(
  count: number | null | undefined,
  approx = false,
): DistinctCountMeta {
  if (count == null || Number.isNaN(count)) {
    return {
      label: "?",
      className: UNKNOWN_STYLES,
      isUnknown: true,
    };
  }
  const compact = formatCompact(count);
  return {
    label: approx ? `~${compact}` : `#${compact}`,
    className: KNOWN_STYLES,
    isUnknown: false,
  };
}
