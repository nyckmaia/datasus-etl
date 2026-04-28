import { distinctCountMeta } from "@/lib/columnDistinctCount";
import { cn } from "@/lib/utils";

interface ColumnDistinctBadgeProps {
  /** Approximate distinct count from /api/query/dictionary; null = unknown. */
  count: number | null | undefined;
  /**
   * Set when the count is an UPPER BOUND inherited from a JOINed reference
   * (e.g. IBGE-enriched columns). Swaps the `#` prefix for `~` and the
   * tooltip explains the bound semantics.
   */
  approx?: boolean;
  /**
   * Fired when the user clicks the badge. The parent typically reacts by
   * writing a histogram query into the SQL editor and running it. Disabled
   * (no-op + visually muted) when count is null.
   */
  onClick?: () => void;
  className?: string;
}

/**
 * Compact `#N` badge showing how many distinct values a column carries.
 *
 * Sits to the right of the type-tag in the Columns panel. Clicking it asks
 * the parent to populate the SQL editor with a `GROUP BY column ORDER BY
 * count DESC LIMIT N` histogram query and run it — a one-tap "show me the
 * shape of this column" affordance.
 *
 * The visual lane is a single indigo tone: the type-tag uses the data-type
 * palette and the fill-pct badge uses the data-quality palette, so giving
 * distinct-count a *different* hue keeps the three readable side-by-side.
 */
export function ColumnDistinctBadge({
  count,
  approx = false,
  onClick,
  className,
}: ColumnDistinctBadgeProps) {
  const {
    label,
    className: toneClasses,
    isUnknown,
  } = distinctCountMeta(count, approx);

  const clickable = !isUnknown && typeof onClick === "function";

  let tooltip: string;
  if (isUnknown) {
    tooltip = "Contagem de valores distintos ainda não calculada";
  } else if (approx) {
    // Upper bound from the IBGE reference table — the actual JOIN result
    // can have at most this many distinct values, never more.
    tooltip =
      `~${count!.toLocaleString("pt-BR")} valores distintos (limite superior, ` +
      "herdado de ibge_locais). Clique para ver o histograma desta coluna.";
  } else {
    tooltip =
      `~${count!.toLocaleString("pt-BR")} valores distintos. ` +
      "Clique para ver o histograma desta coluna.";
  }

  // Render as a real <button> when clickable so keyboard / screen-reader
  // users get the affordance for free; otherwise a passive <span> matches
  // the other badges visually.
  if (clickable) {
    return (
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          onClick!();
        }}
        title={tooltip}
        aria-label={tooltip}
        data-approx={approx ? "true" : undefined}
        className={cn(
          "inline-flex h-[18px] shrink-0 items-center justify-center rounded-[5px] px-1.5",
          "font-mono text-[10px] font-semibold uppercase leading-none tracking-tight",
          "tabular-nums",
          toneClasses,
          className,
        )}
      >
        {label}
      </button>
    );
  }

  return (
    <span
      title={tooltip}
      data-approx={approx ? "true" : undefined}
      className={cn(
        "inline-flex h-[18px] shrink-0 items-center justify-center rounded-[5px] px-1.5",
        "font-mono text-[10px] font-semibold uppercase leading-none tracking-tight",
        "tabular-nums",
        toneClasses,
        className,
      )}
    >
      {label}
    </span>
  );
}
