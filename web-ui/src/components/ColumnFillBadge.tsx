import { fillPctMeta } from "@/lib/columnFillPct";
import { cn } from "@/lib/utils";

interface ColumnFillBadgeProps {
  /** Percentage of non-NULL values in this column (0–100). null = unknown. */
  fillPct: number | null | undefined;
  /**
   * Set when the value was inherited from a residence-municipality join
   * column rather than measured directly. The badge shows a `~` prefix and
   * the tooltip explains it's an upper bound on JOIN success.
   */
  approx?: boolean;
  className?: string;
}

/**
 * Small color-coded badge showing how complete a column's data is.
 *
 * Sits next to the type-tag in the /query Columns panel; same height/shape
 * for visual rhythm but its own tonal bucket scale. Hover reveals the exact
 * percentage via the native `title` attribute (the visible label rounds to
 * one decimal so the panel stays readable).
 */
export function ColumnFillBadge({
  fillPct,
  approx = false,
  className,
}: ColumnFillBadgeProps) {
  const { label, badgeClassName, bucket } = fillPctMeta(fillPct, approx);

  let tooltip: string;
  if (fillPct == null) {
    tooltip = "% preenchido ainda não calculado";
  } else if (approx) {
    // Upper bound — every IBGE column is NULL whenever the JOIN with
    // ibge_locais failed, so its fill_pct can be at most the fill_pct of the
    // join column (codmunres / munic_res).
    tooltip =
      `~${fillPct.toFixed(2)}% — limite superior baseado na coluna de junção. ` +
      `O valor real pode ser menor se houver códigos não cadastrados em ibge_locais.`;
  } else {
    tooltip = `${fillPct.toFixed(2)}% dos valores estão preenchidos`;
  }

  return (
    <span
      title={tooltip}
      data-bucket={bucket}
      data-approx={approx ? "true" : undefined}
      className={cn(
        "inline-flex h-[18px] shrink-0 items-center justify-center rounded-[5px] px-1.5",
        "font-mono text-[10px] font-semibold uppercase leading-none tracking-tight",
        "tabular-nums",
        badgeClassName,
        className,
      )}
    >
      {label}
    </span>
  );
}
