import { abbreviateColumnType } from "@/lib/columnType";
import { cn } from "@/lib/utils";

interface ColumnTypeBadgeProps {
  /** Raw DuckDB SQL type from /api/query/dictionary (e.g. "INTEGER", "VARCHAR[]"). */
  type: string | undefined;
  className?: string;
}

/**
 * Compact, color-coded type tag rendered next to a column name in the
 * "Colunas" panel. Each abbreviation (int / float / str / bool / date / time
 * / ts / list / ?) gets its own hue so users can scan the schema visually.
 *
 * The full DuckDB type is exposed as a native `title` tooltip — no extra
 * Radix wrapper, the panel is dense enough.
 */
export function ColumnTypeBadge({ type, className }: ColumnTypeBadgeProps) {
  const { abbrev, className: toneClasses, fullType } = abbreviateColumnType(type);
  return (
    <span
      title={fullType || abbrev}
      className={cn(
        "inline-flex h-[18px] shrink-0 items-center justify-center rounded-[5px] px-1.5",
        "font-mono text-[10px] font-semibold uppercase leading-none tracking-tight",
        "tabular-nums",
        toneClasses,
        className,
      )}
    >
      {abbrev}
    </span>
  );
}
