// Mapping of DuckDB SQL types (as returned by /api/query/dictionary) to a
// short, human-friendly abbreviation + color tone. Used by ColumnTypeBadge.
// Each abbrev gets a distinct hue so the "Colunas" panel can be scanned at
// a glance — int/float share the cool-blue family, str sits in green, time-
// related types live in the warm red/orange/pink range, list is neutral.

export type ColumnTypeAbbrev =
  | "int"
  | "float"
  | "str"
  | "bool"
  | "date"
  | "time"
  | "ts"
  | "list"
  | "?";

export interface ColumnTypeMeta {
  abbrev: ColumnTypeAbbrev;
  // Tailwind class string. Each tone uses a low-alpha tinted background and
  // a saturated text color, with a subtle ring for definition. Carefully
  // tuned so the same badge reads well in light *and* dark themes without
  // looking neon in either.
  className: string;
  // Long form for the tooltip (the original DuckDB type, lowercased).
  fullType: string;
}

const STYLES: Record<ColumnTypeAbbrev, string> = {
  int:
    "bg-sky-500/10 text-sky-700 ring-1 ring-inset ring-sky-500/30 " +
    "dark:bg-sky-400/15 dark:text-sky-300 dark:ring-sky-400/30",
  float:
    "bg-cyan-500/10 text-cyan-700 ring-1 ring-inset ring-cyan-500/30 " +
    "dark:bg-cyan-400/15 dark:text-cyan-300 dark:ring-cyan-400/30",
  str:
    "bg-emerald-500/10 text-emerald-700 ring-1 ring-inset ring-emerald-500/30 " +
    "dark:bg-emerald-400/15 dark:text-emerald-300 dark:ring-emerald-400/30",
  bool:
    "bg-violet-500/10 text-violet-700 ring-1 ring-inset ring-violet-500/30 " +
    "dark:bg-violet-400/15 dark:text-violet-300 dark:ring-violet-400/30",
  date:
    "bg-amber-500/10 text-amber-700 ring-1 ring-inset ring-amber-500/30 " +
    "dark:bg-amber-400/15 dark:text-amber-300 dark:ring-amber-400/30",
  time:
    "bg-rose-500/10 text-rose-700 ring-1 ring-inset ring-rose-500/30 " +
    "dark:bg-rose-400/15 dark:text-rose-300 dark:ring-rose-400/30",
  ts:
    "bg-fuchsia-500/10 text-fuchsia-700 ring-1 ring-inset ring-fuchsia-500/30 " +
    "dark:bg-fuchsia-400/15 dark:text-fuchsia-300 dark:ring-fuchsia-400/30",
  list:
    "bg-slate-500/10 text-slate-700 ring-1 ring-inset ring-slate-500/30 " +
    "dark:bg-slate-400/15 dark:text-slate-300 dark:ring-slate-400/30",
  "?":
    "bg-muted text-muted-foreground ring-1 ring-inset ring-border/60",
};

// Aggregation eligibility. Used by the visual Question Builder so the UI
// only offers aggregations that DuckDB will actually evaluate without
// raising a type error (SUM on VARCHAR, etc.).
export function isNumericType(rawType: string | undefined): boolean {
  const { abbrev } = abbreviateColumnType(rawType);
  return abbrev === "int" || abbrev === "float";
}

export function isComparableType(rawType: string | undefined): boolean {
  const { abbrev } = abbreviateColumnType(rawType);
  return (
    abbrev === "int" ||
    abbrev === "float" ||
    abbrev === "date" ||
    abbrev === "time" ||
    abbrev === "ts" ||
    abbrev === "str"
  );
}

export function abbreviateColumnType(rawType: string | undefined): ColumnTypeMeta {
  const fullType = (rawType ?? "").trim();
  const upper = fullType.toUpperCase();
  let abbrev: ColumnTypeAbbrev;

  if (!upper) {
    abbrev = "?";
  } else if (upper.endsWith("[]")) {
    // Any array — VARCHAR[], INTEGER[], … — collapses to "list" so users
    // immediately see the column carries multiple values.
    abbrev = "list";
  } else if (
    upper === "TINYINT" ||
    upper === "SMALLINT" ||
    upper === "INTEGER" ||
    upper === "BIGINT" ||
    upper === "HUGEINT" ||
    upper === "UTINYINT" ||
    upper === "USMALLINT" ||
    upper === "UINTEGER" ||
    upper === "UBIGINT" ||
    upper === "INT" ||
    upper === "INT2" ||
    upper === "INT4" ||
    upper === "INT8"
  ) {
    abbrev = "int";
  } else if (
    upper === "FLOAT" ||
    upper === "DOUBLE" ||
    upper === "REAL" ||
    upper.startsWith("DECIMAL") ||
    upper.startsWith("NUMERIC")
  ) {
    abbrev = "float";
  } else if (
    upper === "VARCHAR" ||
    upper === "TEXT" ||
    upper === "STRING" ||
    upper === "CHAR" ||
    upper.startsWith("VARCHAR(") ||
    upper.startsWith("CHAR(")
  ) {
    abbrev = "str";
  } else if (upper === "BOOLEAN" || upper === "BOOL") {
    abbrev = "bool";
  } else if (upper === "DATE") {
    abbrev = "date";
  } else if (upper === "TIME") {
    abbrev = "time";
  } else if (upper.startsWith("TIMESTAMP") || upper === "DATETIME") {
    abbrev = "ts";
  } else {
    abbrev = "?";
  }

  return { abbrev, className: STYLES[abbrev], fullType };
}
