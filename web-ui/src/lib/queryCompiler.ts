// SQL compiler for the in-house Question Builder. Pure, side-effect-free —
// takes a structured QueryBuilderState and emits a DuckDB-flavoured SQL
// string plus a small set of validation diagnostics.
//
// Identifier hardening: every column reference is wrapped in double quotes
// so reserved words and mixed-case columns work; embedded quotes are
// escaped by doubling. Subsystem names come from a server-side whitelist
// (sihsus / sim / siasus), so they're inlined unquoted to match the look
// of the existing templates.

export type AggKind =
  | "none"
  | "count"
  | "count_distinct"
  | "sum"
  | "avg"
  | "min"
  | "max";

export interface Projection {
  /** Dictionary column name (raw, unquoted). */
  column: string;
  aggregation: AggKind;
}

export interface SortSpec {
  /** Either a raw column name or an alias produced by a projection. */
  column: string;
  direction: "ASC" | "DESC";
}

export interface QueryBuilderState {
  projections: Projection[];
  /** Columns that go into GROUP BY. Must be a subset of non-aggregated projections. */
  groupBy: string[];
  /** SQL fragment produced by react-querybuilder's `formatQuery(state, "sql")`. May be empty. */
  whereSql: string;
  orderBy?: SortSpec | null;
  limit?: number | null;
}

export interface CompileResult {
  sql: string;
  /** Human-readable problems that block Apply (e.g. "no columns selected"). */
  errors: string[];
  /** Soft hints that don't block Apply but should be surfaced. */
  warnings: string[];
}

const SAFE_IDENT = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

function quoteIdent(name: string): string {
  // Always quote — handles reserved words, accents, mixed case, and any
  // future identifier that wouldn't be SAFE_IDENT-clean.
  return `"${name.replace(/"/g, '""')}"`;
}

function aliasFor(p: Projection): string | null {
  switch (p.aggregation) {
    case "none":
      return null;
    case "count":
      return `count_${p.column}`;
    case "count_distinct":
      return `count_distinct_${p.column}`;
    case "sum":
      return `sum_${p.column}`;
    case "avg":
      return `avg_${p.column}`;
    case "min":
      return `min_${p.column}`;
    case "max":
      return `max_${p.column}`;
  }
}

function selectFragment(p: Projection): string {
  const ref = quoteIdent(p.column);
  switch (p.aggregation) {
    case "none":
      return ref;
    case "count":
      return `COUNT(${ref}) AS ${quoteIdent(aliasFor(p)!)}`;
    case "count_distinct":
      return `COUNT(DISTINCT ${ref}) AS ${quoteIdent(aliasFor(p)!)}`;
    case "sum":
      return `SUM(${ref}) AS ${quoteIdent(aliasFor(p)!)}`;
    case "avg":
      return `AVG(${ref}) AS ${quoteIdent(aliasFor(p)!)}`;
    case "min":
      return `MIN(${ref}) AS ${quoteIdent(aliasFor(p)!)}`;
    case "max":
      return `MAX(${ref}) AS ${quoteIdent(aliasFor(p)!)}`;
  }
}

export function compileQuery(
  state: QueryBuilderState,
  subsystem: string,
): CompileResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  if (!subsystem) errors.push("subsystem.required");
  if (state.projections.length === 0) errors.push("projections.empty");

  const aggregated = state.projections.filter((p) => p.aggregation !== "none");
  const rawProjected = state.projections
    .filter((p) => p.aggregation === "none")
    .map((p) => p.column);
  const groupBySet = new Set(state.groupBy);

  // If any aggregation exists, every non-aggregated projection must appear
  // in GROUP BY — otherwise DuckDB raises "must appear in GROUP BY clause".
  if (aggregated.length > 0) {
    for (const col of rawProjected) {
      if (!groupBySet.has(col)) {
        errors.push(`projection.missingGroupBy:${col}`);
      }
    }
  }

  // GROUP BY columns must not double up as aggregated projections.
  for (const col of state.groupBy) {
    const conflict = aggregated.find((p) => p.column === col);
    if (conflict) {
      errors.push(`groupBy.conflictsWithAgg:${col}`);
    }
  }

  // Group-by-only-with-aggregation: GROUP BY without any aggregation is a
  // legal SQL but almost never what the user wants — surface as a warning.
  if (state.groupBy.length > 0 && aggregated.length === 0) {
    warnings.push("groupBy.noAggregation");
  }

  // Build the SQL even when invalid — the live preview is more useful with
  // a partial query than with nothing. Apply is the only thing the errors
  // gate.
  const lines: string[] = [];

  // SELECT … with each projection on its own line indented 4 spaces, so
  // long projection lists stay readable inside the editor.
  if (state.projections.length === 0) {
    lines.push("SELECT");
  } else {
    const fragments = state.projections.map(selectFragment);
    const indented = fragments
      .map((frag, i) => `    ${frag}${i < fragments.length - 1 ? "," : ""}`)
      .join("\n");
    lines.push(`SELECT\n${indented}`);
  }
  lines.push(`FROM ${subsystem || "<subsystem>"}`);

  if (state.whereSql.trim()) {
    lines.push(`WHERE ${state.whereSql.trim()}`);
  }

  if (state.groupBy.length > 0) {
    lines.push(
      "GROUP BY " + state.groupBy.map(quoteIdent).join(", "),
    );
  }

  if (state.orderBy && state.orderBy.column) {
    // Match by alias first, then by raw column. We trust the caller to pass
    // either a raw column name or an alias produced by selectFragment.
    const alias = state.projections
      .map(aliasFor)
      .filter((a): a is string => Boolean(a))
      .find((a) => a === state.orderBy!.column);
    const ref = alias
      ? quoteIdent(alias)
      : quoteIdent(state.orderBy.column);
    lines.push(`ORDER BY ${ref} ${state.orderBy.direction}`);
  }

  if (state.limit && state.limit > 0) {
    lines.push(`LIMIT ${Math.floor(state.limit)}`);
  }

  return {
    sql: lines.join("\n") + ";",
    errors,
    warnings,
  };
}

/** Helper for callers that want to enumerate the alias of a projection
 *  (e.g. to populate the Sort dropdown with all referenceable column names). */
export function projectionLabel(p: Projection): string {
  return aliasFor(p) ?? p.column;
}

/** Names that the Sort step can legitimately reference: raw column names of
 *  un-aggregated projections plus aliases of aggregated ones. */
export function sortableTargets(
  projections: Projection[],
): { column: string; label: string }[] {
  return projections.map((p) => {
    const alias = aliasFor(p);
    return alias
      ? { column: alias, label: alias }
      : { column: p.column, label: p.column };
  });
}

// Re-export so callers don't need a deep import path. Keeps the module a
// single landing page for "everything builder-related".
export { SAFE_IDENT };
