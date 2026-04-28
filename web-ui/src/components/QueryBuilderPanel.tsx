import * as React from "react";
import { useTranslation } from "react-i18next";
import { Trash2, AlertTriangle } from "lucide-react";
import { formatQuery, type RuleGroupType } from "react-querybuilder";
import Editor from "@monaco-editor/react";

import { Button } from "@/components/ui/button";
import { useLocalStorage } from "@/hooks/useLocalStorage";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { ColumnFillBadge } from "@/components/ColumnFillBadge";
import { ColumnTypeBadge } from "@/components/ColumnTypeBadge";
import {
  QueryBuilderFilters,
  EMPTY_QUERY,
} from "@/components/QueryBuilderFilters";
import {
  compileQuery,
  sortableTargets,
  type AggKind,
  type Projection,
  type QueryBuilderState,
} from "@/lib/queryCompiler";
import {
  abbreviateColumnType,
  isComparableType,
  isNumericType,
} from "@/lib/columnType";
import { useTheme } from "@/components/ThemeProvider";
import { cn } from "@/lib/utils";

interface DictColumn {
  column: string;
  description?: string;
  type?: string;
  fill_pct?: number | null;
  fill_pct_approx?: boolean;
}

interface QueryBuilderPanelProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  subsystem: string;
  columns: DictColumn[];
  defaultLimit: number;
  onApply: (sql: string) => void;
}

const ALL_AGGS: AggKind[] = [
  "none",
  "count",
  "count_distinct",
  "sum",
  "avg",
  "min",
  "max",
];

function isAggAllowed(agg: AggKind, type: string | undefined): boolean {
  if (agg === "none" || agg === "count" || agg === "count_distinct") return true;
  if (agg === "sum" || agg === "avg") return isNumericType(type);
  if (agg === "min" || agg === "max") return isComparableType(type);
  return false;
}

export function QueryBuilderPanel({
  open,
  onOpenChange,
  subsystem,
  columns,
  defaultLimit,
  onApply,
}: QueryBuilderPanelProps) {
  const { t } = useTranslation();
  const { resolvedTheme } = useTheme();

  // ─── Local state ──────────────────────────────────────────────────────
  // Persisted per subsystem so the user's last builder workspace survives
  // panel close/reopen *and* page reloads. Different subsystems get
  // independent workspaces — the panel is keyed on `subsystem` upstream so
  // these hooks remount when the subsystem switches.
  const keyPrefix = `query.builder.${subsystem || "none"}`;
  const [projections, setProjections] = useLocalStorage<Projection[]>(
    `${keyPrefix}.projections`,
    [],
  );
  const [groupBy, setGroupBy] = useLocalStorage<string[]>(
    `${keyPrefix}.groupBy`,
    [],
  );
  const [whereQuery, setWhereQuery] = useLocalStorage<RuleGroupType>(
    `${keyPrefix}.whereQuery`,
    EMPTY_QUERY,
  );
  const [orderColumn, setOrderColumn] = useLocalStorage<string>(
    `${keyPrefix}.orderColumn`,
    "",
  );
  const [orderDirection, setOrderDirection] = useLocalStorage<"ASC" | "DESC">(
    `${keyPrefix}.orderDirection`,
    "DESC",
  );
  const [limit, setLimit] = useLocalStorage<number>(
    `${keyPrefix}.limit`,
    defaultLimit,
  );

  // First-ever-open seed: if this subsystem has never been touched before,
  // start with one default projection so the live preview isn't blank.
  // Tracked through localStorage so the seed never re-fires after the user
  // intentionally clears every projection.
  const [seeded, setSeeded] = useLocalStorage<boolean>(
    `${keyPrefix}.seeded`,
    false,
  );
  React.useEffect(() => {
    if (!open) return;
    if (seeded) return;
    if (columns.length === 0) return;
    setProjections([{ column: columns[0].column, aggregation: "none" }]);
    setSeeded(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, columns, seeded]);

  // ─── Derived ──────────────────────────────────────────────────────────
  const columnsByName = React.useMemo(() => {
    const map = new Map<string, DictColumn>();
    for (const c of columns) map.set(c.column, c);
    return map;
  }, [columns]);

  // formatQuery returns "(1 = 1)" for an empty rule group; we want WHERE
  // to be omitted in that case. Treat truly empty rules as no filter.
  const whereSql = React.useMemo(() => {
    if (!whereQuery.rules || whereQuery.rules.length === 0) return "";
    const sql = formatQuery(whereQuery, "sql");
    if (sql === "(1 = 1)" || sql === "1 = 1") return "";
    return sql;
  }, [whereQuery]);

  const state: QueryBuilderState = React.useMemo(
    () => ({
      projections,
      groupBy,
      whereSql,
      orderBy:
        orderColumn.trim().length > 0
          ? { column: orderColumn, direction: orderDirection }
          : null,
      limit: limit > 0 ? limit : null,
    }),
    [projections, groupBy, whereSql, orderColumn, orderDirection, limit],
  );

  const compiled = React.useMemo(
    () => compileQuery(state, subsystem),
    [state, subsystem],
  );

  const projectedColumns = projections.map((p) => p.column);
  const eligibleGroupBy = projections
    .filter((p) => p.aggregation === "none")
    .map((p) => p.column);
  const hasErrors = compiled.errors.length > 0;
  const sortTargets = sortableTargets(projections);

  // ─── Mutators ─────────────────────────────────────────────────────────
  const addProjection = (column: string) => {
    if (!column || projectedColumns.includes(column)) return;
    setProjections((prev) => [...prev, { column, aggregation: "none" }]);
  };

  const removeProjection = (column: string) => {
    setProjections((prev) => prev.filter((p) => p.column !== column));
    setGroupBy((prev) => prev.filter((c) => c !== column));
    if (orderColumn === column) setOrderColumn("");
  };

  const setProjectionAgg = (column: string, agg: AggKind) => {
    setProjections((prev) =>
      prev.map((p) => (p.column === column ? { ...p, aggregation: agg } : p)),
    );
    // Aggregating a column means it can no longer be in GROUP BY.
    if (agg !== "none") {
      setGroupBy((prev) => prev.filter((c) => c !== column));
    }
  };

  const toggleGroupBy = (column: string) => {
    setGroupBy((prev) =>
      prev.includes(column) ? prev.filter((c) => c !== column) : [...prev, column],
    );
  };

  // ─── Render ───────────────────────────────────────────────────────────
  const handleApply = () => {
    if (hasErrors) return;
    onApply(compiled.sql);
    onOpenChange(false);
  };

  // Formats validation tokens (e.g. "projection.missingGroupBy:foo") as
  // human messages via i18n. Falls back to the raw token if the key is
  // missing so a regression never silently swallows the diagnostic.
  const renderDiagnostic = (token: string): string => {
    const [code, arg] = token.split(":");
    return t(`query.builder.errors.${code}`, {
      defaultValue: token,
      column: arg ?? "",
    });
  };

  // Available columns for "add projection" excludes already-projected ones.
  const addableColumns = columns.filter(
    (c) => !projectedColumns.includes(c.column),
  );

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        // Override the default `sm:max-w-sm` so the builder has room for
        // the four sections + live preview without horizontal scrolling.
        className="flex w-full flex-col gap-0 p-0 sm:max-w-2xl"
      >
        <SheetHeader className="border-b px-6 py-4">
          <SheetTitle>{t("query.builder.title")}</SheetTitle>
          <SheetDescription>{t("query.builder.subtitle")}</SheetDescription>
        </SheetHeader>

        <ScrollArea className="flex-1">
          <div className="space-y-5 px-6 py-4">
            {/* ─── Columns to project ───────────────────────────────── */}
            <Section
              title={t("query.builder.columnsLabel")}
              hint={t("query.builder.columnsHint")}
            >
              <div className="flex flex-wrap gap-2">
                {projections.map((p) => {
                  const meta = columnsByName.get(p.column);
                  return (
                    <ProjectionChip
                      key={p.column}
                      column={p.column}
                      type={meta?.type}
                      aggregation={p.aggregation}
                      onAggChange={(agg) => setProjectionAgg(p.column, agg)}
                      onRemove={() => removeProjection(p.column)}
                    />
                  );
                })}
              </div>
              <Select
                value=""
                onValueChange={addProjection}
                disabled={addableColumns.length === 0}
              >
                <SelectTrigger className="h-8 max-w-md text-xs">
                  <SelectValue
                    placeholder={t("query.builder.addColumnPlaceholder")}
                  />
                </SelectTrigger>
                <SelectContent className="max-w-[36rem]">
                  {addableColumns.map((c) => (
                    <SelectItem key={c.column} value={c.column}>
                      <span className="flex items-center gap-2">
                        <ColumnFillBadge
                          fillPct={c.fill_pct}
                          approx={c.fill_pct_approx}
                        />
                        <ColumnTypeBadge type={c.type} />
                        <span className="font-mono">{c.column}</span>
                        {c.description ? (
                          <span className="truncate text-muted-foreground">
                            — {c.description}
                          </span>
                        ) : null}
                      </span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </Section>

            {/* ─── Group by ─────────────────────────────────────────── */}
            <Section
              title={t("query.builder.groupByLabel")}
              hint={t("query.builder.groupByHint")}
            >
              {eligibleGroupBy.length === 0 ? (
                <p className="text-xs italic text-muted-foreground">
                  {t("query.builder.groupByEmpty")}
                </p>
              ) : (
                <div className="flex flex-wrap gap-1.5">
                  {eligibleGroupBy.map((col) => {
                    const active = groupBy.includes(col);
                    return (
                      <button
                        key={col}
                        type="button"
                        onClick={() => toggleGroupBy(col)}
                        className={cn(
                          "rounded-full border px-2.5 py-1 font-mono text-[11px] transition-colors",
                          active
                            ? "border-primary bg-primary text-primary-foreground"
                            : "border-border bg-background text-foreground hover:bg-secondary",
                        )}
                      >
                        {col}
                      </button>
                    );
                  })}
                </div>
              )}
            </Section>

            {/* ─── Filters (WHERE) ──────────────────────────────────── */}
            <Section
              title={t("query.builder.filtersLabel")}
              hint={t("query.builder.filtersHint")}
            >
              <QueryBuilderFilters
                columns={columns}
                query={whereQuery}
                onQueryChange={setWhereQuery}
              />
            </Section>

            {/* ─── Sort ─────────────────────────────────────────────── */}
            <Section
              title={t("query.builder.sortLabel")}
              hint={t("query.builder.sortHint")}
            >
              <div className="flex flex-wrap items-center gap-2">
                <Select
                  value={orderColumn}
                  onValueChange={setOrderColumn}
                  disabled={sortTargets.length === 0}
                >
                  <SelectTrigger className="h-8 w-60 text-xs">
                    <SelectValue
                      placeholder={t("query.builder.sortPlaceholder")}
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {sortTargets.map((s) => (
                      <SelectItem key={s.column} value={s.column}>
                        <span className="font-mono">{s.label}</span>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Select
                  value={orderDirection}
                  onValueChange={(v) =>
                    setOrderDirection(v as "ASC" | "DESC")
                  }
                  disabled={!orderColumn}
                >
                  <SelectTrigger className="h-8 w-28 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="ASC">ASC</SelectItem>
                    <SelectItem value="DESC">DESC</SelectItem>
                  </SelectContent>
                </Select>
                {orderColumn ? (
                  <Button
                    type="button"
                    size="sm"
                    variant="ghost"
                    onClick={() => setOrderColumn("")}
                  >
                    {t("common.cancel")}
                  </Button>
                ) : null}
              </div>
            </Section>

            {/* ─── Limit ────────────────────────────────────────────── */}
            <Section
              title={t("query.builder.limitLabel")}
              hint={t("query.builder.limitHint")}
            >
              <Input
                type="number"
                min={1}
                max={1_000_000}
                value={limit}
                onChange={(e) => setLimit(Number(e.target.value) || 0)}
                className="h-8 w-32 text-xs"
              />
            </Section>

            {/* ─── Live SQL preview ─────────────────────────────────── */}
            <Section
              title={t("query.builder.previewLabel")}
              hint={t("query.builder.previewHint")}
            >
              <div className="overflow-hidden rounded-md border border-border/60">
                <Editor
                  height="200px"
                  language="sql"
                  theme={resolvedTheme === "dark" ? "vs-dark" : "light"}
                  value={compiled.sql}
                  options={{
                    readOnly: true,
                    domReadOnly: true,
                    minimap: { enabled: false },
                    fontFamily: "JetBrains Mono, monospace",
                    fontSize: 12,
                    lineNumbers: "off",
                    glyphMargin: false,
                    folding: false,
                    wordWrap: "on",
                    automaticLayout: true,
                    renderLineHighlight: "none",
                    overviewRulerLanes: 0,
                    scrollBeyondLastLine: false,
                    contextmenu: false,
                    // Hide the cursor — it's read-only, the blinking caret
                    // would otherwise advertise a fake editing affordance.
                    cursorStyle: "line-thin",
                    cursorBlinking: "solid",
                    scrollbar: {
                      vertical: "auto",
                      horizontal: "auto",
                      verticalScrollbarSize: 8,
                      horizontalScrollbarSize: 8,
                    },
                  }}
                />
              </div>
              {compiled.warnings.length > 0 ? (
                <ul className="space-y-0.5 text-[11px] text-amber-700 dark:text-amber-400">
                  {compiled.warnings.map((w) => (
                    <li key={w} className="flex items-start gap-1.5">
                      <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0" />
                      {renderDiagnostic(w)}
                    </li>
                  ))}
                </ul>
              ) : null}
              {compiled.errors.length > 0 ? (
                <ul className="space-y-0.5 text-[11px] text-destructive">
                  {compiled.errors.map((e) => (
                    <li key={e} className="flex items-start gap-1.5">
                      <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0" />
                      {renderDiagnostic(e)}
                    </li>
                  ))}
                </ul>
              ) : null}
            </Section>
          </div>
        </ScrollArea>

        <div className="flex items-center justify-end gap-2 border-t px-6 py-3">
          <Button
            type="button"
            variant="ghost"
            onClick={() => onOpenChange(false)}
          >
            {t("common.cancel")}
          </Button>
          <Button type="button" onClick={handleApply} disabled={hasErrors}>
            {t("query.builder.apply")}
          </Button>
        </div>
      </SheetContent>
    </Sheet>
  );
}

// ─────────────────────────────────────────────────────────────────────────
// Local presentational helpers
// ─────────────────────────────────────────────────────────────────────────

function Section({
  title,
  hint,
  children,
}: {
  title: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="space-y-2">
      <header className="space-y-0.5">
        <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          {title}
        </h3>
        {hint ? (
          <p className="text-[11px] text-muted-foreground/80">{hint}</p>
        ) : null}
      </header>
      <div className="space-y-2">{children}</div>
    </section>
  );
}

function ProjectionChip({
  column,
  type,
  aggregation,
  onAggChange,
  onRemove,
}: {
  column: string;
  type: string | undefined;
  aggregation: AggKind;
  onAggChange: (agg: AggKind) => void;
  onRemove: () => void;
}) {
  const { t } = useTranslation();
  const typeMeta = abbreviateColumnType(type);
  return (
    <div className="flex items-center gap-1.5 rounded-md border border-border/60 bg-background px-2 py-1">
      <Badge variant="outline" className={cn("font-mono", typeMeta.className)}>
        {typeMeta.abbrev}
      </Badge>
      <span className="font-mono text-xs">{column}</span>
      <Select
        value={aggregation}
        onValueChange={(v) => onAggChange(v as AggKind)}
      >
        <SelectTrigger className="h-6 w-32 text-[11px]">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {ALL_AGGS.map((a) => {
            const allowed = isAggAllowed(a, type);
            return (
              <SelectItem key={a} value={a} disabled={!allowed}>
                {t(`query.builder.aggregations.${a}`)}
              </SelectItem>
            );
          })}
        </SelectContent>
      </Select>
      <button
        type="button"
        onClick={onRemove}
        aria-label={t("query.builder.removeColumn")}
        title={t("query.builder.removeColumn")}
        className="ml-1 inline-flex h-5 w-5 items-center justify-center rounded text-muted-foreground hover:bg-destructive/15 hover:text-destructive"
      >
        <Trash2 className="h-3 w-3" />
      </button>
    </div>
  );
}

