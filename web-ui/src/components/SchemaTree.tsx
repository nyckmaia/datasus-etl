import * as React from "react";
import { ChevronRight, Search } from "lucide-react";
import { useTranslation } from "react-i18next";

import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import { ColumnTypeBadge } from "@/components/ColumnTypeBadge";
import { ColumnFillBadge } from "@/components/ColumnFillBadge";
import { ColumnFillBar } from "@/components/ColumnFillBar";
import { ColumnDistinctBadge } from "@/components/ColumnDistinctBadge";
import { cn } from "@/lib/utils";
import type {
  SchemaTree as Tree,
  SchemaSubsystem,
  SchemaView,
} from "@/types/api";

interface SchemaTreeProps {
  tree: Tree | undefined;
  loading: boolean;
  /** Inserts `subsystem.column` into the active SQL editor. */
  onColumnPick: (subsystem: string, column: string) => void;
  /** Opens a histogram query for the clicked column (uses subsystem context). */
  onColumnHistogram: (subsystem: string, column: string) => void;
}

export function SchemaTree({
  tree,
  loading,
  onColumnPick,
  onColumnHistogram,
}: SchemaTreeProps) {
  const { t, i18n } = useTranslation();
  const [filter, setFilter] = React.useState("");
  const [openSubs, setOpenSubs] = React.useState<Record<string, boolean>>({});
  const [openViews, setOpenViews] = React.useState<Record<string, boolean>>({});

  const lang = i18n.language.startsWith("pt") ? "pt" : "en";
  const q = filter.trim().toLowerCase();

  const filteredSubs: SchemaSubsystem[] = React.useMemo(() => {
    if (!tree) return [];
    if (!q) return tree.subsystems;
    return tree.subsystems
      .map((sub) => {
        const matchedViews = sub.views
          .map((v) => ({
            ...v,
            columns: v.columns.filter(
              (c) =>
                c.column.toLowerCase().includes(q) ||
                c.description.toLowerCase().includes(q),
            ),
          }))
          .filter(
            (v) =>
              v.columns.length > 0 ||
              v.name.toLowerCase().includes(q) ||
              sub.name.toLowerCase().includes(q),
          );
        return matchedViews.length ? { ...sub, views: matchedViews } : null;
      })
      .filter((s): s is SchemaSubsystem => s !== null);
  }, [tree, q]);

  // When the user types into the filter, auto-expand every matching node so
  // results are visible without manual drilling.
  React.useEffect(() => {
    if (!q) return;
    const subs: Record<string, boolean> = {};
    const views: Record<string, boolean> = {};
    for (const sub of filteredSubs) {
      subs[sub.name] = true;
      for (const v of sub.views) views[`${sub.name}.${v.name}`] = true;
    }
    setOpenSubs((prev) => ({ ...prev, ...subs }));
    setOpenViews((prev) => ({ ...prev, ...views }));
  }, [q, filteredSubs]);

  const viewLabel = (v: SchemaView) =>
    (lang === "pt" ? v.label_pt : v.label_en) ?? v.name;

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="border-b p-2">
        <div className="relative">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-3 w-3 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder={t("query.tree.findColumn")}
            className="h-7 pl-6 text-xs"
          />
        </div>
      </div>
      <ScrollArea className="min-h-0 flex-1">
        <div className="space-y-0.5 p-1.5">
          {loading ? (
            <div className="space-y-1.5 p-1.5">
              <Skeleton className="h-8 w-full" />
              <Skeleton className="h-8 w-full" />
              <Skeleton className="h-8 w-3/4" />
            </div>
          ) : filteredSubs.length === 0 ? (
            <div className="px-2 py-4 text-center text-xs text-muted-foreground">
              {tree && tree.subsystems.length === 0
                ? t("query.tree.empty")
                : t("query.tree.noMatch")}
            </div>
          ) : (
            filteredSubs.map((sub) => {
              const subOpen = openSubs[sub.name] ?? false;
              return (
                <div key={sub.name} className="rounded-md">
                  <button
                    type="button"
                    onClick={() =>
                      setOpenSubs((prev) => ({
                        ...prev,
                        [sub.name]: !subOpen,
                      }))
                    }
                    className="flex w-full items-center gap-1.5 rounded-md px-2 py-1.5 text-left text-xs font-semibold uppercase tracking-wider text-foreground transition-colors hover:bg-secondary/50"
                  >
                    <ChevronRight
                      className={cn(
                        "h-3.5 w-3.5 transition-transform",
                        subOpen && "rotate-90",
                      )}
                    />
                    <span className="flex-1 truncate">{sub.label}</span>
                    <span className="font-mono text-[10px] text-muted-foreground tabular-nums">
                      {sub.views.length}
                    </span>
                  </button>
                  {subOpen ? (
                    <div className="ml-3.5 border-l pl-1.5">
                      {sub.views.map((view) => {
                        const key = `${sub.name}.${view.name}`;
                        const viewOpen = openViews[key] ?? false;
                        return (
                          <div key={key}>
                            <button
                              type="button"
                              onClick={() =>
                                setOpenViews((prev) => ({
                                  ...prev,
                                  [key]: !viewOpen,
                                }))
                              }
                              className="flex w-full items-center gap-1.5 rounded-md px-2 py-1.5 text-left text-xs transition-colors hover:bg-secondary/50"
                              title={view.description ?? undefined}
                            >
                              <ChevronRight
                                className={cn(
                                  "h-3 w-3 transition-transform",
                                  viewOpen && "rotate-90",
                                )}
                              />
                              <span className="flex-1 truncate font-mono">
                                {viewLabel(view)}
                              </span>
                              <Badge
                                variant="outline"
                                className={cn(
                                  "h-4 shrink-0 px-1 text-[9px] font-medium uppercase tracking-wider",
                                  view.role === "main"
                                    ? "border-primary/40 text-primary"
                                    : "border-muted-foreground/40 text-muted-foreground",
                                )}
                              >
                                {view.role === "main"
                                  ? t("query.tree.mainView")
                                  : t("query.tree.dimView")}
                              </Badge>
                              <span className="font-mono text-[10px] text-muted-foreground tabular-nums">
                                {view.columns.length}
                              </span>
                            </button>
                            {viewOpen ? (
                              <div className="ml-3 border-l pl-1.5">
                                {view.columns.map((entry) => (
                                  <div
                                    key={entry.column}
                                    onClick={() =>
                                      onColumnPick(sub.name, entry.column)
                                    }
                                    role="button"
                                    tabIndex={0}
                                    onKeyDown={(e) => {
                                      if (e.key === "Enter" || e.key === " ") {
                                        e.preventDefault();
                                        onColumnPick(sub.name, entry.column);
                                      }
                                    }}
                                    className="group cursor-pointer rounded-md border border-transparent px-2 py-1.5 text-xs transition-colors hover:border-border/60 hover:bg-secondary/50"
                                  >
                                    <div className="flex items-center justify-between gap-2">
                                      <span className="truncate font-mono font-medium">
                                        {entry.column}
                                      </span>
                                      <div
                                        className="flex shrink-0 items-center gap-1"
                                        onClick={(e) => e.stopPropagation()}
                                      >
                                        <ColumnFillBadge
                                          fillPct={entry.fill_pct}
                                          approx={entry.fill_pct_approx}
                                        />
                                        <ColumnDistinctBadge
                                          count={entry.distinct_count}
                                          approx={entry.distinct_count_approx}
                                          onClick={() =>
                                            onColumnHistogram(
                                              sub.name,
                                              entry.column,
                                            )
                                          }
                                        />
                                        <ColumnTypeBadge type={entry.type} />
                                      </div>
                                    </div>
                                    {entry.description ? (
                                      <div className="mt-0.5 line-clamp-2 text-[11px] leading-snug text-muted-foreground">
                                        {entry.description}
                                      </div>
                                    ) : null}
                                    <ColumnFillBar fillPct={entry.fill_pct} />
                                  </div>
                                ))}
                              </div>
                            ) : null}
                          </div>
                        );
                      })}
                    </div>
                  ) : null}
                </div>
              );
            })
          )}
        </div>
      </ScrollArea>
    </div>
  );
}
