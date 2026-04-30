import * as React from "react";
import { useSearch } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import Editor from "@monaco-editor/react";
import {
  Play,
  Download,
  FileSpreadsheet,
  History,
  BookOpen,
  PanelLeftClose,
  PanelLeftOpen,
  PanelRightClose,
  PanelRightOpen,
  Columns3,
  Wand2,
  Star,
  Trash2,
  Pencil,
  Check,
  X,
} from "lucide-react";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Input } from "@/components/ui/input";
import { SchemaTree } from "@/components/SchemaTree";
import { abbreviateColumnType } from "@/lib/columnType";
import { QueryTabsBar, type QueryTab } from "@/components/QueryTabsBar";
import { QueryBuilderPanel } from "@/components/QueryBuilderPanel";
import { api } from "@/lib/api";
import type { SqlResult } from "@/lib/api";
import type { SchemaTree as SchemaTreeData } from "@/types/api";
import { cn } from "@/lib/utils";
import {
  registerSqlAutocomplete,
  updateAutocompleteState,
} from "@/lib/sqlAutocomplete";
import { formatMs, formatCompact, formatBytes } from "@/lib/format";
import { prettySql } from "@/lib/sqlPretty";
import { useStatsOverview } from "@/hooks/useStats";
import { useSqlQuery } from "@/hooks/useSqlQuery";
import { useLocalStorage } from "@/hooks/useLocalStorage";
import {
  useQueryHistory,
  useAppendQueryHistory,
  usePatchQueryHistory,
  useDeleteQueryHistoryEntry,
} from "@/hooks/useQueryHistory";
import { useTheme } from "@/components/ThemeProvider";
import { useSettings } from "@/hooks/useSettings";

type HistoryItem = import("@/lib/api").QueryHistoryEntry;

// Persisted shape of a SQL editor tab. Results are intentionally NOT persisted
// — they can be huge (multi-MB) and bloat localStorage.
interface PersistedTab {
  id: string;
  name: string;
  sql: string;
  // Once `true`, switching subsystems no longer overwrites this tab's SQL
  // with the default template. Per-tab so each editor scratchpad keeps its
  // own "user-edited?" state independently.
  userEdited: boolean;
}

// Default SQL seeded into the editor on first load and refreshed whenever
// the user switches the subsystem (until they type something custom — see
// `userEdited` flag below). Lowercase keywords match the wording requested
// in the original feature ticket.
const buildDefaultSql = (subsystem: string) =>
  `SELECT * FROM ${subsystem} LIMIT 10;`;

const QUERY_TAB_PREFIX = "Query";

function nextQueryName(tabs: PersistedTab[]): string {
  // Pick the lowest unused integer suffix so newly opened tabs get the
  // next "Query N" without ever reusing a number that's already on screen.
  const used = new Set<number>();
  for (const tab of tabs) {
    const match = tab.name.match(/^Query\s+(\d+)$/i);
    if (match) used.add(Number(match[1]));
  }
  let n = 1;
  while (used.has(n)) n += 1;
  return `${QUERY_TAB_PREFIX} ${n}`;
}

function makeTab(name: string, sql: string): PersistedTab {
  return { id: crypto.randomUUID(), name, sql, userEdited: false };
}

// Sidebar widths — kept tight so the Monaco editor can breathe. The "rail"
// width matches the height of the toggle button so collapsed sidebars feel
// like the sidebar of a code IDE rather than a hidden panel.
const LEFT_EXPANDED = "360px";
const RIGHT_EXPANDED = "300px";
const RAIL = "44px";

export function QueryPage() {
  const { t } = useTranslation();
  const { resolvedTheme } = useTheme();
  // useStatsOverview kept for potential future use (e.g. Dashboard deep-link compatibility).
  useStatsOverview(false);
  const runSql = useSqlQuery();
  const { data: settings } = useSettings();

  // The Dashboard's subsystem cards link here with `?subsystem=<name>` so the
  // first tab can be seeded against that subsystem on initial load.
  const search = useSearch({ from: "/query" }) as { subsystem?: string };

  // The "focused" subsystem is persisted across reloads. It's NOT an active
  // filter — every subsystem in the tree is queryable simultaneously. This
  // value only seeds the default SQL on first load and keys the per-
  // subsystem query history bucket.
  const [focusedSubsystem, setFocusedSubsystem] = useLocalStorage<string>(
    "query.focusedSubsystem",
    search.subsystem ?? "",
  );

  // SQL editor tabs are persisted across reloads. The first tab is seeded
  // synchronously when the URL already names a subsystem so the editor doesn't
  // flash empty content before the resolution effect fills it in.
  const [tabs, setTabs] = useLocalStorage<PersistedTab[]>(
    "query.tabs",
    [
      makeTab(
        `${QUERY_TAB_PREFIX} 1`,
        search.subsystem ? buildDefaultSql(search.subsystem) : "",
      ),
    ],
  );
  const [activeTabId, setActiveTabId] = useLocalStorage<string>(
    "query.activeTabId",
    "",
  );
  // Per-tab last result kept in memory only — too heavy for localStorage.
  const [resultsByTab, setResultsByTab] = React.useState<Record<string, SqlResult>>({});
  // Tracks which tabs have an in-flight Run so the per-tab Run button can show
  // "Running…" only on the tab that initiated the query (the user can keep
  // working in another tab while one is still executing).
  const [runningTabs, setRunningTabs] = React.useState<Record<string, boolean>>({});
  const historyQuery = useQueryHistory(focusedSubsystem);
  const history = historyQuery.data ?? [];
  const appendHistoryMutation = useAppendQueryHistory(focusedSubsystem);
  const patchHistoryMutation = usePatchQueryHistory(focusedSubsystem);
  const deleteHistoryMutation = useDeleteQueryHistoryEntry(focusedSubsystem);
  const [historyFavoritesOnly, setHistoryFavoritesOnly] = useLocalStorage<boolean>(
    "query.historyFavoritesOnly",
    false,
  );
  const [limit, setLimit] = React.useState<number>(10000);
  // Visual question builder — opens a Sheet panel; on Apply it writes the
  // compiled SQL into the active tab via `replaceSql`.
  const [builderOpen, setBuilderOpen] = React.useState<boolean>(false);

  // Defensive: if the persisted active id no longer matches a tab (or was
  // never set on a fresh install), snap to the first tab on first render.
  React.useEffect(() => {
    if (tabs.length === 0) {
      const seed = makeTab(`${QUERY_TAB_PREFIX} 1`, focusedSubsystem ? buildDefaultSql(focusedSubsystem) : "");
      setTabs([seed]);
      setActiveTabId(seed.id);
      return;
    }
    if (!tabs.some((tab) => tab.id === activeTabId)) {
      setActiveTabId(tabs[0].id);
    }
    // We deliberately only run when tab structure changes, not on every render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tabs.length]);

  const activeTab = React.useMemo(
    () => tabs.find((tab) => tab.id === activeTabId) ?? tabs[0],
    [tabs, activeTabId],
  );

  const sql = activeTab?.sql ?? "";
  const result = activeTab ? resultsByTab[activeTab.id] ?? null : null;
  const isActiveRunning = activeTab ? Boolean(runningTabs[activeTab.id]) : false;

  // Derived list shown to the QueryTabsBar — keep it lean (id + name).
  const tabBarItems: QueryTab[] = React.useMemo(
    () => tabs.map((tab) => ({ id: tab.id, name: tab.name })),
    [tabs],
  );

  // Tab mutation helpers.
  const patchTab = React.useCallback(
    (id: string, patch: Partial<PersistedTab>) => {
      setTabs((prev) =>
        prev.map((tab) => (tab.id === id ? { ...tab, ...patch } : tab)),
      );
    },
    [setTabs],
  );

  const onAddTab = React.useCallback(() => {
    const seedSql = focusedSubsystem ? buildDefaultSql(focusedSubsystem) : "";
    const tab = makeTab(nextQueryName(tabs), seedSql);
    setTabs((prev) => [...prev, tab]);
    setActiveTabId(tab.id);
  }, [tabs, focusedSubsystem, setTabs, setActiveTabId]);

  const onCloseTab = React.useCallback(
    (id: string) => {
      const idx = tabs.findIndex((tab) => tab.id === id);
      if (idx < 0) return;
      const next = tabs.filter((tab) => tab.id !== id);

      setResultsByTab((prev) => {
        if (!(id in prev)) return prev;
        const { [id]: _removed, ...rest } = prev;
        return rest;
      });
      setRunningTabs((prev) => {
        if (!(id in prev)) return prev;
        const { [id]: _running, ...rest } = prev;
        return rest;
      });

      if (next.length === 0) {
        // Always keep at least one tab so the editor never shows an empty
        // state — closing the only tab spawns a fresh default.
        const fresh = makeTab(
          `${QUERY_TAB_PREFIX} 1`,
          focusedSubsystem ? buildDefaultSql(focusedSubsystem) : "",
        );
        setTabs([fresh]);
        setActiveTabId(fresh.id);
        return;
      }
      if (id === activeTabId) {
        const neighbor = next[Math.min(idx, next.length - 1)];
        setActiveTabId(neighbor.id);
      }
      setTabs(next);
    },
    [tabs, activeTabId, focusedSubsystem, setTabs, setActiveTabId],
  );

  const onReorderTabs = React.useCallback(
    (orderedIds: string[]) => {
      setTabs((prev) => {
        const byId = new Map(prev.map((tab) => [tab.id, tab]));
        const reordered = orderedIds
          .map((id) => byId.get(id))
          .filter((tab): tab is PersistedTab => Boolean(tab));
        // Append any tabs that weren't in the ordered list (defensive — shouldn't happen).
        for (const tab of prev) {
          if (!orderedIds.includes(tab.id)) reordered.push(tab);
        }
        return reordered;
      });
    },
    [setTabs],
  );

  // Both sidebars persist their collapsed state — the user gets the same
  // workspace shape across reloads.
  const [leftCollapsed, setLeftCollapsed] = useLocalStorage(
    "query.leftCollapsed",
    false,
  );
  const [rightCollapsed, setRightCollapsed] = useLocalStorage(
    "query.rightCollapsed",
    false,
  );
  // Right sidebar carries two views (templates / history) — its active tab
  // is also remembered. Useful for users who live in one of the two.
  const [rightTab, setRightTab] = useLocalStorage<"templates" | "history">(
    "query.rightTab",
    "templates",
  );

  const templates = useQuery({
    queryKey: ["query", "templates"],
    queryFn: () => api.templates(),
  });

  const schemaQuery = useQuery({
    queryKey: ["query", "schema"],
    queryFn: () => api.schema(),
  });
  const tree = schemaQuery.data;

  // Auto-seed / refresh the active tab with `SELECT * from <subsystem> limit 10;`
  // whenever the focused subsystem changes. Per-tab — only the active tab is
  // touched, and only if the user hasn't edited it yet. Other tabs keep their own SQL.
  React.useEffect(() => {
    if (!tree || !activeTab || activeTab.userEdited) return;
    const seedSubsystem =
      focusedSubsystem ||
      tree.subsystems[0]?.name ||
      "";
    if (!seedSubsystem) return;
    const seeded = buildDefaultSql(seedSubsystem);
    if (activeTab.sql === seeded) return;
    patchTab(activeTab.id, { sql: seeded });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tree, focusedSubsystem, activeTab?.id]);

  // Keep the SQL autocomplete provider's data in sync. The provider itself
  // is registered once globally (in Editor's onMount); this effect just
  // pushes the latest column dictionary + focused subsystem so the popup
  // reflects the table the user is currently exploring.
  React.useEffect(() => {
    if (!tree) return;
    // Feed every subsystem's main-view columns to the autocompleter so the
    // user gets suggestions for any subsystem in the editor — no "active"
    // subsystem to gate them.
    const flat = tree.subsystems.flatMap((sub) =>
      sub.views
        .filter((v) => v.role === "main")
        .flatMap((v) =>
          v.columns.map((c) => ({
            ...c,
            subsystem: sub.name,
          })),
        ),
    );
    updateAutocompleteState({
      columns: flat,
      subsystem: focusedSubsystem,
    });
  }, [tree, focusedSubsystem]);

  // Editor onChange. Marks the active tab as user-edited only when the new
  // value truly diverges from the auto template — otherwise the act of
  // seeding the template would itself flip the flag and prevent future
  // auto-refreshes.
  const onSqlChange = React.useCallback(
    (next: string | undefined) => {
      if (!activeTab) return;
      const value = next ?? "";
      const diverged =
        Boolean(focusedSubsystem) && value !== buildDefaultSql(focusedSubsystem);
      patchTab(activeTab.id, {
        sql: value,
        userEdited: activeTab.userEdited || diverged,
      });
    },
    [activeTab, focusedSubsystem, patchTab],
  );

  // Loading a template or a history item is an explicit user replacement —
  // freeze the auto-template behaviour on the active tab so a later subsystem
  // switch doesn't overwrite their selection.
  const replaceSql = React.useCallback(
    (next: string) => {
      if (!activeTab) return;
      patchTab(activeTab.id, { sql: next, userEdited: true });
    },
    [activeTab, patchTab],
  );

  const filteredTemplates = React.useMemo(() => {
    if (!templates.data) return [];
    const scoped = focusedSubsystem
      ? templates.data.filter((t) => t.subsystem === focusedSubsystem)
      : templates.data;
    return scoped.map((t) => ({ ...t, sql: prettySql(t.sql) }));
  }, [templates.data, focusedSubsystem]);

  // Runs an arbitrary SQL string. Extracted so both the Run button (uses
  // the current editor value) and the histogram badge action (fires
  // immediately after replacing the editor SQL — before React state would
  // make the new value visible to a caller of onRun) can share the same
  // success/error/history plumbing.
  //
  // Captures the active tab id at call time so the result lands on the tab
  // that initiated the run even if the user switches tabs mid-flight.
  const runWithSql = React.useCallback(
    (sqlToRun: string) => {
      const tabId = activeTabId;
      if (!tabId) return;
      // Snapshot the tab name at call time so the history entry's default
      // label reflects the tab the user actually clicked Run on, not
      // whatever tab they may have switched to before the query returns.
      const tabName = tabs.find((t) => t.id === tabId)?.name ?? "";
      const formatted = prettySql(sqlToRun);
      if (formatted !== sqlToRun) {
        patchTab(tabId, { sql: formatted });
      }
      setRunningTabs((prev) => ({ ...prev, [tabId]: true }));
      runSql.mutate(
        { sql: formatted, limit },
        {
          onSuccess: (data) => {
            setResultsByTab((prev) => ({ ...prev, [tabId]: data }));
            if (focusedSubsystem) {
              const ts = Date.now();
              const stamp = new Date(ts).toLocaleString();
              const defaultName = tabName
                ? `${tabName} — ${stamp}`
                : stamp;
              appendHistoryMutation.mutate({
                id: crypto.randomUUID(),
                sql: formatted,
                ts,
                rows: data.row_count,
                elapsed_ms: data.elapsed_ms,
                name: defaultName,
              });
            }
          },
          onError: (err: Error) => {
            toast.error(t("query.queryFailed"), { description: err.message });
          },
          onSettled: () => {
            setRunningTabs((prev) => {
              if (!prev[tabId]) return prev;
              const { [tabId]: _flag, ...rest } = prev;
              return rest;
            });
          },
        },
      );
    },
    [activeTabId, appendHistoryMutation, limit, patchTab, runSql, focusedSubsystem, t, tabs],
  );

  const onRun = React.useCallback(() => runWithSql(sql), [runWithSql, sql]);

  // Keep the latest onRun reachable from the Monaco Ctrl+Enter handler — the
  // editor's `onMount` only fires once, so a captured-at-mount handler would
  // forever see the original (empty) sql. The ref lets the keybinding always
  // call the current onRun (which closes over the active tab's SQL).
  const onRunRef = React.useRef(onRun);
  React.useEffect(() => {
    onRunRef.current = onRun;
  }, [onRun]);

  // One-tap "show histogram for this column" handler. New signature takes
  // (subsystem, column) since the tree knows which subsystem each column belongs to.
  const onColumnHistogram = React.useCallback(
    (subsystem: string, column: string) => {
      if (!subsystem) return;
      setFocusedSubsystem(subsystem);
      const view = tree?.subsystems
        .find((s) => s.name === subsystem)
        ?.views.find((v) => v.role === "main");
      const colMeta = view?.columns.find((c) => c.column === column);
      const isFloat = abbreviateColumnType(colMeta?.type).abbrev === "float";
      const selectExpr = isFloat
        ? `CAST("${column}" AS DECIMAL(10, 2))`
        : `"${column}"`;
      const histSql = prettySql(`SELECT
    ${selectExpr} AS "${column}",
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS "% population"
FROM ${subsystem}
GROUP BY "${column}"
ORDER BY count DESC;`);
      replaceSql(histSql);
      runWithSql(histSql);
    },
    [tree, replaceSql, runWithSql, setFocusedSubsystem],
  );

  // Inserts `subsystem.column` into the active editor at the cursor position.
  const onColumnPick = React.useCallback(
    (subsystem: string, column: string) => {
      setFocusedSubsystem(subsystem);
      if (!activeTab) return;
      const next = (activeTab.sql ?? "").trimEnd();
      const insert = `${subsystem}.${column}`;
      patchTab(activeTab.id, {
        sql: next.length ? `${next} ${insert}` : insert,
        userEdited: true,
      });
    },
    [activeTab, patchTab, setFocusedSubsystem],
  );

  const onExport = React.useCallback(
    async (format: "csv" | "xlsx") => {
      try {
        const blob = await api.exportQuery({
          sql,
          format,
          limit,
          filename: `datasus-query.${format}`,
        });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `datasus-query.${format}`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
        toast.success(t("query.exportedAs", { format: format.toUpperCase() }));
      } catch (err) {
        toast.error(t("query.exportFailed"), {
          description: err instanceof Error ? err.message : t("common.unknown"),
        });
      }
    },
    [sql, limit, t],
  );

  const onDownloadFullCsv = React.useCallback(async () => {
    if (!result) return;
    const totalRows = result.total_rows ?? result.row_count;
    const sample = result.rows.slice(0, Math.min(50, result.rows.length));
    const sampleBytes = JSON.stringify(sample).length;
    const bytesPerRow = sample.length > 0 ? sampleBytes / sample.length : 0;
    const estimatedBytes = bytesPerRow * totalRows;

    const maxRows = settings?.export_max_rows ?? 1_000_000;
    const maxBytes = settings?.export_max_bytes ?? 1_000_000_000;

    const willTruncateRows = totalRows > maxRows;
    const willTruncateBytes = estimatedBytes > maxBytes;

    if (willTruncateRows || willTruncateBytes) {
      const proceed = window.confirm(
        t("query.downloadFull.warnTruncate", {
          rows: formatCompact(totalRows),
          maxRows: formatCompact(maxRows),
          bytes: formatBytes(estimatedBytes),
          maxBytes: formatBytes(maxBytes),
        }),
      );
      if (!proceed) return;
    }

    try {
      const blob = await api.exportQuery({
        sql,
        format: "csv",
        unlimited: true,
        filename: "datasus-query-full.csv",
      });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "datasus-query-full.csv";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      toast.success(t("query.downloadFull.done"));
    } catch (err) {
      toast.error(t("query.downloadFull.failed"), {
        description: err instanceof Error ? err.message : t("common.unknown"),
      });
    }
  }, [result, sql, settings, t]);

  // Track whether either sidebar is collapsed so the central editor stretches
  // to fill the freed real estate. The transition on `grid-template-columns`
  // gives the layout a smooth, deliberate feel.
  const gridCols = `${leftCollapsed ? RAIL : LEFT_EXPANDED} 1fr ${
    rightCollapsed ? RAIL : RIGHT_EXPANDED
  }`;

  return (
    <div className="flex h-[calc(100vh-8rem)] flex-col gap-4">
      <div className="flex items-end justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{t("query.title")}</h1>
          <p className="text-sm text-muted-foreground">{t("query.subtitle")}</p>
        </div>
      </div>

      <div
        className="grid flex-1 min-h-0 gap-4 transition-[grid-template-columns] duration-200 ease-out"
        style={{ gridTemplateColumns: gridCols }}
      >
        {/* ───────────────── LEFT SIDEBAR — Schema Tree ───────────────── */}
        <LeftSidebar
          collapsed={leftCollapsed}
          onToggle={() => setLeftCollapsed((v) => !v)}
          tree={tree}
          loading={schemaQuery.isLoading}
          onColumnPick={onColumnPick}
          onColumnHistogram={onColumnHistogram}
        />

        {/* ───────────────── CENTER — Editor + Results ───────────────── */}
        {/* `min-w-0` is critical here: without it, the Monaco editor's
            internal canvas becomes the grid item's intrinsic min-width,
            which prevents the central column from shrinking when the right
            sidebar expands — the whole grid then overflows horizontally
            instead of shrinking the editor area. */}
        <div className="flex min-h-0 min-w-0 flex-col gap-3">
          <Card className="flex flex-col overflow-hidden">
            <QueryTabsBar
              tabs={tabBarItems}
              activeId={activeTab?.id ?? ""}
              onActivate={setActiveTabId}
              onClose={onCloseTab}
              onAdd={onAddTab}
              onReorder={onReorderTabs}
              onRename={(id, name) => patchTab(id, { name })}
            />
            <div className="flex items-center justify-between border-b px-3 py-2">
              <span className="text-xs font-medium text-muted-foreground">
                {t("query.sqlEditorHint")}
              </span>
              <div className="flex items-center gap-2">
                <Input
                  type="number"
                  value={limit}
                  min={1}
                  max={100000}
                  onChange={(e) => setLimit(Number(e.target.value) || 10000)}
                  className="h-7 w-24 text-xs"
                />
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button size="sm" variant="outline" disabled={!result}>
                      <Download className="h-3.5 w-3.5" />
                      {t("query.export")}
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuItem onClick={() => onExport("csv")}>
                      <Download className="h-4 w-4" />
                      {t("query.csv")}
                    </DropdownMenuItem>
                    <DropdownMenuItem onClick={() => onExport("xlsx")}>
                      <FileSpreadsheet className="h-4 w-4" />
                      {t("query.excel")}
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => setBuilderOpen(true)}
                  disabled={!tree || tree.subsystems.length === 0}
                  title={t("query.builder.openHint")}
                >
                  <Wand2 className="h-3.5 w-3.5" />
                  {t("query.builder.openLabel")}
                </Button>
                <Button size="sm" onClick={onRun} disabled={isActiveRunning}>
                  <Play className="h-3.5 w-3.5" />
                  {isActiveRunning ? t("query.running") : t("query.run")}
                </Button>
              </div>
            </div>
            <div className="h-64">
              <Editor
                height="100%"
                language="sql"
                theme={resolvedTheme === "dark" ? "vs-dark" : "light"}
                value={sql}
                // Path is keyed on the active tab id so Monaco swaps its
                // model when the user switches tabs, preserving each tab's
                // undo history independently.
                path={activeTab ? `tab-${activeTab.id}.sql` : undefined}
                onChange={onSqlChange}
                options={{
                  minimap: { enabled: false },
                  fontFamily: "JetBrains Mono, monospace",
                  fontSize: 13,
                  scrollBeyondLastLine: false,
                  wordWrap: "on",
                  lineNumbers: "on",
                  // Required so Monaco re-runs its layout when the parent
                  // grid cell resizes (e.g. when a sidebar collapses or
                  // expands). Without it the editor canvas keeps its old
                  // width and visually overflows into neighbouring cells.
                  automaticLayout: true,
                }}
                onMount={(editor, monaco) => {
                  // Register the SQL completion provider once globally.
                  // updateAutocompleteState() above keeps it fed with the
                  // active subsystem's column dictionary.
                  registerSqlAutocomplete(monaco);
                  editor.addCommand(
                    monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
                    () => onRunRef.current(),
                  );
                }}
              />
            </div>
          </Card>

          <QueryBuilderPanel
            // Keyed on focusedSubsystem so the panel's per-subsystem
            // useLocalStorage hooks remount with the right keys when the
            // user switches subsystems (each subsystem keeps an independent
            // builder workspace).
            key={`builder-${focusedSubsystem || "none"}`}
            open={builderOpen}
            onOpenChange={setBuilderOpen}
            subsystem={focusedSubsystem}
            columns={
              tree?.subsystems
                .find((s) => s.name === focusedSubsystem)
                ?.views.find((v) => v.role === "main")
                ?.columns ?? []
            }
            defaultLimit={limit}
            onApply={(sql) => replaceSql(sql)}
          />

          <Card className="flex min-h-0 flex-1 flex-col overflow-hidden">
            <div className="flex items-center justify-between border-b px-3 py-2 text-xs">
              <div className="flex items-center gap-3">
                <span className="font-medium">{t("query.results")}</span>
                {result ? (
                  <>
                    <Badge variant="secondary">
                      {t("query.rowsCount", { count: result.row_count })}
                    </Badge>
                    <span className="text-muted-foreground tabular-nums">
                      {formatMs(result.elapsed_ms)}
                    </span>
                    {result.truncated ? (
                      <>
                        <Badge variant="warning">
                          {result.total_rows != null && result.total_rows > result.row_count
                            ? t("query.truncatedWithCount", {
                                count: formatCompact(result.total_rows - result.row_count),
                              })
                            : t("query.truncated")}
                        </Badge>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={onDownloadFullCsv}
                          title={t("query.downloadFull.tooltip")}
                        >
                          <Download className="h-3.5 w-3.5" />
                          {t("query.downloadFull.label")}
                        </Button>
                      </>
                    ) : null}
                  </>
                ) : null}
              </div>
            </div>
            {/* `min-w-0` is the same fix as the editor wrapper: in a flex
                column the default `min-width: auto` makes a child grow with
                its (very wide) content — here, a wide result table — which
                blows the Card width past the grid cell. With min-w-0 the
                inner shadcn `Table` div's `overflow-auto` properly clips,
                producing a horizontal scrollbar inside the Results card. */}
            <div className="min-h-0 min-w-0 flex-1 overflow-auto">
              {isActiveRunning ? (
                <div className="p-4 space-y-2">
                  <Skeleton className="h-6 w-full" />
                  <Skeleton className="h-6 w-full" />
                  <Skeleton className="h-6 w-3/4" />
                </div>
              ) : result ? (
                <ResultTable result={result} />
              ) : (
                <div className="flex h-full items-center justify-center p-6 text-sm text-muted-foreground">
                  {t("query.runToSeeResults")}
                </div>
              )}
            </div>
          </Card>
        </div>

        {/* ───────────────── RIGHT SIDEBAR — Templates / History tabs ───────────────── */}
        <RightSidebar
          collapsed={rightCollapsed}
          onToggle={() => setRightCollapsed((v) => !v)}
          tab={rightTab}
          setTab={setRightTab}
          templatesLoading={templates.isLoading}
          templates={filteredTemplates}
          onPickTemplate={replaceSql}
          history={history}
          onPickHistory={replaceSql}
          favoritesOnly={historyFavoritesOnly}
          onToggleFavoritesOnly={() => setHistoryFavoritesOnly((v) => !v)}
          onRenameHistory={(id, name) =>
            patchHistoryMutation.mutate({ id, patch: { name } })
          }
          onToggleHistoryFavorite={(id, favorite) =>
            patchHistoryMutation.mutate({ id, patch: { favorite } })
          }
          onDeleteHistory={(id) => deleteHistoryMutation.mutate(id)}
        />
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// LEFT SIDEBAR
// ─────────────────────────────────────────────────────────────────────────────

interface LeftSidebarProps {
  collapsed: boolean;
  onToggle: () => void;
  tree: SchemaTreeData | undefined;
  loading: boolean;
  onColumnPick: (subsystem: string, column: string) => void;
  onColumnHistogram: (subsystem: string, column: string) => void;
}

function LeftSidebar({
  collapsed,
  onToggle,
  tree,
  loading,
  onColumnPick,
  onColumnHistogram,
}: LeftSidebarProps) {
  const { t } = useTranslation();
  return (
    <Card className="flex min-h-0 flex-col overflow-hidden">
      <div
        className={cn(
          "flex items-center border-b",
          collapsed ? "justify-center px-2 py-2" : "justify-between px-3 py-2",
        )}
      >
        {!collapsed ? (
          <span className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            <Columns3 className="h-3.5 w-3.5" />
            {t("query.tree.title")}
          </span>
        ) : null}
        <button
          type="button"
          onClick={onToggle}
          aria-label={collapsed ? t("query.expandSidebar") : t("query.collapseSidebar")}
          title={collapsed ? t("query.expandSidebar") : t("query.collapseSidebar")}
          className="flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-secondary hover:text-foreground"
        >
          {collapsed ? (
            <PanelLeftOpen className="h-4 w-4" />
          ) : (
            <PanelLeftClose className="h-4 w-4" />
          )}
        </button>
      </div>
      {collapsed ? (
        <RailLabel
          icon={<Columns3 className="h-4 w-4" />}
          label={t("query.tree.title")}
          onClick={onToggle}
        />
      ) : (
        <SchemaTree
          tree={tree}
          loading={loading}
          onColumnPick={onColumnPick}
          onColumnHistogram={onColumnHistogram}
        />
      )}
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// RIGHT SIDEBAR
// ─────────────────────────────────────────────────────────────────────────────

interface TemplateLike {
  subsystem: string;
  name: string;
  sql: string;
}

interface HistoryRowProps {
  entry: HistoryItem;
  onPick: () => void;
  onRename: (name: string | null) => void;
  onToggleFavorite: () => void;
  onDelete: () => void;
}

function HistoryRow({
  entry,
  onPick,
  onRename,
  onToggleFavorite,
  onDelete,
}: HistoryRowProps) {
  const { t } = useTranslation();
  const [editing, setEditing] = React.useState(false);
  const [draft, setDraft] = React.useState(entry.name ?? "");
  const inputRef = React.useRef<HTMLInputElement>(null);

  React.useEffect(() => {
    if (editing) inputRef.current?.focus();
  }, [editing]);

  const commit = () => {
    const next = draft.trim();
    onRename(next.length === 0 ? null : next);
    setEditing(false);
  };

  return (
    <div className="group rounded-md border border-border/60 px-2 py-1.5 transition-colors hover:bg-secondary/40">
      <div className="flex items-start gap-1.5">
        <button
          type="button"
          onClick={onToggleFavorite}
          aria-pressed={entry.favorite ?? false}
          aria-label={
            entry.favorite ? t("query.unfavorite") : t("query.favorite")
          }
          title={entry.favorite ? t("query.unfavorite") : t("query.favorite")}
          className={cn(
            "mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded transition-colors",
            entry.favorite
              ? "text-amber-500"
              : "text-muted-foreground/60 hover:text-amber-500",
          )}
        >
          <Star
            className={cn("h-3.5 w-3.5", entry.favorite ? "fill-current" : "")}
          />
        </button>

        <div className="min-w-0 flex-1">
          {editing ? (
            <div className="flex items-center gap-1">
              <Input
                ref={inputRef}
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") commit();
                  if (e.key === "Escape") {
                    setDraft(entry.name ?? "");
                    setEditing(false);
                  }
                }}
                placeholder={t("query.renamePlaceholder")}
                className="h-6 text-[11px]"
              />
              <button
                type="button"
                onClick={commit}
                aria-label={t("common.save")}
                className="flex h-6 w-6 items-center justify-center rounded text-muted-foreground hover:bg-secondary hover:text-foreground"
              >
                <Check className="h-3.5 w-3.5" />
              </button>
              <button
                type="button"
                onClick={() => {
                  setDraft(entry.name ?? "");
                  setEditing(false);
                }}
                aria-label={t("common.cancel")}
                className="flex h-6 w-6 items-center justify-center rounded text-muted-foreground hover:bg-secondary hover:text-foreground"
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
          ) : (
            <button
              type="button"
              onClick={onPick}
              className="block w-full text-left"
            >
              <div className="flex items-center justify-between gap-2 text-[10px] text-muted-foreground tabular-nums">
                <span className="truncate">
                  {entry.name ? (
                    <span className="text-xs font-medium text-foreground">
                      {entry.name}
                    </span>
                  ) : (
                    new Date(entry.ts).toLocaleTimeString()
                  )}
                </span>
                <span>{formatMs(entry.elapsed_ms)}</span>
              </div>
              <div className="mt-1 line-clamp-2 font-mono text-[11px]">
                {entry.sql}
              </div>
            </button>
          )}
        </div>

        {!editing ? (
          <div className="flex shrink-0 items-center gap-0.5 opacity-0 transition-opacity group-hover:opacity-100">
            <button
              type="button"
              onClick={() => {
                setDraft(entry.name ?? "");
                setEditing(true);
              }}
              aria-label={t("query.rename")}
              title={t("query.rename")}
              className="flex h-6 w-6 items-center justify-center rounded text-muted-foreground hover:bg-secondary hover:text-foreground"
            >
              <Pencil className="h-3.5 w-3.5" />
            </button>
            <button
              type="button"
              onClick={onDelete}
              aria-label={t("query.deleteEntry")}
              title={t("query.deleteEntry")}
              className="flex h-6 w-6 items-center justify-center rounded text-muted-foreground hover:bg-destructive/15 hover:text-destructive"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          </div>
        ) : null}
      </div>
    </div>
  );
}

interface RightSidebarProps {
  collapsed: boolean;
  onToggle: () => void;
  tab: "templates" | "history";
  setTab: (v: "templates" | "history") => void;
  templatesLoading: boolean;
  templates: TemplateLike[];
  onPickTemplate: (sql: string) => void;
  history: HistoryItem[];
  onPickHistory: (sql: string) => void;
  favoritesOnly: boolean;
  onToggleFavoritesOnly: () => void;
  onRenameHistory: (id: string, name: string | null) => void;
  onToggleHistoryFavorite: (id: string, favorite: boolean) => void;
  onDeleteHistory: (id: string) => void;
}

function RightSidebar({
  collapsed,
  onToggle,
  tab,
  setTab,
  templatesLoading,
  templates,
  onPickTemplate,
  history,
  onPickHistory,
  favoritesOnly,
  onToggleFavoritesOnly,
  onRenameHistory,
  onToggleHistoryFavorite,
  onDeleteHistory,
}: RightSidebarProps) {
  const { t } = useTranslation();

  return (
    <Card className="flex min-h-0 flex-col overflow-hidden">
      <div
        className={cn(
          "flex items-center border-b",
          collapsed ? "justify-center px-2 py-2" : "justify-between gap-2 px-3 py-2",
        )}
      >
        <button
          type="button"
          onClick={onToggle}
          aria-label={collapsed ? t("query.expandSidebar") : t("query.collapseSidebar")}
          title={collapsed ? t("query.expandSidebar") : t("query.collapseSidebar")}
          className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-secondary hover:text-foreground"
        >
          {collapsed ? (
            <PanelRightOpen className="h-4 w-4" />
          ) : (
            <PanelRightClose className="h-4 w-4" />
          )}
        </button>
        {!collapsed ? (
          <Tabs
            value={tab}
            onValueChange={(v) => setTab(v as "templates" | "history")}
            className="flex-1"
          >
            <TabsList className="h-8 w-full">
              <TabsTrigger value="templates" className="h-6 flex-1 gap-1.5 text-xs">
                <BookOpen className="h-3.5 w-3.5" />
                {t("query.templates")}
              </TabsTrigger>
              <TabsTrigger value="history" className="h-6 flex-1 gap-1.5 text-xs">
                <History className="h-3.5 w-3.5" />
                {t("query.history")}
              </TabsTrigger>
            </TabsList>
          </Tabs>
        ) : null}
      </div>

      {collapsed ? (
        <div className="flex flex-col items-center gap-1 p-1.5">
          <RailIconButton
            icon={<BookOpen className="h-4 w-4" />}
            label={t("query.templates")}
            active={tab === "templates"}
            onClick={() => {
              setTab("templates");
              onToggle();
            }}
          />
          <RailIconButton
            icon={<History className="h-4 w-4" />}
            label={t("query.history")}
            active={tab === "history"}
            onClick={() => {
              setTab("history");
              onToggle();
            }}
          />
        </div>
      ) : (
        <Tabs value={tab} onValueChange={(v) => setTab(v as typeof tab)} className="min-h-0 flex-1">
          <TabsContent
            value="templates"
            className="m-0 min-h-0 flex-1 data-[state=inactive]:hidden"
          >
            <ScrollArea className="h-[calc(100vh-15rem)]">
              <div className="space-y-1 p-2">
                {templatesLoading ? (
                  <Skeleton className="h-16" />
                ) : templates.length === 0 ? (
                  <div className="px-2 py-4 text-xs text-muted-foreground">
                    {t("query.noTemplates")}
                  </div>
                ) : (
                  templates.map((tpl, i) => (
                    <button
                      key={`${tpl.subsystem}-${i}`}
                      onClick={() => onPickTemplate(tpl.sql)}
                      className="w-full rounded-md border border-transparent px-2 py-1.5 text-left text-xs transition-colors hover:border-border/60 hover:bg-secondary/50"
                    >
                      <div className="font-medium">{tpl.name}</div>
                      <div className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">
                        {tpl.sql.split("\n")[0]}
                      </div>
                    </button>
                  ))
                )}
              </div>
            </ScrollArea>
          </TabsContent>
          <TabsContent
            value="history"
            className="m-0 min-h-0 flex-1 data-[state=inactive]:hidden"
          >
            <div className="border-b px-2 py-1.5">
              <button
                type="button"
                onClick={onToggleFavoritesOnly}
                aria-pressed={favoritesOnly}
                className={cn(
                  "flex w-full items-center gap-2 rounded-md px-2 py-1 text-xs transition-colors",
                  favoritesOnly
                    ? "bg-amber-500/15 text-amber-600 dark:text-amber-400"
                    : "text-muted-foreground hover:bg-secondary/50",
                )}
                title={t("query.favoritesOnly")}
              >
                <Star
                  className={cn(
                    "h-3.5 w-3.5",
                    favoritesOnly ? "fill-current" : "",
                  )}
                />
                <span>{t("query.favoritesOnly")}</span>
              </button>
            </div>
            <ScrollArea className="h-[calc(100vh-17rem)]">
              <div className="space-y-1.5 p-2">
                {(() => {
                  const visible = favoritesOnly
                    ? history.filter((h) => h.favorite)
                    : history;
                  if (visible.length === 0) {
                    return (
                      <p className="px-2 py-4 text-center text-xs text-muted-foreground">
                        {favoritesOnly
                          ? t("query.noFavorites")
                          : t("query.noQueriesYet")}
                      </p>
                    );
                  }
                  return visible.map((h) => (
                    <HistoryRow
                      key={h.id}
                      entry={h}
                      onPick={() => onPickHistory(h.sql)}
                      onRename={(name) => onRenameHistory(h.id, name)}
                      onToggleFavorite={() =>
                        onToggleHistoryFavorite(h.id, !h.favorite)
                      }
                      onDelete={() => onDeleteHistory(h.id)}
                    />
                  ));
                })()}
              </div>
            </ScrollArea>
          </TabsContent>
        </Tabs>
      )}
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Rail helpers — used when a sidebar is in its collapsed (44px) state.
// ─────────────────────────────────────────────────────────────────────────────

function RailLabel({
  icon,
  label,
  onClick,
}: {
  icon: React.ReactNode;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={label}
      className="flex flex-1 flex-col items-center gap-2 py-3 text-muted-foreground transition-colors hover:bg-secondary/40 hover:text-foreground"
    >
      {icon}
      <span
        className="font-mono text-[10px] uppercase tracking-[0.18em]"
        style={{ writingMode: "vertical-rl" }}
      >
        {label}
      </span>
    </button>
  );
}

function RailIconButton({
  icon,
  label,
  active,
  onClick,
}: {
  icon: React.ReactNode;
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={label}
      aria-label={label}
      // 32px square: fits inside the 44px rail with the surrounding p-1.5
      // (12px total horizontal padding) without overflowing the cell.
      className={cn(
        "flex h-8 w-8 items-center justify-center rounded-md transition-colors",
        active
          ? "bg-secondary text-foreground"
          : "text-muted-foreground hover:bg-secondary/50 hover:text-foreground",
      )}
    >
      {icon}
    </button>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Result table — unchanged
// ─────────────────────────────────────────────────────────────────────────────

function ResultTable({ result }: { result: SqlResult }) {
  return (
    <Table>
      <TableHeader className="sticky top-0 z-10 bg-card">
        <TableRow>
          {result.columns.map((col) => (
            <TableHead key={col} className="whitespace-nowrap font-mono text-xs">
              {col}
            </TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {result.rows.map((row, i) => (
          <TableRow key={i}>
            {row.map((value, j) => (
              <TableCell key={j} className="whitespace-nowrap font-mono text-xs tabular-nums">
                {value === null ? (
                  <span className="text-muted-foreground italic">null</span>
                ) : (
                  String(value)
                )}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
