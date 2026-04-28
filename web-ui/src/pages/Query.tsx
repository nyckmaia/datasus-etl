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
  Search,
  Columns3,
  Wand2,
} from "lucide-react";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
import { ColumnTypeBadge } from "@/components/ColumnTypeBadge";
import { ColumnFillBadge } from "@/components/ColumnFillBadge";
import { ColumnFillBar } from "@/components/ColumnFillBar";
import { ColumnDistinctBadge } from "@/components/ColumnDistinctBadge";
import { QueryTabsBar, type QueryTab } from "@/components/QueryTabsBar";
import { QueryBuilderPanel } from "@/components/QueryBuilderPanel";
import { api } from "@/lib/api";
import type { SqlResult } from "@/lib/api";
import { cn } from "@/lib/utils";
import {
  registerSqlAutocomplete,
  updateAutocompleteState,
} from "@/lib/sqlAutocomplete";
import { formatMs } from "@/lib/format";
import { useStatsOverview } from "@/hooks/useStats";
import { useSqlQuery } from "@/hooks/useSqlQuery";
import { useLocalStorage } from "@/hooks/useLocalStorage";
import { useTheme } from "@/components/ThemeProvider";

interface HistoryItem {
  id: string;
  sql: string;
  ts: number;
  rows: number;
  elapsed_ms: number;
}

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
const LEFT_EXPANDED = "280px";
const RIGHT_EXPANDED = "300px";
const RAIL = "44px";

export function QueryPage() {
  const { t } = useTranslation();
  const { resolvedTheme } = useTheme();
  // The subsystem dropdown is populated from the overview endpoint (which
  // reports actual file counts) rather than the registry, so subsystems
  // with no downloaded data are hidden — querying them would yield empty
  // VIEWs and confuse the user.
  const overview = useStatsOverview(false);
  const runSql = useSqlQuery();
  // The Dashboard's subsystem cards link here with `?subsystem=<name>` so the
  // selectbox can be pre-filled with the card the user clicked.
  const search = useSearch({ from: "/query" }) as { subsystem?: string };

  const availableSubsystems = React.useMemo(
    () => (overview.data ?? []).filter((d) => d.files > 0),
    [overview.data],
  );

  const [subsystem, setSubsystem] = React.useState<string>(search.subsystem ?? "");
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
  const [history, setHistory] = React.useState<HistoryItem[]>([]);
  const [limit, setLimit] = React.useState<number>(1000);
  const [columnFilter, setColumnFilter] = React.useState<string>("");
  // Visual question builder — opens a Sheet panel; on Apply it writes the
  // compiled SQL into the active tab via `replaceSql`.
  const [builderOpen, setBuilderOpen] = React.useState<boolean>(false);

  // Defensive: if the persisted active id no longer matches a tab (or was
  // never set on a fresh install), snap to the first tab on first render.
  React.useEffect(() => {
    if (tabs.length === 0) {
      const seed = makeTab(`${QUERY_TAB_PREFIX} 1`, subsystem ? buildDefaultSql(subsystem) : "");
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
    const seedSql = subsystem ? buildDefaultSql(subsystem) : "";
    const tab = makeTab(nextQueryName(tabs), seedSql);
    setTabs((prev) => [...prev, tab]);
    setActiveTabId(tab.id);
  }, [tabs, subsystem, setTabs, setActiveTabId]);

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
          subsystem ? buildDefaultSql(subsystem) : "",
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
    [tabs, activeTabId, subsystem, setTabs, setActiveTabId],
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

  const dictionary = useQuery({
    queryKey: ["query", "dictionary", subsystem],
    queryFn: () => api.dictionary(subsystem),
    enabled: Boolean(subsystem),
  });

  React.useEffect(() => {
    const stillAvailable = availableSubsystems.some((d) => d.subsystem === subsystem);
    if (stillAvailable) return;
    const fromUrl = search.subsystem;
    const requestedAvailable =
      fromUrl && availableSubsystems.some((d) => d.subsystem === fromUrl);
    setSubsystem(
      requestedAvailable ? fromUrl! : availableSubsystems[0]?.subsystem ?? "",
    );
  }, [availableSubsystems, subsystem, search.subsystem]);

  // Auto-seed / refresh the active tab with `SELECT * from <subsystem> limit 10;`
  // whenever the subsystem changes. Per-tab — only the active tab is touched,
  // and only if the user hasn't edited it yet. Other tabs keep their own SQL.
  React.useEffect(() => {
    if (!subsystem || !activeTab) return;
    if (activeTab.userEdited) return;
    const seeded = buildDefaultSql(subsystem);
    if (activeTab.sql === seeded) return;
    patchTab(activeTab.id, { sql: seeded });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [subsystem, activeTab?.id]);

  // Keep the SQL autocomplete provider's data in sync. The provider itself
  // is registered once globally (in Editor's onMount); this effect just
  // pushes the latest column dictionary + active subsystem so the popup
  // reflects the table the user is currently exploring.
  React.useEffect(() => {
    updateAutocompleteState({
      columns: dictionary.data ?? [],
      subsystem,
    });
  }, [dictionary.data, subsystem]);

  // Editor onChange. Marks the active tab as user-edited only when the new
  // value truly diverges from the auto template — otherwise the act of
  // seeding the template would itself flip the flag and prevent future
  // auto-refreshes.
  const onSqlChange = React.useCallback(
    (next: string | undefined) => {
      if (!activeTab) return;
      const value = next ?? "";
      const diverged =
        Boolean(subsystem) && value !== buildDefaultSql(subsystem);
      patchTab(activeTab.id, {
        sql: value,
        userEdited: activeTab.userEdited || diverged,
      });
    },
    [activeTab, subsystem, patchTab],
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
    if (!subsystem) return templates.data;
    return templates.data.filter((t) => t.subsystem === subsystem);
  }, [templates.data, subsystem]);

  const filteredColumns = React.useMemo(() => {
    if (!dictionary.data) return [];
    const q = columnFilter.trim().toLowerCase();
    if (!q) return dictionary.data;
    return dictionary.data.filter(
      (e) =>
        e.column.toLowerCase().includes(q) ||
        e.description.toLowerCase().includes(q),
    );
  }, [dictionary.data, columnFilter]);

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
      setRunningTabs((prev) => ({ ...prev, [tabId]: true }));
      runSql.mutate(
        { sql: sqlToRun, limit },
        {
          onSuccess: (data) => {
            setResultsByTab((prev) => ({ ...prev, [tabId]: data }));
            setHistory((h) =>
              [
                {
                  id: crypto.randomUUID(),
                  sql: sqlToRun,
                  ts: Date.now(),
                  rows: data.row_count,
                  elapsed_ms: data.elapsed_ms,
                },
                ...h,
              ].slice(0, 50),
            );
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
    [activeTabId, limit, runSql, t],
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

  // One-tap "show histogram for this column" handler. Wired to the click of
  // the indigo distinct-count badge. Builds a default GROUP BY query and
  // runs it immediately so the user sees the value distribution in the
  // results table without having to type anything.
  const onColumnHistogram = React.useCallback(
    (column: string) => {
      if (!subsystem) return;
      // Quote the column with double-quotes so reserved words / mixed case
      // don't break — DuckDB respects the identifier as-is.
      const sql = `SELECT
    "${column}",
    COUNT(*) AS count
FROM ${subsystem}
WHERE "${column}" IS NOT NULL
GROUP BY "${column}"
ORDER BY count DESC
LIMIT 100;`;
      replaceSql(sql);
      runWithSql(sql);
    },
    [subsystem, replaceSql, runWithSql],
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
        {/* ───────────────── LEFT SIDEBAR — Subsystem + Columns ───────────────── */}
        <LeftSidebar
          collapsed={leftCollapsed}
          onToggle={() => setLeftCollapsed((v) => !v)}
          subsystem={subsystem}
          setSubsystem={setSubsystem}
          availableSubsystems={availableSubsystems.map((d) => d.subsystem)}
          loadingOverview={overview.isLoading}
          loadingDictionary={dictionary.isLoading}
          columns={filteredColumns}
          totalColumns={dictionary.data?.length ?? 0}
          columnFilter={columnFilter}
          setColumnFilter={setColumnFilter}
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
                  max={10000}
                  onChange={(e) => setLimit(Number(e.target.value) || 1000)}
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
                  disabled={!subsystem || (dictionary.data?.length ?? 0) === 0}
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
            // Keyed on subsystem so the panel's per-subsystem
            // useLocalStorage hooks remount with the right keys when the
            // user switches subsystems (each subsystem keeps an independent
            // builder workspace).
            key={`builder-${subsystem || "none"}`}
            open={builderOpen}
            onOpenChange={setBuilderOpen}
            subsystem={subsystem}
            columns={dictionary.data ?? []}
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
                      <Badge variant="warning">{t("query.truncated")}</Badge>
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
  subsystem: string;
  setSubsystem: (s: string) => void;
  availableSubsystems: string[];
  loadingOverview: boolean;
  loadingDictionary: boolean;
  columns: {
    column: string;
    description: string;
    type: string;
    fill_pct?: number | null;
    fill_pct_approx?: boolean;
    distinct_count?: number | null;
    distinct_count_approx?: boolean;
  }[];
  /** Click handler for the distinct-count badge — opens a histogram query. */
  onColumnHistogram: (column: string) => void;
  totalColumns: number;
  columnFilter: string;
  setColumnFilter: (s: string) => void;
}

function LeftSidebar({
  collapsed,
  onToggle,
  subsystem,
  setSubsystem,
  availableSubsystems,
  loadingOverview,
  loadingDictionary,
  columns,
  totalColumns,
  columnFilter,
  setColumnFilter,
  onColumnHistogram,
}: LeftSidebarProps) {
  const { t } = useTranslation();

  return (
    <Card className="flex min-h-0 flex-col overflow-hidden">
      {/* Header bar — always visible. In rail mode it shrinks to just the
          toggle, the rest stays hidden behind the collapsed width. */}
      <div
        className={cn(
          "flex items-center border-b",
          collapsed ? "justify-center px-2 py-2" : "justify-between px-3 py-2",
        )}
      >
        {!collapsed ? (
          <span className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            <Columns3 className="h-3.5 w-3.5" />
            {t("query.subsystem")}
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
          label={t("query.columns")}
          onClick={onToggle}
        />
      ) : (
        <div className="flex min-h-0 flex-1 flex-col">
          <div className="space-y-3 border-b p-3">
            <Select
              value={subsystem}
              onValueChange={setSubsystem}
              disabled={availableSubsystems.length === 0}
            >
              <SelectTrigger>
                <SelectValue
                  placeholder={
                    loadingOverview
                      ? t("common.loading")
                      : availableSubsystems.length === 0
                        ? t("query.noDataDownloaded")
                        : t("query.selectSubsystem")
                  }
                />
              </SelectTrigger>
              <SelectContent>
                {availableSubsystems.map((s) => (
                  <SelectItem key={s} value={s}>
                    {s.toUpperCase()}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {!loadingOverview && availableSubsystems.length === 0 ? (
              <p className="text-xs text-muted-foreground">
                {t("query.downloadToQuery")}
              </p>
            ) : null}
          </div>

          {/* Always-visible columns panel — no tabs, just a dense list. */}
          <div className="flex min-h-0 flex-1 flex-col">
            <div className="flex items-center justify-between gap-2 border-b px-3 py-2">
              <span className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
                {t("query.columns")}
              </span>
              <span className="font-mono text-[10px] text-muted-foreground tabular-nums">
                {totalColumns > 0 && columnFilter
                  ? `${columns.length}/${totalColumns}`
                  : totalColumns || "—"}
              </span>
            </div>
            <div className="border-b p-2">
              <div className="relative">
                <Search className="pointer-events-none absolute left-2 top-1/2 h-3 w-3 -translate-y-1/2 text-muted-foreground" />
                <Input
                  value={columnFilter}
                  onChange={(e) => setColumnFilter(e.target.value)}
                  placeholder={t("query.findColumn")}
                  className="h-7 pl-6 text-xs"
                />
              </div>
            </div>
            <ScrollArea className="min-h-0 flex-1">
              <div className="space-y-0.5 p-1.5">
                {loadingDictionary ? (
                  <div className="space-y-1.5 p-1.5">
                    <Skeleton className="h-8 w-full" />
                    <Skeleton className="h-8 w-full" />
                    <Skeleton className="h-8 w-3/4" />
                  </div>
                ) : columns.length === 0 ? (
                  <div className="px-2 py-4 text-center text-xs text-muted-foreground">
                    {totalColumns === 0
                      ? t("query.noDictionary")
                      : t("query.noColumnMatch")}
                  </div>
                ) : (
                  columns.map((entry) => (
                    <div
                      key={entry.column}
                      className="group rounded-md border border-transparent px-2 py-1.5 text-xs transition-colors hover:border-border/60 hover:bg-secondary/50"
                    >
                      <div className="flex items-center justify-between gap-2">
                        <span className="truncate font-mono font-medium">
                          {entry.column}
                        </span>
                        {/* Three badges, left → right:
                              1. fill-pct  (data-quality lane, emerald→rose)
                              2. distinct-count  (cardinality lane, indigo) — clickable, opens histogram
                              3. type  (data-type lane, multi-color)
                            The three palettes don't overlap so the row stays
                            scannable even when packed tightly. */}
                        <div className="flex shrink-0 items-center gap-1">
                          <ColumnFillBadge
                            fillPct={entry.fill_pct}
                            approx={entry.fill_pct_approx}
                          />
                          <ColumnDistinctBadge
                            count={entry.distinct_count}
                            approx={entry.distinct_count_approx}
                            onClick={() => onColumnHistogram(entry.column)}
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
                  ))
                )}
              </div>
            </ScrollArea>
          </div>
        </div>
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
            <ScrollArea className="h-[calc(100vh-15rem)]">
              <div className="space-y-2 p-2">
                {history.length === 0 ? (
                  <p className="px-2 py-4 text-center text-xs text-muted-foreground">
                    {t("query.noQueriesYet")}
                  </p>
                ) : (
                  history.map((h) => (
                    <button
                      key={h.id}
                      onClick={() => onPickHistory(h.sql)}
                      className="w-full rounded-md border border-border/60 px-2 py-1.5 text-left text-xs transition-colors hover:bg-secondary/50"
                    >
                      <div className="flex items-center justify-between text-[10px] text-muted-foreground tabular-nums">
                        <span>{new Date(h.ts).toLocaleTimeString()}</span>
                        <span>{formatMs(h.elapsed_ms)}</span>
                      </div>
                      <div className="mt-1 line-clamp-2 font-mono text-[11px]">
                        {h.sql}
                      </div>
                    </button>
                  ))
                )}
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
