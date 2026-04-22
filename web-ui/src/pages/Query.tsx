import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import Editor from "@monaco-editor/react";
import { Play, Download, FileSpreadsheet, History, BookOpen } from "lucide-react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
import { api } from "@/lib/api";
import type { SqlResult } from "@/lib/api";
import { formatMs, formatNumber } from "@/lib/format";
import { useStatsOverview } from "@/hooks/useStats";
import { useSqlQuery } from "@/hooks/useSqlQuery";
import { useTheme } from "@/components/ThemeProvider";

interface HistoryItem {
  id: string;
  sql: string;
  ts: number;
  rows: number;
  elapsed_ms: number;
}

const DEFAULT_SQL = "SELECT 1 AS n;";

export function QueryPage() {
  const { resolvedTheme } = useTheme();
  // The subsystem dropdown is populated from the overview endpoint (which
  // reports actual file counts) rather than the registry, so subsystems
  // with no downloaded data are hidden — querying them would yield empty
  // VIEWs and confuse the user.
  const overview = useStatsOverview(false);
  const runSql = useSqlQuery();

  const availableSubsystems = React.useMemo(
    () => (overview.data ?? []).filter((d) => d.files > 0),
    [overview.data],
  );

  const [subsystem, setSubsystem] = React.useState<string>("");
  const [sql, setSql] = React.useState<string>(DEFAULT_SQL);
  const [result, setResult] = React.useState<SqlResult | null>(null);
  const [history, setHistory] = React.useState<HistoryItem[]>([]);
  const [limit, setLimit] = React.useState<number>(1000);

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
    // Auto-pick the first available subsystem on mount, and reset when the
    // currently-selected one no longer has data (e.g. the user navigated
    // away, deleted files, and came back).
    const stillAvailable = availableSubsystems.some((d) => d.subsystem === subsystem);
    if (!stillAvailable) {
      setSubsystem(availableSubsystems[0]?.subsystem ?? "");
    }
  }, [availableSubsystems, subsystem]);

  const filteredTemplates = React.useMemo(() => {
    if (!templates.data) return [];
    if (!subsystem) return templates.data;
    return templates.data.filter((t) => t.subsystem === subsystem);
  }, [templates.data, subsystem]);

  const onRun = React.useCallback(() => {
    const payload = { sql, limit };
    runSql.mutate(payload, {
      onSuccess: (data) => {
        setResult(data);
        setHistory((h) =>
          [
            {
              id: crypto.randomUUID(),
              sql,
              ts: Date.now(),
              rows: data.row_count,
              elapsed_ms: data.elapsed_ms,
            },
            ...h,
          ].slice(0, 50),
        );
      },
      onError: (err: Error) => {
        toast.error("Query failed", { description: err.message });
      },
    });
  }, [sql, limit, runSql]);

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
        toast.success(`Exported as ${format.toUpperCase()}`);
      } catch (err) {
        toast.error("Export failed", {
          description: err instanceof Error ? err.message : "unknown",
        });
      }
    },
    [sql, limit],
  );

  return (
    <div className="flex h-[calc(100vh-8rem)] flex-col gap-4">
      <div className="flex items-end justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Query</h1>
          <p className="text-sm text-muted-foreground">
            Ad-hoc SQL against your local parquet store.
          </p>
        </div>
      </div>

      <div className="grid flex-1 min-h-0 gap-4 lg:grid-cols-[280px_1fr_280px]">
        <Card className="overflow-hidden">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm">Subsystem</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <Select
              value={subsystem}
              onValueChange={setSubsystem}
              disabled={availableSubsystems.length === 0}
            >
              <SelectTrigger>
                <SelectValue
                  placeholder={
                    overview.isLoading
                      ? "Loading…"
                      : availableSubsystems.length === 0
                        ? "No data downloaded yet"
                        : "Select subsystem"
                  }
                />
              </SelectTrigger>
              <SelectContent>
                {availableSubsystems.map((d) => (
                  <SelectItem key={d.subsystem} value={d.subsystem}>
                    {d.subsystem.toUpperCase()}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {!overview.isLoading && availableSubsystems.length === 0 ? (
              <p className="text-xs text-muted-foreground">
                Run a download to query data here.
              </p>
            ) : null}

            <Tabs defaultValue="templates">
              <TabsList className="w-full">
                <TabsTrigger value="templates" className="flex-1">
                  <BookOpen className="h-3.5 w-3.5" />
                  Templates
                </TabsTrigger>
                <TabsTrigger value="dictionary" className="flex-1">
                  Columns
                </TabsTrigger>
              </TabsList>
              <TabsContent value="templates">
                <ScrollArea className="h-[calc(100vh-24rem)]">
                  <div className="space-y-1 pr-2">
                    {templates.isLoading ? (
                      <Skeleton className="h-16" />
                    ) : filteredTemplates.length === 0 ? (
                      <div className="px-2 py-4 text-xs text-muted-foreground">
                        No templates.
                      </div>
                    ) : (
                      filteredTemplates.map((t, i) => (
                        <button
                          key={`${t.subsystem}-${i}`}
                          onClick={() => setSql(t.sql)}
                          className="w-full rounded-md px-2 py-1.5 text-left text-xs transition-colors hover:bg-secondary"
                        >
                          <div className="font-medium">{t.name}</div>
                          <div className="mt-0.5 truncate font-mono text-[10px] text-muted-foreground">
                            {t.sql.split("\n")[0]}
                          </div>
                        </button>
                      ))
                    )}
                  </div>
                </ScrollArea>
              </TabsContent>
              <TabsContent value="dictionary">
                <ScrollArea className="h-[calc(100vh-24rem)]">
                  <div className="space-y-1 pr-2">
                    {dictionary.isLoading ? (
                      <Skeleton className="h-16" />
                    ) : !dictionary.data || dictionary.data.length === 0 ? (
                      <div className="px-2 py-4 text-xs text-muted-foreground">
                        No dictionary entries for this subsystem.
                      </div>
                    ) : (
                      dictionary.data.map((entry) => (
                        <div
                          key={entry.column}
                          className="rounded-md px-2 py-1.5 text-xs hover:bg-secondary"
                        >
                          <div className="font-mono font-medium">{entry.column}</div>
                          <div className="mt-0.5 text-[11px] text-muted-foreground">
                            {entry.description}
                          </div>
                        </div>
                      ))
                    )}
                  </div>
                </ScrollArea>
              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>

        <div className="flex min-h-0 flex-col gap-3">
          <Card className="flex flex-col overflow-hidden">
            <div className="flex items-center justify-between border-b px-3 py-2">
              <span className="text-xs font-medium text-muted-foreground">
                SQL editor — Ctrl+Enter to run
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
                      Export
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuItem onClick={() => onExport("csv")}>
                      <Download className="h-4 w-4" />
                      CSV
                    </DropdownMenuItem>
                    <DropdownMenuItem onClick={() => onExport("xlsx")}>
                      <FileSpreadsheet className="h-4 w-4" />
                      Excel (XLSX)
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
                <Button size="sm" onClick={onRun} disabled={runSql.isPending}>
                  <Play className="h-3.5 w-3.5" />
                  {runSql.isPending ? "Running..." : "Run"}
                </Button>
              </div>
            </div>
            <div className="h-64">
              <Editor
                height="100%"
                language="sql"
                theme={resolvedTheme === "dark" ? "vs-dark" : "light"}
                value={sql}
                onChange={(v) => setSql(v ?? "")}
                options={{
                  minimap: { enabled: false },
                  fontFamily: "JetBrains Mono, monospace",
                  fontSize: 13,
                  scrollBeyondLastLine: false,
                  wordWrap: "on",
                  lineNumbers: "on",
                }}
                onMount={(editor, monaco) => {
                  editor.addCommand(
                    monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
                    onRun,
                  );
                }}
              />
            </div>
          </Card>

          <Card className="flex min-h-0 flex-1 flex-col overflow-hidden">
            <div className="flex items-center justify-between border-b px-3 py-2 text-xs">
              <div className="flex items-center gap-3">
                <span className="font-medium">Results</span>
                {result ? (
                  <>
                    <Badge variant="secondary">
                      {formatNumber(result.row_count)} rows
                    </Badge>
                    <span className="text-muted-foreground tabular-nums">
                      {formatMs(result.elapsed_ms)}
                    </span>
                    {result.truncated ? (
                      <Badge variant="outline">Truncated</Badge>
                    ) : null}
                  </>
                ) : null}
              </div>
            </div>
            <div className="min-h-0 flex-1 overflow-auto">
              {runSql.isPending ? (
                <div className="p-4 space-y-2">
                  <Skeleton className="h-6 w-full" />
                  <Skeleton className="h-6 w-full" />
                  <Skeleton className="h-6 w-3/4" />
                </div>
              ) : result ? (
                <ResultTable result={result} />
              ) : (
                <div className="flex h-full items-center justify-center p-6 text-sm text-muted-foreground">
                  Run a query to see results.
                </div>
              )}
            </div>
          </Card>
        </div>

        <Card className="overflow-hidden">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-sm">
              <History className="h-4 w-4" />
              History
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[calc(100vh-20rem)]">
              <div className="space-y-2 pr-2">
                {history.length === 0 ? (
                  <p className="text-xs text-muted-foreground">No queries yet.</p>
                ) : (
                  history.map((h) => (
                    <button
                      key={h.id}
                      onClick={() => setSql(h.sql)}
                      className="w-full rounded-md border px-2 py-1.5 text-left text-xs transition-colors hover:bg-secondary"
                    >
                      <div className="flex items-center justify-between text-[10px] text-muted-foreground">
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
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

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
