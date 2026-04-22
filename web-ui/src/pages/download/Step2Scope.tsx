import * as React from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import { ArrowLeft, ArrowRight, Info } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { MonthPicker } from "@/components/MonthPicker";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ALL_UFS, BrazilMap } from "@/components/BrazilMap";
import { useSubsystemDetail } from "@/hooks/useStats";
import { cn } from "@/lib/utils";
import { useWizard } from "../DownloadWizard";

function monthToIso(month: string): string {
  // "2024-01" -> "2024-01-01"
  if (/^\d{4}-\d{2}$/.test(month)) return `${month}-01`;
  if (/^\d{4}-\d{2}-\d{2}$/.test(month)) return month;
  return "";
}

function isoToMonth(iso: string): string {
  if (/^\d{4}-\d{2}-\d{2}$/.test(iso)) return iso.slice(0, 7);
  return iso;
}

export function Step2ScopePage() {
  const { state, update } = useWizard();
  const navigate = useNavigate();
  const search = useSearch({ from: "/download/step-2" }) as {
    subsystem?: string;
  };

  React.useEffect(() => {
    if (search.subsystem && search.subsystem !== state.subsystem) {
      update({ subsystem: search.subsystem });
      return;
    }
    if (!search.subsystem && !state.subsystem) {
      navigate({ to: "/download/step-1", replace: true });
    }
  }, [search.subsystem, state.subsystem, update, navigate]);

  const detail = useSubsystemDetail(state.subsystem);
  const perUfByUf = React.useMemo(() => {
    const map = new Map<string, { first: string | null; last: string | null; files: number }>();
    for (const row of detail.data?.per_uf ?? []) {
      map.set(row.uf, {
        first: row.first_period,
        last: row.last_period,
        files: row.files,
      });
    }
    return map;
  }, [detail.data]);

  const selected = React.useMemo(() => new Set(state.ufs), [state.ufs]);

  const toggleUf = (uf: string) => {
    const next = new Set(selected);
    if (next.has(uf)) next.delete(uf);
    else next.add(uf);
    update({ ufs: Array.from(next).sort() });
  };

  const selectAll = () => update({ ufs: [...ALL_UFS] });
  const selectNone = () => update({ ufs: [] });

  const startMonth = isoToMonth(state.start_date);
  const endMonth = isoToMonth(state.end_date);

  const canProceed =
    Boolean(state.start_date) &&
    (state.end_date === "" || state.end_date >= state.start_date);

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-xl font-semibold">Choose scope</h2>
        <p className="text-sm text-muted-foreground">
          Set a date range (month precision) and the federative units to include.
          Leave UFs empty to fetch every state.
        </p>
      </div>

      {state.subsystem === "sim" ? (
        <div className="flex items-start gap-3 rounded-md border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-900 dark:text-amber-200">
          <Info className="mt-0.5 h-4 w-4 shrink-0" />
          <div>
            <div className="font-medium">SIM publication lag</div>
            <p className="mt-0.5 text-xs leading-relaxed">
              SIM data is published with a ~2 year delay due to ICD-10 coding
              review. Pick a date range that ends at least two years in the
              past so the next step can actually find files on the DATASUS
              FTP. If Estimate returns 0 files, widen the range backwards.
            </p>
          </div>
        </div>
      ) : null}

      <Card>
        <CardContent className="grid gap-4 p-5 md:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="start_date">Start month</Label>
            <MonthPicker
              id="start_date"
              value={startMonth}
              onChange={(v) => update({ start_date: monthToIso(v) })}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="end_date">End month</Label>
            <MonthPicker
              id="end_date"
              value={endMonth}
              onChange={(v) => update({ end_date: monthToIso(v) })}
            />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardContent className="p-5">
          <div className="mb-3 flex items-center justify-between">
            <div>
              <div className="font-medium">Federative units</div>
              <div className="text-xs text-muted-foreground">
                {state.ufs.length === 0
                  ? "All UFs"
                  : `${state.ufs.length} of ${ALL_UFS.length} selected`}
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Button size="sm" variant="outline" onClick={selectAll}>
                Select all
              </Button>
              <Button size="sm" variant="outline" onClick={selectNone}>
                Clear
              </Button>
            </div>
          </div>
          <BrazilMap selected={selected} onToggleUf={toggleUf} />
        </CardContent>
      </Card>

      <Card>
        <CardContent className="p-5">
          <div className="mb-3 flex items-center gap-2">
            <div className="font-medium">Existing data per UF</div>
            <div className="text-xs text-muted-foreground">
              {detail.isLoading
                ? "Loading…"
                : detail.isError
                  ? "Unable to load"
                  : perUfByUf.size === 0
                    ? "No data downloaded yet for this subsystem."
                    : `${perUfByUf.size} of ${ALL_UFS.length} UFs with data — click a row to toggle selection.`}
            </div>
          </div>

          {perUfByUf.size === 0 && !detail.isLoading && !detail.isError ? (
            <div className="flex items-start gap-2 rounded-md border border-dashed p-4 text-sm text-muted-foreground">
              <Info className="mt-0.5 h-4 w-4 shrink-0" />
              <span>
                Nothing is downloaded for this subsystem yet. Pick a start month and UFs to
                fetch the full scope.
              </span>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-20">UF</TableHead>
                  <TableHead>Start</TableHead>
                  <TableHead>End</TableHead>
                  <TableHead className="text-right">Files</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {[...ALL_UFS].map((uf) => {
                  const row = perUfByUf.get(uf);
                  const hasData = row !== undefined;
                  const isSelected = selected.has(uf);
                  return (
                    <TableRow
                      key={uf}
                      onClick={() => toggleUf(uf)}
                      className={cn(
                        "cursor-pointer",
                        isSelected && "bg-secondary/60",
                        !hasData && "text-muted-foreground",
                      )}
                      data-state={isSelected ? "selected" : undefined}
                    >
                      <TableCell className="font-mono font-medium">{uf}</TableCell>
                      <TableCell>{row?.first ?? "—"}</TableCell>
                      <TableCell>{row?.last ?? "—"}</TableCell>
                      <TableCell className="text-right tabular-nums">
                        {hasData ? row.files.toLocaleString() : "0"}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <div className="flex items-center justify-between">
        <Button variant="ghost" onClick={() => navigate({ to: "/download/step-1" })}>
          <ArrowLeft className="h-4 w-4" />
          Back
        </Button>
        <Button
          disabled={!canProceed}
          onClick={() => navigate({ to: "/download/step-3" })}
        >
          Continue
          <ArrowRight className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
