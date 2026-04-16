import * as React from "react";
import { useNavigate } from "@tanstack/react-router";
import { ArrowLeft, ArrowRight } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ALL_UFS, BrazilMap } from "@/components/BrazilMap";
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

      <Card>
        <CardContent className="grid gap-4 p-5 md:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="start_date">Start month</Label>
            <Input
              id="start_date"
              type="month"
              value={startMonth}
              onChange={(e) => update({ start_date: monthToIso(e.target.value) })}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="end_date">End month (optional)</Label>
            <Input
              id="end_date"
              type="month"
              value={endMonth}
              onChange={(e) => update({ end_date: monthToIso(e.target.value) })}
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
