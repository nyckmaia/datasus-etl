import * as React from "react";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const MONTHS = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
] as const;

interface MonthPickerProps {
  id?: string;
  value: string;
  onChange: (value: string) => void;
  minYear?: number;
  maxYear?: number;
  className?: string;
}

function splitValue(v: string): [string, string] {
  if (!v) return ["", ""];
  const [y = "", m = ""] = v.split("-");
  return [y, m];
}

export function MonthPicker({
  id,
  value,
  onChange,
  minYear = 1990,
  maxYear = new Date().getFullYear(),
  className,
}: MonthPickerProps) {
  // Local state for partial selections. The parent `value` is the canonical
  // source of truth, but we hold onto half-picked state internally so the
  // user can pick the dropdowns in any order without the picked one
  // visually reverting (which would happen if we only forwarded to the
  // parent once both halves were set — the parent's state would still be
  // empty and the controlled <Select value={...}> would reset).
  const [year, setYear] = React.useState<string>(() => splitValue(value)[0]);
  const [month, setMonth] = React.useState<string>(() => splitValue(value)[1]);

  // Re-sync when the parent value changes (e.g., wizard reset, or
  // external default). The dependency is the string itself; identity
  // comparison plus React's update bail-out keeps this idempotent.
  React.useEffect(() => {
    const [y, m] = splitValue(value);
    setYear(y);
    setMonth(m);
  }, [value]);

  const years = React.useMemo(
    () =>
      Array.from({ length: maxYear - minYear + 1 }, (_, i) =>
        String(maxYear - i),
      ),
    [minYear, maxYear],
  );

  const handleYear = (y: string) => {
    setYear(y);
    if (y && month) onChange(`${y}-${month}`);
  };
  const handleMonth = (m: string) => {
    setMonth(m);
    if (year && m) onChange(`${year}-${m}`);
  };

  return (
    <div id={id} className={`flex gap-2 ${className ?? ""}`}>
      <Select value={year} onValueChange={handleYear}>
        <SelectTrigger className="w-28">
          <SelectValue placeholder="Year" />
        </SelectTrigger>
        <SelectContent>
          {years.map((y) => (
            <SelectItem key={y} value={y}>
              {y}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      <Select value={month} onValueChange={handleMonth}>
        <SelectTrigger className="w-40">
          <SelectValue placeholder="Month" />
        </SelectTrigger>
        <SelectContent>
          {MONTHS.map((name, i) => {
            const mm = String(i + 1).padStart(2, "0");
            return (
              <SelectItem key={mm} value={mm}>
                {name}
              </SelectItem>
            );
          })}
        </SelectContent>
      </Select>
    </div>
  );
}
