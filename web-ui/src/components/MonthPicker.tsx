import * as React from "react";
import { useTranslation } from "react-i18next";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

/**
 * Build a 12-element list of month names in the active language. Uses
 * `Intl.DateTimeFormat` so we don't have to maintain hand-translated lists
 * — every locale the browser supports just works. Capitalises the first
 * letter (browsers return "janeiro" for pt-BR; capitalised reads better in
 * a dropdown without changing the locale's natural casing for the rest of
 * the word).
 */
function buildLocalizedMonths(locale: string): string[] {
  let formatter: Intl.DateTimeFormat;
  try {
    formatter = new Intl.DateTimeFormat(locale, { month: "long" });
  } catch {
    formatter = new Intl.DateTimeFormat("en", { month: "long" });
  }
  return Array.from({ length: 12 }, (_, i) => {
    const name = formatter.format(new Date(2000, i, 1));
    return name.charAt(0).toLocaleUpperCase(locale) + name.slice(1);
  });
}

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
  const { i18n, t } = useTranslation();

  // Map the i18n language code to a BCP-47 locale tag — `pt` → `pt-BR`
  // (Brazilian Portuguese is what the rest of the app uses, see
  // `setLanguage` in src/i18n/index.ts), everything else passes through.
  const localeTag = i18n.language === "pt" ? "pt-BR" : i18n.language;
  const months = React.useMemo(
    () => buildLocalizedMonths(localeTag),
    [localeTag],
  );

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
          <SelectValue placeholder={t("monthPicker.year")} />
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
          <SelectValue placeholder={t("monthPicker.month")} />
        </SelectTrigger>
        <SelectContent>
          {months.map((name, i) => {
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
