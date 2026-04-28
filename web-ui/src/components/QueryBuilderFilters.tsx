import * as React from "react";
import { QueryBuilder, type Field, type RuleGroupType } from "react-querybuilder";

import "react-querybuilder/dist/query-builder-layout.css";

import { abbreviateColumnType } from "@/lib/columnType";
import { cn } from "@/lib/utils";

interface DictColumn {
  column: string;
  description?: string;
  type?: string;
}

interface QueryBuilderFiltersProps {
  columns: DictColumn[];
  query: RuleGroupType;
  onQueryChange: (q: RuleGroupType) => void;
  className?: string;
}

function inputTypeFor(rawType: string | undefined): Field["inputType"] {
  const { abbrev } = abbreviateColumnType(rawType);
  if (abbrev === "int" || abbrev === "float") return "number";
  if (abbrev === "date") return "date";
  if (abbrev === "ts") return "datetime-local";
  if (abbrev === "time") return "time";
  return "text";
}

// shadcn-flavoured classnames for react-querybuilder's bare controls. The
// library's `controlClassnames` prop lets us style each sub-element by key
// without writing a custom controlElements set.
const SHADCN_CONTROL_CLASSNAMES = {
  queryBuilder: "rqb-shadcn flex flex-col gap-2",
  ruleGroup:
    "rounded-md border border-border/60 bg-muted/30 p-2 flex flex-col gap-2",
  header: "flex items-center gap-2 flex-wrap",
  body: "flex flex-col gap-1.5",
  rule: "flex items-center gap-2 flex-wrap rounded-md border border-border/40 bg-background px-2 py-1.5",
  combinators:
    "h-7 rounded-md border border-input bg-background px-2 text-xs",
  fields:
    "h-7 rounded-md border border-input bg-background px-2 text-xs min-w-[10rem]",
  operators:
    "h-7 rounded-md border border-input bg-background px-2 text-xs min-w-[7rem]",
  value: "h-7 rounded-md border border-input bg-background px-2 text-xs min-w-[8rem]",
  addRule:
    "inline-flex h-7 items-center justify-center gap-1 rounded-md border border-input bg-background px-2.5 text-xs font-medium hover:bg-secondary",
  addGroup:
    "inline-flex h-7 items-center justify-center gap-1 rounded-md border border-input bg-background px-2.5 text-xs font-medium hover:bg-secondary",
  removeRule:
    "ml-auto inline-flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:bg-destructive/15 hover:text-destructive",
  removeGroup:
    "ml-auto inline-flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:bg-destructive/15 hover:text-destructive",
  notToggle: "inline-flex items-center gap-1 text-xs text-muted-foreground",
  cloneRule: "hidden",
  cloneGroup: "hidden",
  lockRule: "hidden",
  lockGroup: "hidden",
};

const TRANSLATIONS = {
  // Drop the "x" labels — Tailwind class on removeRule/removeGroup already
  // shows the icon-style hover state. Empty string keeps the button slot
  // present (so click-to-remove still works) but removes the redundant text.
  removeRule: { label: "×", title: "Remove rule" },
  removeGroup: { label: "×", title: "Remove group" },
  addRule: { label: "+ Rule", title: "Add rule" },
  addGroup: { label: "+ Group", title: "Add group" },
};

export function QueryBuilderFilters({
  columns,
  query,
  onQueryChange,
  className,
}: QueryBuilderFiltersProps) {
  const fields: Field[] = React.useMemo(
    () =>
      columns.map((c) => ({
        name: c.column,
        label: c.column,
        inputType: inputTypeFor(c.type),
      })),
    [columns],
  );

  return (
    <div className={cn("rqb-shadcn-wrapper text-xs", className)}>
      <QueryBuilder
        fields={fields}
        query={query}
        onQueryChange={onQueryChange}
        controlClassnames={SHADCN_CONTROL_CLASSNAMES}
        translations={TRANSLATIONS}
        // Keep the UI flat-and-simple for non-SQL users. Allow nested groups
        // — the library's UX still produces useful WHERE clauses without
        // them, but we don't strip the feature in case a power user wants it.
      />
    </div>
  );
}

/** Initial empty state for the filter editor. Exported so the parent panel
 *  can reset cleanly without re-importing react-querybuilder types. */
export const EMPTY_QUERY: RuleGroupType = { combinator: "and", rules: [] };
