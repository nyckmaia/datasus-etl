import { format } from "sql-formatter";

export function prettySql(raw: string): string {
  if (!raw || !raw.trim()) return raw;
  try {
    return format(raw, {
      language: "duckdb",
      keywordCase: "upper",
      indentStyle: "standard",
      logicalOperatorNewline: "before",
      expressionWidth: 80,
    });
  } catch {
    return raw;
  }
}
