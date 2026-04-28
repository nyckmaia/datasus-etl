import type { Monaco } from "@monaco-editor/react";
import type { editor, languages, Position } from "monaco-editor";

import type { DictionaryEntry } from "@/lib/api";
import { abbreviateColumnType } from "@/lib/columnType";

// ─────────────────────────────────────────────────────────────────────────────
// Mutable shared state
//
// Monaco's `registerCompletionItemProvider` is global per language and only
// needs to be registered once. The provider closure reads the latest data
// from this module-level state, which the Query page updates whenever the
// active subsystem or its column dictionary changes.
// ─────────────────────────────────────────────────────────────────────────────

interface AutocompleteState {
  /** DuckDB columns of the currently-selected subsystem's enriched view. */
  columns: DictionaryEntry[];
  /** Active subsystem name — drives the table-name suggestions and dot context. */
  subsystem: string;
}

let state: AutocompleteState = { columns: [], subsystem: "" };
let disposable: { dispose: () => void } | null = null;

export function updateAutocompleteState(next: Partial<AutocompleteState>): void {
  state = { ...state, ...next };
}

// ─────────────────────────────────────────────────────────────────────────────
// Static reference data
// ─────────────────────────────────────────────────────────────────────────────

// SQL keywords. Monaco ships with built-in SQL highlighting but its native
// completion suggestions for `language: "sql"` are weak — we explicitly seed
// the most common DuckDB-flavoured keywords so the user always gets snappy
// keyword completion alongside our column suggestions.
const SQL_KEYWORDS: string[] = [
  "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "BETWEEN",
  "LIKE", "ILIKE", "IS NULL", "IS NOT NULL",
  "GROUP BY", "ORDER BY", "ASC", "DESC", "HAVING", "LIMIT", "OFFSET",
  "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL JOIN", "CROSS JOIN", "ON",
  "AS", "DISTINCT", "ALL", "WITH", "UNION", "UNION ALL", "INTERSECT", "EXCEPT",
  "CASE", "WHEN", "THEN", "ELSE", "END",
  "EXISTS", "ANY", "SOME",
  "COUNT", "SUM", "AVG", "MIN", "MAX", "MEDIAN", "STDDEV",
  "DESCRIBE", "EXPLAIN", "SHOW",
  "CAST", "TRY_CAST", "COALESCE", "NULLIF",
];

// ibge_locais columns we know are stable (defined by utils/ibge_loader.py).
// The full reference table has more metadata columns from the DTB Excel,
// but these 7 are the canonical ones used in JOINs and lookups.
interface StaticColumn {
  name: string;
  type: string;
  description: string;
}

const IBGE_LOCAIS_COLUMNS: StaticColumn[] = [
  {
    name: "codigo_municipio_completo",
    type: "INTEGER",
    description: "Código IBGE de 7 dígitos do município (com dígito verificador)",
  },
  {
    name: "codigo_municipio_6_digitos",
    type: "INTEGER",
    description:
      "Código IBGE de 6 dígitos — chave de JOIN com `sim.codmunres` e `sihsus.munic_res`",
  },
  {
    name: "nome_municipio",
    type: "VARCHAR",
    description: "Nome do município",
  },
  {
    name: "sigla_uf",
    type: "VARCHAR",
    description: "Sigla da UF (SP, RJ, …)",
  },
  {
    name: "nome_uf",
    type: "VARCHAR",
    description: "Nome do estado por extenso",
  },
  {
    name: "nome_regiao_geografica_imediata",
    type: "VARCHAR",
    description: "Região geográfica imediata (IBGE)",
  },
  {
    name: "nome_regiao_geografica_intermediaria",
    type: "VARCHAR",
    description: "Região geográfica intermediária (IBGE)",
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// Provider registration
// ─────────────────────────────────────────────────────────────────────────────

export function registerSqlAutocomplete(monaco: Monaco): void {
  // Idempotent: navigating away and back to /query causes onMount to fire
  // again. Without this guard we'd accumulate one provider per visit, each
  // returning the same suggestions — duplicates in the popup.
  if (disposable) return;

  disposable = monaco.languages.registerCompletionItemProvider("sql", {
    triggerCharacters: ["."],
    provideCompletionItems: (model: editor.ITextModel, position: Position) => {
      const word = model.getWordUntilPosition(position);
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };

      // Detect `<table>.` prefix so we can narrow column suggestions to the
      // relevant table. Monaco's `getWordUntilPosition` only gives us the
      // word at the caret, so we walk back along the current line ourselves.
      const lineUpToWord = model.getValueInRange({
        startLineNumber: position.lineNumber,
        startColumn: 1,
        endLineNumber: position.lineNumber,
        endColumn: word.startColumn,
      });
      const dotMatch = /(\w+)\.\s*$/.exec(lineUpToWord);
      const dotTable = dotMatch ? dotMatch[1].toLowerCase() : null;

      const suggestions: languages.CompletionItem[] = [];
      const sub = state.subsystem;

      const pushColumn = (col: StaticColumn | DictionaryEntry, qualifier?: string) => {
        const rawType = "type" in col ? col.type : "";
        const description = "description" in col ? col.description : "";
        const name = "column" in col ? col.column : col.name;
        const { abbrev, fullType } = abbreviateColumnType(rawType);

        suggestions.push({
          label: {
            label: name,
            // Right-aligned secondary text in the suggestion popup. The
            // abbrev (int / str / date / …) is what the panel already shows.
            description: fullType || abbrev,
          },
          kind: monaco.languages.CompletionItemKind.Field,
          insertText: name,
          // Hover docs in the popup. Includes the table qualifier so the
          // user can disambiguate columns coming from JOINs.
          documentation: {
            value: [
              qualifier ? `**${qualifier}.${name}** · \`${fullType || abbrev}\`` : `**${name}** · \`${fullType || abbrev}\``,
              description ? "" : null,
              description,
            ]
              .filter((x) => x !== null)
              .join("\n\n"),
          },
          range,
          // Sort columns to the very top of the popup. `0_` < `1_` < `2_`.
          sortText: `0_${name}`,
        });
      };

      const pushTable = (name: string, hint: string) => {
        suggestions.push({
          label: { label: name, description: hint },
          kind: monaco.languages.CompletionItemKind.Struct,
          insertText: name,
          detail: hint,
          range,
          sortText: `1_${name}`,
        });
      };

      const pushKeyword = (kw: string) => {
        suggestions.push({
          label: kw,
          kind: monaco.languages.CompletionItemKind.Keyword,
          insertText: kw,
          range,
          sortText: `2_${kw}`,
        });
      };

      // ── Dot context: <table>. — show only the columns of that table ─────
      if (dotTable === "ibge_locais") {
        for (const c of IBGE_LOCAIS_COLUMNS) pushColumn(c, "ibge_locais");
        return { suggestions };
      }
      if (sub && (dotTable === sub || dotTable === `${sub}_all`)) {
        for (const c of state.columns) pushColumn(c, dotTable);
        return { suggestions };
      }
      if (dotTable) {
        // Unknown alias (e.g. `s.` after `FROM sim s`). Without a SQL parser
        // we can't resolve the alias; show the active subsystem's columns
        // as a best-effort fallback rather than nothing.
        for (const c of state.columns) pushColumn(c);
        return { suggestions };
      }

      // ── No dot context: tables + columns + keywords ─────────────────────
      if (sub) {
        pushTable(sub, "view (enriched + IBGE join)");
        pushTable(`${sub}_all`, "view (raw parquet)");
      }
      pushTable("ibge_locais", "view (IBGE municipalities)");

      for (const c of state.columns) pushColumn(c);
      for (const kw of SQL_KEYWORDS) pushKeyword(kw);

      return { suggestions };
    },
  });
}

export function disposeSqlAutocomplete(): void {
  if (disposable) {
    disposable.dispose();
    disposable = null;
  }
}
