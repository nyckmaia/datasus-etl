import * as React from "react";

import { cn } from "@/lib/utils";

// TODO: replace with real SVG geometry (GeoJSON of Brazilian UFs).
// This placeholder lays out the 27 federative units on a rough 7x5 grid,
// positioned to match their geographic neighbourhood so the map still
// communicates regional patterns at a glance. The component API is stable:
// callers pass `valuesByUf` (any numeric score) and optionally control
// selection via `selected` / `onToggleUf`. When the real SVG lands, keep
// these props and swap the internal rendering.

export const ALL_UFS: readonly string[] = [
  "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
  "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
  "RO", "RR", "RS", "SC", "SE", "SP", "TO",
] as const;

// Coarse geographic layout on a 7 (cols) x 5 (rows) grid. North → top,
// South → bottom. Some rows are purposely staggered to approximate
// neighbours (e.g. MG sits next to RJ and ES).
const GRID: Record<string, [number, number]> = {
  RR: [2, 0], AP: [4, 0],
  AM: [1, 1], PA: [3, 1], MA: [4, 1], CE: [5, 1], RN: [6, 1],
  AC: [0, 2], RO: [1, 2], TO: [3, 2], PI: [4, 2], PB: [6, 2],
  MT: [2, 2], BA: [5, 2], PE: [6, 3],
  DF: [3, 3], GO: [2, 3], MG: [4, 3], ES: [5, 3], AL: [6, 4], SE: [5, 4],
  MS: [2, 4], SP: [3, 4], RJ: [4, 4],
  PR: [2, 5], SC: [3, 5], RS: [2, 6],
};

export interface BrazilMapProps {
  /** Optional metric per UF (e.g. file count) used to color cells. */
  valuesByUf?: Record<string, number>;
  /** If provided, enables multi-select mode. */
  selected?: ReadonlySet<string>;
  /** Called when a UF cell is clicked in selectable mode. */
  onToggleUf?: (uf: string) => void;
  /** Optional label rendered for each cell instead of the numeric value. */
  labelFor?: (uf: string) => React.ReactNode;
  className?: string;
  /** If true, cells are rendered as read-only (no hover affordance). */
  readOnly?: boolean;
}

function colorForValue(value: number, max: number): string {
  if (max <= 0 || value <= 0) return "hsl(var(--muted))";
  const t = Math.min(1, value / max);
  // Interpolate toward the primary color (Brazilian green).
  // Using HSL directly so we blend well with the theme variables.
  const lightness = 50 - t * 20; // 50% -> 30%
  const saturation = 70;
  return `hsl(142 ${saturation}% ${lightness}%)`;
}

const CELL = 44;
const GAP = 4;
const COLS = 7;
const ROWS = 7;

export function BrazilMap({
  valuesByUf,
  selected,
  onToggleUf,
  labelFor,
  className,
  readOnly,
}: BrazilMapProps) {
  const max = React.useMemo(() => {
    if (!valuesByUf) return 0;
    return Math.max(0, ...Object.values(valuesByUf));
  }, [valuesByUf]);

  const interactive = !readOnly && typeof onToggleUf === "function";

  return (
    <div
      className={cn("relative inline-block", className)}
      role={interactive ? "group" : "img"}
      aria-label="Map of Brazilian states"
    >
      <svg
        viewBox={`0 0 ${COLS * (CELL + GAP)} ${ROWS * (CELL + GAP)}`}
        width="100%"
        height="100%"
        preserveAspectRatio="xMidYMid meet"
        className="max-w-full"
      >
        {ALL_UFS.map((uf) => {
          const pos = GRID[uf];
          if (!pos) return null;
          const [col, row] = pos;
          const x = col * (CELL + GAP);
          const y = row * (CELL + GAP);
          const value = valuesByUf?.[uf] ?? 0;
          const fill = valuesByUf ? colorForValue(value, max) : "hsl(var(--secondary))";
          const isSelected = selected?.has(uf) ?? false;
          const strokeColor = isSelected ? "hsl(var(--primary))" : "hsl(var(--border))";
          const strokeWidth = isSelected ? 2.5 : 1;

          return (
            <g
              key={uf}
              data-uf={uf}
              transform={`translate(${x} ${y})`}
              onClick={interactive ? () => onToggleUf?.(uf) : undefined}
              style={{ cursor: interactive ? "pointer" : "default" }}
              className={cn(
                "transition-opacity",
                interactive && "hover:opacity-80",
              )}
              role={interactive ? "button" : undefined}
              tabIndex={interactive ? 0 : undefined}
              onKeyDown={
                interactive
                  ? (e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        onToggleUf?.(uf);
                      }
                    }
                  : undefined
              }
            >
              <title>
                {uf}
                {valuesByUf ? ` — ${value}` : ""}
              </title>
              <rect
                width={CELL}
                height={CELL}
                rx={6}
                fill={fill}
                stroke={strokeColor}
                strokeWidth={strokeWidth}
              />
              <text
                x={CELL / 2}
                y={CELL / 2 + 1}
                textAnchor="middle"
                dominantBaseline="middle"
                fontFamily="Inter, sans-serif"
                fontSize={12}
                fontWeight={600}
                fill={
                  valuesByUf && value > max * 0.6
                    ? "hsl(var(--primary-foreground))"
                    : "hsl(var(--foreground))"
                }
              >
                {uf}
              </text>
              {labelFor ? (
                <text
                  x={CELL / 2}
                  y={CELL - 6}
                  textAnchor="middle"
                  fontFamily="JetBrains Mono, monospace"
                  fontSize={9}
                  fill="hsl(var(--muted-foreground))"
                >
                  {labelFor(uf)}
                </text>
              ) : null}
            </g>
          );
        })}
      </svg>
    </div>
  );
}
