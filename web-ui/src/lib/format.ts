import { formatDistanceToNow } from "date-fns";
import { ptBR, enUS } from "date-fns/locale";
import i18n from "@/i18n";

const UNITS = ["B", "KB", "MB", "GB", "TB", "PB"] as const;

export function formatBytes(bytes: number | null | undefined, digits = 1): string {
  if (bytes == null || Number.isNaN(bytes) || bytes < 0) return "—";
  if (bytes === 0) return "0 B";
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), UNITS.length - 1);
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(i === 0 ? 0 : digits)} ${UNITS[i]}`;
}

export function formatNumber(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—";
  return new Intl.NumberFormat("en-US").format(n);
}

export function formatCompact(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—";
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(n);
}

export function formatRelative(unixTsSeconds: number | null | undefined): string {
  if (!unixTsSeconds) return i18n.language === "pt" ? "nunca" : "never";
  try {
    const locale = i18n.language === "pt" ? ptBR : enUS;
    return formatDistanceToNow(new Date(unixTsSeconds * 1000), {
      addSuffix: true,
      locale,
    });
  } catch {
    return i18n.language === "pt" ? "desconhecido" : "unknown";
  }
}

export function formatPercent(fraction: number, digits = 0): string {
  return `${(fraction * 100).toFixed(digits)}%`;
}

export function formatMs(ms: number | null | undefined): string {
  if (ms == null || Number.isNaN(ms)) return "—";
  if (ms < 1000) return `${ms.toFixed(0)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}
