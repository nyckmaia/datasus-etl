import * as React from "react";
import { useTranslation } from "react-i18next";
import { AlertTriangle, Loader2, Trash2 } from "lucide-react";
import { toast } from "sonner";

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { useStatsOverview } from "@/hooks/useStats";
import { useResetStorage } from "@/hooks/useSettings";
import { formatBytes } from "@/lib/format";
import { cn } from "@/lib/utils";

interface ResetStorageDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

/**
 * 4-digit numeric code generated client-side. Re-rolled on every dialog open
 * (and on selection change) so the user can't muscle-memory the same code
 * across separate destructive actions. Cryptographic strength is overkill —
 * this is purely an anti-fat-finger gate, not authorisation.
 */
function generateCode(): string {
  return String(Math.floor(1000 + Math.random() * 9000));
}

export function ResetStorageDialog({ open, onOpenChange }: ResetStorageDialogProps) {
  const { t } = useTranslation();
  // The overview powers per-row size hints. We don't want to hide a subsystem
  // that has 0 files — the user might still want to scrub the empty folder —
  // but the data tells us at a glance which selection actually saves space.
  const overview = useStatsOverview(false);
  const reset = useResetStorage();

  const [selected, setSelected] = React.useState<Set<string>>(new Set());
  const [code, setCode] = React.useState<string>(generateCode());
  const [typed, setTyped] = React.useState<string>("");

  // Reset transient state every time the dialog opens. Closing is handled by
  // onOpenChange (the parent flips `open` to false).
  React.useEffect(() => {
    if (open) {
      setSelected(new Set());
      setCode(generateCode());
      setTyped("");
    }
  }, [open]);

  const items = React.useMemo(() => {
    const list = (overview.data ?? []).map((s) => ({
      name: s.subsystem,
      label: s.subsystem.toUpperCase(),
      size_bytes: s.size_bytes,
      files: s.files,
    }));
    // Always offer IBGE as a separate row even though it isn't a registered
    // subsystem — it lives under datasus_db/ibge/ and the server endpoint
    // accepts the special name. Size is unknown from /stats/overview, so
    // we just omit it for the IBGE row.
    list.push({
      name: "ibge",
      label: "IBGE",
      size_bytes: -1, // sentinel — UI shows "—" instead of formatBytes
      files: -1,
    });
    return list;
  }, [overview.data]);

  const toggle = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const allSelected = items.length > 0 && items.every((i) => selected.has(i.name));
  const toggleAll = () => {
    setSelected(allSelected ? new Set() : new Set(items.map((i) => i.name)));
  };

  const codeOk = typed === code;
  const canSubmit = selected.size > 0 && codeOk && !reset.isPending;

  const onConfirm = () => {
    const subsystems = Array.from(selected);
    reset.mutate(subsystems, {
      onSuccess: (data) => {
        const freedTotal = data.deleted.reduce((acc, d) => acc + d.freed_bytes, 0);
        toast.success(
          t("settings.reset.successTitle", { count: data.deleted.length }),
          {
            description: t("settings.reset.successDesc", {
              freed: formatBytes(freedTotal),
              skipped: data.skipped.length,
            }),
          },
        );
        onOpenChange(false);
      },
      onError: (err: Error) => {
        toast.error(t("settings.reset.failedTitle"), {
          description: err.message,
        });
      },
    });
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-destructive">
            <AlertTriangle className="h-5 w-5" />
            {t("settings.reset.title")}
          </DialogTitle>
          <DialogDescription className="leading-relaxed">
            {t("settings.reset.warning")}
          </DialogDescription>
        </DialogHeader>

        {/* Subsystem checklist — own borders so disabled visual = lower opacity */}
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              {t("settings.reset.selectWhat")}
            </span>
            <button
              type="button"
              onClick={toggleAll}
              disabled={items.length === 0}
              className="text-xs text-muted-foreground underline-offset-2 transition-colors hover:text-foreground hover:underline disabled:opacity-50"
            >
              {allSelected ? t("common.none") : t("common.all")}
            </button>
          </div>

          <div className="overflow-hidden rounded-md border">
            {overview.isLoading ? (
              <div className="space-y-1 p-2">
                <Skeleton className="h-9 w-full" />
                <Skeleton className="h-9 w-full" />
              </div>
            ) : (
              items.map((item, i) => (
                <label
                  key={item.name}
                  className={cn(
                    "flex cursor-pointer items-center gap-3 px-3 py-2 text-sm transition-colors hover:bg-secondary/40",
                    i > 0 && "border-t",
                    selected.has(item.name) && "bg-destructive/5",
                  )}
                >
                  <input
                    type="checkbox"
                    checked={selected.has(item.name)}
                    onChange={() => toggle(item.name)}
                    className="h-4 w-4 cursor-pointer accent-destructive"
                  />
                  <span className="flex-1 font-mono text-xs font-semibold uppercase tracking-wide">
                    {item.label}
                  </span>
                  <span className="font-mono text-[11px] text-muted-foreground tabular-nums">
                    {item.size_bytes < 0
                      ? "—"
                      : item.files === 0
                        ? t("settings.reset.empty")
                        : formatBytes(item.size_bytes)}
                  </span>
                </label>
              ))
            )}
          </div>
        </div>

        {/* 4-digit confirmation gate */}
        <div className="space-y-2 rounded-md border border-destructive/30 bg-destructive/5 p-3">
          <p className="text-xs leading-relaxed text-muted-foreground">
            {t("settings.reset.codePrompt")}
          </p>
          <div className="flex items-center gap-3">
            <span
              aria-label={t("settings.reset.codeAriaLabel")}
              className="select-all rounded bg-background px-2.5 py-1 font-mono text-base font-bold tracking-[0.4em] text-destructive ring-1 ring-destructive/40"
            >
              {code}
            </span>
            <Input
              type="text"
              inputMode="numeric"
              pattern="[0-9]*"
              autoComplete="off"
              maxLength={4}
              placeholder="• • • •"
              value={typed}
              onChange={(e) => {
                // Restrict to digits and clamp to 4 chars — paste attempts at
                // longer strings get silently truncated.
                const cleaned = e.target.value.replace(/[^0-9]/g, "").slice(0, 4);
                setTyped(cleaned);
              }}
              className={cn(
                "h-9 max-w-[7rem] text-center font-mono text-base tracking-[0.3em]",
                typed.length === 4 &&
                  (codeOk
                    ? "border-emerald-500/50 focus-visible:ring-emerald-500/40"
                    : "border-destructive focus-visible:ring-destructive/40"),
              )}
            />
            {typed.length === 4 ? (
              codeOk ? (
                <span className="text-xs text-emerald-600 dark:text-emerald-400">
                  {t("settings.reset.codeOk")}
                </span>
              ) : (
                <span className="text-xs text-destructive">
                  {t("settings.reset.codeMismatch")}
                </span>
              )
            ) : null}
          </div>
        </div>

        <DialogFooter>
          <Button
            variant="ghost"
            onClick={() => onOpenChange(false)}
            disabled={reset.isPending}
          >
            {t("common.cancel")}
          </Button>
          <Button
            variant="destructive"
            onClick={onConfirm}
            disabled={!canSubmit}
          >
            {reset.isPending ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Trash2 className="h-4 w-4" />
            )}
            {reset.isPending
              ? t("settings.reset.deleting")
              : t("settings.reset.confirm", { count: selected.size })}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
