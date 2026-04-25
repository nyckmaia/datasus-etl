import { useNavigate } from "@tanstack/react-router";
import { ArrowRight, Database } from "lucide-react";
import { useTranslation } from "react-i18next";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useSettings } from "@/hooks/useSettings";
import { useWizard } from "../DownloadWizard";

export function Step1SubsystemPage() {
  const { t } = useTranslation();
  const settings = useSettings();
  const { state, update } = useWizard();
  const navigate = useNavigate();

  const canProceed = Boolean(state.subsystem);

  return (
    <div className="space-y-5">
      <div>
        <h2 className="text-xl font-semibold">{t("step1.title")}</h2>
        <p className="text-sm text-muted-foreground">{t("step1.subtitle")}</p>
      </div>

      {settings.isLoading ? (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {Array.from({ length: 6 }).map((_, i) => (
            <Skeleton key={i} className="h-28" />
          ))}
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {settings.data?.subsystems.map((s) => {
            const selected = state.subsystem === s.name;
            return (
              <button
                key={s.name}
                type="button"
                onClick={() => update({ subsystem: s.name })}
                className={cn(
                  "group relative text-left",
                  "focus-visible:outline-none",
                )}
              >
                <Card
                  className={cn(
                    "h-full transition-colors",
                    selected
                      ? "border-primary ring-2 ring-primary/40"
                      : "hover:border-primary/50",
                  )}
                >
                  <CardContent className="p-5">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Database className="h-4 w-4 text-muted-foreground" />
                        <span className="font-mono text-lg font-semibold uppercase tracking-wide">
                          {s.name}
                        </span>
                      </div>
                      <span className="font-mono text-xs uppercase tracking-wide text-muted-foreground">
                        {s.file_prefix}*
                      </span>
                    </div>
                    <p className="mt-2 line-clamp-3 text-sm text-muted-foreground">
                      {s.description || t("step1.fallbackDescription")}
                    </p>
                  </CardContent>
                </Card>
              </button>
            );
          })}
        </div>
      )}

      <div className="flex justify-end">
        <Button
          disabled={!canProceed}
          onClick={() => navigate({ to: "/download/step-2" })}
        >
          {t("common.continue")}
          <ArrowRight className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
