import { FolderOpen, HardDrive } from "lucide-react";
import { useTranslation } from "react-i18next";

import { useSettings } from "@/hooks/useSettings";
import { formatBytes } from "@/lib/format";
import { Skeleton } from "@/components/ui/skeleton";
import { ThemeToggle } from "./ThemeToggle";
import { LanguageToggle } from "./LanguageToggle";

export function TopBar() {
  const { t } = useTranslation();
  const { data: settings, isLoading } = useSettings();
  const displayPath = settings?.data_dir_resolved ?? settings?.data_dir ?? "";

  return (
    <header className="flex h-14 shrink-0 items-center justify-between border-b bg-background px-6">
      <div className="flex min-w-0 items-center gap-6">
        <div className="flex min-w-0 items-center gap-2 text-sm">
          <FolderOpen className="h-4 w-4 shrink-0 text-muted-foreground" />
          <span className="shrink-0 text-muted-foreground">{t("topBar.dataDir")}</span>
          {isLoading ? (
            <Skeleton className="h-4 w-64" />
          ) : displayPath ? (
            <span className="truncate font-mono text-xs" title={displayPath}>
              {displayPath}
            </span>
          ) : (
            <span className="text-xs italic text-muted-foreground">
              {t("topBar.notConfigured")}
            </span>
          )}
        </div>
        {settings?.free_disk_bytes != null && settings.total_disk_bytes != null ? (
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <HardDrive className="h-3.5 w-3.5" />
            <span className="tabular-nums">
              {t("topBar.freeOf", {
                free: formatBytes(settings.free_disk_bytes),
                total: formatBytes(settings.total_disk_bytes),
              })}
            </span>
          </div>
        ) : null}
      </div>
      <div className="flex items-center gap-2">
        <LanguageToggle />
        <ThemeToggle />
      </div>
    </header>
  );
}
