import { FolderOpen, HardDrive } from "lucide-react";

import { useSettings } from "@/hooks/useSettings";
import { formatBytes } from "@/lib/format";
import { Skeleton } from "@/components/ui/skeleton";
import { ThemeToggle } from "./ThemeToggle";

export function TopBar() {
  const { data: settings, isLoading } = useSettings();

  return (
    <header className="flex h-14 shrink-0 items-center justify-between border-b bg-background px-6">
      <div className="flex min-w-0 items-center gap-6">
        <div className="flex min-w-0 items-center gap-2 text-sm">
          <FolderOpen className="h-4 w-4 shrink-0 text-muted-foreground" />
          <span className="shrink-0 text-muted-foreground">Data dir:</span>
          {isLoading ? (
            <Skeleton className="h-4 w-64" />
          ) : settings?.data_dir ? (
            <span className="truncate font-mono text-xs" title={settings.data_dir}>
              {settings.data_dir}
            </span>
          ) : (
            <span className="text-xs italic text-muted-foreground">
              not configured
            </span>
          )}
        </div>
        {settings?.free_disk_bytes != null && settings.total_disk_bytes != null ? (
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <HardDrive className="h-3.5 w-3.5" />
            <span className="tabular-nums">
              {formatBytes(settings.free_disk_bytes)} free of{" "}
              {formatBytes(settings.total_disk_bytes)}
            </span>
          </div>
        ) : null}
      </div>
      <div className="flex items-center gap-2">
        <ThemeToggle />
      </div>
    </header>
  );
}
