import * as React from "react";
import { toast } from "sonner";
import { Save, Info, FolderOpen, AlertTriangle } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import {
  useSettings,
  useUpdateDataDir,
  usePickDirectory,
} from "@/hooks/useSettings";
import { api } from "@/lib/api";
import type { ValidatePathResponse } from "@/lib/api";
import { formatBytes } from "@/lib/format";

export function SettingsPage() {
  const settings = useSettings();
  const update = useUpdateDataDir();
  const pickDir = usePickDirectory();
  const [dataDir, setDataDir] = React.useState<string>("");
  const [validation, setValidation] =
    React.useState<ValidatePathResponse | null>(null);

  React.useEffect(() => {
    if (settings.data?.data_dir) {
      setDataDir(settings.data.data_dir);
    }
  }, [settings.data?.data_dir]);

  // Debounced live path validation. A ref-held token discards stale responses.
  const validateToken = React.useRef(0);
  React.useEffect(() => {
    const trimmed = dataDir.trim();
    if (!trimmed || trimmed === settings.data?.data_dir) {
      setValidation(null);
      return;
    }
    const myToken = ++validateToken.current;
    const handle = setTimeout(() => {
      api
        .validatePath(trimmed)
        .then((res) => {
          if (validateToken.current === myToken) setValidation(res);
        })
        .catch(() => {
          if (validateToken.current === myToken) setValidation(null);
        });
    }, 400);
    return () => clearTimeout(handle);
  }, [dataDir, settings.data?.data_dir]);

  const onSave = () => {
    if (!dataDir.trim()) {
      toast.error("Data directory cannot be empty");
      return;
    }
    update.mutate(dataDir.trim(), {
      onSuccess: () => toast.success("Data directory updated"),
      onError: (err: Error) =>
        toast.error("Update failed", { description: err.message }),
    });
  };

  const handleBrowse = () => {
    pickDir.mutate(undefined, {
      onSuccess: (res) => {
        if (res.error) {
          toast.error(res.error);
          return;
        }
        if (res.cancelled || !res.path) return;
        setDataDir(res.path);
      },
      onError: (err: Error) =>
        toast.error("Folder picker failed", { description: err.message }),
    });
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Settings</h1>
        <p className="text-sm text-muted-foreground">
          Configure where DataSUS ETL stores its parquet files.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Data directory</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="data_dir">Absolute path</Label>
            <div className="flex gap-2">
              <Input
                id="data_dir"
                value={dataDir}
                onChange={(e) => setDataDir(e.target.value)}
                placeholder="/home/user/datasus-data"
                className="flex-1 font-mono"
              />
              <Button
                type="button"
                variant="outline"
                onClick={handleBrowse}
                disabled={pickDir.isPending}
              >
                <FolderOpen className="h-4 w-4" />
                {pickDir.isPending ? "Opening…" : "Browse..."}
              </Button>
            </div>

            {validation?.error ? (
              <div className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive">
                <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                <span>{validation.error}</span>
              </div>
            ) : validation?.exists && !validation.writable ? (
              <div className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive">
                <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                <span>Folder exists but is not writable.</span>
              </div>
            ) : validation?.will_be_created ? (
              <div className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-400">
                <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                <span>
                  This folder does not exist and will be created when you save.
                </span>
              </div>
            ) : null}

            <p className="text-xs text-muted-foreground">
              A <code className="font-mono">datasus_db/</code> subfolder will be
              created inside this directory — that's where every parquet file
              lives. The base directory itself is created if it doesn't exist.
            </p>
          </div>

          {settings.data?.data_dir_resolved ? (
            <div className="rounded-md border bg-muted/40 px-3 py-2 text-xs">
              <span className="text-muted-foreground">
                Files will be stored in:
              </span>{" "}
              <span className="font-mono text-foreground">
                {settings.data.data_dir_resolved}
              </span>
            </div>
          ) : null}

          {settings.data?.free_disk_bytes != null ? (
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <span>
                Disk: {formatBytes(settings.data.free_disk_bytes)} free of{" "}
                {formatBytes(settings.data.total_disk_bytes)}
              </span>
            </div>
          ) : null}

          <div className="flex justify-end">
            <Button onClick={onSave} disabled={update.isPending}>
              <Save className="h-4 w-4" />
              {update.isPending ? "Saving..." : "Save"}
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            <Info className="h-4 w-4" />
            About
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          {settings.isLoading ? (
            <Skeleton className="h-32" />
          ) : settings.data ? (
            <dl className="grid grid-cols-[140px_1fr] gap-2">
              <dt className="text-muted-foreground">Version</dt>
              <dd className="font-mono">{settings.data.version}</dd>

              <dt className="text-muted-foreground">Python</dt>
              <dd className="font-mono">{settings.data.python_version}</dd>

              <dt className="text-muted-foreground">Config file</dt>
              <dd className="truncate font-mono text-xs" title={settings.data.config_file}>
                {settings.data.config_file}
              </dd>

              <dt className="text-muted-foreground">Subsystems</dt>
              <dd className="flex flex-wrap gap-1">
                {settings.data.subsystems.map((s) => (
                  <Badge key={s.name} variant="outline">
                    {s.name.toUpperCase()}
                  </Badge>
                ))}
              </dd>
            </dl>
          ) : null}
        </CardContent>
      </Card>

      <Card className="border-destructive/40">
        <CardHeader>
          <CardTitle className="text-base text-destructive">Danger zone</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          <p className="text-muted-foreground">
            Destructive operations — cleaning caches, resetting storage, etc. —
            will live here. Nothing to see for now.
          </p>
          <Separator />
          <Button variant="destructive" size="sm" disabled>
            Reset local storage (coming soon)
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}
