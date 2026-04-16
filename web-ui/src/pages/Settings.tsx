import * as React from "react";
import { toast } from "sonner";
import { Save, Info } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { useSettings, useUpdateDataDir } from "@/hooks/useSettings";
import { formatBytes } from "@/lib/format";

export function SettingsPage() {
  const settings = useSettings();
  const update = useUpdateDataDir();
  const [dataDir, setDataDir] = React.useState<string>("");

  React.useEffect(() => {
    if (settings.data?.data_dir) {
      setDataDir(settings.data.data_dir);
    }
  }, [settings.data?.data_dir]);

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
            <Input
              id="data_dir"
              value={dataDir}
              onChange={(e) => setDataDir(e.target.value)}
              placeholder="/home/user/datasus-data"
              className="font-mono"
            />
            <p className="text-xs text-muted-foreground">
              The directory will be created if it doesn't exist. Parquet files
              live under <code className="font-mono">datasus_db/</code> within
              this path.
            </p>
          </div>

          {settings.data?.data_dir_resolved ? (
            <div className="rounded-md border bg-muted/40 px-3 py-2 font-mono text-xs">
              Resolved to:{" "}
              <span className="text-foreground">
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
