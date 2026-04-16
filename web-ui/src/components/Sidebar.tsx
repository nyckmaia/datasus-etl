import { Link, useRouterState } from "@tanstack/react-router";
import { BarChart3, Download, Database, Settings, Zap } from "lucide-react";

import { cn } from "@/lib/utils";
import { useSettings } from "@/hooks/useSettings";
import { Skeleton } from "@/components/ui/skeleton";

interface NavItem {
  to: string;
  label: string;
  icon: React.ElementType;
}

const NAV: NavItem[] = [
  { to: "/", label: "Dashboard", icon: BarChart3 },
  { to: "/download", label: "Download", icon: Download },
  { to: "/query", label: "Query", icon: Database },
  { to: "/settings", label: "Settings", icon: Settings },
];

export function Sidebar() {
  const { location } = useRouterState();
  const { data: settings } = useSettings();

  const isActive = (to: string) => {
    if (to === "/") return location.pathname === "/";
    return location.pathname.startsWith(to);
  };

  return (
    <aside className="flex h-full w-60 shrink-0 flex-col border-r bg-card">
      <div className="flex items-center gap-2 px-5 py-5">
        <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary text-primary-foreground">
          <Zap className="h-4 w-4" />
        </div>
        <div className="flex flex-col leading-tight">
          <span className="text-sm font-semibold">DataSUS ETL</span>
          <span className="text-xs text-muted-foreground">
            {settings ? `v${settings.version}` : <Skeleton className="h-3 w-10" />}
          </span>
        </div>
      </div>

      <nav className="flex-1 space-y-1 px-3 py-2">
        {NAV.map((item) => {
          const Icon = item.icon;
          const active = isActive(item.to);
          return (
            <Link
              key={item.to}
              to={item.to}
              className={cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                active
                  ? "bg-secondary text-foreground"
                  : "text-muted-foreground hover:bg-secondary/50 hover:text-foreground",
              )}
            >
              <Icon className="h-4 w-4" />
              {item.label}
            </Link>
          );
        })}
      </nav>

      <div className="border-t px-4 py-3 text-xs text-muted-foreground">
        <div className="truncate font-medium text-foreground">Available subsystems</div>
        <div className="mt-2 flex flex-wrap gap-1">
          {settings ? (
            settings.subsystems.map((s) => (
              <span
                key={s.name}
                title={s.description}
                className="rounded border px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide"
              >
                {s.name}
              </span>
            ))
          ) : (
            <Skeleton className="h-5 w-full" />
          )}
        </div>
      </div>
    </aside>
  );
}
