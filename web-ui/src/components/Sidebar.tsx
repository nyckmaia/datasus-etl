import { Link, useRouterState } from "@tanstack/react-router";
import {
  BarChart3,
  Download,
  Database,
  Settings,
  Zap,
  PanelLeftClose,
  PanelLeftOpen,
} from "lucide-react";
import { useTranslation } from "react-i18next";

import { cn } from "@/lib/utils";
import { useSettings } from "@/hooks/useSettings";
import { useLocalStorage } from "@/hooks/useLocalStorage";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface NavItem {
  to: string;
  labelKey: string;
  icon: React.ElementType;
}

const NAV: NavItem[] = [
  { to: "/", labelKey: "sidebar.dashboard", icon: BarChart3 },
  { to: "/download", labelKey: "sidebar.download", icon: Download },
  { to: "/query", labelKey: "sidebar.query", icon: Database },
  { to: "/settings", labelKey: "sidebar.settings", icon: Settings },
];

export function Sidebar() {
  const { t } = useTranslation();
  const { location } = useRouterState();
  const { data: settings } = useSettings();
  // Persisted across reloads — same pattern as the /query page sidebars.
  const [collapsed, setCollapsed] = useLocalStorage("sidebar.collapsed", false);

  const isActive = (to: string) => {
    if (to === "/") return location.pathname === "/";
    return location.pathname.startsWith(to);
  };

  const toggle = () => setCollapsed((v) => !v);

  return (
    <aside
      className={cn(
        "flex h-full shrink-0 flex-col border-r bg-card",
        "transition-[width] duration-200 ease-out",
        collapsed ? "w-14" : "w-60",
      )}
    >
      {/* Header — branding + toggle.
          When expanded: logo + text + collapse button on a single row.
          When collapsed: stack logo on top, toggle just below — keeps the
          rail visually tidy at 56 px wide. */}
      {!collapsed ? (
        <div className="flex items-center justify-between gap-2 border-b px-4 py-4">
          <div className="flex min-w-0 items-center gap-2">
            <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-primary text-primary-foreground">
              <Zap className="h-4 w-4" />
            </div>
            <div className="flex min-w-0 flex-col leading-tight">
              <span className="truncate text-sm font-semibold">DataSUS ETL</span>
              <span className="text-xs text-muted-foreground">
                {settings ? (
                  `v${settings.version}`
                ) : (
                  <Skeleton className="h-3 w-10" />
                )}
              </span>
            </div>
          </div>
          <button
            type="button"
            onClick={toggle}
            aria-label={t("sidebar.collapse")}
            title={t("sidebar.collapse")}
            className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-secondary hover:text-foreground"
          >
            <PanelLeftClose className="h-4 w-4" />
          </button>
        </div>
      ) : (
        <div className="flex flex-col items-center gap-2 border-b px-2 py-3">
          <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary text-primary-foreground">
            <Zap className="h-4 w-4" />
          </div>
          <button
            type="button"
            onClick={toggle}
            aria-label={t("sidebar.expand")}
            title={t("sidebar.expand")}
            className="flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-secondary hover:text-foreground"
          >
            <PanelLeftOpen className="h-4 w-4" />
          </button>
        </div>
      )}

      <nav
        className={cn(
          "flex-1 space-y-1 py-2",
          collapsed ? "px-2" : "px-3",
        )}
      >
        {NAV.map((item) => {
          const Icon = item.icon;
          const active = isActive(item.to);
          const linkClasses = cn(
            "flex items-center rounded-md transition-colors",
            collapsed
              ? "h-9 w-full justify-center"
              : "gap-3 px-3 py-2 text-sm font-medium",
            active
              ? "bg-secondary text-foreground"
              : "text-muted-foreground hover:bg-secondary/50 hover:text-foreground",
          );

          if (collapsed) {
            // Tooltip surfaces the label so the rail stays usable without
            // labels (TooltipProvider lives in Layout.tsx).
            return (
              <Tooltip key={item.to}>
                <TooltipTrigger asChild>
                  <Link
                    to={item.to}
                    aria-label={t(item.labelKey)}
                    className={linkClasses}
                  >
                    <Icon className="h-4 w-4" />
                  </Link>
                </TooltipTrigger>
                <TooltipContent side="right" sideOffset={8}>
                  {t(item.labelKey)}
                </TooltipContent>
              </Tooltip>
            );
          }

          return (
            <Link key={item.to} to={item.to} className={linkClasses}>
              <Icon className="h-4 w-4" />
              {t(item.labelKey)}
            </Link>
          );
        })}
      </nav>
    </aside>
  );
}
