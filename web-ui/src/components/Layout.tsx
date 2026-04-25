import { Outlet } from "@tanstack/react-router";

import { Sidebar } from "./Sidebar";
import { TopBar } from "./TopBar";
import { UpdateBanner } from "./UpdateBanner";
import { TooltipProvider } from "@/components/ui/tooltip";

export function Layout() {
  return (
    <TooltipProvider delayDuration={200}>
      <div className="flex h-screen overflow-hidden bg-background text-foreground">
        <Sidebar />
        <div className="flex min-w-0 flex-1 flex-col">
          <UpdateBanner />
          <TopBar />
          <main className="flex-1 overflow-auto">
            <div className="mx-auto w-full max-w-7xl px-6 py-6">
              <Outlet />
            </div>
          </main>
        </div>
      </div>
    </TooltipProvider>
  );
}
