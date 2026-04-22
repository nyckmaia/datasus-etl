import {
  createRootRoute,
  createRoute,
  createRouter,
  Navigate,
} from "@tanstack/react-router";

import { Layout } from "@/components/Layout";
import { DashboardPage } from "@/pages/Dashboard";
import { DownloadWizardPage } from "@/pages/DownloadWizard";
import { Step1SubsystemPage } from "@/pages/download/Step1Subsystem";
import { Step2ScopePage } from "@/pages/download/Step2Scope";
import { Step3EstimatePage } from "@/pages/download/Step3Estimate";
import { Step4RunPage } from "@/pages/download/Step4Run";
import { QueryPage } from "@/pages/Query";
import { SettingsPage } from "@/pages/Settings";

const rootRoute = createRootRoute({
  component: Layout,
});

const dashboardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: DashboardPage,
});

const downloadRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "download",
  component: DownloadWizardPage,
});

const downloadIndexRoute = createRoute({
  getParentRoute: () => downloadRoute,
  path: "/",
  component: () => <Navigate to="/download/step-1" />,
});

const step1Route = createRoute({
  getParentRoute: () => downloadRoute,
  path: "step-1",
  component: Step1SubsystemPage,
});

interface Step2Search {
  subsystem?: string;
}

const step2Route = createRoute({
  getParentRoute: () => downloadRoute,
  path: "step-2",
  component: Step2ScopePage,
  validateSearch: (search: Record<string, unknown>): Step2Search => {
    const s = search.subsystem;
    if (typeof s === "string" && s.length > 0) return { subsystem: s };
    return {};
  },
});

const step3Route = createRoute({
  getParentRoute: () => downloadRoute,
  path: "step-3",
  component: Step3EstimatePage,
});

const step4Route = createRoute({
  getParentRoute: () => downloadRoute,
  path: "step-4",
  component: Step4RunPage,
});

const queryRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "query",
  component: QueryPage,
});

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "settings",
  component: SettingsPage,
});

const routeTree = rootRoute.addChildren([
  dashboardRoute,
  downloadRoute.addChildren([
    downloadIndexRoute,
    step1Route,
    step2Route,
    step3Route,
    step4Route,
  ]),
  queryRoute,
  settingsRoute,
]);

export const router = createRouter({
  routeTree,
  defaultPreload: "intent",
});
