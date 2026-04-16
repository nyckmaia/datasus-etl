import * as React from "react";
import { Link, Outlet, useRouterState } from "@tanstack/react-router";
import { Check } from "lucide-react";

import { cn } from "@/lib/utils";

interface StepDef {
  index: number;
  label: string;
  to: "/download/step-1" | "/download/step-2" | "/download/step-3" | "/download/step-4";
}

const STEPS: StepDef[] = [
  { index: 1, label: "Subsystem", to: "/download/step-1" },
  { index: 2, label: "Scope", to: "/download/step-2" },
  { index: 3, label: "Estimate", to: "/download/step-3" },
  { index: 4, label: "Run", to: "/download/step-4" },
];

export interface WizardState {
  subsystem: string | null;
  start_date: string;
  end_date: string;
  ufs: string[];
  runId: string | null;
}

interface WizardContextValue {
  state: WizardState;
  update: (patch: Partial<WizardState>) => void;
  reset: () => void;
}

function currentMonthIso(): string {
  const d = new Date();
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}-01`;
}

function makeDefaultState(): WizardState {
  return {
    subsystem: null,
    start_date: "",
    end_date: currentMonthIso(),
    ufs: [],
    runId: null,
  };
}

const WizardContext = React.createContext<WizardContextValue | null>(null);

export function useWizard(): WizardContextValue {
  const ctx = React.useContext(WizardContext);
  if (!ctx) throw new Error("useWizard must be used inside DownloadWizardPage");
  return ctx;
}

export function DownloadWizardPage() {
  const { location } = useRouterState();
  const [state, setState] = React.useState<WizardState>(makeDefaultState);

  const update = React.useCallback(
    (patch: Partial<WizardState>) =>
      setState((s) => ({ ...s, ...patch })),
    [],
  );
  const reset = React.useCallback(() => setState(makeDefaultState()), []);

  const value = React.useMemo(
    () => ({ state, update, reset }),
    [state, update, reset],
  );

  const currentStep = React.useMemo(() => {
    const path = location.pathname;
    const match = STEPS.find((s) => path.startsWith(s.to));
    return match?.index ?? 1;
  }, [location.pathname]);

  return (
    <WizardContext.Provider value={value}>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Download wizard</h1>
          <p className="text-sm text-muted-foreground">
            Pick a subsystem, choose scope, review the estimate, and watch it run.
          </p>
        </div>

        <nav
          aria-label="Download steps"
          className="flex items-center gap-3 overflow-x-auto"
        >
          {STEPS.map((step, i) => {
            const done = step.index < currentStep;
            const active = step.index === currentStep;
            const clickable =
              step.index === 1 ||
              (step.index === 2 && state.subsystem) ||
              (step.index === 3 && state.subsystem && state.start_date) ||
              (step.index === 4 && state.runId);

            const body = (
              <div
                className={cn(
                  "flex items-center gap-3 rounded-md px-3 py-2 text-sm transition-colors",
                  active && "bg-secondary font-medium",
                  !active && !done && "text-muted-foreground",
                  clickable && !active && "hover:bg-secondary/50",
                )}
              >
                <div
                  className={cn(
                    "flex h-6 w-6 shrink-0 items-center justify-center rounded-full border text-xs font-semibold",
                    done && "border-primary bg-primary text-primary-foreground",
                    active && "border-primary text-primary",
                  )}
                >
                  {done ? <Check className="h-3.5 w-3.5" /> : step.index}
                </div>
                <span>{step.label}</span>
              </div>
            );

            return (
              <React.Fragment key={step.index}>
                {clickable ? (
                  <Link to={step.to}>{body}</Link>
                ) : (
                  <div className="cursor-not-allowed opacity-60">{body}</div>
                )}
                {i < STEPS.length - 1 ? (
                  <div className="h-px flex-1 shrink bg-border" />
                ) : null}
              </React.Fragment>
            );
          })}
        </nav>

        <Outlet />
      </div>
    </WizardContext.Provider>
  );
}
