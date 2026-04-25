import { useState } from "react";
import { ArrowUpRight, X } from "lucide-react";

import { useVersionCheck } from "@/hooks/useVersionCheck";

export function UpdateBanner() {
  const { data } = useVersionCheck();
  const [dismissed, setDismissed] = useState(false);

  if (!data?.update_available || dismissed) return null;

  return (
    <div
      role="status"
      aria-live="polite"
      className="flex items-center justify-between gap-4 border-b border-amber-200/70 bg-amber-50 px-6 py-2 text-sm text-amber-900 dark:border-amber-900/50 dark:bg-amber-950/40 dark:text-amber-100"
    >
      <p className="min-w-0 truncate">
        A new version{" "}
        <span className="font-mono font-semibold">v{data.latest}</span> is
        available
        {data.current ? (
          <>
            {" "}
            <span className="text-amber-700 dark:text-amber-300/70">
              (you have v{data.current})
            </span>
          </>
        ) : null}
        .
      </p>
      <div className="flex shrink-0 items-center gap-3">
        {data.release_url ? (
          <a
            href={data.release_url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 font-medium underline-offset-2 hover:underline"
          >
            Download
            <ArrowUpRight className="h-3.5 w-3.5" />
          </a>
        ) : null}
        <button
          type="button"
          onClick={() => setDismissed(true)}
          aria-label="Dismiss update notification"
          className="rounded p-0.5 text-amber-800 transition-colors hover:bg-amber-100 dark:text-amber-200 dark:hover:bg-amber-900/50"
        >
          <X className="h-4 w-4" />
        </button>
      </div>
    </div>
  );
}
