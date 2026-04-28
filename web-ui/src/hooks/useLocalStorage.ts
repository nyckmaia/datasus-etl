import * as React from "react";

/**
 * Persistent state hook backed by `window.localStorage`. Values are
 * JSON-serialised so anything serialisable works (booleans, strings,
 * numbers, plain objects).
 *
 * Reads are lazy (only on first render) so the hook is cheap to call from
 * many components, and writes are best-effort — a quota error or denied
 * storage falls back to in-memory state without crashing the page.
 */
export function useLocalStorage<T>(
  key: string,
  initialValue: T,
): [T, (value: T | ((prev: T) => T)) => void] {
  const [value, setValue] = React.useState<T>(() => {
    if (typeof window === "undefined") return initialValue;
    try {
      const raw = window.localStorage.getItem(key);
      if (raw === null) return initialValue;
      return JSON.parse(raw) as T;
    } catch {
      return initialValue;
    }
  });

  const setStoredValue = React.useCallback(
    (next: T | ((prev: T) => T)) => {
      setValue((prev) => {
        const resolved =
          typeof next === "function" ? (next as (p: T) => T)(prev) : next;
        try {
          window.localStorage.setItem(key, JSON.stringify(resolved));
        } catch {
          // Storage full / disabled — keep the in-memory value, don't crash.
        }
        return resolved;
      });
    },
    [key],
  );

  return [value, setStoredValue];
}
