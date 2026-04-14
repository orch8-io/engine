import { useState, useEffect, useCallback, useRef } from "react";

interface PollingResult<T> {
  data: T | null;
  error: Error | null;
  loading: boolean;
  reloading: boolean;
  updatedAt: string | null;
  refresh: () => Promise<void>;
}

/**
 * Polls `fetcher` every `intervalMs`:
 *  - fast default interval (1 s)
 *  - pauses while the tab is hidden (saves API load)
 *  - immediate refresh on focus / visibility-change
 *  - aborts in-flight requests on unmount
 *  - exposes `reloading` so UI can show subtle spinners on background updates
 *  - `refresh()` is awaitable
 *  - errors do NOT stop polling; back-off capped at 5× interval
 */
export function usePolling<T>(
  fetcher: () => Promise<T>,
  intervalMs: number = 1000,
): PollingResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const [reloading, setReloading] = useState(false);
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);

  const abortRef = useRef<AbortController | null>(null);
  const inFlight = useRef(false);
  const errorStreak = useRef(0);

  const refresh = useCallback(async () => {
    if (inFlight.current) return;
    inFlight.current = true;

    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    const isBackground = data !== null;
    if (isBackground) setReloading(true);

    try {
      const d = await fetcher();
      if (ac.signal.aborted) return;
      setData(d);
      setError(null);
      setUpdatedAt(new Date().toISOString());
      errorStreak.current = 0;
    } catch (e) {
      if (ac.signal.aborted) return;
      setError(e instanceof Error ? e : new Error(String(e)));
      errorStreak.current += 1;
    } finally {
      inFlight.current = false;
      setLoading(false);
      setReloading(false);
    }
  }, [fetcher, data]);

  useEffect(() => {
    refresh();

    let id: ReturnType<typeof setInterval> | null = null;
    const start = () => {
      if (id !== null) return;
      // Back-off: cap at 5× interval on repeated errors
      const delay = intervalMs * Math.min(1 + errorStreak.current, 5);
      id = setInterval(refresh, delay);
    };
    const stop = () => {
      if (id !== null) {
        clearInterval(id);
        id = null;
      }
    };

    const onVisibility = () => {
      if (document.hidden) {
        stop();
      } else {
        refresh();
        start();
      }
    };
    const onFocus = () => refresh();

    if (!document.hidden) start();
    document.addEventListener("visibilitychange", onVisibility);
    window.addEventListener("focus", onFocus);

    return () => {
      stop();
      abortRef.current?.abort();
      document.removeEventListener("visibilitychange", onVisibility);
      window.removeEventListener("focus", onFocus);
    };
  }, [refresh, intervalMs]);

  return { data, error, loading, reloading, updatedAt, refresh };
}
