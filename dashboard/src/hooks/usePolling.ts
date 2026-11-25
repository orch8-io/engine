import { useState, useEffect, useCallback, useRef } from "react";
import { createPollingLoop } from "../lib/polling";

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
  fetcher: (signal?: AbortSignal) => Promise<T>,
  intervalMs: number = 1000,
): PollingResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const [reloading, setReloading] = useState(false);
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);

  const hasData = useRef(false);
  const pollerRef = useRef<ReturnType<typeof createPollingLoop> | null>(null);

  const refresh = useCallback(async () => {
    await pollerRef.current?.refresh();
  }, []);

  useEffect(() => {
    const poller = createPollingLoop(async (signal) => {
      if (hasData.current) setReloading(true);

      try {
        const value = await fetcher(signal);
        if (signal.aborted) return false;
        setData(value);
        setError(null);
        setUpdatedAt(new Date().toISOString());
        hasData.current = true;
        return true;
      } catch (error) {
        if (!signal.aborted) setError(error instanceof Error ? error : new Error(String(error)));
        return false;
      } finally {
        if (!signal.aborted) {
          setLoading(false);
          setReloading(false);
        }
      }
    }, intervalMs, document.hidden);
    pollerRef.current = poller;
    void poller.refresh();

    const onVisibility = () => {
      if (document.hidden) poller.pause();
      else void poller.resume();
    };
    const onFocus = () => {
      if (!document.hidden) void poller.refresh();
    };

    document.addEventListener("visibilitychange", onVisibility);
    window.addEventListener("focus", onFocus);

    return () => {
      poller.dispose();
      pollerRef.current = null;
      document.removeEventListener("visibilitychange", onVisibility);
      window.removeEventListener("focus", onFocus);
    };
  }, [fetcher, intervalMs]);

  return { data, error, loading, reloading, updatedAt, refresh };
}
