import { useState, useEffect, useCallback } from "react";

/**
 * Polls `fetcher` every `intervalMs`. Tracks the ISO timestamp of the last
 * successful fetch so the page header can surface freshness ("Updated 3s ago")
 * without each page having to thread its own clock.
 */
export function usePolling<T>(
  fetcher: () => Promise<T>,
  intervalMs: number = 5000,
): {
  data: T | null;
  error: Error | null;
  loading: boolean;
  updatedAt: string | null;
  refresh: () => void;
} {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);

  const refresh = useCallback(() => {
    fetcher()
      .then((d) => {
        setData(d);
        setError(null);
        setUpdatedAt(new Date().toISOString());
      })
      .catch((e) => setError(e))
      .finally(() => setLoading(false));
  }, [fetcher]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, intervalMs);
    return () => clearInterval(id);
  }, [refresh, intervalMs]);

  return { data, error, loading, updatedAt, refresh };
}
