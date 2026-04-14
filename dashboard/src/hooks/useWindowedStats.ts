import { useEffect, useRef, useState, useMemo } from "react";
import type { WorkerTaskStats } from "../api";

type Sample = {
  t: number;
  completed: number;
  failed: number;
  pending: number;
  claimed: number;
};

/**
 * Derives time-series from the 1 Hz `WorkerTaskStats` poll. Keeps the last
 * `size` samples and exposes deltas, gauges, and rolling aggregates.
 *
 * Counters are cumulative on the server; we convert to per-second deltas
 * client-side. Samples are deduplicated by full counter fingerprint.
 */
export function useWindowedStats(stats: WorkerTaskStats | null, size = 60) {
  const buf = useRef<Sample[]>([]);
  const [, setTick] = useState(0);

  useEffect(() => {
    if (!stats) return;
    const sample: Sample = {
      t: performance.now(),
      completed: stats.by_state["completed"] ?? 0,
      failed: stats.by_state["failed"] ?? 0,
      pending: stats.by_state["pending"] ?? 0,
      claimed: stats.by_state["claimed"] ?? 0,
    };
    const last = buf.current[buf.current.length - 1];
    // Dedup by full counter fingerprint (includes pending + claimed)
    if (
      last &&
      last.completed === sample.completed &&
      last.failed === sample.failed &&
      last.pending === sample.pending &&
      last.claimed === sample.claimed
    ) {
      return;
    }
    buf.current = [...buf.current, sample].slice(-size);
    setTick((n) => n + 1);
  }, [stats, size]);

  const samples = buf.current;

  const {
    throughput,
    failedDelta,
    pendingSeries,
    claimedSeries,
    successRate,
    throughputNow,
    windowCompleted,
    windowFailed,
    windowSeconds,
  } = useMemo(() => {
    const throughput: number[] = [];
    const failedDelta: number[] = [];
    for (let i = 1; i < samples.length; i++) {
      const a = samples[i - 1];
      const b = samples[i];
      const dt = Math.max((b.t - a.t) / 1000, 0.001);
      throughput.push(Math.max(0, (b.completed - a.completed) / dt));
      failedDelta.push(Math.max(0, b.failed - a.failed));
    }

    const pendingSeries = samples.map((s) => s.pending);
    const claimedSeries = samples.map((s) => s.claimed);

    const first = samples[0];
    const latest = samples[samples.length - 1];
    const windowCompleted =
      latest && first ? latest.completed - first.completed : 0;
    const windowFailed =
      latest && first ? latest.failed - first.failed : 0;
    const windowTotal = windowCompleted + windowFailed;
    const successRate =
      windowTotal > 0 ? (windowCompleted / windowTotal) * 100 : 0;

    let throughputNow = 0;
    for (let i = throughput.length - 1; i >= 0; i--) {
      if (throughput[i] > 0) {
        throughputNow = throughput[i];
        break;
      }
    }

    const windowSeconds =
      first && latest ? Math.max(1, Math.round((latest.t - first.t) / 1000)) : 0;

    return {
      throughput,
      failedDelta,
      pendingSeries,
      claimedSeries,
      successRate,
      throughputNow,
      windowCompleted,
      windowFailed,
      windowSeconds,
    };
  }, [samples]);

  return {
    throughput,
    failedDelta,
    pendingSeries,
    claimedSeries,
    successRate,
    throughputNow,
    windowCompleted,
    windowFailed,
    windowSeconds,
  };
}
