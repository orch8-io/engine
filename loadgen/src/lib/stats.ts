/**
 * In-process counters + periodic print.
 *
 * Tracks:
 *   - Global spawn/complete/fail/cancel counts
 *   - Per-template success rates
 *   - Spawn latency (API response time) via reservoir sampling
 *   - End-to-end latency (spawn → terminal state) via reservoir sampling
 *   - Throughput: actual spawn rate vs configured target
 *   - Worker task metrics
 */

import { isShuttingDown } from "./shutdown.ts";

export class Counters {
  readonly startedAt = Date.now();

  spawned = 0;
  spawnFailed = 0;
  completed = 0;
  failed = 0;
  cancelled = 0;
  signalsSent = 0;
  workerPolls = 0;
  tasksClaimed = 0;
  tasksCompleted = 0;
  tasksFailed = 0;
  errors5xx = 0;
  errorsConn = 0;

  // Per-template tracking: template name → { spawned, completed, failed }
  private templateStats = new Map<string, TemplateMetrics>();

  // Rolling reservoir of spawn latencies (ms).
  private spawnLatencies: number[] = [];
  private readonly spawnReservoirCap = 500;

  // Rolling reservoir of end-to-end latencies (spawn → terminal).
  private e2eLatencies: number[] = [];
  private readonly e2eReservoirCap = 500;
  private e2eSampled = 0;

  // Throughput tracking: windowed spawn counts.
  private throughputWindow: number[] = []; // timestamps of recent spawns
  private readonly throughputWindowMs = 10_000;

  // ─── Template tracking ────────────────────────────────────────────────

  templateSpawned(name: string): void {
    const m = this.getTemplate(name);
    m.spawned++;
  }

  templateCompleted(name: string): void {
    const m = this.getTemplate(name);
    m.completed++;
  }

  templateFailed(name: string): void {
    const m = this.getTemplate(name);
    m.failed++;
  }

  private getTemplate(name: string): TemplateMetrics {
    let m = this.templateStats.get(name);
    if (!m) {
      m = { spawned: 0, completed: 0, failed: 0 };
      this.templateStats.set(name, m);
    }
    return m;
  }

  // ─── Spawn latency ────────────────────────────────────────────────────

  recordLatency(ms: number): void {
    if (this.spawnLatencies.length < this.spawnReservoirCap) {
      this.spawnLatencies.push(ms);
    } else {
      const idx = Math.floor(Math.random() * (this.spawned + 1));
      if (idx < this.spawnReservoirCap) this.spawnLatencies[idx] = ms;
    }
    // Track throughput.
    this.throughputWindow.push(Date.now());
  }

  // ─── End-to-end latency ───────────────────────────────────────────────

  recordE2eLatency(ms: number): void {
    this.e2eSampled++;
    if (this.e2eLatencies.length < this.e2eReservoirCap) {
      this.e2eLatencies.push(ms);
    } else {
      const idx = Math.floor(Math.random() * (this.e2eSampled + 1));
      if (idx < this.e2eReservoirCap) this.e2eLatencies[idx] = ms;
    }
  }

  // ─── Throughput calculation ───────────────────────────────────────────

  private computeThroughput(): number {
    const now = Date.now();
    const cutoff = now - this.throughputWindowMs;
    // Trim old entries.
    while (this.throughputWindow.length > 0 && this.throughputWindow[0]! < cutoff) {
      this.throughputWindow.shift();
    }
    // Rate = count / window_seconds.
    return this.throughputWindow.length / (this.throughputWindowMs / 1000);
  }

  // ─── Snapshot ─────────────────────────────────────────────────────────

  snapshot(): string {
    const uptimeSec = Math.floor((Date.now() - this.startedAt) / 1000);
    const hh = String(Math.floor(uptimeSec / 3600)).padStart(2, "0");
    const mm = String(Math.floor((uptimeSec % 3600) / 60)).padStart(2, "0");
    const ss = String(uptimeSec % 60).padStart(2, "0");

    const spawnSorted = [...this.spawnLatencies].sort((a, b) => a - b);
    const sp50 = pct(spawnSorted, 0.5);
    const sp95 = pct(spawnSorted, 0.95);

    const e2eSorted = [...this.e2eLatencies].sort((a, b) => a - b);
    const e50 = pct(e2eSorted, 0.5);
    const e95 = pct(e2eSorted, 0.95);

    const rate = this.computeThroughput().toFixed(1);

    return (
      `[loadgen] up=${hh}:${mm}:${ss} rate=${rate}/s ` +
      `spawned=${this.spawned} done=${this.completed} fail=${this.failed} cancel=${this.cancelled} ` +
      `spawn_p50=${sp50}ms spawn_p95=${sp95}ms ` +
      `e2e_p50=${e50}ms e2e_p95=${e95}ms ` +
      `tasks=${this.tasksClaimed}/${this.tasksCompleted}/${this.tasksFailed} ` +
      `errs=${this.errors5xx}+${this.errorsConn}`
    );
  }

  /** Detailed per-template breakdown (printed less frequently). */
  templateSnapshot(): string {
    if (this.templateStats.size === 0) return "";
    const lines: string[] = ["[loadgen:templates]"];
    for (const [name, m] of [...this.templateStats.entries()].sort((a, b) => b[1].spawned - a[1].spawned)) {
      const total = m.completed + m.failed;
      const successRate = total > 0 ? ((m.completed / total) * 100).toFixed(0) : "-";
      const pending = m.spawned - m.completed - m.failed;
      lines.push(
        `  ${name}: spawned=${m.spawned} ok=${m.completed} fail=${m.failed} pending=${pending} success=${successRate}%`,
      );
    }
    return lines.join("\n");
  }
}

interface TemplateMetrics {
  spawned: number;
  completed: number;
  failed: number;
}

function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor(p * sorted.length));
  return Math.round(sorted[idx]!);
}

export function startPrinter(counters: Counters, intervalMs: number = 5000): void {
  let tick = 0;
  const run = (): void => {
    if (isShuttingDown()) return;
    process.stderr.write(counters.snapshot() + "\n");
    tick++;
    // Print template breakdown every 6th tick (every 30s at default interval).
    if (tick % 6 === 0) {
      const tmpl = counters.templateSnapshot();
      if (tmpl) process.stderr.write(tmpl + "\n");
    }
    setTimeout(run, intervalMs).unref();
  };
  setTimeout(run, intervalMs).unref();
}
