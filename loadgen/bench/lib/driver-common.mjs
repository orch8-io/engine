// Shared helpers for the per-system benchmark drivers. No dependencies.
//
// Driver contract (every systems/<name>/driver.* follows it):
//   env BENCH_N            number of workflows to submit
//   env BENCH_CONCURRENCY  max workflows kept in flight by the driver
//   env BENCH_WARMUP       workflows run (and discarded) before measuring
//   env BENCH_SAMPLES      JSONL output path, one line per measured workflow:
//                          {"id","submitted_at_ms","completed_at_ms",
//                           "server_started_at_ms","server_closed_at_ms","status","error"}
//   env BENCH_DEADLINE_S   give up waiting for completions after this many seconds
// The driver prints ONE final JSON line on stdout: {"completed","failed","errors"}.

import { appendFileSync, writeFileSync } from "node:fs";

export const cfg = {
  n: intEnv("BENCH_N", 100),
  concurrency: intEnv("BENCH_CONCURRENCY", 50),
  warmup: intEnv("BENCH_WARMUP", 0),
  samples: process.env.BENCH_SAMPLES ?? "samples.jsonl",
  deadlineMs: intEnv("BENCH_DEADLINE_S", 600) * 1000,
};

function intEnv(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isFinite(value) || value < 0) throw new Error(`${name} must be a non-negative integer`);
  return value;
}

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const errorSet = new Map();
export function recordError(error) {
  const message = String(error?.message ?? error).slice(0, 300);
  errorSet.set(message, (errorSet.get(message) ?? 0) + 1);
}

/**
 * Run `runOne(index)` for indices [0, count) with at most `concurrency`
 * in flight. `runOne` resolves to a sample object (without timestamps it
 * did not measure). Returns the list of samples.
 */
export async function runPool(count, concurrency, runOne) {
  const samples = [];
  let next = 0;
  const deadline = Date.now() + cfg.deadlineMs;
  async function lane() {
    while (next < count) {
      const index = next++;
      const submitted = Date.now();
      let sample;
      try {
        const remaining = deadline - Date.now();
        if (remaining <= 0) throw new Error("driver deadline exceeded before submission");
        sample = await runOne(index, deadline);
      } catch (error) {
        recordError(error);
        sample = { id: `idx-${index}`, status: "failed", error: String(error?.message ?? error) };
      }
      samples.push({ submitted_at_ms: submitted, ...sample });
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, concurrency) }, lane));
  return samples;
}

/** Retry an async operation across transient connection failures (crash scenario). */
export async function withRetry(fn, { deadline, delayMs = 250, label = "request" } = {}) {
  for (;;) {
    try {
      return await fn();
    } catch (error) {
      if (deadline && Date.now() > deadline) throw new Error(`${label}: ${error?.message ?? error}`);
      await sleep(delayMs);
    }
  }
}

export async function main(runOne, { setup, teardown } = {}) {
  const context = setup ? await setup() : {};
  if (cfg.warmup > 0) {
    await runPool(cfg.warmup, cfg.concurrency, (i) => runOne(`warmup-${i}`, context, Date.now() + cfg.deadlineMs));
  }
  writeFileSync(cfg.samples, "");
  errorSet.clear();
  const samples = await runPool(cfg.n, cfg.concurrency, (i, deadline) => runOne(i, context, deadline));
  for (const sample of samples) appendFileSync(cfg.samples, `${JSON.stringify(sample)}\n`);
  if (teardown) await teardown(context);
  const completed = samples.filter((s) => s.status === "completed").length;
  const errors = [...errorSet.entries()].slice(0, 50).map(([m, c]) => `${m} (x${c})`);
  process.stdout.write(`${JSON.stringify({ completed, failed: samples.length - completed, errors })}\n`);
}
