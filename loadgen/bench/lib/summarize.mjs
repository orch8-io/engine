#!/usr/bin/env node
// Assemble one result file (results.schema.json) from a driver run.
// Usage: node summarize.mjs --key=value ... (see run.sh for the full list).
// Subprocesses are spawned with an argv array (no shell).
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import os from "node:os";
import { basename, dirname, relative } from "node:path";

export const HARNESS_VERSION = "0.1.0";

const args = Object.fromEntries(
  process.argv.slice(2).map((arg) => {
    const match = /^--([^=]+)=(.*)$/s.exec(arg);
    if (!match) throw new Error(`bad argument ${arg}`);
    return [match[1], match[2]];
  }),
);
const need = (key) => {
  if (args[key] === undefined || args[key] === "") throw new Error(`--${key} is required`);
  return args[key];
};

const readJsonl = (path) =>
  path && existsSync(path)
    ? readFileSync(path, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line))
    : [];

function percentile(sorted, p) {
  if (sorted.length === 0) return null;
  const rank = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return Math.round(sorted[rank] * 100) / 100;
}

function capture(cmd, argv) {
  const out = spawnSync(cmd, argv, { encoding: "utf8" });
  return out.status === 0 ? out.stdout.trim() : "";
}

function dockerInfo() {
  const raw = capture("docker", ["info", "--format", "{{.NCPU}} {{.MemTotal}}"]);
  const [cpus, mem] = raw.split(" ").map(Number);
  return {
    ...(cpus > 0 ? { docker_cpus: cpus } : {}),
    ...(mem > 0 ? { docker_memory_bytes: mem } : {}),
  };
}

const samples = readJsonl(need("samples"));
const driver = JSON.parse(need("driver-summary"));
const completedSamples = samples.filter((s) => s.status === "completed");

const serverLatencies = completedSamples
  .filter((s) => Number.isFinite(s.server_started_at_ms) && Number.isFinite(s.server_closed_at_ms))
  .map((s) => s.server_closed_at_ms - s.server_started_at_ms);
const useServer = completedSamples.length > 0 && serverLatencies.length === completedSamples.length;
const latencies = (useServer
  ? serverLatencies
  : completedSamples.map((s) => s.completed_at_ms - s.submitted_at_ms)
).sort((a, b) => a - b);

const firstSubmit = samples.length ? Math.min(...samples.map((s) => s.submitted_at_ms)) : 0;
const lastDone = completedSamples.length
  ? Math.max(...completedSamples.map((s) => s.completed_at_ms ?? s.submitted_at_ms))
  : 0;
const spanS = (lastDone - firstSubmit) / 1000;

let crash = null;
if (need("scenario") === "crash_recovery") {
  const killAt = Date.parse(need("kill-at"));
  const restartAt = Date.parse(need("restart-at"));
  const recovered = completedSamples.filter(
    (s) => s.submitted_at_ms < killAt && (s.completed_at_ms ?? 0) > restartAt,
  ).length;
  const afterRestart = completedSamples
    .map((s) => s.completed_at_ms)
    .filter((t) => t > restartAt)
    .sort((a, b) => a - b);
  const activity = readJsonl(args["activity-log"]);
  const distinct = new Set(activity.map((a) => `${a.wf}\u0000${a.step}`));
  crash = {
    killed_service: need("killed-service"),
    kill_at: new Date(killAt).toISOString(),
    restart_at: new Date(restartAt).toISOString(),
    in_flight_at_kill: samples.filter(
      (s) => s.submitted_at_ms < killAt && (s.completed_at_ms === undefined || s.completed_at_ms > killAt),
    ).length,
    recovered_count: recovered,
    lost_count: samples.length - completedSamples.length,
    duplicate_activity_executions: activity.length - distinct.size,
    time_to_recover_ms: afterRestart.length ? afterRestart[0] - restartAt : null,
  };
}

const outPath = need("out");
const result = {
  system: need("system"),
  system_version: {
    images: args.images && existsSync(args.images) ? JSON.parse(readFileSync(args.images, "utf8")) : {},
    ...(args.sdk ? { sdk: JSON.parse(args.sdk) } : {}),
  },
  harness_version: HARNESS_VERSION,
  git_sha: args["git-sha"] || "unknown",
  scenario: need("scenario"),
  run_index: Number(args["run-index"] ?? 1),
  workload: {
    workflows: Number(need("n")),
    steps: 3,
    activity_ms: 50,
    concurrency: Number(need("concurrency")),
    warmup_workflows: Number(args.warmup ?? 0),
  },
  hardware: {
    cpu: os.cpus()[0]?.model ?? "unknown",
    cores: os.cpus().length,
    memory_bytes: os.totalmem(),
    os: `${os.type()} ${os.release()} ${os.arch()}`,
    docker_version: capture("docker", ["version", "--format", "{{.Server.Version}}"]) || "unknown",
    ...dockerInfo(),
  },
  timings: {
    started_at: need("started"),
    ended_at: need("ended"),
    total_duration_ms: Date.parse(need("ended")) - Date.parse(need("started")),
  },
  results: {
    completed: completedSamples.length,
    failed: samples.length - completedSamples.length,
    throughput_wf_per_s:
      completedSamples.length && spanS > 0 ? Math.round((completedSamples.length / spanS) * 100) / 100 : null,
    latency_ms: {
      p50: percentile(latencies, 50),
      p95: percentile(latencies, 95),
      p99: percentile(latencies, 99),
      max: latencies.length ? latencies[latencies.length - 1] : null,
    },
    latency_source: useServer ? "server" : "driver",
    errors: driver.errors ?? [],
  },
  crash_recovery: crash,
  raw_samples_file: relative(dirname(outPath), need("samples")) || basename(need("samples")),
  activity_log_file: args["activity-log"] ? relative(dirname(outPath), args["activity-log"]) : null,
  ...(args.notes ? { notes: args.notes } : {}),
};

writeFileSync(outPath, `${JSON.stringify(result, null, 2)}\n`);
console.log(`wrote ${outPath}`);
