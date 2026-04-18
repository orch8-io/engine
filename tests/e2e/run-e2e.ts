#!/usr/bin/env node
/**
 * E2E runner: starts orch8-server ONCE, runs every `*.test.ts` serially
 * against the shared server, then tears down.
 *
 * Why: the previous `npm test` path had each suite's `before()` spawn its
 * own binary. With 20+ suites that's 20 × (port kill + DB cleanup +
 * liveness wait) of overhead. Sharing one server drops that to one-time
 * cost.
 *
 * Does NOT build — the binary at `target/debug/orch8-server` must already
 * exist. CI and dev workflows build separately so this runner stays fast
 * and predictable.
 *
 * Contract with test files:
 *   - `startServer()` in `harness.ts` switches to attach mode when it
 *     sees `ORCH8_E2E_ATTACH=1` — skips spawn/cleanup, just waits for
 *     `/health/live` on `ORCH8_E2E_PORT`.
 *   - `stopServer()` is a no-op for attach-mode handles (lifecycle stays
 *     here in the runner).
 *   - Tests run with `--test-concurrency=1`. Suites share one DB, but
 *     intra-suite rows use unique UUIDs via `testSequence()` so there's
 *     no cross-talk except for global listings (crons, DLQ, workers) —
 *     serial runs + SELF_MANAGED_SUITES carve those out.
 *
 * Standalone debugging still works: a single suite can run via
 * `node --experimental-strip-types --test ./foo.test.ts` without this
 * runner — `ORCH8_E2E_ATTACH` is unset so the harness falls back to
 * spawn mode.
 */

import { spawn } from "node:child_process";
import { readdirSync, statSync } from "node:fs";
import { basename, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { startServer, stopServer } from "./harness.ts";
import type { ServerHandle } from "./harness.ts";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.ORCH8_E2E_PORT) || 18080;

// TTY-aware ANSI. Disabled automatically in CI/pipes so log files stay clean.
const USE_COLOR = process.stdout.isTTY && !process.env.NO_COLOR;
const c = {
  dim: (s: string) => (USE_COLOR ? `\x1b[2m${s}\x1b[0m` : s),
  bold: (s: string) => (USE_COLOR ? `\x1b[1m${s}\x1b[0m` : s),
  green: (s: string) => (USE_COLOR ? `\x1b[32m${s}\x1b[0m` : s),
  red: (s: string) => (USE_COLOR ? `\x1b[31m${s}\x1b[0m` : s),
  cyan: (s: string) => (USE_COLOR ? `\x1b[36m${s}\x1b[0m` : s),
};

function fmtDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = Math.floor(s / 60);
  const rem = Math.round(s - m * 60);
  return `${m}m${rem}s`;
}

// Suites that need their own server.
//
// Two reasons a suite lands here:
//
// 1. Server-lifecycle tests (persistence_recovery): the suite itself does
//    stop/start mid-test, which is incompatible with attach-mode.
//
// 2. Suites exercising **globally-scoped** state — worker task queue,
//    trigger definitions, signal inbox, worker dashboard. These tables
//    aren't keyed by tenant_id, so accumulated rows from earlier suites
//    in a shared server leak in and break assertions like
//    `assert.equal(tasks.length, 1)`. A tenant prefix doesn't help; only
//    a fresh server does.
const SELF_MANAGED_SUITES = new Set<string>([
  "persistence_recovery.test.ts",
  "triggers.test.ts",
  "wait-signal.test.ts",
  "worker-dashboard.test.ts",
  "workers.test.ts",
  // Worker task queue — not keyed by tenant_id (see rule #2).
  "worker_task_timeout.test.ts",
  "worker_heartbeat_timeout.test.ts",
  "retryable_false_open_circuit.test.ts",
  "complex_patterns.test.ts",
  // Signal inbox — not keyed by tenant_id.
  "signal_ordering.test.ts",
  "signal_during_finally.test.ts",
  // Trigger definitions — globally scoped, leak across suites.
  "emit_event_deep_chains.test.ts",
  "emit_event_invalid_target.test.ts",
  // Server-lifecycle tests (rule #1): require mid-test restart or env swap.
  "encryption_key_rotation.test.ts",
  "ab_split_determinism_restart.test.ts",
  // Needs its own server started with ORCH8_ENCRYPTION_KEY set — the shared
  // attach-mode server was launched without a key.
  "encryption_at_rest.test.ts",
]);

// Directories whose tests are organizational scaffolding, not runnable yet.
// Skip them so empty scaffolds don't fail the runner. Remove once they have
// real assertions.
const SKIP_DIRS = new Set<string>(["node_modules", ".git"]);

/**
 * Recursively collect every *.test.ts under the e2e directory.
 *
 * Groups live in subfolders (`features/`, `blocks/`, `resilience/`, ...).
 * SELF_MANAGED_SUITES filtering still matches by basename so a test moved
 * into a subfolder keeps its "needs own server" classification without
 * needing a full-path rewrite in the set.
 */
function findTestFiles(): string[] {
  const out: string[] = [];
  const walk = (dir: string): void => {
    for (const entry of readdirSync(dir)) {
      if (SKIP_DIRS.has(entry)) continue;
      const full = resolve(dir, entry);
      const s = statSync(full);
      if (s.isDirectory()) {
        walk(full);
      } else if (
        entry.endsWith(".test.ts") &&
        !SELF_MANAGED_SUITES.has(basename(entry))
      ) {
        out.push(full);
      }
    }
  };
  walk(__dirname);
  return out.sort();
}

let handle: ServerHandle | undefined;
let exitCode = 1;

const shutdown = async (code?: number | null): Promise<void> => {
  exitCode = code ?? exitCode;
  try {
    await stopServer(handle);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error(`runner: stopServer failed: ${msg}`);
  }
  process.exit(exitCode);
};

// If the user Ctrl-Cs mid-run, make sure we still kill the child.
process.on("SIGINT", () => {
  void shutdown(130);
});
process.on("SIGTERM", () => {
  void shutdown(143);
});

const runStart = Date.now();

try {
  // Pick a human-friendly default reporter. Default Node TAP output is dense
  // and hard to skim; `spec` groups by describe/it and colors pass/fail.
  // `dot` is available for ultra-compact CI logs via ORCH8_E2E_REPORTER=dot.
  const reporter = process.env.ORCH8_E2E_REPORTER || "spec";

  console.log("");
  console.log(c.bold(c.cyan("━━━ orch8 e2e ━━━")));
  console.log(c.dim(`port=${PORT}  reporter=${reporter}  concurrency=1`));
  console.log("");

  console.log(c.dim("› starting shared orch8-server..."));
  const serverStart = Date.now();
  handle = await startServer({ port: PORT });
  console.log(c.dim(`  ready in ${fmtDuration(Date.now() - serverStart)}`));

  const testFiles = findTestFiles();
  if (testFiles.length === 0) {
    console.error(c.red("runner: no *.test.ts files found"));
    await shutdown(1);
  }

  console.log("");
  console.log(c.bold(`› running ${testFiles.length} test file(s)`));
  console.log("");
  const reporterArgs = [`--test-reporter=${reporter}`];
  const child = spawn(
    process.execPath,
    [
      // Child inherits tsx loader so `.ts` test files load directly.
      // `--import tsx/esm` is the modern registration API (Node 20+).
      "--import", "tsx/esm",
      "--test",
      "--test-concurrency=1",
      "--test-timeout=60000",
      ...reporterArgs,
      ...testFiles,
    ],
    {
      stdio: "inherit",
      env: {
        ...process.env,
        ORCH8_E2E_ATTACH: "1",
        ORCH8_E2E_PORT: String(PORT),
      },
    },
  );

  const code: number = await new Promise((resolvePromise) => {
    child.on("exit", (c: number | null, sig: NodeJS.Signals | null) => {
      resolvePromise(c ?? (sig ? 1 : 0));
    });
    child.on("error", (err: Error) => {
      console.error(`runner: child spawn error: ${err.message}`);
      resolvePromise(1);
    });
  });

  const total = fmtDuration(Date.now() - runStart);
  console.log("");
  if (code === 0) {
    console.log(c.bold(c.green(`✓ all test files passed`)) + c.dim(`  (${total})`));
  } else {
    console.log(
      c.bold(c.red(`✗ test run failed`)) +
        c.dim(`  (exit=${code}, ${total})`),
    );
  }
  console.log("");

  await shutdown(code);
} catch (err) {
  const msg = err instanceof Error ? err.message : String(err);
  console.error(c.red(`runner: ${msg}`));
  await shutdown(1);
}
