#!/usr/bin/env node
/**
 * E2E runner: starts orch8-server ONCE, runs every `*.test.js` serially
 * against the shared server, then tears down.
 *
 * Why: the previous `npm test` path had each suite's `before()` spawn its
 * own binary. With 9 suites that's 9 × (port kill + DB cleanup + liveness
 * wait) of overhead. Sharing one server drops that to one-time cost.
 *
 * Does NOT build — the binary at `target/debug/orch8-server` must already
 * exist. CI and dev workflows build separately so this runner stays fast
 * and predictable.
 *
 * Contract with test files:
 *   - `startServer()` in `harness.js` switches to attach mode when it sees
 *     `ORCH8_E2E_ATTACH=1` — skips spawn/cleanup, just waits for
 *     `/health/live` on `ORCH8_E2E_PORT`.
 *   - `stopServer()` is a no-op for attach-mode handles (lifecycle stays
 *     here in the runner).
 *   - Tests run with `--test-concurrency=1`. Suites share one DB, but
 *     intra-suite rows use unique UUIDs via `testSequence()` so there's
 *     no cross-talk except for global listings (crons, DLQ, workers) —
 *     serial runs sidestep that.
 *
 * Standalone debugging still works: a single suite can run via
 * `node --test ./foo.test.js` without this runner — `ORCH8_E2E_ATTACH`
 * is unset so the harness falls back to spawn mode.
 */

import { spawn } from "node:child_process";
import { readdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { startServer, stopServer } from "./harness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.ORCH8_E2E_PORT) || 18080;

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
//
// All other suites use tenant-scoped UUIDs via `testSequence()` and are
// safe to share a server.
const SELF_MANAGED_SUITES = new Set([
  "persistence_recovery.test.js",
  "triggers.test.js",
  "wait-signal.test.js",
  "worker-dashboard.test.js",
  "workers.test.js",
]);

function findTestFiles() {
  return readdirSync(__dirname)
    .filter((f) => f.endsWith(".test.js") && !SELF_MANAGED_SUITES.has(f))
    .sort()
    .map((f) => resolve(__dirname, f));
}

let handle;
let exitCode = 1;

const shutdown = async (code) => {
  exitCode = code ?? exitCode;
  try {
    await stopServer(handle);
  } catch (e) {
    console.error(`runner: stopServer failed: ${e.message}`);
  }
  process.exit(exitCode);
};

// If the user Ctrl-Cs mid-run, make sure we still kill the child.
process.on("SIGINT", () => shutdown(130));
process.on("SIGTERM", () => shutdown(143));

try {
  console.log("runner: starting shared orch8-server...");
  handle = await startServer({ port: PORT });

  const testFiles = findTestFiles();
  if (testFiles.length === 0) {
    console.error("runner: no *.test.js files found");
    await shutdown(1);
  }

  console.log(`runner: executing ${testFiles.length} test file(s) serially`);
  const reporterArgs = process.env.ORCH8_E2E_REPORTER
    ? [`--test-reporter=${process.env.ORCH8_E2E_REPORTER}`]
    : [];
  const child = spawn(
    process.execPath,
    [
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

  const code = await new Promise((resolvePromise) => {
    child.on("exit", (c, sig) => resolvePromise(c ?? (sig ? 1 : 0)));
    child.on("error", (err) => {
      console.error(`runner: child spawn error: ${err.message}`);
      resolvePromise(1);
    });
  });

  await shutdown(code);
} catch (err) {
  console.error(`runner: ${err.message}`);
  await shutdown(1);
}
