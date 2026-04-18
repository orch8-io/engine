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
import { readdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { startServer, stopServer } from "./harness.ts";
import type { ServerHandle } from "./harness.ts";

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
const SELF_MANAGED_SUITES = new Set<string>([
  "persistence_recovery.test.ts",
  "triggers.test.ts",
  "wait-signal.test.ts",
  "worker-dashboard.test.ts",
  "workers.test.ts",
]);

function findTestFiles(): string[] {
  return readdirSync(__dirname)
    .filter((f) => f.endsWith(".test.ts") && !SELF_MANAGED_SUITES.has(f))
    .sort()
    .map((f) => resolve(__dirname, f));
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

try {
  console.log("runner: starting shared orch8-server...");
  handle = await startServer({ port: PORT });

  const testFiles = findTestFiles();
  if (testFiles.length === 0) {
    console.error("runner: no *.test.ts files found");
    await shutdown(1);
  }

  console.log(`runner: executing ${testFiles.length} test file(s) serially`);
  const reporterArgs = process.env.ORCH8_E2E_REPORTER
    ? [`--test-reporter=${process.env.ORCH8_E2E_REPORTER}`]
    : [];
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

  await shutdown(code);
} catch (err) {
  const msg = err instanceof Error ? err.message : String(err);
  console.error(`runner: ${msg}`);
  await shutdown(1);
}
