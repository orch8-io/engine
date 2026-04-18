#!/usr/bin/env node
/**
 * Parallel runner for self-managed suites.
 *
 * Why: the self-managed suites (persistence_recovery + the 4 that exercise
 * globally-scoped state — triggers, wait-signal, worker-dashboard, workers)
 * can't share a server, but they *can* run at the same time if each gets
 * its own port and its own Postgres database. No state bleed, no port
 * collision, wall time drops from `sum(suites)` to `max(suites)`.
 *
 * How it works per suite:
 *   1. Derive `ORCH8_E2E_PORT` (19001..19005) and
 *      `ORCH8_DATABASE_URL` (postgres://.../orch8_std_<suite>).
 *   2. Ensure the per-suite DB exists (created on first run via psql).
 *   3. Spawn `node --import tsx/esm --test <suite>.test.ts` with those
 *      envs. The suite's `before()` calls `startServer()`; harness reads
 *      ORCH8_E2E_PORT + ORCH8_DATABASE_URL so the server comes up
 *      isolated.
 *
 * If psql is missing or Postgres isn't reachable, we skip per-suite DB
 * setup and let the suites fall back to the default DB (caller handles
 * serialisation — same behaviour as before).
 */

import { spawn, execFileSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

interface Suite {
  file: string;
  port: number;
  dbName: string;
}

// Keep the port range well away from the shared-runner default (18080) and
// common app defaults. `run-e2e.ts` defaults to 18080; we start at 19001.
const SUITES: Suite[] = [
  { file: "resilience/persistence_recovery.test.ts", port: 19001, dbName: "orch8_std_persist" },
  { file: "features/triggers.test.ts",               port: 19002, dbName: "orch8_std_triggers" },
  { file: "signals/wait-signal.test.ts",             port: 19003, dbName: "orch8_std_waitsig" },
  { file: "features/worker-dashboard.test.ts",       port: 19004, dbName: "orch8_std_dash" },
  { file: "features/workers.test.ts",                port: 19005, dbName: "orch8_std_workers" },
];

const BASE_DB_URL =
  process.env.ORCH8_DATABASE_URL ||
  "postgres://orch8:orch8@localhost:5434/orch8";

function perSuiteDbUrl(dbName: string): string {
  const url = new URL(BASE_DB_URL);
  url.pathname = `/${dbName}`;
  return url.toString();
}

/**
 * Best-effort create each per-suite DB. Postgres has no `CREATE DATABASE IF
 * NOT EXISTS`, so we check pg_database first and only CREATE on absence.
 * Any failure here is logged and ignored — tests will surface the real
 * error if the DB truly isn't reachable.
 */
function ensureDatabases(suites: Suite[]): void {
  let url: URL;
  try {
    url = new URL(BASE_DB_URL);
  } catch {
    console.log("runner: BASE_DB_URL unparseable, skipping DB provisioning");
    return;
  }
  const admin = [
    "-h", url.hostname,
    "-p", url.port || "5432",
    "-U", url.username,
    "-d", "postgres",
    "-v", "ON_ERROR_STOP=1",
    "-tAc",
  ];
  const env = { ...process.env, PGPASSWORD: url.password };
  for (const s of suites) {
    try {
      const exists = execFileSync(
        "psql",
        [...admin, `SELECT 1 FROM pg_database WHERE datname='${s.dbName}'`],
        { env, encoding: "utf-8", stdio: ["ignore", "pipe", "pipe"] },
      ).trim();
      if (exists !== "1") {
        execFileSync(
          "psql",
          [...admin, `CREATE DATABASE "${s.dbName}"`],
          { env, stdio: "ignore" },
        );
        console.log(`runner: created db ${s.dbName}`);
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.log(`runner: warning: could not ensure db ${s.dbName}: ${msg}`);
    }
  }
}

interface SuiteResult {
  suite: Suite;
  code: number;
}

function runSuite(suite: Suite): Promise<SuiteResult> {
  return new Promise((resolveP) => {
    const reporterArgs = process.env.ORCH8_E2E_REPORTER
      ? [`--test-reporter=${process.env.ORCH8_E2E_REPORTER}`]
      : [];
    const child = spawn(
      process.execPath,
      [
        "--import", "tsx/esm",
        "--test",
        "--test-concurrency=1",
        "--test-timeout=60000",
        ...reporterArgs,
        resolve(__dirname, suite.file),
      ],
      {
        stdio: "inherit",
        env: {
          ...process.env,
          ORCH8_E2E_PORT: String(suite.port),
          ORCH8_DATABASE_URL: perSuiteDbUrl(suite.dbName),
          // Explicitly clear attach mode — each suite still spawns its own
          // server via its own `before()` hook.
          ORCH8_E2E_ATTACH: "",
        },
      },
    );
    child.on("exit", (c, sig) => {
      resolveP({ suite, code: c ?? (sig ? 1 : 0) });
    });
    child.on("error", (err) => {
      console.error(`runner: ${suite.file} spawn error: ${err.message}`);
      resolveP({ suite, code: 1 });
    });
  });
}

console.log(
  `runner: dispatching ${SUITES.length} self-managed suite(s) in parallel`,
);
ensureDatabases(SUITES);

const results = await Promise.all(SUITES.map(runSuite));

const failed = results.filter((r) => r.code !== 0);
if (failed.length > 0) {
  console.error("runner: FAILED suites:");
  for (const f of failed) {
    console.error(`  ${f.suite.file} (exit ${f.code})`);
  }
  process.exit(1);
}

console.log("runner: all self-managed suites passed");
process.exit(0);
