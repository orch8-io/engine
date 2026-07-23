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
import { SELF_MANAGED_SUITES, basenameOf } from "./self-managed.ts";

const __dirname = dirname(fileURLToPath(import.meta.url));

interface Suite {
  file: string;
  port: number;
  dbName: string;
}

// Derive the suite list from the single source of truth (self-managed.ts),
// auto-assigning a unique port and Postgres database to each. Ports start at
// 19001 — well clear of the shared-runner default (18080) and common app
// defaults. DB names are derived from the basename so two suites can't
// collide. This replaces a hand-maintained list that drifted out of sync
// with run-e2e.ts and silently dropped 17 suites from CI.
const SUITES: Suite[] = SELF_MANAGED_SUITES.map((s, i) => ({
  file: s.file,
  port: 19001 + i,
  dbName: `orch8_std_${basenameOf(s.file).replace(/\.test\.ts$/, "").replace(/[^a-z0-9]/gi, "_")}`,
}));

// Cap how many suites run at once. Each suite boots its own debug-build
// server + Postgres DB; 20+ in parallel would exhaust a CI runner's RAM and
// connection slots. A small pool keeps wall time low (≈ceil(N/limit) waves)
// without thrashing. Override with ORCH8_E2E_CONCURRENCY (1 = fully serial).
//
// Default is lower under `CI` (GitHub Actions sets this automatically):
// a standard `ubuntu-latest` runner doesn't reliably have headroom for 6
// concurrent debug-build servers + Postgres connections — observed as a
// synchronized burst of "Server failed to start within 15 seconds" /
// "cancelledByParent" across most of the pool, not any one suite being
// broken. Local dev machines keep the faster default.
const DEFAULT_CONCURRENCY = process.env.CI ? 2 : 6;
const CONCURRENCY = Math.max(
  1,
  Number(process.env.ORCH8_E2E_CONCURRENCY) || DEFAULT_CONCURRENCY,
);

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

// Short label for log prefixing — derived from filename, not the full path.
function suiteLabel(suite: Suite): string {
  const base = suite.file.split("/").pop() ?? suite.file;
  return base.replace(/\.test\.ts$/, "");
}

// Maximum label width, so all prefixes align in a column.
const LABEL_WIDTH = Math.max(...SUITES.map((s) => suiteLabel(s).length));

/**
 * Pipe one of the child's streams through a line-splitter, prefixing each
 * line with the suite's label. Handles partial lines across chunks.
 */
function streamWithPrefix(
  stream: NodeJS.ReadableStream,
  out: NodeJS.WriteStream,
  label: string,
): void {
  let buf = "";
  stream.setEncoding("utf-8");
  stream.on("data", (chunk: string) => {
    buf += chunk;
    const lines = buf.split("\n");
    buf = lines.pop() ?? "";
    for (const line of lines) {
      out.write(`[${label.padEnd(LABEL_WIDTH)}] ${line}\n`);
    }
  });
  stream.on("end", () => {
    if (buf.length > 0) {
      out.write(`[${label.padEnd(LABEL_WIDTH)}] ${buf}\n`);
    }
  });
}

function runSuite(suite: Suite): Promise<SuiteResult> {
  return new Promise((resolveP) => {
    const reporterArgs = process.env.ORCH8_E2E_REPORTER
      ? [`--test-reporter=${process.env.ORCH8_E2E_REPORTER}`]
      : [];
    const label = suiteLabel(suite);
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
        // `pipe` so we can tag every line with the suite label — otherwise
        // 5 parallel suites interleave line-by-line and the CI log is
        // unreadable. Streams forwarded to the runner's stdout/stderr below.
        stdio: ["ignore", "pipe", "pipe"],
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
    streamWithPrefix(child.stdout!, process.stdout, label);
    streamWithPrefix(child.stderr!, process.stderr, label);
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
  `runner: dispatching ${SUITES.length} self-managed suite(s), ${CONCURRENCY} at a time`,
);
for (const s of SUITES) {
  console.log(`  [${suiteLabel(s).padEnd(LABEL_WIDTH)}] ${s.file}`);
}
ensureDatabases(SUITES);

/**
 * Run all suites through a fixed-size worker pool. Each of the `limit`
 * workers pulls the next un-started suite until the queue drains, so at
 * most `limit` servers are live at once. Results preserve completion order
 * (the summary re-sorts for readability anyway).
 */
async function runPool(suites: Suite[], limit: number): Promise<SuiteResult[]> {
  const results: SuiteResult[] = [];
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < suites.length) {
      const suite = suites[next++]!;
      results.push(await runSuite(suite));
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(limit, suites.length) }, worker),
  );
  return results;
}

const start = Date.now();
const results = await runPool(SUITES, CONCURRENCY);
const wallMs = Date.now() - start;

// Concise summary at the bottom so CI reviewers can see status at a glance
// without scrolling through the prefixed output above.
console.log("");
console.log("runner: ───────────── summary ─────────────");
const sorted = [...results].sort((a, b) => a.suite.file.localeCompare(b.suite.file));
for (const r of sorted) {
  const status = r.code === 0 ? "PASS" : `FAIL(${r.code})`;
  console.log(`  ${status.padEnd(8)} ${suiteLabel(r.suite).padEnd(LABEL_WIDTH)}  ${r.suite.file}`);
}
console.log(`runner: wall time ${(wallMs / 1000).toFixed(1)}s`);

const failed = results.filter((r) => r.code !== 0);
if (failed.length > 0) {
  console.error(`runner: ${failed.length} suite(s) FAILED`);
  process.exit(1);
}

console.log("runner: all self-managed suites passed");
process.exit(0);
