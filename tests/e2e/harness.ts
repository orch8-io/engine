/**
 * Test harness: starts/stops the orch8-server binary for E2E tests.
 *
 * Prerequisites (the harness does NOT build):
 *   - Postgres running (docker-compose up -d)
 *   - `cargo build --bin orch8-server` already ran so the binary exists
 *     under `target/debug/` or `target/<triple>/debug/`
 *
 * Building is an orthogonal step that belongs to the caller (CI pipeline,
 * `make`, or the developer before running the suite). Embedding it here
 * meant every suite invocation paid cargo fingerprint overhead even when
 * the binary was already current.
 */

import { spawn, execFileSync } from "node:child_process";
import { existsSync, readdirSync, unlinkSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import type { ChildProcessWithoutNullStreams } from "node:child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, "../..");

// A caller can override the default port via env — lets `run-standalone.ts`
// assign each parallel suite its own port without touching each test file.
const DEFAULT_PORT = Number(process.env.ORCH8_E2E_PORT) || 18080;
const DB_URL =
  process.env.ORCH8_DATABASE_URL || "postgres://orch8:orch8@localhost:5434/orch8";
/**
 * Raw Postgres URL for the test DB. Exported so individual suites can run
 * their own direct queries (e.g. to inspect ciphertext at rest).
 */
export const TEST_DB_URL = DB_URL;
const STORAGE_BACKEND = process.env.ORCH8_STORAGE_BACKEND || "postgres";

// Server logs drown real test output at `info` — the engine emits per-tick
// and per-poll lines. Default to `warn` so warnings/errors still surface;
// opt back into full verbosity with ORCH8_E2E_VERBOSE=1 when debugging.
const VERBOSE = process.env.ORCH8_E2E_VERBOSE === "1";
const SERVER_LOG_LEVEL =
  process.env.ORCH8_LOG_LEVEL || (VERBOSE ? "info" : "warn");

const USE_COLOR = process.stdout.isTTY && !process.env.NO_COLOR;
const dim = (s: string): string => (USE_COLOR ? `\x1b[2m${s}\x1b[0m` : s);

/**
 * Handle returned by `startServer`. In attach mode `child` is `null` and
 * `stopServer` is a no-op — the runner owns the lifecycle.
 */
export interface ServerHandle {
  child: ChildProcessWithoutNullStreams | null;
  port: number;
  attached?: boolean;
}

export interface StartServerOptions {
  port?: number;
  /**
   * Extra environment variables merged into the spawned server's env.
   *
   * Ignored in attach mode (`ORCH8_E2E_ATTACH=1`) since the runner-owned
   * server is already running — a warning is emitted so the test author
   * can move the suite into `SELF_MANAGED_SUITES` if the override matters.
   */
  env?: Record<string, string>;
  /**
   * Skip database cleanup before starting. Useful when restarting a server
   * mid-test (e.g. key rotation) and you need to preserve data from the
   * prior phase.
   */
  skipCleanup?: boolean;
}

/**
 * Find the orch8-server binary.
 *
 * Preference order (fastest runtime first):
 *   1. target/release/
 *   2. target/<triple>/release/
 *   3. target/debug/
 *   4. target/<triple>/debug/
 *
 * Release builds are 3-10x faster on async/DB-heavy paths, so a one-time
 * `cargo build --release --bin orch8-server` locally slashes E2E wall time.
 * CI still builds debug by default — the debug fallback keeps that path
 * working unchanged.
 */
function findBinary(): string {
  const candidates: string[] = [
    resolve(PROJECT_ROOT, "target/release/orch8-server"),
  ];

  const targetDir = resolve(PROJECT_ROOT, "target");
  if (existsSync(targetDir)) {
    for (const entry of readdirSync(targetDir)) {
      candidates.push(resolve(targetDir, entry, "release/orch8-server"));
    }
  }

  candidates.push(resolve(PROJECT_ROOT, "target/debug/orch8-server"));
  if (existsSync(targetDir)) {
    for (const entry of readdirSync(targetDir)) {
      candidates.push(resolve(targetDir, entry, "debug/orch8-server"));
    }
  }

  for (const p of candidates) {
    if (existsSync(p)) return p;
  }

  throw new Error(
    "orch8-server binary not found. Run: cargo build --bin orch8-server (or --release for faster E2E)",
  );
}

/**
 * Start the orch8-server. Returns a handle for `stopServer()`.
 *
 * Two modes:
 *   - **Spawn** (default): locate pre-built binary, clean DB, spawn it,
 *     wait for liveness. Used when a suite runs standalone
 *     (`node --test ./foo.test.ts`).
 *   - **Attach** (`ORCH8_E2E_ATTACH=1`): a parent runner has already
 *     started the server — skip spawn/clean and just verify it's up. Used
 *     by `run-e2e.ts` to share one server across every suite.
 *
 * Does NOT build — the binary must already exist in `target/`. If it's
 * missing, `findBinary()` throws with the `cargo build` hint.
 */
export async function startServer(
  { port = DEFAULT_PORT, env: extraEnv, skipCleanup = false }: StartServerOptions = {},
): Promise<ServerHandle> {
  if (process.env.ORCH8_E2E_ATTACH === "1") {
    if (extraEnv && Object.keys(extraEnv).length > 0) {
      console.log(
        dim(
          "  warning: `env` override ignored in attach mode — move the suite into SELF_MANAGED_SUITES for the override to take effect",
        ),
      );
    }
    const p = Number(process.env.ORCH8_E2E_PORT) || port;
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      try {
        const res = await fetch(`http://localhost:${p}/health/live`);
        if (res.ok) return { child: null, port: p, attached: true };
      } catch {
        /* not ready */
      }
      await sleep(25);
    }
    throw new Error(`Attach mode: server not reachable on port ${p}`);
  }

  const binaryPath = findBinary();
  console.log(dim(`  binary: ${binaryPath}`));

  // Kill any stale process on the target port.
  try {
    const pids = execFileSync("lsof", ["-ti", `:${port}`], {
      encoding: "utf-8",
    }).trim();
    if (pids) {
      execFileSync("kill", ["-9", ...pids.split("\n")], { stdio: "ignore" });
      console.log(dim(`  killed stale process(es) on port ${port}`));
      await sleep(500);
    }
  } catch {
    // No process on port — expected.
  }

  // Clean stale data from previous test runs.
  if (skipCleanup) {
    console.log(dim("  skipping db cleanup (skipCleanup=true)"));
  } else if (STORAGE_BACKEND === "sqlite") {
    const sqlitePath = DB_URL.replace(/^sqlite:\/\//, "").replace(/^sqlite:/, "");
    if (sqlitePath && sqlitePath !== ":memory:") {
      for (const suffix of ["", "-wal", "-shm"]) {
        try {
          unlinkSync(sqlitePath + suffix);
        } catch {
          /* ignore if absent */
        }
      }
      console.log(dim(`  deleted stale sqlite db: ${sqlitePath}`));
    }
  } else {
    try {
      const dbUrl = new URL(DB_URL);
      // Guarded cleanup: on the very first run the binary hasn't applied
      // migrations yet so the tables don't exist. A plain DELETE would emit
      // "relation ... does not exist" noise. The DO block checks pg_tables
      // and only deletes when the schema is already in place.
      const cleanupSql = `
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'task_instances') THEN
            DELETE FROM worker_tasks;
            DELETE FROM block_outputs;
            DELETE FROM execution_tree;
            DELETE FROM signal_inbox;
            DELETE FROM task_instances;
            DELETE FROM cron_schedules;
            DELETE FROM triggers;
            DELETE FROM sessions;
            DELETE FROM checkpoints;
            DELETE FROM externalized_state;
            DELETE FROM emit_event_dedupe;
            DELETE FROM audit_log;
            DELETE FROM sequences;
          END IF;
        END $$;
      `;
      execFileSync(
        "psql",
        [
          "-h", dbUrl.hostname,
          "-p", dbUrl.port,
          "-U", dbUrl.username,
          "-d", dbUrl.pathname.slice(1),
          "-v", "ON_ERROR_STOP=1",
          "-c", cleanupSql,
        ],
        {
          env: { ...process.env, PGPASSWORD: dbUrl.password },
          stdio: "pipe",
        },
      );
      console.log(dim("  cleaned stale test data from db"));
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.log(dim(`  warning: db cleanup failed: ${msg}`));
    }
  }

  const httpAddr = `0.0.0.0:${port}`;

  const spawnStart = Date.now();
  const child = spawn(binaryPath, ["--insecure"], {
    cwd: PROJECT_ROOT,
    env: {
      ...process.env,
      ORCH8_STORAGE_BACKEND: STORAGE_BACKEND,
      ORCH8_DATABASE_URL: DB_URL,
      ORCH8_HTTP_ADDR: httpAddr,
      ORCH8_LOG_LEVEL: SERVER_LOG_LEVEL,
      ORCH8_TICK_INTERVAL_MS: "100",
      ORCH8_CRON_TICK_SECS: "1",
      // Surface a full backtrace if the server panics — the CI cascade
      // we chased in April 2026 died silently because this was unset.
      RUST_BACKTRACE: process.env.RUST_BACKTRACE ?? "full",
      ...(extraEnv ?? {}),
    },
    stdio: ["pipe", "pipe", "pipe"],
  });

  // Stream server stderr, line by line. Verbose mode keeps everything;
  // normal mode relies on the level filter (warn+) set above — whatever
  // makes it through is worth seeing, so keep it visible (just dimmed).
  child.stderr.on("data", (data: Buffer) => {
    const text = data.toString();
    for (const raw of text.split(/\r?\n/)) {
      const line = raw.trimEnd();
      if (line) console.log(dim(`  [server] ${line}`));
    }
  });

  // Also forward stdout. Rust panics typically go to stderr, but println!
  // and some abort paths (tokio task panic with custom hook, libc abort
  // before stderr flushes) can end up here. Streaming both means a crash
  // can't slip through unobserved.
  child.stdout.on("data", (data: Buffer) => {
    const text = data.toString();
    for (const raw of text.split(/\r?\n/)) {
      const line = raw.trimEnd();
      if (line) console.log(dim(`  [server:out] ${line}`));
    }
  });

  child.on("error", (err: Error) => {
    console.error(`  [server] Failed to start: ${err.message}`);
  });

  // Log the instant the server dies, with exit code + signal + uptime.
  // Without this the CI cascade looks like "every suite times out" —
  // having the death timestamp lets us correlate with the test that was
  // running when the process vanished.
  child.on("exit", (code: number | null, signal: NodeJS.Signals | null) => {
    const uptimeMs = Date.now() - spawnStart;
    console.log(
      dim(
        `  [server] exited: code=${code} signal=${signal} uptime_ms=${uptimeMs}`,
      ),
    );
  });

  // Wait for server to be ready. Poll aggressively — the server usually
  // comes up in <1s, so a 200ms interval wastes ~100ms per spawn on
  // average, which compounds across parallel suites.
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://localhost:${port}/health/live`);
      if (res.ok) {
        console.log(dim(`  server ready on port ${port}`));
        return { child, port };
      }
    } catch {
      // Not ready yet.
    }
    await sleep(25);
  }

  child.kill("SIGTERM");
  throw new Error("Server failed to start within 15 seconds");
}

/** Gracefully stop the server. No-op on attach-mode handles. */
export async function stopServer(handle: ServerHandle | undefined): Promise<void> {
  if (!handle?.child) return;
  const child = handle.child;

  child.kill("SIGTERM");

  await new Promise<void>((resolvePromise) => {
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      resolvePromise();
    }, 5_000);

    child.on("exit", () => {
      clearTimeout(timeout);
      resolvePromise();
    });
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
