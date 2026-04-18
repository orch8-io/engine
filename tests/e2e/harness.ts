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

const DEFAULT_PORT = 18080;
const DB_URL =
  process.env.ORCH8_DATABASE_URL || "postgres://orch8:orch8@localhost:5434/orch8";
const STORAGE_BACKEND = process.env.ORCH8_STORAGE_BACKEND || "postgres";

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
}

/**
 * Find the orch8-server binary. Checks target/debug first, then target/<triple>/debug.
 */
function findBinary(): string {
  const direct = resolve(PROJECT_ROOT, "target/debug/orch8-server");
  if (existsSync(direct)) return direct;

  // Check target/<triple>/debug/ (cross-compiled builds land here).
  const targetDir = resolve(PROJECT_ROOT, "target");
  if (existsSync(targetDir)) {
    for (const entry of readdirSync(targetDir)) {
      const candidate = resolve(targetDir, entry, "debug/orch8-server");
      if (existsSync(candidate)) return candidate;
    }
  }

  throw new Error(
    "orch8-server binary not found. Run: cargo build --bin orch8-server",
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
  { port = DEFAULT_PORT }: StartServerOptions = {},
): Promise<ServerHandle> {
  if (process.env.ORCH8_E2E_ATTACH === "1") {
    const p = Number(process.env.ORCH8_E2E_PORT) || port;
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      try {
        const res = await fetch(`http://localhost:${p}/health/live`);
        if (res.ok) return { child: null, port: p, attached: true };
      } catch {
        /* not ready */
      }
      await sleep(100);
    }
    throw new Error(`Attach mode: server not reachable on port ${p}`);
  }

  const binaryPath = findBinary();
  console.log(`  Using binary: ${binaryPath}`);

  // Kill any stale process on the target port.
  try {
    const pids = execFileSync("lsof", ["-ti", `:${port}`], {
      encoding: "utf-8",
    }).trim();
    if (pids) {
      execFileSync("kill", ["-9", ...pids.split("\n")], { stdio: "ignore" });
      console.log(`  Killed stale process(es) on port ${port}`);
      await sleep(500);
    }
  } catch {
    // No process on port — expected.
  }

  // Clean stale data from previous test runs.
  if (STORAGE_BACKEND === "sqlite") {
    const sqlitePath = DB_URL.replace(/^sqlite:\/\//, "").replace(/^sqlite:/, "");
    if (sqlitePath && sqlitePath !== ":memory:") {
      for (const suffix of ["", "-wal", "-shm"]) {
        try {
          unlinkSync(sqlitePath + suffix);
        } catch {
          /* ignore if absent */
        }
      }
      console.log(`  Deleted stale SQLite database: ${sqlitePath}`);
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
      console.log("  Cleaned stale test data from database");
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.log(`  Warning: DB cleanup failed: ${msg}`);
    }
  }

  const httpAddr = `0.0.0.0:${port}`;

  const child = spawn(binaryPath, ["--insecure"], {
    cwd: PROJECT_ROOT,
    env: {
      ...process.env,
      ORCH8_STORAGE_BACKEND: STORAGE_BACKEND,
      ORCH8_DATABASE_URL: DB_URL,
      ORCH8_HTTP_ADDR: httpAddr,
      ORCH8_LOG_LEVEL: "info",
      ORCH8_TICK_INTERVAL_MS: "100",
    },
    stdio: ["pipe", "pipe", "pipe"],
  });

  // Stream all stderr for debugging.
  child.stderr.on("data", (data: Buffer) => {
    const line = data.toString().trim();
    if (line) console.log(`  [server] ${line}`);
  });

  child.on("error", (err: Error) => {
    console.error(`  [server] Failed to start: ${err.message}`);
  });

  // Wait for server to be ready.
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://localhost:${port}/health/live`);
      if (res.ok) {
        console.log(`  Server ready on port ${port}`);
        return { child, port };
      }
    } catch {
      // Not ready yet.
    }
    await sleep(200);
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
