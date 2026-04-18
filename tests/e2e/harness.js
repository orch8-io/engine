/**
 * Test harness: starts/stops the orch8-server binary for E2E tests.
 *
 * Prerequisites:
 *   - Postgres running (docker-compose up -d)
 *   - cargo build (binary must exist)
 */

import { spawn, execSync, execFileSync } from "node:child_process";
import { existsSync, readdirSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, "../..");

const DEFAULT_PORT = 18080;
const DB_URL = process.env.ORCH8_DATABASE_URL || "postgres://orch8:orch8@localhost:5434/orch8";
const STORAGE_BACKEND = process.env.ORCH8_STORAGE_BACKEND || "postgres";

/**
 * Find the orch8-server binary. Checks target/debug first, then target/<triple>/debug.
 */
function findBinary() {
  const direct = resolve(PROJECT_ROOT, "target/debug/orch8-server");
  if (existsSync(direct)) return direct;

  // Check target/<triple>/debug/
  const targetDir = resolve(PROJECT_ROOT, "target");
  if (existsSync(targetDir)) {
    for (const entry of readdirSync(targetDir)) {
      const candidate = resolve(targetDir, entry, "debug/orch8-server");
      if (existsSync(candidate)) return candidate;
    }
  }

  throw new Error("orch8-server binary not found. Run: cargo build --bin orch8-server");
}

/**
 * Build and start the orch8-server. Returns a handle for stopServer().
 *
 * Two modes:
 *   - **Spawn** (default): build, clean DB, spawn binary, wait for liveness.
 *     Used when a suite runs standalone (`node --test ./foo.test.js`).
 *   - **Attach** (`ORCH8_E2E_ATTACH=1`): a parent runner has already started
 *     the server — skip build/clean/spawn and just verify it's up. Used by
 *     `run-e2e.js` to share one server across every suite for ~15-20s of
 *     savings over per-suite startup.
 *
 * The returned handle's `child` is `null` in attach mode, so `stopServer()`
 * becomes a no-op and lifecycle stays owned by the runner.
 */
export async function startServer({ port = DEFAULT_PORT, build = true } = {}) {
  if (process.env.ORCH8_E2E_ATTACH === "1") {
    const p = Number(process.env.ORCH8_E2E_PORT) || port;
    const deadline = Date.now() + 10000;
    while (Date.now() < deadline) {
      try {
        const res = await fetch(`http://localhost:${p}/health/live`);
        if (res.ok) return { child: null, port: p, attached: true };
      } catch { /* not ready */ }
      await sleep(100);
    }
    throw new Error(`Attach mode: server not reachable on port ${p}`);
  }

  if (build) {
    console.log("  Building orch8-server...");
    execFileSync("cargo", ["build", "--bin", "orch8-server"], {
      cwd: PROJECT_ROOT,
      stdio: "pipe",
    });
    console.log("  Build complete.");
  }

  const binaryPath = findBinary();
  console.log(`  Using binary: ${binaryPath}`);

  // Kill any stale process on the target port.
  try {
    const pids = execFileSync("lsof", ["-ti", `:${port}`], { encoding: "utf-8" }).trim();
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
    // For SQLite, delete and recreate the db file so each run starts clean.
    const sqlitePath = DB_URL.replace(/^sqlite:\/\//, "").replace(/^sqlite:/, "");
    if (sqlitePath && sqlitePath !== ":memory:") {
      try {
        const { unlinkSync } = await import("node:fs");
        for (const suffix of ["", "-wal", "-shm"]) {
          try { unlinkSync(sqlitePath + suffix); } catch { /* ignore if absent */ }
        }
        console.log(`  Deleted stale SQLite database: ${sqlitePath}`);
      } catch (e) {
        console.log(`  Warning: SQLite cleanup failed: ${e.message}`);
      }
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
      execFileSync("psql", [
        "-h", dbUrl.hostname,
        "-p", dbUrl.port,
        "-U", dbUrl.username,
        "-d", dbUrl.pathname.slice(1),
        "-v", "ON_ERROR_STOP=1",
        "-c", cleanupSql,
      ], {
        env: { ...process.env, PGPASSWORD: dbUrl.password },
        stdio: "pipe",
      });
      console.log("  Cleaned stale test data from database");
    } catch (e) {
      console.log(`  Warning: DB cleanup failed: ${e.message}`);
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

  // Stream all stderr for debugging
  child.stderr.on("data", (data) => {
    const line = data.toString().trim();
    if (line) {
      console.log(`  [server] ${line}`);
    }
  });

  child.on("error", (err) => {
    console.error(`  [server] Failed to start: ${err.message}`);
  });

  // Wait for server to be ready
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://localhost:${port}/health/live`);
      if (res.ok) {
        console.log(`  Server ready on port ${port}`);
        return { child, port };
      }
    } catch {
      // Not ready yet
    }
    await sleep(200);
  }

  child.kill("SIGTERM");
  throw new Error("Server failed to start within 15 seconds");
}

/**
 * Gracefully stop the server.
 */
export async function stopServer(handle) {
  if (!handle?.child) return;

  handle.child.kill("SIGTERM");

  await new Promise((resolve) => {
    const timeout = setTimeout(() => {
      handle.child.kill("SIGKILL");
      resolve();
    }, 5000);

    handle.child.on("exit", () => {
      clearTimeout(timeout);
      resolve();
    });
  });
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
