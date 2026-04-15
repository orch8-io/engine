/**
 * Test harness: starts/stops the orch8-server binary for E2E tests.
 *
 * Prerequisites:
 *   - Postgres running (docker-compose up -d)
 *   - cargo build (binary must exist)
 */

import { spawn, execFileSync } from "node:child_process";
import { existsSync, readdirSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, "../..");

const DEFAULT_PORT = 18080;
const DB_URL = process.env.ORCH8_DATABASE_URL || "postgres://orch8:orch8@localhost:5434/orch8";

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
 */
export async function startServer({ port = DEFAULT_PORT, build = true } = {}) {
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

  const httpAddr = `0.0.0.0:${port}`;

  const child = spawn(binaryPath, [], {
    cwd: PROJECT_ROOT,
    env: {
      ...process.env,
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
