#!/usr/bin/env node
// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.

/**
 * CLI entry point for the orch8 ActivePieces sidecar.
 *
 * Environment variables:
 *   ORCH8_AP_PORT       — listen port (default 50052)
 *   ORCH8_AP_HOST       — bind address (default 127.0.0.1; use 0.0.0.0 for Docker)
 *   ORCH8_AP_TIMEOUT_MS — per-piece execution timeout (default 60000)
 *   ORCH8_AP_ALLOWLIST  — comma-separated piece names; when set, only these
 *                         pieces may be loaded. Leave unset to allow any
 *                         `@activepieces/piece-*` package installed locally.
 */

import { createServer, ServerOptions } from "./server";
import { createDefaultLoader } from "./registry";

export { createServer } from "./server";
export { createDefaultLoader, findAction } from "./registry";
export { buildActionContext } from "./context";
export { classifyError, PieceExecutionError } from "./errors";

function parseEnvInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(`invalid ${name}='${raw}' — expected a positive integer`);
  }
  return n;
}

function parseAllowlist(raw: string | undefined): string[] | undefined {
  if (!raw) return undefined;
  const list = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  return list.length ? list : undefined;
}

function main(): void {
  const port = parseEnvInt("ORCH8_AP_PORT", 50052);
  const host = process.env.ORCH8_AP_HOST ?? "127.0.0.1";
  const timeoutMs = parseEnvInt("ORCH8_AP_TIMEOUT_MS", 60_000);
  const allowlist = parseAllowlist(process.env.ORCH8_AP_ALLOWLIST);

  const loader = createDefaultLoader({ allowlist });

  const opts: ServerOptions = { port, host, loader, requestTimeoutMs: timeoutMs };
  const server = createServer(opts);

  server.listen(port, host, () => {
    // Intentionally log to stdout so container runtimes (Docker, k8s) capture it.
    process.stdout.write(
      JSON.stringify({
        ts: new Date().toISOString(),
        level: "info",
        msg: "orch8-activepieces-worker listening",
        host,
        port,
        timeout_ms: timeoutMs,
        allowlist: allowlist ?? "any",
      }) + "\n",
    );
  });

  const shutdown = (signal: string) => {
    process.stdout.write(
      JSON.stringify({ ts: new Date().toISOString(), level: "info", msg: "shutting down", signal }) +
        "\n",
    );
    server.close(() => process.exit(0));
    // Hard stop if graceful shutdown stalls (e.g. a piece in a 60s HTTP call).
    setTimeout(() => process.exit(1), 10_000).unref();
  };
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}

// Only start the server when invoked as a binary, not when imported as a library.
if (require.main === module) {
  try {
    main();
  } catch (err) {
    process.stderr.write(
      JSON.stringify({
        ts: new Date().toISOString(),
        level: "error",
        msg: "startup failed",
        error: (err as Error).message,
      }) + "\n",
    );
    process.exit(1);
  }
}
