// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.

import * as http from "node:http";
import { buildActionContext } from "./context";
import { classifyError, PieceExecutionError } from "./errors";
import { createDefaultLoader, findAction, Piece, PieceLoader } from "./registry";

/**
 * HTTP shape exchanged with the Rust `ap://` handler.
 *
 * Request body (POST /execute):
 *   {
 *     "piece": "slack",
 *     "action": "send_message",
 *     "auth":  { "access_token": "..." } | "api-key" | { ... },
 *     "props": { ... action-specific ... },
 *     "instance_id": "uuid",
 *     "block_id": "step_1"
 *   }
 *
 * Response body:
 *   Success (HTTP 200): { "ok": true,  "output": <action return value> }
 *   Failure (HTTP 4xx/5xx): { "ok": false, "error": { "type": "retryable"|"permanent", "message": "...", "details": ... } }
 *
 * Status code mapping:
 *   200  — action completed successfully
 *   422  — piece/action not found, bad request shape (permanent)
 *   502  — piece threw an error classified as retryable
 *   500  — piece threw an error classified as permanent
 *
 * The Rust `ap://` handler translates 5xx → StepError::Retryable and
 * 4xx → StepError::Permanent, matching orch8's existing grpc_plugin contract.
 */
export interface ExecuteRequest {
  piece: string;
  action: string;
  auth?: unknown;
  props?: Record<string, unknown>;
  instance_id?: string;
  block_id?: string;
}

export interface ServerOptions {
  port: number;
  host?: string;
  loader?: PieceLoader;
  /** Optional per-request timeout in ms. Default 60s, matching worker heartbeat cadence. */
  requestTimeoutMs?: number;
  /** Called on each request for structured logging; supply a real logger in prod. */
  log?: (level: "info" | "warn" | "error", msg: string, fields?: Record<string, unknown>) => void;
}

const DEFAULT_TIMEOUT_MS = 60_000;

export function createServer(opts: ServerOptions): http.Server {
  const loader = opts.loader ?? createDefaultLoader();
  const timeoutMs = opts.requestTimeoutMs ?? DEFAULT_TIMEOUT_MS;
  const log = opts.log ?? defaultLog;

  const server = http.createServer((req, res) => {
    handleRequest(req, res, loader, timeoutMs, log).catch((err) => {
      // Unhandled inside handleRequest is a bug — never leak stack traces to clients.
      log("error", "unhandled server error", { err: String(err) });
      if (!res.headersSent) {
        writeJson(res, 500, {
          ok: false,
          error: { type: "retryable", message: "internal sidecar error" },
        });
      }
    });
  });

  // Per-socket timeout guard: if a piece hangs, kill the socket so the orch8
  // engine-side timeout doesn't pile up open connections.
  server.setTimeout(timeoutMs + 5_000);
  return server;
}

async function handleRequest(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  loader: PieceLoader,
  timeoutMs: number,
  log: NonNullable<ServerOptions["log"]>,
): Promise<void> {
  const url = req.url ?? "/";

  if (req.method === "GET" && (url === "/health" || url === "/")) {
    writeJson(res, 200, { ok: true, service: "orch8-activepieces-worker" });
    return;
  }

  if (req.method !== "POST" || url !== "/execute") {
    writeJson(res, 404, {
      ok: false,
      error: { type: "permanent", message: `no route for ${req.method} ${url}` },
    });
    return;
  }

  let body: ExecuteRequest;
  try {
    body = await readJson<ExecuteRequest>(req);
  } catch (err) {
    writeJson(res, 422, {
      ok: false,
      error: { type: "permanent", message: `invalid request body: ${(err as Error).message}` },
    });
    return;
  }

  if (!body || typeof body.piece !== "string" || typeof body.action !== "string") {
    writeJson(res, 422, {
      ok: false,
      error: { type: "permanent", message: "request must include string 'piece' and 'action' fields" },
    });
    return;
  }

  let piece: Piece;
  try {
    piece = await loader.load(body.piece);
  } catch (err) {
    respondWithError(res, err, log, { piece: body.piece });
    return;
  }

  let action;
  try {
    action = findAction(piece, body.action);
  } catch (err) {
    respondWithError(res, err, log, { piece: body.piece, action: body.action });
    return;
  }

  const ctx = buildActionContext({
    auth: body.auth,
    propsValue: body.props ?? {},
    instanceId: body.instance_id ?? "",
    blockId: body.block_id ?? "",
  });

  const started = Date.now();
  try {
    const output = await withTimeout(action.run(ctx), timeoutMs, body.piece, body.action);
    log("info", "piece action completed", {
      piece: body.piece,
      action: body.action,
      duration_ms: Date.now() - started,
      instance_id: body.instance_id,
    });
    writeJson(res, 200, { ok: true, output: output ?? null });
  } catch (err) {
    const classified = classifyError(err);
    const status = classified.type === "retryable" ? 502 : 500;
    log(classified.type === "retryable" ? "warn" : "error", "piece action failed", {
      piece: body.piece,
      action: body.action,
      duration_ms: Date.now() - started,
      instance_id: body.instance_id,
      error_type: classified.type,
      error_message: classified.message,
    });
    writeJson(res, status, { ok: false, error: classified });
  }
}

function respondWithError(
  res: http.ServerResponse,
  err: unknown,
  log: NonNullable<ServerOptions["log"]>,
  fields: Record<string, unknown>,
) {
  if (err instanceof PieceExecutionError) {
    const status = err.type === "permanent" ? 422 : 502;
    log("warn", err.message, { ...fields, error_type: err.type });
    writeJson(res, status, {
      ok: false,
      error: { type: err.type, message: err.message, details: err.details },
    });
    return;
  }
  const classified = classifyError(err);
  log("error", classified.message, { ...fields, error_type: classified.type });
  writeJson(res, classified.type === "permanent" ? 500 : 502, { ok: false, error: classified });
}

function writeJson(res: http.ServerResponse, status: number, payload: unknown): void {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body).toString(),
  });
  res.end(body);
}

const MAX_BODY_BYTES = 4 * 1024 * 1024; // 4 MiB — matches orch8's default block-output cap.

async function readJson<T>(req: http.IncomingMessage): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let size = 0;
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        req.destroy();
        reject(new Error(`request body exceeds ${MAX_BODY_BYTES} bytes`));
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf8");
        resolve(raw.length === 0 ? ({} as T) : (JSON.parse(raw) as T));
      } catch (err) {
        reject(err as Error);
      }
    });
    req.on("error", reject);
  });
}

async function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  piece: string,
  action: string,
): Promise<T> {
  let timer: NodeJS.Timeout | null = null;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new PieceExecutionError("retryable", `piece '${piece}.${action}' timed out after ${ms}ms`));
    }, ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function defaultLog(
  level: "info" | "warn" | "error",
  msg: string,
  fields?: Record<string, unknown>,
): void {
  const entry = { ts: new Date().toISOString(), level, msg, ...fields };
  const line = JSON.stringify(entry);
  if (level === "error") {
    process.stderr.write(line + "\n");
  } else {
    process.stdout.write(line + "\n");
  }
}
