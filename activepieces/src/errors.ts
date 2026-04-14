// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.

/**
 * Error classification for piece execution failures.
 *
 * The orch8 engine distinguishes retryable vs permanent step errors via the
 * `StepError` enum on the Rust side. This module mirrors that contract so the
 * sidecar can return a consistent shape the Rust dispatcher can translate
 * directly into `StepError::Retryable` or `StepError::Permanent`.
 */

export type ErrorType = "retryable" | "permanent";

export interface PieceError {
  type: ErrorType;
  message: string;
  details?: unknown;
}

/**
 * Classify an unknown thrown value into a `PieceError`.
 *
 * Heuristics:
 *   - HTTP 5xx, network/DNS/connection errors, timeouts → retryable
 *   - HTTP 4xx, JSON / validation / auth errors → permanent
 *   - Everything unknown → retryable (fail-open so transient issues recover)
 *
 * Pieces use `@activepieces/pieces-common`'s httpClient which throws errors
 * with a `response.status` field on HTTP failures; we inspect that first.
 */
export function classifyError(err: unknown): PieceError {
  if (err instanceof PieceExecutionError) {
    return { type: err.type, message: err.message, details: err.details };
  }

  const e = err as {
    message?: string;
    name?: string;
    code?: string;
    response?: { status?: number; data?: unknown };
    status?: number;
  } | null;

  const message = e?.message ?? String(err);
  const status = e?.response?.status ?? e?.status;

  if (typeof status === "number") {
    if (status >= 500) {
      return { type: "retryable", message, details: { status, body: e?.response?.data } };
    }
    if (status >= 400) {
      return { type: "permanent", message, details: { status, body: e?.response?.data } };
    }
  }

  // Node/undici network errors that are typically transient.
  const retryableCodes = new Set([
    "ECONNRESET",
    "ECONNREFUSED",
    "ETIMEDOUT",
    "EAI_AGAIN",
    "ENOTFOUND",
    "EPIPE",
    "UND_ERR_SOCKET",
    "UND_ERR_CONNECT_TIMEOUT",
    "UND_ERR_HEADERS_TIMEOUT",
  ]);
  if (e?.code && retryableCodes.has(e.code)) {
    return { type: "retryable", message, details: { code: e.code } };
  }

  // Syntax / type errors inside piece code are almost always bugs, not transient.
  if (e?.name === "TypeError" || e?.name === "SyntaxError" || e?.name === "ReferenceError") {
    return { type: "permanent", message, details: { name: e.name } };
  }

  // Unknown failure: prefer retryable so genuine transients don't get stuck in DLQ.
  return { type: "retryable", message };
}

/**
 * Explicit error with a pre-classified type. Thrown by the sidecar for errors
 * originating *inside* the adapter (piece not found, bad request) — these
 * bypass the classification heuristic above.
 */
export class PieceExecutionError extends Error {
  constructor(
    public readonly type: ErrorType,
    message: string,
    public readonly details?: unknown,
  ) {
    super(message);
    this.name = "PieceExecutionError";
  }
}
