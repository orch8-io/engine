// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.
//
// Unit tests for error classification: PieceExecutionError construction,
// HTTP status heuristics, network error codes, and fail-open defaults.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { classifyError, PieceExecutionError } from "../src/errors.ts";

// ---------------------------------------------------------------------------
// PieceExecutionError — construction
// ---------------------------------------------------------------------------

test("PieceExecutionError is an Error instance", () => {
  const err = new PieceExecutionError("permanent", "bad input");
  assert.ok(err instanceof Error);
});

test("PieceExecutionError is a PieceExecutionError instance", () => {
  const err = new PieceExecutionError("retryable", "timeout");
  assert.ok(err instanceof PieceExecutionError);
});

test("PieceExecutionError.name is 'PieceExecutionError'", () => {
  const err = new PieceExecutionError("permanent", "oops");
  assert.equal(err.name, "PieceExecutionError");
});

test("PieceExecutionError stores type, message, details", () => {
  const err = new PieceExecutionError("permanent", "missing field", { field: "email" });
  assert.equal(err.type, "permanent");
  assert.equal(err.message, "missing field");
  assert.deepEqual(err.details, { field: "email" });
});

test("PieceExecutionError with no details has undefined details", () => {
  const err = new PieceExecutionError("retryable", "try again");
  assert.equal(err.details, undefined);
});

// ---------------------------------------------------------------------------
// classifyError — PieceExecutionError pass-through
// ---------------------------------------------------------------------------

test("classifyError with PieceExecutionError passes through type", () => {
  const err = new PieceExecutionError("permanent", "nope");
  const classified = classifyError(err);
  assert.equal(classified.type, "permanent");
});

test("classifyError with PieceExecutionError passes through message", () => {
  const err = new PieceExecutionError("retryable", "server is down");
  const classified = classifyError(err);
  assert.equal(classified.message, "server is down");
});

test("classifyError with PieceExecutionError passes through details", () => {
  const err = new PieceExecutionError("permanent", "fail", { code: 42 });
  const classified = classifyError(err);
  assert.deepEqual(classified.details, { code: 42 });
});

test("classifyError with retryable PieceExecutionError preserves retryable type", () => {
  const err = new PieceExecutionError("retryable", "try later");
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

// ---------------------------------------------------------------------------
// classifyError — HTTP status codes (5xx = retryable)
// ---------------------------------------------------------------------------

test("classifyError with HTTP 500 is retryable", () => {
  const err = { message: "Internal Server Error", response: { status: 500 } };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

test("classifyError with HTTP 502 is retryable", () => {
  const err = { message: "Bad Gateway", response: { status: 502 } };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

test("classifyError with HTTP 503 is retryable", () => {
  const err = { message: "Service Unavailable", response: { status: 503, data: { reason: "overloaded" } } };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
  assert.deepEqual(classified.details, { status: 503, body: { reason: "overloaded" } });
});

// ---------------------------------------------------------------------------
// classifyError — HTTP status codes (4xx = permanent)
// ---------------------------------------------------------------------------

test("classifyError with HTTP 400 is permanent", () => {
  const err = { message: "Bad Request", response: { status: 400 } };
  const classified = classifyError(err);
  assert.equal(classified.type, "permanent");
});

test("classifyError with HTTP 401 is permanent", () => {
  const err = { message: "Unauthorized", response: { status: 401 } };
  const classified = classifyError(err);
  assert.equal(classified.type, "permanent");
});

test("classifyError with HTTP 403 is permanent", () => {
  const err = { message: "Forbidden", response: { status: 403 } };
  const classified = classifyError(err);
  assert.equal(classified.type, "permanent");
});

test("classifyError with HTTP 404 is permanent", () => {
  const err = { message: "Not Found", response: { status: 404 } };
  const classified = classifyError(err);
  assert.equal(classified.type, "permanent");
});

test("classifyError with HTTP 422 is permanent", () => {
  const err = { message: "Unprocessable Entity", response: { status: 422, data: { field: "name" } } };
  const classified = classifyError(err);
  assert.equal(classified.type, "permanent");
  assert.deepEqual(classified.details, { status: 422, body: { field: "name" } });
});

test("classifyError with HTTP 200 (non-error status) falls through to retryable", () => {
  // A status below 400 won't match the 5xx or 4xx branches, so it falls to
  // the default unknown handler which returns retryable.
  const err = { message: "weird", response: { status: 200 } };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

// ---------------------------------------------------------------------------
// classifyError — HTTP status on top-level .status (not nested .response)
// ---------------------------------------------------------------------------

test("classifyError with top-level status 500 is retryable", () => {
  const err = { message: "fail", status: 500 };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

test("classifyError with top-level status 429 is permanent", () => {
  const err = { message: "rate limited", status: 429 };
  const classified = classifyError(err);
  assert.equal(classified.type, "permanent");
});

// ---------------------------------------------------------------------------
// classifyError — network error codes (retryable)
// ---------------------------------------------------------------------------

test("classifyError with ECONNRESET is retryable", () => {
  const err = { message: "socket hang up", code: "ECONNRESET" };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
  assert.deepEqual(classified.details, { code: "ECONNRESET" });
});

test("classifyError with ECONNREFUSED is retryable", () => {
  const err = { message: "connect refused", code: "ECONNREFUSED" };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

test("classifyError with ETIMEDOUT is retryable", () => {
  const err = { message: "timed out", code: "ETIMEDOUT" };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

test("classifyError with EAI_AGAIN is retryable", () => {
  const err = { message: "dns lookup failed", code: "EAI_AGAIN" };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

test("classifyError with ENOTFOUND is retryable", () => {
  const err = { message: "dns not found", code: "ENOTFOUND" };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

test("classifyError with EPIPE is retryable", () => {
  const err = { message: "broken pipe", code: "EPIPE" };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

test("classifyError with UND_ERR_SOCKET is retryable", () => {
  const err = { message: "undici socket error", code: "UND_ERR_SOCKET" };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

test("classifyError with UND_ERR_CONNECT_TIMEOUT is retryable", () => {
  const err = { message: "connect timeout", code: "UND_ERR_CONNECT_TIMEOUT" };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

test("classifyError with UND_ERR_HEADERS_TIMEOUT is retryable", () => {
  const err = { message: "headers timeout", code: "UND_ERR_HEADERS_TIMEOUT" };
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
});

// ---------------------------------------------------------------------------
// classifyError — JS built-in error types (permanent)
// ---------------------------------------------------------------------------

test("classifyError with TypeError is permanent", () => {
  const err = new TypeError("Cannot read properties of undefined");
  const classified = classifyError(err);
  assert.equal(classified.type, "permanent");
  assert.deepEqual(classified.details, { name: "TypeError" });
});

test("classifyError with SyntaxError is permanent", () => {
  const err = new SyntaxError("Unexpected token }");
  const classified = classifyError(err);
  assert.equal(classified.type, "permanent");
  assert.deepEqual(classified.details, { name: "SyntaxError" });
});

test("classifyError with ReferenceError is permanent", () => {
  const err = new ReferenceError("x is not defined");
  const classified = classifyError(err);
  assert.equal(classified.type, "permanent");
  assert.deepEqual(classified.details, { name: "ReferenceError" });
});

// ---------------------------------------------------------------------------
// classifyError — unknown / fallback (retryable, fail-open)
// ---------------------------------------------------------------------------

test("classifyError with generic Error is retryable (fail-open)", () => {
  const err = new Error("something unexpected");
  const classified = classifyError(err);
  assert.equal(classified.type, "retryable");
  assert.equal(classified.message, "something unexpected");
});

test("classifyError with string is retryable", () => {
  const classified = classifyError("some string error");
  assert.equal(classified.type, "retryable");
  assert.equal(classified.message, "some string error");
});

test("classifyError with null is retryable", () => {
  const classified = classifyError(null);
  assert.equal(classified.type, "retryable");
  assert.equal(classified.message, "null");
});

test("classifyError with undefined is retryable", () => {
  const classified = classifyError(undefined);
  assert.equal(classified.type, "retryable");
  assert.equal(classified.message, "undefined");
});

test("classifyError with number is retryable", () => {
  const classified = classifyError(42);
  assert.equal(classified.type, "retryable");
  assert.equal(classified.message, "42");
});
