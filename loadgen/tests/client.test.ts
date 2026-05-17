import { test } from "node:test";
import * as assert from "node:assert/strict";
import { ApiError, Orch8Client } from "../src/client.ts";

// ── 1-6: ApiError ────────────────────────────────────────────────────

test("1 - ApiError is instance of Error", () => {
  const err = new ApiError(404, "not found", "/foo");
  assert.ok(err instanceof Error);
});

test("2 - ApiError.status stores status code", () => {
  const err = new ApiError(503, "unavailable", "/health");
  assert.equal(err.status, 503);
});

test("3 - ApiError.body stores body text", () => {
  const err = new ApiError(400, '{"msg":"bad"}', "/bar");
  assert.equal(err.body, '{"msg":"bad"}');
});

test("4 - ApiError.path stores path", () => {
  const err = new ApiError(500, "oops", "/api/v1/things");
  assert.equal(err.path, "/api/v1/things");
});

test("5 - ApiError message includes status and path", () => {
  const err = new ApiError(422, "validation", "/submit");
  assert.ok(err.message.includes("422"), `missing status in: ${err.message}`);
  assert.ok(err.message.includes("/submit"), `missing path in: ${err.message}`);
});

test("6 - ApiError message includes body", () => {
  const err = new ApiError(500, "internal server error", "/crash");
  assert.ok(
    err.message.includes("internal server error"),
    `missing body in: ${err.message}`,
  );
});

// ── 7-13: Orch8Client constructor ────────────────────────────────────

test("7 - Orch8Client stores baseUrl", () => {
  const c = new Orch8Client({ baseUrl: "http://localhost:8080" });
  assert.equal(c.baseUrl, "http://localhost:8080");
});

test("8 - Orch8Client strips trailing slash from baseUrl", () => {
  const c = new Orch8Client({ baseUrl: "http://example.com/" });
  assert.equal(c.baseUrl, "http://example.com");
});

test("9 - Orch8Client defaults timeoutMs to 10000", () => {
  const c = new Orch8Client({ baseUrl: "http://x" });
  assert.equal(c.timeoutMs, 10_000);
});

test("10 - Orch8Client respects custom timeoutMs", () => {
  const c = new Orch8Client({ baseUrl: "http://x", timeoutMs: 5000 });
  assert.equal(c.timeoutMs, 5000);
});

test("11 - Orch8Client timeoutMs=0 is stored as 0", () => {
  const c = new Orch8Client({ baseUrl: "http://x", timeoutMs: 0 });
  assert.equal(c.timeoutMs, 0);
});

test("12 - Orch8Client with baseUrl ending in '/' strips it", () => {
  const c = new Orch8Client({ baseUrl: "https://api.example.com/v1/" });
  assert.equal(c.baseUrl, "https://api.example.com/v1");
});

test("13 - Orch8Client with baseUrl not ending in '/' keeps it", () => {
  const c = new Orch8Client({ baseUrl: "https://api.example.com/v1" });
  assert.equal(c.baseUrl, "https://api.example.com/v1");
});

// ── 14-15: ApiError edge cases ───────────────────────────────────────

test("14 - ApiError with empty body works", () => {
  const err = new ApiError(204, "", "/empty");
  assert.equal(err.body, "");
  assert.equal(err.status, 204);
  assert.equal(err.path, "/empty");
  // message still well-formed
  assert.ok(err.message.includes("204"));
  assert.ok(err.message.includes("/empty"));
});

test("15 - ApiError with multiline body works", () => {
  const body = "line1\nline2\nline3";
  const err = new ApiError(500, body, "/multi");
  assert.equal(err.body, body);
  assert.ok(err.message.includes("line1"));
  assert.ok(err.message.includes("line2"));
});
