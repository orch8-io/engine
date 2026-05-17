// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.
//
// Unit tests for the ActionContext builder: auth/props pass-through,
// in-memory store, file encoding, connection stubs, server URLs, and
// execution metadata.

import { test } from "node:test";
import * as assert from "node:assert/strict";
import { buildActionContext } from "../src/context.ts";

// ---------------------------------------------------------------------------
// Helper — build a context with sensible defaults
// ---------------------------------------------------------------------------

function ctx(overrides: Partial<Parameters<typeof buildActionContext>[0]> = {}) {
  return buildActionContext({
    auth: overrides.auth ?? { token: "test-token" },
    propsValue: overrides.propsValue ?? { key: "value" },
    instanceId: overrides.instanceId ?? "inst-123",
    blockId: overrides.blockId ?? "step-7",
    ...overrides,
  }) as any;
}

// ---------------------------------------------------------------------------
// auth / propsValue pass-through
// ---------------------------------------------------------------------------

test("auth passes through from input", () => {
  const c = ctx({ auth: { access_token: "abc", refresh_token: "xyz" } });
  assert.deepEqual(c.auth, { access_token: "abc", refresh_token: "xyz" });
});

test("auth passes through null", () => {
  const c = ctx({ auth: null });
  assert.equal(c.auth, null);
});

test("propsValue passes through from input", () => {
  const c = ctx({ propsValue: { channel: "C1", text: "hello" } });
  assert.deepEqual(c.propsValue, { channel: "C1", text: "hello" });
});

// ---------------------------------------------------------------------------
// store — in-memory KV
// ---------------------------------------------------------------------------

test("store.put stores value and returns it", async () => {
  const c = ctx();
  const result = await c.store.put("counter", 42);
  assert.equal(result, 42);
});

test("store.get retrieves stored value", async () => {
  const c = ctx();
  await c.store.put("name", "orch8");
  const val = await c.store.get("name");
  assert.equal(val, "orch8");
});

test("store.get returns null for missing key", async () => {
  const c = ctx();
  const val = await c.store.get("nonexistent");
  assert.equal(val, null);
});

test("store.delete removes value", async () => {
  const c = ctx();
  await c.store.put("temp", "data");
  await c.store.delete("temp");
  const val = await c.store.get("temp");
  assert.equal(val, null);
});

test("store.get after delete returns null", async () => {
  const c = ctx();
  await c.store.put("x", 1);
  assert.equal(await c.store.get("x"), 1);
  await c.store.delete("x");
  assert.equal(await c.store.get("x"), null);
});

test("store is isolated per context invocation", async () => {
  const c1 = ctx();
  const c2 = ctx();
  await c1.store.put("key", "from-c1");
  assert.equal(await c2.store.get("key"), null);
});

// ---------------------------------------------------------------------------
// files — data URL encoding
// ---------------------------------------------------------------------------

test("files.write returns data URL", async () => {
  const c = ctx();
  const url = await c.files.write({ fileName: "report.csv", data: Buffer.from("a,b,c") });
  assert.ok(url.startsWith("data:application/octet-stream;"));
  assert.ok(url.includes("base64,"));
});

test("files.write encodes filename in URL", async () => {
  const c = ctx();
  const url = await c.files.write({ fileName: "my file (1).txt", data: Buffer.from("hi") });
  assert.ok(url.includes(encodeURIComponent("my file (1).txt")));
});

test("files.write encodes binary data as base64", async () => {
  const c = ctx();
  const data = Buffer.from([0xff, 0xd8, 0xff, 0xe0]); // JPEG magic bytes
  const url = await c.files.write({ fileName: "img.jpg", data });
  const b64Part = url.split("base64,")[1];
  assert.equal(b64Part, data.toString("base64"));
});

// ---------------------------------------------------------------------------
// connections
// ---------------------------------------------------------------------------

test("connections.get always returns null", async () => {
  const c = ctx();
  const result = await c.connections.get("slack-oauth");
  assert.equal(result, null);
});

test("connections.get returns null for any name", async () => {
  const c = ctx();
  assert.equal(await c.connections.get("google"), null);
  assert.equal(await c.connections.get(""), null);
});

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

test("run.id matches instanceId", () => {
  const c = ctx({ instanceId: "run-abc-def" });
  assert.equal(c.run.id, "run-abc-def");
});

test("run.stop is a function (no-op)", () => {
  const c = ctx();
  assert.equal(typeof c.run.stop, "function");
  // Should not throw
  c.run.stop();
  c.run.stop({ reason: "done" });
});

test("run.pause is a function (no-op)", () => {
  const c = ctx();
  assert.equal(typeof c.run.pause, "function");
  // Should not throw
  c.run.pause();
  c.run.pause({ resumePayload: {} });
});

test("run.isTest is false", () => {
  const c = ctx();
  assert.equal(c.run.isTest, false);
});

// ---------------------------------------------------------------------------
// server URLs
// ---------------------------------------------------------------------------

test("server.apiUrl defaults to http://localhost:8080", () => {
  const c = ctx();
  assert.equal(c.server.apiUrl, "http://localhost:8080");
});

test("server.apiUrl uses serverApiUrl override", () => {
  const c = ctx({ serverApiUrl: "https://api.orch8.io" });
  assert.equal(c.server.apiUrl, "https://api.orch8.io");
});

test("server.publicUrl defaults to apiUrl", () => {
  const c = ctx();
  assert.equal(c.server.publicUrl, "http://localhost:8080");
});

test("server.publicUrl defaults to serverApiUrl when only apiUrl is set", () => {
  const c = ctx({ serverApiUrl: "https://internal.orch8.io" });
  assert.equal(c.server.publicUrl, "https://internal.orch8.io");
});

test("server.publicUrl uses serverPublicUrl override", () => {
  const c = ctx({ serverPublicUrl: "https://public.orch8.io" });
  assert.equal(c.server.publicUrl, "https://public.orch8.io");
});

test("server.publicUrl uses serverPublicUrl even when serverApiUrl is also set", () => {
  const c = ctx({
    serverApiUrl: "https://internal.orch8.io",
    serverPublicUrl: "https://public.orch8.io",
  });
  assert.equal(c.server.apiUrl, "https://internal.orch8.io");
  assert.equal(c.server.publicUrl, "https://public.orch8.io");
});

test("server.token is empty string", () => {
  const c = ctx();
  assert.equal(c.server.token, "");
});

// ---------------------------------------------------------------------------
// step / executionType / misc
// ---------------------------------------------------------------------------

test("step.name matches blockId", () => {
  const c = ctx({ blockId: "email-step" });
  assert.equal(c.step.name, "email-step");
});

test("executionType is 0", () => {
  const c = ctx();
  assert.equal(c.executionType, 0);
});

test("generateResumeUrl returns empty string", () => {
  const c = ctx();
  assert.equal(c.generateResumeUrl({}), "");
  assert.equal(c.generateResumeUrl({ queryParams: { x: "1" } }), "");
});

test("project.id is 'orch8'", () => {
  const c = ctx();
  assert.equal(c.project.id, "orch8");
});

test("tags.add is a no-op async function", async () => {
  const c = ctx();
  // Should not throw
  await c.tags.add("important");
});

test("output.update is a no-op async function", async () => {
  const c = ctx();
  // Should not throw
  await c.output.update({ progress: 50 });
});

test("flows.list returns empty array", async () => {
  const c = ctx();
  const result = await c.flows.list();
  assert.deepEqual(result, []);
});

test("flows.current.id matches instanceId", () => {
  const c = ctx({ instanceId: "flow-run-1" });
  assert.equal(c.flows.current.id, "flow-run-1");
});
