/**
 * E2E: Template Debugger (Feature #25)
 *
 * Validates the POST /debug/template endpoint that resolves a raw
 * template string against supplied context/outputs fixtures and
 * returns the resolved value with per-expression provenance trace.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Template Debugger", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // Basic resolution
  // ------------------------------------------------------------------

  it("resolves a simple context.data reference", async () => {
    const res = await client.debugTemplate({
      template: "{{ context.data.name }}",
      context_data: { name: "Alice" },
    });
    assert.equal(res.result, "Alice");
    assert.equal(res.result_type, "string");
    assert.equal(res.error, undefined);
    assert.ok(Array.isArray(res.entries));
    assert.equal(res.entries.length, 1);
    assert.equal(res.entries[0].expression, "context.data.name");
    assert.equal(res.entries[0].status, "ok");
    assert.equal(res.entries[0].source, "context.data.name");
  });

  it("resolves a numeric value preserving type", async () => {
    const res = await client.debugTemplate({
      template: "{{ outputs.calc.total }}",
      outputs: { calc: { total: 99 } },
    });
    assert.equal(res.result, 99);
    assert.equal(res.result_type, "number");
  });

  it("resolves an object value preserving type", async () => {
    const res = await client.debugTemplate({
      template: "{{ context.data.user }}",
      context_data: { user: { id: 1, role: "admin" } },
    });
    assert.deepStrictEqual(res.result, { id: 1, role: "admin" });
    assert.equal(res.result_type, "object");
  });

  it("resolves inline interpolation to string", async () => {
    const res = await client.debugTemplate({
      template: "Hello {{ context.data.name }}, welcome!",
      context_data: { name: "Bob" },
    });
    assert.equal(res.result, "Hello Bob, welcome!");
    assert.equal(res.result_type, "string");
  });

  // ------------------------------------------------------------------
  // Provenance and fallback
  // ------------------------------------------------------------------

  it("reports missing path status", async () => {
    const res = await client.debugTemplate({
      template: "{{ context.data.nope }}",
      context_data: {},
    });
    assert.equal(res.entries[0].status, "missing");
  });

  it("reports null status for explicit null", async () => {
    const res = await client.debugTemplate({
      template: "{{ context.data.val }}",
      context_data: { val: null },
    });
    assert.equal(res.entries[0].status, "null");
  });

  it("traces fallback chain", async () => {
    const res = await client.debugTemplate({
      template: "{{ context.data.primary | context.data.secondary }}",
      context_data: { secondary: "backup" },
    });
    assert.equal(res.result, "backup");
    const e = res.entries[0];
    assert.equal(e.fallback_used, true);
    assert.equal(e.source, "context.data.secondary");
  });

  it("traces literal fallback", async () => {
    const res = await client.debugTemplate({
      template: "{{ context.data.absent | fallback-value }}",
      context_data: {},
    });
    assert.equal(res.result, "fallback-value");
    assert.equal(res.entries[0].source, "literal");
    assert.equal(res.entries[0].fallback_used, true);
  });

  // ------------------------------------------------------------------
  // Pipe filters
  // ------------------------------------------------------------------

  it("applies pipe filter", async () => {
    const res = await client.debugTemplate({
      template: "{{ context.data.name | upper }}",
      context_data: { name: "alice" },
    });
    assert.equal(res.result, "ALICE");
  });

  // ------------------------------------------------------------------
  // Context config
  // ------------------------------------------------------------------

  it("resolves context.config references", async () => {
    const res = await client.debugTemplate({
      template: "{{ context.config.region }}",
      context_config: { region: "eu-west" },
    });
    assert.equal(res.result, "eu-west");
  });

  // ------------------------------------------------------------------
  // Error cases
  // ------------------------------------------------------------------

  it("returns error for unknown root", async () => {
    const res = await client.debugTemplate({
      template: "{{ bogus.path }}",
    });
    assert.ok(res.error, "expected an error");
    assert.equal(res.result, undefined);
  });

  // ------------------------------------------------------------------
  // Plain text (no template)
  // ------------------------------------------------------------------

  it("returns literal text when no templates present", async () => {
    const res = await client.debugTemplate({
      template: "plain text",
    });
    assert.equal(res.result, "plain text");
    assert.equal(res.entries.length, 0);
  });

  // ------------------------------------------------------------------
  // Redaction
  // ------------------------------------------------------------------

  it("redacts secret-shaped values", async () => {
    const res = await client.debugTemplate({
      template: "{{ context.data.tok }}",
      context_data: { tok: "sk_live_abc123" },
    });
    assert.equal(res.entries[0].status, "redacted");
    assert.equal(res.result, "[REDACTED]");
  });
});
