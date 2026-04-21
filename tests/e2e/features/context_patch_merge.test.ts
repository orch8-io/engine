/**
 * Context Patch Merge — verifies PATCH /instances/{id}/context behavior
 * including merges, null handling, and state restrictions.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Context Patch Merge", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("patch context replaces nested objects", async () => {
    const tenantId = `ctx-merge-${uuid().slice(0, 8)}`;
    const seq = testSequence("ctx-merge", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { existing: true, nested: { a: 1 } } },
    });

    await client.updateContext(id, { data: { nested: { b: 2 } } });

    const inst = await client.getInstance(id);
    const data = (inst.context as any)?.data;
    // API performs full replacement, not merge.
    assert.equal(data.existing, undefined, "existing top-level field is replaced");
    assert.equal(data.nested.b, 2, "new nested field should be present");
  });

  it("patch context replaces entire data object", async () => {
    const tenantId = `ctx-null-${uuid().slice(0, 8)}`;
    const seq = testSequence("ctx-null", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { removable: "value", keep: "this" } },
    });

    await client.updateContext(id, { data: { removable: null } });

    const inst = await client.getInstance(id);
    const data = (inst.context as any)?.data;
    // Full replacement: only the new keys exist.
    assert.equal(data.keep, undefined, "old fields are replaced");
    assert.equal(data.removable, null, "new field with null is stored");
  });

  it("patch context on running instance is accepted", async () => {
    const tenantId = `ctx-run-${uuid().slice(0, 8)}`;
    const seq = testSequence("ctx-run", [step("s1", "sleep", { duration_ms: 3000 })], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "running", { timeoutMs: 5_000 });

    // Should not throw.
    await client.updateContext(id, { data: { injected: "while-running" } });

    const inst = await client.getInstance(id);
    const data = (inst.context as any)?.data;
    assert.equal(data.injected, "while-running");
  });

  it("patch context on completed instance is accepted", async () => {
    const tenantId = `ctx-done-${uuid().slice(0, 8)}`;
    const seq = testSequence("ctx-done", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    // Should not throw — context can be updated post-completion for audit.
    await client.updateContext(id, { data: { post_complete: true } });

    const inst = await client.getInstance(id);
    const data = (inst.context as any)?.data;
    assert.equal(data.post_complete, true);
  });

  it("full context replacement via patch", async () => {
    const tenantId = `ctx-full-${uuid().slice(0, 8)}`;
    const seq = testSequence("ctx-full", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { old: "data" } },
    });

    await client.updateContext(id, { data: { completely: "new" } });

    const inst = await client.getInstance(id);
    const data = (inst.context as any)?.data;
    assert.equal(data.completely, "new");
  });

  it("patch context replaces all existing fields", async () => {
    const tenantId = `ctx-pres-${uuid().slice(0, 8)}`;
    const seq = testSequence("ctx-pres", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { a: 1, b: 2, c: 3 } },
    });

    await client.updateContext(id, { data: { b: 99 } });

    const inst = await client.getInstance(id);
    const data = (inst.context as any)?.data;
    // API performs full replacement.
    assert.equal(data.a, undefined, "a is replaced");
    assert.equal(data.b, 99, "b is updated");
    assert.equal(data.c, undefined, "c is replaced");
  });
});
