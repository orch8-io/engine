/**
 * Instance Priority & Metadata — verifies that priority and metadata
 * round-trip on create and are visible in list/get responses.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Instance Priority & Metadata", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("create instance with custom priority and metadata round-trips", async () => {
    const tenantId = `pri-meta-${uuid().slice(0, 8)}`;
    const seq = testSequence("pri-meta", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      priority: "High",
      metadata: { source: "e2e-test", batch: 42 },
    });

    const inst = await client.getInstance(id);
    assert.equal(inst.priority, "High", "priority should round-trip");
    assert.deepEqual(
      inst.metadata,
      { source: "e2e-test", batch: 42 },
      "metadata should round-trip",
    );
  });

  it("list instances includes priority and metadata fields", async () => {
    const tenantId = `pri-list-${uuid().slice(0, 8)}`;
    const seq = testSequence("pri-list", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      priority: "Low",
      metadata: { tag: "listable" },
    });

    // Small delay for visibility.
    await new Promise((r) => setTimeout(r, 150));

    const list = await client.listInstances({ tenant_id: tenantId });
    const ours = list.find((i) => i.id === id);
    assert.ok(ours, "created instance should appear in listing");
    assert.equal(ours!.priority, "Low", "listed instance should carry priority");
    assert.deepEqual(
      (ours as any).metadata,
      { tag: "listable" },
      "listed instance should carry metadata",
    );
  });

  it("default priority is 0 when omitted", async () => {
    const tenantId = `pri-def-${uuid().slice(0, 8)}`;
    const seq = testSequence("pri-def", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    const inst = await client.getInstance(id);
    assert.equal(inst.priority, "Normal", "default priority should be Normal");
  });
});
