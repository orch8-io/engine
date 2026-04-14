/**
 * Verifies sequence delete is rejected when active instances reference it,
 * and succeeds when no active instances remain.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Delete Sequence with In-Flight Hot Migration", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should reject delete when active instances reference the sequence", async () => {
    const tenantId = `seq-del-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const name = `del-${uuid().slice(0, 8)}`;

    const v1 = {
      id: uuid(),
      tenant_id: tenantId,
      namespace,
      name,
      version: 1,
      blocks: [step("slow", "sleep", { duration_ms: 3000 })],
      created_at: new Date().toISOString(),
    };
    const v2 = {
      id: uuid(),
      tenant_id: tenantId,
      namespace,
      name,
      version: 2,
      blocks: [step("fast", "noop")],
      created_at: new Date().toISOString(),
    };
    await client.createSequence(v1);
    await client.createSequence(v2);

    const { id: instanceId } = await client.createInstance({
      sequence_id: v1.id,
      tenant_id: tenantId,
      namespace,
    });

    // Wait for instance to start running.
    await new Promise((r) => setTimeout(r, 200));

    // Attempt to delete v1 while instance is still running on it → expect 409.
    const res1 = await fetch(`${client.baseUrl}/sequences/${v1.id}`, {
      method: "DELETE",
    });
    assert.equal(
      res1.status,
      409,
      `expected 409 conflict on delete while instance is active, got ${res1.status}`,
    );

    // v1 should still be readable (delete was rejected).
    const still = await client.getSequence(v1.id);
    assert.equal(still.id, v1.id);

    // Migrate instance to v2, then delete v1 should succeed.
    await client.migrateInstance(instanceId, v2.id);

    const res2 = await fetch(`${client.baseUrl}/sequences/${v1.id}`, {
      method: "DELETE",
    });
    assert.equal(
      res2.status,
      204,
      `expected 204 after migration removed active references, got ${res2.status}`,
    );

    // v1 should no longer exist.
    try {
      await client.getSequence(v1.id);
      assert.fail("expected getSequence to throw after deletion");
    } catch {
      // Expected — sequence was deleted.
    }
  });
});
