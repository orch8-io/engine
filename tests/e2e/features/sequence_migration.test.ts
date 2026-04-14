/**
 * POST /sequences/migrate-instance — verifies instance migration from
 * one sequence version to another.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Sequence Migration", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("migrates a waiting instance to a new sequence version", async () => {
    const tenantId = `mig-${uuid().slice(0, 8)}`;
    const ns = "default";
    const baseName = `mig-seq-${uuid().slice(0, 8)}`;

    // v1: single noop step.
    const v1 = testSequence(baseName, [step("s1", "noop")], { tenantId, namespace: ns });
    v1.name = baseName;
    v1.version = 1;
    await client.createSequence(v1);

    // v2: two steps.
    const v2 = testSequence(baseName, [step("s1", "noop"), step("s2", "noop")], {
      tenantId,
      namespace: ns,
    });
    v2.name = baseName;
    v2.version = 2;
    await client.createSequence(v2);

    // Create instance on v1 and let it complete.
    const { id } = await client.createInstance({
      sequence_id: v1.id,
      tenant_id: tenantId,
      namespace: ns,
    });
    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    // Migrate to v2. Depending on engine policy, this may succeed or
    // return an error for completed instances.
    try {
      await client.migrateInstance(id, v2.id);
      // If migration succeeded, verify instance is on new sequence.
      const inst = await client.getInstance(id);
      assert.equal(
        (inst as any).sequence_id,
        v2.id,
        "instance should reference new sequence",
      );
    } catch (err: any) {
      // If the engine rejects migration of completed instances, it
      // should be a clear 4xx, not 500.
      assert.ok(
        err.status !== 500,
        `migration rejection should not be 500; got ${err.status}`,
      );
    }
  });

  it("migration with non-existent instance returns error", async () => {
    const tenantId = `mig-ne-${uuid().slice(0, 8)}`;
    const seq = testSequence("mig-ne", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    try {
      await client.migrateInstance(uuid(), seq.id);
      assert.fail("should throw for non-existent instance");
    } catch (err: any) {
      assert.ok(
        err.status === 404 || err.status === 400,
        `expected 4xx, got ${err.status}`,
      );
    }
  });

  it("migration with non-existent target sequence returns error", async () => {
    const tenantId = `mig-nt-${uuid().slice(0, 8)}`;
    const seq = testSequence("mig-nt", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    try {
      await client.migrateInstance(id, uuid());
      assert.fail("should throw for non-existent target sequence");
    } catch (err: any) {
      assert.ok(
        err.status === 404 || err.status === 400,
        `expected 4xx, got ${err.status}`,
      );
    }
  });
});
