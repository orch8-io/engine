/**
 * Verifies that concurrent version creation for the same
 * (tenant, namespace, name) is safe: both distinct versions land, the list
 * is consistent, and the latest wins when resolving by name without an
 * explicit version. Also exercises migrateInstance to move a running
 * instance from an old version to the new one.
 *
 * Note: the engine uses DB-level uniqueness on (tenant, namespace, name,
 * version). Two parallel creates with the *same* version would cause one
 * to 409 — this test avoids that race by assigning distinct version
 * numbers upfront and just exercises the simultaneous insert path.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { SequenceDef, Block } from "../client.ts";

const client = new Orch8Client();

function versionedSequence(
  tenantId: string,
  namespace: string,
  name: string,
  version: number,
  blocks: Block[],
): SequenceDef {
  return {
    id: uuid(),
    tenant_id: tenantId,
    namespace,
    name,
    version,
    blocks,
    created_at: new Date().toISOString(),
  };
}

describe("Concurrent Sequence Version Updates", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("lands both versions when created concurrently and migrates across them", async () => {
    const tenantId = `test-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const name = `conv-${uuid().slice(0, 8)}`;

    // v1 is the long-running version; v2 is the short-circuit version we
    // migrate onto mid-run.
    const v1 = versionedSequence(tenantId, namespace, name, 1, [
      step("slow", "sleep", { duration_ms: 5000 }),
    ]);
    const v2 = versionedSequence(tenantId, namespace, name, 2, [
      step("fast", "noop"),
    ]);

    // Fire both creates concurrently.
    const [r1, r2] = await Promise.all([
      client.createSequence(v1),
      client.createSequence(v2),
    ]);
    assert.ok(r1, "v1 create should resolve");
    assert.ok(r2, "v2 create should resolve");

    // Both versions must be listed in order.
    const versions = await client.listSequenceVersions(tenantId, namespace, name);
    const vs = versions.map((v) => v.version).sort((a, b) => a - b);
    assert.deepEqual(vs, [1, 2], "both versions should be listed");

    // Without a version, getSequenceByName returns the latest (v2).
    const latest = await client.getSequenceByName(tenantId, namespace, name);
    assert.equal(latest.version, 2, "latest should be v2");
    assert.equal(latest.id, v2.id);

    // Explicit version=1 still resolves to v1.
    const pinned = await client.getSequenceByName(tenantId, namespace, name, 1);
    assert.equal(pinned.version, 1);
    assert.equal(pinned.id, v1.id);

    // Create an instance against v1; let it start running, then migrate to v2.
    const { id } = await client.createInstance({
      sequence_id: v1.id,
      tenant_id: tenantId,
      namespace,
    });

    // Give the scheduler a moment to pick it up (per sequence-versioning.test.ts).
    await new Promise((r) => setTimeout(r, 300));

    const res = await client.migrateInstance(id, v2.id);
    assert.equal((res as { migrated?: boolean }).migrated, true);

    const after = await client.getInstance(id);
    assert.equal(after.sequence_id, v2.id, "instance must now reference v2");
  });
});
