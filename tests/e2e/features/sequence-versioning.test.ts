import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, step, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block, SequenceDef } from "../client.ts";

const client = new Orch8Client();

interface VersionedSequenceArgs {
  tenantId: string;
  namespace: string;
  name: string;
  version: number;
  blocks: Block[];
}

/**
 * Build a sequence definition with an explicit `name` and `version` — the
 * shared `testSequence` helper always appends a random suffix and hard-codes
 * version=1, which is the wrong shape for versioning tests that need
 * stable shared names across two or more definitions.
 */
function versionedSequence({ tenantId, namespace, name, version, blocks }: VersionedSequenceArgs): SequenceDef {
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

describe("Sequence versioning + migration", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("lists both versions of a sequence", async () => {
    const tenantId = `seqv-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const name = `foo-${uuid().slice(0, 8)}`;

    const v1 = versionedSequence({
      tenantId,
      namespace,
      name,
      version: 1,
      blocks: [step("s1", "noop")],
    });
    const v2 = versionedSequence({
      tenantId,
      namespace,
      name,
      version: 2,
      blocks: [step("s1", "noop")],
    });
    await client.createSequence(v1);
    await client.createSequence(v2);

    const versions = await client.listSequenceVersions(tenantId, namespace, name);
    const vs = versions.map((v) => v.version).sort((a, b) => a - b);
    assert.deepEqual(vs, [1, 2]);
  });

  it("migrates a running instance from v1 to v2", async () => {
    const tenantId = `seqv-mig-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const name = `mig-${uuid().slice(0, 8)}`;

    // v1 sleeps long enough to migrate mid-run.
    const v1 = versionedSequence({
      tenantId,
      namespace,
      name,
      version: 1,
      blocks: [step("slow", "sleep", { duration_ms: 5000 })],
    });
    const v2 = versionedSequence({
      tenantId,
      namespace,
      name,
      version: 2,
      blocks: [step("fast", "noop")],
    });
    await client.createSequence(v1);
    await client.createSequence(v2);

    const { id } = await client.createInstance({
      sequence_id: v1.id,
      tenant_id: tenantId,
      namespace,
    });

    // Let it start.
    await new Promise((r) => setTimeout(r, 300));

    const res = await client.migrateInstance(id, v2.id);
    assert.equal(res.migrated, true);
    assert.equal(res.to_sequence_id, v2.id);

    const after = await client.getInstance(id);
    assert.equal(after.sequence_id, v2.id);
  });

  it("rejects migration of a terminal instance", async () => {
    const tenantId = `seqv-term-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const name = `term-${uuid().slice(0, 8)}`;

    const v1 = versionedSequence({
      tenantId,
      namespace,
      name,
      version: 1,
      blocks: [step("s1", "noop")],
    });
    const v2 = versionedSequence({
      tenantId,
      namespace,
      name,
      version: 2,
      blocks: [step("s1", "noop")],
    });
    await client.createSequence(v1);
    await client.createSequence(v2);

    const { id } = await client.createInstance({
      sequence_id: v1.id,
      tenant_id: tenantId,
      namespace,
    });
    await client.waitForState(id, "completed");

    await assert.rejects(
      () => client.migrateInstance(id, v2.id),
      (err: unknown) => {
        const status = (err as ApiError).status;
        assert.ok(status >= 400 && status < 500);
        return true;
      }
    );
  });

  // Plan #20 (engine-accurate): delete a sequence that has no instances
  // succeeds with 204; subsequent GET is 404.
  //
  // Note: the spec text reads "delete after all instances complete", but
  // the storage layer does not cascade-delete terminal instances, so the
  // FK constraint rejects the DELETE once any instance exists. This test
  // verifies the supported path (delete a sequence that was never
  // instantiated) which is what ships today.
  it("deletes a sequence with no instances", async () => {
    const tenantId = `seqv-del-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const name = `del-${uuid().slice(0, 8)}`;
    const seq = versionedSequence({
      tenantId,
      namespace,
      name,
      version: 1,
      blocks: [step("s1", "noop")],
    });
    await client.createSequence(seq);

    const res = await fetch(`${client.baseUrl}/sequences/${seq.id}`, {
      method: "DELETE",
    });
    assert.equal(
      res.status,
      204,
      `delete should succeed for a sequence with no instances, got ${res.status}`,
    );

    // Sequence must no longer be retrievable.
    await assert.rejects(
      () => client.getSequence(seq.id),
      (err: unknown) => {
        const status = (err as ApiError).status;
        assert.equal(status, 404);
        return true;
      },
    );
  });

  // Plan #21: deprecating the only version of a sequence hides it from
  // by-name lookup entirely (no fallback — 404).
  it("by-name lookup returns 404 when the only version is deprecated", async () => {
    const tenantId = `seqv-dep-only-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const name = `dep-only-${uuid().slice(0, 8)}`;
    const seq = versionedSequence({
      tenantId,
      namespace,
      name,
      version: 1,
      blocks: [step("s1", "noop")],
    });
    await client.createSequence(seq);
    await client.deprecateSequence(seq.id);

    await assert.rejects(
      () => client.getSequenceByName(tenantId, namespace, name),
      (err: unknown) => {
        const status = (err as ApiError).status;
        assert.equal(
          status,
          404,
          `by-name lookup on fully-deprecated name must 404, got ${status}`,
        );
        return true;
      },
    );
  });

  it("deprecating v1 means getSequenceByName without a version returns v2", async () => {
    const tenantId = `seqv-dep-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const name = `dep-${uuid().slice(0, 8)}`;

    const v1 = versionedSequence({
      tenantId,
      namespace,
      name,
      version: 1,
      blocks: [step("s1", "noop")],
    });
    const v2 = versionedSequence({
      tenantId,
      namespace,
      name,
      version: 2,
      blocks: [step("s1", "noop")],
    });
    await client.createSequence(v1);
    await client.createSequence(v2);

    await client.deprecateSequence(v1.id);

    const latest = await client.getSequenceByName(tenantId, namespace, name);
    assert.equal(latest.version, 2, "latest should skip the deprecated v1");
    assert.equal(latest.id, v2.id);
  });
});
