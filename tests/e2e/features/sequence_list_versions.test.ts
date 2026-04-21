/**
 * Sequence List, Versions, and Deprecate — verifies listing sequences,
 * version history, and deprecation semantics.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Sequence List, Versions and Deprecate", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("list sequences returns created sequences", async () => {
    const tenantId = `sl-${uuid().slice(0, 8)}`;
    const seq = testSequence("listable", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const list = await client.listSequences({ tenant_id: tenantId });
    assert.ok(list.length > 0, "should return at least one sequence");
    assert.ok(
      list.some((s: any) => s.id === seq.id),
      "created sequence should appear in list",
    );
  });

  it("list sequences is tenant-scoped", async () => {
    const tenantA = `sl-a-${uuid().slice(0, 8)}`;
    const tenantB = `sl-b-${uuid().slice(0, 8)}`;

    const seqA = testSequence("sl-ta", [step("s1", "noop")], { tenantId: tenantA });
    const seqB = testSequence("sl-tb", [step("s1", "noop")], { tenantId: tenantB });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    const listA = await client.listSequences({ tenant_id: tenantA });
    assert.ok(listA.some((s: any) => s.id === seqA.id), "A's sequence should be in A's list");
    assert.ok(!listA.some((s: any) => s.id === seqB.id), "B's sequence should NOT be in A's list");
  });

  it("list sequence versions returns all versions", async () => {
    const tenantId = `sv-${uuid().slice(0, 8)}`;
    const name = `versioned-${uuid().slice(0, 8)}`;

    // testSequence randomizes the name, so build sequences manually
    // to share the same name with different versions.
    const v1 = {
      id: uuid(),
      tenant_id: tenantId,
      namespace: "default",
      name,
      version: 1,
      blocks: [step("s1", "noop")],
      created_at: new Date().toISOString(),
    };
    const v2 = {
      id: uuid(),
      tenant_id: tenantId,
      namespace: "default",
      name,
      version: 2,
      blocks: [step("s1", "noop"), step("s2", "noop")],
      created_at: new Date().toISOString(),
    };
    await client.createSequence(v1 as any);
    await client.createSequence(v2 as any);

    const versions = await client.listSequenceVersions(tenantId, "default", name);
    assert.equal(versions.length, 2, "should return both versions");
    assert.ok(versions.some((v: any) => v.version === 1), "should include v1");
    assert.ok(versions.some((v: any) => v.version === 2), "should include v2");
  });

  it("deprecate hides sequence from by-name lookup", async () => {
    const tenantId = `sd-${uuid().slice(0, 8)}`;
    const name = `dep-${uuid().slice(0, 8)}`;

    const seq = testSequence(name, [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.deprecateSequence(seq.id);

    try {
      await client.getSequenceByName(tenantId, "default", name);
      assert.fail("should throw 404 for deprecated sequence");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("get by id still works after deprecation", async () => {
    const tenantId = `sd-id-${uuid().slice(0, 8)}`;
    const seq = testSequence("dep-id", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    await client.deprecateSequence(seq.id);

    const fetched = await client.getSequence(seq.id);
    assert.equal(fetched.id, seq.id, "get by id should still work");
    assert.equal(fetched.deprecated, true, "should be marked deprecated");
  });

  it("deprecate non-existent sequence returns 404", async () => {
    try {
      await client.deprecateSequence("019db000-0000-7000-0000-000000000000");
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });
});
