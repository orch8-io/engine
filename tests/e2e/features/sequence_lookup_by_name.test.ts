/**
 * GET /sequences/by-name — verifies name+namespace lookup, explicit
 * version selection, and 404 for non-existent sequences.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Sequence Lookup by Name", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("finds sequence by tenant+namespace+name", async () => {
    const tenantId = `sbn-${uuid().slice(0, 8)}`;
    const ns = "default";
    const seq = testSequence("sbn-find", [step("s", "noop")], {
      tenantId,
      namespace: ns,
    });
    await client.createSequence(seq);

    const found = await client.getSequenceByName(tenantId, ns, seq.name);
    assert.equal(found.id, seq.id, "should find the created sequence");
    assert.equal(found.name, seq.name);
  });

  it("returns 404 for non-existent name", async () => {
    try {
      await client.getSequenceByName("no-tenant", "default", `nonexistent-${uuid()}`);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("explicit version=1 matches first version", async () => {
    const tenantId = `sbn-v-${uuid().slice(0, 8)}`;
    const ns = "default";
    const seq = testSequence("sbn-ver", [step("s", "noop")], {
      tenantId,
      namespace: ns,
    });
    await client.createSequence(seq);

    const found = await client.getSequenceByName(tenantId, ns, seq.name, 1);
    assert.equal(found.id, seq.id);
    assert.equal(found.version, 1);
  });

  it("non-existent version returns 404", async () => {
    const tenantId = `sbn-nv-${uuid().slice(0, 8)}`;
    const ns = "default";
    const seq = testSequence("sbn-nover", [step("s", "noop")], {
      tenantId,
      namespace: ns,
    });
    await client.createSequence(seq);

    try {
      await client.getSequenceByName(tenantId, ns, seq.name, 999);
      assert.fail("should throw 404 for non-existent version");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });
});
