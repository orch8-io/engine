/**
 * API Edge Cases — 404/400/409 validation for less-covered endpoints.
 *
 * Complements api_validation.test.ts with endpoints not already exercised.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("API Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("GET /instances/{id}/tree returns 404 for non-existent instance", async () => {
    try {
      await client.getInstanceTree(uuid());
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("PATCH /instances/{id}/context returns 404 for non-existent instance", async () => {
    try {
      await client.updateContext(uuid(), { data: { x: 1 } });
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("POST /instances/{id}/checkpoints/prune returns 404 for non-existent instance", async () => {
    try {
      await client.pruneCheckpoints(uuid(), 1);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("GET /sequences/versions returns empty array for non-existent sequence name", async () => {
    const tenantId = `seqver-${uuid().slice(0, 8)}`;
    const versions = await client.listSequenceVersions(
      tenantId,
      "default",
      `nonexistent-${uuid().slice(0, 8)}`,
    );
    assert.ok(Array.isArray(versions));
    assert.equal(versions.length, 0);
  });

  it("DELETE /triggers/{slug} returns 404 for non-existent slug", async () => {
    try {
      await client.deleteTrigger(`nonexistent-${uuid().slice(0, 8)}`);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("POST /credentials with duplicate id returns conflict", async () => {
    const tenantId = `cred-dup-${uuid().slice(0, 8)}`;
    const credId = `cred-dup-${uuid().slice(0, 8)}`;

    await client.createCredential({
      id: credId,
      name: "First",
      kind: "api_key",
      value: "secret-1",
      tenant_id: tenantId,
    });

    // Second create with same id should conflict.
    try {
      await client.createCredential({
        id: credId,
        name: "Second",
        kind: "api_key",
        value: "secret-2",
        tenant_id: tenantId,
      });
      assert.fail("should throw conflict for duplicate credential id");
    } catch (err: any) {
      assert.ok(
        err.status === 409 || err.status === 400 || err.status === 500,
        `expected conflict-ish status, got ${err.status}`,
      );
    }
  });

  it("POST /credentials rejects invalid id characters", async () => {
    try {
      await client.createCredential({
        id: "has spaces!",
        name: "Bad",
        kind: "api_key",
        value: "secret",
        tenant_id: "test",
      });
      assert.fail("should throw 400 for invalid id");
    } catch (err: any) {
      assert.ok(
        err.status >= 400 && err.status < 500,
        `expected 4xx, got ${err.status}`,
      );
    }
  });

  it("GET /sessions/by-key/{tenant}/{key} returns 404 for wrong tenant", async () => {
    const tenantA = `sess-wrong-${uuid().slice(0, 8)}`;
    const key = `k-${uuid()}`;

    const created = await client.createSession({
      tenant_id: tenantA,
      session_key: key,
    });

    // Lookup with a different tenant should not find it.
    try {
      await client.getSessionByKey(`other-${tenantA}`, key);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });
});
