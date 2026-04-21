/**
 * Session Lookup and Instances — verifies session by-key lookup,
 * instance association, and data updates.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Session Lookup and Instances", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("get session by key returns correct session", async () => {
    const tenantId = `sess-key-${uuid().slice(0, 8)}`;
    const key = `my-key-${uuid().slice(0, 6)}`;

    const created = await client.createSession({
      tenant_id: tenantId,
      session_key: key,
      data: { foo: "bar" },
    });

    const fetched = await client.getSessionByKey(tenantId, key);
    assert.equal(fetched.id, created.id, "id should match");
    assert.equal((fetched as any).session_key, key, "key should match");
  });

  it("list session instances returns array", async () => {
    const tenantId = `sess-inst-${uuid().slice(0, 8)}`;
    const seq = testSequence("sess-inst", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const session = await client.createSession({
      tenant_id: tenantId,
      session_key: `inst-${uuid().slice(0, 6)}`,
      data: {},
    });

    // Note: CreateInstanceRequest does not accept session_id via public API,
    // so instances cannot be directly linked to sessions through REST.
    const instances = await client.listSessionInstances(session.id);
    assert.ok(Array.isArray(instances));
  });

  it("update session data replaces fields", async () => {
    const tenantId = `sess-data-${uuid().slice(0, 8)}`;
    const session = await client.createSession({
      tenant_id: tenantId,
      session_key: `data-${uuid().slice(0, 6)}`,
      data: { a: 1 },
    });

    await client.updateSessionData(session.id, { b: 2 });
    const fetched = await client.getSession(session.id);
    const data = fetched.data as Record<string, unknown>;
    // API performs full replacement, not merge.
    assert.equal(data.a, undefined, "original field is replaced");
    assert.equal(data.b, 2, "new field is stored");
  });

  it("list instances empty for new session", async () => {
    const tenantId = `sess-empty-${uuid().slice(0, 8)}`;
    const session = await client.createSession({
      tenant_id: tenantId,
      session_key: `empty-${uuid().slice(0, 6)}`,
      data: {},
    });

    const instances = await client.listSessionInstances(session.id);
    assert.equal(instances.length, 0, "new session should have no instances");
  });

  it("session with expiry round-trips", async () => {
    const tenantId = `sess-exp-${uuid().slice(0, 8)}`;
    const future = new Date(Date.now() + 3600_000).toISOString();
    const session = await client.createSession({
      tenant_id: tenantId,
      session_key: `exp-${uuid().slice(0, 6)}`,
      data: {},
      expires_at: future,
    });

    const fetched = await client.getSession(session.id);
    assert.ok(fetched.expires_at, "expires_at should be present");
  });

  it("duplicate session key across tenants allowed", async () => {
    const key = `shared-${uuid().slice(0, 6)}`;
    const sA = await client.createSession({ tenant_id: "tenant-a", session_key: key, data: {} });
    const sB = await client.createSession({ tenant_id: "tenant-b", session_key: key, data: {} });
    assert.notEqual(sA.id, sB.id, "different tenants should have different session ids");
  });
});
