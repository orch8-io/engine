import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "./client.js";
import { startServer, stopServer } from "./harness.js";

const client = new Orch8Client();

/**
 * Sessions API.
 *
 * Routes from `orch8-api/src/sessions.rs`:
 *   - POST   /sessions
 *   - GET    /sessions/{id}
 *   - GET    /sessions/by-key/{tenant_id}/{key}
 *   - PATCH  /sessions/{id}/state
 *   - PATCH  /sessions/{id}/data
 *   - GET    /sessions/{id}/instances
 *
 * Notable gaps vs. the spec:
 *   - There is NO session DELETE endpoint.
 *   - There is NO GET for `/sessions/{id}/data` as a separate resource
 *     (the full session object already carries its `data` field).
 *   - `CreateInstanceRequest` does NOT accept `session_id`; the instance
 *     `session_id` column is only set internally via the trigger code path
 *     (`create_trigger_instance`). So we cannot directly attach an instance
 *     to a session from the public API with what's currently exposed — the
 *     `/sessions/{id}/instances` list therefore returns `[]` for sessions
 *     created through the REST surface. The test below documents that.
 */
describe("Sessions", () => {
  let server;

  before(async () => {
    server = await startServer({ build: false });
  });

  after(async () => {
    await stopServer(server);
  });

  it("creates, reads by id and by key, and returns expected fields", async () => {
    const tenantId = `sess-${uuid().slice(0, 8)}`;
    const sessionKey = `key-${uuid()}`;

    const created = await client.createSession({
      tenant_id: tenantId,
      session_key: sessionKey,
      data: { started_by: "test" },
    });
    assert.ok(created.id);
    assert.equal(created.session_key, sessionKey);
    assert.equal(created.state, "active");
    assert.deepEqual(created.data, { started_by: "test" });

    const byId = await client.getSession(created.id);
    assert.equal(byId.id, created.id);

    const byKey = await client.getSessionByKey(tenantId, sessionKey);
    assert.equal(byKey.id, created.id);
  });

  it("patches data and state", async () => {
    const tenantId = `sess-patch-${uuid().slice(0, 8)}`;
    const created = await client.createSession({
      tenant_id: tenantId,
      session_key: `k-${uuid()}`,
      data: { v: 1 },
    });

    await client.updateSessionData(created.id, { v: 2, new_field: "x" });
    await client.updateSessionState(created.id, "completed");

    const after = await client.getSession(created.id);
    assert.deepEqual(after.data, { v: 2, new_field: "x" });
    assert.equal(after.state, "completed");
  });

  it("rejects oversized session_key", async () => {
    await assert.rejects(
      () =>
        client.createSession({
          tenant_id: `sess-rej-${uuid().slice(0, 8)}`,
          session_key: "x".repeat(1024), // >512 char limit
        }),
      (err) => {
        assert.ok(err.status >= 400 && err.status < 500);
        return true;
      }
    );
  });

  it("GET /sessions/{id}/instances returns an array", async () => {
    // Note: instances created via POST /instances cannot currently set
    // session_id (the request type doesn't accept it), so for a freshly
    // created session this list is always empty. See module docstring.
    const created = await client.createSession({
      tenant_id: `sess-list-${uuid().slice(0, 8)}`,
      session_key: `k-${uuid()}`,
    });
    const instances = await client.listSessionInstances(created.id);
    assert.ok(Array.isArray(instances));
    assert.equal(instances.length, 0);
  });

  it("returns 404 for an unknown session id", async () => {
    await assert.rejects(
      () => client.getSession(uuid()),
      (err) => {
        assert.equal(err.status, 404);
        return true;
      }
    );
  });
});
