/**
 * PATCH /sessions/{id}/state — direct REST validation for session state
 * updates (active, paused, closed).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Session State Update", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("updates session state from active to paused", async () => {
    const tenantId = `sess-st-${uuid().slice(0, 8)}`;
    const session = await client.createSession({
      tenant_id: tenantId,
      session_key: `key-${uuid().slice(0, 8)}`,
    });
    const sessionId = (session as any).id;
    assert.ok(sessionId, "session should have an id");

    // Update state to paused.
    await client.updateSessionState(sessionId, "paused");

    const updated = await client.getSession(sessionId);
    assert.equal((updated as any).state, "paused");
  });

  it("updates session state to completed", async () => {
    const tenantId = `sess-cl-${uuid().slice(0, 8)}`;
    const session = await client.createSession({
      tenant_id: tenantId,
      session_key: `key-${uuid().slice(0, 8)}`,
    });
    const sessionId = (session as any).id;

    await client.updateSessionState(sessionId, "completed");

    const updated = await client.getSession(sessionId);
    assert.equal((updated as any).state, "completed");
  });

  it("non-existent session returns 404", async () => {
    try {
      await client.updateSessionState(uuid(), "paused");
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });
});
