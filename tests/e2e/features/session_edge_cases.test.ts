/**
 * Session Edge Cases — duplicate key handling, cross-tenant lookup,
 * and data merge semantics.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Session Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("creating a session with duplicate key within same tenant returns conflict", async () => {
    const tenantId = `sess-dup-${uuid().slice(0, 8)}`;
    const key = `dup-key-${uuid()}`;

    const first = await client.createSession({
      tenant_id: tenantId,
      session_key: key,
      data: { version: 1 },
    });
    assert.ok(first.id);

    // Second create with same tenant+key should conflict (409).
    await assert.rejects(
      () =>
        client.createSession({
          tenant_id: tenantId,
          session_key: key,
          data: { version: 2 },
        }),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 409);
        return true;
      },
    );
  });

  it("allows duplicate session_key across different tenants", async () => {
    const tenantA = `sess-dup-a-${uuid().slice(0, 8)}`;
    const tenantB = `sess-dup-b-${uuid().slice(0, 8)}`;
    const key = `shared-key-${uuid()}`;

    const sessA = await client.createSession({
      tenant_id: tenantA,
      session_key: key,
    });
    const sessB = await client.createSession({
      tenant_id: tenantB,
      session_key: key,
    });

    assert.notEqual(
      sessA.id,
      sessB.id,
      "same key across tenants should create distinct sessions",
    );
  });

  it("updateSessionData replaces data entirely", async () => {
    const tenantId = `sess-merge-${uuid().slice(0, 8)}`;
    const created = await client.createSession({
      tenant_id: tenantId,
      session_key: `merge-${uuid()}`,
      data: { a: 1, nested: { x: 10 } },
    });

    await client.updateSessionData(created.id as string, {
      b: 2,
      nested: { y: 20 },
    });

    const after = await client.getSession(created.id as string);
    // The server replaces the entire data payload, not a shallow merge.
    assert.equal((after.data as any).a, undefined, "old top-level key should be gone");
    assert.equal((after.data as any).b, 2, "new top-level key should appear");
    assert.deepEqual(
      (after.data as any).nested,
      { y: 20 },
      "nested object should be replaced",
    );
  });

  it("returns 404 for unknown session id on get", async () => {
    await assert.rejects(
      () => client.getSession(uuid()),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });
});
