/**
 * Credential CRUD — create, list, update, delete.
 * Complements encryption_at_rest.test.ts with focused CRUD validation.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Credential CRUD", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("create and list credentials by tenant", async () => {
    const tenantId = `cred-${uuid().slice(0, 8)}`;
    const credId = `cred-${uuid().slice(0, 8)}`;

    const cred = await client.createCredential({
      id: credId,
      name: "Test Cred",
      kind: "api_key",
      value: "secret-value-123",
      tenant_id: tenantId,
    });
    assert.equal((cred as any).id, credId);

    const list = await client.listCredentials({ tenant_id: tenantId });
    assert.ok(Array.isArray(list), "credentials list should be an array");
    assert.ok(
      list.some((c) => (c as any).id === credId),
      "created credential should appear in listing",
    );
  });

  it("update credential name", async () => {
    const credId = `cred-upd-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id: credId,
      name: "Before",
      kind: "api_key",
      value: "secret",
      tenant_id: "test",
    });

    await client.updateCredential(credId, { name: "After" });

    const fetched = await client.getCredential(credId);
    assert.equal((fetched as any).name, "After");
  });

  it("get non-existent credential returns 404", async () => {
    try {
      await client.getCredential(`nonexistent-${uuid().slice(0, 8)}`);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("delete credential makes it disappear", async () => {
    const credId = `cred-del-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id: credId,
      name: "Delete Me",
      kind: "api_key",
      value: "disposable",
      tenant_id: "test",
    });

    await client.deleteCredential(credId);

    try {
      await client.getCredential(credId);
      assert.fail("should 404 after delete");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });
});
