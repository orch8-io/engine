/**
 * Credential Advanced — verifies credential CRUD, scoping, and value handling.
 * Note: values are never returned in API responses for security.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Credential Advanced", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("create credential with value round-trips id and name", async () => {
    const name = `cred-val-${uuid().slice(0, 8)}`;
    const created = await client.createCredential({
      id: uuid(),
      name,
      tenant_id: "test",
      kind: "api_key",
      value: "secret123",
    });
    assert.equal(created.name, name);
    assert.equal((created as any).kind, "api_key");
    // Value is stripped from response for security.
    assert.equal((created as any).value, undefined);
  });

  it("update credential name changes it", async () => {
    const name = `cred-up-${uuid().slice(0, 8)}`;
    const created = await client.createCredential({
      id: uuid(),
      name,
      tenant_id: "test",
      kind: "api_key",
      value: "old",
    });

    await client.updateCredential(created.id, { name: "new-name" });
    const fetched = await client.getCredential(created.id);
    assert.equal((fetched as any).name, "new-name");
  });

  it("get credential does not expose value", async () => {
    const name = `cred-get-${uuid().slice(0, 8)}`;
    const created = await client.createCredential({
      id: uuid(),
      name,
      tenant_id: "test",
      kind: "basic",
      value: "plaintext",
    });

    const fetched = await client.getCredential(created.id);
    assert.equal(fetched.name, name);
    assert.equal((fetched as any).value, undefined, "value should not be exposed");
  });

  it("list credentials includes kind field", async () => {
    const tenantId = `cred-kind-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id: uuid(),
      name: `cred-api-${uuid().slice(0, 6)}`,
      tenant_id: tenantId,
      kind: "api_key",
      value: "v1",
    });
    await client.createCredential({
      id: uuid(),
      name: `cred-oauth-${uuid().slice(0, 6)}`,
      tenant_id: tenantId,
      kind: "oauth2",
      value: "v2",
    });

    const all = await client.listCredentials({ tenant_id: tenantId });
    assert.ok(all.length >= 2);
    assert.ok(all.some((c: any) => c.kind === "api_key"));
    assert.ok(all.some((c: any) => c.kind === "oauth2"));
  });

  it("delete credential removes it", async () => {
    const name = `cred-del-${uuid().slice(0, 8)}`;
    const created = await client.createCredential({
      id: uuid(),
      name,
      tenant_id: "test",
      kind: "api_key",
      value: "x",
    });

    await client.deleteCredential(created.id);

    try {
      await client.getCredential(created.id);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("duplicate credential id returns conflict", async () => {
    const id = uuid();
    await client.createCredential({
      id,
      name: "first",
      tenant_id: "test",
      kind: "api_key",
      value: "v1",
    });

    try {
      await client.createCredential({
        id,
        name: "second",
        tenant_id: "test",
        kind: "api_key",
        value: "v2",
      });
      assert.fail("should throw conflict");
    } catch (err: any) {
      assert.ok(err.status === 409 || err.status === 422, `expected 409/422, got ${err.status}`);
    }
  });
});
