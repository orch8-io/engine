import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Credentials CRUD", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should create a credential", async () => {
    const id = `cred-${uuid().slice(0, 8)}`;
    const res = await client.createCredential({
      id,
      name: "Test API Key",
      kind: "api_key",
      value: "sk-secret-12345",
      tenant_id: "test",
    });
    assert.equal((res as any).id, id);
    assert.equal((res as any).name, "Test API Key");
    // Value must be completely stripped from response — never leaves the server.
    assert.equal((res as any).value, undefined, "value must not appear in response");
    assert.equal((res as any).refresh_token, undefined, "refresh_token must not appear");
  });

  it("should list credentials filtered by tenant", async () => {
    const tenantId = `t-${uuid().slice(0, 8)}`;
    const id = `cred-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id,
      name: "Tenant Cred",
      kind: "api_key",
      value: "secret",
      tenant_id: tenantId,
    });

    const list = await client.listCredentials({ tenant_id: tenantId });
    assert.ok(list.length >= 1);
    assert.ok(list.some((c: any) => c.id === id));
  });

  it("should get a credential by id", async () => {
    const id = `cred-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id,
      name: "Get Test",
      kind: "api_key",
      value: "secret-value",
      tenant_id: "test",
    });

    const cred = await client.getCredential(id);
    assert.equal((cred as any).id, id);
    assert.equal((cred as any).name, "Get Test");
    assert.equal((cred as any).enabled, true);
  });

  it("should update a credential name", async () => {
    const id = `cred-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id,
      name: "Before Update",
      kind: "api_key",
      value: "secret",
      tenant_id: "test",
    });

    await client.updateCredential(id, { name: "After Update" });
    const updated = await client.getCredential(id);
    assert.equal((updated as any).name, "After Update");
  });

  it("should disable a credential", async () => {
    const id = `cred-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id,
      name: "Disable Me",
      kind: "api_key",
      value: "secret",
      tenant_id: "test",
    });

    await client.updateCredential(id, { enabled: false });
    const updated = await client.getCredential(id);
    assert.equal((updated as any).enabled, false);
  });

  it("should delete a credential", async () => {
    const id = `cred-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id,
      name: "Delete Me",
      kind: "api_key",
      value: "secret",
      tenant_id: "test",
    });

    await client.deleteCredential(id);

    await assert.rejects(
      () => client.getCredential(id),
      (err: any) => err.status === 404,
    );
  });

  it("should reject empty id", async () => {
    await assert.rejects(
      () =>
        client.createCredential({
          id: "",
          name: "No ID",
          kind: "api_key",
          value: "secret",
          tenant_id: "test",
        }),
      (err: any) => err.status === 400,
    );
  });

  it("should reject empty value", async () => {
    await assert.rejects(
      () =>
        client.createCredential({
          id: `cred-${uuid().slice(0, 8)}`,
          name: "No Value",
          kind: "api_key",
          value: "",
          tenant_id: "test",
        }),
      (err: any) => err.status === 400,
    );
  });

  it("should reject invalid characters in id", async () => {
    await assert.rejects(
      () =>
        client.createCredential({
          id: "has spaces!",
          name: "Bad ID",
          kind: "api_key",
          value: "secret",
          tenant_id: "test",
        }),
      (err: any) => err.status === 400,
    );
  });

  it("should reject oauth2 with refresh_url but no refresh_token", async () => {
    await assert.rejects(
      () =>
        client.createCredential({
          id: `cred-${uuid().slice(0, 8)}`,
          name: "OAuth Bad",
          kind: "oauth2",
          value: "token",
          tenant_id: "test",
          refresh_url: "https://example.com/token",
        }),
      (err: any) => err.status === 400,
    );
  });

  it("should create oauth2 credential with refresh_url and refresh_token", async () => {
    const id = `cred-${uuid().slice(0, 8)}`;
    const res = await client.createCredential({
      id,
      name: "OAuth Valid",
      kind: "oauth2",
      value: "access-token",
      tenant_id: "test",
      refresh_url: "https://example.com/token",
      refresh_token: "refresh-abc",
    });
    assert.equal((res as any).id, id);
    assert.equal((res as any).kind, "oauth2");
  });
});
