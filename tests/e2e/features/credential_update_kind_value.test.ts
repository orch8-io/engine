/**
 * Credential Update Kind and Value — verifies update behavior for
 * credential name, kind, and value fields.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Credential Update Kind and Value", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("update credential name persists", async () => {
    const name = `cred-up-name-${uuid().slice(0, 8)}`;
    const created = await client.createCredential({
      id: uuid(),
      name,
      tenant_id: "test",
      kind: "api_key",
      value: "v1",
    });

    await client.updateCredential(created.id, { name: "renamed" });
    const fetched = await client.getCredential(created.id);
    assert.equal((fetched as any).name, "renamed");
  });

  it("update credential kind persists", async () => {
    const name = `cred-up-kind-${uuid().slice(0, 8)}`;
    const created = await client.createCredential({
      id: uuid(),
      name,
      tenant_id: "test",
      kind: "api_key",
      value: "v1",
    });

    await client.updateCredential(created.id, { kind: "basic" });
    const fetched = await client.getCredential(created.id);
    assert.equal((fetched as any).kind, "basic");
  });

  it("get credential never exposes value", async () => {
    const name = `cred-hide-${uuid().slice(0, 8)}`;
    const created = await client.createCredential({
      id: uuid(),
      name,
      tenant_id: "test",
      kind: "oauth2",
      value: "super-secret",
    });

    const fetched = await client.getCredential(created.id);
    assert.equal((fetched as any).value, undefined);
    assert.equal((fetched as any).name, name);
  });

  it("list credentials never expose values", async () => {
    const tenantId = `cred-list-hide-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id: uuid(),
      name: `cred-a-${uuid().slice(0, 6)}`,
      tenant_id: tenantId,
      kind: "api_key",
      value: "secret-a",
    });
    await client.createCredential({
      id: uuid(),
      name: `cred-b-${uuid().slice(0, 6)}`,
      tenant_id: tenantId,
      kind: "oauth2",
      value: "secret-b",
    });

    const list = await client.listCredentials({ tenant_id: tenantId });
    assert.ok(list.length >= 2);
    assert.ok(list.every((c: any) => c.value === undefined), "no value should be exposed");
  });

  it("create credential with invalid kind is rejected", async () => {
    try {
      await client.createCredential({
        id: uuid(),
        name: `cred-bad-kind-${uuid().slice(0, 8)}`,
        tenant_id: "test",
        kind: "not_a_kind",
        value: "v",
      });
      assert.fail("should throw");
    } catch (err: any) {
      assert.ok(err.status >= 400 && err.status < 500);
    }
  });
});
