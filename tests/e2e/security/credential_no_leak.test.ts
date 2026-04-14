/**
 * Credential No-Leak — verifies that credential secret values NEVER appear
 * in any API response. Secrets are write-only: the server accepts them on
 * create/update but never returns them. They are only used internally by
 * step handlers (LLM, HTTP, etc.) when resolving `credentials://` refs.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Credential No-Leak", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  const SECRET = "sk_live_SUPER_SECRET_KEY_12345";
  const REFRESH_SECRET = "rt_REFRESH_TOKEN_SECRET_67890";

  it("create response does not contain value or refresh_token", async () => {
    const id = `noleak-create-${uuid().slice(0, 8)}`;
    const res = await client.createCredential({
      id,
      name: "No Leak Create",
      kind: "oauth2",
      value: SECRET,
      tenant_id: "test",
      refresh_url: "https://example.com/token",
      refresh_token: REFRESH_SECRET,
    });

    const json = JSON.stringify(res);
    assert.ok(!json.includes(SECRET), "secret must not appear in create response");
    assert.ok(!json.includes(REFRESH_SECRET), "refresh_token must not appear in create response");
    assert.equal((res as any).value, undefined);
    assert.equal((res as any).refresh_token, undefined);
    // has_refresh_token is a boolean indicator, not the actual value.
    assert.equal((res as any).has_refresh_token, true);
  });

  it("get response does not contain value or refresh_token", async () => {
    const id = `noleak-get-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id,
      name: "No Leak Get",
      kind: "api_key",
      value: SECRET,
      tenant_id: "test",
    });

    const res = await client.getCredential(id);
    const json = JSON.stringify(res);
    assert.ok(!json.includes(SECRET), "secret must not appear in get response");
    assert.equal((res as any).value, undefined);
  });

  it("list response does not contain value or refresh_token", async () => {
    const tenantId = `noleak-list-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id: `noleak-l1-${uuid().slice(0, 8)}`,
      name: "No Leak List 1",
      kind: "api_key",
      value: SECRET,
      tenant_id: tenantId,
    });
    await client.createCredential({
      id: `noleak-l2-${uuid().slice(0, 8)}`,
      name: "No Leak List 2",
      kind: "oauth2",
      value: "another-secret",
      tenant_id: tenantId,
      refresh_url: "https://example.com/token",
      refresh_token: REFRESH_SECRET,
    });

    const list = await client.listCredentials({ tenant_id: tenantId });
    const json = JSON.stringify(list);
    assert.ok(!json.includes(SECRET), "secret must not appear in list response");
    assert.ok(!json.includes(REFRESH_SECRET), "refresh_token must not appear in list response");
    assert.ok(!json.includes("another-secret"), "no secret value in list response");
    for (const item of list) {
      assert.equal((item as any).value, undefined);
      assert.equal((item as any).refresh_token, undefined);
    }
  });

  it("update response does not contain value or refresh_token", async () => {
    const id = `noleak-upd-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id,
      name: "No Leak Update",
      kind: "api_key",
      value: SECRET,
      tenant_id: "test",
    });

    const newSecret = "sk_live_NEW_SECRET_99999";
    const res = await client.updateCredential(id, { value: newSecret });
    const json = JSON.stringify(res);
    assert.ok(!json.includes(SECRET), "old secret must not appear in update response");
    assert.ok(!json.includes(newSecret), "new secret must not appear in update response");
    assert.equal((res as any).value, undefined);
  });

  it("response includes metadata but never secret material", async () => {
    const id = `noleak-meta-${uuid().slice(0, 8)}`;
    const res = await client.createCredential({
      id,
      name: "Metadata Check",
      kind: "oauth2",
      value: SECRET,
      tenant_id: "test",
      refresh_url: "https://example.com/token",
      refresh_token: REFRESH_SECRET,
      description: "A test credential",
    });

    // Metadata IS present.
    assert.equal((res as any).id, id);
    assert.equal((res as any).name, "Metadata Check");
    assert.equal((res as any).kind, "oauth2");
    assert.equal((res as any).enabled, true);
    assert.equal((res as any).refresh_url, "https://example.com/token");
    assert.equal((res as any).has_refresh_token, true);
    assert.equal((res as any).description, "A test credential");
    assert.ok((res as any).created_at);
    assert.ok((res as any).updated_at);

    // Secret material is NOT present.
    assert.equal((res as any).value, undefined);
    assert.equal((res as any).refresh_token, undefined);
  });
});
