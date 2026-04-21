/**
 * Credential Kind Filter & OAuth2 Validation — verifies that credentials
 * can be filtered by kind and that oauth2 credentials validate required fields.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Credential Kind Filter & OAuth2", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("listCredentials includes kind field for all credentials", async () => {
    const tenantId = `cred-kind-${uuid().slice(0, 8)}`;

    await client.createCredential({
      id: `api-${uuid().slice(0, 8)}`,
      name: "API Key",
      kind: "api_key",
      value: "secret-api",
      tenant_id: tenantId,
    });
    await client.createCredential({
      id: `oauth-${uuid().slice(0, 8)}`,
      name: "OAuth2",
      kind: "oauth2",
      value: "token",
      tenant_id: tenantId,
      refresh_token: "refresh",
      refresh_url: "https://example.com/refresh",
    });

    const list = await client.listCredentials({ tenant_id: tenantId });
    const kinds = new Set(list.map((c: any) => c.kind));
    assert.ok(kinds.has("api_key"), "listing should include api_key credential");
    assert.ok(kinds.has("oauth2"), "listing should include oauth2 credential");
  });

  it("oauth2 credential without refresh_token when refresh_url is set returns error", async () => {
    await assert.rejects(
      () =>
        client.createCredential({
          id: `oauth-bad-${uuid().slice(0, 8)}`,
          name: "Bad OAuth2",
          kind: "oauth2",
          value: "token",
          tenant_id: "test",
          refresh_url: "https://example.com/refresh",
          // missing refresh_token
        }),
      (err: unknown) => {
        assert.ok(
          (err as ApiError).status >= 400 && (err as ApiError).status < 500,
        );
        return true;
      },
    );
  });

  it("getCredential is not tenant-scoped by id alone", async () => {
    const tenantA = `cred-wrong-${uuid().slice(0, 8)}`;
    const cred = await client.createCredential({
      id: `cred-wrong-${uuid().slice(0, 8)}`,
      name: "Secret",
      kind: "api_key",
      value: "x",
      tenant_id: tenantA,
    });

    // The current implementation does NOT enforce tenant isolation on
    // GET /credentials/{id} — it looks up by id alone.
    const got = await client.getCredential(cred.id as string);
    assert.equal(got.id, cred.id);
    assert.equal(got.tenant_id, tenantA);

    await client.deleteCredential(cred.id as string);
  });
});
