/**
 * API key + tenant-header enforcement.
 *
 * Server wiring (`orch8-api/src/auth.rs`):
 *   - `api_key_middleware` rejects with 401 when an expected key is set
 *     and the request lacks a matching `X-API-Key`. Empty expected key
 *     ⇒ middleware is a no-op.
 *   - `tenant_middleware` rejects with 400 when `require_tenant` is true
 *     and the request lacks `X-Tenant-Id`. Missing header is tolerated
 *     when `require_tenant` is false.
 *
 * `orch8-server/src/main.rs` reads the expected key from
 * `ORCH8_API_KEY` and the require-tenant flag from
 * `ORCH8_REQUIRE_TENANT_HEADER`. `--insecure` only gates the
 * "no key configured" bail-out — once a key is set, auth is enforced
 * regardless of that flag. This suite is marked SELF_MANAGED because
 * the shared attach-mode server is started without any key.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const API_KEY = "test-api-key-enforcement-fixture";

describe("API Key + Tenant Header Enforcement", () => {
  let server: ServerHandle | undefined;
  let client: Orch8Client;

  before(async () => {
    // Start a dedicated server with API key auth AND tenant header
    // enforcement enabled. Use a port off the default to avoid colliding
    // with any attach-mode server that's already up.
    server = await startServer({
      port: 18091,
      env: {
        ORCH8_API_KEY: API_KEY,
        ORCH8_REQUIRE_TENANT_HEADER: "true",
      },
    });
    client = new Orch8Client(`http://localhost:${server.port}`);
  });

  after(async () => {
    await stopServer(server);
  });

  it("rejects requests without the X-API-Key header with 401", async () => {
    // Hit a protected endpoint with neither API key nor tenant header —
    // the API key check runs first, so we should see 401 (not 400).
    const res = await fetch(`${client.baseUrl}/instances`);
    assert.equal(
      res.status,
      401,
      `unauthenticated request should be rejected with 401, got ${res.status}`,
    );
  });

  it("rejects requests with a wrong X-API-Key with 401", async () => {
    const res = await fetch(`${client.baseUrl}/instances`, {
      headers: {
        "X-API-Key": "not-the-real-key",
        "X-Tenant-Id": "anything",
      },
    });
    assert.equal(
      res.status,
      401,
      `wrong API key should be rejected with 401, got ${res.status}`,
    );
  });

  it("accepts requests bearing the valid X-API-Key", async () => {
    // With a valid API key AND a tenant header the request should pass
    // both middlewares and get through to the handler.
    const tenantId = `auth-ok-${uuid().slice(0, 8)}`;

    // Create a sequence directly via fetch so we can attach headers.
    const seq = testSequence("auth-ok", [step("s1", "noop")], { tenantId });
    const seqRes = await fetch(`${client.baseUrl}/sequences`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY,
        "X-Tenant-Id": tenantId,
      },
      body: JSON.stringify(seq),
    });
    // POST /sequences returns 200 or 201 on success — both count as "allowed";
    // what matters for this test is that auth didn't block the request.
    assert.ok(
      seqRes.status === 200 || seqRes.status === 201,
      `valid key should allow the request through, got ${seqRes.status}`,
    );
  });

  it("rejects requests missing X-Tenant-Id with 400 when required", async () => {
    // Supply the valid API key but drop the tenant header — the tenant
    // middleware should reject with 400.
    const res = await fetch(`${client.baseUrl}/instances`, {
      headers: {
        "X-API-Key": API_KEY,
      },
    });
    assert.equal(
      res.status,
      400,
      `missing tenant header should be rejected with 400, got ${res.status}`,
    );
  });

  it("allows health endpoints without any headers", async () => {
    // Health endpoints live outside the auth layer — they must stay
    // reachable so probes keep working regardless of auth config.
    const live = await fetch(`${client.baseUrl}/health/live`);
    assert.equal(live.status, 200);
  });
});
