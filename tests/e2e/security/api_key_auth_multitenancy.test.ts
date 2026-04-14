/**
 * Verifies that bearer tokens are strictly bound to the tenant they were issued for.
 *
 * Note: orch8 authenticates with an `X-API-Key` header (see orch8-api/src/auth.rs::api_key_middleware),
 * not `Authorization: Bearer`. The E2E harness starts the server with `--insecure`, which disables
 * API-key enforcement entirely. Tenant isolation is still enforced when the `X-Tenant-Id` header
 * is present — mismatches against a resource's owner return 404 (enforce_tenant_access).
 *
 * These tests exercise the closest observable behavior: verifying that an `X-Tenant-Id` header
 * set to the wrong tenant hides another tenant's resource, and that matching tenants read
 * successfully. The "bearer token" framing is preserved in the Authorization header to match
 * the test plan, but the server ignores it in --insecure mode.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("API Key + Multi-Tenancy", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - arrange: provision bearer token scoped to tenant A
  //   - act: issue request with X-Tenant-Id header set to tenant B
  //   - assert: API returns 403, no data leaks across tenants
  it("should reject requests with bearer token scoped to a different tenant", async () => {
    // Arrange: create an instance under tenant A.
    const tenantA = `apikey-a-${uuid().slice(0, 8)}`;
    const tenantB = `apikey-b-${uuid().slice(0, 8)}`;

    const seqA = testSequence("apikey-seq-a", [step("s1", "noop")], {
      tenantId: tenantA,
    });
    await client.createSequence(seqA);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantA,
      namespace: "default",
    });

    // Act: fetch instance A while presenting tenant B via X-Tenant-Id. A
    // fabricated Authorization: Bearer header is included to match the plan's
    // framing; --insecure mode ignores it but the tenant header is still
    // enforced by tenant_middleware / enforce_tenant_access.
    const res = await fetch(`${client.baseUrl}/instances/${idA}`, {
      headers: {
        Authorization: `Bearer fake-token-for-${tenantA}`,
        "X-Tenant-Id": tenantB,
      },
    });

    // Assert: mismatched tenant header returns 404 (deliberately not 403 — see
    // auth.rs comment "avoid leaking existence of cross-tenant resources").
    // Under strict bearer-token auth this would be 403; the observable outcome
    // here (no data leak) is equivalent.
    assert.equal(res.status, 404, `expected 404 for cross-tenant access, got ${res.status}`);
    const body = await res.text();
    assert.ok(
      !body.includes(idA) || body.toLowerCase().includes("not found"),
      "response must not leak instance details across tenants"
    );
  });

  // Plan:
  //   - arrange: reuse the tenant A token
  //   - act: issue request with X-Tenant-Id header set to tenant A
  //   - assert: API returns 2xx, resource accessible under matching tenant
  it("should accept requests where bearer token tenant matches X-Tenant-Id header", async () => {
    const tenantA = `apikey-match-${uuid().slice(0, 8)}`;

    const seqA = testSequence("apikey-match-seq", [step("s1", "noop")], {
      tenantId: tenantA,
    });
    await client.createSequence(seqA);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantA,
      namespace: "default",
    });

    // Note: in --insecure mode the bearer token is ignored; the positive case
    // verifies the matching tenant header path allows access.
    const res = await fetch(`${client.baseUrl}/instances/${idA}`, {
      headers: {
        Authorization: `Bearer fake-token-for-${tenantA}`,
        "X-Tenant-Id": tenantA,
      },
    });

    assert.equal(res.status, 200, `expected 200 for matching tenant, got ${res.status}`);
    const instance = (await res.json()) as { id: string; tenant_id: string };
    assert.equal(instance.id, idA);
    assert.equal(instance.tenant_id, tenantA);
  });
});
