/**
 * Capability-scoped principals — key lifecycle & admin gating.
 *
 * Covers the management plane of `feat(api): enforce capability-scoped
 * principals` (3b4ad3f): minting (default + explicit capabilities), listing,
 * revocation, expiry, tenant binding, and the rule that only the ROOT key
 * may manage keys. The server runs with `ORCH8_API_KEY` set, so the auth
 * middleware is live; per-tenant keys are minted through the real API.
 *
 * Capability *route-family* enforcement lives in the sibling
 * `principals_<family>.test.ts` suites.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import {
  Orch8Client,
  ApiError,
  testSequence,
  step,
  uuid,
} from "../client.ts";
import type { CreatedApiKey } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const ROOT_KEY = `root-principals-km-${uuid().slice(0, 8)}`;

describe("Principals — API key lifecycle & admin gating", () => {
  let server: ServerHandle | undefined;
  let root: Orch8Client;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_API_KEY: ROOT_KEY,
        // Keys bind their own tenant (the middleware stamps TenantContext
        // from the key record), so header-based isolation stays off — the
        // server refuses this combo unless the risk is explicitly accepted.
        ORCH8_ALLOW_NO_TENANT_ISOLATION: "1",
      },
    });
    root = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": ROOT_KEY,
    });
  });

  after(async () => {
    await stopServer(server);
  });

  function scoped(secret: string): Orch8Client {
    return new Orch8Client(`http://localhost:${server!.port}`, {
      "X-API-Key": secret,
    });
  }

  it("mints a key without capabilities and defaults the grant to operator", async () => {
    const tenantId = `km-def-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({ tenant_id: tenantId, name: "ci" });
    assert.deepEqual(
      key.capabilities,
      ["operator"],
      "omitted capabilities must default to operator",
    );

    // The default grant is functional: operator may write sequences.
    const asKey = scoped(key.secret);
    const seq = testSequence("km-def", [step("s", "noop")], { tenantId });
    await asKey.createSequence(seq);
    const fetched = await asKey.getSequence(seq.id);
    assert.equal(fetched.id, seq.id);
  });

  it("persists an explicit capability grant verbatim in create + list", async () => {
    const tenantId = `km-exp-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({
      tenant_id: tenantId,
      name: "worker-only",
      capabilities: ["worker"],
    });
    assert.deepEqual(key.capabilities, ["worker"]);

    const listed = await root.listApiKeys(tenantId);
    const row = listed.find((k) => k.id === key.id);
    assert.ok(row, "minted key appears in list");
    assert.deepEqual(row.capabilities, ["worker"]);
    assert.equal(row.name, "worker-only");
    assert.equal(row.tenant_id, tenantId);
    assert.equal(row.revoked, false);
    assert.ok(!("secret" in row), "list must never echo the secret");
  });

  it("returns id/secret in the expected formats exactly once", async () => {
    const tenantId = `km-fmt-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({ tenant_id: tenantId });
    assert.match(key.id, /^ak_[0-9a-f]{32}$/, "key id format");
    assert.match(key.secret, /^sk_/, "secret prefix");
    assert.ok(key.created_at, "created_at present");
    assert.equal(key.expires_at, null, "non-expiring by default");

    // The plaintext is unrecoverable: a second read path (list) lacks it.
    const listed = await root.listApiKeys(tenantId);
    assert.equal(listed.length, 1);
    assert.ok(!("secret" in listed[0]!));
  });

  it("supports multi-capability grants as the UNION of route families", async () => {
    const tenantId = `km-union-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({
      tenant_id: tenantId,
      capabilities: ["worker", "auditor"],
    });
    const asKey = scoped(key.secret);

    // auditor half: arbitrary GETs succeed.
    const seqs = await asKey.listSequences({ tenant_id: tenantId });
    assert.ok(Array.isArray(seqs));
    // worker half: worker-family POST succeeds.
    const tasks = await asKey.pollWorkerTasks("no_such_handler", "w-1");
    assert.deepEqual(tasks, []);
    // neither half covers a write outside the families.
    await assert.rejects(
      asKey.createSequence(
        testSequence("km-union", [step("s", "noop")], { tenantId }),
      ),
      (e: unknown) => e instanceof ApiError && e.status === 403,
    );
  });

  it("rejects list calls without a usable tenant_id query parameter", async () => {
    const base = `http://localhost:${server!.port}`;
    // Missing entirely: rejected at query deserialization.
    const missing = await fetch(`${base}/api-keys`, {
      headers: { "X-API-Key": ROOT_KEY },
    });
    assert.equal(missing.status, 400);
    assert.match(await missing.text(), /missing field `tenant_id`/);

    // Present but blank: rejected by the handler's own guard.
    const blank = await fetch(`${base}/api-keys?tenant_id=`, {
      headers: { "X-API-Key": ROOT_KEY },
    });
    assert.equal(blank.status, 400);
    assert.match(
      await blank.text(),
      /tenant_id query parameter is required/,
    );
  });

  it("forbids a tenant key (even operator) from minting keys", async () => {
    const tenantId = `km-nomint-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({ tenant_id: tenantId });
    const asKey = scoped(key.secret);

    let err: ApiError | undefined;
    try {
      await asKey.createApiKey({ tenant_id: tenantId, name: "escalation" });
    } catch (e) {
      err = e as ApiError;
    }
    assert.ok(err, "tenant key must not mint keys");
    assert.equal(err.status, 403);
    assert.match(
      err.body,
      /API key management requires the root API key/,
      "403 must come from the admin gate, not the capability filter",
    );

    // No escalation actually happened.
    const listed = await root.listApiKeys(tenantId);
    assert.equal(listed.length, 1, "only the original key exists");
  });

  it("forbids a tenant key from listing keys", async () => {
    const tenantId = `km-nolist-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({ tenant_id: tenantId });
    await assert.rejects(scoped(key.secret).listApiKeys(tenantId), {
      status: 403,
    } as object);
  });

  it("forbids a tenant key from revoking keys", async () => {
    const tenantId = `km-norevoke-${uuid().slice(0, 8)}`;
    const first = await root.createApiKey({ tenant_id: tenantId });
    const second = await root.createApiKey({ tenant_id: tenantId });

    await assert.rejects(scoped(first.secret).revokeApiKey(second.id), {
      status: 403,
    } as object);

    // The targeted key survived and still authenticates.
    const seq = testSequence("km-norevoke", [step("s", "noop")], { tenantId });
    await scoped(second.secret).createSequence(seq);
  });

  it("denies a revoked key with 401 and marks it revoked in list", async () => {
    const tenantId = `km-rev-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({ tenant_id: tenantId });

    // Works before revocation.
    const asKey = scoped(key.secret);
    await asKey.listSequences({ tenant_id: tenantId });

    await root.revokeApiKey(key.id);

    await assert.rejects(asKey.listSequences({ tenant_id: tenantId }), {
      status: 401,
    } as object);

    const listed = await root.listApiKeys(tenantId);
    const row = listed.find((k) => k.id === key.id);
    assert.equal(row?.revoked, true, "list reflects the revocation");

    // The root key is untouched by the revocation.
    await root.listApiKeys(tenantId);
  });

  it("returns 404 when revoking an unknown key id", async () => {
    await assert.rejects(root.revokeApiKey("ak_doesnotexist"), {
      status: 404,
    } as object);
  });

  it("denies an expired key with 401", async () => {
    const tenantId = `km-expd-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({
      tenant_id: tenantId,
      expires_at: new Date(Date.now() - 60_000).toISOString(),
    });
    await assert.rejects(scoped(key.secret).listSequences({}), {
      status: 401,
    } as object);
  });

  it("denies missing and garbage credentials with 401", async () => {
    const base = `http://localhost:${server!.port}`;
    const noHeader = await fetch(`${base}/instances`);
    assert.equal(noHeader.status, 401);

    const garbage = await fetch(`${base}/instances`, {
      headers: { "X-API-Key": "sk_totally-bogus" },
    });
    assert.equal(garbage.status, 401);
  });

  it("binds a tenant key to its tenant — mismatched X-Tenant-Id is 403", async () => {
    const tenantA = `km-ta-${uuid().slice(0, 8)}`;
    const tenantB = `km-tb-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({ tenant_id: tenantA });
    const base = `http://localhost:${server!.port}`;

    const mismatch = await fetch(`${base}/instances`, {
      headers: { "X-API-Key": key.secret, "X-Tenant-Id": tenantB },
    });
    assert.equal(mismatch.status, 403, "cross-tenant header must be denied");

    const match = await fetch(`${base}/instances`, {
      headers: { "X-API-Key": key.secret, "X-Tenant-Id": tenantA },
    });
    assert.equal(match.status, 200, "matching header is accepted");
  });

  it("scopes a tenant key's listings to its own tenant even when asked for another", async () => {
    const tenantA = `km-iso-a-${uuid().slice(0, 8)}`;
    const tenantB = `km-iso-b-${uuid().slice(0, 8)}`;
    // Root plants one instance in each tenant.
    for (const tenantId of [tenantA, tenantB]) {
      const seq = testSequence("km-iso", [step("s", "noop")], { tenantId });
      await root.createSequence(seq);
      await root.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });
    }

    const keyA = await root.createApiKey({ tenant_id: tenantA });
    // Tenant A's key explicitly asks for tenant B's rows — the key's tenant
    // context must override the query filter.
    const rows = await scoped(keyA.secret).listInstances({
      tenant_id: tenantB,
    });
    assert.ok(rows.length >= 1, "the key still sees its own tenant's rows");
    assert.ok(
      rows.every((i) => i.tenant_id === tenantA),
      "no cross-tenant rows leak through the override",
    );
  });
});
