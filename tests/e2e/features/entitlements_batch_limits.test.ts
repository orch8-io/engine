/**
 * Entitlement batch boundaries — the 10,000-item per-request ceiling.
 *
 * The self-managed plan sets `max_batch_instances = 10_000`, and the batch
 * route enforces the same 10,000 cap (`create_instances_batch`) BEFORE plan
 * admission runs — so the observable rejection at 10,001 is the route-level
 * 400 "batch size must not exceed 10,000", not the plan's 429. These tests
 * pin the boundary exactly as the shipped binary behaves:
 *
 *   - exactly 10,000 → admitted (single tenant, and per tenant concurrently)
 *   - 10,001 → 400 with the exact message, nothing persisted
 *   - no active-instance quota: a tenant at the cap can keep creating
 *   - per-tenant scoping: two tenants can each max the per-request cap
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
import type { CreateInstanceRequest } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

function batchItems(
  sequenceId: string,
  tenantId: string,
  count: number,
): CreateInstanceRequest[] {
  return Array.from({ length: count }, () => ({
    sequence_id: sequenceId,
    tenant_id: tenantId,
    namespace: "default",
  }));
}

describe("Entitlements — batch size boundary", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("admits a batch of exactly 10,000 instances", async () => {
    const tenantId = `cap-exact-${uuid().slice(0, 8)}`;
    const seq = testSequence("cap-exact", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const res = await client.createInstancesBatch(
      batchItems(seq.id, tenantId, 10_000),
    );
    assert.equal((res as any).count, 10_000, "exactly-limit batch admitted");

    // Rows are really there — the list endpoint caps at 1,000 per page.
    const page = await client.listInstances({
      tenant_id: tenantId,
      limit: 1_000,
    });
    assert.equal(page.length, 1_000, "first page of the 10k batch");
    assert.ok(page.every((i) => i.tenant_id === tenantId));
  });

  it("rejects 10,001 instances with 400 and the exact cap message", async () => {
    const tenantId = `cap-over-${uuid().slice(0, 8)}`;
    const seq = testSequence("cap-over", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    let err: ApiError | undefined;
    try {
      await client.createInstancesBatch(
        batchItems(seq.id, tenantId, 10_001),
      );
    } catch (e) {
      err = e as ApiError;
    }
    assert.ok(err, "10,001 items must be rejected");
    assert.equal(err.status, 400, `expected 400, got ${err.status}`);
    assert.match(
      err.body,
      /batch size must not exceed 10,000/,
      "body should carry the exact cap message",
    );

    const persisted = await client.listInstances({ tenant_id: tenantId });
    assert.equal(persisted.length, 0, "rejected batch wrote nothing");
  });

  it("lets the same tenant create normally after a rejected oversize batch", async () => {
    const tenantId = `cap-after-${uuid().slice(0, 8)}`;
    const seq = testSequence("cap-after", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    await assert.rejects(
      client.createInstancesBatch(batchItems(seq.id, tenantId, 10_001)),
      (e: unknown) => e instanceof ApiError && e.status === 400,
    );

    const small = await client.createInstancesBatch(
      batchItems(seq.id, tenantId, 7),
    );
    assert.equal((small as any).count, 7);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    // Admission is the point here — do NOT wait for completion: earlier
    // tests in this file leave a 10k-instance execution backlog the
    // scheduler drains in fire-time order, which would stall this wait
    // far beyond a test timeout. Assert the instance is live and healthy.
    const created = await client.getInstance(id);
    assert.ok(
      ["scheduled", "pending", "running", "waiting", "completed"].includes(
        created.state,
      ),
      `fresh instance should be live, got ${created.state}`,
    );

    const items = await client.listInstances({ tenant_id: tenantId });
    assert.equal(items.length, 8, "7 batch + 1 single, nothing from the 10,001");
  });

  it("imposes no active-instance quota beyond the per-request cap", async () => {
    const tenantId = `cap-quota-${uuid().slice(0, 8)}`;
    const seq = testSequence("cap-quota", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    // Fill the tenant to the per-request cap, then keep creating: the
    // self-managed plan's max_active_instances is u64::MAX, so admission
    // must never turn into 429 QuotaExceeded no matter the active count.
    const first = await client.createInstancesBatch(
      batchItems(seq.id, tenantId, 10_000),
    );
    assert.equal((first as any).count, 10_000);

    const second = await client.createInstancesBatch(
      batchItems(seq.id, tenantId, 500),
    );
    assert.equal((second as any).count, 500, "no cumulative quota");

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    assert.ok(id, "single create still admitted at 10,500 active");
  });

  it("lets two tenants each max the per-request cap independently", async () => {
    const tenantA = `cap-ta-${uuid().slice(0, 8)}`;
    const tenantB = `cap-tb-${uuid().slice(0, 8)}`;
    const seqA = testSequence("cap-ta", [step("s", "noop")], {
      tenantId: tenantA,
    });
    const seqB = testSequence("cap-tb", [step("s", "noop")], {
      tenantId: tenantB,
    });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    // Interleave: B's cap is evaluated after A's 10k already landed.
    const resA = await client.createInstancesBatch(
      batchItems(seqA.id, tenantA, 10_000),
    );
    const resB = await client.createInstancesBatch(
      batchItems(seqB.id, tenantB, 10_000),
    );
    assert.equal((resA as any).count, 10_000);
    assert.equal(
      (resB as any).count,
      10_000,
      "tenant B's admission must not see tenant A's usage",
    );

    const pageA = await client.listInstances({
      tenant_id: tenantA,
      limit: 1_000,
    });
    const pageB = await client.listInstances({
      tenant_id: tenantB,
      limit: 1_000,
    });
    assert.equal(pageA.length, 1_000);
    assert.equal(pageB.length, 1_000);
    assert.ok(pageA.every((i) => i.tenant_id === tenantA));
    assert.ok(pageB.every((i) => i.tenant_id === tenantB));
  });

  it("admits an exactly-limit batch carrying one large-but-legal context", async () => {
    const tenantId = `cap-ctx-${uuid().slice(0, 8)}`;
    const seq = testSequence("cap-ctx", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const items = batchItems(seq.id, tenantId, 10_000);
    // ~200KiB context on one item: under the 256KiB server ceiling, and the
    // plan's context ceiling (u32::MAX) must not fire either.
    items[5_000]!.context = { data: { blob: "y".repeat(200 * 1024) } };

    const res = await client.createInstancesBatch(items);
    assert.equal((res as any).count, 10_000);
  });
});
