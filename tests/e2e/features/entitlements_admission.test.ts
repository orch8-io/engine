/**
 * Tenant entitlement admission — single & batch create paths.
 *
 * The shipped server binary wires `StaticEntitlementCatalog` nowhere: it runs
 * the explicit `self_managed` unlimited provider
 * (`orch8-server/src/main.rs` → `entitlements::unlimited_provider()`), and no
 * env/flag/HTTP surface installs per-tenant plans. These suites therefore pin
 * the OBSERVABLE admission contract of that deployment mode:
 *
 *   - namespaces are unrestricted (empty `allowed_namespaces` = all allowed)
 *   - the plan context ceiling (u32::MAX) never fires; the only context limit
 *     is the pre-existing server ceiling (`max_context_bytes`, 256 KiB → 413)
 *   - admission is evaluated per tenant group, so one tenant's usage never
 *     changes another tenant's outcome
 *   - rejections are atomic: nothing is written when admission/validation
 *     fails
 *
 * The plan-catalog 429/413/403 branches (quota exhaustion, plan context cap,
 * namespace grants) are unreachable through the shipped binary and are covered
 * by Rust-side tests (`entitlements_admission_tests.rs`,
 * `entitlements_boundary_tests.rs`). See the suite report for details.
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
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

/**
 * Build an ExecutionContext whose serialized form is approximately
 * `targetBytes`. The wire shape is `{ data: {...} }` — unknown top-level
 * keys are silently dropped by serde, so the payload MUST live under `data`.
 */
function sizedContext(targetBytes: number): Record<string, unknown> {
  const marker = `m-${uuid().slice(0, 8)}`;
  const data: Record<string, unknown> = { marker, blob: "" };
  const overhead = Buffer.byteLength(JSON.stringify({ data }));
  data.blob = "x".repeat(Math.max(0, targetBytes - overhead));
  return { data };
}

describe("Entitlements — admission under the self-managed plan", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("admits an instance in an arbitrary namespace and executes it to completion", async () => {
    const tenantId = `ent-ns-${uuid().slice(0, 8)}`;
    // A namespace no plan could possibly "grant" — unrestricted admission
    // must accept it anyway (empty allowed_namespaces = no restriction).
    const namespace = `unlisted-ns-${uuid().slice(0, 8)}`;
    const seq = testSequence("ent-ns", [step("s1", "noop")], {
      tenantId,
      namespace,
    });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });
    assert.ok(id, "instance id returned");

    const done = await client.waitForState(id, "completed", {
      timeoutMs: 15_000,
    });
    assert.equal(done.state, "completed");
    assert.equal(done.namespace, namespace);

    // Side effect: the run produced queryable outputs.
    const outputs = await client.getOutputs(id);
    assert.ok(
      outputs.some((o) => o.block_id === "s1"),
      "executed step should have an output row",
    );
  });

  it("admits one tenant across many namespaces without any grant list", async () => {
    const tenantId = `ent-multi-${uuid().slice(0, 8)}`;
    const namespaces = ["prod", "staging", "dev", `ns-${uuid().slice(0, 6)}`, "默认"];

    for (const namespace of namespaces) {
      const seq = testSequence("ent-multi", [step("s", "noop")], {
        tenantId,
        namespace,
      });
      await client.createSequence(seq);
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace,
      });
      assert.ok(id, `instance admitted in namespace ${namespace}`);
    }

    for (const namespace of namespaces) {
      const items = await client.listInstances({
        tenant_id: tenantId,
        namespace,
      });
      assert.equal(
        items.length,
        1,
        `exactly one instance should live in namespace ${namespace}`,
      );
    }
  });

  it("admits a context just below the 256KiB server ceiling", async () => {
    const tenantId = `ent-ctx-ok-${uuid().slice(0, 8)}`;
    const seq = testSequence("ent-ctx-ok", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    // ~200KiB: comfortably under the 256KiB ceiling, far beyond any
    // realistic plan cap — the unlimited plan ceiling (u32::MAX) must not fire.
    const context = sizedContext(200 * 1024);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context,
    });

    const done = await client.waitForState(id, "completed", {
      timeoutMs: 15_000,
    });
    assert.equal(done.state, "completed");
    const stored = JSON.stringify(done.context ?? {});
    assert.ok(
      stored.includes(String((context.data as any).marker)),
      "large context should round-trip through storage",
    );
  });

  it("rejects a context over the 256KiB ceiling with 413 and writes nothing", async () => {
    const tenantId = `ent-ctx-big-${uuid().slice(0, 8)}`;
    const seq = testSequence("ent-ctx-big", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const context = sizedContext(300 * 1024);
    let err: ApiError | undefined;
    try {
      await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
        context,
      });
    } catch (e) {
      err = e as ApiError;
    }
    assert.ok(err, "oversized context must be rejected");
    assert.equal(err.status, 413, `expected 413, got ${err.status}`);
    assert.match(
      err.body,
      /context too large/,
      "413 body should name the context limit",
    );

    // Atomicity: the rejected admission persisted no instance.
    const items = await client.listInstances({ tenant_id: tenantId });
    assert.equal(items.length, 0, "no instance should be written");
  });

  it("rejects a batch containing one oversized item with 413 and writes nothing", async () => {
    const tenantId = `ent-bctx-${uuid().slice(0, 8)}`;
    const seq = testSequence("ent-bctx", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const items = Array.from({ length: 5 }, () => ({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    }));
    (items[2] as Record<string, unknown>).context = sizedContext(300 * 1024);

    let err: ApiError | undefined;
    try {
      await client.createInstancesBatch(items);
    } catch (e) {
      err = e as ApiError;
    }
    assert.ok(err, "batch with oversized item must be rejected");
    assert.equal(err.status, 413, `expected 413, got ${err.status}`);
    assert.match(
      err.body,
      /instances\[2\]: context too large/,
      "413 body should identify the offending item index",
    );

    const persisted = await client.listInstances({ tenant_id: tenantId });
    assert.equal(persisted.length, 0, "batch rejection must be atomic");
  });

  it("admits a batch spanning multiple namespaces of one tenant atomically", async () => {
    const tenantId = `ent-bns-${uuid().slice(0, 8)}`;
    const namespaces = ["alpha", "beta", "gamma"];
    const seqs = await Promise.all(
      namespaces.map(async (namespace) => {
        const seq = testSequence("ent-bns", [step("s", "noop")], {
          tenantId,
          namespace,
        });
        await client.createSequence(seq);
        return { namespace, seq };
      }),
    );

    const items = seqs.flatMap(({ namespace, seq }) =>
      Array.from({ length: 10 }, () => ({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace,
      })),
    );
    const res = await client.createInstancesBatch(items);
    assert.equal((res as any).count, 30, "all 30 admitted");

    for (const { namespace } of seqs) {
      const listed = await client.listInstances({
        tenant_id: tenantId,
        namespace,
      });
      assert.equal(listed.length, 10, `10 instances in ${namespace}`);
    }
  });

  it("admits a mixed-tenant batch per tenant and keeps the tenants isolated", async () => {
    const tenantA = `ent-ma-${uuid().slice(0, 8)}`;
    const tenantB = `ent-mb-${uuid().slice(0, 8)}`;
    const seqA = testSequence("ent-ma", [step("s", "noop")], {
      tenantId: tenantA,
    });
    const seqB = testSequence("ent-mb", [step("s", "noop")], {
      tenantId: tenantB,
    });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    const items = [
      ...Array.from({ length: 25 }, () => ({
        sequence_id: seqA.id,
        tenant_id: tenantA,
        namespace: "default",
      })),
      ...Array.from({ length: 25 }, () => ({
        sequence_id: seqB.id,
        tenant_id: tenantB,
        namespace: "default",
      })),
    ];
    const res = await client.createInstancesBatch(items);
    assert.equal((res as any).count, 50, "both tenant groups admitted");

    const listA = await client.listInstances({ tenant_id: tenantA });
    const listB = await client.listInstances({ tenant_id: tenantB });
    assert.equal(listA.length, 25);
    assert.equal(listB.length, 25);
    assert.ok(
      listA.every((i) => i.tenant_id === tenantA),
      "tenant A list must not leak tenant B rows",
    );
    assert.ok(
      listB.every((i) => i.tenant_id === tenantB),
      "tenant B list must not leak tenant A rows",
    );
  });

  it("rejects the whole batch when any item fails namespace validation", async () => {
    const tenantId = `ent-bad-ns-${uuid().slice(0, 8)}`;
    const seq = testSequence("ent-bad-ns", [step("s", "noop")], { tenantId });
    await client.createSequence(seq);

    const items = [
      { sequence_id: seq.id, tenant_id: tenantId, namespace: "default" },
      { sequence_id: seq.id, tenant_id: tenantId, namespace: "  " },
      { sequence_id: seq.id, tenant_id: tenantId, namespace: "default" },
    ];
    let err: ApiError | undefined;
    try {
      await client.createInstancesBatch(items);
    } catch (e) {
      err = e as ApiError;
    }
    assert.ok(err, "blank-namespace item must reject the batch");
    assert.equal(err.status, 400, `expected 400, got ${err.status}`);
    assert.match(err.body, /instances\[1\]: namespace must not be empty/);

    const persisted = await client.listInstances({ tenant_id: tenantId });
    assert.equal(persisted.length, 0, "no partial writes allowed");
  });

  it("keeps tenant B admission unaffected by tenant A's heavy usage", async () => {
    const tenantA = `ent-ha-${uuid().slice(0, 8)}`;
    const tenantB = `ent-hb-${uuid().slice(0, 8)}`;
    const seqA = testSequence("ent-ha", [step("s", "noop")], {
      tenantId: tenantA,
    });
    const seqB = testSequence("ent-hb", [step("s", "noop")], {
      tenantId: tenantB,
    });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    // Tenant A pushes a 1,000-instance batch through admission.
    const heavy = await client.createInstancesBatch(
      Array.from({ length: 1_000 }, () => ({
        sequence_id: seqA.id,
        tenant_id: tenantA,
        namespace: "default",
      })),
    );
    assert.equal((heavy as any).count, 1_000);

    // Tenant B's admission is evaluated against B's own (zero) usage.
    const single = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: tenantB,
      namespace: "default",
    });
    assert.ok(single.id, "tenant B single create admitted");
    const batch = await client.createInstancesBatch(
      Array.from({ length: 100 }, () => ({
        sequence_id: seqB.id,
        tenant_id: tenantB,
        namespace: "default",
      })),
    );
    assert.equal((batch as any).count, 100);

    const listB = await client.listInstances({
      tenant_id: tenantB,
      limit: 200,
    });
    assert.equal(listB.length, 101, "tenant B sees exactly its own usage");
  });

  it("admits again after a 413 rejection — rejection carries no penalty state", async () => {
    const tenantId = `ent-recover-${uuid().slice(0, 8)}`;
    const seq = testSequence("ent-recover", [step("s", "noop")], {
      tenantId,
    });
    await client.createSequence(seq);

    await assert.rejects(
      client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
        context: sizedContext(300 * 1024),
      }),
      (e: unknown) => e instanceof ApiError && e.status === 413,
    );

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const done = await client.waitForState(id, "completed", {
      timeoutMs: 15_000,
    });
    assert.equal(done.state, "completed");

    const items = await client.listInstances({ tenant_id: tenantId });
    assert.equal(items.length, 1, "only the admitted instance exists");
  });
});
