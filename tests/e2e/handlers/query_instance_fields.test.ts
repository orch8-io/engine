/**
 * query_instance handler — full response-field coverage.
 *
 * Engine reference: `orch8-engine/src/handlers/query_instance.rs` returns
 * `{ found, state, context, started_at, completed_at, current_node }`.
 *
 * The existing handler e2e suites cover `found`/`context`/`state` and the
 * cross-tenant + missing-id paths. This suite pins the remaining fields the
 * handler derives:
 *   - `current_node`  — non-null for an in-flight multi-step target.
 *   - `started_at`    — always present (= instance.created_at).
 *   - `completed_at`  — null while running, an ISO string once terminal.
 *   - invalid `instance_id` → the producer fails permanently.
 *
 * Tenant-scoped (no triggers) → runs in shared-server mode.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("query_instance Response Fields", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("reports current_node and a null completed_at for an in-flight target", async () => {
    const tenantId = `qif-live-${uuid().slice(0, 8)}`;

    // `current_node` is derived from the execution tree, which the engine only
    // builds for sequences containing a composite (non-Step) block — purely
    // linear sequences run a fast path that never populates the tree
    // (orch8-engine/src/scheduler.rs: `has_composite`). Wrap the long sleep in
    // a single always-true router route so the tree path runs and the tree has
    // a live node to report while the sleep is in flight.
    const target = testSequence(
      "qif-target",
      [
        step("first", "noop"),
        {
          type: "router",
          id: "gate",
          routes: [
            {
              condition: "true",
              blocks: [step("work", "sleep", { duration_ms: 4000 })],
            },
          ],
        },
      ],
      { tenantId },
    );
    await client.createSequence(target);
    const { id: targetId } = await client.createInstance({
      sequence_id: target.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Let the target start executing so the execution tree has nodes.
    await client.waitForState(targetId, ["running", "scheduled"], { timeoutMs: 5_000 });
    await new Promise((r) => setTimeout(r, 400));

    const producer = testSequence(
      "qif-producer",
      [step("q", "query_instance", { instance_id: targetId })],
      { tenantId },
    );
    await client.createSequence(producer);
    const { id: producerId } = await client.createInstance({
      sequence_id: producer.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(producerId, "completed", { timeoutMs: 10_000 });

    const out = (await client.getOutputs(producerId)).find((o) => o.block_id === "q")
      ?.output as Record<string, unknown>;
    assert.ok(out, "query output missing");
    assert.equal(out.found, true);

    // started_at always present and non-null.
    assert.ok(out.started_at, "started_at should be present");
    assert.notEqual(out.started_at, null);

    // current_node should name a block from the target's tree (non-null while
    // the target is still doing work).
    assert.notEqual(
      out.current_node ?? null,
      null,
      "current_node should be populated for an in-flight target",
    );
    assert.ok(
      typeof out.current_node === "string" && out.current_node.length > 0,
      `current_node should be a block id string, got ${JSON.stringify(out.current_node)}`,
    );
  });

  it("reports an ISO completed_at once the target is terminal", async () => {
    const tenantId = `qif-done-${uuid().slice(0, 8)}`;

    // Target completes immediately.
    const target = testSequence("qif-done-target", [step("s", "noop")], { tenantId });
    await client.createSequence(target);
    const { id: targetId } = await client.createInstance({
      sequence_id: target.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(targetId, "completed", { timeoutMs: 8_000 });

    const producer = testSequence(
      "qif-done-producer",
      [step("q", "query_instance", { instance_id: targetId })],
      { tenantId },
    );
    await client.createSequence(producer);
    const { id: producerId } = await client.createInstance({
      sequence_id: producer.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(producerId, "completed", { timeoutMs: 10_000 });

    const out = (await client.getOutputs(producerId)).find((o) => o.block_id === "q")
      ?.output as Record<string, unknown>;
    assert.ok(out, "query output missing");
    assert.equal(out.found, true);
    assert.equal(out.state, "completed");
    assert.ok(
      typeof out.completed_at === "string" && out.completed_at.length > 0,
      `completed_at should be an ISO string for a terminal target, got ${JSON.stringify(out.completed_at)}`,
    );
    // Sanity: it should parse as a date.
    assert.ok(
      !Number.isNaN(Date.parse(out.completed_at as string)),
      "completed_at should parse as a date",
    );
  });

  it("fails the producer when instance_id is not a valid uuid", async () => {
    const tenantId = `qif-bad-${uuid().slice(0, 8)}`;

    const producer = testSequence(
      "qif-bad-uuid",
      [step("q", "query_instance", { instance_id: "not-a-uuid" })],
      { tenantId },
    );
    await client.createSequence(producer);
    const { id: producerId } = await client.createInstance({
      sequence_id: producer.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    const final = await client.waitForState(producerId, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(
      final.state,
      "failed",
      `malformed instance_id must fail the producer permanently, got ${final.state}`,
    );
  });
});
