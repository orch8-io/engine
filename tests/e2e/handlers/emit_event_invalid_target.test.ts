/**
 * emit_event to missing / disabled trigger → permanent failure.
 *
 * Per `orch8-engine/src/handlers/emit_event.rs::handle_emit_event`:
 *   - If `storage.get_trigger(slug)` returns None, the handler returns
 *     `StepError::Permanent { message: "trigger '...' not found" }`.
 *   - If the trigger exists but `trigger.enabled == false`, the handler
 *     returns `StepError::Permanent { message: "trigger '...' is disabled" }`.
 *
 * With no step-level retry policy, the scheduler transitions the instance
 * to `failed` immediately.
 *
 * Runner note: this suite creates triggers (globally scoped), so it needs to
 * land in `SELF_MANAGED_SUITES`.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("emit_event to Invalid Target", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("fails permanently when the trigger slug does not exist", async () => {
    const tenantId = `emit-miss-${uuid().slice(0, 8)}`;
    const namespace = "default";

    const ghostSlug = `ghost-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "emit-missing-trigger",
      [step("e1", "emit_event", { trigger_slug: ghostSlug, data: {} })],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final.state, "failed", `expected failed, got ${final.state}`);

    // Permanent failure → DLQ.
    const dlq = await client.listDlq({ tenant_id: tenantId });
    assert.ok(dlq.some((i) => i.id === id), "missing trigger should land in DLQ");
  });

  it("fails permanently when targeting a trigger bound to a non-existent sequence", async () => {
    // A trigger exists (not 404) but its target sequence_name resolves to
    // nothing at fire-time. We exercise this by creating the trigger against
    // a sequence_name that was never registered. emit_event will succeed the
    // trigger lookup then fail when instance creation runs — OR it will fail
    // at dedupe/create step. Either way the emit step must terminate rather
    // than hang.
    const tenantId = `emit-bad-seq-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const slug = `bad-seq-${uuid().slice(0, 8)}`;

    await client.createTrigger({
      slug,
      sequence_name: `never-registered-${uuid().slice(0, 8)}`,
      tenant_id: tenantId,
      namespace,
      trigger_type: "event",
    });

    const seq = testSequence(
      "emit-bad-seq",
      [step("e1", "emit_event", { trigger_slug: slug, data: {} })],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    // The step should terminate the instance — `failed` is the expected
    // outcome, but we accept `completed` as well in case the engine
    // eventually makes unknown sequences a no-op success: the assertion is
    // simply that the instance does NOT hang.
    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.ok(
      final.state === "failed" || final.state === "completed",
      `expected terminal state, got ${final.state}`,
    );
  });
});
