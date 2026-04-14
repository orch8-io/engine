/**
 * SLA Timers — verifies that `deadline` on a step triggers the deadline
 * breach path (and optionally the configured `on_deadline_breach` escalation)
 * exactly when breached, and never when the step completes in time.
 * Closes the gap around deadline-driven escalation paths.
 *
 * Engine contract (`orch8-engine/src/evaluator.rs::check_sla_deadlines`):
 *   - Deadlines are checked against `node.started_at` for nodes in Running
 *     or Waiting state. To exercise the path we need a step that STAYS in
 *     Waiting past the deadline — the simplest way is to dispatch to an
 *     external worker queue and never complete the task.
 *   - On breach: the node is failed and a BlockOutput with
 *     `output._error == "sla_deadline_breached"` is saved for diagnostics.
 *   - The `on_deadline_breach.handler` must be an in-process built-in (the
 *     scheduler only dispatches escalation via the in-memory HandlerRegistry);
 *     we use `log` to exercise it.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("SLA Timers", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("fails the step and records breach output when deadline is exceeded", async () => {
    const tenantId = `sla-breach-${uuid().slice(0, 8)}`;
    // Unknown handler → dispatched to external worker queue → node stays
    // Waiting until a worker completes it (nobody does) → deadline fires.
    const handler = `sla_no_worker_${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "sla-breach",
      [
        step("slow", handler, {}, {
          deadline: 300, // ms
          on_deadline_breach: {
            handler: "log",
            params: { message: "sla breach escalation" },
          },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Instance enters Waiting once the external task is dispatched.
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Deadline check runs on subsequent ticks; instance should fail.
    const final = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(final.state, "failed");

    // Diagnostic output recorded by the breach path.
    const outputs = await client.getOutputs(id);
    const slowOut = outputs.find((o) => o.block_id === "slow");
    assert.ok(slowOut, "expected breach output for step 'slow'");
    const body = slowOut.output as { _error?: string };
    assert.equal(body._error, "sla_deadline_breached");
  });

  it("does not fire breach path when step completes within deadline", async () => {
    const tenantId = `sla-ok-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "sla-in-time",
      [
        step("fast", "noop", {}, {
          deadline: 10_000, // ample
          on_deadline_breach: {
            handler: "log",
            params: { message: "should not fire" },
          },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(final.state, "completed");

    const outputs = await client.getOutputs(id);
    const fastOut = outputs.find((o) => o.block_id === "fast");
    assert.ok(fastOut, "expected success output for step 'fast'");
    const body = fastOut.output as { _error?: string };
    assert.notEqual(body._error, "sla_deadline_breached");
  });
});
