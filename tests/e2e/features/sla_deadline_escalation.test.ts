/**
 * SLA deadline escalation — verifies that deadline breaches trigger
 * escalation handlers, and that the breach metadata is available in
 * block outputs.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

describe("SLA Deadline Escalation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("deadline breach produces error output with metadata", async () => {
    const tenantId = `sla-esc-${uuid().slice(0, 8)}`;

    // An external worker step that nobody will complete, with a short deadline.
    const seq = testSequence(
      "sla-breach",
      [
        step("slow", `unregistered_${uuid().slice(0, 8)}`, {}, {
          deadline: 500,
        }) as Block,
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Wait for the instance to fail due to deadline breach.
    const final = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(final.state, "failed");

    const outputs = await client.getOutputs(id);
    const breachOutput = outputs.find((o) => o.block_id === "slow");
    assert.ok(breachOutput, "breached step should have output");

    const out = breachOutput.output as Record<string, unknown>;
    assert.equal(out._error, "sla_deadline_breached", "should have breach error");
    assert.ok(out._deadline_ms !== undefined, "should have deadline_ms");
    assert.ok(out._elapsed_ms !== undefined, "should have elapsed_ms");
  });

  it("deadline breach with escalation handler runs the handler", async () => {
    const tenantId = `sla-handler-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "sla-escalate",
      [
        step("timeout_step", `unregistered_${uuid().slice(0, 8)}`, {}, {
          deadline: 500,
          on_deadline_breach: {
            handler: "log",
            params: { message: "ESCALATION FIRED" },
          },
        }) as Block,
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    const final = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(final.state, "failed");

    // The escalation handler (log) fired, but it's fire-and-forget — the
    // primary step still fails. The breach output should exist.
    const outputs = await client.getOutputs(id);
    const breach = outputs.find((o) => o.block_id === "timeout_step");
    assert.ok(breach, "breach output should exist");
    assert.equal((breach.output as any)._error, "sla_deadline_breached");
  });

  it("step completes before deadline — no breach", async () => {
    const tenantId = `sla-ok-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "sla-no-breach",
      [
        step("fast", "noop", {}, {
          deadline: 5000,
        }) as Block,
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
    const fast = outputs.find((o) => o.block_id === "fast");
    assert.ok(fast, "fast step should have output");
    assert.ok(
      !(fast.output as any)?._error,
      "no error should be present when step completes before deadline",
    );
  });
});
