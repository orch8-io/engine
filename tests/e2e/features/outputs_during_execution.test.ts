/**
 * Outputs During Execution — verifies that GET /instances/{id}/outputs
 * returns partial outputs while the instance is still running.
 *
 * Plan #130 extension: block outputs retrievable *during* execution.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Outputs During Execution", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("returns outputs for completed steps while later steps are still running", async () => {
    const tenantId = `out-dur-${uuid().slice(0, 8)}`;

    // Sequence: first step is instant (noop), second step sleeps for 3s.
    const seq = testSequence(
      "out-during-run",
      [
        step("s1", "log", { message: "first-done" }),
        step("s2", "sleep", { duration_ms: 3000 }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Poll outputs until we see s1's output (or instance completes).
    let outputs: { block_id: string }[] = [];
    const deadline = Date.now() + 8_000;
    while (Date.now() < deadline) {
      outputs = await client.getOutputs(id);
      if (outputs.some((o) => o.block_id === "s1")) {
        break;
      }
      const inst = await client.getInstance(id);
      if (inst.state === "completed") {
        break;
      }
      await new Promise((r) => setTimeout(r, 150));
    }

    // s1's output must be visible even if s2 hasn't finished yet.
    const s1Out = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1Out, "s1 output should be retrievable before instance completes");
    assert.equal(
      (s1Out as unknown as { output: { message?: string } }).output.message,
      "first-done",
    );

    // Wait for full completion so the next suite starts clean.
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
  });

  it("returns empty outputs for a freshly created instance", async () => {
    const tenantId = `out-fresh-${uuid().slice(0, 8)}`;
    const seq = testSequence("out-fresh", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });

    // Instance is scheduled far in the future — no steps have run.
    const outputs = await client.getOutputs(id);
    assert.ok(Array.isArray(outputs));
    assert.equal(outputs.length, 0, "fresh scheduled instance should have no outputs");
  });
});
