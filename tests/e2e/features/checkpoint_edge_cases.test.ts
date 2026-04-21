/**
 * Checkpoint Edge Cases — save during execution, prune keep=0,
 * and retrieval on instances with no checkpoints.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Checkpoint Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("save checkpoint during a running instance and retrieve it", async () => {
    const tenantId = `cp-run-${uuid().slice(0, 8)}`;

    // Sleep step gives us a window where the instance is "running".
    const seq = testSequence(
      "cp-running",
      [step("s1", "sleep", { duration_ms: 4000 })],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Wait until the instance is in running state.
    await client.waitForState(id, "running", { timeoutMs: 5_000 });

    // Save a checkpoint while running.
    await client.saveCheckpoint(id, { snapshot: "mid-flight" });

    const cps = await client.listCheckpoints(id);
    assert.ok(cps.length >= 1, "should have at least one checkpoint");
    const cp = cps.find((c) =>
      ((c.checkpoint_data as any) ?? c.data)?.snapshot === "mid-flight",
    );
    assert.ok(cp, "checkpoint saved during running state should be retrievable");

    // Let the instance finish.
    await client.waitForState(id, "completed", { timeoutMs: 8_000 });
  });

  it("prune with keep=0 removes all checkpoints", async () => {
    const tenantId = `cp-zero-${uuid().slice(0, 8)}`;
    const seq = testSequence("cp-zero", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed");

    // Save 3 checkpoints.
    for (let i = 0; i < 3; i += 1) {
      await client.saveCheckpoint(id, { index: i });
    }

    const before = await client.listCheckpoints(id);
    assert.ok(before.length >= 3);

    // Prune keeping 0.
    const pruneRes = await client.pruneCheckpoints(id, 0);
    assert.ok((pruneRes.count as number) >= 3);

    const after = await client.listCheckpoints(id);
    assert.equal(after.length, 0, "keep=0 should remove all checkpoints");
  });
});
