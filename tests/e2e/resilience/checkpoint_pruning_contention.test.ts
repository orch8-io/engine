/**
 * Verifies that checkpoint pruning is safe to run concurrently with writers.
 *
 * Notes on the engine surface:
 *   - `saveCheckpoint`/`listCheckpoints`/`pruneCheckpoints` operate against
 *     the instance-scoped checkpoint table (`pools-checkpoints.test.ts` shows
 *     the happy-path API). They do not require the instance to be actively
 *     executing; the tests in `pools-checkpoints.test.ts` run these on a
 *     completed instance. We reuse that shape here and exercise contention
 *     at the HTTP layer — multiple writers + a concurrent pruner.
 *   - The goal is to lock in two invariants: (a) the final `listCheckpoints`
 *     result is internally consistent (a JSON array, no torn writes leaked
 *     through) and (b) no concurrent save or prune returns an error.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Checkpoint Pruning Under Concurrent Access", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should prune old checkpoints safely while writers keep writing", async () => {
    const tenantId = `test-${uuid().slice(0, 8)}`;
    const seq = testSequence("cp-contention", [step("s1", "noop")], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed");

    // Seed with N>10 sequential checkpoints to give prune something to chew
    // through.
    const seedCount = 15;
    for (let i = 0; i < seedCount; i++) {
      await client.saveCheckpoint(id, { phase: "seed", iter: i });
    }

    // Now interleave additional saves with a prune call. Kick off the prune
    // concurrently with 5 more saves — we don't pre-serialise these, so the
    // server handles the interleaving.
    const keep = 3;
    const concurrentSaves = 5;
    const ops: Array<Promise<unknown>> = [];
    ops.push(client.pruneCheckpoints(id, keep));
    for (let i = 0; i < concurrentSaves; i++) {
      ops.push(client.saveCheckpoint(id, { phase: "concurrent", iter: i }));
    }

    // Settled variant: every op must succeed. Promise.all surfaces the first
    // rejection, so any 5xx from either path would fail the test here.
    await Promise.all(ops);

    const remaining = await client.listCheckpoints(id);
    assert.ok(Array.isArray(remaining), "listCheckpoints must return an array");
    assert.ok(
      remaining.length > 0,
      "at least some checkpoints must survive prune+concurrent writes",
    );
    // Upper bound: keep + the maximum number of concurrent writers that could
    // have landed after the prune's snapshot. We don't know the server-side
    // ordering, so allow the full concurrent save count on top of `keep`.
    assert.ok(
      remaining.length <= keep + concurrentSaves,
      `expected at most ${keep + concurrentSaves} checkpoints after prune, got ${remaining.length}`,
    );

    // A second prune with the same `keep` should drive the list down to
    // exactly `keep` (no more concurrent writers in flight).
    await client.pruneCheckpoints(id, keep);
    const finalList = await client.listCheckpoints(id);
    assert.equal(
      finalList.length,
      keep,
      `a quiescent prune(keep=${keep}) must leave exactly ${keep} checkpoints`,
    );
  });
});
