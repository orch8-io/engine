/**
 * sleep handler — delays for configured duration.
 *
 * Per `orch8-engine/src/handlers/builtin.rs::handle_sleep`, the handler
 * awaits for `duration_ms` ms (default 100) and returns
 * `{ slept_ms: <n> }`. We measure wall time from instance creation to
 * completion to verify the delay was honoured; because there's tick
 * overhead (scheduler poll latency, handler dispatch) we only assert a
 * lower bound — the duration SHOULD NOT be shorter than requested.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("sleep Handler", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("delays for at least the configured duration", async () => {
    const tenantId = `sleep-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const durationMs = 500;

    const seq = testSequence(
      "sleep-500",
      [step("nap", "sleep", { duration_ms: durationMs })],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const started = Date.now();
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    const final = await client.waitForState(id, ["completed", "failed"], {
      timeoutMs: 15_000,
      intervalMs: 25,
    });
    const elapsed = Date.now() - started;

    assert.equal(final.state, "completed");
    // Must be at least the sleep duration. Allow tiny slack (20 ms)
    // for any rounding in the polling interval or OS scheduling.
    assert.ok(
      elapsed >= durationMs - 20,
      `expected wall-clock >= ${durationMs}ms, got ${elapsed}ms`,
    );

    const outputs = await client.getOutputs(id);
    const napOut = outputs.find((o) => o.block_id === "nap");
    assert.ok(napOut, "expected sleep block output");
    assert.deepEqual(napOut.output, { slept_ms: durationMs });
  });
});
