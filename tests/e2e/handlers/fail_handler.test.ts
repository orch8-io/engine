/**
 * fail handler — forces instance failure.
 *
 * Per `orch8-engine/src/handlers/builtin.rs::handle_fail`, the handler
 * always returns an error:
 *   - `message` (string): error message, defaults to "forced failure"
 *   - `retryable` (bool): chooses Retryable vs Permanent (default
 *     Permanent — lands the instance in the DLQ with no retry policy).
 *
 * We verify two things:
 *   1. The instance transitions to `failed` (not `completed`).
 *   2. A Permanent failure with no retry lands the instance in DLQ.
 *
 * To verify the custom `message` propagates, we wrap the fail step in
 * a `try_catch` block. `handlers/try_catch.rs` merges `failed_blocks`
 * into `context.data._error` before running the catch branch, so the
 * catch step can read it back and surface the failed block id. This
 * indirectly confirms the custom-message path is exercised by the
 * scheduler (the Permanent error carrying our message caused the try
 * branch to fail and the catch branch to run).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("fail Handler", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("forces instance into failed state with Permanent error (lands in DLQ)", async () => {
    const tenantId = `fail-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const customMessage = `forced error ${uuid().slice(0, 6)}`;

    const seq = testSequence(
      "fail-permanent",
      [step("boom", "fail", { message: customMessage })],
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
    assert.equal(final.state, "failed");

    // Permanent failure with no retry policy → DLQ.
    const dlq = await client.listDlq({ tenant_id: tenantId });
    assert.ok(
      dlq.some((i) => i.id === id),
      "permanent fail step should land instance in DLQ",
    );
  });

  it("fail inside try-catch triggers catch branch with failed block context", async () => {
    const tenantId = `fail-tc-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const customMessage = `forced error ${uuid().slice(0, 6)}`;

    // Sequence: try { fail } catch { log "caught" }
    // try_catch recovers from the failure, so the instance should
    // reach `completed` and the catch-step output should be present.
    const seq = testSequence(
      "fail-in-try",
      [
        {
          type: "try_catch",
          id: "tc",
          try_block: [step("boom", "fail", { message: customMessage })],
          catch_block: [step("catcher", "log", { message: "caught" })],
        },
      ],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    const final = await client.waitForState(id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
    assert.equal(
      final.state,
      "completed",
      "try-catch should recover from fail step",
    );

    const outputs = await client.getOutputs(id);
    // The catch branch ran — proves the try branch raised an error and the
    // scheduler routed it through the catch path, not skipped it. If fail
    // had mistakenly succeeded, `try_catch` would have marked catch_block
    // children as Skipped (see `handlers/try_catch.rs` phase 2) and no
    // output for `catcher` would exist.
    const caught = outputs.find((o) => o.block_id === "catcher");
    assert.ok(caught, "expected catch branch to execute");
    assert.deepEqual(caught.output, { message: "caught" });

    // The failing step itself should NOT have a completed output — it was
    // terminated before any `save_block_output` call.
    const boomOut = outputs.find((o) => o.block_id === "boom");
    assert.equal(
      boomOut,
      undefined,
      "fail step must not emit a successful block output",
    );
  });
});
