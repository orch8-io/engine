/**
 * human_review handler with timeout escalation.
 *
 * BLOCKED BY: `RuntimeContext.started_at` is never populated in production.
 *   - `orch8-engine/src/scheduler.rs::check_human_input` at line 803 reads
 *     `instance.context.runtime.started_at` to compute elapsed time:
 *         if let Some(started) = instance.context.runtime.started_at { ... }
 *     If `None`, the timeout branch is skipped and the escalation path at
 *     lines 807–836 is never reached.
 *   - Grep across `orch8-engine/src/` shows no site that writes
 *     `runtime.started_at` outside unit-test fixtures (default is `None`).
 *   - Consequence: `wait_for_input.timeout` never fires; `escalation_handler`
 *     is never invoked. Nothing observable to assert.
 *
 * UNBLOCK (either option closes the gap):
 *   1. Populate `runtime.started_at = Some(Utc::now())` on the first
 *      `Scheduled → Running` transition inside
 *      `orch8-engine/src/lifecycle.rs::transition_instance` (the function
 *      that owns the instance-state edge).
 *   2. Rework `check_human_input` to derive elapsed time from the step's
 *      own `execution_tree` node `started_at` (already written by the
 *      evaluator) instead of the instance-level runtime marker.
 *
 * Once unblocked, flip `it.skip` → `it` — the body below runs against the
 * documented escalation BlockOutput shape produced by `check_human_input`
 * on timeout (see scheduler.rs lines 816–830: writes
 *   { "_escalated": true, "_escalation_handler": <name>, "_timeout_seconds": N }
 * as the step's BlockOutput).
 *
 * Runner note: this suite does NOT need to land in SELF_MANAGED_SUITES.
 * `human_review` is an in-process handler (see
 * `orch8-engine/src/handlers/human_review.rs`); it does not enqueue
 * worker_tasks rows, so it doesn't touch the globally-scoped worker queue.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Human Review Escalation on Timeout", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // BLOCKED BY: `RuntimeContext.started_at` never populated in prod — see
  //   file header. `check_human_input` short-circuits before reaching the
  //   timeout/escalation branch.
  // UNBLOCK: set `runtime.started_at = Some(Utc::now())` on the first
  //   `Scheduled → Running` edge inside
  //   `orch8-engine/src/lifecycle.rs::transition_instance`, OR switch
  //   `check_human_input` to read the step-node's own started_at from
  //   `execution_tree`.
  it.skip(
    "invokes escalation handler when review exceeds deadline",
    async () => {
      const tenantId = `hri-${uuid().slice(0, 8)}`;
      const seq = testSequence(
        "hri-timeout",
        [
          step(
            "s1",
            "human_review",
            { prompt: "approve?" },
            {
              wait_for_input: {
                prompt: "approve?",
                // 0.2s timeout — far shorter than the poll loop below.
                timeout: 0.2,
                escalation_handler: "log",
              },
            },
          ),
        ],
        { tenantId },
      );
      await client.createSequence(seq);

      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });

      // Deliberately DO NOT send a resume signal. Wait past the timeout.
      // The engine should mark s1 escalated and the instance should either
      // continue to completion (if escalation is recoverable) or transition
      // to failed (if it is not). Either terminal outcome is fine — the
      // assertion is on the escalation BlockOutput marker.
      await client.waitForState(id, ["completed", "failed"], {
        timeoutMs: 5_000,
      });

      // Per `scheduler.rs::check_human_input` (lines 816–830) the engine
      // writes an escalation BlockOutput for the `human_review` step with:
      //   { "_escalated": true,
      //     "_escalation_handler": "<name>",
      //     "_timeout_seconds": <u64> }
      const outputs = await client.getOutputs(id);
      const s1 = outputs.find((o) => o.block_id === "s1");
      assert.ok(s1, "s1 should have produced a BlockOutput on timeout");
      const out = s1!.output as Record<string, unknown>;
      assert.equal(out._escalated, true, "_escalated marker must be true");
      assert.equal(
        out._escalation_handler,
        "log",
        "_escalation_handler should echo the configured handler name",
      );
      assert.ok(
        typeof out._timeout_seconds === "number",
        "_timeout_seconds should be a number",
      );
    },
  );
});
