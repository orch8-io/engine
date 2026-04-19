/**
 * human_review handler with timeout escalation.
 *
 * `runtime.started_at` is now populated on the first Scheduled→Running
 * transition (scheduler.rs::process_instance). `check_human_input` reads
 * this to compute elapsed time against `wait_for_input.timeout`.
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

  it(
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
