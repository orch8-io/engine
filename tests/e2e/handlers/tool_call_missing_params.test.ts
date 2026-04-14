/**
 * tool_call handler: required params missing.
 *
 * Per `orch8-engine/src/handlers/tool_call.rs::handle_tool_call`, the `url`
 * param is required — if absent (or not a string) the handler returns
 * `StepError::Permanent { message: "missing required param: url" }`. With no
 * retry policy on the step, the scheduler transitions the instance to
 * `failed` immediately (see `handle_retryable_failure` / the permanent
 * short-circuit in `handle_step_error`).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("tool_call Missing Required Params", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("fails permanently when url param is missing", async () => {
    const tenantId = `tool-${uuid().slice(0, 8)}`;
    const namespace = "default";

    // No `url` param — handler returns Permanent immediately.
    const seq = testSequence(
      "tool-no-url",
      [step("t1", "tool_call", { tool_name: "search" })],
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

    // A permanent failure with no retry lands the instance in the DLQ.
    const dlq = await client.listDlq({ tenant_id: tenantId });
    assert.ok(
      dlq.some((i) => i.id === id),
      "permanent failure should land in DLQ",
    );
  });
});
