/**
 * Wait-for-input Waiting state — verifies that the fast-path scheduler
 * transitions instances with `wait_for_input` steps to `waiting` state
 * (not stuck at `scheduled`), and that signal delivery wakes them.
 *
 * This was a real bug: the fast path used to reschedule to `scheduled`
 * instead of transitioning to `waiting`, making the approvals API unable
 * to discover pending reviews.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

async function sendHumanInput(
  instanceId: string,
  blockId: string,
  payload: Record<string, unknown>,
): Promise<void> {
  await client.sendSignal(
    instanceId,
    { custom: `human_input:${blockId}` } as unknown as string,
    payload,
  );
}

describe("Wait-for-input Waiting State", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("fast-path step with wait_for_input reaches waiting state", async () => {
    const tenantId = `wfi-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "wfi-waiting",
      [
        step("review", "human_review", {}, {
          wait_for_input: {
            prompt: "Approve?",
            choices: [
              { label: "Yes", value: "yes" },
              { label: "No", value: "no" },
            ],
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

    // Must reach 'waiting', not stay at 'scheduled'.
    const inst = await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
    assert.equal(inst.state, "waiting");
  });

  it("signal wakes instance from waiting and completes it", async () => {
    const tenantId = `wfi-sig-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "wfi-signal",
      [
        step("approval", "human_review", {}, {
          wait_for_input: {
            prompt: "Confirm",
            choices: [
              { label: "Approve", value: "approve" },
              { label: "Reject", value: "reject" },
            ],
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

    await client.waitForState(id, "waiting", { timeoutMs: 10_000 });

    // Send the approval signal.
    await sendHumanInput(id, "approval", { value: "approve" });

    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(final.state, "completed");

    const outputs = await client.getOutputs(id);
    const approval = outputs.find((o) => o.block_id === "approval");
    assert.ok(approval, "approval step should produce output");
  });

  it("multiple wait_for_input steps transition through waiting correctly", async () => {
    const tenantId = `wfi-multi-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "wfi-multi",
      [
        step("step1", "human_review", {}, {
          wait_for_input: { prompt: "First approval" },
        }) as Block,
        step("step2", "human_review", {}, {
          wait_for_input: { prompt: "Second approval" },
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

    // First step waits.
    await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
    await sendHumanInput(id, "step1", { value: "yes" });

    // Second step waits.
    await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
    await sendHumanInput(id, "step2", { value: "yes" });

    // Both steps done → completed.
    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(final.state, "completed");
  });
});
