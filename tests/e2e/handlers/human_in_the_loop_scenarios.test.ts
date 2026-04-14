/**
 * Human-in-the-loop scenarios for the redesigned `wait_for_input` flow.
 *
 * Engine contract (Tasks 1-6 of the plan):
 *   - `HumanInputDef` carries optional `choices: [{label, value}]` and
 *     `store_as: string`. Missing `choices` means default yes/no preset.
 *   - Signal name is `human_input:<block_id>` with payload `{"value":"<s>"}`.
 *   - Invalid payloads (shape wrong or value not in effective choices) are
 *     marked delivered (to avoid poison replay) but the block stays waiting
 *     — the next valid signal resumes it.
 *   - Accepted value lands at `context.data[store_as or block_id]` and as
 *     a block output `{"value":"<s>"}`.
 *
 * These e2e tests drive the full path: REST create → scheduler picks up
 * block → sits waiting → signal POST → scheduler validates + merges →
 * instance either stays waiting or completes.
 *
 * Signal wire shape: `signal_type: { custom: "human_input:<block_id>" }`
 * (the serde representation of `SignalType::Custom(String)`). The Orch8
 * client's `sendSignal(id, signalType, payload)` accepts a string, but the
 * custom variant is an object — cast per the pattern in
 * `workflow_interceptors.test.ts`.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block, Instance } from "../client.ts";

const client = new Orch8Client();

/** Send a `human_input:<block_id>` custom signal. */
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

/** Poll until `cb(instance)` returns true, or timeout. */
async function waitUntil(
  instanceId: string,
  predicate: (inst: Instance) => boolean,
  { timeoutMs = 5_000, intervalMs = 50 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<Instance> {
  const deadline = Date.now() + timeoutMs;
  let last: Instance | null = null;
  while (Date.now() < deadline) {
    last = await client.getInstance(instanceId);
    if (predicate(last)) return last;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(
    `waitUntil timed out; last instance state=${last?.state ?? "<none>"} context=${JSON.stringify(last?.context)}`,
  );
}

/** Read `context.data` off an instance, normalising missing keys to {}. */
function contextData(inst: Instance): Record<string, unknown> {
  const ctx = (inst.context ?? {}) as Record<string, unknown>;
  return (ctx.data ?? {}) as Record<string, unknown>;
}

/**
 * Build a `human_review` step with `wait_for_input` config. The handler
 * name can be anything the registry resolves — `human_review` is the
 * canonical one. The scheduler validates the signal independently of the
 * handler choice, but `human_review` is what real authors would use.
 */
function humanStep(
  id: string,
  waitForInput: Record<string, unknown> = {},
): Block {
  return step(id, "human_review", { prompt: "pick" }, {
    wait_for_input: { prompt: "pick", ...waitForInput },
  });
}

describe("human-in-the-loop scenarios", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("default yes/no → 'yes' completes and stores under block_id", async () => {
    const seq = testSequence("hitl-default-yes", [humanStep("review")]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for the block to actually reach the waiting branch once (the
    // scheduler needs to run the step, hit wait_for_input, then defer).
    await new Promise((r) => setTimeout(r, 250));

    await sendHumanInput(id, "review", { value: "yes" });

    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(final.state, "completed");
    assert.equal(contextData(final).review, "yes");
  });

  it("default yes/no → 'no' completes with value 'no'", async () => {
    const seq = testSequence("hitl-default-no", [humanStep("review")]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await new Promise((r) => setTimeout(r, 250));
    await sendHumanInput(id, "review", { value: "no" });

    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(contextData(final).review, "no");
  });

  it("default yes/no → 'maybe' is ignored; later 'yes' resumes", async () => {
    const seq = testSequence("hitl-default-invalid-then-valid", [
      humanStep("review"),
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await new Promise((r) => setTimeout(r, 250));

    // Invalid value — scheduler marks signal delivered but stays waiting.
    await sendHumanInput(id, "review", { value: "maybe" });

    // Give the scheduler a couple of ticks to process-and-reject the signal.
    await new Promise((r) => setTimeout(r, 400));

    const mid = await client.getInstance(id);
    assert.notEqual(
      mid.state,
      "completed",
      `instance should still be running/scheduled, got ${mid.state}`,
    );

    // Now send a valid value — block resumes.
    await sendHumanInput(id, "review", { value: "yes" });
    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(contextData(final).review, "yes");
  });

  it("advanced 3-choice → 'escalate' stored under store_as='decision'", async () => {
    const seq = testSequence("hitl-adv-escalate", [
      humanStep("review", {
        store_as: "decision",
        choices: [
          { label: "Approve", value: "approve" },
          { label: "Reject", value: "reject" },
          { label: "Escalate", value: "escalate" },
        ],
      }),
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await new Promise((r) => setTimeout(r, 250));
    await sendHumanInput(id, "review", { value: "escalate" });

    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(contextData(final).decision, "escalate");
    // Block id key must NOT be populated when store_as is set.
    assert.equal(contextData(final).review, undefined);
  });

  it("advanced → invalid value ignored; valid value resumes", async () => {
    const seq = testSequence("hitl-adv-invalid-then-valid", [
      humanStep("review", {
        store_as: "decision",
        choices: [
          { label: "Approve", value: "approve" },
          { label: "Reject", value: "reject" },
          { label: "Escalate", value: "escalate" },
        ],
      }),
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await new Promise((r) => setTimeout(r, 250));

    await sendHumanInput(id, "review", { value: "whatever" });
    await new Promise((r) => setTimeout(r, 400));

    const mid = await client.getInstance(id);
    assert.notEqual(
      mid.state,
      "completed",
      `instance should still be waiting, got ${mid.state}`,
    );
    assert.equal(contextData(mid).decision, undefined);

    await sendHumanInput(id, "review", { value: "approve" });
    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(contextData(final).decision, "approve");
  });

  it("advanced + router → each value routes to the correct branch", async () => {
    // One sequence: human_review then router reading context.data.decision.
    // Run it three times (separate instances), one per value. Each run
    // should produce the block output for exactly one branch.
    const buildSeq = (): ReturnType<typeof testSequence> =>
      testSequence("hitl-router", [
        humanStep("review", {
          store_as: "decision",
          choices: [
            { label: "Approve", value: "approve" },
            { label: "Reject", value: "reject" },
            { label: "Escalate", value: "escalate" },
          ],
        }),
        {
          type: "router",
          id: "route",
          routes: [
            {
              condition: "{{ context.data.decision == 'approve' }}",
              blocks: [step("branch_approve", "log", { message: "approve" })],
            },
            {
              condition: "{{ context.data.decision == 'reject' }}",
              blocks: [step("branch_reject", "log", { message: "reject" })],
            },
            {
              condition: "{{ context.data.decision == 'escalate' }}",
              blocks: [step("branch_escalate", "log", { message: "escalate" })],
            },
          ],
        } as Block,
      ]);

    const cases: Array<{ value: string; expected: string; forbidden: string[] }> = [
      {
        value: "approve",
        expected: "branch_approve",
        forbidden: ["branch_reject", "branch_escalate"],
      },
      {
        value: "reject",
        expected: "branch_reject",
        forbidden: ["branch_approve", "branch_escalate"],
      },
      {
        value: "escalate",
        expected: "branch_escalate",
        forbidden: ["branch_approve", "branch_reject"],
      },
    ];

    for (const c of cases) {
      const seq = buildSeq();
      await client.createSequence(seq);
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: "test",
        namespace: "default",
      });

      await new Promise((r) => setTimeout(r, 250));
      await sendHumanInput(id, "review", { value: c.value });
      await client.waitForState(id, "completed", { timeoutMs: 10_000 });

      const outputIds = new Set((await client.getOutputs(id)).map((o) => o.block_id));
      assert.ok(
        outputIds.has(c.expected),
        `value=${c.value}: expected branch ${c.expected} in outputs, got ${[...outputIds].join(",")}`,
      );
      for (const f of c.forbidden) {
        assert.ok(
          !outputIds.has(f),
          `value=${c.value}: forbidden branch ${f} unexpectedly ran`,
        );
      }
    }
  });

  it("back-compat removal: old-shape {approved:true} ignored; new-shape resumes", async () => {
    const seq = testSequence("hitl-back-compat", [humanStep("review")]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await new Promise((r) => setTimeout(r, 250));

    // Old-shape payload — must be ignored (no `value` key).
    await sendHumanInput(id, "review", { approved: true });
    await new Promise((r) => setTimeout(r, 400));

    const mid = await client.getInstance(id);
    assert.notEqual(
      mid.state,
      "completed",
      `old-shape payload must not complete the block; got ${mid.state}`,
    );
    assert.equal(contextData(mid).review, undefined);

    // New-shape signal resumes.
    await sendHumanInput(id, "review", { value: "yes" });
    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(contextData(final).review, "yes");
  });
});

// Silence unused-import warning — `waitUntil` is a reusable helper left in
// place for future scenarios (e.g., asserting `waiting` state explicitly).
void waitUntil;
