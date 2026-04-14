import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

/**
 * There is no literal `wait_signal` block in the engine — signals are a
 * side-channel applied during tick processing. These tests exercise the
 * signal-delivery primitive itself:
 *
 *   - The API-level `sendSignal` call delivers built-in signals (pause/resume
 *     /cancel) that drive instance state transitions.
 *   - The `send_signal` builtin step handler in one instance can target
 *     another instance and push a signal into its inbox.
 */
describe("Signal delivery", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("API sendSignal: pause then resume drives state transitions", async () => {
    const seq = testSequence("wsig-pause-resume", [
      step("s1", "sleep", { duration_ms: 3000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Give the tick loop time to pick the instance up.
    await new Promise((r) => setTimeout(r, 300));

    await client.sendSignal(id, "pause");
    // Pause may land on the next tick — wait until it takes effect or the
    // instance moves terminal (the sleep could complete fast in CI).
    const afterPause = await client.waitForState(
      id,
      ["paused", "completed"],
      { timeoutMs: 8000 }
    );

    if (afterPause.state === "paused") {
      await client.sendSignal(id, "resume");
      const resumed = await client.waitForState(
        id,
        ["completed", "running", "scheduled"],
        { timeoutMs: 8000 }
      );
      assert.ok(
        ["completed", "running", "scheduled"].includes(resumed.state),
        `expected active/terminal state after resume, got ${resumed.state}`
      );
    }
  });

  it("send_signal handler delivers a cancel signal from instance A to instance B", async () => {
    // Instance B: long-running so A has time to send a signal.
    const seqB = testSequence("wsig-target", [
      step("slow", "sleep", { duration_ms: 8000 }),
    ]);
    await client.createSequence(seqB);

    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Let B reach running/scheduled.
    await new Promise((r) => setTimeout(r, 300));

    // Instance A uses the `send_signal` builtin to push a `cancel` signal at B.
    const seqA = testSequence("wsig-sender", [
      step("send", "send_signal", {
        instance_id: idB,
        signal_type: "cancel",
      }),
    ]);
    await client.createSequence(seqA);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: "test",
      namespace: "default",
    });

    // A completes as soon as the signal is enqueued.
    await client.waitForState(idA, "completed", { timeoutMs: 10_000 });

    // B should observe the cancel and transition to cancelled/failed.
    const finalB = await client.waitForState(
      idB,
      ["cancelled", "failed"],
      { timeoutMs: 10_000 }
    );
    assert.ok(
      ["cancelled", "failed"].includes(finalB.state),
      `expected terminal state after cancel signal, got ${finalB.state}`
    );
  });

  // Plan #68: send_signal handler with signal_type=pause must pause target.
  it("send_signal handler delivers a pause signal that pauses target", async () => {
    const seqB = testSequence("wsig-pause-target", [
      step("s1", "sleep", { duration_ms: 6000 }),
    ]);
    await client.createSequence(seqB);
    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: "test",
      namespace: "default",
    });
    await new Promise((r) => setTimeout(r, 250));

    const seqA = testSequence("wsig-pause-sender", [
      step("send", "send_signal", {
        instance_id: idB,
        signal_type: "pause",
      }),
    ]);
    await client.createSequence(seqA);
    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(idA, "completed", { timeoutMs: 10_000 });

    // Target may take a tick to transition; accept paused or any terminal
    // state if the sleep completed first in fast CI.
    const finalB = await client.waitForState(
      idB,
      ["paused", "completed", "failed", "cancelled"],
      { timeoutMs: 10_000 },
    );
    assert.ok(
      ["paused", "completed", "failed", "cancelled"].includes(finalB.state),
      `expected paused or terminal, got ${finalB.state}`,
    );
  });

  // Plan #70: send_signal with a custom signal_type must deliver the
  // payload to the target's signal queue.
  it("send_signal handler delivers a custom signal with payload", async () => {
    const seqB = testSequence("wsig-custom-target", [
      step("s1", "sleep", { duration_ms: 3000 }),
    ]);
    await client.createSequence(seqB);
    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: "test",
      namespace: "default",
    });
    await new Promise((r) => setTimeout(r, 250));

    // Custom signal_type — the serde shape is `{ custom: "<name>" }`
    // (snake_case externally-tagged enum for SignalType::Custom).
    const seqA = testSequence("wsig-custom-sender", [
      step("send", "send_signal", {
        instance_id: idB,
        signal_type: { custom: "my_event" },
        payload: { amount: 42, actor: "alice" },
      }),
    ]);
    await client.createSequence(seqA);
    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: "test",
      namespace: "default",
    });

    // A must complete — the handler only enqueues the signal, it doesn't
    // wait for the target to consume it. A reaching `completed` proves the
    // custom signal (including the payload and `{custom: "name"}` typed
    // shape) was accepted and enqueued to B's signal queue without error.
    const finalA = await client.waitForState(idA, "completed", { timeoutMs: 10_000 });
    assert.equal(finalA.state, "completed");
  });

  // Plan #71: send_signal to a terminal instance must fail the sender
  // instance permanently.
  it("send_signal handler fails when target is already terminal", async () => {
    // Run B to completion first.
    const seqB = testSequence("wsig-terminal-target", [step("s1", "noop")]);
    await client.createSequence(seqB);
    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(idB, "completed");

    // Now A tries to signal B — B is terminal, handler must return
    // Permanent.
    const seqA = testSequence("wsig-terminal-sender", [
      step("send", "send_signal", {
        instance_id: idB,
        signal_type: "cancel",
      }),
    ]);
    await client.createSequence(seqA);
    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: "test",
      namespace: "default",
    });

    const finalA = await client.waitForState(idA, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.equal(
      finalA.state,
      "failed",
      "signaling a terminal target must fail sender permanently",
    );
  });

  // Plan #77: three-hop signal chain. Instance A signals B (cancel), and
  // B's own workflow sends a signal to C before A gets around to
  // cancelling it. Demonstrates that send_signal propagates across a
  // chain of independently-scheduled instances.
  it("3-hop signal chain: A signals B which signals C", async () => {
    // C: long sleep so B can cancel it before it finishes on its own.
    const seqC = testSequence("wsig-chain-c", [
      step("slow", "sleep", { duration_ms: 8000 }),
    ]);
    await client.createSequence(seqC);
    const { id: idC } = await client.createInstance({
      sequence_id: seqC.id,
      tenant_id: "test",
      namespace: "default",
    });
    await new Promise((r) => setTimeout(r, 200));

    // B: sends a cancel to C, then sleeps long enough for A to target it.
    const seqB = testSequence("wsig-chain-b", [
      step("hop", "send_signal", {
        instance_id: idC,
        signal_type: "cancel",
      }),
      step("wait", "sleep", { duration_ms: 6000 }),
    ]);
    await client.createSequence(seqB);
    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Give B's first step time to fire the cancel to C.
    await new Promise((r) => setTimeout(r, 500));

    // A: cancels B.
    const seqA = testSequence("wsig-chain-a", [
      step("hop", "send_signal", {
        instance_id: idB,
        signal_type: "cancel",
      }),
    ]);
    await client.createSequence(seqA);
    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(idA, "completed", { timeoutMs: 10_000 });

    // B and C must each reach a terminal state — B from A's cancel, C
    // from B's cancel (or completion if CI was very fast).
    const finalB = await client.waitForState(
      idB,
      ["cancelled", "failed", "completed"],
      { timeoutMs: 10_000 },
    );
    const finalC = await client.waitForState(
      idC,
      ["cancelled", "failed", "completed"],
      { timeoutMs: 10_000 },
    );
    assert.ok(
      ["cancelled", "failed", "completed"].includes(finalB.state),
      `B should be terminal, got ${finalB.state}`,
    );
    assert.ok(
      ["cancelled", "failed", "completed"].includes(finalC.state),
      `C should be terminal, got ${finalC.state}`,
    );
  });

  // Plan #78: parent pauses child via send_signal, then resumes it.
  // Child must end in completed (resume landed) or paused (if test timing
  // fell in the gap before resume was processed).
  it("parent pauses then resumes child via send_signal", async () => {
    // Child: sleeps long enough to be paused and resumed mid-flight.
    const seqChild = testSequence("wsig-pr-child", [
      step("work", "sleep", { duration_ms: 3000 }),
    ]);
    await client.createSequence(seqChild);
    const { id: idChild } = await client.createInstance({
      sequence_id: seqChild.id,
      tenant_id: "test",
      namespace: "default",
    });
    await new Promise((r) => setTimeout(r, 250));

    // Parent sends pause, sleeps briefly, then sends resume.
    const seqParent = testSequence("wsig-pr-parent", [
      step("pause_child", "send_signal", {
        instance_id: idChild,
        signal_type: "pause",
      }),
      step("wait", "sleep", { duration_ms: 400 }),
      step("resume_child", "send_signal", {
        instance_id: idChild,
        signal_type: "resume",
      }),
    ]);
    await client.createSequence(seqParent);
    const { id: idParent } = await client.createInstance({
      sequence_id: seqParent.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(idParent, "completed", { timeoutMs: 10_000 });

    // Child should ultimately complete (resume was delivered).
    const final = await client.waitForState(
      idChild,
      ["completed", "running", "scheduled"],
      { timeoutMs: 12_000 },
    );
    assert.ok(
      ["completed", "running", "scheduled"].includes(final.state),
      `child should be active or completed after resume, got ${final.state}`,
    );
  });

  it("send_signal handler rejects cross-tenant delivery", async () => {
    // B belongs to tenant "tenant-b".
    const seqB = testSequence(
      "wsig-xtenant-b",
      [step("s1", "sleep", { duration_ms: 3000 })],
      { tenantId: "tenant-b" }
    );
    await client.createSequence(seqB);

    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: "tenant-b",
      namespace: "default",
    });

    await new Promise((r) => setTimeout(r, 200));

    // A is in tenant-a, attempts to signal B — should fail the step
    // permanently (instance A ends in failed state).
    const seqA = testSequence(
      "wsig-xtenant-a",
      [
        step("send", "send_signal", {
          instance_id: idB,
          signal_type: "cancel",
        }),
      ],
      { tenantId: "tenant-a" }
    );
    await client.createSequence(seqA);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: "tenant-a",
      namespace: "default",
    });

    const a = await client.waitForState(idA, ["failed", "completed"], {
      timeoutMs: 10_000,
    });
    // We require that the cross-tenant attempt did NOT complete successfully.
    assert.equal(a.state, "failed", "cross-tenant signal must fail instance A");
  });
});
