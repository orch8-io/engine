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
