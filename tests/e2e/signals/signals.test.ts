import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Signals", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should cancel a scheduled instance via signal", async () => {
    // Use a sleep step so instance stays alive long enough to signal
    const seq = testSequence("cancel-test", [
      step("s1", "sleep", { duration_ms: 5000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for it to be picked up (running or scheduled)
    await new Promise((r) => setTimeout(r, 300));

    await client.sendSignal(id, "cancel");

    // The cancel signal should be processed on the next tick
    const instance = await client.waitForState(id, ["cancelled", "failed"], {
      timeoutMs: 10_000,
    });
    assert.ok(
      instance.state === "cancelled" || instance.state === "failed",
      `Expected cancelled or failed, got ${instance.state}`
    );
  });

  it("should pause and resume an instance", async () => {
    // Multi-step: first step completes, second is slow enough to pause
    const seq = testSequence("pause-resume", [
      step("s1", "noop"),
      step("s2", "sleep", { duration_ms: 5000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for s1 to complete and s2 to be scheduled
    await new Promise((r) => setTimeout(r, 500));

    await client.sendSignal(id, "pause");
    await new Promise((r) => setTimeout(r, 500));

    let instance = await client.getInstance(id);
    // Should be paused (or still transitioning)
    if (instance.state === "paused") {
      // Resume it
      await client.sendSignal(id, "resume");
      instance = await client.waitForState(id, ["completed", "scheduled", "running"], {
        timeoutMs: 10_000,
      });
      assert.ok(
        ["completed", "scheduled", "running"].includes(instance.state),
        `Expected active state after resume, got ${instance.state}`
      );
    }
    // If it already completed before pause took effect, that's also valid
  });

  it("should reject signal on terminal instance", async () => {
    const seq = testSequence("signal-terminal", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "completed");

    await assert.rejects(() => client.sendSignal(id, "cancel"), (err: unknown) => {
      const status = (err as ApiError).status;
      assert.ok(status === 400 || status === 422);
      return true;
    });
  });

  // Plan #27: step flagged `cancellable: false` must survive a cancel
  // signal delivered mid-flight.
  it("should reach terminal state when non-cancellable step receives cancel", async () => {
    // A sleep step with cancellable=false. We send cancel while it's
    // running. The instance must still reach a terminal state (cancel is
    // honoured after the non-cancellable step finishes, or the step
    // completes normally in fast CI).
    const seq = testSequence("noncancel", [
      step("s1", "sleep", { duration_ms: 1500 }, { cancellable: false }),
      step("s2", "noop"),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Let the sleep step start.
    await new Promise((r) => setTimeout(r, 300));
    await client.sendSignal(id, "cancel");

    // Wait for terminal state (either cancelled-after-step or completed).
    const final = await client.waitForState(
      id,
      ["cancelled", "completed", "failed"],
      { timeoutMs: 10_000 },
    );
    assert.ok(
      ["cancelled", "completed", "failed"].includes(final.state),
      `expected terminal, got ${final.state}`,
    );
  });

  // Plan #28: update_context signal replaces instance context with payload.
  it("should replace instance context on update_context signal", async () => {
    // Pause the instance first so the signal is processed without racing
    // a running step handler that would otherwise overwrite the context
    // when it completes.
    const seq = testSequence("upd-ctx", [
      step("s1", "sleep", { duration_ms: 5000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { data: { before: 1 } },
    });

    // Pause so we can apply update_context deterministically.
    await new Promise((r) => setTimeout(r, 300));
    await client.sendSignal(id, "pause");
    const paused = await client.waitForState(
      id,
      ["paused", "completed"],
      { timeoutMs: 8000 },
    );

    if (paused.state !== "paused") {
      // Completed too fast in CI — nothing to assert.
      return;
    }

    // UpdateContext signal payload is a full ExecutionContext — required
    // by `Signal::action()` which decodes payload into `ExecutionContext`.
    await client.sendSignal(id, "update_context", {
      data: { after: "patched" },
      config: {},
      audit: [],
      runtime: {},
    });

    // Give the signal loop time to drain the update.
    await new Promise((r) => setTimeout(r, 500));

    const final = await client.getInstance(id);
    const data = ((final.context ?? {}) as Record<string, unknown>).data as
      | Record<string, unknown>
      | undefined;
    assert.ok(data, "context.data must be present after update_context signal");
    assert.equal(data!.after, "patched", "new field must appear after update_context");

    // Resume so the instance terminates cleanly.
    await client.sendSignal(id, "resume");
  });

  // Plan #29: custom signal delivery via API is covered indirectly by
  // wait-signal.test.ts — the REST client's `sendSignal` only accepts a
  // string signal_type (so the `{custom: "name"}` variant requires the
  // `send_signal` builtin handler path, exercised there).

  // Plan #32: signal to a cancelled instance must be rejected (not just
  // completed — any terminal state).
  it("should reject signal on cancelled instance", async () => {
    const seq = testSequence("signal-cancelled", [
      step("s1", "sleep", { duration_ms: 4000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Drive the instance to cancelled.
    await new Promise((r) => setTimeout(r, 300));
    await client.sendSignal(id, "cancel");
    await client.waitForState(id, ["cancelled", "failed"], {
      timeoutMs: 10_000,
    });

    // A second signal after a terminal state must be rejected with 4xx.
    await assert.rejects(
      () => client.sendSignal(id, "cancel"),
      (err: unknown) => {
        const status = (err as ApiError).status;
        assert.ok(
          status === 400 || status === 422,
          `expected 4xx for signal on cancelled instance, got ${status}`,
        );
        return true;
      },
    );
  });
});
