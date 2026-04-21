/**
 * Signal Update Context — verifies that the `update_context` signal merges
 * new data into a running instance and that custom signals carry payloads.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Signal Update Context", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("update_context signal replaces instance context data", async () => {
    const tenantId = `sig-ctx-${uuid().slice(0, 8)}`;

    // Use a paused instance so the signal is processed by the
    // signalled-instance path (paused/waiting instances are checked
    // for signals independently of the normal claim cycle).
    const seq = testSequence(
      "sig-ctx",
      [step("s1", "noop")],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { original: true } },
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });

    // Pause the instance so signal processing kicks in.
    await client.updateState(id, "paused");
    await client.waitForState(id, "paused", { timeoutMs: 3_000 });

    // Send update_context signal with a full ExecutionContext payload.
    await client.sendSignal(id, "update_context", {
      data: { injected: "value" },
      config: {},
      audit: [],
      runtime: { current_step: null, attempt: 0, started_at: null, resource_key: null },
    });

    // Wait for the scheduler to process the signal.
    await new Promise((r) => setTimeout(r, 1_500));

    const after = await client.getInstance(id);
    const data = (after.context as any).data as Record<string, unknown>;
    assert.ok(data, "context.data should be present");
    assert.equal(data.injected, "value", "injected key should appear");

    // Clean up: resume so the instance can drain.
    await client.updateState(id, "scheduled");
    await client.waitForState(id, "completed", { timeoutMs: 8_000 });
  });

  it("custom signal with payload is accepted by a running instance", async () => {
    const tenantId = `sig-cust-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "sig-cust",
      [step("s1", "sleep", { duration_ms: 3000 })],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "running", { timeoutMs: 5_000 });

    // Send a custom signal with a structured payload.
    // SignalType::Custom is a newtype variant — use { custom: "name" }.
    await client.sendSignal(
      id,
      { custom: "my_custom_event" } as unknown as string,
      {
        nested: { value: 42 },
        tags: ["a", "b"],
      },
    );

    // The signal should be accepted without error. We verify the instance
    // still completes normally after receiving the custom signal.
    await client.waitForState(id, "completed", { timeoutMs: 8_000 });
  });
});
