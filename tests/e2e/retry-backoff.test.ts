import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "./client.ts";
import { startServer, stopServer } from "./harness.ts";
import type { ServerHandle } from "./harness.ts";

const client = new Orch8Client();

/**
 * Worker-task retry semantics.
 *
 * Findings from `orch8-engine/src/scheduler.rs::handle_retryable_failure`:
 *   - Retries are driven by a per-step `retry: { max_attempts, initial_backoff,
 *     max_backoff, backoff_multiplier }` block on the step definition.
 *   - On retryable failure: if `attempt < max_attempts`, instance goes back
 *     to Scheduled with a backoff; otherwise -> Failed.
 *   - Without a retry policy, a retryable failure still fails the instance
 *     immediately (same as permanent).
 *   - Permanent failure (`retryable=false`) -> Failed regardless.
 */
describe("Worker retry / backoff semantics", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("retryable failure re-dispatches a new worker task until max_attempts", async () => {
    const handler = `retry_handler_${uuid().slice(0, 8)}`;
    const maxAttempts = 3;

    const seq = testSequence("retry-until-max", [
      step("s1", handler, {}, {
        retry: {
          max_attempts: maxAttempts,
          initial_backoff: 50,
          max_backoff: 100,
          backoff_multiplier: 1.0,
        },
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Fail retryably N times. After the Nth retryable failure the instance
    // should transition to Failed (since attempt >= max_attempts).
    for (let i = 0; i < maxAttempts; i++) {
      await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
      const tasks = await client.pollWorkerTasks(handler, "worker-1");
      assert.equal(tasks.length, 1, `iteration ${i}: expected one task`);
      await client.failWorkerTask(
        tasks[0]!.id,
        "worker-1",
        `retryable failure #${i + 1}`,
        true
      );
    }

    // Eventually Failed.
    const failed = await client.waitForState(id, "failed", { timeoutMs: 15_000 });
    assert.equal(failed.state, "failed");
  });

  it("retryable failure on a step without retry policy fails immediately", async () => {
    const handler = `no_retry_policy_${uuid().slice(0, 8)}`;
    const seq = testSequence("retry-no-policy", [step("s1", handler, {})]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "waiting");
    const tasks = await client.pollWorkerTasks(handler, "worker-1");
    assert.equal(tasks.length, 1);
    // retryable=true but no retry policy on the step -> fails immediately.
    await client.failWorkerTask(tasks[0]!.id, "worker-1", "transient", true);

    const failed = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(failed.state, "failed");
  });

  it("permanent failure (retryable=false) transitions instance to failed immediately", async () => {
    const handler = `perm_fail_${uuid().slice(0, 8)}`;
    const seq = testSequence("retry-permanent", [
      step("s1", handler, {}, {
        // Even with a retry policy, retryable=false must short-circuit.
        retry: {
          max_attempts: 5,
          initial_backoff: 50,
          max_backoff: 100,
          backoff_multiplier: 1.0,
        },
      }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    await client.waitForState(id, "waiting");
    const tasks = await client.pollWorkerTasks(handler, "worker-1");
    assert.equal(tasks.length, 1);
    await client.failWorkerTask(tasks[0]!.id, "worker-1", "boom", false);

    const failed = await client.waitForState(id, "failed", { timeoutMs: 10_000 });
    assert.equal(failed.state, "failed");
  });

  it("failed instance with retry policy ends up in DLQ", async () => {
    const tenantId = `dlq-retry-${uuid().slice(0, 8)}`;
    const handler = `dlq_retry_handler_${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "retry-dlq",
      [
        step("s1", handler, {}, {
          retry: {
            max_attempts: 2,
            initial_backoff: 25,
            max_backoff: 50,
            backoff_multiplier: 1.0,
          },
        }),
      ],
      { tenantId }
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    for (let i = 0; i < 2; i++) {
      await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
      const tasks = await client.pollWorkerTasks(handler, "worker-1");
      assert.equal(tasks.length, 1);
      await client.failWorkerTask(tasks[0]!.id, "worker-1", `fail ${i}`, true);
    }

    await client.waitForState(id, "failed", { timeoutMs: 15_000 });

    const dlq = await client.listDlq({ tenant_id: tenantId });
    assert.ok(
      dlq.some((i) => i.id === id),
      "exhausted-retry instance should appear in DLQ"
    );
  });
});
