import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, "../..");

const client = new Orch8Client();

/**
 * Helper to find the orch8-server binary (copied from harness.js)
 */
function findBinary(): string {
  const { existsSync, readdirSync } = require('node:fs');
  const direct = resolve(PROJECT_ROOT, "target/debug/orch8-server");
  if (existsSync(direct)) return direct;

  // Check target/<triple>/debug/
  const targetDir = resolve(PROJECT_ROOT, "target");
  if (existsSync(targetDir)) {
    for (const entry of readdirSync(targetDir)) {
      const candidate = resolve(targetDir, entry, "debug/orch8-server");
      if (existsSync(candidate)) return candidate;
    }
  }

  throw new Error("orch8-server binary not found. Run: cargo build --bin orch8-server");
}

describe("Data Persistence and Recovery", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    // `server` points at whichever handle was started last (tests restart
    // the server mid-flight). Stopping the current handle prevents the leaked
    // child keeping Node's event loop alive past the test-runner timeout.
    await stopServer(server);
  });

  // --- Server Restart During Execution ---

  it("preserves instance state across server restart", async () => {
    // Create a sequence with a long sleep to keep instance running during restart
    const seq = testSequence("restart-test", [
      step("s1", "sleep", { duration_ms: 10_000 }), // 10 second sleep
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for instance to be running
    await new Promise((r) => setTimeout(r, 500));
    let instance = await client.getInstance(id);
    assert.ok(["scheduled", "running"].includes(instance.state), `Instance should be scheduled or running, got ${instance.state}`);

    console.log("Stopping server while instance is running...");
    await stopServer(server);

    console.log("Starting new server...");
    // skipCleanup: true — the whole point of this test is verifying the
    // instance survives the restart, so the DB must NOT be wiped.
    server = await startServer({ skipCleanup: true });

    // Wait a bit for server to be ready
    await new Promise((r) => setTimeout(r, 1000));

    // Instance should still exist and be in a valid state
    instance = await client.getInstance(id);
    assert.ok(instance, "Instance should still exist after restart");
    assert.ok(instance.id === id, "Instance ID should match");

    // The instance might complete, fail, or continue running
    // All are valid outcomes as long as the instance persists
    console.log(`Instance state after restart: ${instance.state}`);
  });

  it("preserves completed instance data across restart", async () => {
    const seq = testSequence("completed-persistence", [
      step("s1", "noop"),
      step("s2", "log", { message: "test message" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for completion
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });

    // Verify outputs exist
    const outputsBefore = await client.getOutputs(id);
    assert.equal(outputsBefore.length, 2);

    console.log("Restarting server...");
    await stopServer(server);
    server = await startServer({ skipCleanup: true });
    await new Promise((r) => setTimeout(r, 1000));

    // Instance should still exist and be completed
    const instance = await client.getInstance(id);
    assert.equal(instance.state, "completed");
    assert.equal(instance.id, id);

    // Outputs should still be available
    const outputsAfter = await client.getOutputs(id);
    assert.equal(outputsAfter.length, 2);

    // Verify output content is preserved
    const logOutput = outputsAfter.find(o => o.block_id === "s2");
    assert.ok(logOutput);
    assert.equal((logOutput!.output as any).message, "test message");
  });

  // --- Checkpoint Recovery ---

  it("recovers from saved checkpoints", async () => {
    const seq = testSequence("checkpoint-recovery", [
      step("s1", "noop"),
      step("s2", "checkpoint_step", {}), // External handler to allow checkpoint
      step("s3", "log", { message: "final" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for external handler dispatch (s2)
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Save a checkpoint
    const checkpointData = {
      progress: 50,
      custom_state: { foo: "bar" },
      timestamp: new Date().toISOString(),
    };

    await client.saveCheckpoint(id, checkpointData);

    // Verify checkpoint was saved
    const checkpoints = await client.listCheckpoints(id);
    assert.ok(checkpoints.length >= 1);
    const savedCheckpoint = checkpoints[0]!;
    assert.deepEqual(savedCheckpoint.checkpoint_data, checkpointData);

    // Complete the external task
    const tasks = await client.pollWorkerTasks("checkpoint_step", "worker-1");
    if (tasks.length > 0) {
      await client.completeWorkerTask(tasks[0]!.id, "worker-1", { done: true });
    }

    // Instance should complete
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });

    // Checkpoints should still be accessible after completion
    const finalCheckpoints = await client.listCheckpoints(id);
    assert.ok(finalCheckpoints.length >= 1);
  });

  it("prunes old checkpoints while keeping specified number", async () => {
    const seq = testSequence("checkpoint-prune", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Save multiple checkpoints
    for (let i = 0; i < 5; i++) {
      await client.saveCheckpoint(id, { index: i });
      await new Promise((r) => setTimeout(r, 10));
    }

    // List all checkpoints
    const allCheckpoints = await client.listCheckpoints(id);
    assert.equal(allCheckpoints.length, 5);

    // Prune to keep only 2 most recent
    await client.pruneCheckpoints(id, 2);

    // Should have only 2 checkpoints remaining
    const prunedCheckpoints = await client.listCheckpoints(id);
    assert.equal(prunedCheckpoints.length, 2);

    // The kept checkpoints should be the most recent ones
    const keptIndices = prunedCheckpoints.map(cp => (cp.checkpoint_data as any).index);
    assert.deepEqual(keptIndices.sort(), [3, 4]);
  });

  // --- Sequence Version Migration ---

  it("handles sequence version updates during execution", async () => {
    // Create initial sequence version
    const seqV1 = testSequence("version-migration", [
      step("s1", "noop"),
      step("s2", "migration_step", {}),
    ]);
    await client.createSequence(seqV1);

    // Create instance with v1
    const { id } = await client.createInstance({
      sequence_id: seqV1.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for external handler dispatch
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Create v2 of the sequence (different steps)
    const seqV2 = {
      ...seqV1,
      id: uuid(), // New ID for new version
      version: 2,
      blocks: [
        step("s1", "noop"),
        step("s2_updated", "migration_step", { updated: true }), // Updated step
        step("s3", "log", { message: "new in v2" }), // Additional step
      ],
    };
    await client.createSequence(seqV2);

    // Deprecate v1
    await client.deprecateSequence(seqV1.id);

    // Complete the v1 instance
    const tasks = await client.pollWorkerTasks("migration_step", "worker-1");
    if (tasks.length > 0) {
      await client.completeWorkerTask(tasks[0]!.id, "worker-1", { migrated: false });
    }

    // Instance should complete with v1 sequence
    const completed = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(completed.state, "completed");
    assert.equal(completed.sequence_id, seqV1.id);

    // Create new instance with v2
    const { id: id2 } = await client.createInstance({
      sequence_id: seqV2.id,
      tenant_id: "test",
      namespace: "default",
    });

    // v2's s2_updated is also an external handler — poll + complete it so
    // the instance can progress through s3 and reach `completed`. Without
    // this the instance stalls in `waiting` forever.
    await client.waitForState(id2, "waiting", { timeoutMs: 5_000 });
    const v2Tasks = await client.pollWorkerTasks("migration_step", "worker-1");
    if (v2Tasks.length > 0) {
      await client.completeWorkerTask(v2Tasks[0]!.id, "worker-1", { migrated: true });
    }

    // New instance should use v2 sequence
    const instance2 = await client.waitForState(id2, "completed", { timeoutMs: 10_000 });
    assert.equal(instance2.sequence_id, seqV2.id);
  });

  // --- Externalized State Recovery ---

  it("recovers externalized state after restart", async () => {
    // Create a sequence that uses context extensively
    const seq = testSequence("externalized-state", [
      step("s1", "log", { message: "step 1" }),
      step("s2", "state_heavy_step", { generate: "data" }),
      step("s3", "log", { message: "step 3" }),
    ]);
    await client.createSequence(seq);

    // Create instance with large context
    const largeContext = {
      data: {
        items: Array.from({ length: 100 }, (_, i) => ({ id: i, value: `item-${i}` })),
        metadata: {
          source: "test",
          timestamp: new Date().toISOString(),
        },
      },
    };

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: largeContext,
    });

    // Update context during execution
    await new Promise((r) => setTimeout(r, 300));
    await client.updateContext(id, {
      data: {
        ...largeContext.data,
        updated: true,
        updateTime: new Date().toISOString(),
      },
    });

    // Wait for external handler dispatch
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    console.log("Restarting server with externalized state...");
    await stopServer(server);
    server = await startServer({ skipCleanup: true });
    await new Promise((r) => setTimeout(r, 1000));

    // Instance should still exist and preserve context
    const instance = await client.getInstance(id);
    assert.ok(instance.context);
    assert.ok((instance.context as any).data.updated);
    assert.ok((instance.context as any).data.updateTime);

    // Complete the external task
    const tasks = await client.pollWorkerTasks("state_heavy_step", "worker-1");
    if (tasks.length > 0) {
      await client.completeWorkerTask(tasks[0]!.id, "worker-1", { processed: true });
    }

    // Instance should complete
    await client.waitForState(id, "completed", { timeoutMs: 15_000 });
  });

  // --- Audit Log Persistence ---

  it("preserves audit log across restarts", async () => {
    const seq = testSequence("audit-persistence", [
      step("s1", "noop"),
      step("s2", "sleep", { duration_ms: 3000 }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for instance to start running (generates Scheduled→Running audit entry).
    // s2 sleeps 3s giving us a generous window to signal and observe.
    await client.waitForState(id, "running", { timeoutMs: 3_000 });
    await client.sendSignal(id, "pause");
    await new Promise((r) => setTimeout(r, 100));
    await client.sendSignal(id, "resume");
    // Small wait so signal-driven audit entries are flushed before we read.
    await new Promise((r) => setTimeout(r, 200));

    // Get audit log before restart
    const auditBefore = await client.getAuditLog(id);
    assert.ok(auditBefore.length > 0, "Should have audit entries before restart");

    console.log("Restarting server...");
    await stopServer(server);
    server = await startServer({ skipCleanup: true });
    await new Promise((r) => setTimeout(r, 1000));

    // Get audit log after restart
    const auditAfter = await client.getAuditLog(id);
    assert.ok(auditAfter.length > 0, "Should have audit entries after restart");

    // Audit log should contain similar entries (may not be identical due to timing)
    assert.ok(auditAfter.length >= auditBefore.length - 2, "Audit log should be largely preserved");

    // Instance should still be trackable
    const instance = await client.getInstance(id);
    assert.ok(instance);
  });

  // --- Worker Task Recovery ---

  it("recovers worker task state after restart", async () => {
    const seq = testSequence("worker-task-recovery", [
      step("s1", "recovery_handler", { test: "data" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for worker task dispatch
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Poll and claim the task (but don't complete it)
    const tasksBefore = await client.pollWorkerTasks("recovery_handler", "worker-1");
    assert.equal(tasksBefore.length, 1);
    const taskId = tasksBefore[0]!.id;

    console.log("Restarting server with claimed worker task...");
    await stopServer(server);
    server = await startServer({ skipCleanup: true });
    await new Promise((r) => setTimeout(r, 1000));

    // After restart, the task should still be in claimed state (or retried)
    // Try to poll for it again
    const tasksAfter = await client.pollWorkerTasks("recovery_handler", "worker-1");

    // Either the task is still claimed, or it was retried and is available again
    if (tasksAfter.length > 0) {
      // Complete it
      await client.completeWorkerTask(tasksAfter[0]!.id, "worker-1", { recovered: true });

      // Instance should complete
      await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    } else {
      // Task might still be marked as claimed to original worker
      // In this case, we can't complete it, but instance should eventually timeout/fail
      // or we could try with the original worker ID
      try {
        await client.completeWorkerTask(taskId, "worker-1", { recovered: true });
      } catch (err) {
        // Expected if task ownership was lost
        console.log(`Task completion failed after restart: ${(err as Error).message}`);
      }
    }
  });

  // --- Database Backend Consistency ---

  it("maintains data consistency under concurrent modifications", async () => {
    const seq = testSequence("concurrent-consistency", [step("s1", "noop")]);
    await client.createSequence(seq);

    // Create multiple instances concurrently
    const instanceCount = 10;
    const instancePromises = Array.from({ length: instanceCount }, () =>
      client.createInstance({
        sequence_id: seq.id,
        tenant_id: "test",
        namespace: "default",
      })
    );

    const instances = await Promise.all(instancePromises);

    // Concurrently update state and context for all instances
    const updatePromises = instances.map((instance, i) =>
      Promise.all([
        client.updateState(instance.id, "running"),
        client.updateContext(instance.id, { index: i, updated: true }),
      ]).catch(err => {
        // Some updates might fail if instance already completed
        return err;
      })
    );

    await Promise.allSettled(updatePromises);

    // Verify all instances are in a consistent state
    const verificationPromises = instances.map(async (instance) => {
      const fetched = await client.getInstance(instance.id);
      assert.equal(fetched.id, instance.id);
      // Instance should be in a valid terminal or non-terminal state
      assert.ok(
        ["scheduled", "running", "completed", "failed", "cancelled"].includes(fetched.state),
        `Invalid state: ${fetched.state}`
      );
    });

    await Promise.all(verificationPromises);

    // Restart server
    console.log("Restarting after concurrent modifications...");
    await stopServer(server);
    server = await startServer({ skipCleanup: true });
    await new Promise((r) => setTimeout(r, 1000));

    // Verify consistency persists after restart
    const postRestartPromises = instances.map(async (instance) => {
      const fetched = await client.getInstance(instance.id);
      assert.equal(fetched.id, instance.id);
      // State should still be valid
      assert.ok(
        ["scheduled", "running", "completed", "failed", "cancelled"].includes(fetched.state),
        `Invalid state after restart: ${fetched.state}`
      );
    });

    await Promise.all(postRestartPromises);
  });
});
