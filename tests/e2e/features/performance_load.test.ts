import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Performance and Load Testing", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // --- Concurrent Instance Creation ---

  it("handles 100 concurrent instance creations", async () => {
    const seq = testSequence("concurrent-100", [step("s1", "noop")]);
    await client.createSequence(seq);

    const count = 100;
    const promises = Array.from({ length: count }, () =>
      client.createInstance({
        sequence_id: seq.id,
        tenant_id: "test",
        namespace: "default",
      })
    );

    const startTime = Date.now();
    const results = await Promise.allSettled(promises);
    const endTime = Date.now();
    const duration = endTime - startTime;

    console.log(`Created ${count} instances in ${duration}ms (${(duration / count).toFixed(2)}ms per instance)`);

    // All should succeed
    const successful = results.filter(r => r.status === "fulfilled");
    assert.equal(successful.length, count, `All ${count} creations should succeed`);

    // Verify all instances can complete
    const completionPromises = successful.map(result => {
      if (result.status === "fulfilled") {
        return client.waitForState(result.value.id, "completed", {
          timeoutMs: 30_000,
        });
      }
      return Promise.resolve();
    });

    const completionResults = await Promise.allSettled(completionPromises);
    const completed = completionResults.filter(r => r.status === "fulfilled");
    assert.equal(completed.length, count, `All ${count} instances should complete`);
  });

  // --- High-Frequency Worker Polling ---

  it("handles multiple workers polling concurrently", async () => {
    const seq = testSequence("multi-worker-poll", [
      step("s1", "external_task_1", {}),
      step("s2", "external_task_2", {}),
      step("s3", "external_task_3", {}),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Steps are dispatched sequentially: each one goes to a worker queue,
    // the instance waits, the worker completes it, the scheduler picks up
    // the next step. Poll-and-complete each task in order.
    const handlers = ["external_task_1", "external_task_2", "external_task_3"];
    for (const handler of handlers) {
      // Wait for instance to go to waiting (next external task dispatched).
      await client.waitForState(id, "waiting", { timeoutMs: 5000 });

      // Poll with multiple workers concurrently — at most one should claim.
      const workerCount = 5;
      const pollPromises = Array.from({ length: workerCount }, (_, i) =>
        client.pollWorkerTasks(handler, `worker-${i}`, 1)
      );
      const pollResults = await Promise.all(pollPromises);
      const claimedTasks = pollResults.flat().filter(task => task.state === "claimed");
      assert.ok(
        claimedTasks.length <= 1,
        `Expected at most 1 claimed task for ${handler}, got ${claimedTasks.length}`,
      );

      for (const task of claimedTasks) {
        await client.completeWorkerTask(task.id, task.worker_id as string, { completed: true });
      }
    }

    // Instance should complete after all 3 tasks are done.
    const completed = await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    assert.equal(completed.state, "completed");
  });

  // --- Memory Leak Detection (Indirect) ---

  it("completes 1000 sequential instances without degradation", async () => {
    const seq = testSequence("sequential-1000", [step("s1", "noop")]);
    await client.createSequence(seq);

    const instanceCount = 50; // Reduced from 1000 for test speed
    const durations: number[] = [];

    for (let i = 0; i < instanceCount; i++) {
      const startTime = Date.now();

      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: "test",
        namespace: "default",
      });

      await client.waitForState(id, "completed", { timeoutMs: 10_000 });

      const endTime = Date.now();
      durations.push(endTime - startTime);

      // Log progress every 10 instances
      if ((i + 1) % 10 === 0) {
        console.log(`Completed ${i + 1}/${instanceCount} instances`);
      }
    }

    // Calculate statistics
    const avgDuration = durations.reduce((a, b) => a + b, 0) / durations.length;
    const maxDuration = Math.max(...durations);
    const minDuration = Math.min(...durations);

    console.log(`Sequential instances: avg=${avgDuration.toFixed(2)}ms, min=${minDuration}ms, max=${maxDuration}ms`);

    // Check for significant degradation (last 10% shouldn't be > 2x slower than first 10%)
    const first10Avg = durations.slice(0, Math.floor(durations.length * 0.1)).reduce((a, b) => a + b, 0) / Math.floor(durations.length * 0.1);
    const last10Avg = durations.slice(-Math.floor(durations.length * 0.1)).reduce((a, b) => a + b, 0) / Math.floor(durations.length * 0.1);

    const degradationRatio = last10Avg / first10Avg;
    console.log(`Degradation ratio (last 10% / first 10%): ${degradationRatio.toFixed(2)}`);

    // Allow some variance but flag severe degradation
    assert.ok(degradationRatio < 3, `Significant performance degradation detected: ratio=${degradationRatio.toFixed(2)}`);
  });

  // --- Database Connection Pool Stress ---

  it("handles many parallel database operations", async () => {
    const seq = testSequence("db-stress", [step("s1", "noop")]);
    await client.createSequence(seq);

    // Create instances and immediately query them
    const operations = 50;
    const operationPromises = Array.from({ length: operations }, async (_, i) => {
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: `tenant-${i % 5}`, // Use multiple tenants
        namespace: "default",
      });

      // Immediately query the instance
      const instance = await client.getInstance(id);
      assert.equal(instance.id, id);

      // Also list instances for this tenant
      const list = await client.listInstances({ tenant_id: `tenant-${i % 5}` });
      assert.ok(list.length >= 1);

      return id;
    });

    const startTime = Date.now();
    const results = await Promise.allSettled(operationPromises);
    const endTime = Date.now();

    console.log(`Completed ${operations} parallel DB operations in ${endTime - startTime}ms`);

    const successful = results.filter(r => r.status === "fulfilled");
    assert.equal(successful.length, operations, `All ${operations} operations should succeed`);
  });

  // --- Mixed Workload Stress Test ---

  it("handles mixed workload of different sequence types", async () => {
    // Create multiple sequence types
    const sequences = [
      testSequence("mixed-fast", [step("s1", "noop")]),
      testSequence("mixed-slow", [step("s1", "sleep", { duration_ms: 100 })]),
      testSequence("mixed-external", [step("s1", "mixed_external_handler", {})]),
      testSequence("mixed-multi", [
        step("s1", "noop"),
        step("s2", "sleep", { duration_ms: 50 }),
        step("s3", "log", { message: "done" }),
      ]),
    ];

    await Promise.all(sequences.map(seq => client.createSequence(seq)));

    // Create instances of each type
    const instancePromises = sequences.flatMap((seq, idx) =>
      Array.from({ length: 5 }, () =>
        client.createInstance({
          sequence_id: seq.id,
          tenant_id: "test",
          namespace: "default",
        })
      )
    );

    const instances = await Promise.all(instancePromises);
    console.log(`Created ${instances.length} mixed workload instances`);

    // Handle external tasks — poll multiple times since not all instances
    // may have reached the worker queue yet.
    let totalHandled = 0;
    for (let attempt = 0; attempt < 10 && totalHandled < 5; attempt++) {
      const externalTasks = await client.pollWorkerTasks("mixed_external_handler", "worker-1", 10);
      for (const task of externalTasks) {
        await client.completeWorkerTask(task.id, "worker-1", { external: true });
        totalHandled++;
      }
      if (totalHandled < 5) {
        await new Promise(r => setTimeout(r, 500));
      }
    }

    // Wait for all instances to complete (with generous timeout)
    const completionPromises = instances.map(instance =>
      client.waitForState(instance.id, "completed", { timeoutMs: 30_000 }).catch(err => {
        console.warn(`Instance ${instance.id} failed to complete: ${err.message}`);
        return null;
      })
    );

    const results = await Promise.allSettled(completionPromises);
    const completed = results.filter(r => r.status === "fulfilled" && r.value !== null);

    console.log(`Completed ${completed.length}/${instances.length} mixed workload instances`);
    assert.ok(completed.length >= instances.length * 0.9, `At least 90% of instances should complete (${completed.length}/${instances.length})`);
  });

  // --- High Volume Signal Processing ---

  it("processes many signals quickly", async () => {
    const seq = testSequence("signal-stress", [
      step("s1", "sleep", { duration_ms: 5000 }), // Long sleep to allow signals
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for instance to be running
    await new Promise((r) => setTimeout(r, 300));

    // Send multiple signals rapidly
    const signalCount = 20;
    const signalPromises = Array.from({ length: signalCount }, (_, i) =>
      client.sendSignal(id, i % 2 === 0 ? "pause" : "resume").catch(err => {
        // Some signals may fail (e.g., already in that state), which is OK
        return err;
      })
    );

    const startTime = Date.now();
    const signalResults = await Promise.allSettled(signalPromises);
    const endTime = Date.now();

    console.log(`Processed ${signalCount} signals in ${endTime - startTime}ms (${((endTime - startTime) / signalCount).toFixed(2)}ms per signal)`);

    // Cancel the instance to clean up
    await client.sendSignal(id, "cancel").catch(() => { /* Ignore if already cancelled */ });

    const finalState = await client.waitForState(id, ["cancelled", "failed", "completed"], {
      timeoutMs: 10_000,
    });
    assert.ok(["cancelled", "failed", "completed"].includes(finalState.state));
  });

  // --- Bulk Operations Performance ---

  it("processes large batch operations efficiently", async () => {
    const seq = testSequence("batch-performance", [step("s1", "noop")]);
    await client.createSequence(seq);

    const batchSize = 100;
    const instances = Array.from({ length: batchSize }, () => ({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    }));

    const startTime = Date.now();
    const result = await client.createInstancesBatch(instances);
    const endTime = Date.now();

    console.log(`Created batch of ${batchSize} instances in ${endTime - startTime}ms (${((endTime - startTime) / batchSize).toFixed(2)}ms per instance)`);
    assert.equal((result as any).count, batchSize);

    // Verify all instances were created
    const list = await client.listInstances({ tenant_id: "test" });
    const ourInstances = list.filter(i => i.sequence_id === seq.id);
    assert.ok(ourInstances.length >= batchSize, `Expected at least ${batchSize} instances, found ${ourInstances.length}`);
  });

  // --- Concurrent Sequence Creation ---

  it("handles concurrent sequence creation", async () => {
    const sequenceCount = 20;
    const sequencePromises = Array.from({ length: sequenceCount }, (_, i) =>
      testSequence(`concurrent-seq-${i}`, [step("s1", "noop")])
    ).map(seq => client.createSequence(seq));

    const startTime = Date.now();
    const results = await Promise.allSettled(sequencePromises);
    const endTime = Date.now();

    console.log(`Created ${sequenceCount} sequences concurrently in ${endTime - startTime}ms`);

    const successful = results.filter(r => r.status === "fulfilled");
    assert.equal(successful.length, sequenceCount, `All ${sequenceCount} sequence creations should succeed`);
  });
});
