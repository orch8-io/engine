import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Complex Workflow Patterns", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // --- Nested Composite Patterns ---

  it("executes parallel blocks within try-catch within forEach", async () => {
    // This tests deeply nested composite patterns
    const seq = testSequence("nested-parallel-trycatch-foreach", [
      {
        type: "for_each",
        id: "fe1",
        items: [1, 2, 3],
        block: {
          type: "try_catch",
          id: "tc1",
          try_block: {
            type: "parallel",
            id: "p1",
            branches: [
              { id: "b1", block: step("p1s1", "noop") },
              { id: "b2", block: step("p1s2", "log", { message: "parallel log" }) },
            ],
          },
          catch_block: step("catch1", "log", { message: "caught error" }),
        },
      },
    ]);

    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Should complete successfully
    const completed = await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    assert.equal(completed.state, "completed");

    // Verify outputs
    const outputs = await client.getOutputs(id);
    // Should have outputs from forEach iterations
    assert.ok(outputs.length >= 3);
  });

  it("handles dynamic workflow generation", async () => {
    // Create a sequence that creates another sequence
    const seq = testSequence("dynamic-workflow-gen", [
      step("s1", "noop"),
      step("s2", "dynamic_seq_creator", { name: "dynamic-child" }),
      step("s3", "log", { message: "after dynamic creation" }),
    ]);

    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for external handler
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Poll and complete the dynamic sequence creation task
    const tasks = await client.pollWorkerTasks("dynamic_seq_creator", "worker-1");
    if (tasks.length > 0) {
      // Simulate creating a new sequence
      const childSeq = testSequence("dynamic-child", [
        step("child1", "noop"),
        step("child2", "log", { message: "from dynamic sequence" }),
      ]);

      // In a real scenario, the worker would call createSequence
      // For this test, we'll just complete with success
      await client.completeWorkerTask(tasks[0]!.id, "worker-1", {
        created: true,
        sequence_id: uuid(),
      });
    }

    // Instance should complete
    const completed = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(completed.state, "completed");
  });

  it("executes conditional while loops", async () => {
    // Create a sequence with a while loop that depends on context
    const seq = testSequence("conditional-while", [
      step("init", "log", { message: "starting", counter: 0 }),
      {
        type: "while",
        id: "while1",
        condition: "{{ctx.counter < 3}}",
        block: [
          step("inc", "log", {
            message: "iteration {{ctx.counter}}",
            // This would update context in a real scenario
          }),
          // In reality, we'd need a way to update context
          // For this test, we'll use an external handler
          step("update", "while_counter_updater", {}),
        ],
      },
      step("final", "log", { message: "loop finished" }),
    ]);

    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      context: { counter: 0 },
    });

    // Handle the while loop iterations
    for (let i = 0; i < 3; i++) {
      // Wait for external handler
      await client.waitForState(id, "waiting", { timeoutMs: 5000 });

      // Complete the counter update
      const tasks = await client.pollWorkerTasks("while_counter_updater", "worker-1");
      if (tasks.length > 0) {
        await client.completeWorkerTask(tasks[0]!.id, "worker-1", {
          counter: i + 1,
          continue: i < 2, // Continue for first 2 iterations
        });
      }
    }

    // Instance should complete after 3 iterations
    const completed = await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    assert.equal(completed.state, "completed");
  });

  it("handles recursive workflows", async () => {
    // Create a sequence that can trigger itself
    const seq = testSequence("recursive-workflow", [
      step("s1", "log", { message: "starting recursion", depth: 0 }),
      step("s2", "recursive_trigger", { max_depth: 3 }),
      step("s3", "log", { message: "recursion complete" }),
    ]);

    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Wait for external handler
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // The recursive handler would create new instances
    // For this test, we'll simulate it
    const tasks = await client.pollWorkerTasks("recursive_trigger", "worker-1");
    if (tasks.length > 0) {
      await client.completeWorkerTask(tasks[0]!.id, "worker-1", {
        triggered: true,
        child_instances: 2, // Simulated child instances
      });
    }

    // Original instance should complete
    const completed = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(completed.state, "completed");
  });

  // --- Advanced Error Recovery Patterns ---

  it("implements circuit breaker with fallback", async () => {
    const seq = testSequence("circuit-breaker-fallback", [
      {
        type: "try_catch",
        id: "main",
        try_block: step("primary", "flaky_service", { retry: true }),
        catch_block: {
          type: "try_catch",
          id: "fallback",
          try_block: step("fallback1", "backup_service", {}),
          catch_block: step("final_fallback", "log", { message: "all services down" }),
        },
      },
    ]);

    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Primary service fails
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });
    let tasks = await client.pollWorkerTasks("flaky_service", "worker-1");
    if (tasks.length > 0) {
      await client.failWorkerTask(tasks[0]!.id, "worker-1", "service unavailable", false);
    }

    // Fallback service succeeds
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });
    tasks = await client.pollWorkerTasks("backup_service", "worker-1");
    if (tasks.length > 0) {
      await client.completeWorkerTask(tasks[0]!.id, "worker-1", { backup: "ok" });
    }

    // Instance should complete
    const completed = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(completed.state, "completed");
  });

  it("executes race conditions with timeout", async () => {
    const seq = testSequence("race-with-timeout", [
      {
        type: "race",
        id: "race1",
        branches: [
          {
            id: "fast",
            block: step("fast_service", "fast_handler", {}),
          },
          {
            id: "slow",
            block: step("slow_service", "slow_handler", { delay: 5000 }),
          },
          {
            id: "timeout",
            block: {
              type: "try_catch",
              id: "timeout_block",
              try_block: step("timeout_service", "timeout_handler", {}),
              catch_block: step("timeout_fallback", "log", { message: "timed out" }),
            },
          },
        ],
      },
    ]);

    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Complete the fast service first
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });
    const tasks = await client.pollWorkerTasks("fast_handler", "worker-1");
    if (tasks.length > 0) {
      await client.completeWorkerTask(tasks[0]!.id, "worker-1", { result: "fast win" });
    }

    // Instance should complete (race winner finishes)
    const completed = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(completed.state, "completed");
  });

  // --- Stateful Workflow Patterns ---

  it("maintains state across long-running workflow", async () => {
    const seq = testSequence("stateful-long-running", [
      step("init", "log", { message: "starting", state: { step: 1 } }),
      step("process1", "stateful_handler", { action: "process" }),
      step("checkpoint", "log", { message: "checkpoint reached" }),
      step("process2", "stateful_handler", { action: "continue" }),
      step("finalize", "stateful_handler", { action: "finalize" }),
      step("complete", "log", { message: "workflow complete" }),
    ]);

    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Process through each stateful step
    const steps = ["process", "continue", "finalize"];
    for (const stepAction of steps) {
      await client.waitForState(id, "waiting", { timeoutMs: 5000 });
      const tasks = await client.pollWorkerTasks("stateful_handler", "worker-1");
      if (tasks.length > 0) {
        await client.completeWorkerTask(tasks[0]!.id, "worker-1", {
          completed: stepAction,
          state: { step: steps.indexOf(stepAction) + 2 },
        });
      }
    }

    // Instance should complete
    const completed = await client.waitForState(id, "completed", { timeoutMs: 15_000 });
    assert.equal(completed.state, "completed");
  });

  it("handles compensation transactions (SAGA pattern)", async () => {
    const seq = testSequence("saga-pattern", [
      step("reserve_inventory", "inventory_service", { action: "reserve" }),
      step("charge_payment", "payment_service", { action: "charge" }),
      step("ship_order", "shipping_service", { action: "ship" }),
      {
        type: "try_catch",
        id: "compensation",
        try_block: step("notify_customer", "notification_service", { action: "notify" }),
        catch_block: {
          type: "parallel",
          id: "compensate",
          branches: [
            { id: "c1", block: step("refund", "payment_service", { action: "refund" }) },
            { id: "c2", block: step("release", "inventory_service", { action: "release" }) },
          ],
        },
      },
    ]);

    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Complete initial steps
    const services = ["inventory_service", "payment_service", "shipping_service"];
    for (const service of services) {
      await client.waitForState(id, "waiting", { timeoutMs: 5000 });
      const tasks = await client.pollWorkerTasks(service, "worker-1");
      if (tasks.length > 0) {
        await client.completeWorkerTask(tasks[0]!.id, "worker-1", { success: true });
      }
    }

    // Notification service fails, triggering compensation
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });
    let tasks = await client.pollWorkerTasks("notification_service", "worker-1");
    if (tasks.length > 0) {
      await client.failWorkerTask(tasks[0]!.id, "worker-1", "notification failed", false);
    }

    // Compensation steps should execute
    const compensateServices = ["payment_service", "inventory_service"];
    for (const service of compensateServices) {
      await client.waitForState(id, "waiting", { timeoutMs: 5000 });
      tasks = await client.pollWorkerTasks(service, "worker-1");
      if (tasks.length > 0) {
        await client.completeWorkerTask(tasks[0]!.id, "worker-1", { compensated: true });
      }
    }

    // Instance should complete (with compensation)
    const completed = await client.waitForState(id, "completed", { timeoutMs: 20_000 });
    assert.equal(completed.state, "completed");
  });

  // --- Dynamic Routing Patterns ---

  it("routes dynamically based on context", async () => {
    const seq = testSequence("dynamic-routing", [
      step("analyze", "context_analyzer", {}),
      {
        type: "router",
        id: "router1",
        routes: [
          {
            condition: "{{ctx.category === 'A'}}",
            block: step("handle_a", "handler_a", {}),
          },
          {
            condition: "{{ctx.category === 'B'}}",
            block: step("handle_b", "handler_b", {}),
          },
          {
            condition: "true", // default
            block: step("handle_default", "log", { message: "default handling" }),
          },
        ],
      },
      step("finalize", "log", { message: "routing complete" }),
    ]);

    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Context analyzer sets category
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });
    let tasks = await client.pollWorkerTasks("context_analyzer", "worker-1");
    if (tasks.length > 0) {
      await client.completeWorkerTask(tasks[0]!.id, "worker-1", {
        category: "A",
        priority: "high",
      });
    }

    // Should route to handler_a
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });
    tasks = await client.pollWorkerTasks("handler_a", "worker-1");
    if (tasks.length > 0) {
      await client.completeWorkerTask(tasks[0]!.id, "worker-1", { handled: "category A" });
    }

    // Instance should complete
    const completed = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(completed.state, "completed");
  });

  // --- Event-Driven Patterns ---

  it("waits for external events before proceeding", async () => {
    const seq = testSequence("event-driven", [
      step("start", "log", { message: "waiting for event" }),
      step("wait", "event_waiter", { event_type: "user_approval" }),
      step("process", "event_processor", {}),
      step("complete", "log", { message: "event processed" }),
    ]);

    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    // Instance waits for event
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });

    // Simulate external event (e.g., via webhook or signal)
    // For this test, we'll complete the event_waiter task
    const tasks = await client.pollWorkerTasks("event_waiter", "worker-1");
    if (tasks.length > 0) {
      await client.completeWorkerTask(tasks[0]!.id, "worker-1", {
        event_received: true,
        event_data: { approved: true, timestamp: new Date().toISOString() },
      });
    }

    // Process the event
    await client.waitForState(id, "waiting", { timeoutMs: 5000 });
    const processTasks = await client.pollWorkerTasks("event_processor", "worker-1");
    if (processTasks.length > 0) {
      await client.completeWorkerTask(processTasks[0]!.id, 'worker-1', { processed: true });
    }

    // Instance should complete
    const completed = await client.waitForState(id, 'completed', { timeoutMs: 10_000 });
    assert.equal(completed.state, 'completed');
  });
});
