/**
 * E2E: Saga Block Type (Feature #03)
 *
 * Validates the saga composite block type via the HTTP API:
 * - Creating a sequence with a saga block succeeds and round-trips
 * - All actions succeed → instance completes
 * - An action fails → compensations run in reverse, instance fails
 * - Saga with no compensation steps still works
 * - Error context (_error) injected on failure
 * - Saga serde round-trip preserves structure
 *
 * Deepened with edge cases: multi-step rollback ordering, partial
 * compensation coverage, error context contents, saga nested inside
 * router/parallel/try_catch, `when` guards and `retry` policies on saga
 * action/compensation steps, and output-schema validation on saga steps.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

interface SagaStep {
  id: string;
  action: Record<string, unknown>;
  compensation?: Record<string, unknown>;
}

function saga(id: string, steps: SagaStep[]): Record<string, unknown> {
  return { type: "saga", id, steps };
}

function sagaStep(
  id: string,
  action: Record<string, unknown>,
  compensation?: Record<string, unknown>,
): SagaStep {
  const s: SagaStep = { id, action };
  if (compensation) s.compensation = compensation;
  return s;
}

function parallel(id: string, branches: unknown[]): Record<string, unknown> {
  return { type: "parallel", id, branches };
}

function router(
  id: string,
  routes: { condition: string; blocks: unknown[] }[],
  fallback: unknown[] = [],
): Record<string, unknown> {
  return { type: "router", id, routes, default: fallback };
}

function tryCatch(
  id: string,
  tryBlock: unknown[],
  catchBlock: unknown[] = [],
  finallyBlock: unknown[] = [],
): Record<string, unknown> {
  return {
    type: "try_catch",
    id,
    try_block: tryBlock,
    catch_block: catchBlock,
    finally_block: finallyBlock,
  };
}

async function run(seq: any) {
  const created = await client.createSequence(seq);
  const inst = await client.createInstance({
    sequence_id: created.id,
    tenant_id: seq.tenant_id,
    namespace: seq.namespace,
  });
  const final_ = await client.waitForState(inst.id, ["completed", "failed"], {
    timeoutMs: 10_000,
  });
  return { created, inst, final_ };
}

describe("Saga Block Type", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // Sequence creation
  // ------------------------------------------------------------------

  it("creates a sequence with a saga block", async () => {
    const seq = testSequence("saga-create", [
      saga("saga1", [
        sagaStep("s0", step("a0", "noop"), step("c0", "noop")),
        sagaStep("s1", step("a1", "noop")),
      ]) as any,
    ]);
    const created = await client.createSequence(seq);
    assert.ok(created.id, "sequence should have an id");

    const fetched = await client.getSequence(created.id);
    const sagaBlock = fetched.blocks[0] as any;
    assert.equal(sagaBlock.type, "saga");
    assert.equal(sagaBlock.id, "saga1");
    assert.equal(sagaBlock.steps.length, 2);
    assert.equal(sagaBlock.steps[0].id, "s0");
    assert.ok(sagaBlock.steps[0].compensation, "step 0 should have compensation");
    assert.equal(sagaBlock.steps[1].id, "s1");
  });

  // ------------------------------------------------------------------
  // Happy path: all actions succeed
  // ------------------------------------------------------------------

  it("all saga actions succeed → instance completes", async () => {
    const seq = testSequence("saga-success", [
      saga("saga1", [
        sagaStep("s0", step("a0", "noop"), step("c0", "noop")),
        sagaStep("s1", step("a1", "noop"), step("c1", "noop")),
      ]) as any,
    ]);
    const { final_ } = await run(seq);
    assert.equal(final_.state, "completed");
  });

  // ------------------------------------------------------------------
  // Failure: action fails → compensations run, instance fails
  // ------------------------------------------------------------------

  it("action fails → instance fails after compensation", async () => {
    const seq = testSequence("saga-fail", [
      saga("saga1", [
        sagaStep("s0", step("a0", "noop"), step("c0", "noop")),
        sagaStep(
          "s1",
          step("a1", "fail", { message: "boom" }),
          step("c1", "noop"),
        ),
      ]) as any,
    ]);
    const { final_ } = await run(seq);
    assert.equal(final_.state, "failed");
  });

  // ------------------------------------------------------------------
  // Step without compensation
  // ------------------------------------------------------------------

  it("saga step without compensation works", async () => {
    const seq = testSequence("saga-no-comp", [
      saga("saga1", [
        sagaStep("s0", step("a0", "noop")),
        sagaStep("s1", step("a1", "noop")),
      ]) as any,
    ]);
    const { final_ } = await run(seq);
    assert.equal(final_.state, "completed");
  });

  // ------------------------------------------------------------------
  // Empty saga
  // ------------------------------------------------------------------

  it("empty saga completes immediately", async () => {
    const seq = testSequence("saga-empty", [saga("saga1", []) as any]);
    const { final_ } = await run(seq);
    assert.equal(final_.state, "completed");
  });

  // ------------------------------------------------------------------
  // Single step saga
  // ------------------------------------------------------------------

  it("single step saga succeeds", async () => {
    const seq = testSequence("saga-single", [
      saga("saga1", [sagaStep("s0", step("a0", "noop"), step("c0", "noop"))]) as any,
    ]);
    const { final_ } = await run(seq);
    assert.equal(final_.state, "completed");
  });

  // ------------------------------------------------------------------
  // Saga followed by another step
  // ------------------------------------------------------------------

  it("saga followed by a step — both run on success", async () => {
    const seq = testSequence("saga-then-step", [
      saga("saga1", [sagaStep("s0", step("a0", "noop"), step("c0", "noop"))]) as any,
      step("after", "noop") as any,
    ]);
    const { final_ } = await run(seq);
    assert.equal(final_.state, "completed");
  });

  // ------------------------------------------------------------------
  // Multi-step LIFO rollback ordering
  // ------------------------------------------------------------------

  it("rolls back completed steps in strict LIFO order when a later step fails", async () => {
    const seq = testSequence("saga-lifo", [
      saga("saga1", [
        sagaStep("s0", step("a0", "transform", { marker: "a0" }), step("c0", "transform", { marker: "c0" })),
        sagaStep("s1", step("a1", "transform", { marker: "a1" }), step("c1", "transform", { marker: "c1" })),
        sagaStep("s2", step("a2", "transform", { marker: "a2" }), step("c2", "transform", { marker: "c2" })),
        sagaStep("s3", step("a3", "fail", { message: "boom" }), step("c3", "transform", { marker: "c3" })),
      ]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "failed");

    const outputs = await client.getOutputs(inst.id);
    // Action a3 never completed successfully (it failed), so compensation
    // c3 must never run; only c0..c2 run, in reverse order.
    const c3 = outputs.find((o: any) => o.block_id === "c3");
    assert.ok(!c3, "compensation for the failed step itself must not run");

    for (const id of ["c0", "c1", "c2"]) {
      const o = outputs.find((out: any) => out.block_id === id);
      assert.ok(o, `compensation ${id} should have run`);
      assert.equal((o!.output as any).marker, id);
    }
  });

  // ------------------------------------------------------------------
  // Compensation runs only for steps whose action actually completed
  // ------------------------------------------------------------------

  it("only compensates steps whose action completed before the failure", async () => {
    const seq = testSequence("saga-partial-rollback", [
      saga("saga1", [
        sagaStep("s0", step("a0", "noop"), step("c0", "noop")),
        sagaStep("s1", step("a1", "fail", { message: "stop here" }), step("c1", "noop")),
        sagaStep("s2", step("a2", "noop"), step("c2", "noop")),
      ]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "failed");

    const outputs = await client.getOutputs(inst.id);
    assert.ok(outputs.find((o: any) => o.block_id === "c0"), "c0 should have run");
    // s1's action failed, so its own compensation never runs.
    assert.ok(!outputs.find((o: any) => o.block_id === "c1"), "c1 must not run");
    // s2 never started (sequential), so its action a2 and compensation c2 never run.
    assert.ok(!outputs.find((o: any) => o.block_id === "a2"), "a2 must not run");
    assert.ok(!outputs.find((o: any) => o.block_id === "c2"), "c2 must not run");
  });

  // ------------------------------------------------------------------
  // Best-effort rollback: a compensation failure doesn't stop other rollbacks
  // ------------------------------------------------------------------

  it("continues rolling back remaining steps even if one compensation fails", async () => {
    const seq = testSequence("saga-comp-fails-best-effort", [
      saga("saga1", [
        sagaStep("s0", step("a0", "noop"), step("c0", "transform", { marker: "c0-ran" })),
        sagaStep("s1", step("a1", "noop"), step("c1", "fail", { message: "comp boom" })),
        sagaStep("s2", step("a2", "fail", { message: "trigger rollback" }), step("c2", "noop")),
      ]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "failed", "saga always fails after rollback");

    const outputs = await client.getOutputs(inst.id);
    // c1 fails, but rollback should still proceed to c0.
    assert.ok(
      outputs.find((o: any) => o.block_id === "c0"),
      "compensation for step before the failed compensation should still run",
    );
  });

  // ------------------------------------------------------------------
  // Error context (_error) injected into instance context on failure
  // ------------------------------------------------------------------

  it("injects _error context with failed_step, source, and block_id on failure", async () => {
    const seq = testSequence("saga-error-context", [
      saga("checkout_saga", [
        sagaStep("charge_card", step("a0", "noop"), step("c0", "noop")),
        sagaStep("reserve_inventory", step("a1", "fail", { message: "out of stock" }), step("c1", "noop")),
      ]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "failed");

    const fetchedInst = await client.getInstance(inst.id);
    const errCtx = (fetchedInst.context as any)?.data?._error;
    assert.ok(errCtx, "expected _error to be injected into context.data");
    assert.equal(errCtx.failed_step, "reserve_inventory");
    assert.equal(errCtx.source, "saga");
    assert.equal(errCtx.block_id, "checkout_saga");
  });

  it("does not inject _error context when the saga succeeds", async () => {
    const seq = testSequence("saga-no-error-on-success", [
      saga("saga1", [sagaStep("s0", step("a0", "noop"), step("c0", "noop"))]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "completed");

    const fetchedInst = await client.getInstance(inst.id);
    const errCtx = (fetchedInst.context as any)?.data?._error;
    assert.ok(!errCtx, "no _error should be injected on success");
  });

  // ------------------------------------------------------------------
  // First-step failure — nothing to compensate
  // ------------------------------------------------------------------

  it("first step fails → no compensations run, saga still fails", async () => {
    const seq = testSequence("saga-first-fails", [
      saga("saga1", [
        sagaStep("s0", step("a0", "fail", { message: "immediate" }), step("c0", "transform", { marker: "should-not-run" })),
        sagaStep("s1", step("a1", "noop"), step("c1", "transform", { marker: "should-not-run" })),
      ]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "failed");

    const outputs = await client.getOutputs(inst.id);
    assert.ok(!outputs.find((o: any) => o.block_id === "c0"));
    assert.ok(!outputs.find((o: any) => o.block_id === "c1"));
    assert.ok(!outputs.find((o: any) => o.block_id === "a1"), "a1 must never start");
  });

  // ------------------------------------------------------------------
  // `when` guard on a saga action step
  // ------------------------------------------------------------------

  it("saga action step honors a `when` guard that evaluates false (skipped, saga still succeeds)", async () => {
    const seq = testSequence("saga-when-skip", [
      saga("saga1", [
        sagaStep(
          "s0",
          step("a0", "transform", { marker: "ran" }, { when: "false" }),
          step("c0", "noop"),
        ),
      ]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "completed");
    const outputs = await client.getOutputs(inst.id);
    assert.ok(!outputs.find((o: any) => o.block_id === "a0"), "guarded-off action must not produce output");
  });

  it("saga action step honors a `when` guard that evaluates true", async () => {
    const seq = testSequence("saga-when-run", [
      saga("saga1", [
        sagaStep(
          "s0",
          step("a0", "transform", { marker: "ran" }, { when: "true" }),
          step("c0", "noop"),
        ),
      ]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "completed");
    const outputs = await client.getOutputs(inst.id);
    const a0 = outputs.find((o: any) => o.block_id === "a0");
    assert.ok(a0, "guarded-on action should produce output");
    assert.equal((a0!.output as any).marker, "ran");
  });

  // ------------------------------------------------------------------
  // `retry` policy on a saga action step
  // ------------------------------------------------------------------

  it("saga action step retries on retryable failure per its own retry policy", async () => {
    const seq = testSequence("saga-action-retry", [
      saga("saga1", [
        sagaStep(
          "s0",
          step(
            "a0",
            "fail",
            { message: "transient", retryable: true },
            { retry: { max_attempts: 2, initial_backoff: 5, max_backoff: 20 } },
          ),
          step("c0", "noop"),
        ),
      ]) as any,
    ]);
    const { final_ } = await run(seq);
    // Retries exhaust (handler always fails) → action fails → saga fails.
    assert.equal(final_.state, "failed");
  });

  // ------------------------------------------------------------------
  // Saga nested inside router
  // ------------------------------------------------------------------

  it("saga nested inside a router route executes correctly", async () => {
    const seq = testSequence("saga-in-router", [
      router(
        "rt",
        [
          {
            condition: "true",
            blocks: [
              saga("inner_saga", [
                sagaStep("s0", step("a0", "noop"), step("c0", "noop")),
              ]) as any,
            ],
          },
        ],
        [],
      ) as any,
    ]);
    const { final_ } = await run(seq);
    assert.equal(final_.state, "completed");
  });

  it("saga nested inside a router route rolls back and fails, router propagates failure", async () => {
    const seq = testSequence("saga-in-router-fail", [
      router(
        "rt",
        [
          {
            condition: "true",
            blocks: [
              saga("inner_saga", [
                sagaStep("s0", step("a0", "noop"), step("c0", "transform", { marker: "rolled_back" })),
                sagaStep("s1", step("a1", "fail", { message: "nested boom" })),
              ]) as any,
            ],
          },
        ],
        [],
      ) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "failed");
    const outputs = await client.getOutputs(inst.id);
    assert.ok(outputs.find((o: any) => o.block_id === "c0"), "nested saga compensation should still run");
  });

  // ------------------------------------------------------------------
  // Saga nested inside parallel branches
  // ------------------------------------------------------------------

  it("two independent sagas in parallel branches both complete", async () => {
    const seq = testSequence("saga-in-parallel", [
      parallel("par", [
        [saga("saga_a", [sagaStep("sa0", step("aa0", "noop"), step("ca0", "noop"))]) as any],
        [saga("saga_b", [sagaStep("sb0", step("ab0", "noop"), step("cb0", "noop"))]) as any],
      ]) as any,
    ]);
    const { final_ } = await run(seq);
    assert.equal(final_.state, "completed");
  });

  it("one saga branch fails in parallel — instance fails, sibling branch's saga still completes its own work", async () => {
    const seq = testSequence("saga-in-parallel-fail", [
      parallel("par", [
        [saga("saga_ok", [sagaStep("sa0", step("aa0", "noop"), step("ca0", "noop"))]) as any],
        [
          saga("saga_bad", [
            sagaStep("sb0", step("ab0", "noop"), step("cb0", "transform", { marker: "rolled_back" })),
            sagaStep("sb1", step("ab1", "fail", { message: "parallel boom" })),
          ]) as any,
        ],
      ]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "failed");
    const outputs = await client.getOutputs(inst.id);
    assert.ok(outputs.find((o: any) => o.block_id === "aa0"), "sibling saga's action should still have run");
    assert.ok(outputs.find((o: any) => o.block_id === "cb0"), "failed saga's compensation should have run");
  });

  // ------------------------------------------------------------------
  // Saga nested inside try_catch
  // ------------------------------------------------------------------

  it("failing saga inside a try_catch try_block is caught, instance completes via catch_block", async () => {
    const seq = testSequence("saga-in-trycatch", [
      tryCatch(
        "tc",
        [
          saga("inner_saga", [
            sagaStep("s0", step("a0", "noop"), step("c0", "noop")),
            sagaStep("s1", step("a1", "fail", { message: "caught boom" })),
          ]) as any,
        ],
        [step("handled", "transform", { recovered: true })],
      ) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "completed", "try_catch should catch the saga failure");
    const outputs = await client.getOutputs(inst.id);
    const handled = outputs.find((o: any) => o.block_id === "handled");
    assert.ok(handled, "catch_block should have executed");
    assert.equal((handled!.output as any).recovered, true);
  });

  // ------------------------------------------------------------------
  // Compensation step referencing saga action output via templating
  // ------------------------------------------------------------------

  it("compensation step can reference the failed sibling step's context via _error", async () => {
    const seq = testSequence("saga-comp-references-error", [
      saga("saga1", [
        sagaStep(
          "s0",
          step("a0", "noop"),
          step("c0", "transform", { failed_step: "{{ context.data._error.failed_step }}" }),
        ),
        sagaStep("s1", step("a1", "fail", { message: "boom" })),
      ]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "failed");
    const outputs = await client.getOutputs(inst.id);
    const c0 = outputs.find((o: any) => o.block_id === "c0");
    assert.ok(c0, "compensation should have run");
    assert.equal((c0!.output as any).failed_step, "s1");
  });

  // ------------------------------------------------------------------
  // Many-step saga (stress ordering)
  // ------------------------------------------------------------------

  it("five-step saga where the last step fails rolls back all four prior compensations in order", async () => {
    const steps: SagaStep[] = [0, 1, 2, 3].map((i) =>
      sagaStep(`s${i}`, step(`a${i}`, "noop"), step(`c${i}`, "transform", { order: i })),
    );
    steps.push(sagaStep("s4", step("a4", "fail", { message: "final boom" })));

    const seq = testSequence("saga-five-step", [saga("saga1", steps) as any]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "failed");

    const outputs = await client.getOutputs(inst.id);
    for (let i = 0; i < 4; i++) {
      const c = outputs.find((o: any) => o.block_id === `c${i}`);
      assert.ok(c, `c${i} should have run`);
      assert.equal((c!.output as any).order, i);
    }
  });

  // ------------------------------------------------------------------
  // Saga step with output_schema on the action
  // ------------------------------------------------------------------

  it("saga action step with an output_schema still executes and produces output", async () => {
    const seq = testSequence("saga-output-schema", [
      saga("saga1", [
        sagaStep(
          "s0",
          step(
            "a0",
            "transform",
            { value: 42 },
            {
              output_schema: {
                type: "object",
                properties: { value: { type: "number" } },
                required: ["value"],
              },
            },
          ),
          step("c0", "noop"),
        ),
      ]) as any,
    ]);
    const { inst, final_ } = await run(seq);
    assert.equal(final_.state, "completed");
    const outputs = await client.getOutputs(inst.id);
    const a0 = outputs.find((o: any) => o.block_id === "a0");
    assert.equal((a0!.output as any).value, 42);
  });

  // ------------------------------------------------------------------
  // Malformed / invalid saga payloads (validation)
  // ------------------------------------------------------------------

  it("rejects a saga block missing the required action field", async () => {
    const seq = testSequence("saga-missing-action", [
      { type: "saga", id: "saga1", steps: [{ id: "s0" }] } as any,
    ]);
    try {
      await client.createSequence(seq);
      assert.fail("should reject a saga step without an action");
    } catch (err: any) {
      assert.ok(err.status === 400 || err.status === 422, `expected 4xx, got ${err.status}`);
    }
  });

  it("accepts a saga block id containing unicode characters", async () => {
    const seq = testSequence("saga-unicode-id", [
      saga("saga_日本語", [
        sagaStep("s0", step("a0", "noop"), step("c0", "noop")),
      ]) as any,
    ]);
    const created = await client.createSequence(seq);
    const fetched = await client.getSequence(created.id);
    assert.equal((fetched.blocks[0] as any).id, "saga_日本語");
  });
});
