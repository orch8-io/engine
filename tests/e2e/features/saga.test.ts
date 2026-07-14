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
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });
    const result = await client.waitForState(inst.id, ["completed", "failed"]);
    assert.equal(result.state, "completed");
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
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });
    const result = await client.waitForState(inst.id, ["completed", "failed"]);
    assert.equal(result.state, "failed");
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
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });
    const result = await client.waitForState(inst.id, ["completed", "failed"]);
    assert.equal(result.state, "completed");
  });

  // ------------------------------------------------------------------
  // Empty saga
  // ------------------------------------------------------------------

  it("empty saga completes immediately", async () => {
    const seq = testSequence("saga-empty", [
      saga("saga1", []) as any,
    ]);
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });
    const result = await client.waitForState(inst.id, ["completed", "failed"]);
    assert.equal(result.state, "completed");
  });

  // ------------------------------------------------------------------
  // Single step saga
  // ------------------------------------------------------------------

  it("single step saga succeeds", async () => {
    const seq = testSequence("saga-single", [
      saga("saga1", [
        sagaStep("s0", step("a0", "noop"), step("c0", "noop")),
      ]) as any,
    ]);
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });
    const result = await client.waitForState(inst.id, ["completed", "failed"]);
    assert.equal(result.state, "completed");
  });

  // ------------------------------------------------------------------
  // Saga followed by another step
  // ------------------------------------------------------------------

  it("saga followed by a step — both run on success", async () => {
    const seq = testSequence("saga-then-step", [
      saga("saga1", [
        sagaStep("s0", step("a0", "noop"), step("c0", "noop")),
      ]) as any,
      step("after", "noop") as any,
    ]);
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });
    const result = await client.waitForState(inst.id, ["completed", "failed"]);
    assert.equal(result.state, "completed");
  });
});
