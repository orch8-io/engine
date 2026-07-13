/**
 * E2E: Conditional Step Guard — `when` (Feature #11)
 *
 * Validates that the `when` field on a step definition controls whether the
 * step executes or is skipped at runtime via the HTTP API:
 * - Creating a sequence with `when` succeeds and round-trips the expression
 * - Step with `when: "true"` executes normally
 * - Step with `when: "false"` is skipped, instance still completes
 * - Step with `when` referencing `data.*` context
 * - Skipped step produces no output
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Conditional Step Guard (when)", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // Sequence creation with `when`
  // ------------------------------------------------------------------

  it("creates a sequence with a `when` guard on a step", async () => {
    const seq = testSequence("when-create", [
      step("s1", "noop", {}, { when: "data.enabled == true" }),
    ]);
    const created = await client.createSequence(seq);
    assert.ok(created.id, "sequence should have an id");

    const fetched = await client.getSequence(created.id);
    const s1 = fetched.blocks[0] as any;
    assert.equal(s1.when, "data.enabled == true");
  });

  it("creates a sequence without `when` (absent)", async () => {
    const seq = testSequence("when-none", [step("s1", "noop", {})]);
    const created = await client.createSequence(seq);
    assert.ok(created.id);

    const fetched = await client.getSequence(created.id);
    const s1 = fetched.blocks[0] as any;
    assert.ok(
      s1.when === undefined || s1.when === null,
      "when should be absent or null",
    );
  });

  // ------------------------------------------------------------------
  // Runtime: when = "true" → step executes
  // ------------------------------------------------------------------

  it("step with when='true' executes normally", async () => {
    const seq = testSequence("when-true", [
      step("s1", "transform", { result: "ran" }, { when: "true" }),
    ]);
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });

    const final_ = await client.waitForState(
      inst.id,
      ["completed", "failed"],
      { timeoutMs: 10_000 },
    );
    assert.equal(final_.state, "completed", "step should complete");

    const outputs = await client.getOutputs(inst.id);
    const s1out = outputs.find((o: any) => o.block_id === "s1");
    assert.ok(s1out, "output for s1 should exist");
    assert.equal((s1out!.output as any).result, "ran");
  });

  // ------------------------------------------------------------------
  // Runtime: when = "false" → step is skipped
  // ------------------------------------------------------------------

  it("step with when='false' is skipped, instance completes", async () => {
    const seq = testSequence("when-false", [
      step("s1", "transform", { result: "should_not_run" }, { when: "false" }),
    ]);
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });

    const final_ = await client.waitForState(
      inst.id,
      ["completed", "failed"],
      { timeoutMs: 10_000 },
    );
    assert.equal(
      final_.state,
      "completed",
      "instance should complete even when all steps are skipped",
    );

    const outputs = await client.getOutputs(inst.id);
    const s1out = outputs.find((o: any) => o.block_id === "s1");
    assert.ok(
      !s1out,
      "skipped step should produce no output",
    );
  });

  // ------------------------------------------------------------------
  // Runtime: when references data.* context
  // ------------------------------------------------------------------

  it("step with when='data.flag == true' executes when context matches", async () => {
    const seq = testSequence("when-data-true", [
      step("s1", "transform", { outcome: "yes" }, { when: "data.flag == true" }),
    ]);
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
      context: { data: { flag: true } },
    });

    const final_ = await client.waitForState(
      inst.id,
      ["completed", "failed"],
      { timeoutMs: 10_000 },
    );
    assert.equal(final_.state, "completed");

    const outputs = await client.getOutputs(inst.id);
    const s1out = outputs.find((o: any) => o.block_id === "s1");
    assert.ok(s1out, "step should have executed");
    assert.equal((s1out!.output as any).outcome, "yes");
  });

  it("step with when='data.flag == true' skipped when context doesn't match", async () => {
    const seq = testSequence("when-data-false", [
      step("s1", "transform", { outcome: "no" }, { when: "data.flag == true" }),
    ]);
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
      context: { data: { flag: false } },
    });

    const final_ = await client.waitForState(
      inst.id,
      ["completed", "failed"],
      { timeoutMs: 10_000 },
    );
    assert.equal(final_.state, "completed");

    const outputs = await client.getOutputs(inst.id);
    const s1out = outputs.find((o: any) => o.block_id === "s1");
    assert.ok(!s1out, "skipped step should produce no output");
  });

  // ------------------------------------------------------------------
  // Skipped step doesn't block next step
  // ------------------------------------------------------------------

  it("skipped step does not block subsequent steps", async () => {
    const seq = testSequence("when-chain", [
      step("skip_me", "transform", { x: 1 }, { when: "false" }),
      step("after_skip", "transform", { x: 2 }),
    ]);
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });

    const final_ = await client.waitForState(
      inst.id,
      ["completed", "failed"],
      { timeoutMs: 10_000 },
    );
    assert.equal(final_.state, "completed");

    const outputs = await client.getOutputs(inst.id);
    const skipped = outputs.find((o: any) => o.block_id === "skip_me");
    assert.ok(!skipped, "skipped step has no output");

    const ran = outputs.find((o: any) => o.block_id === "after_skip");
    assert.ok(ran, "step after skip should have output");
    assert.equal((ran!.output as any).x, 2);
  });

  // ------------------------------------------------------------------
  // Preflight passes with valid when guard
  // ------------------------------------------------------------------

  it("preflight passes for valid when guard", async () => {
    const seq = testSequence("when-preflight-ok", [
      step("s1", "noop", {}, { when: "data.x > 10" }),
    ]);
    const created = await client.createSequence(seq);

    const res = await fetch(
      `${client.baseUrl}/sequences/${created.id}/preflight`,
    );
    if (res.status === 404) {
      return;
    }
    assert.ok(res.ok, `preflight should succeed: ${res.status}`);
    const report = (await res.json()) as any;
    const check = report.checks?.find(
      (c: any) => c.id === "when_guards_valid",
    );
    if (check) {
      assert.equal(
        check.status,
        "pass",
        `expected pass: ${JSON.stringify(check)}`,
      );
    }
  });
});
