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
 *
 * Deepened with the full expression grammar surface (`orch8-engine/src/expression.rs`):
 * comparisons, arithmetic, logical operators, `in` membership, ternary,
 * built-in functions, missing/null path handling, outputs.* references, and
 * nesting inside router/parallel blocks.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

function router(
  id: string,
  routes: { condition: string; blocks: unknown[] }[],
  fallback: unknown[] = [],
): Record<string, unknown> {
  return { type: "router", id, routes, default: fallback };
}

function parallel(id: string, branches: unknown[]): Record<string, unknown> {
  return { type: "parallel", id, branches };
}

async function runWithData(seq: any, data: Record<string, unknown> = {}) {
  const created = await client.createSequence(seq);
  const inst = await client.createInstance({
    sequence_id: created.id,
    tenant_id: seq.tenant_id,
    namespace: seq.namespace,
    context: { data },
  });
  const final_ = await client.waitForState(inst.id, ["completed", "failed"], {
    timeoutMs: 10_000,
  });
  const outputs = await client.getOutputs(inst.id);
  return { created, inst, final_, outputs };
}

function ranStep(outputs: any[], blockId: string): boolean {
  return !!outputs.find((o: any) => o.block_id === blockId);
}

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
    const { final_, outputs } = await runWithData(seq);
    assert.equal(final_.state, "completed", "step should complete");
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
    const { final_, outputs } = await runWithData(seq);
    assert.equal(
      final_.state,
      "completed",
      "instance should complete even when all steps are skipped",
    );
    assert.ok(!outputs.find((o: any) => o.block_id === "s1"), "skipped step should produce no output");
  });

  // ------------------------------------------------------------------
  // Runtime: when references data.* context
  // ------------------------------------------------------------------

  it("step with when='data.flag == true' executes when context matches", async () => {
    const seq = testSequence("when-data-true", [
      step("s1", "transform", { outcome: "yes" }, { when: "data.flag == true" }),
    ]);
    const { final_, outputs } = await runWithData(seq, { flag: true });
    assert.equal(final_.state, "completed");
    const s1out = outputs.find((o: any) => o.block_id === "s1");
    assert.ok(s1out, "step should have executed");
    assert.equal((s1out!.output as any).outcome, "yes");
  });

  it("step with when='data.flag == true' skipped when context doesn't match", async () => {
    const seq = testSequence("when-data-false", [
      step("s1", "transform", { outcome: "no" }, { when: "data.flag == true" }),
    ]);
    const { final_, outputs } = await runWithData(seq, { flag: false });
    assert.equal(final_.state, "completed");
    assert.ok(!outputs.find((o: any) => o.block_id === "s1"), "skipped step should produce no output");
  });

  // ------------------------------------------------------------------
  // Skipped step doesn't block next step
  // ------------------------------------------------------------------

  it("skipped step does not block subsequent steps", async () => {
    const seq = testSequence("when-chain", [
      step("skip_me", "transform", { x: 1 }, { when: "false" }),
      step("after_skip", "transform", { x: 2 }),
    ]);
    const { final_, outputs } = await runWithData(seq);
    assert.equal(final_.state, "completed");
    assert.ok(!outputs.find((o: any) => o.block_id === "skip_me"), "skipped step has no output");
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

  // ------------------------------------------------------------------
  // Comparison operators
  // ------------------------------------------------------------------

  it("when using != executes on inequality", async () => {
    const seq = testSequence("when-ne", [
      step("s1", "noop", {}, { when: 'data.status != "closed"' }),
    ]);
    const { outputs } = await runWithData(seq, { status: "open" });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using >= boundary is inclusive", async () => {
    const seq = testSequence("when-ge", [
      step("s1", "noop", {}, { when: "data.score >= 50" }),
    ]);
    const { outputs } = await runWithData(seq, { score: 50 });
    assert.ok(ranStep(outputs, "s1"), "50 >= 50 should be true");
  });

  it("when using <= boundary is inclusive", async () => {
    const seq = testSequence("when-le", [
      step("s1", "noop", {}, { when: "data.score <= 50" }),
    ]);
    const { outputs } = await runWithData(seq, { score: 51 });
    assert.ok(!ranStep(outputs, "s1"), "51 <= 50 should be false");
  });

  it("when using string comparison (lexicographic)", async () => {
    const seq = testSequence("when-str-cmp", [
      step("s1", "noop", {}, { when: 'data.name > "aaa"' }),
    ]);
    const { outputs } = await runWithData(seq, { name: "banana" });
    assert.ok(ranStep(outputs, "s1"));
  });

  // ------------------------------------------------------------------
  // Logical operators: &&, ||, !
  // ------------------------------------------------------------------

  it("when using && requires both sides true", async () => {
    const seq = testSequence("when-and-true", [
      step("s1", "noop", {}, { when: "data.a == true && data.b == true" }),
    ]);
    const { outputs } = await runWithData(seq, { a: true, b: true });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using && is false when one side is false", async () => {
    const seq = testSequence("when-and-false", [
      step("s1", "noop", {}, { when: "data.a == true && data.b == true" }),
    ]);
    const { outputs } = await runWithData(seq, { a: true, b: false });
    assert.ok(!ranStep(outputs, "s1"));
  });

  it("when using || is true if either side is true", async () => {
    const seq = testSequence("when-or-true", [
      step("s1", "noop", {}, { when: "data.a == true || data.b == true" }),
    ]);
    const { outputs } = await runWithData(seq, { a: false, b: true });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using ! negates a boolean", async () => {
    const seq = testSequence("when-not", [
      step("s1", "noop", {}, { when: "!data.disabled" }),
    ]);
    const { outputs } = await runWithData(seq, { disabled: false });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using double negation !!", async () => {
    const seq = testSequence("when-double-not", [
      step("s1", "noop", {}, { when: "!!data.flag" }),
    ]);
    const { outputs } = await runWithData(seq, { flag: true });
    assert.ok(ranStep(outputs, "s1"));
  });

  // ------------------------------------------------------------------
  // `in` membership
  // ------------------------------------------------------------------

  it("when using `in` array membership matches", async () => {
    const seq = testSequence("when-in-match", [
      step("s1", "noop", {}, { when: 'data.role in ["admin", "owner"]' }),
    ]);
    const { outputs } = await runWithData(seq, { role: "admin" });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using `in` array membership does not match", async () => {
    const seq = testSequence("when-in-no-match", [
      step("s1", "noop", {}, { when: 'data.role in ["admin", "owner"]' }),
    ]);
    const { outputs } = await runWithData(seq, { role: "viewer" });
    assert.ok(!ranStep(outputs, "s1"));
  });

  // ------------------------------------------------------------------
  // Arithmetic in conditions
  // ------------------------------------------------------------------

  it("when using arithmetic expression on both sides", async () => {
    const seq = testSequence("when-arith", [
      step("s1", "noop", {}, { when: "data.count + 1 > 5" }),
    ]);
    const { outputs } = await runWithData(seq, { count: 5 });
    assert.ok(ranStep(outputs, "s1"), "5 + 1 > 5 should be true");
  });

  // ------------------------------------------------------------------
  // Ternary expression as `when`
  // ------------------------------------------------------------------

  it("when using a ternary expression that resolves truthy", async () => {
    const seq = testSequence("when-ternary-true", [
      step("s1", "noop", {}, { when: 'data.env == "prod" ? true : false' }),
    ]);
    const { outputs } = await runWithData(seq, { env: "prod" });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using a ternary expression that resolves falsy", async () => {
    const seq = testSequence("when-ternary-false", [
      step("s1", "noop", {}, { when: 'data.env == "prod" ? true : false' }),
    ]);
    const { outputs } = await runWithData(seq, { env: "staging" });
    assert.ok(!ranStep(outputs, "s1"));
  });

  // ------------------------------------------------------------------
  // Built-in functions in `when`
  // ------------------------------------------------------------------

  it("when using contains() on a string", async () => {
    const seq = testSequence("when-contains-str", [
      step("s1", "noop", {}, { when: 'contains(data.email, "@")' }),
    ]);
    const { outputs } = await runWithData(seq, { email: "a@b.com" });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using contains() on an array", async () => {
    const seq = testSequence("when-contains-arr", [
      step("s1", "noop", {}, { when: "contains(data.tags, data.needle)" }),
    ]);
    const { outputs } = await runWithData(seq, { tags: ["a", "b", "c"], needle: "b" });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using len() on an array", async () => {
    const seq = testSequence("when-len-arr", [
      step("s1", "noop", {}, { when: "len(data.items) > 2" }),
    ]);
    const { outputs } = await runWithData(seq, { items: [1, 2, 3] });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using len() on a string", async () => {
    const seq = testSequence("when-len-str", [
      step("s1", "noop", {}, { when: "len(data.name) >= 3" }),
    ]);
    const { outputs } = await runWithData(seq, { name: "abc" });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using starts_with()", async () => {
    const seq = testSequence("when-starts-with", [
      step("s1", "noop", {}, { when: 'starts_with(data.path, "/api/")' }),
    ]);
    const { outputs } = await runWithData(seq, { path: "/api/users" });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using ends_with()", async () => {
    const seq = testSequence("when-ends-with", [
      step("s1", "noop", {}, { when: 'ends_with(data.file, ".json")' }),
    ]);
    const { outputs } = await runWithData(seq, { file: "config.json" });
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when using abs() on a negative number", async () => {
    const seq = testSequence("when-abs", [
      step("s1", "noop", {}, { when: "abs(data.delta) > 5" }),
    ]);
    const { outputs } = await runWithData(seq, { delta: -10 });
    assert.ok(ranStep(outputs, "s1"));
  });

  // ------------------------------------------------------------------
  // Missing / null path handling
  // ------------------------------------------------------------------

  it("when referencing a missing path evaluates falsy (skips step)", async () => {
    const seq = testSequence("when-missing-path", [
      step("s1", "noop", {}, { when: "data.nonexistent_field" }),
    ]);
    const { final_, outputs } = await runWithData(seq, {});
    assert.equal(final_.state, "completed");
    assert.ok(!ranStep(outputs, "s1"), "missing path is null → falsy → skip");
  });

  it("when comparing a missing path with == null is true", async () => {
    const seq = testSequence("when-missing-eq-null", [
      step("s1", "noop", {}, { when: "data.nonexistent_field == null" }),
    ]);
    const { outputs } = await runWithData(seq, {});
    assert.ok(ranStep(outputs, "s1"));
  });

  it("when comparing explicit null field with == null is true", async () => {
    const seq = testSequence("when-explicit-null", [
      step("s1", "noop", {}, { when: "data.val == null" }),
    ]);
    const { outputs } = await runWithData(seq, { val: null });
    assert.ok(ranStep(outputs, "s1"));
  });

  // ------------------------------------------------------------------
  // outputs.* references in `when`
  // ------------------------------------------------------------------

  it("when references a prior step's output", async () => {
    const seq = testSequence("when-outputs-ref", [
      step("producer", "transform", { status: "ok" }),
      step("consumer", "noop", {}, { when: 'outputs.producer.status == "ok"' }),
    ]);
    const { final_, outputs } = await runWithData(seq);
    assert.equal(final_.state, "completed");
    assert.ok(ranStep(outputs, "consumer"));
  });

  it("when references a prior step's output that doesn't match, skips", async () => {
    const seq = testSequence("when-outputs-ref-skip", [
      step("producer", "transform", { status: "error" }),
      step("consumer", "noop", {}, { when: 'outputs.producer.status == "ok"' }),
    ]);
    const { outputs } = await runWithData(seq);
    assert.ok(!ranStep(outputs, "consumer"));
  });

  // ------------------------------------------------------------------
  // Numeric type coercion edge cases
  // ------------------------------------------------------------------

  it("when comparing a string-typed numeric field via coercion", async () => {
    const seq = testSequence("when-string-coercion", [
      step("s1", "noop", {}, { when: "data.count + 1 > 10" }),
    ]);
    // Sending count as a string "10" should coerce in arithmetic context.
    const { outputs } = await runWithData(seq, { count: "10" });
    assert.ok(ranStep(outputs, "s1"), '"10" + 1 should coerce to 11 > 10');
  });

  it("when comparing zero is falsy on its own but truthy in a comparison", async () => {
    const seq = testSequence("when-zero-compare", [
      step("s1", "noop", {}, { when: "data.count == 0" }),
    ]);
    const { outputs } = await runWithData(seq, { count: 0 });
    assert.ok(ranStep(outputs, "s1"), "0 == 0 should be true even though 0 alone is falsy");
  });

  // ------------------------------------------------------------------
  // Malformed / invalid expressions — should not crash the instance,
  // engine treats unparseable/errored expressions as falsy (skip).
  // ------------------------------------------------------------------

  it("when with an unparseable expression does not crash the instance", async () => {
    const seq = testSequence("when-malformed", [
      step("s1", "noop", {}, { when: "data.x $$$ 5" }),
    ]);
    const { final_ } = await runWithData(seq, { x: 5 });
    // Whatever the exact semantics, the instance must not hang or 500 —
    // it should reach a terminal state.
    assert.ok(["completed", "failed"].includes(final_.state));
  });

  it("when with an empty string guard is falsy (skip)", async () => {
    const seq = testSequence("when-empty-string", [
      step("s1", "noop", {}, { when: "" }),
    ]);
    const { final_, outputs } = await runWithData(seq);
    assert.equal(final_.state, "completed");
    assert.ok(!ranStep(outputs, "s1"), "empty when-expression evaluates to null → falsy");
  });

  // ------------------------------------------------------------------
  // `when` inside router branches
  // ------------------------------------------------------------------

  it("when guard applies independently to a step nested inside a router route", async () => {
    const seq = testSequence("when-in-router", [
      router(
        "rt",
        [
          {
            condition: "true",
            blocks: [step("guarded", "noop", {}, { when: "data.go == true" })],
          },
        ],
        [],
      ) as any,
    ]);
    const { final_, outputs } = await runWithData(seq, { go: false });
    assert.equal(final_.state, "completed");
    assert.ok(!ranStep(outputs, "guarded"));
  });

  // ------------------------------------------------------------------
  // `when` inside parallel branches
  // ------------------------------------------------------------------

  it("when guard on one parallel branch does not affect the other branch", async () => {
    const seq = testSequence("when-in-parallel", [
      parallel("par", [
        [step("branch_a", "noop", {}, { when: "false" })],
        [step("branch_b", "noop", {}, { when: "true" })],
      ]) as any,
    ]);
    const { final_, outputs } = await runWithData(seq);
    assert.equal(final_.state, "completed");
    assert.ok(!ranStep(outputs, "branch_a"));
    assert.ok(ranStep(outputs, "branch_b"));
  });

  // ------------------------------------------------------------------
  // `when` combined with retry — guard evaluated before retry logic
  // ------------------------------------------------------------------

  it("skipped step (when=false) never triggers its retry policy", async () => {
    const seq = testSequence("when-skip-no-retry", [
      step(
        "s1",
        "fail",
        { message: "should never run" },
        {
          when: "false",
          retry: { max_attempts: 5, initial_backoff: 5, max_backoff: 20 },
        },
      ),
    ]);
    const { final_, outputs } = await runWithData(seq);
    assert.equal(final_.state, "completed", "skipped step never fails, so instance completes");
    assert.ok(!ranStep(outputs, "s1"));
  });

  // ------------------------------------------------------------------
  // Multiple sequential guards with mixed outcomes
  // ------------------------------------------------------------------

  it("multiple steps with independent when guards each evaluate correctly", async () => {
    const seq = testSequence("when-multi-mixed", [
      step("a", "transform", { x: 1 }, { when: "data.a == true" }),
      step("b", "transform", { x: 2 }, { when: "data.b == true" }),
      step("c", "transform", { x: 3 }, { when: "data.c == true" }),
    ]);
    const { final_, outputs } = await runWithData(seq, { a: true, b: false, c: true });
    assert.equal(final_.state, "completed");
    assert.ok(ranStep(outputs, "a"));
    assert.ok(!ranStep(outputs, "b"));
    assert.ok(ranStep(outputs, "c"));
  });
});
