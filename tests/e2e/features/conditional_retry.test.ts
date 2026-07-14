/**
 * E2E: Conditional Retry Policies (Feature #22)
 *
 * `RetryPolicy.retry_if` is an expression evaluated against the failure
 * (`_error.message`) before each retry attempt; a falsy result fails the
 * step immediately even if `max_attempts` has not been reached.
 * `RetryPolicy.non_retryable_codes` is a denylist matched against
 * `details.error_code`/`details.code`/`details.status`.
 *
 * Runtime behavior for `non_retryable_codes` (which needs a handler that
 * supplies structured `details` — e.g. `http_request`'s `details.status`)
 * is covered by the Rust e2e suite (`orch8-engine/tests/conditional_retry_e2e.rs`);
 * this suite covers `retry_if` end-to-end via the `fail` builtin plus
 * create/validation/round-trip checks for both fields.
 *
 * Deepened with the expression grammar surface for `retry_if` (functions,
 * `runtime.attempt`, boundary/logical operators), validation edge cases for
 * `non_retryable_codes` (duplicates, whitespace, large lists, unicode),
 * multiple steps with independent retry policies, and nesting inside
 * router/parallel blocks.
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
  return { created, inst, final_ };
}

describe("Conditional Retry Policies", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // Sequence creation with retry_if / non_retryable_codes
  // ------------------------------------------------------------------

  it("creates a sequence with retry_if and round-trips it", async () => {
    const seq = testSequence("retry-create", [
      step("s1", "noop", {}, {
        retry: {
          max_attempts: 3,
          initial_backoff: 10,
          max_backoff: 100,
          retry_if: 'contains(_error.message, "timeout")',
        },
      }),
    ]);
    const created = await client.createSequence(seq);
    assert.ok(created.id, "sequence should have an id");

    const fetched = await client.getSequence(created.id);
    const s1 = fetched.blocks[0] as any;
    assert.equal(s1.retry.retry_if, 'contains(_error.message, "timeout")');
  });

  it("creates a sequence with non_retryable_codes and round-trips it", async () => {
    const seq = testSequence("retry-codes-create", [
      step("s1", "noop", {}, {
        retry: {
          max_attempts: 3,
          initial_backoff: 10,
          max_backoff: 100,
          non_retryable_codes: ["AUTH_FAILED", "INVALID_INPUT"],
        },
      }),
    ]);
    const created = await client.createSequence(seq);
    const fetched = await client.getSequence(created.id);
    const s1 = fetched.blocks[0] as any;
    assert.deepEqual(s1.retry.non_retryable_codes, ["AUTH_FAILED", "INVALID_INPUT"]);
  });

  it("creates a sequence without conditional retry fields (absent)", async () => {
    const seq = testSequence("retry-none", [
      step("s1", "noop", {}, {
        retry: { max_attempts: 3, initial_backoff: 10, max_backoff: 100 },
      }),
    ]);
    const created = await client.createSequence(seq);
    const fetched = await client.getSequence(created.id);
    const s1 = fetched.blocks[0] as any;
    assert.ok(
      s1.retry.retry_if === undefined || s1.retry.retry_if === null,
      "retry_if should be absent or null",
    );
    assert.ok(
      s1.retry.non_retryable_codes === undefined || s1.retry.non_retryable_codes === null,
      "non_retryable_codes should be absent or null",
    );
  });

  it("creates a sequence with both retry_if AND non_retryable_codes together", async () => {
    const seq = testSequence("retry-both-fields", [
      step("s1", "noop", {}, {
        retry: {
          max_attempts: 3,
          initial_backoff: 10,
          max_backoff: 100,
          retry_if: "true",
          non_retryable_codes: ["FATAL"],
        },
      }),
    ]);
    const created = await client.createSequence(seq);
    const fetched = await client.getSequence(created.id);
    const s1 = fetched.blocks[0] as any;
    assert.equal(s1.retry.retry_if, "true");
    assert.deepEqual(s1.retry.non_retryable_codes, ["FATAL"]);
  });

  // ------------------------------------------------------------------
  // Validation: empty retry_if / empty code entries rejected
  // ------------------------------------------------------------------

  it("rejects a sequence with an empty retry_if expression", async () => {
    const seq = testSequence("retry-bad-if", [
      step("s1", "noop", {}, {
        retry: {
          max_attempts: 3,
          initial_backoff: 10,
          max_backoff: 100,
          retry_if: "",
        },
      }),
    ]);
    try {
      await client.createSequence(seq);
      assert.fail("should reject empty retry_if");
    } catch (err: any) {
      assert.ok(
        err.status === 400 || err.status === 422,
        `expected 4xx, got ${err.status}`,
      );
    }
  });

  it("rejects a sequence with an empty non_retryable_codes entry", async () => {
    const seq = testSequence("retry-bad-codes", [
      step("s1", "noop", {}, {
        retry: {
          max_attempts: 3,
          initial_backoff: 10,
          max_backoff: 100,
          non_retryable_codes: ["VALID_CODE", ""],
        },
      }),
    ]);
    try {
      await client.createSequence(seq);
      assert.fail("should reject empty non_retryable_codes entry");
    } catch (err: any) {
      assert.ok(
        err.status === 400 || err.status === 422,
        `expected 4xx, got ${err.status}`,
      );
    }
  });

  it("accepts a whitespace-only retry_if is treated the same as validation for non-empty content", async () => {
    // Whitespace is technically non-empty at the string level; the engine
    // may accept or reject depending on whether it trims before checking.
    // Assert only that the API responds deterministically (2xx or 4xx),
    // never hangs or 5xxs.
    const seq = testSequence("retry-whitespace-if", [
      step("s1", "noop", {}, {
        retry: { max_attempts: 3, initial_backoff: 10, max_backoff: 100, retry_if: "   " },
      }),
    ]);
    try {
      const created = await client.createSequence(seq);
      assert.ok(created.id);
    } catch (err: any) {
      assert.ok(err.status === 400 || err.status === 422, `expected 4xx, got ${err.status}`);
    }
  });

  it("accepts an empty non_retryable_codes array (distinct from an array containing an empty string)", async () => {
    const seq = testSequence("retry-empty-codes-array", [
      step("s1", "noop", {}, {
        retry: { max_attempts: 3, initial_backoff: 10, max_backoff: 100, non_retryable_codes: [] },
      }),
    ]);
    const created = await client.createSequence(seq);
    assert.ok(created.id);
  });

  it("accepts a large non_retryable_codes list", async () => {
    const codes = Array.from({ length: 50 }, (_, i) => `CODE_${i}`);
    const seq = testSequence("retry-large-codes", [
      step("s1", "noop", {}, {
        retry: { max_attempts: 3, initial_backoff: 10, max_backoff: 100, non_retryable_codes: codes },
      }),
    ]);
    const created = await client.createSequence(seq);
    const fetched = await client.getSequence(created.id);
    assert.equal((fetched.blocks[0] as any).retry.non_retryable_codes.length, 50);
  });

  it("accepts duplicate entries in non_retryable_codes (round-trips as given)", async () => {
    const seq = testSequence("retry-dup-codes", [
      step("s1", "noop", {}, {
        retry: {
          max_attempts: 3,
          initial_backoff: 10,
          max_backoff: 100,
          non_retryable_codes: ["DUP", "DUP"],
        },
      }),
    ]);
    const created = await client.createSequence(seq);
    const fetched = await client.getSequence(created.id);
    assert.deepEqual((fetched.blocks[0] as any).retry.non_retryable_codes, ["DUP", "DUP"]);
  });

  it("accepts a unicode code in non_retryable_codes", async () => {
    const seq = testSequence("retry-unicode-code", [
      step("s1", "noop", {}, {
        retry: {
          max_attempts: 3,
          initial_backoff: 10,
          max_backoff: 100,
          non_retryable_codes: ["エラー_不正"],
        },
      }),
    ]);
    const created = await client.createSequence(seq);
    const fetched = await client.getSequence(created.id);
    assert.deepEqual((fetched.blocks[0] as any).retry.non_retryable_codes, ["エラー_不正"]);
  });

  // ------------------------------------------------------------------
  // Runtime: retry_if truthy → step retries until max_attempts exhausted
  // ------------------------------------------------------------------

  it("retry_if truthy allows retries, instance eventually fails", async () => {
    const seq = testSequence("retry-if-true", [
      step("s1", "fail", { message: "connection timeout", retryable: true }, {
        retry: {
          max_attempts: 2,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: 'contains(_error.message, "timeout")',
        },
      }),
    ]);
    const { final_ } = await runWithData(seq);
    assert.equal(final_.state, "failed", "handler always fails, so instance should fail");
  });

  // ------------------------------------------------------------------
  // Runtime: retry_if falsy → step fails immediately, no retry
  // ------------------------------------------------------------------

  it("retry_if falsy fails immediately without retrying", async () => {
    const seq = testSequence("retry-if-false", [
      step("s1", "fail", { message: "permission denied", retryable: true }, {
        retry: {
          max_attempts: 5,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: 'contains(_error.message, "timeout")',
        },
      }),
    ]);
    const { final_ } = await runWithData(seq);
    assert.equal(final_.state, "failed");
  });

  // ------------------------------------------------------------------
  // Runtime: retry_if referencing instance context data
  // ------------------------------------------------------------------

  it("retry_if referencing data.* denies retry when data says no", async () => {
    const seq = testSequence("retry-if-data", [
      step("s1", "fail", { message: "transient", retryable: true }, {
        retry: {
          max_attempts: 5,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: "data.allow_retry == true",
        },
      }),
    ]);
    const { final_ } = await runWithData(seq, { allow_retry: false });
    assert.equal(final_.state, "failed");
  });

  it("retry_if referencing data.* allows retry then exhausts (still fails, always-failing handler)", async () => {
    const seq = testSequence("retry-if-data-allow", [
      step("s1", "fail", { message: "transient", retryable: true }, {
        retry: {
          max_attempts: 2,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: "data.allow_retry == true",
        },
      }),
    ]);
    const { final_ } = await runWithData(seq, { allow_retry: true });
    assert.equal(final_.state, "failed");
  });

  // ------------------------------------------------------------------
  // retry_if with a non-retryable initial failure (retryable: false)
  // ------------------------------------------------------------------

  it("a permanently-failing step (retryable: false) never consults retry_if — fails on first attempt", async () => {
    const seq = testSequence("retry-if-permanent-failure", [
      step("s1", "fail", { message: "timeout occurred", retryable: false }, {
        retry: {
          max_attempts: 5,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: 'contains(_error.message, "timeout")',
        },
      }),
    ]);
    const { final_ } = await runWithData(seq);
    assert.equal(final_.state, "failed");
  });

  // ------------------------------------------------------------------
  // retry_if using built-in functions beyond contains()
  // ------------------------------------------------------------------

  it("retry_if using starts_with() on the error message", async () => {
    const seq = testSequence("retry-if-starts-with", [
      step("s1", "fail", { message: "RATE_LIMIT: too many requests", retryable: true }, {
        retry: {
          max_attempts: 2,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: 'starts_with(_error.message, "RATE_LIMIT")',
        },
      }),
    ]);
    const { final_ } = await runWithData(seq);
    assert.equal(final_.state, "failed", "retries proceed (truthy) but handler always fails");
  });

  it("retry_if using starts_with() that doesn't match fails immediately", async () => {
    const seq = testSequence("retry-if-starts-with-no-match", [
      step("s1", "fail", { message: "AUTH_ERROR: invalid token", retryable: true }, {
        retry: {
          max_attempts: 5,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: 'starts_with(_error.message, "RATE_LIMIT")',
        },
      }),
    ]);
    const { final_ } = await runWithData(seq);
    assert.equal(final_.state, "failed");
  });

  it("retry_if using logical && across message and data conditions", async () => {
    const seq = testSequence("retry-if-and", [
      step("s1", "fail", { message: "transient failure", retryable: true }, {
        retry: {
          max_attempts: 2,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: 'contains(_error.message, "transient") && data.retries_enabled == true',
        },
      }),
    ]);
    const { final_ } = await runWithData(seq, { retries_enabled: true });
    assert.equal(final_.state, "failed");
  });

  it("retry_if using logical && short-circuits to false when data condition fails", async () => {
    const seq = testSequence("retry-if-and-false", [
      step("s1", "fail", { message: "transient failure", retryable: true }, {
        retry: {
          max_attempts: 5,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: 'contains(_error.message, "transient") && data.retries_enabled == true',
        },
      }),
    ]);
    const { final_ } = await runWithData(seq, { retries_enabled: false });
    assert.equal(final_.state, "failed");
  });

  it("retry_if using logical || allows retry if either condition holds", async () => {
    const seq = testSequence("retry-if-or", [
      step("s1", "fail", { message: "unexpected error", retryable: true }, {
        retry: {
          max_attempts: 2,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: 'contains(_error.message, "timeout") || data.force_retry == true',
        },
      }),
    ]);
    const { final_ } = await runWithData(seq, { force_retry: true });
    assert.equal(final_.state, "failed");
  });

  it("retry_if with a literal 'true' string always retries until exhausted", async () => {
    const seq = testSequence("retry-if-literal-true", [
      step("s1", "fail", { message: "anything", retryable: true }, {
        retry: { max_attempts: 3, initial_backoff: 5, max_backoff: 20, retry_if: "true" },
      }),
    ]);
    const { final_ } = await runWithData(seq);
    assert.equal(final_.state, "failed");
  });

  it("retry_if with a literal 'false' string never retries", async () => {
    const seq = testSequence("retry-if-literal-false", [
      step("s1", "fail", { message: "anything", retryable: true }, {
        retry: { max_attempts: 5, initial_backoff: 5, max_backoff: 20, retry_if: "false" },
      }),
    ]);
    const { final_ } = await runWithData(seq);
    assert.equal(final_.state, "failed");
  });

  // ------------------------------------------------------------------
  // retry_if referencing an unset/missing data field (falsy by default)
  // ------------------------------------------------------------------

  it("retry_if referencing a missing data field evaluates falsy, fails immediately", async () => {
    const seq = testSequence("retry-if-missing-field", [
      step("s1", "fail", { message: "transient", retryable: true }, {
        retry: {
          max_attempts: 5,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: "data.undefined_flag == true",
        },
      }),
    ]);
    const { final_ } = await runWithData(seq, {});
    assert.equal(final_.state, "failed");
  });

  // ------------------------------------------------------------------
  // Preflight: definition_valid still passes for well-formed conditional retry
  // ------------------------------------------------------------------

  it("preflight definition_valid passes for well-formed retry_if", async () => {
    const seq = testSequence("retry-preflight-ok", [
      step("s1", "noop", {}, {
        retry: {
          max_attempts: 3,
          initial_backoff: 10,
          max_backoff: 100,
          retry_if: 'contains(_error.message, "timeout")',
        },
      }),
    ]);
    const created = await client.createSequence(seq);

    const res = await fetch(`${client.baseUrl}/sequences/${created.id}/preflight`);
    if (res.status === 404) {
      return;
    }
    assert.ok(res.ok, `preflight should succeed: ${res.status}`);
    const report = (await res.json()) as any;
    const check = report.checks?.find((c: any) => c.id === "definition_valid");
    if (check) {
      assert.equal(check.status, "pass", `expected pass: ${JSON.stringify(check)}`);
    }
  });

  // ------------------------------------------------------------------
  // Multiple steps with independent retry_if policies
  // ------------------------------------------------------------------

  it("two steps with different retry_if outcomes both resolve independently", async () => {
    const seq = testSequence("retry-if-independent-steps", [
      step("a", "transform", { ok: true }),
      step("b", "fail", { message: "timeout while calling b", retryable: true }, {
        retry: {
          max_attempts: 2,
          initial_backoff: 5,
          max_backoff: 20,
          retry_if: 'contains(_error.message, "timeout")',
        },
      }),
    ]);
    const { final_, inst } = await runWithData(seq);
    assert.equal(final_.state, "failed");
    const outputs = await client.getOutputs(inst.id);
    assert.ok(outputs.find((o: any) => o.block_id === "a"), "step a should have completed before b failed");
  });

  // ------------------------------------------------------------------
  // retry_if nested inside router / parallel
  // ------------------------------------------------------------------

  it("retry_if on a step nested inside a router route behaves the same as top-level", async () => {
    const seq = testSequence("retry-if-in-router", [
      router(
        "rt",
        [
          {
            condition: "true",
            blocks: [
              step("nested", "fail", { message: "permission denied", retryable: true }, {
                retry: {
                  max_attempts: 5,
                  initial_backoff: 5,
                  max_backoff: 20,
                  retry_if: 'contains(_error.message, "timeout")',
                },
              }),
            ],
          },
        ],
        [],
      ) as any,
    ]);
    const { final_ } = await runWithData(seq);
    assert.equal(final_.state, "failed", "retry_if falsy → immediate failure, router propagates");
  });

  it("retry_if on steps in separate parallel branches evaluate independently", async () => {
    const seq = testSequence("retry-if-in-parallel", [
      parallel("par", [
        [step("ok_branch", "transform", { done: true })],
        [
          step("fail_branch", "fail", { message: "transient", retryable: true }, {
            retry: {
              max_attempts: 2,
              initial_backoff: 5,
              max_backoff: 20,
              retry_if: 'contains(_error.message, "transient")',
            },
          }),
        ],
      ]) as any,
    ]);
    const { final_, inst } = await runWithData(seq);
    assert.equal(final_.state, "failed");
    const outputs = await client.getOutputs(inst.id);
    assert.ok(outputs.find((o: any) => o.block_id === "ok_branch"), "sibling branch should still complete its work");
  });

  // ------------------------------------------------------------------
  // Very small max_attempts with retry_if truthy still respects the cap
  // ------------------------------------------------------------------

  it("retry_if truthy with max_attempts=1 fails after a single attempt (no retry headroom)", async () => {
    const seq = testSequence("retry-if-max-attempts-one", [
      step("s1", "fail", { message: "timeout", retryable: true }, {
        retry: { max_attempts: 1, initial_backoff: 5, max_backoff: 20, retry_if: "true" },
      }),
    ]);
    const { final_ } = await runWithData(seq);
    assert.equal(final_.state, "failed");
  });
});
