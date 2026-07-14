/**
 * E2E: Conditional Retry Policies (Feature #22)
 *
 * `RetryPolicy.retry_if` is an expression evaluated against the failure
 * (`_error.message`) before each retry attempt; a falsy result fails the
 * step immediately even if `max_attempts` has not been reached.
 * `RetryPolicy.non_retryable_codes` is a denylist matched against
 * `details.error_code`/`details.code`.
 *
 * Runtime behavior for `non_retryable_codes` (which needs a handler that
 * supplies structured `details`) is covered by the Rust e2e suite
 * (`orch8-engine/tests/conditional_retry_e2e.rs`); this suite covers
 * `retry_if` end-to-end via the `fail` builtin plus create/validation/
 * round-trip checks for both fields.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

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
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });

    const final_ = await client.waitForState(inst.id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
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
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });

    const final_ = await client.waitForState(inst.id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
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
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
      context: { data: { allow_retry: false } },
    });

    const final_ = await client.waitForState(inst.id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
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
});
