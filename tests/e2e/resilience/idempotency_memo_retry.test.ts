/**
 * Verifies that idempotency_key-based deduplication survives a retry
 * cascade on the surviving instance.
 *
 * What's testable today:
 *   - `createInstance` with the same `idempotency_key` returns the
 *     existing instance id + `deduplicated: true` (see idempotency.test.ts).
 *   - A subsequent retry of the surviving instance does NOT spawn a new
 *     instance via the dedup path, and another same-key create still
 *     resolves to the same id after retry.
 *
 * Memoization of *handler outputs* across retries requires a replay API
 * that doesn't exist today — see features/output_memoization.test.ts for
 * the full reasoning (StepContext memoization only fires when
 * `exec.attempt > 0` AND a saved BlockOutput exists, but no public
 * endpoint forces re-execution of a Completed step node). We therefore
 * assert the observable idempotency surface instead.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Idempotency + Memoization + Retry Triple Interaction", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("dedups creates by idempotency_key across the instance lifecycle (fail -> retry -> fail)", async () => {
    const tenantId = `test-${uuid().slice(0, 8)}`;
    const idempotencyKey = `idem-${uuid()}`;

    // Permanently failing handler so we can observe retry() moving state
    // without introducing non-determinism from a flaky handler.
    const seq = testSequence(
      "idem-retry",
      [step("s1", "fail", { message: "persistent", retryable: false })],
      { tenantId },
    );
    await client.createSequence(seq);

    // First create.
    const first = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      idempotency_key: idempotencyKey,
    });
    assert.ok(first.id);

    // Wait for the instance to settle (it'll fail permanently).
    await client.waitForState(first.id, "failed", { timeoutMs: 10_000 });

    // Second create with the same idempotency_key returns the SAME id and
    // is marked as a duplicate — the dedup path doesn't double-run the
    // side-effectful handler even though the original instance is now
    // terminal.
    const second = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      idempotency_key: idempotencyKey,
    });
    assert.equal(second.id, first.id, "same idempotency_key must return same id");
    assert.equal(
      (second as { deduplicated?: boolean }).deduplicated,
      true,
      "duplicate create must be flagged as deduplicated",
    );

    // Now force a retry of the surviving instance. It will fail again, but
    // no NEW instance is created — the dedup record still binds the key
    // to the same id.
    const retried = await client.retryInstance(first.id);
    assert.equal(retried.state, "scheduled");
    await client.waitForState(first.id, "failed", { timeoutMs: 10_000 });

    // Third create with the same key — still the same id. Side effects (in
    // the idempotency-record sense) fired exactly once at original creation.
    const third = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      idempotency_key: idempotencyKey,
    });
    assert.equal(third.id, first.id, "third create must still dedup to the original id");

    // Sanity: a DIFFERENT key bypasses dedup and creates a distinct instance.
    const distinct = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      idempotency_key: `idem-${uuid()}`,
    });
    assert.notEqual(
      distinct.id,
      first.id,
      "a fresh idempotency_key must produce a new instance id",
    );

    // Engine gap: true handler-output memoization across retries needs a
    // replay API (documented in features/output_memoization.test.ts). Once
    // that lands, extend this test to assert a handler's side-effect
    // counter increments exactly once across the retry.
  });
});
