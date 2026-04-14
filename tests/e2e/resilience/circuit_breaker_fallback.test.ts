/**
 * Verifies the circuit-breaker surface for a fallback-style scenario.
 *
 * Current engine state (see `circuit-breaker.test.ts` for the full write-up):
 *   - The registry IS wired into the step-dispatch fast and tree paths with
 *     per-tenant keying and persistence of `Open` rows. Pure control-flow
 *     built-ins (`fail`, `noop`, `sleep`, …) are excluded by
 *     `is_breaker_tracked` so test suites that intentionally fail don't
 *     trip the breaker.
 *   - The worker-task fail path is NOT wired into the breaker — failures
 *     reported by external workers via the HTTP API don't count toward
 *     trip. That's a separate activation still to do.
 *   - There is still no fallback-handler config on `StepDef`. The HTTP
 *     surface only exposes list/get/reset for operator-driven intervention.
 *
 * This test locks in the operator-facing API contract. When a fallback
 * mechanism is added to `StepDef`, extend it to cover trip + fallback.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, ApiError, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Circuit Breaker Fallback", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("exposes list/get/reset for operator-driven fallback control", async () => {
    // Reset on an untouched handler is idempotent (no-op per circuit_breaker.rs).
    const handlerName = `fallback-handler-${uuid().slice(0, 8)}`;
    await client.resetCircuitBreaker(handlerName);
    await client.resetCircuitBreaker(handlerName);

    // get() does not auto-create — an untouched handler remains 404.
    await assert.rejects(
      () => client.getCircuitBreaker(handlerName),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );

    // list() returns a valid JSON array (may be empty or contain entries
    // from other tests running against the shared server).
    const list = await client.listCircuitBreakers();
    assert.ok(Array.isArray(list), "listCircuitBreakers must return an array");

    // Engine gap: once the scheduler wires record_failure into the worker
    // fail path AND exposes a fallback_handler on StepDef, extend this test
    // to: (a) trip the breaker via N failures, (b) assert fallback handler
    // ran, (c) assert breaker is OPEN on get().
  });
});
