/**
 * Verifies the circuit-breaker surface for a fallback-style scenario.
 *
 * Engine gap (confirmed via `orch8-engine/src/circuit_breaker.rs` and
 * `orch8-api/src/circuit_breakers.rs`, and documented in
 * `tests/e2e/circuit-breaker.test.ts`):
 *   - The breaker registry is in-memory and NOT wired into the scheduler or
 *     the worker-task fail path today. `record_failure` / `record_success`
 *     are only called by the module's own unit tests.
 *   - There is no API to configure a fallback handler tied to an OPEN
 *     breaker; the HTTP surface only exposes list/get/reset.
 *
 * Given that, the plan's "trip breaker via N failures, then watch fallback
 * kick in" cannot be exercised end-to-end. This test instead locks in the
 * operator-facing API contract so that a future change which wires the
 * breaker (and a fallback mechanism) has an existing anchor to extend.
 * When that lands, replace the body below with a real trip+fallback scenario.
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
