import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

/**
 * Circuit breaker API.
 *
 * Findings from `orch8-engine/src/circuit_breaker.rs` and
 * `orch8-api/src/circuit_breakers.rs`:
 *   - The registry is in-memory (DashMap) on the server process, keyed by
 *     `(TenantId, handler)`. `Open` transitions are mirrored to the
 *     `circuit_breakers` table and rehydrated on boot.
 *   - The engine wires the breaker into step dispatch on both the fast path
 *     (`scheduler::step_exec::execute_step_block`) and the tree path
 *     (`handlers::step_block::execute_step_node`). `check` runs pre-flight;
 *     `record_success` / `record_failure` fire after handler completion.
 *   - Pure control-flow built-ins (`noop`, `log`, `sleep`, `fail`,
 *     `self_modify`, `emit_event`, `send_signal`, `query_instance`,
 *     `human_review`) are skipped by `circuit_breaker::is_breaker_tracked`
 *     — they don't represent external dependencies.
 *   - On first GET for a handler that has never been touched, the state is
 *     `NotFound` (404) because `get()` does not auto-create (only `check()`
 *     and `record_failure()` do).
 *
 * These tests lock in the operator-facing API surface (list/get/reset).
 * Engine-level trip-on-failure behaviour is covered by Rust unit tests on
 * `CircuitBreakerRegistry` — driving it end-to-end requires either a
 * test-only mockable handler or reliable network-failing calls, neither of
 * which is currently exposed via the e2e harness.
 */
describe("Circuit breaker API", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("GET /circuit-breakers returns a JSON array", async () => {
    const list = await client.listCircuitBreakers();
    assert.ok(Array.isArray(list), "list should return an array");
  });

  it("GET /circuit-breakers/{handler} returns 404 for an untouched handler", async () => {
    await assert.rejects(
      () => client.getCircuitBreaker("never-seen-handler-abc123"),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      }
    );
  });

  it("POST /circuit-breakers/{handler}/reset is idempotent and accepts unknown handlers", async () => {
    // Per circuit_breaker.rs::reset, resetting an untracked handler is a no-op.
    await client.resetCircuitBreaker("ghost-handler-xyz");
    // Calling again should also succeed.
    await client.resetCircuitBreaker("ghost-handler-xyz");

    // Reset must not auto-create an entry.
    await assert.rejects(
      () => client.getCircuitBreaker("ghost-handler-xyz"),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      }
    );
  });
});
