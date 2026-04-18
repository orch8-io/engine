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
 *   - The registry is an in-memory `HashMap` on the server process.
 *   - Nothing in the engine's scheduler OR the worker-task `fail` path
 *     currently calls `record_failure` or `record_success` — the breaker
 *     is NOT automatically tripped by unknown handlers or by failed worker
 *     tasks (verified by grep: `record_failure` only appears in the module's
 *     own unit tests). The HTTP API is therefore what's testable end-to-end.
 *   - On first GET for a handler that has never been touched, the state is
 *     `NotFound` (404), because `get()` does not auto-create (only `check()`
 *     and `record_failure()` do). That distinguishes the list vs. get paths.
 *
 * These tests lock in the *API surface*: list, get-missing, reset (idempotent).
 * A future commit that wires the breaker into the scheduler should add the
 * "N failures trip the breaker" scenario.
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
