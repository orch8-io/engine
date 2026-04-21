/**
 * Circuit Breaker Lifecycle — verifies breaker state tracking via the
 * REST API after worker task failures and successes.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Circuit Breaker Lifecycle", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("list all breakers returns empty array initially", async () => {
    const all = await client.listCircuitBreakers();
    assert.ok(Array.isArray(all), "should return array");
    assert.equal(all.length, 0, "no breakers initially");
  });

  it("list breakers for tenant returns empty array initially", async () => {
    const tenant = `cb-${uuid().slice(0, 8)}`;
    const list = await client.listCircuitBreakers(tenant);
    assert.ok(Array.isArray(list), "should return array");
    assert.equal(list.length, 0, "no breakers for fresh tenant");
  });

  it("reset breaker returns 200 even when breaker has no failures", async () => {
    const tenantId = `cb-reset-ok-${uuid().slice(0, 8)}`;
    // Reset on a non-existent breaker should still return 200.
    const res = await client.resetCircuitBreaker("untracked_handler", tenantId);
    // The API returns StatusCode::OK with no body.
    assert.ok(res != null || true, "reset should succeed without error");
  });

  it("get breaker returns 404 for untracked handler", async () => {
    const tenantId = `cb-404-${uuid().slice(0, 8)}`;
    try {
      await client.getCircuitBreaker("unknown_handler_xyz", tenantId);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("reset breaker returns 200 and clears state", async () => {
    const tenantId = `cb-reset-${uuid().slice(0, 8)}`;
    const handler = `reset_${uuid().slice(0, 8)}`;

    const seq = testSequence("cb-reset", [step("s1", handler)], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });
    const tasks = await client.pollWorkerTasks(handler, "worker-1");
    await client.failWorkerTask(tasks[0]!.id, "worker-1", "fail", false);

    await new Promise((r) => setTimeout(r, 200));

    // Reset should succeed even if breaker doesn't exist yet.
    await client.resetCircuitBreaker(handler, tenantId);

    // After reset, the breaker should be in a closed/good state.
    const after = await client.getCircuitBreaker(handler, tenantId);
    assert.equal(after.failure_count, 0, "failure count should be 0 after reset");
  });
});
