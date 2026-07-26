/**
 * GET /instances/{id}/diagnosis — OPEN_CIRCUIT_BREAKER evidence
 * (doctor.rs `rule_open_breaker`).
 *
 * The lifecycle suite (circuit_breaker_lifecycle.test.ts) lists and resets
 * breakers but never trips one through real worker failures. This suite
 * opens a breaker by reporting `failure_threshold` (server default: 5)
 * permanent worker-task failures for a unique handler, then asserts the
 * diagnosis endpoint surfaces the open breaker:
 *   - directly-involved handler (instance has a pending worker task for it)
 *     → direct_evidence / high confidence / warning severity, with a
 *     "retries resume in …" cooldown hint while `opened_at` is fresh
 *     (cooldown: 60s);
 *   - instance diagnosed after the trip (no worker task for the handler —
 *     dispatch is deferred by the open breaker) → health_warning / low
 *     confidence;
 *   - after POST …/circuit-breakers/{handler}/reset the warning is gone.
 *
 * Persistence of the Open transition (and its deletion on reset) is
 * fire-and-forget in the breaker registry, so diagnosis assertions poll
 * briefly instead of assuming the store caught up synchronously.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

/** Server default (orch8-server/src/main.rs): threshold 5, cooldown 60s. */
const FAILURE_THRESHOLD = 5;

/**
 * Poll `getDiagnosis` until `predicate` holds or the deadline passes.
 * Returns the last report either way so callers can assert on it.
 */
async function waitForDiagnosis(
  id: string,
  predicate: (report: any) => boolean,
  timeoutMs: number = 10_000,
): Promise<any> {
  const deadline = Date.now() + timeoutMs;
  let report: any;
  do {
    report = await client.getDiagnosis(id);
    if (predicate(report)) return report;
    await new Promise((r) => setTimeout(r, 100));
  } while (Date.now() < deadline);
  return report;
}

/**
 * Trip the breaker for `handler` by running `FAILURE_THRESHOLD` instances
 * whose only step is permanently failed by a worker.
 */
async function openBreakerViaFailures(
  handler: string,
  tenantId: string,
  seqId: string,
  workerId: string,
): Promise<void> {
  for (let i = 0; i < FAILURE_THRESHOLD; i++) {
    const { id } = await client.createInstance({
      sequence_id: seqId,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
    const tasks = await client.pollWorkerTasks(handler, workerId);
    assert.equal(tasks.length, 1, `failure round ${i}: expected one task`);
    await client.failWorkerTask(tasks[0]!.id, workerId, "permanent boom", false);
    await client.waitForState(id, "failed", { timeoutMs: 10_000 });
  }
}

describe("Execution Doctor — open circuit breaker evidence", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("involved handler: direct evidence with cooldown hint; reset clears the warning", async () => {
    const tenantId = `doc-cb-${uuid().slice(0, 8)}`;
    const handler = `doc_breaker_${uuid().slice(0, 8)}`;
    const workerId = `wb-${uuid().slice(0, 6)}`;
    const seq = testSequence("doc-cb-involved", [step("s1", handler, {})], {
      tenantId,
    });
    await client.createSequence(seq);

    // Probe instance: its worker task is created BEFORE the breaker trips,
    // so the handler is "directly involved" in this instance's diagnosis.
    const probe = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(probe.id, "waiting", { timeoutMs: 10_000 });

    await openBreakerViaFailures(handler, tenantId, seq.id, workerId);

    const breaker = await client.getCircuitBreaker(handler, tenantId);
    assert.equal(breaker.state, "open");
    assert.equal(breaker.failure_threshold, FAILURE_THRESHOLD);
    assert.ok(
      breaker.failure_count >= FAILURE_THRESHOLD,
      `expected >= ${FAILURE_THRESHOLD} failures, got ${breaker.failure_count}`,
    );
    assert.ok(breaker.opened_at, "opened_at must be set on a tripped breaker");

    const report = await waitForDiagnosis(probe.id, (r) =>
      r.diagnoses.some((x: any) => x.code === "OPEN_CIRCUIT_BREAKER"),
    );
    const d = report.diagnoses.find(
      (x: any) => x.code === "OPEN_CIRCUIT_BREAKER",
    );
    assert.ok(d, `expected OPEN_CIRCUIT_BREAKER in ${JSON.stringify(report.diagnoses)}`);
    assert.equal(d.category, "direct_evidence");
    assert.equal(d.confidence, "high");
    assert.equal(d.severity, "warning");
    assert.equal(d.health, "degraded");
    assert.ok(
      d.summary.includes(`handler '${handler}'`),
      `summary must name the handler: ${d.summary}`,
    );
    // opened_at is fresh and the 60s cooldown has not elapsed → hint present.
    assert.match(d.summary, /retries resume in /);
    const ev = (d.evidence ?? []).find((e: any) => e.label === "opened_at");
    assert.ok(ev, "opened_at evidence must be attached");
    assert.ok(
      Math.abs(Date.now() - Date.parse(ev.summary)) < 60_000,
      "opened_at evidence must be fresh",
    );

    // Reset → breaker closes → the diagnosis warning disappears.
    await client.resetCircuitBreaker(handler, tenantId);
    const after = await client.getCircuitBreaker(handler, tenantId);
    assert.equal(after.state, "closed");
    assert.equal(after.failure_count, 0);

    const cleared = await waitForDiagnosis(
      probe.id,
      (r) => !r.diagnoses.some((x: any) => x.code === "OPEN_CIRCUIT_BREAKER"),
    );
    assert.ok(
      !cleared.diagnoses.some((x: any) => x.code === "OPEN_CIRCUIT_BREAKER"),
      `warning must be gone after reset: ${JSON.stringify(cleared.diagnoses)}`,
    );
  });

  it("instance created after the trip: low-confidence health warning", async () => {
    const tenantId = `doc-cb-${uuid().slice(0, 8)}`;
    const handler = `doc_breaker_${uuid().slice(0, 8)}`;
    const workerId = `wb-${uuid().slice(0, 6)}`;
    const seq = testSequence("doc-cb-uninvolved", [step("s1", handler, {})], {
      tenantId,
    });
    await client.createSequence(seq);

    // Trip the breaker FIRST. A later instance's dispatch is deferred by the
    // open breaker, so it has no worker task for the handler — the finding
    // classifies as a low-confidence health warning, not direct evidence.
    await openBreakerViaFailures(handler, tenantId, seq.id, workerId);
    const breaker = await client.getCircuitBreaker(handler, tenantId);
    assert.equal(breaker.state, "open");

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    const report = await waitForDiagnosis(id, (r) =>
      r.diagnoses.some((x: any) => x.code === "OPEN_CIRCUIT_BREAKER"),
    );
    const d = report.diagnoses.find(
      (x: any) => x.code === "OPEN_CIRCUIT_BREAKER",
    );
    assert.ok(d, `expected OPEN_CIRCUIT_BREAKER in ${JSON.stringify(report.diagnoses)}`);
    assert.equal(d.category, "health_warning");
    assert.equal(d.confidence, "low");
    assert.equal(d.severity, "warning");
    assert.equal(d.health, "degraded");
    assert.match(d.summary, /retries resume in /);

    // Leave no breaker state behind for other suites sharing the server.
    await client.resetCircuitBreaker(handler, tenantId);
  });
});
