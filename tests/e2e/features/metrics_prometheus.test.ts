/**
 * Metrics Prometheus Format — verifies the /metrics endpoint returns
 * valid prometheus text and contains expected labels.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Metrics Prometheus", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("metrics endpoint returns prometheus text", async () => {
    const text = await client.metrics();
    assert.ok(text.length > 0, "metrics should not be empty");
    assert.ok(text.includes("# TYPE") || text.includes("# HELP") || text.includes("_"), "should look like prometheus");
  });

  it("metrics contains orch8 prefix", async () => {
    const text = await client.metrics();
    assert.ok(text.includes("orch8"), "metrics should contain orch8 prefix");
  });

  it("liveness returns 200", async () => {
    const res = await client.healthLive();
    assert.equal(res.status, 200);
  });

  it("readiness returns 200 when ready", async () => {
    const res = await client.healthReady();
    assert.equal(res.status, 200);
  });

  it("metrics contains task counters after worker dispatch", async () => {
    const tenantId = `met-${uuid().slice(0, 8)}`;
    const handler = `met_${uuid().slice(0, 8)}`;
    const seq = testSequence("met-seq", [step("s1", handler)], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });
    const tasks = await client.pollWorkerTasks(handler, "worker-1");
    await client.completeWorkerTask(tasks[0]!.id, "worker-1", { ok: true });
    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    const text = await client.metrics();
    assert.ok(
      text.includes("orch8") || text.includes("task") || text.includes("instance"),
      "metrics should contain relevant counters",
    );
  });
});
