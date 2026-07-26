/**
 * Capability-scoped principals — the `worker` capability.
 *
 * Worker keys authenticate external executors: they may poll/complete/fail/
 * heartbeat tasks and read the handler catalog (`/workers/**`, `/handlers`),
 * and nothing else. Includes a full poll→complete flow driven entirely by a
 * worker-scoped key, plus negative checks across every other route family.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import {
  Orch8Client,
  ApiError,
  testSequence,
  step,
  uuid,
} from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const ROOT_KEY = `root-principals-wk-${uuid().slice(0, 8)}`;

describe("Principals — worker capability", () => {
  let server: ServerHandle | undefined;
  let root: Orch8Client;
  let tenantId: string;
  let worker: Orch8Client;
  let workerSecret: string;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_API_KEY: ROOT_KEY,
        ORCH8_ALLOW_NO_TENANT_ISOLATION: "1",
      },
    });
    root = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": ROOT_KEY,
    });
    tenantId = `wk-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({
      tenant_id: tenantId,
      name: "worker",
      capabilities: ["worker"],
    });
    workerSecret = key.secret;
    worker = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": workerSecret,
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("reads the handler catalog", async () => {
    const res = await fetch(`http://localhost:${server!.port}/handlers`, {
      headers: { "X-API-Key": workerSecret },
    });
    assert.equal(res.status, 200);
    const body = (await res.json()) as { builtin: string[]; external: string[] };
    assert.ok(body.builtin.includes("noop"), "builtin handlers listed");
    assert.ok(Array.isArray(body.external), "external handler list present");
  });

  it("polls an empty queue without error", async () => {
    const tasks = await worker.pollWorkerTasks("no_such_handler", "w-empty");
    assert.deepEqual(tasks, []);
  });

  it("reads worker inventory endpoints", async () => {
    const stats = await worker.workerTaskStats();
    // WorkerTaskStats shape: by_state / by_handler / active_workers.
    assert.ok(
      stats !== null && typeof stats === "object" && !Array.isArray(stats),
      "stats payload is an object",
    );
    assert.ok(
      stats.by_state !== null && typeof stats.by_state === "object",
      "stats.by_state is a state → count map",
    );
    assert.ok(Array.isArray(stats.active_workers), "stats.active_workers is a list");
    const tasks = await worker.listWorkerTasks({ tenant_id: tenantId });
    assert.deepEqual(tasks, [], "fresh tenant has no worker tasks");
    const res = await fetch(`http://localhost:${server!.port}/workers`, {
      headers: { "X-API-Key": workerSecret },
    });
    assert.equal(res.status, 200);
  });

  it("drives a full poll → complete flow that finishes the instance", async () => {
    const handler = `ext_wk_${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "wk-flow",
      [step("call", handler, { input: 41 })],
      { tenantId },
    );
    await root.createSequence(seq);
    const { id } = await root.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await root.waitForState(id, "waiting", { timeoutMs: 10_000 });

    // The worker key — not the root key — claims the task.
    let tasks = await worker.pollWorkerTasks(handler, "worker-1");
    const deadline = Date.now() + 5_000;
    while (tasks.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
      tasks = await worker.pollWorkerTasks(handler, "worker-1");
    }
    assert.equal(tasks.length, 1, "worker key claims exactly one task");

    await worker.completeWorkerTask(tasks[0]!.id, "worker-1", {
      answer: 42,
    });

    const done = await root.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(done.state, "completed");
    const outputs = await root.getOutputs(id);
    const out = outputs.find((o) => o.block_id === "call");
    assert.ok(out, "worker-produced step output persisted");
    assert.deepEqual((out as any).output, { answer: 42 });
  });

  it("passes capability checks on task completion for a bogus task (not 403)", async () => {
    let err: ApiError | undefined;
    try {
      await worker.completeWorkerTask(uuid(), "worker-1", {});
    } catch (e) {
      err = e as ApiError;
    }
    assert.ok(err, "bogus task id must error");
    assert.notEqual(
      err.status,
      403,
      "capability gate must let worker-family calls through",
    );
  });

  it("denies sequence writes with 403", async () => {
    await assert.rejects(
      worker.createSequence(
        testSequence("wk-deny", [step("s", "noop")], { tenantId }),
      ),
      (e: unknown) => e instanceof ApiError && e.status === 403,
    );
  });

  it("denies sequence and instance reads with 403", async () => {
    await assert.rejects(worker.listSequences({ tenant_id: tenantId }), {
      status: 403,
    } as object);
    await assert.rejects(worker.listInstances({ tenant_id: tenantId }), {
      status: 403,
    } as object);
  });

  it("denies instance creation and state mutation with 403", async () => {
    await assert.rejects(
      worker.createInstance({
        sequence_id: uuid(),
        tenant_id: tenantId,
        namespace: "default",
      }),
      { status: 403 } as object,
    );
    await assert.rejects(worker.updateState(uuid(), "cancelled"), {
      status: 403,
    } as object);
  });

  it("denies key management and approvals with 403", async () => {
    await assert.rejects(worker.revokeApiKey("ak_whatever"), {
      status: 403,
    } as object);
    await assert.rejects(worker.listApprovals({ tenant_id: tenantId }), {
      status: 403,
    } as object);
  });
});
