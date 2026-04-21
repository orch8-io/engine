/**
 * Worker Task Edge Cases — bad IDs, empty queues, and heartbeat on unknown tasks.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Worker Task Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("completing a non-existent worker task returns 404", async () => {
    await assert.rejects(
      () => client.completeWorkerTask(uuid(), "worker-1", { ok: true }),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });

  it("failing a non-existent worker task returns 404", async () => {
    await assert.rejects(
      () => client.failWorkerTask(uuid(), "worker-1", "nope", false),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });

  it("heartbeating a non-existent worker task returns 404", async () => {
    await assert.rejects(
      () => client.heartbeatWorkerTask(uuid(), "worker-1"),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });

  it("polling an empty queue returns empty array", async () => {
    const tasks = await client.pollWorkerTasks(
      `nonexistent_handler_${uuid().slice(0, 8)}`,
      `worker-${uuid().slice(0, 8)}`,
    );
    assert.ok(Array.isArray(tasks));
    assert.equal(tasks.length, 0);
  });

  it("listing worker tasks with no matching state returns empty array", async () => {
    const tasks = await client.listWorkerTasks({
      handler_name: `nonexistent_handler_${uuid().slice(0, 8)}`,
      state: "pending",
    });
    assert.ok(Array.isArray(tasks));
    assert.equal(tasks.length, 0);
  });
});
