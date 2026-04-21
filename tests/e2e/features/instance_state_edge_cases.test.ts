/**
 * Instance State Edge Cases — invalid transitions, missing instances,
 * and retry semantics.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Instance State Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("PATCH /instances/{id}/state returns 404 for non-existent instance", async () => {
    await assert.rejects(
      () => client.updateState(uuid(), "paused"),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });

  it("GET /instances/{id}/outputs returns 404 for non-existent instance", async () => {
    await assert.rejects(
      () => client.getOutputs(uuid()),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });

  it("retry on non-failed instance returns 400", async () => {
    const seq = testSequence("retry-nonfail", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed");

    await assert.rejects(
      () => client.retryInstance(id),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 400);
        return true;
      },
    );
  });

  it("retry on non-existent instance returns 404", async () => {
    await assert.rejects(
      () => client.retryInstance(uuid()),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });

  it("updateState to invalid value on terminal instance is rejected", async () => {
    const seq = testSequence("state-term", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed");

    await assert.rejects(
      () => client.updateState(id, "running"),
      (err: unknown) => {
        assert.ok(
          (err as ApiError).status >= 400 && (err as ApiError).status < 500,
        );
        return true;
      },
    );
  });
});
