/**
 * Cron 404 Edge Cases — update and delete on non-existent cron schedules.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Cron 404 Edge Cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("PATCH /cron/{id} returns 404 for non-existent cron", async () => {
    await assert.rejects(
      () =>
        client.updateCron(uuid(), {
          cron_expr: "0 0 * * * * *",
          enabled: false,
        }),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });

  it("DELETE /cron/{id} returns 404 for non-existent cron", async () => {
    await assert.rejects(
      () => client.deleteCron(uuid()),
      (err: unknown) => {
        assert.equal((err as ApiError).status, 404);
        return true;
      },
    );
  });
});
