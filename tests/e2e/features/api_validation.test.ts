/**
 * API validation — verifies that PATCH/DELETE endpoints handle invalid
 * input gracefully (proper 4xx errors, not 500s).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("API Validation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("GET /instances/{id} with non-existent ID returns 404", async () => {
    try {
      await client.getInstance(uuid());
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("POST /instances with non-existent sequence_id returns error", async () => {
    try {
      await client.createInstance({
        sequence_id: uuid(),
        tenant_id: "test",
        namespace: "default",
      });
      assert.fail("should throw");
    } catch (err: any) {
      assert.ok(err.status === 400 || err.status === 404, `expected 4xx, got ${err.status}`);
    }
  });

  it("GET /sequences/{id} with non-existent ID returns 404", async () => {
    try {
      await client.getSequence(uuid());
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("DELETE /sequences/{id} with non-existent ID returns 404", async () => {
    try {
      await client.deleteSequence(uuid());
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("GET /plugins/{name} with non-existent name returns 404", async () => {
    try {
      await client.getPlugin(`nonexistent-${uuid().slice(0, 8)}`);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("PATCH /plugins/{name} with non-existent name returns 404", async () => {
    try {
      await client.updatePlugin(`nonexistent-${uuid().slice(0, 8)}`, { enabled: false });
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("DELETE /plugins/{name} with non-existent name returns 404", async () => {
    try {
      await client.deletePlugin(`nonexistent-${uuid().slice(0, 8)}`);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("GET /credentials/{id} with non-existent ID returns 404", async () => {
    try {
      await client.getCredential(uuid());
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("GET /triggers/{slug} with non-existent slug returns 404", async () => {
    try {
      await client.getTrigger(`nonexistent-${uuid().slice(0, 8)}`);
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("GET /sessions/{id} with non-existent ID returns 404", async () => {
    try {
      await client.getSession(uuid());
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("POST /instances/{id}/signals with non-existent ID returns 404", async () => {
    try {
      await client.sendSignal(uuid(), "cancel");
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("deprecating a non-existent sequence returns 404", async () => {
    try {
      await client.deprecateSequence(uuid());
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });
});
