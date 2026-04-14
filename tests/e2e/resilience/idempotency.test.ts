import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Idempotency", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should deduplicate instance creation by idempotency key", async () => {
    const seq = testSequence("idem-test", [step("s1", "noop")]);
    await client.createSequence(seq);

    const idempotencyKey = `idem-${uuid()}`;

    // First creation.
    const first = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      idempotency_key: idempotencyKey,
    });
    assert.ok(first.id);

    // Wait for it to complete to make the duplicate check clear.
    await client.waitForState(first.id, ["completed"], { timeoutMs: 10_000 });

    // Second creation with same key should return existing id.
    const second = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      idempotency_key: idempotencyKey,
    });
    assert.equal(second.id, first.id, "should return same instance id");
    assert.equal((second as any).deduplicated, true, "should be marked as deduplicated");
  });

  it("should allow different idempotency keys", async () => {
    const seq = testSequence("idem-diff", [step("s1", "noop")]);
    await client.createSequence(seq);

    const first = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      idempotency_key: `idem-${uuid()}`,
    });

    const second = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
      idempotency_key: `idem-${uuid()}`,
    });

    assert.notEqual(first.id, second.id, "different keys should create different instances");
  });

  it("should allow instances without idempotency key", async () => {
    const seq = testSequence("no-idem", [step("s1", "noop")]);
    await client.createSequence(seq);

    const first = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    const second = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });

    assert.notEqual(first.id, second.id, "no key means no deduplication");
  });
});
