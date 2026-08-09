import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { ApiError, Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { EventIngestRequest } from "../types.ts";

const client = new Orch8Client();

function outboxEvent(
  tenantId: string,
  producerEventId: string,
  correlationKey: string,
): EventIngestRequest {
  return {
    tenant_id: tenantId,
    event_name: "order.created",
    producer_event_id: producerEventId,
    correlation_key: correlationKey,
    payload: { correlationKey },
  };
}

describe("Transactional-outbox intake", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("accepts a batch and reports an unchanged replay as duplicate", async () => {
    const tenantId = `outbox-${uuid().slice(0, 8)}`;
    const eventA = outboxEvent(tenantId, `row-a-${uuid()}`, "order-a");
    const eventB = outboxEvent(tenantId, `row-b-${uuid()}`, "order-b");

    const accepted = await client.ingestEventBatch([eventA, eventB]);
    assert.deepEqual(accepted.map((outcome) => outcome.duplicate), [false, false]);

    const replayed = await client.ingestEventBatch([eventA, eventB]);
    assert.deepEqual(replayed.map((outcome) => outcome.duplicate), [true, true]);

    const stored = await client.listEvents({ tenant_id: tenantId });
    assert.equal(stored.length, 2);
  });

  it("validates every item before writing the first event", async () => {
    const tenantId = `outbox-validate-${uuid().slice(0, 8)}`;
    const valid = outboxEvent(tenantId, `not-written-${uuid()}`, "order-valid");
    const invalid = { ...outboxEvent(tenantId, `invalid-${uuid()}`, "order-invalid"), event_name: " " };

    await assert.rejects(
      () => client.ingestEventBatch([valid, invalid]),
      (error: unknown) => error instanceof ApiError && error.status === 400,
    );

    const stored = await client.listEvents({ tenant_id: tenantId });
    assert.equal(stored.length, 0);
  });

  it("rejects empty and over-limit batches", async () => {
    const tenantId = `outbox-bound-${uuid().slice(0, 8)}`;
    const oversized = Array.from({ length: 101 }, (_, index) =>
      outboxEvent(tenantId, `row-${index}-${uuid()}`, `order-${index}`),
    );

    await assert.rejects(
      () => client.ingestEventBatch([]),
      (error: unknown) => error instanceof ApiError && error.status === 400,
    );
    await assert.rejects(
      () => client.ingestEventBatch(oversized),
      (error: unknown) => error instanceof ApiError && error.status === 400,
    );
  });

  it("prevents a tenant-scoped relay from ingesting another tenant's row", async () => {
    const tenantId = `outbox-owner-${uuid().slice(0, 8)}`;
    const scoped = new Orch8Client(undefined, { "X-Tenant-Id": tenantId });

    await assert.rejects(
      () => scoped.ingestEventBatch([
        outboxEvent(`other-${uuid().slice(0, 8)}`, `foreign-${uuid()}`, "foreign-order"),
      ]),
      (error: unknown) => error instanceof ApiError && error.status === 403,
    );
  });
});
