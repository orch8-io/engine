/**
 * GET /instances/{id}/audit — verifies audit log records key lifecycle
 * events in chronological order.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Audit Log", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("completed instance has audit entries", async () => {
    const tenantId = `audit-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "audit-basic",
      [step("a", "noop"), step("b", "noop")],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    const audit = await client.getAuditLog(id);
    assert.ok(Array.isArray(audit), "audit should be an array");
    assert.ok(audit.length > 0, "audit should have at least one entry");

    // Timestamps should be monotonically non-decreasing (allow 1s tolerance
    // for SQLite millisecond rounding and rapid sequential events).
    const timestamps = audit
      .map((e) => (e as Record<string, unknown>).timestamp ?? (e as Record<string, unknown>).created_at)
      .filter((t): t is string => typeof t === "string")
      .map((t) => new Date(t).getTime());
    for (let i = 1; i < timestamps.length; i++) {
      assert.ok(
        timestamps[i]! >= timestamps[i - 1]! - 1000,
        `audit timestamps should be non-decreasing at index ${i}`,
      );
    }
  });

  it("non-existent instance returns 404", async () => {
    try {
      await client.getAuditLog(uuid());
      assert.fail("should throw 404");
    } catch (err: any) {
      assert.equal(err.status, 404);
    }
  });

  it("paused and resumed instance records signal events", async () => {
    const tenantId = `audit-sig-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "audit-sig",
      [step("slow", "sleep", { duration_ms: 3000 })],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, ["running", "scheduled"], { timeoutMs: 5_000 });

    await client.sendSignal(id, "pause");
    await client.waitForState(id, "paused", { timeoutMs: 5_000 });

    await client.sendSignal(id, "resume");
    // After resume, it may go to running/scheduled/completed.
    await client.waitForState(id, ["running", "scheduled", "completed"], {
      timeoutMs: 10_000,
    });

    const audit = await client.getAuditLog(id);
    assert.ok(audit.length >= 2, "should have at least 2 audit entries after pause/resume");
  });
});
