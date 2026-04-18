/**
 * Verifies sequence delete is rejected or rolled back cleanly when a hot migration is in flight.
 *
 * TODO: Implement — plan below.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Delete Sequence with In-Flight Hot Migration", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - Arrange: deploy sequence v1 with running instances, then begin hot migration v1 -> v2.
  //   - Act: before migration completes, issue DELETE on the sequence.
  //   - Assert: either a 409 Conflict response with a clear error message is returned,
  //     or the delete is atomically rolled back leaving v1 intact and running instances unaffected.
  //
  // Skipped: DELETE /sequences/{id} is not exposed by the server.
  // `orch8-api/src/sequences.rs::routes()` only registers POST /sequences,
  // GET /sequences/{id}, POST /sequences/{id}/deprecate, GET /sequences/by-name,
  // GET /sequences/versions, and POST /sequences/migrate-instance. The
  // supported mutation path for retiring a sequence is `deprecateSequence`,
  // which is exercised in `sequence-versioning.test.ts`. If a DELETE route
  // is added later, the expected behavior this test should verify is:
  //   1. Create v1 + v2 of the same (tenant, namespace, name).
  //   2. Start a long-running instance on v1 (e.g. sleep 3000ms).
  //   3. Call migrateInstance(instance, v2) to begin a hot migration.
  //   4. Immediately fetch(DELETE `/sequences/{v1.id}`) via raw fetch:
  //        `await fetch(`${client.baseUrl}/sequences/${v1.id}`, {method:"DELETE"})`
  //   5. Assert res.status is 4xx (409 preferred) OR that v1 remains
  //      readable via getSequence(v1.id) and the running instance still
  //      reaches a non-failed terminal state.
  it.skip("should reject delete or rollback cleanly when migration is in progress", async () => {
    // Reference skeleton for the future implementation:
    const tenantId = `seq-del-${uuid().slice(0, 8)}`;
    const namespace = "default";
    const name = `del-${uuid().slice(0, 8)}`;

    const v1 = {
      id: uuid(),
      tenant_id: tenantId,
      namespace,
      name,
      version: 1,
      blocks: [step("slow", "sleep", { duration_ms: 3000 })],
      created_at: new Date().toISOString(),
    };
    const v2 = {
      id: uuid(),
      tenant_id: tenantId,
      namespace,
      name,
      version: 2,
      blocks: [step("fast", "noop")],
      created_at: new Date().toISOString(),
    };
    await client.createSequence(v1);
    await client.createSequence(v2);

    const { id: instanceId } = await client.createInstance({
      sequence_id: v1.id,
      tenant_id: tenantId,
      namespace,
    });

    await new Promise((r) => setTimeout(r, 200));
    await client.migrateInstance(instanceId, v2.id);

    const res = await fetch(`${client.baseUrl}/sequences/${v1.id}`, {
      method: "DELETE",
    });
    assert.ok(
      res.status >= 400 && res.status < 500,
      `expected 4xx conflict on delete during migration, got ${res.status}`
    );

    // v1 should still be readable (rollback) OR the delete cleanly rejected.
    const still = await client.getSequence(v1.id);
    assert.equal(still.id, v1.id);
  });
});
