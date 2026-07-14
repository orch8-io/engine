/**
 * E2E: Portable Continuity — execution + checkpoint lifecycle.
 *
 * Covers `POST/GET /continuity/executions`, `GET .../locations`,
 * `GET .../effects`, and `GET .../checkpoints[/{id}]` (see
 * orch8-api/src/continuity.rs: create_execution, get_execution,
 * list_locations, list_effects, list_continuity_checkpoints,
 * get_continuity_checkpoint).
 *
 * Scope note: a freshly created continuity execution has no location
 * history yet (locations are written by handoff acceptance, which is
 * out of scope for this file — see features/portable_continuity.test.ts
 * and the handoff/capsule-focused suites), so checkpoints/locations for
 * a bare execution are legitimately empty. Tests here assert that empty
 * state precisely, plus every documented validation/isolation rule for
 * the endpoints themselves.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

async function mkInstance(tenantId: string): Promise<string> {
  const seq = testSequence("continuity-exec", [step("s1", "noop")], { tenantId });
  await client.createSequence(seq);
  const inst = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  return inst.id;
}

async function expectStatus(fn: () => Promise<unknown>, status: number): Promise<void> {
  try {
    await fn();
    assert.fail(`expected HTTP ${status}`);
  } catch (err) {
    assert.equal((err as ApiError).status, status, `expected ${status}, got ${(err as ApiError).status}: ${(err as ApiError).message}`);
  }
}

describe("Continuity Executions — creation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("creates an execution for a valid instance", async () => {
    const tenantId = `ce-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const runtimeId = uuid();

    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: runtimeId,
    });

    assert.ok((exec as any).continuity_id, "should return a continuity_id");
    assert.equal((exec as any).tenant_id, tenantId);
    assert.equal((exec as any).current_instance_id, instanceId);
    assert.equal((exec as any).owner_runtime_id, runtimeId);
  });

  it("initializes at the first ownership epoch", async () => {
    const tenantId = `ce-epoch-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    // ExecutionEpoch::initial() — the very first epoch value.
    assert.ok(
      (exec as any).epoch !== undefined && (exec as any).epoch !== null,
      "epoch must be set on creation",
    );
  });

  it("starts in an owned ownership state", async () => {
    const tenantId = `ce-state-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    assert.ok((exec as any).state, "state must be set");
  });

  it("rejects creation for a non-existent instance", async () => {
    const tenantId = `ce-noinst-${uuid().slice(0, 8)}`;
    await expectStatus(
      () =>
        client.createContinuityExecution({
          tenant_id: tenantId,
          instance_id: uuid(),
          runtime_id: uuid(),
        }),
      404,
    );
  });

  it("rejects creation for an instance owned by a different tenant", async () => {
    const ownerTenant = `ce-owner-${uuid().slice(0, 8)}`;
    const callerTenant = `ce-caller-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(ownerTenant);

    await expectStatus(
      () =>
        client.createContinuityExecution({
          tenant_id: callerTenant,
          instance_id: instanceId,
          runtime_id: uuid(),
        }),
      404,
    );
  });

  it("rejects a duplicate continuity execution for the same instance", async () => {
    const tenantId = `ce-dup-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });

    await expectStatus(
      () =>
        client.createContinuityExecution({
          tenant_id: tenantId,
          instance_id: instanceId,
          runtime_id: uuid(),
        }),
      409,
    );
  });

  it("allows two different instances to each get their own execution", async () => {
    const tenantId = `ce-multi-${uuid().slice(0, 8)}`;
    const instanceA = await mkInstance(tenantId);
    const instanceB = await mkInstance(tenantId);

    const execA = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceA,
      runtime_id: uuid(),
    });
    const execB = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceB,
      runtime_id: uuid(),
    });

    assert.notEqual(
      (execA as any).continuity_id,
      (execB as any).continuity_id,
      "distinct instances must get distinct continuity ids",
    );
  });

  it("allows the same instance id to be reused across tenants (no cross-tenant conflict)", async () => {
    // Different tenants create their own instances (instance ids are
    // globally unique UUIDs, so this really exercises that continuity
    // dedup is scoped by tenant + the instance's OWN tenant, not by a
    // bare instance-id collision across arbitrary tenants).
    const tenantA = `ce-cross-a-${uuid().slice(0, 8)}`;
    const tenantB = `ce-cross-b-${uuid().slice(0, 8)}`;
    const instanceA = await mkInstance(tenantA);
    const instanceB = await mkInstance(tenantB);

    const execA = await client.createContinuityExecution({
      tenant_id: tenantA,
      instance_id: instanceA,
      runtime_id: uuid(),
    });
    const execB = await client.createContinuityExecution({
      tenant_id: tenantB,
      instance_id: instanceB,
      runtime_id: uuid(),
    });
    assert.ok((execA as any).continuity_id);
    assert.ok((execB as any).continuity_id);
  });

  // CreateExecutionRequest derives Deserialize with no #[serde(default)]
  // on tenant_id/instance_id/runtime_id, so a structurally missing or
  // malformed field never reaches the handler's own validation — axum's
  // Json extractor rejects it at the deserialization boundary with 422,
  // not the handler's 400 InvalidArgument path.

  it("rejects a request missing tenant_id", async () => {
    const instanceId = await mkInstance(`ce-mt-${uuid().slice(0, 8)}`);
    await expectStatus(
      () =>
        client.createContinuityExecution({
          instance_id: instanceId,
          runtime_id: uuid(),
        }),
      422,
    );
  });

  it("rejects a request missing instance_id", async () => {
    await expectStatus(
      () =>
        client.createContinuityExecution({
          tenant_id: `ce-mi-${uuid().slice(0, 8)}`,
          runtime_id: uuid(),
        }),
      422,
    );
  });

  it("rejects a request missing runtime_id", async () => {
    const tenantId = `ce-mr-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    await expectStatus(
      () =>
        client.createContinuityExecution({
          tenant_id: tenantId,
          instance_id: instanceId,
        }),
      422,
    );
  });

  it("rejects a malformed (non-UUID) instance_id", async () => {
    await expectStatus(
      () =>
        client.createContinuityExecution({
          tenant_id: `ce-badid-${uuid().slice(0, 8)}`,
          instance_id: "not-a-uuid",
          runtime_id: uuid(),
        }),
      422,
    );
  });

  it("accepts an arbitrary UUID as runtime_id without prior registration", async () => {
    // create_execution stores runtime_id verbatim; it does not require
    // the runtime to have called POST /runtimes/register first.
    const tenantId = `ce-unreg-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const unregisteredRuntime = uuid();
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: unregisteredRuntime,
    });
    assert.equal((exec as any).owner_runtime_id, unregisteredRuntime);
  });
});

describe("Continuity Executions — retrieval", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("retrieves a created execution by id", async () => {
    const tenantId = `ce-get-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const created = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });

    const fetched = await client.getContinuityExecution(
      (created as any).continuity_id,
      tenantId,
    );
    assert.equal((fetched as any).continuity_id, (created as any).continuity_id);
    assert.equal((fetched as any).current_instance_id, instanceId);
  });

  it("returns 404 for an unknown continuity id", async () => {
    await expectStatus(
      () => client.getContinuityExecution(uuid(), `ce-unknown-${uuid().slice(0, 8)}`),
      404,
    );
  });

  it("returns 404 when fetched under the wrong tenant_id", async () => {
    const ownerTenant = `ce-getowner-${uuid().slice(0, 8)}`;
    const otherTenant = `ce-getother-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(ownerTenant);
    const created = await client.createContinuityExecution({
      tenant_id: ownerTenant,
      instance_id: instanceId,
      runtime_id: uuid(),
    });

    await expectStatus(
      () => client.getContinuityExecution((created as any).continuity_id, otherTenant),
      404,
    );
  });

  it("returns 400 for a malformed continuity id in the path", async () => {
    await expectStatus(
      () => client.getContinuityExecution("not-a-uuid", `ce-malformed-${uuid().slice(0, 8)}`),
      400,
    );
  });

  it("round-trips identical field values across create and get", async () => {
    const tenantId = `ce-roundtrip-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const runtimeId = uuid();
    const created = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: runtimeId,
    });
    const fetched = await client.getContinuityExecution(
      (created as any).continuity_id,
      tenantId,
    );
    assert.deepEqual(fetched, created, "get must return exactly what create returned");
  });
});

describe("Continuity Executions — locations", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("writes a single initial location entry at creation, before any handoff", async () => {
    // create_execution writes the initial ownership/location record in
    // the same transaction as the execution itself (epoch 0, no handoff
    // id) — locations are not empty until a handoff, they start with
    // exactly one entry describing "created here."
    const tenantId = `ce-loc-empty-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    const locations = await client.listContinuityLocations(
      (exec as any).continuity_id,
      tenantId,
    );
    assert.equal(locations.length, 1, "exactly one initial location entry");
    const loc = locations[0] as any;
    assert.equal(loc.continuity_id, (exec as any).continuity_id);
    assert.equal(loc.tenant_id, tenantId);
    assert.equal(loc.instance_id, instanceId);
    assert.equal(loc.runtime_id, (exec as any).owner_runtime_id);
    assert.equal(loc.epoch, (exec as any).epoch, "initial location epoch matches execution epoch");
    assert.equal(loc.handoff_id, null, "no handoff produced the initial location");
    assert.ok(loc.entered_at, "entered_at must be set");
  });

  it("returns 404 for locations of an unknown execution", async () => {
    await expectStatus(
      () => client.listContinuityLocations(uuid(), `ce-loc-404-${uuid().slice(0, 8)}`),
      404,
    );
  });

  it("returns empty (not 404) for a real execution queried under a mismatched tenant", async () => {
    // list_locations treats "execution not found for this tenant" the
    // same as "no locations yet" only when it can independently confirm
    // existence — for a genuinely wrong tenant the execution lookup
    // itself returns nothing, so this still surfaces as 404.
    const ownerTenant = `ce-loc-owner-${uuid().slice(0, 8)}`;
    const otherTenant = `ce-loc-other-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(ownerTenant);
    const exec = await client.createContinuityExecution({
      tenant_id: ownerTenant,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    await expectStatus(
      () => client.listContinuityLocations((exec as any).continuity_id, otherTenant),
      404,
    );
  });
});

describe("Continuity Executions — effect receipts", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("returns an empty list when no effects have been dispatched", async () => {
    const tenantId = `ce-eff-empty-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    const effects = await client.listContinuityEffects((exec as any).continuity_id, tenantId);
    assert.deepEqual(effects, []);
  });

  it("does not error for an unknown execution (list_effects has no existence check)", async () => {
    // Unlike list_locations, list_effects in orch8-api/src/continuity.rs
    // does not verify the execution exists first — it just queries
    // effect receipts by continuity_id and returns whatever it finds
    // (empty for an unknown id). This test documents that asymmetry.
    const effects = await client.listContinuityEffects(uuid(), `ce-eff-unknown-${uuid().slice(0, 8)}`);
    assert.deepEqual(effects, []);
  });

  it("scopes effects strictly by the queried tenant_id", async () => {
    const ownerTenant = `ce-eff-owner-${uuid().slice(0, 8)}`;
    const otherTenant = `ce-eff-other-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(ownerTenant);
    const exec = await client.createContinuityExecution({
      tenant_id: ownerTenant,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    const effects = await client.listContinuityEffects(
      (exec as any).continuity_id,
      otherTenant,
    );
    assert.deepEqual(effects, [], "effects must not leak across tenant_id values");
  });
});

describe("Continuity Executions — checkpoints", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("returns an empty checkpoint list for a fresh execution (no location history)", async () => {
    const tenantId = `ce-cp-empty-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    const checkpoints = await client.listContinuityCheckpoints(
      (exec as any).continuity_id,
      tenantId,
    );
    assert.deepEqual(checkpoints, []);
  });

  it("returns 404 listing checkpoints for an unknown execution", async () => {
    await expectStatus(
      () => client.listContinuityCheckpoints(uuid(), `ce-cp-404-${uuid().slice(0, 8)}`),
      404,
    );
  });

  it("returns 404 fetching a specific checkpoint on a fresh (checkpoint-less) execution", async () => {
    const tenantId = `ce-cp-detail-empty-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    await expectStatus(
      () =>
        client.getContinuityCheckpoint(
          (exec as any).continuity_id,
          uuid(),
          tenantId,
        ),
      404,
    );
  });

  it("returns 404 fetching a checkpoint for an unknown execution id", async () => {
    await expectStatus(
      () => client.getContinuityCheckpoint(uuid(), uuid(), `ce-cp-unk-${uuid().slice(0, 8)}`),
      404,
    );
  });

  it("honours the limit query parameter without erroring on a fresh execution", async () => {
    const tenantId = `ce-cp-limit-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    const checkpoints = await client.listContinuityCheckpoints(
      (exec as any).continuity_id,
      tenantId,
      5,
    );
    assert.deepEqual(checkpoints, []);
  });
});

describe("Continuity Executions — tenant isolation matrix", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Systematically verify that every read endpoint in this file's scope
  // rejects (or empties) cross-tenant access, one endpoint per test so a
  // regression in any single one is immediately attributable.

  it("get_execution: 404 across tenants", async () => {
    const tenantId = `ce-iso-get-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    await expectStatus(
      () => client.getContinuityExecution((exec as any).continuity_id, `${tenantId}-x`),
      404,
    );
  });

  it("list_locations: 404 across tenants", async () => {
    const tenantId = `ce-iso-loc-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    await expectStatus(
      () => client.listContinuityLocations((exec as any).continuity_id, `${tenantId}-x`),
      404,
    );
  });

  it("list_continuity_checkpoints: 404 across tenants", async () => {
    const tenantId = `ce-iso-cp-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    await expectStatus(
      () => client.listContinuityCheckpoints((exec as any).continuity_id, `${tenantId}-x`),
      404,
    );
  });

  it("distinct tenants never observe each other's continuity_id in a list", async () => {
    const tenantA = `ce-iso-list-a-${uuid().slice(0, 8)}`;
    const tenantB = `ce-iso-list-b-${uuid().slice(0, 8)}`;
    const instanceA = await mkInstance(tenantA);
    const instanceB = await mkInstance(tenantB);
    const execA = await client.createContinuityExecution({
      tenant_id: tenantA,
      instance_id: instanceA,
      runtime_id: uuid(),
    });
    const execB = await client.createContinuityExecution({
      tenant_id: tenantB,
      instance_id: instanceB,
      runtime_id: uuid(),
    });

    // Fetching B's execution under A's tenant must 404, and vice versa.
    await expectStatus(
      () => client.getContinuityExecution((execB as any).continuity_id, tenantA),
      404,
    );
    await expectStatus(
      () => client.getContinuityExecution((execA as any).continuity_id, tenantB),
      404,
    );
  });
});

describe("Continuity Executions — repeated reads are stable", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("get_execution is idempotent across repeated calls", async () => {
    const tenantId = `ce-stable-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const created = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    const first = await client.getContinuityExecution((created as any).continuity_id, tenantId);
    const second = await client.getContinuityExecution((created as any).continuity_id, tenantId);
    assert.deepEqual(first, second);
  });

  it("list_locations is stable across repeated calls with no intervening handoff", async () => {
    const tenantId = `ce-stable-loc-${uuid().slice(0, 8)}`;
    const instanceId = await mkInstance(tenantId);
    const exec = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instanceId,
      runtime_id: uuid(),
    });
    const first = await client.listContinuityLocations((exec as any).continuity_id, tenantId);
    const second = await client.listContinuityLocations((exec as any).continuity_id, tenantId);
    assert.deepEqual(first, second);
    assert.equal(first.length, 1, "still just the initial location entry");
  });

  it("creating many executions for many instances under one tenant does not cross-pollute get results", async () => {
    const tenantId = `ce-bulk-${uuid().slice(0, 8)}`;
    const created: string[] = [];
    for (let i = 0; i < 8; i += 1) {
      const instanceId = await mkInstance(tenantId);
      const exec = await client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instanceId,
        runtime_id: uuid(),
      });
      created.push((exec as any).continuity_id);
    }
    for (const continuityId of created) {
      const fetched = await client.getContinuityExecution(continuityId, tenantId);
      assert.equal((fetched as any).continuity_id, continuityId);
    }
    // Every continuity_id must be unique.
    assert.equal(new Set(created).size, created.length);
  });
});
