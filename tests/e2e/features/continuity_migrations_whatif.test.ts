/**
 * E2E: Portable Continuity — live migration compiler/apply/rollback,
 * comparative what-if simulation, and checkpoint-diff time travel.
 *
 * Covers `orch8-api/src/continuity.rs`:
 *   POST /continuity/migrations/plan
 *   GET  /continuity/migrations/{id}
 *   POST /continuity/migrations/{id}/apply
 *   POST /continuity/migrations/{id}/rollback
 *   GET  /continuity/executions/{id}/what-if
 *   POST /continuity/executions/{id}/what-if
 *   POST /continuity/executions/{id}/test-fixture
 *   GET  /continuity/executions/{id}/checkpoints/{checkpoint_id}  (diff only —
 *     basic checkpoint CRUD/listing is covered by continuity_executions.test.ts)
 */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { ApiError, Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block, SequenceDef } from "../types.ts";

const client = new Orch8Client();

/**
 * Drive an instance to `waiting` via a `human_review` gate, register a
 * continuity execution over it, and durably checkpoint it — the
 * precondition every migration/what-if/fixture endpoint requires.
 */
async function pausedContinuityFixture(
  tenantId: string,
  namePrefix: string,
  extraBlocks: Block[] = [],
  contextData: Record<string, unknown> = { order_id: "abc-123" },
) {
  const sequence = testSequence(
    namePrefix,
    [
      step("gate", "human_review", {}, {
        wait_for_input: {
          prompt: "continue?",
          choices: [{ label: "Continue", value: "continue" }],
        },
      }),
      ...extraBlocks,
    ],
    { tenantId },
  );
  await client.createSequence(sequence);
  const instance = await client.createInstance({
    sequence_id: sequence.id,
    tenant_id: tenantId,
    namespace: "default",
    context: { data: contextData },
  });
  await client.waitForState(instance.id, "waiting");
  await client.saveCheckpoint(instance.id, {
    safe_boundary: "gate",
    context_snapshot: contextData,
  });
  const runtimeId = uuid();
  const execution = await client.createContinuityExecution({
    tenant_id: tenantId,
    instance_id: instance.id,
    runtime_id: runtimeId,
  });
  const checkpoints = await client.listContinuityCheckpoints(
    execution.continuity_id,
    tenantId,
  );
  assert.ok(checkpoints.length >= 1, "fixture must produce at least one checkpoint");
  const checkpointId = checkpoints[checkpoints.length - 1].checkpoint_id;
  return { sequence, instance, execution, checkpointId };
}

/** A same-shape target sequence (version 2) — trivially replay-compatible. */
function sameShapeTarget(source: SequenceDef): SequenceDef {
  return { ...source, id: uuid(), version: 2 };
}

/** A target sequence with a newly-required input field — INCOMPATIBLE. */
function narrowedInputSchemaTarget(source: SequenceDef): SequenceDef {
  return {
    ...source,
    id: uuid(),
    version: 2,
    input_schema: {
      type: "object",
      required: ["must_have_field"],
      properties: { must_have_field: { type: "string" } },
    },
  };
}

/** A target sequence that inserts an unmocked step BEFORE the wait boundary
 * so historical replay actually reaches it and fails (PIN disposition).
 * (Blocks after the boundary are never replayed since the case runner has
 * no signals to resume the parked wait_for_input step.) */
function divergentTarget(source: SequenceDef): SequenceDef {
  return {
    ...source,
    id: uuid(),
    version: 2,
    blocks: [step("extra_unmocked", "unregistered_handler_xyz"), ...source.blocks],
  };
}

describe("Continuity Migrations & What-If", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "17".repeat(32),
        ORCH8_ARTIFACT_BACKEND: "local",
        ORCH8_ARTIFACT_PATH: `/tmp/continuity-migwhatif-artifacts-${uuid()}`,
        ORCH8_LOG_LEVEL: "error",
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  // ==================================================================
  // Migration planning: preconditions
  // ==================================================================

  it("plan rejects a Running instance (no waiting/paused boundary)", async () => {
    const tenantId = `mig-run-${uuid().slice(0, 8)}`;
    const sequence = testSequence("mig-running", [step("s1", "noop")], { tenantId });
    await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: sequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(instance.id, "completed");
    const runtimeId = uuid();
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: runtimeId,
    });
    const target = sameShapeTarget(sequence);
    await client.createSequence(target);
    await assert.rejects(
      () =>
        client.planContinuityMigration({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          to_sequence_id: target.id,
          to_version: target.version,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("plan rejects an instance with no durable checkpoint", async () => {
    const tenantId = `mig-nockpt-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "mig-no-checkpoint",
      [step("gate", "human_review", {}, { wait_for_input: { prompt: "x" } })],
      { tenantId },
    );
    await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: sequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(instance.id, "waiting");
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const target = sameShapeTarget(sequence);
    await client.createSequence(target);
    await assert.rejects(
      () =>
        client.planContinuityMigration({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          to_sequence_id: target.id,
          to_version: target.version,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("plan 404s on target sequence tenant mismatch", async () => {
    const tenantId = `mig-tenmis-${uuid().slice(0, 8)}`;
    const otherTenant = `mig-other-${uuid().slice(0, 8)}`;
    const { execution } = await pausedContinuityFixture(tenantId, "mig-tenant-mismatch");
    const foreignSeq = testSequence("mig-foreign", [step("s1", "noop")], {
      tenantId: otherTenant,
    });
    const createdForeign = await client.createSequence(foreignSeq);
    await assert.rejects(
      () =>
        client.planContinuityMigration({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          to_sequence_id: createdForeign.id,
          to_version: foreignSeq.version,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("plan 404s on target sequence version mismatch", async () => {
    const tenantId = `mig-vermis-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-ver-mismatch");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    await assert.rejects(
      () =>
        client.planContinuityMigration({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          to_sequence_id: createdTarget.id,
          to_version: 999,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("plan 404s on unknown continuity_id", async () => {
    const tenantId = `mig-unk-${uuid().slice(0, 8)}`;
    const sequence = testSequence("mig-unk-seq", [step("s1", "noop")], { tenantId });
    await client.createSequence(sequence);
    await assert.rejects(
      () =>
        client.planContinuityMigration({
          tenant_id: tenantId,
          continuity_id: uuid(),
          to_sequence_id: sequence.id,
          to_version: sequence.version,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  // ==================================================================
  // Migration planning: disposition classification
  // ==================================================================

  it("plan classifies a same-shape target with no transforms as automatic", async () => {
    const tenantId = `mig-auto-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-auto");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    assert.equal(plan.disposition, "automatic");
    assert.deepEqual(plan.transforms, []);
    assert.ok(
      plan.finding_codes.includes("HISTORICAL_REPLAY_PASS"),
      `expected replay pass, got: ${JSON.stringify(plan.finding_codes)}`,
    );
  });

  it("plan classifies a non-empty transform as approval_required when replay passes", async () => {
    const tenantId = `mig-appr-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-approval");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
      transforms: [{ version: 1, transform: "drop", from_path: "context_snapshot.order_id", to_path: "" }],
    });
    assert.equal(plan.disposition, "approval_required");
    assert.equal(plan.transforms.length, 1);
  });

  it("plan classifies input-schema narrowing as incompatible", async () => {
    const tenantId = `mig-incompat-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-incompatible");
    const target = narrowedInputSchemaTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    assert.equal(plan.disposition, "incompatible");
    assert.ok(
      plan.finding_codes.some((code: string) => code.startsWith("INCOMPATIBLE:")),
      `expected an INCOMPATIBLE: finding, got: ${JSON.stringify(plan.finding_codes)}`,
    );
  });

  it("plan classifies a divergent target (unmocked new step) as pin", async () => {
    const tenantId = `mig-pin-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-pin");
    const target = divergentTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    assert.equal(plan.disposition, "pin");
    assert.ok(
      plan.finding_codes.includes("HISTORICAL_REPLAY_FAIL"),
      `expected replay fail, got: ${JSON.stringify(plan.finding_codes)}`,
    );
  });

  it("plan clamps rollback_retention_seconds below the 60s floor", async () => {
    const tenantId = `mig-clamp-lo-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-clamp-lo");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
      rollback_retention_seconds: 1,
    });
    const record = await client.getContinuityMigration(plan.id, tenantId);
    const windowSecs =
      (new Date(record.rollback_expires_at).getTime() - Date.now()) / 1000;
    assert.ok(windowSecs >= 55 && windowSecs <= 65, `expected ~60s window, got ${windowSecs}s`);
  });

  it("plan clamps rollback_retention_seconds above the 30-day ceiling", async () => {
    const tenantId = `mig-clamp-hi-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-clamp-hi");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
      rollback_retention_seconds: 999_999_999,
    });
    const record = await client.getContinuityMigration(plan.id, tenantId);
    const windowDays =
      (new Date(record.rollback_expires_at).getTime() - Date.now()) / 86_400_000;
    assert.ok(windowDays <= 30.1, `expected <=30d window, got ${windowDays}d`);
  });

  it("plan reports CHECKPOINT_OUTPUT_REMOVED when a completed block vanishes from target", async () => {
    const tenantId = `mig-ckptout-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "mig-ckpt-removed",
      [
        step("produced", "transform", { keep: true }),
        step("gate", "human_review", {}, { wait_for_input: { prompt: "x" } }),
      ],
      { tenantId },
    );
    await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: sequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(instance.id, "waiting");
    await client.saveCheckpoint(instance.id, { safe_boundary: "gate" });
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const target: SequenceDef = {
      ...sequence,
      id: uuid(),
      version: 2,
      blocks: [step("gate", "human_review", {}, { wait_for_input: { prompt: "x" } })],
    };
    await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: target.id,
      to_version: target.version,
    });
    assert.ok(
      plan.finding_codes.some((code: string) => code.startsWith("CHECKPOINT_OUTPUT_REMOVED:")),
      `expected CHECKPOINT_OUTPUT_REMOVED finding, got: ${JSON.stringify(plan.finding_codes)}`,
    );
  });

  // ==================================================================
  // Get migration
  // ==================================================================

  it("get returns the saved plan verbatim", async () => {
    const tenantId = `mig-get-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-get");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    const fetched = await client.getContinuityMigration(plan.id, tenantId);
    assert.equal(fetched.plan.id, plan.id);
    assert.equal(fetched.state, "planned");
  });

  it("get 404s for wrong tenant", async () => {
    const tenantId = `mig-getwt-${uuid().slice(0, 8)}`;
    const wrongTenant = `mig-getwt-wrong-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-get-wrong-tenant");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await assert.rejects(
      () => client.getContinuityMigration(plan.id, wrongTenant),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("get 404s for an unknown plan id", async () => {
    await assert.rejects(
      () => client.getContinuityMigration(uuid(), "any-tenant"),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  // ==================================================================
  // Apply migration
  // ==================================================================

  it("apply succeeds for an automatic-disposition plan", async () => {
    const tenantId = `mig-apply-auto-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-apply-auto");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    assert.equal(plan.disposition, "automatic");
    const applied = await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    assert.equal(applied.state, "applied");
    assert.ok(applied.applied_epoch !== null && applied.applied_epoch !== undefined);
    assert.ok(applied.rollback_capsule, "applied record should retain a rollback capsule");
  });

  it("apply advances the continuity epoch by exactly one", async () => {
    const tenantId = `mig-apply-epoch-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-apply-epoch");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    const applied = await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    const after = await client.getContinuityExecution(execution.continuity_id, tenantId);
    assert.equal(after.epoch, execution.epoch + 1);
    assert.equal(applied.applied_epoch, after.epoch);
  });

  it("apply transitions the instance to paused and switches sequence_id", async () => {
    const tenantId = `mig-apply-inst-${uuid().slice(0, 8)}`;
    const { execution, sequence, instance } = await pausedContinuityFixture(
      tenantId,
      "mig-apply-inst",
    );
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    const afterInstance = await client.getInstance(instance.id);
    assert.equal(afterInstance.state, "paused");
    assert.equal((afterInstance as any).sequence_id, createdTarget.id);
  });

  it("apply appends a migration_applied provenance boundary", async () => {
    const tenantId = `mig-apply-prov-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-apply-prov");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
    assert.ok(
      provenance.some((entry: any) => entry.kind === "migration_applied"),
      `expected migration_applied provenance entry, got: ${JSON.stringify(provenance)}`,
    );
  });

  it("apply rejects a plan that is no longer Planned (double-apply)", async () => {
    const tenantId = `mig-apply-twice-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-apply-twice");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    await assert.rejects(
      () => client.applyContinuityMigration(plan.id, { tenant_id: tenantId }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("apply rejects an approval_required plan without approved=true", async () => {
    const tenantId = `mig-apply-noappr-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-apply-noappr");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
      transforms: [{ version: 1, transform: "drop", from_path: "context_snapshot.order_id", to_path: "" }],
    });
    assert.equal(plan.disposition, "approval_required");
    await assert.rejects(
      () => client.applyContinuityMigration(plan.id, { tenant_id: tenantId }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("apply succeeds for an approval_required plan when approved=true", async () => {
    const tenantId = `mig-apply-appr-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-apply-appr");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
      transforms: [{ version: 1, transform: "drop", from_path: "context_snapshot.order_id", to_path: "" }],
    });
    const applied = await client.applyContinuityMigration(plan.id, {
      tenant_id: tenantId,
      approved: true,
    });
    assert.equal(applied.state, "applied");
  });

  it("apply rejects a pin-disposition plan even with approved=true", async () => {
    const tenantId = `mig-apply-pin-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-apply-pin");
    const target = divergentTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    assert.equal(plan.disposition, "pin");
    await assert.rejects(
      () => client.applyContinuityMigration(plan.id, { tenant_id: tenantId, approved: true }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("apply rejects an incompatible-disposition plan", async () => {
    const tenantId = `mig-apply-incompat-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-apply-incompat");
    const target = narrowedInputSchemaTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    assert.equal(plan.disposition, "incompatible");
    await assert.rejects(
      () => client.applyContinuityMigration(plan.id, { tenant_id: tenantId, approved: true }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("apply rejects when the source changed epoch after planning (stale plan)", async () => {
    const tenantId = `mig-apply-staleepoch-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(
      tenantId,
      "mig-apply-stale-epoch",
    );
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const firstPlan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    // Advance the epoch out from under the plan via an unrelated migration.
    const otherTarget = sameShapeTarget(sequence);
    otherTarget.version = 3;
    const createdOther = await client.createSequence(otherTarget);
    const otherPlan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdOther.id,
      to_version: otherTarget.version,
    });
    await client.applyContinuityMigration(otherPlan.id, { tenant_id: tenantId });

    await assert.rejects(
      () => client.applyContinuityMigration(firstPlan.id, { tenant_id: tenantId }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("apply 404s for an unknown plan id", async () => {
    await assert.rejects(
      () => client.applyContinuityMigration(uuid(), { tenant_id: "whoever" }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  // ==================================================================
  // Rollback migration
  // ==================================================================

  it("rollback succeeds and restores the pre-migration sequence/state/context", async () => {
    const tenantId = `mig-rb-ok-${uuid().slice(0, 8)}`;
    const { execution, sequence, instance } = await pausedContinuityFixture(
      tenantId,
      "mig-rollback-ok",
      [],
      { keep: "original" },
    );
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    const rolledBack = await client.rollbackContinuityMigration(plan.id, {
      tenant_id: tenantId,
    });
    assert.equal(rolledBack.state, "rolled_back");
    const afterInstance = await client.getInstance(instance.id);
    assert.equal((afterInstance as any).sequence_id, sequence.id);
    assert.equal(afterInstance.state, "waiting");
  });

  it("rollback advances the epoch again", async () => {
    const tenantId = `mig-rb-epoch-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-rollback-epoch");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    const afterApply = await client.getContinuityExecution(execution.continuity_id, tenantId);
    await client.rollbackContinuityMigration(plan.id, { tenant_id: tenantId });
    const afterRollback = await client.getContinuityExecution(execution.continuity_id, tenantId);
    assert.equal(afterRollback.epoch, afterApply.epoch + 1);
  });

  it("rollback appends a migration_rolled_back provenance boundary", async () => {
    const tenantId = `mig-rb-prov-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-rollback-prov");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    await client.rollbackContinuityMigration(plan.id, { tenant_id: tenantId });
    const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
    assert.ok(
      provenance.some((entry: any) => entry.kind === "migration_rolled_back"),
      `expected migration_rolled_back provenance entry, got: ${JSON.stringify(provenance)}`,
    );
  });

  it("rollback rejects a plan that was never applied (still Planned)", async () => {
    const tenantId = `mig-rb-notapplied-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(
      tenantId,
      "mig-rollback-not-applied",
    );
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await assert.rejects(
      () => client.rollbackContinuityMigration(plan.id, { tenant_id: tenantId }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("rollback rejects a plan that was already rolled back (double-rollback)", async () => {
    const tenantId = `mig-rb-twice-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-rollback-twice");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    await client.rollbackContinuityMigration(plan.id, { tenant_id: tenantId });
    await assert.rejects(
      () => client.rollbackContinuityMigration(plan.id, { tenant_id: tenantId }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("rollback rejects when the target instance already resumed (state changed)", async () => {
    const tenantId = `mig-rb-resumed-${uuid().slice(0, 8)}`;
    const { execution, sequence, instance } = await pausedContinuityFixture(
      tenantId,
      "mig-rollback-resumed",
    );
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    // Resume the paused instance out from under the rollback.
    await client.updateState(instance.id, "scheduled");
    await assert.rejects(
      () => client.rollbackContinuityMigration(plan.id, { tenant_id: tenantId }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("rollback 404s for an unknown plan id", async () => {
    await assert.rejects(
      () => client.rollbackContinuityMigration(uuid(), { tenant_id: "whoever" }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("rollback 404s / rejects for wrong tenant", async () => {
    const tenantId = `mig-rb-wt-${uuid().slice(0, 8)}`;
    const wrongTenant = `mig-rb-wt-wrong-${uuid().slice(0, 8)}`;
    const { execution, sequence } = await pausedContinuityFixture(tenantId, "mig-rollback-wrong-tenant");
    const target = sameShapeTarget(sequence);
    const createdTarget = await client.createSequence(target);
    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdTarget.id,
      to_version: target.version,
    });
    await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
    await assert.rejects(
      () => client.rollbackContinuityMigration(plan.id, { tenant_id: wrongTenant }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  // ==================================================================
  // What-if simulation
  // ==================================================================

  it("what-if 404s on an unknown checkpoint_id", async () => {
    const tenantId = `wi-unk-${uuid().slice(0, 8)}`;
    const { execution } = await pausedContinuityFixture(tenantId, "wi-unknown-checkpoint");
    await assert.rejects(
      () =>
        client.runContinuityWhatIf(execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: uuid(),
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("what-if runs baseline vs candidate and returns a comparison", async () => {
    const tenantId = `wi-basic-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "wi-basic");
    const result = await client.runContinuityWhatIf(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
    });
    assert.ok(result.baseline_report);
    assert.ok(result.report);
    assert.ok(result.comparison);
    assert.equal(result.comparison.effects.production_receipts_created, 0);
  });

  it("what-if context_patch merges onto the checkpointed context", async () => {
    const tenantId = `wi-ctxpatch-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(
      tenantId,
      "wi-context-patch",
      [],
      { order_id: "base-order" },
    );
    const result = await client.runContinuityWhatIf(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
      context_patch: { discount: 0.2 },
    });
    assert.equal(result.scenario.context_patch.discount, 0.2);
  });

  it("what-if config_patch is recorded on the scenario", async () => {
    const tenantId = `wi-cfgpatch-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "wi-config-patch");
    const result = await client.runContinuityWhatIf(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
      config_patch: { region: "eu-west-1" },
    });
    assert.equal(result.scenario.config_patch.region, "eu-west-1");
  });

  it("what-if rejects a non-object context_patch", async () => {
    const tenantId = `wi-badctx-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "wi-bad-context");
    await assert.rejects(
      () =>
        client.runContinuityWhatIf(execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: checkpointId,
          context_patch: "not-an-object",
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 400);
        return true;
      },
    );
  });

  it("what-if block_param_overrides rejects an unknown block id", async () => {
    const tenantId = `wi-unkblock-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "wi-unknown-block");
    await assert.rejects(
      () =>
        client.runContinuityWhatIf(execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: checkpointId,
          block_param_overrides: { does_not_exist: { foo: "bar" } },
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 400);
        return true;
      },
    );
  });

  it("what-if target_sequence_version 404s on an unknown version", async () => {
    const tenantId = `wi-unkver-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "wi-unknown-version");
    await assert.rejects(
      () =>
        client.runContinuityWhatIf(execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: checkpointId,
          target_sequence_version: 777,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("what-if reports added_blocks/removed_blocks for a differing target sequence", async () => {
    // The what-if simulation mocks each block from its REAL recorded output
    // (regardless of whether that output postdates the checkpoint), so the
    // boundary block itself must actually resolve in the live instance —
    // a checkpoint frozen mid-wait provides no mock for its own boundary.
    const tenantId = `wi-diff-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "wi-diff-blocks",
      [
        step("gate", "human_review", {}, {
          wait_for_input: {
            prompt: "continue?",
            choices: [{ label: "Continue", value: "continue" }],
          },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: sequence.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { order_id: "abc-123" } },
    });
    await client.waitForState(instance.id, "waiting");
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const checkpoint = await client.saveCheckpoint(instance.id, {
      safe_boundary: "gate",
      context_snapshot: { order_id: "abc-123" },
    });
    await client.sendSignal(
      instance.id,
      { custom: "human_input:gate" } as unknown as string,
      { value: "continue" },
    );
    await client.waitForState(instance.id, "completed");
    const target: SequenceDef = {
      ...sequence,
      id: uuid(),
      version: 2,
      blocks: [...sequence.blocks, step("added_step", "noop")],
    };
    await client.createSequence(target);
    const result = await client.runContinuityWhatIf(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpoint.id,
      target_sequence_version: target.version,
      signals: [{ signal_type: "custom:human_input:gate", payload: { value: "continue" } }],
    });
    assert.ok(
      result.comparison.added_blocks.includes("added_step"),
      `expected added_step in added_blocks, got: ${JSON.stringify(result.comparison.added_blocks)}`,
    );
  });

  it("what-if signals array over 1000 entries is rejected as too large", async () => {
    const tenantId = `wi-sigbig-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "wi-signals-big");
    const signals = Array.from({ length: 1001 }, (_, i) => ({
      signal_type: "resume",
      payload: { i },
    }));
    await assert.rejects(
      () =>
        client.runContinuityWhatIf(execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: checkpointId,
          signals,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 413);
        return true;
      },
    );
  });

  it("what-if output_overrides over 1000 entries is rejected as too large", async () => {
    const tenantId = `wi-ovrbig-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "wi-overrides-big");
    const output_overrides: Record<string, unknown> = {};
    for (let i = 0; i < 1001; i += 1) output_overrides[`block_${i}`] = { v: i };
    await assert.rejects(
      () =>
        client.runContinuityWhatIf(execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: checkpointId,
          output_overrides,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 413);
        return true;
      },
    );
  });

  it("what-if run is persisted and appears in list_what_if_runs", async () => {
    const tenantId = `wi-list-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "wi-list-runs");
    const result = await client.runContinuityWhatIf(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
    });
    const runs = await client.listContinuityWhatIfRuns(execution.continuity_id, tenantId);
    assert.ok(
      runs.some((run: any) => run.scenario.id === result.scenario.id),
      "expected the just-created run in the list",
    );
  });

  it("what-if list is tenant-isolated", async () => {
    const tenantId = `wi-iso-a-${uuid().slice(0, 8)}`;
    const otherTenant = `wi-iso-b-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "wi-isolation");
    await client.runContinuityWhatIf(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
    });
    await assert.rejects(
      () => client.listContinuityWhatIfRuns(execution.continuity_id, otherTenant),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("what-if requires tenant_id to match the caller (create-tenant enforcement)", async () => {
    const tenantId = `wi-tenforce-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "wi-tenant-enforce");
    // No auth headers configured in this harness — tenant enforcement is
    // exercised at the body level; a mismatched body tenant_id against an
    // unrelated execution must 404 since the execution won't resolve there.
    await assert.rejects(
      () =>
        client.runContinuityWhatIf(execution.continuity_id, {
          tenant_id: `${tenantId}-different`,
          checkpoint_id: checkpointId,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  // ==================================================================
  // Test fixture extraction
  // ==================================================================

  it("extract 404s on an unknown checkpoint_id", async () => {
    const tenantId = `fx-unk-${uuid().slice(0, 8)}`;
    const { execution } = await pausedContinuityFixture(tenantId, "fx-unknown-checkpoint");
    await assert.rejects(
      () =>
        client.extractContinuityTestFixture(execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: uuid(),
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("extract with an empty allowlist redacts the whole context to {}", async () => {
    const tenantId = `fx-empty-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(
      tenantId,
      "fx-empty-allowlist",
      [],
      { secret_field: "s3cr3t", public_field: "hello" },
    );
    const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
    });
    assert.deepEqual(fixture.sanitized_context, {});
  });

  it("extract allowlist selects only named fields", async () => {
    const tenantId = `fx-allow-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(
      tenantId,
      "fx-allowlist",
      [],
      { keep_me: "visible", drop_me: "hidden" },
    );
    const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
      allowlisted_fields: ["keep_me"],
    });
    assert.equal(fixture.sanitized_context.keep_me, "visible");
    assert.ok(!("drop_me" in fixture.sanitized_context));
  });

  it("extract redacts secret-shaped allowlisted values", async () => {
    const tenantId = `fx-redact-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(
      tenantId,
      "fx-redact-secrets",
      [],
      { api_token: "sk_live_abcdef123456" },
    );
    const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
      allowlisted_fields: ["api_token"],
    });
    assert.notEqual(fixture.sanitized_context.api_token, "sk_live_abcdef123456");
  });

  it("extract allowlist over 256 fields is rejected as too large", async () => {
    const tenantId = `fx-bigallow-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "fx-big-allowlist");
    const allowlisted_fields = Array.from({ length: 257 }, (_, i) => `field_${i}`);
    await assert.rejects(
      () =>
        client.extractContinuityTestFixture(execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: checkpointId,
          allowlisted_fields,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 413);
        return true;
      },
    );
  });

  it("extract produces a deterministic stable_id for identical inputs", async () => {
    const tenantId = `fx-stable-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "fx-stable-id");
    const first = await client.extractContinuityTestFixture(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
    });
    const second = await client.extractContinuityTestFixture(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
    });
    assert.equal(first.stable_id, second.stable_id);
  });

  it("extract flags missing context_snapshot evidence when the checkpoint lacks one", async () => {
    const tenantId = `fx-missing-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "fx-missing-ctx",
      [step("gate", "human_review", {}, { wait_for_input: { prompt: "x" } })],
      { tenantId },
    );
    const createdSequence = await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: createdSequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(instance.id, "waiting");
    // Save a checkpoint deliberately WITHOUT a context_snapshot field.
    await client.saveCheckpoint(instance.id, { safe_boundary: "gate" });
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const checkpoints = await client.listContinuityCheckpoints(
      execution.continuity_id,
      tenantId,
    );
    const checkpointId = checkpoints[checkpoints.length - 1].checkpoint_id;
    const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
    });
    assert.ok(
      fixture.missing_evidence.includes("checkpoint.context_snapshot"),
      `expected missing context_snapshot evidence, got: ${JSON.stringify(fixture.missing_evidence)}`,
    );
    assert.equal(fixture.complete, false);
  });

  it("extract is complete (no missing evidence) for a checkpoint on a terminal instance", async () => {
    const tenantId = `fx-complete-${uuid().slice(0, 8)}`;
    const sequence = testSequence("fx-complete", [step("s1", "noop")], { tenantId });
    await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: sequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(instance.id, "completed");
    await client.saveCheckpoint(instance.id, { safe_boundary: "s1", context_snapshot: {} });
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const checkpoints = await client.listContinuityCheckpoints(
      execution.continuity_id,
      tenantId,
    );
    const checkpointId = checkpoints[checkpoints.length - 1].checkpoint_id;
    const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpointId,
    });
    assert.equal(fixture.complete, true);
    assert.deepEqual(fixture.missing_evidence, []);
  });

  it("extract 404s for wrong tenant", async () => {
    const tenantId = `fx-wt-${uuid().slice(0, 8)}`;
    const wrongTenant = `fx-wt-wrong-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "fx-wrong-tenant");
    await assert.rejects(
      () =>
        client.extractContinuityTestFixture(execution.continuity_id, {
          tenant_id: wrongTenant,
          checkpoint_id: checkpointId,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  // ==================================================================
  // Checkpoint diff (get_continuity_checkpoint) — diff-specific behavior
  // ==================================================================

  it("checkpoint detail 404s on an unknown checkpoint_id", async () => {
    const tenantId = `ckpt-unk-${uuid().slice(0, 8)}`;
    const { execution } = await pausedContinuityFixture(tenantId, "ckpt-unknown");
    await assert.rejects(
      () => client.getContinuityCheckpoint(execution.continuity_id, uuid(), tenantId),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("checkpoint detail reports an empty before-state for the first checkpoint", async () => {
    const tenantId = `ckpt-first-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "ckpt-first");
    const detail = await client.getContinuityCheckpoint(
      execution.continuity_id,
      checkpointId,
      tenantId,
    );
    assert.equal(detail.previous_checkpoint_id, null);
  });

  it("checkpoint detail reports a redacted state diff between consecutive checkpoints", async () => {
    const tenantId = `ckpt-diff-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "ckpt-diff",
      [step("gate", "human_review", {}, { wait_for_input: { prompt: "x" } })],
      { tenantId },
    );
    const createdSequence = await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: createdSequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(instance.id, "waiting");
    await client.saveCheckpoint(instance.id, {
      safe_boundary: "gate",
      context_snapshot: { step: 1 },
    });
    await client.saveCheckpoint(instance.id, {
      safe_boundary: "gate",
      context_snapshot: { step: 2 },
    });
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const checkpoints = await client.listContinuityCheckpoints(
      execution.continuity_id,
      tenantId,
    );
    assert.ok(checkpoints.length >= 2, "expected at least two checkpoints");
    const latest = checkpoints[checkpoints.length - 1];
    const detail = await client.getContinuityCheckpoint(
      execution.continuity_id,
      latest.checkpoint_id,
      tenantId,
    );
    assert.ok(detail.previous_checkpoint_id, "expected a previous checkpoint id");
    assert.ok(
      detail.redacted_state_diff.some((change: any) => change.path.includes("step")),
      `expected a diff entry for the changed step field, got: ${JSON.stringify(detail.redacted_state_diff)}`,
    );
  });

  it("checkpoint detail 404s for wrong tenant", async () => {
    const tenantId = `ckpt-wt-${uuid().slice(0, 8)}`;
    const wrongTenant = `ckpt-wt-wrong-${uuid().slice(0, 8)}`;
    const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "ckpt-wrong-tenant");
    await assert.rejects(
      () => client.getContinuityCheckpoint(execution.continuity_id, checkpointId, wrongTenant),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });
});
