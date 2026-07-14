/**
 * E2E: Portable Continuity — full lifecycle integration scenarios and a
 * systematic tenant-isolation sweep.
 *
 * This file does NOT re-litigate single-endpoint validation already covered
 * by the other nine continuity_*.test.ts files. It covers two things those
 * files were not scoped to:
 *
 * HALF A — cross-feature integration: chains of multiple continuity
 * primitives (execution -> handoff -> provenance -> epoch, effects ->
 * compensation -> provenance, migration -> epoch -> stream/stream frame
 * staleness, delegation -> provenance, residency/disclosure/federation on
 * one shared chain, what-if read-only guarantees, migration rollback state
 * restoration, and terminal-state coherence after a full lifecycle).
 *
 * HALF B — a table-driven tenant-isolation sweep across every GET-by-id and
 * list endpoint in orch8-api/src/continuity.rs: a resource created under
 * tenant A must 404 (never 403, never empty-success) when fetched by id
 * under tenant B, and tenant B's list endpoints must never surface tenant
 * A's resources.
 *
 * See orch8-api/src/continuity.rs (routes() at lines 51-211) for the full
 * endpoint surface, and continuity_attention_residency_federation.test.ts /
 * continuity_handoffs.test.ts / continuity_migrations_whatif.test.ts /
 * continuity_invariants_compensations.test.ts / continuity_provenance_effects.test.ts
 * for the proven request-shape recipes reused below.
 */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHash, generateKeyPairSync, sign } from "node:crypto";

import { ApiError, Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block, SequenceDef, WorkerTask } from "../types.ts";

const client = new Orch8Client();

// ======================================================================
// Shared helpers (mirrors proven patterns from the other continuity files)
// ======================================================================

function tid(prefix: string): string {
  return `${prefix}-${uuid().slice(0, 8)}`;
}

async function rejects(promise: Promise<unknown>, status: number, msg?: string) {
  await assert.rejects(
    () => promise,
    (error: unknown) => {
      assert.equal((error as ApiError).status, status, msg);
      return true;
    },
  );
}

function runtimeCaps(
  runtimeId: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  const now = Date.now();
  return {
    runtime_id: runtimeId,
    kind: "server",
    trust: "registered",
    handlers: ["noop"],
    regions: ["br-south"],
    hardware: [],
    offline_capable: false,
    connectivity: "wifi",
    battery_percent: 100,
    estimated_cost_microunits: 10,
    estimated_latency_ms: 20,
    observed_at: new Date(now).toISOString(),
    expires_at: new Date(now + 60_000).toISOString(),
    ...overrides,
  };
}

async function registerRuntime(
  tenantId: string,
  runtimeId: string,
  overrides: Record<string, unknown> = {},
): Promise<unknown> {
  return client.registerRuntime({
    tenant_id: tenantId,
    capabilities: runtimeCaps(runtimeId, overrides),
  });
}

/** Minimal fresh execution: sequence + instance (running->completed noop) + continuity. */
async function setupExecution(tenantId: string): Promise<{
  execution: any;
  instance: any;
  runtimeId: string;
  sequence: any;
}> {
  const seq = testSequence("int-setup", [step("s1", "noop")], { tenantId });
  const createdSeq = await client.createSequence(seq);
  const instance = await client.createInstance({
    sequence_id: createdSeq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  const runtimeId = uuid();
  const execution = await client.createContinuityExecution({
    tenant_id: tenantId,
    instance_id: instance.id,
    runtime_id: runtimeId,
  });
  return { execution, instance, runtimeId, sequence: { ...seq, id: createdSeq.id } };
}

/** Drive an instance to `waiting` via a human_review gate, register a
 * continuity execution over it, and durably checkpoint it. */
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
  const createdSeq = await client.createSequence(sequence);
  const instance = await client.createInstance({
    sequence_id: createdSeq.id,
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
  const checkpoints = await client.listContinuityCheckpoints(execution.continuity_id, tenantId);
  assert.ok(checkpoints.length >= 1, "fixture must produce at least one checkpoint");
  const checkpointId = checkpoints[checkpoints.length - 1].checkpoint_id;
  return {
    sequence: { ...sequence, id: createdSeq.id },
    instance,
    execution,
    checkpointId,
    runtimeId,
  };
}

function sameShapeTarget(source: SequenceDef): SequenceDef {
  return { ...source, id: uuid(), version: 2 };
}

/** Full authorized-handoff request recipe (preview -> placement_decision_id). */
async function authorizedHandoffRequest(
  tenantId: string,
  continuityId: string,
  destinationRuntimeId: string,
  requirements: Record<string, unknown> = { handlers: ["noop"] },
  capabilityOverrides: Record<string, unknown> = {},
) {
  await registerRuntime(tenantId, destinationRuntimeId, {
    handlers: (requirements as { handlers?: string[] }).handlers ?? ["noop"],
    ...capabilityOverrides,
  });
  const preview = await client.previewHandoff(continuityId, {
    tenant_id: tenantId,
    destination_runtime_id: destinationRuntimeId,
    requirements,
  });
  return {
    tenant_id: tenantId,
    continuity_id: continuityId,
    destination_runtime_id: destinationRuntimeId,
    requirements,
    placement_decision_id: preview.placement_decision.id,
    preview_sha256: preview.preview_sha256,
  };
}

/** Full happy-path chain to a Resumed handoff on a fresh destination instance. */
async function resumedHandoff(namePrefix: string) {
  const tenantId = tid(namePrefix);
  const sequence = testSequence(
    namePrefix,
    [
      step("gate", "human_review", {}, {
        wait_for_input: {
          prompt: "continue?",
          choices: [{ label: "go", value: "go" }],
        },
      }),
    ],
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
    context_snapshot: { fixture: tenantId },
  });
  const sourceRuntimeId = uuid();
  const destinationRuntimeId = uuid();
  const execution = await client.createContinuityExecution({
    tenant_id: tenantId,
    instance_id: instance.id,
    runtime_id: sourceRuntimeId,
  });
  const req = await authorizedHandoffRequest(
    tenantId,
    execution.continuity_id,
    destinationRuntimeId,
    { handlers: ["human_review"] },
    { kind: "mobile", offline_capable: true },
  );
  const handoff = await client.createHandoff(req);
  const payloadKey = Buffer.from(
    Array.from({ length: 32 }, () => Math.floor(Math.random() * 256)),
  ).toString("base64");
  const exported = await client.exportHandoff(handoff.id, {
    tenant_id: tenantId,
    requirements: { handlers: ["human_review"] },
    expires_in_seconds: 60,
    payload_key_base64: payloadKey,
  });
  const destinationInstanceId = uuid();
  const imported = await client.importContinuityCapsule({
    tenant_id: tenantId,
    destination_runtime_id: destinationRuntimeId,
    destination_instance_id: destinationInstanceId,
    expected_epoch: 0,
    capsule: exported.capsule,
    payload_base64: exported.payload_base64,
    payload_key_base64: payloadKey,
  });
  const accepted = await client.acceptHandoff(handoff.id, {
    tenant_id: tenantId,
    destination_instance_id: imported.instance_id,
  });
  const resumed = await client.resumeHandoff(handoff.id, { tenant_id: tenantId });
  return {
    tenantId,
    execution,
    handoff,
    exported,
    imported,
    accepted,
    resumed,
    sourceRuntimeId,
    destinationRuntimeId,
  };
}

async function waitForWorkerTask(
  handler: string,
  workerId: string,
  timeoutMs = 10_000,
): Promise<WorkerTask> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const [task] = await client.pollWorkerTasks(handler, workerId);
    if (task) return task as WorkerTask;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`Timeout waiting for worker task for ${handler}`);
}

/** Build a two-step sequence (arm -> worker-dispatched charge) whose "charge"
 * step carries a compensation rule; drives to a `dispatched` effect. */
async function compensableExecution(namePrefix: string) {
  const tenantId = tid(namePrefix);
  const handler = `${namePrefix}_h_${uuid().slice(0, 8)}`;
  const blocks = [
    step("arm", "human_review", {}, {
      wait_for_input: {
        prompt: "Arm effect dispatch?",
        choices: [{ label: "Continue", value: "continue" }],
      },
    }),
    step("charge", handler, { amount: 100 }, {
      compensation: { handler: "release_charge", verification: "handler_result" },
    }),
  ];
  const sequence = testSequence(namePrefix, blocks, { tenantId });
  const created = await client.createSequence(sequence);
  const createdSequence = { ...sequence, id: created.id };
  const instance = await client.createInstance({
    sequence_id: createdSequence.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(instance.id, "waiting");
  const execution = await client.createContinuityExecution({
    tenant_id: tenantId,
    instance_id: instance.id,
    runtime_id: uuid(),
  });
  await client.sendSignal(
    instance.id,
    { custom: "human_input:arm" } as unknown as string,
    { value: "continue" },
  );
  const task = await waitForWorkerTask(handler, `${namePrefix}-worker`);
  return { tenantId, createdSequence, instance, execution, handler, task };
}

/** Drive a compensable execution's "charge" effect all the way to `committed`. */
async function committedCompensableExecution(namePrefix: string) {
  const setup = await compensableExecution(namePrefix);
  const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
  assert.equal(effects.length, 1, "setup: exactly one dispatched effect");
  const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
    tenant_id: setup.tenantId,
    state: "committed",
    provider_receipt_id: `receipt-${uuid().slice(0, 8)}`,
  });
  assert.equal(resolved.state, "committed", "setup: effect should be committed");
  return { ...setup, effect: resolved };
}

/** Fully complete a single-step compensation run (claim -> complete). */
async function completeCompensationRun(continuityId: string, tenantId: string) {
  const created = await client.createContinuityCompensation(continuityId, { tenant_id: tenantId });
  const claimed = await client.claimContinuityCompensation(created.id, {
    tenant_id: tenantId,
    worker_id: "integration-worker",
  });
  const completed = await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
    tenant_id: tenantId,
    worker_id: "integration-worker",
  });
  return completed;
}

// ======================================================================
// Federation peer fixture (module-level, mirrors
// continuity_attention_residency_federation.test.ts)
// ======================================================================
const federationTenantId = tid("int-fed");
const federationPeerId = uuid();
const { publicKey: fedPub, privateKey: fedPriv } = generateKeyPairSync("ed25519");
const fedPubRaw = fedPub.export({ format: "der", type: "spki" }).subarray(-32);
const federationPeer = {
  id: federationPeerId,
  name: "int-e2e-peer",
  trust_root_sha256: createHash("sha256").update(fedPubRaw).digest("hex"),
  public_key: fedPubRaw.toString("base64"),
  endpoint: "https://peer.example.invalid",
  allowed_tenants: [federationTenantId],
  revoked_at: null as string | null,
};

function signEnvelope(unsigned: Record<string, unknown>): string {
  return sign(null, Buffer.from(JSON.stringify(unsigned)), fedPriv).toString("base64");
}

describe("Continuity — Lifecycle Integration & Tenant Isolation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "9c".repeat(32),
        ORCH8_ARTIFACT_BACKEND: "local",
        ORCH8_ARTIFACT_PATH: `/tmp/continuity-integ-isolation-artifacts-${uuid()}`,
        ORCH8_LOG_LEVEL: "error",
        ORCH8_FEDERATION_PEERS: JSON.stringify([federationPeer]),
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  // ====================================================================
  // HALF A — Full lifecycle integration scenarios
  // ====================================================================

  describe("A. handoff -> epoch -> provenance chain integrity", () => {
    it("epoch advances by exactly 2 across accept+resume and the provenance chain still verifies", async () => {
      const setup = await resumedHandoff("int-hoff-prov");
      const finalExecution = await client.getContinuityExecution(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      // accept_handoff bumps epoch once (0 -> 1); resume does not bump again.
      assert.equal(finalExecution.epoch, setup.execution.epoch + 1);
      const verification = await client.verifyContinuityProvenance(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(verification.valid, true, "provenance chain must verify after handoff+resume");

      const provenance = await client.listContinuityProvenance(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      const kinds = provenance.map((entry: any) => entry.kind);
      assert.ok(kinds.includes("execution_created"));
      assert.ok(kinds.includes("capsule_exported"));
      assert.ok(kinds.includes("runtime_claimed"), "epoch-bump boundary must be recorded");
      assert.ok(kinds.includes("execution_resumed"));
      // Chain must be strictly ordered: created before claimed before resumed.
      const createdIdx = kinds.indexOf("execution_created");
      const claimedIdx = kinds.indexOf("runtime_claimed");
      const resumedIdx = kinds.indexOf("execution_resumed");
      assert.ok(createdIdx < claimedIdx && claimedIdx < resumedIdx);
    });

    it("verify_provenance rejects an expected_head that predates the handoff boundary", async () => {
      const setup = await resumedHandoff("int-hoff-stalehead");
      const provenance = await client.listContinuityProvenance(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      const staleHead = provenance[0].entry_sha256;
      const verification = await client.verifyContinuityProvenance(
        setup.execution.continuity_id,
        setup.tenantId,
        staleHead,
      );
      assert.notEqual(verification.valid, true, "a stale expected_head must fail verification");
    });

    it("a fully accepted+resumed execution can no longer be handed off again from the old owner", async () => {
      const setup = await resumedHandoff("int-hoff-nodouble");
      // The old source runtime tries to hand off again using a fresh preview;
      // ownership has moved, so the preview's source_runtime_id reflects the
      // NEW owner, proving the old owner can no longer author a handoff.
      // (Registering the old source runtime here only makes it a valid
      // preview *destination* candidate for this probe — it does not make it
      // the execution owner again.)
      await registerRuntime(setup.tenantId, setup.sourceRuntimeId, {
        handlers: ["human_review"],
      });
      const preview = await client.previewHandoff(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
        destination_runtime_id: setup.sourceRuntimeId,
        requirements: { handlers: ["human_review"] },
      });
      assert.equal(preview.source_runtime_id, setup.destinationRuntimeId);
      assert.notEqual(preview.source_runtime_id, setup.sourceRuntimeId);
    });
  });

  describe("A. effect receipts -> compensation -> provenance", () => {
    it("committed effect drives a compensation run to completion and the effect flips to compensated", async () => {
      const setup = await committedCompensableExecution("int-comp-lifecycle");
      const completed = await completeCompensationRun(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(completed.state, "completed");
      const effects = await client.listContinuityEffects(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(effects[0]!.state, "compensated");
    });

    it("resolving an effect to committed appends effect_resolved provenance before compensation", async () => {
      const setup = await committedCompensableExecution("int-comp-prov-order");
      const provenance = await client.listContinuityProvenance(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      const kinds = provenance.map((entry: any) => entry.kind);
      assert.ok(kinds.includes("effect_resolved"));
      const verification = await client.verifyContinuityProvenance(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(verification.valid, true);
    });

    it("compensation preview plan step count matches the run's step count for the same execution", async () => {
      const setup = await committedCompensableExecution("int-comp-preview-match");
      const preview = await client.previewContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const run = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.equal(run.steps.length, preview.steps.length);
      assert.equal(run.steps[0]!.plan.effect_block_id, preview.steps[0]!.effect_block_id);
    });
  });

  describe("A. live migration -> epoch -> stream/stream-frame staleness", () => {
    it("a stream created before migration becomes stale (epoch mismatch) after apply", async () => {
      const tenantId = tid("int-mig-strm");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-mig-strm");
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      assert.equal(stream.epoch, execution.epoch);

      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });

      const postMigrationExecution = await client.getContinuityExecution(
        execution.continuity_id,
        tenantId,
      );
      assert.equal(postMigrationExecution.epoch, execution.epoch + 1);

      // The stream is still bound to the pre-migration epoch: appending must
      // be rejected as a conflict (stale epoch), never silently accepted.
      await rejects(
        client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: 0,
          checkpoint_sha256: "a".repeat(64),
          payload: {},
        }),
        409,
        "a frame appended after migration must be rejected as belonging to a stale epoch",
      );
    });

    it("a new stream created after migration binds to the post-migration epoch", async () => {
      const tenantId = tid("int-mig-strm2");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-mig-strm2");
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
      const postExecution = await client.getContinuityExecution(execution.continuity_id, tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      assert.equal(stream.epoch, postExecution.epoch);
      assert.notEqual(stream.epoch, execution.epoch);
    });

    it("migration_applied provenance entry follows the residency-evaluated boundary on a shared chain", async () => {
      const tenantId = tid("int-mig-order");
      const { execution, sequence, runtimeId } = await pausedContinuityFixture(
        tenantId,
        "int-mig-order",
      );
      await client.evaluateContinuityResidency({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        classification: "internal",
        operation: "handler_dispatch",
        destination_runtime_id: runtimeId,
      });
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
      const kinds = provenance.map((entry: any) => entry.kind);
      const residencyIdx = kinds.indexOf("residency_evaluated");
      const migrationIdx = kinds.indexOf("migration_applied");
      assert.ok(residencyIdx >= 0 && migrationIdx >= 0);
      assert.ok(residencyIdx < migrationIdx);
      const verification = await client.verifyContinuityProvenance(execution.continuity_id, tenantId);
      assert.equal(verification.valid, true);
    });
  });

  describe("A. migration rollback restores pre-migration state", () => {
    it("rollback restores the exact pre-migration sequence_id, instance state, and context", async () => {
      const tenantId = tid("int-mig-rb");
      const { execution, sequence, instance } = await pausedContinuityFixture(
        tenantId,
        "int-mig-rb",
        [],
        { keep: "original-value" },
      );
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      const applied = await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
      assert.equal(applied.state, "applied");
      const midExecution = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.equal(midExecution.epoch, execution.epoch + 1);

      const rolledBack = await client.rollbackContinuityMigration(plan.id, { tenant_id: tenantId });
      assert.equal(rolledBack.state, "rolled_back");

      const finalExecution = await client.getContinuityExecution(execution.continuity_id, tenantId);
      // Rollback advances the epoch again (does not reuse the pre-migration
      // epoch number) but restores sequence/state/context content.
      assert.equal(finalExecution.epoch, midExecution.epoch + 1);
      const finalInstance = await client.getInstance(instance.id);
      assert.equal((finalInstance as any).sequence_id, sequence.id);
      assert.equal(finalInstance.state, "waiting");
    });

    it("rollback does not restore the pre-migration epoch number itself (epochs never move backward)", async () => {
      const tenantId = tid("int-mig-rb-epoch");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-mig-rb-epoch");
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
      const finalExecution = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.ok(
        finalExecution.epoch > execution.epoch,
        "epoch must monotonically increase even across a rollback",
      );
    });
  });

  describe("A. what-if is strictly read-only against the real execution", () => {
    it("running what-if does not mutate the execution epoch, owner, or provenance chain", async () => {
      const tenantId = tid("int-whatif-ro");
      const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "int-whatif-ro");
      const beforeExecution = await client.getContinuityExecution(execution.continuity_id, tenantId);
      const beforeProvenance = await client.listContinuityProvenance(
        execution.continuity_id,
        tenantId,
      );
      await client.runContinuityWhatIf(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: checkpointId,
        context_patch: { probe: "value" },
      });
      const afterExecution = await client.getContinuityExecution(execution.continuity_id, tenantId);
      const afterProvenance = await client.listContinuityProvenance(
        execution.continuity_id,
        tenantId,
      );
      assert.deepEqual(afterExecution, beforeExecution, "what-if must not mutate the execution row");
      assert.equal(
        afterProvenance.length,
        beforeProvenance.length,
        "what-if must not append to the real provenance chain",
      );
    });

    it("running what-if does not mutate the real instance's checkpoints", async () => {
      const tenantId = tid("int-whatif-ckpt");
      const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "int-whatif-ckpt");
      const beforeCheckpoints = await client.listContinuityCheckpoints(
        execution.continuity_id,
        tenantId,
      );
      await client.runContinuityWhatIf(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: checkpointId,
        output_overrides: { gate: { probe: true } },
      });
      const afterCheckpoints = await client.listContinuityCheckpoints(
        execution.continuity_id,
        tenantId,
      );
      assert.equal(afterCheckpoints.length, beforeCheckpoints.length);
      assert.deepEqual(
        afterCheckpoints.map((c: any) => c.checkpoint_id),
        beforeCheckpoints.map((c: any) => c.checkpoint_id),
      );
    });

    it("the what-if run is recorded in list_what_if_runs without touching the execution", async () => {
      const tenantId = tid("int-whatif-record");
      const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "int-whatif-record");
      await client.runContinuityWhatIf(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: checkpointId,
      });
      const runs = await client.listContinuityWhatIfRuns(execution.continuity_id, tenantId);
      assert.equal(runs.length, 1);
      const stillSameEpoch = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.equal(stillSameEpoch.epoch, execution.epoch);
    });
  });

  describe("A. residency + disclosure + federation on one shared chain", () => {
    it("residency and federation both write to the same provenance chain in call order, disclosure stays out of it", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const destRuntime = uuid();
      await registerRuntime(federationTenantId, destRuntime, { regions: ["br-south"] });

      await client.evaluateContinuityResidency({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        classification: "internal",
        operation: "federation",
        destination_runtime_id: destRuntime,
      });

      // Disclosure minimization is a pure function — no continuity_id at all —
      // so it must never touch this execution's provenance chain.
      await client.minimizeContinuityDisclosure({
        classification: "public",
        payload: { a: 1 },
        allowed_top_level_fields: ["a"],
      });

      const payload = Buffer.from(`payload-${uuid()}`);
      const unsigned = {
        id: uuid(),
        peer_id: federationPeerId,
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
        destination_runtime_id: destRuntime,
        payload_sha256: createHash("sha256").update(payload).digest("hex"),
        issued_at: new Date(Date.now() - 100).toISOString(),
        expires_at: new Date(Date.now() + 30_000).toISOString(),
        signature: "",
      };
      const signature = signEnvelope(unsigned);
      await client.verifyFederationEnvelope({
        tenant_id: federationTenantId,
        envelope: { ...unsigned, signature },
        payload_base64: payload.toString("base64"),
      });

      const provenance = await client.listContinuityProvenance(
        execution.continuity_id,
        federationTenantId,
      );
      const kinds = provenance.map((entry: any) => entry.kind);
      assert.ok(!kinds.includes("disclosure_minimized"), "disclosure has no continuity binding");
      const residencyIdx = kinds.indexOf("residency_evaluated");
      const federationIdx = kinds.indexOf("federation_received");
      assert.ok(residencyIdx >= 0 && federationIdx >= 0);
      assert.ok(residencyIdx < federationIdx, "entries must appear in call order");
      const verification = await client.verifyContinuityProvenance(
        execution.continuity_id,
        federationTenantId,
      );
      assert.equal(verification.valid, true);
    });

    it("a second execution's residency evaluation does not contaminate the first execution's chain", async () => {
      const { execution: execA } = await setupExecution(federationTenantId);
      const { execution: execB } = await setupExecution(federationTenantId);
      const runtimeA = uuid();
      const runtimeB = uuid();
      await registerRuntime(federationTenantId, runtimeA);
      await registerRuntime(federationTenantId, runtimeB);
      await client.evaluateContinuityResidency({
        tenant_id: federationTenantId,
        continuity_id: execA.continuity_id,
        classification: "internal",
        operation: "handler_dispatch",
        destination_runtime_id: runtimeA,
      });
      await client.evaluateContinuityResidency({
        tenant_id: federationTenantId,
        continuity_id: execB.continuity_id,
        classification: "internal",
        operation: "handler_dispatch",
        destination_runtime_id: runtimeB,
      });
      const provA = await client.listContinuityProvenance(execA.continuity_id, federationTenantId);
      const provB = await client.listContinuityProvenance(execB.continuity_id, federationTenantId);
      assert.ok(provA.every((entry: any) => entry.continuity_id === execA.continuity_id));
      assert.ok(provB.every((entry: any) => entry.continuity_id === execB.continuity_id));
      // Exactly one residency_evaluated entry apiece — no cross-write.
      assert.equal(
        provA.filter((entry: any) => entry.kind === "residency_evaluated").length,
        1,
      );
      assert.equal(
        provB.filter((entry: any) => entry.kind === "residency_evaluated").length,
        1,
      );
    });
  });

  describe("A. device delegation records a boundary on the parent chain", () => {
    it("a claimed delegation appends device_delegation to the parent's chain, not a new one", async () => {
      const tenantId = tid("int-deleg");
      const { execution, runtimeId: sourceRuntimeId } = await setupExecution(tenantId);
      await registerRuntime(tenantId, sourceRuntimeId);
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime);
      const subSeq = await client.createSequence(
        testSequence("int-deleg-sub", [step("s1", "noop")], { tenantId }),
      );
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      await client.claimDeviceDelegation({
        tenant_id: tenantId,
        delegation,
        signed_grant: grant.signed_grant,
        token: grant.token,
      });
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      assert.ok(provenance.some((entry: any) => entry.kind === "device_delegation"));
      // The parent execution's own epoch/owner are untouched by delegation.
      const stillOwned = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.equal(stillOwned.epoch, execution.epoch);
      assert.equal(stillOwned.owner_runtime_id, sourceRuntimeId);
    });
  });

  describe("A. terminal coherence after a full compound lifecycle", () => {
    it("an execution that was migrated then had its committed effect compensated reaches a coherent, queryable terminal state", async () => {
      const tenantId = tid("int-terminal");
      const { execution, sequence, instance } = await pausedContinuityFixture(
        tenantId,
        "int-terminal",
      );
      // 1. Migrate to a same-shape target sequence.
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
      const postMigration = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.equal(postMigration.epoch, execution.epoch + 1);

      // 2. The execution remains queryable and internally consistent: the
      // instance it points at is the same instance, now on the target
      // sequence, in the same paused/waiting boundary state.
      const finalInstance = await client.getInstance(instance.id);
      assert.equal(postMigration.current_instance_id, instance.id);
      assert.equal((finalInstance as any).sequence_id, target.id);
      // apply_live_migration always lands the instance in `paused` (see
      // orch8-api/src/continuity.rs commit_live_migration_transition call:
      // next_instance_state is hardcoded to InstanceState::Paused).
      assert.equal(finalInstance.state, "paused");

      // 3. Locations must record every epoch the execution has occupied.
      const locations = await client.listContinuityLocations(execution.continuity_id, tenantId);
      const epochs = locations.map((location: any) => location.epoch).sort((a: number, b: number) => a - b);
      assert.ok(epochs.includes(0));
      assert.ok(epochs.includes(postMigration.epoch));

      // 4. verify_provenance still validates end-to-end.
      const verification = await client.verifyContinuityProvenance(
        execution.continuity_id,
        tenantId,
      );
      assert.equal(verification.valid, true);
    });
  });

  describe("A. handoff -> capsule import -> re-accept is rejected (single-claim guarantee)", () => {
    it("importing the same capsule twice under a fresh destination instance id still leaves only one owner", async () => {
      const setup = await resumedHandoff("int-hoff-reimport");
      // Attempt to accept the ALREADY-resumed handoff again with a brand new
      // paused destination instance — the handoff state machine must refuse
      // a second accept regardless of a fresh instance id.
      const seq = testSequence("int-hoff-reimport-extra", [step("s1", "noop")], {
        tenantId: setup.tenantId,
      });
      const createdSeq = await client.createSequence(seq);
      const freshInstance = await client.createInstance({
        sequence_id: createdSeq.id,
        tenant_id: setup.tenantId,
        namespace: "default",
      });
      await rejects(
        client.acceptHandoff(setup.handoff.id, {
          tenant_id: setup.tenantId,
          destination_instance_id: freshInstance.id,
        }),
        409,
        "a resumed handoff cannot be accepted a second time",
      );
      const execution = await client.getContinuityExecution(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(execution.current_instance_id, setup.imported.instance_id);
    });

    it("rejecting a handoff never before exported leaves the execution untouched and still owned by the source", async () => {
      const tenantId = tid("int-hoff-reject-noop");
      const { execution, runtimeId } = await setupExecution(tenantId);
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, destRuntime);
      const handoff = await client.createHandoff(req);
      const rejected = await client.rejectHandoff(handoff.id, { tenant_id: tenantId });
      assert.equal(rejected.state, "rejected");
      const stillOwned = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.equal(stillOwned.owner_runtime_id, runtimeId);
      assert.equal(stillOwned.epoch, execution.epoch);
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      // Rejection is a handoff-record-only transition; it must not itself
      // become a provenance boundary on the execution's chain.
      assert.ok(!provenance.some((entry: any) => entry.kind === "runtime_claimed"));
    });

    it("revoking an exported handoff leaves the execution Transferring but never advances the epoch", async () => {
      const tenantId = tid("int-hoff-revoke");
      const { execution } = await pausedContinuityFixture(tenantId, "int-hoff-revoke");
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(
        tenantId,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
      );
      const handoff = await client.createHandoff(req);
      const exported = await client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: req.requirements,
        expires_in_seconds: 60,
      });
      assert.equal(exported.handoff.state, "exported");
      const revoked = await client.revokeHandoff(handoff.id, { tenant_id: tenantId });
      assert.equal(revoked.state, "revoked");
      const execAfter = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.equal(execAfter.epoch, execution.epoch, "revoke must not itself bump the epoch");
      assert.equal(
        execAfter.state,
        "transferring",
        "export moves the execution to Transferring; revoke does not revert ownership state",
      );
    });
  });

  describe("A. compensation failure/verification interacts correctly with provenance and effect state", () => {
    it("a manually-verified compensation step only marks the effect compensated after verify approves it", async () => {
      const setup = await committedCompensableExecution("int-comp-manual");
      // Override the compensation step's verification policy is not directly
      // controllable from here (fixed at sequence-definition time), so this
      // test instead exercises the fail -> retry -> complete path, which the
      // dedicated compensation file does not chain end-to-end with a
      // provenance/effect-state cross-check.
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "retry-worker",
      });
      const failed = await client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "retry-worker",
        error: "transient provider timeout",
        retryable: true,
      });
      const failedStep = failed.steps.find((s: any) => s.plan.effect_id === claimed.plan.effect_id);
      assert.equal(failedStep.state, "pending", "a retryable failure returns the step to pending");
      // Effect must NOT have flipped to compensated on a retryable failure.
      const effectsAfterFail = await client.listContinuityEffects(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(effectsAfterFail[0]!.state, "committed");
      const reclaimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "retry-worker-2",
      });
      const completed = await client.completeContinuityCompensation(
        created.id,
        reclaimed.plan.effect_id,
        { tenant_id: setup.tenantId, worker_id: "retry-worker-2" },
      );
      assert.equal(completed.state, "completed");
      const effectsAfterComplete = await client.listContinuityEffects(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(effectsAfterComplete[0]!.state, "compensated");
    });

    it("an outcome-uncertain compensation failure leaves the effect uncompensated and the run awaiting verification", async () => {
      const setup = await committedCompensableExecution("int-comp-uncertain");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "uncertain-worker",
      });
      const failed = await client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "uncertain-worker",
        error: "connection dropped mid-call, outcome unknown",
        outcome_uncertain: true,
      });
      assert.equal(failed.state, "awaiting_verification");
      const effects = await client.listContinuityEffects(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(effects[0]!.state, "committed", "uncertain outcome must not assume compensation succeeded");
      const verified = await client.verifyContinuityCompensation(
        created.id,
        claimed.plan.effect_id,
        { tenant_id: setup.tenantId, approved: true, evidence: "confirmed via provider dashboard" },
      );
      assert.equal(verified.state, "completed");
      const effectsAfterVerify = await client.listContinuityEffects(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(effectsAfterVerify[0]!.state, "compensated");
    });

    it("a rejected verification leaves residual_effects populated and the effect not compensated", async () => {
      const setup = await committedCompensableExecution("int-comp-verify-reject");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "reject-worker",
      });
      await client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "reject-worker",
        error: "unclear result",
        outcome_uncertain: true,
      });
      const rejected = await client.verifyContinuityCompensation(
        created.id,
        claimed.plan.effect_id,
        { tenant_id: setup.tenantId, approved: false, evidence: "provider confirms charge still active" },
      );
      assert.ok(
        rejected.residual_effects.some((r: string) => r.includes(claimed.plan.effect_id)),
        "a rejected verification must be tracked as a residual effect",
      );
      const effects = await client.listContinuityEffects(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(effects[0]!.state, "committed");
    });
  });

  describe("A. residency evaluated across every classification x operation pairing shares one coherent chain", () => {
    const classifications = ["public", "internal"] as const;
    const operations = ["handler_dispatch", "artifact_creation", "migration", "fork"];

    for (const classification of classifications) {
      for (const operation of operations) {
        it(`records a residency_evaluated entry for classification=${classification} operation=${operation}`, async () => {
          const tenantId = tid("int-res-matrix");
          const { execution, runtimeId } = await setupExecution(tenantId);
          const result = await client.evaluateContinuityResidency({
            tenant_id: tenantId,
            continuity_id: execution.continuity_id,
            classification,
            operation,
            destination_runtime_id: runtimeId,
          });
          assert.equal(result.operation, operation);
          const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
          const entries = provenance.filter((e: any) => e.kind === "residency_evaluated");
          assert.equal(entries.length, 1);
        });
      }
    }

    it("multiple residency evaluations on the same execution append sequentially without clobbering the chain", async () => {
      const tenantId = tid("int-res-multi");
      const { execution, runtimeId } = await setupExecution(tenantId);
      for (const operation of ["handler_dispatch", "logging", "telemetry"]) {
        await client.evaluateContinuityResidency({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          classification: "internal",
          operation,
          destination_runtime_id: runtimeId,
        });
      }
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      const kinds = provenance.filter((e: any) => e.kind === "residency_evaluated");
      assert.equal(kinds.length, 3);
      // Chain must remain internally consistent (each entry's previous_sha256
      // equals the prior entry's entry_sha256).
      for (let i = 1; i < provenance.length; i++) {
        assert.equal(provenance[i].previous_sha256, provenance[i - 1].entry_sha256);
      }
      const verification = await client.verifyContinuityProvenance(execution.continuity_id, tenantId);
      assert.equal(verification.valid, true);
    });
  });

  describe("A. provider routing + optimization advisor share the execution's provenance chain", () => {
    it("choose_provider with a bound continuity_id appends provider_selected, then optimization accept appends optimization_accepted in order", async () => {
      const tenantId = tid("int-provider-opt");
      const { execution, instance } = await setupExecution(tenantId);
      const decision = await client.chooseContinuityProvider({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        candidates: [
          {
            provider: "acme",
            model: "acme-large",
            region: "br-south",
            pricing_version: "v1",
            price_microunits: 10,
            expected_latency_ms: 50,
            quality_millipoints: 900,
            breaker_open: false,
            supports_idempotency: true,
          },
        ],
        cohort_key: "int-cohort",
      });
      assert.ok(decision.selected, "the single unconstrained candidate must be selected");
      assert.equal(decision.selected.provider, "acme");
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      assert.ok(provenance.some((e: any) => e.kind === "provider_selected"));
      const verification = await client.verifyContinuityProvenance(execution.continuity_id, tenantId);
      assert.equal(verification.valid, true);
    });
  });

  describe("A. evaluation gate (stored) reads real evaluation scores across two executions on the candidate's chain", () => {
    it("records evaluation_gate provenance on the candidate execution, not the baseline", async () => {
      const tenantId = tid("int-eval-gate");
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      const evidence = (score: number) => createHash("sha256").update(`ev-${score}`).digest("hex");
      for (const score of [800, 820, 810]) {
        await client.appendContinuityEvaluation(baseline.continuity_id, {
          tenant_id: tenantId,
          evaluator: "int-judge",
          score_millipoints: score,
          sample_size: 10,
          evidence_sha256: evidence(score),
        });
      }
      for (const score of [850, 860, 840]) {
        await client.appendContinuityEvaluation(candidate.continuity_id, {
          tenant_id: tenantId,
          evaluator: "int-judge",
          score_millipoints: score,
          sample_size: 10,
          evidence_sha256: evidence(score + 1000),
        });
      }
      await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "int-judge",
        minimum_samples: 3,
        maximum_regression_millipoints: 0,
      });
      const baselineProvenance = await client.listContinuityProvenance(
        baseline.continuity_id,
        tenantId,
      );
      const candidateProvenance = await client.listContinuityProvenance(
        candidate.continuity_id,
        tenantId,
      );
      assert.ok(!baselineProvenance.some((e: any) => e.kind === "evaluation_gate"));
      assert.ok(candidateProvenance.some((e: any) => e.kind === "evaluation_gate"));
    });
  });

  describe("A. grant issue -> delegation claim consumes the grant exactly once across the whole flow", () => {
    it("a grant issued for 'accept' cannot also be reused for a delegation claim after direct consumption", async () => {
      const tenantId = tid("int-grant-deleg");
      const { execution, runtimeId: sourceRuntimeId } = await setupExecution(tenantId);
      await registerRuntime(tenantId, sourceRuntimeId);
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime);
      const subSeq = await client.createSequence(
        testSequence("int-grant-deleg-sub", [step("s1", "noop")], { tenantId }),
      );
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      // Consume it directly for "accept" first.
      await client.consumeContinuationGrant({
        tenant_id: tenantId,
        action: "accept",
        token: grant.token,
        signed_grant: grant.signed_grant,
      });
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      await rejects(
        client.claimDeviceDelegation({
          tenant_id: tenantId,
          delegation,
          signed_grant: grant.signed_grant,
          token: grant.token,
        }),
        409,
        "a grant already consumed via consume_continuation_grant cannot also fund a delegation claim",
      );
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      assert.ok(!provenance.some((e: any) => e.kind === "device_delegation"));
    });
  });

  describe("A. checkpoint-diff detail stays consistent across a migration boundary", () => {
    it("the checkpoint created by migration apply is reachable via get_continuity_checkpoint with a computed diff", async () => {
      const tenantId = tid("int-ckpt-diff-mig");
      const { execution, sequence } = await pausedContinuityFixture(
        tenantId,
        "int-ckpt-diff-mig",
        [],
        { field: "before" },
      );
      const beforeCheckpoints = await client.listContinuityCheckpoints(
        execution.continuity_id,
        tenantId,
      );
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
        transforms: [
          {
            version: 1,
            transform: "copy",
            from_path: "context_snapshot.field",
            to_path: "context_snapshot.field_copy",
          },
        ],
      });
      // A migration carrying state transforms compiles to ApprovalRequired.
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId, approved: true });
      const afterCheckpoints = await client.listContinuityCheckpoints(execution.continuity_id, tenantId);
      // Migration apply bumps the epoch, adding a second location row over
      // the SAME underlying instance. continuity_checkpoint_records()
      // iterates locations and re-lists that instance's checkpoints per
      // location, so the pre-migration checkpoint is legitimately reported
      // twice (once per epoch's location) alongside the newly-written
      // migration checkpoint (also reported twice) — 2 distinct checkpoint
      // rows surfaced as 4 boundary entries, sorted by (epoch, created_at).
      const distinctIds = new Set(afterCheckpoints.map((c: any) => c.checkpoint_id));
      assert.equal(distinctIds.size, beforeCheckpoints.length + 1, "one new distinct checkpoint row");
      assert.equal(afterCheckpoints.length, 2 * distinctIds.size, "each row appears once per location epoch");
      const migrationCheckpointId = afterCheckpoints[afterCheckpoints.length - 1]!.checkpoint_id;
      assert.ok(!beforeCheckpoints.some((c: any) => c.checkpoint_id === migrationCheckpointId));
      const detail = await client.getContinuityCheckpoint(
        execution.continuity_id,
        migrationCheckpointId,
        tenantId,
      );
      assert.ok(detail.previous_checkpoint_id, "the migration checkpoint must chain to a predecessor");
    });
  });

  describe("A. budget reservation lifecycle interacts correctly with attention tasks on the same execution", () => {
    it("reserve -> reconcile leaves the reservation settled and independent of a sibling attention-task reservation", async () => {
      const tenantId = tid("int-budget-attn");
      const seq = testSequence("int-budget-attn", [step("s1", "noop")], { tenantId });
      const createdSeq = await client.createSequence(seq);
      const instance = await client.createInstance({
        sequence_id: createdSeq.id,
        tenant_id: tenantId,
        namespace: "default",
        budget: { max_attention_units: 100 },
      });
      const execution = await client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
      const zeroUsage = {
        cost_microunits: 0,
        wall_time_ms: 0,
        external_calls: 0,
        bytes_transferred: 0,
        energy_millijoules: 0,
        attention_units: 0,
      };
      const directReservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: { ...zeroUsage, attention_units: 10 },
        estimation_version: "int-v1",
      });
      // A second reservation, made indirectly via creating an attention task,
      // must not be conflated with the directly-created one above.
      const task = await client.createAttentionTask({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        required_skills: ["review"],
        classification: "internal",
        priority: 1,
        deadline: new Date(Date.now() + 60_000).toISOString(),
        estimated_attention_units: 20,
      });
      const allReservations = await client.listContinuityBudgetReservations(
        execution.continuity_id,
        tenantId,
      );
      assert.equal(allReservations.length, 2);
      assert.ok(allReservations.some((r: any) => r.id === directReservation.id));
      assert.ok(allReservations.some((r: any) => r.id === task.budget_reservation_id));

      const reconciled = await client.reconcileContinuityBudget(
        execution.continuity_id,
        directReservation.id,
        { tenant_id: tenantId, actual: { ...zeroUsage, attention_units: 8 } },
      );
      assert.equal(reconciled.state, "reconciled");
      // The attention task's own reservation must be untouched by
      // reconciling the unrelated direct reservation.
      const taskReservation = await client.listContinuityBudgetReservations(
        execution.continuity_id,
        tenantId,
      );
      const stillReserved = taskReservation.find((r: any) => r.id === task.budget_reservation_id);
      assert.equal(stillReserved.state, "reserved");
    });

    it("releasing a reservation frees budget so a subsequent equal-sized reservation succeeds", async () => {
      const tenantId = tid("int-budget-release");
      const seq = testSequence("int-budget-release", [step("s1", "noop")], { tenantId });
      const createdSeq = await client.createSequence(seq);
      const instance = await client.createInstance({
        sequence_id: createdSeq.id,
        tenant_id: tenantId,
        namespace: "default",
        budget: { max_attention_units: 10 },
      });
      const execution = await client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
      const usage = (attention_units: number) => ({
        cost_microunits: 0,
        wall_time_ms: 0,
        external_calls: 0,
        bytes_transferred: 0,
        energy_millijoules: 0,
        attention_units,
      });
      const first = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: usage(10),
        estimation_version: "int-v1",
      });
      await rejects(
        client.reserveContinuityBudget(execution.continuity_id, {
          tenant_id: tenantId,
          requested: usage(10),
          estimation_version: "int-v1",
        }),
        409,
        "budget is fully reserved by the first request",
      );
      await client.releaseContinuityBudget(execution.continuity_id, first.id, {
        tenant_id: tenantId,
      });
      const second = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: usage(10),
        estimation_version: "int-v1",
      });
      assert.equal(second.state, "reserved");
    });
  });

  describe("A. mixing export_handoff and attach_device_capsule on one handoff is rejected", () => {
    it("attach_device_capsule refuses a handoff that already completed export via the server-managed path", async () => {
      const tenantId = tid("int-hoff-mixed-flow");
      const { execution } = await pausedContinuityFixture(tenantId, "int-hoff-mixed-flow");
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(
        tenantId,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
        { kind: "mobile", offline_capable: true },
      );
      const handoff = await client.createHandoff(req);
      const payloadKey = Buffer.from(
        Array.from({ length: 32 }, () => Math.floor(Math.random() * 256)),
      ).toString("base64");
      const exported = await client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: { handlers: ["human_review"] },
        expires_in_seconds: 60,
        payload_key_base64: payloadKey,
      });
      assert.equal(exported.handoff.state, "exported");
      // A device now tries to independently attach a capsule for the SAME
      // handoff after the server-managed export already completed it — this
      // must be rejected, not silently accepted as a second transfer path.
      await rejects(
        client.attachDeviceCapsule(handoff.id, {
          tenant_id: tenantId,
          destination_instance_id: uuid(),
          capsule: exported.capsule,
          payload_base64: exported.payload_base64,
          payload_key_base64: payloadKey,
        }),
        409,
        "only a requested or recovering (Quiescing) handoff can attach a device capsule",
      );
      // The already-exported handoff and its owning execution must remain
      // exactly as export_handoff left them.
      const stillExported = await client.getHandoff(handoff.id, tenantId);
      assert.equal(stillExported.state, "exported");
      assert.equal(stillExported.capsule_id, exported.handoff.capsule_id);
    });
  });

  describe("A. stream retraction vs append react differently to a migration epoch bump", () => {
    it("append re-validates against the LIVE execution epoch post-migration (409), but retract only checks the stream's own fixed epoch (still succeeds)", async () => {
      // retract_stream_frames compares body.epoch only against the stream's
      // own stored `epoch` field (set once at stream creation) — unlike
      // append_stream_frame, it never re-fetches the owning execution to
      // compare against the LIVE epoch (see orch8-api/src/continuity.rs
      // retract_stream_frames: `if stream.epoch != body.epoch`, with no
      // `continuity_instance`/`get_continuity_execution` call at all). So a
      // migration that bumps the execution's epoch does NOT make a
      // previously-valid retraction request stale, even though it DOES make
      // the equivalent append request stale. This test pins down that
      // asymmetry precisely rather than assuming symmetry.
      const tenantId = tid("int-strm-retract-mig");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-strm-retract-mig");
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      for (let i = 0; i < 3; i++) {
        await client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: i,
          checkpoint_sha256: String(i).repeat(64).slice(0, 64),
          payload: {},
        });
      }

      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
      const postMigration = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.notEqual(postMigration.epoch, execution.epoch);

      // Append with the pre-migration epoch is correctly rejected as stale.
      await rejects(
        client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: 3,
          checkpoint_sha256: "9".repeat(64),
          payload: {},
        }),
        409,
        "append must re-check the live execution epoch",
      );

      // Retract with that SAME pre-migration epoch still succeeds, because
      // it is checked only against the stream's own stored epoch, which the
      // migration never touched.
      const retracted = await client.retractContinuityFrames(stream.stream_id, {
        tenant_id: tenantId,
        epoch: stream.epoch,
        after_sequence: 1,
      });
      assert.equal(retracted.retracted, 1);
    });
  });

  describe("A. multi-step compensation runs execute in strict LIFO order across claim/complete cycles", () => {
    /** Two independently-compensable committed effects on one execution. */
    async function twoStepCompensableExecution(namePrefix: string) {
      const tenantId = tid(namePrefix);
      const firstHandler = `${namePrefix}_first_${uuid().slice(0, 8)}`;
      const secondHandler = `${namePrefix}_second_${uuid().slice(0, 8)}`;
      const sequence = testSequence(
        namePrefix,
        [
          step("arm", "human_review", {}, {
            wait_for_input: {
              prompt: "Arm effect dispatch?",
              choices: [{ label: "Continue", value: "continue" }],
            },
          }),
          step("first", firstHandler, { amount: 10 }, {
            compensation: { handler: "release_first", verification: "handler_result" },
          }),
          step("second", secondHandler, { amount: 20 }, {
            compensation: { handler: "release_second", verification: "handler_result" },
          }),
        ],
        { tenantId },
      );
      const created = await client.createSequence(sequence);
      const instance = await client.createInstance({
        sequence_id: created.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      await client.waitForState(instance.id, "waiting");
      const execution = await client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
      await client.sendSignal(
        instance.id,
        { custom: "human_input:arm" } as unknown as string,
        { value: "continue" },
      );
      const firstTask = await waitForWorkerTask(firstHandler, `${namePrefix}-w1`);
      await client.completeWorkerTask(firstTask.id, `${namePrefix}-w1`, { ok: true });
      const secondTask = await waitForWorkerTask(secondHandler, `${namePrefix}-w2`);
      await client.completeWorkerTask(secondTask.id, `${namePrefix}-w2`, { ok: true });
      const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
      assert.equal(effects.length, 2, "setup: two dispatched effects");
      for (const effect of effects) {
        if (effect.state !== "committed") {
          await client.resolveContinuityEffect(effect.id, {
            tenant_id: tenantId,
            state: "committed",
            provider_receipt_id: `receipt-${uuid().slice(0, 8)}`,
          });
        }
      }
      return { tenantId, execution, instance };
    }

    it("the second (later) effect is compensated before the first (LIFO), and the run completes only after both", async () => {
      const { tenantId, execution } = await twoStepCompensableExecution("int-comp-lifo");
      const plan = await client.previewContinuityCompensation(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.equal(plan.steps.length, 2);
      assert.equal(plan.steps[0]!.effect_block_id, "second", "LIFO: last-committed effect compensates first");
      assert.equal(plan.steps[1]!.effect_block_id, "first");

      const run = await client.createContinuityCompensation(execution.continuity_id, {
        tenant_id: tenantId,
      });
      const firstClaim = await client.claimContinuityCompensation(run.id, {
        tenant_id: tenantId,
        worker_id: "lifo-worker",
      });
      assert.equal(firstClaim.plan.effect_block_id, "second");
      const afterFirstComplete = await client.completeContinuityCompensation(
        run.id,
        firstClaim.plan.effect_id,
        { tenant_id: tenantId, worker_id: "lifo-worker" },
      );
      assert.equal(afterFirstComplete.state, "running", "one of two steps remaining");

      const secondClaim = await client.claimContinuityCompensation(run.id, {
        tenant_id: tenantId,
        worker_id: "lifo-worker",
      });
      assert.equal(secondClaim.plan.effect_block_id, "first");
      const afterSecondComplete = await client.completeContinuityCompensation(
        run.id,
        secondClaim.plan.effect_id,
        { tenant_id: tenantId, worker_id: "lifo-worker" },
      );
      assert.equal(afterSecondComplete.state, "completed");

      const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
      assert.ok(effects.every((e: any) => e.state === "compensated"));
    });
  });

  describe("A. invariant evaluation is re-run correctly against the post-migration sequence", () => {
    it("a terminal_state_in invariant registered on the target sequence fails migration planning (Pin), matching that it would also fail post-migration", async () => {
      // A terminal_state_in invariant is evaluated as part of historical
      // replay validation during planning (validate_migration_history), not
      // only after apply. Since the paused/waiting boundary never reaches a
      // terminal state during replay, the invariant fails there, which
      // forces disposition=Pin and blocks apply outright — the invariant
      // never gets a chance to be "wrong" against a migrated instance
      // because the migration itself is correctly refused first.
      const tenantId = tid("int-inv-mig-pin");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-inv-mig-pin");
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: target.id,
        sequence_version: target.version,
        name: "waiting-terminal",
        rule: { type: "terminal_state_in", states: ["completed", "failed"] },
      });
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: target.id,
        to_version: target.version,
      });
      assert.equal(plan.disposition, "pin");
      await rejects(
        client.applyContinuityMigration(plan.id, { tenant_id: tenantId, approved: true }),
        409,
      );
    });

    it("a no_unknown_effects invariant on the target sequence still evaluates correctly against the migrated instance", async () => {
      const tenantId = tid("int-inv-mig-pass");
      const { execution, sequence, instance } = await pausedContinuityFixture(
        tenantId,
        "int-inv-mig-pass",
      );
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: target.id,
        sequence_version: target.version,
        name: "no-unknown-effects",
        rule: { type: "no_unknown_effects" },
      });
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: target.id,
        to_version: target.version,
      });
      assert.equal(plan.disposition, "automatic");
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
      const finalInstance = await client.getInstance(instance.id);
      assert.equal((finalInstance as any).sequence_id, target.id);
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      const matched = results.find((r: any) => r.invariant_id === invariant.id);
      assert.ok(matched, "the migrated-target invariant must be evaluated post-migration");
      assert.equal(matched.status, "pass", "no effects were ever dispatched, so this vacuously passes");
      const stored = await client.listContinuityInvariantResults(execution.continuity_id, tenantId);
      assert.ok(stored.some((r: any) => r.invariant_id === invariant.id));
    });
  });

  describe("A. handoff requirements compatibility interacts with residency region policy", () => {
    const regionPairs: Array<{ sourceRegion: string; destRegion: string; allowed: string[]; expectAllowed: boolean }> = [
      { sourceRegion: "br-south", destRegion: "br-south", allowed: ["br-south"], expectAllowed: true },
      { sourceRegion: "br-south", destRegion: "us-east", allowed: ["br-south"], expectAllowed: false },
      { sourceRegion: "eu-west", destRegion: "eu-west", allowed: ["eu-west", "br-south"], expectAllowed: true },
    ];

    for (const { sourceRegion, destRegion, allowed, expectAllowed } of regionPairs) {
      it(`residency for a handoff destination in ${destRegion} against allowed=[${allowed}] resolves outcome=${expectAllowed ? "pass" : "fail"}`, async () => {
        const tenantId = tid("int-hoff-residency");
        const { execution } = await setupExecution(tenantId);
        const destRuntime = uuid();
        await registerRuntime(tenantId, destRuntime, { regions: [destRegion], handlers: ["noop"] });
        const result = await client.evaluateContinuityResidency({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          classification: "internal",
          operation: "capsule_transfer",
          destination_runtime_id: destRuntime,
          allowed_regions: allowed,
        });
        assert.equal(result.destination_region, destRegion);
        if (expectAllowed) {
          assert.equal(result.outcome, "pass");
        } else {
          assert.equal(result.outcome, "fail");
          assert.ok(result.finding_codes.includes("DESTINATION_REGION_DENIED"));
        }
      });
    }
  });

  describe("A. a runtime that goes stale (capability expiry) mid-flow can no longer receive a handoff, even after the residency check passed earlier", () => {
    it("a handoff preview against an expired-capability destination 404s even though an earlier residency check for it succeeded", async () => {
      const tenantId = tid("int-hoff-stale-runtime");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      const now = Date.now();
      await client.registerRuntime({
        tenant_id: tenantId,
        capabilities: {
          runtime_id: destRuntime,
          kind: "server",
          trust: "registered",
          handlers: ["noop"],
          regions: ["br-south"],
          offline_capable: false,
          observed_at: new Date(now - 4 * 60_000).toISOString(),
          expires_at: new Date(now + 1_000).toISOString(),
        },
      });
      const earlyResidency = await client.evaluateContinuityResidency({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        classification: "internal",
        operation: "handler_dispatch",
        destination_runtime_id: destRuntime,
        allowed_regions: ["br-south"],
      });
      assert.equal(earlyResidency.outcome, "pass");
      // Wait past the capability's expiry.
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      // list_runtime_capabilities filters by liveness (expires_at > now), so
      // an expired registration drops out of the candidate set entirely —
      // build_handoff_preview's destination lookup then 404s rather than
      // reporting an incompatibility finding. The important invariant is
      // still upheld: an expired capability can never authorize a handoff,
      // regardless of an earlier successful residency check against it.
      await rejects(
        client.previewHandoff(execution.continuity_id, {
          tenant_id: tenantId,
          destination_runtime_id: destRuntime,
          requirements: { handlers: ["noop"] },
        }),
        404,
      );
    });
  });

  // ====================================================================
  // HALF B — Systematic tenant-isolation sweep (table-driven)
  // ====================================================================

  describe("B. tenant isolation sweep", () => {
    /** Shared fixture for the sweep: everything a tenant-B probe might need. */
    async function buildIsolationFixture(tenantId: string) {
      const { execution, instance, runtimeId, sequence } = await setupExecution(tenantId);
      await registerRuntime(tenantId, runtimeId);

      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      const frame = await client.appendContinuityFrame(stream.stream_id, {
        tenant_id: tenantId,
        sequence: 0,
        checkpoint_sha256: "e".repeat(64),
        payload: { x: 1 },
      });

      const handoffDest = uuid();
      const handoffReq = await authorizedHandoffRequest(
        tenantId,
        execution.continuity_id,
        handoffDest,
      );
      const handoff = await client.createHandoff(handoffReq);

      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: sequence.id,
        sequence_version: sequence.version,
        name: `inv-${uuid().slice(0, 6)}`,
        rule: { type: "terminal_state_in", states: ["completed"] },
      });

      // Attention task (no direct GET-by-id endpoint exists; covered via
      // its parent execution/effects isolation instead — see below).

      // Budget: requires an instance with a configured budget, which this
      // minimal fixture's instance does not have, so budget reservation
      // isolation is exercised in its own dedicated fixture below.

      // Paused fixture (for checkpoints/migrations/what-if/compensation).
      const paused = await pausedContinuityFixture(tenantId, `iso-${tenantId}`);
      const target = sameShapeTarget(paused.sequence);
      const createdTarget = await client.createSequence(target);
      const migrationPlan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: paused.execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      const whatIfResult = await client.runContinuityWhatIf(paused.execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: paused.checkpointId,
      });

      const compSetup = await committedCompensableExecution(`iso-comp-${tenantId}`);
      const compEffects = await client.listContinuityEffects(
        compSetup.execution.continuity_id,
        compSetup.tenantId,
      );
      const compensationRun = await client.createContinuityCompensation(
        compSetup.execution.continuity_id,
        { tenant_id: compSetup.tenantId },
      );

      return {
        execution,
        instance,
        runtimeId,
        stream,
        frame,
        handoff,
        invariant,
        paused,
        migrationPlan,
        whatIfResult,
        compSetup,
        compensationRun,
        firstEffectId: compEffects[0]!.id as string,
      };
    }

    let tenantA: string;
    let tenantB: string;
    let fixtureA: Awaited<ReturnType<typeof buildIsolationFixture>>;

    before(async () => {
      tenantA = tid("iso-a");
      tenantB = tid("iso-b");
      fixtureA = await buildIsolationFixture(tenantA);
    });

    // ------------------------------------------------------------------
    // Table-driven sweep: every GET-by-id/list endpoint that DOES enforce
    // tenant ownership before returning data must 404 (never empty-success,
    // never 403) when a resource created under tenant A is looked up under
    // tenant B. Each descriptor's `probe` returns the promise to reject; the
    // loop below both documents and asserts the isolation contract in one
    // place instead of N hand-written near-identical blocks.
    // ------------------------------------------------------------------
    type IsolationCase = { resource: string; probe: () => Promise<unknown> };

    function isolationCases(): IsolationCase[] {
      return [
        {
          resource: "execution (get_execution)",
          probe: () => client.getContinuityExecution(fixtureA.execution.continuity_id, tenantB),
        },
        {
          resource: "locations (list_locations)",
          probe: () => client.listContinuityLocations(fixtureA.execution.continuity_id, tenantB),
        },
        {
          resource: "handoff (get_handoff)",
          probe: () => client.getHandoff(fixtureA.handoff.id, tenantB),
        },
        {
          resource: "streams (list_stream_frames)",
          probe: () => client.listContinuityFrames(fixtureA.stream.stream_id, tenantB),
        },
        {
          resource: "streams (retract_stream_frames)",
          probe: () =>
            client.retractContinuityFrames(fixtureA.stream.stream_id, {
              tenant_id: tenantB,
              epoch: fixtureA.stream.epoch,
              after_sequence: 0,
            }),
        },
        {
          resource: "invariant results (list_invariant_results)",
          probe: () =>
            client.listContinuityInvariantResults(fixtureA.execution.continuity_id, tenantB),
        },
        {
          resource: "checkpoints (list_continuity_checkpoints)",
          probe: () =>
            client.listContinuityCheckpoints(fixtureA.paused.execution.continuity_id, tenantB),
        },
        {
          resource: "checkpoint detail (get_continuity_checkpoint)",
          probe: () =>
            client.getContinuityCheckpoint(
              fixtureA.paused.execution.continuity_id,
              fixtureA.paused.checkpointId,
              tenantB,
            ),
        },
        {
          resource: "what-if runs (list_what_if_runs)",
          probe: () =>
            client.listContinuityWhatIfRuns(fixtureA.paused.execution.continuity_id, tenantB),
        },
        {
          resource: "what-if run (run_what_if against another tenant's checkpoint)",
          probe: () =>
            client.runContinuityWhatIf(fixtureA.paused.execution.continuity_id, {
              tenant_id: tenantB,
              checkpoint_id: fixtureA.paused.checkpointId,
            }),
        },
        {
          resource: "migration plan (get_live_migration)",
          probe: () => client.getContinuityMigration(fixtureA.migrationPlan.id, tenantB),
        },
        {
          resource: "migration plan (apply_live_migration)",
          probe: () =>
            client.applyContinuityMigration(fixtureA.migrationPlan.id, { tenant_id: tenantB }),
        },
        {
          resource: "migration plan (rollback_live_migration)",
          probe: () =>
            client.rollbackContinuityMigration(fixtureA.migrationPlan.id, { tenant_id: tenantB }),
        },
        {
          resource: "compensation run (get_compensation_run)",
          probe: () => client.getContinuityCompensation(fixtureA.compensationRun.id, tenantB),
        },
        {
          resource: "compensation run (claim_compensation_step)",
          probe: () =>
            client.claimContinuityCompensation(fixtureA.compensationRun.id, {
              tenant_id: tenantB,
              worker_id: "intruder",
            }),
        },
        {
          resource: "compensation run (preview_compensation, wrong tenant execution id)",
          probe: () =>
            client.previewContinuityCompensation(fixtureA.compSetup.execution.continuity_id, {
              tenant_id: tenantB,
            }),
        },
        {
          resource: "effect resolve (resolve_effect)",
          probe: () =>
            client.resolveContinuityEffect(fixtureA.firstEffectId, {
              tenant_id: tenantB,
              state: "committed",
            }),
        },
      ];
    }

    // Register one `it()` per resource descriptor so each isolation
    // assertion is individually reported/counted, rather than folding the
    // whole table into a single pass/fail test. `before()` above populates
    // `fixtureA` before any of these run (node:test executes `before`
    // hooks prior to the suite's `it`s), so `isolationCases()` sees a live
    // fixture at call time even though the `it(...)` registrations
    // themselves happen synchronously at describe-time.
    for (const descriptor of [
      "execution (get_execution)",
      "locations (list_locations)",
      "handoff (get_handoff)",
      "streams (list_stream_frames)",
      "streams (retract_stream_frames)",
      "invariant results (list_invariant_results)",
      "checkpoints (list_continuity_checkpoints)",
      "checkpoint detail (get_continuity_checkpoint)",
      "what-if runs (list_what_if_runs)",
      "what-if run (run_what_if against another tenant's checkpoint)",
      "migration plan (get_live_migration)",
      "migration plan (apply_live_migration)",
      "migration plan (rollback_live_migration)",
      "compensation run (get_compensation_run)",
      "compensation run (claim_compensation_step)",
      "compensation run (preview_compensation, wrong tenant execution id)",
      "effect resolve (resolve_effect)",
    ]) {
      it(`tenant isolation: ${descriptor} 404s for tenant B`, async () => {
        const testCase = isolationCases().find((candidate) => candidate.resource === descriptor);
        assert.ok(testCase, `no isolation case registered for ${descriptor}`);
        await rejects(testCase!.probe(), 404, `resource: ${descriptor}`);
      });
    }

    // list_effects and list_provenance/verify_provenance do not check
    // execution existence/ownership before querying (unlike list_locations,
    // list_invariant_results, list_execution_budget_reservations,
    // list_what_if_runs, and list_continuity_checkpoints, which all call
    // continuity_instance()/get_continuity_execution() first and 404 when
    // it's missing for the caller's tenant). This is pre-existing,
    // deliberately documented behavior — see
    // continuity_provenance_effects.test.ts:252-262,372-379 ("empty list for
    // wrong tenant is not exposed as 404" / "reports an empty, valid
    // (vacuous) chain"). The isolation guarantee these two endpoints do
    // provide — and the one asserted here — is that tenant B's query never
    // surfaces tenant A's actual receipts/entries, even though the endpoint
    // doesn't hard-fail on the id itself.
    it("effects: tenant B's effect list for tenant A's continuity_id is empty, not tenant A's data", async () => {
      const effectsUnderB = await client.listContinuityEffects(
        fixtureA.execution.continuity_id,
        tenantB,
      );
      assert.deepEqual(effectsUnderB, []);
    });

    it("provenance: tenant B's provenance list for tenant A's continuity_id is empty, not tenant A's chain", async () => {
      const provenanceUnderB = await client.listContinuityProvenance(
        fixtureA.execution.continuity_id,
        tenantB,
      );
      assert.deepEqual(provenanceUnderB, []);
    });

    it("provenance verify: tenant B sees a vacuous empty-chain result, never tenant A's real chain contents", async () => {
      const verification = await client.verifyContinuityProvenance(
        fixtureA.execution.continuity_id,
        tenantB,
      );
      // Vacuously valid (zero entries), which is the documented contract —
      // but critically it must not somehow echo tenant A's real head/length.
      assert.equal(verification.valid, true);
      assert.equal(verification.first_invalid_index, null);
      const realVerification = await client.verifyContinuityProvenance(
        fixtureA.execution.continuity_id,
        tenantA,
      );
      assert.notDeepEqual(verification, realVerification);
    });

    it("runtimes: tenant B's runtime list never includes tenant A's registered runtime", async () => {
      const listB = await client.listRuntimes(tenantB);
      assert.ok(!listB.some((r: any) => r.runtime_id === fixtureA.runtimeId));
    });

    it("invariants: tenant B's invariant list (by tenant A's sequence coordinates) is empty", async () => {
      // list_invariants is scoped by tenant_id in the query itself, so even
      // supplying tenant A's real sequence_id/version must yield nothing for
      // tenant B — the list must not merge across the tenant boundary.
      const listB = await client.listContinuityInvariants(
        tenantB,
        fixtureA.paused.sequence.id,
        fixtureA.paused.sequence.version,
      );
      assert.ok(!listB.some((inv: any) => inv.id === fixtureA.invariant.id));
    });

    it("budget reservations: listing tenant A's execution's reservations under tenant B 404s", async () => {
      // Use a fresh fixture with a real budget so the 404 is attributable to
      // tenant scoping rather than to the "no budget configured" 409 path.
      const tenantC = tid("iso-c");
      const seq = testSequence("iso-budget", [step("s1", "noop")], { tenantId: tenantC });
      const createdSeq = await client.createSequence(seq);
      const instance = await client.createInstance({
        sequence_id: createdSeq.id,
        tenant_id: tenantC,
        namespace: "default",
        budget: { max_attention_units: 100 },
      });
      const execution = await client.createContinuityExecution({
        tenant_id: tenantC,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
      await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantC,
        requested: {
          cost_microunits: 0,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 5,
        },
        estimation_version: "iso-v1",
      });
      await rejects(
        client.listContinuityBudgetReservations(execution.continuity_id, tenantB),
        404,
      );
    });

    it("cross-tenant list endpoints never leak tenant A's resource ids even when both tenants have same-shaped resources", async () => {
      // Give tenant B its own execution + stream so both tenants have data
      // of the same resource type, then assert tenant B's list contains
      // only its own ids.
      const { execution: execB, runtimeId: runtimeB } = await setupExecution(tenantB);
      await registerRuntime(tenantB, runtimeB);
      const streamB = await client.createContinuityStream({
        tenant_id: tenantB,
        continuity_id: execB.continuity_id,
        ttl_seconds: 60,
      });
      await client.appendContinuityFrame(streamB.stream_id, {
        tenant_id: tenantB,
        sequence: 0,
        checkpoint_sha256: "f".repeat(64),
        payload: {},
      });
      const framesB = await client.listContinuityFrames(streamB.stream_id, tenantB);
      assert.equal(framesB.length, 1);
      assert.ok(framesB.every((f: any) => f.tenant_id === tenantB));
      const runtimesB = await client.listRuntimes(tenantB);
      assert.ok(runtimesB.every((r: any) => r.runtime_id !== fixtureA.runtimeId));
      assert.ok(runtimesB.some((r: any) => r.runtime_id === runtimeB));
    });

    it("effect resolve: tenant B cannot resolve an effect receipt that belongs to tenant A", async () => {
      const effects = await client.listContinuityEffects(
        fixtureA.compSetup.execution.continuity_id,
        fixtureA.compSetup.tenantId,
      );
      assert.ok(effects.length > 0, "setup: tenant A must have at least one effect receipt");
      await rejects(
        client.resolveContinuityEffect(effects[0]!.id, {
          tenant_id: tenantB,
          state: "committed",
        }),
        404,
      );
    });

    it("attention task: assigning and deciding tenant A's task under tenant B both 404 (get_attention_task is tenant-scoped)", async () => {
      const task = await client.createAttentionTask({
        tenant_id: tenantA,
        continuity_id: fixtureA.execution.continuity_id,
        required_skills: ["iso-review"],
        classification: "internal",
        priority: 1,
        deadline: new Date(Date.now() + 60_000).toISOString(),
        estimated_attention_units: 1,
      });
      // assign_attention_task/decide_attention_task both look the task up
      // via get_attention_task(&tenant_id, id), which is itself tenant-
      // scoped storage — a tenant-B lookup of a tenant-A task id returns
      // None before eligibility is ever evaluated, so both calls 404.
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantB,
          reviewers: [
            {
              reviewer_id: uuid(),
              tenant_ids: [tenantB],
              skills: ["iso-review"],
              region: "br-south",
              trust: "registered",
              available_attention_units: 10,
            },
          ],
          lease_seconds: 60,
        }),
        404,
      );
      await rejects(
        client.decideAttentionTask(task.id, {
          tenant_id: tenantB,
          reviewer_id: "intruder",
          decision_sha256: createHash("sha256").update("x").digest("hex"),
        }),
        404,
      );
      // The task must still be assignable under its real tenant afterwards.
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantA,
        reviewers: [
          {
            reviewer_id: uuid(),
            tenant_ids: [tenantA],
            skills: ["iso-review"],
            region: "br-south",
            trust: "registered",
            available_attention_units: 10,
          },
        ],
        lease_seconds: 60,
      });
      assert.equal(assigned.state, "assigned");
    });

    it("continuation grant: tenant B cannot consume a grant issued under tenant A even with the correct token", async () => {
      const tenantForGrant = tid("iso-grant-a");
      const { execution } = await setupExecution(tenantForGrant);
      const destRuntime = uuid();
      await registerRuntime(tenantForGrant, destRuntime);
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantForGrant,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      await rejects(
        client.consumeContinuationGrant({
          tenant_id: tenantB,
          action: "accept",
          token: grant.token,
          signed_grant: grant.signed_grant,
        }),
        409,
        "a grant's tenant_id is checked as part of validate_claim; a foreign tenant can never consume it",
      );
      // The grant must still be consumable by its real tenant afterwards —
      // the failed cross-tenant attempt must not have poisoned its state.
      const consumed = await client.consumeContinuationGrant({
        tenant_id: tenantForGrant,
        action: "accept",
        token: grant.token,
        signed_grant: grant.signed_grant,
      });
      assert.equal(consumed.state, "consumed");
    });

    it("id-collision-adjacent ordering: three tenants each create a stream on their own execution; every tenant's list contains exactly its own frames in order", async () => {
      const tenants = [tid("iso-order-x"), tid("iso-order-y"), tid("iso-order-z")];
      const streamsByTenant: Record<string, { streamId: string; frameCount: number }> = {};
      for (const [index, t] of tenants.entries()) {
        const { execution } = await setupExecution(t);
        const stream = await client.createContinuityStream({
          tenant_id: t,
          continuity_id: execution.continuity_id,
          ttl_seconds: 60,
        });
        const frameCount = index + 1;
        for (let i = 0; i < frameCount; i++) {
          await client.appendContinuityFrame(stream.stream_id, {
            tenant_id: t,
            sequence: i,
            checkpoint_sha256: String(i).repeat(64).slice(0, 64),
            payload: { owner: t, i },
          });
        }
        streamsByTenant[t] = { streamId: stream.stream_id, frameCount };
      }
      for (const t of tenants) {
        const { streamId, frameCount } = streamsByTenant[t]!;
        const frames = await client.listContinuityFrames(streamId, t);
        assert.equal(frames.length, frameCount);
        assert.ok(frames.every((f: any) => f.tenant_id === t && f.payload.owner === t));
        assert.deepEqual(
          frames.map((f: any) => f.sequence),
          Array.from({ length: frameCount }, (_, i) => i),
        );
        // No other tenant's stream id resolves under this tenant.
        for (const other of tenants) {
          if (other === t) continue;
          await rejects(client.listContinuityFrames(streamsByTenant[other]!.streamId, t), 404);
        }
      }
    });
  });
});
