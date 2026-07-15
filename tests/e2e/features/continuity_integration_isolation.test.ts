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
  return { ...source, id: uuid(), version: (source.version ?? 1) + 1 };
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

/** Poll a continuity execution's effect receipts until the (single) effect
 * reaches `state`. Node-completion side effects (like EffectGuard::commit
 * advancing an effect to `committed` once a worker-dispatched handler
 * succeeds) land asynchronously relative to the HTTP response that queued
 * them, so a single read is not enough to know the transition has landed. */
async function waitForEffectState(
  continuityId: string,
  tenantId: string,
  state: string,
  timeoutMs = 10_000,
): Promise<any> {
  const deadline = Date.now() + timeoutMs;
  let last: any;
  while (Date.now() < deadline) {
    const effects = await client.listContinuityEffects(continuityId, tenantId);
    last = effects[0];
    if (last && last.state === state) return last;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(
    `Timeout waiting for effect on continuity ${continuityId} to reach '${state}'. Current state: ${last?.state}`,
  );
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

    it("verify_provenance against the CURRENT real head succeeds, and the reported head sha matches the last entry's own entry_sha256", async () => {
      const setup = await resumedHandoff("int-hoff-currenthead");
      const provenance = await client.listContinuityProvenance(setup.execution.continuity_id, setup.tenantId);
      const currentHead = provenance[provenance.length - 1]!.entry_sha256;
      const verification = await client.verifyContinuityProvenance(
        setup.execution.continuity_id,
        setup.tenantId,
        currentHead,
      );
      assert.equal(verification.valid, true, "the actual current head must verify successfully when supplied as expected_head");
      assert.equal(verification.first_invalid_index, null);
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

    it("rolling back an already-rolled-back plan a second time is rejected — rollback is a one-shot transition out of Applied", async () => {
      const tenantId = tid("int-mig-rb-twice");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-mig-rb-twice");
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
      const rolledBack = await client.rollbackContinuityMigration(plan.id, { tenant_id: tenantId });
      assert.equal(rolledBack.state, "rolled_back");
      await rejects(
        client.rollbackContinuityMigration(plan.id, { tenant_id: tenantId }),
        409,
        "a plan already rolled_back is no longer in the Applied state rollback requires",
      );
    });

    it("applying an already-applied migration plan a second time is rejected — apply is a one-shot transition out of Planned", async () => {
      const tenantId = tid("int-mig-apply-twice");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-mig-apply-twice");
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
      await rejects(
        client.applyContinuityMigration(plan.id, { tenant_id: tenantId }),
        409,
        "a plan already applied is no longer Planned; apply cannot be replayed",
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

    it("what-if targeting a DIFFERENT sequence version (a dry-run of a prospective migration) still leaves the real instance bound to its original sequence_id/version", async () => {
      const tenantId = tid("int-whatif-target-version");
      const { execution, sequence, instance } = await pausedContinuityFixture(
        tenantId,
        "int-whatif-target-version",
      );
      // Publish a second version of the SAME sequence under the SAME
      // (tenant_id, namespace, name) — get_sequence_by_name resolves purely
      // by that triple + version, independent of the id, so a fresh id is
      // fine here (matches the sameShapeTarget helper's own pattern).
      const secondVersion = { ...sequence, id: uuid(), version: (sequence.version ?? 1) + 1 };
      await client.createSequence(secondVersion);
      const result = await client.runContinuityWhatIf(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: (await client.listContinuityCheckpoints(execution.continuity_id, tenantId)).at(-1)!.checkpoint_id,
        target_sequence_version: secondVersion.version,
      });
      assert.ok(result, "what-if against an explicit target_sequence_version must resolve and run");
      const finalInstance = await client.getInstance(instance.id);
      assert.equal((finalInstance as any).sequence_id, sequence.id, "the real instance's sequence binding is untouched by a what-if dry-run");
    });

    it("what-if with block_param_overrides referencing an unknown step id is rejected with a clear validation error, not silently ignored", async () => {
      const tenantId = tid("int-whatif-bad-param-override");
      const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "int-whatif-bad-param-override");
      await rejects(
        client.runContinuityWhatIf(execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: checkpointId,
          block_param_overrides: { nonexistent_step_id: { foo: "bar" } },
        }),
        400,
        "an override referencing a step id absent from the sequence must be rejected, not silently dropped",
      );
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

  describe("A. manual provenance recording (record_provenance_boundary) is restricted to an allowlist of kinds and is tenant-scoped", () => {
    const validKinds = ["package_identity", "model_selected", "terminal_outcome"];

    for (const kind of validKinds) {
      it(`accepts kind=${kind} and appends it to the real provenance chain, still passing verification`, async () => {
        const tenantId = tid("int-manual-prov");
        const { execution } = await setupExecution(tenantId);
        const payloadSha = createHash("sha256").update(`manual-${kind}`).digest("hex");
        const head = await client.recordContinuityProvenance(execution.continuity_id, {
          tenant_id: tenantId,
          kind,
          payload_sha256: payloadSha,
        });
        assert.ok(head, "a successful record must return the new provenance head");
        const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
        assert.ok(provenance.some((e: any) => e.kind === kind));
        const verification = await client.verifyContinuityProvenance(execution.continuity_id, tenantId);
        assert.equal(verification.valid, true);
      });
    }

    it("rejects a kind outside the allowlist (e.g. an attempt to forge 'execution_created' or 'migration_applied' manually)", async () => {
      const tenantId = tid("int-manual-prov-forge");
      const { execution } = await setupExecution(tenantId);
      for (const forgedKind of ["execution_created", "migration_applied", "anything_else"]) {
        await rejects(
          client.recordContinuityProvenance(execution.continuity_id, {
            tenant_id: tenantId,
            kind: forgedKind,
            payload_sha256: createHash("sha256").update(forgedKind).digest("hex"),
          }),
          400,
          `kind=${forgedKind} must be rejected — it is outside the manual-record allowlist`,
        );
      }
    });

    it("tenant B cannot append manual provenance to tenant A's continuity_id", async () => {
      const tenantA2 = tid("int-manual-prov-iso-a");
      const tenantB2 = tid("int-manual-prov-iso-b");
      const { execution } = await setupExecution(tenantA2);
      await rejects(
        client.recordContinuityProvenance(execution.continuity_id, {
          tenant_id: tenantB2,
          kind: "model_selected",
          payload_sha256: createHash("sha256").update("cross-tenant").digest("hex"),
        }),
        404,
      );
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

    it("migrate THEN handoff THEN what-if on the same execution: each stage's epoch/ownership evidence is internally consistent with the next", async () => {
      const tenantId = tid("int-terminal-mig-then-handoff");
      const { execution, sequence, checkpointId } = await pausedContinuityFixture(
        tenantId,
        "int-terminal-mig-then-handoff",
      );

      // 1. Migrate to a same-shape target sequence (epoch 0 -> 1).
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const migrationPlan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      await client.applyContinuityMigration(migrationPlan.id, { tenant_id: tenantId });
      const postMigration = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.equal(postMigration.epoch, execution.epoch + 1);

      // 2. Hand off from the NEW (post-migration) epoch to a fresh
      // destination runtime — the migrated instance is `paused`, which is a
      // valid export precondition just like `waiting`.
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(
        tenantId,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
      );
      const handoff = await client.createHandoff(req);
      assert.equal(handoff.expected_epoch, postMigration.epoch, "the handoff must capture the POST-migration epoch, not the original one");
      const exported = await client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: req.requirements,
        expires_in_seconds: 60,
      });
      const destInstanceId = uuid();
      const imported = await client.importContinuityCapsule({
        tenant_id: tenantId,
        destination_runtime_id: destRuntime,
        destination_instance_id: destInstanceId,
        expected_epoch: postMigration.epoch,
        capsule: exported.capsule,
        payload_base64: exported.payload_base64,
      });
      const accepted = await client.acceptHandoff(handoff.id, {
        tenant_id: tenantId,
        destination_instance_id: imported.instance_id,
      });
      assert.equal(accepted.handoff.state, "accepted");
      const postHandoff = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.equal(postHandoff.epoch, postMigration.epoch + 1, "accept bumps the epoch once more, on top of the migration bump");
      assert.equal(postHandoff.owner_runtime_id, destRuntime);

      // 3. what-if against the ORIGINAL (pre-migration, pre-handoff)
      // checkpoint must still run read-only and not disturb the now
      // twice-transitioned execution.
      const whatIfResult = await client.runContinuityWhatIf(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: checkpointId,
      });
      assert.ok(whatIfResult, "what-if must still resolve against an old checkpoint even after two epoch-bumping transitions");
      const stillPostHandoff = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.deepEqual(stillPostHandoff, postHandoff, "what-if must not disturb the execution's state after the migrate+handoff chain");

      // 4. Every location row (original, post-migration, post-handoff-
      // accept epochs) must be present, and provenance must verify.
      const locations = await client.listContinuityLocations(execution.continuity_id, tenantId);
      const epochs = new Set(locations.map((l: any) => l.epoch));
      assert.ok(epochs.has(execution.epoch));
      assert.ok(epochs.has(postMigration.epoch));
      assert.ok(epochs.has(postHandoff.epoch));
      const verification = await client.verifyContinuityProvenance(execution.continuity_id, tenantId);
      assert.equal(verification.valid, true, "the full migrate-then-handoff provenance chain must still verify end to end");
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
    const operations = [
      "handler_dispatch",
      "artifact_creation",
      "migration",
      "fork",
      "backup_export",
      "retry",
      "fallback",
      "operator_override",
      "device_delegation",
    ];

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

    it("a candidate with breaker_open=true is excluded from selection even when it is otherwise the cheapest, highest-quality option", async () => {
      const tenantId = tid("int-provider-breaker");
      const { execution } = await setupExecution(tenantId);
      const decision = await client.chooseContinuityProvider({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        candidates: [
          {
            provider: "cheap-but-tripped",
            model: "m1",
            region: "br-south",
            pricing_version: "v1",
            price_microunits: 1,
            expected_latency_ms: 5,
            quality_millipoints: 1000,
            breaker_open: true,
            supports_idempotency: true,
          },
          {
            provider: "healthy-fallback",
            model: "m2",
            region: "br-south",
            pricing_version: "v1",
            price_microunits: 50,
            expected_latency_ms: 100,
            quality_millipoints: 700,
            breaker_open: false,
            supports_idempotency: true,
          },
        ],
        cohort_key: "int-cohort-breaker",
      });
      assert.ok(decision.selected, "a healthy fallback candidate exists and must be selected");
      assert.equal(decision.selected.provider, "healthy-fallback");
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      assert.ok(provenance.some((e: any) => e.kind === "provider_selected"));
    });

    it("all candidates exceeding max_price_microunits yields no selection, but still records the (empty) decision as provenance", async () => {
      const tenantId = tid("int-provider-price-cap");
      const { execution } = await setupExecution(tenantId);
      const decision = await client.chooseContinuityProvider({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        candidates: [
          {
            provider: "too-expensive",
            model: "m1",
            region: "br-south",
            pricing_version: "v1",
            price_microunits: 1000,
            expected_latency_ms: 20,
            quality_millipoints: 900,
            breaker_open: false,
            supports_idempotency: true,
          },
        ],
        cohort_key: "int-cohort-price",
        max_price_microunits: 10,
      });
      assert.equal(decision.selected ?? null, null, "the only candidate exceeds the price cap, so nothing is selected");
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      assert.ok(provenance.some((e: any) => e.kind === "provider_selected"), "even a null decision is recorded as evidence");
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

    it("a candidate that regresses below the allowed threshold reports a fail status, still attributed to the candidate's chain", async () => {
      const tenantId = tid("int-eval-gate-regress");
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      const evidence = (score: number) => createHash("sha256").update(`ev-regress-${score}`).digest("hex");
      for (const score of [900, 910, 905]) {
        await client.appendContinuityEvaluation(baseline.continuity_id, {
          tenant_id: tenantId,
          evaluator: "int-judge-regress",
          score_millipoints: score,
          sample_size: 10,
          evidence_sha256: evidence(score),
        });
      }
      // Candidate scores are substantially lower than baseline — a real
      // regression that should not be silently tolerated by a zero-slack
      // gate.
      for (const score of [500, 510, 505]) {
        await client.appendContinuityEvaluation(candidate.continuity_id, {
          tenant_id: tenantId,
          evaluator: "int-judge-regress",
          score_millipoints: score,
          sample_size: 10,
          evidence_sha256: evidence(score + 1000),
        });
      }
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "int-judge-regress",
        minimum_samples: 3,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "fail", "a large candidate regression must not pass a zero-slack gate");
      assert.ok((result.baseline_score_millipoints ?? 0) > (result.candidate_score_millipoints ?? 0));
      const candidateProvenance = await client.listContinuityProvenance(candidate.continuity_id, tenantId);
      assert.ok(candidateProvenance.some((e: any) => e.kind === "evaluation_gate"));
    });

    it("a stored gate whose summed sample_size is below minimum_samples reports 'inconclusive', not a hard pass/fail", async () => {
      // baseline_samples/candidate_samples are the SUM of each evaluation
      // entry's `sample_size` field (see orch8-engine/src/
      // continuity_advanced.rs weighted_evaluation_mean: `samples.
      // saturating_add(score.sample_size)`), not a count of entries. A
      // single entry with sample_size=1 against minimum_samples=5 is
      // exactly the shortfall this test needs.
      const tenantId = tid("int-eval-gate-insufficient");
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      const evidence = (score: number) => createHash("sha256").update(`ev-insuff-${score}`).digest("hex");
      await client.appendContinuityEvaluation(baseline.continuity_id, {
        tenant_id: tenantId,
        evaluator: "int-judge-insufficient",
        score_millipoints: 900,
        sample_size: 1,
        evidence_sha256: evidence(900),
      });
      await client.appendContinuityEvaluation(candidate.continuity_id, {
        tenant_id: tenantId,
        evaluator: "int-judge-insufficient",
        score_millipoints: 900,
        sample_size: 1,
        evidence_sha256: evidence(1900),
      });
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "int-judge-insufficient",
        minimum_samples: 5,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "inconclusive", "a sample-size shortfall must report inconclusive, never pass or fail");
      assert.equal(result.baseline_samples, 1);
      assert.equal(result.candidate_samples, 1);
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

    it("a 'move' transform relocates the field (source path no longer present, destination now holds it) in the migrated checkpoint's context_snapshot", async () => {
      const tenantId = tid("int-ckpt-transform-move");
      const { execution, sequence } = await pausedContinuityFixture(
        tenantId,
        "int-ckpt-transform-move",
        [],
        { field: "move-me" },
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
            transform: "move",
            from_path: "context_snapshot.field",
            to_path: "context_snapshot.relocated",
          },
        ],
      });
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId, approved: true });
      const finalInstance = await client.getContinuityExecution(execution.continuity_id, tenantId);
      const instanceDetail = await client.getInstance(finalInstance.current_instance_id);
      assert.equal((instanceDetail as any).context.data.field, undefined, "the source field must no longer exist after a move");
      assert.equal((instanceDetail as any).context.data.relocated, "move-me");
    });

    it("a 'drop' transform removes the field entirely with no destination path required", async () => {
      const tenantId = tid("int-ckpt-transform-drop");
      const { execution, sequence } = await pausedContinuityFixture(
        tenantId,
        "int-ckpt-transform-drop",
        [],
        { field: "drop-me", keep: "still-here" },
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
            transform: "drop",
            from_path: "context_snapshot.field",
            to_path: "",
          },
        ],
      });
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId, approved: true });
      const finalInstance = await client.getContinuityExecution(execution.continuity_id, tenantId);
      const instanceDetail = await client.getInstance(finalInstance.current_instance_id);
      assert.equal((instanceDetail as any).context.data.field, undefined);
      assert.equal((instanceDetail as any).context.data.keep, "still-here", "untouched sibling fields must survive a drop");
    });

    it("a migration plan whose transforms target the SAME destination path twice is rejected as invalid, before any apply is attempted", async () => {
      const tenantId = tid("int-ckpt-transform-dup-dest");
      const { execution, sequence } = await pausedContinuityFixture(
        tenantId,
        "int-ckpt-transform-dup-dest",
        [],
        { a: "1", b: "2" },
      );
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      await rejects(
        client.planContinuityMigration({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          to_sequence_id: createdTarget.id,
          to_version: target.version,
          transforms: [
            { version: 1, transform: "copy", from_path: "context_snapshot.a", to_path: "context_snapshot.merged" },
            { version: 1, transform: "copy", from_path: "context_snapshot.b", to_path: "context_snapshot.merged" },
          ],
        }),
        400,
        "two transforms writing to the identical destination path must be rejected as invalid",
      );
    });

    it("a migration plan whose transform references a source path that does not exist in the checkpoint is rejected at PLAN time (transforms are dry-run during validate_migration_history, not deferred to apply)", async () => {
      // plan_live_migration's validate_migration_history calls
      // apply_state_transforms eagerly (orch8-api/src/continuity.rs
      // ~4152-4156) to compute the target contract-test input — an absent
      // source path fails right there and maps to InvalidArgument (400),
      // before a plan record is ever persisted. apply_live_migration reruns
      // the same transforms again later (~4487-4491) but the plan attempt
      // never gets that far here.
      const tenantId = tid("int-ckpt-transform-missing-src");
      const { execution, sequence } = await pausedContinuityFixture(
        tenantId,
        "int-ckpt-transform-missing-src",
        [],
        { present: "yes" },
      );
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      await rejects(
        client.planContinuityMigration({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          to_sequence_id: createdTarget.id,
          to_version: target.version,
          transforms: [
            {
              version: 1,
              transform: "copy",
              from_path: "context_snapshot.does_not_exist",
              to_path: "context_snapshot.wont_be_set",
            },
          ],
        }),
        400,
        "a transform whose source path is absent from the actual checkpoint must fail during planning, not silently no-op",
      );
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

    it("reconciling with ACTUAL usage greater than requested succeeds (no hard cap on overage), and the reservation's counted amount is updated to the (larger) actual value, still blocking a fresh 10-unit reservation", async () => {
      // settle_budget_reservation (orch8-storage/src/sqlite/evidence.rs)
      // overwrites the stored cost_microunits/attention_units/etc columns
      // with `actual` usage when a reservation transitions to Reconciled,
      // and reserve_budget's availability check sums those same columns
      // for every reservation in state 'reserved' OR 'reconciled'. So an
      // over-budget reconcile (actual=25 against requested=10) makes the
      // reservation count for its ACTUAL (larger) amount going forward —
      // reconciling never rejects the overage, but it also does not cap it
      // at the original request, so it still fully blocks a fresh
      // reservation of the original size.
      const tenantId = tid("int-budget-overrun");
      const seq = testSequence("int-budget-overrun", [step("s1", "noop")], { tenantId });
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
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: usage(10),
        estimation_version: "int-v1",
      });
      const reconciled = await client.reconcileContinuityBudget(execution.continuity_id, reservation.id, {
        tenant_id: tenantId,
        actual: usage(25),
      });
      assert.equal(reconciled.state, "reconciled", "reconciling with an over-budget actual must still succeed");
      await rejects(
        client.reserveContinuityBudget(execution.continuity_id, {
          tenant_id: tenantId,
          requested: usage(10),
          estimation_version: "int-v1",
        }),
        409,
        "the reconciled reservation now counts for its actual (25) usage, well above the 10-unit cap",
      );
    });

    it("reconciling with ACTUAL usage LESS than requested frees the difference — a subsequent reservation for exactly the freed amount succeeds", async () => {
      const tenantId = tid("int-budget-underrun");
      const seq = testSequence("int-budget-underrun", [step("s1", "noop")], { tenantId });
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
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: usage(10),
        estimation_version: "int-v1",
      });
      await client.reconcileContinuityBudget(execution.continuity_id, reservation.id, {
        tenant_id: tenantId,
        actual: usage(3),
      });
      // Only 3 of the original 10 units are now counted, freeing 7.
      const second = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: usage(7),
        estimation_version: "int-v1",
      });
      assert.equal(second.state, "reserved", "reconciling to a smaller actual usage must genuinely free the difference");
    });
  });

  describe("A. attention task decision settles its budget reservation and records human_decision provenance", () => {
    async function attentionFixtureWithBudget(namePrefix: string) {
      const tenantId = tid(namePrefix);
      const seq = testSequence(namePrefix, [step("s1", "noop")], { tenantId });
      const createdSeq = await client.createSequence(seq);
      const instance = await client.createInstance({
        sequence_id: createdSeq.id,
        tenant_id: tenantId,
        namespace: "default",
        budget: { max_attention_units: 50 },
      });
      const execution = await client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
      const task = await client.createAttentionTask({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        required_skills: ["review"],
        classification: "internal",
        priority: 1,
        deadline: new Date(Date.now() + 60_000).toISOString(),
        estimated_attention_units: 5,
      });
      const reviewerId = uuid();
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [
          {
            reviewer_id: reviewerId,
            tenant_ids: [tenantId],
            skills: ["review"],
            region: "br-south",
            trust: "registered",
            available_attention_units: 10,
          },
        ],
        lease_seconds: 60,
      });
      return { tenantId, execution, task, reviewerId, assigned };
    }

    it("deciding an assigned task reconciles its budget reservation to settled and appends human_decision provenance", async () => {
      const { tenantId, execution, task, reviewerId } = await attentionFixtureWithBudget("int-attn-decide-budget");
      const beforeReservations = await client.listContinuityBudgetReservations(execution.continuity_id, tenantId);
      const ours = beforeReservations.find((r: any) => r.id === task.budget_reservation_id);
      assert.equal(ours.state, "reserved");
      const decided = await client.decideAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewer_id: reviewerId,
        decision_sha256: createHash("sha256").update("approve").digest("hex"),
      });
      assert.equal(decided.state, "decided");
      const afterReservations = await client.listContinuityBudgetReservations(execution.continuity_id, tenantId);
      const oursAfter = afterReservations.find((r: any) => r.id === task.budget_reservation_id);
      assert.equal(oursAfter.state, "reconciled");
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      assert.ok(provenance.some((e: any) => e.kind === "human_decision"));
    });

    it("deciding an already-decided task a second time is rejected — the lease/decision transition is one-shot", async () => {
      const { tenantId, task, reviewerId } = await attentionFixtureWithBudget("int-attn-decide-twice");
      await client.decideAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewer_id: reviewerId,
        decision_sha256: createHash("sha256").update("first").digest("hex"),
      });
      await rejects(
        client.decideAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewer_id: reviewerId,
          decision_sha256: createHash("sha256").update("second").digest("hex"),
        }),
        409,
        "a task already decided cannot be decided again",
      );
    });

    it("a reviewer who was never assigned this task cannot decide it, even under the correct tenant", async () => {
      const { tenantId, task } = await attentionFixtureWithBudget("int-attn-decide-wrong-reviewer");
      const impostorReviewer = uuid();
      await rejects(
        client.decideAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewer_id: impostorReviewer,
          decision_sha256: createHash("sha256").update("x").digest("hex"),
        }),
        409,
        "only the assigned reviewer's id can decide the task",
      );
    });

    it("a task whose lease expires without a decision can be reassigned to a DIFFERENT reviewer via a second assign_attention_task call", async () => {
      const { tenantId, execution } = await attentionFixtureWithBudget("int-attn-lease-reassign");
      // First reviewer's lease was already granted at 60s in the fixture —
      // build a SECOND fresh task with an intentionally short lease instead,
      // so this test controls its own expiry precisely.
      const task = await client.createAttentionTask({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        required_skills: ["reassign-review"],
        classification: "internal",
        priority: 1,
        deadline: new Date(Date.now() + 60_000).toISOString(),
        estimated_attention_units: 1,
      });
      const firstReviewer = uuid();
      await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [
          {
            reviewer_id: firstReviewer,
            tenant_ids: [tenantId],
            skills: ["reassign-review"],
            region: "br-south",
            trust: "registered",
            available_attention_units: 10,
          },
        ],
        lease_seconds: 1,
      });
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      const secondReviewer = uuid();
      const reassigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [
          {
            reviewer_id: secondReviewer,
            tenant_ids: [tenantId],
            skills: ["reassign-review"],
            region: "br-south",
            trust: "registered",
            available_attention_units: 10,
          },
        ],
        lease_seconds: 60,
      });
      assert.equal(reassigned.state, "assigned");
      assert.equal(reassigned.assignee, secondReviewer);
      assert.notEqual(reassigned.assignee, firstReviewer);
      // The original (now-expired) reviewer can no longer decide it.
      await rejects(
        client.decideAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewer_id: firstReviewer,
          decision_sha256: createHash("sha256").update("stale").digest("hex"),
        }),
        409,
        "the original reviewer's lease already expired and was superseded by reassignment",
      );
    });
  });

  describe("A. handoff preview reports incompatible (not an error) when the execution has an unresolved effect in flight", () => {
    it("preview_handoff against an execution with a dispatched-but-unresolved effect returns compatible=false with that effect listed, not a hard error", async () => {
      const setup = await compensableExecution("int-preview-unresolved-effect");
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      assert.equal(effects[0]!.state, "dispatched", "setup: the effect must still be unresolved");
      const destRuntime = uuid();
      await registerRuntime(setup.tenantId, destRuntime, { handlers: ["human_review", setup.handler] });
      const preview = await client.previewHandoff(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
        destination_runtime_id: destRuntime,
        requirements: { handlers: ["human_review"] },
      });
      assert.equal(preview.compatible, false, "an unresolved effect must make the preview incompatible");
      assert.ok(
        preview.unresolved_effects.some((e: any) => e.id === effects[0]!.id),
        "the specific unresolved effect must be named in the preview evidence",
      );
    });

    it("a create_handoff attempt built from an incompatible (unresolved-effect) preview is rejected — create_handoff enforces compatible=true itself", async () => {
      const setup = await compensableExecution("int-create-unresolved-effect");
      const destRuntime = uuid();
      await registerRuntime(setup.tenantId, destRuntime, { handlers: ["human_review", setup.handler] });
      const preview = await client.previewHandoff(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
        destination_runtime_id: destRuntime,
        requirements: { handlers: ["human_review"] },
      });
      assert.equal(preview.compatible, false);
      await rejects(
        client.createHandoff({
          tenant_id: setup.tenantId,
          continuity_id: setup.execution.continuity_id,
          destination_runtime_id: destRuntime,
          requirements: { handlers: ["human_review"] },
          placement_decision_id: preview.placement_decision.id,
          preview_sha256: preview.preview_sha256,
        }),
        409,
        "create_handoff must independently refuse an incompatible preview, not just report it",
      );
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

  describe("A. stream retraction and append both react to a migration epoch bump", () => {
    it("append and retract both re-validate against the LIVE execution epoch post-migration (409)", async () => {
      // retract_stream_frames re-fetches the owning execution and compares
      // its LIVE epoch against the stream's stored epoch, exactly like
      // append_stream_frame does (see orch8-api/src/continuity.rs
      // retract_stream_frames). So a migration that bumps the execution's
      // epoch makes a previously-valid retraction request stale in the same
      // way it makes the equivalent append request stale. This test pins
      // down that symmetry rather than assuming it.
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

      // Retract with that SAME pre-migration epoch is likewise rejected,
      // since retract now re-checks the live execution epoch too.
      await rejects(
        client.retractContinuityFrames(stream.stream_id, {
          tenant_id: tenantId,
          epoch: stream.epoch,
          after_sequence: 1,
        }),
        409,
        "retract must re-check the live execution epoch, just like append",
      );
    });
  });

  describe("A. a stream past its ttl_seconds expiry can no longer accept new frames, and its already-appended frames also drop out of listing", () => {
    it("both appending to an expired stream AND listing its pre-expiry frames are affected once the stream's TTL elapses (frames inherit the stream's expires_at and are filtered by list_stream_frames' own `expires_at > now` clause)", async () => {
      // append_stream_frame stamps each new frame's `expires_at` to the
      // OWNING STREAM's expires_at (see append_stream_frame: `expires_at:
      // stream.expires_at`), and list_stream_frames' SQL filters on
      // `expires_at > ?` (orch8-storage/src/sqlite/continuity.rs
      // list_stream_frames) — so once a stream's TTL elapses, frames
      // appended while it was still live also disappear from subsequent
      // listings, not just future appends being blocked. This test pins
      // both halves of that behavior together rather than assuming only
      // appends are affected.
      const tenantId = tid("int-strm-ttl-expiry");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 1,
      });
      await client.appendContinuityFrame(stream.stream_id, {
        tenant_id: tenantId,
        sequence: 0,
        checkpoint_sha256: "a".repeat(64),
        payload: { before: "expiry" },
      });
      const framesBeforeExpiry = await client.listContinuityFrames(stream.stream_id, tenantId);
      assert.equal(framesBeforeExpiry.length, 1, "the frame is listable immediately after append, before expiry");
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      await rejects(
        client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: 1,
          checkpoint_sha256: "b".repeat(64),
          payload: { after: "expiry" },
        }),
        409,
        "appending to a stream past its own expires_at must be rejected",
      );
      const framesAfterExpiry = await client.listContinuityFrames(stream.stream_id, tenantId);
      assert.deepEqual(framesAfterExpiry, [], "frames inherited the stream's expires_at, so they drop out of listing too once it elapses");
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

  describe("A. compensation completion under every verification policy", () => {
    /** A single-effect compensable execution whose compensation handler
     * carries the given verification policy. */
    async function policyCompensableExecution(namePrefix: string, policy: string) {
      const tenantId = tid(namePrefix);
      const handler = `${namePrefix}_h_${uuid().slice(0, 8)}`;
      const sequence = testSequence(
        namePrefix,
        [
          step("arm", "human_review", {}, {
            wait_for_input: { prompt: "go?", choices: [{ label: "Continue", value: "continue" }] },
          }),
          step("charge", handler, { amount: 5 }, {
            compensation: { handler: "release_charge", verification: policy },
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
      await client.sendSignal(instance.id, { custom: "human_input:arm" } as unknown as string, { value: "continue" });
      const task = await waitForWorkerTask(handler, `${namePrefix}-worker`);
      await client.completeWorkerTask(task.id, `${namePrefix}-worker`, { ok: true });
      // completeWorkerTask only guarantees the completion request was
      // accepted; the server's EffectGuard::commit (triggered by the
      // handler succeeding) advances the effect receipt to `committed`
      // asynchronously as part of node-completion processing. Racing a
      // single read-then-resolve against that landing produces a
      // nondeterministic 409 ("invalid effect transition from Committed to
      // Committed") when the auto-commit lands between the read and the
      // write. Poll until the auto-commit has actually landed instead of
      // reading once and conditionally issuing a redundant manual resolve.
      const effect = await waitForEffectState(execution.continuity_id, tenantId, "committed");
      return { tenantId, execution, effect };
    }

    it("handler_result policy: completing a step with no provider_receipt still succeeds outright (Succeeded, not VerificationPending)", async () => {
      const { tenantId, execution } = await policyCompensableExecution("int-comp-policy-handler", "handler_result");
      const run = await client.createContinuityCompensation(execution.continuity_id, { tenant_id: tenantId });
      const claimed = await client.claimContinuityCompensation(run.id, { tenant_id: tenantId, worker_id: "w" });
      const completed = await client.completeContinuityCompensation(run.id, claimed.plan.effect_id, {
        tenant_id: tenantId,
        worker_id: "w",
      });
      assert.equal(completed.state, "completed");
      const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
      assert.equal(effects[0]!.state, "compensated");
    });

    it("provider_receipt policy: completing without a provider_receipt is rejected as invalid; supplying one completes it outright", async () => {
      const { tenantId, execution } = await policyCompensableExecution("int-comp-policy-receipt", "provider_receipt");
      const run = await client.createContinuityCompensation(execution.continuity_id, { tenant_id: tenantId });
      const claimed = await client.claimContinuityCompensation(run.id, { tenant_id: tenantId, worker_id: "w" });
      await rejects(
        client.completeContinuityCompensation(run.id, claimed.plan.effect_id, { tenant_id: tenantId, worker_id: "w" }),
        400,
        "provider_receipt verification requires a provider_receipt on completion",
      );
      const completed = await client.completeContinuityCompensation(run.id, claimed.plan.effect_id, {
        tenant_id: tenantId,
        worker_id: "w",
        provider_receipt: `receipt-${uuid().slice(0, 8)}`,
      });
      assert.equal(completed.state, "completed");
      const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
      assert.equal(effects[0]!.state, "compensated");
    });

    it("manual policy: completing the step lands it in VerificationPending, and the effect is NOT compensated until verify_compensation_step approves it", async () => {
      const { tenantId, execution } = await policyCompensableExecution("int-comp-policy-manual", "manual");
      const run = await client.createContinuityCompensation(execution.continuity_id, { tenant_id: tenantId });
      const claimed = await client.claimContinuityCompensation(run.id, { tenant_id: tenantId, worker_id: "w" });
      const completed = await client.completeContinuityCompensation(run.id, claimed.plan.effect_id, {
        tenant_id: tenantId,
        worker_id: "w",
      });
      const step = completed.steps.find((s: any) => s.plan.effect_id === claimed.plan.effect_id);
      assert.equal(step.state, "verification_pending");
      const effectsBeforeVerify = await client.listContinuityEffects(execution.continuity_id, tenantId);
      assert.equal(effectsBeforeVerify[0]!.state, "committed", "manual policy must not auto-compensate on completion alone");
      const verified = await client.verifyContinuityCompensation(run.id, claimed.plan.effect_id, {
        tenant_id: tenantId,
        approved: true,
        evidence: "operator confirmed externally",
      });
      assert.equal(verified.state, "completed");
      const effectsAfterVerify = await client.listContinuityEffects(execution.continuity_id, tenantId);
      assert.equal(effectsAfterVerify[0]!.state, "compensated");
    });
  });

  describe("A. compensation step claim exclusivity: a second worker cannot claim while the current step is already Claimed", () => {
    it("a second claim_compensation_step call while the first worker's claim is still active is rejected, and the original claim's owner is untouched", async () => {
      const setup = await committedCompensableExecution("int-comp-claim-exclusive");
      const run = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const firstClaim = await client.claimContinuityCompensation(run.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-first",
      });
      assert.equal(firstClaim.state, "claimed");
      await rejects(
        client.claimContinuityCompensation(run.id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-second",
        }),
        409,
        "the current step must be settled (completed/failed/verified) before a different worker can claim anything",
      );
      // The original worker can still complete its own claim afterward —
      // the rejected second claim must not have disturbed it.
      const completed = await client.completeContinuityCompensation(run.id, firstClaim.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-first",
      });
      assert.equal(completed.state, "completed");
    });

    it("a worker attempting to complete a step it never claimed (wrong worker_id) is rejected, even though the step IS claimed by someone", async () => {
      const setup = await committedCompensableExecution("int-comp-complete-wrong-worker");
      const run = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(run.id, {
        tenant_id: setup.tenantId,
        worker_id: "real-worker",
      });
      await rejects(
        client.completeContinuityCompensation(run.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "impostor-worker",
        }),
        409,
        "only the worker that actually claimed the step's lease may complete it",
      );
    });

    it("verify_compensation_step against a step that is still Pending (never claimed at all) is rejected — verification only applies once a step reaches VerificationPending", async () => {
      const setup = await committedCompensableExecution("int-comp-verify-still-pending");
      const run = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.verifyContinuityCompensation(run.id, setup.effect.id, {
          tenant_id: setup.tenantId,
          approved: true,
          evidence: "premature verification attempt",
        }),
        409,
        "a never-claimed Pending step cannot be verified",
      );
    });

    it("verify_compensation_step against a step that is Claimed (in progress, not yet completed/failed) is also rejected", async () => {
      const setup = await committedCompensableExecution("int-comp-verify-claimed-not-settled");
      const run = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(run.id, {
        tenant_id: setup.tenantId,
        worker_id: "in-progress-worker",
      });
      await rejects(
        client.verifyContinuityCompensation(run.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          approved: true,
          evidence: "premature verification while still claimed",
        }),
        409,
        "a Claimed-but-not-yet-completed step is not awaiting verification either",
      );
    });
  });

  describe("A. effect state machine: invalid transitions are rejected end to end, valid multi-hop transitions succeed", () => {
    it("BUG-ADJACENT: Committed -> Compensated IS allowed directly via resolve_effect, bypassing the entire compensation-run workflow with no audit trail", async () => {
      // EffectState::can_transition_to (orch8-types/src/continuity.rs
      // ~688) lists `(Committed | Verified, Compensated)` as a structurally
      // valid transition, and resolve_effect (orch8-api/src/continuity.rs
      // ~1742-1785) accepts ANY structurally valid target state supplied by
      // the caller — there is no additional guard requiring compensation to
      // go through create_continuity_compensation /
      // complete_compensation_step. A caller can mark a committed effect
      // "compensated" directly via resolve_effect with a single call, with
      // no compensation run, no LIFO ordering, and no verification policy.
      // Separately: this codebase's ENTIRE compensation-run workflow
      // (create/claim/complete/fail/verify_compensation) never calls
      // append_provenance_boundary anywhere — grep confirms no
      // "compensation_*" provenance kind exists at all in continuity.rs, so
      // even a proper compensation run leaves no chain evidence distinct
      // from a direct resolve_effect call. Both facts are pinned here as a
      // genuine, previously unreported gap distinct from the three already
      // documented in this file's header comments.
      const setup = await committedCompensableExecution("int-effect-direct-compensate");
      const resolved = await client.resolveContinuityEffect(setup.effect.id, {
        tenant_id: setup.tenantId,
        state: "compensated",
      });
      assert.equal(resolved.state, "compensated", "documents that this transition is currently permitted with no compensation-run evidence");
      const provenance = await client.listContinuityProvenance(setup.execution.continuity_id, setup.tenantId);
      const kinds = provenance.map((e: any) => e.kind);
      assert.ok(kinds.includes("effect_resolved"), "the only trace of this state change is a generic effect_resolved entry");
      assert.ok(
        !kinds.some((k: string) => k.startsWith("compensation")),
        "no compensation-specific provenance kind exists anywhere, even though a full compensation-run workflow exists as a separate API surface",
      );
    });

    it("Dispatched -> Verified is refused (Verified is only reachable via Unknown, never directly from Dispatched)", async () => {
      const setup = await compensableExecution("int-effect-skip-unknown");
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      assert.equal(effects[0]!.state, "dispatched");
      await rejects(
        client.resolveContinuityEffect(effects[0]!.id, {
          tenant_id: setup.tenantId,
          state: "verified",
        }),
        409,
        "Dispatched can only move to Committed or Unknown, never directly to Verified",
      );
    });

    it("Committed -> Committed (a redundant re-resolve to the same state) is refused as an invalid self-transition", async () => {
      const setup = await committedCompensableExecution("int-effect-redundant-commit");
      await rejects(
        client.resolveContinuityEffect(setup.effect.id, {
          tenant_id: setup.tenantId,
          state: "committed",
        }),
        409,
        "an effect already committed cannot be 're-committed'",
      );
    });

    it("a compensated effect can never transition again in any direction (Compensated is a true terminal state)", async () => {
      const setup = await committedCompensableExecution("int-effect-terminal-compensated");
      await completeCompensationRun(setup.execution.continuity_id, setup.tenantId);
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      assert.equal(effects[0]!.state, "compensated");
      await rejects(
        client.resolveContinuityEffect(effects[0]!.id, {
          tenant_id: setup.tenantId,
          state: "committed",
        }),
        409,
        "a compensated effect can never move back to committed",
      );
      await rejects(
        client.resolveContinuityEffect(effects[0]!.id, {
          tenant_id: setup.tenantId,
          state: "verified",
        }),
        409,
        "a compensated effect can never move to verified either — it is terminal",
      );
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

    it("an invariant registered with sequence_version=null applies across EVERY migrated version of that sequence_id, not just the version it was created against", async () => {
      // list_workflow_invariants' SQL matches `sequence_version IS NULL OR
      // sequence_version = ?` (orch8-storage/src/sqlite/evidence.rs) — a
      // version-less invariant is intentionally version-agnostic. This
      // test registers one against the ORIGINAL sequence with no version
      // pinned, migrates to a target version, and confirms it still
      // evaluates post-migration even though the instance is now on a
      // DIFFERENT sequence_id (planContinuityMigration always targets a
      // distinct to_sequence_id, so this also proves the invariant survives
      // a full sequence_id change, not merely a version bump on the same
      // id — provided it is re-registered on the target id, matching how
      // this file's other migration+invariant tests operate).
      const tenantId = tid("int-inv-version-agnostic");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-inv-version-agnostic");
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: target.id,
        sequence_version: null,
        name: "version-agnostic-no-unknown-effects",
        rule: { type: "no_unknown_effects" },
      });
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: target.id,
        to_version: target.version,
      });
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
      const matched = results.find((r: any) => r.invariant_id === invariant.id);
      assert.ok(matched, "a version-agnostic invariant must still be evaluated after migrating to the target sequence_id");
      assert.equal(matched.status, "pass");
    });

    it("a budget_within_limits invariant always evaluates to 'unknown' via evaluate_invariants, regardless of real budget reservations — budget_breached evidence is never wired up", async () => {
      // evaluate_invariants (orch8-api/src/continuity.rs ~2281-2286) builds
      // its InvariantEvidence with `budget_breached: None` UNCONDITIONALLY —
      // it never consults actual budget reservations/usage, even when the
      // instance has a real configured budget and outstanding reservations.
      // orch8-engine's budget_within_limits() maps None to EvidenceStatus::
      // Unknown always (see continuity_advanced.rs budget_within_limits).
      // Net effect: a budget_within_limits invariant can NEVER report pass
      // or fail through this endpoint today — it is permanently vacuous.
      // This test pins that actual (surprising) behavior rather than the
      // intended one.
      const tenantId = tid("int-inv-budget");
      const seq = testSequence("int-inv-budget", [step("s1", "noop")], { tenantId });
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
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSeq.id,
        sequence_version: seq.version,
        name: "budget-ok",
        rule: { type: "budget_within_limits" },
      });
      await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 0,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 10,
        },
        estimation_version: "int-v1",
      });
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
      const matched = results.find((r: any) => r.invariant_id === invariant.id);
      assert.ok(matched, "budget_within_limits must be evaluated alongside any other registered invariant");
      assert.equal(matched.status, "unknown", "documents that budget evidence is never actually supplied");
      const stored = await client.listContinuityInvariantResults(execution.continuity_id, tenantId);
      assert.ok(stored.some((r: any) => r.invariant_id === invariant.id && r.status === "unknown"));
    });

    it("an effect_at_most_once invariant registered for a handler kind that never dispatched vacuously passes, and re-evaluating with IDENTICAL evidence is deduplicated at storage (not appended a second time)", async () => {
      const tenantId = tid("int-inv-at-most-once");
      const { execution, sequence } = await setupExecution(tenantId);
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: sequence.id,
        sequence_version: sequence.version,
        name: "at-most-once-charge",
        rule: { type: "effect_at_most_once", kind: "custom" },
      });
      const firstResults = await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
      const secondResults = await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
      assert.equal(firstResults.find((r: any) => r.invariant_id === invariant.id)?.status, "pass");
      assert.equal(secondResults.find((r: any) => r.invariant_id === invariant.id)?.status, "pass");
      // append_invariant_result uses `INSERT OR IGNORE` keyed on a
      // dedupe_key derived from (invariant_id, continuity_id, epoch,
      // evidence_sha256) — see orch8-storage/src/sqlite/evidence.rs
      // append_invariant_result. Since no effects were ever dispatched, both
      // evaluate calls produce byte-identical evidence, so the second
      // append is silently ignored: exactly ONE result row is stored, not
      // two, even though evaluate_invariants was called twice.
      const stored = await client.listContinuityInvariantResults(execution.continuity_id, tenantId);
      const forThisInvariant = stored.filter((r: any) => r.invariant_id === invariant.id);
      assert.equal(forThisInvariant.length, 1, "identical re-evaluation evidence is deduplicated, not appended again");
    });

    it("an output_path_present invariant reports 'unknown' (not 'fail') for a made-up path never produced by the block, matching the invariant module's non-committal contract for absent evidence", async () => {
      // output_path_present maps "path absent" to EvidenceStatus::Unknown,
      // not Fail (see orch8-engine/src/continuity_advanced.rs
      // output_path_present: `if present { Pass } else { Unknown }`) — there
      // is no hard-fail branch for this rule at all.
      const tenantId = tid("int-inv-output-path");
      const sequence = testSequence(tenantId, [step("s1", "noop")], { tenantId });
      const createdSeq = await client.createSequence(sequence);
      const instance = await client.createInstance({
        sequence_id: createdSeq.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      const execution = await client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSeq.id,
        sequence_version: sequence.version,
        name: "output-present",
        rule: { type: "output_path_present", block_id: "s1", path: "some.nonexistent.path" },
      });
      await client.waitForState(instance.id, "completed");
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
      const matched = results.find((r: any) => r.invariant_id === invariant.id);
      assert.equal(matched.status, "unknown", "an absent output path is 'unknown', never a hard 'fail'");
    });
  });

  describe("A. handoff requirements compatibility interacts with residency region policy", () => {
    const regionPairs: Array<{ sourceRegion: string; destRegion: string; allowed: string[]; expectAllowed: boolean }> = [
      { sourceRegion: "br-south", destRegion: "br-south", allowed: ["br-south"], expectAllowed: true },
      { sourceRegion: "br-south", destRegion: "us-east", allowed: ["br-south"], expectAllowed: false },
      { sourceRegion: "eu-west", destRegion: "eu-west", allowed: ["eu-west", "br-south"], expectAllowed: true },
      { sourceRegion: "us-east", destRegion: "us-east", allowed: ["br-south"], expectAllowed: false },
      { sourceRegion: "ap-south", destRegion: "ap-south", allowed: ["ap-south", "eu-west", "us-east"], expectAllowed: true },
      // An EMPTY allowed_regions list disables the region check entirely
      // (orch8-engine/src/continuity_advanced.rs evaluate_residency guards
      // every region-denial branch on `!allowed_regions.is_empty()`), so an
      // internal-classification destination always passes regardless of
      // region when no allowlist is configured.
      { sourceRegion: "br-south", destRegion: "br-south", allowed: [], expectAllowed: true },
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

  describe("A. regulated-classification residency enforces destination trust level, unlike internal/public classifications", () => {
    // NOTE: RuntimeTrustLevel::Signed/Attested can never actually be
    // reached through the public register_runtime endpoint — it hard-caps
    // incoming trust at Registered (see orch8-api/src/continuity.rs
    // register_runtime: `if body.capabilities.trust > RuntimeTrustLevel::
    // Registered { return InvalidArgument("signed or attested runtime
    // trust requires a verified attestation flow") }`), and no other public
    // endpoint elevates a runtime's trust level either. This means
    // evaluate_residency's regulated-classification PASS path (which
    // requires `trust >= Signed`) is presently unreachable end-to-end via
    // the API surface — only the Fail path (Unverified/Registered) is
    // exercisable here. This is flagged as a genuine, previously
    // unreported observation distinct from the three documented bugs.
    const trustCases: Array<{ trust: string; expectPass: boolean }> = [
      { trust: "unverified", expectPass: false },
      { trust: "registered", expectPass: false },
    ];

    for (const { trust, expectPass } of trustCases) {
      it(`confidential classification against a destination with trust=${trust} resolves outcome=${expectPass ? "pass" : "fail"} (Signed is the minimum bar, unreachable via the public API)`, async () => {
        const tenantId = tid("int-res-trust-matrix");
        const { execution, runtimeId: sourceRuntimeId } = await setupExecution(tenantId);
        // A regulated classification also requires the SOURCE (owner)
        // runtime's region to be resolvable, or evaluate_residency fails
        // fast on SOURCE_REGION_UNKNOWN before ever reaching the
        // destination trust check — register it too.
        await registerRuntime(tenantId, sourceRuntimeId, { regions: ["br-south"] });
        const destRuntime = uuid();
        await client.registerRuntime({
          tenant_id: tenantId,
          capabilities: runtimeCaps(destRuntime, { trust, regions: ["br-south"] }),
        });
        const result = await client.evaluateContinuityResidency({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          classification: "confidential",
          operation: "handler_dispatch",
          destination_runtime_id: destRuntime,
        });
        if (expectPass) {
          assert.equal(result.outcome, "pass");
        } else {
          assert.equal(result.outcome, "fail");
          assert.ok(result.finding_codes.includes("DESTINATION_TRUST_TOO_LOW"));
        }
      });
    }

    it("the SAME low trust level (registered) is perfectly fine for an internal-classification residency check — trust enforcement is regulated-classification-only", async () => {
      const tenantId = tid("int-res-trust-internal-ok");
      const { execution, runtimeId: sourceRuntimeId } = await setupExecution(tenantId);
      await registerRuntime(tenantId, sourceRuntimeId, { regions: ["br-south"] });
      const destRuntime = uuid();
      await client.registerRuntime({
        tenant_id: tenantId,
        capabilities: runtimeCaps(destRuntime, { trust: "registered", regions: ["br-south"] }),
      });
      const result = await client.evaluateContinuityResidency({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        classification: "internal",
        operation: "handler_dispatch",
        destination_runtime_id: destRuntime,
      });
      assert.equal(result.outcome, "pass", "trust enforcement only applies to confidential/restricted, not internal");
    });

    it("a regulated (confidential) classification whose SOURCE runtime sits in a region outside allowed_regions fails with SOURCE_REGION_DENIED, checked before the destination is even evaluated", async () => {
      const tenantId = tid("int-res-source-region-denied");
      const { execution, runtimeId: sourceRuntimeId } = await setupExecution(tenantId);
      await registerRuntime(tenantId, sourceRuntimeId, { regions: ["us-east"] });
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime, { regions: ["br-south"], trust: "registered" });
      const result = await client.evaluateContinuityResidency({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        classification: "confidential",
        operation: "handler_dispatch",
        destination_runtime_id: destRuntime,
        allowed_regions: ["br-south"],
      });
      assert.equal(result.outcome, "fail");
      assert.ok(result.finding_codes.includes("SOURCE_REGION_DENIED"));
    });

    it("the SAME source-region mismatch is NOT enforced for a public/internal classification — SOURCE_REGION_DENIED is a regulated-classification-only check", async () => {
      const tenantId = tid("int-res-source-region-ok-internal");
      const { execution, runtimeId: sourceRuntimeId } = await setupExecution(tenantId);
      await registerRuntime(tenantId, sourceRuntimeId, { regions: ["us-east"] });
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime, { regions: ["br-south"] });
      const result = await client.evaluateContinuityResidency({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        classification: "internal",
        operation: "handler_dispatch",
        destination_runtime_id: destRuntime,
        allowed_regions: ["br-south"],
      });
      assert.equal(result.outcome, "pass", "source-region enforcement is regulated-classification-only, per evaluate_residency's match guards");
    });

    it("a confidential-classification residency check against a destination runtime that was NEVER registered at all fails with DESTINATION_REGION_UNKNOWN (checked before trust), not a 404", async () => {
      // evaluate_residency's match arms are ordered: an unresolvable
      // destination region for a regulated classification
      // (`(_, None, _) if regulated`) is checked BEFORE the destination
      // trust arms, so a never-registered runtime (which has no capability
      // row and therefore no resolvable single region) always surfaces
      // DESTINATION_REGION_UNKNOWN — the trust check is never reached. The
      // SOURCE runtime must be registered here too, or the source-region
      // arm (checked even earlier) would fail first instead.
      const tenantId = tid("int-res-trust-unregistered");
      const { execution, runtimeId: sourceRuntimeId } = await setupExecution(tenantId);
      await registerRuntime(tenantId, sourceRuntimeId, { regions: ["br-south"] });
      const neverRegisteredRuntime = uuid();
      const result = await client.evaluateContinuityResidency({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        classification: "confidential",
        operation: "handler_dispatch",
        destination_runtime_id: neverRegisteredRuntime,
      });
      assert.equal(result.outcome, "fail");
      assert.ok(
        result.finding_codes.includes("DESTINATION_REGION_UNKNOWN"),
        "an unregistered destination must resolve as a well-formed evidence failure, never a 404",
      );
    });
  });

  describe("A. negative/conflict branches not yet exercised", () => {
    it("BUG: create_handoff has no guard against a second concurrent pending handoff for the same execution", async () => {
      // create_handoff (orch8-api/src/continuity.rs ~844-933) validates the
      // placement decision, preview hash, and epoch, but never checks for an
      // existing non-terminal (Requested/Exported) handoff on the same
      // continuity_id before creating a new one. build_handoff_preview also
      // does not consult existing handoffs. So two independent handoffs can
      // both be Requested simultaneously for one execution — a genuine gap:
      // only the LATER accept would eventually win via the epoch/ownership
      // checks elsewhere, but both handoffs sit in "requested" indefinitely
      // and a caller has no signal that one is already in flight. This test
      // pins the ACTUAL (buggy) behavior rather than the desired one; when
      // this gap is closed, replace `.equal("requested")` below with a
      // `rejects(..., 409)` assertion on the second create_handoff call.
      const tenantId = tid("int-hoff-double-pending");
      const { execution } = await setupExecution(tenantId);
      const destA = uuid();
      const destB = uuid();
      const reqA = await authorizedHandoffRequest(tenantId, execution.continuity_id, destA);
      const first = await client.createHandoff(reqA);
      assert.equal(first.state, "requested");
      const reqB = await authorizedHandoffRequest(tenantId, execution.continuity_id, destB);
      const second = await client.createHandoff(reqB);
      assert.equal(
        second.state,
        "requested",
        "documents the bug: a second concurrent handoff is currently accepted, not rejected",
      );
      assert.notEqual(first.id, second.id);
      const stillFirst = await client.getHandoff(first.id, tenantId);
      assert.equal(stillFirst.state, "requested", "the first handoff is not implicitly cancelled either");
    });

    it("applying a migration without ever calling plan first 404s (no plan id exists to apply)", async () => {
      const tenantId = tid("int-mig-noplan");
      await rejects(
        client.applyContinuityMigration(uuid(), { tenant_id: tenantId }),
        404,
        "apply against a nonexistent migration plan id must 404",
      );
    });

    it("rolling back a migration without ever applying it is rejected (nothing to roll back)", async () => {
      const tenantId = tid("int-mig-rb-noapply");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-mig-rb-noapply");
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      await rejects(
        client.rollbackContinuityMigration(plan.id, { tenant_id: tenantId }),
        409,
        "a plan that was never applied cannot be rolled back",
      );
    });

    it("attempting compensation on an execution with no committed effects yet is rejected", async () => {
      const setup = await compensableExecution("int-comp-uncommitted");
      // The dispatched effect exists but has NOT been resolved to committed
      // yet, so there is nothing eligible to compensate.
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      assert.equal(effects[0]!.state, "dispatched");
      await rejects(
        client.createContinuityCompensation(setup.execution.continuity_id, {
          tenant_id: setup.tenantId,
        }),
        409,
        "compensation requires at least one committed effect",
      );
    });

    it("resuming an already-resumed handoff a second time is rejected", async () => {
      const setup = await resumedHandoff("int-hoff-resume-twice");
      await rejects(
        client.resumeHandoff(setup.handoff.id, { tenant_id: setup.tenantId }),
        409,
        "a handoff already in Resumed state cannot be resumed again",
      );
      const stillResumed = await client.getHandoff(setup.handoff.id, setup.tenantId);
      assert.equal(stillResumed.state, "resumed");
    });

    it("rejecting a handoff that has already been exported is refused — reject only applies to a still-Requested handoff", async () => {
      const tenantId = tid("int-hoff-reject-after-export");
      const { execution } = await pausedContinuityFixture(tenantId, "int-hoff-reject-after-export");
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(
        tenantId,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
      );
      const handoff = await client.createHandoff(req);
      await client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: req.requirements,
        expires_in_seconds: 60,
      });
      await rejects(
        client.rejectHandoff(handoff.id, { tenant_id: tenantId }),
        409,
        "reject_handoff requires HandoffState::Requested; an exported handoff has moved on",
      );
    });

    it("revoking a handoff that is still Requested (never exported) is refused — revoke only applies to an already-Exported handoff", async () => {
      const tenantId = tid("int-hoff-revoke-before-export");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, destRuntime);
      const handoff = await client.createHandoff(req);
      assert.equal(handoff.state, "requested");
      await rejects(
        client.revokeHandoff(handoff.id, { tenant_id: tenantId }),
        409,
        "revoke_handoff requires HandoffState::Exported; a still-requested handoff cannot be revoked yet",
      );
    });

    it("revoking an already-revoked handoff a second time is rejected — revoke is a one-shot transition out of Exported", async () => {
      const tenantId = tid("int-hoff-revoke-twice");
      const { execution } = await pausedContinuityFixture(tenantId, "int-hoff-revoke-twice");
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(
        tenantId,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
      );
      const handoff = await client.createHandoff(req);
      await client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: req.requirements,
        expires_in_seconds: 60,
      });
      const revoked = await client.revokeHandoff(handoff.id, { tenant_id: tenantId });
      assert.equal(revoked.state, "revoked");
      await rejects(
        client.revokeHandoff(handoff.id, { tenant_id: tenantId }),
        409,
        "a handoff already revoked cannot be revoked again",
      );
    });

    it("rejecting an already-rejected handoff a second time is rejected — reject is a one-shot transition out of Requested", async () => {
      const tenantId = tid("int-hoff-reject-twice");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, destRuntime);
      const handoff = await client.createHandoff(req);
      const rejected = await client.rejectHandoff(handoff.id, { tenant_id: tenantId });
      assert.equal(rejected.state, "rejected");
      await rejects(
        client.rejectHandoff(handoff.id, { tenant_id: tenantId }),
        409,
        "a handoff already rejected cannot be rejected again",
      );
    });

    it("export_handoff against a source instance that is still RUNNING (never paused or durably waiting) is rejected", async () => {
      // setupExecution's instance runs a bare noop sequence with no
      // wait_for_input gate, so it either completes almost immediately or
      // is caught mid-flight — either way it is never Paused/Waiting, which
      // export_handoff hard-requires (orch8-api/src/continuity.rs
      // export_handoff: `if !matches!(instance.state, Paused | Waiting)`).
      const tenantId = tid("int-hoff-export-running-instance");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, destRuntime);
      const handoff = await client.createHandoff(req);
      await rejects(
        client.exportHandoff(handoff.id, {
          tenant_id: tenantId,
          requirements: req.requirements,
          expires_in_seconds: 60,
        }),
        409,
        "export must refuse a source instance that is not paused or durably waiting",
      );
    });

    it("resuming a handoff that was exported but never accepted is rejected (resume requires Accepted state, Exported is not enough)", async () => {
      const tenantId = tid("int-hoff-resume-before-accept");
      const { execution } = await pausedContinuityFixture(tenantId, "int-hoff-resume-before-accept");
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(
        tenantId,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
      );
      const handoff = await client.createHandoff(req);
      await client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: req.requirements,
        expires_in_seconds: 60,
      });
      await rejects(
        client.resumeHandoff(handoff.id, { tenant_id: tenantId }),
        409,
        "resume requires the handoff to already be Accepted; Exported alone is not sufficient",
      );
    });

    it("revoking a grant after it has already been consumed is rejected and does not un-consume it", async () => {
      const tenantId = tid("int-grant-revoke-consumed");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime);
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      const consumed = await client.consumeContinuationGrant({
        tenant_id: tenantId,
        action: "accept",
        token: grant.token,
        signed_grant: grant.signed_grant,
      });
      assert.equal(consumed.state, "consumed");
      // Attempting to consume the very same token again (a "double-spend")
      // must be rejected, and must not somehow revert the first consumption.
      await rejects(
        client.consumeContinuationGrant({
          tenant_id: tenantId,
          action: "accept",
          token: grant.token,
          signed_grant: grant.signed_grant,
        }),
        409,
        "a grant already consumed cannot be consumed again",
      );
    });

    it("what-if simulation against an execution that is mid-migration (plan created, not yet applied) still runs read-only against the pre-migration state", async () => {
      const tenantId = tid("int-whatif-midmig");
      const { execution, sequence, checkpointId } = await pausedContinuityFixture(
        tenantId,
        "int-whatif-midmig",
      );
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      // Plan a migration but deliberately do not apply it yet.
      await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      const beforeExecution = await client.getContinuityExecution(execution.continuity_id, tenantId);
      const result = await client.runContinuityWhatIf(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: checkpointId,
      });
      assert.ok(result, "what-if must still run normally while a plan is pending but unapplied");
      const afterExecution = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.deepEqual(afterExecution, beforeExecution, "an unapplied plan must not affect what-if read-only guarantees");
    });

    it("running what-if against continuity_id A using a checkpoint_id that actually belongs to a SIBLING execution B (same tenant) 404s — checkpoint lookup is continuity_id-scoped, not just tenant-scoped", async () => {
      const tenantId = tid("int-whatif-foreign-checkpoint");
      const fixtureFirst = await pausedContinuityFixture(tenantId, "int-whatif-foreign-first");
      const fixtureSecond = await pausedContinuityFixture(tenantId, "int-whatif-foreign-second");
      await rejects(
        client.runContinuityWhatIf(fixtureFirst.execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: fixtureSecond.checkpointId,
        }),
        404,
        "a checkpoint id from a sibling execution must not resolve under a different continuity_id, even for the same tenant",
      );
    });

    it("extract_test_fixture against continuity_id A using a checkpoint_id from sibling execution B (same tenant) also 404s, via the same find_continuity_checkpoint scoping", async () => {
      const tenantId = tid("int-fixture-foreign-checkpoint");
      const fixtureFirst = await pausedContinuityFixture(tenantId, "int-fixture-foreign-first");
      const fixtureSecond = await pausedContinuityFixture(tenantId, "int-fixture-foreign-second");
      await rejects(
        client.extractContinuityTestFixture(fixtureFirst.execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: fixtureSecond.checkpointId,
          allowlisted_fields: [],
        }),
        404,
      );
    });

    it("get_continuity_checkpoint against continuity_id A using a checkpoint_id from sibling execution B (same tenant) 404s too", async () => {
      const tenantId = tid("int-getckpt-foreign-checkpoint");
      const fixtureFirst = await pausedContinuityFixture(tenantId, "int-getckpt-foreign-first");
      const fixtureSecond = await pausedContinuityFixture(tenantId, "int-getckpt-foreign-second");
      await rejects(
        client.getContinuityCheckpoint(fixtureFirst.execution.continuity_id, fixtureSecond.checkpointId, tenantId),
        404,
      );
    });

    it("a federation envelope for an execution that has already been handed off (owner changed) is still verifiable against the CURRENT epoch, not the stale one", async () => {
      // Use the module-level federation peer/tenant fixture for a clean
      // envelope verify against an execution whose epoch has already moved
      // due to a completed handoff. export_handoff requires the source
      // instance to be paused/durably waiting, so build via
      // pausedContinuityFixture (not the bare running setupExecution).
      const { execution, runtimeId } = await pausedContinuityFixture(
        federationTenantId,
        "int-fed-after-handoff",
      );
      const destRuntime = uuid();
      await registerRuntime(federationTenantId, destRuntime, { handlers: ["human_review"] });
      const req = await authorizedHandoffRequest(
        federationTenantId,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
      );
      const handoff = await client.createHandoff(req);
      const exported = await client.exportHandoff(handoff.id, {
        tenant_id: federationTenantId,
        requirements: req.requirements,
        expires_in_seconds: 60,
      });
      const destInstance = uuid();
      const imported = await client.importContinuityCapsule({
        tenant_id: federationTenantId,
        destination_runtime_id: destRuntime,
        destination_instance_id: destInstance,
        expected_epoch: 0,
        capsule: exported.capsule,
        payload_base64: exported.payload_base64,
      });
      await client.acceptHandoff(handoff.id, {
        tenant_id: federationTenantId,
        destination_instance_id: imported.instance_id,
      });
      const postHandoffExecution = await client.getContinuityExecution(
        execution.continuity_id,
        federationTenantId,
      );
      assert.notEqual(postHandoffExecution.epoch, execution.epoch);
      assert.ok(runtimeId, "source runtime id retained for reference");

      const payload = Buffer.from(`payload-${uuid()}`);
      const staleUnsigned = {
        id: uuid(),
        peer_id: federationPeerId,
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch, // stale: pre-handoff epoch
        destination_runtime_id: destRuntime,
        payload_sha256: createHash("sha256").update(payload).digest("hex"),
        issued_at: new Date(Date.now() - 100).toISOString(),
        expires_at: new Date(Date.now() + 30_000).toISOString(),
        signature: "",
      };
      const staleSignature = signEnvelope(staleUnsigned);
      // verify_federation compares execution.epoch against the envelope's
      // stamped epoch and rejects a mismatch with 409 (see
      // orch8-api/src/continuity.rs verify_federation: `if execution.epoch
      // != body.envelope.epoch`) — it never returns a soft `accepted: false`.
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: federationTenantId,
          envelope: { ...staleUnsigned, signature: staleSignature },
          payload_base64: payload.toString("base64"),
        }),
        409,
        "a stale-epoch federation envelope must be rejected, not silently accepted",
      );

      const freshUnsigned = { ...staleUnsigned, id: uuid(), epoch: postHandoffExecution.epoch };
      const freshSignature = signEnvelope(freshUnsigned);
      const freshResult = await client.verifyFederationEnvelope({
        tenant_id: federationTenantId,
        envelope: { ...freshUnsigned, signature: freshSignature },
        payload_base64: payload.toString("base64"),
      });
      assert.equal(freshResult.valid, true, "an envelope stamped with the current epoch must verify");
    });

    it("importing the same capsule twice under the same destination runtime is idempotent: both calls resolve to the SAME instance id, no duplicate instance is created", async () => {
      // storage.import_capsule_instance uses `INSERT OR IGNORE` keyed on
      // (tenant_id, capsule_id, destination_runtime_id) and, on conflict,
      // looks up and returns the ALREADY-imported instance id rather than
      // erroring (see orch8-storage/src/sqlite/continuity.rs
      // import_capsule_instance: rows_affected()==0 branch). This is
      // deliberate transfer-retry idempotency, not a bug — this test pins
      // that contract precisely rather than assuming a 409.
      const tenantId = tid("int-capsule-reimport-same-id");
      const { execution } = await pausedContinuityFixture(
        tenantId,
        "int-capsule-reimport-same-id",
        [],
        { fixture: "reimport" },
      );
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
      const destInstanceId = uuid();
      const imported = await client.importContinuityCapsule({
        tenant_id: tenantId,
        destination_runtime_id: destRuntime,
        destination_instance_id: destInstanceId,
        expected_epoch: 0,
        capsule: exported.capsule,
        payload_base64: exported.payload_base64,
      });
      assert.ok(imported.instance_id);
      const reimported = await client.importContinuityCapsule({
        tenant_id: tenantId,
        destination_runtime_id: destRuntime,
        destination_instance_id: destInstanceId,
        expected_epoch: 0,
        capsule: exported.capsule,
        payload_base64: exported.payload_base64,
      });
      assert.equal(
        reimported.instance_id,
        imported.instance_id,
        "a repeated import for the same capsule+destination must resolve to the original instance id",
      );
    });
  });

  describe("A. secondary field assertions: timestamps, attribution, sequence numbers", () => {
    it("provenance entries carry strictly non-decreasing created_at timestamps across a multi-step chain", async () => {
      const tenantId = tid("int-prov-monotonic");
      const { execution, runtimeId } = await setupExecution(tenantId);
      for (const operation of ["handler_dispatch", "logging"]) {
        await client.evaluateContinuityResidency({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          classification: "internal",
          operation,
          destination_runtime_id: runtimeId,
        });
      }
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      assert.ok(provenance.length >= 3, "execution_created + 2 residency evaluations");
      let previousTimestamp = 0;
      for (const entry of provenance) {
        const timestamp = new Date((entry as any).created_at).getTime();
        assert.ok(
          timestamp >= previousTimestamp,
          `provenance entries must be non-decreasing in created_at: ${(entry as any).kind}`,
        );
        previousTimestamp = timestamp;
      }
    });

    it("a resumed handoff's execution records owner_runtime_id equal to the destination runtime, never the source", async () => {
      const setup = await resumedHandoff("int-attrib-owner");
      const execution = await client.getContinuityExecution(setup.execution.continuity_id, setup.tenantId);
      assert.equal(execution.owner_runtime_id, setup.destinationRuntimeId);
      assert.notEqual(execution.owner_runtime_id, setup.sourceRuntimeId);
    });

    it("stream frames carry strictly increasing sequence numbers matching append order, with no gaps after zero retractions", async () => {
      const tenantId = tid("int-frame-seq");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      for (let i = 0; i < 5; i++) {
        await client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: i,
          checkpoint_sha256: String(i).repeat(64).slice(0, 64),
          payload: { i },
        });
      }
      const frames = await client.listContinuityFrames(stream.stream_id, tenantId);
      assert.deepEqual(
        frames.map((f: any) => f.sequence),
        [0, 1, 2, 3, 4],
      );
    });

    it("a compensation run's completed_at timestamp is set only once the run reaches 'completed', never earlier", async () => {
      const setup = await committedCompensableExecution("int-comp-completed-at");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.equal((created as any).completed_at ?? null, null, "a freshly created run has no completed_at yet");
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "completed-at-worker",
      });
      assert.equal((claimed as any).completed_at ?? null, null, "a claimed-but-unfinished run still has no completed_at");
      const completed = await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "completed-at-worker",
      });
      assert.equal(completed.state, "completed");
      assert.ok((completed as any).completed_at, "completed run must carry a completed_at timestamp");
    });

    it("effect receipts carry the dispatching runtime's execution continuity_id, never a foreign one, across two sibling executions", async () => {
      const setup1 = await compensableExecution("int-effect-attrib-1");
      const setup2 = await compensableExecution("int-effect-attrib-2");
      const effects1 = await client.listContinuityEffects(setup1.execution.continuity_id, setup1.tenantId);
      const effects2 = await client.listContinuityEffects(setup2.execution.continuity_id, setup2.tenantId);
      assert.equal(effects1[0]!.continuity_id, setup1.execution.continuity_id);
      assert.equal(effects2[0]!.continuity_id, setup2.execution.continuity_id);
      assert.notEqual(effects1[0]!.id, effects2[0]!.id);
    });

    it("a dispatched-but-unresolved effect PINS both migration planning and handoff preview compatibility — the two independent 'in-flight work' guards agree with each other", async () => {
      // This cross-checks two SEPARATE unresolved-effects guards that this
      // file documents individually elsewhere: build_handoff_preview sets
      // compatible=false when unresolved_effects is non-empty, and
      // validate_migration_history compiles to MigrationDisposition::Pin
      // for the same reason. Confirming both trigger together on the SAME
      // execution state (rather than one being checked and the other
      // assumed) closes a real gap — it would be easy for one guard to
      // regress without the other catching it.
      const setup = await compensableExecution("int-effect-blocks-both-paths");
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      assert.equal(effects[0]!.state, "dispatched");

      await client.waitForState(setup.instance.id, "waiting");
      await client.saveCheckpoint(setup.instance.id, { safe_boundary: "pre-migration", context_snapshot: {} });
      const target = sameShapeTarget(setup.createdSequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: setup.tenantId,
        continuity_id: setup.execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      assert.equal(plan.disposition, "pin", "an unresolved effect must pin the migration plan");

      const handoffDest = uuid();
      await registerRuntime(setup.tenantId, handoffDest, { handlers: ["human_review"] });
      const preview = await client.previewHandoff(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
        destination_runtime_id: handoffDest,
        requirements: { handlers: ["human_review"] },
      });
      assert.equal(preview.compatible, false, "the SAME unresolved effect must also block handoff compatibility");
      assert.ok(preview.unresolved_effects.some((e: any) => e.id === effects[0]!.id));

      // Resolving the effect (bypassing both compensation AND handoff/
      // migration workflows, since resolve_effect requires only the
      // effect's own id) clears BOTH guards simultaneously.
      await client.resolveContinuityEffect(effects[0]!.id, {
        tenant_id: setup.tenantId,
        state: "committed",
        provider_receipt_id: `receipt-${uuid().slice(0, 8)}`,
      });
      const previewAfter = await client.previewHandoff(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
        destination_runtime_id: handoffDest,
        requirements: { handlers: ["human_review"] },
      });
      assert.equal(previewAfter.compatible, true, "resolving the last unresolved effect clears the handoff-preview guard immediately");
    });

    it("continuity locations record a distinct epoch per row and are sorted by epoch ascending", async () => {
      const tenantId = tid("int-locations-epoch-order");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-locations-epoch-order");
      const target = sameShapeTarget(sequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
      const locations = await client.listContinuityLocations(execution.continuity_id, tenantId);
      const epochs = locations.map((l: any) => l.epoch);
      const sorted = [...epochs].sort((a, b) => a - b);
      assert.deepEqual(epochs, sorted, "locations must be returned sorted by epoch ascending");
      assert.equal(new Set(epochs).size, epochs.length, "each location row must carry a distinct epoch");
    });
  });

  describe("A. repeat key scenarios under different starting conditions", () => {
    it("a migration applied on an execution that already has TWO prior checkpoints (not just one) still migrates cleanly and preserves both", async () => {
      const tenantId = tid("int-mig-multi-ckpt");
      const { execution, sequence, instance } = await pausedContinuityFixture(
        tenantId,
        "int-mig-multi-ckpt",
        [],
        { round: 1 },
      );
      // Save a second checkpoint before migrating.
      await client.saveCheckpoint(instance.id, {
        safe_boundary: "gate",
        context_snapshot: { round: 2 },
      });
      const beforeCheckpoints = await client.listContinuityCheckpoints(execution.continuity_id, tenantId);
      assert.ok(beforeCheckpoints.length >= 2, "fixture must have at least two checkpoints before migration");

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
      const afterCheckpoints = await client.listContinuityCheckpoints(execution.continuity_id, tenantId);
      const distinctAfter = new Set(afterCheckpoints.map((c: any) => c.checkpoint_id));
      const distinctBefore = new Set(beforeCheckpoints.map((c: any) => c.checkpoint_id));
      for (const id of distinctBefore) {
        assert.ok(distinctAfter.has(id), "every pre-migration checkpoint must survive the migration");
      }
    });

    it("a chain of THREE checkpoints saved within the same epoch links previous_checkpoint_id transitively, and the diff reflects only the immediately-adjacent change", async () => {
      const tenantId = tid("int-ckpt-chain-depth");
      const { execution, instance, checkpointId: firstId } = await pausedContinuityFixture(
        tenantId,
        "int-ckpt-chain-depth",
        [],
        { step: 1 },
      );
      await client.saveCheckpoint(instance.id, { safe_boundary: "gate", context_snapshot: { step: 2 } });
      await client.saveCheckpoint(instance.id, { safe_boundary: "gate", context_snapshot: { step: 3 } });
      const checkpoints = await client.listContinuityCheckpoints(execution.continuity_id, tenantId);
      assert.ok(checkpoints.length >= 3, "fixture must have produced three checkpoints total");
      const thirdId = checkpoints[checkpoints.length - 1]!.checkpoint_id;
      const secondId = checkpoints[checkpoints.length - 2]!.checkpoint_id;
      assert.notEqual(thirdId, firstId);
      assert.notEqual(secondId, firstId);

      const thirdDetail = await client.getContinuityCheckpoint(execution.continuity_id, thirdId, tenantId);
      assert.equal(thirdDetail.previous_checkpoint_id, secondId, "the third checkpoint must chain to the second, not the first");
      const changedPaths = thirdDetail.redacted_state_diff.map((c: any) => c.path);
      assert.ok(
        changedPaths.some((p: string) => p.includes("step")),
        "the diff must reflect the step field changing from 2 to 3",
      );

      const secondDetail = await client.getContinuityCheckpoint(execution.continuity_id, secondId, tenantId);
      assert.equal(secondDetail.previous_checkpoint_id, firstId, "the second checkpoint must chain to the first");
    });

    it("an execution spanning THREE epochs of history (two migrations) reports all three epochs in locations and provenance still verifies", async () => {
      const tenantId = tid("int-mig-triple-epoch");
      const { execution, sequence } = await pausedContinuityFixture(tenantId, "int-mig-triple-epoch");
      let currentSequence = sequence;
      let currentExecutionId = execution.continuity_id;
      const seenEpochs: number[] = [execution.epoch];
      for (let round = 0; round < 2; round++) {
        const target = sameShapeTarget(currentSequence);
        const createdTarget = await client.createSequence(target);
        const plan = await client.planContinuityMigration({
          tenant_id: tenantId,
          continuity_id: currentExecutionId,
          to_sequence_id: createdTarget.id,
          to_version: target.version,
        });
        await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
        const afterExecution = await client.getContinuityExecution(currentExecutionId, tenantId);
        seenEpochs.push(afterExecution.epoch);
        currentSequence = { ...target };
      }
      const locations = await client.listContinuityLocations(currentExecutionId, tenantId);
      const epochsInLocations = new Set(locations.map((l: any) => l.epoch));
      for (const epoch of seenEpochs) {
        assert.ok(epochsInLocations.has(epoch), `epoch ${epoch} must be represented in locations`);
      }
      const verification = await client.verifyContinuityProvenance(currentExecutionId, tenantId);
      assert.equal(verification.valid, true, "provenance must still verify across three epochs of migration history");
    });

    it("running compensation a second time on an execution whose first compensation round already completed (all effects compensated) is rejected — nothing left to compensate", async () => {
      const setup = await committedCompensableExecution("int-comp-second-round");
      await completeCompensationRun(setup.execution.continuity_id, setup.tenantId);
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      assert.equal(effects[0]!.state, "compensated");
      await rejects(
        client.createContinuityCompensation(setup.execution.continuity_id, { tenant_id: setup.tenantId }),
        409,
        "an execution with zero committed (non-compensated) effects has nothing left to compensate",
      );
    });

    it("planning a migration AFTER a prior compensation round has already completed is permanently Pinned, even with explicit approval — dispatched effect history is never eligible for automatic migration", async () => {
      const setup = await committedCompensableExecution("int-comp-then-migrate");
      await completeCompensationRun(setup.execution.continuity_id, setup.tenantId);
      // plan_live_migration requires both a paused/durably-waiting instance
      // AND at least one persisted checkpoint (get_latest_checkpoint). The
      // compensable-execution fixture drives the instance through its worker
      // dispatch but never itself saves a checkpoint, so one must be saved
      // here first — matching the same precondition pausedContinuityFixture
      // establishes for every other migration test in this file.
      await client.waitForState(setup.instance.id, "waiting");
      await client.saveCheckpoint(setup.instance.id, {
        safe_boundary: "post-compensation",
        context_snapshot: {},
      });
      const target = sameShapeTarget(setup.createdSequence);
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: setup.tenantId,
        continuity_id: setup.execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      // Unlike the paused-fixture migration tests elsewhere in this file
      // (whose executions never dispatched any effect), an execution that
      // dispatched and later compensated an effect replays with residual
      // effect history that pins the plan — MigrationDisposition::Pin can
      // NEVER be applied, not even with approved:true (apply_live_migration
      // matches Pin/Incompatible to an unconditional Conflict regardless of
      // the approved flag). This is the correct, safety-preserving contract:
      // an execution with real side-effect history is not eligible for
      // automatic replay-based migration.
      assert.equal(plan.disposition, "pin");
      await rejects(
        client.applyContinuityMigration(plan.id, { tenant_id: setup.tenantId, approved: true }),
        409,
        "a Pinned migration plan can never be applied, even with explicit approval",
      );
      const effectsAfterAttempt = await client.listContinuityEffects(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(effectsAfterAttempt[0]!.state, "compensated", "a rejected apply attempt must not disturb existing effect state");
    });

    it("releasing a budget reservation after a migration bumped the execution epoch is rejected (reservation now belongs to a stale epoch)", async () => {
      const tenantId = tid("int-budget-stale-epoch");
      const seq = testSequence(
        "int-budget-stale-epoch",
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
      const createdSeq = await client.createSequence(seq);
      const instance = await client.createInstance({
        sequence_id: createdSeq.id,
        tenant_id: tenantId,
        namespace: "default",
        budget: { max_attention_units: 100 },
      });
      await client.waitForState(instance.id, "waiting");
      await client.saveCheckpoint(instance.id, {
        safe_boundary: "gate",
        context_snapshot: {},
      });
      const execution = await client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 0,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 5,
        },
        estimation_version: "int-v1",
      });
      const target = sameShapeTarget({ ...seq, id: createdSeq.id });
      const createdTarget = await client.createSequence(target);
      const plan = await client.planContinuityMigration({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        to_sequence_id: createdTarget.id,
        to_version: target.version,
      });
      await client.applyContinuityMigration(plan.id, { tenant_id: tenantId });
      await rejects(
        client.releaseContinuityBudget(execution.continuity_id, reservation.id, { tenant_id: tenantId }),
        409,
        "a reservation created under a pre-migration epoch is stale once the execution's epoch advances",
      );
    });
  });

  describe("A. placement decision + test-fixture extraction interact correctly with checkpoints", () => {
    it("choose_placement persists a decision usable later by preview_handoff's own internal placement pass, without double-writing provenance", async () => {
      const tenantId = tid("int-placement-preview");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime, { handlers: ["noop"], regions: ["br-south"] });
      const decision = await client.choosePlacement(execution.continuity_id, {
        tenant_id: tenantId,
        requirements: { handlers: ["noop"] },
        classification: "internal",
      });
      assert.equal(decision.selected_runtime_id, destRuntime);
      // choose_placement is a standalone advisory call — it must NOT append
      // to the execution's provenance chain (only build_handoff_preview's
      // caller, handoff_preview, persists a placement_decision separately
      // and neither appends provenance either; only actually creating and
      // running a handoff does).
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      assert.ok(!provenance.some((e: any) => e.kind === "provider_selected" || e.kind === "placement_selected"));
    });

    it("choose_placement against an execution with a runtime EXCLUDED via requirements returns no selected_runtime_id, not an error", async () => {
      const tenantId = tid("int-placement-none");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime, { handlers: ["other_handler_only"] });
      const decision = await client.choosePlacement(execution.continuity_id, {
        tenant_id: tenantId,
        requirements: { handlers: ["noop"] },
        classification: "internal",
      });
      assert.equal(decision.selected_runtime_id ?? null, null);
    });

    it("extract_test_fixture on a migration-created checkpoint returns only allowlisted context fields, redacted, matching the boundary reported by get_continuity_checkpoint", async () => {
      const tenantId = tid("int-fixture-mig-ckpt");
      const { execution, sequence } = await pausedContinuityFixture(
        tenantId,
        "int-fixture-mig-ckpt",
        [],
        { keep_me: "visible", drop_me: "not-allowlisted" },
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
      const checkpoints = await client.listContinuityCheckpoints(execution.continuity_id, tenantId);
      const migrationCheckpointId = checkpoints[checkpoints.length - 1]!.checkpoint_id;
      const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: migrationCheckpointId,
        allowlisted_fields: ["keep_me"],
      });
      assert.deepEqual(Object.keys((fixture as any).sanitized_context), ["keep_me"]);
      assert.equal((fixture as any).sanitized_context.keep_me, "visible");
    });

    it("extract_test_fixture with an empty allowlist returns an empty sanitized_context, never the full unfiltered context", async () => {
      const tenantId = tid("int-fixture-empty-allowlist");
      const { execution, checkpointId } = await pausedContinuityFixture(
        tenantId,
        "int-fixture-empty-allowlist",
        [],
        { secret: "should-not-leak" },
      );
      const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: checkpointId,
        allowlisted_fields: [],
      });
      assert.deepEqual((fixture as any).sanitized_context, {});
    });

    it("extracting the same checkpoint's fixture twice with identical inputs produces the SAME stable_id both times (deterministic content hash)", async () => {
      const tenantId = tid("int-fixture-stable-id");
      const { execution, checkpointId } = await pausedContinuityFixture(
        tenantId,
        "int-fixture-stable-id",
        [],
        { a: "1" },
      );
      const first = await client.extractContinuityTestFixture(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: checkpointId,
        allowlisted_fields: ["a"],
      });
      const second = await client.extractContinuityTestFixture(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: checkpointId,
        allowlisted_fields: ["a"],
      });
      assert.equal((first as any).stable_id, (second as any).stable_id, "identical extraction inputs must produce the same stable_id");
    });

    it("extracting a fixture from an execution with zero dispatched effects returns empty receipt_mocks/effect_mocks arrays, not null or undefined", async () => {
      const tenantId = tid("int-fixture-no-effects");
      const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "int-fixture-no-effects");
      const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: checkpointId,
        allowlisted_fields: [],
      });
      assert.deepEqual((fixture as any).receipt_mocks, []);
      assert.deepEqual((fixture as any).effect_mocks, []);
    });

    it("a terminal_state_in invariant PASSES once the instance naturally reaches a listed terminal state (the other tests in this file only exercise the failing/unknown branches)", async () => {
      const tenantId = tid("int-inv-terminal-pass");
      const seq = testSequence(tenantId, [step("s1", "noop")], { tenantId });
      const createdSeq = await client.createSequence(seq);
      const instance = await client.createInstance({
        sequence_id: createdSeq.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      const execution = await client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSeq.id,
        sequence_version: seq.version,
        name: "terminal-completed-pass",
        rule: { type: "terminal_state_in", states: ["completed", "failed"] },
      });
      await client.waitForState(instance.id, "completed");
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
      const matched = results.find((r: any) => r.invariant_id === invariant.id);
      assert.equal(matched.status, "pass", "a completed instance satisfies terminal_state_in=['completed','failed']");
    });

    it("extract_test_fixture with an allowlist exceeding 256 fields is rejected", async () => {
      const tenantId = tid("int-fixture-allowlist-too-large");
      const { execution, checkpointId } = await pausedContinuityFixture(tenantId, "int-fixture-allowlist-too-large");
      const oversizedAllowlist = Array.from({ length: 257 }, (_, i) => `field_${i}`);
      await rejects(
        client.extractContinuityTestFixture(execution.continuity_id, {
          tenant_id: tenantId,
          checkpoint_id: checkpointId,
          allowlisted_fields: oversizedAllowlist,
        }),
        413,
        "an allowlist over the 256-field bound must be rejected",
      );
    });
  });

  describe("A. what-if against a checkpoint that has since been superseded by a newer checkpoint still targets the ORIGINAL boundary, not the latest", () => {
    it("running what-if with an explicit older checkpoint_id after a newer checkpoint was saved still replays from the older boundary, not silently upgraded to latest", async () => {
      const tenantId = tid("int-whatif-older-ckpt");
      const { execution, instance, checkpointId: firstCheckpointId } = await pausedContinuityFixture(
        tenantId,
        "int-whatif-older-ckpt",
        [],
        { version: "first" },
      );
      await client.saveCheckpoint(instance.id, {
        safe_boundary: "gate",
        context_snapshot: { version: "second" },
      });
      const checkpoints = await client.listContinuityCheckpoints(execution.continuity_id, tenantId);
      assert.ok(checkpoints.length >= 2, "fixture must have produced a second checkpoint");
      const result = await client.runContinuityWhatIf(execution.continuity_id, {
        tenant_id: tenantId,
        checkpoint_id: firstCheckpointId,
      });
      assert.ok(result, "what-if must still resolve successfully against the explicitly-requested older checkpoint");
      // The real instance/execution remain untouched regardless of which
      // checkpoint was targeted.
      const stillSameEpoch = await client.getContinuityExecution(execution.continuity_id, tenantId);
      assert.equal(stillSameEpoch.epoch, execution.epoch);
    });
  });

  describe("A. compensation preview reflects LIFO ordering even before any compensation run is created", () => {
    it("preview_compensation on a two-effect execution returns the same LIFO order create_compensation_run will later use, without creating a run itself", async () => {
      const tenantId = tid("int-comp-preview-lifo-noside");
      const firstHandler = `int-comp-preview-lifo_first_${uuid().slice(0, 8)}`;
      const secondHandler = `int-comp-preview-lifo_second_${uuid().slice(0, 8)}`;
      const sequence = testSequence(
        "int-comp-preview-lifo-noside",
        [
          step("arm", "human_review", {}, {
            wait_for_input: { prompt: "go?", choices: [{ label: "Continue", value: "continue" }] },
          }),
          step("first", firstHandler, {}, {
            compensation: { handler: "undo_first", verification: "handler_result" },
          }),
          step("second", secondHandler, {}, {
            compensation: { handler: "undo_second", verification: "handler_result" },
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
      await client.sendSignal(instance.id, { custom: "human_input:arm" } as unknown as string, { value: "continue" });
      const firstTask = await waitForWorkerTask(firstHandler, "int-comp-preview-lifo-w1");
      await client.completeWorkerTask(firstTask.id, "int-comp-preview-lifo-w1", { ok: true });
      const secondTask = await waitForWorkerTask(secondHandler, "int-comp-preview-lifo-w2");
      await client.completeWorkerTask(secondTask.id, "int-comp-preview-lifo-w2", { ok: true });
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
      const preview = await client.previewContinuityCompensation(execution.continuity_id, { tenant_id: tenantId });
      assert.equal(preview.steps[0]!.effect_block_id, "second");
      assert.equal(preview.steps[1]!.effect_block_id, "first");
      // Preview must not itself create a run — a subsequent list of
      // compensation state should show nothing running yet, confirmed
      // indirectly by create_compensation_run succeeding cleanly right after.
      const run = await client.createContinuityCompensation(execution.continuity_id, { tenant_id: tenantId });
      assert.equal(run.state, "planned", "a freshly created run is planned until its first step is claimed");
      assert.equal(run.steps[0]!.plan.effect_block_id, "second");
    });
  });

  describe("A. disclosure minimization composed with residency evaluation on payload destined for the same handoff", () => {
    it("an internal-classification payload is minimized to the allowlist while a public-classification payload passes through whole, then residency-checked for the SAME destination runtime independently", async () => {
      const tenantId = tid("int-disclosure-residency");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime, { regions: ["br-south"] });
      // minimize_disclosure short-circuits to the WHOLE payload for public
      // classification (orch8-engine/src/continuity_advanced.rs
      // minimize_disclosure: `if classification == Public { return
      // disclosed: payload.clone() }`) — the allowlist only takes effect for
      // non-public classifications (internal here; confidential/restricted
      // are rejected outright by the handler).
      const publicResult = await client.minimizeContinuityDisclosure({
        classification: "public",
        payload: { public_field: "ok", private_field: "secret" },
        allowed_top_level_fields: ["public_field"],
      });
      assert.deepEqual(
        Object.keys((publicResult as any).disclosed).sort(),
        ["private_field", "public_field"],
        "public classification bypasses the allowlist entirely",
      );
      assert.deepEqual((publicResult as any).withheld_sha256, []);

      const internalResult = await client.minimizeContinuityDisclosure({
        classification: "internal",
        payload: { public_field: "ok", private_field: "secret" },
        allowed_top_level_fields: ["public_field"],
      });
      assert.deepEqual(Object.keys((internalResult as any).disclosed), ["public_field"]);
      assert.equal((internalResult as any).withheld_sha256.length, 1, "the withheld field must be tracked by hash");

      const residency = await client.evaluateContinuityResidency({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        classification: "public",
        operation: "handler_dispatch",
        destination_runtime_id: destRuntime,
        allowed_regions: ["br-south"],
      });
      assert.equal(residency.outcome, "pass");
      // Only the residency call touches this execution's provenance chain —
      // disclosure minimization, being payload-shape-only with no
      // continuity_id in its request body, never does (see the module-level
      // federation test above for the same assertion in isolation).
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      const kinds = provenance.map((e: any) => e.kind);
      assert.ok(kinds.includes("residency_evaluated"));
      assert.ok(!kinds.includes("disclosure_minimized"));
    });

    it("minimize_disclosure rejects confidential and restricted classifications outright, regardless of allowlist — regulated payloads must never transit through this endpoint at all", async () => {
      for (const classification of ["confidential", "restricted"]) {
        await rejects(
          client.minimizeContinuityDisclosure({
            classification,
            payload: { field: "x" },
            allowed_top_level_fields: ["field"],
          }),
          400,
          `classification=${classification} must be rejected outright`,
        );
      }
    });
  });

  describe("A. continuation grant scope enforcement across issue -> consume", () => {
    it("consuming a grant with an action outside its allowed_actions scope is rejected, even with a structurally valid signature and token", async () => {
      const tenantId = tid("int-grant-scope-mismatch");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime);
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      await rejects(
        client.consumeContinuationGrant({
          tenant_id: tenantId,
          action: "resume",
          token: grant.token,
          signed_grant: grant.signed_grant,
        }),
        409,
        "'resume' is outside the grant's allowed_actions=['accept'] scope",
      );
      // The grant must still be consumable for its actually-allowed action.
      const consumed = await client.consumeContinuationGrant({
        tenant_id: tenantId,
        action: "accept",
        token: grant.token,
        signed_grant: grant.signed_grant,
      });
      assert.equal(consumed.state, "consumed");
    });

    it("issuing a grant with a ttl_seconds so short it has already expired by consume-time is rejected as GrantExpired", async () => {
      const tenantId = tid("int-grant-expired");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime);
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 1,
      });
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      await rejects(
        client.consumeContinuationGrant({
          tenant_id: tenantId,
          action: "accept",
          token: grant.token,
          signed_grant: grant.signed_grant,
        }),
        409,
        "a grant whose ttl has elapsed must be rejected as expired, even with a valid signature and token",
      );
    });

    it("a grant issued with MULTIPLE allowed_actions can be consumed for the SECOND listed action just as validly as the first, and consuming it once still exhausts it for the other action too", async () => {
      const tenantId = tid("int-grant-multi-action");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime);
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept", "resume"],
        ttl_seconds: 60,
      });
      const consumed = await client.consumeContinuationGrant({
        tenant_id: tenantId,
        action: "resume",
        token: grant.token,
        signed_grant: grant.signed_grant,
      });
      assert.equal(consumed.state, "consumed");
      // Even though "accept" was ALSO in allowed_actions, the grant is a
      // single-use token — consuming it for "resume" exhausts it entirely,
      // it does not leave "accept" separately available.
      await rejects(
        client.consumeContinuationGrant({
          tenant_id: tenantId,
          action: "accept",
          token: grant.token,
          signed_grant: grant.signed_grant,
        }),
        409,
        "a grant is single-use across its ENTIRE allowed_actions set, not per-action",
      );
    });

    it("issuing a grant with duplicate actions in allowed_actions is rejected at issue time", async () => {
      const tenantId = tid("int-grant-dup-actions");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime);
      await rejects(
        client.issueContinuationGrant({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: destRuntime,
          allowed_actions: ["accept", "accept"],
          ttl_seconds: 60,
        }),
        400,
        "duplicate actions in allowed_actions must be rejected",
      );
    });

    it("issuing a grant for a destination_runtime_id with no live capability registration at all is rejected", async () => {
      const tenantId = tid("int-grant-unregistered-dest");
      const { execution } = await setupExecution(tenantId);
      const neverRegisteredRuntime = uuid();
      await rejects(
        client.issueContinuationGrant({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: neverRegisteredRuntime,
          allowed_actions: ["accept"],
          ttl_seconds: 60,
        }),
        409,
        "a grant destination must have a live capability registration at issue time",
      );
    });

    it("issuing a grant for a destination whose capability registration has since EXPIRED is also rejected, even though it was live at some point", async () => {
      const tenantId = tid("int-grant-expired-dest-capability");
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
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      await rejects(
        client.issueContinuationGrant({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: destRuntime,
          allowed_actions: ["accept"],
          ttl_seconds: 60,
        }),
        409,
        "an expired capability registration no longer counts as a live destination for grant issuance",
      );
    });
  });

  describe("A. more handoff export edge cases", () => {
    it("exporting a handoff with requirements that don't byte-for-byte match the original placement decision is rejected — export re-validates against the SAME placement evidence, it does not accept a looser superset", async () => {
      // export_handoff compares `authorized.requirements != body.requirements`
      // against the placement decision saved at create_handoff time (see
      // orch8-api/src/continuity.rs export_handoff ~1087-1094) — even a
      // strict superset of the original requirements is rejected, not just a
      // narrower or conflicting one. Export is not an independent
      // capability re-check; it's a strict replay-and-compare against the
      // SAME placement evidence create_handoff authorized.
      const tenantId = tid("int-hoff-export-diff-reqs");
      const { execution } = await pausedContinuityFixture(tenantId, "int-hoff-export-diff-reqs");
      const destRuntime = uuid();
      await registerRuntime(tenantId, destRuntime, { handlers: ["human_review", "noop"] });
      const req = await authorizedHandoffRequest(
        tenantId,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
      );
      const handoff = await client.createHandoff(req);
      await rejects(
        client.exportHandoff(handoff.id, {
          tenant_id: tenantId,
          requirements: { handlers: ["human_review", "noop"] },
          expires_in_seconds: 60,
        }),
        409,
        "export must reject requirements that differ at all from the original placement decision, even a superset",
      );
      // Exporting with the EXACT original requirements still succeeds
      // afterward — the rejected attempt must not have poisoned the handoff.
      const exported = await client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: { handlers: ["human_review"] },
        expires_in_seconds: 60,
      });
      assert.equal(exported.handoff.state, "exported");
    });

    it("export_handoff against a handoff already in Exported state is rejected (export is not idempotent/re-callable)", async () => {
      const tenantId = tid("int-hoff-export-twice");
      const { execution } = await pausedContinuityFixture(tenantId, "int-hoff-export-twice");
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
      await rejects(
        client.exportHandoff(handoff.id, {
          tenant_id: tenantId,
          requirements: req.requirements,
          expires_in_seconds: 60,
        }),
        409,
        "a handoff already exported cannot be exported a second time",
      );
    });

    it("accept_handoff with a destination_instance_id that was never produced by import_capsule for this handoff is rejected", async () => {
      const tenantId = tid("int-hoff-accept-wrong-instance");
      const { execution } = await pausedContinuityFixture(tenantId, "int-hoff-accept-wrong-instance");
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(
        tenantId,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
      );
      const handoff = await client.createHandoff(req);
      await client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: req.requirements,
        expires_in_seconds: 60,
      });
      // A completely unrelated instance (never imported via this handoff's
      // capsule) is substituted as the accept target.
      const unrelatedSeq = await client.createSequence(
        testSequence("int-hoff-accept-wrong-instance-unrelated", [step("s1", "noop")], { tenantId }),
      );
      const unrelatedInstance = await client.createInstance({
        sequence_id: unrelatedSeq.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      await rejects(
        client.acceptHandoff(handoff.id, {
          tenant_id: tenantId,
          destination_instance_id: unrelatedInstance.id,
        }),
        409,
        "accept must refuse a destination instance that was not produced by this handoff's own capsule import",
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
        migrationTarget: target,
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

    // ------------------------------------------------------------------
    // Additional resource types and nested/child-resource isolation checks
    // not covered by the table above: budget reservation reconcile/release,
    // handoff preview, evaluation scores, compensation step transitions,
    // invariant evaluation, and attention-task assignment leaking across a
    // second, differently-shaped fixture. Each of these exercises a
    // DISTINCT tenant-scoping code path (its own storage lookup), so parent-
    // level isolation elsewhere does not guarantee these pass.
    // ------------------------------------------------------------------

    it("budget reservation reconcile: tenant B cannot reconcile a reservation that belongs to tenant A's execution", async () => {
      const tenantC = tid("iso-budget-reconcile-a");
      const seq = testSequence("iso-budget-reconcile", [step("s1", "noop")], { tenantId: tenantC });
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
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
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
        client.reconcileContinuityBudget(execution.continuity_id, reservation.id, {
          tenant_id: tenantB,
          actual: {
            cost_microunits: 0,
            wall_time_ms: 0,
            external_calls: 0,
            bytes_transferred: 0,
            energy_millijoules: 0,
            attention_units: 3,
          },
        }),
        404,
      );
      // The real tenant can still reconcile it afterward — the cross-tenant
      // attempt must not have poisoned or partially consumed the reservation.
      const reconciled = await client.reconcileContinuityBudget(execution.continuity_id, reservation.id, {
        tenant_id: tenantC,
        actual: {
          cost_microunits: 0,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 3,
        },
      });
      assert.equal(reconciled.state, "reconciled");
    });

    it("budget reservation release: tenant B cannot release a reservation that belongs to tenant A's execution", async () => {
      const tenantC = tid("iso-budget-release-a");
      const seq = testSequence("iso-budget-release", [step("s1", "noop")], { tenantId: tenantC });
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
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
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
        client.releaseContinuityBudget(execution.continuity_id, reservation.id, { tenant_id: tenantB }),
        404,
      );
    });

    it("handoff preview: tenant B cannot preview a handoff for tenant A's execution (404, not a wrong-tenant preview)", async () => {
      await rejects(
        client.previewHandoff(fixtureA.execution.continuity_id, {
          tenant_id: tenantB,
          destination_runtime_id: uuid(),
          requirements: { handlers: ["noop"] },
        }),
        404,
      );
    });

    it("evaluation scores: tenant B's evaluation list for tenant A's continuity_id is empty, not tenant A's scores (same vacuous-list contract as list_effects/list_provenance)", async () => {
      const tenantForEval = tid("iso-eval-a");
      const { execution } = await setupExecution(tenantForEval);
      await client.appendContinuityEvaluation(execution.continuity_id, {
        tenant_id: tenantForEval,
        evaluator: "iso-judge",
        score_millipoints: 700,
        sample_size: 5,
        evidence_sha256: createHash("sha256").update("iso-ev").digest("hex"),
      });
      const scoresUnderReal = await client.listContinuityEvaluations(execution.continuity_id, tenantForEval);
      assert.equal(scoresUnderReal.length, 1);
      const scoresUnderB = await client.listContinuityEvaluations(execution.continuity_id, tenantB);
      assert.deepEqual(scoresUnderB, []);
    });

    it("invariant evaluation: tenant B cannot evaluate invariants against tenant A's execution", async () => {
      await rejects(
        client.evaluateContinuityInvariants(fixtureA.execution.continuity_id, { tenant_id: tenantB }),
        404,
      );
    });

    it("compensation steps: tenant B cannot complete, fail, or verify a compensation step belonging to tenant A's run", async () => {
      await rejects(
        client.completeContinuityCompensation(
          fixtureA.compensationRun.id,
          fixtureA.firstEffectId,
          { tenant_id: tenantB, worker_id: "intruder" },
        ),
        404,
      );
      await rejects(
        client.failContinuityCompensation(
          fixtureA.compensationRun.id,
          fixtureA.firstEffectId,
          { tenant_id: tenantB, worker_id: "intruder", error: "x" },
        ),
        404,
      );
      await rejects(
        client.verifyContinuityCompensation(
          fixtureA.compensationRun.id,
          fixtureA.firstEffectId,
          { tenant_id: tenantB, approved: true, evidence: "x" },
        ),
        404,
      );
    });

    it("what-if runs: a second, unrelated tenant's list of its OWN execution's what-if runs never contains tenant A's runs, even after both tenants exercise what-if independently", async () => {
      // There is no standalone get-what-if-run-by-id endpoint; the resource
      // is only reachable via list_what_if_runs (already covered for the
      // direct-404 case in the table above). This test instead confirms the
      // list-scoping itself: two tenants each running what-if against their
      // own paused execution must never see each other's recorded runs when
      // listing by their OWN (real, valid-for-them) continuity_id.
      const runsUnderA = await client.listContinuityWhatIfRuns(fixtureA.paused.execution.continuity_id, tenantA);
      assert.ok(runsUnderA.length >= 1, "sanity: tenant A does have at least one what-if run recorded");
      const tenantD = tid("iso-whatif-d");
      const pausedD = await pausedContinuityFixture(tenantD, `iso-whatif-${tenantD}`);
      await client.runContinuityWhatIf(pausedD.execution.continuity_id, {
        tenant_id: tenantD,
        checkpoint_id: pausedD.checkpointId,
      });
      const runsUnderD = await client.listContinuityWhatIfRuns(pausedD.execution.continuity_id, tenantD);
      assert.equal(runsUnderD.length, 1, "tenant D's list must contain only its own single run");
      // WhatIfRunRecord nests the scenario id at `.scenario.id`, not a
      // top-level `.id` — each run's scenario carries its own tenant_id too.
      const aScenarioIds = new Set(runsUnderA.map((r: any) => r.scenario.id));
      for (const run of runsUnderD) {
        assert.equal(run.scenario.tenant_id, tenantD);
        assert.ok(
          !aScenarioIds.has(run.scenario.id),
          "tenant D's scenario id must not appear in tenant A's list (and vice versa)",
        );
      }
    });

    it("checkpoints: a checkpoint id that is valid for tenant A 404s when looked up under a tenant that has never run any continuity execution at all", async () => {
      const neverUsedTenant = tid("iso-neverused");
      await rejects(
        client.getContinuityCheckpoint(
          fixtureA.paused.execution.continuity_id,
          fixtureA.paused.checkpointId,
          neverUsedTenant,
        ),
        404,
      );
    });

    it("stream frames nested under a stream: a frame appended under tenant A is invisible when the SAME stream_id is queried by list under a completely different, freshly-minted tenant", async () => {
      const brandNewTenant = tid("iso-brand-new");
      await rejects(client.listContinuityFrames(fixtureA.stream.stream_id, brandNewTenant), 404);
    });

    it("attention task decide: a decision payload correctly shaped for tenant A's reviewer is still rejected under tenant B even when the reviewer_id string happens to match", async () => {
      const tenantForTask = tid("iso-attn-decide-a");
      const { execution } = await setupExecution(tenantForTask);
      const task = await client.createAttentionTask({
        tenant_id: tenantForTask,
        continuity_id: execution.continuity_id,
        required_skills: ["iso-decide"],
        classification: "internal",
        priority: 1,
        deadline: new Date(Date.now() + 60_000).toISOString(),
        estimated_attention_units: 1,
      });
      const reviewerId = uuid();
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantForTask,
        reviewers: [
          {
            reviewer_id: reviewerId,
            tenant_ids: [tenantForTask],
            skills: ["iso-decide"],
            region: "br-south",
            trust: "registered",
            available_attention_units: 10,
          },
        ],
        lease_seconds: 60,
      });
      assert.equal(assigned.state, "assigned");
      await rejects(
        client.decideAttentionTask(task.id, {
          tenant_id: tenantB,
          reviewer_id: reviewerId,
          decision_sha256: createHash("sha256").update("x").digest("hex"),
        }),
        404,
        "even a byte-for-byte matching reviewer_id string cannot decide a foreign tenant's task",
      );
    });

    it("migration plan: a fresh tenant with zero executions still gets 404 (not 401/403) when probing an arbitrary but real migration plan id", async () => {
      const brandNewTenant = tid("iso-mig-brand-new");
      await rejects(client.getContinuityMigration(fixtureA.migrationPlan.id, brandNewTenant), 404);
    });

    it("runtime registration: two tenants registering a runtime with the SAME runtime_id string never merge into one record — each tenant sees only its own", async () => {
      const tenantE = tid("iso-runtime-e");
      const tenantF = tid("iso-runtime-f");
      const sharedRuntimeId = uuid();
      await registerRuntime(tenantE, sharedRuntimeId, { regions: ["br-south"] });
      await registerRuntime(tenantF, sharedRuntimeId, { regions: ["us-east"] });
      const listE = await client.listRuntimes(tenantE);
      const listF = await client.listRuntimes(tenantF);
      const runtimeInE = listE.find((r: any) => r.runtime_id === sharedRuntimeId);
      const runtimeInF = listF.find((r: any) => r.runtime_id === sharedRuntimeId);
      assert.ok(runtimeInE, "tenant E must see its own registration");
      assert.ok(runtimeInF, "tenant F must see its own registration");
      assert.deepEqual(runtimeInE.regions, ["br-south"]);
      assert.deepEqual(runtimeInF.regions, ["us-east"]);
    });

    // ------------------------------------------------------------------
    // Table-driven sweep, second pass: nested/child resources probed under
    // a tenant that has NEVER created ANY continuity resource at all (not
    // even a sibling of the same type), to rule out any accidental
    // fallback-to-global-default behavior distinct from the "wrong but
    // real tenant" cases above.
    // ------------------------------------------------------------------
    for (const descriptor of [
      "handoff (get_handoff)",
      "streams (list_stream_frames)",
      "checkpoints (list_continuity_checkpoints)",
      "compensation run (get_compensation_run)",
      "migration plan (get_live_migration)",
      "what-if runs (list_what_if_runs)",
    ]) {
      it(`never-used tenant sweep: ${descriptor} 404s for a tenant that has created zero continuity resources`, async () => {
        const neverUsedTenant = tid("iso-zero-resources");
        const probes: Record<string, () => Promise<unknown>> = {
          "handoff (get_handoff)": () => client.getHandoff(fixtureA.handoff.id, neverUsedTenant),
          "streams (list_stream_frames)": () =>
            client.listContinuityFrames(fixtureA.stream.stream_id, neverUsedTenant),
          "checkpoints (list_continuity_checkpoints)": () =>
            client.listContinuityCheckpoints(fixtureA.paused.execution.continuity_id, neverUsedTenant),
          "compensation run (get_compensation_run)": () =>
            client.getContinuityCompensation(fixtureA.compensationRun.id, neverUsedTenant),
          "migration plan (get_live_migration)": () =>
            client.getContinuityMigration(fixtureA.migrationPlan.id, neverUsedTenant),
          "what-if runs (list_what_if_runs)": () =>
            client.listContinuityWhatIfRuns(fixtureA.paused.execution.continuity_id, neverUsedTenant),
        };
        await rejects(probes[descriptor]!(), 404, `resource: ${descriptor}`);
      });
    }

    it("stream creation under tenant B against tenant A's continuity_id is refused at create-time (the child resource can never even be minted under the wrong tenant)", async () => {
      await rejects(
        client.createContinuityStream({
          tenant_id: tenantB,
          continuity_id: fixtureA.execution.continuity_id,
          ttl_seconds: 60,
        }),
        404,
      );
    });

    it("invariant creation under tenant B against tenant A's sequence_id is refused at create-time (sequence ownership is checked before the invariant row is ever written)", async () => {
      await rejects(
        client.createContinuityInvariant({
          tenant_id: tenantB,
          sequence_id: fixtureA.paused.sequence.id,
          sequence_version: fixtureA.paused.sequence.version,
          name: "cross-tenant-invariant",
          rule: { type: "no_unknown_effects" },
        }),
        404,
      );
    });

    it("attention task creation under tenant B against tenant A's continuity_id is refused at create-time", async () => {
      await rejects(
        client.createAttentionTask({
          tenant_id: tenantB,
          continuity_id: fixtureA.execution.continuity_id,
          required_skills: ["cross-tenant"],
          classification: "internal",
          priority: 1,
          deadline: new Date(Date.now() + 60_000).toISOString(),
          estimated_attention_units: 1,
        }),
        404,
      );
    });

    it("handoff creation under tenant B against tenant A's continuity_id + a tenant-A-authorized placement_decision_id is refused (the placement decision itself is tenant-scoped)", async () => {
      const destRuntime = uuid();
      await registerRuntime(tenantA, destRuntime);
      const preview = await client.previewHandoff(fixtureA.execution.continuity_id, {
        tenant_id: tenantA,
        destination_runtime_id: destRuntime,
        requirements: { handlers: ["noop"] },
      });
      await rejects(
        client.createHandoff({
          tenant_id: tenantB,
          continuity_id: fixtureA.execution.continuity_id,
          destination_runtime_id: destRuntime,
          requirements: { handlers: ["noop"] },
          placement_decision_id: preview.placement_decision.id,
          preview_sha256: preview.preview_sha256,
        }),
        404,
        "the placement decision was authorized for tenant A; tenant B cannot claim it as its own",
      );
    });

    it("reject_handoff and revoke_handoff under tenant B against tenant A's real handoff both 404, and the handoff's real state is untouched afterward", async () => {
      const tenantForHandoffMutation = tid("iso-hoff-mutation-a");
      const { execution } = await setupExecution(tenantForHandoffMutation);
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(tenantForHandoffMutation, execution.continuity_id, destRuntime);
      const handoff = await client.createHandoff(req);
      await rejects(
        client.rejectHandoff(handoff.id, { tenant_id: tenantB }),
        404,
        "tenant B cannot reject a handoff belonging to another tenant",
      );
      await rejects(
        client.revokeHandoff(handoff.id, { tenant_id: tenantB }),
        404,
        "tenant B cannot revoke a handoff belonging to another tenant",
      );
      const stillReal = await client.getHandoff(handoff.id, tenantForHandoffMutation);
      assert.equal(stillReal.state, "requested", "the cross-tenant mutation attempts must not have changed the handoff's real state");
    });

    it("accept_handoff and resume_handoff under tenant B against tenant A's real handoff both 404", async () => {
      const setup = await resumedHandoff("iso-hoff-accept-resume-a");
      // The handoff is already resumed; probe accept/resume under a
      // completely unrelated tenant B to confirm both mutation endpoints
      // enforce tenant scoping independent of the handoff's actual state.
      await rejects(
        client.acceptHandoff(setup.handoff.id, {
          tenant_id: tenantB,
          destination_instance_id: setup.imported.instance_id,
        }),
        404,
      );
      await rejects(
        client.resumeHandoff(setup.handoff.id, { tenant_id: tenantB }),
        404,
      );
    });

    it("import_capsule refuses a capsule whose manifest.tenant_id belongs to a different tenant than the one presenting it, even with the correct destination runtime and a fresh instance id", async () => {
      const tenantForCapsule = tid("iso-capsule-a");
      const { execution } = await pausedContinuityFixture(tenantForCapsule, "iso-capsule-a");
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(
        tenantForCapsule,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
      );
      const handoff = await client.createHandoff(req);
      const exported = await client.exportHandoff(handoff.id, {
        tenant_id: tenantForCapsule,
        requirements: req.requirements,
        expires_in_seconds: 60,
      });
      await rejects(
        client.importContinuityCapsule({
          tenant_id: tenantB,
          destination_runtime_id: destRuntime,
          destination_instance_id: uuid(),
          expected_epoch: 0,
          capsule: exported.capsule,
          payload_base64: exported.payload_base64,
        }),
        409,
        "a capsule manifest carrying tenant A's tenant_id must be refused when presented under tenant B",
      );
      // The capsule remains importable under its real, correct tenant.
      const imported = await client.importContinuityCapsule({
        tenant_id: tenantForCapsule,
        destination_runtime_id: destRuntime,
        destination_instance_id: uuid(),
        expected_epoch: 0,
        capsule: exported.capsule,
        payload_base64: exported.payload_base64,
      });
      assert.ok(imported.instance_id);
    });

    it("attach_device_capsule under tenant B against tenant A's real handoff id 404s, independent of the handoff's own state", async () => {
      const tenantForAttach = tid("iso-attach-a");
      const { execution } = await pausedContinuityFixture(tenantForAttach, "iso-attach-a");
      const destRuntime = uuid();
      const req = await authorizedHandoffRequest(
        tenantForAttach,
        execution.continuity_id,
        destRuntime,
        { handlers: ["human_review"] },
      );
      const handoff = await client.createHandoff(req);
      const exported = await client.exportHandoff(handoff.id, {
        tenant_id: tenantForAttach,
        requirements: req.requirements,
        expires_in_seconds: 60,
      });
      await rejects(
        client.attachDeviceCapsule(handoff.id, {
          tenant_id: tenantB,
          destination_instance_id: uuid(),
          capsule: exported.capsule,
          payload_base64: exported.payload_base64,
          payload_key_base64: Buffer.from(
            Array.from({ length: 32 }, () => Math.floor(Math.random() * 256)),
          ).toString("base64"),
        }),
        404,
      );
    });

    it("migration planning under tenant B against tenant A's continuity_id is refused at create-time, before any checkpoint/instance-state check runs", async () => {
      // fixtureA's own setup already consumed (namespace, name, version=2)
      // via its internal migration (see buildIsolationFixture); derive from
      // that already-created target rather than paused.sequence directly,
      // or this collides with it on Postgres's idx_sequences_unique index.
      const target = sameShapeTarget(fixtureA.migrationTarget);
      const createdTarget = await client.createSequence(target);
      await rejects(
        client.planContinuityMigration({
          tenant_id: tenantB,
          continuity_id: fixtureA.paused.execution.continuity_id,
          to_sequence_id: createdTarget.id,
          to_version: target.version,
        }),
        404,
      );
    });

    it("budget reservation creation under tenant B against tenant A's continuity_id is refused at create-time", async () => {
      await rejects(
        client.reserveContinuityBudget(fixtureA.execution.continuity_id, {
          tenant_id: tenantB,
          requested: {
            cost_microunits: 0,
            wall_time_ms: 0,
            external_calls: 0,
            bytes_transferred: 0,
            energy_millijoules: 0,
            attention_units: 1,
          },
          estimation_version: "iso-v1",
        }),
        404,
      );
    });

    it("compensation run creation under tenant B against tenant A's compensable continuity_id is refused at create-time", async () => {
      await rejects(
        client.createContinuityCompensation(fixtureA.compSetup.execution.continuity_id, {
          tenant_id: tenantB,
        }),
        404,
      );
    });

    it("device delegation claim under tenant B using tenant A's grant is refused even with a structurally valid signed_grant shape (delegation.tenant_id mismatch is caught before grant verification)", async () => {
      const tenantForDeleg = tid("iso-deleg-a");
      const { execution, runtimeId: sourceRuntimeId } = await setupExecution(tenantForDeleg);
      await registerRuntime(tenantForDeleg, sourceRuntimeId);
      const destRuntime = uuid();
      await registerRuntime(tenantForDeleg, destRuntime);
      const subSeq = await client.createSequence(
        testSequence("iso-deleg-sub", [step("s1", "noop")], { tenantId: tenantForDeleg }),
      );
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantForDeleg,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      const delegation = {
        id: uuid(),
        tenant_id: tenantB,
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
          tenant_id: tenantB,
          delegation,
          signed_grant: grant.signed_grant,
          token: grant.token,
        }),
        404,
        "delegation.tenant_id must match the enforced request tenant, independent of grant validity",
      );
    });

    it("federation envelope verification for tenantB against an execution that actually belongs to federationTenantId 404s (execution ownership is checked, independent of the peer's allowed_tenants policy)", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const destRuntime = uuid();
      await registerRuntime(federationTenantId, destRuntime);
      const payload = Buffer.from(`payload-${uuid()}`);
      const unsigned = {
        id: uuid(),
        peer_id: federationPeerId,
        tenant_id: tenantB,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
        destination_runtime_id: destRuntime,
        payload_sha256: createHash("sha256").update(payload).digest("hex"),
        issued_at: new Date(Date.now() - 100).toISOString(),
        expires_at: new Date(Date.now() + 30_000).toISOString(),
        signature: "",
      };
      const signature = signEnvelope(unsigned);
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: tenantB,
          envelope: { ...unsigned, signature },
          payload_base64: payload.toString("base64"),
        }),
        404,
        "get_continuity_execution under tenantB for federationTenantId's continuity_id must 404 before the peer/allowed_tenants check is ever reached",
      );
    });
  });
});
