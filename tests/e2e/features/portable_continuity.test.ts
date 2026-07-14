/** E2E: portable continuity identity, capabilities, preview, and handoff request. */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  createCipheriv,
  createDecipheriv,
  createHash,
  generateKeyPairSync,
  randomBytes,
  sign,
} from "node:crypto";
import { rmSync } from "node:fs";

import { ApiError, Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { WorkerTask } from "../types.ts";

const client = new Orch8Client();
const artifactDir = `/tmp/o8-continuity-e2e-${uuid().slice(0, 8)}`;

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, child]) => `${JSON.stringify(key)}:${canonicalJson(child)}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

function capsuleAad(
  tenantId: string,
  continuityId: string,
  epoch: number,
  destinationRuntimeId: string,
): Buffer {
  return Buffer.from(
    `orch8-capsule-v1\0${tenantId}\0${continuityId}\0${epoch}\0${destinationRuntimeId}`,
  );
}

function openCapsule(sealedBase64: string, keyBase64: string, aad: Buffer): any {
  const sealed = Buffer.from(sealedBase64, "base64");
  const decipher = createDecipheriv(
    "aes-256-gcm",
    Buffer.from(keyBase64, "base64"),
    sealed.subarray(0, 12),
  );
  decipher.setAAD(aad);
  decipher.setAuthTag(sealed.subarray(sealed.length - 16));
  return JSON.parse(
    Buffer.concat([
      decipher.update(sealed.subarray(12, sealed.length - 16)),
      decipher.final(),
    ]).toString("utf8"),
  );
}

function sealCapsule(payload: unknown, key: Buffer, aad: Buffer): Buffer {
  const nonce = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, nonce);
  cipher.setAAD(aad);
  const ciphertext = Buffer.concat([
    cipher.update(canonicalJson(payload)),
    cipher.final(),
  ]);
  return Buffer.concat([nonce, ciphertext, cipher.getAuthTag()]);
}

async function waitForWorkerTask(
  handler: string,
  workerId: string,
  timeoutMs = 10_000,
): Promise<WorkerTask> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const [task] = await client.pollWorkerTasks(handler, workerId);
    if (task) return task;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`Timeout waiting for worker task for ${handler}`);
}

describe("Portable Continuity", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "42".repeat(32),
        ORCH8_ARTIFACT_BACKEND: "local",
        ORCH8_ARTIFACT_PATH: artifactDir,
        ORCH8_LOG_LEVEL: "error",
      },
    });
  });

  after(async () => {
    await stopServer(server);
    try {
      rmSync(artifactDir, { recursive: true, force: true });
    } catch {
      // Best-effort cleanup must not hide test failures.
    }
  });

  it("creates a tenant-scoped identity and a preview-bound handoff request", async () => {
    const tenantId = `continuity-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "portable",
      [step("work", "noop")],
      { tenantId },
    );
    const createdSequence = await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: createdSequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const sourceRuntimeId = uuid();
    const destinationRuntimeId = uuid();
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: sourceRuntimeId,
    });
    assert.equal(execution.epoch, 0);
    assert.equal(execution.owner_runtime_id, sourceRuntimeId);
    await assert.rejects(
      () =>
        client.createContinuityExecution({
          tenant_id: tenantId,
          instance_id: instance.id,
          runtime_id: uuid(),
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );

    const now = Date.now();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: {
        runtime_id: destinationRuntimeId,
        kind: "mobile",
        trust: "registered",
        handlers: ["camera", "noop"],
        regions: ["br-south"],
        hardware: ["camera"],
        offline_capable: true,
        observed_at: new Date(now).toISOString(),
        expires_at: new Date(now + 60_000).toISOString(),
      },
    });
    const runtimes = await client.listRuntimes(tenantId);
    assert.ok(runtimes.some((runtime) => runtime.runtime_id === destinationRuntimeId));

    const issuedGrant = await client.issueContinuationGrant({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      destination_runtime_id: destinationRuntimeId,
      allowed_actions: ["accept"],
      subject: "e2e-device",
      ttl_seconds: 60,
    });
    const consumedGrant = await client.consumeContinuationGrant({
      tenant_id: tenantId,
      action: "accept",
      token: issuedGrant.token,
      signed_grant: issuedGrant.signed_grant,
    });
    assert.equal(consumedGrant.state, "consumed");
    await assert.rejects(
      () =>
        client.consumeContinuationGrant({
          tenant_id: tenantId,
          action: "accept",
          token: issuedGrant.token,
          signed_grant: issuedGrant.signed_grant,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );

    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: {
        handlers: ["camera"],
        regions: ["br-south"],
        hardware: ["camera"],
        minimum_trust: "registered",
      },
    });
    assert.equal(preview.compatible, true);
    assert.deepEqual(preview.unresolved_effects, []);

    const placement = await client.choosePlacement(execution.continuity_id, {
      tenant_id: tenantId,
      requirements: { handlers: ["camera"] },
      classification: "internal",
    });
    assert.equal(placement.selected_runtime_id, destinationRuntimeId);

    await assert.rejects(
      () =>
        client.createHandoff({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: destinationRuntimeId,
          requirements: {
            handlers: ["camera"],
            regions: ["br-south"],
            hardware: ["camera"],
            minimum_trust: "registered",
          },
          preview_sha256: "a".repeat(64),
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );

    const handoff = await client.createHandoff({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      destination_runtime_id: destinationRuntimeId,
      requirements: {
        handlers: ["camera"],
        regions: ["br-south"],
        hardware: ["camera"],
        minimum_trust: "registered",
      },
      preview_sha256: preview.preview_sha256,
    });
    assert.equal(handoff.state, "requested");
    assert.equal(handoff.expected_epoch, 0);
    assert.equal(
      (await client.getHandoff(handoff.id, tenantId)).destination_runtime_id,
      destinationRuntimeId,
    );

    const stream = await client.createContinuityStream({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      ttl_seconds: 60,
    });
    for (const sequenceNumber of [0, 1]) {
      await client.appendContinuityFrame(stream.stream_id, {
        tenant_id: tenantId,
        sequence: sequenceNumber,
        checkpoint_sha256: "b".repeat(64),
        payload: { token: `part-${sequenceNumber}` },
      });
    }
    assert.equal(
      (await client.listContinuityFrames(stream.stream_id, tenantId, 0))[0].payload.token,
      "part-1",
    );
    assert.equal(
      (
        await client.retractContinuityFrames(stream.stream_id, {
          tenant_id: tenantId,
          epoch: execution.epoch,
          after_sequence: 0,
        })
      ).retracted,
      1,
    );
    assert.equal(
      (await client.listContinuityFrames(stream.stream_id, tenantId, 0))[0].state,
      "retracted",
    );

    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "no unresolved effects",
      rule: { type: "no_unknown_effects" },
    });
    const invariantResults = await client.evaluateContinuityInvariants(
      execution.continuity_id,
      { tenant_id: tenantId },
    );
    assert.equal(invariantResults[0].status, "pass");

    const evaluation = {
      tenant_id: tenantId,
      evaluator: "e2e-quality",
      score_millipoints: 900,
      sample_size: 1,
      deferred: false,
      evidence_sha256: "c".repeat(64),
    };
    await client.appendContinuityEvaluation(execution.continuity_id, evaluation);
    await assert.rejects(
      () => client.appendContinuityEvaluation(execution.continuity_id, evaluation),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );

    const attention = await client.createAttentionTask({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      required_skills: ["fraud"],
      classification: "confidential",
      allowed_regions: ["br-south"],
      priority: 2,
      deadline: new Date(Date.now() + 60_000).toISOString(),
      estimated_attention_units: 2,
    });
    const assigned = await client.assignAttentionTask(attention.id, {
      tenant_id: tenantId,
      lease_seconds: 60,
      reviewers: [
        {
          reviewer_id: "reviewer-a",
          tenant_ids: [tenantId],
          skills: ["fraud"],
          region: "br-south",
          trust: "signed",
          available_attention_units: 10,
        },
      ],
    });
    assert.equal(assigned.assignee, "reviewer-a");

    const checkpoint = await client.saveCheckpoint(instance.id, {
      safe_boundary: "work",
      context_snapshot: {
        order_id: "order-1",
        password: "must-not-leak",
      },
    });
    const boundaries = await client.listContinuityCheckpoints(
      execution.continuity_id,
      tenantId,
    );
    assert.equal(boundaries[0].block_id, "work");
    const whatIf = await client.runContinuityWhatIf(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpoint.id,
      context_patch: { alternate: true },
      output_overrides: {},
      handler_mocks: { work: { simulated: true } },
      max_ticks: 100,
    });
    assert.equal(whatIf.scenario.virtual_time, true);
    assert.equal(whatIf.scenario.effect_mode, "blocked");
    const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpoint.id,
      allowlisted_fields: ["order_id", "password"],
    });
    assert.equal(fixture.sanitized_context.order_id, "order-1");
    assert.equal(fixture.sanitized_context.password, "[REDACTED]");

    await assert.rejects(
      () => client.getContinuityExecution(execution.continuity_id, `${tenantId}-other`),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("returns an ordered runtime and epoch location history", async () => {
    const tenantId = `continuity-locations-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "portable-locations",
      [
        step("handoff", "human_review", {}, {
          wait_for_input: {
            prompt: "Continue on another runtime?",
            choices: [{ label: "Continue", value: "continue" }],
          },
        }),
      ],
      { tenantId },
    );
    const createdSequence = await client.createSequence(sequence);
    const sourceInstance = await client.createInstance({
      sequence_id: createdSequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(sourceInstance.id, "waiting");
    await client.saveCheckpoint(sourceInstance.id, {
      safe_boundary: "handoff",
      context_snapshot: { order_id: "location-order" },
    });

    const sourceRuntimeId = uuid();
    const destinationRuntimeId = uuid();
    const deviceKeys = generateKeyPairSync("ed25519");
    const devicePublicKey = Buffer.from(
      deviceKeys.publicKey.export({ format: "der", type: "spki" }),
    )
      .subarray(-32)
      .toString("base64");
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: sourceInstance.id,
      runtime_id: sourceRuntimeId,
    });
    const initialLocations = await client.listContinuityLocations(
      execution.continuity_id,
      tenantId,
    );
    assert.deepEqual(
      initialLocations.map((location) => ({
        epoch: location.epoch,
        runtime_id: location.runtime_id,
        instance_id: location.instance_id,
      })),
      [
        {
          epoch: 0,
          runtime_id: sourceRuntimeId,
          instance_id: sourceInstance.id,
        },
      ],
    );

    const now = Date.now();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: {
        runtime_id: destinationRuntimeId,
        kind: "mobile",
        trust: "registered",
        handlers: ["human_review"],
        offline_capable: true,
        capsule_signing_public_key: devicePublicKey,
        observed_at: new Date(now).toISOString(),
        expires_at: new Date(now + 60_000).toISOString(),
      },
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["human_review"] },
    });
    const handoff = await client.createHandoff({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["human_review"] },
      preview_sha256: preview.preview_sha256,
    });
    const payloadKey = randomBytes(32).toString("base64");
    const destinationInstanceId = uuid();
    const exported = await client.exportHandoff(handoff.id, {
      tenant_id: tenantId,
      requirements: { handlers: ["human_review"] },
      expires_in_seconds: 60,
      payload_key_base64: payloadKey,
    });
    assert.match(exported.payload_base64, /^[A-Za-z0-9+/]+=*$/);
    assert.match(
      exported.capsule.manifest.encryption_key_id,
      /^continuity-transfer-/,
    );
    const transported = Buffer.from(exported.payload_base64, "base64");
    assert.ok(transported.length > 0);
    const finalByte = transported.length - 1;
    transported[finalByte] = (transported[finalByte] ?? 0) ^ 1;
    await assert.rejects(
      () =>
        client.importContinuityCapsule({
          tenant_id: tenantId,
          destination_runtime_id: destinationRuntimeId,
          destination_instance_id: destinationInstanceId,
          expected_epoch: 0,
          capsule: exported.capsule,
          payload_base64: transported.toString("base64"),
          payload_key_base64: payloadKey,
        }),
      (error: unknown) =>
        error instanceof ApiError &&
        error.status === 409 &&
        /artifact hash mismatch/.test(error.message),
    );
    await assert.rejects(
      () =>
        client.importContinuityCapsule({
          tenant_id: tenantId,
          destination_runtime_id: destinationRuntimeId,
          destination_instance_id: destinationInstanceId,
          expected_epoch: 0,
          capsule: exported.capsule,
          payload_base64: exported.payload_base64,
          payload_key_base64: randomBytes(32).toString("base64"),
        }),
      (error: unknown) =>
        error instanceof ApiError &&
        error.status === 409 &&
        /decryption failed/.test(error.message),
    );
    const imported = await client.importContinuityCapsule({
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      destination_instance_id: destinationInstanceId,
      expected_epoch: 0,
      capsule: exported.capsule,
      payload_base64: exported.payload_base64,
      payload_key_base64: payloadKey,
    });
    assert.equal(imported.instance_id, destinationInstanceId);
    const redelivered = await client.importContinuityCapsule({
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      destination_instance_id: destinationInstanceId,
      expected_epoch: 0,
      capsule: exported.capsule,
      payload_base64: exported.payload_base64,
      payload_key_base64: payloadKey,
    });
    assert.equal(redelivered.instance_id, imported.instance_id);

    const accepted = await client.acceptHandoff(handoff.id, {
      tenant_id: tenantId,
      destination_instance_id: imported.instance_id,
    });
    assert.equal(accepted.execution.epoch, 1);
    const locations = await client.listContinuityLocations(
      execution.continuity_id,
      tenantId,
    );
    assert.deepEqual(
      locations.map((location) => ({
        epoch: location.epoch,
        runtime_id: location.runtime_id,
        instance_id: location.instance_id,
        handoff_id: location.handoff_id,
      })),
      [
        {
          epoch: 0,
          runtime_id: sourceRuntimeId,
          instance_id: sourceInstance.id,
          handoff_id: null,
        },
        {
          epoch: 1,
          runtime_id: destinationRuntimeId,
          instance_id: imported.instance_id,
          handoff_id: handoff.id,
        },
      ],
    );
    await assert.rejects(
      () =>
        client.listContinuityLocations(
          execution.continuity_id,
          `${tenantId}-other`,
        ),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
    const resumed = await client.resumeHandoff(handoff.id, {
      tenant_id: tenantId,
    });
    assert.equal(resumed.state, "resumed");

    // Simulate the device's host-managed exporter after offline work. It
    // re-seals the portable payload for the server and signs the canonical
    // manifest with the key registered for the mobile runtime.
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: {
        runtime_id: sourceRuntimeId,
        kind: "server",
        trust: "registered",
        handlers: ["human_review"],
        offline_capable: false,
        observed_at: new Date().toISOString(),
        expires_at: new Date(Date.now() + 60_000).toISOString(),
      },
    });
    const returnPreview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: sourceRuntimeId,
      requirements: { handlers: ["human_review"] },
    });
    const returnHandoff = await client.createHandoff({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      destination_runtime_id: sourceRuntimeId,
      requirements: { handlers: ["human_review"] },
      preview_sha256: returnPreview.preview_sha256,
    });
    const portablePayload = openCapsule(
      exported.payload_base64,
      payloadKey,
      capsuleAad(tenantId, execution.continuity_id, 0, destinationRuntimeId),
    );
    portablePayload.checkpoint.instance_id = destinationInstanceId;
    portablePayload.checkpoint.created_at = new Date().toISOString();
    const returnKey = randomBytes(32);
    const returnPayload = sealCapsule(
      portablePayload,
      returnKey,
      capsuleAad(tenantId, execution.continuity_id, 1, sourceRuntimeId),
    );
    const returnManifest = {
      ...exported.capsule.manifest,
      capsule_id: uuid(),
      source_instance_id: destinationInstanceId,
      epoch: 1,
      source_runtime_id: destinationRuntimeId,
      allowed_destination_runtime_id: sourceRuntimeId,
      checkpoint: {
        ...exported.capsule.manifest.checkpoint,
        sha256: createHash("sha256")
          .update(canonicalJson(portablePayload.checkpoint))
          .digest("hex"),
      },
      payload_artifact: {
        key: `device/${uuid()}`,
        sha256: createHash("sha256").update(returnPayload).digest("hex"),
        bytes: returnPayload.length,
      },
      issued_at: new Date().toISOString(),
      expires_at: new Date(Date.now() + 60_000).toISOString(),
      signing_key_id: "device-secure-key",
      encryption_key_id: "destination-transfer-v1",
    };
    const returnManifestSha = createHash("sha256")
      .update(canonicalJson(returnManifest))
      .digest("hex");
    const returnCapsule = {
      manifest: returnManifest,
      manifest_sha256: returnManifestSha,
      signature: sign(
        null,
        Buffer.from(returnManifestSha),
        deviceKeys.privateKey,
      ).toString("base64"),
      public_key: devicePublicKey,
    };
    const returnedInstanceId = uuid();
    const attached = await client.attachDeviceCapsule(returnHandoff.id, {
      tenant_id: tenantId,
      destination_instance_id: returnedInstanceId,
      capsule: returnCapsule,
      payload_base64: returnPayload.toString("base64"),
      payload_key_base64: returnKey.toString("base64"),
    });
    assert.equal(attached.handoff.state, "exported");
    assert.equal(attached.destination_instance_id, returnedInstanceId);
    const reattached = await client.attachDeviceCapsule(returnHandoff.id, {
      tenant_id: tenantId,
      destination_instance_id: returnedInstanceId,
      capsule: returnCapsule,
      payload_base64: returnPayload.toString("base64"),
      payload_key_base64: returnKey.toString("base64"),
    });
    assert.equal(reattached.destination_instance_id, returnedInstanceId);
    const returned = await client.acceptHandoff(returnHandoff.id, {
      tenant_id: tenantId,
      destination_instance_id: returnedInstanceId,
    });
    assert.equal(returned.execution.epoch, 2);
    await client.resumeHandoff(returnHandoff.id, { tenant_id: tenantId });
    const returnedLocations = await client.listContinuityLocations(
      execution.continuity_id,
      tenantId,
    );
    assert.deepEqual(
      returnedLocations.map((location) => location.runtime_id),
      [sourceRuntimeId, destinationRuntimeId, sourceRuntimeId],
    );
    await client.waitForState(returnedInstanceId, "waiting");
    await client.sendSignal(
      returnedInstanceId,
      { custom: "human_input:handoff" } as unknown as string,
      { value: "continue" },
    );
    await client.waitForState(returnedInstanceId, "completed");
    assert.equal(
      (await client.getInstance(returnedInstanceId)).state,
      "completed",
    );
  });

  it("fails preview when a required capability is missing", async () => {
    const tenantId = `continuity-gap-${uuid().slice(0, 8)}`;
    const sequence = testSequence("portable-gap", [step("work", "noop")], {
      tenantId,
    });
    const createdSequence = await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: createdSequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const destinationRuntimeId = uuid();
    const now = Date.now();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: {
        runtime_id: destinationRuntimeId,
        kind: "server",
        trust: "registered",
        observed_at: new Date(now).toISOString(),
        expires_at: new Date(now + 60_000).toISOString(),
      },
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["camera"], minimum_trust: "signed" },
    });
    assert.equal(preview.compatible, false);
    assert.ok(preview.findings.some((finding: any) => finding.code === "HANDLERS_MISSING"));
    assert.ok(preview.findings.some((finding: any) => finding.code === "TRUST_TOO_LOW"));
  });

  it("classifies external worker effects across dispatch and completion", async () => {
    const tenantId = `continuity-effect-${uuid().slice(0, 8)}`;
    const handler = `effect_worker_${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "portable-effect",
      [
        step("arm", "human_review", {}, {
          wait_for_input: {
            prompt: "Arm effect dispatch?",
            choices: [{ label: "Continue", value: "continue" }],
          },
        }),
        step(
          "charge",
          handler,
          { amount: 4200, idempotency_key: "order-effect-1" },
        ),
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
    const task = await waitForWorkerTask(handler, "effect-worker-1");
    const dispatched = await client.listContinuityEffects(
      execution.continuity_id,
      tenantId,
    );
    assert.equal(dispatched.length, 1);
    assert.equal(dispatched[0]!.state, "dispatched");

    await assert.rejects(
      () =>
        client.completeWorkerTask(task.id, "wrong-effect-worker", {
          provider_receipt_id: "forged-receipt",
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
    const afterRejectedCallback = await client.listContinuityEffects(
      execution.continuity_id,
      tenantId,
    );
    assert.equal(afterRejectedCallback[0]!.state, "dispatched");
    assert.equal(afterRejectedCallback[0]!.provider_receipt_id, null);

    await client.completeWorkerTask(task.id, "effect-worker-1", {
      ok: true,
      provider_receipt_id: "provider-effect-42",
    });
    await client.waitForState(instance.id, "completed");

    const receipts = await client.listContinuityEffects(
      execution.continuity_id,
      tenantId,
    );
    assert.equal(receipts.length, 1);
    assert.equal(receipts[0]!.block_id, "charge");
    assert.equal(receipts[0]!.kind, "worker");
    assert.equal(receipts[0]!.state, "committed");
    assert.equal(receipts[0]!.idempotency_key, "order-effect-1");
    assert.equal(receipts[0]!.provider_receipt_id, "provider-effect-42");
    assert.match(receipts[0]!.request_sha256, /^[0-9a-f]{64}$/);
  });

  it("rejects self-asserted elevated runtime trust", async () => {
    const tenantId = `continuity-security-${uuid().slice(0, 8)}`;
    const runtimeId = uuid();
    const now = Date.now();
    await assert.rejects(
      () =>
        client.registerRuntime({
          tenant_id: tenantId,
          capabilities: {
            runtime_id: runtimeId,
            kind: "server",
            trust: "attested",
            observed_at: new Date(now).toISOString(),
            expires_at: new Date(now + 60_000).toISOString(),
          },
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 400);
        return true;
      },
    );
    await assert.rejects(
      () =>
        client.registerRuntime({
          tenant_id: tenantId,
          capabilities: {
            runtime_id: runtimeId,
            kind: "mobile",
            trust: "registered",
            capsule_signing_public_key: "not-a-public-key",
            observed_at: new Date(now).toISOString(),
            expires_at: new Date(now + 60_000).toISOString(),
          },
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 400);
        return true;
      },
    );
  });
});
