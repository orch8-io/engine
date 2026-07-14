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
import { execFileSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { ApiError, Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { WorkerTask } from "../types.ts";

const client = new Orch8Client();
const artifactDir = `/tmp/o8-continuity-e2e-${uuid().slice(0, 8)}`;
const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

function findCliBinary(): string {
  const target = resolve(projectRoot, "target");
  const candidates = [resolve(target, "debug/orch8")];
  if (existsSync(target)) {
    for (const entry of readdirSync(target)) {
      candidates.push(resolve(target, entry, "debug/orch8"));
    }
  }
  const binary = candidates.find(existsSync);
  if (!binary) throw new Error("orch8 CLI binary not found; run cargo build -p orch8-cli");
  return binary;
}

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

function cameraRuntimeCapabilities(
  runtimeId: string,
  observedAt: number,
  estimatedCostMicrounits: number,
): Record<string, unknown> {
  return {
    runtime_id: runtimeId,
    kind: "mobile",
    trust: "registered",
    handlers: ["camera", "noop"],
    regions: ["br-south"],
    hardware: ["camera"],
    offline_capable: true,
    connectivity: "wifi",
    battery_percent: 80,
    estimated_cost_microunits: estimatedCostMicrounits,
    estimated_latency_ms: 100,
    observed_at: new Date(observedAt).toISOString(),
    expires_at: new Date(observedAt + 60_000).toISOString(),
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
      capabilities: cameraRuntimeCapabilities(destinationRuntimeId, now, 40),
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

    const localityPolicy = {
      version: 1,
      rules: [{
        classification: "restricted",
        allowed_runtime_ids: [destinationRuntimeId],
        allowed_runtime_kinds: ["mobile"],
        allowed_regions: ["br-south"],
        minimum_trust: "registered",
        require_hardware: "camera",
        allowed_connectivity: ["wifi"],
        minimum_battery_percent: 20,
        maximum_cost_microunits: 50,
        maximum_latency_ms: 500,
      }],
    };
    const initialPreview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: {
        handlers: ["camera"],
        regions: ["br-south"],
        hardware: ["camera"],
        minimum_trust: "registered",
      },
      policy: localityPolicy,
      classification: "restricted",
    });
    assert.equal(initialPreview.compatible, true);
    assert.deepEqual(initialPreview.unresolved_effects, []);
    assert.equal(initialPreview.placement_decision.selected_runtime_id, destinationRuntimeId);

    // A live capability change between preview and mutation must invalidate
    // dispatch, even when the caller replays the original evidence hash.
    const changedAt = Date.now();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: cameraRuntimeCapabilities(destinationRuntimeId, changedAt, 100),
    });
    await assert.rejects(
      () => client.createHandoff({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destinationRuntimeId,
        requirements: {
          handlers: ["camera"],
          regions: ["br-south"],
          hardware: ["camera"],
          minimum_trust: "registered",
        },
        policy: localityPolicy,
        classification: "restricted",
        placement_decision_id: initialPreview.placement_decision.id,
        preview_sha256: initialPreview.preview_sha256,
      }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );

    const restoredAt = Date.now() + 5;
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: cameraRuntimeCapabilities(destinationRuntimeId, restoredAt, 40),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: {
        handlers: ["camera"],
        regions: ["br-south"],
        hardware: ["camera"],
        minimum_trust: "registered",
      },
      policy: localityPolicy,
      classification: "restricted",
    });

    const placement = await client.choosePlacement(execution.continuity_id, {
      tenant_id: tenantId,
      requirements: { handlers: ["camera"] },
      policy: localityPolicy,
      classification: "restricted",
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
          policy: localityPolicy,
          classification: "restricted",
          placement_decision_id: preview.placement_decision.id,
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
      policy: localityPolicy,
      classification: "restricted",
      placement_decision_id: preview.placement_decision.id,
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

    await client.waitForState(instance.id, "completed");
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
    assert.equal(boundaries[0].checkpoint_id, checkpoint.id);
    const checkpointDetail = await client.getContinuityCheckpoint(
      execution.continuity_id,
      checkpoint.id,
      tenantId,
    );
    assert.equal(
      checkpointDetail.checkpoint_data.context_snapshot.password,
      "must-not-leak",
      "authorized detail returns the exact checkpoint state",
    );
    assert.ok(checkpointDetail.redacted_state_diff.some(
      (change: any) =>
        change.path === "context_snapshot.password" &&
        change.after === "[REDACTED]",
    ));
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
    assert.deepEqual(
      whatIf.report.executed_blocks,
      [],
      "sandbox resumes after the selected boundary instead of replaying it",
    );
    const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpoint.id,
      allowlisted_fields: ["order_id", "password"],
    });
    assert.equal(fixture.sanitized_context.order_id, "order-1");
    assert.equal(fixture.sanitized_context.password, "[REDACTED]");
    assert.equal(fixture.complete, true);

    const fixtureDir = resolve(artifactDir, "extracted-fixture");
    mkdirSync(fixtureDir, { recursive: true });
    const extractRequest = resolve(fixtureDir, "extract-request.json");
    writeFileSync(extractRequest, JSON.stringify({
      tenant_id: tenantId,
      checkpoint_id: checkpoint.id,
      allowlisted_fields: ["order_id", "password"],
    }));
    execFileSync(findCliBinary(), [
      "--url", `http://127.0.0.1:${server!.port}`,
      "test", "extract", execution.continuity_id, extractRequest,
      "--out-dir", fixtureDir,
    ]);
    const generated = readdirSync(fixtureDir);
    const contractName = generated.find((name) => name.endsWith(".contracts.json"));
    assert.ok(contractName);
    const sequenceName = contractName.replace(".contracts.json", ".json");
    const contractPath = resolve(fixtureDir, contractName);
    const sequencePath = resolve(fixtureDir, sequenceName);
    const generatedText = generated
      .filter((name) => name.startsWith("continuation-"))
      .map((name) => readFileSync(resolve(fixtureDir, name), "utf8"))
      .join("\n");
    assert.doesNotMatch(generatedText, /must-not-leak/);
    const replay = execFileSync(findCliBinary(), [
      "test", "run", contractPath,
      "--sequence", sequencePath,
      "--report", "json",
    ], { encoding: "utf8" });
    assert.equal(JSON.parse(replay).passed, true);

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
      placement_decision_id: preview.placement_decision.id,
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
      placement_decision_id: returnPreview.placement_decision.id,
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
    const historicalCheckpoints = await client.listContinuityCheckpoints(
      execution.continuity_id,
      tenantId,
    );
    assert.ok(historicalCheckpoints.some((boundary) =>
      boundary.instance_id === sourceInstance.id &&
      boundary.epoch === 0 &&
      boundary.block_id === "handoff"
    ));
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

  it("atomically applies and rolls back a validated live migration", async () => {
    const tenantId = `continuity-migration-${uuid().slice(0, 8)}`;
    const v1 = testSequence(
      "portable-live-migration",
      [
        step("review", "human_review", {}, {
          wait_for_input: {
            prompt: "Hold at the migration boundary?",
            choices: [{ label: "Continue", value: "continue" }],
          },
        }),
      ],
      { tenantId },
    );
    const createdV1 = await client.createSequence(v1);
    const v2 = {
      ...v1,
      id: uuid(),
      version: 2,
      blocks: [
        ...v1.blocks,
        step("after_migration", "noop", {}, {
          when: "data.profile.display_name != null",
        }),
      ],
      created_at: new Date().toISOString(),
    };
    const createdV2 = await client.createSequence(v2);
    const instance = await client.createInstance({
      sequence_id: createdV1.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(instance.id, "waiting");
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    await assert.rejects(
      () => client.migrateInstance(instance.id, createdV2.id),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
      "the legacy hot-rebind endpoint must not bypass continuity migration safety",
    );
    await client.saveCheckpoint(instance.id, {
      safe_boundary: "review",
      context_snapshot: { profile: { name: "Ada" } },
    });

    const plan = await client.planContinuityMigration({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      to_sequence_id: createdV2.id,
      to_version: 2,
      rollback_retention_seconds: 3600,
      transforms: [{
        version: 1,
        from_path: "context_snapshot.profile.name",
        to_path: "context_snapshot.profile.display_name",
        transform: "copy",
      }],
    });
    assert.equal(plan.disposition, "approval_required");
    assert.equal(plan.rollback_capsule_required, true);
    assert.ok(plan.finding_codes.includes("BLOCK_ADDED"));

    const applied = await client.applyContinuityMigration(plan.id, {
      tenant_id: tenantId,
      approved: true,
    });
    assert.equal(applied.state, "applied");
    assert.equal(applied.applied_epoch, 1);
    assert.ok(applied.rollback_capsule);
    assert.equal(applied.rollback_capsule.capsule_id.length, 36);
    assert.equal(applied.rollback_capsule.payload_artifact.sha256.length, 64);
    assert.equal(applied.rollback_capsule.manifest_sha256.length, 64);
    const migrated = await client.getInstance(instance.id);
    assert.equal(migrated.sequence_id, createdV2.id);
    assert.equal(migrated.state, "paused");
    const migratedData = migrated.context?.data as {
      profile?: { name?: string; display_name?: string };
    };
    assert.equal(migratedData.profile?.name, "Ada");
    assert.equal(migratedData.profile?.display_name, "Ada");
    assert.equal((await client.getContinuityMigration(plan.id, tenantId)).state, "applied");

    const rolledBack = await client.rollbackContinuityMigration(plan.id, {
      tenant_id: tenantId,
    });
    assert.equal(rolledBack.state, "rolled_back");
    const restored = await client.getInstance(instance.id);
    assert.equal(restored.sequence_id, createdV1.id);
    assert.equal(restored.state, "waiting");
    const restoredData = restored.context?.data as
      | { profile?: unknown }
      | null
      | undefined;
    assert.equal(restoredData?.profile, undefined);
    const locations = await client.listContinuityLocations(
      execution.continuity_id,
      tenantId,
    );
    assert.deepEqual(locations.map((location) => location.epoch), [0, 1, 2]);
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

    const checkpoint = await client.saveCheckpoint(instance.id, {
      safe_boundary: "charge",
      context_snapshot: {},
    });
    const fixture = await client.extractContinuityTestFixture(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpoint.id,
      allowlisted_fields: [],
    });
    assert.equal(fixture.effect_mocks.length, 1);
    assert.equal(fixture.effect_mocks[0].block_id, "charge");
    assert.equal(fixture.effect_mocks[0].state, "committed");
    assert.equal(
      fixture.effect_mocks[0].provider_receipt_id,
      "provider-effect-42",
    );
  });

  it("compares downstream behavior and usage in an effect-free what-if", async () => {
    const tenantId = `continuity-what-if-${uuid().slice(0, 8)}`;
    const modelHandler = `what_if_model_${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "portable-what-if",
      [
        step("arm", "human_review", {}, {
          wait_for_input: {
            prompt: "Capture simulation boundary?",
            choices: [{ label: "Continue", value: "continue" }],
          },
        }),
        step("model", modelHandler, { model: "baseline-v1" }),
        step("standard", "noop", {}, {
          when: 'outputs.model.tier == "basic"',
        }),
        step("premium", "noop", {}, {
          when: 'outputs.model.tier == "premium"',
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
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const checkpoint = await client.saveCheckpoint(instance.id, {
      safe_boundary: "arm",
      context_snapshot: {},
    });
    await client.sendSignal(
      instance.id,
      { custom: "human_input:arm" } as unknown as string,
      { value: "continue" },
    );
    const task = await waitForWorkerTask(modelHandler, "what-if-model-worker");
    await client.completeWorkerTask(task.id, "what-if-model-worker", {
      tier: "basic",
      usage: {
        input_tokens: 10,
        output_tokens: 5,
        total_tokens: 15,
        cost_microunits: 100,
        external_calls: 1,
      },
    });
    await client.waitForState(instance.id, "completed");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "what-if reaches a terminal success",
      rule: { type: "terminal_state_in", states: ["completed"] },
    });

    const whatIf = await client.runContinuityWhatIf(execution.continuity_id, {
      tenant_id: tenantId,
      checkpoint_id: checkpoint.id,
      context_patch: {},
      config_patch: { routing_policy: "quality_first" },
      block_param_overrides: {
        model: { model: "premium-v2" },
      },
      output_overrides: {
        model: {
          tier: "premium",
          usage: {
            input_tokens: 20,
            output_tokens: 12,
            total_tokens: 32,
            cost_microunits: 1000,
            external_calls: 1,
          },
        },
      },
      handler_mocks: {
        premium: { selected: true },
      },
      signals: [{
        signal_type: "custom:human_input:arm",
        payload: { value: "continue" },
      }],
      max_ticks: 5000,
    });
    assert.equal(
      whatIf.baseline_report.final_state,
      "completed",
      JSON.stringify(whatIf.baseline_report),
    );
    assert.equal(
      whatIf.report.final_state,
      "completed",
      JSON.stringify(whatIf.report),
    );
    assert.deepEqual(whatIf.comparison.added_blocks, ["premium"]);
    assert.deepEqual(whatIf.comparison.removed_blocks, ["standard"]);
    assert.equal(whatIf.comparison.output_changes.model.baseline.tier, "basic");
    assert.equal(whatIf.comparison.output_changes.model.candidate.tier, "premium");
    assert.equal(whatIf.comparison.output_changes.standard.candidate, null);
    assert.equal(whatIf.comparison.output_changes.premium.baseline, null);
    assert.equal(whatIf.comparison.effects.production_receipts_created, 0);
    assert.equal(whatIf.comparison.effects.simulated_external_call_delta, 0);
    assert.equal(whatIf.comparison.baseline_invariants[0].status, "pass");
    assert.equal(whatIf.comparison.candidate_invariants[0].status, "pass");
    assert.equal(whatIf.comparison.usage_delta.input_tokens, 10);
    assert.equal(whatIf.comparison.usage_delta.output_tokens, 7);
    assert.equal(whatIf.comparison.usage_delta.total_tokens, 17);
    assert.equal(whatIf.comparison.usage_delta.cost_microunits, 900);
    assert.equal(whatIf.scenario.config_patch.routing_policy, "quality_first");
    assert.equal(
      whatIf.scenario.block_param_overrides.model.model,
      "premium-v2",
    );
    assert.equal(
      (await client.listContinuityEffects(execution.continuity_id, tenantId)).length,
      1,
      "sandbox runs must not add production effect receipts",
    );
    const persistedRuns = await client.listContinuityWhatIfRuns(
      execution.continuity_id,
      tenantId,
    );
    assert.equal(persistedRuns.length, 1);
    assert.equal(persistedRuns[0].scenario.id, whatIf.scenario.id);
    assert.equal(
      persistedRuns[0].summary.comparison.usage_delta.cost_microunits,
      900,
    );
    assert.equal(persistedRuns[0].scenario.retain_full_evidence, false);
  });

  it("blocks duplicate effects before dispatch and records one violation", async () => {
    const tenantId = `continuity-dedup-${uuid().slice(0, 8)}`;
    const handler = `dedup_worker_${uuid().slice(0, 8)}`;
    const params = { provider: "payments", amount: 4200 };
    const sequence = testSequence(
      "portable-effect-dedup",
      [
        step("arm", "human_review", {}, {
          wait_for_input: {
            prompt: "Arm guarded effect dispatch?",
            choices: [{ label: "Continue", value: "continue" }],
          },
        }),
        step("charge-a", handler, params),
        step("charge-b", handler, params),
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
    const invariant = await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "worker request at most once",
      rule: { type: "effect_at_most_once", kind: "worker" },
      commit_guard: true,
    });
    await assert.rejects(
      () => client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        sequence_version: createdSequence.version,
        name: "unsupported commit guard",
        rule: { type: "no_unknown_effects" },
        commit_guard: true,
      }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 400);
        return true;
      },
    );

    await client.sendSignal(
      instance.id,
      { custom: "human_input:arm" } as unknown as string,
      { value: "continue" },
    );
    const first = await waitForWorkerTask(handler, "dedup-worker");
    await client.completeWorkerTask(first.id, "dedup-worker", {
      provider_receipt_id: "provider-dedup-1",
    });
    await client.waitForState(instance.id, "failed");

    assert.deepEqual(
      await client.pollWorkerTasks(handler, "dedup-worker"),
      [],
      "the duplicate must never be published to a worker",
    );
    const receipts = await client.listContinuityEffects(
      execution.continuity_id,
      tenantId,
    );
    assert.equal(
      receipts.filter((receipt) =>
        ["dispatched", "committed"].includes(String(receipt.state))
      ).length,
      1,
    );
    const results = await client.listContinuityInvariantResults(
      execution.continuity_id,
      tenantId,
    );
    const failures = results.filter((result) => result.status === "fail");
    assert.equal(failures.length, 1);
    assert.equal(failures[0]!.invariant_id, invariant.id);
    assert.match(
      String(failures[0]!.summary),
      /duplicate effect dispatch or commit evidence/,
    );
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
