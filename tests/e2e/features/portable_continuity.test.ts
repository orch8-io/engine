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
const federationTenantId = "continuity-federation-e2e";
const federationPeerId = uuid();
const {
  publicKey: federationPublicKey,
  privateKey: federationPrivateKey,
} = generateKeyPairSync("ed25519");
const federationPublicDer = federationPublicKey.export({
  format: "der",
  type: "spki",
});
const federationPublicRaw = federationPublicDer.subarray(-32);
const federationPeer = {
  id: federationPeerId,
  name: "e2e-peer",
  trust_root_sha256: createHash("sha256").update(federationPublicRaw).digest("hex"),
  public_key: federationPublicRaw.toString("base64"),
  endpoint: "https://peer.example.invalid",
  allowed_tenants: [federationTenantId],
  revoked_at: null,
};

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
        ORCH8_CONTINUITY_LAB_ENABLED: "true",
        ORCH8_FEDERATION_PEERS: JSON.stringify([federationPeer]),
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

  it("durably compensates committed effects in reverse dependency order", async () => {
    const tenantId = `continuity-compensation-${uuid().slice(0, 8)}`;
    const reserveHandler = `reserve_${uuid().slice(0, 8)}`;
    const chargeHandler = `charge_${uuid().slice(0, 8)}`;
    const failHandler = `fail_${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "portable-compensation",
      [
        step("review", "human_review", {}, {
          wait_for_input: {
            prompt: "Start effects?",
            choices: [{ label: "Continue", value: "continue" }],
          },
        }),
        step("reserve", reserveHandler, { resource: "seat-7" }, {
          compensation: {
            handler: "release_reservation",
            params: { resource: "{{ outputs.reserve.resource }}" },
            verification: "handler_result",
          },
        }),
        step("charge", chargeHandler, { amount: 4200 }, {
          compensation: {
            handler: "refund_payment",
            params: { amount: "{{ outputs.charge.amount }}" },
            depends_on: ["reserve"],
            verification: "provider_receipt",
          },
        }),
        step("notify", failHandler, { channel: "email" }),
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
      { custom: "human_input:review" } as unknown as string,
      { value: "continue" },
    );
    const reserveTask = await waitForWorkerTask(reserveHandler, "effects-worker");
    await client.completeWorkerTask(reserveTask.id, "effects-worker", {
      provider_receipt_id: "reservation-7",
      resource: "seat-7",
    });
    const chargeTask = await waitForWorkerTask(chargeHandler, "effects-worker");
    await client.completeWorkerTask(chargeTask.id, "effects-worker", {
      provider_receipt_id: "charge-7",
      amount: 4200,
    });
    const failingTask = await waitForWorkerTask(failHandler, "effects-worker");
    await client.failWorkerTask(
      failingTask.id,
      "effects-worker",
      "provider unavailable",
      false,
    );
    await client.waitForState(instance.id, "failed");

    const preview = await client.previewContinuityCompensation(
      execution.continuity_id,
      { tenant_id: tenantId },
    );
    assert.deepEqual(
      preview.steps.map((candidate: Record<string, unknown>) => candidate.handler),
      ["refund_payment", "release_reservation"],
    );
    assert.equal(preview.steps[0].params.amount, 4200);
    assert.equal(preview.steps[1].params.resource, "seat-7");
    assert.ok(preview.hazards.some((hazard: string) =>
      hazard.startsWith("UNKNOWN_EFFECT_MUST_BE_RESOLVED:notify")
    ));
    const run = await client.createContinuityCompensation(
      execution.continuity_id,
      { tenant_id: tenantId },
    );
    assert.equal(run.state, "planned");

    const refund = await client.claimContinuityCompensation(run.id, {
      tenant_id: tenantId,
      worker_id: "compensation-worker",
      lease_seconds: 5,
    });
    assert.equal(refund.plan.handler, "refund_payment");
    assert.ok(refund.plan.idempotency_key.startsWith("compensate:"));
    await new Promise((resolve) => setTimeout(resolve, 5_100));
    await assert.rejects(
      () => client.claimContinuityCompensation(run.id, {
        tenant_id: tenantId,
        worker_id: "replacement-worker",
        lease_seconds: 30,
      }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
    const crashed = await client.getContinuityCompensation(run.id, tenantId);
    assert.equal(crashed.steps[0].state, "unknown");
    await client.verifyContinuityCompensation(run.id, refund.plan.effect_id, {
      tenant_id: tenantId,
      approved: true,
      evidence: "provider confirms refund under the supplied idempotency key",
    });

    const release = await client.claimContinuityCompensation(run.id, {
      tenant_id: tenantId,
      worker_id: "compensation-worker",
      lease_seconds: 30,
    });
    assert.equal(release.plan.handler, "release_reservation");
    const finished = await client.completeContinuityCompensation(
      run.id,
      release.plan.effect_id,
      {
        tenant_id: tenantId,
        worker_id: "compensation-worker",
        provider_receipt_id: "release-7",
      },
    );
    assert.equal(finished.state, "completed_with_residuals");
    assert.ok(finished.residual_effects.some((residual: string) =>
      residual.startsWith("UNKNOWN_EFFECT_MUST_BE_RESOLVED:notify")
    ));
    const receipts = await client.listContinuityEffects(
      execution.continuity_id,
      tenantId,
    );
    assert.equal(receipts.find((receipt) => receipt.block_id === "charge")?.state, "compensated");
    assert.equal(receipts.find((receipt) => receipt.block_id === "reserve")?.state, "compensated");
    assert.equal(receipts.find((receipt) => receipt.block_id === "notify")?.state, "unknown");
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

  it("fault-injects every ownership write and minimizes deterministic scenarios", async () => {
    const transitions = [
      "request_to_quiescing",
      "quiescing_to_exported",
      "exported_to_accepted",
      "accepted_to_resumed",
      "resumed_to_completed",
    ];
    for (const transition of transitions) {
      for (const phase of ["before", "after"]) {
        const run = await client.runContinuityFaultLab({
          profile: transition === "exported_to_accepted" ? "stale_owner" : "worker_death",
          transition,
          phase,
          initial_epoch: 4,
        });
        assert.equal(run.retry_safe, true);
        assert.equal(run.committed, phase === "after");
        assert.equal(
          run.final_epoch,
          transition === "exported_to_accepted" && phase === "after" ? 5 : 4,
        );
        assert.ok(Array.isArray(run.trace));
      }
    }

    const request = {
      events: ["payment", "inventory"],
      faults: [{ point: "ownership_claim", kind: "stale_owner", occurrence: 1 }],
      input_schema_cases: ["valid", "boundary"],
      router_branches: ["cloud", "device"],
      event_joins: ["all"],
      policy_facts: ["trusted", "untrusted"],
      invariant_codes: ["single_owner"],
      retry_attempts: [0, 1, 2],
      handoff_delays_ms: [0, 50],
      max_scenarios: 12,
      max_steps: 100,
      max_virtual_time_ms: 1_000,
      seed: 42,
    };
    const first = await client.generateContinuityScenarios(request);
    const second = await client.generateContinuityScenarios(request);
    assert.deepEqual(first, second, "same seed must produce byte-stable fixtures");
    assert.equal(first.length, 12);
    assert.ok(first.some((scenario) => scenario.retry_attempt === 2));
    assert.ok(first.some((scenario) => scenario.handoff_delay_ms === 50));

    const candidate = structuredClone(first[5]!);
    candidate.faults = [
      ...(candidate.faults as unknown[]),
      { point: "ownership_claim", kind: "database_timeout", occurrence: 1 },
    ];
    const minimized = await client.reproduceContinuityIncident({
      scenario: candidate,
      required_fault_kind: "database_timeout",
    });
    assert.equal(minimized.status, "pass");
    assert.equal((minimized.scenario as any).faults.length, 1);
    assert.deepEqual((minimized.scenario as any).event_order, []);
    assert.deepEqual((minimized.scenario as any).policy_facts, []);
  });

  it("records signed payload-free provenance and verifies the retained head", async () => {
    const tenantId = `continuity-provenance-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      `provenance-${uuid().slice(0, 8)}`,
      [
        step("wait", "human_review", {}, {
          wait_for_input: {
            prompt: "Hold for provenance inspection?",
            choices: [{ label: "Continue", value: "continue" }],
          },
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
    const packageDigest = createHash("sha256")
      .update("package-with-secret-that-must-not-be-stored")
      .digest("hex");
    await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "package_identity",
      payload_sha256: packageDigest,
    });
    const terminal = await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "terminal_outcome",
      payload_sha256: createHash("sha256").update("completed").digest("hex"),
    });
    const entries = await client.listContinuityProvenance(
      execution.continuity_id,
      tenantId,
    );
    assert.deepEqual(
      entries.map((entry) => entry.kind),
      ["execution_created", "package_identity", "terminal_outcome"],
    );
    assert.ok(entries.every((entry) => typeof entry.signature === "string"));
    assert.ok(entries.every((entry) => !("payload" in entry)));
    assert.ok(!JSON.stringify(entries).includes("package-with-secret"));
    const verification = await client.verifyContinuityProvenance(
      execution.continuity_id,
      tenantId,
      String(terminal.entry_sha256),
    );
    assert.equal(verification.valid, true);
    assert.equal(verification.entries_checked, 3);
  });

  it("attaches a minimized sanitized reproduction to a real DLQ fingerprint", async () => {
    const tenantId = `continuity-incident-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      `incident-${uuid().slice(0, 8)}`,
      [
        step("provider", "assert", {
          condition: "false",
          message: "upstream connection timeout",
          api_key: "sk-incident-secret-must-not-survive",
        }),
      ],
      { tenantId },
    );
    const created = await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: created.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 3_000).toISOString(),
    });
    await client.waitForState(instance.id, "scheduled");
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    await client.saveCheckpoint(instance.id, {
      block_id: "provider",
      completed_blocks: [],
      context_snapshot: {},
    });
    await client.waitForState(instance.id, "failed");

    const groups = await client.listDlqGroups(tenantId);
    const group = groups.find((candidate) =>
      (candidate.sample_instance_ids as unknown[]).includes(instance.id)
    );
    assert.ok(group);
    const reproduction = await client.reproduceDlqGroup(String(group.fingerprint), {
      tenant_id: tenantId,
      allowlisted_fields: [],
      max_scenarios: 16,
      seed: 9,
    });
    assert.equal(reproduction.status, "reproduced", JSON.stringify(reproduction));
    assert.equal(reproduction.fingerprint, group.fingerprint);
    assert.equal(reproduction.source_instance_id, instance.id);
    assert.equal((reproduction.scenario as any).faults.length, 1);
    assert.deepEqual((reproduction.scenario as any).event_order, []);
    assert.equal((reproduction.fixture as any).complete, true);
    assert.ok(!JSON.stringify(reproduction).includes("sk-incident-secret"));
    const attached = await client.listDlqReproductions(
      String(group.fingerprint),
      tenantId,
    );
    assert.equal(attached[0]!.id, reproduction.id);
  });

  it("reconciles budget usage atomically and keeps actual spend cumulative", async () => {
    const tenantId = `continuity-budget-${uuid().slice(0, 8)}`;
    const created = await client.createSequence(testSequence(
      `budget-${uuid().slice(0, 8)}`,
      [step("work", "noop")],
      { tenantId },
    ));
    const instance = await client.createInstance({
      sequence_id: created.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
      budget: { max_cost_microunits: 100 },
    });
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const usage = (cost: number) => ({
      cost_microunits: cost,
      wall_time_ms: 0,
      external_calls: 0,
      bytes_transferred: 0,
      energy_millijoules: 0,
      attention_units: 0,
    });
    const first = await client.reserveContinuityBudget(execution.continuity_id, {
      tenant_id: tenantId,
      requested: usage(70),
      estimation_version: "prices-2026-07",
    });
    const reconciled = await client.reconcileContinuityBudget(
      execution.continuity_id,
      first.id,
      { tenant_id: tenantId, actual: usage(60) },
    );
    assert.equal(reconciled.state, "reconciled");
    assert.equal(reconciled.actual.cost_microunits, 60);
    await assert.rejects(
      () => client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: usage(50),
        estimation_version: "prices-2026-07",
      }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
    const releasable = await client.reserveContinuityBudget(execution.continuity_id, {
      tenant_id: tenantId,
      requested: usage(40),
      estimation_version: "prices-2026-07",
    });
    const released = await client.releaseContinuityBudget(
      execution.continuity_id,
      releasable.id,
      { tenant_id: tenantId },
    );
    assert.equal(released.state, "released");
    const concurrent = await client.reserveContinuityBudget(execution.continuity_id, {
      tenant_id: tenantId,
      requested: usage(40),
      estimation_version: "prices-2026-07",
    });
    const settlements = await Promise.allSettled([
      client.reconcileContinuityBudget(execution.continuity_id, concurrent.id, {
        tenant_id: tenantId,
        actual: usage(40),
      }),
      client.reconcileContinuityBudget(execution.continuity_id, concurrent.id, {
        tenant_id: tenantId,
        actual: usage(40),
      }),
    ]);
    assert.equal(settlements.filter((result) => result.status === "fulfilled").length, 1);
    const reservations = await client.listContinuityBudgetReservations(
      execution.continuity_id,
      tenantId,
    );
    assert.deepEqual(
      reservations.map((reservation) => reservation.state).sort(),
      ["reconciled", "reconciled", "released"],
    );
  });

  it("blocks unsafe provider failover and records policy-approved routing", async () => {
    const tenantId = `continuity-router-${uuid().slice(0, 8)}`;
    const created = await client.createSequence(testSequence(
      `router-${uuid().slice(0, 8)}`,
      [step("work", "noop")],
      { tenantId },
    ));
    const instance = await client.createInstance({
      sequence_id: created.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const candidates = [
      {
        provider: "provider-a",
        model: "agent-large",
        region: "br-south",
        price_microunits: 40,
        expected_latency_ms: 100,
        quality_millipoints: 950,
        breaker_open: true,
        supports_idempotency: true,
        pricing_version: "prices-2026-07",
      },
      {
        provider: "provider-b",
        model: "agent-medium",
        region: "br-south",
        price_microunits: 20,
        expected_latency_ms: 80,
        quality_millipoints: 900,
        breaker_open: false,
        supports_idempotency: true,
        pricing_version: "prices-2026-07",
      },
    ];
    const request = {
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      candidates,
      allowed_regions: ["br-south"],
      max_price_microunits: 50,
      max_latency_ms: 200,
      minimum_quality_millipoints: 850,
      require_idempotency: true,
      prior_provider: "provider-a",
      operation_idempotent: false,
      effect_policy_approved: false,
      cohort_key: "tenant:release:operation-1",
    };
    const blocked = await client.chooseContinuityProvider(request);
    assert.equal(blocked.selected, null);
    assert.ok(blocked.finding_codes.includes("NON_IDEMPOTENT_FAILOVER_BLOCKED"));
    const approved = await client.chooseContinuityProvider({
      ...request,
      effect_policy_approved: true,
    });
    assert.equal(approved.selected.provider, "provider-b");
    assert.equal(approved.selected.pricing_version, "prices-2026-07");
    assert.equal(approved.cohort, blocked.cohort);
    const provenance = await client.listContinuityProvenance(
      execution.continuity_id,
      tenantId,
    );
    assert.ok(provenance.some((entry) => entry.kind === "provider_selected"));
  });

  it("gates candidates from scoped durable evaluation evidence", async () => {
    const tenantId = `continuity-eval-${uuid().slice(0, 8)}`;
    const created = await client.createSequence(testSequence(
      `eval-${uuid().slice(0, 8)}`,
      [step("work", "noop")],
      { tenantId },
    ));
    const createExecution = async () => {
      const instance = await client.createInstance({
        sequence_id: created.id,
        tenant_id: tenantId,
        namespace: "default",
        next_fire_at: new Date(Date.now() + 60_000).toISOString(),
      });
      return client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
    };
    const baseline = await createExecution();
    const candidate = await createExecution();
    const scope = {
      sequence_id: created.id,
      sequence_version: created.version,
      block_id: "work",
      model: "agent-medium",
      release_id: uuid(),
    };
    const append = (
      continuityId: string,
      score: number,
      evidence: string,
      outcome = "complete",
    ) => client.appendContinuityEvaluation(continuityId, {
      tenant_id: tenantId,
      evaluator: "quality-v2",
      score_millipoints: score,
      sample_size: 100,
      deferred: false,
      outcome,
      scope,
      evidence_sha256: evidence.repeat(64),
    });
    await append(baseline.continuity_id, 900, "a");
    await append(candidate.continuity_id, 850, "b");
    const gateRequest = {
      tenant_id: tenantId,
      baseline_continuity_id: baseline.continuity_id,
      candidate_continuity_id: candidate.continuity_id,
      evaluator: "quality-v2",
      scope,
      minimum_samples: 100,
      maximum_regression_millipoints: 20,
    };
    const failed = await client.evaluateStoredContinuityGate(gateRequest);
    assert.equal(failed.status, "fail");
    assert.equal(failed.baseline_score_millipoints, 900);
    assert.equal(failed.candidate_score_millipoints, 850);
    await append(candidate.continuity_id, 1_000, "c", "pending");
    const pending = await client.evaluateStoredContinuityGate(gateRequest);
    assert.equal(pending.status, "inconclusive");
    assert.equal(pending.pending_outcomes, 1);
    const provenance = await client.listContinuityProvenance(
      candidate.continuity_id,
      tenantId,
    );
    assert.equal(
      provenance.filter((entry) => entry.kind === "evaluation_gate").length,
      2,
    );
  });

  it("converts an authentic optimization into an idempotent draft release", async () => {
    const tenantId = `continuity-advisor-${uuid().slice(0, 8)}`;
    const created = await client.createSequence(testSequence(
      `advisor-${uuid().slice(0, 8)}`,
      [step("work", "noop")],
      { tenantId },
    ));
    const instance = await client.createInstance({
      sequence_id: created.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    await client.saveCheckpoint(instance.id, {
      block_id: "work",
      completed_blocks: [],
      context_snapshot: {},
    });
    const [source] = await client.listContinuityCheckpoints(
      execution.continuity_id,
      tenantId,
    );
    const baseScenario = {
      id: uuid(),
      tenant_id: tenantId,
      source,
      context_patch: {},
      config_patch: {},
      output_overrides: {},
      handler_mocks: {},
      block_param_overrides: {},
      signals: [],
      target_sequence_version: null,
      effect_mode: "blocked",
      virtual_time: true,
      retain_full_evidence: false,
    };
    const recommendations = await client.recommendContinuityOptimizations({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      serial_work_millipoints: 0,
      retry_rate_millipoints: 0,
      average_payload_bytes: 2_000_000,
      average_cost_microunits: 0,
      dead_branch_count: 0,
      base_scenario: baseScenario,
    });
    assert.equal(recommendations.length, 1);
    assert.equal(recommendations[0]!.kind, "payload_compaction");
    const request = {
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      recommendation: recommendations[0],
    };
    const accepted = await client.acceptContinuityOptimization(
      recommendations[0]!.id,
      request,
    );
    assert.equal(accepted.draft_sequence.status, "draft");
    assert.equal(accepted.draft_sequence.id, recommendations[0]!.id);
    assert.equal(accepted.release.id, recommendations[0]!.id);
    assert.equal(accepted.release.state, "draft");
    assert.equal(
      accepted.release.validation_summary.optimization_recommendation_id,
      recommendations[0]!.id,
    );
    const repeated = await client.acceptContinuityOptimization(
      recommendations[0]!.id,
      request,
    );
    assert.equal(repeated.draft_sequence.id, accepted.draft_sequence.id);
    assert.equal(repeated.release.id, accepted.release.id);
    await assert.rejects(
      () => client.acceptContinuityOptimization(recommendations[0]!.id, {
        ...request,
        recommendation: {
          ...recommendations[0],
          estimated_impact_millipoints: 999,
        },
      }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
    const provenance = await client.listContinuityProvenance(
      execution.continuity_id,
      tenantId,
    );
    assert.equal(
      provenance.filter((entry) => entry.kind === "optimization_accepted").length,
      1,
    );
  });

  it("reassigns expired attention leases and settles one hashed decision", async () => {
    const tenantId = `continuity-attention-${uuid().slice(0, 8)}`;
    const created = await client.createSequence(testSequence(
      `attention-${uuid().slice(0, 8)}`,
      [step("work", "noop")],
      { tenantId },
    ));
    const instance = await client.createInstance({
      sequence_id: created.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
      budget: { max_attention_units: 2 },
    });
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    const task = await client.createAttentionTask({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      required_skills: ["fraud"],
      classification: "confidential",
      allowed_regions: ["br-south"],
      priority: 2,
      deadline: new Date(Date.now() + 60_000).toISOString(),
      estimated_attention_units: 2,
    });
    assert.ok(!("payload" in task));
    await assert.rejects(
      () => client.createAttentionTask({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        required_skills: ["fraud"],
        classification: "confidential",
        allowed_regions: ["br-south"],
        priority: 2,
        deadline: new Date(Date.now() + 60_000).toISOString(),
        estimated_attention_units: 1,
      }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
    await client.assignAttentionTask(task.id, {
      tenant_id: tenantId,
      lease_seconds: 1,
      reviewers: [{
        reviewer_id: "reviewer-a",
        tenant_ids: [tenantId],
        skills: ["fraud"],
        region: "br-south",
        trust: "signed",
        available_attention_units: 2,
      }],
    });
    await new Promise((resolve) => setTimeout(resolve, 1_100));
    const reassigned = await client.assignAttentionTask(task.id, {
      tenant_id: tenantId,
      lease_seconds: 60,
      reviewers: [{
        reviewer_id: "reviewer-b",
        tenant_ids: [tenantId],
        skills: ["fraud"],
        region: "br-south",
        trust: "signed",
        available_attention_units: 2,
      }],
    });
    assert.equal(reassigned.assignee, "reviewer-b");
    const decisionSha256 = createHash("sha256")
      .update("approve-with-secret-comment-not-uploaded")
      .digest("hex");
    const decisions = await Promise.allSettled([
      client.decideAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewer_id: "reviewer-b",
        decision_sha256: decisionSha256,
      }),
      client.decideAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewer_id: "reviewer-b",
        decision_sha256: decisionSha256,
      }),
    ]);
    assert.equal(decisions.filter((result) => result.status === "fulfilled").length, 1);
    const decided = decisions.find((result) => result.status === "fulfilled")!;
    assert.equal(decided.value.state, "decided");
    assert.equal(decided.value.decision_sha256, decisionSha256);
    assert.ok(!JSON.stringify(decided.value).includes("secret-comment"));
    const reservations = await client.listContinuityBudgetReservations(
      execution.continuity_id,
      tenantId,
    );
    assert.equal(reservations[0].state, "reconciled");
    assert.equal(reservations[0].actual.attention_units, 2);
  });

  it("derives sovereign-edge trust and accepts only configured federation peers", async () => {
    const tenantId = federationTenantId;
    const sourceRuntimeId = uuid();
    const destinationRuntimeId = uuid();
    const observedAt = Date.now();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: cameraRuntimeCapabilities(sourceRuntimeId, observedAt, 10),
    });
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: {
        ...cameraRuntimeCapabilities(destinationRuntimeId, observedAt, 5),
        kind: "desktop",
        hardware: [],
      },
    });
    const parentSequence = await client.createSequence(testSequence(
      `sovereign-parent-${uuid().slice(0, 8)}`,
      [step("work", "noop")],
      { tenantId },
    ));
    const subSequence = await client.createSequence(testSequence(
      `sovereign-child-${uuid().slice(0, 8)}`,
      [step("delegated", "noop")],
      { tenantId },
    ));
    const instance = await client.createInstance({
      sequence_id: parentSequence.id,
      tenant_id: tenantId,
      namespace: "default",
      next_fire_at: new Date(Date.now() + 60_000).toISOString(),
    });
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: sourceRuntimeId,
    });
    const publicResidency = await client.evaluateContinuityResidency({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      classification: "public",
      operation: "device_delegation",
      destination_runtime_id: destinationRuntimeId,
      allowed_regions: ["br-south"],
    });
    assert.equal(publicResidency.outcome, "pass");
    const protectedResidency = await client.evaluateContinuityResidency({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      classification: "restricted",
      operation: "device_delegation",
      destination_runtime_id: destinationRuntimeId,
      allowed_regions: ["br-south"],
    });
    assert.equal(protectedResidency.outcome, "fail");
    assert.ok(protectedResidency.finding_codes.includes("DESTINATION_TRUST_TOO_LOW"));
    await assert.rejects(
      () => client.minimizeContinuityDisclosure({
        classification: "restricted",
        payload: { secret: "must-never-reach-server-minimization" },
        allowed_top_level_fields: [],
      }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 400);
        return true;
      },
    );
    const grant = await client.issueContinuationGrant({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      destination_runtime_id: destinationRuntimeId,
      subject: "device-mesh-child",
      allowed_actions: ["accept"],
      ttl_seconds: 60,
    });
    const delegation = {
      id: uuid(),
      tenant_id: tenantId,
      parent_continuity_id: execution.continuity_id,
      parent_epoch: execution.epoch,
      source_runtime_id: sourceRuntimeId,
      destination_runtime_id: destinationRuntimeId,
      sub_sequence_id: subSequence.id,
      grant_id: grant.signed_grant.grant.id,
      expires_at: new Date(Date.now() + 30_000).toISOString(),
    };
    const claimed = await client.claimDeviceDelegation({
      tenant_id: tenantId,
      delegation,
      signed_grant: grant.signed_grant,
      token: grant.token,
    });
    assert.equal(claimed.destination_runtime_id, destinationRuntimeId);
    await assert.rejects(
      () => client.claimDeviceDelegation({
        tenant_id: tenantId,
        delegation,
        signed_grant: grant.signed_grant,
        token: grant.token,
      }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );

    const federationPayload = Buffer.from("encrypted-artifact-reference-only");
    const unsignedEnvelope = {
      id: uuid(),
      peer_id: federationPeerId,
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      epoch: execution.epoch,
      destination_runtime_id: destinationRuntimeId,
      payload_sha256: createHash("sha256").update(federationPayload).digest("hex"),
      issued_at: new Date(Date.now() - 100).toISOString(),
      expires_at: new Date(Date.now() + 30_000).toISOString(),
      signature: "",
    };
    const signature = sign(
      null,
      Buffer.from(JSON.stringify(unsignedEnvelope)),
      federationPrivateKey,
    ).toString("base64");
    const federationRequest = {
      tenant_id: tenantId,
      envelope: { ...unsignedEnvelope, signature },
      payload_base64: federationPayload.toString("base64"),
    };
    const deliveries = await Promise.allSettled([
      client.verifyFederationEnvelope(federationRequest),
      client.verifyFederationEnvelope(federationRequest),
    ]);
    assert.equal(
      deliveries.filter((delivery) => delivery.status === "fulfilled").length,
      1,
      "the durable federation inbox must admit one concurrent delivery",
    );
    const replay = deliveries.find(
      (delivery): delivery is PromiseRejectedResult => delivery.status === "rejected",
    );
    assert.ok(replay);
    assert.equal((replay.reason as ApiError).status, 409);
    const provenance = await client.listContinuityProvenance(
      execution.continuity_id,
      tenantId,
    );
    assert.ok(provenance.some((entry) => entry.kind === "residency_evaluated"));
    assert.ok(provenance.some((entry) => entry.kind === "device_delegation"));
    assert.ok(provenance.some((entry) => entry.kind === "federation_received"));
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
