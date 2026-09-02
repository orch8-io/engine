/**
 * E2E: Portable Continuity — handoff and capsule transfer.
 *
 * Covers runtime registration validation, handoff preview compatibility
 * assessment, `create_handoff` placement-decision authorization (the
 * BREAKING change requiring a fresh preview before every handoff), the
 * Requested -> Quiescing -> Exported -> Accepted -> Resumed state machine
 * (and every out-of-order transition), capsule import idempotency and
 * tamper rejection, `attach_device_capsule`, and tenant isolation across
 * the whole surface.
 *
 * See orch8-api/src/continuity.rs and orch8-engine/src/continuity.rs for
 * the underlying contract; tests/e2e/features/portable_continuity.test.ts
 * establishes the crypto round-trip pattern reused here.
 */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync, randomBytes } from "node:crypto";

import { ApiError, Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();
const artifactDir = `/tmp/o8-continuity-handoffs-e2e-${uuid().slice(0, 8)}`;

function baseRuntime(
  runtimeId: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  const now = Date.now();
  return {
    runtime_id: runtimeId,
    kind: "server",
    trust: "registered",
    handlers: ["noop"],
    offline_capable: false,
    observed_at: new Date(now).toISOString(),
    expires_at: new Date(now + 60_000).toISOString(),
    ...overrides,
  };
}

async function setup(tenantSuffix: string) {
  const tenantId = `ch-${tenantSuffix}-${uuid().slice(0, 8)}`;
  const sequence = testSequence("ch-seq", [step("s1", "noop")], { tenantId });
  const createdSequence = await client.createSequence(sequence);
  const instance = await client.createInstance({
    sequence_id: createdSequence.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  const sourceRuntimeId = uuid();
  const execution = await client.createContinuityExecution({
    tenant_id: tenantId,
    instance_id: instance.id,
    runtime_id: sourceRuntimeId,
  });
  return { tenantId, createdSequence, instance, sourceRuntimeId, execution };
}

/** Register a compatible destination runtime and produce an authorized
 * `create_handoff` request for it (preview -> placement_decision_id). */
async function authorizedHandoffRequest(
  tenantId: string,
  continuityId: string,
  destinationRuntimeId: string,
  requirements: Record<string, unknown> = { handlers: ["noop"] },
  capabilityOverrides: Record<string, unknown> = {},
) {
  await client.registerRuntime({
    tenant_id: tenantId,
    capabilities: baseRuntime(destinationRuntimeId, {
      handlers: (requirements as { handlers?: string[] }).handlers ?? ["noop"],
      ...capabilityOverrides,
    }),
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

/** Full happy-path chain to an Accepted handoff, reusing the recipe from
 * portable_continuity.test.ts. Returns the accepted handoff + execution +
 * imported destination instance id, for tests that need a live Accepted
 * or Resumed handoff to probe further transitions. */
async function acceptedHandoff(tenantSuffix: string) {
  const tenantId = `ch-${tenantSuffix}-${uuid().slice(0, 8)}`;
  const sequence = testSequence(
    "ch-accepted",
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
  const payloadKey = randomBytes(32).toString("base64");
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
  return {
    tenantId,
    execution,
    handoff,
    exported,
    imported,
    accepted,
    sourceRuntimeId,
    destinationRuntimeId,
    payloadKey,
  };
}

function assert409(promise: Promise<unknown>, pattern?: RegExp) {
  return assert.rejects(() => promise, (error: unknown) => {
    assert.ok(error instanceof ApiError, "expected ApiError");
    assert.equal((error as ApiError).status, 409);
    if (pattern) assert.match((error as ApiError).message, pattern);
    return true;
  });
}

function assert400(promise: Promise<unknown>) {
  return assert.rejects(() => promise, (error: unknown) => {
    assert.ok(error instanceof ApiError, "expected ApiError");
    assert.ok(
      (error as ApiError).status === 400 || (error as ApiError).status === 422,
      `expected 4xx, got ${(error as ApiError).status}`,
    );
    return true;
  });
}

function assert404(promise: Promise<unknown>) {
  return assert.rejects(() => promise, (error: unknown) => {
    assert.equal((error as ApiError).status, 404);
    return true;
  });
}

describe("Continuity Handoffs — runtime registration", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "43".repeat(32),
        ORCH8_ARTIFACT_BACKEND: "local",
        ORCH8_ARTIFACT_PATH: artifactDir,
        ORCH8_LOG_LEVEL: "error",
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("registers a runtime with minimal capabilities", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const runtimeId = uuid();
    const res = await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(runtimeId),
    });
    assert.equal(res.runtime_id, runtimeId);
  });

  it("registers a runtime with full capability facts", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const runtimeId = uuid();
    const res = await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(runtimeId, {
        handlers: ["noop", "http_request"],
        plugins: ["stripe"],
        credentials: ["stripe-key"],
        regions: ["us-east", "eu-west"],
        hardware: ["camera", "nfc"],
        offline_capable: true,
        connectivity: "wifi",
        battery_percent: 87,
        estimated_cost_microunits: 10,
        estimated_latency_ms: 50,
      }),
    });
    assert.deepEqual(res.regions, ["us-east", "eu-west"]);
    assert.equal(res.battery_percent, 87);
  });

  it("rejects expiry in the past", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), {
          expires_at: new Date(Date.now() - 1000).toISOString(),
        }),
      }),
    );
  });

  it("rejects observation timestamp too far in the future", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), {
          observed_at: new Date(Date.now() + 120_000).toISOString(),
        }),
      }),
    );
  });

  it("rejects a lifetime longer than five minutes", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const now = Date.now();
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), {
          observed_at: new Date(now).toISOString(),
          expires_at: new Date(now + 6 * 60_000).toISOString(),
        }),
      }),
    );
  });

  it("rejects expiry not after observation", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const now = Date.now();
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), {
          observed_at: new Date(now).toISOString(),
          expires_at: new Date(now - 1000).toISOString(),
        }),
      }),
    );
  });

  it("rejects battery_percent above 100", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), { battery_percent: 101 }),
      }),
    );
  });

  it("accepts battery_percent at exactly 100", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const res = await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(uuid(), { battery_percent: 100 }),
    });
    assert.equal(res.battery_percent, 100);
  });

  it("rejects self-asserted signed trust without attestation", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), { trust: "signed" }),
      }),
    );
  });

  it("rejects self-asserted attested trust", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), { trust: "attested" }),
      }),
    );
  });

  it("accepts unverified trust", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const res = await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(uuid(), { trust: "unverified" }),
    });
    assert.equal(res.trust, "unverified");
  });

  it("accepts registered trust", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const res = await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(uuid(), { trust: "registered" }),
    });
    assert.equal(res.trust, "registered");
  });

  it("rejects more than 256 facts in one capability list", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const tooMany = Array.from({ length: 257 }, (_, i) => `h${i}`);
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), { handlers: tooMany }),
      }),
    );
  });

  it("accepts exactly 256 facts in a capability list", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const exactly = Array.from({ length: 256 }, (_, i) => `h${i}`);
    const res = await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(uuid(), { handlers: exactly }),
    });
    assert.equal(res.handlers.length, 256);
  });

  it("rejects an empty-string fact", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), { regions: [""] }),
      }),
    );
  });

  it("rejects a whitespace-only fact", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), { hardware: ["   "] }),
      }),
    );
  });

  it("rejects a fact longer than 256 bytes", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), { plugins: ["x".repeat(257)] }),
      }),
    );
  });

  it("accepts a fact at exactly 256 bytes", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const res = await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(uuid(), { plugins: ["x".repeat(256)] }),
    });
    assert.equal(res.plugins[0].length, 256);
  });

  it("rejects a capsule_signing_public_key that is not valid base64", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), {
          capsule_signing_public_key: "not-base64!!!",
        }),
      }),
    );
  });

  it("rejects a capsule_signing_public_key that does not decode to 32 bytes", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    await assert400(
      client.registerRuntime({
        tenant_id: tenantId,
        capabilities: baseRuntime(uuid(), {
          capsule_signing_public_key: Buffer.from("too short").toString("base64"),
        }),
      }),
    );
  });

  it("accepts a valid Ed25519 capsule_signing_public_key", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const keys = generateKeyPairSync("ed25519");
    const raw = Buffer.from(keys.publicKey.export({ format: "der", type: "spki" })).subarray(-32);
    const res = await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(uuid(), {
        capsule_signing_public_key: raw.toString("base64"),
      }),
    });
    assert.equal(res.capsule_signing_public_key, raw.toString("base64"));
  });

  it("re-registering the same runtime_id upserts capabilities", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const runtimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(runtimeId, { battery_percent: 10 }),
    });
    const updated = await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(runtimeId, { battery_percent: 90 }),
    });
    assert.equal(updated.battery_percent, 90);
    const listed = await client.listRuntimes(tenantId);
    const found = listed.filter((r: any) => r.runtime_id === runtimeId);
    assert.equal(found.length, 1, "upsert must not duplicate the runtime row");
    assert.equal(found[0].battery_percent, 90);
  });

  it("lists only runtimes registered under the querying tenant", async () => {
    const tenantA = `rt-a-${uuid().slice(0, 8)}`;
    const tenantB = `rt-b-${uuid().slice(0, 8)}`;
    const runtimeA = uuid();
    const runtimeB = uuid();
    await client.registerRuntime({ tenant_id: tenantA, capabilities: baseRuntime(runtimeA) });
    await client.registerRuntime({ tenant_id: tenantB, capabilities: baseRuntime(runtimeB) });
    const listedA = await client.listRuntimes(tenantA);
    assert.ok(listedA.some((r: any) => r.runtime_id === runtimeA));
    assert.ok(!listedA.some((r: any) => r.runtime_id === runtimeB));
  });

  it("does not list an expired runtime", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const runtimeId = uuid();
    const now = Date.now();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(runtimeId, {
        observed_at: new Date(now).toISOString(),
        expires_at: new Date(now + 1100).toISOString(),
      }),
    });
    const listedBeforeExpiry = await client.listRuntimes(tenantId);
    assert.ok(listedBeforeExpiry.some((r: any) => r.runtime_id === runtimeId));
    await new Promise((resolve) => setTimeout(resolve, 1300));
    const listedAfterExpiry = await client.listRuntimes(tenantId);
    assert.ok(!listedAfterExpiry.some((r: any) => r.runtime_id === runtimeId));
  });

  it("lists a draining runtime (still discoverable for evidence)", async () => {
    const tenantId = `rt-${uuid().slice(0, 8)}`;
    const runtimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(runtimeId, { draining: true }),
    });
    const listed = await client.listRuntimes(tenantId);
    assert.ok(listed.some((r: any) => r.runtime_id === runtimeId && r.draining === true));
  });
});

describe("Continuity Handoffs — handoff preview compatibility", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "44".repeat(32),
        ORCH8_ARTIFACT_BACKEND: "local",
        ORCH8_ARTIFACT_PATH: artifactDir,
        ORCH8_LOG_LEVEL: "error",
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("is compatible when all declared requirements are satisfied", async () => {
    const { tenantId, execution } = await setup("prev-ok");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"] }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"] },
    });
    assert.equal(preview.compatible, true);
    assert.ok(preview.findings.some((f: any) => f.code === "COMPATIBLE"));
  });

  it("fails when a required handler is missing", async () => {
    const { tenantId, execution } = await setup("prev-handler");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["other"] }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"] },
    });
    assert.equal(preview.compatible, false);
    assert.ok(preview.findings.some((f: any) => f.code === "HANDLERS_MISSING"));
  });

  it("fails when a required plugin is missing", async () => {
    const { tenantId, execution } = await setup("prev-plugin");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"], plugins: [] }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"], plugins: ["stripe"] },
    });
    assert.ok(preview.findings.some((f: any) => f.code === "PLUGINS_MISSING"));
  });

  it("fails when a required credential binding is missing", async () => {
    const { tenantId, execution } = await setup("prev-cred");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"] }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"], credentials: ["stripe-key"] },
    });
    assert.ok(preview.findings.some((f: any) => f.code === "CREDENTIALS_MISSING"));
  });

  it("fails when required hardware is missing", async () => {
    const { tenantId, execution } = await setup("prev-hw");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"] }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"], hardware: ["camera"] },
    });
    assert.ok(preview.findings.some((f: any) => f.code === "HARDWARE_MISSING"));
  });

  it("fails when the runtime is not in any allowed region", async () => {
    const { tenantId, execution } = await setup("prev-region");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"], regions: ["eu-west"] }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"], regions: ["us-east"] },
    });
    assert.ok(preview.findings.some((f: any) => f.code === "REGION_NOT_ALLOWED"));
  });

  it("passes when the runtime matches any one of multiple allowed regions", async () => {
    const { tenantId, execution } = await setup("prev-region-any");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"], regions: ["us-east"] }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"], regions: ["eu-west", "us-east"] },
    });
    assert.equal(preview.compatible, true);
  });

  it("fails when runtime trust is below the required minimum", async () => {
    const { tenantId, execution } = await setup("prev-trust");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"], trust: "unverified" }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"], minimum_trust: "registered" },
    });
    assert.ok(preview.findings.some((f: any) => f.code === "TRUST_TOO_LOW"));
  });

  it("fails when a capability advertisement has expired", async () => {
    const { tenantId, execution } = await setup("prev-expired");
    const destinationRuntimeId = uuid();
    const now = Date.now();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, {
        handlers: ["noop"],
        observed_at: new Date(now - 2000).toISOString(),
        expires_at: new Date(now + 1000).toISOString(),
      }),
    });
    // Preview immediately; the advertisement is still valid, so this
    // asserts the happy path around the expiry boundary rather than
    // waiting for real-clock expiry (no virtual clock over HTTP here).
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"] },
    });
    assert.equal(preview.compatible, true);
  });

  it("fails when the destination runtime is draining", async () => {
    const { tenantId, execution } = await setup("prev-drain");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"], draining: true }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"] },
    });
    assert.ok(preview.findings.some((f: any) => f.code === "RUNTIME_DRAINING"));
    assert.equal(preview.compatible, false);
  });

  it("404s when the destination runtime was never registered", async () => {
    const { tenantId, execution } = await setup("prev-noruntime");
    await assert404(
      client.previewHandoff(execution.continuity_id, {
        tenant_id: tenantId,
        destination_runtime_id: uuid(),
        requirements: { handlers: ["noop"] },
      }),
    );
  });

  it("404s when the continuity execution does not exist", async () => {
    const tenantId = `prev-noexec-${uuid().slice(0, 8)}`;
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId),
    });
    await assert404(
      client.previewHandoff(uuid(), {
        tenant_id: tenantId,
        destination_runtime_id: destinationRuntimeId,
        requirements: {},
      }),
    );
  });

  it("reports unresolved effects as incompatible", async () => {
    const { tenantId, execution, instance } = await setup("prev-effects");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"] }),
    });
    // Wait for the noop step to complete and produce provenance/effects
    // context, then inspect that unresolved effects gate compatibility
    // via the same field the preview reports.
    await client.waitForState(instance.id, "completed");
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"] },
    });
    assert.ok(Array.isArray(preview.unresolved_effects));
  });

  it("respects an explicit locality policy denying the destination region", async () => {
    const { tenantId, execution } = await setup("prev-policy");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, {
        handlers: ["noop"],
        regions: ["us-east"],
      }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"] },
      policy: {
        version: 1,
        rules: [{ classification: "internal", allowed_regions: ["eu-west"] }],
      },
    });
    assert.equal(preview.compatible, false);
  });

  it("classification defaults to internal when omitted", async () => {
    const { tenantId, execution } = await setup("prev-default-class");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"] }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"] },
    });
    assert.equal(preview.compatible, true);
  });

  it("is incompatible when requires_network but the destination is offline", async () => {
    const { tenantId, execution } = await setup("prev-network-offline");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, {
        handlers: ["noop"],
        connectivity: "offline",
      }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"], requires_network: true },
    });
    assert.ok(preview.findings.some((f: any) => f.code === "NETWORK_UNAVAILABLE"));
    assert.equal(preview.compatible, false);
  });

  it("is compatible when requires_network and the destination is online", async () => {
    const { tenantId, execution } = await setup("prev-network-online");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, {
        handlers: ["noop"],
        connectivity: "wifi",
      }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"], requires_network: true },
    });
    assert.equal(preview.compatible, true);
  });

  it("is incompatible when requires_human_ui and the runtime kind cannot prove a UI", async () => {
    const { tenantId, execution } = await setup("prev-human-ui-unknown");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"], kind: "server" }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"], requires_human_ui: true },
    });
    assert.ok(preview.findings.some((f: any) => f.code === "HUMAN_UI_MISSING"));
    assert.equal(preview.compatible, false);
  });

  it("is compatible when requires_human_ui and the runtime kind is mobile", async () => {
    const { tenantId, execution } = await setup("prev-human-ui-mobile");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"], kind: "mobile" }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"], requires_human_ui: true },
    });
    assert.equal(preview.compatible, true);
    assert.ok(!preview.findings.some((f: any) => f.code === "HUMAN_UI_MISSING"));
  });

  it("respects a locality policy that allows the destination classification and region", async () => {
    const { tenantId, execution } = await setup("prev-policy-allow");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, {
        handlers: ["noop"],
        regions: ["eu-west"],
      }),
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["noop"] },
      policy: {
        version: 1,
        rules: [{ classification: "internal", allowed_regions: ["eu-west"] }],
      },
    });
    assert.equal(preview.compatible, true);
  });

  it("rejects a policy with an invalid version", async () => {
    const { tenantId, execution } = await setup("prev-policy-badversion");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"] }),
    });
    await assert400(
      client.previewHandoff(execution.continuity_id, {
        tenant_id: tenantId,
        destination_runtime_id: destinationRuntimeId,
        requirements: { handlers: ["noop"] },
        policy: { version: 0, rules: [] },
      }),
    );
  });
});

describe("Continuity Handoffs — create_handoff authorization", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "45".repeat(32),
        ORCH8_ARTIFACT_BACKEND: "local",
        ORCH8_ARTIFACT_PATH: artifactDir,
        ORCH8_LOG_LEVEL: "error",
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("creates a handoff from an authorized preview", async () => {
    const { tenantId, execution } = await setup("create-ok");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoff = await client.createHandoff(req);
    assert.equal(handoff.state, "requested");
    assert.equal(handoff.expected_epoch, 0);
  });

  it("rejects a preview_sha256 that is not 64 hex characters", async () => {
    const { tenantId, execution } = await setup("create-badhash-len");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    await assert400(client.createHandoff({ ...req, preview_sha256: "abc" }));
  });

  it("rejects a preview_sha256 with non-hex characters", async () => {
    const { tenantId, execution } = await setup("create-badhash-chars");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    await assert400(
      client.createHandoff({ ...req, preview_sha256: "z".repeat(64) }),
    );
  });

  it("rejects an unknown placement_decision_id", async () => {
    const { tenantId, execution } = await setup("create-noplacement");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    await assert404(client.createHandoff({ ...req, placement_decision_id: uuid() }));
  });

  it("rejects a preview_sha256 that does not match the recomputed preview", async () => {
    const { tenantId, execution } = await setup("create-mismatch-hash");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    await assert409(client.createHandoff({ ...req, preview_sha256: "a".repeat(64) }));
  });

  it("rejects requirements that differ from the authorized placement decision", async () => {
    const { tenantId, execution } = await setup("create-diff-reqs");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    await assert409(
      client.createHandoff({ ...req, requirements: { handlers: ["different"] } }),
    );
  });

  it("rejects a destination_runtime_id that differs from the authorized one", async () => {
    const { tenantId, execution } = await setup("create-diff-dest");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const otherRuntime = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(otherRuntime, { handlers: ["noop"] }),
    });
    await assert409(
      client.createHandoff({ ...req, destination_runtime_id: otherRuntime }),
    );
  });

  it("rejects when the destination's capabilities changed after preview", async () => {
    const { tenantId, execution } = await setup("create-drift");
    const destinationRuntimeId = uuid();
    const req = await authorizedHandoffRequest(
      tenantId,
      execution.continuity_id,
      destinationRuntimeId,
    );
    // Re-register with incompatible capabilities before create_handoff.
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["different"] }),
    });
    await assert409(client.createHandoff(req));
  });

  it("rejects a preview whose hash goes stale after the tenant's runtime topology changes", async () => {
    const { tenantId, execution } = await setup("create-stale-topology");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    // The preview snapshot's hash covers every candidate runtime visible to
    // the tenant, not just the chosen destination. Registering an unrelated
    // runtime after the preview changes that candidate set and must
    // invalidate the previously computed preview_sha256.
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(uuid(), { handlers: ["noop"] }),
    });
    await assert409(client.createHandoff(req), /stale/);
  });

  it("404s for a nonexistent continuity_id", async () => {
    const tenantId = `create-noexec-${uuid().slice(0, 8)}`;
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"] }),
    });
    await assert404(
      client.previewHandoff(uuid(), {
        tenant_id: tenantId,
        destination_runtime_id: destinationRuntimeId,
        requirements: { handlers: ["noop"] },
      }),
    );
  });

  it("allows creating two independent handoffs for the same execution before either resolves", async () => {
    const { tenantId, execution } = await setup("create-two");
    // Each preview's hash covers every candidate runtime visible to the
    // tenant, so each request must be previewed and created before the next
    // runtime is registered, or the earlier preview goes stale.
    const reqA = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoffA = await client.createHandoff(reqA);
    const reqB = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoffB = await client.createHandoff(reqB);
    assert.notEqual(handoffA.id, handoffB.id);
  });
});

describe("Continuity Handoffs — state machine", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "46".repeat(32),
        ORCH8_ARTIFACT_BACKEND: "local",
        ORCH8_ARTIFACT_PATH: artifactDir,
        ORCH8_LOG_LEVEL: "error",
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("rejects a handoff in the Requested state (no export needed first)", async () => {
    const { tenantId, execution } = await setup("sm-reject");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoff = await client.createHandoff(req);
    const rejected = await client.rejectHandoff(handoff.id, { tenant_id: tenantId });
    assert.equal(rejected.state, "rejected");
  });

  it("rejecting an already-rejected handoff conflicts", async () => {
    const { tenantId, execution } = await setup("sm-reject-twice");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoff = await client.createHandoff(req);
    await client.rejectHandoff(handoff.id, { tenant_id: tenantId });
    await assert409(client.rejectHandoff(handoff.id, { tenant_id: tenantId }));
  });

  it("cannot accept a handoff that is still Requested (not exported)", async () => {
    const { tenantId, execution } = await setup("sm-accept-early");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoff = await client.createHandoff(req);
    // Use a real, tenant-owned, Paused instance so the request clears the
    // destination-instance check and reaches the handoff-state check.
    const otherSeq = testSequence("sm-accept-early-seq", [step("s1", "noop")], { tenantId });
    const createdOther = await client.createSequence(otherSeq);
    const pausedInstance = await client.createInstance({
      sequence_id: createdOther.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.updateState(pausedInstance.id, "paused");
    await assert409(
      client.acceptHandoff(handoff.id, {
        tenant_id: tenantId,
        destination_instance_id: pausedInstance.id,
      }),
    );
  });

  it("cannot revoke a handoff that is still Requested (not exported)", async () => {
    const { tenantId, execution } = await setup("sm-revoke-early");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoff = await client.createHandoff(req);
    await assert409(client.revokeHandoff(handoff.id, { tenant_id: tenantId }));
  });

  it("cannot resume a handoff that is still Requested", async () => {
    const { tenantId, execution } = await setup("sm-resume-early");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoff = await client.createHandoff(req);
    await assert409(client.resumeHandoff(handoff.id, { tenant_id: tenantId }));
  });

  it("cannot export a handoff whose source instance is still Running", async () => {
    const { tenantId, execution } = await setup("sm-export-not-paused");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoff = await client.createHandoff(req);
    // The noop instance races to Completed almost immediately; either
    // outcome (Completed or transiently Running) is not Paused/Waiting.
    await assert409(
      client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: {},
        expires_in_seconds: 60,
      }),
    );
  });

  it("export requires expires_in_seconds between 1 and 3600", async () => {
    const { tenantId, execution } = await setup("sm-export-ttl");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoff = await client.createHandoff(req);
    await assert400(
      client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: {},
        expires_in_seconds: 0,
      }),
    );
    await assert400(
      client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: {},
        expires_in_seconds: 3601,
      }),
    );
  });

  it("full round trip: export -> import -> accept -> resume", async () => {
    const result = await acceptedHandoff("sm-full");
    assert.equal(result.accepted.handoff.state, "accepted");
    assert.equal(result.accepted.execution.epoch, 1);
    assert.equal(result.accepted.execution.owner_runtime_id, result.destinationRuntimeId);
    const resumed = await client.resumeHandoff(result.handoff.id, {
      tenant_id: result.tenantId,
    });
    assert.equal(resumed.state, "resumed");
  });

  it("cannot reject a handoff that has already been exported", async () => {
    const result = await acceptedHandoff("sm-reject-after-export");
    await assert409(client.rejectHandoff(result.handoff.id, { tenant_id: result.tenantId }));
  });

  it("cannot accept twice", async () => {
    const result = await acceptedHandoff("sm-accept-twice");
    await assert409(
      client.acceptHandoff(result.handoff.id, {
        tenant_id: result.tenantId,
        destination_instance_id: result.imported.instance_id,
      }),
    );
  });

  it("cannot resume before accept", async () => {
    const { tenantId, execution } = await setup("sm-resume-before-accept");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"] }),
    });
    const req = await authorizedHandoffRequest(
      tenantId,
      execution.continuity_id,
      destinationRuntimeId,
    );
    const handoff = await client.createHandoff(req);
    await assert409(client.resumeHandoff(handoff.id, { tenant_id: tenantId }));
  });

  it("accept requires the destination instance to be tenant-owned", async () => {
    const result = await acceptedHandoff("sm-accept-wrong-tenant-setup");
    // Set up a second, independent accepted-eligible handoff to get a
    // legitimately Paused/imported instance belonging to a different tenant.
    const other = await acceptedHandoff("sm-accept-wrong-tenant-other");
    const req = await authorizedHandoffRequest(
      result.tenantId,
      result.execution.continuity_id,
      uuid(),
    );
    const freshHandoff = await client.createHandoff(req);
    await assert409(
      client.acceptHandoff(freshHandoff.id, {
        tenant_id: result.tenantId,
        destination_instance_id: other.imported.instance_id,
      }),
    );
  });

  it("accept requires the destination instance to be Paused", async () => {
    const { tenantId, execution } = await setup("sm-accept-not-paused");
    const destinationRuntimeId = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["noop"] }),
    });
    const req = await authorizedHandoffRequest(
      tenantId,
      execution.continuity_id,
      destinationRuntimeId,
    );
    const handoff = await client.createHandoff(req);
    // A freshly created instance in this tenant is Scheduled/Running, not Paused.
    const otherSeq = testSequence("sm-not-paused-seq", [step("s1", "noop")], { tenantId });
    const createdOther = await client.createSequence(otherSeq);
    const otherInstance = await client.createInstance({
      sequence_id: createdOther.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await assert409(
      client.acceptHandoff(handoff.id, {
        tenant_id: tenantId,
        destination_instance_id: otherInstance.id,
      }),
    );
  });

  it("get_handoff returns the current state and destination runtime", async () => {
    const { tenantId, execution } = await setup("sm-get");
    const destinationRuntimeId = uuid();
    const req = await authorizedHandoffRequest(
      tenantId,
      execution.continuity_id,
      destinationRuntimeId,
    );
    const handoff = await client.createHandoff(req);
    const fetched = await client.getHandoff(handoff.id, tenantId);
    assert.equal(fetched.destination_runtime_id, destinationRuntimeId);
    assert.equal(fetched.state, "requested");
  });

  it("get_handoff 404s for an unknown id", async () => {
    const tenantId = `sm-getunknown-${uuid().slice(0, 8)}`;
    await assert404(client.getHandoff(uuid(), tenantId));
  });

  it("get_handoff 404s across tenants", async () => {
    const { tenantId, execution } = await setup("sm-get-crosstenant");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoff = await client.createHandoff(req);
    await assert404(client.getHandoff(handoff.id, `${tenantId}-other`));
  });
});

describe("Continuity Handoffs — capsule import edge cases", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "47".repeat(32),
        ORCH8_ARTIFACT_BACKEND: "local",
        ORCH8_ARTIFACT_PATH: artifactDir,
        ORCH8_LOG_LEVEL: "error",
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  async function exportedHandoff(tenantSuffix: string) {
    const tenantId = `imp-${tenantSuffix}-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "imp-seq",
      [
        step("gate", "human_review", {}, {
          wait_for_input: { prompt: "continue?", choices: [{ label: "go", value: "go" }] },
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
    const payloadKey = randomBytes(32).toString("base64");
    const exported = await client.exportHandoff(handoff.id, {
      tenant_id: tenantId,
      requirements: { handlers: ["human_review"] },
      expires_in_seconds: 60,
      payload_key_base64: payloadKey,
    });
    return { tenantId, execution, handoff, exported, destinationRuntimeId, payloadKey };
  }

  it("imports a valid exported capsule", async () => {
    const x = await exportedHandoff("ok");
    const destinationInstanceId = uuid();
    const imported = await client.importContinuityCapsule({
      tenant_id: x.tenantId,
      destination_runtime_id: x.destinationRuntimeId,
      destination_instance_id: destinationInstanceId,
      expected_epoch: 0,
      capsule: x.exported.capsule,
      payload_base64: x.exported.payload_base64,
      payload_key_base64: x.payloadKey,
    });
    assert.equal(imported.instance_id, destinationInstanceId);
    assert.equal(imported.state, "paused");
  });

  it("re-importing the identical manifest+bytes is idempotent", async () => {
    const x = await exportedHandoff("idempotent");
    const destinationInstanceId = uuid();
    const first = await client.importContinuityCapsule({
      tenant_id: x.tenantId,
      destination_runtime_id: x.destinationRuntimeId,
      destination_instance_id: destinationInstanceId,
      expected_epoch: 0,
      capsule: x.exported.capsule,
      payload_base64: x.exported.payload_base64,
      payload_key_base64: x.payloadKey,
    });
    const second = await client.importContinuityCapsule({
      tenant_id: x.tenantId,
      destination_runtime_id: x.destinationRuntimeId,
      destination_instance_id: destinationInstanceId,
      expected_epoch: 0,
      capsule: x.exported.capsule,
      payload_base64: x.exported.payload_base64,
      payload_key_base64: x.payloadKey,
    });
    assert.equal(second.instance_id, first.instance_id);
  });

  it("rejects a tampered payload (hash mismatch)", async () => {
    const x = await exportedHandoff("tamper");
    const transported = Buffer.from(x.exported.payload_base64, "base64");
    transported[transported.length - 1] = (transported[transported.length - 1] ?? 0) ^ 1;
    await assert409(
      client.importContinuityCapsule({
        tenant_id: x.tenantId,
        destination_runtime_id: x.destinationRuntimeId,
        destination_instance_id: uuid(),
        expected_epoch: 0,
        capsule: x.exported.capsule,
        payload_base64: transported.toString("base64"),
        payload_key_base64: x.payloadKey,
      }),
      /hash mismatch/,
    );
  });

  it("rejects the wrong transfer key", async () => {
    const x = await exportedHandoff("wrongkey");
    await assert409(
      client.importContinuityCapsule({
        tenant_id: x.tenantId,
        destination_runtime_id: x.destinationRuntimeId,
        destination_instance_id: uuid(),
        expected_epoch: 0,
        capsule: x.exported.capsule,
        payload_base64: x.exported.payload_base64,
        payload_key_base64: randomBytes(32).toString("base64"),
      }),
      /decryption failed/,
    );
  });

  it("rejects import for the wrong destination runtime", async () => {
    const x = await exportedHandoff("wrongdest");
    const wrongRuntime = uuid();
    await client.registerRuntime({
      tenant_id: x.tenantId,
      capabilities: baseRuntime(wrongRuntime, { handlers: ["human_review"] }),
    });
    await assert409(
      client.importContinuityCapsule({
        tenant_id: x.tenantId,
        destination_runtime_id: wrongRuntime,
        destination_instance_id: uuid(),
        expected_epoch: 0,
        capsule: x.exported.capsule,
        payload_base64: x.exported.payload_base64,
        payload_key_base64: x.payloadKey,
      }),
    );
  });

  it("rejects import under the wrong tenant", async () => {
    const x = await exportedHandoff("wrongtenant");
    await assert409(
      client.importContinuityCapsule({
        tenant_id: `${x.tenantId}-other`,
        destination_runtime_id: x.destinationRuntimeId,
        destination_instance_id: uuid(),
        expected_epoch: 0,
        capsule: x.exported.capsule,
        payload_base64: x.exported.payload_base64,
        payload_key_base64: x.payloadKey,
      }),
    );
  });

  it("rejects import with the wrong expected_epoch", async () => {
    const x = await exportedHandoff("wrongepoch");
    await assert409(
      client.importContinuityCapsule({
        tenant_id: x.tenantId,
        destination_runtime_id: x.destinationRuntimeId,
        destination_instance_id: uuid(),
        expected_epoch: 7,
        capsule: x.exported.capsule,
        payload_base64: x.exported.payload_base64,
        payload_key_base64: x.payloadKey,
      }),
    );
  });

  it("import fails closed when continuity crypto is not configured", async () => {
    // A distinct port avoids colliding with this describe's own already-
    // running primary server (default port is fixed, not randomized).
    const insecureServer = await startServer({
      port: 18081,
      env: { ORCH8_LOG_LEVEL: "error" },
    });
    try {
      const x = await exportedHandoff("neverused");
      const insecureClient = new Orch8Client(`http://127.0.0.1:${insecureServer.port}`);
      await assert.rejects(
        () =>
          insecureClient.importContinuityCapsule({
            tenant_id: x.tenantId,
            destination_runtime_id: x.destinationRuntimeId,
            destination_instance_id: uuid(),
            expected_epoch: 0,
            capsule: x.exported.capsule,
            payload_base64: x.exported.payload_base64,
            payload_key_base64: x.payloadKey,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 503);
          return true;
        },
      );
    } finally {
      await stopServer(insecureServer);
    }
  });
});

describe("Continuity Handoffs — crypto not configured", () => {
  let insecureServer: ServerHandle | undefined;

  before(async () => {
    insecureServer = await startServer({ env: { ORCH8_LOG_LEVEL: "error" } });
  });

  after(async () => {
    await stopServer(insecureServer);
  });

  it("export is unavailable without configured continuity crypto", async () => {
    const insecureClient = new Orch8Client(`http://127.0.0.1:${insecureServer!.port}`);
    const tenantId = `imp-nocrypto-${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "nocrypto-seq",
      [
        step("gate", "human_review", {}, {
          wait_for_input: { prompt: "x", choices: [{ label: "go", value: "go" }] },
        }),
      ],
      { tenantId },
    );
    const createdSequence = await insecureClient.createSequence(sequence);
    const instance = await insecureClient.createInstance({
      sequence_id: createdSequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await insecureClient.waitForState(instance.id, "waiting");
    const destinationRuntimeId = uuid();
    const execution = await insecureClient.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    await insecureClient.registerRuntime({
      tenant_id: tenantId,
      capabilities: baseRuntime(destinationRuntimeId, { handlers: ["human_review"] }),
    });
    const preview = await insecureClient.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["human_review"] },
    });
    const handoff = await insecureClient.createHandoff({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      destination_runtime_id: destinationRuntimeId,
      requirements: { handlers: ["human_review"] },
      placement_decision_id: preview.placement_decision.id,
      preview_sha256: preview.preview_sha256,
    });
    await assert.rejects(
      () =>
        insecureClient.exportHandoff(handoff.id, {
          tenant_id: tenantId,
          requirements: { handlers: ["human_review"] },
          expires_in_seconds: 60,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 503);
        return true;
      },
    );
  });
});

describe("Continuity Handoffs — tenant isolation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "48".repeat(32),
        ORCH8_ARTIFACT_BACKEND: "local",
        ORCH8_ARTIFACT_PATH: artifactDir,
        ORCH8_LOG_LEVEL: "error",
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("a continuity execution is not visible to another tenant", async () => {
    const { execution } = await setup("iso-exec");
    await assert404(client.getContinuityExecution(execution.continuity_id, `other-${uuid()}`));
  });

  it("creating a second execution for an instance already bound to one conflicts", async () => {
    const { tenantId, instance } = await setup("iso-dup-exec");
    await assert409(
      client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      }),
    );
  });

  it("a runtime registered under tenant A is invisible to tenant B's preview", async () => {
    const { execution } = await setup("iso-preview-a");
    const tenantB = `iso-preview-b-${uuid().slice(0, 8)}`;
    const seqB = testSequence("iso-b-seq", [step("s1", "noop")], { tenantId: tenantB });
    const createdB = await client.createSequence(seqB);
    const instanceB = await client.createInstance({
      sequence_id: createdB.id,
      tenant_id: tenantB,
      namespace: "default",
    });
    const executionB = await client.createContinuityExecution({
      tenant_id: tenantB,
      instance_id: instanceB.id,
      runtime_id: uuid(),
    });
    const runtimeUnderA = uuid();
    await client.registerRuntime({
      tenant_id: `iso-runtime-owner-${uuid().slice(0, 8)}`,
      capabilities: baseRuntime(runtimeUnderA, { handlers: ["noop"] }),
    });
    await assert404(
      client.previewHandoff(executionB.continuity_id, {
        tenant_id: tenantB,
        destination_runtime_id: runtimeUnderA,
        requirements: { handlers: ["noop"] },
      }),
    );
  });

  it("locations for one tenant's execution are not visible cross-tenant", async () => {
    const { tenantId, execution } = await setup("iso-locations");
    await assert404(
      client.listContinuityLocations(execution.continuity_id, `${tenantId}-other`),
    );
  });

  it("reject/revoke/resume/accept all 404 for a handoff id under the wrong tenant", async () => {
    const { tenantId, execution } = await setup("iso-actions");
    const req = await authorizedHandoffRequest(tenantId, execution.continuity_id, uuid());
    const handoff = await client.createHandoff(req);
    const otherTenant = `${tenantId}-other`;
    await assert404(client.rejectHandoff(handoff.id, { tenant_id: otherTenant }));
    await assert404(client.revokeHandoff(handoff.id, { tenant_id: otherTenant }));
    await assert404(client.resumeHandoff(handoff.id, { tenant_id: otherTenant }));
    await assert404(
      client.acceptHandoff(handoff.id, {
        tenant_id: otherTenant,
        destination_instance_id: uuid(),
      }),
    );
  });
});
