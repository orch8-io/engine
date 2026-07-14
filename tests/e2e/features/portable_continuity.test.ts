/** E2E: portable continuity identity, capabilities, preview, and handoff request. */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";

import { ApiError, Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Portable Continuity", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "42".repeat(32),
        ORCH8_LOG_LEVEL: "error",
      },
    });
  });

  after(async () => {
    await stopServer(server);
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
  });
});
