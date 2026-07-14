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
    server = await startServer();
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
