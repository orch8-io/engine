/**
 * Capability-scoped principals — the `auditor` capability.
 *
 * Auditor keys are read-only across every route family: any GET/HEAD passes
 * the capability gate, any write verb (POST/PATCH/DELETE) is 403 — even on
 * paths other capabilities would allow (worker poll, instance signals).
 * The suite plants real data as root, reads it all back as auditor, then
 * sweeps write verbs across families.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import {
  Orch8Client,
  testSequence,
  step,
  uuid,
} from "../client.ts";
import type { SequenceDef } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const ROOT_KEY = `root-principals-au-${uuid().slice(0, 8)}`;

describe("Principals — auditor capability (GET-only)", () => {
  let server: ServerHandle | undefined;
  let root: Orch8Client;
  let tenantId: string;
  let auditor: Orch8Client;
  let auditorSecret: string;
  let seq: SequenceDef;
  let instanceId: string;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_API_KEY: ROOT_KEY,
        ORCH8_ALLOW_NO_TENANT_ISOLATION: "1",
      },
    });
    root = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": ROOT_KEY,
    });
    tenantId = `au-${uuid().slice(0, 8)}`;

    // Plant data as root for the auditor to read.
    seq = testSequence("au-read", [step("s1", "noop")], { tenantId });
    await root.createSequence(seq);
    const created = await root.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    instanceId = created.id;
    await root.waitForState(instanceId, "completed", { timeoutMs: 15_000 });

    const key = await root.createApiKey({
      tenant_id: tenantId,
      name: "auditor",
      capabilities: ["auditor"],
    });
    auditorSecret = key.secret;
    auditor = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": auditorSecret,
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("reads instance listings and individual instances", async () => {
    const list = await auditor.listInstances({ tenant_id: tenantId });
    assert.ok(
      list.some((i) => i.id === instanceId),
      "auditor sees the planted instance",
    );
    const one = await auditor.getInstance(instanceId);
    assert.equal(one.state, "completed");
  });

  it("reads sequences, outputs, and the per-instance audit trail", async () => {
    const fetched = await auditor.getSequence(seq.id);
    assert.equal(fetched.id, seq.id);

    const outputs = await auditor.getOutputs(instanceId);
    assert.ok(outputs.some((o) => o.block_id === "s1"));

    const audit = await auditor.getAuditLog(instanceId);
    assert.ok(Array.isArray(audit), "audit trail readable");
  });

  it("reads approvals, handlers, and worker inventory", async () => {
    const inbox = await auditor.listApprovals({ tenant_id: tenantId });
    assert.ok(Array.isArray((inbox as any).items));

    const handlers = await fetch(`http://localhost:${server!.port}/handlers`, {
      headers: { "X-API-Key": auditorSecret },
    });
    assert.equal(handlers.status, 200, "GET /handlers is a read — allowed");

    const tasks = await auditor.listWorkerTasks({ tenant_id: tenantId });
    assert.ok(Array.isArray(tasks));
  });

  it("denies instance creation with 403", async () => {
    await assert.rejects(
      auditor.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      }),
      { status: 403 } as object,
    );
  });

  it("denies sequence writes and deletes with 403", async () => {
    await assert.rejects(
      auditor.createSequence(
        testSequence("au-deny", [step("s", "noop")], { tenantId }),
      ),
      { status: 403 } as object,
    );
    await assert.rejects(auditor.deleteSequence(seq.id), {
      status: 403,
    } as object);
    // The delete really did not happen — readable as both root and auditor.
    await auditor.getSequence(seq.id);
  });

  it("denies instance state and context mutation with 403", async () => {
    await assert.rejects(auditor.updateState(instanceId, "cancelled"), {
      status: 403,
    } as object);
    await assert.rejects(auditor.updateContext(instanceId, { data: {} }), {
      status: 403,
    } as object);
  });

  it("denies POST even on paths other capabilities would allow", async () => {
    // worker family path:
    await assert.rejects(auditor.pollWorkerTasks("h", "w-1"), {
      status: 403,
    } as object);
    // approver family path:
    await assert.rejects(auditor.sendSignal(instanceId, "cancel"), {
      status: 403,
    } as object);
  });

  it("denies key management writes with 403", async () => {
    await assert.rejects(
      auditor.createApiKey({ tenant_id: tenantId }),
      { status: 403 } as object,
    );
    await assert.rejects(auditor.revokeApiKey("ak_whatever"), {
      status: 403,
    } as object);
  });

  it("auditor reads leave no side effects — instance and sequence unchanged", async () => {
    // Re-read everything, then verify via root that nothing drifted.
    await auditor.getInstance(instanceId);
    await auditor.getOutputs(instanceId);
    await auditor.listInstances({ tenant_id: tenantId });

    const asRoot = await root.getInstance(instanceId);
    assert.equal(asRoot.state, "completed");
    const rootSeq = await root.getSequence(seq.id);
    assert.equal(rootSeq.version, seq.version);
  });
});
