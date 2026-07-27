/**
 * Capability-scoped principals — the `operator` capability (the default).
 *
 * Operator is the full-access tenant grant: every route family passes the
 * capability gate (`capabilities_allow` short-circuits on Operator). The
 * only thing an operator key still may NOT do is manage API keys — that
 * requires the root/admin key. The suite also verifies two operator keys
 * for different tenants stay isolated from each other.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import {
  Orch8Client,
  testSequence,
  step,
  uuid,
} from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const ROOT_KEY = `root-principals-op-${uuid().slice(0, 8)}`;

describe("Principals — operator capability (default grant)", () => {
  let server: ServerHandle | undefined;
  let root: Orch8Client;
  let tenantA: string;
  let tenantB: string;
  let operatorA: Orch8Client;
  let operatorB: Orch8Client;
  let secretA: string;

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
    tenantA = `op-a-${uuid().slice(0, 8)}`;
    tenantB = `op-b-${uuid().slice(0, 8)}`;
    // Explicit operator for A, default (omitted → operator) for B.
    const keyA = await root.createApiKey({
      tenant_id: tenantA,
      capabilities: ["operator"],
    });
    const keyB = await root.createApiKey({ tenant_id: tenantB });
    secretA = keyA.secret;
    operatorA = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": secretA,
    });
    operatorB = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": keyB.secret,
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("runs a full sequence + instance lifecycle end-to-end", async () => {
    const seq = testSequence("op-life", [step("s1", "noop")], {
      tenantId: tenantA,
    });
    await operatorA.createSequence(seq);

    const { id } = await operatorA.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantA,
      namespace: "default",
    });
    const done = await operatorA.waitForState(id, "completed", {
      timeoutMs: 15_000,
    });
    assert.equal(done.state, "completed");

    const outputs = await operatorA.getOutputs(id);
    assert.ok(outputs.some((o) => o.block_id === "s1"));

    await operatorA.deleteSequence(seq.id);
    await assert.rejects(operatorA.getSequence(seq.id), {
      status: 404,
    } as object);
  });

  it("passes every route family: workers, approvals, handlers", async () => {
    const tasks = await operatorA.pollWorkerTasks("no_such_handler", "w-1");
    assert.deepEqual(tasks, []);

    const inbox = await operatorA.listApprovals({ tenant_id: tenantA });
    assert.ok(Array.isArray((inbox as any).items));

    const handlers = await fetch(`http://localhost:${server!.port}/handlers`, {
      headers: { "X-API-Key": secretA },
    });
    assert.equal(handlers.status, 200);
  });

  it("may mutate instance state (cancel a waiting instance)", async () => {
    const handler = `ext_op_${uuid().slice(0, 8)}`;
    const seq = testSequence("op-cancel", [step("call", handler)], {
      tenantId: tenantA,
    });
    await operatorA.createSequence(seq);
    const { id } = await operatorA.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantA,
      namespace: "default",
    });
    await operatorA.waitForState(id, "waiting", { timeoutMs: 10_000 });

    await operatorA.sendSignal(id, "cancel");
    const done = await operatorA.waitForState(id, ["cancelled", "failed"], {
      timeoutMs: 10_000,
    });
    assert.ok(
      ["cancelled", "failed"].includes(done.state),
      `operator-initiated cancel took effect (${done.state})`,
    );
  });

  it("still cannot manage API keys — admin gate is separate from capabilities", async () => {
    await assert.rejects(
      operatorA.createApiKey({ tenant_id: tenantA }),
      { status: 403 } as object,
    );
    await assert.rejects(operatorA.listApiKeys(tenantA), {
      status: 403,
    } as object);
    await assert.rejects(operatorA.revokeApiKey("ak_whatever"), {
      status: 403,
    } as object);
  });

  it("keeps two operator tenants isolated from each other", async () => {
    const seqA = testSequence("op-iso-a", [step("s", "noop")], {
      tenantId: tenantA,
    });
    const seqB = testSequence("op-iso-b", [step("s", "noop")], {
      tenantId: tenantB,
    });
    await operatorA.createSequence(seqA);
    await operatorB.createSequence(seqB);
    await operatorA.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantA,
      namespace: "default",
    });
    await operatorB.createInstance({
      sequence_id: seqB.id,
      tenant_id: tenantB,
      namespace: "default",
    });

    // Listings are force-scoped to the key's own tenant.
    const listA = await operatorA.listInstances({ tenant_id: tenantB });
    assert.ok(
      listA.length >= 1 && listA.every((i) => i.tenant_id === tenantA),
      "operator A sees its own rows even when asking for tenant B",
    );
    const listB = await operatorB.listInstances({ tenant_id: tenantA });
    assert.ok(
      listB.length >= 1 && listB.every((i) => i.tenant_id === tenantB),
      "operator B sees its own rows even when asking for tenant A",
    );

    // Direct cross-tenant reads are 404 (anti-enumeration convention).
    await assert.rejects(operatorB.getInstance(listA[0]!.id), {
      status: 404,
    } as object);

    // Cross-tenant sequence instantiation is denied.
    await assert.rejects(
      operatorB.createInstance({
        sequence_id: seqA.id,
        tenant_id: tenantB,
        namespace: "default",
      }),
      { status: 404 } as object,
    );
  });
});
