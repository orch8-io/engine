/**
 * Approval Workflow — verifies approvals listing for wait_for_input steps.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Approval Workflow", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("list approvals empty initially", async () => {
    const approvals = await client.listApprovals();
    assert.ok(Array.isArray((approvals as any).items), "should have items array");
    assert.equal((approvals as any).total, 0, "should be empty initially");
  });

  it("approval for wait_for_input instance appears in list", async () => {
    const tenantId = `app-wait-${uuid().slice(0, 8)}`;
    const seq = testSequence("app-wait", [
      step("s1", "noop"),
      step("s2", "noop", {}, {
        wait_for_input: {
          prompt: "Approve?",
          choices: [
            { label: "Yes", value: "yes" },
            { label: "No", value: "no" },
          ],
        },
      }),
    ], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });

    const approvals = await client.listApprovals({ tenant_id: tenantId });
    assert.ok(
      (approvals as any).items.some((a: any) => a.instance_id === id),
      "waiting instance should appear in approvals",
    );
  });

  it("approval includes custom choices", async () => {
    const tenantId = `app-ch-${uuid().slice(0, 8)}`;
    const seq = testSequence("app-ch", [
      step("s1", "noop", {}, {
        wait_for_input: {
          prompt: "Choose",
          choices: [
            { label: "Red", value: "red" },
            { label: "Green", value: "green" },
            { label: "Blue", value: "blue" },
          ],
        },
      }),
    ], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });

    const approvals = await client.listApprovals({ tenant_id: tenantId });
    const approval = (approvals as any).items.find((a: any) => a.instance_id === id);
    assert.ok(approval, "approval should exist");
    assert.equal(approval.choices.length, 3, "should have 3 choices");
    assert.equal(approval.choices[0].value, "red");
  });

  it("approval is tenant-scoped", async () => {
    const tenantA = `app-a-${uuid().slice(0, 8)}`;
    const tenantB = `app-b-${uuid().slice(0, 8)}`;

    const seqA = testSequence("app-ta", [
      step("s1", "noop", {}, {
        wait_for_input: { prompt: "A?", choices: [{ label: "OK", value: "ok" }] },
      }),
    ], { tenantId: tenantA });
    const seqB = testSequence("app-tb", [
      step("s1", "noop", {}, {
        wait_for_input: { prompt: "B?", choices: [{ label: "OK", value: "ok" }] },
      }),
    ], { tenantId: tenantB });
    await client.createSequence(seqA);
    await client.createSequence(seqB);

    const { id: idA } = await client.createInstance({
      sequence_id: seqA.id,
      tenant_id: tenantA,
      namespace: "default",
    });
    const { id: idB } = await client.createInstance({
      sequence_id: seqB.id,
      tenant_id: tenantB,
      namespace: "default",
    });

    await client.waitForState(idA, "waiting", { timeoutMs: 5_000 });
    await client.waitForState(idB, "waiting", { timeoutMs: 5_000 });

    const listA = await client.listApprovals({ tenant_id: tenantA });
    assert.ok((listA as any).items.some((a: any) => a.instance_id === idA));
    assert.ok(!(listA as any).items.some((a: any) => a.instance_id === idB));
  });

  it("approval disappears after instance completes", async () => {
    const tenantId = `app-done-${uuid().slice(0, 8)}`;
    const seq = testSequence("app-done", [
      step("s1", "noop"),
    ], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 5_000 });

    const approvals = await client.listApprovals({ tenant_id: tenantId });
    assert.ok(
      !(approvals as any).items.some((a: any) => a.instance_id === id),
      "completed instance should not appear in approvals",
    );
  });

  it("approval includes prompt and timeout", async () => {
    const tenantId = `app-meta-${uuid().slice(0, 8)}`;
    const seq = testSequence("app-meta", [
      step("s1", "noop", {}, {
        wait_for_input: {
          prompt: "Please confirm",
          timeout: 300,
          choices: [{ label: "Confirm", value: "confirm" }],
        },
      }),
    ], { tenantId });
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 5_000 });

    const approvals = await client.listApprovals({ tenant_id: tenantId });
    const approval = (approvals as any).items.find((a: any) => a.instance_id === id);
    assert.ok(approval, "approval should exist");
    assert.equal(approval.prompt, "Please confirm");
    assert.ok(approval.deadline, "should have deadline when timeout is set");
  });
});
