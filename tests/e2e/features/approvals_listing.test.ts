import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Approvals Listing", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should return empty list when no approvals pending", async () => {
    const tenantId = `t-${uuid().slice(0, 8)}`;
    const res = await client.listApprovals({ tenant_id: tenantId });
    assert.ok((res as any).items);
    assert.equal((res as any).items.length, 0);
  });

  it("should list pending approvals for wait_for_input step", async () => {
    const tenantId = "test";
    const reviewStep = step(
      "review",
      "human_review",
      { prompt: "Please approve" },
      {
        wait_for_input: {
          prompt: "Please approve this request",
          choices: [
            { label: "Approve", value: "approve" },
            { label: "Reject", value: "reject" },
          ],
        },
      },
    );
    const seq = testSequence("approval-test", [reviewStep]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Wait for instance to reach waiting state (human input needed).
    await client.waitForState(id, "waiting", { timeoutMs: 15_000 });

    // Now query approvals.
    const res = await client.listApprovals({ tenant_id: tenantId });
    const items = (res as any).items;
    assert.ok(items.length >= 1, "should have at least 1 approval");

    const approval = items.find((a: any) => a.instance_id === id);
    assert.ok(approval, "approval for our instance should exist");
    assert.equal(approval.block_id, "review");

    // Clean up: send signal to resolve the wait.
    await client.sendSignal(
      id,
      { custom: "human_input:review" } as unknown as string,
      { value: "approve" },
    );
  });

  it("should return empty approvals for random tenant", async () => {
    const randomTenant = `t-${uuid().slice(0, 8)}`;
    const res = await client.listApprovals({ tenant_id: randomTenant });
    const items = (res as any).items;
    assert.ok(Array.isArray(items));
    assert.equal(items.length, 0);
  });
});
