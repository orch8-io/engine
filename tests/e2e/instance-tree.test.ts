import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid, ApiError } from "./client.ts";
import { startServer, stopServer } from "./harness.ts";
import type { ServerHandle } from "./harness.ts";
import type { Block } from "./client.ts";

const client = new Orch8Client();

function parallel(id: string, branches: unknown[]): Record<string, unknown> {
  return { type: "parallel", id, branches };
}

describe("Instance Execution Tree + Audit Log", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("execution tree returns hierarchy of nodes with expected block_ids", async () => {
    const seq = testSequence("tree-par", [
      step("pre", "log", { message: "before" }),
      parallel("p1", [[step("a1", "noop")], [step("b1", "noop")]]) as Block,
      step("post", "log", { message: "after" }),
    ]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    const completed = await client.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    assert.equal(completed.state, "completed");

    const tree = (await client.getInstanceTree(id)) as unknown as Array<Record<string, any>>;
    assert.ok(Array.isArray(tree), "tree should be an array");
    assert.ok(tree.length > 0, "tree should be non-empty");

    // Every node should reference this instance.
    for (const node of tree) {
      assert.equal(
        node.instance_id,
        id,
        "node.instance_id should equal the instance ID"
      );
      assert.ok(node.block_id, "node should have a block_id");
      assert.ok(node.block_type, "node should have a block_type");
      assert.ok(node.state, "node should have a state");
    }

    // Collect block ids appearing in the tree.
    const blockIds = tree.map((n) => n.block_id);
    for (const expected of ["pre", "p1", "a1", "b1", "post"]) {
      assert.ok(
        blockIds.includes(expected),
        `tree missing expected block ${expected}, got ${JSON.stringify(blockIds)}`
      );
    }

    // All nodes should be terminal after completion.
    const terminalStates = new Set(["completed", "failed", "cancelled"]);
    assert.ok(
      tree.every((n) => terminalStates.has(n.state)),
      "all nodes should be terminal after instance completes"
    );
  });

  it("audit log returns non-empty entries with timestamps for a completed instance", async () => {
    const seq = testSequence("audit-basic", [step("s1", "noop")]);
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: "test",
      namespace: "default",
    });
    await client.waitForState(id, "completed");

    const entries = await client.getAuditLog(id);
    assert.ok(Array.isArray(entries), "audit log should be an array");
    assert.ok(entries.length > 0, "audit log should be non-empty");

    for (const entry of entries) {
      assert.ok(entry.id, "entry should have id");
      assert.equal(
        entry.instance_id,
        id,
        "entry.instance_id should match the instance"
      );
      assert.ok(entry.event_type, "entry should have event_type");
      // At minimum, timestamp should parse.
      assert.ok(
        !Number.isNaN(new Date((entry.created_at || entry.timestamp) as string).getTime()),
        "entry should have a parseable timestamp"
      );
    }
  });

  it("audit log returns 404 for nonexistent instance", async () => {
    const fakeId = uuid();
    try {
      await client.getAuditLog(fakeId);
      assert.fail("expected 404 for nonexistent instance");
    } catch (err) {
      assert.equal((err as ApiError).status, 404);
    }
  });
});
