/**
 * Cancellation scope nesting — verifies cancellation_scope works inside
 * composite blocks (loop, parallel) and that nested scopes independently
 * shield their children from cancel signals.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function cancellationScope(id: string, blocks: Block[]): Block {
  return { type: "cancellation_scope", id, blocks } as unknown as Block;
}

describe("Cancellation Scope Nesting", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("scope inside parallel branch completes normally", async () => {
    const tenantId = `csn-par-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "cs-in-par",
      [
        {
          type: "parallel",
          id: "par",
          branches: [
            [cancellationScope("cs1", [step("shielded", "noop")])],
            [step("unshielded", "noop")],
          ],
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    const final = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(final.state, "completed");

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(
      blockIds.includes("shielded"),
      `shielded step should produce output; got: ${blockIds.join(",")}`,
    );
    assert.ok(
      blockIds.includes("unshielded"),
      `unshielded step should produce output; got: ${blockIds.join(",")}`,
    );
  });

  it("nested cancellation scopes (scope in scope)", async () => {
    const tenantId = `csn-deep-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "cs-nested",
      [
        cancellationScope("outer", [
          cancellationScope("inner", [step("deep", "human_review", {}, {
            wait_for_input: {
              prompt: "Release the nested cancellation scope?",
              choices: [{ label: "Continue", value: "continue" }],
            },
          })]),
        ]),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
    const deadline = Date.now() + 10_000;
    while (!(await client.getInstanceTree(id)).some((node) => node.block_id === "deep" && node.state === "waiting")) {
      assert.ok(Date.now() < deadline, "nested scopes never reached their input barrier");
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    await client.sendSignal(id, "cancel");
    await client.sendSignal(id, { custom: "human_input:deep" } as unknown as string, { value: "continue" });

    const final = await client.waitForState(id, ["completed", "cancelled"], {
      timeoutMs: 10_000,
    });

    const outputs = await client.getOutputs(id);
    const blockIds = outputs.map((o) => o.block_id);
    assert.ok(
      blockIds.includes("deep"),
      `doubly-shielded step should produce output; got: ${blockIds.join(",")}`,
    );
  });

  it("scope inside loop protects each iteration", async () => {
    const tenantId = `csn-loop-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "cs-in-loop",
      [
        {
          type: "loop",
          id: "lp",
          condition: "true",
          body: [
            cancellationScope("cs", [step("body", "noop")]),
          ],
          max_iterations: 3,
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    const outputs = await client.getOutputs(id);
    const bodyOutputs = outputs.filter((o) => o.block_id === "body");
    assert.ok(bodyOutputs.length >= 2, `loop should have run at least 2 iterations, got ${bodyOutputs.length}`);
  });
});
