/**
 * Arithmetic expressions in the condition grammar — verifies +, -, *, /
 * alongside comparison operators behave correctly with context-driven
 * operands. Precedence follows standard math order.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

function router(
  id: string,
  routes: unknown[],
  defaultBlocks?: unknown,
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "router", id, routes };
  if (defaultBlocks) block.default = defaultBlocks;
  return block;
}

async function cond(
  name: string,
  condExpr: string,
  data: Record<string, unknown>,
): Promise<boolean> {
  const seq = testSequence(name, [
    router(
      "rt",
      [{ condition: condExpr, blocks: [step("hit", "log", { message: "y" })] }],
      [step("miss", "log", { message: "n" })],
    ) as Block,
  ]);
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: "test",
    namespace: "default",
    context: { data },
  });
  await client.waitForState(id, "completed", { timeoutMs: 10_000 });
  return new Set((await client.getOutputs(id)).map((o) => o.block_id)).has("hit");
}

describe("Arithmetic in conditions", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("addition: a + b == 10", async () => {
    assert.equal(
      await cond("ar-add", "context.data.a + context.data.b == 10", { a: 4, b: 6 }),
      true,
    );
    assert.equal(
      await cond("ar-add2", "context.data.a + context.data.b == 10", { a: 3, b: 6 }),
      false,
    );
  });

  it("subtraction: a - b > 0", async () => {
    assert.equal(
      await cond("ar-sub", "context.data.a - context.data.b > 0", { a: 9, b: 3 }),
      true,
    );
    assert.equal(
      await cond("ar-sub-neg", "context.data.a - context.data.b > 0", { a: 1, b: 5 }),
      false,
    );
  });

  it("multiplication: a * b >= 100", async () => {
    assert.equal(
      await cond("ar-mul", "context.data.a * context.data.b >= 100", { a: 10, b: 10 }),
      true,
    );
    assert.equal(
      await cond("ar-mul-low", "context.data.a * context.data.b >= 100", {
        a: 9,
        b: 10,
      }),
      false,
    );
  });

  it("division: a / b <= 2", async () => {
    assert.equal(
      await cond("ar-div", "context.data.a / context.data.b <= 2", { a: 10, b: 5 }),
      true,
    );
    assert.equal(
      await cond("ar-div-big", "context.data.a / context.data.b <= 2", {
        a: 20,
        b: 5,
      }),
      false,
    );
  });

  it("precedence: a + b * c > d", async () => {
    // 1 + 2*3 = 7, 7 > 5 -> true
    assert.equal(
      await cond(
        "ar-prec",
        "context.data.a + context.data.b * context.data.c > context.data.d",
        { a: 1, b: 2, c: 3, d: 5 },
      ),
      true,
    );
    // 1 + 2*3 = 7, 7 > 10 -> false
    assert.equal(
      await cond(
        "ar-prec-false",
        "context.data.a + context.data.b * context.data.c > context.data.d",
        { a: 1, b: 2, c: 3, d: 10 },
      ),
      false,
    );
  });

  it("chained: (a + b) * c == expected", async () => {
    // (1+2)*4 = 12
    assert.equal(
      await cond(
        "ar-chain",
        "(context.data.a + context.data.b) * context.data.c == 12",
        { a: 1, b: 2, c: 4 },
      ),
      true,
    );
  });
});
