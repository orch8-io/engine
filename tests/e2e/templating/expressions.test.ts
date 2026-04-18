import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

const client = new Orch8Client();

/**
 * Helper: build a router block definition.
 */
function router(
  id: string,
  routes: unknown[],
  defaultBlocks?: unknown
): Record<string, unknown> {
  const block: Record<string, unknown> = { type: "router", id, routes };
  if (defaultBlocks) block.default = defaultBlocks;
  return block;
}

/**
 * Run a sequence with a single router block and the given data context,
 * and return the set of block_ids that produced outputs.
 */
async function runRouterWithContext(
  namePrefix: string,
  routes: unknown[],
  defaultBlocks: unknown,
  data: Record<string, unknown>
): Promise<Set<string>> {
  const seq = testSequence(namePrefix, [router("rt1", routes, defaultBlocks) as Block]);
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: "test",
    namespace: "default",
    context: { data },
  });
  const completed = await client.waitForState(id, "completed", {
    timeoutMs: 10_000,
  });
  assert.equal(completed.state, "completed");
  const outputs = await client.getOutputs(id);
  return new Set(outputs.map((o) => o.block_id));
}

describe("Expression Grammar (router conditions)", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("greater-than on integer: count > 5", async () => {
    const routes = [
      {
        condition: "context.data.count > 5",
        blocks: [step("big", "log", { message: "big" })],
      },
    ];
    const def = [step("small", "log", { message: "small" })];

    const hitBig = await runRouterWithContext("expr-gt-a", routes, def, {
      count: 10,
    });
    assert.ok(hitBig.has("big"));
    assert.ok(!hitBig.has("small"));

    const hitSmall = await runRouterWithContext("expr-gt-b", routes, def, {
      count: 3,
    });
    assert.ok(hitSmall.has("small"));
    assert.ok(!hitSmall.has("big"));
  });

  it("logical AND with >= and truthy bool: count >= 10 && active", async () => {
    const routes = [
      {
        condition: "context.data.count >= 10 && context.data.active",
        blocks: [step("yes", "log", { message: "yes" })],
      },
    ];
    const def = [step("no", "log", { message: "no" })];

    const hit = await runRouterWithContext("expr-and-a", routes, def, {
      count: 10,
      active: true,
    });
    assert.ok(hit.has("yes"));

    const miss = await runRouterWithContext("expr-and-b", routes, def, {
      count: 10,
      active: false,
    });
    assert.ok(miss.has("no"));
    assert.ok(!miss.has("yes"));

    const miss2 = await runRouterWithContext("expr-and-c", routes, def, {
      count: 9,
      active: true,
    });
    assert.ok(miss2.has("no"));
  });

  it('logical OR with string equality: mode == "fast" || force', async () => {
    const routes = [
      {
        condition: 'context.data.mode == "fast" || context.data.force',
        blocks: [step("go", "log", { message: "go" })],
      },
    ];
    const def = [step("wait", "log", { message: "wait" })];

    const hitByMode = await runRouterWithContext("expr-or-a", routes, def, {
      mode: "fast",
      force: false,
    });
    assert.ok(hitByMode.has("go"));

    const hitByForce = await runRouterWithContext("expr-or-b", routes, def, {
      mode: "slow",
      force: true,
    });
    assert.ok(hitByForce.has("go"));

    const miss = await runRouterWithContext("expr-or-c", routes, def, {
      mode: "slow",
      force: false,
    });
    assert.ok(miss.has("wait"));
    assert.ok(!miss.has("go"));
  });

  it("unary negation: !disabled", async () => {
    const routes = [
      {
        condition: "!context.data.disabled",
        blocks: [step("active_step", "log", { message: "active" })],
      },
    ];
    const def = [step("off_step", "log", { message: "off" })];

    const hitActive = await runRouterWithContext("expr-not-a", routes, def, {
      disabled: false,
    });
    assert.ok(hitActive.has("active_step"));

    const hitOff = await runRouterWithContext("expr-not-b", routes, def, {
      disabled: true,
    });
    assert.ok(hitOff.has("off_step"));
    assert.ok(!hitOff.has("active_step"));
  });

  it("arithmetic: a + b > 10", async () => {
    const routes = [
      {
        condition: "context.data.a + context.data.b > 10",
        blocks: [step("over", "log", { message: "over" })],
      },
    ];
    const def = [step("under", "log", { message: "under" })];

    const hitOver = await runRouterWithContext("expr-arith-a", routes, def, {
      a: 7,
      b: 5,
    });
    assert.ok(hitOver.has("over"));

    const hitUnder = await runRouterWithContext("expr-arith-b", routes, def, {
      a: 1,
      b: 2,
    });
    assert.ok(hitUnder.has("under"));
  });

  it("negative numbers: n < 0", async () => {
    const routes = [
      {
        condition: "context.data.n < 0",
        blocks: [step("neg", "log", { message: "neg" })],
      },
    ];
    const def = [step("nonneg", "log", { message: "nonneg" })];

    const hitNeg = await runRouterWithContext("expr-neg-a", routes, def, {
      n: -5,
    });
    assert.ok(hitNeg.has("neg"));

    const hitNonNeg = await runRouterWithContext("expr-neg-b", routes, def, {
      n: 3,
    });
    assert.ok(hitNonNeg.has("nonneg"));
  });

  it('string equality on nested path: user.role == "admin"', async () => {
    const routes = [
      {
        condition: 'context.data.user.role == "admin"',
        blocks: [step("admin_step", "log", { message: "admin" })],
      },
    ];
    const def = [step("user_step", "log", { message: "user" })];

    const hitAdmin = await runRouterWithContext("expr-str-a", routes, def, {
      user: { role: "admin" },
    });
    assert.ok(hitAdmin.has("admin_step"));

    const hitUser = await runRouterWithContext("expr-str-b", routes, def, {
      user: { role: "editor" },
    });
    assert.ok(hitUser.has("user_step"));
    assert.ok(!hitUser.has("admin_step"));
  });
});
