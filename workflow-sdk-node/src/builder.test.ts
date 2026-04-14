import { describe, expect, it } from "vitest";
import { workflow } from "./builder.js";
import { BlockDefinitionSchema, SequenceCreateSchema } from "./schema.js";

describe("workflow() builder", () => {
  it("emits a sequence create payload with snake_case discriminators", () => {
    const wf = workflow("onboarding")
      .step("send_welcome", "send_email", { template: "welcome" })
      .step("wait", "noop", {}, { delay: { duration: 60_000 } })
      .build();

    expect(wf.name).toBe("onboarding");
    expect(wf.namespace).toBe("default");
    expect(wf.blocks).toHaveLength(2);
    expect(wf.blocks[0]).toMatchObject({
      type: "step",
      id: "send_welcome",
      handler: "send_email",
    });
    expect(wf.blocks[1]).toMatchObject({
      type: "step",
      delay: { duration: 60_000 },
    });
  });

  it("validates through Zod — illegal shapes throw", () => {
    const wf = workflow("bad");
    expect(() =>
      wf
        .raw({
          // @ts-expect-error deliberately malformed
          type: "step",
          id: "broken",
        })
        .build(),
    ).toThrow();
  });

  it("builds parallel branches as BlockDefinition[][]", () => {
    const wf = workflow("parallel_demo")
      .parallel(
        "fan_out",
        (b) => b.step("a", "work_a"),
        (b) => b.step("b", "work_b"),
      )
      .build();

    expect(wf.blocks).toHaveLength(1);
    const parallel = wf.blocks[0];
    expect(parallel.type).toBe("parallel");
    if (parallel.type !== "parallel") throw new Error("type narrowing");
    expect(parallel.branches).toHaveLength(2);
    expect(parallel.branches[0][0]).toMatchObject({ type: "step", handler: "work_a" });
    expect(parallel.branches[1][0]).toMatchObject({ type: "step", handler: "work_b" });
  });

  it("builds try/catch/finally with nested blocks", () => {
    const wf = workflow("tx")
      .tryCatch(
        "safe_work",
        (b) => b.step("risky", "may_fail"),
        (b) => b.step("handle", "log_error"),
        (b) => b.step("cleanup", "release_lock"),
      )
      .build();

    const tc = wf.blocks[0];
    if (tc.type !== "try_catch") throw new Error("type narrowing");
    expect(tc.try_block[0]).toMatchObject({ handler: "may_fail" });
    expect(tc.catch_block[0]).toMatchObject({ handler: "log_error" });
    expect(tc.finally_block?.[0]).toMatchObject({ handler: "release_lock" });
  });

  it("encodes race semantics", () => {
    const wf = workflow("race_demo")
      .race(
        "first_win",
        "first_to_succeed",
        (b) => b.step("a", "call_api_a"),
        (b) => b.step("b", "call_api_b"),
      )
      .build();

    const race = wf.blocks[0];
    if (race.type !== "race") throw new Error("type narrowing");
    expect(race.semantics).toBe("first_to_succeed");
    expect(race.branches).toHaveLength(2);
  });

  it("builds router with a default branch", () => {
    const wf = workflow("route_demo")
      .router(
        "by_plan",
        [
          {
            condition: "data.plan == 'pro'",
            blocks: (b) => b.step("pro_flow", "upgrade_flow"),
          },
        ],
        (b) => b.step("basic_flow", "default_flow"),
      )
      .build();

    const router = wf.blocks[0];
    if (router.type !== "router") throw new Error("type narrowing");
    expect(router.routes[0].condition).toBe("data.plan == 'pro'");
    expect(router.default?.[0]).toMatchObject({ handler: "default_flow" });
  });

  it("builds for_each — defaults left to engine, fields preserved on the wire", () => {
    const wf = workflow("iter")
      .forEach("per_item", "data.items", (b) => b.step("do_it", "process"), {
        item_var: "thing",
        max_iterations: 50,
      })
      .build();

    const fe = wf.blocks[0];
    if (fe.type !== "for_each") throw new Error("type narrowing");
    expect(fe.collection).toBe("data.items");
    expect(fe.item_var).toBe("thing");
    expect(fe.max_iterations).toBe(50);
  });

  it("builds ab_split with weighted variants", () => {
    const wf = workflow("ab")
      .abSplit("experiment", [
        { name: "control", weight: 70, blocks: (b) => b.step("c", "original") },
        { name: "variant", weight: 30, blocks: (b) => b.step("v", "new_flow") },
      ])
      .build();

    const ab = wf.blocks[0];
    if (ab.type !== "ab_split") throw new Error("type narrowing");
    expect(ab.variants).toHaveLength(2);
    expect(ab.variants[0].weight).toBe(70);
  });

  it("cancellation_scope blocks nested execution from cancel", () => {
    const wf = workflow("safe")
      .cancellationScope("cleanup", (b) => b.step("release", "release_lock"))
      .build();

    const scope = wf.blocks[0];
    if (scope.type !== "cancellation_scope") throw new Error("type narrowing");
    expect(scope.blocks[0]).toMatchObject({ handler: "release_lock" });
  });

  it("sub_sequence carries input payload", () => {
    const wf = workflow("parent")
      .subSequence("invoke_child", "child_flow", {
        input: { user_id: "u1" },
        version: 3,
      })
      .build();

    const sub = wf.blocks[0];
    if (sub.type !== "sub_sequence") throw new Error("type narrowing");
    expect(sub.sequence_name).toBe("child_flow");
    expect(sub.version).toBe(3);
    expect(sub.input).toEqual({ user_id: "u1" });
  });

  it("produces JSON accepted by BlockDefinitionSchema round-trip", () => {
    const wf = workflow("round_trip")
      .step("s1", "http_request", { url: "https://example.com" })
      .step("s2", "log", {}, { retry: { max_attempts: 3, initial_backoff: 1000, max_backoff: 10000 } })
      .build();

    const json = JSON.parse(JSON.stringify(wf));
    const reparsed = SequenceCreateSchema.parse(json);
    expect(reparsed.blocks).toHaveLength(2);
    for (const block of reparsed.blocks) {
      expect(BlockDefinitionSchema.parse(block)).toBeTruthy();
    }
  });

  it("rejects unknown block types on parse", () => {
    expect(() =>
      BlockDefinitionSchema.parse({ type: "not_a_real_block", id: "x" }),
    ).toThrow();
  });

  it("respects custom namespace", () => {
    const wf = workflow("x", "tenant_a").step("s", "h").build();
    expect(wf.namespace).toBe("tenant_a");
  });
});
