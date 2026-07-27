import { after, before, describe, it } from "node:test";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import {
  caseDeepParallelTryLoopProducer,
  caseDeepScopeNestingCompilesDeterministically,
  caseFallbackVariantsCountedSeparately,
  caseFindingsSortedDeterministically,
  caseInputSchemaDepthLimitRejected,
  caseInputSchemaDepthWithinBudget,
  caseInputSchemaNodeLimitRejected,
  caseMultiLevelOutputsCollected,
  caseMultipleReferencesInOneTemplate,
  caseNestedProducerInGeneratedOutputs,
  caseRepeatedReferenceDeduped,
  caseRouterForEachNestedProducer,
  caseSagaInsideScopeInsideParallel,
  caseSameReferencePerConsumerCounted,
} from "../plansem_cases.ts";

describe("Plan semantics: nested references, dedup, budgets", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("parallel > try_catch > loop producer resolves at depth 4", async () => {
    await caseDeepParallelTryLoopProducer("plansem-nesting-001");
  });

  it("router route > for_each body producer resolves across edge kinds", async () => {
    await caseRouterForEachNestedProducer("plansem-nesting-002");
  });

  it("parallel > cancellation_scope > saga action producer resolves", async () => {
    await caseSagaInsideScopeInsideParallel("plansem-nesting-003");
  });

  it("one template string with two references counts both", async () => {
    await caseMultipleReferencesInOneTemplate("plansem-nesting-004");
  });

  it("repeated reference is interned once per consumer", async () => {
    await caseRepeatedReferenceDeduped("plansem-nesting-005");
  });

  it("same reference is checked per consumer", async () => {
    await caseSameReferencePerConsumerCounted("plansem-nesting-006");
  });

  it("fallback-carrying twin of an optional reference is accepted", async () => {
    await caseFallbackVariantsCountedSeparately("plansem-nesting-007");
  });

  it("findings sort by consumer, code, reference regardless of block order", async () => {
    await caseFindingsSortedDeterministically("plansem-nesting-008");
  });

  it("nested producers are flattened into generated bindings", async () => {
    await caseNestedProducerInGeneratedOutputs("plansem-nesting-009");
  });

  it("output schemas from every nesting level reach the generated model", async () => {
    await caseMultiLevelOutputsCollected("plansem-nesting-010");
  });

  it("40-level cancellation_scope nesting compiles deterministically", async () => {
    await caseDeepScopeNestingCompilesDeterministically("plansem-nesting-011");
  });

  it("input schema beyond the 32-level generation budget is rejected", async () => {
    await caseInputSchemaDepthLimitRejected("plansem-nesting-012");
  });

  it("input schema inside the depth budget compiles cleanly", async () => {
    await caseInputSchemaDepthWithinBudget("plansem-nesting-013");
  });

  it("input schema beyond the 4096-node generation budget is rejected", async () => {
    await caseInputSchemaNodeLimitRejected("plansem-nesting-014");
  });
});
