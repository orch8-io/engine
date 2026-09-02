import { after, before, describe, it } from "node:test";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import {
  caseAbSplitVariantProducersResolve,
  caseCancellationScopeProducerResolves,
  caseForEachCollectionDeclared,
  caseForEachCollectionUndeclared,
  caseLoopBodyProducerAndConditionChecked,
  caseLoopBreakOnReferenceChecked,
  caseLoopConditionUndeclaredInput,
  caseParallelBranchMissingProducer,
  caseParallelBranchProducersResolve,
  caseRaceBranchProducerResolves,
  caseRouterRouteAndDefaultProducersResolve,
  caseRouterRouteConditionsChecked,
  caseSagaActionAndCompensationProducersResolve,
  caseSubSequenceInputCheckedAndOutputUnknown,
  caseTryCatchCatchConsumerWrongPath,
  caseTryCatchFinallyProducersResolve,
  caseWhenGuardReferenceChecked,
} from "../plansem_cases.ts";

describe("Plan semantics: composite block compilation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("parallel: producers in branch:0 and branch:1 resolve downstream", async () => {
    await caseParallelBranchProducersResolve("plansem-composite-001");
  });

  it("parallel: missing producer inside a branch fails closed", async () => {
    await caseParallelBranchMissingProducer("plansem-composite-002");
  });

  it("race: producer inside a race branch resolves", async () => {
    await caseRaceBranchProducerResolves("plansem-composite-003");
  });

  it("loop: body producer resolves and condition is checked", async () => {
    await caseLoopBodyProducerAndConditionChecked("plansem-composite-004");
  });

  it("loop: condition over undeclared input fails closed", async () => {
    await caseLoopConditionUndeclaredInput("plansem-composite-005");
  });

  it("loop: break_on expression is checked against producer schemas", async () => {
    await caseLoopBreakOnReferenceChecked("plansem-composite-006");
  });

  it("for_each: collection resolves against the input schema", async () => {
    await caseForEachCollectionDeclared("plansem-composite-007");
  });

  it("for_each: collection over undeclared input fails closed", async () => {
    await caseForEachCollectionUndeclared("plansem-composite-008");
  });

  it("router: route conditions are inspected as router-owned references", async () => {
    await caseRouterRouteConditionsChecked("plansem-composite-009");
  });

  it("router: route and default branch producers resolve", async () => {
    await caseRouterRouteAndDefaultProducersResolve("plansem-composite-010");
  });

  it("try_catch: try, catch, and finally producers all resolve", async () => {
    await caseTryCatchFinallyProducersResolve("plansem-composite-011");
  });

  it("try_catch: catch-region consumer with wrong path fails closed", async () => {
    await caseTryCatchCatchConsumerWrongPath("plansem-composite-012");
  });

  it("sub_sequence: input is checked; untyped output is a warning", async () => {
    await caseSubSequenceInputCheckedAndOutputUnknown("plansem-composite-013");
  });

  it("a_b_split: variant producers resolve downstream", async () => {
    await caseAbSplitVariantProducersResolve("plansem-composite-014");
  });

  it("cancellation_scope: scoped producer resolves downstream", async () => {
    await caseCancellationScopeProducerResolves("plansem-composite-015");
  });

  it("saga: action and compensation producers resolve downstream", async () => {
    await caseSagaActionAndCompensationProducersResolve("plansem-composite-016");
  });

  it("step: when-guard expressions are reference-checked", async () => {
    await caseWhenGuardReferenceChecked("plansem-composite-017");
  });
});
