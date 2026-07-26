import { after, before, describe, it } from "node:test";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import {
  caseAggregateReferencesAcrossRoots,
  caseCompositeOnlyRootsCompile,
  caseConsumerBeforeProducerRootOrder,
  caseEmptyBlocksCompile,
  caseThreeRootProducersResolve,
  caseTwelveRootFanOutDeterministic,
} from "../plansem_cases.ts";

describe("Plan semantics: multi-root sequences", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("three roots of different kinds all expose their producers", async () => {
    await caseThreeRootProducersResolve("plansem-roots-001");
  });

  it("consumers may precede producers in root order", async () => {
    await caseConsumerBeforeProducerRootOrder("plansem-roots-002");
  });

  it("reference checks aggregate across roots, conditions, and bodies", async () => {
    await caseAggregateReferencesAcrossRoots("plansem-roots-003");
  });

  it("a sequence with only composite roots compiles every producer", async () => {
    await caseCompositeOnlyRootsCompile("plansem-roots-004");
  });

  it("a 12-root typed fan-out compiles deterministically", async () => {
    await caseTwelveRootFanOutDeterministic("plansem-roots-005");
  });

  it("an empty block list compiles to an empty deterministic plan", async () => {
    await caseEmptyBlocksCompile("plansem-roots-006");
  });
});
