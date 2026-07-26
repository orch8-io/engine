import { after, before, describe, it } from "node:test";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import {
  caseApHandlerDraftCompiles,
  caseGrpcHandlerDraftCompiles,
  caseMixedPluginSchemesDeterministic,
  casePluginStepAsTypedProducer,
  caseRunInjectedCompositeRootFallsBack,
  caseRunMixedRootsApPluginFails,
  caseRunMixedRootsGrpcPluginFails,
  caseRunMixedRootsWasmPluginFails,
  caseWasmHandlerDraftCompiles,
} from "../plansem_cases.ts";

describe("Plan semantics: plugin schemes and dispatch decisions", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("ap:// steps compile with reference-checked params", async () => {
    await caseApHandlerDraftCompiles("plansem-plugin-001");
  });

  it("grpc:// steps compile with reference-checked params", async () => {
    await caseGrpcHandlerDraftCompiles("plansem-plugin-002");
  });

  it("wasm:// steps compile with reference-checked params", async () => {
    await caseWasmHandlerDraftCompiles("plansem-plugin-003");
  });

  it("plugin step with an output schema is a typed producer", async () => {
    await casePluginStepAsTypedProducer("plansem-plugin-004");
  });

  it("all three plugin schemes compile deterministically in one plan", async () => {
    await caseMixedPluginSchemesDeterministic("plansem-plugin-005");
  });

  it("mixed roots with an ap:// step dispatch through the tree evaluator", async () => {
    await caseRunMixedRootsApPluginFails("plansem-plugin-006");
  });

  it("mixed roots with a wasm:// step dispatch through the tree evaluator", async () => {
    await caseRunMixedRootsWasmPluginFails("plansem-plugin-007");
  });

  it("mixed roots with a grpc:// step dispatch through the tree evaluator", async () => {
    await caseRunMixedRootsGrpcPluginFails("plansem-plugin-008");
  });

  it("injected composite root falls back from the cached plan safely", async () => {
    await caseRunInjectedCompositeRootFallsBack("plansem-plugin-009");
  });
});
