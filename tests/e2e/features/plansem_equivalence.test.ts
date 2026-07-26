import { after, before, describe, it } from "node:test";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import {
  caseCompositeKeyOrderInvariant,
  caseDraftCompileIsPure,
  caseFlatKeyOrderInvariant,
  caseGeneratedBindingsEmbedSourceHash,
  caseSchemaEvolutionChangesHash,
  caseShaFormatAndDeterminism,
  caseStoredCompileDeterministic,
  caseStoredRoundTripEquivalence,
  caseTamperedHandlerChangesHash,
  caseTamperedParamValueChangesHash,
  caseTamperedProducerIdChangesHashAndReport,
  caseUnknownSequenceDataflowIs404,
  caseVersionIsolationFailClosed,
} from "../plansem_cases.ts";

describe("Plan semantics: equivalence and tamper evidence", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("source hash is 64-hex SHA-256 and compiles are deterministic", async () => {
    await caseShaFormatAndDeterminism("plansem-equiv-001");
  });

  it("flat definitions hash identically regardless of key order", async () => {
    await caseFlatKeyOrderInvariant("plansem-equiv-002");
  });

  it("composite definitions hash identically regardless of key order", async () => {
    await caseCompositeKeyOrderInvariant("plansem-equiv-003");
  });

  it("changing one param value changes the source hash", async () => {
    await caseTamperedParamValueChangesHash("plansem-equiv-004");
  });

  it("changing a handler changes the source hash", async () => {
    await caseTamperedHandlerChangesHash("plansem-equiv-005");
  });

  it("renaming a producer changes the hash and the report fails closed", async () => {
    await caseTamperedProducerIdChangesHashAndReport("plansem-equiv-006");
  });

  it("evolving an output schema rehashes while staying compatible", async () => {
    await caseSchemaEvolutionChangesHash("plansem-equiv-007");
  });

  it("stored and draft compilation of one definition are equivalent", async () => {
    await caseStoredRoundTripEquivalence("plansem-equiv-008");
  });

  it("two versions of one name compile against their own definitions", async () => {
    await caseVersionIsolationFailClosed("plansem-equiv-009");
  });

  it("draft compilation persists nothing", async () => {
    await caseDraftCompileIsPure("plansem-equiv-010");
  });

  it("stored compilation is deterministic across repeated GETs", async () => {
    await caseStoredCompileDeterministic("plansem-equiv-011");
  });

  it("compiling a stored plan for an unknown id is a 404", async () => {
    await caseUnknownSequenceDataflowIs404();
  });

  it("every generated binding embeds the canonical source hash", async () => {
    await caseGeneratedBindingsEmbedSourceHash("plansem-equiv-012");
  });
});
