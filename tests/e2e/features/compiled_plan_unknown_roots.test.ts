import { after, before, describe, it } from "node:test";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import { expectUnknownRoot } from "../compiled_plan_cases.ts";

describe("Compiled plan untyped roots", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("reports untyped state path phase", async () => {
    await expectUnknownRoot("compiled-unknown-001", "state", "phase");
  });

  it("reports untyped state path attempt", async () => {
    await expectUnknownRoot("compiled-unknown-002", "state", "attempt");
  });

  it("reports untyped state path status", async () => {
    await expectUnknownRoot("compiled-unknown-003", "state", "status");
  });

  it("reports untyped state path cursor", async () => {
    await expectUnknownRoot("compiled-unknown-004", "state", "cursor");
  });

  it("reports untyped state path checkpoint", async () => {
    await expectUnknownRoot("compiled-unknown-005", "state", "checkpoint");
  });

  it("reports untyped state path iteration", async () => {
    await expectUnknownRoot("compiled-unknown-006", "state", "iteration");
  });

  it("reports untyped state path branch", async () => {
    await expectUnknownRoot("compiled-unknown-007", "state", "branch");
  });

  it("reports untyped state path started_at", async () => {
    await expectUnknownRoot("compiled-unknown-008", "state", "started_at");
  });

  it("reports untyped state path deadline", async () => {
    await expectUnknownRoot("compiled-unknown-009", "state", "deadline");
  });

  it("reports untyped state path owner", async () => {
    await expectUnknownRoot("compiled-unknown-010", "state", "owner");
  });

  it("reports untyped state path lease", async () => {
    await expectUnknownRoot("compiled-unknown-011", "state", "lease");
  });

  it("reports untyped state path revision", async () => {
    await expectUnknownRoot("compiled-unknown-012", "state", "revision");
  });

  it("reports untyped state path mode", async () => {
    await expectUnknownRoot("compiled-unknown-013", "state", "mode");
  });

  it("reports untyped state path progress", async () => {
    await expectUnknownRoot("compiled-unknown-014", "state", "progress");
  });

  it("reports untyped state path result", async () => {
    await expectUnknownRoot("compiled-unknown-015", "state", "result");
  });

  it("reports untyped config path region", async () => {
    await expectUnknownRoot("compiled-unknown-016", "config", "region");
  });

  it("reports untyped config path environment", async () => {
    await expectUnknownRoot("compiled-unknown-017", "config", "environment");
  });

  it("reports untyped config path endpoint", async () => {
    await expectUnknownRoot("compiled-unknown-018", "config", "endpoint");
  });

  it("reports untyped config path timeout", async () => {
    await expectUnknownRoot("compiled-unknown-019", "config", "timeout");
  });

  it("reports untyped config path retries", async () => {
    await expectUnknownRoot("compiled-unknown-020", "config", "retries");
  });

  it("reports untyped config path feature", async () => {
    await expectUnknownRoot("compiled-unknown-021", "config", "feature");
  });

  it("reports untyped config path policy", async () => {
    await expectUnknownRoot("compiled-unknown-022", "config", "policy");
  });

  it("reports untyped config path namespace", async () => {
    await expectUnknownRoot("compiled-unknown-023", "config", "namespace");
  });

  it("reports untyped config path queue", async () => {
    await expectUnknownRoot("compiled-unknown-024", "config", "queue");
  });

  it("reports untyped config path pool", async () => {
    await expectUnknownRoot("compiled-unknown-025", "config", "pool");
  });

  it("reports untyped config path runtime", async () => {
    await expectUnknownRoot("compiled-unknown-026", "config", "runtime");
  });

  it("reports untyped config path version", async () => {
    await expectUnknownRoot("compiled-unknown-027", "config", "version");
  });

  it("reports untyped config path locale", async () => {
    await expectUnknownRoot("compiled-unknown-028", "config", "locale");
  });

  it("reports untyped config path currency", async () => {
    await expectUnknownRoot("compiled-unknown-029", "config", "currency");
  });

  it("reports untyped config path timezone", async () => {
    await expectUnknownRoot("compiled-unknown-030", "config", "timezone");
  });
});

