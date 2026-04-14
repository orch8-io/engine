/**
 * gRPC sidecar plugin handler: `grpc://host:port/service.Method`.
 *
 * Without standing up a mock gRPC server the only testable path is the
 * failure case: the handler builds an HTTP/2 request to an unreachable
 * endpoint and the reqwest client returns a connect error. The handler
 * classifies that as `StepError::Retryable`; with no retry policy on the
 * step the scheduler transitions the instance straight to `failed` (see
 * `handle_retryable_failure` in scheduler.rs).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("gRPC Sidecar Plugin", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should fail step when the grpc endpoint is unreachable", async () => {
    const tenantId = `grpc-${uuid().slice(0, 8)}`;
    const namespace = "default";

    // Port 1 is always closed; connection is refused immediately.
    const seq = testSequence(
      "grpc-unreachable",
      [step("g1", "grpc://127.0.0.1:1/Svc.Method")],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 15_000,
    });
    assert.equal(final.state, "failed", `expected failed, got ${final.state}`);
  });
});
