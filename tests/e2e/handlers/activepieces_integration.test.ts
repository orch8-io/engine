/**
 * Activepieces handler (`ap://piece.action`).
 *
 * Sidecar URL is resolved once at first use from `ORCH8_ACTIVEPIECES_URL`
 * (default `http://127.0.0.1:50052/execute`). Without a running sidecar the
 * HTTP call fails with a connect error → `StepError::Retryable` → with no
 * retry policy on the step the scheduler marks the instance failed.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Activepieces Handler", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should fail step when the activepieces sidecar is unreachable", async () => {
    const tenantId = `ap-${uuid().slice(0, 8)}`;
    const namespace = "default";

    const seq = testSequence(
      "ap-no-sidecar",
      [step("a1", "ap://slack.send_message", { props: { text: "hello" } })],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    // Sidecar HTTP client has a 5s connect timeout. Give the instance enough
    // wall time for the connect to refuse and the scheduler to transition.
    const final = await client.waitForState(id, ["failed", "completed"], {
      timeoutMs: 20_000,
    });
    assert.equal(final.state, "failed", `expected failed, got ${final.state}`);
  });
});
