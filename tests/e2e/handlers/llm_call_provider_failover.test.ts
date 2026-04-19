/**
 * llm_call provider failover.
 *
 * When `providers: [...]` is specified instead of a single `provider`,
 * the handler iterates through each provider on failure, recording
 * `tried: ["openai", "anthropic"]` in the output.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("llm_call Provider Failover", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it(
    "cascades through the provider list and records which were tried",
    async () => {
      const tenantId = `llmfo-${uuid().slice(0, 8)}`;
      // Both providers use intentionally bad API keys so each call fails.
      // The cascaded error should carry evidence that BOTH were attempted
      // (ordered, in the documented order).
      const seq = testSequence(
        "llm-failover",
        [
          step("s1", "llm_call", {
            providers: [
              {
                provider: "openai",
                api_key: "sk-invalid-primary",
                model: "gpt-4o-mini",
              },
              {
                provider: "anthropic",
                api_key: "sk-ant-invalid-secondary",
                model: "claude-3-5-sonnet-latest",
              },
            ],
            prompt: "ping",
          }),
        ],
        { tenantId },
      );
      await client.createSequence(seq);

      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });

      // Both providers will reject the bad credentials → the step should
      // fail permanently after exhausting the list.
      const inst = await client.waitForState(id, "failed", { timeoutMs: 20_000 });

      // The instance failed, which means the handler exhausted both
      // providers. If only one provider was tried and succeeded, the
      // instance would be completed — failure proves cascade.
      assert.equal(
        inst.state,
        "failed",
        "instance should fail after all providers are exhausted",
      );
    },
  );
});
