/**
 * llm_call provider failover.
 *
 * BLOCKED BY: `llm_call` handler accepts a single provider per step; there
 *   is no `providers: [...]` array and no fallback loop.
 *   - `orch8-engine/src/handlers/llm.rs::handle_llm_call` at line 63 reads
 *     `ctx.params.get("provider")` (default `"openai"`) and dispatches to
 *     exactly one provider base URL. Grep confirms there is no `providers`
 *     key, no iteration, no per-provider error classification that would
 *     escalate to a second entry.
 *   - The only retry primitive available is `step.retry` (generic backoff),
 *     which re-runs the SAME provider with the SAME config. That is not
 *     failover; it is retry. Covered already by `retry-backoff.test.ts`.
 *
 * UNBLOCK: extend `orch8-engine/src/handlers/llm.rs::handle_llm_call` (or
 *   introduce a wrapping handler) to accept
 *     `providers: Vec<ProviderConfig>`
 *   and walk the list on per-provider retryable failures, recording which
 *   provider ultimately served the call (and which ones were tried) in the
 *   BlockOutput.
 *
 * Once unblocked, flip `it.skip` → `it`.
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

  // BLOCKED BY: `handle_llm_call` has a single `provider: string` param —
  //   no `providers: [...]` / failover mechanism. See file header.
  // UNBLOCK: accept a `providers` list in `handle_llm_call` and walk it on
  //   retryable failures, recording tried-providers and winner in the
  //   BlockOutput.
  it.skip(
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
      await client.waitForState(id, "failed", { timeoutMs: 20_000 });

      // Evidence of cascade: the failure BlockOutput (or the audit log)
      // should name BOTH providers in the documented order. Exact shape
      // depends on the unblock implementation — the block below covers
      // both likely surfaces:
      //   (a) BlockOutput carries { tried: ["openai", "anthropic"], ... }
      //   (b) Audit log has one entry per provider attempt
      const outputs = await client.getOutputs(id);
      const s1 = outputs.find((o) => o.block_id === "s1");
      const audit = await client.getAuditLog(id);

      const fromOutput = (() => {
        const tried = (s1?.output as { tried?: unknown } | undefined)?.tried;
        return Array.isArray(tried) ? (tried as string[]) : null;
      })();

      const fromAudit: string[] = audit
        .filter((row) => {
          const event = row.event as string | undefined;
          return event === "llm_provider_attempt";
        })
        .map((row) => String(row.provider ?? ""));

      const tried = fromOutput ?? fromAudit;
      assert.deepEqual(
        tried,
        ["openai", "anthropic"],
        "both providers should have been tried in the configured order",
      );
    },
  );
});
