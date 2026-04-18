/**
 * WASM plugin handler: exercise dispatch for `wasm://` handler prefix.
 *
 * The engine is gated behind `#[cfg(feature = "wasm")]` but `wasm` is in the
 * default feature set (see orch8-engine/Cargo.toml), so normal builds include
 * the handler. Dispatching to `wasm://missing-module` reaches
 * `cache::get_or_compile` which tries `Module::from_file` and returns
 * `StepError::Permanent` with "failed to load module".
 *
 * We can't bundle a real .wasm module in the test so we only assert the
 * failure shape. If a future build strips the `wasm` feature, this test will
 * still pass: the handler would route to the external worker queue (handler
 * not registered in-process) — in that case the instance stays in `waiting`
 * forever. We guard with a short timeout and, on timeout, skip gracefully.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("WASM Plugin Handler", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("should fail permanently when wasm module cannot be loaded", async () => {
    const tenantId = `wasm-${uuid().slice(0, 8)}`;
    const namespace = "default";

    // `wasm://nonexistent-module` — handler prefix is recognised but the
    // module path doesn't resolve to a file on disk.
    const seq = testSequence(
      "wasm-missing",
      [step("w1", "wasm://nonexistent-module-abc123")],
      { tenantId, namespace },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace,
    });

    // Two acceptable outcomes:
    //   - wasm feature enabled → handler returns Permanent → instance `failed`.
    //   - wasm feature disabled → handler routes to external worker queue
    //     (unregistered), instance sits in `waiting` indefinitely.
    // We short-timeout to distinguish.
    try {
      const final = await client.waitForState(id, ["failed", "completed"], {
        timeoutMs: 5_000,
      });
      assert.equal(final.state, "failed", `expected failed, got ${final.state}`);
    } catch {
      // Timeout — feature likely disabled. Verify the instance is still waiting
      // (handler was dispatched to worker queue, not terminal).
      const inst = await client.getInstance(id);
      assert.ok(
        inst.state === "waiting" || inst.state === "scheduled" || inst.state === "running",
        `unexpected state when wasm feature appears disabled: ${inst.state}`,
      );
    }
  });
});
