/**
 * Workflow Interceptors — verifies that before/after-step hooks and signal/
 * completion hooks registered on a sequence fire in the documented order
 * around block execution.
 *
 * BLOCKED BY: interceptor dispatch is not wired in the engine.
 *   - `orch8-types/src/interceptor.rs` defines `InterceptorDef` with
 *     `before_step`, `after_step`, `on_signal`, `on_complete`, `on_failure`.
 *   - `orch8-types/src/sequence.rs` accepts
 *     `interceptors: Option<InterceptorDef>` on `SequenceDefinition`.
 *   - Grep across `orch8-engine/src/` finds only defaults (`interceptors: None`
 *     at `orch8-engine/src/required_fields.rs:179`) and zero dispatch call
 *     sites: no `execute_interceptor`, no invocation from the scheduler,
 *     evaluator, or signal-delivery paths. The field is inert.
 *
 * UNBLOCK:
 *   - Add a dispatch call in `orch8-engine/src/scheduler.rs::execute_step_block`
 *     around `crate::handlers::step::execute_step_dry` (near line 1017):
 *     invoke `interceptors.before_step` before, `interceptors.after_step`
 *     after.
 *   - Add a dispatch call in the signal-delivery path (see
 *     `scheduler.rs::check_human_input` or the general signal inbox handler)
 *     for `on_signal`.
 *   - Add a dispatch call on the `Running → Completed` edge in
 *     `orch8-engine/src/lifecycle.rs::transition_instance` for `on_complete`
 *     (and the `Running → Failed` edge for `on_failure`).
 *   - Emit an observable artefact (e.g. a dedicated `BlockOutput` with
 *     `block_id = "_interceptor:<phase>:<step_id>"`) so E2E tests can assert
 *     ordering without reading logs.
 *
 * Once dispatch lands, flip `it.skip` → `it` on both cases and the
 * ready-to-run assertions below take over.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Workflow Interceptors", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // BLOCKED BY: no engine dispatch — `InterceptorDef` on SequenceDefinition
  //   is accepted by the API but ignored by the scheduler. No `before_step`
  //   / `after_step` invocation anywhere in `orch8-engine/src/`.
  // UNBLOCK: wire `interceptors.before_step` / `after_step` calls into
  //   `orch8-engine/src/scheduler.rs::execute_step_block` around the handler
  //   dispatch site (near line 1017). Emit a trace BlockOutput per invocation
  //   so E2E can observe order.
  it.skip(
    "fires before/after step interceptors around each step",
    async () => {
      const tenantId = `intercept-${uuid().slice(0, 8)}`;
      const seq = testSequence(
        "intercept-before-after",
        [step("s1", "log", { message: "hello" })],
        { tenantId },
      );
      // Interceptors are a sequence-level field. Passing via index signature
      // keeps the typed SequenceDef happy while the Rust side reads it.
      (seq as Record<string, unknown>).interceptors = {
        before_step: {
          handler: "log",
          params: { message: "BEFORE-{{step.id}}" },
        },
        after_step: {
          handler: "log",
          params: { message: "AFTER-{{step.id}}" },
        },
      };
      await client.createSequence(seq);

      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });

      await client.waitForState(id, "completed");

      // Once dispatch is wired, each step runs: before_step → step →
      // after_step. The interceptor emits a BlockOutput under a synthetic
      // block_id (proposed: "_interceptor:before:<step_id>") — adjust to
      // whatever the engine chooses when the feature lands.
      const outputs = await client.getOutputs(id);
      const beforeOut = outputs.find(
        (o) => o.block_id === "_interceptor:before:s1",
      );
      const afterOut = outputs.find(
        (o) => o.block_id === "_interceptor:after:s1",
      );
      const s1 = outputs.find((o) => o.block_id === "s1");
      assert.ok(beforeOut, "before_step interceptor should have run for s1");
      assert.ok(s1, "s1 should have run");
      assert.ok(afterOut, "after_step interceptor should have run for s1");

      // Ordering: before <= step <= after (by created_at).
      const beforeTs = Date.parse(
        (beforeOut as unknown as { created_at: string }).created_at,
      );
      const stepTs = Date.parse((s1 as unknown as { created_at: string }).created_at);
      const afterTs = Date.parse(
        (afterOut as unknown as { created_at: string }).created_at,
      );
      assert.ok(
        beforeTs <= stepTs,
        "before_step must not fire after the step",
      );
      assert.ok(
        stepTs <= afterTs,
        "after_step must not fire before the step",
      );
    },
  );

  // BLOCKED BY: same as above — signal-delivery and completion hooks have
  //   no dispatch site in the engine today.
  // UNBLOCK: wire `on_signal` into the signal-inbox delivery code path (see
  //   `orch8-engine/src/scheduler.rs` signal handling near
  //   `check_human_input`) and `on_complete` into
  //   `orch8-engine/src/lifecycle.rs::transition_instance` on the
  //   `Running → Completed` edge.
  it.skip(
    "fires on_signal and on_complete interceptors",
    async () => {
      const tenantId = `intercept-sig-${uuid().slice(0, 8)}`;
      const seq = testSequence(
        "intercept-on-signal",
        [step("s1", "log", { message: "x" })],
        { tenantId },
      );
      (seq as Record<string, unknown>).interceptors = {
        on_signal: {
          handler: "log",
          params: { message: "GOT-SIGNAL-{{signal.type}}" },
        },
        on_complete: {
          handler: "log",
          params: { message: "DONE-{{instance.id}}" },
        },
      };
      await client.createSequence(seq);

      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenantId,
        namespace: "default",
      });

      // Deliver a signal mid-flight. In the unblocked world, `on_signal`
      // fires synchronously on delivery.
      await client.sendSignal(id, "probe", { ping: true });

      await client.waitForState(id, "completed");

      // Proposed observable artefacts (exact block_id scheme TBD when the
      // engine author picks one — adjust when the dispatch code lands).
      const outputs = await client.getOutputs(id);
      const onSignal = outputs.find(
        (o) => o.block_id === "_interceptor:on_signal",
      );
      const onComplete = outputs.find(
        (o) => o.block_id === "_interceptor:on_complete",
      );
      assert.ok(onSignal, "on_signal interceptor should have produced a trace");
      assert.ok(
        onComplete,
        "on_complete interceptor should have produced a trace",
      );

      // Ordering: on_signal during run, on_complete at the end.
      const sigTs = Date.parse(
        (onSignal as unknown as { created_at: string }).created_at,
      );
      const doneTs = Date.parse(
        (onComplete as unknown as { created_at: string }).created_at,
      );
      assert.ok(
        sigTs <= doneTs,
        "on_signal must fire at-or-before on_complete",
      );
    },
  );
});
