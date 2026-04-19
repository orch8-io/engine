/**
 * SubSequence failure, cancellation, and versioning.
 *
 * Engine contract (`orch8-engine/src/evaluator.rs`, SubSequence branch
 * around line 922):
 *   - When the child instance reaches a terminal state that is NOT
 *     `Completed`, the parent `SubSequence` node is marked failed via
 *     `fail_node`. That propagates to the parent instance terminal state
 *     (`failed`).
 *   - `get_sequence_by_name(tenant, ns, name, version)` returns the
 *     latest non-deprecated version when `version` is `None`, otherwise
 *     the exact version requested.
 *
 * Tests here pin the observable consequences — parent failure on child
 * failure/cancel, and correct child version resolution — so regressions
 * in either codepath surface at the E2E layer.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block, SequenceDef } from "../client.ts";

const client = new Orch8Client();

interface ChildInstance {
  id: string;
  parent_instance_id?: string | null;
  state?: string;
  sequence_id?: string;
}

describe("SubSequence Failure & Versioning", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("fails the parent SubSequence node when child fails", async () => {
    const tenantId = `ss-fail-${uuid().slice(0, 8)}`;

    // Child deliberately fails via the built-in `fail` handler.
    const child: SequenceDef = testSequence(
      "ss-fail-child",
      [step("boom", "fail", { message: "child error" })],
      { tenantId },
    );
    await client.createSequence(child);

    const parent: SequenceDef = testSequence(
      "ss-fail-parent",
      [
        {
          type: "sub_sequence",
          id: "ss",
          sequence_name: child.name,
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(parent);

    const { id: parentId } = await client.createInstance({
      sequence_id: parent.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // The child failing → parent SubSequence node fails → parent instance
    // terminates in `failed`.
    const final = await client.waitForState(parentId, "failed", {
      timeoutMs: 20_000,
    });
    assert.equal(final.state, "failed");

    // Sanity check: the child was actually created and reached a
    // terminal non-Completed state.
    const all = (await client.listInstances({ tenant_id: tenantId })) as ChildInstance[];
    const children = all.filter((i) => i.parent_instance_id === parentId);
    assert.equal(children.length, 1, "expected one child instance");
    assert.ok(
      ["failed", "cancelled"].includes(children[0]!.state ?? ""),
      `child should be terminal non-Completed, got: ${children[0]?.state}`,
    );
  });

  it("fails the parent when the child is cancelled mid-flight", async () => {
    const tenantId = `ss-cancel-${uuid().slice(0, 8)}`;

    // Child sleeps long enough that we can cancel it before completion.
    const child: SequenceDef = testSequence(
      "ss-cancel-child",
      [step("waiter", "sleep", { duration_ms: 15_000 })],
      { tenantId },
    );
    await client.createSequence(child);

    const parent: SequenceDef = testSequence(
      "ss-cancel-parent",
      [
        {
          type: "sub_sequence",
          id: "ss",
          sequence_name: child.name,
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(parent);

    const { id: parentId } = await client.createInstance({
      sequence_id: parent.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Wait until the child instance is actually scheduled by the engine AND
    // the parent has transitioned into `waiting`. Both conditions matter:
    //   - child must exist so we can cancel it
    //   - parent must be in `waiting` so the API-side "wake parent on child
    //     terminal transition" hook fires. If we cancel while the parent is
    //     still `scheduled` / `running` the wake is a no-op and the parent
    //     would sit waiting forever.
    const deadline = Date.now() + 10_000;
    let childInstance: ChildInstance | undefined;
    while (Date.now() < deadline) {
      const all = (await client.listInstances({ tenant_id: tenantId })) as ChildInstance[];
      childInstance = all.find((i) => i.parent_instance_id === parentId);
      const parentInst = all.find((i) => i.id === parentId);
      if (childInstance && parentInst?.state === "waiting") break;
      await new Promise((r) => setTimeout(r, 50));
    }
    assert.ok(childInstance, "child instance should be created by the engine");

    // Cancel the child directly. The parent's SubSequence node will detect
    // the terminal non-Completed state on its next tick and fail.
    await client.updateState(childInstance!.id, "cancelled");

    const final = await client.waitForState(parentId, "failed", {
      timeoutMs: 20_000,
    });
    assert.equal(final.state, "failed");
  });

  it("resolves the latest version of the target sequence when version is omitted", async () => {
    const tenantId = `ss-ver-latest-${uuid().slice(0, 8)}`;

    // Same `name` with two successive `version` values — the engine picks
    // the latest when the caller omits `version`.
    const baseName = `ss-target-${uuid().slice(0, 8)}`;

    const v1: SequenceDef = {
      ...testSequence("ignored", [step("s", "log", { message: "v1" })], { tenantId }),
      name: baseName,
      version: 1,
    };
    await client.createSequence(v1);

    const v2: SequenceDef = {
      ...testSequence("ignored", [step("s", "log", { message: "v2" })], { tenantId }),
      name: baseName,
      version: 2,
    };
    await client.createSequence(v2);

    const parent: SequenceDef = testSequence(
      "ss-ver-latest-parent",
      [
        {
          type: "sub_sequence",
          id: "ss",
          sequence_name: baseName,
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(parent);

    const { id: parentId } = await client.createInstance({
      sequence_id: parent.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(parentId, "completed", { timeoutMs: 20_000 });

    const all = (await client.listInstances({ tenant_id: tenantId })) as ChildInstance[];
    const children = all.filter((i) => i.parent_instance_id === parentId);
    assert.equal(children.length, 1);
    assert.equal(
      children[0]!.sequence_id,
      v2.id,
      "child should run against the latest version (v2)",
    );
  });

  it("pins to the requested explicit version even when newer versions exist", async () => {
    const tenantId = `ss-ver-pin-${uuid().slice(0, 8)}`;
    const baseName = `ss-target-${uuid().slice(0, 8)}`;

    const v1: SequenceDef = {
      ...testSequence("ignored", [step("s", "log", { message: "v1" })], { tenantId }),
      name: baseName,
      version: 1,
    };
    await client.createSequence(v1);

    const v2: SequenceDef = {
      ...testSequence("ignored", [step("s", "log", { message: "v2" })], { tenantId }),
      name: baseName,
      version: 2,
    };
    await client.createSequence(v2);

    const parent: SequenceDef = testSequence(
      "ss-ver-pin-parent",
      [
        {
          type: "sub_sequence",
          id: "ss",
          sequence_name: baseName,
          version: 1,
        } as unknown as Block,
      ],
      { tenantId },
    );
    await client.createSequence(parent);

    const { id: parentId } = await client.createInstance({
      sequence_id: parent.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(parentId, "completed", { timeoutMs: 20_000 });

    const all = (await client.listInstances({ tenant_id: tenantId })) as ChildInstance[];
    const children = all.filter((i) => i.parent_instance_id === parentId);
    assert.equal(children.length, 1);
    assert.equal(
      children[0]!.sequence_id,
      v1.id,
      "child should run against the pinned version (v1), not v2",
    );
  });
});
