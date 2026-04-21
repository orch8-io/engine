/**
 * Context Access Restriction — verifies that `context_access` on a step
 * limits which `context.data` fields are visible during template resolution.
 *
 * Plan #89: step with context_access restricts visible fields.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Context Access Restriction", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("allows access to permitted fields and hides restricted ones", async () => {
    const tenantId = `ctxa-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "ctx-access",
      [
        // Step 1: can see "allowed_key" via template.
        step("s1", "log", { message: "{{context.data.allowed_key}}" }, {
          context_access: {
            data: { fields: ["allowed_key"] },
            config: false,
            audit: false,
            runtime: false,
          },
        }),
        // Step 2: tries to read "secret_key" which is not in the field list.
        // Template resolution runs against the filtered context, so the
        // missing path resolves to null.
        step("s2", "log", { message: "{{context.data.secret_key}}" }, {
          context_access: {
            data: { fields: ["allowed_key"] },
            config: false,
            audit: false,
            runtime: false,
          },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: {
        data: {
          allowed_key: "visible_value",
          secret_key: "hidden_value",
        },
      },
    });

    await client.waitForState(id, "completed", { timeoutMs: 10_000 });

    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    const s2 = outputs.find((o) => o.block_id === "s2");

    assert.ok(s1, "s1 should have an output");
    assert.ok(s2, "s2 should have an output");

    assert.equal(
      (s1!.output as { message?: unknown }).message,
      "visible_value",
      "permitted field should resolve in template",
    );
    assert.equal(
      (s2!.output as { message?: unknown }).message,
      null,
      "restricted field should resolve to null because it is filtered from context",
    );
  });

  it("allows all fields when context_access is omitted (legacy default)", async () => {
    const tenantId = `ctxa-default-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "ctx-access-default",
      [
        step("s1", "log", { message: "{{context.data.secret_key}}" }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: {
        data: {
          secret_key: "should_be_visible",
        },
      },
    });

    await client.waitForState(id, "completed", { timeoutMs: 10_000 });

    const outputs = await client.getOutputs(id);
    const s1 = outputs.find((o) => o.block_id === "s1");
    assert.ok(s1, "s1 should have an output");
    assert.equal(
      (s1!.output as { message?: unknown }).message,
      "should_be_visible",
      "without context_access all fields should be visible",
    );
  });
});
